# R-H1 checkpoint retry reconciliation — RESULTS

## Scope

- Lane ID: `checkpoint_retry_reconciliation` (R-H1)
- Base SHA: `751febf21477cfc6b4fae720c5797756204db05d`
- Validated implementation head SHA: `de3e6c37266ba3616d69e528b20c512b7aa3fe3d`
- Branch: `agent/operational-mvp/checkpoint-retry-reconciliation`

## Outcome

Implemented crash-safe Kaggle checkpoint retry/reconciliation without adding a transport or moving checkpoint bytes through the devstand:

- Candidate checkpoint IDs, provider effect IDs/idempotency keys, verifier run IDs, and verifier intent timestamps are stable across retries.
- A provider-side dataset create/version that succeeded before a lost journal response is reconciled against the exact current numeric version and exact package hash; the adapter re-persists the same intent/receipt/claim without another Kaggle mutation.
- Permanent dataset versioning refreshes the exact durable current claim on every attempt and refuses to advance when Kaggle has moved beyond that claim unless the current bytes exactly match the pending candidate.
- Retry retains and reuses the already-built checkpoint package rather than rebuilding different bytes under the same candidate ID.
- Verifier retries reconcile the exact pushed notebook source/run and reuse an already validated local typed output receipt; no duplicate notebook push is issued.
- Promotion-response ambiguity is resolved from exact durable HEAD metadata. The returned marker explicitly records HEAD reconciliation and does not fabricate the original verifier Notebook receipt.
- A reused checkpoint ID with different current provider bytes fails closed instead of creating another version.

No control-plane endpoint or payload contract changed.

## Validation evidence

All commands ran from the isolated lane worktree with the integration virtualenv:

- `ruff check .` — passed.
- `python -m compileall -q src tests scripts` — passed.
- `python scripts/create_notebooks.py --check` — passed with `drift: []`.
- `python scripts/validate_repository.py` — passed, 2,850 checks, zero errors.
- `pytest -q tests/provider/test_checkpoint_runtime_wiring.py tests/provider/test_kaggle_adapter.py` — 18 passed.
- `pytest -q -rs` — full 491-test collection completed successfully: 490 passed, 1 intentionally skipped (`MDH_RUN_DISPOSABLE_POSTGRES` live proof gate).
- `git diff --check` — passed before commit.

Focused tests cover lost receipt/claim recovery with exactly one dataset version mutation, no advance from stale claims, deterministic verifier recovery with one push/download, package reuse and durable claim refresh across runtime retry, and lost promotion response recovery.

## Changed files

- `src/my_data_hub/checkpoints/kaggle_runtime.py`
- `src/my_data_hub/providers/kaggle/adapter.py`
- `tests/provider/test_checkpoint_runtime_wiring.py`
- `tests/provider/test_kaggle_adapter.py`
- `.codex/lanes/checkpoint_retry_reconciliation/RESULTS.md`

## Residual risks / operational notes

- Exact reconciliation intentionally fails closed if the current Kaggle version or package hash does not match the pending candidate. It never guesses and never retries a possibly-applied mutation.
- A process-level restart after losing the local package cannot reconstruct byte-identical PostgreSQL archives from metadata alone; the code refuses a duplicate/corrupt version if the same deterministic candidate ID is observed with different provider bytes. The normal master retry loop remains in-process and reuses the exact package.
- The disposable live PostgreSQL proof remains opt-in and was not run because `MDH_RUN_DISPOSABLE_POSTGRES=1` was not set; this lane does not add local PostgreSQL.
