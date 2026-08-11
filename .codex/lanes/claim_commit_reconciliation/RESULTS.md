# R-H1b / F1 durable claim reconciliation — RESULTS

## Scope

- Lane ID: `claim_commit_reconciliation` (R-H1b / F1)
- Base SHA: `eef876166ee097b128794d909aeb5ecca5a15c54`
- Validated implementation head SHA: `856a546a1cf0377f338fae5656bf03635f6bcdbd`
- Branch: `agent/operational-mvp/fix-claim-commit-reconciliation`

## Outcome

Closed the committed-claim/lost-response retry gap:

- Adapter-generated dataset and Notebook claim identities now bind `registered_at` to the immutable `ProviderEffectIntent.requested_at`, so reconciliation recreates the identical claim hash even when the adapter clock advances.
- The remote VERSION_DATASET claim endpoint accepts either the authorized prior-version-plus-one transition or an exact claim already persisted for the same effect/version/hash.
- An altered same-version claim is not treated as a replay and remains forbidden.
- Ledger persistence returns idempotently for byte-identical claim JSON and rejects a different claim for an already-bound effect or provider resource version.
- The commit-then-lost-response provider test advances the fake clock, commits the v2 claim before raising, reconciles successfully, and proves exactly one Kaggle `dataset_create_version` mutation.

No remote endpoint or payload shape changed.

## Validation

- `ruff check .` — passed.
- `python -m compileall -q src tests scripts` — passed.
- `python scripts/create_notebooks.py --check` — passed, no drift.
- `python scripts/validate_repository.py` — passed, 2,866 checks, zero errors.
- Focused pytest (`test_kaggle_adapter.py`, `test_control_journal.py`, `test_runtime_checkpoint_api.py`) — 12 passed.
- Full `pytest -q -rs` — 511 collected: 510 passed, 1 intentional opt-in live PostgreSQL skip.
- `git diff --check` — passed before commit.

## Changed files

- `src/my_data_hub/providers/kaggle/adapter.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/control_plane/ledger/store.py`
- `tests/provider/test_kaggle_adapter.py`
- `tests/provider/test_control_journal.py`
- `tests/control/test_runtime_checkpoint_api.py`
- `.codex/lanes/claim_commit_reconciliation/RESULTS.md`

## Risks / notes

- Exact replay requires full persisted claim JSON equality, not merely matching version or effect ID.
- Existing conflicting legacy rows are not rewritten; append-only authority remains intact.
- The live PostgreSQL test remains opt-in and this lightweight-control-plane lane did not create local PostgreSQL.
