# Lane checkpoint-broker Results

## Status

committed implementation; deterministic acceptance complete; live disposable Kaggle canary pending integration/deploy

## Requirement IDs

- BROKER-01 through BROKER-14: split direct upload, encrypted token custody,
  bounded reconciliation, exact verifier, CAS promotion and current/previous retention
- BROKER-T01 through BROKER-T25: deterministic credential, metadata, replay,
  failure, restart/fencing and promotion coverage

## Branch

`agent/operational-mvp/checkpoint-broker`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/checkpoint-broker`

## Base SHA

`0f370d080056c1608d62b4390fa4bb8ae67adaf8` plus canonical central-adapter
primitive commit `47f0293b1d3ec295ead65da0e4869112420a8bee`

## Head SHA

Recorded by the implementation commit that includes this report.

## Files changed

- append-only mirrored control migration 025 for publication, file claims and
  append-only hash-only events
- central Kaggle adapter blob-start/finalize/reconcile primitives and contracts
- encrypted control-side upload ledger and broker service
- task-bound metadata-only control API and periodic recovery
- credential-free Notebook runtime publisher using direct HTTPS PUT
- production single-adapter assembly, installer key generation and control-only mount
- deterministic failure/replay/lease/retention/API/runtime tests
- publication schema/example and broker runbook

## Commands run

- `ruff check src tests scripts`
- `python -m compileall -q src tests scripts`
- `python scripts/validate_repository.py`
- `python scripts/create_notebooks.py --check`
- `python scripts/scan_tracked_secrets.py`
- `mypy`
- `bash -n deploy/control-plane/install.sh`
- focused broker/adapter/control/runtime/master suites
- `pytest -q`

## Tests / verification

- full local suite: PASS, 1,101 collected, two expected opt-in skips
- repository validator: PASS, 3,748 checks, zero errors
- Ruff, compileall, configured mypy, notebook drift, tracked-secret scan,
  installer syntax and diff check: PASS
- focused broker tests prove exact duplicate prepare/completion, terminal conflict,
  no-secret public projection, lost blob-start quarantine, lost Dataset-version
  response reconciliation without a second version, bounded unresolved response,
  verifier timeout/failure, partial upload, expired/stale epoch, exactly-once
  promotion, and current-to-previous retention
- API tests prove 256-KiB metadata cap, no binary envelope, exact run/operation/epoch
  binding, raw token non-disclosure and URL-free status
- master runtime tests reject all admitted Kaggle account environment variables
  and credential files before checkpoint activity
- unrelated Unix supervisor shutdown race was root-caused to the server closing the
  listener before the test wake connection; the test now accepts the valid
  `ConnectionRefusedError` terminal condition and passed 30 repeated runs

## Risks

- No live provider mutation, Dataset version, verifier run or cleanup was claimed in
  this code lane. The required disposable private Kaggle canary remains the next
  operational gate after integration and deployment.
- A blob-start response loss is deliberately terminal/quarantined because the official
  provider surface exposes no exact orphan-token reconciliation. Dataset-version
  response loss is bounded and exactly reconcilable.
- Rotating the broker AES key with unfinished claims makes those claims unrecoverable;
  the runbook therefore requires a zero-pending-publication rotation window.

## Merge notes

Cherry-pick the implementation commit after the adapter primitive implementation
`e22a415634f9c717de4a59bb6c1fe2a700acf9ec`; do not separately cherry-pick the worker
evidence-only commit unless its lane report is desired. Migration 025 is mirrored byte
for byte and must remain the next control migration.
