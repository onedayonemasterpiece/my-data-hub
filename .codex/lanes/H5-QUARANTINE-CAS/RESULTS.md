# H5-QUARANTINE-CAS results

## Identity

- Lane: `H5-QUARANTINE-CAS`
- Requirement IDs: `FM16_H5_QUARANTINE_PROJECTION_UNAVAILABLE`, `H3/H5_ADMISSION_TOCTOU`
- Requested base: `42cf59e097cda177fb3b194296824000eec5e1ef`
- Required migration-016 dependency integrated by cherry-pick: `35ce4e7`, `2712b36`, `cf1dc3d`
- Implementation head: `98fce030285ecb3c4f7c7fc0aacc53eca1afebc9`

## Delivered

- The durably committed rejected blogger import now raises
  `BloggerMigrationQuarantined` with a strict, bounded, metadata-only
  `BloggerQuarantineReceipt`. It binds request/operation SHA, export batch,
  run/attempt/master/epoch, exact lossless counts/hashes, and deterministic
  duplicate group/member/actor projections. It contains no source rows or owner
  decision.
- The master callback retries the exact receipt. Control migration 017 stores its
  canonical JSON and SHA-256 on the claimed request. Exact lost-response replay is
  idempotent; any altered receipt is denied and an SQLite trigger prevents later
  rewrite.
- Public request status hides the internal receipt and exposes the exact
  `quarantine_evidence`, `duplicate_review`, and `duplicate_review_inputs`
  projections needed to prepare a mode-0600 owner envelope.
- Replay validation requires the immutable quarantine receipt, exact source
  bindings, complete identity/member coverage, and the deterministic permissible
  canonical actor. It no longer incorrectly requires a successful checkpoint for
  a rejected v1 import.
- Blogger and embedding admission now perform operation/service/attempt/master/
  epoch/lease checks and insertion in one `BEGIN IMMEDIATE` transaction. Embedding
  additionally compares canonical revision and exact VERIFIED checkpoint HEAD in
  that transaction. Exact existing requests remain replayable after drain.
- Requests admitted immediately before a terminal drain are reconciled from
  `REQUESTED` to explicit `FAILED / ADMISSION_RUNTIME_TERMINAL_BEFORE_CLAIM`; no
  request is silently stranded.

## Migration 017

- Repository and packaged files are byte-identical.
- SHA-256:
  `8b66aed7ef21c03e37edd819a1ee7d4beeda232d2cad7578b0d9acdc8cdf15e2`.
- Effect: add nullable `quarantine_receipt_json` and checked
  `quarantine_receipt_sha256` to the metadata-only SQLite control ledger, plus an
  immutable-after-first-write trigger.
- PostgreSQL/canonical schema and roles: **no change**.
- Migration sequence is contiguous `001..017`; repository validation passed.

## Evidence and commands

- `PYTHONPATH=src .../python -m compileall -q src tests` — pass.
- `PYTHONPATH=src .../pytest -q` — pass at 100%; two existing opt-in skips.
- `MDH_RUN_DISPOSABLE_POSTGRES=1 PYTHONPATH=src .../pytest -q tests/bloggers/test_duplicate_resolution_postgres.py -x`
  — pass against disposable tmpfs PostgreSQL/pgvector 18.
- Focused blogger/control/ledger suite (`test_final_closure.py`,
  `test_duplicate_resolution.py`, `test_control_runtime_wiring.py`,
  `test_ledger_master.py`) — 78 passed.
- `PYTHONPATH=src .../python scripts/validate_repository.py` — pass,
  `3440` checks, `0` errors.
- Focused Ruff over every file changed by this lane — pass.
- `git diff --check` — pass.
- A broad ad-hoc mypy invocation is not a repository gate and reported the
  existing Pydantic/decorator/baseline errors (85 across the five large modules);
  no lane-specific clean baseline exists for comparison.

## Branch-only inherited gate note

Full-tree Ruff on this branch reports one inherited `RUF022` in
`src/my_data_hub/acceptance/__init__.py` from the required migration-016 dependency.
That file is outside this lane and was not edited. Integration fixed it
mechanically at `a546ec8`; after cherry-picking this lane onto that integration
head, full-tree Ruff is expected to be clean. Focused Ruff for all owned changes
passes.

## Changed files

- `control_migrations/017_blogger_quarantine_evidence.sql`
- `src/my_data_hub/control_plane/ledger/sql/017_blogger_quarantine_evidence.sql`
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- `src/my_data_hub/workloads/bloggers/importer.py`
- `src/my_data_hub/workloads/bloggers/master_stage.py`
- `schemas/region-talk-ydb-bloggers-quarantine-receipt.v1.schema.json`
- `examples/bloggers/region-talk-ydb-bloggers-quarantine-receipt.v1.example.json`
- `docs/operations/final-blogger-closure.md`
- `tests/bloggers/test_duplicate_resolution_postgres.py`
- `tests/bloggers/test_final_closure.py`
- `tests/control/test_control_runtime_wiring.py`
- `tests/control/test_ledger_master.py`

## Residual risks / handoff

- Status publishes only review facts; an owner must still make and protect the
  mode-0600 decision envelope. No synthetic decision or silent merge exists.
- The callback receipt is bounded by the existing 256 KiB metadata limit. A real
  266-row import producing more review metadata than that limit fails closed
  before callback rather than truncating evidence.
- Integration should cherry-pick both lane-owned commits `25d4c38` and `98fce03`,
  then this RESULTS commit, onto a head containing control migration 016 and the
  `a546ec8` lint correction.
