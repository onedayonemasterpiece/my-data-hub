# Lane L02-control-runtime Results

## Status
committed

## Requirement IDs
- R03
- R04

## Branch
`agent/operational-mvp/l02-control-runtime`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/operational-l02-control-runtime`

## Base SHA
`74f3bf457040f078e42b489252642dcf352760d4`

## Head SHA
Implementation head before this results-only commit: `95c2f6abdbe2b9cfc99ea7fb849144cc07b9f856`.
The final clean lane head is the commit containing this report and is also stated in the parent handoff.

## Files changed
- `control_migrations/001_control_ledger.sql`
- `control_migrations/002_registries_security_checkpoints.sql`
- `control_migrations/003_bounded_runtime_retention.sql`
- `examples/contracts/content-runtime-event.v1.example.json`
- `schemas/content-runtime-event.v1.schema.json`
- `src/my_data_hub/control_plane/clock.py`
- `src/my_data_hub/control_plane/ledger/**`
- `src/my_data_hub/orchestrator/master/**`
- `src/my_data_hub/runtime_sdk/**`
- `tests/control/**`
- `tests/runtime/**`
- `.codex/lanes/L02-control-runtime/RESULTS.md`

## Delivered behavior
- Versioned, atomic SQLite ledger migrations with WAL, foreign keys, 5-second busy timeout, `BEGIN IMMEDIATE`, parent `0700`, and database/WAL/SHM `0600` enforcement.
- Append-only operation/effect/event/audit histories with mutable projections, explicit bounded event retention receipts, durable idempotency, provider registry, exact run/attempt/source/version/epoch records, service/capability resolution, monotonic fencing epochs, resource leases, checkpoint candidate/current/previous HEAD, and hashed OAuth/runtime-token revocations only.
- Pure master lifecycle transitions plus a provider-neutral persist-before-side-effect coordinator, tri-state exact reconciliation (`found`/`absent`/`ambiguous`), concurrency collapse, crash-window recovery, callback dedupe/coalescing/64-KiB bounds, stale exact-identity rejection, lease expiry/fencing, exact-output terminal evidence, and scripted FakeKaggle.
- Generic `content-runtime-event/v1` SDK with HTTPS-only callbacks, bearer secret in headers only, recursive sanitization, fsync JSONL spool, bounded deterministic exponential backoff/jitter, restart replay, heartbeat coalescing/thread, resource lease events, terminal durability, artifact locators, and donor-envelope adaptation.
- 10,000 deterministic generated lifecycle sequences plus concurrency, crash-before/crash-after, duplicate/stale callback, checkpoint, OAuth, resource lease, retention, SDK outage/replay, secret-negative, schema, and packaged-migration tests.

## Commands run
- `uv venv .venv --python 3.12`
- `uv pip install --python .venv/bin/python -e '.[dev]'`
- `.venv/bin/ruff format ...`
- `.venv/bin/ruff check .`
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/pytest -q tests/control tests/runtime`
- `.venv/bin/pytest`
- `make validate notebooks`
- `uv build --wheel` plus wheel ZIP inspection for all three packaged control migrations
- `git diff 74f3bf457040f078e42b489252642dcf352760d4..HEAD --check`

## Tests / verification
- Targeted lane suite: `23 passed`.
- Full repository suite: `266 passed in 6.05s`.
- Ruff: `All checks passed!`.
- Compileall: passed.
- Repository validator: `2477` checks, `0` errors, `ok: true`.
- Notebook generator check: no drift.
- Wheel inspection: all `control_plane/ledger/sql/001..003` migrations packaged.
- No real provider calls or external mutations were performed.

## Risks
- The shared control-plane HTTP app/config/CLI were forbidden in this lane, so callback routing, bearer-header extraction, ledger path configuration, and health/reconciliation-loop startup still require integrator wiring.
- The real Kaggle adapter must implement `MasterRuntimeProvider` and preserve tri-state exact reconciliation. `AMBIGUOUS` must remain fail-closed; only proven `ABSENT` permits idempotent re-execution.
- The repository validator was forbidden, so the new schema/example mapping is proven by lane contract tests but is not yet added to the validator's example map.
- Deployment must configure the ledger on a writable persistent control-only volume. Packaged SQL fallback is present, but the existing read-only container wiring is integrator-owned.
- No claim is made about real Kaggle, PostgreSQL master, tunnel, checkpoint readback/restore, or remote OAuth readiness; those belong to dependent lanes and gates.

## Merge notes
- Cherry-pick implementation commits `457c7d16f341f555637709258570295877dd0599` and `95c2f6abdbe2b9cfc99ea7fb849144cc07b9f856`, then the results commit containing this file (or cherry-pick the final lane range).
- Instantiate `ControlLedger(path, clock=...)`; use `MasterCoordinator(ledger, provider)`; send callback bytes and the extracted bearer value to `accept_runtime_event(raw_body, header_token=...)`.
- Provider integration contract is exported from `my_data_hub.orchestrator.master`: `MasterRuntimeProvider`, `PlannedProviderEffect`, `ProviderEffectReceipt`, `EffectReconciliation`, and `ReconciliationStatus`.
- Runtime notebook entry points are exported from `my_data_hub.runtime_sdk`: `RuntimeClient`, `RuntimeEvent`, `RuntimeEventType`, and `RetryPolicy`.
- Keep the root and packaged migration copies byte-identical; `test_packaged_and_repository_control_migrations_are_identical` enforces this.
