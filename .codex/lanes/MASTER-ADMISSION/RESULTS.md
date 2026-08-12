# Atomic master-admission results

## Scope and base

- Lane: `MASTER-ADMISSION`
- Isolated branch/worktree: `agent/master-admission` / `master-admission`
- Exact base: `ed95ee2f9503650c08bb6bb56d1444fe46414cb8`
- Scope: control-ledger master admission, coordinator integration, and focused lifecycle tests only.
- No MCP catalog, H6 scenario API, deployment, provider mutation, PostgreSQL migration, or live environment change was performed.

## Root cause

`MasterCoordinator.ensure_master()` previously called the generic operation insertion path with
`allocate_epoch_for="postgres-master"`. That transaction incremented `service_epochs` and fenced
older service projections without checking for a distinct incomplete lifecycle, ACTIVE/DRAINING
service, or verified STOPPED checkpoint handoff. Two distinct concurrent keys could therefore both
allocate epochs and execute provider effects; a distinct request could also fence a still-ACTIVE
master before its PostgreSQL write gate had drained.

## Implemented contract

- Added `ControlLedger.ensure_master_operation()`, whose single SQLite `BEGIN IMMEDIATE`
  transaction:
  - returns an exact same-idempotency-key replay before any admission mutation;
  - rejects every distinct prior nonterminal state (`REQUESTED`, `STARTING`, `RESTORING`,
    `REGISTERING`, `ACTIVE`, `DRAINING`, `CHECKPOINTING`, `CHECKPOINT_FAILED`);
  - rejects any remaining `ACTIVE` or `DRAINING` service projection;
  - validates that the latest master-operation epoch equals the durable service epoch;
  - permits a new epoch from `FAILED`, `FENCED`, or `ORPHANED` after the above safety checks;
  - permits a successor to `STOPPED` only when current HEAD is VERIFIED and bound to that exact
    operation, master instance, and epoch;
  - atomically revalidates `forced-rotation:<request-id>` against the REQUESTED rotation operation,
    exact HEAD generation/checkpoint, latest source operation, source epoch/master, and VERIFIED
    STOPPED handoff;
  - only after all checks allocates one epoch and inserts the REQUESTED operation/log.
- Added typed `MasterAdmissionRejected` without exposing provider or credential detail.
- Wired `MasterCoordinator` exclusively to the atomic admission method.
- Preserved same-key crash/retry behavior and the existing single-provider-effect journal.

## Test coverage

`tests/control/test_ledger_master.py` now proves:

- 20 concurrent same-key requests still converge on one operation, epoch, and physical effect set;
- two distinct concurrent keys admit exactly one winner and create no losing attempt/effect;
- a distinct request cannot fence an ACTIVE service or change epoch/attempt/effects;
- every nonterminal lifecycle state blocks a distinct epoch;
- `FAILED`, `FENCED`, and `ORPHANED` permit the exact next epoch;
- `STOPPED` without its verified checkpoint is rejected;
- checkpoint verification alone is insufficient while the service is still DRAINING; the exact
  runtime-terminal handoff is required;
- a forced rotation cannot reuse an older stopped checkpoint after an intervening failed epoch;
- stale callback fencing coverage now first performs an explicit terminal fence, rather than
  relying on the unsafe distinct-ensure behavior.

## Validation evidence

All commands ran in the isolated worktree against this implementation.

- `python -m compileall -q src tests` — PASS.
- `ruff check .` — PASS.
- configured `mypy --config-file pyproject.toml` target set — PASS (5 source files).
- focused ledger/control/provider runtime suites — PASS (69 tests before the final additions; full
  ledger file then passed 37 tests, and final full suite covers all additions).
- `pytest -q` — PASS; `804` collected, two expected opt-in skips, only the two pre-existing
  `jsonschema.RefResolver` deprecation warnings.
- `python scripts/create_notebooks.py --check` — PASS, zero drift.
- `python scripts/validate_repository.py` — PASS (`3249` checks, zero errors/notes).
- `git diff --check` — PASS.

No disposable PostgreSQL run was required because this lane changes no PostgreSQL migration or
physical data-plane SQL. The repository's opt-in PostgreSQL tests remain unchanged and the full
suite exercised their expected skip gate.
