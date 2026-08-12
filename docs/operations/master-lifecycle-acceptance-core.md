# Master lifecycle acceptance core (FM04/FM07–FM12/FM24)

This core is a **live-evidence protocol**, not proof that any scenario ran. It persists one
closed, task-owned command in control-ledger migration `016`; the command can be claimed
only by its exact ACTIVE `(operation, run, attempt, master, epoch)` through the authenticated
runtime endpoint.

## Admission and safety

- Operator admission requires the dedicated `acceptance:operate` scope. This scope is not
  added to the default reader catalog.
- Requests contain only task/idempotency/source/operation identities. They accept no SQL,
  byte payload, provider ref, duration, clock, or fault mode.
- FM04 and FM07 can be admitted only while the master is ABSENT/terminal. FM04 additionally
  rejects an existing checkpoint HEAD. A control executor must then perform the fixed boot
  or twenty same-key ensures and bind the one observed ACTIVE operation.
- Other scenarios bind the exact current ACTIVE operation atomically. Runtime command claim
  rechecks the current epoch and ACTIVE service.
- Receipt evidence is discriminated by command kind. A partial observation cannot validate
  as a successful receipt.

## Exact fixed assertions

| Scenario | Required live receipt assertions |
|---|---|
| FM04 | empty baseline, revision 0, row count 0, ACTIVE |
| FM07 | exactly 20 observations converging on one operation, provider run and epoch |
| FM08 | one suppressed callback, exact event/body hash, different control boot IDs, replay recovery |
| FM09 | duplicate exact event, stale runtime auth and stale epoch rejected, state hash unchanged |
| FM10 | at least 60 seconds observed wait, expired lease, H1 bounded operator DML rejected in rollback-only transaction, revision unchanged |
| FM11 | old runtime DRAINING first; renew/register/bounded-write/tunnel all rejected; later epoch ACTIVE |
| FM12 | closed gate, exact private checkpoint version/hash, readback and restore verified, HEAD promoted, STOPPED |
| FM24 | real monotonic 3600–5400 second soak, session/lease/tunnel rotations and stale session rejection in one epoch |

## Runtime transport

- `GET /internal/runtime/master-acceptance/{run_id}/{attempt_id}` claims the fixed command.
- `POST .../receipt` accepts at most 64 KiB and validates the exact command hash and runtime
  binding before terminalizing the task.
- `run_master(..., acceptance_effects=...)` exposes the task-owned production injection seam.
  The ordinary notebook does not enable an effect implementation implicitly. Consequently,
  scaffold/contract tests cannot write a `PASSED` live task; deployment must supply the real
  task-owned effects and the authenticated runtime receipt.

FM05/FM14/FM15 remain in `my_data_hub.checkpoints.acceptance`. This module does not replace
those checkpoint operations. No successful live scenario is claimed by this document.
