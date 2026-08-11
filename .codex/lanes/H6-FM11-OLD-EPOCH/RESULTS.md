# H6-FM11-OLD-EPOCH results

## Implemented

- Added a concrete, task-bound `OldEpochDenialPort` with a fixed monotonic
  900-second TTL and sanitized response-loss cache.
- Added metadata-only old runtime and exact replacement/checkpoint contexts.
  The type system cannot carry a raw bearer, DSN, password, certificate or
  private key; only token/principal/key hashes, UUID handles and IDs remain.
- Added four narrow production client boundaries for runtime heartbeat/renewal,
  credential registration/binding, H1 bounded write, and tunnel
  lease/certificate denial.
- Added `PsycopgRetiredBoundedWriteClient`, which resolves a pre-opened
  restricted session by UUID handle and executes only the mandatory epoch
  assertion plus fixed no-row `hub.project` UPDATE. It requires SQLSTATE 55000,
  PostgreSQL rollback-only state and unchanged canonical revision.
- Added exact observation validation, protected-resource release after the
  first physical probe, identical cached retry after response loss, sanitized
  receipt hashes, JSON Schema/example, tests and composition documentation.

## Evidence status

No live FM11 result is claimed. Injected client tests prove the admission and
receipt contract only. Production still must compose all four narrow clients
with deployed admission paths and execute a real old-to-new epoch rotation.

## Gates

- `pytest`: PASS, 958 passed / 2 skipped.
- Focused FM11 and master-production tests: PASS, 28 tests.
- `ruff check .`: PASS.
- `python -m compileall -q src tests`: PASS.
- `python scripts/validate_repository.py`: PASS, 3,531 checks / zero errors.

No live Kaggle, deployed control-plane, or PostgreSQL claim is made by these
gates.
