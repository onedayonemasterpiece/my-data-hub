# H5-TRANSPORT results

## Scope

Closed the production transport and replay gap for explicit Region Talk blogger duplicate decisions without adding a database migration or moving raw YDB/business rows through the devstand.

## Implemented

- Added the strict `region-talk-blogger-duplicate-resolution-envelope.v1` metadata contract and `my-data-hub-blogger-migration-request.v2` replay request.
- Bound every request to an owner authorization identity/time, exact prior request and operation, prior request SHA-256, deterministic export batch, project, snapshot, expected count, source revision and pinned query hash.
- Required a new ACTIVE master operation, a terminal prior `BloggerMigrationQuarantined` request and the prior operation's current VERIFIED checkpoint before the loopback control endpoint accepts a replay.
- Bound the importer receipt to request hash, operation, deterministic export batch and exact claimed run/attempt/master/epoch before durable acknowledgement.
- Passed only the request-bound typed decisions from the Notebook claim to the transactional importer; a caller cannot substitute a different decision tuple.
- Added mode-0600, regular-file, 256-KiB-bounded CLI transport through `--duplicate-resolution-envelope`.
- Kept raw rows, original quarantine dispositions and prior requests append-only. No PostgreSQL/control migration, local PostgreSQL, provider mutation, credential or raw export was added.
- Documented the first quarantine/checkpoint/new-operation replay lifecycle and deterministic receipt-response-loss behavior.

## Validation (2026-08-11 UTC)

- `python -m compileall -q src tests`: PASS
- `ruff check .`: PASS
- configured `mypy`: PASS (5 configured strict files)
- `python scripts/validate_repository.py`: PASS, 3237 checks / 0 errors
- full `pytest -q`: PASS, 772 collected; 770 passed and 2 opt-in skips
- disposable PostgreSQL: `MDH_RUN_DISPOSABLE_POSTGRES=1 pytest -q tests/master/test_live_postgres.py tests/bloggers/test_duplicate_resolution_postgres.py`: PASS (2 tests)
- `git diff --check`: PASS

No live Kaggle/YDB run is claimed by this lane.
