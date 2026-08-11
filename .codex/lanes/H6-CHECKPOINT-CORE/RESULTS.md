# H6-CHECKPOINT-CORE results

## Implemented

- Fixed typed FM05 empty private checkpoint publish/readback/independent-restore/CAS flow.
- Fixed typed FM14 task-owned corruption and exact hash-mismatch rejection flow.
- Fixed typed FM15 disposable exact-readback then forced isolated-restore rejection flow.
- Durable pre-effect intent and per-stage/final receipt protocols with exact intent hashes.
- Deterministic candidate identities, exact current/previous/generation protection and
  idempotent response-loss resume through scenario-specific `ensure_*` effects.
- Bounded metadata-only receipt schema/example. No caller bytes, generic fault mode,
  canonical SQL, control/MCP/driver mutation or provider adapter was added.
- Injected tests emit only `CONTRACT_PASS`; no live Kaggle evidence is claimed.

## Validation

- `python -m compileall -q src tests`: PASS
- `ruff check .`: PASS
- configured strict `mypy`: PASS
- deterministic notebook drift: PASS
- repository validator: PASS, 3289 checks / 0 errors
- full `pytest -q`: PASS, 796 collected; 794 passed and 2 opt-in skips
- focused checkpoint/provider suite: PASS, 32 tests
- disposable PostgreSQL fencing/migration proof: PASS, 1 test
- `git diff --check`: PASS
