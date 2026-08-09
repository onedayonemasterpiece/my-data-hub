# L03-connectors results

## Scope

- Lane: `L03-connectors`
- Requirement: `R07` core connector runtime and producer delivery
- Base SHA: `0b6b7311081bdfecdd4f3004e5d6842a42f64253`
- Implementation head SHA: `70e935535f238e6cdd72ac558baa3c41dcabde45`
- Integration/API/SQL/MCP work was intentionally left to the integrator.

## Delivered evidence

- Strict `my-data-hub-data-connector.v1` runtime models reject unknown fields,
  duplicate JSON keys, oversized envelopes, invalid versions, naive timestamps,
  inconsistent observation periods, payload ambiguity, record-count mismatches, and
  payload/artifact hash mismatches.
- Canonical inline payload hashing implements JCS/RFC 8785 ordering and number-format
  rules for the interoperable I-JSON subset. Exact submitted bytes and both exact-byte
  and canonical-envelope SHA-256 values remain available to persistence.
- Acceptance repository protocol specifies one-transaction PostgreSQL behavior without
  providing generic SQL or writing canonical tables. Replay classification returns the
  existing receipt only for the same identity, batch, payload hash, and canonical
  envelope hash; mutations become explicit quarantine evidence.
- Intake service enforces authenticated connector binding before the repository call.
- File spool atomically writes and fsyncs exact envelope bytes and retry state, recovers
  an interrupted state write, persists the attesting receipt before deleting pending
  evidence, retains terminal failures in local quarantine, and retries ambiguous
  transport failures with bounded exponential backoff.
- Deterministic synthetic producer derives a stable UUID/idempotency identity from the
  reporting date, timezone, and sequence. The runnable script can perform enqueue-only
  outage exercises or HTTP delivery without logging its bearer token.
- Targeted tests prove example-envelope compatibility, strict validation, exact replay,
  conflict quarantine, connector/principal binding, restart/outage eventual delivery,
  exact-byte reuse, receipt durability, terminal local quarantine, and deterministic
  synthetic identity.

## Commands run

```text
python3 -m compileall src tests
uv run --extra dev pytest -q tests/test_connectors.py
uv run --extra dev ruff check src/my_data_hub/connectors scripts/run_synthetic_connector.py tests/test_connectors.py
.venv/bin/ruff check src/my_data_hub/connectors scripts/run_synthetic_connector.py tests/test_connectors.py
.venv/bin/pytest -q tests/test_connectors.py
.venv/bin/python -m compileall -q src tests
.venv/bin/pytest -q
uv run --extra dev python scripts/run_synthetic_connector.py --spool-dir /tmp/<temporary> --reporting-date 2026-08-09 --enqueue-only
git diff --check
```

Results: targeted connector tests passed (`12` tests); the full repository test suite
passed; compileall, Ruff, and diff whitespace checks passed; the synthetic enqueue-only
smoke run produced a stable batch/idempotency identity and durable spool entry.

The initial system-Python test attempt reported that `pytest` was not installed. The
project dev environment was then created with `uv`; all subsequent targeted and full
checks passed.

## Changed files

- `src/my_data_hub/connectors/__init__.py`
- `src/my_data_hub/connectors/contracts.py`
- `src/my_data_hub/connectors/repository.py`
- `src/my_data_hub/connectors/service.py`
- `src/my_data_hub/connectors/spool.py`
- `src/my_data_hub/connectors/synthetic.py`
- `src/my_data_hub/connectors/transport.py`
- `scripts/run_synthetic_connector.py`
- `tests/test_connectors.py`
- `.codex/lanes/L03-connectors/RESULTS.md`

## Risks and deferred integration

- The live PostgreSQL repository, migration, authenticated API route, receipt schema
  integration, and live PostgreSQL verification are deliberately outside this lane.
  The integrator must implement the repository protocol with unique constraints and
  row locking in a single transaction and run the required live PostgreSQL tests.
- Artifact envelopes validate their declared artifact hash immediately; artifact-byte
  download/scanning and manifest record-count verification remain a bounded downstream
  integration step. The runtime accepts an independently validated artifact count when
  that evidence is available.
- The provisional HTTP transport expects a receipt matching `ConnectorReceipt`; the
  integrator must keep the API response shape aligned when exposing the intake route.
- The file spool is designed for one producer process per spool directory. If a future
  deployment runs multiple delivery processes against one directory, it should add an
  inter-process claim/lock before enabling that topology.
