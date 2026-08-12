# Gate L connector-plane results

## Scope

- Lane: `gate-l-connectors`
- Base SHA: `6b1cebdd1e81541669b66f63e6369905c58dcc11`
- Implementation head SHA: `5d163cc92b3172859a7bd53aefc2fa568bed1d27`
- Results evidence is committed as the immediate child of the implementation head; the final lane SHA is reported to the integrator because a commit cannot contain its own SHA.

## Requirement evidence

- **L1 — dynamic ACTIVE-master delivery:** `ActiveMasterConnectorRuntime` uses the existing `MasterResolver`, requires `ACTIVE` plus `connector-intake`, and asks an injected broker for an exact instance/epoch `connector` session. The API no longer constructs connector intake from a static database URL.
- **L2 — ensure-master:** an `ABSENT` resolution calls the resolver's durable `ensure_master` interface and returns `MASTER_ENSURE_REQUESTED` with operation identity before any data mutation.
- **L3 — registry policy / one vocabulary:** runtime enum, JSON envelope, PostgreSQL connector registry, and batch rows use `push`, `pull`, `artifact_handoff`, `trusted_database_landing`. Intake checks both registered mode and `allowed_delivery_modes` before acceptance.
- **L4 — spool retention:** acceptance is persisted but exact envelope/state remain pending. Only an attested `DURABLE_COMPLETE` receipt removes producer evidence.
- **L5 — checkpoint and replay:** typed checkpoint request/status/durability contracts, deterministic request identity, canonical revision binding, PostgreSQL lifecycle storage, terminal receipt hashing, exact replay, and changed-request/terminal conflict rejection are implemented.
- **L6 — Region Talk paused:** migration 0018 registers `region-talk-ydb-bloggers-v1` as `pull`, `paused`, `no_live_import=true`; its product is disabled. Pull execution stops before adapter invocation or spool mutation.
- **L7 — non-push interfaces:** callable orchestrator-pull, private-artifact, and trusted-landing interfaces verify mode/policy/integrity. Missing capabilities return typed blockers with `mutation_started=false` rather than PASS.

## Commands and evidence

- `ruff check .` — PASS.
- `python -m compileall -q src tests scripts/verify_connector_flow.py` — PASS (using the shared Python 3.12 development venv).
- `pytest -q` — PASS; full suite completed at 100%, with two existing `jsonschema.RefResolver` deprecation warnings and the repository's expected skips.
- Focused: `pytest tests/test_connectors.py tests/test_connector_gate_l.py tests/test_api.py tests/test_db_migrations.py -q` — PASS (`34 passed`).
- `python scripts/validate_repository.py` — PASS (`3731` checks, no errors/notes).
- Disposable PostgreSQL 18 + pgvector migration/bootstrap — PASS: migrations `0001..0018`, schema revision `18`, both delivery-mode checks validated, Region Talk connector observed as `pull|paused|no_live_import=true`, product observed disabled.
- Disposable live connector flow — acceptance/replay/conflict, semantic quarantine, bounded lock timeout, canonical commit/replay and restricted reader all passed. Overall verifier correctly returned nonzero because no external checkpoint gateway advanced the second batch beyond `CANONICAL_COMMITTED`; exact producer evidence remained and no durable receipt was emitted.

## Residual external blockers / risks

1. The base runtime credential registrar accepts only reader/operator credential envelopes. Until its owner deploys an epoch-bound `connector` credential and injects `DirectoryConnectorSessionBroker` (or an equivalent broker), operational intake returns `CONNECTOR_EPOCH_CREDENTIAL_UNAVAILABLE` before mutation.
2. No deployed root-owned connector checkpoint gateway was available in this lane. The live verifier honestly stops at `CANONICAL_COMMITTED` and fails until an external gateway returns an exact verified `DURABLE_COMPLETE` receipt. No checkpoint/provider authentication code was changed and no completion was fabricated.
3. The API factory is ready for resolver/broker injection, but the production process composition outside this lane must supply `ActiveMasterConnectorRuntime`; absence returns `CONNECTOR_ACTIVE_MASTER_RUNTIME_UNAVAILABLE` before mutation.

## Follow-up verifier audit

The bounded follow-up rechecked `scripts/verify_connector_flow.py` at lane tip. The
verifier persists the `.accepted.json` receipt only as nonterminal evidence, commits the
batch, polls the durability endpoint through the transport contract, excludes acceptance
receipts from the final receipt set, and requires both a delivery summary completion and
an exact final receipt whose state is `DURABLE_COMPLETE`. Its prior live observation at
`CANONICAL_COMMITTED` therefore remains an honest nonzero result with the spool retained.

Follow-up commands:

- `ruff check scripts/verify_connector_flow.py tests/test_connector_gate_l.py` — PASS.
- `python -m compileall -q scripts/verify_connector_flow.py` — PASS.
- `pytest tests/test_connector_gate_l.py tests/test_connectors.py -q` — PASS (`24 passed`).
- `python scripts/verify_connector_flow.py --help` — PASS; bounded
  `--durability-timeout-seconds` polling option exposed.

## Changed files

- `docs/16-data-connectors.md`
- `examples/contracts/connector-durability-receipt.v1.example.json`
- `schemas/connector-checkpoint-request.v1.schema.json`
- `schemas/connector-checkpoint-status.v1.schema.json`
- `schemas/connector-durability-receipt.v1.schema.json`
- `scripts/verify_connector_flow.py`
- `sql/admin/role_contract.sql`
- `sql/migrations/0018_connector_durable_delivery.sql`
- `src/my_data_hub/api/app.py`
- `src/my_data_hub/connectors/__init__.py`
- `src/my_data_hub/connectors/contracts.py`
- `src/my_data_hub/connectors/durability.py`
- `src/my_data_hub/connectors/interfaces.py`
- `src/my_data_hub/connectors/postgres.py`
- `src/my_data_hub/connectors/runtime.py`
- `src/my_data_hub/connectors/spool.py`
- `src/my_data_hub/connectors/transport.py`
- `tests/test_api.py`
- `tests/test_connector_gate_l.py`
- `tests/test_connectors.py`
