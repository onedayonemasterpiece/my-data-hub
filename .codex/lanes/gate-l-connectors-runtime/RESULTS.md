# Gate L connector runtime results

- Lane: `gate-l-connectors-runtime`
- Base SHA: `5589629b3449998cdc1459855f2bbabe19927378`
- Implementation SHA: `af7c4fa071c3224398475232940ff09a5f15a028`
- Final lane HEAD: the results-only child commit of the implementation SHA (reported in handoff)
- Live deployment: not performed

## Delivered

- Added a connector-only production API construction path with no static database URL,
  canonical data, or PGDATA on the devstand.
- Wired intake through `LedgerMasterResolver`, including existing ensure-master behavior,
  and `DirectoryConnectorSessionBroker` for an exact ACTIVE epoch.
- Corrected the ACTIVE master capability check to the advertised `sql` vocabulary rather
  than the declaration-only `connector-intake` alias.
- Added an exact injected verified-checkpoint coordinator Protocol/factory. Missing or
  malformed injection fails closed before intake mutation.
- Added persisted durability scanning and ACTIVE-epoch `canonical_committer` sessions so
  `CANONICAL_COMMITTED`, `CHECKPOINT_REQUESTED`, and `CHECKPOINTING` rows resume after
  process restart.
- Accepts `DURABLE_COMPLETE` only when coordinator evidence says `VERIFIED`, matches the
  current checkpoint head, and supplies exact checkpoint, manifest hash, and timestamp.
- Added opt-in production Compose wiring for connector intake using only the control
  ledger and read-only epoch credential directory.

## Evidence and commands

- `python -m compileall -q src tests scripts` — pass.
- `python -m ruff check src/my_data_hub/connectors src/my_data_hub/api tests/test_connectors.py tests/test_api.py tests/test_control_plane_deployment.py tests/test_architecture_invariants.py` — pass.
- Focused connector/API/Compose/architecture pytest — `42 passed`.
- Full `pytest -q` — pass (`2 skipped`; only pre-existing jsonschema deprecation warnings).
- `python scripts/create_notebooks.py --check` — pass, no drift.
- `python scripts/validate_repository.py` — expected integration failure only:
  `production profile must contain only control API, OAuth and opt-in remote MCP services`.
  The root-owned final-integrity lane was notified and explicitly retained ownership of
  updating this hard-coded service allow-list after merge.
- Focused mypy was attempted; it reports the existing project-wide Pydantic-as-`Any`,
  untyped psycopg, and decorated FastAPI errors. No clean mypy baseline exists for these
  modules; pytest/compile/ruff evidence above is authoritative for this lane.

## Honest residual dependencies / risks

1. The general master checkpoint coordinator still must inject the exact
   `request_verified_checkpoint(...)` / `checkpoint_status(...)` implementation. The
   production module intentionally does not invent an environment or control-ledger
   substitute and currently returns
   `CONNECTOR_VERIFIED_CHECKPOINT_COORDINATOR_UNAVAILABLE` before mutation.
2. The master Notebook credential registrar currently publishes only reader/operator
   epoch credentials. It must separately publish bounded `connector` and
   `canonical_committer` credential envelopes before the corresponding brokers can open
   sessions; until then the exact blockers are
   `CONNECTOR_EPOCH_CREDENTIAL_UNAVAILABLE` and
   `CONNECTOR_COMMITTER_EPOCH_CREDENTIAL_UNAVAILABLE`.
3. The Compose service is opt-in under profile `connectors`; the existing installer starts
   only its prior service list. Installer opt-in remains with the deployment/integrity
   owner. No service was deployed or started in this lane.
4. No live ACTIVE master, PostgreSQL role, or private checkpoint was available or used;
   no live durability claim is made.

## Changed files

- `compose.control-plane.yaml`
- `docs/16-data-connectors.md`
- `src/my_data_hub/api/app.py`
- `src/my_data_hub/api/connector_runtime.py`
- `src/my_data_hub/connectors/__init__.py`
- `src/my_data_hub/connectors/durability.py`
- `src/my_data_hub/connectors/errors.py`
- `src/my_data_hub/connectors/interfaces.py`
- `src/my_data_hub/connectors/postgres.py`
- `src/my_data_hub/connectors/runtime.py`
- `tests/test_api.py`
- `tests/test_architecture_invariants.py`
- `tests/test_connectors.py`
- `tests/test_control_plane_deployment.py`
- `.codex/lanes/gate-l-connectors-runtime/RESULTS.md`
