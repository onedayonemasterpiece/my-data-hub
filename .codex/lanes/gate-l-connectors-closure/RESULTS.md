# Gate L connector internal closure results

- Lane: `gate-l-connectors-closure`
- Base SHA: `7a1cad5da8aad7c99f4da6f24160f90d5d6dd775`
- Implementation SHA: `a80a6f648c1d8b4d8bf1887592a16810ffc5419c`
- Live deployment: not performed

## Delivered

- Added mirrored contiguous control migration 027 for metadata-only connector checkpoint
  requests, exact replay identity, ACTIVE operation/epoch binding, task-bound claim,
  terminal verified evidence, and restart recovery.
- Production connector runtime now injects `ControlLedgerVerifiedCheckpointCoordinator`.
  It does not invent or duplicate checkpoint upload behavior: the ACTIVE master claims the
  durable request, drains through the existing terminal `RuntimeCheckpointCoordinator`, and
  uses `BrokeredCheckpointUploadService` for provider upload, restore verification, and HEAD
  promotion. `DURABLE_COMPLETE` is returned only from the exact current VERIFIED head whose
  manifest protects at least the requested canonical revision.
- Master activation publishes opt-in short-lived `connector` and
  `canonical_committer` credentials, bound to the exact task, ACTIVE epoch, lease, group
  role, TLS CA, and host tunnel. Connector intake/commit do not use a static database URL.
- Durability reconciliation now commits persisted `ACCEPTED` batches with the bounded
  canonical committer, then progresses persisted committed work through checkpoint demand.
  Semantic failures use the existing quarantine path.
- Connector Compose/install remains default-off. Enabling requires the exact
  `I_ACKNOWLEDGE_CONNECTOR_CANONICAL_WRITES` token, a private connector bearer environment,
  data-plane environment rejection, the `connectors` profile, health validation, and the
  existing systemd restart/reload lifecycle.

## Evidence

- Full `pytest -q`: pass, 100% (`2 skipped`; only existing jsonschema deprecation warnings).
- Focused connector/API/control-ledger/master/broker/installer tests: pass.
- Real in-process broker integration:
  `test_connector_request_restarts_through_real_broker_verified_head` drives the durable
  connector request through task/epoch claim, actual `BrokeredCheckpointUploadService`,
  restore verifier, current HEAD promotion, and a new coordinator instance recovering the
  exact `DURABLE_COMPLETE` receipt. No synthetic checkpoint adapter is used.
- `python -m compileall -q src tests scripts`: pass.
- Ruff on changed production/tests: pass.
- `bash -n deploy/control-plane/install.sh`: pass.
- `python scripts/create_notebooks.py --check`: pass, no drift.
- `python scripts/validate_repository.py`: pass (`3909` checks, zero errors).

## Exact remaining live prerequisites

1. Owner must merge/deploy an approved commit and explicitly enable the connector profile
   with the acknowledgement and a private `MY_DATA_HUB_CONNECTOR_CREDENTIALS_JSON` file.
2. A live ACTIVE Kaggle master must run the updated Notebook, publish both new epoch role
   credentials, and retain enough lease/checkpoint budget to claim a connector checkpoint.
3. The root-managed host tunnel/DNS/TLS CA and central checkpoint broker/provider credentials
   must be healthy; no credentials or provider mutations were exercised in this lane.
4. A real producer must submit an exact envelope and retain its spool until the returned
   verified `DURABLE_COMPLETE` receipt. No live row counts, hashes, checkpoint IDs, or
   external readiness are claimed.

## Changed files

- `.codex/lanes/gate-l-connectors-closure/RESULTS.md`
- `compose.control-plane.yaml`
- `deploy/control-plane/install.sh`
- `docs/16-data-connectors.md`
- `control_migrations/027_connector_checkpoint_requests.sql`
- `src/my_data_hub/control_plane/ledger/sql/027_connector_checkpoint_requests.sql`
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/master_runtime/notebook_entrypoint.py`
- `src/my_data_hub/mcp/postgres_broker.py`
- `src/my_data_hub/api/connector_runtime.py`
- `src/my_data_hub/connectors/checkpoint_control.py`
- `src/my_data_hub/connectors/postgres.py`
- `src/my_data_hub/connectors/runtime.py`
- focused tests under `tests/control`, `tests/master`, and connector deployment/API tests.
