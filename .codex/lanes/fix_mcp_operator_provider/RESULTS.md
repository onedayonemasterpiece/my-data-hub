# Lane results: fix_mcp_operator_provider

## Scope

- Lane: `fix_mcp_operator_provider`
- Requirements: H1, H2, H3, and assigned H4 tunnel-certificate control endpoint.
- Base SHA: `4916d166e7df80ab676c619a8e2eae7d0ada7b8b`
- Implementation SHA: `a8f93fac396ca3bc5dbf7ccc8a3758244504da31`
- Evidence commit: this file's commit; final SHA is reported in the integration handoff.

## Outcome

- Added an injected, fail-closed `LedgerWriteGate` for exact ACTIVE master instance/epoch/revision and verified pre-change checkpoint binding.
- Added durable idempotent MCP write state/events through `PREVIEWED -> APPLYING -> COMMITTED_PENDING_CHECKPOINT -> CHECKPOINTING -> CHECKPOINT_VERIFIED -> DURABLE_COMPLETE`; apply cannot replay after entering the ambiguous `APPLYING` state.
- Restricted DML to injected AST policy targets, parameters, row caps, exact write permits, current epoch assertions, and a non-owner/non-superuser `mdh_mcp_editor` login. Generic DDL/SQL authority was not added.
- Kept the reader profile unchanged. Operator credential issuance is advertised only by explicit control-app injection and only for an exact ACTIVE attempt; credential envelopes must contain exact authorized roles.
- Added expired/malformed/superseded credential envelope pruning with bounded directory scans while preserving atomic mode-0600 writes for the current instance/epoch.
- Added a single Kaggle MCP provider gateway over the repository adapter/journal and provider policy/leases for exact private `mcp_managed`/`mcp_exchange` create, version, run, read, and disposable delete operations. Control ledger stores typed metadata/claims/receipts, not request payload bytes.
- Routed `provider.resources.read` directly through the control gateway, without master resolution, cold-start, or PostgreSQL session. Exact dataset/notebook locator fields are returned from a registered claim.
- Aligned provider lifecycle scope with the production-configured exact `provider:write` scope.
- Made embedding production capabilities fail unavailable/`ready:false` without an ACTIVE operation, verified production request, two-model receipt, and positive observed document coverage.
- Added `POST /internal/runtime/tunnel-certificates/{run_id}/{attempt_id}` with per-attempt Bearer validation, exact 16-KiB contract, run/attempt/master/epoch fencing, Ed25519 public-key validation, REGISTERING/ACTIVE lease bounds, injected `issue_public_key` broker call, and public certificate metadata only.
- Added exact MCP bindings for checkpoint restore, master rotation, connector coverage, stale epoch, and protected resource probe rather than using provider-shaped fallback arguments.

## Validation evidence

All commands were run in `/home/dev/.codex/worktrees/my-data-hub/mcp-operator-provider`.

- `python3 -m compileall -q src tests` — passed.
- `.venv/bin/ruff check .` — passed.
- `.venv/bin/python scripts/validate_repository.py` — passed: `3183` checks, zero errors/notes.
- `.venv/bin/python scripts/create_notebooks.py --check` — passed with zero drift.
- `.venv/bin/pytest -q tests/control/test_mcp_operator_provider.py tests/control/test_control_runtime_wiring.py tests/mcp` — passed.
- `.venv/bin/pytest -q` — passed; two repository-wide deprecation warnings only.
- `cmp control_migrations/014_mcp_write_lifecycle.sql src/my_data_hub/control_plane/ledger/sql/014_mcp_write_lifecycle.sql` — passed.
- `git diff --check` — passed before commit.

## Risks / integration requirements

- Production composition must explicitly inject the HMAC signing secret-backed `LedgerWriteGate`, exact `BoundedSQLPolicy`, single authenticated `KaggleProviderAdapter`, `operator_credential_enabled=True`, and the run/attempt-bound `TunnelCertificateBroker`. Absence of any write dependency fails closed.
- The write lifecycle remains `CHECKPOINTING` until the normal master checkpoint publisher records and promotes a distinct, newer verified checkpoint with the committed canonical revision; it never fabricates checkpoint completion.
- Tunnel lifecycle deactivation/revocation remains with the tunnel broker/coordinator integration lane; this lane owns only validated certificate issuance.

## Changed files

- `control_migrations/014_mcp_write_lifecycle.sql`
- `src/my_data_hub/control_plane/adapters.py`
- `src/my_data_hub/control_plane/app.py`
- `src/my_data_hub/control_plane/ledger/sql/014_mcp_write_lifecycle.sql`
- `src/my_data_hub/control_plane/ledger/store.py`
- `src/my_data_hub/control_plane/runtime.py`
- `src/my_data_hub/mcp/catalog.py`
- `src/my_data_hub/mcp/contracts.py`
- `src/my_data_hub/mcp/postgres_broker.py`
- `src/my_data_hub/mcp/runtime.py`
- `src/my_data_hub/mcp/server.py`
- `src/my_data_hub/mcp/service.py`
- `tests/control/test_control_runtime_wiring.py`
- `tests/control/test_ledger_master.py`
- `tests/control/test_mcp_operator_provider.py`
- `tests/mcp/test_dynamic_contracts.py`
- `tests/mcp/test_postgres_broker.py`
- `tests/mcp/test_remote_runtime.py`
- `tests/test_control_plane.py`
