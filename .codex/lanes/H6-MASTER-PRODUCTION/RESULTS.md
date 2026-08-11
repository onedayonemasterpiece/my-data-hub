# H6 master production execution results

## Integration and commits

- Required integration base for the final reconciliation: `10860d6`.
- Production effects/executor commit: `1dbbeaf`.
- Owner-host claim/CAS, control migration 018, notebook drain hook and real FM10 probe: `c20f3b8`.
- Protected exact stored callback replay adapter: `69b0617`.

The branch is `agent/h6-master-production`. Control migration 018 is mirrored
byte-for-byte in `control_migrations/` and the packaged ledger migrations.

## Delivered safety properties

- Closed FM04/FM07/FM08/FM09/FM10/FM11/FM12/FM24 enum; no request SQL,
  payload bytes, fault selector, resource name, duration or clock.
- `acceptance:operate` on request, status, owner claim and receipt completion.
- Runtime-token claims are limited to FM04 and remain exact
  run/attempt/instance/epoch bound.
- FM07/FM08/FM09/FM10/FM11/FM12/FM24 use a separate owner-host claim. Migration
  018 atomically binds task, command, principal, client, old operation and
  receipt hash. Runtime tokens cannot claim or complete an owner-host command.
- Both runtime and owner-host receipt paths reject completion after the stored
  absolute deadline.
- FM11/FM12 drain uses an authenticated identity-only boolean directive; the
  notebook follows its ordinary gate-close/checkpoint/terminal path.
- Metadata-only receipts; no credential or canonical row is stored.
- `live_pass` is derived only from a durable `PASSED` task. No unit or contract
  result is projected as live evidence.

## Scenario status

| Scenario | Status | Production path / exact remaining dependency |
|---|---|---|
| FM04 | **Done (callable)** | Production notebook factory is wired by `main`; empty-baseline revision and 12 source-pinned canonical relations are probed after ACTIVE. |
| FM07 | **Done (callable)** | Official `KaggleMasterRuntimeProvider` required; exactly 20 concurrent same-key ensures must converge to one operation/epoch/provider ref/numeric kernel ID before owner-host CAS completion. |
| FM08 | **Partial** | Concrete orchestration validates one stored callback ID/hash, changed control-process boot UUID, replay disposition and ACTIVE recovery. Deployment must inject `CallbackLossSupervisorPort` implemented by a privileged external supervisor able to survive restarting the control service. No such companion exists in this repository, so no live claim is possible. |
| FM09 | **Done (callable when prerequisites exist)** | `ControlLedgerStoredReplay` replays one canonical protected ACKed body, uses a genuinely revoked previous-run token, injects a task-derived older epoch, and proves the current operation/service/event hash unchanged. Requires epoch > 1 and a prior revoked runtime token, as the scenario itself demands. |
| FM10 | **Partial** | `PostgresH1ExpiredLeaseDenialProbe` is real: restricted role verification, exact epoch, 60–900 second monotonic wait, real lease-expiry readback, H1 pre-DML assertion, SQLSTATE 55000, PostgreSQL `INERROR`, rollback, revision equality and stable UUID/hash. Production must inject the task-bound `LeaseExpiryRenewalPort` that stops renewal for only this runtime. |
| FM11 | **Partial** | Ledger proves old STOPPED and DRAINING-before-terminal, selects current VERIFIED handoff checkpoint, and ensures a fixed-key epoch+1 replacement before evidence completion. Production must inject `OldEpochDenialPort` for the four old-binding renew/register/bounded-write/tunnel probes. |
| FM12 | **Done (callable)** | Owner claim exposes the exact drain directive; notebook performs normal clean drain; finalizer requires STOPPED and the operation-owned current VERIFIED checkpoint (which is reachable only after readback and independent restore smoke). |
| FM24 | **Partial** | Fixed controller uses real `time.monotonic_ns`, exactly twelve 300-second steps, lease+tunnel renewal, credential rotation, bounded read and stale reconnect denial, with a 3600–5400 second evidence bound. Production must inject `SoakSessionPort` over the deployed credential/tunnel/session authorities. |

These four Partial rows are fail-closed injected deployment authorities, not
mock PASS paths. Their absence raises a stable blocker before a live receipt.

## Validation

Run from this exact branch with the integration worktree virtual environment:

```text
PYTHONPATH=src .../bin/python scripts/validate_repository.py
  checks: 3510, errors: 0

PYTHONPATH=src .../bin/python -m compileall -q src tests
  PASS

.../bin/python -m ruff check src tests scripts
  All checks passed

PYTHONPATH=src .../bin/python -m pytest -q
  PASS (923 collected; 2 expected skips; only pre-existing jsonschema deprecation warnings)
```

Focused acceptance/control/notebook tests cover fixed SQL allowlists, FM10
pre-admission and real rollback-state receipt construction, owner/runtime claim
separation, principal/client CAS, exact drain directive, stored-body duplicate
and revoked-token replay, FM11 transition ordering, FM12 verified terminal,
FM24 fixed schedule, schemas and operator scope.

## Live evidence

No Kaggle, PostgreSQL, systemd/host, credential-rotation or tunnel live run was
executed in this isolated implementation lane. Therefore this result records
**no LIVE_PASS** for any scenario. FM04/FM07/FM09/FM12 are production-callable
code paths, not evidence that the corresponding required real run occurred.
