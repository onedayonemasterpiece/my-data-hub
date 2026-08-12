# Gate K task-bound worker tunnel results

Status: code gates complete; no host install or live mutation performed.

## Base and scope

- Exact base: `4701cce16fc46dfd0168ecc964d986ded3055329`.
- Isolated branch: `agent/operational-mvp/gate-k-worker-tunnel`.
- Changed only tunnel broker/IPC, root installer, focused tunnel tests and this operation doc/evidence.

## Contract delivered

- Separate `issue_worker_public_key` IPC action accepts only ACTIVE master instance/epoch,
  UUID task/credential identity, one Ed25519 public key and bounded expiry.
- Exact replay returns the same public certificate; changed or revoked replay is rejected.
- Exact task/credential/serial revoke updates KRL, principal authorization and terminates
  only the dedicated worker account sessions.
- `mdh-master-tunnel` remains remote-forward-only with exact `PermitListen`.
- `mdh-embedding-worker` is local-forward-only with exact
  `PermitOpen 127.0.0.1:25432` and `PermitListen none`.
- Rotation, deactivation, lease expiry and corrupted state blank both principal files,
  revoke relevant serials and terminate both dedicated account sessions.
- Broker accepts no private key, gateway host/pin, PostgreSQL credential or business bytes.
  Assembly supplies reviewed gateway host/port and hashed known_hosts separately.

## Observed gates

- Focused `tests/control/test_master_tunnel_broker.py`: 15 passed.
- Ruff for broker/IPC/focused tests: PASS.
- Installer `bash -n`: PASS.
- Compileall: PASS.
- Repository validator: PASS (3,984 checks, zero errors/notes).
- Full suite: pending in final evidence update.
