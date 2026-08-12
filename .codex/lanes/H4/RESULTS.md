# Lane H4 Results

## Status

committed

## Requirement IDs

- H4 — concrete devstand master tunnel broker/account/OpenSSH `PermitListen`,
  epoch-bound issuance/revocation, and fail-closed lifecycle enforcement.

## Branch

`agent/operational-mvp/h4-master-tunnel`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/h4-master-tunnel`

## Base SHA

`4916d166e7df80ab676c619a8e2eae7d0ada7b8b`

## Head SHA

Implementation head: `1cf7da78ce9c2ec5a9e5dd2ab69a4eb7145f8413`

The lane-results commit is a documentation-only successor to this implementation
head; use `git rev-parse agent/operational-mvp/h4-master-tunnel` for the merge head.

## Files changed

- `deploy/control-plane/install_master_tunnel_broker.sh`
- `docs/operations/master-tunnel-broker.md`
- `scripts/validate_repository.py`
- `src/my_data_hub/tunnel_broker.py`
- `tests/control/test_master_tunnel_broker.py`
- `tests/test_architecture_invariants.py`
- `.codex/lanes/H4/RESULTS.md` (documentation-only successor)

## Evidence

- The root-gated installer creates/reconciles only the dedicated
  `mdh-master-tunnel` nologin account, a tunnel-only Ed25519 user CA, root-owned
  authorization/KRL state, an exact sshd `Match User` fragment, and a five-second
  fail-closed reconciliation timer.
- The sshd contract is certificate-only, `AllowTcpForwarding remote`, exact
  `PermitListen 127.0.0.1:25432` (configurable to one non-privileged port),
  `GatewayPorts no`, no local forward, PTY, session, agent, X11, tunnel, user rc,
  password, host-based or GSSAPI access. `Match all` closes the include scope.
- `TunnelBroker` activates only a strictly newer epoch and binds master instance,
  run, attempt, epoch, lease, principal and loopback port. Issuance accepts only one
  public Ed25519 key, uses a standard OpenSSH certificate with `clear` plus only
  `permit-port-forwarding`, and limits expiry to 15 seconds..10 minutes within the
  exact active lease.
- Durable broker state contains operational identity/lease/serial/key-id,
  public-key SHA-256 and revocation metadata only. Tests prove it contains neither
  submitted public/certificate bodies nor the ephemeral private key. The issuer API
  cannot accept a private-key field.
- Exact serial revocation and epoch deactivate generate/query a real OpenSSH KRL.
  Rotation, deactivate, expiry, malformed authority and internal failures blank the
  current principal, revoke serials or the CA, and terminate only sshd children owned
  by the dedicated account. Invalid/stale requests deny without tearing down a valid
  current authorization.
- No host mutation was performed. No listener, public/VPN port, PostgreSQL, PGDATA,
  business data or master private key was added to the devstand repository/runtime.

## Commands run

- `bash -n deploy/control-plane/install_master_tunnel_broker.sh`
- `python -m compileall -q src tests`
- `ruff check .`
- `mypy --strict src/my_data_hub/tunnel_broker.py`
- `pytest -q tests/control/test_master_tunnel_broker.py tests/test_control_plane_deployment.py tests/test_architecture_invariants.py`
- `python scripts/validate_repository.py`
- `python scripts/create_notebooks.py --check`
- `python scripts/scan_tracked_secrets.py`
- `python -m pytest -q`
- temporary-directory real `ssh-keygen` certificate inspection and KRL queries
  performed by the focused tests

## Tests / verification

- Focused/deployment/architecture: `23 passed`.
- Full suite: `736 passed, 1 skipped`, exit `0`; the skip is the existing explicit
  opt-in disposable PostgreSQL integration test. Two existing `jsonschema.RefResolver`
  deprecation warnings remain.
- Repository validator: `3205` checks, zero errors.
- Notebook drift: none.
- Tracked-secret scan: PASS.
- Ruff: PASS.
- Strict mypy for the new broker: PASS.
- Compileall and bash syntax: PASS.

## Risks

- This lane deliberately did not mutate a real host, create the production account,
  reload sshd, or claim a live tunnel.
- The authenticated control endpoint is owned by `fix_mcp_operator_provider`. The
  exact injected hook sent to that lane is
  `issue_public_key(master_instance_id, run_id, attempt_id, epoch, public_key,
  valid_before, now) -> TunnelCertificate`; that lane confirmed its route uses the
  refined callable and revalidates the public result.
- Notebook ephemeral-key generation, certificate retrieval, `CertificateFile` SSH
  use, and deletion on fence/terminal require the root integrator's ordered serial
  patch because the Gate-K lane correctly avoided broadening into tunnel ownership.
- Control lifecycle integration must call `activate` after epoch allocation,
  `renew` on accepted lease renewal, and `deactivate` before/with terminal fencing or
  registry removal. A deployment is not complete until those cross-lane hooks and a
  host broker transport are integrated and tested together.

## Merge notes

- Cherry-pick implementation commit `1cf7da78ce9c2ec5a9e5dd2ab69a4eb7145f8413`,
  then the following documentation-only RESULTS commit.
- The only shared-inventory edits are one expected deploy-file entry each in
  `scripts/validate_repository.py` and `tests/test_architecture_invariants.py`.
- Preserve the exact `PermitListen`/`GatewayPorts no` contract. Do not substitute a
  non-standard `permit-listen` certificate critical option and do not restore a
  static tunnel private identity.
