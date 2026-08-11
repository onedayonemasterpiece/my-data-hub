# Lane H4-RECONCILE Results

## Status

committed

## Requirement IDs

- H4-R1 — reconcile a lost activation response for the exact durable identity.
- H4-R2 — make activation/renew retries monotonic and idempotent.
- H4-R3 — preserve expiry, fencing, corrupt-state, listener and epoch high-water denial.

## Branch

`agent/operational-mvp/h4-tunnel-reconcile`

## Worktree

`/home/dev/.codex/worktrees/my-data-hub/h4-tunnel-reconcile`

## Base SHA

`0b86000cf2a0adaf15a99feae44fb823474d5bb7`

## Head SHA

Implementation head: `e003cb08d3e66dc8efe43dc3c42dc567b98df5c2`

The RESULTS commit is a documentation-only successor. Resolve the final merge tip
with `git rev-parse agent/operational-mvp/h4-tunnel-reconcile`.

## Files changed

- `src/my_data_hub/tunnel_broker.py`
- `tests/control/test_master_tunnel_broker.py`
- `docs/operations/master-tunnel-broker.md`
- `.codex/lanes/H4-RECONCILE/RESULTS.md`

## Root cause and evidence

`MasterCoordinator` already durably claims `trigger_run`, and after a crash before
the provider effect it reconciles the IN_PROGRESS effect as ABSENT and calls tunnel
activation again with the same run/attempt/master/epoch and `now + lease_ttl`.
`TunnelBroker.activate` treated idempotency as full dataclass equality, including
`lease_until`. The later lease therefore missed the equality branch and was rejected
by the already-advanced epoch high-water mark.

The broker now distinguishes exact identity/listener equality from lease equality:

- an equal or older retry returns the durable current lease and never shortens it;
- a newer retry for the still-active exact identity durably advances the lease;
- even a delayed request whose proposed lease is now in the past returns a newer
  still-active durable lease rather than shortening it;
- a changed listener is rejected without changing the active authorization;
- an expired matching epoch is blanked, serial-revoked, persisted inactive, and its
  dedicated-account SSH sessions are terminated before rejection;
- an inactive/deactivated epoch cannot pass the durable high-water mark and revive;
- corrupt state still invokes the CA-level fail-closed path;
- fenced-epoch certificate requests now raise a typed denial instead of returning
  `None` from a certificate-returning API.

A real `MasterCoordinator` + `ControlLedger` + `FakeKaggleRuntime` + `TunnelBroker`
test injects a crash before `trigger_run`, advances the deterministic clock, observes
ABSENT reconciliation, and proves the same epoch recovers with a 30-second monotonic
lease extension and exactly one physical provider effect. The second activation does
not terminate the current tunnel account session.

## Commands run

- `python -m compileall -q src tests`
- `ruff check src/my_data_hub/tunnel_broker.py tests/control/test_master_tunnel_broker.py`
- `mypy --strict src/my_data_hub/tunnel_broker.py`
- `pytest -q tests/control/test_master_tunnel_broker.py`
- `pytest -q tests/control/test_master_tunnel_broker.py tests/control/test_ledger_master.py tests/control/test_control_runtime_wiring.py`
- `python scripts/validate_repository.py`
- `ruff check .`
- `python scripts/create_notebooks.py --check`
- `python scripts/scan_tracked_secrets.py`
- `bash -n deploy/control-plane/install_master_tunnel_broker.sh`
- `python -m pytest -q`
- `python -m pytest --collect-only`

## Tests / verification

- Focused broker/coordinator/control tests: `40 passed`.
- Full suite: exit `0`; `769` tests collected, with `2` existing skips observed
  (`767` passing tests).
- Repository validator: `3230` checks, zero errors.
- Compileall, full Ruff, strict broker mypy, notebook drift, tracked-secret scan and
  shell syntax: PASS.

## Risks

- No production host, SSH daemon, account, socket or network listener was mutated.
- Coordinator production code did not require a change: the integration regression
  proves its existing IN_PROGRESS+ABSENT call sequence against the corrected broker.
- Once an exact identity's durable lease is expired, this lane deliberately rejects
  same-epoch revival. Recovery requires a new allocated epoch; callers must not turn
  that denial into a static-key or listener-widening fallback.

## Merge notes

- Cherry-pick implementation commit `e003cb08d3e66dc8efe43dc3c42dc567b98df5c2`,
  then the following documentation-only RESULTS commit.
- There are no deploy/operator/H5/H6 changes and no coordinator implementation
  conflict to resolve.
