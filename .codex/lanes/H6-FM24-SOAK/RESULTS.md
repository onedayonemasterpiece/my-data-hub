# H6 FM24 session-rotation soak results

## Scope and status

- Lane: `H6-FM24-SOAK`
- Branch: `agent/h6-fm24-soak`
- Exact base: `9b2da3725be857c7e04a0dc9f414fefd920647cb`
- Implementation commit: `d3dfc2d0c1526f56ad9850224cbd1c4d3ffdd98e`
- Status: implementation and injected contract validation complete; **no live soak claimed**.

The lane changed only the new FM24 module, focused tests, its state schema/example,
operator documentation and this result record. It did not change the ledger, MCP
catalog/server/service, master notebook/entrypoint, deploy/provider driver, H1 or H5.

## Delivered

- Concrete `ProductionSoakSessionPort` matching the existing five-method synchronous
  `SoakSessionPort` surface, plus read-only durable progress/deadline composition hooks.
- Hook-driven, non-sleeping 12-step schedule with a 300-second cadence, 3,600-second
  real minimum and original 5,400-second absolute deadline.
- Live evidence accepts only `SystemSoakClock`; fake/accelerated clocks require explicit
  `evidence_class="injected"`.
- Exact runtime-client binding checks and delivered heartbeat ACK before PostgreSQL
  `DatabaseGate.renew` or `TunnelBrokerClient.renew`.
- Periodic epoch-bound credential rotation, fixed bounded one-row ACTIVE-epoch read,
  explicit prior credential expiration, and stale reconnect denial bound to the
  production broker/session contract through narrow metadata-only injected ports.
- Atomic, fsync-backed, maximum-64-KiB task state with mode-0600 file and mode-0700
  directory requirements. Every side effect has a persisted deterministic intent hash
  and post-effect ACK hash; a lost response resumes the same intent without incrementing
  durable counters twice.
- No credential, DSN, principal, certificate, SQL or result row is representable in the
  journal/state models. Raw task/runtime/master identities are hash-bound rather than
  persisted.
- Durable cancellation and deadline terminals; exact binding/evidence-class conflict
  rejection; final exact-service-active proof after twelve ACKed steps.
- JSON Schema and example for `my-data-hub-fm24-soak-state.v1` plus an exact composition
  handoff in `docs/operations/fm24-session-rotation-soak.md`.

## Validation

All checks ran in the isolated lane worktree on implementation commit
`d3dfc2d0c1526f56ad9850224cbd1c4d3ffdd98e`.

- Focused FM24: `7 passed`.
- Full repository: `953 collected`, therefore `951 passed, 2 skipped`; exit status 0.
- Full Ruff: `All checks passed!`.
- `python -m compileall -q src tests`: passed.
- Repository validator: `3553` checks, zero errors/notes.
- `git diff --check`: passed.

Focused tests prove fixed ordering, one measured injected hour, twelve durable counters,
mode-0600 and secret-negative state, response-loss restart reconciliation without a
second rotation effect, cancellation, absolute deadline, fake-clock/live rejection, and
schema/example validity.

## Exact integration handoff

1. Cherry-pick the implementation commit and this results commit.
2. Instantiate `ProductionSoakSessionPort` per exact claimed FM24 task under
   `/kaggle/working/.../<task>/fm24.json`, injecting the existing live `RuntimeClient`,
   `DatabaseGate`, `TunnelBrokerClient`, production credential registrar, fixed read/
   session-binding probe and cancellation hook.
3. The master scenario controller must resume from `completed_steps(binding)` and use
   the persisted start/deadline. Treat `SoakSessionNotDue` as a cooperative nonterminal
   yield so the ordinary heartbeat loop continues. Do not restart a 12-step loop after
   process loss.
4. Invoke the FM24 hook before an ordinary coalescible heartbeat when a step is due; gate
   and tunnel renewal remain forbidden until this task heartbeat is delivered.

The current blocking `12 x sleep(300)` implementation in the integration-base
`master_production.py` cannot by itself satisfy cooperative heartbeat progress or resume
within the original deadline. The master-scenarios owner was notified and owns that
composition change. Until real production adapters are assembled and a 3,600-5,400
second run is independently retained and verified, FM24 remains operationally blocked.
