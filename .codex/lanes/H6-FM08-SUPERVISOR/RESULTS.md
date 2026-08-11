# Lane H6-FM08-SUPERVISOR Results

## Status
Implemented and committed; no live restart claimed.

## Requirement accounting
- FM08 owner claim / exact run-attempt-epoch binding — Done in the concrete ledger adapter.
- Persist directive before suppression — Done; fixed heartbeat, count one, 120-second expiry, receipt hash.
- Durable runtime spool with ID/hash-only control evidence — Done by the callback ingress/runtime SDK contract.
- Private authenticated host IPC — Done; HMAC plus Unix peer UID and mode-0600 socket.
- Only control-plane restart — Done; immutable base compose file and `restart --no-deps control-plane`; no Docker socket mount.
- Before/after process boot UUID and health — Done in host and control journals.
- Response-loss/restart recovery — Done with deterministic request ID and fsynced INTENT/COMPLETE journal.
- Expiry/timeout disarm — Done for ARMED/CAPTURED callback state; host refuses expired pre-effect intent.
- Missing host permission pre-effect blocker — Done; socket reachability is checked before arm.
- Public/MCP endpoint — Not added (intentionally forbidden).
- Real FM08 execution — Blocked outside this lane; no live evidence fabricated.

## Unique implementation commits
1. `c91b1c4` — host controller, signed Unix IPC, restart journal, initial deploy wiring and focused tests.
2. `aeac669` — hardened default-off installer/systemd wiring and immutable restart path.
3. `6fd43c5` — exact migration-020 control-ledger adapter and production composition factory.

Dependency commits `8490155`, `9fcfe1e`, and `905fe0e` came from H6-MASTER-SCENARIOS
and must not be cherry-picked from this lane when their integration equivalents
are already present.

## Validation
Final exact-head gate results are recorded in the final RESULTS commit and handoff.
