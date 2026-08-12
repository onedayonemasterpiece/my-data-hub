# FM08 callback-loss restart supervisor

FM08 uses a private host authority, not an MCP or HTTP fault endpoint. The owner-only
`acceptance:operate` claim binds one command to the exact task, operation, run,
attempt, master instance, and epoch. Control migration 020 persists a fixed
`runtime.heartbeat`, maximum-one directive with a 900-second expiry, the control
process boot UUID, and a canonical directive receipt hash before the callback can
be suppressed. The fixed expiry lets one legitimate provider
termination/recovery call cannot be mistaken for a crashed owner claim.

## Callback sequence

1. `LedgerCallbackLossSupervisor` proves that the private Unix socket is reachable
   before arming any callback effect.
2. `ControlLedgerCallbackLossPort` persists and validates the fixed directive. No
   event body, event type, count, timeout, command, service, or path comes from the
   operator request.
3. Callback ingress authenticates and deduplicates the exact heartbeat, records
   only its event ID and body SHA-256 in the FM08 control row, and returns 503. The
   Kaggle runtime retains the exact body in its fsync-backed local JSONL spool.
   Exact retries remain 503 until a restart receipt exists; altered events are not
   captured as the task callback.
4. The control-owned Kaggle adapter terminates the exact persisted numeric old
   run with one provider delete call. Exact absence reconciles a lost response;
   it never blindly repeats the destructive call. The ledger then atomically
   fences the old operation/service/attempt, revokes its token, and admits one
   task-derived Notebook ref at exactly the next epoch. Runtime source
   attestation is required before that recovery becomes ACTIVE. The old and new
   operation IDs, provider run refs, kernel IDs, consecutive epochs, and
   termination/recovery receipt hashes are durable.
5. The separately enabled host daemon accepts only a HMAC-authenticated,
   task-derived `RESTART_CONTROL_PLANE` envelope over its mode-0600 Unix socket.
   Its immutable command is `docker compose ... restart --no-deps control-plane`.
   The Docker socket is never mounted in a container, and neither `remote-mcp` nor
   `oauth-server` is a selectable restart target.
6. The host journal fsyncs `INTENT` before restart, waits for the fixed loopback
   health endpoint to report a different process boot UUID, then fsyncs the exact
   before/after receipt. A deterministic request ID makes a lost response
   resumable without a second restart.
7. The new control process records the before/after UUIDs in the control ledger.
   Because the old master is intentionally dead and its raw Bearer is gone, the
   immutable already-authenticated event row is replayed only by its task/event/
   body-hash identity. This atomically changes the directive to `REPLAYED` and
   can complete FM08 only while the distinct recovery binding remains ACTIVE.

Expired ARMED or CAPTURED directives disarm and clear capture metadata. Missing
host/socket permission blocks before arming. The daemon never accepts arbitrary
argv, a service name, a compose file, a URL, a filesystem path, callback bytes, or
an event body through IPC.

## Deployment gate

The supervisor is default-off. It is installed only with the operator install and
this exact additional acknowledgement:

```text
MY_DATA_HUB_ENABLE_ACCEPTANCE_SUPERVISOR=I_ACKNOWLEDGE_TASK_BOUND_CONTROL_RESTART
```

The owner supplies a private 32–256 byte signing key at
`MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_KEY_FILE`. The installer creates a separate
user systemd service, mounts only the private supervisor socket directory
read-only into `control-plane`, and preserves the normal three-service control
unit semantics. Removing the acknowledgement on a later install disables and
stops the supervisor.

## Evidence status

Unit and integration tests prove the contract, one-shot exact termination,
atomic old-epoch fencing, distinct consecutive recovery identity, IPC
authentication, persist-before-effect journals, stored-event replay by identity,
expiry, and immutable restart target. They are not live FM08 evidence. A live
PASS still requires the real Kaggle old/recovery runs, real container restart,
and the resulting durable typed receipt.
