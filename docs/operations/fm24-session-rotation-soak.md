# FM24 session-rotation soak port

`my_data_hub.acceptance.soak_session.ProductionSoakSessionPort` is the concrete,
task-owned runtime/data-plane port for the fixed FM24 acceptance scenario. It is
contract implementation only until a real task runs for at least one measured hour;
this document and the tests are **not** live evidence.

## Fixed behavior

- exactly 12 steps are due at 300-second monotonic intervals;
- the earliest valid completion is 3,600 measured seconds and the original absolute
  deadline is 5,400 seconds;
- live evidence accepts only `SystemSoakClock`. An accelerated/fake clock is accepted
  only when `evidence_class="injected"`;
- every step requires, in order: delivered runtime heartbeat ACK, PostgreSQL epoch
  lease renewal, exact tunnel lease renewal, periodic credential rotation, one fixed
  bounded read, explicit prior-credential expiry, and a stale reconnect denial from
  the production broker/session binding;
- the callback ACK is persisted before either local lease authority is invoked;
- the adapter never sleeps. It is a hook called by the normal cooperative runtime
  loop, so callback heartbeats and cancellation remain schedulable;
- each side effect has a task/binding/step/action-derived intent hash written before
  the call and a receipt hash written after the ACK. Production registrar/read-probe
  adapters must reconcile repeated intent hashes after response loss;
- the journal is atomic, fsync-backed, at most 64 KiB, and mode `0600` below a mode
  `0700` task directory. Live state must remain below `/kaggle/working`;
- state contains only hashes, counters, bounded timestamps, action names and status.
  It cannot contain credentials, passwords, DSNs, SSH material, principals, SQL or
  result rows.

The state contract is
[`schemas/fm24-soak-state.v1.schema.json`](../../schemas/fm24-soak-state.v1.schema.json).

## Exact composition handoff

Instantiate one port **when the exact FM24 command is first claimed**, not at process
startup, and reuse its task-owned path after restart:

```python
soak = ProductionSoakSessionPort(
    task_id=command.task_id,
    binding=command.binding,
    journal=SoakStateJournal(paths.working / "acceptance" / str(command.task_id) / "fm24.json"),
    runtime_client=runtime,
    database_gate=gate,
    tunnel_authority=tunnel_broker_client,
    credential_registrar=fm24_credential_registrar,
    read_probe=fm24_read_probe,
    evidence_class="live",
    cancelled=task_cancelled,
)
```

The injected registrar owns secret generation, PostgreSQL role/session binding and
the secret-bearing control registration envelope. Its public FM24 methods return only
`CredentialRotationReceipt` and `CredentialExpiryReceipt`. The injected read probe
owns the fixed allowlisted SELECT and production broker reconnect attempt; it returns
only the metadata receipt models declared in `soak_session.py`.

The runtime controller must use `completed_steps(binding)` after restart and resume at
that durable counter. It must use `session_started_monotonic_ns(binding)` and
`session_deadline_monotonic_ns(binding)` for the final `RotationSoakEvidence`, rather
than starting a second 12-step/hour loop. Invoke the five `SoakSessionPort` methods in
their existing order only when the next 300-second hook is due. `SoakSessionNotDue` is
a nonterminal yield back to the ordinary heartbeat loop; cancellation and deadline
exceptions are terminal. After twelve durable steps, call `exact_service_active` and
copy the durable counters into the FM24 evidence.

Do not call the current blocking `sleep(300)` loop from inside the notebook's normal
heartbeat loop. Doing so would prevent the very callback/lease maintenance FM24 is
required to prove.

## What remains to prove live

Deployment must assemble the real credential registrar and fixed read/session probe,
run the exact source revision for 3,600–5,400 real seconds on one ACTIVE Kaggle master
epoch, download the independently retained receipt, and verify twelve heartbeat,
lease, tunnel, rotation, bounded-read, explicit-expiry and stale-denial ACKs. Until
that occurs, FM24 remains blocked and no `live` result may be claimed.
