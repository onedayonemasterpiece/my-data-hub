# FM10 lease-expiry denial

FM10 is a destructive-fault acceptance scenario, not a normal MCP write tool.
Its public request contains only the exact task and ACTIVE master binding. It
cannot carry SQL, SQL parameters, a duration, a clock, a DSN, a token, or row
data.

## Fixed production sequence

`BrokeredH1ExpiredLeaseDenial` performs the following fail-closed sequence:

1. Reconcile an existing mode-0600 completion receipt. A matching receipt is
   returned without opening PostgreSQL sessions or issuing DML.
2. Open an epoch-bound `mdh_mcp_editor` session and a separate epoch-bound
   `mdh_mcp_reader` observation session. Both logins are checked to be
   non-owner, non-role-admin, and without PostgreSQL privilege attributes.
3. Read aggregate-only state: canonical revision, project/content-item counts,
   external-outbox count, and audit-event count. No row or payload is returned
   or persisted.
4. Stage the source-defined one-row project insert in an uncommitted operator
   transaction while the exact epoch lease is valid. There is no request SQL or
   parameter channel.
5. Invoke `ControlLedgerLeaseExpiryRenewal.suspend_exact_renewal`. That port
   durably binds the directive to task, command hash, run, attempt, master and
   epoch, and returns only after the runtime acknowledges that callback
   heartbeat, local `DatabaseGate` renewal, and tunnel/session renewal have
   stopped.
6. Compute the bounded wait from PostgreSQL's own `lease_until`, wait at least
   60 and at most 900 real monotonic seconds, and prove the same exact epoch is
   expired.
7. Force `mdh_epoch_write_guard` for the staged transaction. PostgreSQL must
   return SQLSTATE `55000` and leave the connection `INERROR`; the adapter then
   rolls back. A new transaction calls the immediate epoch assertion and must
   produce the same `55000`/`INERROR`/rollback result.
8. Re-read aggregate state through the reader session. Revision, canonical row
   counts, outbox count, and audit count must all be bit-for-bit equal to the
   pre-fault values.
9. Persist the validated metadata-only completion with stable UUID and SHA-256
   identities. The completion contains counts and bindings only—never DSNs,
   tokens, SQL, parameters, or canonical rows.

The returned `LeaseExpiryEvidence.denial_code` is always
`MDH_EPOCH_LEASE_EXPIRED`. Unit tests use an injected monotonic clock and are
contract evidence only; they are not live acceptance evidence.

## Production composition

Compose the adapter on the control host after the master-scenario runtime
control migration is applied:

- operator connection: `DirectoryOperatorConnectionFactory`, loading the exact
  `operator` envelope only at `open` time;
- observation connection:
  `DirectoryAcceptanceObserverConnectionFactory`, loading the exact `reader`
  envelope only at `open` time;
- renewal port: `ControlLedgerLeaseExpiryRenewal(runtime)`;
- durable completion journal: `AtomicLeaseExpiryCompletionJournal` rooted in a
  service-owned mode-0700 directory on the control state volume.

Inject that composed object as the `H1ExpiredLeaseDenialPort` used by
`ProductionControlHostEffects`. Do not keep the older revision-only probe in
the production composition: it neither forces both deferred and immediate
guards nor proves row/outbox/audit invariance.

The completion journal is a response-loss fence, not canonical application
state. The authoritative task receipt remains the control ledger's exact
owner-bound acceptance receipt. After that receipt is terminally retained, its
task-owned FM10 completion file may be removed by bounded control-state
retention.

## Fail-closed results

No PASS is emitted when any of the following is missing or different:

- exact ACTIVE binding or acknowledged renewal suspension;
- private role-bound operator and observer sessions;
- a 60–900 second monotonic wait and database-observed expiry;
- SQLSTATE `55000` plus PostgreSQL `INERROR` for both guards;
- explicit rollback and unchanged revision/rows/outbox/audit;
- exact create-once completion receipt.
