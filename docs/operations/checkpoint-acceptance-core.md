# Fixed checkpoint acceptance core (FM05, FM14, FM15)

`my_data_hub.checkpoints.acceptance` defines three fixed task-owned operations. It is an
execution contract, not evidence that Kaggle was exercised. A fake or injected effects
port emits `evidence_class=injected` and can produce only `verdict=CONTRACT_PASS`.
`LIVE_PASS` requires a production effects implementation to identify itself as live and
must still be backed by the external acceptance receipt/claims collected by the driver.

## Common safety sequence

1. Observe the exact checkpoint HEAD tuple: generation, current, previous.
2. Derive the candidate UUID from scenario, operation and task run. Callers cannot supply
   a candidate ref, package bytes, corruption bytes or a fault mode.
3. Commit `my-data-hub-checkpoint-acceptance-intent.v1` through the durable journal.
   The intent binds the HEAD snapshot/hash, task/operation, candidate, source revision,
   evidence class, fixed 900-second timeout and three-attempt retry contract.
4. Invoke only scenario-specific idempotent `ensure_*` effects. Persist each returned
   receipt against the exact intent hash. A provider response lost after an effect is
   reconciled by calling the same `ensure_*` method; no second physical effect is valid.
5. Verify HEAD after every stage. Finish with a canonical receipt no larger than 64 KiB.

A journal implementation must atomically reject conflicting operation/idempotency reuse,
retain intents/stages/terminal receipts, and implement `record_attempt_failure` so the
third failed attempt becomes terminal `FAILED`. The coordinator rejects later execution
of that terminal operation. It deliberately contains no in-memory production journal.

## FM05 — empty roundtrip

The fixed stages are empty-candidate creation (canonical revision and canonical row count
both zero), private upload with an exact version, authenticated exact-hash readback,
independent restore verification and a generation-CAS promotion. Before promotion the
HEAD must equal the committed snapshot. After promotion it must be exactly
`generation+1`, `current=candidate`, `previous=old current`.

## FM14 — deterministic corruption rejection

The effects implementation creates a disposable task-owned corruption candidate. It must
return distinct expected and observed content hashes with
`EXACT_READBACK_HASH_MISMATCH_REJECTED`. The coordinator never calls promotion and requires
the complete HEAD tuple to remain byte-for-byte equal to the committed snapshot. Current
and previous checkpoints are never corruption targets.

## FM15 — forced restore-smoke failure

The effects implementation creates a disposable task-owned candidate, first proves exact
readback, then runs the fixed failing isolated restore fixture. The terminal stage must be
`FORCED_DISPOSABLE_RESTORE_FAILURE_REJECTED`. There is no caller-selected command or
failure payload and no promotion. The complete canonical HEAD must remain unchanged.

Contract schema/example:

- `schemas/checkpoint-acceptance-fm05-request.v1.schema.json`
- `schemas/checkpoint-acceptance-fm14-request.v1.schema.json`
- `schemas/checkpoint-acceptance-fm15-request.v1.schema.json`
- `schemas/checkpoint-acceptance-intent.v1.schema.json`
- `schemas/checkpoint-acceptance-receipt.v1.schema.json`
- matching examples under `examples/contracts/`
