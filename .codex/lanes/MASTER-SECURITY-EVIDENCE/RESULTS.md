# MASTER-SECURITY-EVIDENCE

## Outcome

- The ACTIVE Kaggle master runs all PostgreSQL positive-role and adversarial denial
  probes in a rollback-only transaction before issuing any remote session credential.
- The devstand receives only bounded counts and SHA-256 receipts, bound to the exact
  release commit, master instance, epoch, schema revision and canonical revision.
- Control-ledger migration `033` stores the evidence append-only. Runtime callback
  authority requires the exact ACTIVE run/attempt/master/epoch and supports exact replay.
- Operator gates can be issued from the current ledger evidence and current VERIFIED
  checkpoint. The installer independently revalidates that authority before enabling
  canonical writes, so placeholder UUIDs/hashes are no longer sufficient.

## Validation

- Focused master, control-ledger, runtime callback, operator-gate and deployment tests
  pass.
- A disposable tmpfs PostgreSQL 18 + pgvector instance applied every migration and the
  role contract, then passed 90/90 real probes: 16 positive role probes and 74
  adversarial security probes. The instance and its tmpfs data were destroyed.
- Full repository gates are recorded in the integration commit that contains this file.

## Honest live boundary

This implementation does not itself claim a production operator gate or live canonical
write. A fresh exact-release master must post the evidence and produce the protecting
VERIFIED checkpoint before `issue-from-ledger` and the explicit operator install can
succeed.
