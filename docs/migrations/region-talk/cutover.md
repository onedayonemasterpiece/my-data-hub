# Region Talk cutover plan

## Gates

### Gate A — database

- clean migration on an empty PostgreSQL instance;
- backup and restore drill passes;
- migration/staging storage sized with margin;
- application and migration roles separated.

### Gate B — data

- baseline plus catch-up export imported;
- all source rows accounted for with exact manifest/raw counts;
- no undispositioned or quarantined rows remain;
- every row-kind reports `cutover_ready = true`;
- critical identity and semantic reconciliation passes;
- unknown kinds have an explicit accepted non-quarantine disposition.

### Gate C — behaviour

- shadow orchestrator cycles are stable;
- duplicate/idempotency replay tests pass;
- E5, BGE and image result acceptance proven;
- exact candidate revision survives review flow;
- zero-result and failure reasons are observable.

### Gate D — private canary

- external effects target a private test channel only;
- one candidate reaches review and one approved exact revision reaches a receipt;
- retry does not duplicate review card or publication;
- operator can pause all scheduling immediately.

## Freeze and final delta

1. Disable all YDB writers while retaining reads.
2. Record freeze time and legacy deployment revision.
3. Export all changes since previous watermark plus a hash comparison index.
4. Import, normalize and reconcile.
5. Take a PostgreSQL pre-cutover backup.
6. Switch service configuration to PostgreSQL.
7. Start MCP and orchestrator with publication still disabled.
8. Verify health and one bounded non-side-effect cycle.
9. Enable private review canary; production publication remains a separate decision.

## Post-cutover monitoring

For at least the rollback window monitor:

- queue inflow/outflow and oldest actionable age;
- stage failure/retry/quarantine counts;
- duplicate command/result conflicts;
- source/content/candidate creation rates;
- gate funnel versus pre-cutover baseline;
- database size, slow queries, locks and backup success;
- MCP auth/limit failures;
- outbox lag and provider receipts.

Do not delete YDB data or credentials until the rollback window is closed by an explicit
receipt.
