# Region Talk rollback

## Rollback triggers

- unexplained loss/duplication of canonical identity;
- persistent queue selection or gate divergence caused by migration;
- inability to create exact review revisions or receipts;
- PostgreSQL corruption/unrecoverable migration defect;
- security incident involving MCP or database exposure.

Transient provider failure alone is not a database rollback trigger.

## Strategy

YDB is frozen at cutover. PostgreSQL commands accepted after cutover are preserved in the
semantic command/outbox ledger. A rollback must therefore avoid silently losing them.

1. Stop orchestrator, MCP writes and dispatchers.
2. Record the PostgreSQL canonical revision and backup.
3. Export post-cutover semantic commands and external receipts.
4. Re-enable the proven legacy YDB application revision only after checking whether each
   post-cutover command can be replayed or must be held for manual reconciliation.
5. Keep production publication disabled until exact approval/delivery state is reconciled.
6. Diagnose and correct PostgreSQL; re-run migration using the frozen source plus command
   ledger rather than editing source rows manually.

## Recovery preference

Prefer restoring PostgreSQL from a verified backup and replaying idempotent commands over
returning to YDB. YDB rollback is an emergency behaviour-preservation route, not the normal
recovery mechanism.
