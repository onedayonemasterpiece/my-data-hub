# Code-agent completion task

Repository: `onedayonemasterpiece/my-data-hub`
Target host: existing devstand, which is also the initial production runtime.

## Goal

Complete and deploy the existing scaffold without redesigning it. PostgreSQL is canonical;
Region Talk is the first workload; YDB is read-only migration source; Kaggle notebooks are
immutable-result workers; MCP exposes bounded semantic tools.

## Required work

1. Review accepted ADRs and run repository validation/tests.
2. Pin tested Python/MCP/PostgreSQL/pgvector and container image versions/digests.
3. Deploy PostgreSQL on the devstand, create separated runtime/migration/backup roles, apply
   migrations, run integrity checks and a restore drill.
4. Run live PostgreSQL integration for the implemented repositories/UoW and
   complete only gaps found by evidence; add the missing production OAuth/gateway
   integration for MCP while retaining request/response, scope, rate, concurrency,
   origin and egress limits from the proven `events-bot-new/private_events_mcp`
   patterns.
5. Inspect `events-bot-new` at an exact commit and create the adaptation provenance manifest.
6. Implement complete read-only YDB inventory/export, import every row, finish mappings for
   all discovered kinds and produce reconciliation evidence. Do not discard unknown rows.
7. Port the actual Region Talk candidate/E5/BGE/image/finalizer/review/publication
   processors behind the already defined PostgreSQL/orchestrator and notebook
   contracts. Workers may not write canonical state directly.
8. Run shadow cycles, then a private-channel canary. Keep production publication disabled.
9. Configure orchestrator/MCP/database auto-start and reboot verification.
10. Return exact commit/PR, deployment receipt, migration/export/reconciliation IDs, service
    health, backup/restore evidence, test results and the remaining blocked secrets/decisions.

## Acceptance

Use `docs/migrations/region-talk/acceptance.md`. Do not mark migration complete on green CI
alone; prove data accounting, behaviour, idempotency, private review/publication receipt and
rollback.
