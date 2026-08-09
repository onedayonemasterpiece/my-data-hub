# Region Talk: YDB -> my-data-hub PostgreSQL

Status: **implementation-ready migration design; live export not yet executed**

## Objective

Move Region Talk's accumulated state and operating pipeline into `my-data-hub`
without resetting learned identities, duplicate memory, analysis evidence, queues,
review decisions or publication history.

## Migration phases

1. **Inventory and freeze contract** — discover all source tables, row kinds and counts.
2. **Read-only export** — produce paginated JSONL plus manifest and per-file hashes.
3. **Raw preservation** — import every row into `migration.raw_record`.
4. **Normalization** — map known rows into shared/analysis/orchestration/Region Talk tables.
5. **Reconciliation** — prove counts, identities, relationships, states and critical samples.
6. **Incremental catch-up** — repeat export for rows changed after the baseline watermark.
7. **Shadow operation** — run PostgreSQL planning/results without external production effects.
8. **Write freeze and final delta** — stop YDB writes, import the final watermark interval.
9. **Cutover** — switch Region Talk readers/writers and MCP to PostgreSQL.
10. **Rollback window** — retain frozen YDB and exact command ledger.
11. **Retirement** — remove credentials only after acceptance and restore evidence.

## Non-negotiable rule

`normalized + deduplicated + intentionally_excluded + retained_raw + quarantined = exported` for every export batch and row kind.
No row may disappear because its current business meaning is unclear. This equation proves
lossless accounting, not cutover readiness: `quarantined = 0`, `undispositioned = 0` and an
exact raw/manifest count match are separate mandatory cutover gates.
The repository CI exercises this contract against a real PostgreSQL service with `scripts/verify_region_talk_migration_flow.py`: first landing, exact replay, an intentional quarantine that blocks cutover, explicit resolution and a final `passed=true` report. This fixture is proof of the mechanism, not proof that the real YDB dataset has already been migrated.

## Documents

- `data-inventory.md` — known row families and discovery requirements.
- `mapping.md` — initial source-to-target mapping.
- `procedure.md` — executable sequence and idempotency model.
- `reconciliation.md` — evidence and acceptance queries.
- `cutover.md` — shadow/canary/freeze/cutover gates.
- `rollback.md` — return path and write-forward handling.
- `acceptance.md` — release criteria.
