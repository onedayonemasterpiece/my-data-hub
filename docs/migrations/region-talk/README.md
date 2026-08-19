# Region Talk: YDB -> my-data-hub PostgreSQL

Status: **implementation-ready migration design; live export not yet executed**

The current production transfer design is the no-row-file
[direct snapshot v2](direct-snapshot-v2.md). The JSONL bundle steps below are retained
as historical design evidence and are not the autonomous pipeline's live data path.

## Objective

Move Region Talk's accumulated state and operating pipeline into `my-data-hub`
without resetting learned identities, duplicate memory, analysis evidence, queues,
review decisions or publication history. Every source row must remain attributable to
Region Talk, and every normalized/deduplicated shared target must carry an explicit Region
Talk project relation without copying the canonical object.

## Migration phases

1. **Inventory and freeze contract** — discover all source tables, row kinds and counts.
2. **Read-only export** — produce paginated JSONL plus manifest and per-file hashes.
3. **Raw preservation** — import every row into `migration.raw_record` under an attested
   Region Talk origin project scope.
4. **Normalization** — map known rows into shared/analysis/orchestration/Region Talk tables
   and atomically attach required Region Talk relations/scoped state/usage.
5. **Reconciliation** — prove counts, identities, scope completeness, relationships, states
   and critical samples.
6. **Incremental catch-up** — repeat export for rows changed after the baseline watermark.
7. **Shadow operation** — run PostgreSQL planning/results without external production effects.
8. **Write freeze and final delta** — stop YDB writes, import the final watermark interval.
9. **Cutover** — switch Region Talk readers/writers and MCP to PostgreSQL.
10. **Rollback window** — retain frozen YDB and exact command ledger.
11. **Retirement** — remove credentials only after acceptance and restore evidence.

## Non-negotiable rule

For every export batch and row kind:

```text
normalized + deduplicated + intentionally_excluded + retained_raw + quarantined = exported
```

No row may disappear because its current business meaning is unclear. This equation proves
lossless accounting, not cutover readiness: `quarantined = 0`, `undispositioned = 0` and an
exact raw/manifest count match are separate mandatory cutover gates. In addition:

```text
raw_without_region_talk_batch_scope = 0
normalized_target_without_region_talk_relation = 0
deduplicated_target_without_region_talk_relation = 0
region_talk_projection_without_scoped_shared_root = 0
scope_relation_without_raw_or_provenance_evidence_during_migration = 0
work_or_usage_with_ambiguous_project_pipeline_scope = 0
```

Scope lineage proves that all transferred data belongs to the Region Talk migration even
when a row is intentionally excluded, retained raw or quarantined. Semantic project
membership is required only for normalized/deduplicated shared targets where the mapping
contract declares it.
The repository CI exercises the bootstrap accounting contract against a real PostgreSQL
service with `scripts/verify_region_talk_migration_flow.py`: first landing, exact replay, an
intentional quarantine that blocks cutover, explicit resolution and a final `passed=true`
report. This fixture is proof of the mechanism, not proof that the real YDB dataset has
already been migrated. Before live export, append-only scope migrations and new versioned
export/reconciliation contracts carrying scope metrics must be implemented; existing v1
schemas are not silently edited.

## Documents

- `data-inventory.md` — known row families and discovery requirements.
- `mapping.md` — initial source-to-target mapping.
- `procedure.md` — executable sequence and idempotency model.
- `reconciliation.md` — evidence and acceptance queries.
- `cutover.md` — shadow/canary/freeze/cutover gates.
- `rollback.md` — return path and write-forward handling.
- `acceptance.md` — release criteria.
- [`../../22-data-scope-and-pipeline-participation.md`](../../22-data-scope-and-pipeline-participation.md) — canonical scope, usage and policy contract.
