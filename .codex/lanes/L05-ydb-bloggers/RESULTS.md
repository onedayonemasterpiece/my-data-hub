# L05 bounded YDB blogger workload results

## Implemented

- exact 27-column `region_talk_external_blogger_evidence` source contract;
- fixed normalized query identity `25dc6a...`, `QuerySnapshotReadOnly`, ordered `record_id`, no LIMIT;
- strict unknown/missing/oversized-field rejection and no local payload spool;
- deterministic actor/account projections with `unknown` actor kind rather than false person coercion;
- streaming accounting, logical and ID-set hashes, every-row terminal dispositions;
- transactional PostgreSQL landing/canonical/provenance/project membership/disposition writer;
- exact replay no-op and conflicting immutable identity rejection;
- explicit duplicate groups instead of silent account/actor merge;
- canonical revision + semantic checkpoint outbox in the same PostgreSQL transaction;
- receipt remains `COMMITTED_PENDING_CHECKPOINT`, never false durable success;
- append-only migration `0012_bloggers_search.sql`: public `bloggers_ru_v1`, separate exact E5/BGE halfvec spaces, FTS, exact models, HNSW off until benchmark receipt;
- raw migration payload and file evidence removed from MCP reader, immutable to migration operator after insert.

## Live source evidence

A task-created service account `ajeri3qs6jbijih0bs5d` has exactly database-scoped `ydb.viewer`, with no folder/cloud roles. A zero-row UPDATE was denied before SELECT. Live aggregate read observed 266 rows/266 record IDs/14 batches/14 source files, 202 confirmed and 64 review. Temporary YDB RCU limit 10 was restored to 0. No source rows were persisted on devstand. Receipt: `.codex/operational-mvp/evidence/ydb-readonly-inventory.json`.

## Validation

- 8 focused blogger tests passed.
- 25 master/blogger tests passed with one opt-in live test skipped in the normal run.
- Opt-in PG18 tmpfs test passed: migration 12, role negative probes, one restricted-role blogger import, exact replay, one revision/outbox effect.
- Ruff, compileall, repository validator 2601/0, SQL parse and diff check passed.

## Operational gates still pending

- Full 266-row live snapshot must stream into an ACTIVE Kaggle master; only aggregate inventory has run.
- Repeated account identities must be reviewed/resolved so pending duplicate groups are zero.
- Post-import checkpoint/readback/independent cold restore and MCP list/get/search evidence are not yet proven.
- Historical raw YDB residue in events-bot-new remains outside this repository and must be transferred to protected storage or removed without losing incident evidence.
