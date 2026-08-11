# Lane H5 Results

## Status
committed

## Requirement IDs
- H5 — explicit blogger duplicate resolution and replay

## Branch
`agent/operational-mvp/h5-blogger-dedupe`

## Worktree
`/home/dev/.codex/worktrees/my-data-hub/h5-blogger-dedupe`

## Base SHA
`4916d166e7df80ab676c619a8e2eae7d0ada7b8b`

## Head SHA
Implementation head before this evidence-only results commit:
`cf1af50c737f7df1cc30320415bd2962751eb5d5`

## Files changed
- `src/my_data_hub/workloads/bloggers/{__init__,closure,importer,master_stage,postgres}.py`
- `sql/migrations/0015_blogger_duplicate_resolution.sql`
- `schemas/region-talk-blogger-duplicate-resolution-set.v1.schema.json`
- `schemas/region-talk-ydb-bloggers-import-receipt.v3.schema.json`
- `schemas/blogger-closure-receipt.v2.schema.json`
- `examples/bloggers/*.json`
- `docs/operations/final-blogger-closure.md`
- `docs/migrations/region-talk/{procedure,reconciliation}.md`
- `tests/bloggers/test_duplicate_resolution.py`
- `tests/bloggers/test_duplicate_resolution_postgres.py`
- `tests/test_db_migrations.py`

Existing receipt schemas v2/v1 remain byte-identical to the base; resolved duplicate receipts use append-only v3/v2 schemas.

## Implementation evidence
- A first shared-account observation remains non-canonical and persists immutable raw rows, quarantined first dispositions, deterministic durable duplicate groups, and exact member evidence.
- Replay requires a complete explicit identity/member/canonical-record/canonical-actor decision set. Partial, stale, connected-group-inconsistent, changed-owner, missing-profile, and source-derived actor conflicts do not canonicalize.
- A valid replay adds append-only `migration.blogger_replay`, `migration.blogger_duplicate_resolution`, and `migration.blogger_replay_disposition` rows. It never updates raw rows or first dispositions.
- Effective accounting overlays append-only replay dispositions: canonical members are normalized, noncanonical members are deduplicated, pending groups are zero, and exact source accounting remains lossless.
- Exact later replay returns the stored revision/hash/count receipt and creates neither another revision nor another checkpoint outbox effect.
- Actor count is the number of distinct explicit canonical targets rather than a hardcoded copy of the 266 source rows. Durable resolved group count may be nonzero; only pending group count must be zero.

## Migration checksum and role effects
- `0015_blogger_duplicate_resolution.sql` SHA-256: `134459c8cef5491c0c8d12092f61536217c67f7f21f05a9cdf50e3be0ec9d29e`.
- `mdh_migration_operator`: `SELECT, INSERT` on the three private append-only replay/resolution tables; explicit `REVOKE UPDATE, DELETE` on them.
- `mdh_migration_operator` and `mdh_mcp_reader`: `SELECT` only on sanitized `migration.blogger_duplicate_accounting`.
- No raw/member/canonical-target resolution table is newly exposed to MCP reader/editor. Backup visibility remains fail-closed until the reviewed role contract is reapplied, consistent with the existing new-object policy.

## Commands run
- `.venv/bin/ruff check src/my_data_hub/workloads/bloggers tests/bloggers tests/test_db_migrations.py`
- `.venv/bin/python -m compileall -q src tests`
- `.venv/bin/python -m pytest -q`
- `MDH_RUN_DISPOSABLE_POSTGRES=1 .venv/bin/python -m pytest -q tests/bloggers/test_duplicate_resolution_postgres.py`
- `.venv/bin/python scripts/validate_repository.py`
- `.venv/bin/mypy`
- `git diff --check`
- `sha256sum sql/migrations/0015_blogger_duplicate_resolution.sql`

## Tests / verification
- Ruff: PASS.
- Compileall: PASS.
- Full pytest suite: PASS, with the repository's three expected opt-in skips and only the pre-existing `jsonschema.RefResolver` deprecation warning.
- Focused disposable PostgreSQL 18 tmpfs proof: PASS. It covered quarantine, rejected stale resolution, explicit resolution, normalized/deduplicated effective accounting, append-only original evidence, one canonical actor, exact replay no-op, one canonical revision/outbox effect, schema revision 15, and operator overwrite denial.
- Repository validator: PASS (`3193` checks, `0` errors).
- Configured strict mypy gate: PASS (`5` configured source files).
- Migration/receipt examples validate under their strict JSON schemas.

## Risks
- No real YDB import or owner duplicate decision was executed; the disposable PostgreSQL evidence proves mechanism only.
- Duplicate decision content must remain an ACTIVE-master-local reviewed input. The new optional master-stage argument does not authorize transport through the metadata-only devstand.
- Resolved duplicate closure receipts are v2 and nested import receipts are v3. Historical v1/v2 receipts remain supported and unchanged; downstream stages that only authorize historical closure v1 must explicitly add v2 support in their own lane before consuming a resolved corpus.

## Merge notes
Cherry-pick both implementation commits (`5a44c29`, `cf1af50`) and this final results commit. Migration 0015 was reserved by the integration owner. No embeddings, MCP implementation, control-plane, deploy, or matrix files were changed.
