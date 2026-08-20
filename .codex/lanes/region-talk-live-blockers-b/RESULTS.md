# REGION-TALK-LIVE-BLOCKERS-B results

## Status

Done. No live or production state was mutated.

## Revisions

- Base SHA: `660b06a`
- Implementation SHA: `f8addce`
- Results receipt SHA: recorded by the commit containing this file

## Delivered

- Added append-only migration `0026_region_talk_bootstrap_and_current_state.sql` and advanced the schema revision to 26.
- Seeded the complete versioned `region-talk-main` definition during migrations, forced the pipeline to remain `paused`, and kept `publication_dispatch` disabled.
- Fixed exact-payload replay so `source_updated_at` is monotonic and a later stale changed payload cannot overwrite the current head.
- Applied the first `source_status_item` to the mutable `region_talk.source.status` projection without duplicating the immutable 0024 status history row.
- Added the fixed task-bound `execute_region_talk_post_import_stages(uuid,uuid,jsonb)` PREPARE/COMMIT seam with deterministic UUIDv5 identities, response-loss-stable request hashing, complete-outcome validation, durable typed run/outcome/stage receipts, review queue rows, and honest missing-evidence work items.
- Constrained all post-import publication and notification dispatch fields to `false`; no generic SQL/table/stage choice is accepted.
- Added the narrow pgcrypto owner grant required by the UUIDv5 SECURITY DEFINER helper; the pipeline LOGIN must still `SET LOCAL ROLE mdh_region_talk_pipeline`, and exact task/accepted-batch authorization is enforced inside the function.
- Updated Region Talk migration documentation and disposable PostgreSQL regressions.

## Evidence and commands

- `.venv/bin/python -m pytest -q tests/region_talk/test_snapshot_current_state_v5.py tests/region_talk/test_snapshot_current_state_v4.py tests/region_talk/test_snapshot_integrity_v3.py tests/region_talk/test_direct_snapshot_sql.py tests/test_db_migrations.py` — 21 passed.
- `MDH_RUN_DISPOSABLE_POSTGRES=1 .venv/bin/python -m pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x` — 1 passed against a fresh tmpfs PostgreSQL 18 container, migrations only (no Python pipeline registration). This covered initial source status, source queue canonicalization, post-import PREPARE/COMMIT work formation, exact older replay monotonicity, and stale changed-payload rejection.
- `.venv/bin/python -m pytest -q tests/region_talk/test_stage_execution.py` — 9 passed.
- `.venv/bin/python -m pytest -q tests/region_talk/test_pipeline_core.py tests/region_talk/test_direct_snapshot.py tests/region_talk/test_long_run_authority.py` — 23 passed.
- `.venv/bin/python scripts/validate_repository.py` — 5068 checks, 0 errors.
- `.venv/bin/python -m compileall -q src tests` — passed.
- `.venv/bin/ruff check tests/region_talk/test_snapshot_integrity_postgres.py tests/region_talk/test_snapshot_current_state_v5.py` — passed.
- `git diff --check` for all lane-owned files — passed.
- Full `.venv/bin/python -m pytest -q` — one failure outside this lane in the concurrently edited, uncommitted `test_production_assembly_response_loss.py` provider fixture (`_Provider` lacks `push_private_notebook_pending_runtime_attestation`); all other tests passed or skipped.

## Changed files

- `sql/migrations/0026_region_talk_bootstrap_and_current_state.sql`
- `sql/admin/role_contract.sql`
- `tests/region_talk/test_snapshot_current_state_v5.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `docs/migrations/region-talk/mapping.md`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
- `.codex/lanes/region-talk-live-blockers-b/RESULTS.md`

## Risks / follow-up

- Existing heavy evidence is intentionally surfaced as `MISSING`; 0026 never infers a current model PASS from legacy payloads. The typed stage runtime must submit exact work receipts before future non-legacy candidates can enter review.
- The shared integration worktree still contains other lanes' uncommitted files. All files owned by this lane except this receipt were clean immediately after `f8addce`; none of those unrelated edits were staged or committed here.
