# REGION-TALK-DATA-HARDENING results

## Identity

- Lane: `REGION-TALK-DATA-HARDENING`
- Assigned integrated base: `2dcd7797a9600c412e6d7a4b6040aba025466433`
- Implementation head: `10d4710a4dc8d3bf2a94efbc1dcb955f1b1ef796`
- Integration branch at implementation start had already advanced to assembly head `132891fcf49922e209b9440ae7059c8779fdb947`; no assembly/shared MCP/control/deploy/provider/master/tunnel/launcher file was staged by this lane.

## Requirements closed

1. Server-side integrity recomputation: PostgreSQL recomputes canonical payload, row, page, table and final snapshot evidence and independently reconciles it to Pass A/Pass B. Forged row/page/Pass-B hashes and same-count payload changes fail closed.
2. Latest accepted typed reads: article/post/queue views use only the latest exact `complete`, integrity-verified, canonical-applied, accepted, non-quarantined snapshot with deterministic source identity dedupe. Landing/failed/older snapshots cannot leak.
3. Executable canonical data: supported articles/posts/sources/frontier/publication candidates/revisions/schedules/reviews project into existing `hub.*`, `region_talk.*`, and `orchestration.*` contracts. One canonical revision, semantic outbox item and immutable receipt commit atomically. Publication dispatch remains false and no publication attempt is created.
4. Task/epoch/replay boundary: every land/finalize/apply call requires the exact Region Talk role, current ACTIVE epoch, live credential and durable task/credential/batch binding. Exact replay returns the receipt without a revision; conflicting replay is denied; quarantine prevents canonical apply.
5. Reader/mapping correction: `canonical_url` and legacy queue/publication status aliases are covered; public `category` reaches private `queue_family`; fixed canonical publication queue/summary readers are present.
6. Semantic test: added an opt-in disposable PG18/pgvector test covering migration 1..24, apply/readback, exact replay, conflicting replay, a second accepted snapshot without semantic duplicates, same-count payload mutation, and latest-view isolation.

## Changed files

- `sql/migrations/0024_region_talk_snapshot_integrity_and_canonicalize.sql`
- `src/my_data_hub/workloads/region_talk/direct_snapshot.py`
- `src/my_data_hub/workloads/region_talk/reader.py`
- `tests/region_talk/test_snapshot_integrity_v3.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `tests/region_talk/test_reader.py`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
- `docs/migrations/region-talk/mapping.md`

## Commands and evidence

- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/python -m compileall -q src tests` — PASS.
- `PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider tests/region_talk tests/test_db_migrations.py tests/test_region_talk_migration.py tests/mcp/test_region_talk_contracts.py` — PASS (`1 skipped`, the opt-in disposable test).
- `MDH_RUN_DISPOSABLE_POSTGRES=1 PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider tests/region_talk/test_snapshot_integrity_postgres.py` — PASS.
- Disposable image: `pgvector/pgvector:0.8.6-pg18-bookworm`, digest `sha256:2ba9ca5f2e7daa0f0e7723cba1ee9167bab54efd3640516a44ac1a928dd67e7a`.
- Fresh migration sequence: 24 contiguous migrations applied; role contract applied.
- First semantic snapshot observed: 12 landed/dispositioned, 0 quarantined, canonical revision 1; 1 receipt; `hub.content_item=4`, `region_talk.source=3`, `orchestration.work_item=1`, candidate/revision/plan/review each 1; article/post/publication queue each 1.
- Exact replay retained revision 1 and one receipt; forged Pass-B replay raised `SerializationFailure`; forged row hash raised `InvalidParameterValue`; same-count changed payload raised `ObjectNotInPrerequisiteState` from the server evidence comparison.
- A second accepted snapshot advanced exactly to revision 2 while candidate revision, source status, review decision and plan remained deduplicated at one; a newer landing mutation did not replace the accepted typed snapshot.
- Disposable containers were removed. The known image was retained for integration gates per integrator instruction.
- Full suite command `PYTHONDONTWRITEBYTECODE=1 .venv/bin/pytest -q -p no:cacheprovider` reached 99% and had one unrelated concurrent-integration failure: `tests/test_post_deploy_acceptance.py::test_real_verify_all_shape_matches_committed_report_schema` expected the old committed tool list while shared MCP changes exposed a changed list. Region Talk focused/disposable gates passed.

## Honest remaining semantic mappings / risks

- Historical run/metric/cursor/feedback/delivery rows, image/model diagnostic history, embeddings and LLM request/budget records remain terminal `retained_raw` (malformed rows quarantine). They are deliberately not treated as executable canonical semantics until an append-only target contract exists.
- Legacy values outside constrained lifecycle enums remain exact evidence while only neutral operational states are used. No missing model result or editorial decision is invented.
- Blogger evidence continues to reuse the dedicated reviewed 266-to-263 path; this snapshot mapper does not create duplicate blogger actors.
- This proves schema and disposable semantics, not the supervised production YDB cutover, live 58,554-row reconciliation or checkpoint publication. Those remain integration/operations gates outside this lane.
- Publication dispatch is intentionally disabled; the canonical publication queue is readable/executable state but no external publication side effect is enabled.
