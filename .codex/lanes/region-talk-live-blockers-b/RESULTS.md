# REGION-TALK-LIVE-BLOCKERS-B results

## Status

Done. No live or production state was mutated.

## Revisions

- Base SHA: `660b06a`
- Implementation SHA: `f8addce`
- Final-audit follow-up base SHA: `91e22ce`
- Migration 0027 implementation SHA: `d6f9ed5`
- Dynamic proof/hardening SHA: `36f1037`
- Three-digest tamper proof SHA: `1e614ae`
- Private stage payload split base SHA: `a550c2e`
- Migration 0028 implementation/proof SHA: `ab470cc`
- Heavy-stage rotation base SHA: `a02b86d`
- Migration 0029 implementation/proof SHA: `2983b22`
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

### Final-audit follow-up B1-B4

- Added append-only migration 0027 and advanced the schema revision to 27.
- Made exact-payload replay observation-only: canonical revision, export batch, raw
  pointer, source clock, mutable projection, and head observation time remain unchanged;
  changed stale payloads remain denied.
- Added compact canonical-JSON hashing in PostgreSQL and independently recomputed every
  submitted stage input, output, and receipt digest before durable persistence. UTC
  timestamp normalization matches the typed Python contract; tampered hashes fail.
- Reconciled the MCP-visible canonical publication queue with the durable post-import
  review queue, constrained to the latest accepted batch/current candidate revision and
  one projection per review/plan identity. Publication and notification remain false.
- Added fixed, task/ACTIVE-epoch-bound `claim_region_talk_stage_work`,
  `submit_region_talk_stage_result`, and `region_talk_stage_work_status` functions.
  Claims atomically select dependency-ready current work, carry deterministic effect and
  lease identities, reclaim expired bounded attempts, and expose no generic queue read or
  SQL choice.
- Added immutable, bounded worker-result landing with exact stage/contract/subject/
  revision/input/attempt identity and server-recomputed metadata hash. Exact submit replay
  is idempotent; cross-epoch, stale, or conflicting results fail closed.
- PREPARE now marks evidence `CURRENT` only from an exact successful immutable landing and
  supports append-only follow-up cycles after `WAITING_WORK`; it no longer hardcodes every
  heavy stage as missing.
- Explicitly revoked the renamed unverified v1 function from the pipeline and PUBLIC;
  only the validating fixed seam is granted to `mdh_region_talk_pipeline`.

### Architecture correction: migration 0028

- Split the stage data plane so the supervisor receives only exact task/batch/stage/
  work/effect/dispatch identities, attempt policy, timestamps, and hashes. The metadata
  receipt contains no execution payload, text, URL, topics, upstream result contents,
  raw lease, database URL, task token, or result metadata.
- Stored the execution payload and raw lease only inside PostgreSQL. Revoked the 0027
  payload-returning claim, submit, and status entry points from PUBLIC and
  `mdh_region_talk_pipeline`; they remain internal implementation helpers only.
- Added deterministic `dispatch_id` and child `worker_task_run_id` identities and an
  append-only supervisor-to-worker binding. Binding verifies distinct credential IDs and
  task IDs plus exact registered generation, command hash, task-token hash, master
  instance, epoch, work item, effect, attempt, and claim hash.
- Added fixed direct-worker payload fetch and result-submit functions. Only the separately
  registered worker LOGIN can fetch the stored payload or submit; PostgreSQL infers the
  stored lease and rejects the wrong task, credential, generation, epoch, effect, binding
  hash, work item, attempt, input fingerprint, or expired lease.
- Kept exact worker-result replay idempotent, including response replay after the original
  lease expires, while new late results remain denied. Result evidence continues to be
  attributed to the supervisor stage run/current accepted snapshot.
- Added a metadata-only supervisor status receipt for polling before PREPARE/reprepare;
  result references contain hashes only and all dispatch flags remain false.

### Heavy-stage credential rotation: migration 0029

- Added append-only worker-generation authority without changing the deterministic worker
  task, dispatch, effect, work item, attempt, input fingerprint, or database lease.
- Captured the initial 0028 binding as the first generation and exposed a typed status
  view that marks only the greatest generation `ACTIVE`; every prior row is `FENCED`.
- Added fixed `rotate_region_talk_stage_worker_credential` authorization. Rotation requires
  the current binding hash, exact `N+1` registered worker generation, live credential,
  same worker task/master/epoch, exact supervisor task, and current work/effect/lease.
- Allowed a refreshed supervisor credential generation for the same supervisor task and
  epoch to authorize rotation, so a four-minute supervisor LOGIN does not cap a
  900–1800-second worker stage. Worker credentials still cannot rotate or poll supervisor
  authority.
- Replaced direct fetch and submit implementations so only the latest bound, unexpired
  worker session generation succeeds. Exact rotation and current-generation submit replay
  are idempotent; older generations and unrelated worker tasks fail closed.
- Rotation receipts contain only opaque identities, generations, and hashes. They contain
  no payload, raw lease, task token, command body/hash, database URL, or publication effect.

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

Follow-up verification on current integration:

- `MDH_RUN_DISPOSABLE_POSTGRES=1 ./.venv/bin/pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x` — 1 passed against a fresh tmpfs PostgreSQL 18 container. It proves migrations-only schema 27, full head-tuple non-regression on exact replay, stale changed rejection, server receipt-tamper rejection, deterministic PREPARE/COMMIT, bounded claim, immutable exact result submit/replay, verified `CURRENT` evidence, append-only second-cycle COMMIT, MCP queue visibility, and fixed false dispatch.
- `./.venv/bin/pytest -q tests/region_talk/test_snapshot_current_state_v5.py tests/region_talk/test_snapshot_current_state_v6.py tests/region_talk/test_stage_execution.py tests/region_talk/test_reader.py` — 28 passed.
- `./.venv/bin/python scripts/validate_repository.py` — 5100 checks, 0 errors.
- `./.venv/bin/python -m compileall -q src tests` — passed.
- `./.venv/bin/ruff check tests/region_talk/test_snapshot_integrity_postgres.py tests/region_talk/test_snapshot_current_state_v6.py` — passed.
- Full pytest was intentionally not rerun in this follow-up because the integration owner
  reserved the constrained disk/full-suite slot for final integration validation.

Migration 0028 verification:

- `MDH_RUN_DISPOSABLE_POSTGRES=1 ./.venv/bin/pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x` — 1 passed on fresh tmpfs PostgreSQL 18. It proves metadata-only claim and exact replay; denial of the old payload-returning claim; separate deterministic worker registration/binding; wrong generation, wrong supervisor task, and wrong binding rejection; direct worker payload fetch and immutable submit replay; supervisor PREPARE `CURRENT`; metadata-only status; and schema revision 28.
- `./.venv/bin/pytest -q tests/region_talk/test_private_stage_payload_v7.py tests/region_talk/test_snapshot_current_state_v6.py` — 11 passed.
- `./.venv/bin/python scripts/validate_repository.py` — 5104 checks, 0 errors.
- `./.venv/bin/python -m compileall -q src tests` — passed.
- `./.venv/bin/ruff check tests/region_talk/test_private_stage_payload_v7.py tests/region_talk/test_snapshot_integrity_postgres.py` — passed.

Migration 0029 verification:

- `MDH_RUN_DISPOSABLE_POSTGRES=1 ./.venv/bin/pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x` — 1 passed on fresh tmpfs PostgreSQL 18. It proves generation-one fetch, supervisor-credential refresh, exact generation-two rotation/replay, prior-generation fencing, `FENCED`/`ACTIVE` readback, unrelated worker denial, current-generation fetch/direct submit/replay, and schema revision 29.
- `./.venv/bin/pytest -q tests/region_talk/test_stage_worker_rotation_v8.py tests/region_talk/test_private_stage_payload_v7.py tests/region_talk/test_snapshot_current_state_v6.py` — 15 passed.
- `./.venv/bin/python scripts/validate_repository.py` — 5108 checks, 0 errors.
- `./.venv/bin/python -m compileall -q src tests` — passed.
- `./.venv/bin/ruff check tests/region_talk/test_stage_worker_rotation_v8.py tests/region_talk/test_snapshot_integrity_postgres.py` — passed.

## Changed files

- `sql/migrations/0026_region_talk_bootstrap_and_current_state.sql`
- `sql/migrations/0027_region_talk_stage_dispatch_and_queue.sql`
- `sql/migrations/0028_region_talk_private_stage_payload.sql`
- `sql/migrations/0029_region_talk_stage_worker_rotation.sql`
- `sql/admin/role_contract.sql`
- `tests/region_talk/test_snapshot_current_state_v5.py`
- `tests/region_talk/test_snapshot_current_state_v6.py`
- `tests/region_talk/test_private_stage_payload_v7.py`
- `tests/region_talk/test_stage_worker_rotation_v8.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `docs/migrations/region-talk/mapping.md`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
- `.codex/lanes/region-talk-live-blockers-b/RESULTS.md`

## Risks / follow-up

- Migration 0027 supplies the durable dispatcher/result boundary but does not itself attach
  external model providers. Missing or unavailable stage inputs remain honest waiting or
  retryable work until the separately owned typed runtime lands verified evidence.
- Migration 0028 requires the master/controller to register a distinct short-lived child
  worker credential before binding and launch. The separately owned runtime/control lane
  must preserve the committed metadata-only callback and journal contracts.
- Migration 0029 supplies database rotation authority; the separately owned dispatcher
  must invoke it before the current worker LOGIN expires and pass only its receipt hashes
  through the control journal/callback.
- No live Kaggle, YDB, production PostgreSQL, publication, notification, or canonical
  business-data mutation was performed.
- The shared integration worktree contains another lane's uncommitted results receipt;
  it was not staged or modified by this lane.
