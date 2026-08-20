# R19-PIN-SUPERSESSION-FIX results

## Scope and revisions

- Requirement IDs: HIGH-R19-01 (runtime-pin supersession identity), HIGH-R19-02
  (authoritative image acquisition/object claim).
- Base SHA: `1010604848638ce208d7064bf68869226d658f38`.
- Verified implementation/head SHA before this evidence-only commit:
  `4e3f0cfe2c35a7234f070371dfbfe8955883a2b3`.
- No live database, provider, object store, publication, or notification state was read or
  mutated.

## Delivered

- Append-only migration 0031 and schema revision 31.
- E5/BGE work fingerprints and UUIDv5 identities now include the complete private input,
  including the exact runtime-pin receipt. Generation N+1 deterministically creates one new
  work item; replay creates none.
- Current-evidence and claim fencing recursively revalidate immutable upstream results. A result
  produced under superseded pin N is no longer current, cannot advance vector/downstream work,
  and fails direct-result resubmission.
- Append-only owner/master-only
  `register_region_talk_media_artifact_acquisition(jsonb)` contract. The receipt binds the ACTIVE
  epoch/current accepted stage run and candidate revision to exact canonical asset, source/media,
  private object reference, byte/type/dimension, artifact, and acquisition-evidence hashes.
- Image work now requires that authoritative receipt and a still-exact current canonical asset.
  Mutable legacy `content_asset.metadata` alone creates no image work. Pipeline/public roles
  cannot register or read acquisition authority.
- Task/master/epoch, private worker generation, lease/effect, and publication/notification=false
  fences remain intact. No generic SQL/DML surface was added.

## Evidence

- Disposable fresh PostgreSQL:
  `MDH_RUN_DISPOSABLE_POSTGRES=1 .../.venv/bin/pytest -q tests/region_talk/test_snapshot_integrity_postgres.py -x`
  -> `1 passed`.
  This proves pin generation 1 success, generation 2 registration/replay, a distinct second
  work/input identity, stale generation 1 projection and submit rejection, generation 2 current
  evidence, recursive vector progression, zero image work from fabricated mutable metadata,
  exact acquisition registration/replay, pipeline registration denial, authoritative image
  materialization, seven unique work identities, and terminal DAG completion.
- Focused static/contracts:
  `pytest -q tests/region_talk/test_pin_supersession_v10.py tests/region_talk/test_dag_materialization_v9.py tests/region_talk/test_snapshot_current_state_v6.py tests/region_talk/test_private_stage_payload_v7.py tests/region_talk/test_stage_worker_rotation_v8.py`
  -> `22 passed`.
- Ruff:
  `ruff check tests/region_talk/test_snapshot_integrity_postgres.py tests/region_talk/test_pin_supersession_v10.py`
  -> passed.
- Compile:
  `python -m compileall -q tests/region_talk/test_snapshot_integrity_postgres.py tests/region_talk/test_pin_supersession_v10.py`
  -> passed.
- Repository validator: `python scripts/validate_repository.py`
  -> `4566` checks, `0` errors.
- `git diff --check` -> passed.

## Changed files

- `sql/migrations/0031_region_talk_pin_supersession_and_media_authority.sql`
- `tests/region_talk/test_pin_supersession_v10.py`
- `tests/region_talk/test_snapshot_integrity_postgres.py`
- `docs/migrations/region-talk/mapping.md`
- `docs/migrations/region-talk/direct-snapshot-v2.md`
- `docs/operations/region-talk-stage-execution.md`
- `.codex/lanes/R19-PIN-SUPERSESSION-FIX/RESULTS.md`

## Risks and integration notes

- The owner/master registration is the database authority for the private object claim. PostgreSQL
  verifies the canonical asset columns and immutable hashes; object-store byte retrieval remains
  the private worker/runtime responsibility and is not claimed by this lane.
- R20 confirmed its closed receipt model matches
  `region-talk-media-artifact-acquisition-receipt.v1`. Migration 0031 image input adds only the
  exact `acquisition_receipt` object and matching `acquisition_receipt_sha256`; it does not add
  top-level width/height fields.
- Full repository pytest was intentionally not run in this serial schema lane; root owns final
  integrated full gates.
