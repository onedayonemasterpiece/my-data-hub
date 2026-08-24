from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0030_region_talk_dag_materialization.sql"


def test_v9_materializes_only_exact_dependency_ready_work() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert MIGRATION.name.startswith("0030_")
    assert "SET schema_revision=30" in sql
    assert "region_talk_stage_work_input_v9" in sql
    assert "ensure_region_talk_stage_work_v9" in sql
    assert "region-talk-vector-fusion-input.v1" in sql
    assert "jsonb_each(v_e5.result_metadata->'metrics'->'scores')" in sql
    assert "jsonb_each(v_bge.result_metadata->'metrics'->'scores')" in sql
    assert "result_sha256',v_e5.result_sha256" in sql
    assert "result_sha256',v_bge.result_sha256" in sql
    assert "ON CONFLICT(work_item_id) DO NOTHING" in sql


def test_v9_image_input_requires_current_task_readable_artifact_manifest() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    for required in (
        "region-talk-media-artifact-manifest.v1",
        "candidate_revision",
        "normalized_source_url",
        "source_media_id",
        "object_ref",
        "artifact_sha256",
        "byte_size",
        "content_type",
        "acquisition_receipt_sha256",
        "task_readable",
        "'availability','AVAILABLE'",
    ):
        assert required in sql
    assert "Historical" not in sql
    assert "image_queue_item" not in sql


def test_v9_success_requires_registered_pin_and_exact_stage_schema() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "register_region_talk_stage_runtime_pin" in sql
    assert "region_talk_stage_runtime_pin_current_v1" in sql
    assert "prior_pin_receipt_sha256" in sql
    assert "runtime pin supersession is stale" in sql
    assert "requested_result_sha256<>migration.region_talk_json_sha256(requested_metadata)" in sql
    assert "region-talk.image-diagnostic-result.v1" in sql
    assert "region-talk.final-verifier-result.v1" in sql
    assert "region-talk.writer-result.v1" in sql
    assert "direct stage worker result fails exact v9 stage validation" in sql
    assert "TO mdh_owner,mdh_master_controller" in sql


def test_v9_private_inputs_and_internal_helpers_are_not_exposed() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "REVOKE ALL ON migration.region_talk_stage_runtime_pin" in sql
    assert "migration.region_talk_stage_work_input_v9 FROM PUBLIC" in sql
    assert "claim_region_talk_stage_work_v8_untyped" in sql
    assert "submit_region_talk_stage_worker_result_v8_unverified" in sql
    assert "'publication_dispatch',false" in sql
    assert "'notification_dispatch',false" in sql
    assert "GRANT SELECT ON migration.region_talk_stage_work_input_v9" not in sql
    assert "GRANT INSERT ON migration.region_talk_stage_worker_result" not in sql
