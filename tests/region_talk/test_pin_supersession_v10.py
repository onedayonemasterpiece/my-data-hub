from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0031_region_talk_pin_supersession_and_media_authority.sql"


def test_v10_pin_supersession_changes_the_immutable_work_identity() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert MIGRATION.name.startswith("0031_")
    assert "SET schema_revision=31" in sql
    assert "requested_input_data" in sql
    assert "'upstream_results',requested_upstream_results" in sql
    assert "pin.receipt=requested_input_data->'runtime_pin'" in sql
    assert "region_talk_stage_work_input_current_v10" in sql
    assert "runtime pin, acquisition, or dependency input was superseded" in sql
    assert "ON CONFLICT(work_item_id) DO NOTHING" in sql


def test_v10_image_work_requires_an_authoritative_immutable_acquisition() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    for required in (
        "region_talk_media_artifact_acquisition",
        "register_region_talk_media_artifact_acquisition",
        "region-talk-media-artifact-acquisition.v1",
        "region-talk-media-artifact-acquisition-receipt.v1",
        "acquisition_evidence_sha256",
        "source_url_sha256",
        "object_ref",
        "artifact_sha256",
        "task_readable",
        "acquisition_receipt_sha256",
    ):
        assert required in sql
    assert "CREATE TRIGGER region_talk_media_artifact_acquisition_append_only" in sql
    assert "asset.status='available'" in sql
    assert "asset.sha256=v_acquisition.artifact_sha256" in sql
    assert "TO mdh_owner,mdh_master_controller" in sql
    assert "TO mdh_region_talk_pipeline" not in sql.split(
        "GRANT EXECUTE ON FUNCTION migration.register_region_talk_media_artifact_acquisition"
    )[1].split(";")[0]


def test_v10_current_evidence_recursively_revalidates_dependencies() -> None:
    sql = MIGRATION.read_text(encoding="utf-8")
    assert "region_talk_stage_result_valid_v9_unchecked_dependencies" in sql
    assert "migration.region_talk_stage_result_valid_v9(landed.stage" in sql
    assert "landed.result_metadata=v_upstream->'result_metadata'" in sql
    assert "count(DISTINCT value->>'stage')" in sql
    assert "claim_region_talk_stage_work_v9_unfenced_pin" in sql
    assert "'publication_dispatch',false" in sql
    assert "'notification_dispatch',false" in sql
