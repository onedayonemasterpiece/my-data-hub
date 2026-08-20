from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0032_region_talk_heavy_runtime_wiring.sql"


def test_v11_is_append_only_private_and_uses_two_hash_authority() -> None:
    sql = MIGRATION.read_text()
    assert "SET schema_revision=32" in sql
    assert "UPDATE hub.canonical_state SET schema_revision=32" in sql
    assert "CREATE TABLE migration.region_talk_heavy_evidence_pack" in sql
    assert "CREATE TABLE migration.region_talk_heavy_stage_result_artifact" in sql
    assert sql.count("hub_meta.reject_update_delete()") >= 2
    assert "work_input_fingerprint" in sql
    assert "enrichment_sha256" in sql
    assert "rich_input_fingerprint" in sql
    assert "region-talk-heavy-stage-input-receipt.v1" in sql
    assert "region-talk-heavy-stage-private-result.v1" in sql
    assert "publication_dispatch',false" in sql
    assert "notification_dispatch',false" in sql
    assert "GRANT EXECUTE ON FUNCTION migration.fetch_region_talk_heavy_stage_input" in sql
    assert "TO mdh_region_talk_pipeline" in sql
    assert "TO mdh_mcp_reader" not in sql
    assert "TO mdh_mcp_editor" not in sql
    assert "SELECT * FROM" not in sql.upper()


def test_v11_corrects_raw_url_hash_without_rewriting_0031() -> None:
    sql = MIGRATION.read_text()
    prior = (ROOT / "sql/migrations/0031_region_talk_pin_supersession_and_media_authority.sql").read_text()
    assert "region-talk-media-artifact-acquisition-receipt.v2" in sql
    assert "legacy_receipt_sha256" in sql
    assert "sha256(convert_to(v_acq.normalized_source_url,'UTF8'))" in sql
    assert "sha256(convert_to(coalesce(asset.source_url,''),'UTF8'))" in sql
    assert "region-talk-media-artifact-acquisition-receipt.v2" not in prior
    assert "ALTER TABLE migration.region_talk_media_artifact_acquisition" not in sql


def test_v11_closes_direct_heavy_submit_bypass_and_bounds_private_payloads() -> None:
    sql = MIGRATION.read_text()
    assert "submit_region_talk_heavy_stage_worker_result" in sql
    assert "REVOKE EXECUTE ON FUNCTION" in sql
    assert "migration.submit_region_talk_stage_worker_result(uuid,uuid,jsonb)" in sql
    assert "octet_length((v_private->'result_data')::text)>65536" in sql
    assert "octet_length(requested_request::text)>262144" in sql
    assert "private heavy result differs from current enriched work" in sql
