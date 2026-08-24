from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0028_region_talk_private_stage_payload.sql"


def test_v7_is_append_only_and_advances_revision() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert MIGRATION.name.startswith("0028_")
    assert "SET schema_revision=28" in sql


def test_supervisor_claim_is_metadata_only() -> None:
    sql = MIGRATION.read_text()
    claim = sql.split("CREATE FUNCTION migration.claim_region_talk_stage_work_metadata", 1)[1]
    receipt = claim.split("v_base:=jsonb_build_object(\n      'schema_version'", 1)[1]
    receipt = receipt.split("INSERT INTO migration.region_talk_stage_dispatch_claim", 1)[0]
    assert "'lease_token_sha256',v_lease_hash" in receipt
    assert "'lease_capability_sha256',v_capability_hash" in receipt
    assert "'payload',v_source->'payload'" not in receipt
    assert "'lease_token',v_lease_token" not in receipt
    for forbidden in ("canonical_url", "canonical_source_key", "input_data", "upstream_results"):
        assert forbidden not in receipt


def test_payload_fetch_and_submit_require_exact_worker_binding() -> None:
    sql = MIGRATION.read_text()
    assert "CREATE TABLE migration.region_talk_stage_worker_binding" in sql
    assert "worker_task_run_id<>supervisor_task_run_id" in sql
    assert "worker_credential_id<>supervisor_credential_id" in sql
    assert "master_control.assert_registered_task_credential('region_talk',requested_worker_task_run_id)" in sql
    assert "exact_binding.worker_credential_id=worker.credential_id" in sql
    assert "exact_binding.worker_generation=worker.generation" in sql
    assert "item.lease_token<>claim.lease_token" in sql


def test_old_payload_returning_pipeline_entrypoints_are_revoked() -> None:
    sql = MIGRATION.read_text()
    revoke = sql.split("REVOKE EXECUTE ON FUNCTION", 1)[1].split("FROM PUBLIC", 1)[0]
    assert "migration.claim_region_talk_stage_work(uuid,uuid,jsonb)" in revoke
    assert "migration.submit_region_talk_stage_result(uuid,uuid,jsonb)" in revoke
    assert "migration.region_talk_stage_work_status(uuid,uuid,jsonb)" in revoke
    assert "migration.claim_region_talk_stage_work_metadata(uuid,uuid,jsonb)" in sql
    assert "migration.fetch_region_talk_stage_work_payload(uuid,uuid,jsonb)" in sql


def test_all_new_receipts_keep_dispatch_disabled() -> None:
    sql = MIGRATION.read_text()
    assert sql.count("'publication_dispatch',false") >= 7
    assert sql.count("'notification_dispatch',false") >= 7
