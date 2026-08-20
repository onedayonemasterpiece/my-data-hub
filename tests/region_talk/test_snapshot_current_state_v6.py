from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0027_region_talk_stage_dispatch_and_queue.sql"


def test_v6_is_append_only_and_advances_schema_revision() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert MIGRATION.name.startswith("0027_")
    assert "SET schema_revision=27" in sql
    assert "UPDATE sql/migrations/0026" not in sql


def test_exact_replay_is_observation_only() -> None:
    sql = MIGRATION.read_text()
    replay = sql.split("IF incoming_payload_sha256=head.payload_sha256 THEN", 1)[1]
    replay = replay.split("ELSIF", 1)[0]
    assert "v_disposition:='replay'" in replay
    assert "UPDATE migration.region_talk_canonical_state_head" not in replay


def test_stage_receipts_are_recomputed_server_side() -> None:
    sql = MIGRATION.read_text()
    assert "migration.region_talk_json_sha256(v_input)" in sql
    assert "migration.region_talk_json_sha256(v_output)" in sql
    assert "v_fixed:=jsonb_set(v_receipt-'receipt_sha256'" in sql
    assert "post-import stage receipt hash verification failed" in sql


def test_worker_dispatch_is_fixed_epoch_bound_and_immutable() -> None:
    sql = MIGRATION.read_text()
    assert "CREATE TABLE migration.region_talk_stage_worker_result" in sql
    assert "region_talk_stage_worker_result_append_only" in sql
    assert "master_control.assert_registered_task_credential" in sql
    assert "migration.claim_region_talk_stage_work(" in sql
    assert "migration.submit_region_talk_stage_result(" in sql
    assert "migration.region_talk_stage_work_status(" in sql
    assert "Region Talk result crosses task epoch" in sql
    assert "execute_region_talk_post_import_stages_v1_unverified(uuid,uuid,jsonb)" in sql
    assert "migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)," in sql
    assert "publication_dispatch',false" in sql
    assert "notification_dispatch',false" in sql


def test_prepare_uses_only_exact_verified_current_results() -> None:
    sql = MIGRATION.read_text()
    assert "migration.region_talk_stage_worker_result landed" in sql
    assert "landed.revision_fingerprint=v_candidate->>'revision_fingerprint'" in sql
    assert "landed.input_fingerprint=v_fingerprint" in sql
    assert "landed.master_instance_id=v_registration.master_instance_id" in sql
    assert "WHEN 'SUCCEEDED' THEN 'CURRENT'" in sql


def test_publication_queue_requires_current_plan_or_review_queue() -> None:
    sql = MIGRATION.read_text()
    queue = sql.split("CREATE OR REPLACE VIEW region_talk.publication_queue_v3", 1)[1]
    assert "region_talk.post_import_review_queue" in queue
    assert "queue.candidate_revision=candidate.current_revision" in queue
    assert "stage_run.export_batch_id=accepted.export_batch_id" in queue
    assert "NOT queue.publication_dispatch" in queue
    assert "plan.publication_plan_id IS NOT NULL OR review_queue.candidate_id IS NOT NULL" in queue
