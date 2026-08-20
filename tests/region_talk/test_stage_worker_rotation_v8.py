from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0029_region_talk_stage_worker_rotation.sql"


def test_v8_is_append_only_and_advances_revision() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert MIGRATION.name.startswith("0029_")
    assert "SET schema_revision=29" in sql
    assert "region_talk_stage_worker_generation_append_only" in sql


def test_rotation_is_exact_successive_and_response_loss_idempotent() -> None:
    sql = MIGRATION.read_text()
    assert "CREATE FUNCTION migration.rotate_region_talk_stage_worker_credential" in sql
    assert "v_new_generation<>current_binding.worker_generation+1" in sql
    assert "stage worker rotation is stale or skips a generation" in sql
    existing = sql.split("IF FOUND THEN", 1)[1].split("SELECT * INTO STRICT current_binding", 1)[0]
    assert "RETURN existing.binding_receipt" in existing
    assert "stage worker rotation idempotency conflict" in existing


def test_only_latest_generation_can_fetch_or_submit() -> None:
    sql = MIGRATION.read_text()
    assert sql.count("worker_generation=(SELECT max(current.worker_generation)") == 2
    assert sql.count("exact_binding.worker_credential_id=worker.credential_id") == 2
    assert sql.count("exact_binding.worker_generation=worker.generation") == 2
    assert "binding_status" in sql
    assert "THEN 'ACTIVE' ELSE 'FENCED'" in sql


def test_rotation_keeps_stable_work_authority_and_dispatch_disabled() -> None:
    sql = MIGRATION.read_text()
    rotate = sql.split("CREATE FUNCTION migration.rotate_region_talk_stage_worker_credential", 1)[1]
    assert "dispatch.effect_id=(requested_request->>'effect_id')::uuid" in rotate
    assert "dispatch.work_item_id=(requested_request->>'work_item_id')::uuid" in rotate
    assert "dispatch.worker_task_run_id=(requested_request->>'worker_task_run_id')::uuid" in rotate
    assert "worker.master_instance_id<>supervisor.master_instance_id" in rotate
    assert "worker.epoch<>supervisor.epoch" in rotate
    assert "'publication_dispatch',false" in rotate
    assert "'notification_dispatch',false" in rotate
    receipt = rotate.split("v_base:=jsonb_build_object", 1)[1].split(
        "INSERT INTO migration.region_talk_stage_worker_generation", 1
    )[0]
    for forbidden in ("payload", "lease_token", "task_token", "command_sha256", "database_url"):
        assert forbidden not in receipt
