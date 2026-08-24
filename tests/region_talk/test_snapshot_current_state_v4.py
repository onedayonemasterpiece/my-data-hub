from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0025_region_talk_task_binding_and_current_state.sql"


def test_v4_migration_is_append_only_parseable_and_contiguous() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert "SET schema_revision=25" in sql
    assert "CREATE TABLE master_control.task_credential_registration" in sql
    assert "CREATE TABLE migration.region_talk_canonical_state_observation" in sql
    assert "CREATE TABLE migration.region_talk_canonical_state_head" in sql


def test_task_auth_comes_from_master_registration_not_first_use() -> None:
    sql = MIGRATION.read_text()
    assert "register_task_credential_binding" in sql
    assert "assert_registered_task_credential" in sql
    assert "requested_task_run_id" in sql
    assert "requested_generation" in sql
    assert "requested_worker_kind" in sql
    assert "credential registration conflicts with immutable task binding" in sql
    assert "begin_region_talk_direct_snapshot_v2_unbound" in sql
    assert "migration.begin_region_talk_direct_snapshot_v2_unbound(jsonb)" in sql


def test_current_state_refresh_is_ordered_and_preserves_append_only_observations() -> None:
    sql = MIGRATION.read_text()
    assert "region_talk_claim_canonical_state" in sql
    assert "incoming_source_updated_at<head.source_updated_at" in sql
    assert "incoming_payload_sha256=head.payload_sha256" in sql
    assert "requested_canonical_revision<=head.canonical_revision" in sql
    assert "region_talk_canonical_state_observation_append_only" in sql
    refresh = sql.split("CREATE FUNCTION migration.refresh_region_talk_canonical_current_state", 1)[1]
    for table in (
        "region_talk.source_candidate",
        "region_talk.source_status",
        "orchestration.work_item",
        "region_talk.publication_plan",
        "region_talk.review_decision",
    ):
        assert table in refresh
    queue = sql.split("CREATE OR REPLACE VIEW region_talk.publication_queue_v3", 1)[1]
    assert "review.decision AS review_decision" in queue
    assert "ORDER BY decision_row.occurred_at DESC" in queue
