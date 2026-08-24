from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0026_region_talk_bootstrap_and_current_state.sql"


def test_v5_migration_is_append_only_parseable_and_contiguous() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert "SET schema_revision=26" in sql
    assert "'region-talk','region-talk-main','1.0.0','paused'" in sql
    assert '"key":"source_discovery","version":"v1","compute_lane":"kaggle-candidate-report"' in sql
    assert '"key":"publication_dispatch","version":"v1","compute_lane":"local-side-effect"' in sql
    assert '"enabled_by_default":false' in sql


def test_v5_current_state_replay_is_monotonic_and_initial_status_applies() -> None:
    sql = MIGRATION.read_text()
    claim = sql.split("CREATE OR REPLACE FUNCTION migration.region_talk_claim_canonical_state", 1)[1]
    assert "greatest(current_head.source_updated_at,incoming_source_updated_at)" in claim
    assert "incoming_source_updated_at<head.source_updated_at" in claim
    assert "v_disposition:='stale'" in claim
    assert "CREATE FUNCTION migration.apply_region_talk_initial_source_status" in sql
    assert "current_status_raw_record_id" in sql


def test_v5_helpers_remain_internal_and_pipeline_entrypoint_is_bounded() -> None:
    sql = MIGRATION.read_text()
    assert "REVOKE EXECUTE ON FUNCTION" in sql
    assert "migration.region_talk_claim_canonical_state" in sql
    assert "migration.refresh_region_talk_canonical_current_state" in sql
    assert "migration.execute_region_talk_post_import_stages(uuid,uuid,jsonb)" in sql
    assert "TO mdh_region_talk_pipeline" in sql
    assert "'publication_dispatch',false" in sql
    assert "'notification_dispatch',false" in sql
