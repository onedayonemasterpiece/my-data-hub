from __future__ import annotations

from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0023_region_talk_direct_snapshot.sql"


def test_direct_snapshot_migration_is_valid_sql_and_current_revision() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert "SET schema_revision=23" in sql


def test_pipeline_role_is_epoch_guarded_fixed_and_not_a_table_writer() -> None:
    sql = MIGRATION.read_text()
    roles = (ROOT / "sql/admin/role_contract.sql").read_text()
    bootstrap = (ROOT / "sql/admin/bootstrap_roles.sql").read_text()
    credentials = (ROOT / "src/my_data_hub/master_runtime/credentials.py").read_text()
    assert "mdh_region_talk_pipeline" in bootstrap
    assert "mdh_region_talk_pipeline" in credentials
    assert "master_control.assert_session_write_epoch()" in sql
    assert "session_user,'mdh_region_talk_pipeline','member'" in sql
    assert "GRANT EXECUTE ON FUNCTION migration.begin_region_talk_direct_snapshot" in roles
    assert "GRANT INSERT ON migration.raw_record TO mdh_region_talk_pipeline" not in roles
    assert "GRANT SELECT ON migration.raw_record TO mdh_region_talk_pipeline" not in roles


def test_source_scope_kind_and_publication_effects_are_fail_closed() -> None:
    sql = MIGRATION.read_text()
    for table in (
        "acq_discovery_opportunities",
        "acq_discovery_runs",
        "acq_discovery_surfaces",
        "region_talk_compact_state_kv",
        "region_talk_external_blogger_evidence",
    ):
        assert table in sql
    assert "requested_manifest->>'publication_effects_enabled' <> 'false'" in sql
    assert "requested_payload->>'kind'" not in sql
    assert "row_kind := page_row->>'row_kind'" in sql


def test_every_raw_row_gets_a_terminal_disposition_and_bloggers_are_not_duplicated() -> None:
    sql = MIGRATION.read_text()
    assert "INSERT INTO migration.row_disposition" in sql
    assert "valid_unsupported_kind_v2" in sql
    assert "malformed_source_record_v2" in sql
    assert "dedicated_blogger_materialization_reused_v2" in sql
    blogger_branch = sql.split("ELSIF requested_row_kind = 'external_blogger_evidence_item'", 1)[1].split(
        "END IF;", 1
    )[0]
    assert "INSERT INTO hub.actor" not in blogger_branch
    assert "INSERT INTO region_talk.blogger_profile" not in blogger_branch


def test_reader_views_do_not_require_citext_extension_execute() -> None:
    sql = MIGRATION.read_text()
    assert "p.slug::text = 'region-talk'" in sql
    assert "citext_eq" not in sql
    assert "GRANT EXECUTE ON FUNCTION public.citext_eq" not in sql


def test_raw_relations_remain_hidden_from_mcp_reader() -> None:
    sql = MIGRATION.read_text()
    roles = (ROOT / "sql/admin/role_contract.sql").read_text()
    assert "REVOKE ALL ON migration.region_talk_direct_snapshot" in sql
    assert "REVOKE ALL ON region_talk.imported_content_v2" in roles
    assert "GRANT SELECT ON region_talk.snapshot_inventory_v2" in roles
    assert "GRANT SELECT ON migration.raw_record" not in roles
