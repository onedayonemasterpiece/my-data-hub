from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MIGRATION = (ROOT / "sql/migrations/0020_blogger_discovery_intake.sql").read_text()
RECONCILE_TIMESTAMP_MIGRATION = (
    ROOT / "sql/migrations/0021_blogger_reconcile_committed_at.sql"
).read_text()
ROLE_CONTRACT = (ROOT / "sql/admin/role_contract.sql").read_text()


def _function(name: str) -> str:
    start = MIGRATION.index(f"CREATE FUNCTION integration.{name}")
    next_function = MIGRATION.find("CREATE FUNCTION", start + 1)
    return MIGRATION[start : next_function if next_function != -1 else len(MIGRATION)]


def test_canonical_apply_is_fixed_fenced_and_atomic() -> None:
    apply_sql = _function("apply_blogger_discovery")
    assert "SECURITY DEFINER" in apply_sql
    assert "master_control.assert_session_write_epoch" in apply_sql
    assert "pg_has_role(session_user, 'mdh_canonical_committer', 'member')" in apply_sql
    assert apply_sql.count("hub.advance_canonical_revision") == 1
    assert "INSERT INTO sync.external_outbox" in apply_sql
    assert "INSERT INTO integration.blogger_discovery_apply_receipt" in apply_sql
    assert "INSERT INTO integration.receipt" in apply_sql
    assert "EXECUTE " not in apply_sql


def test_staging_preview_apply_and_reconcile_are_closed_contracts() -> None:
    for name in (
        "materialize_blogger_discovery_artifact",
        "preview_blogger_discovery",
        "apply_blogger_discovery",
        "reconcile_blogger_discovery",
    ):
        function_sql = _function(name)
        assert "SECURITY DEFINER" in function_sql
        assert "master_control.assert_session_write_epoch" in function_sql
    assert "REVOKE ALL ON FUNCTION" in MIGRATION
    materialize_sql = _function("materialize_blogger_discovery_artifact")
    assert "mdh_blogger_materializer" in materialize_sql
    assert "accepted.authenticated_principal <> 'service:mcp-blogger-discovery-artifact-v1'" in materialize_sql
    assert "artifact record violates closed blogger discovery contract" in materialize_sql
    assert "TO mdh_blogger_materializer" in ROLE_CONTRACT
    assert "TO mdh_connector_intake" not in ROLE_CONTRACT.split(
        "integration.materialize_blogger_discovery_artifact", 1
    )[1].split(";", 1)[0]
    assert "requested_sql" not in MIGRATION
    assert "sql_text" not in MIGRATION
    assert "integration.blogger_discovery_quarantine" in MIGRATION
    assert "UNIQUE (batch_id, source_record_id)" in MIGRATION
    assert "normalized_record jsonb NOT NULL CHECK (jsonb_typeof(normalized_record) = 'object')" in MIGRATION


def test_reconcile_returns_the_immutable_postgres_commit_timestamp() -> None:
    assert "DROP FUNCTION integration.reconcile_blogger_discovery" in RECONCILE_TIMESTAMP_MIGRATION
    assert "committed_at timestamptz" in RECONCILE_TIMESTAMP_MIGRATION
    assert "receipt.committed_at, true" in RECONCILE_TIMESTAMP_MIGRATION
    assert "clock_timestamp()" not in RECONCILE_TIMESTAMP_MIGRATION.split("RETURNS TABLE", 1)[1].split(
        "UPDATE hub.canonical_state", 1
    )[0]
    assert "TO mdh_canonical_committer" in RECONCILE_TIMESTAMP_MIGRATION


def test_reader_view_is_sanitized_and_role_contract_is_minimum() -> None:
    view_start = MIGRATION.index("CREATE VIEW hub.bloggers_v1")
    view_end = MIGRATION.index("CREATE FUNCTION", view_start)
    view_sql = MIGRATION[view_start:view_end]
    assert "FROM hub.project_actor membership" in view_sql
    assert "LEFT JOIN region_talk.blogger_profile" in view_sql
    assert "raw_payload" not in view_sql
    assert "evidence_uri" not in view_sql
    assert "GRANT SELECT ON hub.bloggers_v1 TO mdh_mcp_reader" in ROLE_CONTRACT
    assert "TO mdh_canonical_committer" in ROLE_CONTRACT
    assert "GRANT INSERT ON hub.actor TO mdh_canonical_committer" not in ROLE_CONTRACT
    assert "GRANT ALL" not in ROLE_CONTRACT
    assert "ALTER ROLE mdh_mcp_reader BYPASSRLS" not in ROLE_CONTRACT
    assert "ALTER ROLE mdh_canonical_committer SUPERUSER" not in ROLE_CONTRACT
