from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_postgres_verifier_uses_current_accounting_contract() -> None:
    source = (ROOT / "scripts" / "verify_postgres_bootstrap.py").read_text(
        encoding="utf-8"
    )
    assert "SELECT exported, accounted, unaccounted" not in source
    assert "expected_row_count, raw_count, dispositioned_count" in source
    assert "quarantined_count" in source
    assert "cutover_ready" in source
    assert 'str(projects[0][1]) != "paused"' in source
    assert "migrations != expected_migrations" in source
    assert "expected_schema_revision = expected_migrations[-1][0]" in source


def test_sql_accounting_starts_from_manifest_kinds_and_blocks_quarantine() -> None:
    source = (ROOT / "sql" / "migrations" / "0007_region_talk_migration.sql").read_text(
        encoding="utf-8"
    )
    row_view = source.split("CREATE VIEW migration.row_accounting AS", 1)[1].split(
        "CREATE VIEW migration.batch_accounting AS", 1
    )[0]
    assert "FROM migration.export_batch_kind expected" in row_view
    assert "expected.expected_row_count" in row_view
    assert "disp.disposition = 'quarantined'" in row_view
    assert "AS cutover_ready" in row_view


def test_raw_record_row_kind_has_manifest_foreign_key() -> None:
    source = (ROOT / "sql" / "migrations" / "0007_region_talk_migration.sql").read_text(
        encoding="utf-8"
    )
    assert "FOREIGN KEY (export_batch_id, row_kind)" in source
    assert (
        "REFERENCES migration.export_batch_kind(export_batch_id, row_kind)" in source
    )


def test_ci_runs_live_region_talk_migration_flow() -> None:
    source = (ROOT / ".github" / "workflows" / "ci.yml").read_text(encoding="utf-8")
    assert "python scripts/verify_region_talk_migration_flow.py" in source
    integration = (ROOT / "scripts" / "verify_region_talk_migration_flow.py").read_text(
        encoding="utf-8"
    )
    assert "replay_inserted != 0" in integration
    assert 'disposition="quarantined"' in integration
    assert 'disposition="intentionally_excluded"' in integration
    assert 'if blocked_report["passed"]' in integration
    assert 'if not final_report["passed"]' in integration
