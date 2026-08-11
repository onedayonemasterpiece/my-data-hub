from __future__ import annotations

from pathlib import Path

from pglast import parse_sql

ROOT = Path(__file__).resolve().parents[2]


def test_bloggers_and_separate_embedding_spaces_are_append_only_and_capacity_gated() -> None:
    sql = (ROOT / "sql/migrations/0012_bloggers_search.sql").read_text()
    parse_sql(sql)
    assert "CREATE VIEW region_talk.bloggers_ru_v1" in sql
    assert "CREATE TABLE search.embedding_768" in sql
    assert "halfvec(768)" in sql
    assert "CREATE TABLE search.embedding_1024" in sql
    assert "halfvec(1024)" in sql
    assert "USING hnsw" not in sql
    assert "benchmark_receipt_sha256 IS NOT NULL" in sql
    assert "d128750597153bb5987e10b1c3493a34e5a4502a" in sql
    assert "5617a9f61b028005a4858fdac845db406aefb181" in sql
    assert "SET schema_revision = 12" in sql


def test_raw_payloads_are_not_reader_visible_or_mutable_evidence() -> None:
    migration = (ROOT / "sql/migrations/0012_bloggers_search.sql").read_text()
    roles = (ROOT / "sql/admin/role_contract.sql").read_text()
    assert "raw_record_append_only" in migration
    assert "export_file_append_only" in migration
    assert "REVOKE ALL ON migration.raw_record, migration.export_file" in migration
    assert "REVOKE ALL ON migration.raw_record, migration.export_file" in roles
    assert "REVOKE UPDATE, DELETE ON migration.export_batch_kind, migration.export_file" in roles
    assert "GRANT SELECT ON ALL TABLES IN SCHEMA hub, analysis, orchestration, sync, region_talk, joplin" in roles
