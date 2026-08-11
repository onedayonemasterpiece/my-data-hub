from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]


def test_query_vectors_are_canonical_master_only_append_only_and_epoch_fenced() -> None:
    sql = (ROOT / "sql/migrations/0014_embedding_query_cache.sql").read_text()
    assert "query_embedding_768" in sql and "query_embedding_1024" in sql
    assert "halfvec(768)" in sql and "halfvec(1024)" in sql
    assert sql.count("hub_meta.reject_update_delete") == 2
    assert sql.count("master_control.enforce_write_epoch") == 2
    assert "GRANT SELECT" in sql and "mdh_mcp_reader" in sql
    assert "GRANT SELECT,INSERT" in sql and "mdh_canonical_committer" in sql
    assert "document_text" not in sql and "query text" not in sql.lower()
