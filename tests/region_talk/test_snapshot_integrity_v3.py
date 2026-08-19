from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from pglast import parse_sql

from my_data_hub.workloads.region_talk.constants import DIRECT_SOURCE_TABLES
from my_data_hub.workloads.region_talk.direct_snapshot import source_row


ROOT = Path(__file__).resolve().parents[2]
MIGRATION = ROOT / "sql/migrations/0024_region_talk_snapshot_integrity_and_canonicalize.sql"


def test_v3_integrity_migration_is_append_only_and_valid() -> None:
    sql = MIGRATION.read_text()
    assert parse_sql(sql)
    assert "SET schema_revision=24" in sql
    assert "region_talk_direct_row_logical_sha256" in sql
    assert "region_talk_direct_table_logical_sha256" in sql
    assert "region_talk_direct_snapshot_logical_sha256" in sql
    assert "payload_canonical_text" in sql


def test_server_recomputes_row_page_table_and_snapshot_evidence() -> None:
    sql = MIGRATION.read_text()
    landing = sql.split("CREATE OR REPLACE FUNCTION migration.land_region_talk_direct_page", 1)[1]
    finalizer = sql.split("CREATE OR REPLACE FUNCTION migration.finalize_region_talk_direct_snapshot", 1)[1]
    assert "server_row_sha:=migration.region_talk_direct_row_logical_sha256" in landing
    assert "server_row_sha<>logical_sha" in landing
    assert "server_page_sha<>requested_page->>'logical_sha256'" in landing
    assert "migration.region_talk_direct_page_persisted_sha256" in finalizer
    assert "migration.region_talk_direct_table_logical_sha256" in finalizer
    assert "migration.region_talk_direct_snapshot_logical_sha256" in finalizer
    assert "pass B differs from server-recomputed evidence" in finalizer
    assert "direct snapshot replay conflicts with verified Pass B" in finalizer
    assert "direct snapshot replay conflicts with persisted evidence" in finalizer


def test_typed_views_are_latest_complete_canonical_and_deduplicated() -> None:
    sql = MIGRATION.read_text()
    accepted = sql.split("CREATE OR REPLACE VIEW region_talk.accepted_snapshot_v2", 1)[1]
    assert "snapshot.state='complete'" in accepted
    assert "batch.status='accepted'" in accepted
    assert "snapshot.integrity_verified" in accepted
    assert "canonical_apply" in accepted
    for view in ("articles_v2", "posts_v2", "queue_v2"):
        body = sql.split(f"CREATE OR REPLACE VIEW region_talk.{view}", 1)[1]
        assert "accepted_snapshot_v2" in body
        assert "row_number() OVER" in body
        assert "WHERE ranked.identity_rank=1" in body


def test_only_canonicalizable_core_is_promoted_and_unsupported_history_stays_raw() -> None:
    sql = MIGRATION.read_text()
    normalize = sql.split(
        "CREATE OR REPLACE FUNCTION migration.normalize_region_talk_direct_record", 1
    )[1].split("CREATE OR REPLACE FUNCTION migration.land_region_talk_direct_page", 1)[0]
    assert "'online_source_item','external_publication_source_item'" in normalize
    assert "'region_talk_llm_request_item'" not in normalize
    assert "INSERT INTO region_talk.imported_llm_request_v2" not in normalize
    assert "INSERT INTO region_talk.imported_discovery_run_v2" not in normalize
    assert "v_disposition text := 'retained_raw'" in normalize
    assert "v_reason text := 'valid_unsupported_kind_v3'" in normalize


def test_canonical_apply_is_epoch_task_bound_replay_safe_and_transactional() -> None:
    sql = MIGRATION.read_text()
    apply = sql.split("CREATE OR REPLACE FUNCTION migration.canonicalize_region_talk_direct_snapshot", 1)[1]
    assert "migration.assert_region_talk_direct_task" in apply
    assert "snapshot.state<>'complete' OR NOT snapshot.integrity_verified" in apply
    assert "snapshot contains quarantine and cannot become canonical" in apply
    assert "canonical apply replay conflicts with immutable receipt" in apply
    assert "hub.advance_canonical_revision" in apply
    assert "INSERT INTO sync.external_outbox" in apply
    assert "INSERT INTO migration.region_talk_canonical_apply_receipt" in apply
    assert "publication dispatch remains disabled" in apply
    assert "INSERT INTO hub.content_item" in sql
    assert "INSERT INTO region_talk.publication_candidate" in apply
    assert "INSERT INTO region_talk.candidate_revision" in apply
    assert "INSERT INTO region_talk.publication_plan" in apply
    assert "INSERT INTO orchestration.work_item" in apply


def test_python_and_sql_use_the_same_fixed_timestamp_length_framing() -> None:
    spec = next(item for item in DIRECT_SOURCE_TABLES if item.name == "region_talk_compact_state_kv")
    row = source_row(
        spec,
        {
            "pk": "ключ-1",
            "kind": "external_publication_intake_item",
            "updated_at": datetime(2026, 8, 19, 22, 1, 2, 3456, tzinfo=UTC),
            "payload_json": {"canonical_url": "https://example.test/a"},
        },
    )
    assert row.logical_sha256 == "e3d783b78cc529ceba0c1473cdb59e14dcd42be54b0f912ea15ea377ae2b36c3"
