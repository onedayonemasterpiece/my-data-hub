from __future__ import annotations

import hashlib
import os
import shutil
import socket
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path

import pytest

from my_data_hub.db.migrations import migrate
from my_data_hub.workloads.bloggers.importer import BloggerSnapshotImporter, DuplicateResolution
from my_data_hub.workloads.bloggers.schema import BloggerSourceRow
from my_data_hub.workloads.bloggers.transform import transform_row

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _row(record_id: str, name: str) -> dict[str, object]:
    return {
        "record_id": record_id, "batch_id": "duplicate-batch", "list_order": 1,
        "level": "person", "blogger_name": name, "segment": "культура",
        "region_relation_status": "external", "visit_period_text": "2025",
        "locations_text": "Россия", "confirmation_basis": "owner-reviewed evidence",
        "evidence_url": f"https://example.test/evidence/{record_id}",
        "telegram_url": "https://t.me/shared_identity", "vk_public_url": None,
        "vk_video_url": None, "rutube_url": None,
        "source_kind": "manual_external_confirmation", "confirmation_status": "confirmed_external",
        "pipeline_status": "stored_only", "source_file_sha256": "a" * 64,
        "ingested_at": "2026-08-03T13:30:00Z", "updated_at": "2026-08-03T13:31:00Z",
        "external_region_basis": None, "external_region_evidence_url": None,
        "submission_batch_ids_json": None, "other_primary_url": None,
        "social_links_type": "person", "evidence_type": None,
    }


@pytest.mark.skipif(
    os.getenv("MDH_RUN_DISPOSABLE_POSTGRES") != "1" or shutil.which("docker") is None,
    reason="set MDH_RUN_DISPOSABLE_POSTGRES=1 for disposable tmpfs PostgreSQL proof",
)
def test_duplicate_quarantine_explicit_resolution_and_exact_replay_are_lossless() -> None:
    import psycopg

    port = _free_port()
    name = f"mdh-h5-{os.getpid()}"
    password = "fixture-admin-password-not-a-secret"
    subprocess.run(
        [
            "docker", "run", "--detach", "--rm", "--name", name,
            "--tmpfs", "/var/lib/postgresql:rw,nosuid,nodev,size=768m",
            "-e", f"POSTGRES_PASSWORD={password}", "-p", f"127.0.0.1:{port}:5432", IMAGE,
        ],
        check=True, capture_output=True, text=True,
    )
    database_url = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(database_url, connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise AssertionError("disposable PostgreSQL did not become ready") from None
                time.sleep(0.5)
        with psycopg.connect(database_url) as connection:
            connection.execute((ROOT / "sql/admin/bootstrap_roles.sql").read_text())
            connection.commit()
        migrate(database_url, ROOT / "sql/migrations")
        with psycopg.connect(database_url) as connection:
            connection.execute((ROOT / "sql/admin/role_contract.sql").read_text())
            connection.commit()

        rows = [_row("blogger-001", "Канонический автор"), _row("blogger-002", "Дубликат")]
        snapshot_at = datetime(2026, 8, 11, tzinfo=UTC)
        identity_hash = hashlib.sha256(
            b"telegram\0https://t.me/shared_identity"
        ).hexdigest()
        canonical_actor = transform_row(BloggerSourceRow.from_mapping(rows[0])).actor_id
        resolution = DuplicateResolution(
            identity_sha256=identity_hash,
            canonical_record_id="blogger-001",
            canonical_actor_id=canonical_actor,
            member_record_ids=("blogger-001", "blogger-002"),
            decided_by="owner-review:disposable-postgres",
            reason="The exact test evidence identifies one person.",
        )
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("SET ROLE mdh_migration_operator")
            project_id = connection.execute(
                "SELECT project_id FROM hub.project WHERE slug='region-talk'"
            ).fetchone()[0]
            blocked = BloggerSnapshotImporter().import_rows(
                connection, project_id=project_id, snapshot_at=snapshot_at,
                expected_row_count=2, rows=rows, source_code_revision="fixture",
            )
            assert not blocked.accounting_complete
            assert blocked.duplicate_group_count == blocked.duplicate_groups_pending == 1
            assert blocked.export.dispositions["quarantined"] == 2
            assert tuple(group.identity_sha256 for group in blocked.duplicate_review_groups) == (
                identity_hash,
            )
            assert blocked.duplicate_review_groups[0].member_record_ids == (
                "blogger-001",
                "blogger-002",
            )

            stale = DuplicateResolution(
                identity_sha256="f" * 64,
                canonical_record_id="blogger-001",
                canonical_actor_id=canonical_actor,
                member_record_ids=("blogger-001", "blogger-002"),
                decided_by="owner-review:disposable-postgres",
                reason="This stale identity must not be applied.",
            )
            conflict = BloggerSnapshotImporter().import_rows(
                connection, project_id=project_id, snapshot_at=snapshot_at,
                expected_row_count=2, rows=rows, source_code_revision="fixture",
                duplicate_resolutions=(stale,),
            )
            assert conflict == blocked
            assert connection.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton"
            ).fetchone()[0] == 0
            assert connection.execute(
                "SELECT count(*) FROM migration.blogger_replay"
            ).fetchone()[0] == 0

            resolved = BloggerSnapshotImporter().import_rows(
                connection, project_id=project_id, snapshot_at=snapshot_at,
                expected_row_count=2, rows=rows, source_code_revision="fixture",
                duplicate_resolutions=(resolution,),
            )
            assert resolved.accounting_complete
            assert resolved.actor_count == 1
            assert resolved.duplicate_group_count == 1
            assert resolved.duplicate_groups_pending == 0
            assert resolved.export.dispositions["normalized"] == 1
            assert resolved.export.dispositions["deduplicated"] == 1
            assert resolved.export.dispositions["quarantined"] == 0
            assert resolved.replayed_count == 2

            replay = BloggerSnapshotImporter().import_rows(
                connection, project_id=project_id, snapshot_at=snapshot_at,
                expected_row_count=2, rows=rows, source_code_revision="fixture",
            )
            assert replay == resolved

        with psycopg.connect(database_url) as connection:
            assert connection.execute(
                "SELECT count(*) FROM migration.raw_record"
            ).fetchone()[0] == 2
            assert connection.execute(
                "SELECT count(*) FROM migration.row_disposition WHERE disposition='quarantined'"
            ).fetchone()[0] == 2
            assert connection.execute(
                "SELECT array_agg(disposition ORDER BY disposition) "
                "FROM migration.blogger_replay_disposition"
            ).fetchone()[0] == ["deduplicated", "normalized"]
            assert connection.execute(
                "SELECT duplicate_group_count,resolved_duplicate_group_count,duplicate_groups_pending "
                "FROM migration.blogger_duplicate_accounting"
            ).fetchone() == (1, 1, 0)
            assert connection.execute(
                "SELECT count(*),count(DISTINCT target_pk->>'actor_id') "
                "FROM migration.legacy_identity_map WHERE source_table='region_talk_external_blogger_evidence'"
            ).fetchone() == (2, 1)
            assert connection.execute(
                "SELECT count(*) FROM region_talk.bloggers_ru_v1"
            ).fetchone()[0] == 1
            assert connection.execute(
                "SELECT count(*) FROM migration.blogger_replay"
            ).fetchone()[0] == 1
            assert connection.execute(
                "SELECT count(*) FROM sync.external_outbox "
                "WHERE aggregate_type='blogger_import' AND effect_kind='verified_checkpoint_required'"
            ).fetchone()[0] == 1
            assert connection.execute(
                "SELECT schema_revision,canonical_revision FROM hub.canonical_state WHERE singleton"
                ).fetchone() == (16, 1)
            connection.execute("SET ROLE mdh_migration_operator")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(
                    "UPDATE migration.blogger_duplicate_resolution SET reason='overwrite'"
                )
    finally:
        subprocess.run(["docker", "rm", "--force", name], check=False, capture_output=True)
