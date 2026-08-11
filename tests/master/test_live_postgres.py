from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.db.migrations import migrate
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner
from my_data_hub.master_runtime.database_gate import DatabaseGate
from my_data_hub.workloads.bloggers.importer import BloggerSnapshotImporter

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.skipif(
    os.getenv("MDH_RUN_DISPOSABLE_POSTGRES") != "1" or shutil.which("docker") is None,
    reason="set MDH_RUN_DISPOSABLE_POSTGRES=1 for disposable tmpfs PostgreSQL proof",
)
def test_live_old_session_commit_is_rejected_after_fence_and_epoch_rotation() -> None:
    import psycopg

    port = _free_port()
    name = f"mdh-l03-{os.getpid()}"
    password = "fixture-admin-password-not-a-secret"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--tmpfs",
            "/var/lib/postgresql:rw,nosuid,nodev,size=768m",
            "-e",
            f"POSTGRES_PASSWORD={password}",
            "-p",
            f"127.0.0.1:{port}:5432",
            IMAGE,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    admin_url = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(admin_url, connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    logs = subprocess.run(
                        ["docker", "logs", name], capture_output=True, text=True, check=False
                    ).stdout
                    raise AssertionError(f"PostgreSQL did not become ready: {logs[-2000:]}") from None
                time.sleep(0.5)

        with psycopg.connect(admin_url) as admin:
            admin.execute((ROOT / "sql/admin/bootstrap_roles.sql").read_text())
            admin.commit()
        migrate(admin_url, ROOT / "sql/migrations")
        with psycopg.connect(admin_url) as admin:
            admin.execute((ROOT / "sql/admin/role_contract.sql").read_text())
            admin.commit()

        now = datetime.now(UTC)
        a = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "docker-run-a", 1)
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.acquire(a, now + timedelta(minutes=5))
            gate.activate(a)
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e1_writer_deadbeef",
                password="writer-a-password-long-enough",
                group="mdh_application",
                identity=a,
                credential_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e1_embed_facefeed",
                password="embedding-committer-password-long-enough",
                group="mdh_canonical_committer",
                identity=a,
                credential_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )

        a_url = f"postgresql://mdh_e1_writer_deadbeef:writer-a-password-long-enough@127.0.0.1:{port}/postgres"
        old = psycopg.connect(a_url)
        old.execute("SET ROLE mdh_application")
        old.execute(
            "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('a','a','before','ok')"
        )
        old.commit()

        # Begin a transaction while A is active, then fence it.  The deferred guard
        # re-evaluates at commit, proving already-open sessions cannot sneak a commit.
        old.execute(
            "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('a','a','after','blocked')"
        )
        committer_url = (
            f"postgresql://mdh_e1_embed_facefeed:embedding-committer-password-long-enough"
            f"@127.0.0.1:{port}/postgres"
        )
        stale_committer = psycopg.connect(committer_url)
        stale_committer.execute("SET ROLE mdh_canonical_committer")
        stale_committer.execute(
            "INSERT INTO sync.external_outbox(aggregate_type,effect_kind,idempotency_key,payload,required_revision) "
            "VALUES ('embedding_import','verified_checkpoint_required','live-fenced-embedding',"
            "'{\"artifact_id\":\"fenced\"}'::jsonb,0)"
        )
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.fence(a, "forced_rotation")
        with pytest.raises(psycopg.Error, match="epoch lease gate"):
            old.commit()
        old.rollback()
        with pytest.raises(psycopg.Error, match="epoch lease gate"):
            stale_committer.commit()
        stale_committer.rollback()

        # Epoch 2 was allocated by the owner-authoritative control ledger to a
        # failed attempt.  The next restored master must reconcile directly to 3.
        b = MasterIdentity(UUID("22222222-2222-4222-8222-222222222222"), "docker-run-b", 3)
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.acquire(b, now + timedelta(minutes=6))
            gate.activate(b)
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e3_writer_cafebabe",
                password="writer-b-password-long-enough",
                group="mdh_application",
                identity=b,
                credential_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e3_migration_deadbeef",
                password="migration-password-long-enough",
                group="mdh_migration_operator",
                identity=b,
                credential_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e3_operator_a11ce001",
                password="operator-password-long-enough",
                group="mdh_mcp_editor",
                identity=b,
                credential_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )

        # A remains connected and knows B's public epoch, but session_user is
        # immutably bound to epoch 1 and therefore remains fenced.
        old.execute("SET ROLE mdh_application")
        old.execute(
            "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('a','a','stale','blocked')"
        )
        with pytest.raises(psycopg.Error, match="epoch lease gate"):
            old.commit()
        old.close()
        # The dedicated canonical committer remains rejected after a newer
        # epoch activates, even though its PostgreSQL session is still open.
        stale_committer.execute("SET ROLE mdh_canonical_committer")
        stale_committer.execute(
            "INSERT INTO sync.external_outbox(aggregate_type,effect_kind,idempotency_key,payload,required_revision) "
            "VALUES ('embedding_import','verified_checkpoint_required','live-stale-embedding',"
            "'{\"artifact_id\":\"stale\"}'::jsonb,0)"
        )
        with pytest.raises(psycopg.Error, match="epoch lease gate"):
            stale_committer.commit()
        stale_committer.close()

        b_url = f"postgresql://mdh_e3_writer_cafebabe:writer-b-password-long-enough@127.0.0.1:{port}/postgres"
        with psycopg.connect(b_url) as current:
            current.execute("SET ROLE mdh_application")
            current.execute(
                "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('b','b','current','ok')"
            )
            current.commit()
        migration_url = (
            f"postgresql://mdh_e3_migration_deadbeef:migration-password-long-enough"
            f"@127.0.0.1:{port}/postgres"
        )
        blogger_row = {
            "record_id": "live-test-001",
            "batch_id": "live-batch-001",
            "list_order": 1,
            "level": "regional",
            "blogger_name": "Тестовый автор",
            "segment": "культура",
            "region_relation_status": "external",
            "visit_period_text": "2025",
            "locations_text": "Россия",
            "confirmation_basis": "public profile",
            "evidence_url": "https://example.test/evidence/live-test-001",
            "telegram_url": "https://t.me/live_test_001",
            "vk_public_url": None,
            "vk_video_url": None,
            "rutube_url": None,
            "source_kind": "manual_external_confirmation",
            "confirmation_status": "confirmed_external",
            "pipeline_status": "stored_only",
            "source_file_sha256": "a" * 64,
            "ingested_at": "2026-08-03T13:30:00Z",
            "updated_at": "2026-08-03T13:31:00Z",
            "external_region_basis": None,
            "external_region_evidence_url": None,
            "submission_batch_ids_json": None,
            "other_primary_url": None,
            "social_links_type": None,
            "evidence_type": None,
        }
        with psycopg.connect(admin_url) as admin:
            project_id = admin.execute("SELECT project_id FROM hub.project WHERE slug='region-talk'").fetchone()[0]
        with psycopg.connect(migration_url, autocommit=True) as migration:
            migration.execute("SET ROLE mdh_migration_operator")
            first_import = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, tzinfo=UTC),
                expected_row_count=1,
                rows=[blogger_row],
                source_code_revision="fixture",
            )
            assert first_import.accounting_complete
            assert not first_import.durable_complete
            assert first_import.replayed_count == 0
            assert first_import.canonical_revision == 1
            replay = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, tzinfo=UTC),
                expected_row_count=1,
                rows=[blogger_row],
                source_code_revision="fixture",
            )
            assert replay.replayed_count == 1
            assert replay.canonical_revision == first_import.canonical_revision

            conflicting_replay_row = {**blogger_row, "blogger_name": "Подменённое имя"}
            conflict = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, tzinfo=UTC),
                expected_row_count=1,
                rows=[conflicting_replay_row],
                source_code_revision="fixture",
            )
            assert not conflict.accounting_complete
            assert conflict.durability_state == "BLOCKED_QUARANTINE"
            assert conflict.export.dispositions["quarantined"] == 1
            conflict_replay = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, tzinfo=UTC),
                expected_row_count=1,
                rows=[conflicting_replay_row],
                source_code_revision="fixture",
            )
            assert conflict_replay == conflict

            duplicate_row = {
                **blogger_row,
                "record_id": "live-test-duplicate",
                "batch_id": "live-batch-duplicate",
                "blogger_name": "Другой тестовый автор",
                "evidence_url": "https://example.test/evidence/live-test-duplicate",
            }
            duplicate = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, 0, 0, 1, tzinfo=UTC),
                expected_row_count=1,
                rows=[duplicate_row],
                source_code_revision="fixture",
            )
            assert not duplicate.accounting_complete
            assert duplicate.duplicate_group_count == 1

            malformed_row = {**blogger_row, "unknown_source_column": "x" * 200_000}
            malformed = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, 0, 0, 2, tzinfo=UTC),
                expected_row_count=1,
                rows=[malformed_row],
                source_code_revision="fixture",
            )
            assert not malformed.accounting_complete
            assert malformed.export.dispositions["quarantined"] == 1
            malformed_replay = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, 0, 0, 2, tzinfo=UTC),
                expected_row_count=1,
                rows=[malformed_row],
                source_code_revision="fixture",
            )
            assert malformed_replay == malformed

            oversized = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, 0, 0, 3, tzinfo=UTC),
                expected_row_count=1,
                rows=[{**blogger_row, "blogger_name": "я" * 20_000}],
                source_code_revision="fixture",
            )
            assert not oversized.accounting_complete
            assert oversized.export.dispositions["quarantined"] == 1

            missing = BloggerSnapshotImporter().import_rows(
                migration,
                project_id=project_id,
                snapshot_at=datetime(2026, 8, 11, 0, 0, 4, tzinfo=UTC),
                expected_row_count=1,
                rows=[],
                source_code_revision="fixture",
            )
            assert not missing.accounting_complete
            assert missing.export.row_count == 0
        with psycopg.connect(admin_url) as admin:
            # Faults preserve terminal raw evidence but never advance canonical
            # state or create a second required checkpoint effect.
            assert admin.execute("SELECT count(*) FROM sync.audit_event").fetchone()[0] == 3
            assert admin.execute("SELECT count(*) FROM region_talk.bloggers_ru_v1").fetchone()[0] == 1
            assert admin.execute(
                "SELECT count(*) FROM sync.external_outbox "
                "WHERE aggregate_type='blogger_import' AND effect_kind='verified_checkpoint_required'"
            ).fetchone()[0] == 1
            assert admin.execute(
                "SELECT count(*) FROM migration.export_batch "
                "WHERE source_scope='region-talk-bloggers-v1'"
            ).fetchone()[0] == 5
            assert admin.execute(
                "SELECT count(*) FROM migration.duplicate_group"
            ).fetchone()[0] == 1
            assert admin.execute(
                "SELECT decision_status FROM migration.duplicate_group"
            ).fetchone()[0] == "quarantined"
            assert admin.execute("SELECT count(*) FROM migration.raw_record").fetchone()[0] == 5
            assert admin.execute(
                "SELECT count(*) FROM migration.raw_record raw "
                "JOIN migration.row_disposition disposition USING(raw_record_id)"
            ).fetchone()[0] == 5
            assert admin.execute(
                "SELECT count(*) FROM migration.row_disposition WHERE disposition='quarantined'"
            ).fetchone()[0] == 4
            assert admin.execute(
                "SELECT count(*) FROM migration.row_disposition "
                "WHERE disposition='quarantined' "
                "AND reason_code='same_source_key_different_payload'"
            ).fetchone()[0] == 1
            assert admin.execute(
                "SELECT payload->>'blogger_name' FROM migration.raw_record "
                "WHERE source_pk='live-test-001'"
            ).fetchone()[0] == "Тестовый автор"
            assert admin.execute(
                "SELECT array_agg(reason_code ORDER BY reason_code) "
                "FROM migration.row_disposition WHERE disposition='quarantined'"
            ).fetchone()[0] == [
                "duplicate_account_requires_explicit_resolution",
                "oversized_source_value",
                "same_source_key_different_payload",
                "unknown_source_value",
            ]
            assert admin.execute(
                "SELECT count(*) FROM migration.batch_accounting "
                "WHERE undispositioned_count <> 0"
            ).fetchone()[0] == 0
            assert admin.execute(
                "SELECT max(pg_column_size(payload)) FROM migration.raw_record "
                "WHERE payload->>'schema_version'='region-talk-blogger-quarantine-evidence.v1'"
            ).fetchone()[0] < 128 * 1024
            state = admin.execute(
                "SELECT highest_epoch,current_epoch,gate_state FROM master_control.epoch_state"
            ).fetchone()
            assert state == (3, 3, "open")
            assert admin.execute(
                "SELECT schema_revision FROM hub.canonical_state WHERE singleton"
            ).fetchone()[0] == 17
            assert admin.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton"
            ).fetchone()[0] == first_import.canonical_revision
            assert admin.execute("SELECT count(*) FROM search.embedding_model").fetchone()[0] == 2
            assert admin.execute(
                "SELECT count(*) FROM pg_indexes WHERE schemaname='search' AND indexdef ILIKE '%hnsw%'"
            ).fetchone()[0] == 0
            assert admin.execute(
                "SELECT has_table_privilege('mdh_mcp_reader','migration.raw_record','SELECT')"
            ).fetchone()[0] is False
            assert admin.execute(
                "SELECT has_table_privilege('mdh_migration_operator','migration.raw_record','INSERT'), "
                "has_table_privilege('mdh_migration_operator','migration.raw_record','UPDATE')"
            ).fetchone() == (True, False)

        operator_url = (
            f"postgresql://mdh_e3_operator_a11ce001:operator-password-long-enough"
            f"@127.0.0.1:{port}/postgres"
        )
        with psycopg.connect(operator_url) as operator:
            operator.execute("SET ROLE mdh_mcp_editor")
            operator.execute(
                "INSERT INTO hub.project(project_id,slug,name,description,status,metadata) "
                "VALUES (%s,%s,%s,%s,%s,%s::jsonb)",
                (
                    UUID("33333333-3333-4333-8333-333333333333"),
                    "operator-no-receipt",
                    "Must roll back",
                    "receipt guard proof",
                    "active",
                    "{}",
                ),
            )
            with pytest.raises(psycopg.Error, match="lacks transactional receipt/outbox"):
                operator.commit()
            operator.rollback()

            operator.execute("SET ROLE mdh_mcp_editor")
            operator.execute(
                "INSERT INTO hub.project(project_id,slug,name,description,status,metadata) "
                "VALUES (%s,%s,%s,%s,%s,%s::jsonb)",
                (
                    UUID("44444444-4444-4444-8444-444444444444"),
                    "operator-committed",
                    "Bounded operator proof",
                    "same transaction audit and outbox",
                    "active",
                    "{}",
                ),
            )
            revision = operator.execute(
                "SELECT operator_control.commit_mcp_change_v2(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    "f" * 64,
                    "e" * 64,
                    first_import.canonical_revision,
                    "hub.project",
                    "insert",
                    1,
                    "1" * 64,
                    "2" * 64,
                    "owner",
                    "owner-operator",
                ),
            ).fetchone()[0]
            operator.commit()
            assert revision == first_import.canonical_revision + 1
            reconciled = operator.execute(
                "SELECT affected_rows,revision_after FROM operator_control.reconcile_mcp_change("
                "%s,%s,%s,%s,%s,%s,%s)",
                (
                    "f" * 64,
                    "e" * 64,
                    b.master_instance_id,
                    b.epoch,
                    first_import.canonical_revision,
                    "owner",
                    "owner-operator",
                ),
            ).fetchone()
            assert reconciled == (1, revision)
            with pytest.raises(psycopg.Error, match="differs from exact reconciliation request"):
                operator.execute(
                    "SELECT * FROM operator_control.reconcile_mcp_change(%s,%s,%s,%s,%s,%s,%s)",
                    (
                        "f" * 64,
                        "d" * 64,
                        b.master_instance_id,
                        b.epoch,
                        first_import.canonical_revision,
                        "owner",
                        "owner-operator",
                    ),
                )
            operator.rollback()

        with psycopg.connect(admin_url) as admin:
            assert admin.execute(
                "SELECT count(*) FROM hub.project WHERE slug='operator-committed'"
            ).fetchone()[0] == 1
            assert admin.execute(
                "SELECT count(*) FROM sync.audit_event "
                "WHERE action='mcp_operator_change' AND details->>'operation_id'=%s",
                ("f" * 64,),
            ).fetchone()[0] == 1
            assert admin.execute(
                "SELECT required_revision FROM sync.external_outbox "
                "WHERE idempotency_key=%s",
                ("mcp-operator:" + "f" * 64,),
            ).fetchone()[0] == revision
            assert admin.execute(
                "SELECT has_column_privilege('mdh_mcp_editor','hub.project','name','UPDATE'), "
                "has_column_privilege('mdh_mcp_editor','hub.project','revision','UPDATE'), "
                "has_table_privilege('mdh_mcp_editor','hub.canonical_state','UPDATE'), "
                "has_function_privilege('mdh_mcp_editor',"
                "'operator_control.commit_mcp_change_v2(text,text,bigint,text,text,integer,text,text,text,text)',"
                "'EXECUTE'), "
                "has_function_privilege('mdh_mcp_editor',"
                "'operator_control.commit_mcp_change(text,bigint,text,text,integer,text,text,text,text)',"
                "'EXECUTE')"
            ).fetchone() == (True, False, False, True, False)
    finally:
        subprocess.run(["docker", "rm", "--force", name], check=False, capture_output=True)
