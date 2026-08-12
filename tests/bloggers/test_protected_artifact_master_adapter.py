from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.workloads.bloggers.accounting import BloggerExportReceipt
from my_data_hub.workloads.bloggers.importer import ImportReceipt, batch_identity
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerMigrationRequest,
    BloggerStageContext,
    execute_blogger_migration_stage,
)
from my_data_hub.workloads.bloggers.schema import SOURCE_QUERY_SHA256

SNAPSHOT = datetime(2026, 8, 11, 23, 27, 5, tzinfo=UTC)
REVISION = "b" * 40
MASTER = UUID("33333333-3333-4333-8333-333333333333")


def test_active_master_imports_validated_protected_rows_without_ydb(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    events: list[str] = []
    request = BloggerMigrationRequest(
        request_id=UUID("11111111-1111-4111-8111-111111111111"),
        operation_id=UUID("22222222-2222-4222-8222-222222222222"),
        project_id=UUID("44444444-4444-4444-8444-444444444444"),
        snapshot_at=SNAPSHOT,
        source_revision=REVISION,
    )

    class Artifact:
        def assert_import_binding(self, **kwargs):  # type: ignore[no-untyped-def]
            events.append("artifact.binding")
            assert kwargs == {
                "snapshot_at": SNAPSHOT,
                "expected_row_count": 266,
                "source_revision": REVISION,
            }

        def iter_rows(self):  # type: ignore[no-untyped-def]
            events.append("artifact.rows")
            return iter(())

    class Provisioner:
        def __init__(self, connection):  # type: ignore[no-untyped-def]
            pass

        def create(self, **kwargs):  # type: ignore[no-untyped-def]
            events.append("credential.create")

        def drop(self, principal):  # type: ignore[no-untyped-def]
            events.append("credential.drop")

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):  # type: ignore[no-untyped-def]
            return None

        def execute(self, statement):  # type: ignore[no-untyped-def]
            return None

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):  # type: ignore[no-untyped-def]
            return None

        def cursor(self):
            return Cursor()

    export = BloggerExportReceipt(
        export_batch_id=batch_identity(SNAPSHOT, 266),
        exported_at=SNAPSHOT,
        query_sha256=SOURCE_QUERY_SHA256,
        row_count=266,
        distinct_record_ids=266,
        record_id_set_sha256="a" * 64,
        logical_sha256="b" * 64,
        dispositions={"imported": 266, "quarantined": 0},
        undispositioned=0,
        source_file_count=14,
    )
    imported = ImportReceipt(
        export=export,
        canonical_outcome_sha256="c" * 64,
        actor_count=266,
        account_count=100,
        duplicate_group_count=0,
        replayed_count=0,
        canonical_revision=1,
    )

    class Importer:
        def import_rows(self, connection, **kwargs):  # type: ignore[no-untyped-def]
            events.append("postgres.import")
            assert list(kwargs["rows"]) == []
            return imported

    monkeypatch.setattr(
        "my_data_hub.workloads.bloggers.master_stage.load_protected_artifact",
        lambda path: Artifact(),
    )
    monkeypatch.setattr(
        "my_data_hub.workloads.bloggers.master_stage.CredentialProvisioner", Provisioner
    )
    monkeypatch.setattr("psycopg.connect", lambda *args, **kwargs: Connection())
    receipt = execute_blogger_migration_stage(
        BloggerStageContext(
            identity=MasterIdentity(MASTER, "77777777-7777-4777-8777-777777777777", 7),
            request=request,
            local_database_url="postgresql://postgres@/postgres?host=%2Fkaggle%2Fworking%2Fsocket&port=5432",
            lease_until=datetime.now(UTC) + timedelta(minutes=6),
            attempt_id="attempt-1",
        ),
        owner_connection=object(),
        importer=Importer(),
        protected_artifact_manifest=Path("/kaggle/working/private/manifest.json"),
    )

    assert receipt.export_batch_id == batch_identity(SNAPSHOT, 266)
    assert events.index("artifact.binding") < events.index("credential.create")
    assert "artifact.rows" in events
    assert "postgres.import" in events
    assert events[-1] == "credential.drop"
