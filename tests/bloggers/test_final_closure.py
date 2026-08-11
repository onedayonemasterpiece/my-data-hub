from __future__ import annotations

import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, RefResolver

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.workloads.bloggers.closure import (
    ClosureConfig,
    modern_kaggle_token_configured,
    run_blogger_closure,
)
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
)
from my_data_hub.workloads.bloggers.schema import SOURCE_QUERY_SHA256

OPERATION = UUID("11111111-1111-4111-8111-111111111111")
REQUEST = UUID("22222222-2222-4222-8222-222222222222")
MASTER = UUID("33333333-3333-4333-8333-333333333333")
BATCH = UUID("44444444-4444-4444-8444-444444444444")
CHECKPOINT = UUID("55555555-5555-4555-8555-555555555555")
PROJECT = UUID("66666666-6666-4666-8666-666666666666")
SHA = "a" * 64


def request() -> BloggerMigrationRequest:
    return BloggerMigrationRequest(
        request_id=REQUEST,
        operation_id=OPERATION,
        project_id=PROJECT,
        snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        source_revision="b" * 40,
    )


def imported() -> BloggerImportStageReceipt:
    value = request()
    return BloggerImportStageReceipt(
        request_id=REQUEST,
        operation_id=OPERATION,
        master_instance_id=MASTER,
        run_id="77777777-7777-4777-8777-777777777777",
        epoch=7,
        request_sha256=value.request_sha256,
        export_batch_id=BATCH,
        row_count=266,
        distinct_record_ids=266,
        source_file_count=14,
        dispositions={"imported": 266, "quarantined": 0},
        record_id_set_sha256=SHA,
        logical_sha256="b" * 64,
        canonical_outcome_sha256="c" * 64,
        actor_count=266,
        account_count=210,
        duplicate_group_count=0,
        replayed_count=0,
        canonical_revision=9,
    )


class FakeControl:
    def __init__(self) -> None:
        self.created: BloggerMigrationRequest | None = None

    def ensure_master(self, idempotency_key: str) -> dict[str, object]:
        assert idempotency_key.endswith(":master")
        return {"operation_id": str(OPERATION)}

    def master_status(self) -> dict[str, object]:
        return {"master_state": "ACTIVE", "master_instance_id": str(MASTER), "master_epoch": 7}

    def create_request(self, value: BloggerMigrationRequest) -> dict[str, object]:
        self.created = value
        return {"request_sha256": value.request_sha256}

    def request_status(self, request_id: UUID) -> dict[str, object]:
        assert request_id == self.created.request_id  # type: ignore[union-attr]
        receipt = imported().model_copy(update={
            "request_id": self.created.request_id,
            "request_sha256": self.created.request_sha256,
        })
        return {
            "state": "CHECKPOINT_VERIFIED",
            "claimed_run_id": receipt.run_id,
            "claimed_attempt_id": "88888888-8888-4888-8888-888888888888",
            "claimed_master_instance_id": str(MASTER),
            "claimed_epoch": 7,
            "import_receipt": receipt.model_dump(mode="json"),
            "checkpoint_receipt": {
                "request_id": str(REQUEST),
                "checkpoint_id": str(CHECKPOINT),
                "current_checkpoint_id": str(CHECKPOINT),
                "manifest_sha256": "d" * 64,
                "canonical_revision": 9,
            },
        }


class FakeMcp:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []

    def call(self, tool: str, arguments: dict[str, object]) -> dict[str, object]:
        self.calls.append((tool, arguments))
        receipt = imported()
        if tool == "checkpoint.status":
            return {
                "generation": 3,
                "current_checkpoint_id": str(CHECKPOINT),
                "current": {
                    "checkpoint_id": str(CHECKPOINT),
                    "manifest_sha256": "d" * 64,
                    "exact_version_ref": "private/checkpoints/4",
                    "status": "VERIFIED",
                },
            }
        if tool == "master.rotation.request":
            assert arguments["expected_canonical_revision"] == 9
            assert arguments["expected_active_epoch"] == 7
            return {"operation_id": "rotation-operation"}
        if tool == "operation.get":
            return {"state": "DURABLE_COMPLETE"}
        if tool == "master.status":
            return {
                "master_state": "ACTIVE",
                "instance_id": "99999999-9999-4999-8999-999999999999",
                "master_epoch": 8,
                "canonical_revision": 9,
            }
        if tool == "bloggers.migration.accounting":
            return {
                "found": True,
                "canonical_revision": 9,
                "accounting": {
                    "export_batch_id": str(BATCH),
                    "expected_row_count": 266,
                    "status": "accepted",
                    "logical_sha256": receipt.logical_sha256,
                    "record_id_set_sha256": receipt.record_id_set_sha256,
                    "canonical_outcome_sha256": receipt.canonical_outcome_sha256,
                    "duplicate_groups_pending": 0,
                    "imported_canonical_revision": 9,
                    "raw_count": 266,
                    "dispositioned_count": 266,
                    "undispositioned_count": 0,
                    "quarantined_count": 0,
                    "actor_count": 266,
                    "account_count": 210,
                    "checkpoint_required": True,
                },
            }
        if tool == "bloggers.statistics":
            return {"canonical_revision": 9, "statistics": {"bloggers": 266, "requires_review": 0, "with_public_accounts": 210}}
        raise AssertionError(tool)


def config() -> ClosureConfig:
    return ClosureConfig(
        control_url="https://control.example",
        control_token="control-token-long-enough-for-validation",
        idempotency_key="final-blogger-test",
        project_id=PROJECT,
        snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        source_revision="b" * 40,
        timeout_seconds=600,
        poll_seconds=1,
    )


def test_final_closure_reaches_durable_complete_only_after_restore_and_mcp() -> None:
    mcp = FakeMcp()
    receipt = run_blogger_closure(config(), control=FakeControl(), mcp=mcp)
    assert receipt["status"] == "DURABLE_COMPLETE"
    assert receipt["ensure_operation_id"] == str(OPERATION)
    assert receipt["import_runtime"]["epoch"] == 7
    assert receipt["checkpoint"]["checkpoint_id"] == str(CHECKPOINT)
    assert receipt["cold_restore"]["epoch"] == 8
    assert [name for name, _ in mcp.calls] == [
        "checkpoint.status", "master.rotation.request", "operation.get", "master.status",
        "bloggers.migration.accounting", "bloggers.statistics",
    ]


def test_receipts_validate_against_strict_schemas() -> None:
    receipt = run_blogger_closure(config(), control=FakeControl(), mcp=FakeMcp())
    root = Path("schemas")
    import_schema = json.loads((root / "region-talk-ydb-bloggers-import-receipt.v2.schema.json").read_text())
    final_schema = json.loads((root / "blogger-closure-receipt.v1.schema.json").read_text())
    Draft202012Validator(import_schema).validate(receipt["import_receipt"])
    resolver = RefResolver(
        base_uri=(root.resolve().as_uri() + "/"),
        referrer=final_schema,
        store={import_schema["$id"]: import_schema},
    )
    Draft202012Validator(final_schema, resolver=resolver).validate(receipt)


def test_accounting_mismatch_never_emits_durable_complete() -> None:
    mcp = FakeMcp()
    original = mcp.call

    def mismatch(tool: str, arguments: dict[str, object]) -> dict[str, object]:
        result = original(tool, arguments)
        if tool == "bloggers.migration.accounting":
            result["accounting"]["raw_count"] = 265  # type: ignore[index]
        return result

    mcp.call = mismatch  # type: ignore[method-assign]
    with pytest.raises(RuntimeError, match="accounting differs"):
        run_blogger_closure(config(), control=FakeControl(), mcp=mcp)


def test_modern_token_absence_is_detected_without_creating_state(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path))
    assert modern_kaggle_token_configured() is False
    assert list(tmp_path.iterdir()) == []
    token = tmp_path / "access_token"
    token.write_text("x" * 32)
    assert modern_kaggle_token_configured() is True
    token.unlink()
    target = tmp_path / "other"
    target.write_text("x" * 32)
    token.symlink_to(target)
    assert modern_kaggle_token_configured() is False


def test_control_ledger_claims_one_exact_runtime_and_preserves_metadata_only(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    value = request()
    record, created = ledger.ensure_blogger_migration_request(
        request_id=str(REQUEST), operation_id=str(OPERATION), request_sha256=value.request_sha256,
        request=value.model_dump(mode="json"),
    )
    assert created and record["state"] == "REQUESTED"
    claimed = ledger.claim_blogger_migration_request(
        operation_id=str(OPERATION), run_id="run", attempt_id="attempt",
        master_instance_id=str(MASTER), epoch=7,
    )
    assert claimed is not None and claimed["claimed_epoch"] == 7
    serialized = json.dumps(claimed).lower()
    assert "blogger_name" not in serialized
    assert "postgresql://" not in serialized
    assert "ydb_access_token" not in serialized
    with pytest.raises(Exception, match="another runtime epoch"):
        ledger.claim_blogger_migration_request(
            operation_id=str(OPERATION), run_id="other", attempt_id="attempt",
            master_instance_id=str(MASTER), epoch=7,
        )


def test_cli_exits_78_before_control_or_receipt_mutation_without_modern_token(tmp_path) -> None:
    receipt = tmp_path / "must-not-exist.json"
    environment = dict(os.environ)
    environment.pop("KAGGLE_API_TOKEN", None)
    environment["KAGGLE_CONFIG_DIR"] = str(tmp_path / "no-token")
    completed = subprocess.run(
        [
            sys.executable, "scripts/bloggers/run_final_closure.py", "run",
            "--idempotency-key", "no-token-test",
            "--project-id", str(PROJECT),
            "--snapshot-at", "2026-08-09T00:00:00Z",
            "--source-revision", "b" * 40,
            "--receipt", str(receipt),
        ],
        cwd=Path.cwd(), env=environment, check=False, capture_output=True, text=True,
    )
    assert completed.returncode == 78
    assert not receipt.exists()
    assert not (tmp_path / "control.sqlite3").exists()


def test_in_master_stage_uses_epoch_bound_migration_login_and_drops_it(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import sys
    from types import SimpleNamespace

    from my_data_hub.master_runtime.contracts import MasterIdentity
    from my_data_hub.workloads.bloggers.accounting import BloggerExportReceipt
    from my_data_hub.workloads.bloggers.importer import ImportReceipt
    from my_data_hub.workloads.bloggers.master_stage import (
        BloggerStageContext,
        execute_blogger_migration_stage,
    )

    events: list[str] = []

    class Provisioner:
        def __init__(self, connection): events.append("provisioner")
        def create(self, **kwargs):
            events.append("credential.create")
            assert kwargs["group"] == "mdh_migration_operator"
            assert kwargs["policy"].connection_limit == 1
        def drop(self, principal): events.append("credential.drop")

    class Cursor:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def execute(self, statement):
            events.append(statement)

    class Connection:
        def __enter__(self): return self
        def __exit__(self, *args): return None
        def cursor(self): return Cursor()

    class Snapshot:
        def __init__(self, driver): pass
        def assert_write_denied(self): events.append("ydb.denied")
        def iter_rows(self):
            class Rows:
                def __enter__(self):
                    events.append("ydb.rows")
                    return iter(())
                def __exit__(self, *args): return None
            return Rows()

    export = BloggerExportReceipt(
        export_batch_id=BATCH,
        exported_at=datetime(2026, 8, 9, tzinfo=UTC),
        query_sha256=SOURCE_QUERY_SHA256,
        row_count=266,
        distinct_record_ids=266,
        record_id_set_sha256=SHA,
        logical_sha256="b" * 64,
        dispositions={"imported": 266, "quarantined": 0},
        undispositioned=0,
        source_file_count=14,
    )
    result = ImportReceipt(
        export=export,
        canonical_outcome_sha256="c" * 64,
        actor_count=266,
        account_count=210,
        duplicate_group_count=0,
        replayed_count=0,
        canonical_revision=9,
    )

    class Importer:
        def import_rows(self, connection, **kwargs):
            events.append("postgres.import")
            assert kwargs["expected_row_count"] == 266
            assert kwargs["source_code_revision"] == "b" * 40
            return result

    monkeypatch.setitem(sys.modules, "ydb", SimpleNamespace())
    monkeypatch.setattr("my_data_hub.workloads.bloggers.master_stage.CredentialProvisioner", Provisioner)
    monkeypatch.setattr("my_data_hub.workloads.bloggers.master_stage.YdbBloggerSnapshot", Snapshot)
    monkeypatch.setattr("psycopg.connect", lambda *args, **kwargs: Connection())
    receipt = execute_blogger_migration_stage(
        BloggerStageContext(
            identity=MasterIdentity(MASTER, "77777777-7777-4777-8777-777777777777", 7),
            request=request(),
            local_database_url="postgresql://postgres@/postgres?host=%2Fkaggle%2Fworking%2Fsocket&port=5432",
            lease_until=datetime.now(UTC).replace(microsecond=0) + __import__("datetime").timedelta(minutes=3),
        ),
        owner_connection=object(),
        driver=object(),
        importer=Importer(),
    )
    assert receipt.transaction_committed is True
    assert receipt.row_count == receipt.distinct_record_ids == receipt.actor_count == 266
    assert events.index("credential.create") < events.index("ydb.denied") < events.index("postgres.import") < events.index("credential.drop")
