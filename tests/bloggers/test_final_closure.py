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
    CANONICAL_MCP_URL,
    LOCAL_CONTROL_URL,
    ClosureConfig,
    StreamableHttpClosureMcp,
    modern_kaggle_token_configured,
    run_blogger_closure,
)
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerDuplicateResolutionEnvelope,
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
        receipt = imported().model_copy(
            update={
                "request_id": self.created.request_id,
                "request_sha256": self.created.request_sha256,
            }
        )
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
        self.master_status_calls = 0
        self.list_offset = 0
        self.bloggers = [
            {"blogger_id": str(UUID(int=index + 1)), "display_name": f"Blogger {index + 1}"} for index in range(266)
        ]

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
            self.master_status_calls += 1
            if self.master_status_calls == 1:
                return {"master_state": "ABSENT"}
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
            return {
                "canonical_revision": 9,
                "statistics": {"bloggers": 266, "requires_review": 0, "with_public_accounts": 210},
            }
        if tool == "bloggers.list":
            start = self.list_offset
            page = self.bloggers[start : start + 100]
            self.list_offset += len(page)
            complete = self.list_offset == len(self.bloggers)
            return {
                "items": page,
                "cursor": None if complete else page[-1]["blogger_id"],
                "complete": complete,
            }
        if tool == "bloggers.get":
            return {"found": True, "blogger": self.bloggers[0]}
        if tool == "bloggers.provenance":
            return {"items": [{"event_type": "imported"}], "complete": True}
        if tool == "bloggers.search":
            return {
                "items": [self.bloggers[0]],
                "retrievers": {
                    "requested": ["exact", "fts", "e5", "bge_m3"],
                    "completed": ["exact", "fts"],
                    "unavailable": ["e5", "bge_m3"],
                },
            }
        raise AssertionError(tool)


def config() -> ClosureConfig:
    return ClosureConfig(
        control_url=LOCAL_CONTROL_URL,
        idempotency_key="final-blogger-test",
        project_id=PROJECT,
        snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        source_revision="b" * 40,
        timeout_seconds=600,
        poll_seconds=1,
    )


@pytest.mark.parametrize(
    "url",
    [
        "https://control.example",
        "http://localhost:8080",
        "http://127.0.0.1:8081",
        "http://user@127.0.0.1:8080",
    ],
)
def test_closure_control_rejects_noncanonical_or_nonloopback_url(url: str) -> None:
    with pytest.raises(ValueError, match="loopback-only"):
        ClosureConfig(
            control_url=url,
            idempotency_key="final-blogger-test",
            project_id=PROJECT,
            snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
            source_revision="b" * 40,
            timeout_seconds=600,
            poll_seconds=1,
        )


@pytest.mark.parametrize(
    "url",
    [
        "https://attacker.example/mcp",
        "https://user@mcp-datahub.kenigevents.ru/mcp",
        "https://mcp-datahub.kenigevents.ru:444/mcp",
        "https://mcp-datahub.kenigevents.ru/mcp?redirect=attacker",
    ],
)
def test_closure_mcp_rejects_noncanonical_token_audience(url: str) -> None:
    with pytest.raises(ValueError, match="owner-approved"):
        StreamableHttpClosureMcp(url, "reader-token-long-enough-for-validation")


def test_closure_mcp_accepts_only_canonical_token_audience() -> None:
    StreamableHttpClosureMcp(CANONICAL_MCP_URL, "reader-token-long-enough-for-validation")


def test_final_closure_reaches_durable_complete_only_after_restore_and_mcp() -> None:
    mcp = FakeMcp()
    receipt = run_blogger_closure(config(), control=FakeControl(), mcp=mcp)
    assert receipt["status"] == "DURABLE_COMPLETE"
    assert receipt["ensure_operation_id"] == str(OPERATION)
    assert receipt["import_runtime"]["epoch"] == 7
    assert receipt["checkpoint"]["checkpoint_id"] == str(CHECKPOINT)
    assert receipt["cold_restore"]["epoch"] == 8
    assert [name for name, _ in mcp.calls] == [
        "checkpoint.status",
        "master.status",
        "master.rotation.request",
        "operation.get",
        "master.status",
        "bloggers.migration.accounting",
        "bloggers.statistics",
        "bloggers.list",
        "bloggers.list",
        "bloggers.list",
        "bloggers.get",
        "bloggers.provenance",
        "bloggers.search",
    ]


def test_final_closure_transports_exact_duplicate_envelope_in_v2_request() -> None:
    envelope = BloggerDuplicateResolutionEnvelope.model_validate_json(
        Path("examples/bloggers/region-talk-blogger-duplicate-resolution-envelope.v1.example.json").read_bytes()
    )
    replay_config = ClosureConfig(
        control_url=LOCAL_CONTROL_URL, idempotency_key="final-blogger-replay-test",
        project_id=envelope.project_id, snapshot_at=envelope.snapshot_at,
        source_revision=envelope.source_revision, duplicate_resolution=envelope,
        timeout_seconds=600, poll_seconds=1,
    )
    control = FakeControl()
    receipt = run_blogger_closure(replay_config, control=control, mcp=FakeMcp())
    assert receipt["status"] == "DURABLE_COMPLETE"
    assert control.created is not None
    assert control.created.schema_version == "my-data-hub-blogger-migration-request.v2"
    assert control.created.duplicate_resolution == envelope
    assert control.created.replay_of_request_id == envelope.source_request_id


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


def test_quarantine_receipt_example_validates_against_schema() -> None:
    schema = json.loads(
        Path("schemas/region-talk-ydb-bloggers-quarantine-receipt.v1.schema.json").read_text()
    )
    example = json.loads(
        Path(
            "examples/bloggers/region-talk-ydb-bloggers-quarantine-receipt.v1.example.json"
        ).read_text()
    )
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema).validate(example)


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
        request_id=str(REQUEST),
        operation_id=str(OPERATION),
        request_sha256=value.request_sha256,
        request=value.model_dump(mode="json"),
    )
    assert created and record["state"] == "REQUESTED"
    claimed = ledger.claim_blogger_migration_request(
        operation_id=str(OPERATION),
        run_id="run",
        attempt_id="attempt",
        master_instance_id=str(MASTER),
        epoch=7,
    )
    assert claimed is not None and claimed["claimed_epoch"] == 7
    serialized = json.dumps(claimed).lower()
    assert "blogger_name" not in serialized
    assert "postgresql://" not in serialized
    assert "ydb_access_token" not in serialized
    with pytest.raises(Exception, match="another runtime epoch"):
        ledger.claim_blogger_migration_request(
            operation_id=str(OPERATION),
            run_id="other",
            attempt_id="attempt",
            master_instance_id=str(MASTER),
            epoch=7,
        )


def test_import_receipt_response_loss_replay_returns_exact_commit_and_cannot_downgrade(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    value = request()
    ledger.ensure_blogger_migration_request(
        request_id=str(value.request_id), operation_id=str(value.operation_id),
        request_sha256=value.request_sha256, request=value.model_dump(mode="json"),
    )
    ledger.claim_blogger_migration_request(
        operation_id=str(value.operation_id), run_id="run", attempt_id="attempt",
        master_instance_id=str(MASTER), epoch=7,
    )
    payload = imported().model_dump(mode="json")
    first = ledger.record_blogger_import_receipt(
        request_id=str(value.request_id), run_id="run", attempt_id="attempt", receipt=payload,
    )
    replay = ledger.record_blogger_import_receipt(
        request_id=str(value.request_id), run_id="run", attempt_id="attempt", receipt=payload,
    )
    assert first["state"] == replay["state"] == "IMPORT_COMMITTED"
    assert first["import_receipt"] == replay["import_receipt"] == payload
    with pytest.raises(Exception, match="cannot be downgraded"):
        ledger.fail_blogger_migration_request(
            request_id=str(value.request_id), run_id="run", attempt_id="attempt",
            failure_code="lost_response",
        )


def test_cli_exits_78_before_control_or_receipt_mutation_without_modern_token(tmp_path) -> None:
    receipt = tmp_path / "must-not-exist.json"
    environment = dict(os.environ)
    environment.pop("KAGGLE_API_TOKEN", None)
    environment["KAGGLE_CONFIG_DIR"] = str(tmp_path / "no-token")
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/bloggers/run_final_closure.py",
            "run",
            "--idempotency-key",
            "no-token-test",
            "--project-id",
            str(PROJECT),
            "--snapshot-at",
            "2026-08-09T00:00:00Z",
            "--source-revision",
            "b" * 40,
            "--receipt",
            str(receipt),
        ],
        cwd=Path.cwd(),
        env=environment,
        check=False,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 78
    assert not receipt.exists()
    assert not (tmp_path / "control.sqlite3").exists()


def test_in_master_stage_uses_epoch_bound_migration_login_and_drops_it(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    import sys
    from types import SimpleNamespace

    from my_data_hub.master_runtime.contracts import MasterIdentity
    from my_data_hub.workloads.bloggers.accounting import BloggerExportReceipt
    from my_data_hub.workloads.bloggers.importer import (
        DuplicateReviewGroup,
        DuplicateReviewMember,
        ImportReceipt,
    )
    from my_data_hub.workloads.bloggers.master_stage import (
        BloggerMigrationQuarantined,
        BloggerStageContext,
        execute_blogger_migration_stage,
    )

    events: list[str] = []

    class Provisioner:
        def __init__(self, connection):
            events.append("provisioner")

        def create(self, **kwargs):
            events.append("credential.create")
            assert kwargs["group"] == "mdh_migration_operator"
            assert kwargs["policy"].connection_limit == 1

        def drop(self, principal):
            events.append("credential.drop")

    class Cursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, statement):
            events.append(statement)

    class Connection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def cursor(self):
            return Cursor()

    class Snapshot:
        def __init__(self, driver):
            pass

        def assert_write_denied(self):
            events.append("ydb.denied")

        def iter_rows(self):
            class Rows:
                def __enter__(self):
                    events.append("ydb.rows")
                    return iter(())

                def __exit__(self, *args):
                    return None

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
            assert kwargs["duplicate_resolutions"] == ()
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
            lease_until=datetime.now(UTC).replace(microsecond=0) + __import__("datetime").timedelta(minutes=6),
            attempt_id="attempt-1",
        ),
        owner_connection=object(),
        driver=object(),
        importer=Importer(),
    )
    assert receipt.transaction_committed is True
    assert receipt.row_count == receipt.distinct_record_ids == receipt.actor_count == 266
    assert (
        events.index("credential.create")
        < events.index("ydb.denied")
        < events.index("postgres.import")
        < events.index("credential.drop")
    )

    replay_request = BloggerMigrationRequest.model_validate_json(
        Path("examples/bloggers/blogger-migration-request.v2.example.json").read_bytes()
    )

    class ReplayImporter:
        def import_rows(self, connection, **kwargs):
            assert kwargs["duplicate_resolutions"] == replay_request.duplicate_resolutions
            return result

    execute_blogger_migration_stage(
        BloggerStageContext(
            identity=MasterIdentity(MASTER, "77777777-7777-4777-8777-777777777777", 7),
            request=replay_request,
            local_database_url="postgresql://postgres@/postgres?host=%2Fkaggle%2Fworking%2Fsocket&port=5432",
            lease_until=datetime.now(UTC).replace(microsecond=0)
            + __import__("datetime").timedelta(minutes=6),
            attempt_id="attempt-1",
        ),
        owner_connection=object(), driver=object(), importer=ReplayImporter(),
    )

    blocked_export = BloggerExportReceipt(
        export_batch_id=BATCH,
        exported_at=datetime(2026, 8, 9, tzinfo=UTC),
        query_sha256=SOURCE_QUERY_SHA256,
        row_count=266,
        distinct_record_ids=266,
        record_id_set_sha256=SHA,
        logical_sha256="d" * 64,
        dispositions={"retained_raw": 265, "quarantined": 1},
        undispositioned=0,
        source_file_count=14,
    )
    blocked = ImportReceipt(
        export=blocked_export,
        canonical_outcome_sha256="e" * 64,
        actor_count=0,
        account_count=0,
        duplicate_group_count=1,
        replayed_count=0,
        canonical_revision=9,
        duplicate_groups_pending=1,
        durability_state="BLOCKED_QUARANTINE",
        duplicate_review_groups=(
            DuplicateReviewGroup(
                identity_sha256="f" * 64,
                members=(
                    DuplicateReviewMember("record-1", UUID("77777777-7777-4777-8777-777777777771")),
                    DuplicateReviewMember("record-2", UUID("77777777-7777-4777-8777-777777777772")),
                ),
                existing_actor_id=None,
            ),
        ),
    )

    class BlockedImporter:
        def import_rows(self, connection, **kwargs):
            return blocked

    with pytest.raises(BloggerMigrationQuarantined, match="durably quarantined") as captured:
        execute_blogger_migration_stage(
            BloggerStageContext(
                identity=MasterIdentity(MASTER, "77777777-7777-4777-8777-777777777777", 7),
                request=request(),
                local_database_url=(
                    "postgresql://postgres@/postgres?host=%2Fkaggle%2Fworking%2Fsocket&port=5432"
                ),
                lease_until=datetime.now(UTC).replace(microsecond=0)
                + __import__("datetime").timedelta(minutes=6),
                attempt_id="attempt-1",
            ),
            owner_connection=object(),
            driver=object(),
            importer=BlockedImporter(),
        )
    quarantine = captured.value.receipt
    assert quarantine.attempt_id == "attempt-1"
    assert quarantine.transaction_committed is True
    assert quarantine.duplicate_group_count == quarantine.duplicate_groups_pending == 1
    assert quarantine.duplicate_review_inputs.groups[0].identity_sha256 == "f" * 64
