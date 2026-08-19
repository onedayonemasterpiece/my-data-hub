from __future__ import annotations

import hashlib
import json
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from my_data_hub.checkpoints.brokered_upload import (
    BrokeredCheckpointError,
    BrokeredCheckpointQuarantined,
    BrokeredCheckpointRuntimeCoordinator,
    BrokeredCheckpointRuntimeProvider,
    BrokeredCheckpointUploadService,
    CheckpointBlobCompletion,
    CheckpointBlobSpec,
    CheckpointUploadSecretBox,
    RuntimeUploadAuthority,
)
from my_data_hub.checkpoints.kaggle_runtime import CheckpointRuntimeError
from my_data_hub.checkpoints.manifest import RestoreProbe, build_manifest, canonical_json, write_manifest
from my_data_hub.checkpoints.provider_storage import checkpoint_provider_file_name
from my_data_hub.checkpoints.registry import ControlLedgerCheckpointRegistry
from my_data_hub.connectors.checkpoint_control import (
    ControlLedgerVerifiedCheckpointCoordinator,
)
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.ledger.errors import IdempotencyConflict, LeaseRejected
from my_data_hub.providers.kaggle.contracts import BrokeredBlobGrant, BrokeredDatasetFile

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
RUN = UUID("11111111-1111-4111-8111-111111111111")
ATTEMPT = UUID("22222222-2222-4222-8222-222222222222")
MASTER = UUID("33333333-3333-4333-8333-333333333333")
OPERATION = UUID("44444444-4444-4444-8444-444444444444")
SERVICE = UUID("55555555-5555-4555-8555-555555555555")
CHECKPOINT = UUID("66666666-6666-4666-8666-666666666666")


class SimulatedProcessLoss(BaseException):
    """Abrupt process boundary: bypasses in-process exception recovery by design."""


class FakeBrokeredKaggle:
    def __init__(self) -> None:
        self.current_version: int | None = None
        self.started: list[str] = []
        self.finalized = 0
        self.exact_files: tuple[tuple[str, int, str], ...] = ()

    def current_private_dataset_version(self, *, provider_ref: str) -> int | None:
        assert provider_ref == "owner/checkpoints"
        return self.current_version

    def start_brokered_dataset_blob(self, **kwargs: object) -> BrokeredBlobGrant:
        name = str(kwargs["file_name"])
        self.started.append(name)
        return BrokeredBlobGrant(
            blob_token=f"opaque-token-{len(self.started)}",
            create_url=f"https://storage.example.test/upload/{len(self.started)}?signature=secret",
        )

    def finalize_brokered_checkpoint_dataset(
        self,
        *,
        provider_ref: str,
        title: str,
        files: tuple[BrokeredDatasetFile, ...],
        version_notes: str,
        expected_previous_version: int | None,
    ) -> int:
        assert provider_ref == "owner/checkpoints"
        assert title == "checkpoints"
        assert "operation=" in version_notes and "manifest=" in version_notes
        assert expected_previous_version == self.current_version
        self.finalized += 1
        self.current_version = (self.current_version or 0) + 1
        self.exact_files = tuple((item.name, item.total_bytes, item.description) for item in files)
        return self.current_version

    def reconcile_brokered_checkpoint_dataset(
        self,
        *,
        provider_ref: str,
        version: int,
        expected_files: tuple[tuple[str, int, str], ...],
    ) -> bool:
        return (
            provider_ref == "owner/checkpoints"
            and version == self.current_version
            and (expected_files == self.exact_files)
        )


class FakeRestoreVerifier:
    calls = 0

    def verify_restore(self, **kwargs: object) -> dict[str, object]:
        self.calls += 1
        identity = kwargs["dataset_identity"]
        manifest = kwargs["manifest"]
        return {
            "ok": True,
            "provider_run_ref": "owner/checkpoint-verifier/31",
            "checkpoint_id": str(manifest.checkpoint_id),
            "package_sha256": identity.package_sha256,
        }


class FailingRestoreVerifier:
    def __init__(self, failure: Exception) -> None:
        self.failure = failure

    def verify_restore(self, **kwargs: object) -> dict[str, object]:
        del kwargs
        raise self.failure


class RevisionedRestoreVerifier(FakeRestoreVerifier):
    def __init__(self, revision: str) -> None:
        self.revision_sha256 = revision
        self.calls = 0


class RevisionedFailingRestoreVerifier(FailingRestoreVerifier):
    def __init__(self, revision: str) -> None:
        super().__init__(CheckpointRuntimeError("missing exact runtime dependency"))
        self.revision_sha256 = revision


def _fixture(tmp_path: Path):  # type: ignore[no-untyped-def]
    clock = DeterministicClock(NOW)
    ledger = ControlLedger(tmp_path / "private" / "control.sqlite3", clock=clock)
    identity = {
        "run_id": str(RUN),
        "attempt_id": str(ATTEMPT),
        "service_instance_id": str(SERVICE),
        "master_instance_id": str(MASTER),
        "epoch": 1,
    }
    ledger.ensure_operation(
        operation_id=str(OPERATION),
        idempotency_key="brokered-checkpoint-operation",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="READY",
        identity=identity,
        allocate_epoch_for="postgres-master",
    )
    ledger.record_attempt(
        attempt_id=str(ATTEMPT),
        run_id=str(RUN),
        operation_id=str(OPERATION),
        source_identity="owner/master",
        source_version="git:" + "a" * 40,
        service_instance_id=str(SERVICE),
        master_instance_id=str(MASTER),
        epoch=1,
        state="RUNNING",
    )
    ledger.activate_service_operation(
        operation_id=str(OPERATION),
        expected_state="READY",
        service_instance_id=str(SERVICE),
        service_kind="postgres-master",
        run_id=str(RUN),
        attempt_id=str(ATTEMPT),
        master_instance_id=str(MASTER),
        epoch=1,
        endpoint="tunnel://127.0.0.1:25432",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "b" * 64,
        capabilities=("sql",),
        canonical_revision=7,
        schema_version="18",
        lease_until=NOW + timedelta(minutes=10),
        latest_event_id="event-ready",
    )
    package = tmp_path / "package"
    values = {
        "physical/base.tar.gz": b"base",
        "physical/backup_manifest": b"native manifest",
        "physical/pg_wal.tar.gz": b"wal",
        "logical/hub.dump": b"logical",
        "receipts/verification.json": b'{"ok":true}',
    }
    for relative, content in values.items():
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = build_manifest(
        package_directory=package,
        checkpoint_id=CHECKPOINT,
        master_instance_id=MASTER,
        epoch=1,
        parent_checkpoint_id=None,
        postgres_version="18.4",
        pgvector_version="0.8.6",
        schema_version=18,
        canonical_revision=7,
        source_run_id=str(RUN),
        source_identity="owner/master/17",
        created_at=NOW,
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "physical/base.tar.gz": "physical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/pg_wal.tar.gz": "physical",
            "logical/hub.dump": "logical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(18, 7, "c" * 64, {"hub.canonical_state": 1}),
    )
    registry = ControlLedgerCheckpointRegistry(ledger, operation_id=str(OPERATION), dataset_ref="owner/checkpoints")
    registry.add_candidate(manifest)
    adapter = FakeBrokeredKaggle()
    verifier = FakeRestoreVerifier()
    service = BrokeredCheckpointUploadService(
        ledger,
        adapter,
        CheckpointUploadSecretBox(b"k" * 32),
        verifier,
    )
    authority = RuntimeUploadAuthority(
        operation_id=str(OPERATION),
        run_id=str(RUN),
        attempt_id=str(ATTEMPT),
        master_instance_id=str(MASTER),
        service_instance_id=str(SERVICE),
        epoch=1,
        master_run_ref="owner/master/17",
        lease_until=NOW + timedelta(minutes=10),
    )
    return ledger, package, manifest, adapter, verifier, service, authority


def _specs(package: Path, manifest: object) -> list[CheckpointBlobSpec]:
    manifest_bytes = canonical_json(manifest.payload()) + b"\n"
    values = {item.path: (item.byte_size, item.sha256) for item in manifest.files}
    values["checkpoint-manifest.json"] = (
        len(manifest_bytes),
        hashlib.sha256(manifest_bytes).hexdigest(),
    )
    return [
        CheckpointBlobSpec(
            operation_id=OPERATION,
            checkpoint_id=manifest.checkpoint_id,
            master_run_ref="owner/master/17",
            epoch=1,
            file_name=name,
            content_length=size,
            content_type="application/json" if name.endswith(".json") else "application/octet-stream",
            content_sha256=digest,
            manifest_sha256=manifest.manifest_sha256,
        )
        for name, (size, digest) in sorted(values.items())
    ]


def _upload_all(
    package: Path,
    manifest: object,
    service: BrokeredCheckpointUploadService,
    authority: RuntimeUploadAuthority,
) -> list[object]:
    grants = []
    for spec in _specs(package, manifest):
        grant = service.prepare(spec, authority)
        grants.append(grant)
        service.complete(
            CheckpointBlobCompletion(
                claim_id=grant.claim_id,
                operation_id=OPERATION,
                checkpoint_id=manifest.checkpoint_id,
                epoch=1,
                file_name=spec.file_name,
                bytes_sent=spec.content_length,
                content_sha256=spec.content_sha256,
                outcome="uploaded",
            ),
            authority,
        )
    return grants


def _restarted_service(
    ledger: ControlLedger,
    adapter: FakeBrokeredKaggle,
    verifier: FakeRestoreVerifier,
) -> tuple[ControlLedger, BrokeredCheckpointUploadService]:
    restarted_ledger = ControlLedger(ledger.path, clock=ledger.clock)
    return restarted_ledger, BrokeredCheckpointUploadService(
        restarted_ledger,
        adapter,
        CheckpointUploadSecretBox(b"k" * 32),
        verifier,
    )


class _ServiceMetadataClient:
    """Runtime metadata transport backed by a restartable durable broker service."""

    def __init__(
        self,
        service: BrokeredCheckpointUploadService,
        authority: RuntimeUploadAuthority,
        *,
        lose_completion_response_after: int | None = None,
    ) -> None:
        self.service = service
        self.authority = authority
        self.lose_completion_response_after = lose_completion_response_after
        self.completions = 0

    def get(self, path: str) -> dict[str, object]:
        if path == "/internal/checkpoints/runtime-upload-authority":
            return {"master_run_ref": self.authority.master_run_ref}
        if path.endswith("/publication"):
            return self.service.status(CHECKPOINT)
        if path == "/internal/checkpoints/postgres-master/head":
            head = self.service.control.checkpoint_head("postgres-master")
            assert head is not None
            return {
                "current": {"checkpoint_id": head.current_checkpoint_id},
                "previous": (
                    {"checkpoint_id": head.previous_checkpoint_id}
                    if head.previous_checkpoint_id is not None
                    else None
                ),
            }
        raise AssertionError(path)

    def post(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        if path.endswith("/blob-uploads/prepare"):
            return self.service.prepare(
                CheckpointBlobSpec.model_validate(payload), self.authority
            ).model_dump(mode="json")
        if path.endswith("/blob-uploads/complete"):
            result = self.service.complete(
                CheckpointBlobCompletion.model_validate(payload), self.authority
            )
            self.completions += 1
            if self.completions == self.lose_completion_response_after:
                raise SimulatedProcessLoss("process lost after durable completion")
            return result
        if path.endswith("/finalize"):
            return self.service.finalize(CHECKPOINT, self.authority)
        raise AssertionError(path)


def test_broker_uploads_direct_metadata_and_promotes_once(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    grants = _upload_all(package, manifest, service, authority)
    assert all("signature=secret" not in repr(grant) for grant in grants)
    receipt = service.finalize(CHECKPOINT, authority)
    assert receipt["state"] == "PROMOTED"
    assert receipt["exact_version_ref"] == "owner/checkpoints/1"
    assert adapter.finalized == 1
    assert verifier.calls == 1
    head = ledger.checkpoint_head("postgres-master")
    assert head is not None and head.generation == 1
    assert head.current_checkpoint_id == str(CHECKPOINT) and head.previous_checkpoint_id is None
    ControlLedgerCheckpointRegistry(ledger, operation_id=str(OPERATION), dataset_ref="owner/checkpoints").add_candidate(
        manifest
    )
    assert service.finalize(CHECKPOINT, authority)["state"] == "PROMOTED"
    assert adapter.finalized == 1


def test_connector_request_restarts_through_real_broker_verified_head(tmp_path: Path) -> None:
    ledger, package, manifest, _adapter, _verifier, service, authority = _fixture(tmp_path)
    coordinator = ControlLedgerVerifiedCheckpointCoordinator(ledger)
    idempotency_key = "a" * 64
    operation_id = f"connector-checkpoint:{idempotency_key}"

    requested = coordinator.request_verified_checkpoint(
        operation_id=operation_id,
        canonical_revision=7,
        idempotency_key=idempotency_key,
    )
    assert requested["state"] == "REQUESTED"
    assert coordinator.request_verified_checkpoint(
        operation_id=operation_id,
        canonical_revision=7,
        idempotency_key=idempotency_key,
    ) == requested
    with pytest.raises(IdempotencyConflict):
        coordinator.request_verified_checkpoint(
            operation_id=operation_id,
            canonical_revision=8,
            idempotency_key=idempotency_key,
        )
    token = "r" * 64
    ledger.store_runtime_token_hash(str(RUN), str(ATTEMPT), token)
    client = TestClient(
        create_app(
            ControlPlaneSettings(
                ledger_path=ledger.path, connector_runtime_enabled=True
            ),
            ledger=ledger,
        )
    )
    claim_response = client.get(
        f"/internal/runtime/connector-checkpoint/{RUN}/{ATTEMPT}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": str(MASTER),
            "X-MDH-Epoch": "1",
        },
    )
    assert claim_response.status_code == 200
    assert claim_response.json() == {
        "available": True,
        "operation_id": operation_id,
        "canonical_revision": 7,
    }
    assert coordinator.checkpoint_status(operation_id)["state"] == "CHECKPOINTING"

    _upload_all(package, manifest, service, authority)
    assert service.finalize(CHECKPOINT, authority)["state"] == "PROMOTED"

    # A new process reconstructs only from the shared durable ledger and the
    # actual broker-promoted current head.
    restarted = ControlLedgerVerifiedCheckpointCoordinator(ledger)
    completed = restarted.checkpoint_status(operation_id)
    assert completed == {
        "operation_id": operation_id,
        "idempotency_key": idempotency_key,
        "canonical_revision": 7,
        "state": "DURABLE_COMPLETE",
        "checkpoint_status": "VERIFIED",
        "checkpoint_id": str(CHECKPOINT),
        "current_checkpoint_id": str(CHECKPOINT),
        "manifest_sha256": manifest.manifest_sha256,
        "verified_at": NOW.isoformat().replace("+00:00", "Z"),
    }


def test_unified_bootstrap_master_can_claim_its_gate_checkpoint(tmp_path: Path) -> None:
    ledger, _package, _manifest, _adapter, _verifier, _service, _authority = _fixture(tmp_path)
    coordinator = ControlLedgerVerifiedCheckpointCoordinator(ledger)
    idempotency_key = "b" * 64
    operation_id = f"connector-checkpoint:{idempotency_key}"
    coordinator.request_verified_checkpoint(
        operation_id=operation_id,
        canonical_revision=7,
        idempotency_key=idempotency_key,
    )
    token = "u" * 64
    ledger.store_runtime_token_hash(str(RUN), str(ATTEMPT), token)
    runtime = type(
        "Runtime",
        (),
        {
            "ledger": ledger,
            "coordinator": None,
            "reconcile_requested_once": lambda self: None,
        },
    )()
    client = TestClient(
        create_app(
            ControlPlaneSettings(
                ledger_path=ledger.path,
                master_runtime=object(),  # type: ignore[arg-type]
                provider_gateway_enabled=True,
                unified_bootstrap_mode=True,
            ),
            ledger=ledger,
            master_runtime=runtime,  # type: ignore[arg-type]
            provider_gateway=object(),  # type: ignore[arg-type]
            provider_gateway_token=b"g" * 32,
        )
    )

    claim = client.get(
        f"/internal/runtime/connector-checkpoint/{RUN}/{ATTEMPT}",
        headers={
            "Authorization": f"Bearer {token}",
            "X-MDH-Master-Instance-ID": str(MASTER),
            "X-MDH-Epoch": "1",
        },
    )

    assert claim.status_code == 200
    assert claim.json() == {
        "available": True,
        "operation_id": operation_id,
        "canonical_revision": 7,
    }


def test_checkpoint_recovery_is_not_blocked_by_master_provider_polling(tmp_path: Path) -> None:
    """A slow provider run poll must not starve a ready checkpoint publication."""

    ledger = ControlLedger(tmp_path / "independent-checkpoint-recovery.sqlite3")
    broker_called = threading.Event()
    master_finished = threading.Event()

    class BlockingMasterRuntime:
        def __init__(self, control: ControlLedger) -> None:
            self.ledger = control
            self.coordinator = None

        def reconcile_requested_once(self) -> None:
            assert broker_called.wait(timeout=2), "checkpoint recovery was starved by master polling"
            master_finished.set()

    class RecoveryBroker:
        def reconcile_pending_once(self) -> list[dict[str, object]]:
            broker_called.set()
            return []

    app = create_app(
        ControlPlaneSettings(
            ledger_path=ledger.path,
            master_runtime=object(),  # type: ignore[arg-type]
            provider_gateway_enabled=True,
            unified_bootstrap_mode=True,
        ),
        ledger=ledger,
        master_runtime=BlockingMasterRuntime(ledger),  # type: ignore[arg-type]
        checkpoint_upload_broker=RecoveryBroker(),  # type: ignore[arg-type]
        provider_gateway=object(),  # type: ignore[arg-type]
        provider_gateway_token=b"g" * 32,
    )

    with TestClient(app) as client:
        assert client.get("/health/ready").status_code == 200
        assert broker_called.wait(timeout=2)
        assert master_finished.wait(timeout=2)


def test_second_verified_candidate_preserves_current_as_previous(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, _verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    service.finalize(CHECKPOINT, authority)

    next_checkpoint = UUID("77777777-7777-4777-8777-777777777777")
    package_2 = tmp_path / "package-2"
    values = {
        "physical/base.tar.gz": b"base-2",
        "physical/backup_manifest": b"native manifest-2",
        "physical/pg_wal.tar.gz": b"wal-2",
        "logical/hub.dump": b"logical-2",
        "receipts/verification.json": b'{"ok":true,"generation":2}',
    }
    for relative, content in values.items():
        target = package_2 / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest_2 = build_manifest(
        package_directory=package_2,
        checkpoint_id=next_checkpoint,
        master_instance_id=MASTER,
        epoch=1,
        parent_checkpoint_id=CHECKPOINT,
        postgres_version="18.4",
        pgvector_version="0.8.6",
        schema_version=18,
        canonical_revision=8,
        source_run_id=str(RUN),
        source_identity="owner/master/17",
        created_at=NOW,
        checkpoint_lsn="0/16B6C60",
        file_kinds={
            "physical/base.tar.gz": "physical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/pg_wal.tar.gz": "physical",
            "logical/hub.dump": "logical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(18, 8, "d" * 64, {"hub.canonical_state": 1}),
    )
    ControlLedgerCheckpointRegistry(
        ledger,
        operation_id=str(OPERATION),
        dataset_ref="owner/checkpoints",
    ).add_candidate(manifest_2)
    _upload_all(package_2, manifest_2, service, authority)
    receipt = service.finalize(next_checkpoint, authority)

    assert receipt["exact_version_ref"] == "owner/checkpoints/2"
    head = ledger.checkpoint_head("postgres-master")
    assert head is not None and head.generation == 2
    assert head.current_checkpoint_id == str(next_checkpoint)
    assert head.previous_checkpoint_id == str(CHECKPOINT)
    assert adapter.finalized == 2
    assert _verifier.calls == 2


def test_complete_child_recreates_externally_missing_checkpoint_dataset(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    service.finalize(CHECKPOINT, authority)

    next_checkpoint = UUID("77777777-7777-4777-8777-777777777778")
    package_2 = tmp_path / "replacement-package"
    values = {
        "physical/base.tar.gz": b"replacement-base",
        "physical/backup_manifest": b"replacement-native-manifest",
        "physical/pg_wal.tar.gz": b"replacement-wal",
        "logical/hub.dump": b"replacement-logical",
        "receipts/verification.json": b'{"ok":true,"replacement":true}',
    }
    for relative, content in values.items():
        target = package_2 / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest_2 = build_manifest(
        package_directory=package_2,
        checkpoint_id=next_checkpoint,
        master_instance_id=MASTER,
        epoch=1,
        parent_checkpoint_id=CHECKPOINT,
        postgres_version="18.4",
        pgvector_version="0.8.6",
        schema_version=18,
        canonical_revision=8,
        source_run_id=str(RUN),
        source_identity="owner/master/17",
        created_at=NOW,
        checkpoint_lsn="0/16B6C61",
        file_kinds={
            "physical/base.tar.gz": "physical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/pg_wal.tar.gz": "physical",
            "logical/hub.dump": "logical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(18, 8, "e" * 64, {"hub.canonical_state": 1}),
    )
    ControlLedgerCheckpointRegistry(
        ledger,
        operation_id=str(OPERATION),
        dataset_ref="owner/checkpoints",
    ).add_candidate(manifest_2)
    _upload_all(package_2, manifest_2, service, authority)

    # Exact provider inventory proves the durable Dataset vanished externally.
    adapter.current_version = None
    original_retire = service.control.retire_missing_checkpoint_dataset_incarnation

    def process_lost_before_retirement(*_args: object, **_kwargs: object) -> object:
        raise SimulatedProcessLoss("process lost after replacement Dataset resolution")

    service.control.retire_missing_checkpoint_dataset_incarnation = (  # type: ignore[method-assign]
        process_lost_before_retirement
    )
    with pytest.raises(SimulatedProcessLoss):
        service.finalize(next_checkpoint, authority)

    retirement = ledger.checkpoint_dataset_incarnation_retirement(str(next_checkpoint))
    assert retirement is not None
    assert json.loads(str(retirement["retired_versions_json"])) == [
        {"checkpoint_id": str(CHECKPOINT), "version_ref": "owner/checkpoints/1"}
    ]
    assert service.status(next_checkpoint)["state"] == "DATASET_RESOLVED"
    assert ledger.checkpoint_candidate(str(CHECKPOINT))["version_ref"] == "owner/checkpoints/1"  # type: ignore[index]

    service.control.retire_missing_checkpoint_dataset_incarnation = original_retire  # type: ignore[method-assign]
    restarted_ledger, restarted = _restarted_service(ledger, adapter, verifier)
    receipt = restarted.finalize(next_checkpoint, authority)

    assert receipt["exact_version_ref"] == "owner/checkpoints/1"
    publication = service.ledger.publication(str(next_checkpoint))
    assert publication is not None
    assert publication["expected_provider_version"] == 1
    assert publication["finalize_attempts"] == 1
    assert restarted_ledger.checkpoint_candidate(str(CHECKPOINT))["version_ref"] is None  # type: ignore[index]
    assert restarted_ledger.checkpoint_candidate(str(next_checkpoint))["version_ref"] == (  # type: ignore[index]
        "owner/checkpoints/1"
    )
    head = restarted_ledger.checkpoint_head("postgres-master")
    assert head is not None and head.generation == 2
    assert head.current_checkpoint_id == str(next_checkpoint)
    assert head.previous_checkpoint_id == str(CHECKPOINT)
    assert adapter.finalized == 2
    assert verifier.calls == 2


def test_invalid_prepare_is_rejected_and_exact_ready_claim_replays(tmp_path: Path) -> None:
    _ledger, package, manifest, _adapter, _verifier, service, authority = _fixture(tmp_path)
    spec = _specs(package, manifest)[0]
    grant = service.prepare(spec, authority)
    with pytest.raises(BrokeredCheckpointError):
        service.prepare(spec.model_copy(update={"content_length": spec.content_length + 1}), authority)
    replay = service.prepare(spec, authority)
    assert replay.claim_id == grant.claim_id and replay.create_url == grant.create_url
    assert grant.create_url not in str(service.status(CHECKPOINT))


def test_conflicting_prepare_is_terminally_quarantined(tmp_path: Path) -> None:
    _ledger, package, manifest, _adapter, _verifier, service, authority = _fixture(tmp_path)
    spec = _specs(package, manifest)[0]
    service.prepare(spec, authority)

    with pytest.raises(BrokeredCheckpointError, match="differs from its manifest"):
        service.prepare(spec.model_copy(update={"content_length": spec.content_length + 1}), authority)

    assert service.status(CHECKPOINT)["state"] == "QUARANTINED"
    assert service.status(CHECKPOINT)["failure_code"] == "BLOB_PREPARE_CONFLICT"


@pytest.mark.parametrize(
    ("field", "value"),
    (("bytes_sent", 999), ("content_sha256", "f" * 64)),
)
def test_completion_size_or_hash_mismatch_quarantines(tmp_path: Path, field: str, value: object) -> None:
    _ledger, package, manifest, _adapter, _verifier, service, authority = _fixture(tmp_path)
    spec = _specs(package, manifest)[0]
    grant = service.prepare(spec, authority)
    values: dict[str, object] = {
        "claim_id": grant.claim_id,
        "operation_id": OPERATION,
        "checkpoint_id": CHECKPOINT,
        "epoch": 1,
        "file_name": spec.file_name,
        "bytes_sent": spec.content_length,
        "content_sha256": spec.content_sha256,
        "outcome": "uploaded",
    }
    values[field] = value

    with pytest.raises(Exception, match="differs from its claim"):
        service.complete(CheckpointBlobCompletion.model_validate(values), authority)

    assert service.status(CHECKPOINT)["state"] == "QUARANTINED"


def test_lost_blob_start_response_is_quarantined_without_retry(tmp_path: Path) -> None:
    _ledger, package, manifest, adapter, _verifier, service, authority = _fixture(tmp_path)
    original = adapter.start_brokered_dataset_blob

    def lost_response(**kwargs: object) -> BrokeredBlobGrant:
        original(**kwargs)
        raise ConnectionError("response lost after provider mutation")

    adapter.start_brokered_dataset_blob = lost_response  # type: ignore[method-assign]
    with pytest.raises(BrokeredCheckpointQuarantined, match="outcome is ambiguous"):
        service.prepare(_specs(package, manifest)[0], authority)
    assert len(adapter.started) == 1
    with pytest.raises(BrokeredCheckpointQuarantined):
        service.prepare(_specs(package, manifest)[0], authority)
    assert len(adapter.started) == 1
    assert service.status(CHECKPOINT)["state"] == "QUARANTINED"


def test_lost_dataset_version_response_reconciles_without_duplicate(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    original = adapter.finalize_brokered_checkpoint_dataset

    def lost_response(**kwargs: object) -> int:
        original(**kwargs)
        raise ConnectionError("response lost after dataset version")

    adapter.finalize_brokered_checkpoint_dataset = lost_response  # type: ignore[method-assign]
    receipt = service.finalize(CHECKPOINT, authority)
    assert receipt["state"] == "PROMOTED"
    assert adapter.finalized == 1
    assert verifier.calls == 1
    assert ledger.checkpoint_head("postgres-master").generation == 1  # type: ignore[union-attr]


def test_fresh_process_reconciles_lost_dataset_finalize_response_without_duplicate(
    tmp_path: Path,
) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    original = adapter.finalize_brokered_checkpoint_dataset

    def process_lost(**kwargs: object) -> int:
        original(**kwargs)
        raise SimulatedProcessLoss("process lost after exact Dataset finalize")

    adapter.finalize_brokered_checkpoint_dataset = process_lost  # type: ignore[method-assign]
    with pytest.raises(SimulatedProcessLoss):
        service.finalize(CHECKPOINT, authority)

    assert service.status(CHECKPOINT)["state"] == "FINALIZING"
    assert ledger.checkpoint_head("postgres-master") is None
    adapter.finalize_brokered_checkpoint_dataset = original  # type: ignore[method-assign]
    restarted_ledger, restarted = _restarted_service(ledger, adapter, verifier)
    receipt = restarted.finalize(CHECKPOINT, authority)

    assert receipt["state"] == "PROMOTED"
    assert receipt["exact_version_ref"] == "owner/checkpoints/1"
    assert adapter.finalized == 1
    assert verifier.calls == 1
    head = restarted_ledger.checkpoint_head("postgres-master")
    assert head is not None and head.generation == 1
    assert head.current_checkpoint_id == str(CHECKPOINT) and head.previous_checkpoint_id is None


def test_fresh_process_resumes_verified_evidence_before_head_cas_once(
    tmp_path: Path,
) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)

    def process_lost_before_cas(*_args: object, **_kwargs: object) -> object:
        raise SimulatedProcessLoss("process lost after verified journal before HEAD CAS")

    service.control.promote_checkpoint = process_lost_before_cas  # type: ignore[method-assign]
    with pytest.raises(SimulatedProcessLoss):
        service.finalize(CHECKPOINT, authority)

    before = service.status(CHECKPOINT)
    assert before["state"] == "VERIFIED"
    assert before["exact_version_ref"] == "owner/checkpoints/1"
    assert before["verifier_run_ref"] == "owner/checkpoint-verifier/31"
    assert before["verifier_evidence"] == {
        "ok": True,
        "provider_run_ref": "owner/checkpoint-verifier/31",
        "checkpoint_id": str(CHECKPOINT),
        "package_sha256": before["verifier_evidence"]["package_sha256"],
    }
    assert ledger.checkpoint_head("postgres-master") is None

    restarted_ledger, restarted = _restarted_service(ledger, adapter, verifier)
    after = restarted.finalize(CHECKPOINT, authority)
    head = restarted_ledger.checkpoint_head("postgres-master")

    assert after["state"] == "PROMOTED"
    assert after["verifier_evidence"] == before["verifier_evidence"]
    assert adapter.finalized == 1 and verifier.calls == 1
    assert head is not None and head.generation == 1
    assert head.current_checkpoint_id == str(CHECKPOINT) and head.previous_checkpoint_id is None


def test_fresh_process_reconciles_head_cas_response_loss_without_second_advance(
    tmp_path: Path,
) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    original_transition = service.ledger.transition

    def process_lost_before_promoted_journal(*args: object, **kwargs: object) -> dict[str, object]:
        if kwargs.get("state") == "PROMOTED":
            raise SimulatedProcessLoss("process lost after HEAD CAS")
        return original_transition(*args, **kwargs)

    service.ledger.transition = process_lost_before_promoted_journal  # type: ignore[method-assign]
    with pytest.raises(SimulatedProcessLoss):
        service.finalize(CHECKPOINT, authority)

    head_after_loss = ledger.checkpoint_head("postgres-master")
    assert head_after_loss is not None and head_after_loss.generation == 1
    assert head_after_loss.current_checkpoint_id == str(CHECKPOINT)
    assert head_after_loss.previous_checkpoint_id is None
    assert service.status(CHECKPOINT)["state"] == "VERIFIED"

    restarted_ledger, restarted = _restarted_service(ledger, adapter, verifier)
    receipt = restarted.finalize(CHECKPOINT, authority)
    head = restarted_ledger.checkpoint_head("postgres-master")

    assert receipt["state"] == "PROMOTED"
    assert receipt["verifier_evidence"]["provider_run_ref"] == "owner/checkpoint-verifier/31"
    assert adapter.finalized == 1 and verifier.calls == 1
    assert head is not None and head.generation == 1
    assert head.current_checkpoint_id == str(CHECKPOINT) and head.previous_checkpoint_id is None


def test_missing_dataset_version_exhausts_bounded_reconcile_and_quarantines(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, _verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)

    def unavailable(**kwargs: object) -> int:
        del kwargs
        raise ConnectionError("provider response unavailable")

    adapter.finalize_brokered_checkpoint_dataset = unavailable  # type: ignore[method-assign]
    for _ in range(2):
        with pytest.raises(BrokeredCheckpointError, match=r"remains unresolved|not yet reconciled"):
            service.finalize(CHECKPOINT, authority)
    with pytest.raises(BrokeredCheckpointQuarantined, match="exhausted bounded reconciliation"):
        service.finalize(CHECKPOINT, authority)

    assert adapter.finalized == 0
    assert service.status(CHECKPOINT)["state"] == "QUARANTINED"
    assert ledger.checkpoint_head("postgres-master") is None


@pytest.mark.parametrize("failure", [TimeoutError("timeout"), RuntimeError("restore failed")])
def test_verifier_failure_preserves_head_and_fails_candidate(tmp_path: Path, failure: Exception) -> None:
    ledger, package, manifest, adapter, _verifier, _service, authority = _fixture(tmp_path)
    service = BrokeredCheckpointUploadService(
        ledger,
        adapter,
        CheckpointUploadSecretBox(b"k" * 32),
        FailingRestoreVerifier(failure),
    )
    _upload_all(package, manifest, service, authority)

    with pytest.raises(type(failure)):
        service.finalize(CHECKPOINT, authority)

    assert ledger.checkpoint_head("postgres-master") is None
    assert ledger.checkpoint_candidate(str(CHECKPOINT))["status"] == "FAILED"  # type: ignore[index]
    assert service.status(CHECKPOINT)["state"] == "FAILED"


def test_new_verifier_revision_recovers_exact_failed_dataset_without_reupload(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, _verifier, _service, authority = _fixture(tmp_path)
    old = RevisionedFailingRestoreVerifier("a" * 64)
    failed = BrokeredCheckpointUploadService(
        ledger,
        adapter,
        CheckpointUploadSecretBox(b"k" * 32),
        old,
    )
    _upload_all(package, manifest, failed, authority)
    with pytest.raises(CheckpointRuntimeError, match="runtime dependency"):
        failed.finalize(CHECKPOINT, authority)

    first = failed.status(CHECKPOINT)
    assert first["state"] == "FAILED"
    assert failed.reconcile_pending_once() == []
    assert adapter.finalized == 1

    fixed_verifier = RevisionedRestoreVerifier("b" * 64)
    recovered = BrokeredCheckpointUploadService(
        ledger,
        adapter,
        CheckpointUploadSecretBox(b"k" * 32),
        fixed_verifier,
    )
    receipts = recovered.reconcile_pending_once()

    assert len(receipts) == 1 and receipts[0]["state"] == "PROMOTED"
    assert adapter.finalized == 1
    assert fixed_verifier.calls == 1
    publication = recovered.ledger.publication(str(CHECKPOINT))
    assert publication is not None
    assert publication["verifier_attempts"] == 2
    assert publication["verifier_revision_sha256"] == "b" * 64
    assert ledger.checkpoint_head("postgres-master").current_checkpoint_id == str(CHECKPOINT)  # type: ignore[union-attr]


def test_incompatible_historical_verifier_does_not_block_new_ready_publication(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _ledger, package, manifest, adapter, _verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    real_publication = service.ledger.publication

    monkeypatch.setattr(service.ledger, "failed_verifier_publications", lambda: ["legacy-failed"])
    monkeypatch.setattr(
        service.ledger,
        "publication",
        lambda checkpoint_id: (
            {"checkpoint_id": "legacy-failed"}
            if checkpoint_id == "legacy-failed"
            else real_publication(checkpoint_id)
        ),
    )

    def incompatible_factory(_operation_id: UUID, _task_id: UUID) -> FakeRestoreVerifier:
        raise RuntimeError("historical verifier assets are unavailable")

    service.restore_verifier = None
    service.restore_verifier_factory = incompatible_factory

    with pytest.raises(RuntimeError, match="historical verifier assets"):
        service.reconcile_pending_once()
    publication = real_publication(str(CHECKPOINT))
    assert publication is not None and publication["state"] == "FAILED"
    assert publication["exact_version_ref"] == "owner/checkpoints/1"
    assert adapter.finalized == 1


def test_failed_second_candidate_preserves_exact_current_and_previous_head(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    service.finalize(CHECKPOINT, authority)
    original_head = ledger.checkpoint_head("postgres-master")
    assert original_head is not None

    next_checkpoint = UUID("88888888-8888-4888-8888-888888888888")
    next_package = tmp_path / "failed-package"
    values = {
        "physical/base.tar.gz": b"failed-base",
        "physical/backup_manifest": b"failed-native-manifest",
        "physical/pg_wal.tar.gz": b"failed-wal",
        "logical/hub.dump": b"failed-logical",
        "receipts/verification.json": b'{"ok":true,"candidate":2}',
    }
    for relative, content in values.items():
        target = next_package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    next_manifest = build_manifest(
        package_directory=next_package,
        checkpoint_id=next_checkpoint,
        master_instance_id=MASTER,
        epoch=1,
        parent_checkpoint_id=CHECKPOINT,
        postgres_version="18.4",
        pgvector_version="0.8.6",
        schema_version=18,
        canonical_revision=8,
        source_run_id=str(RUN),
        source_identity="owner/master/17",
        created_at=NOW,
        checkpoint_lsn="0/16B6C70",
        file_kinds={
            "physical/base.tar.gz": "physical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/pg_wal.tar.gz": "physical",
            "logical/hub.dump": "logical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(18, 8, "e" * 64, {"hub.canonical_state": 1}),
    )
    ControlLedgerCheckpointRegistry(
        ledger, operation_id=str(OPERATION), dataset_ref="owner/checkpoints"
    ).add_candidate(next_manifest)
    failed = BrokeredCheckpointUploadService(
        ledger,
        adapter,
        CheckpointUploadSecretBox(b"k" * 32),
        FailingRestoreVerifier(RuntimeError("restore failed")),
    )
    _upload_all(next_package, next_manifest, failed, authority)

    with pytest.raises(RuntimeError, match="restore failed"):
        failed.finalize(next_checkpoint, authority)

    assert ledger.checkpoint_head("postgres-master") == original_head
    assert ledger.checkpoint_candidate(str(next_checkpoint))["status"] == "FAILED"  # type: ignore[index]
    assert failed.status(next_checkpoint)["state"] == "FAILED"
    assert adapter.finalized == 2 and verifier.calls == 1


def test_partial_upload_and_expired_authority_cannot_promote(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, _verifier, service, authority = _fixture(tmp_path)
    spec = _specs(package, manifest)[0]
    service.prepare(spec, authority)
    with pytest.raises(BrokeredCheckpointError, match="missing exact artifacts"):
        service.finalize(CHECKPOINT, authority)
    assert adapter.finalized == 0
    assert ledger.checkpoint_head("postgres-master") is None
    ledger.clock.advance(delta=timedelta(minutes=11))  # type: ignore[attr-defined]
    with pytest.raises(LeaseRejected):
        service.prepare(_specs(package, manifest)[1], authority)
    assert ledger.checkpoint_head("postgres-master") is None


def test_complete_publication_reconciles_after_runtime_lease_expires(tmp_path: Path) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    _upload_all(package, manifest, service, authority)
    ledger.project_master_lifecycle(
        operation_id=str(OPERATION),
        service_instance_id=str(SERVICE),
        epoch=1,
        expected_operation_state="ACTIVE",
        operation_state="DRAINING",
        service_state="DRAINING",
        event_id="runtime-draining",
    )
    ledger.project_master_lifecycle(
        operation_id=str(OPERATION),
        service_instance_id=str(SERVICE),
        epoch=1,
        expected_operation_state="DRAINING",
        operation_state="CHECKPOINTING",
        service_state="DRAINING",
        event_id="checkpoint-started",
    )
    ledger.project_master_lifecycle(
        operation_id=str(OPERATION),
        service_instance_id=str(SERVICE),
        epoch=1,
        expected_operation_state="CHECKPOINTING",
        operation_state="CHECKPOINT_FAILED",
        service_state="DRAINING",
        event_id="checkpoint-client-timeout",
    )
    ledger.clock.advance(delta=timedelta(minutes=11))  # type: ignore[attr-defined]

    with pytest.raises(LeaseRejected):
        service.finalize(CHECKPOINT, authority)

    recovered = service.reconcile_pending_once()

    assert recovered == [service.status(CHECKPOINT)]
    assert recovered[0]["state"] == "PROMOTED"
    assert adapter.finalized == 1
    assert verifier.calls == 1
    head = ledger.checkpoint_head("postgres-master")
    assert head is not None and head.current_checkpoint_id == str(CHECKPOINT)


def test_terminal_failed_checkpoint_can_be_explicitly_fenced_for_replacement(tmp_path: Path) -> None:
    ledger, package, manifest, _adapter, _verifier, service, authority = _fixture(tmp_path)
    ledger.store_runtime_token_hash(str(RUN), str(ATTEMPT), "runtime-token")
    _upload_all(package, manifest, service, authority)
    ledger.project_master_lifecycle(
        operation_id=str(OPERATION),
        service_instance_id=str(SERVICE),
        epoch=1,
        expected_operation_state="ACTIVE",
        operation_state="DRAINING",
        service_state="DRAINING",
        event_id="runtime-draining",
    )
    ledger.project_master_lifecycle(
        operation_id=str(OPERATION),
        service_instance_id=str(SERVICE),
        epoch=1,
        expected_operation_state="DRAINING",
        operation_state="CHECKPOINTING",
        service_state="DRAINING",
        event_id="checkpoint-started",
    )
    service._fail(CHECKPOINT, "RUNTIME_AUTHORITY_EXPIRED", quarantine=False)
    ledger.project_master_lifecycle(
        operation_id=str(OPERATION),
        service_instance_id=str(SERVICE),
        epoch=1,
        expected_operation_state="CHECKPOINTING",
        operation_state="CHECKPOINT_FAILED",
        service_state="DRAINING",
        event_id="checkpoint-failed",
    )

    ledger.fence_checkpoint_failed_master(
        operation_id=str(OPERATION),
        run_id=str(RUN),
        attempt_id=str(ATTEMPT),
        service_instance_id=str(SERVICE),
        epoch=1,
        event_id="operator-checkpoint-failure-recovery",
    )
    # Exact replay is idempotent and all runtime admission is retired.
    ledger.fence_checkpoint_failed_master(
        operation_id=str(OPERATION),
        run_id=str(RUN),
        attempt_id=str(ATTEMPT),
        service_instance_id=str(SERVICE),
        epoch=1,
        event_id="operator-checkpoint-failure-recovery",
    )
    assert ledger.get_operation(str(OPERATION)).state == "FENCED"  # type: ignore[union-attr]
    assert ledger.resolve_service("postgres-master") is None
    assert not ledger.runtime_token_valid(str(RUN), str(ATTEMPT), "runtime-token")


class _RuntimeMetadataClient:
    def __init__(self, manifest: object) -> None:
        self.manifest = manifest
        self.posts: list[tuple[str, dict[str, object]]] = []
        self.claims = 0

    def get(self, path: str) -> dict[str, object]:
        if path == "/internal/checkpoints/runtime-upload-authority":
            return {"master_run_ref": "owner/master/17"}
        if path.endswith("/publication"):
            return {
                "state": "PROMOTED",
                "exact_version_ref": "owner/checkpoints/1",
                "verifier_run_ref": "owner/verifier/7",
                "verifier_receipt_sha256": "e" * 64,
            }
        if path == "/internal/checkpoints/postgres-master/head":
            return {
                "current": {"checkpoint_id": str(self.manifest.checkpoint_id)},
                "previous": None,
            }
        raise AssertionError(path)

    def post(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        self.posts.append((path, payload))
        if path.endswith("/blob-uploads/prepare"):
            self.claims += 1
            return {
                "claim_id": str(UUID(int=self.claims)),
                "checkpoint_id": str(self.manifest.checkpoint_id),
                "file_name": payload["file_name"],
                "content_length": payload["content_length"],
                "content_sha256": payload["content_sha256"],
                "expires_at": NOW.isoformat(),
                "create_url": f"https://storage.test/{self.claims}?sig=secret",
            }
        if path.endswith("/blob-uploads/complete"):
            return {"state": "UPLOADED"}
        if path.endswith("/finalize"):
            return {"state": "READY_TO_FINALIZE"}
        raise AssertionError(path)


def test_runtime_provider_sends_only_metadata_and_direct_puts_each_file(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _ledger, package, manifest, _adapter, _verifier, _service, _authority = _fixture(tmp_path)
    manifest_path = tmp_path / "checkpoint-manifest.json"
    write_manifest(manifest_path, manifest)
    client = _RuntimeMetadataClient(manifest)
    provider = BrokeredCheckpointRuntimeProvider(
        client,  # type: ignore[arg-type]
        dataset_ref="owner/checkpoints",
        operation_id=OPERATION,
        timeout_seconds=60,
    )
    direct_puts: list[tuple[str, Path, int, str]] = []

    def put(url: str, path: Path, *, content_length: int, content_type: str) -> None:
        direct_puts.append((url, path, content_length, content_type))

    monkeypatch.setattr(provider, "_put_exact", put)
    receipt = provider.publish(package=package, manifest_path=manifest_path)

    assert receipt.current_checkpoint_id == str(CHECKPOINT)
    assert len(direct_puts) == len(manifest.files) + 1
    for _path, payload in client.posts:
        assert all(not isinstance(value, bytes) for value in payload.values())
        encoded = str(payload)
        assert "blob_token" not in encoded and "storage.test" not in encoded and "sig=secret" not in encoded
    assert not hasattr(provider, "adapter")


def test_runtime_provider_restart_skips_exact_completed_blob(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    _ledger, package, manifest, _adapter, _verifier, _service, _authority = _fixture(tmp_path)
    manifest_path = tmp_path / "checkpoint-manifest.json"
    write_manifest(manifest_path, manifest)
    first = sorted(manifest.files, key=lambda item: item.path)[0]

    class ReplayClient(_RuntimeMetadataClient):
        def get(self, path: str) -> dict[str, object]:
            if path.endswith("/publication"):
                return {
                    "state": "PROMOTED",
                    "exact_version_ref": "owner/checkpoints/1",
                    "verifier_run_ref": "owner/verifier/7",
                    "verifier_receipt_sha256": "e" * 64,
                    "completed_files": [
                        {
                            "file_name": first.path,
                            "content_length": first.byte_size,
                            "content_sha256": first.sha256,
                        }
                    ],
                }
            return super().get(path)

    client = ReplayClient(manifest)
    provider = BrokeredCheckpointRuntimeProvider(
        client, dataset_ref="owner/checkpoints", operation_id=OPERATION, timeout_seconds=60
    )
    puts: list[str] = []
    monkeypatch.setattr(
        provider,
        "_put_exact",
        lambda _url, path, **_kwargs: puts.append(
            path.relative_to(package).as_posix() if path.is_relative_to(package) else path.name
        ),
    )
    provider.publish(package=package, manifest_path=manifest_path)
    assert first.path not in puts
    assert len(puts) == len(manifest.files)


@pytest.mark.parametrize("completed_before_loss", [1, 3])
def test_fresh_runtime_process_skips_durable_completed_put_without_capability_leak_or_duplicate(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, completed_before_loss: int
) -> None:
    ledger, package, manifest, adapter, verifier, service, authority = _fixture(tmp_path)
    manifest_path = tmp_path / "checkpoint-manifest.json"
    write_manifest(manifest_path, manifest)
    lost_client = _ServiceMetadataClient(
        service, authority, lose_completion_response_after=completed_before_loss
    )
    first_provider = BrokeredCheckpointRuntimeProvider(
        lost_client,
        dataset_ref="owner/checkpoints",
        operation_id=OPERATION,
        timeout_seconds=60,
    )
    puts: list[str] = []

    def put_once(_url: str, path: Path, **_kwargs: object) -> None:
        puts.append(path.relative_to(package).as_posix() if path.is_relative_to(package) else path.name)

    monkeypatch.setattr(first_provider, "_put_exact", put_once)
    with pytest.raises(SimulatedProcessLoss):
        first_provider.publish(package=package, manifest_path=manifest_path)

    durable = service.status(CHECKPOINT)
    assert durable["state"] == "UPLOADING"
    assert len(durable["completed_files"]) == completed_before_loss
    encoded = str(durable)
    assert "storage.example.test" not in encoded
    assert "signature=secret" not in encoded
    assert "opaque-token" not in encoded
    assert b"storage.example.test" not in ledger.path.read_bytes()
    assert b"signature=secret" not in ledger.path.read_bytes()
    assert b"opaque-token" not in ledger.path.read_bytes()

    _restarted_ledger, restarted_service = _restarted_service(ledger, adapter, verifier)
    restarted_provider = BrokeredCheckpointRuntimeProvider(
        _ServiceMetadataClient(restarted_service, authority),
        dataset_ref="owner/checkpoints",
        operation_id=OPERATION,
        timeout_seconds=60,
    )
    monkeypatch.setattr(restarted_provider, "_put_exact", put_once)
    receipt = restarted_provider.publish(package=package, manifest_path=manifest_path)

    expected_files = {item.path for item in manifest.files} | {"checkpoint-manifest.json"}
    expected_provider_files = {checkpoint_provider_file_name(name) for name in expected_files}
    assert receipt.exact_version_ref == "owner/checkpoints/1"
    assert set(puts) == expected_files and len(puts) == len(expected_files)
    assert set(adapter.started) == expected_provider_files and len(adapter.started) == len(expected_files)
    assert adapter.finalized == 1 and verifier.calls == 1


def test_runtime_coordinator_registers_candidate_before_first_blob_prepare(
    tmp_path: Path,
) -> None:
    _ledger, package, manifest, _adapter, _verifier, _service, _authority = _fixture(tmp_path)
    manifest_path = tmp_path / "checkpoint-manifest.json"
    write_manifest(manifest_path, manifest)
    calls: list[str] = []

    class Registry:
        def add_candidate(self, value: object) -> None:
            assert value == manifest
            calls.append("candidate")

    class Provider:
        def publish(self, **_kwargs: object) -> object:
            assert calls == ["candidate"]
            calls.append("publish")
            return object()

    coordinator = BrokeredCheckpointRuntimeCoordinator(Registry(), Provider())  # type: ignore[arg-type]
    coordinator.publish(package=package, manifest_path=manifest_path, readback_directory=tmp_path / "unused")
    assert calls == ["candidate", "publish"]


def test_stale_epoch_and_wrong_completion_identity_fail_before_mutation(tmp_path: Path) -> None:
    _ledger, package, manifest, adapter, _verifier, service, authority = _fixture(tmp_path)
    spec = _specs(package, manifest)[0]
    stale = replace(authority, epoch=2)
    with pytest.raises(LeaseRejected):
        service.prepare(spec, stale)
    grant = service.prepare(spec, authority)
    completion = CheckpointBlobCompletion(
        claim_id=grant.claim_id,
        operation_id=OPERATION,
        checkpoint_id=CHECKPOINT,
        epoch=1,
        file_name=spec.file_name,
        bytes_sent=spec.content_length,
        content_sha256=spec.content_sha256,
        outcome="uploaded",
    )
    with pytest.raises(LeaseRejected):
        service.complete(completion, stale)
    assert adapter.finalized == 0
