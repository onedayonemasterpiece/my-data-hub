from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from my_data_hub.checkpoints.kaggle_runtime import (
    CHECKPOINT_RESTORE_RECEIPT_NAME,
    CheckpointRuntimeError,
    ExactCheckpointReference,
    KaggleCheckpointDatasetProvider,
    KaggleCheckpointRestoreVerifier,
    KaggleCheckpointVerifierAssets,
    RemoteControlCheckpointRegistry,
    RuntimeCheckpointCoordinator,
)
from my_data_hub.checkpoints.manifest import RestoreProbe, build_manifest, write_manifest
from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.checkpoints.registry import CheckpointHead
from my_data_hub.providers.kaggle import (
    AuthenticatedControlPlaneClient,
    ControlPlaneRuntimeIdentity,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    MetadataHttpResponse,
)
from my_data_hub.providers.models import ProviderFingerprint

NOW = datetime(2026, 8, 11, tzinfo=UTC)
RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
ATTEMPT_ID = UUID("22222222-2222-4222-8222-222222222222")
MASTER_ID = UUID("33333333-3333-4333-8333-333333333333")
CHECKPOINT_ID = UUID("44444444-4444-4444-8444-444444444444")


@pytest.fixture
def kaggle_working(tmp_path: Path, monkeypatch) -> Path:  # type: ignore[no-untyped-def]
    from my_data_hub.checkpoints import kaggle_runtime

    root = tmp_path / f"pytest-checkpoint-{uuid4()}"
    root.mkdir()
    monkeypatch.setattr(kaggle_runtime, "_KAGGLE_WORKING_ROOT", tmp_path)
    yield root


class OneResponseTransport:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def request(self, **kwargs: object) -> MetadataHttpResponse:
        self.calls.append(kwargs)
        return MetadataHttpResponse(200, json.dumps(self.payload).encode())


def _client(transport: object) -> AuthenticatedControlPlaneClient:
    return AuthenticatedControlPlaneClient(
        base_url="https://control.example.test",
        bearer_token="runtime-token-that-is-long-enough",
        runtime_identity=ControlPlaneRuntimeIdentity(RUN_ID, ATTEMPT_ID, MASTER_ID, 9),
        transport=transport,  # type: ignore[arg-type]
    )


def test_remote_head_resolves_exact_numeric_current_and_previous_for_boot() -> None:
    transport = OneResponseTransport(
        {
            "generation": 2,
            "current": {
                "checkpoint_id": str(CHECKPOINT_ID),
                "dataset_ref": "owner/private-checkpoints",
                "exact_version_ref": "owner/private-checkpoints/7",
                "manifest_sha256": "a" * 64,
            },
            "previous": {
                "checkpoint_id": "55555555-5555-4555-8555-555555555555",
                "dataset_ref": "owner/private-checkpoints",
                "exact_version_ref": "owner/private-checkpoints/6",
                "manifest_sha256": "b" * 64,
            },
        }
    )
    registry = RemoteControlCheckpointRegistry(
        _client(transport),
        operation_id="checkpoint-operation",
        dataset_ref="owner/private-checkpoints",
    )
    exact = registry.resolve_head()
    assert exact.generation == 2
    assert exact.current is not None and exact.current.exact_version_ref.endswith("/7")
    assert exact.previous is not None and exact.previous.exact_version_ref.endswith("/6")
    assert registry.head.current == CHECKPOINT_ID
    assert str(transport.calls[0]["url"]).endswith("/internal/checkpoints/postgres-master/head")


def _manifest(package: Path):  # type: ignore[no-untyped-def]
    for relative, content in {
        "physical/base.tar.gz": b"base",
        "physical/backup_manifest": b"native",
        "physical/pg_wal.tar.gz": b"wal",
        "logical/hub.dump": b"logical",
        "receipts/verification.json": b'{"ok":true}',
    }.items():
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = build_manifest(
        package_directory=package,
        checkpoint_id=CHECKPOINT_ID,
        master_instance_id=MASTER_ID,
        epoch=9,
        parent_checkpoint_id=None,
        postgres_version="18.0",
        pgvector_version="0.8.1",
        schema_version=13,
        canonical_revision=4,
        source_run_id=str(RUN_ID),
        source_identity="owner/postgres-master/3",
        created_at=NOW,
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "logical/hub.dump": "logical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/base.tar.gz": "physical",
            "physical/pg_wal.tar.gz": "physical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(13, 4, "c" * 64, {"hub.canonical_state": 1}),
    )
    write_manifest(package / "checkpoint-manifest.json", manifest)
    return manifest


class FakeVerifierAdapter:
    def __init__(self, manifest: object, dataset: KaggleDatasetIdentity) -> None:
        self.manifest = manifest
        self.dataset = dataset
        self.run: KaggleKernelRunIdentity | None = None
        self.dataset_sources: tuple[str, ...] = ()

    def push_private_notebook(self, **kwargs: object) -> object:
        run_id = kwargs["task_run_id"]
        assert isinstance(run_id, UUID)
        source = kwargs["source"]
        assert isinstance(source, bytes) and str(run_id).encode() in source
        assert kwargs["intent"].task_id == ATTEMPT_ID  # type: ignore[union-attr]
        self.dataset_sources = tuple(kwargs["dataset_sources"])  # type: ignore[arg-type]
        self.run = KaggleKernelRunIdentity(
            task_run_id=run_id,
            provider_ref="owner/checkpoint-verifier",
            source_version=4,
            source_sha256="d" * 64,
            provider_kernel_id=77,
            provider_run_ref="owner/checkpoint-verifier/4",
            started_at=NOW,
        )
        return SimpleNamespace(run=self.run)

    def poll_run(self, run: KaggleKernelRunIdentity, policy: object) -> object:
        assert run == self.run and policy is not None
        return SimpleNamespace(state="complete")

    def download_exact_run_output_tree(self, run: KaggleKernelRunIdentity, *, destination: Path) -> object:
        assert run == self.run
        destination.mkdir()
        manifest = self.manifest
        receipt = {
            "schema_version": "my-data-hub-checkpoint-restore-smoke.v1",
            "task_run_id": str(run.task_run_id),
            "checkpoint_id": str(manifest.checkpoint_id),
            "manifest_sha256": manifest.manifest_sha256,
            "dataset_ref": self.dataset.provider_ref,
            "dataset_version": self.dataset.version,
            "package_sha256": self.dataset.package_sha256,
            "ok": True,
            "observed": {
                "schema_version": manifest.restore_probe.schema_version,
                "canonical_revision": manifest.restore_probe.canonical_revision,
                "logical_hash_sha256": manifest.restore_probe.logical_hash_sha256,
                "row_counts": manifest.restore_probe.row_counts,
            },
        }
        (destination / CHECKPOINT_RESTORE_RECEIPT_NAME).write_text(json.dumps(receipt))
        return SimpleNamespace(output_tree_sha256="e" * 64)


def test_verifier_launch_binds_exact_dataset_version_and_typed_restore_receipt(
    kaggle_working: Path,
) -> None:
    package = kaggle_working / "candidate"
    package.mkdir()
    manifest = _manifest(package)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    adapter = FakeVerifierAdapter(manifest, dataset)
    notebook = json.dumps({"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}).encode()
    output_root = kaggle_working / "verifier-output"
    output_root.mkdir()
    verifier = KaggleCheckpointRestoreVerifier(
        adapter,  # type: ignore[arg-type]
        KaggleCheckpointVerifierAssets(
            notebook_ref="owner/checkpoint-verifier",
            notebook_source=notebook,
        ),
        output_directory=output_root,
        operation_id=UUID("66666666-6666-4666-8666-666666666666"),
        authorization_task_id=ATTEMPT_ID,
        clock=lambda: NOW,
        run_id_factory=lambda: RUN_ID,
    )
    receipt = verifier.verify_restore(
        exact_version_ref="owner/private-checkpoints/7",
        dataset_identity=dataset,
        manifest=manifest,
    )
    assert adapter.dataset_sources == ("owner/private-checkpoints/7",)
    assert receipt["provider_run_ref"] == "owner/checkpoint-verifier/4"
    assert receipt["checkpoint_id"] == str(CHECKPOINT_ID)


def test_boot_readback_uses_resolved_numeric_head_and_rechecks_manifest(
    kaggle_working: Path,
) -> None:
    source = kaggle_working / "remote-dataset"
    source.mkdir()
    manifest = _manifest(source)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )

    class DownloadAdapter:
        seen: tuple[str, int] | None = None

        def download_private_dataset_exact(
            self, *, provider_ref: str, version: int, destination: Path
        ) -> KaggleDatasetIdentity:
            self.seen = (provider_ref, version)
            shutil.copytree(source, destination)
            return dataset

    adapter = DownloadAdapter()
    provider = KaggleCheckpointDatasetProvider(
        adapter,  # type: ignore[arg-type]
        dataset_ref="owner/private-checkpoints",
        operation_id=uuid4(),
        resource_task_id=uuid4(),
    )
    destination = kaggle_working / "boot-readback"
    readback = provider.exact_head_readback(
        ExactCheckpointReference(
            checkpoint_id=CHECKPOINT_ID,
            dataset_ref="owner/private-checkpoints",
            exact_version_ref="owner/private-checkpoints/7",
            manifest_sha256=manifest.manifest_sha256,
        ),
        destination,
    )
    assert adapter.seen == ("owner/private-checkpoints", 7)
    assert readback.package == destination


def test_checkpoint_runtime_rejects_devstand_byte_paths(tmp_path: Path) -> None:
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=1,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    adapter = FakeVerifierAdapter(SimpleNamespace(), dataset)
    with pytest.raises(CheckpointRuntimeError, match="only below /kaggle/working"):
        KaggleCheckpointRestoreVerifier(
            adapter,  # type: ignore[arg-type]
            KaggleCheckpointVerifierAssets(
                notebook_ref="owner/checkpoint-verifier",
                notebook_source=b"{}",
            ),
            output_directory=tmp_path,
            operation_id=uuid4(),
            authorization_task_id=ATTEMPT_ID,
        )


def test_runtime_composite_matches_master_create_and_publish_protocol(
    kaggle_working: Path,
) -> None:
    captured: dict[str, object] = {}

    class Cursor:
        query = ""

        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> Cursor:
            self.query = query
            return self

        def fetchone(self) -> tuple[str] | None:
            if self.query == "SHOW server_version":
                return ("18.1",)
            if "extversion" in self.query:
                return ("0.8.1",)
            if "pg_current_wal_lsn" in self.query:
                return ("0/16B6C50",)
            return None

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> Cursor:
            return Cursor()

    class Builder:
        def build(self, **kwargs: object):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            package = kwargs["package_directory"]
            assert isinstance(package, Path)
            package.mkdir(parents=True)
            manifest = SimpleNamespace(checkpoint_id=CHECKPOINT_ID)
            manifest_path = package / "checkpoint-manifest.json"
            manifest_path.write_text("{}")
            return package, manifest_path, manifest

    expected = PublishReceipt(
        checkpoint_id=str(CHECKPOINT_ID),
        exact_version_ref="owner/private-checkpoints/7",
        manifest_sha256="a" * 64,
        current_checkpoint_id=str(CHECKPOINT_ID),
        previous_checkpoint_id=None,
        upload_seconds=1,
        readback_seconds=2,
        restore_seconds=3,
        package_bytes=4,
        restore_receipt={"ok": True},
    )

    class Publisher:
        registry = SimpleNamespace(head=CheckpointHead())
        provider = SimpleNamespace(claim=None, dataset_ref="owner/private-checkpoints")

        def publish(self, **kwargs: object) -> PublishReceipt:
            assert str(kwargs["package"]).startswith(str(kaggle_working))
            assert str(kwargs["readback_directory"]).startswith(str(kaggle_working))
            return expected

    coordinator = RuntimeCheckpointCoordinator(
        builder=Builder(),  # type: ignore[arg-type]
        coordinator=Publisher(),  # type: ignore[arg-type]
        probe_relations=("hub.canonical_state",),
        source_identity="owner/postgres-master/3",
        connect=lambda *_args, **_kwargs: Connection(),
        clock=lambda: NOW,
        checkpoint_id_factory=lambda: CHECKPOINT_ID,
    )
    receipt = coordinator.create_and_publish(
        database_url="postgresql:///postgres",
        package_directory=kaggle_working / "checkpoints",
        identity=SimpleNamespace(master_instance_id=MASTER_ID, run_id=str(RUN_ID), epoch=9),
    )
    assert receipt == expected
    build_identity = captured["identity"]
    assert build_identity.postgres_version == "18.1"  # type: ignore[union-attr]
    assert build_identity.pgvector_version == "0.8.1"  # type: ignore[union-attr]
    assert build_identity.checkpoint_lsn == "0/16B6C50"  # type: ignore[union-attr]
