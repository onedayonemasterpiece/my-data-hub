from __future__ import annotations

from types import SimpleNamespace
from uuid import UUID

import pytest

import my_data_hub.master_runtime.fm24_checkpoint_recovery as recovery_module
from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceBinding
from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.master_runtime.fm24_checkpoint_recovery import (
    FM24CheckpointRecoveryError,
    RuntimeCheckpointRecoveryAdapter,
)


class Coordinator:
    timeout_seconds = 1_800
    checkpoint_id_factory = None

    def __init__(self, *, restore_receipt: dict[str, object] | None = None) -> None:
        self.restore_receipt = restore_receipt or {"ok": True, "provider_run_ref": "owner/verifier/3"}
        self.created: list[tuple[str, object, object]] = []
        self.recovered: list[object] = []
        self.checkpoint_id: UUID | None = None

    def create_and_publish(self, *, database_url, package_directory, identity):
        self.created.append((database_url, package_directory, identity))
        self.checkpoint_id = UUID(str(self.checkpoint_id_factory()))
        return PublishReceipt(
            checkpoint_id=str(self.checkpoint_id),
            exact_version_ref="owner/checkpoints/9",
            manifest_sha256="a" * 64,
            current_checkpoint_id=str(self.checkpoint_id),
            previous_checkpoint_id=None,
            upload_seconds=1.0,
            readback_seconds=2.0,
            restore_seconds=3.0,
            package_bytes=42,
            restore_receipt=self.restore_receipt,
        )

    def resolve_boot_checkpoint(self, destination):
        self.recovered.append(destination)
        package = destination / "package"
        package.mkdir(parents=True)
        (package / "checkpoint-manifest.json").write_text("{}")
        return package


def _binding() -> MasterAcceptanceBinding:
    return MasterAcceptanceBinding(
        operation_id=UUID(int=1),
        run_id=UUID(int=2),
        attempt_id=UUID(int=3),
        service_instance_id="master-7",
        master_instance_id=UUID(int=4),
        epoch=7,
    )


def test_fixed_adapter_runs_real_coordinator_then_exact_head_recovery(monkeypatch, tmp_path) -> None:
    coordinator = Coordinator()
    monkeypatch.setattr(recovery_module, "FM24_CHECKPOINT_PACKAGE_DIRECTORY", tmp_path / "checkpoints")
    monkeypatch.setattr(recovery_module, "FM24_RECOVERY_ROOT", tmp_path / "recovery")
    monkeypatch.setattr(recovery_module, "tree_sha256", lambda _path: "b" * 64)
    monkeypatch.setattr(
        recovery_module,
        "load_and_verify",
        lambda _manifest, _package: SimpleNamespace(
            checkpoint_id=coordinator.checkpoint_id,
            manifest_sha256="a" * 64,
        ),
    )
    adapter = RuntimeCheckpointRecoveryAdapter(
        coordinator=coordinator,  # type: ignore[arg-type]
        database_url="postgresql://runtime-private",
    )
    receipt = adapter.ensure_checkpoint_recovery(_binding(), intent_sha256="c" * 64)

    assert receipt.evidence_class == "live"
    assert receipt.checkpoint_verified and receipt.recovery_succeeded
    assert receipt.checkpoint_id == coordinator.checkpoint_id
    assert receipt.exact_version_ref == "owner/checkpoints/9"
    assert receipt.manifest_sha256 == "a" * 64
    assert len(receipt.checkpoint_receipt_sha256) == 64
    assert len(receipt.recovery_receipt_sha256) == 64
    assert len(coordinator.created) == len(coordinator.recovered) == 1
    assert coordinator.created[0][2].epoch == 7


def test_adapter_refuses_live_evidence_without_independent_restore() -> None:
    coordinator = Coordinator(restore_receipt={"ok": False})
    adapter = RuntimeCheckpointRecoveryAdapter(
        coordinator=coordinator,  # type: ignore[arg-type]
        database_url="postgresql://runtime-private",
    )
    with pytest.raises(FM24CheckpointRecoveryError, match="lacks exact verified metadata"):
        adapter.ensure_checkpoint_recovery(_binding(), intent_sha256="d" * 64)
    assert coordinator.recovered == []


def test_adapter_has_fixed_archive_bound_and_no_effect_method_clock_bytes_or_sql() -> None:
    coordinator = Coordinator()
    coordinator.timeout_seconds = 1_801
    with pytest.raises(ValueError, match="fixed 30-minute bound"):
        RuntimeCheckpointRecoveryAdapter(
            coordinator=coordinator,  # type: ignore[arg-type]
            database_url="postgresql://runtime-private",
        )
    parameter_names = set(
        RuntimeCheckpointRecoveryAdapter.ensure_checkpoint_recovery.__annotations__
    )
    assert not parameter_names.intersection({"clock", "sql", "bytes", "database_url"})
