"""Fixed runtime-local checkpoint and recovery effect for FM24.

The acceptance hook receives only the exact master binding and its durable
intent digest.  PostgreSQL credentials remain private constructor state, paths
are fixed below Kaggle working, and all SQL/archive/provider behavior stays in
the existing :class:`RuntimeCheckpointCoordinator`.
"""

from __future__ import annotations

import hashlib
import re
import shutil
from dataclasses import asdict, dataclass, field
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceBinding
from my_data_hub.acceptance.soak_session import FM24CheckpointRecoveryEvidence
from my_data_hub.checkpoints.kaggle_runtime import RuntimeCheckpointCoordinator
from my_data_hub.checkpoints.manifest import load_and_verify
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.providers.kaggle.adapter import tree_sha256

FM24_CHECKPOINT_PACKAGE_DIRECTORY = Path("/kaggle/working/checkpoints")
FM24_RECOVERY_ROOT = Path("/kaggle/working/fm24-recovery")
FM24_CHECKPOINT_MAX_SECONDS = 1_800


class FM24CheckpointRecoveryError(RuntimeError):
    """The fixed FM24 checkpoint/recovery effect could not be proven."""


@dataclass(slots=True)
class RuntimeCheckpointRecoveryAdapter:
    """Run/reconcile one task-derived checkpoint and independently read HEAD.

    ``database_url`` is internal runtime wiring rather than acceptance input and
    is excluded from representations.  The coordinator owns bounded archive,
    upload, exact readback, independent restore verification and HEAD CAS.
    """

    coordinator: RuntimeCheckpointCoordinator
    database_url: str = field(repr=False)

    def __post_init__(self) -> None:
        if not self.database_url.startswith(("postgresql://", "postgres://")):
            raise ValueError("FM24 checkpoint runtime requires a PostgreSQL URL")
        if not 60 <= self.coordinator.timeout_seconds <= FM24_CHECKPOINT_MAX_SECONDS:
            raise ValueError("FM24 checkpoint archive exceeds its fixed 30-minute bound")

    def ensure_checkpoint_recovery(
        self,
        binding: MasterAcceptanceBinding,
        *,
        intent_sha256: str,
    ) -> FM24CheckpointRecoveryEvidence:
        if not re.fullmatch(r"[a-f0-9]{64}", intent_sha256):
            raise ValueError("FM24 checkpoint/recovery intent is not SHA-256")
        checkpoint_id = uuid5(
            NAMESPACE_URL,
            f"my-data-hub:fm24-checkpoint-recovery:{intent_sha256}",
        )
        configured_factory = self.coordinator.checkpoint_id_factory
        if configured_factory is not None and UUID(str(configured_factory())) != checkpoint_id:
            raise FM24CheckpointRecoveryError(
                "FM24 checkpoint coordinator has another candidate identity"
            )
        # The task-derived ID makes a process restart reconcile the promoted
        # exact HEAD rather than accidentally create a second checkpoint.
        self.coordinator.checkpoint_id_factory = lambda: checkpoint_id
        receipt = self.coordinator.create_and_publish(
            database_url=self.database_url,
            package_directory=FM24_CHECKPOINT_PACKAGE_DIRECTORY,
            identity=MasterIdentity(
                master_instance_id=binding.master_instance_id,
                run_id=str(binding.run_id),
                epoch=binding.epoch,
            ),
        )
        if (
            receipt.checkpoint_id != str(checkpoint_id)
            or receipt.current_checkpoint_id != str(checkpoint_id)
            or not re.fullmatch(
                r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*",
                receipt.exact_version_ref,
            )
            or not re.fullmatch(r"[a-f0-9]{64}", receipt.manifest_sha256)
            or not self._restore_was_verified(receipt.restore_receipt)
        ):
            raise FM24CheckpointRecoveryError(
                "FM24 checkpoint publication lacks exact verified metadata"
            )
        checkpoint_receipt_sha256 = _sha(asdict(receipt))

        recovery_destination = FM24_RECOVERY_ROOT / intent_sha256
        if recovery_destination.is_symlink():
            raise FM24CheckpointRecoveryError("FM24 recovery destination is a symbolic link")
        if recovery_destination.exists():
            if not recovery_destination.is_dir():
                raise FM24CheckpointRecoveryError("FM24 recovery destination is not a directory")
            shutil.rmtree(recovery_destination)
        recovery_destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        recovered_package = self.coordinator.resolve_boot_checkpoint(recovery_destination)
        if recovered_package is None:
            raise FM24CheckpointRecoveryError("FM24 durable checkpoint HEAD is absent")
        recovered_manifest = load_and_verify(
            recovered_package / "checkpoint-manifest.json",
            recovered_package,
        )
        if (
            recovered_manifest.checkpoint_id != checkpoint_id
            or recovered_manifest.manifest_sha256 != receipt.manifest_sha256
        ):
            raise FM24CheckpointRecoveryError(
                "FM24 fixed recovery differs from the published exact checkpoint"
            )
        recovery_receipt_sha256 = _sha(
            {
                "schema_version": "my-data-hub-fm24-recovery-readback.v1",
                "intent_sha256": intent_sha256,
                "checkpoint_id": str(checkpoint_id),
                "exact_version_ref": receipt.exact_version_ref,
                "manifest_sha256": recovered_manifest.manifest_sha256,
                "package_tree_sha256": tree_sha256(recovered_package),
                "recovery_succeeded": True,
            }
        )
        return FM24CheckpointRecoveryEvidence(
            evidence_class="live",
            checkpoint_verified=True,
            recovery_succeeded=True,
            checkpoint_id=checkpoint_id,
            exact_version_ref=receipt.exact_version_ref,
            manifest_sha256=receipt.manifest_sha256,
            checkpoint_receipt_sha256=checkpoint_receipt_sha256,
            recovery_receipt_sha256=recovery_receipt_sha256,
        )

    @staticmethod
    def _restore_was_verified(receipt: dict[str, object]) -> bool:
        # Fresh publication has the independent verifier's `ok`; a retry after
        # lost promotion response has the durable verified-HEAD reconciliation
        # marker produced by KaggleCheckpointCoordinator.
        return receipt.get("ok") is True or receipt.get("reconciled_from_durable_verified_head") is True


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


__all__ = [
    "FM24_CHECKPOINT_MAX_SECONDS",
    "FM24_CHECKPOINT_PACKAGE_DIRECTORY",
    "FM24_RECOVERY_ROOT",
    "FM24CheckpointRecoveryError",
    "RuntimeCheckpointRecoveryAdapter",
]
