"""Provider-side Kaggle checkpoint publication and independent verification.

All archive and notebook-output paths in this module are provider-side paths.
The remote control-plane ports exchange bounded JSON metadata only; they never
accept a PostgreSQL URL, PGDATA path, archive bytes, or notebook output bytes.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from time import monotonic, sleep
from typing import Any, Protocol
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.db.migrations import discover_migrations
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.providers.kaggle.adapter import (
    KaggleProviderAdapter,
    directory_sha256,
    tree_sha256,
)
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    KaggleAmbiguousMutation,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KernelState,
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.control_journal import (
    AuthenticatedControlPlaneClient,
    ControlPlaneRuntimeIdentity,
)
from my_data_hub.providers.kaggle.source_attestation import executable_source_sha256
from my_data_hub.providers.models import ControlClass, ProviderKind
from my_data_hub.runtime_sdk.lifetime import (
    CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS,
    CHECKPOINT_VERIFIER_TIMEOUT_SECONDS,
)

from .archive import ArchiveCreator, BackupTools, write_probe_receipt
from .manifest import (
    CheckpointManifest,
    RestoreProbe,
    build_manifest,
    load_and_verify,
    write_manifest,
)
from .publisher import PublishError, PublishReceipt
from .registry import CheckpointHead, CheckpointRegistryContract
from .restore_probe import collect_restore_probe

CHECKPOINT_MANIFEST_NAME = "checkpoint-manifest.json"
CHECKPOINT_RESTORE_RECEIPT_NAME = "checkpoint-restore-receipt.json"
RESTORE_RECEIPT_CONTRACT = "my-data-hub-checkpoint-restore-smoke.v2"
_KAGGLE_WORKING_ROOT = Path("/kaggle/working")


class CheckpointRuntimeError(RuntimeError):
    """A provider-side checkpoint or verifier contract failed closed."""


class CheckpointRetryableError(PublishError):
    """Publication is incomplete but safe to retry with the same candidate."""


class CheckpointRestoreObservedReceipt(BaseModel):
    """Bounded metadata observed from the isolated restored PostgreSQL."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: int = Field(ge=1)
    canonical_revision: int = Field(ge=0)
    logical_hash_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    row_counts: dict[str, int]
    postgres_version: str = Field(pattern=r"^18\.[0-9]+(?:\.[0-9]+)?$")
    extensions: dict[str, str]
    migration_boundary: dict[str, object]
    database_invariants: dict[str, int]
    vector_query: dict[str, object]
    bounded_read_smoke: dict[str, int]


class CheckpointRestoreReceipt(BaseModel):
    """Secret-free verifier output; business/checkpoint bytes remain in Kaggle."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(pattern=r"^my-data-hub-checkpoint-restore-smoke\.v2$")
    task_run_id: UUID
    checkpoint_id: UUID
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    manifest_file_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    dataset_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    dataset_version: int = Field(ge=1)
    package_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    restore_mode: str = Field(pattern=r"^isolated_physical_restore$")
    execution_pins_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    runtime_image_identity: str = Field(pattern=r"^[^@\s]+@sha256:[a-f0-9]{64}$")
    runtime_image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    input_dataset_versions: list[str] = Field(min_length=2, max_length=2)
    ok: bool
    observed: CheckpointRestoreObservedReceipt


class CheckpointRestoreVerifiedReceipt(CheckpointRestoreReceipt):
    """Central metadata receipt binding runtime evidence to exact provider output."""

    provider_run_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$"
    )
    source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_tree_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class RemoteCheckpointRegistryPort(CheckpointRegistryContract, Protocol):
    """Metadata-only registry port implemented by the devstand HTTPS API."""

    def resolve_head(self) -> RemoteCheckpointHeadSnapshot: ...


class CurrentResourceClaimSource(Protocol):
    def current_resource_claim(
        self,
        *,
        provider_ref: str,
        kind: ProviderKind,
        control_class: ControlClass,
    ) -> TaskResourceClaim: ...


@dataclass(frozen=True, slots=True)
class ExactCheckpointReference:
    checkpoint_id: UUID
    dataset_ref: str
    exact_version_ref: str
    manifest_sha256: str

    def __post_init__(self) -> None:
        provider_ref, _version = _parse_exact_dataset_version(self.exact_version_ref)
        if provider_ref != self.dataset_ref:
            raise CheckpointRuntimeError("checkpoint HEAD dataset/version refs differ")
        if not re.fullmatch(r"[a-f0-9]{64}", self.manifest_sha256):
            raise CheckpointRuntimeError("checkpoint HEAD manifest hash is invalid")


@dataclass(frozen=True, slots=True)
class RemoteCheckpointHeadSnapshot:
    generation: int
    current: ExactCheckpointReference | None
    previous: ExactCheckpointReference | None

    def __post_init__(self) -> None:
        if self.generation < 0 or (self.generation == 0) != (self.current is None):
            raise CheckpointRuntimeError("checkpoint HEAD generation/current mismatch")
        if self.previous is not None and self.current is None:
            raise CheckpointRuntimeError("checkpoint HEAD previous exists without current")


class RemoteControlCheckpointRegistry:
    """Checkpoint registry client for a Kaggle master/verifier runtime.

    Manifest metadata (IDs, versions, paths, sizes and hashes) is safe control
    state.  Archive bytes remain in Kaggle and cannot be passed to this class.
    """

    def __init__(
        self,
        client: AuthenticatedControlPlaneClient,
        *,
        operation_id: str,
        dataset_ref: str,
        service_kind: str = "postgres-master",
    ) -> None:
        if not operation_id or not dataset_ref or not service_kind:
            raise ValueError("remote checkpoint registry identity is incomplete")
        self.client = client
        self.operation_id = operation_id
        self.dataset_ref = dataset_ref
        self.service_kind = service_kind

    @property
    def head(self) -> CheckpointHead:
        exact = self.resolve_head()
        return CheckpointHead(
            generation=exact.generation,
            current=exact.current.checkpoint_id if exact.current else None,
            previous=exact.previous.checkpoint_id if exact.previous else None,
        )

    def resolve_head(self) -> RemoteCheckpointHeadSnapshot:
        """Resolve boot-safe exact current/previous refs; never a dataset slug/latest."""

        value = self.client.get(f"/internal/checkpoints/{quote(self.service_kind, safe='')}/head")
        try:
            generation = int(value["generation"])
            current = _exact_checkpoint_reference(value.get("current"))
            previous = _exact_checkpoint_reference(value.get("previous"))
            if generation < 0 or (generation == 0) != (current is None):
                raise ValueError("checkpoint HEAD generation/current mismatch")
            if previous is not None and current is None:
                raise ValueError("checkpoint HEAD previous exists without current")
            return RemoteCheckpointHeadSnapshot(
                generation=generation,
                current=current,
                previous=previous,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckpointRuntimeError("remote checkpoint HEAD response is invalid") from exc

    def add_candidate(self, manifest: CheckpointManifest) -> None:
        manifest.validate()
        self.client.post(
            "/internal/checkpoints/candidates",
            {
                "operation_id": self.operation_id,
                "dataset_ref": self.dataset_ref,
                "service_kind": self.service_kind,
                "manifest": manifest.payload(),
            },
        )

    def uploaded(self, checkpoint_id: UUID, exact_version_ref: str) -> None:
        self._transition(checkpoint_id, "uploaded", {"exact_version_ref": exact_version_ref})

    def package_uploaded(self, checkpoint_id: UUID, package_sha256: str) -> None:
        if len(package_sha256) != 64 or any(char not in "0123456789abcdef" for char in package_sha256):
            raise ValueError("checkpoint package hash is invalid")
        self._transition(checkpoint_id, "package-identity", {"package_sha256": package_sha256})

    def readback_verified(self, checkpoint_id: UUID) -> None:
        self._transition(checkpoint_id, "readback-verified", {})

    def restore_verified(self, checkpoint_id: UUID) -> None:
        self._transition(checkpoint_id, "restore-verified", {})

    def reject(self, checkpoint_id: UUID, reason: str) -> None:
        if not reason or len(reason) > 512:
            raise ValueError("checkpoint rejection reason is invalid")
        self._transition(checkpoint_id, "reject", {"reason": reason})

    def promote(self, checkpoint_id: UUID, *, expected_generation: int) -> CheckpointHead:
        value = self._transition(
            checkpoint_id,
            "promote",
            {"expected_generation": expected_generation},
        )
        try:
            exact = RemoteCheckpointHeadSnapshot(
                generation=int(value["generation"]),
                current=_exact_checkpoint_reference(value.get("current")),
                previous=_exact_checkpoint_reference(value.get("previous")),
            )
            if exact.current is None or exact.current.checkpoint_id != checkpoint_id:
                raise ValueError("promoted checkpoint is not exact current")
            return CheckpointHead(
                generation=exact.generation,
                current=exact.current.checkpoint_id,
                previous=exact.previous.checkpoint_id if exact.previous else None,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CheckpointRuntimeError("remote checkpoint promotion response is invalid") from exc

    def _transition(
        self,
        checkpoint_id: UUID,
        action: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        return self.client.post(
            f"/internal/checkpoints/{checkpoint_id}/{action}",
            {"service_kind": self.service_kind, **payload},
        )


@dataclass(frozen=True, slots=True)
class CheckpointBuildIdentity:
    checkpoint_id: UUID
    master_instance_id: UUID
    epoch: int
    parent_checkpoint_id: UUID | None
    postgres_version: str
    pgvector_version: str
    source_run_id: str
    source_identity: str
    created_at: datetime
    checkpoint_lsn: str
    probe_relations: tuple[str, ...]


class KaggleCheckpointCandidateBuilder:
    """Create physical/logical archives and their exact manifest inside Kaggle."""

    def __init__(self, archive_creator: ArchiveCreator) -> None:
        self.archive_creator = archive_creator

    def build(
        self,
        *,
        database_url: str,
        connection: Any,
        package_directory: Path,
        identity: CheckpointBuildIdentity,
        timeout_seconds: int = CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS,
    ) -> tuple[Path, Path, CheckpointManifest]:
        _assert_kaggle_working_path(package_directory)
        if package_directory.exists():
            raise CheckpointRuntimeError("checkpoint package destination must be absent")
        artifacts = self.archive_creator.create(
            database_url=database_url,
            package=package_directory,
            timeout_seconds=timeout_seconds,
        )
        observed = collect_restore_probe(connection, identity.probe_relations)
        verification_path = package_directory / "receipts" / "verification.json"
        write_probe_receipt(
            verification_path,
            {
                "contract": "my-data-hub-checkpoint-verification.v1",
                "checkpoint_id": str(identity.checkpoint_id),
                "master_instance_id": str(identity.master_instance_id),
                "epoch": identity.epoch,
                "observed": observed,
            },
        )
        artifacts["receipts/verification.json"] = "verification_receipt"
        probe = RestoreProbe(
            schema_version=int(observed["schema_version"]),
            canonical_revision=int(observed["canonical_revision"]),
            logical_hash_sha256=str(observed["logical_hash_sha256"]),
            row_counts={str(key): int(value) for key, value in dict(observed["row_counts"]).items()},
        )
        manifest = build_manifest(
            package_directory=package_directory,
            checkpoint_id=identity.checkpoint_id,
            master_instance_id=identity.master_instance_id,
            epoch=identity.epoch,
            parent_checkpoint_id=identity.parent_checkpoint_id,
            postgres_version=identity.postgres_version,
            pgvector_version=identity.pgvector_version,
            schema_version=probe.schema_version,
            canonical_revision=probe.canonical_revision,
            source_run_id=identity.source_run_id,
            source_identity=identity.source_identity,
            created_at=identity.created_at,
            checkpoint_lsn=identity.checkpoint_lsn,
            file_kinds=artifacts,
            restore_probe=probe,
        )
        manifest_path = package_directory / CHECKPOINT_MANIFEST_NAME
        write_manifest(manifest_path, manifest)
        load_and_verify(manifest_path, package_directory)
        return package_directory, manifest_path, manifest


@dataclass(frozen=True, slots=True)
class KaggleCheckpointReadback:
    package: Path
    identity: KaggleDatasetIdentity


class KaggleCheckpointDatasetProvider:
    """Permanent protected checkpoint dataset backed by the one Kaggle adapter."""

    def __init__(
        self,
        adapter: KaggleProviderAdapter,
        *,
        dataset_ref: str,
        operation_id: UUID,
        resource_task_id: UUID,
        claim: TaskResourceClaim | None = None,
    ) -> None:
        if len(dataset_ref.split("/")) != 2:
            raise ValueError("checkpoint dataset ref must be exact owner/slug")
        self.adapter = adapter
        self.dataset_ref = dataset_ref
        self.operation_id = operation_id
        self.resource_task_id = resource_task_id
        self.claim = claim

    def upload_candidate(self, package: Path, manifest: CheckpointManifest) -> str:
        _assert_kaggle_working_path(package)
        manifest.validate()
        content_sha = directory_sha256(package)
        current_version = self.adapter.current_private_dataset_version(provider_ref=self.dataset_ref)

        # First reconcile the provider current version against this exact
        # candidate.  This repairs a lost receipt/claim response without ever
        # issuing another create/version side effect.
        if current_version is not None:
            recovered = self._reconcile_current_candidate(
                package=package,
                manifest=manifest,
                content_sha=content_sha,
                current_version=current_version,
            )
            if recovered is not None:
                self.claim = recovered.claim
                self.resource_task_id = recovered.claim.task_id
                return f"{recovered.identity.provider_ref}/{recovered.identity.version}"

        claim = self.claim
        if claim is None:
            if current_version is not None:
                raise CheckpointRuntimeError(
                    "checkpoint dataset exists but has no exact durable claim for a new candidate"
                )
            action = MutationAction.CREATE_DATASET
            arguments = self._create_arguments(content_sha)
            intent = self._intent(manifest, action, arguments)
            expected_version = 1
            try:
                result = self.adapter.create_private_dataset_from_directory(
                    intent=intent,
                    source_directory=package,
                    title=self.dataset_ref.split("/", 1)[1],
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                    disposable=False,
                )
            except Exception:
                result = self._reconcile_after_ambiguous_effect(
                    intent=intent,
                    package=package,
                    arguments=arguments,
                    expected_version=expected_version,
                )
        else:
            self._validate_claim(claim)
            if current_version != claim.provider_version:
                raise CheckpointRuntimeError(
                    "checkpoint dataset provider version advanced beyond the exact durable claim"
                )
            self._assert_checkpoint_id_not_reused(
                package=package,
                manifest=manifest,
                current_version=current_version,
            )
            notes = self._version_notes(manifest)
            action = MutationAction.VERSION_DATASET
            arguments = self._version_arguments(
                content_sha=content_sha,
                previous_version=claim.provider_version,
                notes=notes,
            )
            intent = self._intent(
                manifest,
                action,
                arguments,
                expected_fingerprint=claim.fingerprint,
            )
            expected_version = claim.provider_version + 1
            try:
                result = self.adapter.create_private_dataset_version_from_directory(
                    intent=intent,
                    claim=claim,
                    source_directory=package,
                    version_notes=notes,
                )
            except Exception:
                result = self._reconcile_after_ambiguous_effect(
                    intent=intent,
                    package=package,
                    arguments=arguments,
                    expected_version=expected_version,
                )
        self.claim = result.claim
        self.resource_task_id = result.claim.task_id
        return f"{result.identity.provider_ref}/{result.identity.version}"

    def _reconcile_current_candidate(
        self,
        *,
        package: Path,
        manifest: CheckpointManifest,
        content_sha: str,
        current_version: int,
    ) -> DatasetMutationResult | None:
        if current_version == 1:
            action = MutationAction.CREATE_DATASET
            arguments = self._create_arguments(content_sha)
            intent = self._intent(manifest, action, arguments)
        else:
            previous = self.adapter.read_private_dataset(
                provider_ref=self.dataset_ref,
                version=current_version - 1,
            )
            notes = self._version_notes(manifest)
            action = MutationAction.VERSION_DATASET
            arguments = self._version_arguments(
                content_sha=content_sha,
                previous_version=current_version - 1,
                notes=notes,
            )
            intent = self._intent(
                manifest,
                action,
                arguments,
                expected_fingerprint=previous.fingerprint,
            )
        try:
            return self.adapter.reconcile_private_dataset_directory_mutation(
                intent=intent,
                source_directory=package,
                expected_version=current_version,
                arguments=arguments,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
            )
        except KaggleAmbiguousMutation:
            return None

    def _reconcile_after_ambiguous_effect(
        self,
        *,
        intent: ProviderEffectIntent,
        package: Path,
        arguments: dict[str, Any],
        expected_version: int,
    ) -> DatasetMutationResult:
        try:
            return self.adapter.reconcile_private_dataset_directory_mutation(
                intent=intent,
                source_directory=package,
                expected_version=expected_version,
                arguments=arguments,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
            )
        except Exception as reconciliation_error:
            raise CheckpointRuntimeError(
                "checkpoint provider effect remains ambiguous and was not repeated"
            ) from reconciliation_error

    def _validate_claim(self, claim: TaskResourceClaim) -> None:
        if (
            claim.provider_ref != self.dataset_ref
            or claim.kind is not ProviderKind.DATASET
            or claim.control_class is not ControlClass.ORCHESTRATOR_PROTECTED
            or claim.disposable
        ):
            raise CheckpointRuntimeError("checkpoint dataset claim is not permanent/exact")

    def _assert_checkpoint_id_not_reused(
        self,
        *,
        package: Path,
        manifest: CheckpointManifest,
        current_version: int,
    ) -> None:
        scratch = package.parent / f".current-checkpoint-{manifest.checkpoint_id}"
        shutil.rmtree(scratch, ignore_errors=True)
        try:
            self.adapter.download_private_dataset_exact(
                provider_ref=self.dataset_ref,
                version=current_version,
                destination=scratch,
            )
            current_manifest = load_and_verify(
                scratch / CHECKPOINT_MANIFEST_NAME,
                scratch,
            )
        finally:
            shutil.rmtree(scratch, ignore_errors=True)
        if current_manifest.checkpoint_id == manifest.checkpoint_id:
            raise CheckpointRuntimeError(
                "checkpoint id already exists with different provider bytes; refusing another version"
            )

    @staticmethod
    def _create_arguments(content_sha: str) -> dict[str, Any]:
        return {
            "content_tree_sha256": content_sha,
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
            "disposable": False,
        }

    @staticmethod
    def _version_notes(manifest: CheckpointManifest) -> str:
        return f"checkpoint {manifest.checkpoint_id} manifest {manifest.manifest_sha256}"

    @staticmethod
    def _version_arguments(
        *,
        content_sha: str,
        previous_version: int,
        notes: str,
    ) -> dict[str, Any]:
        return {
            "content_tree_sha256": content_sha,
            "previous_version": previous_version,
            "version_notes_sha256": hashlib.sha256(notes.encode()).hexdigest(),
        }

    def exact_readback(self, exact_version_ref: str, destination: Path) -> KaggleCheckpointReadback:
        _assert_kaggle_working_path(destination)
        provider_ref, version = _parse_exact_dataset_version(exact_version_ref)
        if provider_ref != self.dataset_ref:
            raise CheckpointRuntimeError("checkpoint exact readback targets another dataset")
        identity = self.adapter.download_private_dataset_exact(
            provider_ref=provider_ref,
            version=version,
            destination=destination,
        )
        return KaggleCheckpointReadback(destination, identity)

    def exact_head_readback(
        self,
        reference: ExactCheckpointReference,
        destination: Path,
    ) -> KaggleCheckpointReadback:
        """Boot from a resolved HEAD descriptor, never from dataset latest."""

        if reference.dataset_ref != self.dataset_ref:
            raise CheckpointRuntimeError("boot checkpoint belongs to another protected dataset")
        readback = self.exact_readback(reference.exact_version_ref, destination)
        manifest = load_and_verify(
            readback.package / CHECKPOINT_MANIFEST_NAME,
            readback.package,
        )
        if manifest.checkpoint_id != reference.checkpoint_id or manifest.manifest_sha256 != reference.manifest_sha256:
            raise CheckpointRuntimeError("boot checkpoint differs from exact resolved HEAD metadata")
        return readback

    def _intent(
        self,
        manifest: CheckpointManifest,
        action: MutationAction,
        arguments: dict[str, Any],
        *,
        expected_fingerprint: Any | None = None,
    ) -> ProviderEffectIntent:
        effect_id = uuid5(
            NAMESPACE_URL,
            f"my-data-hub:checkpoint:{manifest.checkpoint_id}:{action.value}",
        )
        return ProviderEffectIntent.create(
            operation_id=self.operation_id,
            effect_id=effect_id,
            idempotency_key=f"checkpoint:{manifest.checkpoint_id}:{action.value}",
            task_id=self.resource_task_id,
            action=action,
            provider_ref=self.dataset_ref,
            expected_fingerprint=expected_fingerprint,
            arguments=arguments,
            requested_at=manifest.created_at,
        )


@dataclass(frozen=True, slots=True)
class KaggleCheckpointVerifierAssets:
    notebook_ref: str
    notebook_source: bytes
    code_file: str = "worker.ipynb"
    kernel_type: str = "notebook"
    language: str = "python"
    timeout_seconds: int = CHECKPOINT_VERIFIER_TIMEOUT_SECONDS
    runtime_dataset_exact_ref: str | None = None
    runtime_image_identity: str | None = None
    runtime_image_source_commit: str | None = None
    runtime_python_series: str | None = None
    wheel_relative_path: str | None = None
    wheel_sha256: str | None = None
    postgres_runtime_archive_relative_path: str | None = None
    postgres_runtime_archive_sha256: str | None = None
    postgres_runtime_manifest_relative_path: str | None = None
    postgres_runtime_manifest_sha256: str | None = None

    def __post_init__(self) -> None:
        if len(self.notebook_ref.split("/")) != 2 or not self.notebook_source:
            raise ValueError("checkpoint verifier notebook identity/source is incomplete")
        if self.kernel_type not in {"notebook", "script"}:
            raise ValueError("checkpoint verifier kernel type is invalid")
        if not 60 <= self.timeout_seconds <= CHECKPOINT_VERIFIER_TIMEOUT_SECONDS:
            raise ValueError("checkpoint verifier timeout exceeds its attempt allocation")

    def execution_contract(self) -> dict[str, str]:
        values = {
            "runtime_dataset_exact_ref": self.runtime_dataset_exact_ref,
            "runtime_image_identity": self.runtime_image_identity,
            "runtime_image_source_commit": self.runtime_image_source_commit,
            "runtime_python_series": self.runtime_python_series,
            "wheel_relative_path": self.wheel_relative_path,
            "wheel_sha256": self.wheel_sha256,
            "postgres_runtime_archive_relative_path": self.postgres_runtime_archive_relative_path,
            "postgres_runtime_archive_sha256": self.postgres_runtime_archive_sha256,
            "postgres_runtime_manifest_relative_path": self.postgres_runtime_manifest_relative_path,
            "postgres_runtime_manifest_sha256": self.postgres_runtime_manifest_sha256,
        }
        if any(not isinstance(value, str) or not value for value in values.values()):
            raise CheckpointRuntimeError("checkpoint verifier exact runtime assets are incomplete")
        contract = {key: str(value) for key, value in values.items()}
        _parse_exact_dataset_version(contract["runtime_dataset_exact_ref"])
        if (
            not re.fullmatch(r"[^@\s]+@sha256:[a-f0-9]{64}", contract["runtime_image_identity"])
            or not re.fullmatch(r"[a-f0-9]{40}", contract["runtime_image_source_commit"])
            or not re.fullmatch(r"[0-9]+\.[0-9]+", contract["runtime_python_series"])
            or any(
                not re.fullmatch(r"[a-f0-9]{64}", contract[name])
                for name in (
                    "wheel_sha256",
                    "postgres_runtime_archive_sha256",
                    "postgres_runtime_manifest_sha256",
                )
            )
        ):
            raise CheckpointRuntimeError("checkpoint verifier runtime provenance is invalid")
        for name in (
            "wheel_relative_path",
            "postgres_runtime_archive_relative_path",
            "postgres_runtime_manifest_relative_path",
        ):
            path = Path(contract[name])
            if path.is_absolute() or ".." in path.parts or not 1 <= len(path.parts) <= 8:
                raise CheckpointRuntimeError("checkpoint verifier runtime asset path is unsafe")
        return contract


class KaggleCheckpointRestoreVerifier:
    """Launch a separate protected verifier Notebook for one exact dataset version."""

    def __init__(
        self,
        adapter: KaggleProviderAdapter,
        assets: KaggleCheckpointVerifierAssets,
        *,
        output_directory: Path,
        operation_id: UUID,
        authorization_task_id: UUID,
        clock: Any = lambda: datetime.now(UTC),
        run_id_factory: Any | None = None,
        poll_policy: PollPolicy | None = None,
        metadata_only_output: bool = True,
    ) -> None:
        if not metadata_only_output:
            raise ValueError("checkpoint verifier output must be metadata-only")
        if not output_directory.is_dir() or output_directory.is_symlink():
            raise ValueError("verifier output root must be a real provider-side directory")
        self.adapter = adapter
        self.assets = assets
        self.output_directory = output_directory
        self.operation_id = operation_id
        self.authorization_task_id = authorization_task_id
        self.clock = clock
        self.run_id_factory = run_id_factory
        self.poll_policy = poll_policy or PollPolicy(
            timeout_seconds=CHECKPOINT_VERIFIER_TIMEOUT_SECONDS,
            max_polls=120,
        )
        self.metadata_only_output = metadata_only_output
        if self.poll_policy.timeout_seconds > CHECKPOINT_VERIFIER_TIMEOUT_SECONDS:
            raise ValueError("checkpoint verifier polling exceeds its attempt allocation")

    def verify_restore(
        self,
        *,
        exact_version_ref: str,
        dataset_identity: KaggleDatasetIdentity,
        manifest: CheckpointManifest,
    ) -> dict[str, object]:
        provider_ref, version = _parse_exact_dataset_version(exact_version_ref)
        if provider_ref != dataset_identity.provider_ref or version != dataset_identity.version:
            raise CheckpointRuntimeError("verifier dataset identity differs from exact readback")
        execution = self.assets.execution_contract()
        run_id = (
            UUID(str(self.run_id_factory()))
            if self.run_id_factory is not None
            else uuid5(
                NAMESPACE_URL,
                f"my-data-hub:checkpoint-verifier-run:{manifest.checkpoint_id}:{version}",
            )
        )
        source, execution_pins_sha256 = _render_verifier_source(
            self.assets,
            run_id=run_id,
            dataset_identity=dataset_identity,
            manifest=manifest,
            execution=execution,
        )
        source_sha = executable_source_sha256(source, kernel_type=self.assets.kernel_type)
        dataset_source = f"{provider_ref}/{version}"
        dataset_sources = (execution["runtime_dataset_exact_ref"], dataset_source)
        if len(dataset_sources) != len(set(dataset_sources)):
            raise CheckpointRuntimeError("verifier runtime/checkpoint Dataset inputs must be distinct")
        destination = self.output_directory / str(run_id)
        launch_cache = destination / "control-launch.json"
        if destination.exists() and launch_cache.is_file() and not launch_cache.is_symlink():
            try:
                cached_run = KaggleKernelRunIdentity.model_validate_json(launch_cache.read_bytes())
                if (
                    cached_run.task_run_id != run_id
                    or cached_run.provider_ref != self.assets.notebook_ref
                    or cached_run.source_sha256 != source_sha
                ):
                    raise CheckpointRuntimeError("cached verifier launch identity differs")
                receipt = _load_restore_receipt(destination / CHECKPOINT_RESTORE_RECEIPT_NAME)
                _assert_restore_receipt(
                    receipt,
                    run=cached_run,
                    manifest=manifest,
                    dataset_identity=dataset_identity,
                    execution=execution,
                    execution_pins_sha256=execution_pins_sha256,
                )
                return _verified_restore_receipt(
                    receipt,
                    run=cached_run,
                    receipt_path=destination / CHECKPOINT_RESTORE_RECEIPT_NAME,
                )
            except (CheckpointRuntimeError, ValueError):
                shutil.rmtree(destination, ignore_errors=True)
        intent = ProviderEffectIntent.create(
            operation_id=self.operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"my-data-hub:checkpoint-verifier:{run_id}"),
            idempotency_key=f"checkpoint-verifier:{manifest.checkpoint_id}:{version}:{run_id}",
            task_id=self.authorization_task_id,
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=self.assets.notebook_ref,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": source_sha,
                "dataset_sources": dataset_sources,
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": False,
                "docker_image": execution["runtime_image_identity"],
                "docker_image_pinning_type": "original",
            },
            # The intent is append-only and may be re-submitted after a lost
            # control-plane response.  Bind its timestamp to the immutable
            # checkpoint rather than to the retry attempt.
            requested_at=manifest.created_at,
        )
        try:
            launched = self.adapter.push_private_notebook_pending_runtime_attestation(
                    intent=intent,
                    task_run_id=run_id,
                    source=source,
                    title=self.assets.notebook_ref.split("/", 1)[1],
                    code_file=self.assets.code_file,
                    kernel_type=self.assets.kernel_type,
                    language=self.assets.language,
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                    disposable=False,
                    dataset_sources=dataset_sources,
                    enable_internet=False,
                    timeout_seconds=self.assets.timeout_seconds,
                    docker_image=execution["runtime_image_identity"],
                    docker_image_pinning_type="original",
                )
        except Exception as push_error:
            raise CheckpointRuntimeError(
                "checkpoint verifier pending-attested provider effect was not repeated"
            ) from push_error
        started = monotonic()
        for poll in range(self.poll_policy.max_polls):
            status = self.adapter.read_attested_master_run_status(launched.run)
            if status.state is KernelState.COMPLETE:
                break
            if status.state is KernelState.FAILED:
                raise CheckpointRuntimeError("checkpoint verifier run failed")
            if (
                poll + 1 >= self.poll_policy.max_polls
                or monotonic() - started + self.poll_policy.interval_seconds
                > self.poll_policy.timeout_seconds
            ):
                raise CheckpointRuntimeError("checkpoint verifier exceeded bounded polling")
            sleep(self.poll_policy.interval_seconds)
        receipt: dict[str, object] | None = None
        if destination.exists():
            try:
                receipt = _load_restore_receipt(destination / CHECKPOINT_RESTORE_RECEIPT_NAME)
                _assert_restore_receipt(
                    receipt,
                    run=launched.run,
                    manifest=manifest,
                    dataset_identity=dataset_identity,
                    execution=execution,
                    execution_pins_sha256=execution_pins_sha256,
                )
            except CheckpointRuntimeError:
                # A crash can leave a partial local output tree.  It is only a
                # cache of the exact provider output and is safe to replace.
                shutil.rmtree(destination, ignore_errors=True)
                receipt = None
        if receipt is None:
            output_identity = self.adapter.download_attested_master_output_file(
                launched.run,
                destination=destination,
                file_name=CHECKPOINT_RESTORE_RECEIPT_NAME,
                max_bytes=64 * 1024,
            )
            receipt = _load_restore_receipt(destination / CHECKPOINT_RESTORE_RECEIPT_NAME)
            _assert_restore_receipt(
                receipt,
                run=launched.run,
                manifest=manifest,
                dataset_identity=dataset_identity,
                execution=execution,
                execution_pins_sha256=execution_pins_sha256,
            )
            output_tree_sha256 = _receipt_output_tree_sha256(
                destination / CHECKPOINT_RESTORE_RECEIPT_NAME
            )
            if output_identity.output_tree_sha256 != output_tree_sha256:
                raise CheckpointRuntimeError("verifier provider output tree differs from exact receipt")
        else:
            output_tree_sha256 = _receipt_output_tree_sha256(
                destination / CHECKPOINT_RESTORE_RECEIPT_NAME
            )
        launch_cache.write_text(launched.run.model_dump_json(), encoding="utf-8")
        launch_cache.chmod(0o600)
        return _verified_restore_receipt(
            receipt,
            run=launched.run,
            receipt_path=destination / CHECKPOINT_RESTORE_RECEIPT_NAME,
            output_tree_sha256=output_tree_sha256,
        )


class KaggleCheckpointCoordinator:
    """Execute upload/readback/independent-restore/CAS promotion provider-side."""

    def __init__(
        self,
        *,
        registry: CheckpointRegistryContract,
        provider: KaggleCheckpointDatasetProvider,
        restore_verifier: KaggleCheckpointRestoreVerifier,
    ) -> None:
        self.registry = registry
        self.provider = provider
        self.restore_verifier = restore_verifier

    def publish(
        self,
        *,
        package: Path,
        manifest_path: Path,
        readback_directory: Path,
    ) -> PublishReceipt:
        _assert_kaggle_working_path(package)
        _assert_kaggle_working_path(readback_directory)
        manifest = load_and_verify(manifest_path, package)
        initial_head = self.registry.head
        self.registry.add_candidate(manifest)
        exact_ref = ""
        upload_seconds = 0.0
        readback_seconds = 0.0
        restore_seconds = 0.0
        restore_receipt: dict[str, object] = {}
        try:
            started = monotonic()
            exact_ref = self.provider.upload_candidate(package, manifest)
            upload_seconds = monotonic() - started
            self.registry.uploaded(manifest.checkpoint_id, exact_ref)
            package_recorder = getattr(self.registry, "package_uploaded", None)
            if callable(package_recorder):
                package_recorder(manifest.checkpoint_id, tree_sha256(package))

            started = monotonic()
            readback = self.provider.exact_readback(exact_ref, readback_directory)
            readback_manifest = load_and_verify(
                readback.package / manifest_path.name,
                readback.package,
            )
            if readback_manifest.manifest_sha256 != manifest.manifest_sha256:
                raise PublishError("exact readback manifest differs from candidate")
            readback_seconds = monotonic() - started
            self.registry.readback_verified(manifest.checkpoint_id)

            started = monotonic()
            restore_receipt = self.restore_verifier.verify_restore(
                exact_version_ref=exact_ref,
                dataset_identity=readback.identity,
                manifest=readback_manifest,
            )
            if restore_receipt.get("ok") is not True:
                raise PublishError("independent verifier Notebook did not pass")
            restore_seconds = monotonic() - started
            self.registry.restore_verified(manifest.checkpoint_id)
            head = self.registry.promote(
                manifest.checkpoint_id,
                expected_generation=initial_head.generation,
            )
        except Exception as exc:
            durable_head: CheckpointHead | None = None
            with suppress(Exception):
                durable_head = self.registry.head
            if durable_head is not None and durable_head.current == manifest.checkpoint_id and exact_ref:
                return self._receipt(
                    manifest=manifest,
                    exact_ref=exact_ref,
                    head=durable_head,
                    upload_seconds=upload_seconds,
                    readback_seconds=readback_seconds,
                    restore_seconds=restore_seconds,
                    restore_receipt=restore_receipt,
                )
            if isinstance(exc, PublishError) and not isinstance(exc, CheckpointRetryableError):
                with suppress(Exception):
                    self.registry.reject(manifest.checkpoint_id, type(exc).__name__)
                raise
            raise CheckpointRetryableError(
                "checkpoint candidate is incomplete and must resume with the same identity"
            ) from exc
        return self._receipt(
            manifest=manifest,
            exact_ref=exact_ref,
            head=head,
            upload_seconds=upload_seconds,
            readback_seconds=readback_seconds,
            restore_seconds=restore_seconds,
            restore_receipt=restore_receipt,
        )

    def reconcile_promoted(
        self,
        *,
        package: Path,
        manifest_path: Path,
    ) -> PublishReceipt | None:
        """Recover a promotion whose success response was not observed.

        Only the remote exact HEAD carries enough durable metadata to prove
        this.  The marker in ``restore_receipt`` is reconciliation evidence,
        not a fabricated copy of the verifier Notebook's typed receipt.
        """

        resolver = getattr(self.registry, "resolve_head", None)
        if not callable(resolver):
            return None
        manifest = load_and_verify(manifest_path, package)
        snapshot = resolver()
        current = snapshot.current
        if (
            current is None
            or current.checkpoint_id != manifest.checkpoint_id
            or current.dataset_ref != self.provider.dataset_ref
            or current.manifest_sha256 != manifest.manifest_sha256
        ):
            return None
        return PublishReceipt(
            checkpoint_id=str(manifest.checkpoint_id),
            exact_version_ref=current.exact_version_ref,
            manifest_sha256=manifest.manifest_sha256,
            current_checkpoint_id=str(current.checkpoint_id),
            previous_checkpoint_id=(str(snapshot.previous.checkpoint_id) if snapshot.previous is not None else None),
            upload_seconds=0.0,
            readback_seconds=0.0,
            restore_seconds=0.0,
            package_bytes=sum(item.byte_size for item in manifest.files),
            restore_receipt={
                "reconciled_from_durable_verified_head": True,
                "checkpoint_id": str(manifest.checkpoint_id),
                "manifest_sha256": manifest.manifest_sha256,
                "head_generation": snapshot.generation,
            },
        )

    @staticmethod
    def _receipt(
        *,
        manifest: CheckpointManifest,
        exact_ref: str,
        head: CheckpointHead,
        upload_seconds: float,
        readback_seconds: float,
        restore_seconds: float,
        restore_receipt: dict[str, object],
    ) -> PublishReceipt:
        return PublishReceipt(
            checkpoint_id=str(manifest.checkpoint_id),
            exact_version_ref=exact_ref,
            manifest_sha256=manifest.manifest_sha256,
            current_checkpoint_id=str(head.current),
            previous_checkpoint_id=str(head.previous) if head.previous else None,
            upload_seconds=upload_seconds,
            readback_seconds=readback_seconds,
            restore_seconds=restore_seconds,
            package_bytes=sum(item.byte_size for item in manifest.files),
            restore_receipt=restore_receipt,
        )


class RuntimeCheckpointCoordinator:
    """Composite matching ``master_runtime.RuntimeCheckpointCoordinator``.

    One call probes the ACTIVE PostgreSQL session, creates physical/logical
    archives below ``/kaggle/working``, builds the immutable manifest, and runs
    the full Kaggle upload/readback/verifier/CAS promotion sequence.
    """

    def __init__(
        self,
        *,
        builder: KaggleCheckpointCandidateBuilder,
        coordinator: Any,
        probe_relations: tuple[str, ...],
        source_identity: str,
        claim_source: CurrentResourceClaimSource | None = None,
        connect: Any | None = None,
        clock: Any = lambda: datetime.now(UTC),
        checkpoint_id_factory: Any | None = None,
        timeout_seconds: int = CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS,
    ) -> None:
        if not probe_relations or len(probe_relations) > 100:
            raise ValueError("runtime checkpoint probe relation set is invalid")
        if not source_identity:
            raise ValueError("runtime checkpoint source identity is required")
        if not 60 <= timeout_seconds <= CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS:
            raise ValueError("checkpoint archive timeout exceeds its attempt allocation")
        if connect is None:
            import psycopg

            connect = psycopg.connect
        self.builder = builder
        self.coordinator = coordinator
        self.probe_relations = probe_relations
        self.source_identity = source_identity
        self.claim_source = claim_source
        self.connect = connect
        self.clock = clock
        self.checkpoint_id_factory = checkpoint_id_factory
        self.timeout_seconds = timeout_seconds
        self._pending_checkpoint_id: UUID | None = None
        self._pending_created_at: datetime | None = None

    def resolve_boot_checkpoint(self, destination: Path) -> Path | None:
        """Download only the exact numeric verified HEAD for master bootstrap."""

        _assert_kaggle_working_path(destination)
        registry = self.coordinator.registry
        if not isinstance(registry, RemoteControlCheckpointRegistry):
            raise CheckpointRuntimeError("production boot requires the remote durable checkpoint registry")
        snapshot = registry.resolve_head()
        if snapshot.current is None:
            return None
        if destination.exists():
            raise CheckpointRuntimeError("boot checkpoint destination must be absent")
        readback = self.coordinator.provider.exact_head_readback(snapshot.current, destination)
        return readback.package

    def create_and_publish(
        self,
        *,
        database_url: str,
        package_directory: Path,
        identity: Any,
    ) -> PublishReceipt:
        _assert_kaggle_working_path(package_directory)
        head = self.coordinator.registry.head
        provider = self.coordinator.provider
        if head.current is not None and not getattr(provider, "brokered_upload", False):
            if self.claim_source is None:
                raise CheckpointRuntimeError("existing checkpoint dataset requires its durable exact claim")
            provider.claim = self.claim_source.current_resource_claim(
                provider_ref=provider.dataset_ref,
                kind=ProviderKind.DATASET,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            )
            provider.resource_task_id = provider.claim.task_id
        else:
            provider.claim = None
        checkpoint_id = self._pending_checkpoint_id or self._derive_checkpoint_id(
            identity=identity,
            parent_checkpoint_id=head.current,
        )
        self._pending_checkpoint_id = checkpoint_id
        self._pending_created_at = self._pending_created_at or self.clock()
        package = package_directory / str(checkpoint_id)
        readback = package_directory / f"readback-{checkpoint_id}"
        provider_started = package_directory / f".provider-started-{checkpoint_id}"
        try:
            manifest_path = package / CHECKPOINT_MANIFEST_NAME
            if head.current == checkpoint_id:
                recovered = self.coordinator.reconcile_promoted(
                    package=package,
                    manifest_path=manifest_path,
                )
                if recovered is None:
                    raise CheckpointRuntimeError(
                        "pending checkpoint is HEAD but its exact durable metadata cannot be reconciled"
                    )
                self._pending_checkpoint_id = None
                self._pending_created_at = None
                return recovered
            manifest: CheckpointManifest | None = None
            if package.exists() or package.is_symlink():
                if package.is_symlink() or not package.is_dir():
                    raise CheckpointRuntimeError("pending checkpoint package is not a real directory")
                try:
                    manifest = load_and_verify(manifest_path, package)
                except Exception as invalid_package:
                    if provider_started.exists() or provider_started.is_symlink():
                        raise CheckpointRuntimeError(
                            "provider-started checkpoint package is invalid and cannot be rebuilt"
                        ) from invalid_package
                    # No provider effect was allowed to start for this local
                    # candidate.  A failed ArchiveCreator/build may therefore
                    # be retried under the same deterministic identity.
                    shutil.rmtree(package)
            elif provider_started.exists() or provider_started.is_symlink():
                raise CheckpointRuntimeError("provider-started checkpoint package is absent and cannot be rebuilt")
            if manifest is None:
                with self.connect(database_url, connect_timeout=15) as connection:
                    postgres_version, pgvector_version, checkpoint_lsn = _postgres_checkpoint_identity(connection)
                    build_identity = CheckpointBuildIdentity(
                        checkpoint_id=checkpoint_id,
                        master_instance_id=UUID(str(identity.master_instance_id)),
                        epoch=int(identity.epoch),
                        parent_checkpoint_id=head.current,
                        postgres_version=postgres_version,
                        pgvector_version=pgvector_version,
                        source_run_id=str(identity.run_id),
                        source_identity=self.source_identity,
                        created_at=self._pending_created_at,
                        checkpoint_lsn=checkpoint_lsn,
                        probe_relations=self.probe_relations,
                    )
                    built_package, built_manifest_path, _built_manifest = self.builder.build(
                        database_url=database_url,
                        connection=connection,
                        package_directory=package,
                        identity=build_identity,
                        timeout_seconds=self.timeout_seconds,
                    )
                    if built_package != package or built_manifest_path != manifest_path:
                        raise CheckpointRuntimeError("checkpoint builder returned a different package identity")
                    manifest = load_and_verify(manifest_path, package)
            if (
                manifest.checkpoint_id != checkpoint_id
                or manifest.parent_checkpoint_id != head.current
                or manifest.master_instance_id != UUID(str(identity.master_instance_id))
                or manifest.epoch != int(identity.epoch)
                or manifest.source_run_id != str(identity.run_id)
                or manifest.source_identity != self.source_identity
            ):
                raise CheckpointRuntimeError("pending checkpoint package identity changed across retry")
            _mark_provider_started(provider_started)
            receipt = self.coordinator.publish(
                package=package,
                manifest_path=manifest_path,
                readback_directory=readback,
            )
            # A completed publication is no longer retry state.  A later
            # checkpoint request derives a new identity from the advanced
            # durable parent HEAD.
            self._pending_checkpoint_id = None
            self._pending_created_at = None
            return receipt
        finally:
            shutil.rmtree(readback, ignore_errors=True)

    def _derive_checkpoint_id(
        self,
        *,
        identity: Any,
        parent_checkpoint_id: UUID | None,
    ) -> UUID:
        if self.checkpoint_id_factory is not None:
            return UUID(str(self.checkpoint_id_factory()))
        return uuid5(
            NAMESPACE_URL,
            (
                "my-data-hub:checkpoint-candidate:"
                f"{self.coordinator.provider.operation_id}:"
                f"{identity.master_instance_id}:{identity.run_id}:{identity.epoch}:"
                f"{parent_checkpoint_id or 'empty'}"
            ),
        )


def build_runtime_checkpoint_coordinator_from_environment(
    *,
    identity: Any,
    attempt_id: UUID,
    postgres_bin: Path,
) -> RuntimeCheckpointCoordinator:
    """Build the production master checkpoint composite from exact runtime inputs."""

    _reject_notebook_kaggle_credentials()
    run_id = UUID(str(identity.run_id))
    master_instance_id = UUID(str(identity.master_instance_id))
    operation_id = UUID(_required_environment("MY_DATA_HUB_OPERATION_ID"))
    control_client = AuthenticatedControlPlaneClient(
        base_url=_required_environment("MY_DATA_HUB_CONTROL_PLANE_URL"),
        bearer_token=_required_environment("MY_DATA_HUB_RUN_SECRET"),
        runtime_identity=ControlPlaneRuntimeIdentity(
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=int(identity.epoch),
        ),
    )
    dataset_ref = _exact_owner_slug(
        _required_environment("MY_DATA_HUB_CHECKPOINT_DATASET_REF"),
        "checkpoint dataset",
    )
    registry = RemoteControlCheckpointRegistry(
        control_client,
        operation_id=str(operation_id),
        dataset_ref=dataset_ref,
    )
    from .brokered_upload import (
        BrokeredCheckpointRuntimeCoordinator,
        BrokeredCheckpointRuntimeProvider,
    )

    provider = BrokeredCheckpointRuntimeProvider(
        control_client,
        dataset_ref=dataset_ref,
        operation_id=operation_id,
    )
    try:
        raw_relations = json.loads(_required_environment("MY_DATA_HUB_CHECKPOINT_PROBE_RELATIONS_JSON"))
    except json.JSONDecodeError as exc:
        raise CheckpointRuntimeError("checkpoint probe relation contract is invalid JSON") from exc
    if (
        not isinstance(raw_relations, list)
        or not raw_relations
        or len(raw_relations) > 100
        or any(not isinstance(item, str) for item in raw_relations)
    ):
        raise CheckpointRuntimeError("checkpoint probe relation contract is invalid")
    probe_relations = tuple(raw_relations)
    publisher = BrokeredCheckpointRuntimeCoordinator(registry, provider)
    pg_basebackup = postgres_bin / "pg_basebackup"
    pg_dump = postgres_bin / "pg_dump"
    if any(
        not tool.is_file() or tool.is_symlink() or not os.access(tool, os.X_OK) for tool in (pg_basebackup, pg_dump)
    ):
        raise CheckpointRuntimeError("exact PostgreSQL checkpoint tools are unavailable")
    return RuntimeCheckpointCoordinator(
        builder=KaggleCheckpointCandidateBuilder(
            ArchiveCreator(BackupTools(pg_basebackup=pg_basebackup, pg_dump=pg_dump))
        ),
        coordinator=publisher,
        probe_relations=probe_relations,
        source_identity=_required_environment("MY_DATA_HUB_SOURCE_IDENTITY"),
        claim_source=None,
        timeout_seconds=CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS,
    )


def _reject_notebook_kaggle_credentials() -> None:
    """Keep all account-level Kaggle authority in the central adapter.

    A signed, one-file blob upload URL is the only provider capability the
    broker may return to the master.  Ambient SDK credentials would silently
    turn the Notebook into a second lifecycle client, so fail before creating
    a checkpoint or contacting the control plane.
    """

    # Kaggle itself exposes the account name as non-secret runtime metadata.
    # A username alone cannot authenticate the SDK and must not make every
    # official Notebook fail before bootstrap.  Reject every secret-bearing
    # half of the supported credential modes (and credential files) instead.
    forbidden_environment = (
        "KAGGLE_KEY",
        "KAGGLE_API_TOKEN",
        "KAGGLE_API_V1_TOKEN",
        "KAGGLE_ACCESS_TOKEN",
    )
    present_environment = sorted(
        name for name in forbidden_environment if os.environ.get(name, "").strip()
    )
    if present_environment:
        # Names are public contract identifiers; values remain undisclosed.
        raise CheckpointRuntimeError(
            "Kaggle lifecycle credential environment is forbidden in the master Notebook: "
            + ",".join(present_environment)
        )
    home = Path(os.environ.get("HOME", "~")).expanduser()
    forbidden_files = (home / ".kaggle" / "kaggle.json", home / ".kaggle" / "access_token")
    if any(path.exists() for path in forbidden_files):
        raise CheckpointRuntimeError("Kaggle lifecycle credential files are forbidden in the master Notebook")


def _required_environment(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise CheckpointRuntimeError(f"required checkpoint runtime value is absent: {name}")
    return value


def _exact_owner_slug(value: str, label: str) -> str:
    parts = value.split("/")
    if len(parts) != 2 or not all(parts):
        raise CheckpointRuntimeError(f"{label} ref must be exact owner/slug")
    return value


def _postgres_checkpoint_identity(connection: Any) -> tuple[str, str, str]:
    with connection.cursor() as cursor:
        postgres_version = str(cursor.execute("SHOW server_version").fetchone()[0])
        row = cursor.execute("SELECT extversion FROM pg_extension WHERE extname='vector'").fetchone()
        if row is None or not str(row[0]):
            raise CheckpointRuntimeError("pgvector extension version is unavailable")
        pgvector_version = str(row[0])
        checkpoint_lsn = str(cursor.execute("SELECT pg_current_wal_lsn()::text").fetchone()[0])
    if not postgres_version.startswith("18."):
        raise CheckpointRuntimeError("runtime checkpoint requires PostgreSQL 18")
    if not re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", checkpoint_lsn):
        raise CheckpointRuntimeError("runtime checkpoint LSN is invalid")
    return postgres_version, pgvector_version, checkpoint_lsn


def _parse_exact_dataset_version(value: str) -> tuple[str, int]:
    parts = value.split("/")
    if len(parts) != 3 or not parts[0] or not parts[1] or not parts[2].isdigit():
        raise CheckpointRuntimeError("checkpoint dataset ref must be exact owner/slug/version")
    version = int(parts[2])
    if version < 1:
        raise CheckpointRuntimeError("checkpoint dataset version must be positive")
    return "/".join(parts[:2]), version


def _exact_checkpoint_reference(value: object) -> ExactCheckpointReference | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) != {
        "checkpoint_id",
        "dataset_ref",
        "exact_version_ref",
        "manifest_sha256",
    }:
        raise ValueError("checkpoint HEAD entry shape differs")
    return ExactCheckpointReference(
        checkpoint_id=UUID(str(value["checkpoint_id"])),
        dataset_ref=str(value["dataset_ref"]),
        exact_version_ref=str(value["exact_version_ref"]),
        manifest_sha256=str(value["manifest_sha256"]),
    )


def _assert_kaggle_working_path(path: Path) -> None:
    if not path.is_absolute() or path.is_symlink():
        raise CheckpointRuntimeError("checkpoint bytes require an absolute non-symlink Kaggle working path")
    resolved = path.resolve()
    root = _KAGGLE_WORKING_ROOT.resolve()
    if resolved == root or root not in resolved.parents:
        raise CheckpointRuntimeError("checkpoint bytes may exist only below /kaggle/working")


def _mark_provider_started(path: Path) -> None:
    """Create a fail-closed local fence before any provider mutation may run."""

    _assert_kaggle_working_path(path)
    if path.is_symlink() or (path.exists() and not path.is_file()):
        raise CheckpointRuntimeError("checkpoint provider-started fence is unsafe")
    if path.exists():
        return
    try:
        with path.open("xb"):
            pass
    except FileExistsError:
        if path.is_symlink() or not path.is_file():
            raise CheckpointRuntimeError("checkpoint provider-started fence raced unsafely") from None
    path.chmod(0o600)


def _render_verifier_source(
    assets: KaggleCheckpointVerifierAssets,
    *,
    run_id: UUID,
    dataset_identity: KaggleDatasetIdentity,
    manifest: CheckpointManifest,
    execution: dict[str, str],
) -> tuple[bytes, str]:
    checkpoint_ref = f"{dataset_identity.provider_ref}/{dataset_identity.version}"
    if assets.kernel_type == "script":
        primary_source_sha256 = hashlib.sha256(assets.notebook_source).hexdigest()
        pin_contract = {
            "schema": "my-data-hub-notebook-execution-pins/v1",
            "notebook": "03-checkpoint-verifier-restore-smoke",
            "output_contract": RESTORE_RECEIPT_CONTRACT,
            "model": None,
            "privacy": "private",
            "resource_class": "orchestrator_protected",
            "cleanup_retention_policy": {
                "cleanup_receipt_required": True,
                "notebook_resource": "orchestrator_protected_until_owner_supersedes",
                "run_outputs": "retain_until_terminal_receipt_then_control_policy",
                "task_owned_inputs": "claim_bound_delete_after_terminal_or_expiry",
            },
        }
    else:
        try:
            notebook_document = json.loads(assets.notebook_source)
            primary_body = Path(__file__).with_name("verifier_runtime.py").read_bytes()
            primary_sha256 = hashlib.sha256(primary_body).hexdigest()
            primary_text = primary_body.decode("utf-8")
            cells = notebook_document["cells"]
            primary_cells = [cell for cell in cells if cell.get("id") == "primary-source"]
            install_cells = [cell for cell in cells if cell.get("id") == "install-exact-wheel"]
            if len(primary_cells) != 1 or len(install_cells) != 1:
                raise CheckpointRuntimeError("verifier notebook operational cells differ")
            old_primary = str(notebook_document["metadata"]["my_data_hub"]["primary_source_sha256"])
            primary_cells[0]["source"] = (
                f"PRIMARY_SOURCE = {primary_text!r}\n"
                "if hashlib.sha256(PRIMARY_SOURCE.encode()).hexdigest() != EXPECTED_SOURCE_SHA256:\n"
                "    raise RuntimeError('embedded primary source hash mismatch')\n"
                "exec(compile(PRIMARY_SOURCE, '<my-data-hub-primary-source>', 'exec'), globals())"
            )
            install_cells[0]["source"] = str(install_cells[0]["source"]).replace(
                old_primary, primary_sha256
            ).replace(
                "my-data-hub-checkpoint-restore-smoke.v1", RESTORE_RECEIPT_CONTRACT
            )
            notebook_metadata = notebook_document["metadata"]["my_data_hub"]
            notebook_metadata["primary_source_sha256"] = primary_sha256
            notebook_metadata["runtime_contract"] = RESTORE_RECEIPT_CONTRACT
            notebook_metadata["execution_pin_contract"]["output_contract"] = RESTORE_RECEIPT_CONTRACT
            pin_contract = notebook_metadata["execution_pin_contract"]
            primary_source_sha256 = primary_sha256
        except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise CheckpointRuntimeError("verifier notebook execution pin contract is absent") from exc
    pins = {
        "schema": pin_contract["schema"],
        "notebook": pin_contract["notebook"],
        "python_series": execution["runtime_python_series"],
        "image_source_commit": execution["runtime_image_source_commit"],
        "kaggle_runtime_image_identity": execution["runtime_image_identity"],
        "input_dataset_versions": [execution["runtime_dataset_exact_ref"], checkpoint_ref],
        "immutable_asset_sha256s": {
            "my_data_hub_wheel_sha256": execution["wheel_sha256"],
            "primary_source_sha256": primary_source_sha256,
        },
        "output_contract": pin_contract["output_contract"],
        "model": pin_contract["model"],
        "privacy": pin_contract["privacy"],
        "resource_class": pin_contract["resource_class"],
        "cleanup_retention_policy": pin_contract["cleanup_retention_policy"],
    }
    values = {
        "MY_DATA_HUB_VERIFIER_TASK_RUN_ID": str(run_id),
        "MY_DATA_HUB_CHECKPOINT_DATASET_REF": dataset_identity.provider_ref,
        "MY_DATA_HUB_CHECKPOINT_DATASET_VERSION": str(dataset_identity.version),
        "MY_DATA_HUB_CHECKPOINT_PACKAGE_SHA256": dataset_identity.package_sha256,
        "MY_DATA_HUB_CHECKPOINT_ID": str(manifest.checkpoint_id),
        "MY_DATA_HUB_CHECKPOINT_MANIFEST_SHA256": manifest.manifest_sha256,
        "MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY": execution["runtime_image_identity"],
        "MY_DATA_HUB_KAGGLE_RUNTIME_SOURCE_COMMIT": execution["runtime_image_source_commit"],
        "MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON": json.dumps(
            pins["input_dataset_versions"], separators=(",", ":")
        ),
    }
    pins_body = json.dumps(pins, sort_keys=True, separators=(",", ":")).encode()
    runtime_names = {
        "wheel": execution["wheel_relative_path"],
        "archive": execution["postgres_runtime_archive_relative_path"],
        "manifest": execution["postgres_runtime_manifest_relative_path"],
    }
    runtime_hashes = {
        "wheel": execution["wheel_sha256"],
        "archive": execution["postgres_runtime_archive_sha256"],
        "manifest": execution["postgres_runtime_manifest_sha256"],
    }
    bootstrap = (
        "import hashlib as _mdh_hashlib, json as _mdh_json, os as _mdh_os, "
        "pathlib as _mdh_pathlib, platform as _mdh_platform\n"
        f"_mdh_values = {json.dumps(values, sort_keys=True)}\n"
        f"_mdh_pins_body = {pins_body!r}\n"
        f"_mdh_runtime_names = {runtime_names!r}\n"
        f"_mdh_runtime_hashes = {runtime_hashes!r}\n"
        "_mdh_input = _mdh_pathlib.Path('/kaggle/input')\n"
        "if not _mdh_input.is_dir() or _mdh_input.is_symlink(): "
        "raise RuntimeError('Kaggle input root is unavailable')\n"
        "_mdh_files=[]; _mdh_total=0\n"
        "for _mdh_index,_mdh_path in enumerate(_mdh_input.rglob('*')):\n"
        "    if _mdh_index >= 4096: raise RuntimeError('Kaggle input inventory exceeds bound')\n"
        "    _mdh_relative=_mdh_path.relative_to(_mdh_input)\n"
        "    if _mdh_path.is_symlink() or any((_mdh_input.joinpath(*_mdh_relative.parts[:i])).is_symlink() "
        "for i in range(1,len(_mdh_relative.parts))): raise RuntimeError('Kaggle input contains a symlink')\n"
        "    if _mdh_path.is_file():\n"
        "        _mdh_size=_mdh_path.stat().st_size\n"
        "        if _mdh_size > 536870912: raise RuntimeError('Kaggle input file exceeds bound')\n"
        "        _mdh_total += _mdh_size; _mdh_files.append(_mdh_path)\n"
        "if _mdh_total > 1073741824: raise RuntimeError('Kaggle input bytes exceed bound')\n"
        "def _mdh_file_sha(path):\n"
        "    digest = _mdh_hashlib.sha256()\n"
        "    with path.open('rb') as stream:\n"
        "        while block := stream.read(1048576): digest.update(block)\n"
        "    return digest.hexdigest()\n"
        "_mdh_runtime_files={}\n"
        "for _mdh_key,_mdh_name in _mdh_runtime_names.items():\n"
        "    _mdh_matches=[path for path in _mdh_files if path.name == _mdh_pathlib.Path(_mdh_name).name "
        "and _mdh_file_sha(path) == _mdh_runtime_hashes[_mdh_key]]\n"
        "    if len(_mdh_matches) != 1: raise RuntimeError('exact runtime Dataset file is absent or ambiguous')\n"
        "    _mdh_runtime_files[_mdh_key]=_mdh_matches[0]\n"
        "_mdh_runtime_mounts={_mdh_input/path.relative_to(_mdh_input).parts[0] "
        "for path in _mdh_runtime_files.values()}\n"
        "if len(_mdh_runtime_mounts) != 1: raise RuntimeError('runtime Dataset file set differs')\n"
        "_mdh_runtime_root=next(iter(_mdh_runtime_mounts))\n"
        f"_mdh_checkpoint_manifests=[path for path in _mdh_files if path.name == {CHECKPOINT_MANIFEST_NAME!r}]\n"
        "_mdh_checkpoint_matches=[]\n"
        "for _mdh_checkpoint_manifest in _mdh_checkpoint_manifests:\n"
        "    try: _mdh_checkpoint_payload=_mdh_json.loads(_mdh_checkpoint_manifest.read_bytes())\n"
        "    except (OSError,ValueError): continue\n"
        "    if _mdh_checkpoint_payload.get('manifest_sha256') == "
        "_mdh_values['MY_DATA_HUB_CHECKPOINT_MANIFEST_SHA256']:\n"
        "        _mdh_checkpoint_matches.append(_mdh_checkpoint_manifest.parent)\n"
        "if len(_mdh_checkpoint_matches) != 1: raise RuntimeError('exact checkpoint Dataset mount is ambiguous')\n"
        "_mdh_checkpoint_root=_mdh_checkpoint_matches[0]\n"
        "if _mdh_input/_mdh_checkpoint_root.relative_to(_mdh_input).parts[0] == _mdh_runtime_root:\n"
        "    raise RuntimeError('runtime and checkpoint claims resolve to one mount')\n"
        f"_mdh_values['MY_DATA_HUB_CHECKPOINT_DIRECTORY'] = str(_mdh_checkpoint_root)\n"
        f"_mdh_values['MY_DATA_HUB_CHECKPOINT_MANIFEST'] = str(_mdh_checkpoint_root / {CHECKPOINT_MANIFEST_NAME!r})\n"
        "_mdh_values['MY_DATA_HUB_WHEEL_PATH'] = str(_mdh_runtime_files['wheel'])\n"
        "_mdh_values['MY_DATA_HUB_WHEEL_SHA256'] = _mdh_runtime_hashes['wheel']\n"
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE'] = str(_mdh_runtime_files['archive'])\n"
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_ARCHIVE_SHA256'] = _mdh_runtime_hashes['archive']\n"
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST'] = str(_mdh_runtime_files['manifest'])\n"
        "_mdh_values['MY_DATA_HUB_POSTGRES_RUNTIME_MANIFEST_SHA256'] = _mdh_runtime_hashes['manifest']\n"
        "if _mdh_platform.python_version().split('.')[:2] != "
        "_mdh_json.loads(_mdh_pins_body)['python_series'].split('.'):\n"
        "    raise RuntimeError('checkpoint verifier Python series differs')\n"
        "if _mdh_pathlib.Path('/etc/git_commit').read_text().strip() != "
        "_mdh_json.loads(_mdh_pins_body)['image_source_commit']:\n"
        "    raise RuntimeError('checkpoint verifier image source commit differs')\n"
        "_mdh_pins_path = _mdh_pathlib.Path('/kaggle/working/checkpoint-verifier-execution-pins.json')\n"
        "_mdh_pins_path.write_bytes(_mdh_pins_body); _mdh_pins_path.chmod(0o600)\n"
        "_mdh_values['MY_DATA_HUB_EXECUTION_PINS_PATH'] = str(_mdh_pins_path)\n"
        "_mdh_values['MY_DATA_HUB_EXECUTION_PINS_SHA256'] = _mdh_hashlib.sha256(_mdh_pins_body).hexdigest()\n"
        "_mdh_values['MY_DATA_HUB_NOTEBOOK_IS_PRIVATE'] = 'true'\n"
        "_mdh_os.environ.update(_mdh_values)\n"
    )
    pins_sha256 = hashlib.sha256(pins_body).hexdigest()
    if assets.kernel_type == "script":
        return bootstrap.encode() + assets.notebook_source, pins_sha256
    try:
        body = notebook_document
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointRuntimeError("verifier notebook source is invalid JSON") from exc
    if not isinstance(body, dict) or not isinstance(body.get("cells"), list):
        raise CheckpointRuntimeError("verifier notebook source lacks cells")
    body["cells"].insert(
        0,
        {
            "cell_type": "code",
            "execution_count": None,
            "id": "my-data-hub-checkpoint-identity",
            "metadata": {},
            "outputs": [],
            "source": bootstrap,
        },
    )
    return json.dumps(body).encode(), pins_sha256


def _load_restore_receipt(path: Path) -> dict[str, object]:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > 64 * 1024:
        raise CheckpointRuntimeError("verifier output lacks a bounded restore receipt")
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointRuntimeError("verifier restore receipt is invalid JSON") from exc
    try:
        receipt = CheckpointRestoreReceipt.model_validate(value)
    except ValueError as exc:
        raise CheckpointRuntimeError("verifier restore receipt differs from the typed contract") from exc
    rendered = receipt.model_dump(mode="json")
    if path.read_bytes() != canonical_json_bytes(rendered):
        raise CheckpointRuntimeError("verifier restore receipt is not canonical JSON")
    return rendered


def _receipt_output_tree_sha256(path: Path) -> str:
    body = path.read_bytes()
    return sha256_value(
        {
            "files": [
                {
                    "path": CHECKPOINT_RESTORE_RECEIPT_NAME,
                    "byte_size": len(body),
                    "sha256": hashlib.sha256(body).hexdigest(),
                }
            ]
        }
    )


def _verified_restore_receipt(
    receipt: dict[str, object],
    *,
    run: KaggleKernelRunIdentity,
    receipt_path: Path,
    output_tree_sha256: str | None = None,
) -> dict[str, object]:
    body = receipt_path.read_bytes()
    verified = CheckpointRestoreVerifiedReceipt.model_validate(
        {
            **receipt,
            "provider_run_ref": run.provider_run_ref,
            "source_sha256": run.source_sha256,
            "output_receipt_sha256": hashlib.sha256(body).hexdigest(),
            "output_tree_sha256": output_tree_sha256 or _receipt_output_tree_sha256(receipt_path),
        }
    )
    return verified.model_dump(mode="json")


def _assert_restore_receipt(
    receipt: dict[str, object],
    *,
    run: KaggleKernelRunIdentity,
    manifest: CheckpointManifest,
    dataset_identity: KaggleDatasetIdentity,
    execution: dict[str, str],
    execution_pins_sha256: str,
) -> None:
    expected = {
        "schema_version": RESTORE_RECEIPT_CONTRACT,
        "task_run_id": str(run.task_run_id),
        "checkpoint_id": str(manifest.checkpoint_id),
        "manifest_sha256": manifest.manifest_sha256,
        "manifest_file_sha256": hashlib.sha256(
            canonical_json_bytes(manifest.payload()) + b"\n"
        ).hexdigest(),
        "dataset_ref": dataset_identity.provider_ref,
        "dataset_version": dataset_identity.version,
        "package_sha256": dataset_identity.package_sha256,
        "restore_mode": "isolated_physical_restore",
        "execution_pins_sha256": execution_pins_sha256,
        "runtime_image_identity": execution["runtime_image_identity"],
        "runtime_image_source_commit": execution["runtime_image_source_commit"],
        "input_dataset_versions": [
            execution["runtime_dataset_exact_ref"],
            f"{dataset_identity.provider_ref}/{dataset_identity.version}",
        ],
        "ok": True,
    }
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise CheckpointRuntimeError("verifier restore receipt identity differs from the exact run/checkpoint")
    observed = receipt.get("observed")
    if not isinstance(observed, dict):
        raise CheckpointRuntimeError("verifier restore receipt lacks observed probe metadata")
    required_receipt_keys = {
        "schema_version", "task_run_id", "checkpoint_id", "manifest_sha256",
        "manifest_file_sha256", "dataset_ref", "dataset_version", "package_sha256",
        "restore_mode", "execution_pins_sha256", "runtime_image_identity",
        "runtime_image_source_commit", "input_dataset_versions", "ok", "observed",
    }
    if set(receipt) != required_receipt_keys or not re.fullmatch(
        r"[a-f0-9]{64}", str(receipt.get("execution_pins_sha256", ""))
    ):
        raise CheckpointRuntimeError("verifier restore receipt shape/pins differ")
    expected_probe = manifest.restore_probe
    if observed.get("schema_version") != expected_probe.schema_version:
        raise CheckpointRuntimeError("verifier restore schema version differs")
    if observed.get("canonical_revision") != expected_probe.canonical_revision:
        raise CheckpointRuntimeError("verifier restore canonical revision differs")
    if observed.get("logical_hash_sha256") != expected_probe.logical_hash_sha256:
        raise CheckpointRuntimeError("verifier restore logical hash differs")
    if observed.get("row_counts") != expected_probe.row_counts:
        raise CheckpointRuntimeError("verifier restore row counts differ")
    expected_observed_keys = {
        "schema_version", "canonical_revision", "logical_hash_sha256", "row_counts",
        "postgres_version", "extensions", "migration_boundary", "database_invariants",
        "vector_query", "bounded_read_smoke",
    }
    if set(observed) != expected_observed_keys:
        raise CheckpointRuntimeError("verifier restore probe receipt shape differs")
    extensions = observed.get("extensions")
    migration = observed.get("migration_boundary")
    invariants = observed.get("database_invariants")
    vector = observed.get("vector_query")
    smoke = observed.get("bounded_read_smoke")
    if (
        not str(observed.get("postgres_version", "")).startswith("18.")
        or not isinstance(extensions, dict)
        or set(extensions) != {"citext", "pg_trgm", "pgcrypto", "vector"}
        or extensions.get("vector") != manifest.pgvector_version
        or any(not isinstance(value, str) or not value for value in extensions.values())
        or not isinstance(migration, dict)
        or migration.get("first_version") != 1
        or migration.get("last_version") != expected_probe.schema_version
        or migration.get("applied_count") != expected_probe.schema_version
        or migration.get("contiguous") is not True
        or migration.get("history_sha256")
        != _expected_migration_history_sha256(expected_probe.schema_version)
        or invariants != {
            "canonical_state_singletons": 1,
            "epoch_state_singletons": 1,
            "unvalidated_constraints": 0,
        }
        or vector != {"operator": "cosine_distance", "dimensions": 3, "distance": 0.0}
        or not isinstance(smoke, dict)
        or smoke.get("relation_count") != len(expected_probe.row_counts)
        or smoke.get("total_rows") != sum(expected_probe.row_counts.values())
        or smoke.get("statement_timeout_ms") != 30_000
        or smoke.get("lock_timeout_ms") != 3_000
    ):
        raise CheckpointRuntimeError("verifier live restore evidence differs")


def _expected_migration_history_sha256(schema_version: int) -> str:
    candidates = (
        Path(__file__).parents[3] / "sql/migrations",
        Path(__file__).parents[1] / "master_runtime/sql/migrations",
    )
    roots = tuple(path for path in candidates if path.is_dir() and not path.is_symlink())
    if len(roots) != 1:
        raise CheckpointRuntimeError("exact checkpoint migration source is absent or ambiguous")
    migrations = discover_migrations(roots[0])
    prefix = tuple(item for item in migrations if item.version <= schema_version)
    if [item.version for item in prefix] != list(range(1, schema_version + 1)):
        raise CheckpointRuntimeError("checkpoint schema boundary differs from repository migrations")
    return sha256_value(
        {
            "migrations": [
                {"version": item.version, "filename": item.filename, "sha256": item.sha256}
                for item in prefix
            ]
        }
    )
