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
from time import monotonic
from typing import Any, Protocol
from urllib.parse import quote
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.providers.kaggle.adapter import (
    KaggleProviderAdapter,
    _canonical_notebook_source,
    directory_sha256,
)
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    KaggleAmbiguousMutation,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.control_journal import (
    AuthenticatedControlPlaneClient,
    ControlPlaneRuntimeIdentity,
    RemoteControlLedgerKaggleJournal,
)
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
RESTORE_RECEIPT_CONTRACT = "my-data-hub-checkpoint-restore-smoke.v1"
_KAGGLE_WORKING_ROOT = Path("/kaggle/working")


class CheckpointRuntimeError(RuntimeError):
    """A provider-side checkpoint or verifier contract failed closed."""


class CheckpointRetryableError(PublishError):
    """Publication is incomplete but safe to retry with the same candidate."""


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

    def __post_init__(self) -> None:
        if len(self.notebook_ref.split("/")) != 2 or not self.notebook_source:
            raise ValueError("checkpoint verifier notebook identity/source is incomplete")
        if self.kernel_type not in {"notebook", "script"}:
            raise ValueError("checkpoint verifier kernel type is invalid")
        if not 60 <= self.timeout_seconds <= CHECKPOINT_VERIFIER_TIMEOUT_SECONDS:
            raise ValueError("checkpoint verifier timeout exceeds its attempt allocation")


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
    ) -> None:
        _assert_kaggle_working_path(output_directory)
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
        run_id = (
            UUID(str(self.run_id_factory()))
            if self.run_id_factory is not None
            else uuid5(
                NAMESPACE_URL,
                f"my-data-hub:checkpoint-verifier-run:{manifest.checkpoint_id}:{version}",
            )
        )
        source = _render_verifier_source(
            self.assets,
            run_id=run_id,
            dataset_identity=dataset_identity,
            manifest=manifest,
        )
        canonical_source = _canonical_notebook_source(source, kernel_type=self.assets.kernel_type)
        source_sha = hashlib.sha256(canonical_source).hexdigest()
        dataset_source = f"{provider_ref}/{version}"
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
                "dataset_sources": (dataset_source,),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": False,
            },
            # The intent is append-only and may be re-submitted after a lost
            # control-plane response.  Bind its timestamp to the immutable
            # checkpoint rather than to the retry attempt.
            requested_at=manifest.created_at,
        )
        launched = self.adapter.reconcile_private_notebook_mutation(
            intent=intent,
            task_run_id=run_id,
            expected_source_sha256=source_sha,
            dataset_sources=(dataset_source,),
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
        )
        if launched is None:
            try:
                launched = self.adapter.push_private_notebook(
                    intent=intent,
                    task_run_id=run_id,
                    source=source,
                    title=self.assets.notebook_ref.split("/", 1)[1],
                    code_file=self.assets.code_file,
                    kernel_type=self.assets.kernel_type,
                    language=self.assets.language,
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                    disposable=False,
                    dataset_sources=(dataset_source,),
                    enable_internet=False,
                    timeout_seconds=self.assets.timeout_seconds,
                )
            except Exception as push_error:
                launched = self.adapter.reconcile_private_notebook_mutation(
                    intent=intent,
                    task_run_id=run_id,
                    expected_source_sha256=source_sha,
                    dataset_sources=(dataset_source,),
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                    disposable=False,
                )
                if launched is None:
                    raise CheckpointRuntimeError(
                        "checkpoint verifier provider effect remains ambiguous and was not repeated"
                    ) from push_error
        self.adapter.poll_run(launched.run, self.poll_policy)
        destination = self.output_directory / str(run_id)
        receipt: dict[str, object] | None = None
        if destination.exists():
            try:
                receipt = _load_restore_receipt(destination / CHECKPOINT_RESTORE_RECEIPT_NAME)
                _assert_restore_receipt(
                    receipt,
                    run=launched.run,
                    manifest=manifest,
                    dataset_identity=dataset_identity,
                )
            except CheckpointRuntimeError:
                # A crash can leave a partial local output tree.  It is only a
                # cache of the exact provider output and is safe to replace.
                shutil.rmtree(destination, ignore_errors=True)
                receipt = None
        if receipt is None:
            self.adapter.download_exact_run_output_tree(launched.run, destination=destination)
            receipt = _load_restore_receipt(destination / CHECKPOINT_RESTORE_RECEIPT_NAME)
            _assert_restore_receipt(
                receipt,
                run=launched.run,
                manifest=manifest,
                dataset_identity=dataset_identity,
            )
        return {
            **receipt,
            "provider_run_ref": launched.run.provider_run_ref,
            "source_sha256": launched.run.source_sha256,
        }


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
            previous_checkpoint_id=(
                str(snapshot.previous.checkpoint_id) if snapshot.previous is not None else None
            ),
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
        coordinator: KaggleCheckpointCoordinator,
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
        if head.current is not None:
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
                raise CheckpointRuntimeError(
                    "provider-started checkpoint package is absent and cannot be rebuilt"
                )
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
    journal = RemoteControlLedgerKaggleJournal(control_client)
    adapter = KaggleProviderAdapter.from_environment(journal=journal)
    dataset_ref = _exact_owner_slug(
        _required_environment("MY_DATA_HUB_CHECKPOINT_DATASET_REF"),
        "checkpoint dataset",
    )
    registry = RemoteControlCheckpointRegistry(
        control_client,
        operation_id=str(operation_id),
        dataset_ref=dataset_ref,
    )
    provider = KaggleCheckpointDatasetProvider(
        adapter,
        dataset_ref=dataset_ref,
        operation_id=operation_id,
        resource_task_id=run_id,
    )
    verifier_ref = _exact_owner_slug(
        _required_environment("MY_DATA_HUB_CHECKPOINT_VERIFIER_REF"),
        "checkpoint verifier",
    )
    verifier_source_path = Path(_required_environment("MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_PATH"))
    if (
        not verifier_source_path.is_absolute()
        or not verifier_source_path.is_file()
        or verifier_source_path.is_symlink()
        or verifier_source_path.stat().st_size > 10 * 1024 * 1024
    ):
        raise CheckpointRuntimeError("checkpoint verifier source path is not an exact bounded file")
    verifier_source = verifier_source_path.read_bytes()
    expected_source_sha = _required_environment("MY_DATA_HUB_CHECKPOINT_VERIFIER_SOURCE_SHA256")
    if (
        not re.fullmatch(r"[a-f0-9]{64}", expected_source_sha)
        or hashlib.sha256(verifier_source).hexdigest() != expected_source_sha
    ):
        raise CheckpointRuntimeError("checkpoint verifier source hash differs")
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
    output_directory = _KAGGLE_WORKING_ROOT / "checkpoint-verifier-outputs"
    output_directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    output_directory.chmod(0o700)
    restore_verifier = KaggleCheckpointRestoreVerifier(
        adapter,
        KaggleCheckpointVerifierAssets(
            notebook_ref=verifier_ref,
            notebook_source=verifier_source,
            timeout_seconds=CHECKPOINT_VERIFIER_TIMEOUT_SECONDS,
        ),
        output_directory=output_directory,
        operation_id=operation_id,
        authorization_task_id=run_id,
    )
    publisher = KaggleCheckpointCoordinator(
        registry=registry,
        provider=provider,
        restore_verifier=restore_verifier,
    )
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
        claim_source=journal,
        timeout_seconds=CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS,
    )


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
) -> bytes:
    dataset_slug = dataset_identity.provider_ref.split("/", 1)[1]
    values = {
        "MY_DATA_HUB_VERIFIER_TASK_RUN_ID": str(run_id),
        "MY_DATA_HUB_CHECKPOINT_DIRECTORY": f"/kaggle/input/{dataset_slug}",
        "MY_DATA_HUB_CHECKPOINT_MANIFEST": f"/kaggle/input/{dataset_slug}/{CHECKPOINT_MANIFEST_NAME}",
        "MY_DATA_HUB_CHECKPOINT_DATASET_REF": dataset_identity.provider_ref,
        "MY_DATA_HUB_CHECKPOINT_DATASET_VERSION": str(dataset_identity.version),
        "MY_DATA_HUB_CHECKPOINT_PACKAGE_SHA256": dataset_identity.package_sha256,
        "MY_DATA_HUB_CHECKPOINT_ID": str(manifest.checkpoint_id),
        "MY_DATA_HUB_CHECKPOINT_MANIFEST_SHA256": manifest.manifest_sha256,
    }
    bootstrap = f"import os as _mdh_os\n_mdh_os.environ.update({json.dumps(values, sort_keys=True)})\n"
    if assets.kernel_type == "script":
        return bootstrap.encode() + assets.notebook_source
    try:
        body = json.loads(assets.notebook_source)
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
    return json.dumps(body).encode()


def _load_restore_receipt(path: Path) -> dict[str, object]:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > 64 * 1024:
        raise CheckpointRuntimeError("verifier output lacks a bounded restore receipt")
    try:
        value = json.loads(path.read_bytes())
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise CheckpointRuntimeError("verifier restore receipt is invalid JSON") from exc
    if not isinstance(value, dict):
        raise CheckpointRuntimeError("verifier restore receipt must be an object")
    return value


def _assert_restore_receipt(
    receipt: dict[str, object],
    *,
    run: KaggleKernelRunIdentity,
    manifest: CheckpointManifest,
    dataset_identity: KaggleDatasetIdentity,
) -> None:
    expected = {
        "schema_version": RESTORE_RECEIPT_CONTRACT,
        "task_run_id": str(run.task_run_id),
        "checkpoint_id": str(manifest.checkpoint_id),
        "manifest_sha256": manifest.manifest_sha256,
        "dataset_ref": dataset_identity.provider_ref,
        "dataset_version": dataset_identity.version,
        "package_sha256": dataset_identity.package_sha256,
        "ok": True,
    }
    if any(receipt.get(key) != value for key, value in expected.items()):
        raise CheckpointRuntimeError("verifier restore receipt identity differs from the exact run/checkpoint")
    observed = receipt.get("observed")
    if not isinstance(observed, dict):
        raise CheckpointRuntimeError("verifier restore receipt lacks observed probe metadata")
    expected_probe = manifest.restore_probe
    if observed.get("schema_version") != expected_probe.schema_version:
        raise CheckpointRuntimeError("verifier restore schema version differs")
    if observed.get("canonical_revision") != expected_probe.canonical_revision:
        raise CheckpointRuntimeError("verifier restore canonical revision differs")
    if observed.get("logical_hash_sha256") != expected_probe.logical_hash_sha256:
        raise CheckpointRuntimeError("verifier restore logical hash differs")
    if observed.get("row_counts") != expected_probe.row_counts:
        raise CheckpointRuntimeError("verifier restore row counts differ")
