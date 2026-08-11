"""Candidate upload/readback/restore-smoke/promotion coordinator."""

from __future__ import annotations

from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from time import monotonic
from typing import Protocol

from .manifest import CheckpointManifest, load_and_verify
from .registry import CheckpointRegistry


class PublishError(RuntimeError):
    """Candidate was rejected and checkpoint HEAD was not advanced."""


class PrivateCheckpointProvider(Protocol):
    def upload_candidate(self, package: Path, manifest: CheckpointManifest) -> str: ...
    def exact_readback(self, exact_version_ref: str, destination: Path) -> Path: ...


class IndependentRestoreVerifier(Protocol):
    def verify_restore(self, package: Path, manifest: CheckpointManifest) -> dict[str, object]: ...


@dataclass(frozen=True, slots=True)
class PublishReceipt:
    checkpoint_id: str
    exact_version_ref: str
    manifest_sha256: str
    current_checkpoint_id: str
    previous_checkpoint_id: str | None
    upload_seconds: float
    readback_seconds: float
    restore_seconds: float
    package_bytes: int
    restore_receipt: dict[str, object]


class CheckpointPublisher:
    def __init__(
        self,
        *,
        registry: CheckpointRegistry,
        provider: PrivateCheckpointProvider,
        restore_verifier: IndependentRestoreVerifier,
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
        manifest = load_and_verify(manifest_path, package)
        initial_head = self.registry.head
        self.registry.add_candidate(manifest)
        exact_ref = ""
        try:
            started = monotonic()
            exact_ref = self.provider.upload_candidate(package, manifest)
            upload_seconds = monotonic() - started
            self.registry.uploaded(manifest.checkpoint_id, exact_ref)

            started = monotonic()
            readback_package = self.provider.exact_readback(exact_ref, readback_directory)
            readback_manifest = load_and_verify(readback_package / manifest_path.name, readback_package)
            if readback_manifest.manifest_sha256 != manifest.manifest_sha256:
                raise PublishError("exact readback manifest differs from candidate")
            readback_seconds = monotonic() - started
            self.registry.readback_verified(manifest.checkpoint_id)

            started = monotonic()
            restore_receipt = self.restore_verifier.verify_restore(readback_package, readback_manifest)
            if restore_receipt.get("ok") is not True:
                raise PublishError("independent restore smoke did not pass")
            restore_seconds = monotonic() - started
            self.registry.restore_verified(manifest.checkpoint_id)
            head = self.registry.promote(
                manifest.checkpoint_id,
                expected_generation=initial_head.generation,
            )
        except Exception as exc:
            with suppress(ValueError):
                self.registry.reject(manifest.checkpoint_id, type(exc).__name__)
            if self.registry.head != initial_head:
                raise AssertionError("failed candidate advanced checkpoint HEAD") from exc
            if isinstance(exc, PublishError):
                raise
            raise PublishError("checkpoint candidate failed verification") from exc

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


def assert_restore_equality(manifest: CheckpointManifest, observed: dict[str, object]) -> None:
    """Validate the independent restore probe against the exact manifest contract."""

    expected = manifest.restore_probe
    if observed.get("schema_version") != expected.schema_version:
        raise PublishError("restore schema version differs")
    if observed.get("canonical_revision") != expected.canonical_revision:
        raise PublishError("restore canonical revision differs")
    if observed.get("logical_hash_sha256") != expected.logical_hash_sha256:
        raise PublishError("restore logical hash differs")
    if observed.get("row_counts") != expected.row_counts:
        raise PublishError("restore row counts differ")
