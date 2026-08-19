"""Brokered direct checkpoint upload without a Notebook Kaggle credential."""

from __future__ import annotations

import hashlib
import http.client
import json
import os
import re
import stat
import time
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.control_plane.checkpoint_upload_ledger import CheckpointUploadLedger
from my_data_hub.control_plane.ledger.errors import IdempotencyConflict, LeaseRejected
from my_data_hub.control_plane.ledger.store import ControlLedger
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.providers.kaggle.contracts import (
    BrokeredBlobGrant,
    BrokeredDatasetFile,
    KaggleDatasetIdentity,
    ProviderFingerprint,
)

from .manifest import CheckpointManifest, canonical_json, load_and_verify
from .provider_storage import checkpoint_provider_file_name
from .publisher import PublishReceipt
from .registry import ControlLedgerCheckpointRegistry

CHECKPOINT_MANIFEST_NAME = "checkpoint-manifest.json"
MAX_BLOB_BYTES = 10 * 1024**3
MAX_CHECKPOINT_BYTES = 20 * 1024**3
UPLOAD_GRANT_TTL = timedelta(minutes=15)
_SHA256 = re.compile(r"^[a-f0-9]{64}$")
_CONTENT_TYPES = frozenset({"application/octet-stream", "application/json"})
_REQUIRED_FILES = frozenset(
    {
        "physical/base.tar.gz",
        "physical/pg_wal.tar.gz",
        "physical/backup_manifest",
        "logical/hub.dump",
        "receipts/verification.json",
        CHECKPOINT_MANIFEST_NAME,
    }
)


class BrokeredCheckpointError(RuntimeError):
    """A checkpoint upload did not satisfy its exact fenced contract."""


class BrokeredCheckpointQuarantined(BrokeredCheckpointError):
    """An ambiguous or conflicting provider effect cannot be retried."""


class CheckpointBlobSpec(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    checkpoint_id: UUID
    master_run_ref: str = Field(min_length=3, max_length=300)
    epoch: int = Field(ge=1)
    file_name: str = Field(min_length=1, max_length=200)
    content_length: int = Field(ge=1, le=MAX_BLOB_BYTES)
    content_type: str
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def validate_file(self) -> CheckpointBlobSpec:
        pure = PurePosixPath(self.file_name)
        if (
            self.file_name not in _REQUIRED_FILES
            or pure.is_absolute()
            or ".." in pure.parts
            or self.content_type not in _CONTENT_TYPES
        ):
            raise ValueError("checkpoint blob is outside the fixed artifact contract")
        expected_type = "application/json" if self.file_name.endswith(".json") else "application/octet-stream"
        if self.content_type != expected_type:
            raise ValueError("checkpoint blob content type differs from its fixed artifact")
        return self

    @property
    def intent_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class CheckpointBlobCompletion(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    claim_id: UUID
    operation_id: UUID
    checkpoint_id: UUID
    epoch: int = Field(ge=1)
    file_name: str = Field(min_length=1, max_length=200)
    bytes_sent: int = Field(ge=1, le=MAX_BLOB_BYTES)
    content_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    outcome: str = Field(pattern=r"^uploaded$")


class CheckpointBlobGrant(BaseModel):
    """One JIT response. Callers must never persist or log the URL."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    claim_id: UUID
    checkpoint_id: UUID
    file_name: str
    content_length: int
    content_sha256: str
    expires_at: datetime
    create_url: str = Field(repr=False)

    @model_validator(mode="after")
    def validate_url(self) -> CheckpointBlobGrant:
        parsed = urlsplit(self.create_url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
            or parsed.port not in {None, 443}
            or len(self.create_url) > 8192
        ):
            raise ValueError("provider upload URL is not a bounded HTTPS capability")
        return self

    def __repr__(self) -> str:
        return (
            "CheckpointBlobGrant("
            f"claim_id={self.claim_id!r}, checkpoint_id={self.checkpoint_id!r}, "
            f"file_name={self.file_name!r}, content_length={self.content_length!r}, "
            f"content_sha256={self.content_sha256!r}, expires_at={self.expires_at!r}, "
            "create_url=<redacted>)"
        )


class BrokeredKaggleAdapter(Protocol):
    def current_private_dataset_version(self, *, provider_ref: str) -> int | None: ...

    def start_brokered_dataset_blob(
        self,
        *,
        file_name: str,
        content_length: int,
        content_type: str,
        last_modified_epoch_seconds: int,
    ) -> BrokeredBlobGrant: ...

    def finalize_brokered_checkpoint_dataset(
        self,
        *,
        provider_ref: str,
        title: str,
        files: tuple[BrokeredDatasetFile, ...],
        version_notes: str,
        expected_previous_version: int | None,
    ) -> int: ...

    def reconcile_brokered_checkpoint_dataset(
        self,
        *,
        provider_ref: str,
        version: int,
        expected_files: tuple[tuple[str, int, str], ...],
    ) -> bool: ...


class BrokeredRestoreVerifier(Protocol):
    @property
    def revision_sha256(self) -> str: ...

    def verify_restore(
        self,
        *,
        exact_version_ref: str,
        dataset_identity: KaggleDatasetIdentity,
        manifest: CheckpointManifest,
    ) -> dict[str, object]: ...


class BrokeredForcedFailureVerifier(Protocol):
    def verify_forced_failure(
        self,
        *,
        exact_version_ref: str,
        dataset_identity: KaggleDatasetIdentity,
        manifest: CheckpointManifest,
        authority: RuntimeUploadAuthority,
    ) -> dict[str, object]: ...


class BrokeredMetadataClient(Protocol):
    def get(self, path: str) -> dict[str, Any]: ...

    def post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]: ...


class BrokeredCheckpointRegistry(Protocol):
    def add_candidate(self, manifest: CheckpointManifest) -> None: ...


class BrokeredCheckpointRuntimeProvider:
    """Provider-shaped metadata client used by RuntimeCheckpointCoordinator.

    It intentionally has no ``adapter`` attribute and cannot authenticate to
    Kaggle. The central control process remains the only lifecycle client.
    """

    brokered_upload = True

    def __init__(
        self,
        client: BrokeredMetadataClient,
        *,
        dataset_ref: str,
        operation_id: UUID,
        timeout_seconds: int = 2_100,
    ) -> None:
        if len(dataset_ref.split("/")) != 2 or not 60 <= timeout_seconds <= 2_400:
            raise ValueError("brokered checkpoint runtime provider identity is invalid")
        self.client = client
        self.dataset_ref = dataset_ref
        self.operation_id = operation_id
        self.resource_task_id: UUID | None = None
        self.claim: object | None = None
        self.timeout_seconds = timeout_seconds

    def exact_head_readback(self, reference: object, destination: Path) -> object:
        raise BrokeredCheckpointError(
            "brokered runtime boots only from the exact Dataset input attached by the central launcher"
        )

    def publish(self, *, package: Path, manifest_path: Path) -> PublishReceipt:
        status, started = self._direct_upload(package=package, manifest_path=manifest_path, scenario=None)
        manifest = load_and_verify(manifest_path, package)
        head = self.client.get("/internal/checkpoints/postgres-master/head")
        current = head.get("current")
        previous = head.get("previous")
        if not isinstance(current, dict) or current.get("checkpoint_id") != str(manifest.checkpoint_id):
            raise BrokeredCheckpointError("promoted checkpoint is not exact current HEAD")
        return PublishReceipt(
            checkpoint_id=str(manifest.checkpoint_id),
            exact_version_ref=str(status["exact_version_ref"]),
            manifest_sha256=manifest.manifest_sha256,
            current_checkpoint_id=str(current["checkpoint_id"]),
            previous_checkpoint_id=(str(previous["checkpoint_id"]) if isinstance(previous, dict) else None),
            upload_seconds=time.monotonic() - started,
            readback_seconds=0.0,
            restore_seconds=0.0,
            package_bytes=sum(item.byte_size for item in manifest.files),
            restore_receipt={
                "brokered_direct_upload": True,
                "verifier_run_ref": status["verifier_run_ref"],
                "verifier_receipt_sha256": status["verifier_receipt_sha256"],
            },
        )

    def publish_acceptance(
        self, *, package: Path, manifest_path: Path, scenario: Literal["FM05", "FM14", "FM15"]
    ) -> dict[str, Any]:
        """Direct-upload one fixed task-owned acceptance candidate.

        The return is metadata-only.  Negative scenarios succeed only when the
        central broker reports their fixed expected rejection code.
        """

        status, _started = self._direct_upload(package=package, manifest_path=manifest_path, scenario=scenario)
        if status.get("acceptance_scenario") != scenario:
            raise BrokeredCheckpointError("acceptance publication scenario changed")
        expected = {
            "FM05": ("PROMOTED", None),
            "FM14": ("FAILED", "FM14_EXPECTED_HASH_MISMATCH"),
            "FM15": ("FAILED", "FM15_EXPECTED_RESTORE_FAILURE"),
        }[scenario]
        if (status.get("state"), status.get("failure_code")) != expected:
            raise BrokeredCheckpointError("acceptance publication did not reach its fixed terminal state")
        return status

    def _direct_upload(
        self,
        *,
        package: Path,
        manifest_path: Path,
        scenario: Literal["FM05", "FM14", "FM15"] | None,
    ) -> tuple[dict[str, Any], float]:
        if scenario == "FM14":
            try:
                manifest = CheckpointManifest.from_payload(json.loads(manifest_path.read_bytes()))
            except Exception as exc:
                raise BrokeredCheckpointError("FM14 manifest is invalid") from exc
        else:
            manifest = load_and_verify(manifest_path, package)
        authority = self.client.get(
            f"/internal/checkpoints/{manifest.checkpoint_id}/runtime-upload-authority"
            if scenario is not None
            else "/internal/checkpoints/runtime-upload-authority"
        )
        expected_authority_keys = (
            {"master_run_ref", "epoch", "acceptance_scenario"} if scenario is not None else {"master_run_ref"}
        )
        if set(authority) != expected_authority_keys or not isinstance(authority["master_run_ref"], str):
            raise BrokeredCheckpointError("central checkpoint upload authority is invalid")
        if scenario is not None and (
            int(authority["epoch"]) != manifest.epoch or authority["acceptance_scenario"] != scenario
        ):
            raise BrokeredCheckpointError("central checkpoint upload authority differs from the candidate")
        master_run_ref = str(authority["master_run_ref"])
        files = []
        for item in manifest.files:
            path = package / item.path
            size = path.stat().st_size if path.is_file() and not path.is_symlink() else item.byte_size
            digest = self._sha256(path) if path.is_file() and not path.is_symlink() else item.sha256
            if scenario != "FM14" or item.path != "physical/base.tar.gz":
                if (size, digest) != (item.byte_size, item.sha256):
                    raise BrokeredCheckpointError("checkpoint artifact changed before direct upload")
            elif size != item.byte_size or digest == item.sha256:
                raise BrokeredCheckpointError("FM14 fixed corruption identity is invalid")
            files.append((item.path, path, size, digest))
        manifest_bytes = canonical_json(manifest.payload()) + b"\n"
        files.append(
            (
                CHECKPOINT_MANIFEST_NAME,
                manifest_path,
                len(manifest_bytes),
                hashlib.sha256(manifest_bytes).hexdigest(),
            )
        )
        started = time.monotonic()
        for name, path, size, digest in files:
            if not path.is_file() or path.is_symlink() or path.stat().st_size != size or self._sha256(path) != digest:
                raise BrokeredCheckpointError("checkpoint artifact changed before direct upload")
            try:
                replay = self.client.get(f"/internal/checkpoints/{manifest.checkpoint_id}/publication")
            except Exception:
                replay = {}
            completed = {
                (item.get("file_name"), item.get("content_length"), item.get("content_sha256"))
                for item in replay.get("completed_files", ())
                if isinstance(item, dict)
            }
            if (name, size, digest) in completed:
                continue
            if any(item[0] == name for item in completed):
                raise BrokeredCheckpointError("completed checkpoint blob replay changed identity")
            prepare = self.client.post(
                f"/internal/checkpoints/{manifest.checkpoint_id}/blob-uploads/prepare",
                {
                    "operation_id": str(self.operation_id),
                    "checkpoint_id": str(manifest.checkpoint_id),
                    "master_run_ref": master_run_ref,
                    "epoch": manifest.epoch,
                    "file_name": name,
                    "content_length": size,
                    "content_type": "application/json" if name.endswith(".json") else "application/octet-stream",
                    "content_sha256": digest,
                    "manifest_sha256": manifest.manifest_sha256,
                },
            )
            expected = {
                "claim_id",
                "checkpoint_id",
                "file_name",
                "content_length",
                "content_sha256",
                "expires_at",
                "create_url",
            }
            if set(prepare) != expected or prepare["checkpoint_id"] != str(manifest.checkpoint_id):
                raise BrokeredCheckpointError("checkpoint upload grant differs from the exact artifact")
            self._put_exact(
                str(prepare["create_url"]),
                path,
                content_length=size,
                content_type="application/json" if name.endswith(".json") else "application/octet-stream",
            )
            completion = self.client.post(
                f"/internal/checkpoints/{manifest.checkpoint_id}/blob-uploads/complete",
                {
                    "claim_id": str(prepare["claim_id"]),
                    "operation_id": str(self.operation_id),
                    "checkpoint_id": str(manifest.checkpoint_id),
                    "epoch": manifest.epoch,
                    "file_name": name,
                    "bytes_sent": size,
                    "content_sha256": digest,
                    "outcome": "uploaded",
                },
            )
            if completion.get("state") != "UPLOADED":
                raise BrokeredCheckpointError("checkpoint upload completion was not durably accepted")
        self.client.post(
            f"/internal/checkpoints/{manifest.checkpoint_id}/finalize",
            {"operation_id": str(self.operation_id), "manifest_sha256": manifest.manifest_sha256},
        )
        deadline = time.monotonic() + self.timeout_seconds
        while time.monotonic() < deadline:
            status = self.client.get(f"/internal/checkpoints/{manifest.checkpoint_id}/publication")
            if status.get("state") == "PROMOTED" or (
                scenario in {"FM14", "FM15"}
                and status.get("state") == "FAILED"
                and status.get("failure_code")
                == {"FM14": "FM14_EXPECTED_HASH_MISMATCH", "FM15": "FM15_EXPECTED_RESTORE_FAILURE"}[scenario]
            ):
                return status, started
            if status.get("state") in {"FAILED", "QUARANTINED"}:
                raise BrokeredCheckpointError("brokered checkpoint publication failed closed")
            time.sleep(5.0)
        raise BrokeredCheckpointError("brokered checkpoint publication exceeded its fixed deadline")

    def reconcile_promoted(self, *, manifest: CheckpointManifest) -> PublishReceipt | None:
        status = self.client.get(f"/internal/checkpoints/{manifest.checkpoint_id}/publication")
        if status.get("state") != "PROMOTED" or status.get("manifest_sha256") != manifest.manifest_sha256:
            return None
        head = self.client.get("/internal/checkpoints/postgres-master/head")
        current = head.get("current")
        previous = head.get("previous")
        if not isinstance(current, dict) or current.get("checkpoint_id") != str(manifest.checkpoint_id):
            return None
        return PublishReceipt(
            checkpoint_id=str(manifest.checkpoint_id),
            exact_version_ref=str(status["exact_version_ref"]),
            manifest_sha256=manifest.manifest_sha256,
            current_checkpoint_id=str(manifest.checkpoint_id),
            previous_checkpoint_id=str(previous["checkpoint_id"]) if isinstance(previous, dict) else None,
            upload_seconds=0.0,
            readback_seconds=0.0,
            restore_seconds=0.0,
            package_bytes=sum(item.byte_size for item in manifest.files),
            restore_receipt={"reconciled_from_brokered_head": True},
        )

    @staticmethod
    def _put_exact(url: str, path: Path, *, content_length: int, content_type: str) -> None:
        parsed = urlsplit(url)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.fragment
            or parsed.port not in {None, 443}
        ):
            raise BrokeredCheckpointError("signed upload capability is invalid")
        connection = http.client.HTTPSConnection(parsed.hostname, parsed.port or 443, timeout=60)
        target = parsed.path or "/"
        if parsed.query:
            target += "?" + parsed.query
        try:
            with path.open("rb") as stream:
                connection.request(
                    "PUT",
                    target,
                    body=stream,
                    headers={"Content-Length": str(content_length), "Content-Type": content_type},
                )
                response = connection.getresponse()
                response.read(4_097)
                if response.status < 200 or response.status >= 300:
                    raise BrokeredCheckpointError("direct provider blob upload was rejected")
        except BrokeredCheckpointError:
            raise
        except Exception as exc:
            raise BrokeredCheckpointError("direct provider blob upload failed") from exc
        finally:
            connection.close()

    @staticmethod
    def _sha256(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as stream:
            while block := stream.read(1024 * 1024):
                digest.update(block)
        return digest.hexdigest()


class BrokeredCheckpointRuntimeCoordinator:
    """Publisher facade matching KaggleCheckpointCoordinator without a local adapter."""

    def __init__(self, registry: BrokeredCheckpointRegistry, provider: BrokeredCheckpointRuntimeProvider) -> None:
        self.registry = registry
        self.provider = provider

    def publish(self, *, package: Path, manifest_path: Path, readback_directory: Path) -> PublishReceipt:
        del readback_directory
        manifest = load_and_verify(manifest_path, package)
        self.registry.add_candidate(manifest)
        return self.provider.publish(package=package, manifest_path=manifest_path)

    def reconcile_promoted(self, *, package: Path, manifest_path: Path) -> PublishReceipt | None:
        return self.provider.reconcile_promoted(manifest=load_and_verify(manifest_path, package))


class CheckpointUploadSecretBox:
    """AES-GCM wrapper for provider tokens/URLs stored in the 0600 ledger."""

    def __init__(self, key: bytes) -> None:
        if len(key) != 32:
            raise ValueError("checkpoint broker encryption key must be exactly 32 bytes")
        self._cipher = AESGCM(key)

    @classmethod
    def from_file(cls, path: Path) -> CheckpointUploadSecretBox:
        try:
            metadata = path.lstat()
        except OSError as exc:
            raise ValueError("checkpoint broker encryption key file is unavailable") from exc
        if (
            path.is_symlink()
            or not stat.S_ISREG(metadata.st_mode)
            or metadata.st_uid != os.geteuid()
            or metadata.st_mode & 0o077
            or not 32 <= metadata.st_size <= 65
        ):
            raise ValueError("checkpoint broker encryption key file is not private and bounded")
        raw = path.read_bytes().strip()
        if len(raw) == 64 and _SHA256.fullmatch(raw.decode("ascii", errors="ignore")):
            raw = bytes.fromhex(raw.decode())
        return cls(raw)

    def seal(self, value: str, *, claim_id: UUID, kind: str) -> bytes:
        if not value or kind not in {"blob", "url"}:
            raise ValueError("checkpoint broker secret value is invalid")
        nonce = os.urandom(12)
        aad = f"my-data-hub:checkpoint-upload:{claim_id}:{kind}".encode()
        return nonce + self._cipher.encrypt(nonce, value.encode(), aad)

    def open(self, value: bytes, *, claim_id: UUID, kind: str) -> str:
        if len(value) < 29 or kind not in {"blob", "url"}:
            raise BrokeredCheckpointError("sealed checkpoint upload authority is invalid")
        nonce, ciphertext = value[:12], value[12:]
        aad = f"my-data-hub:checkpoint-upload:{claim_id}:{kind}".encode()
        try:
            return self._cipher.decrypt(nonce, ciphertext, aad).decode()
        except Exception as exc:
            raise BrokeredCheckpointError("sealed checkpoint upload authority cannot be opened") from exc


@dataclass(frozen=True, slots=True)
class RuntimeUploadAuthority:
    operation_id: str
    run_id: str
    attempt_id: str
    master_instance_id: str
    service_instance_id: str
    epoch: int
    master_run_ref: str
    lease_until: datetime
    authority_kind: Literal["master", "acceptance"] = "master"
    acceptance_scenario: Literal["FM05", "FM14", "FM15"] | None = None
    source_revision: str | None = None
    verifier_dataset_version_ref: str | None = None
    verifier_notebook_ref: str | None = None


class BrokeredCheckpointUploadService:
    """Validate runtime metadata and broker direct Notebook→Kaggle uploads."""

    def __init__(
        self,
        ledger: ControlLedger,
        adapter: BrokeredKaggleAdapter,
        secret_box: CheckpointUploadSecretBox,
        restore_verifier: BrokeredRestoreVerifier | None = None,
        restore_verifier_factory: Callable[[UUID, UUID], BrokeredRestoreVerifier] | None = None,
        forced_failure_verifier_factory: (
            Callable[[RuntimeUploadAuthority], BrokeredForcedFailureVerifier] | None
        ) = None,
    ) -> None:
        self.control = ledger
        self.ledger = CheckpointUploadLedger(ledger)
        self.adapter = adapter
        self.secret_box = secret_box
        self.restore_verifier = restore_verifier
        self.restore_verifier_factory = restore_verifier_factory
        self.forced_failure_verifier_factory = forced_failure_verifier_factory

    def _restore_verifier_for(self, publication: dict[str, Any]) -> BrokeredRestoreVerifier:
        verifier = self.restore_verifier
        if verifier is None and self.restore_verifier_factory is not None:
            verifier = self.restore_verifier_factory(
                UUID(str(publication["operation_id"])),
                UUID(str(publication["run_id"])),
            )
        if verifier is None:
            raise BrokeredCheckpointError("independent restore verifier is unavailable")
        return verifier

    @staticmethod
    def _verifier_revision(verifier: BrokeredRestoreVerifier) -> str:
        revision = getattr(verifier, "revision_sha256", None)
        if isinstance(revision, str) and _SHA256.fullmatch(revision):
            return revision
        # Compatibility for injected test verifiers. Production verifiers expose
        # an exact asset/source-bound revision above.
        return sha256_value(
            {
                "schema_version": "my-data-hub-checkpoint-verifier-revision.v1",
                "verifier_type": f"{type(verifier).__module__}.{type(verifier).__qualname__}",
            }
        )

    def prepare(self, spec: CheckpointBlobSpec, authority: RuntimeUploadAuthority) -> CheckpointBlobGrant:
        try:
            candidate, manifest = self._validate(spec, authority)
        except BrokeredCheckpointError:
            publication = self.ledger.publication(str(spec.checkpoint_id))
            if publication is not None and publication["operation_id"] == authority.operation_id:
                self._fail(spec.checkpoint_id, "BLOB_PREPARE_CONFLICT", quarantine=True)
            raise
        generation = int(candidate["source_head_generation"])
        source_head = self.control.checkpoint_head("postgres-master")
        manifest_bytes = canonical_json(manifest.payload()) + b"\n"
        expected_total_bytes = sum(item.byte_size for item in manifest.files) + len(manifest_bytes)
        self.ledger.ensure_publication(
            checkpoint_id=str(spec.checkpoint_id),
            operation_id=str(spec.operation_id),
            run_id=authority.run_id,
            attempt_id=authority.attempt_id,
            master_instance_id=authority.master_instance_id,
            service_instance_id=authority.service_instance_id,
            master_run_ref=authority.master_run_ref,
            epoch=authority.epoch,
            dataset_ref=str(candidate["dataset_ref"]),
            manifest_sha256=manifest.manifest_sha256,
            source_head_generation=generation,
            expected_file_count=len(manifest.files) + 1,
            expected_total_bytes=expected_total_bytes,
            authority_kind=authority.authority_kind,
            acceptance_scenario=authority.acceptance_scenario,
            source_previous_checkpoint_id=(source_head.previous_checkpoint_id if source_head is not None else None),
        )
        expires_at = min(authority.lease_until, self.control.clock.now() + UPLOAD_GRANT_TTL)
        if expires_at <= self.control.clock.now() + timedelta(seconds=30):
            raise LeaseRejected("checkpoint upload lease is too short")
        claim_id = uuid5(NAMESPACE_URL, f"checkpoint-blob:{spec.checkpoint_id}:{spec.file_name}:{spec.intent_sha256}")
        row, _created = self.ledger.ensure_claim(
            claim_id=str(claim_id),
            checkpoint_id=str(spec.checkpoint_id),
            operation_id=str(spec.operation_id),
            epoch=spec.epoch,
            file_name=spec.file_name,
            content_length=spec.content_length,
            content_type=spec.content_type,
            content_sha256=spec.content_sha256,
            manifest_sha256=spec.manifest_sha256,
            intent_sha256=spec.intent_sha256,
            expires_at=expires_at,
        )
        if row["state"] in {"READY", "UPLOADED"}:
            if row["state"] == "UPLOADED" or row["sealed_create_url"] is None:
                raise IdempotencyConflict("completed upload claims do not reissue their signed URL")
            url = self.secret_box.open(bytes(row["sealed_create_url"]), claim_id=claim_id, kind="url")
            return self._public_grant(spec, claim_id, expires_at, url)
        if row["state"] == "STARTING":
            self.ledger.mark_start_ambiguous(str(claim_id))
            raise BrokeredCheckpointQuarantined("provider blob start was interrupted and is ambiguous")
        if row["state"] != "PREPARING":
            raise BrokeredCheckpointQuarantined("checkpoint blob preparation is not safely retryable")
        self.ledger.claim_start(str(claim_id))
        try:
            grant = self.adapter.start_brokered_dataset_blob(
                file_name=checkpoint_provider_file_name(spec.file_name),
                content_length=spec.content_length,
                content_type=spec.content_type,
                last_modified_epoch_seconds=int(manifest.created_at.timestamp()),
            )
        except Exception as exc:
            self.ledger.mark_start_ambiguous(str(claim_id))
            raise BrokeredCheckpointQuarantined("provider blob start outcome is ambiguous") from exc
        sealed_token = self.secret_box.seal(grant.blob_token, claim_id=claim_id, kind="blob")
        sealed_url = self.secret_box.seal(grant.create_url, claim_id=claim_id, kind="url")
        self.ledger.mark_ready(str(claim_id), sealed_blob_token=sealed_token, sealed_create_url=sealed_url)
        return self._public_grant(spec, claim_id, expires_at, grant.create_url)

    def complete(self, completion: CheckpointBlobCompletion, authority: RuntimeUploadAuthority) -> dict[str, Any]:
        if (
            str(completion.operation_id) != authority.operation_id
            or completion.epoch != authority.epoch
            or self.control.clock.now() >= authority.lease_until
        ):
            raise LeaseRejected("checkpoint upload completion authority is stale")
        row = self.ledger.claim(str(completion.claim_id))
        if (
            row["operation_id"] != str(completion.operation_id)
            or row["checkpoint_id"] != str(completion.checkpoint_id)
            or row["epoch"] != completion.epoch
            or row["file_name"] != completion.file_name
        ):
            raise IdempotencyConflict("checkpoint upload completion identity differs from its claim")
        row = self.ledger.complete_claim(
            str(completion.claim_id),
            bytes_sent=completion.bytes_sent,
            content_sha256=completion.content_sha256,
        )
        return {
            "claim_id": str(completion.claim_id),
            "checkpoint_id": str(completion.checkpoint_id),
            "file_name": completion.file_name,
            "state": str(row["state"]),
            "content_sha256": str(row["content_sha256"]),
        }

    def finalize(self, checkpoint_id: UUID, authority: RuntimeUploadAuthority) -> dict[str, Any]:
        """Finalize, independently verify, and CAS-promote one exact publication."""

        return self._finalize(checkpoint_id, authority, durable_recovery=False)

    def _finalize(
        self,
        checkpoint_id: UUID,
        authority: RuntimeUploadAuthority,
        *,
        durable_recovery: bool,
    ) -> dict[str, Any]:
        """Finalize normally or recover centrally after every blob is durable."""

        publication = self.ledger.publication(str(checkpoint_id))
        if publication is None:
            raise BrokeredCheckpointError("checkpoint publication is absent")
        _candidate, manifest = self._validate_publication(
            publication,
            authority,
            durable_recovery=durable_recovery,
        )
        if authority.authority_kind == "acceptance":
            return self._finalize_acceptance(checkpoint_id, publication, manifest, authority)
        if publication["state"] == "PROMOTED":
            return self._public_status(publication)
        claims = self.ledger.claims(str(checkpoint_id))
        expected_files, provider_files, package_sha256 = self._provider_files(
            publication=publication,
            manifest=manifest,
            claims=claims,
        )
        persisted_expected = publication.get("expected_provider_version")
        reset = self.control.checkpoint_dataset_incarnation_retirement(str(checkpoint_id))
        dataset_incarnation_reset = reset is not None
        source_head_checkpoint_id = (
            str(reset["source_head_checkpoint_id"]) if reset is not None else None
        )
        if reset is not None:
            expected_version = 1
            expected_previous_version = None
        elif persisted_expected is not None:
            expected_version = int(persisted_expected)
            expected_previous_version = expected_version - 1 or None
        else:
            head = self.control.checkpoint_head("postgres-master")
            current_version = self.adapter.current_private_dataset_version(provider_ref=str(publication["dataset_ref"]))
            if head is None or head.current_checkpoint_id is None:
                if current_version is not None:
                    raise BrokeredCheckpointQuarantined("checkpoint Dataset exists without an exact current HEAD claim")
                expected_previous_version = None
                expected_version = 1
            else:
                current = self.control.checkpoint_candidate(head.current_checkpoint_id)
                if current is None or not current.get("version_ref"):
                    raise BrokeredCheckpointError("current checkpoint lacks an exact Dataset version")
                current_ref, version_text = str(current["version_ref"]).rsplit("/", 1)
                if current_ref != publication["dataset_ref"] or not version_text.isdigit():
                    raise BrokeredCheckpointError("current checkpoint Dataset identity is invalid")
                durable_previous_version = int(version_text)
                if current_version is None:
                    # The provider Dataset may have been externally removed even
                    # though the control ledger still retains its exact HEAD.
                    # A fully uploaded child checkpoint is a complete replacement
                    # snapshot, so recreate the same private slug from version 1.
                    # ``begin_finalize`` persists this reset as expected version 1
                    # before the provider effect; interrupted replay therefore
                    # never guesses whether create already happened.
                    expected_previous_version = None
                    expected_version = 1
                    dataset_incarnation_reset = True
                    source_head_checkpoint_id = head.current_checkpoint_id
                    self.control.prepare_missing_checkpoint_dataset_incarnation(
                        str(checkpoint_id),
                        dataset_ref=str(publication["dataset_ref"]),
                        source_head_checkpoint_id=source_head_checkpoint_id,
                        source_head_generation=int(publication["source_head_generation"]),
                    )
                elif current_version != durable_previous_version:
                    raise BrokeredCheckpointQuarantined("checkpoint Dataset advanced beyond the exact current HEAD")
                else:
                    expected_previous_version = durable_previous_version
                    expected_version = expected_previous_version + 1

        original_state = str(publication["state"])
        publication = self.ledger.begin_finalize(str(checkpoint_id), expected_provider_version=expected_version)
        exact_ref = f"{publication['dataset_ref']}/{expected_version}"
        resolved = str(publication["state"]) in {"DATASET_RESOLVED", "VERIFYING", "VERIFIED", "PROMOTED"}
        if not resolved:
            if original_state == "READY_TO_FINALIZE":
                notes = (
                    f"mdh-checkpoint operation={publication['operation_id']} epoch={publication['epoch']} "
                    f"manifest={publication['manifest_sha256']}"
                )
                try:
                    observed_version = self.adapter.finalize_brokered_checkpoint_dataset(
                        provider_ref=str(publication["dataset_ref"]),
                        title=str(publication["dataset_ref"]).split("/", 1)[1],
                        files=provider_files,
                        version_notes=notes,
                        expected_previous_version=expected_previous_version,
                    )
                except Exception as exc:
                    if not self.adapter.reconcile_brokered_checkpoint_dataset(
                        provider_ref=str(publication["dataset_ref"]),
                        version=expected_version,
                        expected_files=expected_files,
                    ):
                        raise BrokeredCheckpointError("checkpoint Dataset finalization remains unresolved") from exc
                else:
                    if observed_version != expected_version:
                        self._fail(checkpoint_id, "DATASET_VERSION_UNEXPECTED", quarantine=True)
                        raise BrokeredCheckpointQuarantined(
                            "checkpoint Dataset finalization returned an unexpected version"
                        )
            elif not self.adapter.reconcile_brokered_checkpoint_dataset(
                provider_ref=str(publication["dataset_ref"]),
                version=expected_version,
                expected_files=expected_files,
            ):
                if int(publication["finalize_attempts"]) >= 3:
                    self._fail(checkpoint_id, "DATASET_VERSION_UNRESOLVED", quarantine=True)
                    raise BrokeredCheckpointQuarantined(
                        "interrupted checkpoint Dataset finalization exhausted bounded reconciliation"
                    )
                raise BrokeredCheckpointError("interrupted checkpoint Dataset finalization is not yet reconciled")
            publication = self.ledger.resolve_dataset(str(checkpoint_id), exact_version_ref=exact_ref)

        if dataset_incarnation_reset:
            if source_head_checkpoint_id is None:
                raise BrokeredCheckpointError("checkpoint Dataset reset lacks its source HEAD")
            self.control.retire_missing_checkpoint_dataset_incarnation(
                str(checkpoint_id),
                dataset_ref=str(publication["dataset_ref"]),
                source_head_checkpoint_id=source_head_checkpoint_id,
                source_head_generation=int(publication["source_head_generation"]),
            )

        registry = ControlLedgerCheckpointRegistry(
            self.control,
            operation_id=str(publication["operation_id"]),
            dataset_ref=str(publication["dataset_ref"]),
        )
        try:
            registry.uploaded(checkpoint_id, exact_ref)
            registry.package_uploaded(checkpoint_id, package_sha256)
            verifier: BrokeredRestoreVerifier | None = None
            if publication["state"] in {"DATASET_RESOLVED", "VERIFYING"}:
                verifier = self._restore_verifier_for(publication)
            if publication["state"] == "DATASET_RESOLVED":
                assert verifier is not None
                publication = self.ledger.start_verification(
                    str(checkpoint_id),
                    verifier_revision_sha256=self._verifier_revision(verifier),
                )
            if publication["state"] == "VERIFYING":
                assert verifier is not None
                dataset_identity = KaggleDatasetIdentity(
                    provider_ref=str(publication["dataset_ref"]),
                    version=expected_version,
                    privacy="private",
                    package_sha256=package_sha256,
                    fingerprint=ProviderFingerprint(
                        value=sha256_value(
                            {
                                "provider_ref": publication["dataset_ref"],
                                "version": expected_version,
                                "privacy": "private",
                                "package_sha256": package_sha256,
                            }
                        )
                    ),
                    observed_at=self.control.clock.now(),
                )
                receipt = verifier.verify_restore(
                    exact_version_ref=exact_ref,
                    dataset_identity=dataset_identity,
                    manifest=manifest,
                )
                if receipt.get("ok") is not True or not isinstance(receipt.get("provider_run_ref"), str):
                    raise BrokeredCheckpointError("independent restore verifier did not pass")
                receipt_sha256 = hashlib.sha256(canonical_json_bytes(receipt)).hexdigest()
                registry.readback_verified(checkpoint_id)
                registry.restore_verified(checkpoint_id)
                publication = self.ledger.transition(
                    str(checkpoint_id),
                    expected_states=frozenset({"VERIFYING"}),
                    state="VERIFIED",
                    event_type="verifier.passed",
                    evidence={"receipt_sha256": receipt_sha256},
                    verifier_run_ref=str(receipt["provider_run_ref"]),
                    verifier_receipt_sha256=receipt_sha256,
                    verifier_evidence=receipt,
                )
            refreshed = self.ledger.publication_runtime_authority(str(checkpoint_id))
            if refreshed is None:
                raise LeaseRejected("checkpoint runtime authority expired during independent verification")
            current_authority = RuntimeUploadAuthority(
                operation_id=str(refreshed["operation_id"]),
                run_id=str(refreshed["run_id"]),
                attempt_id=str(refreshed["attempt_id"]),
                master_instance_id=str(refreshed["master_instance_id"]),
                service_instance_id=str(publication["service_instance_id"]),
                epoch=int(refreshed["epoch"]),
                master_run_ref=str(refreshed["master_run_ref"]),
                lease_until=datetime.fromisoformat(str(refreshed["lease_until"]).replace("Z", "+00:00")),
            )
            self._validate_publication(
                publication,
                current_authority,
                durable_recovery=durable_recovery,
            )
            if publication["state"] == "VERIFIED":
                promoted = registry.promote(
                    checkpoint_id,
                    expected_generation=int(publication["source_head_generation"]),
                )
                publication = self.ledger.transition(
                    str(checkpoint_id),
                    expected_states=frozenset({"VERIFIED"}),
                    state="PROMOTED",
                    event_type="publication.promoted",
                    evidence={"generation": promoted.generation, "checkpoint_id": str(checkpoint_id)},
                )
        except Exception as exc:
            if publication["state"] not in {"PROMOTED", "FAILED", "QUARANTINED"}:
                self._fail(checkpoint_id, type(exc).__name__, quarantine=False)
            raise
        return self._public_status(publication)

    def _finalize_acceptance(
        self,
        checkpoint_id: UUID,
        publication: dict[str, Any],
        manifest: CheckpointManifest,
        authority: RuntimeUploadAuthority,
    ) -> dict[str, Any]:
        """Finalize one task-bound disposable Dataset without master impersonation."""

        scenario = authority.acceptance_scenario
        if scenario not in {"FM05", "FM14", "FM15"}:
            raise BrokeredCheckpointError("checkpoint acceptance scenario is unavailable")
        if publication["state"] in {"PROMOTED", "FAILED"}:
            return self._public_status(publication)
        claims = self.ledger.claims(str(checkpoint_id))
        expected_files, provider_files, package_sha256 = self._provider_files(
            publication=publication, manifest=manifest, claims=claims
        )
        persisted = publication.get("expected_provider_version")
        expected_previous_version: int | None = None
        if persisted is None and scenario == "FM05":
            head = self.control.checkpoint_head("postgres-master")
            current_version = self.adapter.current_private_dataset_version(provider_ref=str(publication["dataset_ref"]))
            if head is None or head.current_checkpoint_id is None:
                if current_version is not None:
                    raise BrokeredCheckpointQuarantined("FM05 checkpoint Dataset exists without a current HEAD")
                expected_version = 1
            else:
                current = self.control.checkpoint_candidate(head.current_checkpoint_id)
                if current is None or not current.get("version_ref"):
                    raise BrokeredCheckpointError("FM05 current HEAD lacks an exact Dataset version")
                current_ref, version_text = str(current["version_ref"]).rsplit("/", 1)
                if current_ref != publication["dataset_ref"] or not version_text.isdigit():
                    raise BrokeredCheckpointError("FM05 must use the normal checkpoint Dataset")
                expected_previous_version = int(version_text)
                if current_version != expected_previous_version:
                    raise BrokeredCheckpointQuarantined("FM05 Dataset advanced beyond current HEAD")
                expected_version = expected_previous_version + 1
        elif persisted is None:
            current_version = self.adapter.current_private_dataset_version(provider_ref=str(publication["dataset_ref"]))
            if current_version is not None:
                raise BrokeredCheckpointQuarantined("task-owned acceptance Dataset existed before its fixed mutation")
            expected_version = 1
        else:
            expected_version = int(persisted)
            expected_previous_version = expected_version - 1 if scenario == "FM05" and expected_version > 1 else None
        original_state = str(publication["state"])
        publication = self.ledger.begin_finalize(str(checkpoint_id), expected_provider_version=expected_version)
        exact_ref = f"{publication['dataset_ref']}/{expected_version}"
        if publication["state"] == "FINALIZING":
            if original_state == "READY_TO_FINALIZE":
                notes = (
                    f"mdh-checkpoint-acceptance scenario={scenario} operation={publication['operation_id']} "
                    f"manifest={publication['manifest_sha256']}"
                )
                try:
                    observed = self.adapter.finalize_brokered_checkpoint_dataset(
                        provider_ref=str(publication["dataset_ref"]),
                        title=str(publication["dataset_ref"]).split("/", 1)[1],
                        files=provider_files,
                        version_notes=notes,
                        expected_previous_version=expected_previous_version,
                    )
                except Exception as exc:
                    if not self.adapter.reconcile_brokered_checkpoint_dataset(
                        provider_ref=str(publication["dataset_ref"]),
                        version=expected_version,
                        expected_files=expected_files,
                    ):
                        raise BrokeredCheckpointError("acceptance Dataset finalization remains unresolved") from exc
                else:
                    if observed != expected_version:
                        self._fail(checkpoint_id, "DATASET_VERSION_UNEXPECTED", quarantine=True)
                        raise BrokeredCheckpointQuarantined("acceptance Dataset returned an unexpected version")
            elif not self.adapter.reconcile_brokered_checkpoint_dataset(
                provider_ref=str(publication["dataset_ref"]),
                version=expected_version,
                expected_files=expected_files,
            ):
                raise BrokeredCheckpointError("acceptance Dataset finalization is not reconciled")
            publication = self.ledger.resolve_dataset(str(checkpoint_id), exact_version_ref=exact_ref)

        registry = ControlLedgerCheckpointRegistry(
            self.control,
            operation_id=str(publication["operation_id"]),
            dataset_ref=str(publication["dataset_ref"]),
        )
        registry.uploaded(checkpoint_id, exact_ref)
        registry.package_uploaded(checkpoint_id, package_sha256)
        dataset_identity = KaggleDatasetIdentity(
            provider_ref=str(publication["dataset_ref"]),
            version=expected_version,
            privacy="private",
            package_sha256=package_sha256,
            fingerprint=ProviderFingerprint(
                value=sha256_value(
                    {
                        "provider_ref": publication["dataset_ref"],
                        "version": expected_version,
                        "privacy": "private",
                        "package_sha256": package_sha256,
                    }
                )
            ),
            observed_at=self.control.clock.now(),
        )
        if scenario == "FM14":
            expected_digest = next(item.sha256 for item in manifest.files if item.path == "physical/base.tar.gz")
            observed_digest = next(
                str(row["content_sha256"]) for row in claims if row["file_name"] == "physical/base.tar.gz"
            )
            if expected_digest == observed_digest:
                raise BrokeredCheckpointError("FM14 fixed corruption was not observed")
            registry.reject(checkpoint_id, "FM14_EXACT_READBACK_HASH_MISMATCH")
            publication = self.ledger.transition(
                str(checkpoint_id),
                expected_states=frozenset({"DATASET_RESOLVED"}),
                state="FAILED",
                event_type="publication.failed",
                evidence={"failure_code": "FM14_EXPECTED_HASH_MISMATCH"},
                failure_code="FM14_EXPECTED_HASH_MISMATCH",
            )
            return self._public_status(publication)

        if publication["state"] == "DATASET_RESOLVED":
            publication = self.ledger.transition(
                str(checkpoint_id),
                expected_states=frozenset({"DATASET_RESOLVED"}),
                state="VERIFYING",
                event_type="verifier.started",
                evidence={"exact_version_ref": exact_ref, "scenario": scenario},
            )
        if scenario == "FM15":
            registry.readback_verified(checkpoint_id)
            factory = self.forced_failure_verifier_factory
            if factory is None:
                raise BrokeredCheckpointError("FM15 central forced-failure verifier is unavailable")
            receipt = factory(authority).verify_forced_failure(
                exact_version_ref=exact_ref,
                dataset_identity=dataset_identity,
                manifest=manifest,
                authority=authority,
            )
            if receipt.get("expected_failure") is not True or not isinstance(receipt.get("provider_run_ref"), str):
                raise BrokeredCheckpointError("FM15 verifier did not prove the fixed provider failure")
            receipt_sha256 = hashlib.sha256(canonical_json_bytes(receipt)).hexdigest()
            registry.reject(checkpoint_id, "FM15_FORCED_RESTORE_SMOKE_FAILURE")
            publication = self.ledger.transition(
                str(checkpoint_id),
                expected_states=frozenset({"VERIFYING"}),
                state="FAILED",
                event_type="publication.failed",
                evidence={"failure_code": "FM15_EXPECTED_RESTORE_FAILURE"},
                verifier_run_ref=str(receipt["provider_run_ref"]),
                verifier_receipt_sha256=receipt_sha256,
                verifier_evidence=receipt,
                failure_code="FM15_EXPECTED_RESTORE_FAILURE",
            )
            return self._public_status(publication)

        verifier = self.restore_verifier
        if verifier is None and self.restore_verifier_factory is not None:
            verifier = self.restore_verifier_factory(
                UUID(str(publication["operation_id"])), UUID(str(publication["run_id"]))
            )
        if verifier is None:
            raise BrokeredCheckpointError("FM05 independent restore verifier is unavailable")
        receipt = verifier.verify_restore(
            exact_version_ref=exact_ref, dataset_identity=dataset_identity, manifest=manifest
        )
        if receipt.get("ok") is not True or not isinstance(receipt.get("provider_run_ref"), str):
            raise BrokeredCheckpointError("FM05 independent restore verifier did not pass")
        receipt_sha256 = hashlib.sha256(canonical_json_bytes(receipt)).hexdigest()
        registry.readback_verified(checkpoint_id)
        registry.restore_verified(checkpoint_id)
        publication = self.ledger.transition(
            str(checkpoint_id),
            expected_states=frozenset({"VERIFYING"}),
            state="VERIFIED",
            event_type="verifier.passed",
            evidence={"receipt_sha256": receipt_sha256},
            verifier_run_ref=str(receipt["provider_run_ref"]),
            verifier_receipt_sha256=receipt_sha256,
            verifier_evidence=receipt,
        )
        promoted = registry.promote(checkpoint_id, expected_generation=int(publication["source_head_generation"]))
        publication = self.ledger.transition(
            str(checkpoint_id),
            expected_states=frozenset({"VERIFIED"}),
            state="PROMOTED",
            event_type="publication.promoted",
            evidence={"generation": promoted.generation, "checkpoint_id": str(checkpoint_id)},
        )
        return self._public_status(publication)

    def status(self, checkpoint_id: UUID) -> dict[str, Any]:
        publication = self.ledger.publication(str(checkpoint_id))
        if publication is None:
            raise KeyError(str(checkpoint_id))
        return self._public_status(publication)

    def reconcile_pending_once(self) -> list[dict[str, Any]]:
        results: list[dict[str, Any]] = []
        for checkpoint_value in self.ledger.failed_verifier_publications():
            publication = self.ledger.publication(checkpoint_value)
            if publication is None:
                continue
            try:
                verifier = self._restore_verifier_for(publication)
                reopened = self.ledger.retry_failed_verification(
                    checkpoint_value,
                    verifier_revision_sha256=self._verifier_revision(verifier),
                )
            except Exception:
                # A historical failed verifier may depend on assets that are
                # no longer present in the current release.  It must not block
                # a newer fully uploaded checkpoint from reaching its durable
                # provider/HEAD reconciliation.  Each failed publication is
                # independently retryable on a later compatible release.
                continue
            if reopened is None:
                continue
        for checkpoint_value in self.ledger.pending_publications():
            checkpoint_id = UUID(checkpoint_value)
            raw = self.ledger.publication_runtime_authority(checkpoint_value)
            if raw is None:
                self._fail(checkpoint_id, "RUNTIME_AUTHORITY_EXPIRED", quarantine=False)
                continue
            authority = RuntimeUploadAuthority(
                operation_id=str(raw["operation_id"]),
                run_id=str(raw["run_id"]),
                attempt_id=str(raw["attempt_id"]),
                master_instance_id=str(raw["master_instance_id"]),
                service_instance_id=str(raw["service_instance_id"]),
                epoch=int(raw["epoch"]),
                master_run_ref=str(raw["master_run_ref"]),
                lease_until=datetime.fromisoformat(str(raw["lease_until"]).replace("Z", "+00:00")),
            )
            try:
                results.append(self._finalize(checkpoint_id, authority, durable_recovery=True))
            except BrokeredCheckpointError:
                continue
        return results

    def _provider_files(
        self,
        *,
        publication: dict[str, Any],
        manifest: CheckpointManifest,
        claims: list[dict[str, Any]],
    ) -> tuple[tuple[tuple[str, int, str], ...], tuple[BrokeredDatasetFile, ...], str]:
        if len(claims) != len(manifest.files) + 1 or {row["file_name"] for row in claims} != _REQUIRED_FILES:
            raise BrokeredCheckpointError("checkpoint publication is missing exact artifacts")
        entries: list[dict[str, object]] = []
        expected: list[tuple[str, int, str]] = []
        provider_files: list[BrokeredDatasetFile] = []
        for row in claims:
            if row["state"] not in {"UPLOADED", "CONSUMED"}:
                raise BrokeredCheckpointError("checkpoint publication contains an incomplete upload")
            description = canonical_json_bytes(
                {
                    "operation_id": str(publication["operation_id"]),
                    "master_run_ref": str(publication["master_run_ref"]),
                    "epoch": int(publication["epoch"]),
                    "manifest_sha256": str(publication["manifest_sha256"]),
                    "file_sha256": str(row["content_sha256"]),
                    "total_bytes": int(row["content_length"]),
                }
            ).decode()
            provider_name = checkpoint_provider_file_name(str(row["file_name"]))
            expected.append((provider_name, int(row["content_length"]), description))
            entries.append(
                {
                    "path": str(row["file_name"]),
                    "byte_size": int(row["content_length"]),
                    "sha256": str(row["content_sha256"]),
                }
            )
            if row["state"] == "UPLOADED":
                sealed = row.get("sealed_blob_token")
                if not isinstance(sealed, bytes):
                    raise BrokeredCheckpointError("checkpoint provider blob token is unavailable")
                provider_files.append(
                    BrokeredDatasetFile(
                        name=provider_name,
                        total_bytes=int(row["content_length"]),
                        description=description,
                        blob_token=self.secret_box.open(
                            sealed,
                            claim_id=UUID(str(row["claim_id"])),
                            kind="blob",
                        ),
                    )
                )
        package_sha256 = sha256_value({"files": sorted(entries, key=lambda item: str(item["path"]))})
        return tuple(expected), tuple(provider_files), package_sha256

    def _validate_publication(
        self,
        publication: dict[str, Any],
        authority: RuntimeUploadAuthority,
        *,
        durable_recovery: bool = False,
    ) -> tuple[dict[str, Any], CheckpointManifest]:
        if (
            publication["operation_id"] != authority.operation_id
            or publication["run_id"] != authority.run_id
            or publication["attempt_id"] != authority.attempt_id
            or publication["master_instance_id"] != authority.master_instance_id
            or publication["master_run_ref"] != authority.master_run_ref
            or int(publication["epoch"]) != authority.epoch
        ):
            raise LeaseRejected("checkpoint publication authority differs from the exact runtime")
        checkpoint_id = str(publication["checkpoint_id"])
        candidate = self.control.checkpoint_candidate(checkpoint_id)
        if candidate is None or not isinstance(candidate.get("manifest"), dict):
            raise BrokeredCheckpointError("checkpoint publication candidate is unavailable")
        manifest = CheckpointManifest.from_payload(candidate["manifest"])
        probe = CheckpointBlobSpec(
            operation_id=UUID(str(publication["operation_id"])),
            checkpoint_id=UUID(checkpoint_id),
            master_run_ref=authority.master_run_ref,
            epoch=int(publication["epoch"]),
            file_name=CHECKPOINT_MANIFEST_NAME,
            content_length=len(canonical_json(manifest.payload()) + b"\n"),
            content_type="application/json",
            content_sha256=hashlib.sha256(canonical_json(manifest.payload()) + b"\n").hexdigest(),
            manifest_sha256=manifest.manifest_sha256,
        )
        return self._validate(probe, authority, durable_recovery=durable_recovery)

    def _fail(self, checkpoint_id: UUID, code: str, *, quarantine: bool) -> None:
        state = "QUARANTINED" if quarantine else "FAILED"
        event = "publication.quarantined" if quarantine else "publication.failed"
        with_error = re.sub(r"[^A-Za-z0-9_.-]", "_", code)[:120] or "BROKER_FAILURE"
        publication = self.ledger.publication(str(checkpoint_id))
        with suppress(Exception):
            if publication is None:
                raise KeyError(str(checkpoint_id))
            ControlLedgerCheckpointRegistry(
                self.control,
                operation_id=str(publication["operation_id"]),
                dataset_ref=str(publication["dataset_ref"]),
            ).reject(checkpoint_id, with_error)
        with suppress(Exception):
            self.ledger.transition(
                str(checkpoint_id),
                expected_states=frozenset(
                    {
                        "PREPARING",
                        "UPLOADING",
                        "READY_TO_FINALIZE",
                        "FINALIZING",
                        "DATASET_RESOLVED",
                        "VERIFYING",
                        "VERIFIED",
                    }
                ),
                state=state,
                event_type=event,
                evidence={"failure_code": with_error},
                failure_code=with_error,
            )

    def _public_status(self, publication: dict[str, Any]) -> dict[str, Any]:
        completed = tuple(
            {
                "file_name": str(row["file_name"]),
                "content_length": int(row["content_length"]),
                "content_sha256": str(row["content_sha256"]),
            }
            for row in self.ledger.claims(str(publication["checkpoint_id"]))
            if row["state"] in {"UPLOADED", "CONSUMED"}
        )
        evidence = publication.get("verifier_evidence_json")
        return {
            "schema_version": "my-data-hub-checkpoint-broker-publication.v1",
            "completed_files": completed,
            "verifier_evidence": json.loads(evidence) if isinstance(evidence, str) else None,
            **{
                key: publication.get(key)
                for key in (
                    "checkpoint_id",
                    "operation_id",
                    "epoch",
                    "manifest_sha256",
                    "source_head_generation",
                    "source_previous_checkpoint_id",
                    "state",
                    "exact_version_ref",
                    "verifier_run_ref",
                    "verifier_receipt_sha256",
                    "failure_code",
                    "authority_kind",
                    "acceptance_scenario",
                    "created_at",
                    "updated_at",
                )
            },
        }

    def _validate(
        self,
        spec: CheckpointBlobSpec,
        authority: RuntimeUploadAuthority,
        *,
        durable_recovery: bool = False,
    ) -> tuple[dict[str, Any], CheckpointManifest]:
        if authority.authority_kind == "acceptance":
            if durable_recovery:
                raise LeaseRejected("checkpoint acceptance authority cannot use central recovery")
            self._validate_acceptance_authority(spec, authority)
        elif durable_recovery:
            recovered = self.ledger.publication_runtime_authority(str(spec.checkpoint_id))
            if not (
                recovered is not None
                and str(spec.operation_id) == authority.operation_id == str(recovered["operation_id"])
                and authority.run_id == str(recovered["run_id"])
                and authority.attempt_id == str(recovered["attempt_id"])
                and authority.master_instance_id == str(recovered["master_instance_id"])
                and authority.service_instance_id == str(recovered["service_instance_id"])
                and spec.master_run_ref == authority.master_run_ref == str(recovered["master_run_ref"])
                and spec.epoch == authority.epoch == int(recovered["epoch"])
            ):
                raise LeaseRejected("checkpoint central recovery authority is unavailable")
        elif (
            str(spec.operation_id) != authority.operation_id
            or spec.master_run_ref != authority.master_run_ref
            or spec.epoch != authority.epoch
            or self.control.current_epoch("postgres-master") != authority.epoch
            or self.control.clock.now() >= authority.lease_until
            or not self.ledger.runtime_service_authorized(
                service_instance_id=authority.service_instance_id,
                run_id=authority.run_id,
                attempt_id=authority.attempt_id,
                master_instance_id=authority.master_instance_id,
                epoch=authority.epoch,
            )
        ):
            raise LeaseRejected("checkpoint upload authority is fenced or expired")
        operation = self.control.get_operation(authority.operation_id)
        allowed_operation_states = (
            {"INTENT_COMMITTED", "RUNNING"}
            if authority.authority_kind == "acceptance"
            else (
                {"DRAINING", "CHECKPOINTING", "CHECKPOINT_FAILED", "ACTIVE"}
                if durable_recovery
                else {"DRAINING", "CHECKPOINTING", "ACTIVE"}
            )
        )
        if operation is None or operation.state not in allowed_operation_states:
            raise BrokeredCheckpointError("checkpoint operation is not in an upload-capable phase")
        candidate = self.control.checkpoint_candidate(str(spec.checkpoint_id))
        if candidate is None or candidate["operation_id"] != authority.operation_id:
            raise BrokeredCheckpointError("checkpoint candidate does not belong to this operation")
        manifest_value = candidate.get("manifest")
        if not isinstance(manifest_value, dict):
            raise BrokeredCheckpointError("checkpoint candidate manifest is unavailable")
        manifest = CheckpointManifest.from_payload(manifest_value)
        if (
            manifest.manifest_sha256 != spec.manifest_sha256
            or manifest.master_instance_id != UUID(authority.master_instance_id)
            or manifest.epoch != authority.epoch
            or manifest.source_run_id != authority.run_id
        ):
            raise BrokeredCheckpointError("checkpoint manifest differs from runtime authority")
        expected = {item.path: (item.byte_size, item.sha256) for item in manifest.files}
        manifest_bytes = canonical_json(manifest.payload()) + b"\n"
        expected[CHECKPOINT_MANIFEST_NAME] = (len(manifest_bytes), hashlib.sha256(manifest_bytes).hexdigest())
        observed = (spec.content_length, spec.content_sha256)
        fixed_fm14_corruption = (
            authority.authority_kind == "acceptance"
            and authority.acceptance_scenario == "FM14"
            and spec.file_name == "physical/base.tar.gz"
            and expected.get(spec.file_name) is not None
            and expected[spec.file_name][0] == spec.content_length
            and expected[spec.file_name][1] != spec.content_sha256
        )
        if expected.get(spec.file_name) != observed and not fixed_fm14_corruption:
            raise BrokeredCheckpointError("checkpoint blob metadata differs from its manifest")
        return candidate, manifest

    def _validate_acceptance_authority(self, spec: CheckpointBlobSpec, authority: RuntimeUploadAuthority) -> None:
        if (
            authority.acceptance_scenario not in {"FM05", "FM14", "FM15"}
            or not authority.source_revision
            or str(spec.operation_id) != authority.operation_id
            or spec.master_run_ref != authority.master_run_ref
            or spec.epoch != authority.epoch
            or self.control.clock.now() >= authority.lease_until
        ):
            raise LeaseRejected("checkpoint acceptance upload authority is stale")
        launch = self.control.checkpoint_acceptance_launch(authority.run_id)
        if launch is None or launch.get("result") is not None:
            raise LeaseRejected("checkpoint acceptance launch is not active")
        config = launch.get("config")
        request = launch.get("request")
        provider_run = launch.get("provider_run")
        if not isinstance(config, dict) or not isinstance(request, dict) or not isinstance(provider_run, dict):
            raise LeaseRejected("checkpoint acceptance launch binding is unavailable")
        expected_verifier = config.get("verifier")
        if (
            str(launch.get("operation_id")) != authority.operation_id
            or str(launch.get("task_run_id")) != authority.run_id
            or str(launch.get("attempt_id")) != authority.attempt_id
            or str(provider_run.get("provider_run_ref")) != authority.master_run_ref
            or str(request.get("scenario")) != authority.acceptance_scenario
            or str(request.get("source_revision")) != authority.source_revision
            or str(request.get("candidate_dataset_ref")) != str(config.get("dataset_ref"))
            or (expected_verifier or {}).get("dataset_version_ref") != authority.verifier_dataset_version_ref
            or config.get("verifier_notebook_ref") != authority.verifier_notebook_ref
        ):
            raise LeaseRejected("checkpoint acceptance launch binding changed")

    @staticmethod
    def _public_grant(
        spec: CheckpointBlobSpec, claim_id: UUID, expires_at: datetime, create_url: str
    ) -> CheckpointBlobGrant:
        return CheckpointBlobGrant(
            claim_id=claim_id,
            checkpoint_id=spec.checkpoint_id,
            file_name=spec.file_name,
            content_length=spec.content_length,
            content_sha256=spec.content_sha256,
            expires_at=expires_at,
            create_url=create_url,
        )
