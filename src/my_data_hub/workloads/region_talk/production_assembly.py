"""Concrete central adapter assembly for the private Region Talk supervisor.

The SQLite journal receives metadata only.  PostgreSQL credentials, callback
tokens and SSH private keys are kept in a dedicated 0700 capability directory
until exact task cleanup, while provider effects all use the one injected
``KaggleProviderAdapter`` instance.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import secrets
import tempfile
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from pydantic import SecretStr

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.adapter import mapping_sha256
from my_data_hub.providers.kaggle.contracts import (
    KaggleAmbiguousMutation,
    MutationAction,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass

from .central_launcher import RegionTalkSupervisorCapability, render_region_talk_supervisor_source
from .pipeline_contracts import (
    RegionTalkAccessBinding,
    RegionTalkCleanupReceipt,
    RegionTalkDirectMasterAccess,
    RegionTalkLaunchMetadata,
    RegionTalkLaunchReceipt,
    RegionTalkRunSnapshot,
    TaskWorkerCredentialBatch,
    TaskWorkerCredentialCommand,
    TaskWorkerCredentialRegistration,
    TaskWorkerCredentialRegistrationResponse,
    TaskWorkerCredentialRevocation,
)
from .pipeline_runtime import LaunchObservation, LaunchObservationKind

_MAX_SECRET_BYTES = 1024 * 1024


class RegionTalkAssemblyUnavailable(RuntimeError):
    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


def _atomic(path: Path, body: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(slots=True)
class DirectoryRegionTalkTaskAuthority:
    """Exact generic-task command mailbox and private capability publisher."""

    root: Path
    broker: Any
    tls_ca_path: Path
    known_hosts_path: Path
    gateway_host: str
    gateway_port: int
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    wait_seconds: float = 45.0

    def __post_init__(self) -> None:
        if not self.root.is_absolute() or self.root.is_symlink():
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CAPABILITY_ROOT_UNSAFE")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        if self.root.stat().st_mode & 0o077:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CAPABILITY_ROOT_UNSAFE")
        for path in (self.tls_ca_path, self.known_hosts_path):
            if not path.is_absolute() or path.is_symlink() or not path.is_file():
                raise RegionTalkAssemblyUnavailable("REGION_TALK_TUNNEL_PINS_UNAVAILABLE")
        if not self.gateway_host or not 1 <= self.gateway_port <= 65535:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_TUNNEL_ENDPOINT_INVALID")

    def prepare(
        self, metadata: RegionTalkLaunchMetadata, *, task_token: str, generation: int = 1
    ) -> TaskWorkerCredentialCommand:
        token_sha = hashlib.sha256(task_token.encode()).hexdigest()
        command = TaskWorkerCredentialCommand.create(
            task_run_id=metadata.task_run_id,
            epoch=metadata.master.epoch,
            generation=generation,
            task_token_sha256=token_sha,
        )
        payload = {
            "schema_version": "region-talk-private-command.v1",
            "request_id": str(metadata.request_id),
            "master_instance_id": str(metadata.master.master_instance_id),
            "master_run_id": str(metadata.master.run_id),
            "master_attempt_id": str(metadata.master.attempt_id),
            "command": command.model_dump(mode="json"),
            "task_token": task_token,
            "created_at": self.clock().astimezone(UTC).isoformat(),
        }
        path = self._task_path(metadata.task_run_id)
        encoded = canonical_json_bytes(payload)
        if path.exists():
            existing = self._read(path)
            if existing.get("command") != payload["command"]:
                raise RegionTalkAssemblyUnavailable("REGION_TALK_TASK_COMMAND_CONFLICT")
            return command
        _atomic(path, encoded)
        return command

    def batch(self, *, master_instance_id: UUID, epoch: int) -> TaskWorkerCredentialBatch:
        commands: list[TaskWorkerCredentialCommand] = []
        revocations: list[TaskWorkerCredentialRevocation] = []
        for path in sorted(self.root.glob("*.task.json")):
            value = self._read(path)
            if value.get("master_instance_id") != str(master_instance_id):
                continue
            command = TaskWorkerCredentialCommand.model_validate(value.get("command"))
            if command.epoch == epoch and not self._registration_path(command.task_run_id).exists():
                commands.append(command)
        for path in sorted(self.root.glob("*.revoke.json")):
            value = self._read(path)
            revoke = TaskWorkerCredentialRevocation.model_validate(value)
            if revoke.epoch == epoch:
                revocations.append(revoke)
        revoked_tasks = {item.task_run_id for item in revocations}
        commands = [item for item in commands if item.task_run_id not in revoked_tasks]
        batch = TaskWorkerCredentialBatch(commands=tuple(commands), revocations=tuple(revocations))
        # The master protocol has no separate revocation-ack message.  The SSH
        # certificate was already revoked synchronously and the DB LOGIN is
        # short-lived; consume the command only after constructing the exact
        # response to avoid poisoning every subsequent poll.
        for revoke in revocations:
            path = self.root / f"{revoke.task_run_id.hex}.{revoke.generation}.revoke.json"
            path.unlink(missing_ok=True)
            self._task_path(revoke.task_run_id).unlink(missing_ok=True)
        return batch

    def register(
        self, registration: TaskWorkerCredentialRegistration
    ) -> TaskWorkerCredentialRegistrationResponse:
        task = self._read(self._task_path(registration.task_run_id))
        command = TaskWorkerCredentialCommand.model_validate(task.get("command"))
        if (
            registration.master_instance_id != UUID(str(task["master_instance_id"]))
            or registration.epoch != command.epoch
            or registration.generation != command.generation
            or registration.task_token_sha256 != command.task_token_sha256
            or registration.command_sha256 != command.command_sha256
            or registration.worker_kind != "region_talk"
            or registration.role != "region_talk_pipeline"
            or registration.expires_at <= self.clock().astimezone(UTC)
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CREDENTIAL_BINDING_INVALID")
        path = self._registration_path(registration.task_run_id)
        registration_value = registration.model_dump(mode="json")
        registration_value["database_url"] = registration.database_url.get_secret_value()
        encoded = canonical_json_bytes(registration_value)
        if path.exists() and path.read_bytes() != encoded:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CREDENTIAL_REGISTRATION_CONFLICT")
        _atomic(path, encoded)
        return TaskWorkerCredentialRegistrationResponse(
            task_run_id=registration.task_run_id,
            epoch=registration.epoch,
            generation=registration.generation,
            credential_id=registration.credential_id,
            command_sha256=registration.command_sha256,
        )

    def await_access(
        self, metadata: RegionTalkLaunchMetadata, command: TaskWorkerCredentialCommand
    ) -> RegionTalkDirectMasterAccess:
        path = self._registration_path(metadata.task_run_id)
        deadline = time.monotonic() + self.wait_seconds
        while not path.is_file() and time.monotonic() < deadline:
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
        if not path.is_file():
            raise RegionTalkAssemblyUnavailable("REGION_TALK_TASK_CREDENTIAL_PENDING")
        registration = TaskWorkerCredentialRegistration.model_validate(self._read(path))
        if (
            registration.task_run_id != metadata.task_run_id
            or registration.master_instance_id != metadata.master.master_instance_id
            or registration.epoch != metadata.master.epoch
            or registration.command_sha256 != command.command_sha256
            or registration.expires_at
            <= self.clock().astimezone(UTC)
            + timedelta(seconds=metadata.max_runtime_seconds + 15)
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CREDENTIAL_BINDING_INVALID")
        key = Ed25519PrivateKey.generate()
        private = key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.OpenSSH,
            serialization.NoEncryption(),
        ).decode()
        public = key.public_key().public_bytes(
            serialization.Encoding.OpenSSH, serialization.PublicFormat.OpenSSH
        ).decode()
        certificate = self.broker.issue_task_worker_public_key(
            master_instance_id=str(registration.master_instance_id),
            epoch=registration.epoch,
            worker_kind="region_talk",
            task_run_id=str(registration.task_run_id),
            credential_id=str(registration.credential_id),
            generation=registration.generation,
            binding_sha256=registration.command_sha256,
            public_key=public,
            valid_before=registration.expires_at,
            now=self.clock(),
        )
        return RegionTalkDirectMasterAccess(
            credential_id=registration.credential_id,
            task_run_id=registration.task_run_id,
            master_instance_id=registration.master_instance_id,
            epoch=registration.epoch,
            generation=registration.generation,
            command_sha256=registration.command_sha256,
            task_token_sha256=registration.task_token_sha256,
            database_url=registration.database_url,
            tls_ca_pem=self.tls_ca_path.read_text(encoding="ascii"),
            expires_at=registration.expires_at,
            tunnel_endpoint="127.0.0.1:25432",
            ssh_private_key=private,
            ssh_certificate=certificate.certificate,
            ssh_known_hosts=self.known_hosts_path.read_text(encoding="utf-8"),
            ssh_gateway_host=self.gateway_host,
            ssh_gateway_port=self.gateway_port,
            ssh_account=certificate.account,
            ssh_certificate_serial=certificate.serial,
        )

    def validate_token(self, task_run_id: UUID, supplied: str) -> Mapping[str, Any]:
        task = self._read(self._task_path(task_run_id))
        token = str(task.get("task_token", ""))
        if not token or not hmac.compare_digest(token, supplied):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_TASK_TOKEN_INVALID")
        return task

    def task_token(self, task_run_id: UUID) -> str:
        value = str(self._read(self._task_path(task_run_id)).get("task_token", ""))
        if not 32 <= len(value) <= 256:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_TASK_TOKEN_INVALID")
        return value

    def request_revocation(self, run: RegionTalkRunSnapshot) -> None:
        if run.task_run_id is None or run.master is None or run.access is None:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CLEANUP_BINDING_MISSING")
        revoke = TaskWorkerCredentialRevocation(
            worker_kind="region_talk",
            task_run_id=run.task_run_id,
            epoch=run.master.epoch,
            generation=run.access.generation,
            task_token_sha256=run.access.task_token_sha256,
            command_sha256=run.access.command_sha256,
            credential_id=run.access.credential_id,
            reason="region_talk_terminal",
        )
        _atomic(
            self.root / f"{run.task_run_id.hex}.{run.access.generation}.revoke.json",
            canonical_json_bytes(revoke.model_dump(mode="json")),
        )
        self.broker.revoke_task_worker_certificate(
            master_instance_id=str(run.master.master_instance_id),
            epoch=run.master.epoch,
            worker_kind="region_talk",
            task_run_id=str(run.task_run_id),
            credential_id=str(run.access.credential_id),
            generation=run.access.generation,
            binding_sha256=run.access.command_sha256,
            serial=run.access.ssh_certificate_serial,
            reason="region_talk_terminal",
        )

    def purge(self, task_run_id: UUID) -> None:
        # Revocation stays until the ACTIVE master consumes it.  Only the
        # callback token and plaintext registration are removed here.
        for path in (self._registration_path(task_run_id),):
            if path.is_file() and not path.is_symlink():
                path.unlink()

    def _task_path(self, task_run_id: UUID) -> Path:
        return self.root / f"{task_run_id.hex}.task.json"

    def _registration_path(self, task_run_id: UUID) -> Path:
        return self.root / f"{task_run_id.hex}.registration.json"

    @staticmethod
    def _read(path: Path) -> dict[str, Any]:
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_mode & 0o077
            or path.stat().st_size > _MAX_SECRET_BYTES
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_PRIVATE_STATE_INVALID")
        value = json.loads(path.read_bytes())
        if not isinstance(value, dict):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_PRIVATE_STATE_INVALID")
        return value


@dataclass(slots=True)
class CentralRegionTalkNotebookAdapter:
    """Deterministic private Dataset/Notebook launcher and cleanup adapter."""

    adapter: Any
    authority: DirectoryRegionTalkTaskAuthority
    owner: str
    callback_base_url: str
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    journal_path: Path | None = None
    _receipts: dict[UUID, RegionTalkLaunchReceipt] = field(default_factory=dict)
    _claims: dict[UUID, tuple[TaskResourceClaim, TaskResourceClaim]] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.journal_path is None:
            self.journal_path = self.authority.root / "launcher-metadata.json"
        self._load_journal()

    def observe(self, metadata: RegionTalkLaunchMetadata) -> LaunchObservation:
        receipt = self._receipts.get(metadata.task_run_id)
        return LaunchObservation(
            LaunchObservationKind.PRESENT if receipt else LaunchObservationKind.ABSENT,
            receipt,
        )

    def launch(self, metadata: RegionTalkLaunchMetadata) -> RegionTalkLaunchReceipt:
        existing = self._receipts.get(metadata.task_run_id)
        if existing is not None:
            return existing
        now = self.clock().astimezone(UTC)
        task_token = secrets.token_urlsafe(36)
        command = self.authority.prepare(metadata, task_token=task_token)
        task_token = self.authority.task_token(metadata.task_run_id)
        access = self.authority.await_access(metadata, command)
        capability = RegionTalkSupervisorCapability(
            launch=metadata,
            direct_access=access,
            callback_base_url=self.callback_base_url,
            task_token=SecretStr(task_token),
            task_token_sha256=command.task_token_sha256,
        )
        status_ref = f"{self.owner}/mdh-region-talk-{metadata.task_run_id.hex[:20]}"
        notebook_ref = f"{self.owner}/mdh-region-talk-run-{metadata.task_run_id.hex[:16]}"
        files = {
            "region-talk-supervisor.json": capability.private_dataset_bytes(),
            "execution-pins.json": canonical_json_bytes(
                {
                    "schema": "my-data-hub-notebook-execution-pins/v1",
                    "notebook": notebook_ref,
                    "runtime_dataset_exact_ref": metadata.runtime_dataset_exact_ref,
                    "runtime_image_identity": metadata.runtime_image_identity,
                    "runtime_image_source_commit": metadata.runtime_image_source_commit,
                    "wheel_relative_path": metadata.wheel_relative_path,
                    "wheel_sha256": metadata.wheel_sha256,
                    "publication_dispatch": False,
                    "privacy": "private",
                }
            ),
        }
        operation_id = uuid5(NAMESPACE_URL, f"region-talk-supervisor:{metadata.request_id}")
        create = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"region-talk-status:{metadata.task_run_id}"),
            idempotency_key=f"region-talk-status:{metadata.task_run_id}",
            task_id=metadata.task_run_id,
            action=MutationAction.CREATE_DATASET,
            provider_ref=status_ref,
            arguments={
                "content_tree_sha256": mapping_sha256(files),
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": True,
            },
            requested_at=now,
        )
        dataset = self.adapter.create_private_dataset(
            intent=create,
            files=files,
            title=status_ref.split("/", 1)[1],
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=True,
        )
        exact_status = f"{status_ref}/{int(dataset.claim.provider_version)}"
        source = render_region_talk_supervisor_source(metadata)
        source_sha = hashlib.sha256(source).hexdigest()
        sources = (metadata.runtime_dataset_exact_ref, exact_status)
        push = ProviderEffectIntent.create(
            operation_id=operation_id,
            effect_id=uuid5(NAMESPACE_URL, f"region-talk-run:{metadata.task_run_id}"),
            idempotency_key=f"region-talk-run:{metadata.task_run_id}",
            task_id=metadata.task_run_id,
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=notebook_ref,
            arguments={
                "task_run_id": str(metadata.task_run_id),
                "source_sha256": source_sha,
                "dataset_sources": sources,
                "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
                "disposable": True,
                "docker_image": metadata.runtime_image_identity,
                "docker_image_pinning_type": "original",
            },
            requested_at=now,
        )
        try:
            launched = self.adapter.push_private_worker_notebook_pending_attestation(
                intent=push,
                task_run_id=metadata.task_run_id,
                source=source,
                title=notebook_ref.split("/", 1)[1],
                code_file="region_talk_supervisor.py",
                kernel_type="script",
                language="python",
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=True,
                dataset_sources=sources,
                enable_internet=True,
                timeout_seconds=metadata.max_runtime_seconds,
                docker_image=metadata.runtime_image_identity,
                docker_image_pinning_type="original",
            )
        except KaggleAmbiguousMutation:
            raise
        receipt = RegionTalkLaunchReceipt(
            task_run_id=metadata.task_run_id,
            master_instance_id=metadata.master.master_instance_id,
            epoch=metadata.master.epoch,
            source_sha256=source_sha,
            status_dataset_exact_ref=exact_status,
            provider_run_ref=launched.run.provider_run_ref,
            access=RegionTalkAccessBinding(
                credential_id=access.credential_id,
                generation=access.generation,
                command_sha256=access.command_sha256,
                task_token_sha256=access.task_token_sha256,
                expires_at=access.expires_at,
                ssh_certificate_serial=access.ssh_certificate_serial,
            ),
        )
        self._receipts[metadata.task_run_id] = receipt
        self._claims[metadata.task_run_id] = (dataset.claim, launched.claim)
        self._save_journal()
        return receipt

    def cleanup(self, run: RegionTalkRunSnapshot) -> RegionTalkCleanupReceipt:
        if run.task_run_id is None or run.access is None:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CLEANUP_BINDING_MISSING")
        self.authority.request_revocation(run)
        claims = self._claims.get(run.task_run_id, ())
        deleted = 0
        for label, claim, action in (
            ("notebook", claims[1] if len(claims) == 2 else None, MutationAction.DELETE_NOTEBOOK),
            ("status", claims[0] if len(claims) == 2 else None, MutationAction.DELETE_DATASET),
        ):
            if claim is None:
                continue
            intent = ProviderEffectIntent.create(
                operation_id=uuid5(NAMESPACE_URL, f"region-talk-cleanup:{run.task_run_id}"),
                effect_id=uuid5(NAMESPACE_URL, f"region-talk-cleanup:{label}:{run.task_run_id}"),
                idempotency_key=f"region-talk-cleanup:{label}:{run.task_run_id}",
                task_id=run.task_run_id,
                action=action,
                provider_ref=claim.provider_ref,
                expected_fingerprint=claim.fingerprint,
                arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
                requested_at=self.clock(),
            )
            self.adapter.delete_task_created_resource(intent=intent, claim=claim)
            deleted += 1
        cleaned_at = self.clock().astimezone(UTC)
        base = {
            "task_run_id": str(run.task_run_id),
            "credential_id": str(run.access.credential_id),
            "generation": run.access.generation,
            "command_sha256": run.access.command_sha256,
            "task_token_sha256": run.access.task_token_sha256,
            "ssh_certificate_serial": run.access.ssh_certificate_serial,
            "resources_deleted": deleted,
            "cleaned_at": cleaned_at.isoformat(),
        }
        receipt = RegionTalkCleanupReceipt(
            **base,
            receipt_sha256=hashlib.sha256(canonical_json_bytes(base)).hexdigest(),
        )
        self._receipts.pop(run.task_run_id, None)
        self._claims.pop(run.task_run_id, None)
        self._save_journal()
        self.authority.purge(run.task_run_id)
        return receipt

    def _save_journal(self) -> None:
        assert self.journal_path is not None
        if not self.journal_path.is_absolute() or self.journal_path.is_symlink():
            raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_UNSAFE")
        value = {
            "schema_version": "region-talk-launch-journal.v1",
            "receipts": {
                str(task): receipt.model_dump(mode="json")
                for task, receipt in sorted(self._receipts.items(), key=lambda item: str(item[0]))
            },
            "claims": {
                str(task): [claim.model_dump(mode="json") for claim in claims]
                for task, claims in sorted(self._claims.items(), key=lambda item: str(item[0]))
            },
        }
        encoded = canonical_json_bytes(value)
        if len(encoded) > _MAX_SECRET_BYTES:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_OVERSIZED")
        _atomic(self.journal_path, encoded)

    def _load_journal(self) -> None:
        assert self.journal_path is not None
        if not self.journal_path.exists():
            return
        value = DirectoryRegionTalkTaskAuthority._read(self.journal_path)
        if set(value) != {"schema_version", "receipts", "claims"} or value.get(
            "schema_version"
        ) != "region-talk-launch-journal.v1":
            raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
        receipts = value.get("receipts")
        claims = value.get("claims")
        if not isinstance(receipts, dict) or not isinstance(claims, dict):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
        for task, receipt in receipts.items():
            task_id = UUID(task)
            parsed = RegionTalkLaunchReceipt.model_validate(receipt)
            if parsed.task_run_id != task_id:
                raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
            self._receipts[task_id] = parsed
        for task, values in claims.items():
            task_id = UUID(task)
            if not isinstance(values, list) or len(values) != 2:
                raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
            self._claims[task_id] = tuple(
                TaskResourceClaim.model_validate(item) for item in values
            )  # type: ignore[assignment]


@dataclass(frozen=True, slots=True)
class RegionTalkAssemblySettings:
    enabled: bool
    schedule_enabled: bool
    owner: str = ""
    callback_base_url: str = ""
    runtime_image_identity: str = ""
    runtime_image_source_commit: str = ""
    wheel_relative_path: str = ""
    wheel_sha256: str = ""
    capability_root: Path = Path("/nonexistent")

    @classmethod
    def from_env(cls) -> RegionTalkAssemblySettings:
        def flag(name: str) -> bool:
            value = os.getenv(name, "false").strip().lower()
            if value in {"1", "true", "yes", "on"}:
                return True
            if value in {"0", "false", "no", "off"}:
                return False
            raise RegionTalkAssemblyUnavailable(f"{name}_INVALID")

        return cls(
            enabled=flag("MY_DATA_HUB_REGION_TALK_PIPELINE_ENABLED"),
            schedule_enabled=flag("MY_DATA_HUB_REGION_TALK_SCHEDULE_ENABLED"),
            owner=os.getenv("MY_DATA_HUB_KAGGLE_OWNER", "").strip(),
            callback_base_url=os.getenv("MY_DATA_HUB_CALLBACK_URL", "").strip(),
            runtime_image_identity=os.getenv("MY_DATA_HUB_REGION_TALK_RUNTIME_IMAGE_IDENTITY", "").strip(),
            runtime_image_source_commit=os.getenv("MY_DATA_HUB_REGION_TALK_RUNTIME_SOURCE_COMMIT", "").strip(),
            wheel_relative_path=os.getenv("MY_DATA_HUB_REGION_TALK_WHEEL_RELATIVE_PATH", "").strip(),
            wheel_sha256=os.getenv("MY_DATA_HUB_REGION_TALK_WHEEL_SHA256", "").strip(),
            capability_root=Path(
                os.getenv("MY_DATA_HUB_REGION_TALK_CAPABILITY_DIR", "/state/region-talk-private")
            ),
        )

    def validate(self) -> None:
        if not self.enabled:
            return
        if (
            not self.owner
            or not self.callback_base_url.startswith("https://")
            or not re.fullmatch(r"[^@\s]+@sha256:[a-f0-9]{64}", self.runtime_image_identity)
            or not re.fullmatch(r"[a-f0-9]{40}", self.runtime_image_source_commit)
            or not self.wheel_relative_path
            or not re.fullmatch(r"[a-f0-9]{64}", self.wheel_sha256)
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_ASSEMBLY_ENVIRONMENT_INCOMPLETE")


__all__ = [
    "CentralRegionTalkNotebookAdapter",
    "DirectoryRegionTalkTaskAuthority",
    "RegionTalkAssemblySettings",
    "RegionTalkAssemblyUnavailable",
    "RegionTalkMCPController",
]


@dataclass(slots=True)
class RegionTalkMCPController:
    """Small authenticated MCP facade over metadata-only coordinator state."""

    coordinator: Any

    @staticmethod
    def _public(snapshot: Any) -> dict[str, Any]:
        if snapshot is None:
            return {
                "ready": True,
                "state": "IDLE",
                "publication_dispatch": False,
                "latest": None,
            }
        value = snapshot.model_dump(mode="json")
        # Access contains identifiers/hashes only, but MCP does not need even
        # those credential lifecycle internals.
        value.pop("access", None)
        return {
            "ready": True,
            "state": value["state"],
            "publication_dispatch": False,
            "latest": value,
        }

    def status(self) -> dict[str, Any]:
        return self._public(self.coordinator.status())

    def request_supervised_run(self, *, request: Any, principal: Any) -> dict[str, Any]:
        if "region-talk:operate" not in principal.scopes:
            raise PermissionError("Region Talk supervised run requires operator scope")
        snapshot, created = self.coordinator.request_supervised(
            idempotency_key=request.idempotency_key,
            source_revision=request.source_revision,
        )
        return {
            "operation_id": str(snapshot.request.request_id),
            "duplicate": not created,
            "state": snapshot.state.value,
            "idempotency_key": request.idempotency_key,
            "publication_dispatch": False,
        }
