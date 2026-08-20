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
    MutationAction,
    ProviderEffectIntent,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass

from .central_launcher import RegionTalkSupervisorCapability, render_region_talk_supervisor_source
from .pipeline_contracts import (
    RegionTalkAccessBinding,
    RegionTalkCleanupReceipt,
    RegionTalkCredentialActivation,
    RegionTalkCredentialRefreshRequest,
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
from .stage_dispatch import StageWorkerCredentialStatus, StageWorkMetadataClaimReceipt

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
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
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
        self,
        metadata: RegionTalkLaunchMetadata,
        *,
        task_token: str,
        source_sha256: str,
        generation: int = 1,
    ) -> TaskWorkerCredentialCommand:
        path = self._task_path(metadata.task_run_id)
        if path.exists():
            existing = self._read(path)
            self._validate_task_metadata(existing, metadata, source_sha256=source_sha256)
            return TaskWorkerCredentialCommand.model_validate(existing.get("command"))
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
            "source_sha256": source_sha256,
            "image_identity": metadata.runtime_image_identity,
            "image_source_commit": metadata.runtime_image_source_commit,
            "launch": metadata.model_dump(mode="json"),
            "command": command.model_dump(mode="json"),
            "activated_generation": generation,
            "bindings": {},
            "revoked_generations": [],
            "terminal_revoked_generations": [],
            "expired_generations": [],
            "terminal": False,
            "task_token": task_token,
            "created_at": self.clock().astimezone(UTC).isoformat(),
        }
        encoded = canonical_json_bytes(payload)
        _atomic(path, encoded)
        return command

    def prepare_stage_worker(
        self,
        claim: StageWorkMetadataClaimReceipt,
        *,
        task_token: str,
        source_sha256: str,
        image_identity: str,
        image_source_commit: str,
        generation: int = 1,
    ) -> TaskWorkerCredentialCommand:
        """Publish a child command while persisting only bounded claim metadata."""

        path = self._task_path(claim.worker_task_run_id)
        if path.exists():
            existing = self._read(path)
            if (
                existing.get("schema_version") != "region-talk-private-stage-command.v1"
                or existing.get("master_instance_id") != str(claim.master_instance_id)
                or existing.get("epoch") != claim.epoch
                or existing.get("source_sha256") != source_sha256
                or existing.get("image_identity") != image_identity
                or existing.get("image_source_commit") != image_source_commit
                or existing.get("claim") != claim.model_dump(mode="json")
            ):
                raise RegionTalkAssemblyUnavailable("REGION_TALK_STAGE_COMMAND_CONFLICT")
            return TaskWorkerCredentialCommand.model_validate(existing.get("command"))
        token_sha = hashlib.sha256(task_token.encode()).hexdigest()
        command = TaskWorkerCredentialCommand.create(
            task_run_id=claim.worker_task_run_id,
            epoch=claim.epoch,
            generation=generation,
            task_token_sha256=token_sha,
        )
        payload = {
            "schema_version": "region-talk-private-stage-command.v1",
            "master_instance_id": str(claim.master_instance_id),
            "epoch": claim.epoch,
            "source_sha256": source_sha256,
            "image_identity": image_identity,
            "image_source_commit": image_source_commit,
            "claim": claim.model_dump(mode="json"),
            "command": command.model_dump(mode="json"),
            "activated_generation": generation,
            "bindings": {},
            "revoked_generations": [],
            "terminal_revoked_generations": [],
            "expired_generations": [],
            "terminal": False,
            "task_token": task_token,
            "created_at": self.clock().astimezone(UTC).isoformat(),
        }
        _atomic(path, canonical_json_bytes(payload))
        return command

    def batch(self, *, master_instance_id: UUID, epoch: int) -> TaskWorkerCredentialBatch:
        self._reap_expired_sidecars()
        commands: list[TaskWorkerCredentialCommand] = []
        revocations: list[TaskWorkerCredentialRevocation] = []
        for path in sorted(self.root.glob("*.task.json")):
            value = self._read(path)
            if value.get("master_instance_id") != str(master_instance_id):
                continue
            command = TaskWorkerCredentialCommand.model_validate(value.get("command"))
            if (
                command.epoch == epoch
                and not bool(value.get("terminal"))
                and command.generation
                not in {int(item) for item in value.get("expired_generations", [])}
                and not self._registration_ack_path(
                    command.task_run_id, command.generation
                ).exists()
            ):
                commands.append(command)
        for path in sorted(self.root.glob("*.revoke.json")):
            value = self._read(path)
            revoke = TaskWorkerCredentialRevocation.model_validate(value)
            if revoke.epoch == epoch:
                revocations.append(revoke)
        # Revocations remain until the ACTIVE master explicitly acknowledges
        # them.  A lost GET response therefore replays the same exact binding.
        return TaskWorkerCredentialBatch(
            commands=tuple(commands), revocations=tuple(revocations)
        )

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
        path = self._registration_path(
            registration.task_run_id, registration.generation
        )
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
        return self._await_access_identity(
            task_run_id=metadata.task_run_id,
            master_instance_id=metadata.master.master_instance_id,
            epoch=metadata.master.epoch,
            command=command,
        )

    def await_stage_worker_access(
        self,
        claim: StageWorkMetadataClaimReceipt,
        command: TaskWorkerCredentialCommand,
    ) -> RegionTalkDirectMasterAccess:
        return self._await_access_identity(
            task_run_id=claim.worker_task_run_id,
            master_instance_id=claim.master_instance_id,
            epoch=claim.epoch,
            command=command,
        )

    def stage_worker_registration(
        self,
        claim: StageWorkMetadataClaimReceipt,
        command: TaskWorkerCredentialCommand,
    ) -> TaskWorkerCredentialRegistration | None:
        path = self._registration_path(claim.worker_task_run_id, command.generation)
        ack_path = self._registration_ack_path(claim.worker_task_run_id, command.generation)
        if not path.is_file() or not ack_path.is_file():
            return None
        registration = TaskWorkerCredentialRegistration.model_validate(self._read(path))
        if (
            registration.task_run_id != claim.worker_task_run_id
            or registration.master_instance_id != claim.master_instance_id
            or registration.epoch != claim.epoch
            or registration.command_sha256 != command.command_sha256
            or registration.task_token_sha256 != command.task_token_sha256
            or registration.expires_at <= self.clock().astimezone(UTC) + timedelta(seconds=60)
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CREDENTIAL_BINDING_INVALID")
        return registration

    def _await_access_identity(
        self,
        *,
        task_run_id: UUID,
        master_instance_id: UUID,
        epoch: int,
        command: TaskWorkerCredentialCommand,
    ) -> RegionTalkDirectMasterAccess:
        access_path = self._access_path(task_run_id, command.generation)
        if access_path.exists():
            return RegionTalkDirectMasterAccess.model_validate(self._read(access_path))
        path = self._registration_path(task_run_id, command.generation)
        ack_path = self._registration_ack_path(task_run_id, command.generation)
        deadline = time.monotonic() + self.wait_seconds
        while (not path.is_file() or not ack_path.is_file()) and time.monotonic() < deadline:
            time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
        if not path.is_file() or not ack_path.is_file():
            raise RegionTalkAssemblyUnavailable("REGION_TALK_TASK_CREDENTIAL_PENDING")
        registration = TaskWorkerCredentialRegistration.model_validate(self._read(path))
        if (
            registration.task_run_id != task_run_id
            or registration.master_instance_id != master_instance_id
            or registration.epoch != epoch
            or registration.command_sha256 != command.command_sha256
            or registration.expires_at
            <= self.clock().astimezone(UTC) + timedelta(seconds=60)
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
        access = RegionTalkDirectMasterAccess(
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
        encoded = self.private_access_bytes(access)
        _atomic(access_path, encoded)
        self._record_binding(access)
        return access

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

    @staticmethod
    def private_access_bytes(access: RegionTalkDirectMasterAccess) -> bytes:
        value = access.model_dump(mode="json")
        for field_name in (
            "database_url",
            "tls_ca_pem",
            "ssh_private_key",
            "ssh_certificate",
            "ssh_known_hosts",
        ):
            value[field_name] = getattr(access, field_name).get_secret_value()
        return canonical_json_bytes(value)

    def refresh(
        self,
        request: RegionTalkCredentialRefreshRequest,
    ) -> RegionTalkDirectMasterAccess:
        """Return the exact next private generation, replaying response loss."""

        task_path = self._task_path(request.task_run_id)
        task = self._read(task_path)
        self._validate_refresh_binding(task, request)
        command = TaskWorkerCredentialCommand.model_validate(task.get("command"))
        next_generation = request.previous.generation + 1
        if command.generation == request.previous.generation:
            command = TaskWorkerCredentialCommand.create(
                task_run_id=request.task_run_id,
                epoch=request.epoch,
                generation=next_generation,
                task_token_sha256=request.previous.task_token_sha256,
            )
            task["command"] = command.model_dump(mode="json")
            _atomic(task_path, canonical_json_bytes(task))
        elif command.generation != next_generation:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_GENERATION_CONFLICT"
            )
        if next_generation in {
            int(value) for value in task.get("expired_generations", [])
        }:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_REPLACEMENT_EXPIRED"
            )
        access_path = self._access_path(request.task_run_id, next_generation)
        if access_path.exists():
            access = RegionTalkDirectMasterAccess.model_validate(self._read(access_path))
            if access.expires_at > self.clock().astimezone(UTC):
                self._validate_replacement(request, access)
                return access
            # A generation is immutable in the authoritative PostgreSQL task
            # binding table.  It cannot be reissued with a fresh credential
            # after expiry.  Purge its secrets, retain a non-secret tombstone,
            # and fail closed so a later supervised task gets a new identity.
            self._expire_generation(access)
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_REPLACEMENT_EXPIRED"
            )
        metadata = self._launch_metadata(task)
        access = self.await_access(metadata, command)
        self._validate_replacement(request, access)
        return access

    def activate(self, activation: RegionTalkCredentialActivation) -> None:
        """Publish the previous exact revocation only after worker DB proof."""

        task_path = self._task_path(activation.task_run_id)
        task = self._read(task_path)
        self._validate_activation_binding(task, activation)
        if self._task_binding(task, activation.previous.generation) != activation.previous or self._task_binding(
            task, activation.replacement.generation
        ) != activation.replacement:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_ACTIVATION_CONFLICT"
            )
        activated = int(task.get("activated_generation", 0))
        if activated not in {
            activation.previous.generation,
            activation.replacement.generation,
        }:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_ACTIVATION_CONFLICT"
            )
        revoked = {int(value) for value in task.get("revoked_generations", [])}
        if (
            activated == activation.replacement.generation
            and activation.previous.generation in revoked
        ):
            return
        # Persist the master-polled revocation first.  If the process dies
        # after this boundary, exact activation replay sees the mailbox and
        # repeats only the idempotent certificate revocation.  The task state
        # is advanced last, so it is the durable completion marker.
        self._write_revocation_binding(
            task,
            activation.previous,
            reason="task_credential_rotated",
        )
        self._revoke_certificate_binding(
            task,
            activation.previous,
            reason="task_credential_rotated",
        )
        task["activated_generation"] = activation.replacement.generation
        task["revoked_generations"] = sorted(
            {*revoked, activation.previous.generation}
        )
        _atomic(task_path, canonical_json_bytes(task))

    def acknowledge_revocations(
        self, revocations: tuple[TaskWorkerCredentialRevocation, ...]
    ) -> None:
        """Delete only the exact mailbox entries acknowledged by ACTIVE master."""

        touched: set[UUID] = set()
        for revoke in revocations:
            path = self._revocation_path(revoke.task_run_id, revoke.generation)
            if not path.exists():
                continue
            persisted = TaskWorkerCredentialRevocation.model_validate(self._read(path))
            if persisted != revoke:
                raise RegionTalkAssemblyUnavailable(
                    "REGION_TALK_REVOCATION_ACK_CONFLICT"
                )
            path.unlink()
            if revoke.reason == "task_credential_rotated":
                self._access_path(
                    revoke.task_run_id, revoke.generation
                ).unlink(missing_ok=True)
                self._registration_path(
                    revoke.task_run_id, revoke.generation
                ).unlink(missing_ok=True)
                self._registration_ack_path(
                    revoke.task_run_id, revoke.generation
                ).unlink(missing_ok=True)
            touched.add(revoke.task_run_id)
        for task_id in touched:
            task_path = self._task_path(task_id)
            if not task_path.exists():
                continue
            task = self._read(task_path)
            if bool(task.get("terminal")) and not any(
                self.root.glob(f"{task_id.hex}.*.revoke.json")
            ):
                self._purge_task_private_state(task_id)

    def acknowledge_registrations(
        self, receipts: tuple[TaskWorkerCredentialRegistrationResponse, ...]
    ) -> None:
        for receipt in receipts:
            registration_path = self._registration_path(
                receipt.task_run_id, receipt.generation
            )
            if not registration_path.exists():
                raise RegionTalkAssemblyUnavailable(
                    "REGION_TALK_REGISTRATION_ACK_CONFLICT"
                )
            registration = TaskWorkerCredentialRegistration.model_validate(
                self._read(registration_path)
            )
            expected = TaskWorkerCredentialRegistrationResponse(
                task_run_id=registration.task_run_id,
                epoch=registration.epoch,
                generation=registration.generation,
                credential_id=registration.credential_id,
                command_sha256=registration.command_sha256,
            )
            if expected != receipt:
                raise RegionTalkAssemblyUnavailable(
                    "REGION_TALK_REGISTRATION_ACK_CONFLICT"
                )
            path = self._registration_ack_path(
                receipt.task_run_id, receipt.generation
            )
            encoded = canonical_json_bytes(receipt.model_dump(mode="json"))
            if path.exists() and path.read_bytes() != encoded:
                raise RegionTalkAssemblyUnavailable(
                    "REGION_TALK_REGISTRATION_ACK_CONFLICT"
                )
            _atomic(path, encoded)

    def request_revocation(self, run: RegionTalkRunSnapshot) -> None:
        if run.task_run_id is None or run.master is None or run.access is None:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CLEANUP_BINDING_MISSING")
        task_path = self._task_path(run.task_run_id)
        task = self._read(task_path)
        task["terminal"] = True
        _atomic(task_path, canonical_json_bytes(task))
        bindings = tuple(
            RegionTalkAccessBinding.model_validate(value)
            for value in dict(task.get("bindings", {})).values()
        )
        if not bindings:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CLEANUP_BINDING_MISSING")
        revoked_generations = {
            int(value) for value in task.get("revoked_generations", [])
        }
        terminal_revoked_generations = {
            int(value) for value in task.get("terminal_revoked_generations", [])
        }
        for binding in bindings:
            if (
                binding.generation in revoked_generations
                or binding.generation in terminal_revoked_generations
            ):
                continue
            # The mailbox is durable intent/delivery state, not proof that the
            # broker committed its KRL/certificate revocation.  Replaying an
            # existing exact mailbox must therefore repeat the idempotent
            # broker effect until the per-generation completion marker is
            # fsynced last.
            self._write_revocation_binding(
                task, binding, reason="region_talk_terminal"
            )
            self._revoke_certificate_binding(
                task, binding, reason="region_talk_terminal"
            )
            terminal_revoked_generations.add(binding.generation)
            task["terminal_revoked_generations"] = sorted(
                terminal_revoked_generations
            )
            _atomic(task_path, canonical_json_bytes(task))

    def active_binding(self, task_run_id: UUID) -> RegionTalkAccessBinding:
        task = self._read(self._task_path(task_run_id))
        return self._task_binding(task, int(task.get("activated_generation", 0)))

    def purge(self, task_run_id: UUID) -> None:
        # Secret sidecars are removed after the ACTIVE master explicitly ACKs
        # every terminal revocation.  Deleting here would recreate the former
        # response-loss race and strand a live LOGIN.
        if not any(self.root.glob(f"{task_run_id.hex}.*.revoke.json")):
            self._purge_task_private_state(task_run_id)

    def _task_path(self, task_run_id: UUID) -> Path:
        return self.root / f"{task_run_id.hex}.task.json"

    def _registration_path(self, task_run_id: UUID, generation: int) -> Path:
        return self.root / f"{task_run_id.hex}.{generation}.registration.json"

    def _access_path(self, task_run_id: UUID, generation: int) -> Path:
        return self.root / f"{task_run_id.hex}.{generation}.access.json"

    def _registration_ack_path(self, task_run_id: UUID, generation: int) -> Path:
        return self.root / f"{task_run_id.hex}.{generation}.registration-ack.json"

    def _revocation_path(self, task_run_id: UUID, generation: int) -> Path:
        return self.root / f"{task_run_id.hex}.{generation}.revoke.json"

    @staticmethod
    def _binding(access: RegionTalkDirectMasterAccess) -> RegionTalkAccessBinding:
        return RegionTalkAccessBinding(
            credential_id=access.credential_id,
            generation=access.generation,
            command_sha256=access.command_sha256,
            task_token_sha256=access.task_token_sha256,
            expires_at=access.expires_at,
            ssh_certificate_serial=access.ssh_certificate_serial,
        )

    def _validate_task_metadata(
        self,
        task: Mapping[str, Any],
        metadata: RegionTalkLaunchMetadata,
        *,
        source_sha256: str,
    ) -> None:
        if (
            task.get("request_id") != str(metadata.request_id)
            or task.get("master_instance_id")
            != str(metadata.master.master_instance_id)
            or task.get("master_run_id") != str(metadata.master.run_id)
            or task.get("master_attempt_id") != str(metadata.master.attempt_id)
            or task.get("source_sha256") != source_sha256
            or task.get("image_identity") != metadata.runtime_image_identity
            or task.get("image_source_commit")
            != metadata.runtime_image_source_commit
            or task.get("launch") != metadata.model_dump(mode="json")
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_TASK_COMMAND_CONFLICT")

    @staticmethod
    def _launch_metadata(task: Mapping[str, Any]) -> RegionTalkLaunchMetadata:
        try:
            return RegionTalkLaunchMetadata.model_validate(task.get("launch"))
        except ValueError as exc:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_PRIVATE_STATE_INVALID"
            ) from exc

    @staticmethod
    def _validate_refresh_binding(
        task: Mapping[str, Any], request: RegionTalkCredentialRefreshRequest
    ) -> None:
        launch = RegionTalkLaunchMetadata.model_validate(task.get("launch"))
        if (
            bool(task.get("terminal"))
            or request.request_id != launch.request_id
            or request.task_run_id != launch.task_run_id
            or request.master_instance_id != launch.master.master_instance_id
            or request.epoch != launch.master.epoch
            or request.source_sha256 != task.get("source_sha256")
            or request.image_identity != launch.runtime_image_identity
            or request.image_source_commit != launch.runtime_image_source_commit
            or request.previous.generation
            != int(task.get("activated_generation", 0))
        ):
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_REFRESH_BINDING_INVALID"
            )

    @classmethod
    def _validate_replacement(
        cls,
        request: RegionTalkCredentialRefreshRequest,
        access: RegionTalkDirectMasterAccess,
    ) -> None:
        if (
            access.task_run_id != request.task_run_id
            or access.master_instance_id != request.master_instance_id
            or access.epoch != request.epoch
            or access.generation != request.previous.generation + 1
            or access.task_token_sha256 != request.previous.task_token_sha256
        ):
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_REFRESH_BINDING_INVALID"
            )

    @staticmethod
    def _validate_activation_binding(
        task: Mapping[str, Any], activation: RegionTalkCredentialActivation
    ) -> None:
        launch = RegionTalkLaunchMetadata.model_validate(task.get("launch"))
        if (
            bool(task.get("terminal"))
            or activation.request_id != launch.request_id
            or activation.task_run_id != launch.task_run_id
            or activation.master_instance_id != launch.master.master_instance_id
            or activation.epoch != launch.master.epoch
            or activation.source_sha256 != task.get("source_sha256")
            or activation.image_identity != launch.runtime_image_identity
            or activation.image_source_commit
            != launch.runtime_image_source_commit
        ):
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_ACTIVATION_BINDING_INVALID"
            )

    def _record_binding(self, access: RegionTalkDirectMasterAccess) -> None:
        task_path = self._task_path(access.task_run_id)
        task = self._read(task_path)
        bindings = dict(task.get("bindings", {}))
        key = str(access.generation)
        value = self._binding(access).model_dump(mode="json")
        if key in bindings and bindings[key] != value:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_GENERATION_CONFLICT"
            )
        bindings[key] = value
        task["bindings"] = bindings
        _atomic(task_path, canonical_json_bytes(task))

    @staticmethod
    def _task_binding(
        task: Mapping[str, Any], generation: int
    ) -> RegionTalkAccessBinding:
        try:
            value = dict(task.get("bindings", {}))[str(generation)]
            return RegionTalkAccessBinding.model_validate(value)
        except (KeyError, TypeError, ValueError) as exc:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_CREDENTIAL_BINDING_INVALID"
            ) from exc

    def _write_revocation_binding(
        self,
        task: Mapping[str, Any],
        binding: RegionTalkAccessBinding,
        *,
        reason: str,
    ) -> bool:
        metadata = self._launch_metadata(task)
        revoke = TaskWorkerCredentialRevocation(
            worker_kind="region_talk",
            task_run_id=metadata.task_run_id,
            epoch=metadata.master.epoch,
            generation=binding.generation,
            task_token_sha256=binding.task_token_sha256,
            command_sha256=binding.command_sha256,
            credential_id=binding.credential_id,
            reason=reason,
        )
        path = self._revocation_path(metadata.task_run_id, binding.generation)
        encoded = canonical_json_bytes(revoke.model_dump(mode="json"))
        if path.exists() and path.read_bytes() != encoded:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_REVOCATION_CONFLICT"
            )
        if path.exists():
            return False
        _atomic(path, encoded)
        return True

    def _revoke_certificate_binding(
        self,
        task: Mapping[str, Any],
        binding: RegionTalkAccessBinding,
        *,
        reason: str,
    ) -> None:
        metadata = self._launch_metadata(task)
        self.broker.revoke_task_worker_certificate(
            master_instance_id=str(metadata.master.master_instance_id),
            epoch=metadata.master.epoch,
            worker_kind="region_talk",
            task_run_id=str(metadata.task_run_id),
            credential_id=str(binding.credential_id),
            generation=binding.generation,
            binding_sha256=binding.command_sha256,
            serial=binding.ssh_certificate_serial,
            reason=reason,
        )

    def _purge_task_private_state(self, task_run_id: UUID) -> None:
        for path in self.root.glob(f"{task_run_id.hex}.*.json"):
            if path.is_file() and not path.is_symlink():
                path.unlink()
        task_path = self._task_path(task_run_id)
        if task_path.is_file() and not task_path.is_symlink():
            task_path.unlink()

    def _reap_expired_sidecars(self) -> None:
        observed = self.clock().astimezone(UTC)
        for path in tuple(self.root.glob("*.access.json")):
            try:
                access = RegionTalkDirectMasterAccess.model_validate(self._read(path))
            except (ValueError, RegionTalkAssemblyUnavailable) as exc:
                raise RegionTalkAssemblyUnavailable(
                    "REGION_TALK_PRIVATE_STATE_INVALID"
                ) from exc
            if access.expires_at <= observed:
                self._expire_generation(access)

    def _expire_generation(self, access: RegionTalkDirectMasterAccess) -> None:
        task_path = self._task_path(access.task_run_id)
        task = self._read(task_path)
        expired = {int(value) for value in task.get("expired_generations", [])}
        expired.add(access.generation)
        task["expired_generations"] = sorted(expired)
        _atomic(task_path, canonical_json_bytes(task))
        self._access_path(access.task_run_id, access.generation).unlink(
            missing_ok=True
        )
        self._registration_path(access.task_run_id, access.generation).unlink(
            missing_ok=True
        )
        self._registration_ack_path(access.task_run_id, access.generation).unlink(
            missing_ok=True
        )

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
class CentralRegionTalkStageCredentialBroker:
    """Nonblocking child-credential handshake for one metadata-only claim."""

    authority: DirectoryRegionTalkTaskAuthority
    image_identity: str
    image_source_commit: str
    source_sha256_by_stage: Mapping[str, str]

    def prepare(
        self, claim_value: Mapping[str, Any] | StageWorkMetadataClaimReceipt
    ) -> StageWorkerCredentialStatus:
        claim = (
            claim_value
            if isinstance(claim_value, StageWorkMetadataClaimReceipt)
            else StageWorkMetadataClaimReceipt.model_validate(claim_value)
        )
        try:
            source_sha256 = self.source_sha256_by_stage[claim.stage]
        except KeyError as exc:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_STAGE_NOTEBOOK_SOURCE_UNAVAILABLE"
            ) from exc
        command = self.authority.prepare_stage_worker(
            claim,
            task_token=secrets.token_urlsafe(36),
            source_sha256=source_sha256,
            image_identity=self.image_identity,
            image_source_commit=self.image_source_commit,
        )
        registration = self.authority.stage_worker_registration(claim, command)
        return StageWorkerCredentialStatus(
            status="READY" if registration is not None else "PENDING",
            dispatch_id=claim.dispatch_id,
            effect_id=claim.effect_id,
            worker_task_run_id=claim.worker_task_run_id,
            worker_credential_id=(registration.credential_id if registration else None),
            worker_generation=(registration.generation if registration else None),
            worker_command_sha256=command.command_sha256,
            worker_task_token_sha256=command.task_token_sha256,
        )


@dataclass(slots=True)
class CentralRegionTalkNotebookAdapter:
    """Deterministic private Dataset/Notebook launcher and cleanup adapter."""

    adapter: Any
    authority: DirectoryRegionTalkTaskAuthority
    owner: str
    callback_base_url: str
    notebook_ref: str | None = None
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    journal_path: Path | None = None
    _receipts: dict[UUID, RegionTalkLaunchReceipt] = field(default_factory=dict)
    _claims: dict[UUID, dict[str, TaskResourceClaim]] = field(default_factory=dict)
    _intents: dict[UUID, dict[str, ProviderEffectIntent]] = field(default_factory=dict)
    _notebook_previous_versions: dict[UUID, int] = field(default_factory=dict)
    _cleanup_receipts: dict[UUID, RegionTalkCleanupReceipt] = field(default_factory=dict)
    _cleanup_bindings: dict[UUID, RegionTalkAccessBinding] = field(default_factory=dict)

    def __post_init__(self) -> None:
        expected_notebook = f"{self.owner}/mdh-region-talk-supervisor"
        if self.notebook_ref is None:
            self.notebook_ref = expected_notebook
        if self.notebook_ref != expected_notebook:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_SECRET_BOUND_NOTEBOOK_REF_INVALID"
            )
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
        source = render_region_talk_supervisor_source(metadata)
        source_sha = hashlib.sha256(source).hexdigest()
        task_token = secrets.token_urlsafe(36)
        command = self.authority.prepare(
            metadata,
            task_token=task_token,
            source_sha256=source_sha,
        )
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
        assert self.notebook_ref is not None
        notebook_ref = self.notebook_ref
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
                    "ydb_endpoint": metadata.ydb_endpoint,
                    "ydb_database": metadata.ydb_database,
                    "ydb_viewer_secret_label": metadata.ydb_viewer_secret_label,
                    "ydb_dependency_manifest_sha256": (
                        metadata.ydb_dependency_manifest_sha256
                    ),
                    "publication_dispatch": False,
                    "privacy": "private",
                }
            ),
        }
        operation_id = uuid5(NAMESPACE_URL, f"region-talk-supervisor:{metadata.request_id}")
        intents = self._intents.setdefault(metadata.task_run_id, {})
        claims = self._claims.setdefault(metadata.task_run_id, {})
        create_arguments = {
            "content_tree_sha256": mapping_sha256(files),
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
            "disposable": True,
        }
        create = intents.get("status")
        if create is None:
            create = ProviderEffectIntent.create(
                operation_id=operation_id,
                effect_id=uuid5(NAMESPACE_URL, f"region-talk-status:{metadata.task_run_id}"),
                idempotency_key=f"region-talk-status:{metadata.task_run_id}",
                task_id=metadata.task_run_id,
                action=MutationAction.CREATE_DATASET,
                provider_ref=status_ref,
                arguments=create_arguments,
                requested_at=self.clock().astimezone(UTC),
            )
            intents["status"] = create
            self._save_journal()  # persist the original time/intent before the effect
        elif create.provider_ref != status_ref:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_CONFLICT")
        status_claim = claims.get("status")
        if status_claim is None:
            version = self.adapter.current_private_dataset_version(provider_ref=status_ref)
            if version is None:
                dataset = self.adapter.create_private_dataset(
                    intent=create,
                    files=files,
                    title=status_ref.split("/", 1)[1],
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                    disposable=True,
                )
            else:
                with tempfile.TemporaryDirectory(prefix="mdh-region-talk-reconcile-") as raw:
                    directory = Path(raw)
                    for relative, content in files.items():
                        target = directory.joinpath(*relative.split("/"))
                        target.parent.mkdir(parents=True, exist_ok=True)
                        target.write_bytes(content)
                    dataset = self.adapter.reconcile_private_dataset_directory_mutation(
                        intent=create,
                        source_directory=directory,
                        expected_version=1,
                        arguments=create_arguments,
                        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                        disposable=True,
                    )
            status_claim = dataset.claim
            claims["status"] = status_claim
            self._save_journal()
        exact_status = f"{status_ref}/{int(status_claim.provider_version)}"
        sources = (metadata.runtime_dataset_exact_ref, exact_status)
        previous_notebook_version = self._notebook_previous_versions.get(metadata.task_run_id)
        push = intents.get("notebook")
        if push is None:
            previous_notebook_version = self.adapter.current_private_notebook_version(
                provider_ref=notebook_ref
            )
            if previous_notebook_version is None:
                raise RegionTalkAssemblyUnavailable(
                    "REGION_TALK_SECRET_BOUND_NOTEBOOK_MISSING"
                )
            self._notebook_previous_versions[metadata.task_run_id] = previous_notebook_version
        elif previous_notebook_version is None:
            raise RegionTalkAssemblyUnavailable(
                "REGION_TALK_NOTEBOOK_PREVIOUS_VERSION_MISSING"
            )
        push_arguments = {
            "task_run_id": str(metadata.task_run_id),
            "source_sha256": source_sha,
            "dataset_sources": sources,
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
            "disposable": False,
            "docker_image": metadata.runtime_image_identity,
            "docker_image_pinning_type": "original",
            "expected_previous_version": previous_notebook_version,
        }
        if push is None:
            push = ProviderEffectIntent.create(
                operation_id=operation_id,
                effect_id=uuid5(NAMESPACE_URL, f"region-talk-run:{metadata.task_run_id}"),
                idempotency_key=f"region-talk-run:{metadata.task_run_id}",
                task_id=metadata.task_run_id,
                action=MutationAction.PUSH_NOTEBOOK,
                provider_ref=notebook_ref,
                arguments=push_arguments,
                requested_at=self.clock().astimezone(UTC),
            )
            intents["notebook"] = push
            self._save_journal()
        elif push.provider_ref != notebook_ref:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_CONFLICT")
        notebook_claim = claims.get("notebook")
        if notebook_claim is None:
            launched = self.adapter.reconcile_private_notebook_mutation(
                intent=push,
                task_run_id=metadata.task_run_id,
                expected_source_sha256=source_sha,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
                dataset_sources=sources,
                docker_image=metadata.runtime_image_identity,
                docker_image_pinning_type="original",
                expected_previous_version=previous_notebook_version,
            )
            if launched is None:
                launched = self.adapter.push_private_notebook_pending_runtime_attestation(
                    intent=push,
                    task_run_id=metadata.task_run_id,
                    source=source,
                    title=notebook_ref.split("/", 1)[1],
                    code_file="region_talk_supervisor.py",
                    kernel_type="script",
                    language="python",
                    control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                    disposable=False,
                    dataset_sources=sources,
                    enable_internet=True,
                    timeout_seconds=metadata.max_runtime_seconds,
                    docker_image=metadata.runtime_image_identity,
                    docker_image_pinning_type="original",
                    expected_previous_version=previous_notebook_version,
                )
            notebook_claim = launched.claim
            claims["notebook"] = notebook_claim
            self._save_journal()
        receipt = RegionTalkLaunchReceipt(
            task_run_id=metadata.task_run_id,
            master_instance_id=metadata.master.master_instance_id,
            epoch=metadata.master.epoch,
            source_sha256=source_sha,
            status_dataset_exact_ref=exact_status,
            provider_run_ref=(
                f"{notebook_claim.provider_ref}/{notebook_claim.provider_version}"
            ),
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
        self._save_journal()
        return receipt

    def cleanup(self, run: RegionTalkRunSnapshot) -> RegionTalkCleanupReceipt:
        if run.task_run_id is None or run.access is None:
            raise RegionTalkAssemblyUnavailable("REGION_TALK_CLEANUP_BINDING_MISSING")
        replay = self._cleanup_receipts.get(run.task_run_id)
        if replay is not None:
            return replay
        active_access = self._cleanup_bindings.get(run.task_run_id)
        if active_access is None:
            active_access = self.authority.active_binding(run.task_run_id)
            self._cleanup_bindings[run.task_run_id] = active_access
            self._save_journal()
        self.authority.request_revocation(run)
        claims = self._claims.get(run.task_run_id, {})
        intents = self._intents.setdefault(run.task_run_id, {})
        deleted = 0
        for label, claim, action in (
            ("notebook", claims.get("notebook"), MutationAction.DELETE_NOTEBOOK),
            ("status", claims.get("status"), MutationAction.DELETE_DATASET),
        ):
            if claim is None:
                continue
            if not claim.disposable:
                # The stable private Notebook retains its reviewed Kaggle User
                # Secret attachment across task-bound source versions.  It is
                # orchestrator-protected and never task-cleanup authority.
                continue
            intent_key = f"cleanup_{label}"
            intent = intents.get(intent_key)
            if intent is None:
                intent = ProviderEffectIntent.create(
                    operation_id=uuid5(NAMESPACE_URL, f"region-talk-cleanup:{run.task_run_id}"),
                    effect_id=uuid5(NAMESPACE_URL, f"region-talk-cleanup:{label}:{run.task_run_id}"),
                    idempotency_key=f"region-talk-cleanup:{label}:{run.task_run_id}",
                    task_id=run.task_run_id,
                    action=action,
                    provider_ref=claim.provider_ref,
                    expected_fingerprint=claim.fingerprint,
                    arguments={
                        "claim_sha256": claim.claim_sha256,
                        "provider_version": claim.provider_version,
                    },
                    requested_at=self.clock().astimezone(UTC),
                )
                intents[intent_key] = intent
                self._save_journal()
            self.adapter.delete_task_created_resource(intent=intent, claim=claim)
            deleted += 1
        cleaned_at = self.clock().astimezone(UTC)
        base = {
            "task_run_id": str(run.task_run_id),
            "credential_id": str(active_access.credential_id),
            "generation": active_access.generation,
            "command_sha256": active_access.command_sha256,
            "task_token_sha256": active_access.task_token_sha256,
            "ssh_certificate_serial": active_access.ssh_certificate_serial,
            "resources_deleted": deleted,
            "cleaned_at": cleaned_at.isoformat(),
        }
        receipt = RegionTalkCleanupReceipt(
            **base,
            receipt_sha256=hashlib.sha256(canonical_json_bytes(base)).hexdigest(),
        )
        self._cleanup_receipts[run.task_run_id] = receipt
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
            "schema_version": "region-talk-launch-journal.v3",
            "receipts": {
                str(task): receipt.model_dump(mode="json")
                for task, receipt in sorted(self._receipts.items(), key=lambda item: str(item[0]))
            },
            "claims": {
                str(task): {
                    label: claim.model_dump(mode="json")
                    for label, claim in sorted(claims.items())
                }
                for task, claims in sorted(self._claims.items(), key=lambda item: str(item[0]))
            },
            "intents": {
                str(task): {
                    label: intent.model_dump(mode="json")
                    for label, intent in sorted(intents.items())
                }
                for task, intents in sorted(self._intents.items(), key=lambda item: str(item[0]))
            },
            "notebook_previous_versions": {
                str(task): version
                for task, version in sorted(
                    self._notebook_previous_versions.items(), key=lambda item: str(item[0])
                )
            },
            "cleanup_receipts": {
                str(task): receipt.model_dump(mode="json")
                for task, receipt in sorted(
                    self._cleanup_receipts.items(), key=lambda item: str(item[0])
                )
            },
            "cleanup_bindings": {
                str(task): binding.model_dump(mode="json")
                for task, binding in sorted(
                    self._cleanup_bindings.items(), key=lambda item: str(item[0])
                )
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
        schema = value.get("schema_version")
        expected_keys = (
            {"schema_version", "receipts", "claims"}
            if schema == "region-talk-launch-journal.v1"
            else {
                "schema_version",
                "receipts",
                "claims",
                "intents",
                "cleanup_receipts",
                "cleanup_bindings",
            }
            if schema == "region-talk-launch-journal.v2"
            else {
                "schema_version",
                "receipts",
                "claims",
                "intents",
                "notebook_previous_versions",
                "cleanup_receipts",
                "cleanup_bindings",
            }
        )
        if set(value) != expected_keys or schema not in {
            "region-talk-launch-journal.v1",
            "region-talk-launch-journal.v2",
            "region-talk-launch-journal.v3",
        }:
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
            if schema == "region-talk-launch-journal.v1":
                if not isinstance(values, list) or len(values) != 2:
                    raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
                self._claims[task_id] = {
                    "status": TaskResourceClaim.model_validate(values[0]),
                    "notebook": TaskResourceClaim.model_validate(values[1]),
                }
            else:
                if not isinstance(values, dict) or not set(values) <= {"status", "notebook"}:
                    raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
                self._claims[task_id] = {
                    label: TaskResourceClaim.model_validate(item)
                    for label, item in values.items()
                }
        if schema in {"region-talk-launch-journal.v2", "region-talk-launch-journal.v3"}:
            for task, values in value["intents"].items():
                if not isinstance(values, dict):
                    raise RegionTalkAssemblyUnavailable("REGION_TALK_LAUNCH_JOURNAL_INVALID")
                self._intents[UUID(task)] = {
                    label: ProviderEffectIntent.model_validate(item)
                    for label, item in values.items()
                }
            self._cleanup_receipts = {
                UUID(task): RegionTalkCleanupReceipt.model_validate(item)
                for task, item in value["cleanup_receipts"].items()
            }
            self._cleanup_bindings = {
                UUID(task): RegionTalkAccessBinding.model_validate(item)
                for task, item in value["cleanup_bindings"].items()
            }
            if schema == "region-talk-launch-journal.v3":
                previous_versions = value["notebook_previous_versions"]
                if not isinstance(previous_versions, dict) or any(
                    not isinstance(version, int) or version < 1
                    for version in previous_versions.values()
                ):
                    raise RegionTalkAssemblyUnavailable(
                        "REGION_TALK_LAUNCH_JOURNAL_INVALID"
                    )
                self._notebook_previous_versions = {
                    UUID(task): version for task, version in previous_versions.items()
                }


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
    ydb_endpoint: str = ""
    ydb_database: str = ""
    ydb_viewer_secret_label: str = ""
    ydb_dependency_manifest_sha256: str = ""
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
            ydb_endpoint=os.getenv("MY_DATA_HUB_REGION_TALK_YDB_ENDPOINT", "").strip(),
            ydb_database=os.getenv("MY_DATA_HUB_REGION_TALK_YDB_DATABASE", "").strip(),
            ydb_viewer_secret_label=os.getenv(
                "MY_DATA_HUB_REGION_TALK_YDB_VIEWER_SECRET_LABEL", ""
            ).strip(),
            ydb_dependency_manifest_sha256=os.getenv(
                "MY_DATA_HUB_MASTER_YDB_DEPENDENCY_MANIFEST_SHA256", ""
            ).strip(),
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
            or not re.fullmatch(r"grpcs?://[A-Za-z0-9.-]+(?::[1-9][0-9]{0,4})?", self.ydb_endpoint)
            or not re.fullmatch(r"/[A-Za-z0-9_./-]+", self.ydb_database)
            or not re.fullmatch(r"[A-Z][A-Z0-9_]{7,127}", self.ydb_viewer_secret_label)
            or not re.fullmatch(r"[a-f0-9]{64}", self.ydb_dependency_manifest_sha256)
        ):
            raise RegionTalkAssemblyUnavailable("REGION_TALK_ASSEMBLY_ENVIRONMENT_INCOMPLETE")


__all__ = [
    "CentralRegionTalkNotebookAdapter",
    "CentralRegionTalkStageCredentialBroker",
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
