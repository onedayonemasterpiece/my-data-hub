"""Metadata-only contracts for the autonomous Region Talk supervisor.

The devstand persists these contracts, but never a PostgreSQL URL, token, key,
certificate, source record, article, post, or publication body.  Direct-master
capabilities are deliberately represented by a separate private model and are
only exchanged between the task credential authority and the private Kaggle
Notebook.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SecretStr,
    field_validator,
    model_validator,
)

from my_data_hub.hashing import canonical_json_bytes

_SHA256_PATTERN = r"^[a-f0-9]{64}$"
_PROJECT_SLUG = "region-talk"
_WORKLOAD_KIND = "region-talk-pipeline"


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC)


class RegionTalkTrigger(StrEnum):
    SCHEDULED = "scheduled"
    SUPERVISED = "supervised"


class RegionTalkRunState(StrEnum):
    WAITING_MASTER = "WAITING_MASTER"
    LAUNCHING = "LAUNCHING"
    PENDING_ATTESTATION = "PENDING_ATTESTATION"
    ATTESTED = "ATTESTED"
    RUNNING = "RUNNING"
    TERMINAL = "TERMINAL"
    TIMED_OUT = "TIMED_OUT"
    FENCED = "FENCED"
    CLEANUP_PENDING = "CLEANUP_PENDING"
    CLEANED = "CLEANED"


class RegionTalkTerminalStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    EPOCH_FENCED = "EPOCH_FENCED"


class RegionTalkSupervisorOutcome(StrEnum):
    """Business-safe bounded outcome, separate from control-run cleanup state."""

    SUCCEEDED = "SUCCEEDED"
    IMPORT_COMPLETE_WAITING_STAGES = "IMPORT_COMPLETE_WAITING_STAGES"
    RETRYABLE = "RETRYABLE"
    IMPORT_FAILED = "IMPORT_FAILED"


class ActiveMasterBinding(BaseModel):
    """Exact ACTIVE-master runtime identity used to fence a pipeline run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    run_id: UUID
    attempt_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)


class RegionTalkRunRequest(BaseModel):
    """Idempotent request accepted from the timer or supervised MCP action."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-pipeline-request.v1"] = (
        "region-talk-pipeline-request.v1"
    )
    request_id: UUID
    project_slug: Literal["region-talk"] = _PROJECT_SLUG
    trigger: RegionTalkTrigger
    schedule_slot: str = Field(min_length=8, max_length=300)
    idempotency_key_sha256: str = Field(pattern=_SHA256_PATTERN)
    source_revision: str | None = Field(default=None, max_length=200)
    publication_dispatch: Literal[False] = False
    requested_at: datetime

    @field_validator("requested_at")
    @classmethod
    def requested_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)

    @classmethod
    def scheduled(
        cls,
        *,
        schedule_slot: datetime,
        source_revision: str | None = None,
        requested_at: datetime | None = None,
    ) -> RegionTalkRunRequest:
        slot = _as_utc(schedule_slot).replace(microsecond=0).isoformat()
        key = f"region-talk:scheduled:{slot}"
        digest = hashlib.sha256(key.encode()).hexdigest()
        return cls(
            request_id=uuid5(NAMESPACE_URL, key),
            trigger=RegionTalkTrigger.SCHEDULED,
            schedule_slot=slot,
            idempotency_key_sha256=digest,
            source_revision=source_revision,
            requested_at=requested_at or schedule_slot,
        )

    @classmethod
    def supervised(
        cls,
        *,
        idempotency_key: str,
        requested_at: datetime,
        source_revision: str | None = None,
    ) -> RegionTalkRunRequest:
        if not 8 <= len(idempotency_key) <= 300:
            raise ValueError("supervised idempotency_key must contain 8..300 characters")
        digest = hashlib.sha256(idempotency_key.encode()).hexdigest()
        return cls(
            request_id=uuid5(NAMESPACE_URL, f"region-talk:supervised:{digest}"),
            trigger=RegionTalkTrigger.SUPERVISED,
            schedule_slot=f"supervised:{digest}",
            idempotency_key_sha256=digest,
            source_revision=source_revision,
            requested_at=requested_at,
        )


class RegionTalkLaunchMetadata(BaseModel):
    """Secret-free identity and immutable pins for one private Notebook run."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-supervisor-launch.v1"] = (
        "region-talk-supervisor-launch.v1"
    )
    request_id: UUID
    task_run_id: UUID
    project_slug: Literal["region-talk"] = _PROJECT_SLUG
    workload_kind: Literal["region-talk-pipeline"] = _WORKLOAD_KIND
    trigger: RegionTalkTrigger
    schedule_slot: str = Field(min_length=8, max_length=300)
    source_revision: str | None = Field(default=None, max_length=200)
    master: ActiveMasterBinding
    runtime_dataset_exact_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$"
    )
    runtime_image_identity: str = Field(min_length=3, max_length=500)
    runtime_image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    wheel_relative_path: str = Field(min_length=1, max_length=500)
    wheel_sha256: str = Field(pattern=_SHA256_PATTERN)
    ydb_endpoint: str = Field(min_length=12, max_length=500)
    ydb_database: str = Field(pattern=r"^/[A-Za-z0-9_./-]+$", max_length=500)
    ydb_viewer_secret_label: str = Field(pattern=r"^[A-Z][A-Z0-9_]{7,127}$")
    ydb_dependency_manifest_sha256: str = Field(pattern=_SHA256_PATTERN)
    max_cycles: int = Field(ge=1, le=96)
    max_runtime_seconds: int = Field(ge=60, le=10_800)
    publication_dispatch: Literal[False] = False

    @field_validator("wheel_relative_path")
    @classmethod
    def relative_wheel_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("wheel_relative_path must be a safe relative path")
        return normalized

    @field_validator("ydb_endpoint")
    @classmethod
    def credential_free_ydb_endpoint(cls, value: str) -> str:
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"grpc", "grpcs"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.path not in {"", "/"}
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("ydb_endpoint must be a credential-free grpc(s) endpoint")
        return value.rstrip("/")


class RegionTalkRuntimeAttestation(BaseModel):
    """Metadata-only callback submitted before any PostgreSQL connection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-runtime-attestation.v1"] = (
        "region-talk-runtime-attestation.v1"
    )
    request_id: UUID
    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    source_sha256: str = Field(pattern=_SHA256_PATTERN)
    image_identity: str = Field(min_length=3, max_length=500)
    image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    attested_at: datetime

    @field_validator("attested_at")
    @classmethod
    def attested_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class RegionTalkTerminalReceipt(BaseModel):
    """Bounded aggregate receipt; no source rows or publication content."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-terminal-receipt.v1"] = (
        "region-talk-terminal-receipt.v1"
    )
    request_id: UUID
    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    status: Literal["SUCCEEDED", "FAILED"]
    outcome: RegionTalkSupervisorOutcome
    cycles_completed: int = Field(ge=0, le=96)
    rows_observed: int = Field(ge=0)
    rows_changed: int = Field(ge=0)
    queue_revision: int | None = Field(default=None, ge=0)
    accepted_snapshot_receipt_sha256: str | None = Field(
        default=None, pattern=_SHA256_PATTERN
    )
    stage_receipt_sha256: str | None = Field(
        default=None, pattern=_SHA256_PATTERN
    )
    aggregate_receipt_sha256: str = Field(pattern=_SHA256_PATTERN)
    completed_at: datetime
    publication_dispatch: Literal[False] = False

    @field_validator("completed_at")
    @classmethod
    def completed_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)

    @model_validator(mode="after")
    def outcome_matches_terminal_class(self) -> RegionTalkTerminalReceipt:
        if (self.status == "SUCCEEDED") != (
            self.outcome is RegionTalkSupervisorOutcome.SUCCEEDED
        ):
            raise ValueError("terminal status and bounded outcome differ")
        receipt_ids = (
            self.accepted_snapshot_receipt_sha256,
            self.stage_receipt_sha256,
        )
        if (receipt_ids[0] is None) != (receipt_ids[1] is None):
            raise ValueError("accepted snapshot and stage receipt IDs must be paired")
        if self.outcome in {
            RegionTalkSupervisorOutcome.SUCCEEDED,
            RegionTalkSupervisorOutcome.IMPORT_COMPLETE_WAITING_STAGES,
            RegionTalkSupervisorOutcome.IMPORT_FAILED,
        } and receipt_ids[0] is None:
            raise ValueError("import outcome requires accepted snapshot and stage receipt IDs")
        return self


class TaskWorkerCredentialCommand(BaseModel):
    """Exact master-polled command for a Region Talk task credential."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-command.v1"] = (
        "my-data-hub-task-credential-command.v1"
    )
    worker_kind: Literal["region_talk"] = "region_talk"
    task_run_id: UUID
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    task_token_sha256: str = Field(pattern=_SHA256_PATTERN)
    command_sha256: str = Field(pattern=_SHA256_PATTERN)

    @model_validator(mode="after")
    def hash_matches_command(self) -> TaskWorkerCredentialCommand:
        body = self.model_dump(mode="json", exclude={"command_sha256"})
        expected = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        if self.command_sha256 != expected:
            raise ValueError("command_sha256 differs from the canonical command")
        return self

    @classmethod
    def create(
        cls,
        *,
        task_run_id: UUID,
        epoch: int,
        generation: int,
        task_token_sha256: str,
    ) -> TaskWorkerCredentialCommand:
        body: dict[str, Any] = {
            "schema_version": "my-data-hub-task-credential-command.v1",
            "worker_kind": "region_talk",
            "task_run_id": str(task_run_id),
            "epoch": epoch,
            "generation": generation,
            "task_token_sha256": task_token_sha256,
        }
        body["command_sha256"] = hashlib.sha256(canonical_json_bytes(body)).hexdigest()
        return cls.model_validate(body)


class TaskWorkerCredentialRevocation(BaseModel):
    """Exact credential revocation retaining the original command binding."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-revocation.v1"] = (
        "my-data-hub-task-credential-revocation.v1"
    )
    worker_kind: Literal["region_talk"] = "region_talk"
    task_run_id: UUID
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    task_token_sha256: str = Field(pattern=_SHA256_PATTERN)
    command_sha256: str = Field(pattern=_SHA256_PATTERN)
    credential_id: UUID
    reason: str = Field(pattern=r"^[a-z][a-z0-9_.-]{0,63}$")


class TaskWorkerCredentialBatch(BaseModel):
    """GET response; HTTP 404 maps to this exact empty batch during rollout."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-batch.v1"] = (
        "my-data-hub-task-credential-batch.v1"
    )
    commands: tuple[TaskWorkerCredentialCommand, ...] = ()
    revocations: tuple[TaskWorkerCredentialRevocation, ...] = ()

    @model_validator(mode="after")
    def bounded_unique_batch(self) -> TaskWorkerCredentialBatch:
        if len(self.commands) + len(self.revocations) > 256:
            raise ValueError("task credential command batch exceeds 256 items")
        keys = [(item.worker_kind, item.task_run_id, item.generation) for item in self.commands]
        if len(keys) != len(set(keys)):
            raise ValueError("task credential command batch contains duplicate generations")
        return self

    @classmethod
    def rolling_upgrade_empty(cls) -> TaskWorkerCredentialBatch:
        return cls()


class TaskWorkerCredentialRegistration(BaseModel):
    """Task-private master response; ``database_url`` is never journaled."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-task-credential-registration.v1"] = (
        "my-data-hub-task-credential-registration.v1"
    )
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    worker_kind: Literal["region_talk"] = "region_talk"
    task_run_id: UUID
    generation: int = Field(ge=1)
    credential_id: UUID
    role: Literal["region_talk_pipeline"] = "region_talk_pipeline"
    database_url: SecretStr
    expires_at: datetime
    task_token_sha256: str = Field(pattern=_SHA256_PATTERN)
    command_sha256: str = Field(pattern=_SHA256_PATTERN)

    @field_validator("expires_at")
    @classmethod
    def expires_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class TaskWorkerCredentialRegistrationResponse(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    registered: Literal[True] = True
    worker_kind: Literal["region_talk"] = "region_talk"
    task_run_id: UUID
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    credential_id: UUID
    command_sha256: str = Field(pattern=_SHA256_PATTERN)


def task_worker_credentials_endpoint(master: ActiveMasterBinding) -> str:
    return (
        "/internal/runtime/task-worker-credentials/"
        f"{master.run_id}/{master.attempt_id}/commands"
    )


class RegionTalkDirectMasterAccess(BaseModel):
    """Private capability; this model must never enter the control journal."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-direct-master-access.v1"] = (
        "region-talk-direct-master-access.v1"
    )
    credential_id: UUID
    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    generation: int = Field(ge=1)
    command_sha256: str = Field(pattern=_SHA256_PATTERN)
    task_token_sha256: str = Field(pattern=_SHA256_PATTERN)
    database_url: SecretStr
    tls_ca_pem: SecretStr
    expires_at: datetime
    tunnel_endpoint: str = Field(min_length=3, max_length=300)
    ssh_private_key: SecretStr
    ssh_certificate: SecretStr
    ssh_known_hosts: SecretStr
    ssh_gateway_host: str = Field(min_length=1, max_length=253)
    ssh_gateway_port: int = Field(ge=1, le=65535)
    ssh_account: str = Field(min_length=1, max_length=64)
    ssh_certificate_serial: int = Field(ge=1)

    @field_validator("expires_at")
    @classmethod
    def expires_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class RegionTalkAccessBinding(BaseModel):
    """Non-secret capability identity safe to persist for exact revocation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    credential_id: UUID
    generation: int = Field(ge=1)
    command_sha256: str = Field(pattern=_SHA256_PATTERN)
    task_token_sha256: str = Field(pattern=_SHA256_PATTERN)
    expires_at: datetime
    ssh_certificate_serial: int = Field(ge=1)

    @field_validator("expires_at")
    @classmethod
    def expires_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class RegionTalkCredentialRefreshRequest(BaseModel):
    """Secret-free, task-bound request for the next short-lived generation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-credential-refresh.v1"] = (
        "region-talk-credential-refresh.v1"
    )
    request_id: UUID
    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    source_sha256: str = Field(pattern=_SHA256_PATTERN)
    image_identity: str = Field(min_length=3, max_length=500)
    image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    previous: RegionTalkAccessBinding
    requested_at: datetime
    publication_dispatch: Literal[False] = False

    @field_validator("requested_at")
    @classmethod
    def requested_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class RegionTalkCredentialActivation(BaseModel):
    """Worker proof that the replacement tunnel and epoch assertion succeeded."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-credential-activation.v1"] = (
        "region-talk-credential-activation.v1"
    )
    request_id: UUID
    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    source_sha256: str = Field(pattern=_SHA256_PATTERN)
    image_identity: str = Field(min_length=3, max_length=500)
    image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    previous: RegionTalkAccessBinding
    replacement: RegionTalkAccessBinding
    asserted_at: datetime
    publication_dispatch: Literal[False] = False

    @field_validator("asserted_at")
    @classmethod
    def asserted_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)

    @model_validator(mode="after")
    def exact_next_generation(self) -> RegionTalkCredentialActivation:
        if self.replacement.generation != self.previous.generation + 1:
            raise ValueError("replacement must be the exact next generation")
        if self.replacement.task_token_sha256 != self.previous.task_token_sha256:
            raise ValueError("replacement differs from the task token binding")
        return self


class RegionTalkLaunchReceipt(BaseModel):
    """Secret-free central launch receipt persisted by the coordinator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    source_sha256: str = Field(pattern=_SHA256_PATTERN)
    status_dataset_exact_ref: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$"
    )
    provider_run_ref: str = Field(min_length=3, max_length=500)
    access: RegionTalkAccessBinding


class RegionTalkCleanupReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_run_id: UUID
    credential_id: UUID
    generation: int = Field(ge=1)
    command_sha256: str = Field(pattern=_SHA256_PATTERN)
    task_token_sha256: str = Field(pattern=_SHA256_PATTERN)
    ssh_certificate_serial: int = Field(ge=1)
    resources_deleted: int = Field(ge=0, le=2)
    receipt_sha256: str = Field(pattern=_SHA256_PATTERN)
    cleaned_at: datetime

    @field_validator("cleaned_at")
    @classmethod
    def cleaned_at_is_utc(cls, value: datetime) -> datetime:
        return _as_utc(value)


class RegionTalkRunSnapshot(BaseModel):
    """Pure-read status response used by MCP and operational introspection."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    request: RegionTalkRunRequest
    state: RegionTalkRunState
    task_run_id: UUID | None = None
    master: ActiveMasterBinding | None = None
    provider_run_ref: str | None = None
    status_dataset_exact_ref: str | None = None
    source_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    access: RegionTalkAccessBinding | None = None
    timeout_at: datetime | None = None
    terminal_status: RegionTalkTerminalStatus | None = None
    terminal_receipt_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    cleanup_receipt_sha256: str | None = Field(default=None, pattern=_SHA256_PATTERN)
    error_code: str | None = Field(default=None, max_length=80)
    created_at: datetime
    updated_at: datetime

    @field_validator("timeout_at", "created_at", "updated_at")
    @classmethod
    def timestamps_are_utc(cls, value: datetime | None) -> datetime | None:
        return _as_utc(value) if value is not None else None

    @model_validator(mode="after")
    def binding_matches_state(self) -> RegionTalkRunSnapshot:
        bound = self.task_run_id is not None or self.master is not None
        if self.state is RegionTalkRunState.WAITING_MASTER and bound:
            raise ValueError("WAITING_MASTER must not retain an ACTIVE epoch binding")
        if self.state is not RegionTalkRunState.WAITING_MASTER and not (
            self.task_run_id is not None and self.master is not None
        ):
            raise ValueError("advanced pipeline state requires exact task/master binding")
        return self
