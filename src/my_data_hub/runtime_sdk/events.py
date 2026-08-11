from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class RuntimeEventType(StrEnum):
    RUNTIME_CREATED = "runtime.created"
    RUNTIME_STARTED = "runtime.started"
    RUNTIME_HEARTBEAT = "runtime.heartbeat"
    RUNTIME_PROGRESS = "runtime.progress"
    RUNTIME_DRAINING = "runtime.draining"
    RUNTIME_TERMINAL = "runtime.terminal"
    RUNTIME_FAILED = "runtime.failed"
    SERVICE_ANNOUNCED = "service.announced"
    SERVICE_READY = "service.ready"
    SERVICE_UNAVAILABLE = "service.unavailable"
    SERVICE_ENDPOINT_CHANGED = "service.endpoint_changed"
    JOB_CLAIMED = "job.claimed"
    JOB_PROGRESS = "job.progress"
    JOB_RESULT_AVAILABLE = "job.result_available"
    JOB_COMPLETED = "job.completed"
    JOB_FAILED = "job.failed"
    RESOURCE_ACQUIRE = "resource.acquire"
    RESOURCE_RENEW = "resource.renew"
    RESOURCE_RELEASE = "resource.release"
    CHECKPOINT_STARTED = "checkpoint.started"
    CHECKPOINT_CANDIDATE_UPLOADED = "checkpoint.candidate_uploaded"
    CHECKPOINT_VERIFIED = "checkpoint.verified"
    CHECKPOINT_FAILED = "checkpoint.failed"


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: str = Field(min_length=1, max_length=100)
    locator: str = Field(min_length=1, max_length=4_000)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(ge=0)


class DurableResourceLease(BaseModel):
    """Exact owner-task lease identity copied from the durable status authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_id: str = Field(min_length=1, max_length=200)
    resource_kind: str = Field(min_length=1, max_length=100)
    resource_ref: str = Field(min_length=1, max_length=500)
    holder_id: str = Field(min_length=1, max_length=200)
    lease_until: datetime
    epoch: int = Field(ge=1)

    @field_validator("lease_until")
    @classmethod
    def lease_deadline_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("resource lease deadline must be timezone-aware")
        return value


class RuntimeEvent(BaseModel):
    """Secret-free callback body. Authentication is carried only in an HTTP header."""

    model_config = ConfigDict(extra="allow", frozen=True, populate_by_name=True)

    schema_version: Literal["content-runtime-event/v1"] = Field(default="content-runtime-event/v1", alias="schema")
    event_id: str = Field(
        pattern=r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}$"
    )
    run_id: str = Field(min_length=1, max_length=200)
    attempt_id: str = Field(min_length=1, max_length=200)
    service_instance_id: str = Field(min_length=1, max_length=200)
    source_identity: str = Field(min_length=1, max_length=500)
    source_version: str = Field(min_length=1, max_length=200)
    event_type: RuntimeEventType
    emitted_at: datetime
    local_sequence: int = Field(ge=1)
    epoch: int = Field(ge=1)
    phase: str | None = Field(default=None, min_length=1, max_length=100)
    status: str | None = Field(default=None, min_length=1, max_length=100)
    data: dict[str, Any] = Field(default_factory=dict)
    artifact_refs: tuple[ArtifactRef, ...] = Field(default_factory=tuple, max_length=100)
    metrics: dict[str, int | float | str | bool | None] = Field(default_factory=dict)

    @field_validator("emitted_at")
    @classmethod
    def emitted_at_is_aware(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            raise ValueError("emitted_at must be timezone-aware")
        return value
