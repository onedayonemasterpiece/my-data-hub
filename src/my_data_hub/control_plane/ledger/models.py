from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import StrEnum
from typing import Any


class EffectState(StrEnum):
    PLANNED = "PLANNED"
    IN_PROGRESS = "IN_PROGRESS"
    APPLIED = "APPLIED"
    FAILED = "FAILED"


class EventDisposition(StrEnum):
    ACCEPTED = "accepted"
    DUPLICATE = "duplicate"
    COALESCED = "coalesced"
    FENCED = "fenced"


class KaggleResearchState(StrEnum):
    DRAFT = "DRAFT"
    READY = "READY"
    RUNNING = "RUNNING"
    REVIEW_REQUIRED = "REVIEW_REQUIRED"
    COMPLETED = "COMPLETED"
    ARCHIVED = "ARCHIVED"


class KaggleRevisionState(StrEnum):
    DRAFT = "DRAFT"
    FROZEN = "FROZEN"
    SUBMITTED = "SUBMITTED"


class KaggleRunState(StrEnum):
    PREPARED = "PREPARED"
    SUBMITTING = "SUBMITTING"
    SUBMISSION_UNKNOWN = "SUBMISSION_UNKNOWN"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    COLLECTING = "COLLECTING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


@dataclass(frozen=True, slots=True)
class OperationRecord:
    operation_id: str
    idempotency_key: str
    operation_kind: str
    intent_hash: str
    state: str
    identity: dict[str, Any]
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class EffectRecord:
    effect_id: str
    operation_id: str
    idempotency_key: str
    effect_kind: str
    exact_identity: dict[str, Any]
    state: EffectState
    receipt: dict[str, Any] | None
    planned_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class EventReceipt:
    event_id: str
    disposition: EventDisposition
    body_sha256: str


@dataclass(frozen=True, slots=True)
class ServiceRecord:
    service_instance_id: str
    service_kind: str
    run_id: str
    attempt_id: str
    master_instance_id: str | None
    epoch: int
    endpoint: str
    protocol: str
    tls_fingerprint: str | None
    capabilities: tuple[str, ...]
    canonical_revision: int | None
    schema_version: str | None
    lease_until: datetime
    state: str
    latest_event_id: str
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class CheckpointHead:
    service_kind: str
    generation: int
    current_checkpoint_id: str | None
    previous_checkpoint_id: str | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class ResourceLeaseRecord:
    lease_id: str
    resource_kind: str
    resource_ref: str
    holder_id: str
    epoch: int
    acquired_at: datetime
    lease_until: datetime
    released_at: datetime | None


@dataclass(frozen=True, slots=True)
class KaggleResearchRecord:
    research_id: str
    owner_subject: str
    alias: str | None
    title: str
    goal: str
    state: KaggleResearchState
    primary_dataset_ref: str
    notebook_ref: str | None
    current_revision_id: str | None
    active_run_id: str | None
    last_completed_run_id: str | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class KaggleRevisionRecord:
    revision_id: str
    research_id: str
    revision_no: int
    parent_revision_id: str | None
    state: KaggleRevisionState
    code_file: str
    kernel_type: str
    language: str
    source_utf8: str
    source_sha256: str
    runtime: dict[str, Any]
    inputs: list[dict[str, Any]]
    inputs_sha256: str
    provider_source_version: int | None
    created_at: datetime
    frozen_at: datetime | None


@dataclass(frozen=True, slots=True)
class KaggleRunRecord:
    run_id: str
    research_id: str
    revision_id: str
    attempt_no: int
    retry_of_run_id: str | None
    operation_id: str
    effect_id: str | None
    state: KaggleRunState
    provider_run_ref: str | None
    provider_kernel_id: str | None
    provider_source_version: int | None
    provider_source_sha256: str | None
    last_provider_status: str | None
    failure_summary: str | None
    next_poll_at: datetime | None
    poll_attempts: int
    output_manifest_sha256: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    updated_at: datetime


@dataclass(frozen=True, slots=True)
class KaggleArtifactRecord:
    artifact_id: str
    run_id: str
    path: str
    role: str
    media_type: str
    byte_size: int
    sha256: str
    storage_mode: str
    cache_relpath: str | None
    created_at: datetime
