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
