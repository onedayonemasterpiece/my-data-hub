from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import sha256_value


class ProviderKind(StrEnum):
    NOTEBOOK = "notebook"
    DATASET = "dataset"


class ControlClass(StrEnum):
    ORCHESTRATOR_PROTECTED = "orchestrator_protected"
    MCP_MANAGED = "mcp_managed"
    MCP_EXCHANGE = "mcp_exchange"
    EXTERNAL_READ_ONLY = "external_read_only"


class Origin(StrEnum):
    ORCHESTRATOR = "orchestrator"
    MCP = "mcp"
    EXTERNAL = "external"
    MIGRATION = "migration"


class ProviderAction(StrEnum):
    """Supported capabilities.

    Cancellation is intentionally not represented: the provider compatibility
    boundary has not proven a cancellation operation. Dataset creation is exposed
    only by the Kaggle adapter's ``create_private_dataset`` method.
    """

    LIST = "list"
    READ_STATUS = "read_status"
    READ_SOURCE = "read_source"
    READ_OUTPUT = "read_output"
    PUSH = "push"
    RUN = "run"
    DOWNLOAD = "download"
    CREATE_VERSION = "create_version"
    DELETE = "delete"


class ProviderFingerprint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    algorithm: Literal["sha256"] = "sha256"
    value: str = Field(pattern=r"^[a-f0-9]{64}$")


class ObservedProviderResource(BaseModel):
    """Provider observation with no provider-supplied authorization fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str = Field(min_length=1, max_length=100)
    provider_ref: str = Field(min_length=3, max_length=500)
    kind: ProviderKind
    owner: str = Field(min_length=1, max_length=300)
    private: bool | None = None
    fingerprint: ProviderFingerprint | None = None
    state: str = Field(default="unknown", min_length=1, max_length=100)
    observed_at: datetime


class ProviderResource(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    provider: str = Field(min_length=1, max_length=100)
    provider_ref: str = Field(min_length=3, max_length=500)
    kind: ProviderKind
    owner: str = Field(min_length=1, max_length=300)
    origin: Origin
    control_class: ControlClass
    private: bool | None = None
    fingerprint: ProviderFingerprint | None = None
    state: str = Field(default="unknown", min_length=1, max_length=100)
    observed_at: datetime
    workload: str | None = Field(default=None, min_length=1, max_length=200)
    policy_revision: str = Field(default="provider-control.v1", min_length=1, max_length=100)

    @model_validator(mode="after")
    def control_class_has_valid_provenance(self) -> ProviderResource:
        if self.control_class == ControlClass.ORCHESTRATOR_PROTECTED and self.origin not in {
            Origin.ORCHESTRATOR,
            Origin.MIGRATION,
        }:
            raise ValueError("protected resources require orchestrator or migration origin")
        if self.control_class == ControlClass.MCP_EXCHANGE and self.kind != ProviderKind.DATASET:
            raise ValueError("mcp_exchange resources must be datasets")
        if self.control_class in {ControlClass.MCP_MANAGED, ControlClass.MCP_EXCHANGE} and self.private is not True:
            raise ValueError("MCP-controlled provider resources must be private")
        if self.control_class == ControlClass.EXTERNAL_READ_ONLY and self.origin != Origin.EXTERNAL:
            raise ValueError("external_read_only resources require external origin")
        return self


class ResourceLease(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    lease_id: UUID
    provider_ref: str = Field(min_length=3, max_length=500)
    principal: str = Field(min_length=1, max_length=300)
    fencing_token: int = Field(ge=1)
    acquired_at: datetime
    expires_at: datetime

    @model_validator(mode="after")
    def expiry_follows_acquisition(self) -> ResourceLease:
        if self.expires_at <= self.acquired_at:
            raise ValueError("lease expiry must follow acquisition")
        return self

    def assert_held(self, *, principal: str, provider_ref: str, now: datetime) -> None:
        if principal != self.principal or provider_ref != self.provider_ref:
            raise LeaseDenied("lease principal/resource mismatch")
        if now >= self.expires_at:
            raise LeaseDenied("lease expired")


class ProviderOperation(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    principal: str = Field(min_length=1, max_length=300)
    provider_ref: str = Field(min_length=3, max_length=500)
    action: ProviderAction
    expected_fingerprint: ProviderFingerprint
    lease_id: UUID
    fencing_token: int = Field(ge=1)
    arguments: dict[str, Any] = Field(default_factory=dict)
    request_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    requested_at: datetime

    @classmethod
    def create(
        cls,
        *,
        operation_id: UUID,
        idempotency_key: str,
        principal: str,
        provider_ref: str,
        action: ProviderAction,
        expected_fingerprint: ProviderFingerprint,
        lease_id: UUID,
        fencing_token: int,
        arguments: dict[str, Any],
        requested_at: datetime,
    ) -> ProviderOperation:
        unsigned = {
            "operation_id": operation_id,
            "idempotency_key": idempotency_key,
            "principal": principal,
            "provider_ref": provider_ref,
            "action": action,
            "expected_fingerprint": expected_fingerprint,
            "lease_id": lease_id,
            "fencing_token": fencing_token,
            "arguments": arguments,
            "requested_at": requested_at,
        }
        draft = cls.model_construct(**unsigned, request_hash="0" * 64)
        return cls(**unsigned, request_hash=draft.calculated_request_hash())

    def calculated_request_hash(self) -> str:
        return sha256_value(
            {
                "principal": self.principal,
                "provider_ref": self.provider_ref,
                "action": self.action.value,
                "expected_fingerprint": self.expected_fingerprint.model_dump(mode="json"),
                "lease_id": str(self.lease_id),
                "fencing_token": self.fencing_token,
                "arguments": self.arguments,
            }
        )

    @model_validator(mode="after")
    def request_hash_is_exact(self) -> ProviderOperation:
        if self.request_hash != self.calculated_request_hash():
            raise ValueError("request_hash does not match the operation payload")
        return self


class OperationLedger:
    """Small provider-neutral idempotency guard; persistence belongs to PostgreSQL."""

    def __init__(self) -> None:
        self._by_key: dict[str, ProviderOperation] = {}

    def record(self, operation: ProviderOperation) -> ProviderOperation:
        existing = self._by_key.get(operation.idempotency_key)
        if existing is None:
            self._by_key[operation.idempotency_key] = operation
            return operation
        if existing.request_hash != operation.request_hash:
            raise IdempotencyConflict("idempotency key was already used for a different request")
        return existing


class ProviderControlError(RuntimeError):
    """Base class for fail-closed provider-control errors."""


class LeaseDenied(ProviderControlError):
    pass


class IdempotencyConflict(ProviderControlError):
    pass


class StaleFingerprint(ProviderControlError):
    pass
