from __future__ import annotations

from collections.abc import Awaitable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from my_data_hub.auth.control import OAuthAuditEvent
from my_data_hub.mcp.oauth import AccessIdentity


class MasterState(StrEnum):
    ABSENT = "ABSENT"
    REQUESTED = "REQUESTED"
    STARTING = "STARTING"
    RESTORING = "RESTORING"
    REGISTERING = "REGISTERING"
    ACTIVE = "ACTIVE"
    DRAINING = "DRAINING"
    CHECKPOINTING = "CHECKPOINTING"
    STOPPED = "STOPPED"
    FAILED = "FAILED"
    FENCED = "FENCED"
    CHECKPOINT_FAILED = "CHECKPOINT_FAILED"
    ORPHANED = "ORPHANED"


@dataclass(frozen=True, slots=True)
class MasterSnapshot:
    state: MasterState
    operation_id: str | None = None
    instance_id: str | None = None
    epoch: int | None = None
    canonical_revision: int | None = None
    lease_expires_at: str | None = None
    capabilities: frozenset[str] = field(default_factory=frozenset)

    def __post_init__(self) -> None:
        if self.state is MasterState.ACTIVE:
            if not self.instance_id or self.epoch is None or self.epoch < 1:
                raise ValueError("ACTIVE master requires instance_id and positive epoch")
        elif self.epoch is not None and self.epoch < 1:
            raise ValueError("master epoch must be positive")

    def public(self) -> dict[str, Any]:
        return {
            "master_state": self.state.value,
            "operation_id": self.operation_id,
            "instance_id": self.instance_id,
            "master_epoch": self.epoch,
            "canonical_revision": self.canonical_revision,
            "lease_expires_at": self.lease_expires_at,
            "capabilities": sorted(self.capabilities),
        }


@dataclass(frozen=True, slots=True)
class EnsureMasterReceipt:
    operation_id: str
    state: MasterState
    duplicate: bool
    intent: str

    def __post_init__(self) -> None:
        if not self.operation_id:
            raise ValueError("ensure_master must return a durable operation_id")

    def public(self) -> dict[str, Any]:
        return {
            "operation_id": self.operation_id,
            "master_state": self.state.value,
            "duplicate": self.duplicate,
            "intent": self.intent,
            "terminal": False,
        }


@dataclass(frozen=True, slots=True)
class ExecutionLimits:
    timeout_ms: int = 5_000
    max_rows: int = 200
    max_bytes: int = 262_144

    def __post_init__(self) -> None:
        if not 100 <= self.timeout_ms <= 30_000:
            raise ValueError("execution timeout must be between 100 and 30000 ms")
        if not 1 <= self.max_rows <= 1_000:
            raise ValueError("row cap must be between 1 and 1000")
        if not 1_024 <= self.max_bytes <= 2_097_152:
            raise ValueError("byte cap must be between 1 KiB and 2 MiB")


@dataclass(frozen=True, slots=True)
class SessionRequest:
    principal: AccessIdentity
    master_instance_id: str
    epoch: int
    role: str
    tool: str
    limits: ExecutionLimits


@runtime_checkable
class MasterSession(Protocol):
    """One short-lived, role-bound, epoch-bound direct master session."""

    async def execute(self, arguments: Mapping[str, Any]) -> Mapping[str, Any]: ...

    async def close(self) -> None: ...


@runtime_checkable
class MasterResolver(Protocol):
    """Resolve ACTIVE master state from the control plane, never a static URL."""

    def resolve_master(
        self, principal: AccessIdentity
    ) -> MasterSnapshot | Awaitable[MasterSnapshot]: ...

    def ensure_master(
        self, principal: AccessIdentity, *, intent: str
    ) -> EnsureMasterReceipt | Awaitable[EnsureMasterReceipt]: ...


@runtime_checkable
class MasterSessionBroker(Protocol):
    """Issue restricted ephemeral data-plane sessions for an exact epoch."""

    def issue_session(
        self, request: SessionRequest
    ) -> MasterSession | Awaitable[MasterSession]: ...


@runtime_checkable
class ControlPlaneReader(Protocol):
    """Bounded control-ledger reads that work with no master runtime."""

    def invoke_control(
        self, tool: str, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]: ...


@runtime_checkable
class MCPAuditSink(Protocol):
    def record_mcp_audit(
        self, event: OAuthAuditEvent
    ) -> Awaitable[None] | None: ...


@dataclass(frozen=True, slots=True)
class WritePermit:
    permit_id: str
    tool: str
    principal: str
    client_id: str
    master_epoch: int
    canonical_revision: int
    expires_at: int
    preview_bound: bool
    checkpoint_lifecycle_bound: bool
    pre_change_checkpoint_verified: bool = False
    allowed_resource_class: str | None = None
    private_resource_only: bool = True


@runtime_checkable
class WriteGate(Protocol):
    """Control-plane preview/checkpoint policy; absence always denies writes."""

    def authorize_write(
        self,
        *,
        principal: AccessIdentity,
        tool: str,
        arguments: Mapping[str, Any],
        master: MasterSnapshot,
    ) -> WritePermit | Awaitable[WritePermit]: ...
