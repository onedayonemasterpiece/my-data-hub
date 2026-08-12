"""ACTIVE-master connector routing without a static database URL.

The control resolver owns availability/epoch truth.  A separately injected broker
owns the short-lived ``connector`` capability for that exact epoch.  This module
never persists or returns a PostgreSQL URL.
"""

from __future__ import annotations

import asyncio
import inspect
import time
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from my_data_hub.connectors.contracts import ConnectorDurabilityReceipt, ConnectorReceipt
from my_data_hub.connectors.durability import (
    ConnectorCheckpointGateway,
    ConnectorDurabilitySupervisor,
)
from my_data_hub.connectors.errors import ConnectorCapabilityBlocked
from my_data_hub.connectors.repository import RepositoryDecision
from my_data_hub.connectors.service import ConnectorIntakeService
from my_data_hub.mcp.contracts import MasterResolver, MasterSnapshot, MasterState


@dataclass(frozen=True, slots=True)
class ConnectorPrincipal:
    connector_id: str
    subject: str


@dataclass(frozen=True, slots=True)
class ConnectorSessionRequest:
    principal: ConnectorPrincipal
    master_instance_id: str
    epoch: int
    role: str = "connector"
    timeout_ms: int = 30_000

    def __post_init__(self) -> None:
        if self.role != "connector":
            raise ValueError("connector session role must be connector")
        if not self.master_instance_id or self.epoch < 1:
            raise ValueError("connector session requires exact ACTIVE master identity")
        if not 100 <= self.timeout_ms <= 30_000:
            raise ValueError("connector session timeout is outside the bounded contract")


@runtime_checkable
class ConnectorMasterSession(Protocol):
    async def submit(
        self,
        exact_bytes: bytes,
        *,
        authenticated_connector_id: str,
        authenticated_principal: str,
        correlation_id: str,
    ) -> RepositoryDecision: ...

    async def acceptance_receipt(self, batch_id: UUID) -> ConnectorReceipt | None: ...

    async def durability_receipt(self, batch_id: UUID) -> ConnectorDurabilityReceipt | None: ...

    async def health(self, connector_id: str) -> dict[str, Any]: ...

    async def close(self) -> None: ...


@runtime_checkable
class ConnectorSessionBroker(Protocol):
    def issue_connector_session(
        self, request: ConnectorSessionRequest
    ) -> ConnectorMasterSession | Any: ...


async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


@dataclass(slots=True)
class ActiveMasterConnectorRuntime:
    resolver: MasterResolver
    broker: ConnectorSessionBroker | None
    max_envelope_bytes: int = 2 * 1024 * 1024

    async def _active(self, principal: ConnectorPrincipal, *, intent: str) -> MasterSnapshot:
        # LedgerMasterResolver only consumes ``subject``; a connector principal is
        # deliberately not promoted into an OAuth/MCP identity.
        snapshot = await _await(self.resolver.resolve_master(principal))  # type: ignore[arg-type]
        if not isinstance(snapshot, MasterSnapshot):
            raise ConnectorCapabilityBlocked("MASTER_RESOLVER_INVALID", retryable=True)
        if snapshot.state is MasterState.ABSENT:
            ensured = await _await(
                self.resolver.ensure_master(principal, intent=intent)  # type: ignore[arg-type]
            )
            raise ConnectorCapabilityBlocked(
                "MASTER_ENSURE_REQUESTED",
                master_state=ensured.state.value,
                operation_id=ensured.operation_id,
                retryable=True,
            )
        if snapshot.state is not MasterState.ACTIVE:
            raise ConnectorCapabilityBlocked(
                "MASTER_NOT_ACTIVE",
                master_state=snapshot.state.value,
                operation_id=snapshot.operation_id,
                retryable=snapshot.state
                not in {
                    MasterState.FAILED,
                    MasterState.FENCED,
                    MasterState.CHECKPOINT_FAILED,
                    MasterState.ORPHANED,
                },
            )
        # The master advertises concrete data-plane capabilities (``sql``, FTS,
        # pgvector), not service-specific aliases. Connector intake requires only
        # the bounded landing SQL capability and its separately brokered role.
        if "sql" not in snapshot.capabilities:
            raise ConnectorCapabilityBlocked(
                "ACTIVE_MASTER_CONNECTOR_INTAKE_CAPABILITY_MISSING",
                master_state=snapshot.state.value,
                operation_id=snapshot.operation_id,
                retryable=False,
            )
        if self.broker is None:
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_SESSION_BROKER_UNAVAILABLE",
                master_state=snapshot.state.value,
                operation_id=snapshot.operation_id,
                retryable=True,
            )
        return snapshot

    async def _session(
        self, principal: ConnectorPrincipal, *, intent: str
    ) -> ConnectorMasterSession:
        snapshot = await self._active(principal, intent=intent)
        assert snapshot.instance_id is not None and snapshot.epoch is not None
        assert self.broker is not None
        session = await _await(
            self.broker.issue_connector_session(
                ConnectorSessionRequest(
                    principal=principal,
                    master_instance_id=snapshot.instance_id,
                    epoch=snapshot.epoch,
                )
            )
        )
        if not isinstance(session, ConnectorMasterSession):
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_SESSION_INVALID",
                master_state=snapshot.state.value,
                operation_id=snapshot.operation_id,
                retryable=True,
            )
        return session

    async def submit(
        self,
        exact_bytes: bytes,
        *,
        principal: ConnectorPrincipal,
        correlation_id: str,
    ) -> RepositoryDecision:
        session = await self._session(
            principal, intent=f"connector-intake:{principal.connector_id}"
        )
        try:
            return await session.submit(
                exact_bytes,
                authenticated_connector_id=principal.connector_id,
                authenticated_principal=principal.subject,
                correlation_id=correlation_id,
            )
        finally:
            await session.close()

    async def acceptance_receipt(
        self, batch_id: UUID, *, principal: ConnectorPrincipal
    ) -> ConnectorReceipt | None:
        session = await self._session(
            principal, intent=f"connector-status:{principal.connector_id}"
        )
        try:
            return await session.acceptance_receipt(batch_id)
        finally:
            await session.close()

    async def durability_receipt(
        self, batch_id: UUID, *, principal: ConnectorPrincipal
    ) -> ConnectorDurabilityReceipt | None:
        session = await self._session(
            principal, intent=f"connector-status:{principal.connector_id}"
        )
        try:
            return await session.durability_receipt(batch_id)
        finally:
            await session.close()

    async def health(
        self, connector_id: str, *, principal: ConnectorPrincipal
    ) -> dict[str, Any]:
        session = await self._session(principal, intent=f"connector-health:{connector_id}")
        try:
            return await session.health(connector_id)
        finally:
            await session.close()


class PostgresConnectorMasterSession:
    """One transient repository facade around a broker-supplied credential."""

    def __init__(self, repository: Any, *, max_envelope_bytes: int) -> None:
        self.repository = repository
        self.intake = ConnectorIntakeService(repository, max_envelope_bytes=max_envelope_bytes)
        self._closed = False

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("connector master session is closed")

    async def submit(
        self,
        exact_bytes: bytes,
        *,
        authenticated_connector_id: str,
        authenticated_principal: str,
        correlation_id: str,
    ) -> RepositoryDecision:
        self._require_open()
        return await asyncio.to_thread(
            self.intake.submit,
            exact_bytes,
            authenticated_connector_id=authenticated_connector_id,
            authenticated_principal=authenticated_principal,
            correlation_id=correlation_id,
        )

    async def acceptance_receipt(self, batch_id: UUID) -> ConnectorReceipt | None:
        self._require_open()
        return await asyncio.to_thread(self.repository.get_receipt, batch_id)

    async def durability_receipt(self, batch_id: UUID) -> ConnectorDurabilityReceipt | None:
        self._require_open()
        return await asyncio.to_thread(self.repository.get_durability_receipt, batch_id)

    async def health(self, connector_id: str) -> dict[str, Any]:
        self._require_open()
        return await asyncio.to_thread(self.repository.health, connector_id)

    async def close(self) -> None:
        self._closed = True


@dataclass(slots=True)
class DirectoryConnectorSessionBroker:
    """Adapter for an existing epoch credential source; no URL is configured here."""

    credential_source: Any
    max_envelope_bytes: int = 2 * 1024 * 1024

    def issue_connector_session(self, request: ConnectorSessionRequest) -> ConnectorMasterSession:
        from my_data_hub.connectors.postgres import PostgresConnectorAcceptanceRepository
        from my_data_hub.mcp.contracts import ExecutionLimits, SessionRequest

        credential_request = SessionRequest(
            principal=request.principal,  # type: ignore[arg-type]
            master_instance_id=request.master_instance_id,
            epoch=request.epoch,
            role=request.role,
            tool="connector.intake",
            limits=ExecutionLimits(timeout_ms=request.timeout_ms),
        )
        try:
            credential = self.credential_source.load(credential_request)
        except Exception as exc:
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_EPOCH_CREDENTIAL_UNAVAILABLE",
                master_state=MasterState.ACTIVE.value,
                retryable=True,
            ) from exc
        repository = PostgresConnectorAcceptanceRepository(credential.database_url)
        return PostgresConnectorMasterSession(
            repository, max_envelope_bytes=self.max_envelope_bytes
        )


@dataclass(frozen=True, slots=True)
class ConnectorDurabilitySessionRequest:
    master_instance_id: str
    epoch: int
    role: str = "canonical_committer"
    timeout_ms: int = 30_000

    def __post_init__(self) -> None:
        if self.role != "canonical_committer":
            raise ValueError("connector durability role must be canonical_committer")
        if not self.master_instance_id or self.epoch < 1:
            raise ValueError("connector durability session requires exact ACTIVE master identity")


@runtime_checkable
class ConnectorDurabilityMasterSession(Protocol):
    async def probe(self) -> None: ...

    async def reconcile_once(self, *, limit: int) -> int: ...

    async def close(self) -> None: ...


@runtime_checkable
class ConnectorDurabilitySessionBroker(Protocol):
    def issue_durability_session(
        self, request: ConnectorDurabilitySessionRequest
    ) -> ConnectorDurabilityMasterSession | Any: ...


class PostgresConnectorDurabilityMasterSession:
    def __init__(
        self, repository: Any, checkpoint_gateway: ConnectorCheckpointGateway | None
    ) -> None:
        self.repository = repository
        self.supervisor = ConnectorDurabilitySupervisor(repository, checkpoint_gateway)
        self._closed = False

    def _require_open(self) -> None:
        if self._closed:
            raise RuntimeError("connector durability session is closed")

    async def probe(self) -> None:
        self._require_open()
        await asyncio.to_thread(self.repository.pending_durability_batch_ids, limit=1)

    async def reconcile_once(self, *, limit: int) -> int:
        self._require_open()
        return await self.supervisor.reconcile_once(limit=limit)

    async def close(self) -> None:
        self._closed = True


@dataclass(slots=True)
class DirectoryConnectorDurabilitySessionBroker:
    credential_source: Any
    checkpoint_gateway: ConnectorCheckpointGateway | None

    def issue_durability_session(
        self, request: ConnectorDurabilitySessionRequest
    ) -> ConnectorDurabilityMasterSession:
        from my_data_hub.connectors.postgres import PostgresConnectorAcceptanceRepository
        from my_data_hub.mcp.contracts import ExecutionLimits, SessionRequest

        credential_request = SessionRequest(
            principal=connector_principal("durability-supervisor"),  # type: ignore[arg-type]
            master_instance_id=request.master_instance_id,
            epoch=request.epoch,
            role=request.role,
            tool="connector.durability.reconcile",
            limits=ExecutionLimits(timeout_ms=request.timeout_ms),
        )
        try:
            credential = self.credential_source.load(credential_request)
        except Exception as exc:
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_COMMITTER_EPOCH_CREDENTIAL_UNAVAILABLE",
                master_state=MasterState.ACTIVE.value,
                retryable=True,
            ) from exc
        return PostgresConnectorDurabilityMasterSession(
            PostgresConnectorAcceptanceRepository(credential.database_url),
            self.checkpoint_gateway,
        )


@dataclass(slots=True)
class ActiveMasterConnectorDurabilityRuntime:
    resolver: MasterResolver
    broker: ConnectorDurabilitySessionBroker | None
    checkpoint_gateway: ConnectorCheckpointGateway | None

    async def _session(self) -> ConnectorDurabilityMasterSession:
        if self.checkpoint_gateway is None:
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_VERIFIED_CHECKPOINT_COORDINATOR_UNAVAILABLE", retryable=True
            )
        principal = connector_principal("durability-supervisor")
        snapshot = await _await(self.resolver.resolve_master(principal))  # type: ignore[arg-type]
        if not isinstance(snapshot, MasterSnapshot):
            raise ConnectorCapabilityBlocked("MASTER_RESOLVER_INVALID", retryable=True)
        if snapshot.state is MasterState.ABSENT:
            ensured = await _await(
                self.resolver.ensure_master(
                    principal, intent="connector-durability-reconcile"  # type: ignore[arg-type]
                )
            )
            raise ConnectorCapabilityBlocked(
                "MASTER_ENSURE_REQUESTED",
                master_state=ensured.state.value,
                operation_id=ensured.operation_id,
                retryable=True,
            )
        if snapshot.state is not MasterState.ACTIVE:
            raise ConnectorCapabilityBlocked(
                "MASTER_NOT_ACTIVE", master_state=snapshot.state.value, retryable=True
            )
        if "sql" not in snapshot.capabilities:
            raise ConnectorCapabilityBlocked(
                "ACTIVE_MASTER_CONNECTOR_DURABILITY_CAPABILITY_MISSING",
                master_state=snapshot.state.value,
                retryable=False,
            )
        if self.broker is None:
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_DURABILITY_SESSION_BROKER_UNAVAILABLE", retryable=True
            )
        assert snapshot.instance_id is not None and snapshot.epoch is not None
        session = await _await(
            self.broker.issue_durability_session(
                ConnectorDurabilitySessionRequest(snapshot.instance_id, snapshot.epoch)
            )
        )
        if not isinstance(session, ConnectorDurabilityMasterSession):
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_DURABILITY_SESSION_INVALID", retryable=True
            )
        return session

    async def preflight(self) -> None:
        session = await self._session()
        try:
            await session.probe()
        finally:
            await session.close()

    async def reconcile_once(self, *, limit: int = 25) -> int:
        session = await self._session()
        try:
            return await session.reconcile_once(limit=limit)
        finally:
            await session.close()


def connector_principal(connector_id: str) -> ConnectorPrincipal:
    return ConnectorPrincipal(connector_id=connector_id, subject=f"service:{connector_id}")


def unix_now() -> int:
    """Small seam retained for callers that attach expiry telemetry without secrets."""

    return int(time.time())
