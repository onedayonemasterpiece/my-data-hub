"""Connector canonical-commit to verified-checkpoint closure."""

from __future__ import annotations

import inspect
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Protocol
from uuid import UUID

from my_data_hub.connectors.contracts import (
    ConnectorCheckpointRequest,
    ConnectorCheckpointState,
    ConnectorCheckpointStatusReceipt,
    ConnectorDurabilityReceipt,
    ConnectorDurabilityState,
    canonical_json_bytes,
    sha256_bytes,
)
from my_data_hub.connectors.errors import ConnectorCapabilityBlocked


class ConnectorDurabilityConflict(RuntimeError):
    pass


class ConnectorDurabilityRepository(Protocol):
    def get_durability_receipt(self, batch_id: UUID) -> ConnectorDurabilityReceipt | None: ...

    def record_checkpoint_request(
        self,
        batch_id: UUID,
        *,
        request: ConnectorCheckpointRequest,
        operation: ConnectorCheckpointStatusReceipt,
    ) -> ConnectorDurabilityReceipt: ...

    def record_checkpoint_status(
        self,
        batch_id: UUID,
        *,
        status: ConnectorCheckpointStatusReceipt,
    ) -> ConnectorDurabilityReceipt: ...


class ConnectorCheckpointGateway(Protocol):
    """Durable idempotent control-plane checkpoint interface.

    ``request_checkpoint`` must return the same operation for the same exact
    request and reject a changed body under an existing ``request_id``.
    """

    def request_checkpoint(
        self, request: ConnectorCheckpointRequest
    ) -> ConnectorCheckpointStatusReceipt | Any: ...

    def checkpoint_status(
        self, operation_id: str
    ) -> ConnectorCheckpointStatusReceipt | Any: ...


class VerifiedCheckpointCoordinator(Protocol):
    """Injected master-checkpoint boundary.

    The connector plane deliberately does not know how the master creates or
    publishes a checkpoint.  The control/master owner may inject only these two
    exact callables.  Absence is a capability blocker, never an invitation to
    infer durability from the current checkpoint head.
    """

    def request_verified_checkpoint(
        self, *, operation_id: str, canonical_revision: int, idempotency_key: str
    ) -> Mapping[str, Any] | Any: ...

    def checkpoint_status(self, operation_id: str) -> Mapping[str, Any] | Any: ...


@dataclass(slots=True)
class CoordinatedConnectorCheckpointGateway:
    """Adapt the exact injected coordinator contract to connector receipts."""

    coordinator: VerifiedCheckpointCoordinator

    @staticmethod
    def operation_id(request_id: str) -> str:
        return f"connector-checkpoint:{request_id}"

    def request_checkpoint(
        self, request: ConnectorCheckpointRequest
    ) -> ConnectorCheckpointStatusReceipt | Any:
        operation_id = self.operation_id(request.request_id)
        result = self.coordinator.request_verified_checkpoint(
            operation_id=operation_id,
            canonical_revision=request.canonical_revision,
            idempotency_key=request.request_id,
        )
        if inspect.isawaitable(result):
            return self._request_async(result, request, operation_id)
        return self._receipt(result, request_id=request.request_id, operation_id=operation_id)

    async def _request_async(
        self, result: Any, request: ConnectorCheckpointRequest, operation_id: str
    ) -> ConnectorCheckpointStatusReceipt:
        observed = await result
        return self._receipt(
            observed, request_id=request.request_id, operation_id=operation_id
        )

    def checkpoint_status(self, operation_id: str) -> ConnectorCheckpointStatusReceipt | Any:
        request_id = self._request_id(operation_id)
        result = self.coordinator.checkpoint_status(operation_id)
        if inspect.isawaitable(result):
            return self._status_async(result, request_id, operation_id)
        return self._receipt(result, request_id=request_id, operation_id=operation_id)

    async def _status_async(
        self, result: Any, request_id: str, operation_id: str
    ) -> ConnectorCheckpointStatusReceipt:
        return self._receipt(
            await result, request_id=request_id, operation_id=operation_id
        )

    @staticmethod
    def _request_id(operation_id: str) -> str:
        prefix = "connector-checkpoint:"
        request_id = operation_id.removeprefix(prefix)
        if not operation_id.startswith(prefix) or len(request_id) != 64 or any(
            character not in "0123456789abcdef" for character in request_id
        ):
            raise ConnectorDurabilityConflict("checkpoint operation identity is invalid")
        return request_id

    @staticmethod
    def _receipt(
        value: Mapping[str, Any] | Any, *, request_id: str, operation_id: str
    ) -> ConnectorCheckpointStatusReceipt:
        if not isinstance(value, Mapping):
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_CHECKPOINT_COORDINATOR_RECEIPT_INVALID", retryable=True
            )
        if (
            value.get("operation_id") != operation_id
            or value.get("idempotency_key") != request_id
            or not isinstance(value.get("canonical_revision"), int)
            or isinstance(value.get("canonical_revision"), bool)
        ):
            raise ConnectorDurabilityConflict(
                "checkpoint coordinator receipt differs from the exact connector request"
            )
        raw_state = str(value.get("state", ""))
        state_map = {
            "REQUESTED": ConnectorCheckpointState.REQUESTED,
            "RUNNING": ConnectorCheckpointState.RUNNING,
            "CHECKPOINTING": ConnectorCheckpointState.RUNNING,
            "FAILED": ConnectorCheckpointState.FAILED,
            "FENCED": ConnectorCheckpointState.FENCED,
            "ORPHANED": ConnectorCheckpointState.ORPHANED,
            "DURABLE_COMPLETE": ConnectorCheckpointState.DURABLE_COMPLETE,
        }
        state = state_map.get(raw_state)
        if state is None:
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_CHECKPOINT_COORDINATOR_STATE_INVALID", retryable=True
            )
        terminal: dict[str, Any] = {}
        if state is ConnectorCheckpointState.DURABLE_COMPLETE:
            checkpoint_id = value.get("checkpoint_id")
            verified_at = value.get("verified_at")
            if (
                value.get("checkpoint_status") != "VERIFIED"
                or value.get("current_checkpoint_id") != checkpoint_id
                or not isinstance(checkpoint_id, str)
                or not checkpoint_id
                or not isinstance(value.get("manifest_sha256"), str)
                or not isinstance(verified_at, (str, datetime))
            ):
                raise ConnectorCapabilityBlocked(
                    "CONNECTOR_CHECKPOINT_VERIFIED_RECEIPT_INCOMPLETE", retryable=True
                )
            terminal = {
                "checkpoint_id": checkpoint_id,
                "manifest_sha256": value["manifest_sha256"],
                "verified_at": verified_at,
            }
        elif state in {
            ConnectorCheckpointState.FAILED,
            ConnectorCheckpointState.FENCED,
            ConnectorCheckpointState.ORPHANED,
        }:
            failure_code = value.get("failure_code")
            if not isinstance(failure_code, str) or not failure_code:
                raise ConnectorCapabilityBlocked(
                    "CONNECTOR_CHECKPOINT_FAILURE_RECEIPT_INCOMPLETE", retryable=True
                )
            terminal = {"failure_code": failure_code}
        return ConnectorCheckpointStatusReceipt(
            request_id=request_id,
            operation_id=operation_id,
            state=state,
            canonical_revision=int(value["canonical_revision"]),
            **terminal,
        )


def build_connector_checkpoint_gateway(
    coordinator: VerifiedCheckpointCoordinator | None,
) -> CoordinatedConnectorCheckpointGateway | None:
    """Return a gateway only for the exact callable coordinator contract."""

    if coordinator is None:
        return None
    if not callable(getattr(coordinator, "request_verified_checkpoint", None)) or not callable(
        getattr(coordinator, "checkpoint_status", None)
    ):
        raise ConnectorCapabilityBlocked(
            "CONNECTOR_VERIFIED_CHECKPOINT_COORDINATOR_INVALID", retryable=False
        )
    return CoordinatedConnectorCheckpointGateway(coordinator)


async def _await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def checkpoint_request_for(receipt: ConnectorDurabilityReceipt) -> ConnectorCheckpointRequest:
    if receipt.canonical_revision is None:
        raise ValueError("connector batch has not reached canonical commit")
    acceptance = receipt.acceptance
    identity = {
        "schema_version": "my-data-hub-connector-checkpoint-request.v1",
        "connector_id": acceptance.connector_id,
        "batch_id": str(acceptance.batch_id),
        "canonical_revision": receipt.canonical_revision,
        "payload_sha256": acceptance.payload_sha256,
        "envelope_sha256": acceptance.envelope_sha256,
    }
    request_id = sha256_bytes(canonical_json_bytes(identity))
    return ConnectorCheckpointRequest(request_id=request_id, **identity)


@dataclass(slots=True)
class ConnectorDurabilityService:
    repository: ConnectorDurabilityRepository
    checkpoint_gateway: ConnectorCheckpointGateway | None

    async def advance(self, batch_id: UUID) -> ConnectorDurabilityReceipt:
        current = self.repository.get_durability_receipt(batch_id)
        if current is None:
            raise LookupError("connector batch durability receipt was not found")
        if current.state in {
            ConnectorDurabilityState.ACCEPTED,
            ConnectorDurabilityState.DURABLE_COMPLETE,
            ConnectorDurabilityState.FAILED,
        }:
            return current
        if self.checkpoint_gateway is None:
            # No repository mutation has happened in this call.
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_CHECKPOINT_GATEWAY_UNAVAILABLE",
                retryable=True,
            )

        if current.state is ConnectorDurabilityState.CANONICAL_COMMITTED:
            request = checkpoint_request_for(current)
            operation = await _await(self.checkpoint_gateway.request_checkpoint(request))
            if not isinstance(operation, ConnectorCheckpointStatusReceipt):
                raise ConnectorCapabilityBlocked(
                    "CONNECTOR_CHECKPOINT_REQUEST_RECEIPT_INVALID", retryable=True
                )
            self._validate_operation(request, operation)
            current = self.repository.record_checkpoint_request(
                batch_id, request=request, operation=operation
            )

        assert current.checkpoint_operation_id is not None
        observed = await _await(
            self.checkpoint_gateway.checkpoint_status(current.checkpoint_operation_id)
        )
        if not isinstance(observed, ConnectorCheckpointStatusReceipt):
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_CHECKPOINT_STATUS_RECEIPT_INVALID", retryable=True
            )
        expected = checkpoint_request_for(current)
        self._validate_operation(expected, observed)
        return self.repository.record_checkpoint_status(batch_id, status=observed)

    @staticmethod
    def _validate_operation(
        request: ConnectorCheckpointRequest,
        operation: ConnectorCheckpointStatusReceipt,
    ) -> None:
        if (
            operation.request_id != request.request_id
            or operation.canonical_revision != request.canonical_revision
        ):
            raise ConnectorDurabilityConflict(
                "checkpoint receipt differs from exact connector checkpoint request"
            )
        if operation.state is ConnectorCheckpointState.DURABLE_COMPLETE:
            assert operation.checkpoint_id is not None
            assert operation.manifest_sha256 is not None


@dataclass(slots=True)
class ConnectorDurabilitySupervisor:
    """Restart-safe bounded scan over PostgreSQL durability work."""

    repository: ConnectorDurabilityRepository
    checkpoint_gateway: ConnectorCheckpointGateway | None

    async def reconcile_once(self, *, limit: int = 25) -> int:
        if not 1 <= limit <= 100:
            raise ValueError("connector durability scan limit must be between 1 and 100")
        pending = getattr(self.repository, "pending_durability_batch_ids", None)
        if not callable(pending):
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_DURABILITY_SCAN_UNAVAILABLE", retryable=True
            )
        batch_ids = await _await(pending(limit=limit))
        if not isinstance(batch_ids, (list, tuple)) or any(
            not isinstance(batch_id, UUID) for batch_id in batch_ids
        ):
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_DURABILITY_SCAN_INVALID", retryable=True
            )
        completed = 0
        service = ConnectorDurabilityService(self.repository, self.checkpoint_gateway)
        for batch_id in batch_ids:
            receipt = await service.advance(batch_id)
            if receipt.state is ConnectorDurabilityState.DURABLE_COMPLETE:
                completed += 1
        return completed
