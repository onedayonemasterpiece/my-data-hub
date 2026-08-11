"""Connector canonical-commit to verified-checkpoint closure."""

from __future__ import annotations

import inspect
from dataclasses import dataclass
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
from my_data_hub.connectors.runtime import ConnectorCapabilityBlocked


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
