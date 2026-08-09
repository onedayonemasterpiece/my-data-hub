"""Versioned connector contracts, intake semantics, and durable producer delivery."""

from my_data_hub.connectors.contracts import (
    CONTRACT_VERSION,
    ConnectorContractError,
    ConnectorEnvelope,
    ConnectorReceipt,
    ValidatedEnvelope,
    canonical_json_bytes,
    payload_sha256,
    validate_envelope_bytes,
)
from my_data_hub.connectors.repository import (
    AcceptanceDisposition,
    AcceptanceSubmission,
    ConnectorAcceptanceRepository,
    ReplayDisposition,
    RepositoryDecision,
    classify_replay,
)
from my_data_hub.connectors.service import ConnectorAuthorizationError, ConnectorIntakeService
from my_data_hub.connectors.spool import (
    ConnectorDeliveryService,
    ConnectorTransport,
    DurableConnectorSpool,
)
from my_data_hub.connectors.synthetic import SyntheticConnectorProducer

__all__ = [
    "CONTRACT_VERSION",
    "AcceptanceDisposition",
    "AcceptanceSubmission",
    "ConnectorAcceptanceRepository",
    "ConnectorAuthorizationError",
    "ConnectorContractError",
    "ConnectorDeliveryService",
    "ConnectorEnvelope",
    "ConnectorIntakeService",
    "ConnectorReceipt",
    "ConnectorTransport",
    "DurableConnectorSpool",
    "ReplayDisposition",
    "RepositoryDecision",
    "SyntheticConnectorProducer",
    "ValidatedEnvelope",
    "canonical_json_bytes",
    "classify_replay",
    "payload_sha256",
    "validate_envelope_bytes",
]
