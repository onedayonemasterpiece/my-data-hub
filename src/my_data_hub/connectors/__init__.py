"""Versioned connector contracts, intake semantics, and durable producer delivery."""

from my_data_hub.connectors.contracts import (
    CONTRACT_VERSION,
    ConnectorCheckpointRequest,
    ConnectorCheckpointStatusReceipt,
    ConnectorContractError,
    ConnectorDurabilityReceipt,
    ConnectorDurabilityState,
    ConnectorEnvelope,
    ConnectorReceipt,
    DeliveryMode,
    ValidatedEnvelope,
    canonical_json_bytes,
    payload_sha256,
    validate_envelope_bytes,
)
from my_data_hub.connectors.postgres import (
    CommitReceipt,
    PostgresConnectorAcceptanceRepository,
    PostgresDailyStatisticsCommitter,
)
from my_data_hub.connectors.repository import (
    AcceptanceDisposition,
    AcceptanceSubmission,
    ConnectorAcceptanceRepository,
    ReplayDisposition,
    RepositoryDecision,
    classify_replay,
)
from my_data_hub.connectors.runtime import (
    ActiveMasterConnectorRuntime,
    ConnectorCapabilityBlocked,
    ConnectorPrincipal,
    ConnectorSessionBroker,
    DirectoryConnectorSessionBroker,
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
    "ActiveMasterConnectorRuntime",
    "CommitReceipt",
    "ConnectorAcceptanceRepository",
    "ConnectorAuthorizationError",
    "ConnectorCapabilityBlocked",
    "ConnectorCheckpointRequest",
    "ConnectorCheckpointStatusReceipt",
    "ConnectorContractError",
    "ConnectorDeliveryService",
    "ConnectorDurabilityReceipt",
    "ConnectorDurabilityState",
    "ConnectorEnvelope",
    "ConnectorIntakeService",
    "ConnectorPrincipal",
    "ConnectorReceipt",
    "ConnectorSessionBroker",
    "ConnectorTransport",
    "DeliveryMode",
    "DirectoryConnectorSessionBroker",
    "DurableConnectorSpool",
    "PostgresConnectorAcceptanceRepository",
    "PostgresDailyStatisticsCommitter",
    "ReplayDisposition",
    "RepositoryDecision",
    "SyntheticConnectorProducer",
    "ValidatedEnvelope",
    "canonical_json_bytes",
    "classify_replay",
    "payload_sha256",
    "validate_envelope_bytes",
]
