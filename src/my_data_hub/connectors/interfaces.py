"""Callable non-push connector interfaces with fail-closed capability checks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol

from my_data_hub.connectors.contracts import (
    ConnectorEnvelope,
    DeliveryMode,
    ValidatedEnvelope,
    sha256_bytes,
    validate_envelope_bytes,
)
from my_data_hub.connectors.errors import ConnectorCapabilityBlocked
from my_data_hub.connectors.spool import DurableConnectorSpool, SpoolItem


@dataclass(frozen=True, slots=True)
class ConnectorRegistration:
    connector_id: str
    delivery_mode: DeliveryMode
    status: str
    policy_revision: int
    enabled_data_products: frozenset[str]

    def authorize(self, envelope: ConnectorEnvelope) -> None:
        if self.status != "active":
            raise ConnectorCapabilityBlocked(
                "CONNECTOR_REGISTRY_PAUSED",
                retryable=False,
            )
        if envelope.connector_id != self.connector_id:
            raise PermissionError("connector registry identity differs from envelope")
        if envelope.delivery_mode is not self.delivery_mode:
            raise PermissionError("delivery_mode is not authorized by connector registry policy")
        if envelope.data_product not in self.enabled_data_products:
            raise PermissionError("data product is disabled by connector registry policy")


class OrchestratorPullAdapter(Protocol):
    def pull(self, registration: ConnectorRegistration) -> bytes: ...


@dataclass(slots=True)
class OrchestratorPullInterface:
    spool: DurableConnectorSpool
    adapter: OrchestratorPullAdapter | None

    def run_once(self, registration: ConnectorRegistration) -> SpoolItem:
        if registration.delivery_mode is not DeliveryMode.PULL:
            raise ConnectorCapabilityBlocked("ORCHESTRATOR_PULL_MODE_NOT_AUTHORIZED", retryable=False)
        if registration.status != "active":
            # The Region Talk registration intentionally stops here: no adapter
            # call and no producer-spool mutation can occur while it is paused.
            raise ConnectorCapabilityBlocked("CONNECTOR_REGISTRY_PAUSED", retryable=False)
        if self.adapter is None:
            raise ConnectorCapabilityBlocked("ORCHESTRATOR_PULL_ADAPTER_UNAVAILABLE", retryable=True)
        exact_bytes = self.adapter.pull(registration)
        validated = validate_envelope_bytes(exact_bytes)
        registration.authorize(validated.envelope)
        return self.spool.enqueue(exact_bytes)


class PrivateArtifactReader(Protocol):
    def read_private(self, locator: str, *, maximum_bytes: int) -> bytes: ...


@dataclass(slots=True)
class PrivateArtifactInterface:
    reader: PrivateArtifactReader | None
    maximum_bytes: int = 10_737_418_240

    def fetch_verified(self, validated: ValidatedEnvelope) -> bytes:
        envelope = validated.envelope
        if envelope.delivery_mode is not DeliveryMode.ARTIFACT_HANDOFF or envelope.artifact is None:
            raise ConnectorCapabilityBlocked("PRIVATE_ARTIFACT_MODE_NOT_AUTHORIZED", retryable=False)
        if self.reader is None:
            raise ConnectorCapabilityBlocked("PRIVATE_ARTIFACT_READER_UNAVAILABLE", retryable=True)
        artifact = self.reader.read_private(
            envelope.artifact.locator,
            maximum_bytes=min(self.maximum_bytes, envelope.artifact.byte_size),
        )
        if len(artifact) != envelope.artifact.byte_size:
            raise ValueError("private artifact byte size differs from manifest")
        if sha256_bytes(artifact) != envelope.artifact.sha256:
            raise ValueError("private artifact SHA-256 differs from manifest")
        return artifact


class TrustedLandingWriter(Protocol):
    def land(self, validated: ValidatedEnvelope) -> str: ...


@dataclass(slots=True)
class TrustedLandingInterface:
    writer: TrustedLandingWriter | None

    def land(self, validated: ValidatedEnvelope, registration: ConnectorRegistration) -> str:
        if registration.delivery_mode is not DeliveryMode.TRUSTED_DATABASE_LANDING:
            raise ConnectorCapabilityBlocked("TRUSTED_LANDING_MODE_NOT_AUTHORIZED", retryable=False)
        registration.authorize(validated.envelope)
        if self.writer is None:
            raise ConnectorCapabilityBlocked("TRUSTED_LANDING_WRITER_UNAVAILABLE", retryable=True)
        return self.writer.land(validated)
