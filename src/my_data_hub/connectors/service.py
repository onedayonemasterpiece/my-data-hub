from __future__ import annotations

from dataclasses import dataclass

from my_data_hub.connectors.contracts import ValidatedEnvelope, validate_envelope_bytes
from my_data_hub.connectors.repository import (
    AcceptanceSubmission,
    ConnectorAcceptanceRepository,
    RepositoryDecision,
)


class ConnectorAuthorizationError(PermissionError):
    pass


@dataclass(slots=True)
class ConnectorIntakeService:
    """Validate and authenticate intake before one atomic repository call."""

    repository: ConnectorAcceptanceRepository
    max_envelope_bytes: int = 2 * 1024 * 1024

    def validate(
        self,
        exact_bytes: bytes,
        *,
        artifact_record_count: int | None = None,
    ) -> ValidatedEnvelope:
        return validate_envelope_bytes(
            exact_bytes,
            max_envelope_bytes=self.max_envelope_bytes,
            artifact_record_count=artifact_record_count,
        )

    def submit(
        self,
        exact_bytes: bytes,
        *,
        authenticated_connector_id: str,
        authenticated_principal: str | None = None,
        correlation_id: str | None = None,
        artifact_record_count: int | None = None,
    ) -> RepositoryDecision:
        validated = self.validate(
            exact_bytes,
            artifact_record_count=artifact_record_count,
        )
        if validated.envelope.connector_id != authenticated_connector_id:
            raise ConnectorAuthorizationError(
                "authenticated principal is not bound to the submitted connector_id"
            )
        return self.repository.accept(
            AcceptanceSubmission.from_validated(
                validated,
                authenticated_principal=authenticated_principal,
                correlation_id=correlation_id,
            )
        )
