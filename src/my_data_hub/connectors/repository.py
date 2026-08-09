from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol
from uuid import UUID

from my_data_hub.connectors.contracts import ConnectorReceipt, ValidatedEnvelope


@dataclass(frozen=True, slots=True)
class AcceptanceIdentity:
    connector_id: str
    idempotency_key: str


@dataclass(frozen=True, slots=True)
class AcceptanceSubmission:
    """Immutable evidence passed to one repository transaction."""

    identity: AcceptanceIdentity
    batch_id: UUID
    payload_sha256: str
    envelope_sha256: str
    exact_bytes_sha256: str
    exact_bytes: bytes
    validated: ValidatedEnvelope

    @classmethod
    def from_validated(cls, validated: ValidatedEnvelope) -> AcceptanceSubmission:
        envelope = validated.envelope
        return cls(
            identity=AcceptanceIdentity(envelope.connector_id, envelope.idempotency_key),
            batch_id=envelope.batch_id,
            payload_sha256=envelope.payload_sha256,
            envelope_sha256=validated.envelope_sha256,
            exact_bytes_sha256=validated.exact_bytes_sha256,
            exact_bytes=validated.exact_bytes,
            validated=validated,
        )


@dataclass(frozen=True, slots=True)
class ExistingAcceptance:
    identity: AcceptanceIdentity
    batch_id: UUID
    payload_sha256: str
    envelope_sha256: str
    receipt: ConnectorReceipt


class ReplayDisposition(StrEnum):
    EXACT_REPLAY = "exact_replay"
    CONFLICTING_REPLAY = "conflicting_replay"


def classify_replay(
    existing: ExistingAcceptance,
    incoming: AcceptanceSubmission,
) -> ReplayDisposition:
    """Classify a row locked by connector/idempotency identity.

    Payload equality alone is insufficient: changing a batch ID, observation period,
    correction link, or other attested metadata under an existing identity is a
    conflicting replay.  JSON member order and insignificant whitespace do not cause a
    conflict because ``envelope_sha256`` hashes the canonical semantic envelope.
    """
    if (
        existing.identity == incoming.identity
        and existing.batch_id == incoming.batch_id
        and existing.payload_sha256 == incoming.payload_sha256
        and existing.envelope_sha256 == incoming.envelope_sha256
    ):
        return ReplayDisposition.EXACT_REPLAY
    return ReplayDisposition.CONFLICTING_REPLAY


@dataclass(frozen=True, slots=True)
class QuarantineEvidence:
    quarantine_id: UUID
    reason: str
    identity: AcceptanceIdentity
    incoming_batch_id: UUID
    existing_batch_id: UUID | None
    incoming_payload_sha256: str
    existing_payload_sha256: str | None
    incoming_envelope_sha256: str
    existing_envelope_sha256: str | None


class AcceptanceDisposition(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED = "replayed"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class RepositoryDecision:
    disposition: AcceptanceDisposition
    receipt: ConnectorReceipt | None = None
    quarantine: QuarantineEvidence | None = None

    def __post_init__(self) -> None:
        has_receipt = self.receipt is not None
        has_quarantine = self.quarantine is not None
        if self.disposition is AcceptanceDisposition.QUARANTINED:
            if has_receipt or not has_quarantine:
                raise ValueError("quarantined decision requires only quarantine evidence")
        elif not has_receipt or has_quarantine:
            raise ValueError("accepted/replayed decision requires only a receipt")


class ConnectorAcceptanceRepository(Protocol):
    """PostgreSQL-oriented atomic connector intake boundary.

    Implementations must perform identity lookup/locking, immutable batch/payload
    persistence, receipt creation, and conflicting-replay quarantine in one transaction.
    They must enforce unique ``(connector_id, idempotency_key)`` and ``batch_id`` values.
    No method in this interface writes shared canonical domain tables.
    """

    def accept(self, submission: AcceptanceSubmission) -> RepositoryDecision: ...
