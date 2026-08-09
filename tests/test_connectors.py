from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from uuid import uuid5

import pytest

from my_data_hub.connectors.contracts import (
    ConnectorContractError,
    ConnectorReceipt,
    ReceiptStatus,
    canonical_json_bytes,
    payload_sha256,
    validate_envelope_bytes,
)
from my_data_hub.connectors.repository import (
    AcceptanceDisposition,
    AcceptanceSubmission,
    ExistingAcceptance,
    QuarantineEvidence,
    ReplayDisposition,
    RepositoryDecision,
    classify_replay,
)
from my_data_hub.connectors.service import ConnectorAuthorizationError, ConnectorIntakeService
from my_data_hub.connectors.spool import (
    ConnectorDeliveryService,
    DeliveryDisposition,
    DeliveryResult,
    DurableConnectorSpool,
    SpoolConflict,
)
from my_data_hub.connectors.synthetic import SYNTHETIC_NAMESPACE, SyntheticConnectorProducer

ROOT = Path(__file__).resolve().parents[1]
ACCEPTED_AT = datetime(2026, 8, 9, 12, tzinfo=UTC)


class MemoryAcceptanceRepository:
    """Transaction-shaped pure fake for core replay semantics."""

    def __init__(self) -> None:
        self.accepted: dict[tuple[str, str], ExistingAcceptance] = {}
        self.quarantines: list[QuarantineEvidence] = []

    def accept(self, submission: AcceptanceSubmission) -> RepositoryDecision:
        key = (submission.identity.connector_id, submission.identity.idempotency_key)
        existing = self.accepted.get(key)
        if existing is not None:
            disposition = classify_replay(existing, submission)
            if disposition is ReplayDisposition.EXACT_REPLAY:
                return RepositoryDecision(AcceptanceDisposition.REPLAYED, receipt=existing.receipt)
            quarantine = QuarantineEvidence(
                quarantine_id=uuid5(SYNTHETIC_NAMESPACE, f"quarantine:{submission.envelope_sha256}"),
                reason="conflicting_replay",
                identity=submission.identity,
                incoming_batch_id=submission.batch_id,
                existing_batch_id=existing.batch_id,
                incoming_payload_sha256=submission.payload_sha256,
                existing_payload_sha256=existing.payload_sha256,
                incoming_envelope_sha256=submission.envelope_sha256,
                existing_envelope_sha256=existing.envelope_sha256,
            )
            self.quarantines.append(quarantine)
            return RepositoryDecision(AcceptanceDisposition.QUARANTINED, quarantine=quarantine)

        receipt = ConnectorReceipt(
            receipt_id=uuid5(SYNTHETIC_NAMESPACE, f"receipt:{submission.envelope_sha256}"),
            status=ReceiptStatus.ACCEPTED,
            connector_id=submission.identity.connector_id,
            batch_id=submission.batch_id,
            idempotency_key=submission.identity.idempotency_key,
            payload_sha256=submission.payload_sha256,
            envelope_sha256=submission.envelope_sha256,
            accepted_at=ACCEPTED_AT,
        )
        self.accepted[key] = ExistingAcceptance(
            identity=submission.identity,
            batch_id=submission.batch_id,
            payload_sha256=submission.payload_sha256,
            envelope_sha256=submission.envelope_sha256,
            receipt=receipt,
        )
        return RepositoryDecision(AcceptanceDisposition.ACCEPTED, receipt=receipt)


def synthetic_bytes(*, sequence: int = 1, values: dict[str, int] | None = None) -> bytes:
    return SyntheticConnectorProducer().exact_bytes(
        date(2026, 8, 9),
        sequence=sequence,
        values=values,
    )


def test_documented_example_validates_payload_hash_and_count() -> None:
    raw = (ROOT / "examples/contracts/data-connector-envelope.v1.example.json").read_bytes()
    validated = validate_envelope_bytes(raw)

    assert validated.envelope.record_count == len(validated.envelope.inline_records or [])
    assert validated.envelope.payload_sha256 == payload_sha256(
        validated.envelope.inline_records or []
    )
    assert validated.exact_bytes == raw


def test_canonical_json_is_stable_and_uses_ecmascript_number_thresholds() -> None:
    first = {"z": 1e-7, "a": 1e20, "middle": -0.0}
    second = {"middle": -0.0, "a": 1e20, "z": 1e-7}

    assert canonical_json_bytes(first) == canonical_json_bytes(second)
    assert canonical_json_bytes(first) == b'{"a":100000000000000000000,"middle":0,"z":1e-7}'
    with pytest.raises(ConnectorContractError, match="IEEE-754"):
        canonical_json_bytes({"unsafe_integer": 2**53})


@pytest.mark.parametrize(
    ("mutation", "message"),
    [
        (lambda value: value.update({"unknown": True}), "extra"),
        (lambda value: value.update({"record_count": 2}), "record_count"),
        (lambda value: value.update({"payload_sha256": "0" * 64}), "payload_sha256"),
        (lambda value: value.update({"contract_version": "future.v2"}), "contract_version"),
    ],
)
def test_runtime_validation_rejects_unknown_fields_count_hash_and_version(
    mutation: object,
    message: str,
) -> None:
    value = json.loads(synthetic_bytes())
    mutation(value)  # type: ignore[operator]
    with pytest.raises(ConnectorContractError, match=message):
        validate_envelope_bytes(canonical_json_bytes(value))


def test_runtime_validation_rejects_duplicate_keys_and_body_limit() -> None:
    with pytest.raises(ConnectorContractError, match="duplicate JSON object key"):
        validate_envelope_bytes(b'{"contract_version":"x","contract_version":"y"}')
    with pytest.raises(ConnectorContractError, match="exceeds"):
        validate_envelope_bytes(synthetic_bytes(), max_envelope_bytes=10)


def test_exact_replay_returns_same_receipt_but_changed_content_is_quarantined() -> None:
    repository = MemoryAcceptanceRepository()
    service = ConnectorIntakeService(repository)
    exact = synthetic_bytes()

    accepted = service.submit(
        exact,
        authenticated_connector_id="synthetic.daily-statistics",
    )
    reordered = json.dumps(json.loads(exact), ensure_ascii=False, indent=2).encode()
    replayed = service.submit(
        reordered,
        authenticated_connector_id="synthetic.daily-statistics",
    )

    changed = json.loads(exact)
    changed["inline_records"][0]["counts"]["accepted"] = 99
    changed["payload_sha256"] = payload_sha256(changed["inline_records"])
    conflict = service.submit(
        canonical_json_bytes(changed),
        authenticated_connector_id="synthetic.daily-statistics",
    )

    assert accepted.disposition is AcceptanceDisposition.ACCEPTED
    assert replayed.disposition is AcceptanceDisposition.REPLAYED
    assert replayed.receipt is accepted.receipt
    assert conflict.disposition is AcceptanceDisposition.QUARANTINED
    assert conflict.quarantine is repository.quarantines[0]
    assert len(repository.accepted) == 1


def test_authenticated_principal_cannot_submit_for_another_connector() -> None:
    repository = MemoryAcceptanceRepository()
    service = ConnectorIntakeService(repository)

    with pytest.raises(ConnectorAuthorizationError, match="not bound"):
        service.submit(synthetic_bytes(), authenticated_connector_id="other.connector")
    assert repository.accepted == {}


class RecordingTransport:
    def __init__(self, *, fail: bool) -> None:
        self.fail = fail
        self.submissions: list[bytes] = []

    def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult:
        self.submissions.append(exact_envelope_bytes)
        if self.fail:
            raise TimeoutError("synthetic outage")
        validated = validate_envelope_bytes(exact_envelope_bytes)
        envelope = validated.envelope
        receipt = ConnectorReceipt(
            receipt_id=uuid5(SYNTHETIC_NAMESPACE, f"delivery:{validated.envelope_sha256}"),
            status=ReceiptStatus.ACCEPTED,
            connector_id=envelope.connector_id,
            batch_id=envelope.batch_id,
            idempotency_key=envelope.idempotency_key,
            payload_sha256=envelope.payload_sha256,
            envelope_sha256=validated.envelope_sha256,
            accepted_at=ACCEPTED_AT,
        )
        return DeliveryResult(DeliveryDisposition.ACCEPTED, receipt=receipt)


def test_outage_spool_survives_restart_and_eventually_delivers_exact_bytes(tmp_path: Path) -> None:
    exact = synthetic_bytes()
    queued_at = datetime(2026, 8, 9, 10, tzinfo=UTC)
    first_spool = DurableConnectorSpool(tmp_path / "spool")
    original = first_spool.enqueue(exact, queued_at=queued_at)
    unavailable = RecordingTransport(fail=True)

    first = ConnectorDeliveryService(first_spool, unavailable).deliver_ready(now=queued_at)
    assert first.deferred == 1
    assert original.envelope_path.exists()

    restarted_spool = DurableConnectorSpool(tmp_path / "spool")
    available = RecordingTransport(fail=False)
    second = ConnectorDeliveryService(restarted_spool, available).deliver_ready(
        now=queued_at + timedelta(seconds=2)
    )

    assert second.delivered == 1
    assert unavailable.submissions == [exact]
    assert available.submissions == [exact]
    assert restarted_spool.pending(ready_at=queued_at + timedelta(seconds=2)) == []
    assert len(list(restarted_spool.receipts_dir.glob("*.json"))) == 1
    with pytest.raises(SpoolConflict, match="durable receipt"):
        restarted_spool.enqueue(exact)


def test_terminal_conflict_is_retained_in_local_quarantine(tmp_path: Path) -> None:
    class ConflictTransport:
        def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult:
            return DeliveryResult(DeliveryDisposition.CONFLICT, message="server hash conflict")

    spool = DurableConnectorSpool(tmp_path / "spool")
    item = spool.enqueue(synthetic_bytes(), queued_at=ACCEPTED_AT)
    summary = ConnectorDeliveryService(spool, ConflictTransport()).deliver_ready(now=ACCEPTED_AT)

    assert summary.quarantined == 1
    assert not item.envelope_path.exists()
    assert (spool.quarantine_dir / f"{item.spool_id}.json").read_bytes() == item.exact_bytes
    evidence = json.loads((spool.quarantine_dir / f"{item.spool_id}.state.json").read_bytes())
    assert evidence["reason"] == "conflict"


def test_synthetic_producer_is_deterministic_and_changes_identity_by_sequence() -> None:
    producer = SyntheticConnectorProducer()
    first = producer.exact_bytes(date(2026, 8, 9), sequence=7)
    replay = producer.exact_bytes(date(2026, 8, 9), sequence=7)
    correction = producer.exact_bytes(date(2026, 8, 9), sequence=8)

    assert first == replay
    assert validate_envelope_bytes(first).envelope.batch_id != validate_envelope_bytes(
        correction
    ).envelope.batch_id
