from __future__ import annotations

import hashlib
import json
import math
import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Any, Protocol

from my_data_hub.connectors.contracts import (
    ConnectorDurabilityReceipt,
    ConnectorDurabilityState,
    ConnectorReceipt,
    ValidatedEnvelope,
    canonical_json_bytes,
    sha256_bytes,
    validate_envelope_bytes,
)


class SpoolConflict(RuntimeError):
    pass


class SpoolIntegrityError(RuntimeError):
    pass


def _format_time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("spool timestamps must include a timezone offset")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise SpoolIntegrityError("spool state contains a naive timestamp")
    return parsed


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_write(path: Path, content: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


@dataclass(frozen=True, slots=True)
class SpoolState:
    queued_at: datetime
    attempts: int = 0
    next_attempt_at: datetime | None = None
    last_error: str | None = None
    acceptance: ConnectorReceipt | None = None

    def to_bytes(self) -> bytes:
        return canonical_json_bytes(
            {
                "attempts": self.attempts,
                "acceptance": (
                    self.acceptance.model_dump(mode="json")
                    if self.acceptance is not None
                    else None
                ),
                "last_error": self.last_error,
                "next_attempt_at": (
                    _format_time(self.next_attempt_at) if self.next_attempt_at is not None else None
                ),
                "queued_at": _format_time(self.queued_at),
            }
        )

    @classmethod
    def from_bytes(cls, raw: bytes) -> SpoolState:
        try:
            value = json.loads(raw)
            return cls(
                queued_at=_parse_time(value["queued_at"]),
                attempts=int(value["attempts"]),
                next_attempt_at=(
                    _parse_time(value["next_attempt_at"])
                    if value.get("next_attempt_at") is not None
                    else None
                ),
                last_error=value.get("last_error"),
                acceptance=(
                    ConnectorReceipt.model_validate(value["acceptance"])
                    if value.get("acceptance") is not None
                    else None
                ),
            )
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise SpoolIntegrityError("invalid durable spool state") from exc


@dataclass(frozen=True, slots=True)
class SpoolItem:
    spool_id: str
    envelope_path: Path
    state_path: Path
    validated: ValidatedEnvelope
    state: SpoolState

    @property
    def exact_bytes(self) -> bytes:
        return self.validated.exact_bytes


class DurableConnectorSpool:
    """Restart-safe file spool retaining the producer's exact envelope bytes."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.pending_dir = root / "pending"
        self.receipts_dir = root / "receipts"
        self.quarantine_dir = root / "quarantine"
        for directory in (self.pending_dir, self.receipts_dir, self.quarantine_dir):
            directory.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def identity_spool_id(validated: ValidatedEnvelope) -> str:
        envelope = validated.envelope
        return sha256_bytes(
            canonical_json_bytes(
                {
                    "connector_id": envelope.connector_id,
                    "idempotency_key": envelope.idempotency_key,
                }
            )
        )

    def enqueue(
        self,
        exact_bytes: bytes,
        *,
        queued_at: datetime | None = None,
    ) -> SpoolItem:
        validated = validate_envelope_bytes(exact_bytes)
        spool_id = self.identity_spool_id(validated)
        envelope_path = self.pending_dir / f"{spool_id}.json"
        state_path = self.pending_dir / f"{spool_id}.state.json"
        if envelope_path.exists():
            existing = validate_envelope_bytes(envelope_path.read_bytes())
            if existing.envelope_sha256 != validated.envelope_sha256:
                raise SpoolConflict(
                    "connector/idempotency identity is already spooled with different content"
                )
            state = self._load_or_recover_state(envelope_path, state_path)
            return SpoolItem(spool_id, envelope_path, state_path, existing, state)
        if (self.receipts_dir / f"{spool_id}.json").exists():
            raise SpoolConflict("connector/idempotency identity already has a durable receipt")

        state = SpoolState(queued_at=queued_at or datetime.now(UTC))
        # Envelope first: a crash before the state write leaves recoverable source evidence.
        _atomic_write(envelope_path, exact_bytes)
        _atomic_write(state_path, state.to_bytes())
        return SpoolItem(spool_id, envelope_path, state_path, validated, state)

    def _load_or_recover_state(self, envelope_path: Path, state_path: Path) -> SpoolState:
        if state_path.exists():
            return SpoolState.from_bytes(state_path.read_bytes())
        recovered = SpoolState(
            queued_at=datetime.fromtimestamp(envelope_path.stat().st_mtime, tz=UTC),
            last_error="state_recovered_after_interrupted_enqueue",
        )
        _atomic_write(state_path, recovered.to_bytes())
        return recovered

    def pending(self, *, ready_at: datetime | None = None) -> list[SpoolItem]:
        ready_at = ready_at or datetime.now(UTC)
        if ready_at.tzinfo is None:
            raise ValueError("ready_at must include a timezone offset")
        items: list[SpoolItem] = []
        for envelope_path in self.pending_dir.glob("*.json"):
            if envelope_path.name.endswith(".state.json"):
                continue
            spool_id = envelope_path.stem
            state_path = self.pending_dir / f"{spool_id}.state.json"
            try:
                validated = validate_envelope_bytes(envelope_path.read_bytes())
            except Exception as exc:
                raise SpoolIntegrityError(f"invalid pending envelope {envelope_path.name}") from exc
            if self.identity_spool_id(validated) != spool_id:
                raise SpoolIntegrityError(f"pending envelope identity mismatch: {envelope_path.name}")
            state = self._load_or_recover_state(envelope_path, state_path)
            if state.next_attempt_at is None or state.next_attempt_at <= ready_at:
                items.append(SpoolItem(spool_id, envelope_path, state_path, validated, state))
        return sorted(items, key=lambda item: (item.state.queued_at, item.spool_id))

    def record_retry(
        self,
        item: SpoolItem,
        *,
        error: str,
        next_attempt_at: datetime,
    ) -> None:
        if next_attempt_at.tzinfo is None:
            raise ValueError("next_attempt_at must include a timezone offset")
        state = SpoolState(
            queued_at=item.state.queued_at,
            attempts=item.state.attempts + 1,
            next_attempt_at=next_attempt_at,
            last_error=error[:2000],
            acceptance=item.state.acceptance,
        )
        _atomic_write(item.state_path, state.to_bytes())

    def record_acceptance(self, item: SpoolItem, receipt: ConnectorReceipt) -> None:
        envelope = item.validated.envelope
        if (
            receipt.connector_id != envelope.connector_id
            or receipt.idempotency_key != envelope.idempotency_key
            or receipt.batch_id != envelope.batch_id
            or receipt.payload_sha256 != envelope.payload_sha256
            or receipt.envelope_sha256 != item.validated.envelope_sha256
        ):
            raise SpoolIntegrityError("receipt does not attest the spooled envelope")
        receipt_path = self.receipts_dir / f"{item.spool_id}.accepted.json"
        _atomic_write(
            receipt_path,
            canonical_json_bytes(receipt.model_dump(mode="json")),
        )
        state = SpoolState(
            queued_at=item.state.queued_at,
            attempts=item.state.attempts,
            next_attempt_at=None,
            last_error=None,
            acceptance=receipt,
        )
        _atomic_write(item.state_path, state.to_bytes())

    def acknowledge_durable(
        self, item: SpoolItem, receipt: ConnectorDurabilityReceipt
    ) -> None:
        envelope = item.validated.envelope
        acceptance = receipt.acceptance
        if receipt.state is not ConnectorDurabilityState.DURABLE_COMPLETE:
            raise SpoolIntegrityError("source evidence can be removed only at DURABLE_COMPLETE")
        if (
            acceptance.connector_id != envelope.connector_id
            or acceptance.idempotency_key != envelope.idempotency_key
            or acceptance.batch_id != envelope.batch_id
            or acceptance.payload_sha256 != envelope.payload_sha256
            or acceptance.envelope_sha256 != item.validated.envelope_sha256
        ):
            raise SpoolIntegrityError("durability receipt does not attest the spooled envelope")
        receipt_path = self.receipts_dir / f"{item.spool_id}.json"
        _atomic_write(receipt_path, canonical_json_bytes(receipt.model_dump(mode="json")))
        # The verified checkpoint receipt is durable before exact source evidence
        # is removed.  ACCEPTED/CANONICAL_COMMITTED never enter this branch.
        item.envelope_path.unlink(missing_ok=True)
        item.state_path.unlink(missing_ok=True)
        (self.receipts_dir / f"{item.spool_id}.accepted.json").unlink(missing_ok=True)
        _fsync_directory(self.pending_dir)

    def quarantine(
        self,
        item: SpoolItem,
        *,
        reason: str,
        details: dict[str, Any] | None = None,
        quarantined_at: datetime | None = None,
    ) -> None:
        target_envelope = self.quarantine_dir / f"{item.spool_id}.json"
        target_state = self.quarantine_dir / f"{item.spool_id}.state.json"
        evidence = {
            "details": details or {},
            "envelope_sha256": item.validated.envelope_sha256,
            "exact_bytes_sha256": item.validated.exact_bytes_sha256,
            "quarantined_at": _format_time(quarantined_at or datetime.now(UTC)),
            "reason": reason,
            "spool_state": json.loads(item.state.to_bytes()),
        }
        _atomic_write(target_envelope, item.exact_bytes)
        _atomic_write(target_state, canonical_json_bytes(evidence))
        item.envelope_path.unlink(missing_ok=True)
        item.state_path.unlink(missing_ok=True)
        _fsync_directory(self.pending_dir)


class DeliveryDisposition(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED = "replayed"
    RETRY = "retry"
    CONFLICT = "conflict"
    REJECTED = "rejected"
    AUTH_FAILURE = "auth_failure"


@dataclass(frozen=True, slots=True)
class DeliveryResult:
    disposition: DeliveryDisposition
    receipt: ConnectorReceipt | None = None
    message: str = ""
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        success = self.disposition in {
            DeliveryDisposition.ACCEPTED,
            DeliveryDisposition.REPLAYED,
        }
        if success != (self.receipt is not None):
            raise ValueError("only accepted/replayed delivery results carry a receipt")


class ConnectorTransport(Protocol):
    def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult: ...

    def durability(self, acceptance: ConnectorReceipt) -> DurabilityDeliveryResult: ...


class DurabilityDisposition(StrEnum):
    PENDING = "pending"
    COMPLETE = "complete"
    RETRY = "retry"
    CONFLICT = "conflict"
    REJECTED = "rejected"
    AUTH_FAILURE = "auth_failure"


@dataclass(frozen=True, slots=True)
class DurabilityDeliveryResult:
    disposition: DurabilityDisposition
    receipt: ConnectorDurabilityReceipt | None = None
    message: str = ""
    retry_after_seconds: float | None = None

    def __post_init__(self) -> None:
        observed = self.disposition in {
            DurabilityDisposition.PENDING,
            DurabilityDisposition.COMPLETE,
        }
        if observed != (self.receipt is not None):
            raise ValueError("only observed durability results carry a receipt")
        if self.disposition is DurabilityDisposition.COMPLETE and (
            self.receipt is None
            or self.receipt.state is not ConnectorDurabilityState.DURABLE_COMPLETE
        ):
            raise ValueError("complete delivery requires DURABLE_COMPLETE receipt")


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    initial_seconds: float = 1.0
    maximum_seconds: float = 300.0
    jitter_fraction: float = 0.2

    def __post_init__(self) -> None:
        if self.initial_seconds <= 0 or self.maximum_seconds < self.initial_seconds:
            raise ValueError("retry bounds must be positive and ordered")
        if not 0 <= self.jitter_fraction <= 1:
            raise ValueError("retry jitter fraction must be between 0 and 1")

    def delay(self, attempts_already_made: int, *, jitter_key: str = "") -> timedelta:
        attempts = max(0, attempts_already_made)
        maximum_exponent = max(
            0, math.ceil(math.log2(self.maximum_seconds / self.initial_seconds))
        )
        base = min(
            self.maximum_seconds,
            self.initial_seconds * (2 ** min(attempts, maximum_exponent)),
        )
        digest = hashlib.sha256(
            f"{jitter_key}:{attempts_already_made}".encode()
        ).digest()
        unit = int.from_bytes(digest[:8], "big") / (2**64 - 1)
        factor = 1.0 + self.jitter_fraction * ((2.0 * unit) - 1.0)
        seconds = min(self.maximum_seconds, max(0.0, base * factor))
        return timedelta(seconds=seconds)


@dataclass(frozen=True, slots=True)
class DeliverySummary:
    attempted: int = 0
    delivered: int = 0
    deferred: int = 0
    quarantined: int = 0


@dataclass(slots=True)
class ConnectorDeliveryService:
    spool: DurableConnectorSpool
    transport: ConnectorTransport
    retry_policy: RetryPolicy = RetryPolicy()

    def deliver_ready(
        self,
        *,
        now: datetime | None = None,
        limit: int = 100,
    ) -> DeliverySummary:
        now = now or datetime.now(UTC)
        attempted = delivered = deferred = quarantined = 0
        for item in self.spool.pending(ready_at=now)[:limit]:
            attempted += 1
            if item.state.acceptance is not None:
                try:
                    durability = self.transport.durability(item.state.acceptance)
                except Exception as exc:
                    durability = DurabilityDeliveryResult(
                        DurabilityDisposition.RETRY,
                        message=f"durability status exception: {type(exc).__name__}: {exc}",
                    )
                if durability.disposition is DurabilityDisposition.COMPLETE:
                    assert durability.receipt is not None
                    self.spool.acknowledge_durable(item, durability.receipt)
                    delivered += 1
                elif durability.disposition in {
                    DurabilityDisposition.PENDING,
                    DurabilityDisposition.RETRY,
                }:
                    retry_delay = (
                        timedelta(seconds=durability.retry_after_seconds)
                        if durability.retry_after_seconds is not None
                        else self.retry_policy.delay(
                            item.state.attempts, jitter_key=str(item.spool_id)
                        )
                    )
                    self.spool.record_retry(
                        item,
                        error=durability.message or "durability not complete",
                        next_attempt_at=now + retry_delay,
                    )
                    deferred += 1
                else:
                    self.spool.quarantine(
                        item,
                        reason=f"durability_{durability.disposition.value}",
                        details={"message": durability.message},
                        quarantined_at=now,
                    )
                    quarantined += 1
                continue
            try:
                result = self.transport.submit(item.exact_bytes)
            except Exception as exc:  # transport failures are ambiguous and retryable
                result = DeliveryResult(
                    DeliveryDisposition.RETRY,
                    message=f"transport exception: {type(exc).__name__}: {exc}",
                )
            if result.disposition in {
                DeliveryDisposition.ACCEPTED,
                DeliveryDisposition.REPLAYED,
            }:
                assert result.receipt is not None
                self.spool.record_acceptance(item, result.receipt)
                deferred += 1
            elif result.disposition is DeliveryDisposition.RETRY:
                retry_delay = (
                    timedelta(seconds=result.retry_after_seconds)
                    if result.retry_after_seconds is not None
                    else self.retry_policy.delay(
                        item.state.attempts, jitter_key=str(item.spool_id)
                    )
                )
                self.spool.record_retry(
                    item,
                    error=result.message or "retryable delivery failure",
                    next_attempt_at=now + retry_delay,
                )
                deferred += 1
            else:
                self.spool.quarantine(
                    item,
                    reason=result.disposition.value,
                    details={"message": result.message},
                    quarantined_at=now,
                )
                quarantined += 1
        return DeliverySummary(attempted, delivered, deferred, quarantined)
