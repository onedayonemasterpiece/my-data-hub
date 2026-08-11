from __future__ import annotations

import hashlib
import random
import threading
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from itertools import pairwise
from pathlib import Path
from typing import Any
from urllib.parse import urlparse
from uuid import uuid4

from pydantic import ValidationError

from .events import ArtifactRef, RuntimeEvent, RuntimeEventType
from .sanitize import sanitize
from .spool import JsonlEventSpool
from .transport import CallbackTransport, UrllibCallbackTransport, json_body


@dataclass(frozen=True, slots=True)
class RetryPolicy:
    max_attempts: int = 4
    base_seconds: float = 0.25
    max_seconds: float = 5.0
    jitter_ratio: float = 0.2
    timeout_seconds: float = 10.0

    def __post_init__(self) -> None:
        if self.max_attempts < 1 or self.base_seconds < 0 or self.max_seconds < self.base_seconds:
            raise ValueError("invalid bounded retry policy")
        if not 0 <= self.jitter_ratio <= 1 or self.timeout_seconds <= 0:
            raise ValueError("invalid retry jitter or timeout")

    def delays(self, event_id: str) -> tuple[float, ...]:
        seed = int(hashlib.sha256(event_id.encode()).hexdigest()[:16], 16)
        rng = random.Random(seed)
        values = []
        for attempt in range(self.max_attempts - 1):
            base = min(self.max_seconds, self.base_seconds * (2**attempt))
            jitter = base * self.jitter_ratio * rng.uniform(-1, 1)
            values.append(max(0.0, min(self.max_seconds, base + jitter)))
        return tuple(values)


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    event_id: str | None
    status: str
    attempts: int
    durable_local: bool


class RuntimeClient:
    """Generic notebook callback SDK with durable local replay."""

    def __init__(
        self,
        *,
        callback_url: str,
        run_secret: str,
        run_id: str,
        attempt_id: str,
        service_instance_id: str,
        source_identity: str,
        source_version: str,
        epoch: int,
        spool_path: Path,
        transport: CallbackTransport | None = None,
        retry_policy: RetryPolicy | None = None,
        now: Callable[[], datetime] | None = None,
        sleep: Callable[[float], None] | None = None,
        heartbeat_interval_seconds: float = 30.0,
        automatic_replay_limit: int = 100,
    ) -> None:
        parsed = urlparse(callback_url)
        if parsed.scheme.lower() != "https" or not parsed.netloc:
            raise ValueError("runtime callback URL must use HTTPS")
        if len(run_secret) < 16:
            raise ValueError("per-run callback secret must be at least 16 characters")
        if epoch < 1 or heartbeat_interval_seconds <= 0:
            raise ValueError("runtime epoch and heartbeat interval must be positive")
        if not 1 <= automatic_replay_limit <= 1_000:
            raise ValueError("automatic replay limit must be 1..1000")
        self.callback_url = callback_url
        self._run_secret = run_secret
        self.run_id = run_id
        self.attempt_id = attempt_id
        self.service_instance_id = service_instance_id
        self.source_identity = source_identity
        self.source_version = source_version
        self.epoch = epoch
        self.transport = transport or UrllibCallbackTransport()
        self.retry_policy = retry_policy or RetryPolicy()
        self._now = now or (lambda: datetime.now(UTC))
        self._sleep = sleep or time.sleep
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.automatic_replay_limit = automatic_replay_limit
        self.spool = JsonlEventSpool(spool_path)
        self._sequence = self.spool.highest_local_sequence()
        self._last_heartbeat_at: datetime | None = None
        self._lock = threading.RLock()
        self._delivery_lock = threading.RLock()
        self._heartbeat_stop: threading.Event | None = None
        self._heartbeat_thread: threading.Thread | None = None
        # Construction is the restart boundary: queued callbacks from a prior
        # process are replayed without requiring notebook-specific glue code.
        self.replay_pending(max_events=self.automatic_replay_limit)

    def emit(
        self,
        event_type: RuntimeEventType | str,
        *,
        phase: str | None = None,
        status: str | None = None,
        data: Mapping[str, Any] | None = None,
        artifact_refs: tuple[ArtifactRef, ...] = (),
        metrics: Mapping[str, int | float | str | bool | None] | None = None,
    ) -> DeliveryReceipt:
        event_type = RuntimeEventType(event_type)
        with self._delivery_lock:
            # Every new callback is also an automatic recovery opportunity.
            # Serializing replay + append + delivery preserves local_sequence
            # order across main and heartbeat threads.
            self.replay_pending(max_events=self.automatic_replay_limit)
            return self._emit_new(
                event_type,
                phase=phase,
                status=status,
                data=data,
                artifact_refs=artifact_refs,
                metrics=metrics,
            )

    def _emit_new(
        self,
        event_type: RuntimeEventType,
        *,
        phase: str | None,
        status: str | None,
        data: Mapping[str, Any] | None,
        artifact_refs: tuple[ArtifactRef, ...],
        metrics: Mapping[str, int | float | str | bool | None] | None,
    ) -> DeliveryReceipt:
        now = self._aware_now()
        with self._lock:
            if (
                event_type == RuntimeEventType.RUNTIME_HEARTBEAT
                and self._last_heartbeat_at is not None
                and (now - self._last_heartbeat_at).total_seconds() < self.heartbeat_interval_seconds
            ):
                return DeliveryReceipt(event_id=None, status="coalesced", attempts=0, durable_local=False)
            self._sequence += 1
            sanitized_data = sanitize(dict(data or {}), secrets=(self._run_secret,))
            sanitized_metrics = sanitize(dict(metrics or {}), secrets=(self._run_secret,))
            try:
                event = RuntimeEvent(
                    event_id=str(uuid4()),
                    run_id=self.run_id,
                    attempt_id=self.attempt_id,
                    service_instance_id=self.service_instance_id,
                    source_identity=self.source_identity,
                    source_version=self.source_version,
                    event_type=event_type,
                    emitted_at=now,
                    local_sequence=self._sequence,
                    epoch=self.epoch,
                    phase=phase,
                    status=status,
                    data=sanitized_data,
                    artifact_refs=artifact_refs,
                    metrics=sanitized_metrics,
                )
            except ValidationError:
                self._sequence -= 1
                raise
            payload = event.model_dump(mode="json", by_alias=True, exclude_none=True)
            encoded = json_body(payload)
            if len(encoded) > 64 * 1024:
                self._sequence -= 1
                raise ValueError("runtime event body exceeds 64 KiB")
            self.spool.append_event(payload)
            if event_type == RuntimeEventType.RUNTIME_HEARTBEAT:
                self._last_heartbeat_at = now
        return self._deliver(payload)

    def replay_pending(self, *, max_events: int | None = None) -> list[DeliveryReceipt]:
        if max_events is not None and not 1 <= max_events <= 1_000:
            raise ValueError("replay batch limit must be 1..1000")
        with self._delivery_lock:
            pending = self.spool.pending()
            if max_events is not None:
                pending = pending[:max_events]
            return [self._deliver(event) for event in pending]

    def flush_pending(self, *, max_events: int | None = None) -> bool:
        """Attempt exact replay once and report whether the durable spool is empty."""

        self.replay_pending(max_events=max_events)
        return not self.spool.pending()

    def durable_event_bodies(
        self,
        event_types: tuple[RuntimeEventType, ...],
    ) -> tuple[dict[str, Any], ...]:
        """Read the latest exact durable body for each requested type in order."""

        if not event_types or len(set(event_types)) != len(event_types):
            raise ValueError("durable event body request must contain unique event types")
        requested = {item.value for item in event_types}
        selected: dict[str, dict[str, Any]] = {}
        expected_identity = {
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "service_instance_id": self.service_instance_id,
            "source_identity": self.source_identity,
            "source_version": self.source_version,
            "epoch": self.epoch,
        }
        for event in self.spool.events():
            event_type = event.get("event_type")
            if event_type not in requested:
                continue
            if any(event.get(key) != value for key, value in expected_identity.items()):
                continue
            selected[str(event_type)] = event
        missing = requested - selected.keys()
        if missing:
            raise ValueError(f"durable runtime spool lacks terminal event bodies: {sorted(missing)}")
        result = tuple(selected[item.value] for item in event_types)
        sequences = tuple(event.get("local_sequence") for event in result)
        if not all(isinstance(value, int) for value in sequences) or any(
            left >= right for left, right in pairwise(sequences)
        ):
            raise ValueError("durable terminal event bodies are not in local sequence order")
        return result

    def emit_donor_envelope(self, envelope: Mapping[str, Any]) -> DeliveryReceipt:
        """Adapt the proven events-bot status shape without weakening replay safety.

        events-bot/CherryFlash uses a caller-stable ``event_uid`` for callback
        deduplication.  Preserve that contract by replaying the first durable
        body for a repeated UID rather than manufacturing a new event with a
        different timestamp/local sequence.  The legacy body token is ignored;
        this runtime always authenticates with its per-attempt header secret.
        """

        donor_run_id = envelope.get("run_id")
        if donor_run_id is not None and str(donor_run_id) != self.run_id:
            raise ValueError("donor envelope run_id does not match this exact runtime")
        donor_event = str(envelope.get("event", "progress")).lower()
        donor_event_uid = envelope.get("event_uid")
        if donor_event_uid is not None and (
            not isinstance(donor_event_uid, str) or not 1 <= len(donor_event_uid) <= 200
        ):
            raise ValueError("donor event_uid must be a bounded non-empty string")
        event_type = {
            "alive": RuntimeEventType.RUNTIME_HEARTBEAT,
            "heartbeat": RuntimeEventType.RUNTIME_HEARTBEAT,
            "kernel_started": RuntimeEventType.RUNTIME_STARTED,
            "started": RuntimeEventType.RUNTIME_STARTED,
            "terminal": RuntimeEventType.RUNTIME_TERMINAL,
            "completed": RuntimeEventType.RUNTIME_TERMINAL,
            "done": RuntimeEventType.RUNTIME_TERMINAL,
            "failed": RuntimeEventType.RUNTIME_FAILED,
            "error": RuntimeEventType.RUNTIME_FAILED,
            "service.ready": RuntimeEventType.SERVICE_READY,
            "resource_acquire": RuntimeEventType.RESOURCE_ACQUIRE,
            "resource.acquire": RuntimeEventType.RESOURCE_ACQUIRE,
            "resource_renew": RuntimeEventType.RESOURCE_RENEW,
            "resource.renew": RuntimeEventType.RESOURCE_RENEW,
            "resource_release": RuntimeEventType.RESOURCE_RELEASE,
            "resource.release": RuntimeEventType.RESOURCE_RELEASE,
            "report_written": RuntimeEventType.JOB_RESULT_AVAILABLE,
        }.get(donor_event, RuntimeEventType.RUNTIME_PROGRESS)
        data = {
            "donor_event": donor_event,
            "donor_event_uid": donor_event_uid,
            "progress": envelope.get("progress", {}),
            "resource": envelope.get("resource"),
            "message": envelope.get("message"),
        }
        normalized_data = {key: value for key, value in data.items() if value is not None}
        donor_body_sha256 = hashlib.sha256(
            json_body(
                {
                    "event_type": event_type.value,
                    "phase": envelope.get("phase"),
                    "status": envelope.get("status"),
                    "data": sanitize(normalized_data, secrets=(self._run_secret,)),
                }
            )
        ).hexdigest()
        normalized_data["donor_body_sha256"] = donor_body_sha256
        with self._delivery_lock:
            self.replay_pending(max_events=self.automatic_replay_limit)
            if donor_event_uid is not None:
                for event in self.spool.events():
                    event_data = event.get("data")
                    if not isinstance(event_data, dict) or event_data.get("donor_event_uid") != donor_event_uid:
                        continue
                    if event_data.get("donor_body_sha256") != donor_body_sha256:
                        raise ValueError("donor event_uid was reused with a different callback body")
                    return self._deliver(event)
            return self._emit_new(
                event_type,
                phase=str(envelope["phase"]) if envelope.get("phase") is not None else None,
                status=str(envelope["status"]) if envelope.get("status") is not None else None,
                data=normalized_data,
                artifact_refs=(),
                metrics=None,
            )

    def acquire_resource(self, resource_kind: str, resource_ref: str, lease_until: datetime) -> DeliveryReceipt:
        return self.emit(
            RuntimeEventType.RESOURCE_ACQUIRE,
            data={"resource_kind": resource_kind, "resource_ref": resource_ref, "lease_until": lease_until.isoformat()},
        )

    def renew_resource(self, resource_kind: str, resource_ref: str, lease_until: datetime) -> DeliveryReceipt:
        return self.emit(
            RuntimeEventType.RESOURCE_RENEW,
            data={"resource_kind": resource_kind, "resource_ref": resource_ref, "lease_until": lease_until.isoformat()},
        )

    def release_resource(self, resource_kind: str, resource_ref: str) -> DeliveryReceipt:
        return self.emit(
            RuntimeEventType.RESOURCE_RELEASE,
            data={"resource_kind": resource_kind, "resource_ref": resource_ref},
        )

    def start_heartbeat(
        self,
        progress_provider: Callable[[], Mapping[str, Any]] | None = None,
    ) -> None:
        with self._lock:
            if self._heartbeat_thread and self._heartbeat_thread.is_alive():
                return
            self._heartbeat_stop = threading.Event()

            def run() -> None:
                assert self._heartbeat_stop is not None
                while not self._heartbeat_stop.wait(self.heartbeat_interval_seconds):
                    progress = progress_provider() if progress_provider else {}
                    self.emit(RuntimeEventType.RUNTIME_HEARTBEAT, data=progress)

            self._heartbeat_thread = threading.Thread(target=run, name="content-runtime-heartbeat", daemon=True)
            self._heartbeat_thread.start()

    def stop_heartbeat(self, timeout_seconds: float = 5.0) -> None:
        with self._lock:
            stop = self._heartbeat_stop
            thread = self._heartbeat_thread
            if stop is not None:
                stop.set()
        if thread is not None:
            thread.join(timeout_seconds)

    def _deliver(self, event: dict[str, Any]) -> DeliveryReceipt:
        with self._delivery_lock:
            event_id = str(event["event_id"])
            body = json_body(event)
            headers = {
                "Authorization": f"Bearer {self._run_secret}",
                "Content-Type": "application/json",
                "User-Agent": "my-data-hub-runtime-sdk/1",
            }
            delays = self.retry_policy.delays(event_id)
            attempts = 0
            for attempt in range(self.retry_policy.max_attempts):
                attempts += 1
                try:
                    response = self.transport.post(
                        self.callback_url,
                        body,
                        headers,
                        self.retry_policy.timeout_seconds,
                    )
                    if 200 <= response.status < 300:
                        self.spool.acknowledge(event_id, self._aware_now().isoformat())
                        return DeliveryReceipt(event_id, "delivered", attempts, True)
                    retryable = response.status == 429 or response.status >= 500
                    if not retryable:
                        return DeliveryReceipt(event_id, "rejected", attempts, True)
                except (TimeoutError, ConnectionError, OSError):
                    pass
                if attempt < len(delays):
                    self._sleep(delays[attempt])
            return DeliveryReceipt(event_id, "queued", attempts, True)

    def _aware_now(self) -> datetime:
        value = self._now()
        if value.tzinfo is None:
            raise ValueError("runtime SDK clock must return timezone-aware timestamps")
        return value.astimezone(UTC)
