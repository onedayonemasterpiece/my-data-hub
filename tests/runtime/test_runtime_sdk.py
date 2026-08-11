from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

import jsonschema
import pytest

from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.runtime_sdk import RetryPolicy, RuntimeClient, RuntimeEventType, TransportResponse


class ScriptedTransport:
    def __init__(self, script):  # type: ignore[no-untyped-def]
        self.script = list(script)
        self.calls: list[tuple[str, bytes, dict[str, str], float]] = []

    def post(self, url: str, body: bytes, headers: dict[str, str], timeout_seconds: float) -> TransportResponse:
        self.calls.append((url, body, headers, timeout_seconds))
        outcome = self.script.pop(0) if self.script else TransportResponse(200)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome


def client(tmp_path: Path, transport: ScriptedTransport, clock: DeterministicClock) -> RuntimeClient:
    return RuntimeClient(
        callback_url="https://control.example/internal/runtime/events",
        run_secret="this-is-a-private-run-secret",
        run_id="run-1",
        attempt_id="attempt-1",
        service_instance_id="service-1",
        source_identity="my-data-hub/test-runtime",
        source_version="git:abcdef",
        epoch=1,
        spool_path=tmp_path / "runtime-private" / "events.jsonl",
        transport=transport,
        retry_policy=RetryPolicy(max_attempts=3, base_seconds=0.1, max_seconds=0.2, jitter_ratio=0),
        now=clock.now,
        sleep=lambda seconds: clock.advance(seconds),
        heartbeat_interval_seconds=30,
    )


def test_header_only_secret_and_sanitized_jsonl(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(200)])
    runtime = client(tmp_path, transport, clock)
    receipt = runtime.emit(
        RuntimeEventType.RUNTIME_PROGRESS,
        data={
            "processed": 3,
            "password": "must-not-survive",
            "note": "prefix this-is-a-private-run-secret suffix",
        },
    )
    assert receipt.status == "delivered"
    _, body, headers, _ = transport.calls[0]
    assert headers["Authorization"] == "Bearer this-is-a-private-run-secret"
    assert b"this-is-a-private-run-secret" not in body
    assert b"must-not-survive" not in body
    assert b"password" not in body
    assert b"[REDACTED]" in body
    spool_bytes = runtime.spool.path.read_bytes()
    assert b"this-is-a-private-run-secret" not in spool_bytes
    assert b"must-not-survive" not in spool_bytes
    assert runtime.spool.path.stat().st_mode & 0o777 == 0o600
    assert runtime.spool.path.parent.stat().st_mode & 0o777 == 0o700


def test_outage_is_bounded_and_restart_replays_pending_terminal_event(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    offline = ScriptedTransport([ConnectionError("offline")] * 3)
    first = client(tmp_path, offline, clock)
    queued = first.emit(RuntimeEventType.RUNTIME_TERMINAL, status="succeeded", data={"receipt_ref": "local://1"})
    assert queued.status == "queued"
    assert queued.attempts == 3
    assert queued.durable_local
    assert len(first.spool.pending()) == 1

    online = ScriptedTransport([TransportResponse(200)])
    restarted = client(tmp_path, online, clock)
    # Process construction is an automatic restart/replay boundary.
    assert len(online.calls) == 1
    assert restarted.replay_pending() == []
    assert restarted.spool.pending() == []
    assert restarted.spool.highest_local_sequence() == 1


def test_next_callback_automatically_replays_queued_heartbeat_in_order(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport(
        [
            ConnectionError("offline"),
            ConnectionError("offline"),
            ConnectionError("offline"),
            TransportResponse(200),
            TransportResponse(200),
        ]
    )
    runtime = client(tmp_path, transport, clock)
    assert runtime.emit(RuntimeEventType.RUNTIME_HEARTBEAT, data={"step": 1}).status == "queued"
    clock.advance(31)
    assert runtime.emit(RuntimeEventType.RUNTIME_PROGRESS, data={"step": 2}).status == "delivered"
    delivered_types = [json.loads(call[1])["event_type"] for call in transport.calls[-2:]]
    assert delivered_types == ["runtime.heartbeat", "runtime.progress"]
    assert runtime.flush_pending()


def test_heartbeat_is_coalesced_but_terminal_is_always_durable(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(200), TransportResponse(200), TransportResponse(200)])
    runtime = client(tmp_path, transport, clock)
    first = runtime.emit(RuntimeEventType.RUNTIME_HEARTBEAT, data={"queue_depth": 0})
    coalesced = runtime.emit(RuntimeEventType.RUNTIME_HEARTBEAT, data={"queue_depth": 1})
    terminal = runtime.emit(RuntimeEventType.RUNTIME_FAILED, status="failed", data={"failure_code": "controlled"})
    assert first.status == "delivered"
    assert coalesced.status == "coalesced"
    assert not coalesced.durable_local
    assert terminal.durable_local
    assert len([record for record in runtime.spool.records() if record["record"] == "event"]) == 2
    clock.advance(31)
    assert runtime.emit(RuntimeEventType.RUNTIME_HEARTBEAT).status == "delivered"


def test_exact_terminal_event_bodies_remain_available_after_delivery(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(200)] * 4)
    runtime = client(tmp_path, transport, clock)
    expected_types = (
        RuntimeEventType.RUNTIME_DRAINING,
        RuntimeEventType.CHECKPOINT_STARTED,
        RuntimeEventType.CHECKPOINT_VERIFIED,
        RuntimeEventType.RUNTIME_TERMINAL,
    )
    for event_type in expected_types:
        runtime.emit(event_type, status="succeeded", data={"event": event_type.value})
    bodies = runtime.durable_event_bodies(expected_types)
    assert tuple(body["event_type"] for body in bodies) == tuple(item.value for item in expected_types)
    assert [body["local_sequence"] for body in bodies] == [1, 2, 3, 4]
    assert bodies == tuple(json.loads(call[1]) for call in transport.calls)


def test_retry_backoff_is_deterministic_bounded_and_only_retries_transient_status(tmp_path: Path) -> None:
    policy = RetryPolicy(max_attempts=5, base_seconds=1, max_seconds=3, jitter_ratio=0.2)
    assert policy.delays("same-event") == policy.delays("same-event")
    assert len(policy.delays("same-event")) == 4
    assert all(0 <= delay <= 3 for delay in policy.delays("same-event"))

    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(400), TransportResponse(200)])
    runtime = client(tmp_path, transport, clock)
    receipt = runtime.emit(RuntimeEventType.RUNTIME_STARTED)
    assert receipt.status == "rejected"
    assert len(transport.calls) == 1


def test_callback_requires_https_and_payload_is_bounded(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    with pytest.raises(ValueError, match="HTTPS"):
        RuntimeClient(
            callback_url="http://control.example/callback",
            run_secret="this-is-a-private-run-secret",
            run_id="run",
            attempt_id="attempt",
            service_instance_id="service",
            source_identity="source",
            source_version="version",
            epoch=1,
            spool_path=tmp_path / "x.jsonl",
        )
    runtime = client(tmp_path, ScriptedTransport([]), clock)
    with pytest.raises(ValueError, match="64 KiB"):
        runtime.emit(RuntimeEventType.RUNTIME_PROGRESS, data={"large": "x" * (65 * 1024)})
    assert runtime.spool.pending() == []


def test_runtime_event_schema_and_example_validate() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas/content-runtime-event.v1.schema.json").read_text())
    example = json.loads((root / "examples/contracts/content-runtime-event.v1.example.json").read_text())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(example)
    assert "token" not in example


def test_donor_status_envelope_is_adapted_without_body_token(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(200)])
    runtime = client(tmp_path, transport, clock)
    result = runtime.emit_donor_envelope(
        {
            "event": "heartbeat",
            "event_uid": "legacy-event-1",
            "run_id": "run-1",
            "token": "legacy-body-token-must-be-ignored",
            "phase": "serve",
            "status": "running",
            "progress": {"processed": 4},
        }
    )
    assert result.status == "delivered"
    body = transport.calls[0][1]
    assert b"legacy-event-1" in body
    assert b"legacy-body-token" not in body
    with pytest.raises(ValueError, match="exact runtime"):
        runtime.emit_donor_envelope({"event": "heartbeat", "run_id": "stale-run"})


def test_donor_event_uid_replays_exact_body_and_rejects_conflicting_reuse(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(200), TransportResponse(200)])
    runtime = client(tmp_path, transport, clock)
    envelope = {
        "event": "report_written",
        "event_uid": "cherryflash:report:1",
        "run_id": "run-1",
        "phase": "report",
        "status": "done",
        "progress": {"done": 1, "total": 1, "progress_label": "report 1/1"},
    }

    first = runtime.emit_donor_envelope(envelope)
    clock.advance(10)
    duplicate = runtime.emit_donor_envelope(envelope)

    assert first.status == duplicate.status == "delivered"
    assert transport.calls[0][1] == transport.calls[1][1]
    body = json.loads(transport.calls[0][1])
    assert body["event_type"] == "job.result_available"
    assert body["data"]["donor_event"] == "report_written"
    assert body["data"]["donor_event_uid"] == "cherryflash:report:1"
    with pytest.raises(ValueError, match="different callback body"):
        runtime.emit_donor_envelope({**envelope, "status": "failed"})


@pytest.mark.parametrize(
    ("donor_event", "runtime_event"),
    [
        ("alive", "runtime.heartbeat"),
        ("kernel_started", "runtime.started"),
        ("resource_acquire", "resource.acquire"),
        ("resource_renew", "resource.renew"),
        ("resource_release", "resource.release"),
    ],
)
def test_donor_custom_runtime_states_keep_typed_event_semantics(
    tmp_path: Path, donor_event: str, runtime_event: str
) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    transport = ScriptedTransport([TransportResponse(200)])
    runtime = client(tmp_path / donor_event, transport, clock)
    runtime.emit_donor_envelope(
        {
            "event": donor_event,
            "event_uid": f"uid:{donor_event}",
            "run_id": "run-1",
            "phase": "runtime",
            "status": "running",
            "resource": {"kind": "telegram_session", "ref": "s22"},
        }
    )
    assert json.loads(transport.calls[0][1])["event_type"] == runtime_event
