from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.master_runtime.contracts import BootSource, MasterIdentity
from my_data_hub.master_runtime.notebook_entrypoint import (
    CheckpointAdmissionError,
    CheckpointRetryStage,
    CheckpointShutdownError,
    NotebookMasterConfig,
    _activation_url,
    _checkpoint_before_stop,
    _checkpoint_until_deadline,
    _credential_registration_url,
    _emit_service_ready,
    _register_reader_credential,
    _runtime_deadlines,
    main,
)
from my_data_hub.runtime_sdk import (
    CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
    CHECKPOINT_TRANSITION_GUARD_SECONDS,
    KAGGLE_PROVIDER_TIMEOUT_SECONDS,
    MIN_CHECKPOINT_RESERVE_SECONDS,
    RuntimeEventType,
)


def _payload() -> dict[str, object]:
    return {
        "master_instance_id": "11111111-1111-4111-8111-111111111111",
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "service_instance_id": "service-1",
        "epoch": 1,
        "boot_source": "empty_baseline",
        "checkpoint_directory": None,
        "lease_seconds": 120,
        "postgres_bin": "/kaggle/input/postgresql-18/bin",
        "postgres_port": 15432,
        "tunnel_gateway_host": "gateway.example.test",
        "tunnel_gateway_port": 22,
        "tunnel_gateway_user": "mdh-tunnel",
        "tunnel_remote_port": 25432,
        "maximum_runtime_seconds": 21_600,
        "checkpoint_reserve_seconds": 10_800,
        "source_identity": "owner/postgres-master",
        "source_version": "1",
    }


def test_master_notebook_config_requires_exact_fields_and_source_binding(tmp_path: Path) -> None:
    path = tmp_path / "master.json"
    path.write_text(json.dumps(_payload()))
    config = NotebookMasterConfig.load(path)
    assert config.boot_source is BootSource.EMPTY_BASELINE
    assert config.epoch == 1
    assert config.checkpoint_reserve_seconds == MIN_CHECKPOINT_RESERVE_SECONDS

    payload = _payload()
    payload["checkpoint_reserve_seconds"] = MIN_CHECKPOINT_RESERVE_SECONDS - 1
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="exactly 10800"):
        NotebookMasterConfig.load(path)

    payload = _payload()
    payload["checkpoint_directory"] = "/kaggle/input/checkpoint"
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="checkpoint source"):
        NotebookMasterConfig.load(path)

    payload = _payload()
    payload["checkpoint_reserve_seconds"] = payload["maximum_runtime_seconds"]
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="checkpoint reserve"):
        NotebookMasterConfig.load(path)

    payload = _payload()
    payload["maximum_runtime_seconds"] = KAGGLE_PROVIDER_TIMEOUT_SECONDS
    path.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="provider timeout"):
        NotebookMasterConfig.load(path)


def test_runtime_deadlines_are_fixed_at_process_entry_and_charge_boot_time(tmp_path: Path) -> None:
    path = tmp_path / "master.json"
    path.write_text(json.dumps(_payload()))
    config = NotebookMasterConfig.load(path)
    active_deadline, session_deadline = _runtime_deadlines(config, 100.0)
    assert active_deadline == 10_840.0
    assert session_deadline == 21_700.0
    assert session_deadline - active_deadline == (
        MIN_CHECKPOINT_RESERVE_SECONDS + CHECKPOINT_TRANSITION_GUARD_SECONDS
    )
    # A 600-second bootstrap reduces the active window; it cannot reset either deadline.
    boot_completed_at = 700.0
    assert active_deadline - boot_completed_at == 10_140.0


def test_long_boot_refuses_ready_before_control_or_local_write_activation() -> None:
    events: list[str] = []

    class Ready:
        @staticmethod
        def event_payload() -> dict[str, object]:
            return {"epoch": 1}

    with pytest.raises(RuntimeError, match="boot consumed the ACTIVE window"):
        _emit_service_ready(
            runtime=_Runtime(events),
            ready=Ready(),
            active_deadline=100.0,
            monotonic=lambda: 100.0,
        )
    assert events == []


def test_activation_url_is_https_and_exact() -> None:
    assert (
        _activation_url("https://control.example/internal/runtime/events", "run-1", "attempt-1")
        == "https://control.example/internal/runtime/activation/run-1/attempt-1"
    )
    with pytest.raises(ValueError, match="exact HTTPS"):
        _activation_url("http://control.example/internal/runtime/events", "run", "attempt")


def test_reader_credential_handoff_is_epoch_bound_tls_and_not_returned(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {}

    class Provisioner:
        def __init__(self, connection, gate) -> None:  # type: ignore[no-untyped-def]
            captured["connection"] = connection
            captured["gate"] = gate

        def create(self, **kwargs) -> str:  # type: ignore[no-untyped-def]
            captured["create"] = kwargs
            return str(kwargs["principal"])

        def drop(self, principal: str) -> None:
            captured["drop"] = principal

    class Response:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *args) -> None:  # type: ignore[no-untyped-def]
            return None

        def read(self, limit: int) -> bytes:
            assert limit == 16 * 1024
            return b'{"registered":1,"credential_refs":["opaque.json"]}'

    def open_request(request, timeout):  # type: ignore[no-untyped-def]
        captured["url"] = request.full_url
        captured["authorization"] = request.headers["Authorization"]
        captured["body"] = json.loads(request.data)
        assert timeout == 10
        return Response()

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner", Provisioner)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.urllib.request.urlopen", open_request)
    config = NotebookMasterConfig(
        master_instance_id=UUID("11111111-1111-4111-8111-111111111111"),
        run_id="run-1",
        attempt_id="attempt-1",
        service_instance_id="service-1",
        epoch=7,
        boot_source=BootSource.EMPTY_BASELINE,
        checkpoint_directory=None,
        lease_seconds=120,
        postgres_bin=Path("/kaggle/input/postgresql-18/bin"),
        postgres_port=15432,
        tunnel_gateway_host="gateway.example.test",
        tunnel_gateway_port=22,
        tunnel_gateway_user="mdh-tunnel",
        tunnel_remote_port=25432,
        maximum_runtime_seconds=21_600,
        checkpoint_reserve_seconds=10_800,
        source_identity="owner/postgres-master",
        source_version="1",
    )
    now = datetime.now(UTC)
    principal, expiry = _register_reader_credential(
        connection=object(),
        gate=object(),  # type: ignore[arg-type]
        config=config,
        callback_url="https://control.example/internal/runtime/events",
        run_secret="runtime-secret-long-enough",
        expires_at=now + timedelta(minutes=3),
        now=now,
    )
    assert principal.startswith("mdh_e7_reader_") and expiry == now + timedelta(minutes=3)
    assert captured["url"] == _credential_registration_url(
        "https://control.example/internal/runtime/events", "run-1", "attempt-1"
    )
    assert captured["authorization"] == "Bearer runtime-secret-long-enough"
    body = captured["body"]
    assert isinstance(body, dict) and body["epoch"] == 7
    database_url = body["credentials"][0]["database_url"]
    assert "sslmode=verify-ca" in database_url
    assert "sslrootcert=%2Fstate%2Fmaster-tls%2Fca.pem" in database_url
    assert "connect_timeout=5" in database_url
    assert "runtime-secret-long-enough" not in json.dumps(body)


@dataclass
class _Delivery:
    status: str = "delivered"


class _Runtime:
    def __init__(
        self,
        events: list[str],
        *,
        terminal_status: str = "delivered",
        verified_status: str = "delivered",
        flush_results: list[bool] | None = None,
    ) -> None:
        self.events = events
        self.terminal_status = terminal_status
        self.verified_status = verified_status
        self.flush_results = list(flush_results or [True])
        self.run_id = "run-1"
        self.attempt_id = "attempt-1"
        self.service_instance_id = "service-1"
        self.source_identity = "owner/postgres-master"
        self.source_version = "1"
        self.epoch = 1
        self.event_bodies: list[dict[str, object]] = []

    def emit(self, event_type, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append(event_type.value)
        sequence = len(self.event_bodies) + 1
        self.event_bodies.append(
            {
                "schema": "content-runtime-event/v1",
                "event_id": str(uuid5(NAMESPACE_URL, f"test-runtime-event:{sequence}")),
                "run_id": self.run_id,
                "attempt_id": self.attempt_id,
                "service_instance_id": self.service_instance_id,
                "source_identity": self.source_identity,
                "source_version": self.source_version,
                "event_type": event_type.value,
                "emitted_at": "2026-08-11T00:00:00Z",
                "local_sequence": sequence,
                "epoch": self.epoch,
                "phase": kwargs.get("phase"),
                "status": kwargs.get("status"),
                "data": kwargs.get("data", {}),
                "artifact_refs": [],
                "metrics": {},
            }
        )
        if event_type is RuntimeEventType.RUNTIME_TERMINAL:
            return _Delivery(self.terminal_status)
        if event_type is RuntimeEventType.CHECKPOINT_VERIFIED:
            return _Delivery(self.verified_status)
        return _Delivery("delivered")

    def flush_pending(self, *, max_events: int | None = None) -> bool:
        self.events.append("runtime.flush")
        return self.flush_results.pop(0) if self.flush_results else True

    def durable_event_bodies(self, event_types):  # type: ignore[no-untyped-def]
        selected = []
        for event_type in event_types:
            selected.append(
                next(body for body in reversed(self.event_bodies) if body["event_type"] == event_type.value)
            )
        return tuple(selected)


class _Gate:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def drain(self, identity, reason):  # type: ignore[no-untyped-def]
        self.events.append("gate.drain")


class _Process:
    def __init__(self, events: list[str], name: str) -> None:
        self.events = events
        self.name = name

    def stop(self, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append(f"{self.name}.stop")


class _Coordinator:
    def __init__(self, events: list[str], *, fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def create_and_publish(self, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append("checkpoint.publish")
        if self.fail:
            raise RuntimeError("upload failed")
        return PublishReceipt(
            checkpoint_id="11111111-1111-4111-8111-111111111111",
            exact_version_ref="private/checkpoints/v1",
            manifest_sha256="a" * 64,
            current_checkpoint_id="11111111-1111-4111-8111-111111111111",
            previous_checkpoint_id=None,
            upload_seconds=1.0,
            readback_seconds=1.0,
            restore_seconds=1.0,
            package_bytes=1,
            restore_receipt={"ok": True},
        )


IDENTITY = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "run-1", 1)


def test_master_stops_only_after_verified_checkpoint_and_durable_terminal(tmp_path: Path) -> None:
    events: list[str] = []
    _checkpoint_before_stop(
        gate=_Gate(events),
        runtime=_Runtime(events),
        tunnel=_Process(events, "tunnel"),
        supervisor=_Process(events, "postgres"),
        coordinator=_Coordinator(events),
        database_url="postgresql:///postgres",
        package_directory=tmp_path,
        identity=IDENTITY,
    )
    assert events == [
        "gate.drain",
        "runtime.draining",
        "checkpoint.started",
        "checkpoint.publish",
        "checkpoint.verified",
        "runtime.terminal",
        "tunnel.stop",
        "postgres.stop",
    ]


@pytest.mark.parametrize("failure", ["publish", "terminal", "missing"])
def test_checkpoint_failure_leaves_drained_master_nonterminal_and_running(tmp_path: Path, failure: str) -> None:
    events: list[str] = []
    coordinator = None if failure == "missing" else _Coordinator(events, fail=failure == "publish")
    runtime = _Runtime(
        events,
        terminal_status="queued" if failure == "terminal" else "delivered",
        flush_results=[False] if failure == "terminal" else None,
    )
    with pytest.raises(CheckpointShutdownError, match=r"remains|retry"):
        _checkpoint_before_stop(
            gate=_Gate(events),
            runtime=runtime,
            tunnel=_Process(events, "tunnel"),
            supervisor=_Process(events, "postgres"),
            coordinator=coordinator,
            database_url="postgresql:///postgres",
            package_directory=tmp_path,
            identity=IDENTITY,
        )
    assert "tunnel.stop" not in events and "postgres.stop" not in events
    if failure in {"publish", "missing"}:
        assert "runtime.terminal" not in events
        assert events[-1] == "checkpoint.failed"


def test_durable_promotion_writes_exact_terminal_recovery_before_ack_failure(tmp_path: Path) -> None:
    events: list[str] = []
    output_path = tmp_path / "my-data-hub-master-terminal.json"
    with pytest.raises(CheckpointShutdownError, match="not acknowledged"):
        _checkpoint_before_stop(
            gate=_Gate(events),
            runtime=_Runtime(events, terminal_status="queued", flush_results=[False]),
            tunnel=_Process(events, "tunnel"),
            supervisor=_Process(events, "postgres"),
            coordinator=_Coordinator(events),
            database_url="postgresql:///postgres",
            package_directory=tmp_path / "checkpoints",
            identity=IDENTITY,
            terminal_output_path=output_path,
        )
    assert output_path.stat().st_mode & 0o777 == 0o600
    assert output_path.stat().st_size <= 256 * 1024
    raw_output = output_path.read_bytes()
    body = json.loads(raw_output)
    assert raw_output == json.dumps(
        body,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode()
    assert set(body) == {
        "schema_version",
        "run_id",
        "attempt_id",
        "service_instance_id",
        "master_instance_id",
        "source_identity",
        "source_version",
        "epoch",
        "status",
        "checkpoint",
        "events",
    }
    assert body["schema_version"] == "my-data-hub-master-terminal.v1"
    assert body["status"] == "succeeded"
    assert body["checkpoint"] == {
        "checkpoint_id": "11111111-1111-4111-8111-111111111111",
        "manifest_sha256": "a" * 64,
        "current_checkpoint_id": "11111111-1111-4111-8111-111111111111",
    }
    assert [event["event_type"] for event in body["events"]] == [
        "runtime.draining",
        "checkpoint.started",
        "checkpoint.verified",
        "runtime.terminal",
    ]
    assert [event["local_sequence"] for event in body["events"]] == [1, 2, 3, 4]
    for event in body["events"]:
        assert event["run_id"] == body["run_id"]
        assert event["attempt_id"] == body["attempt_id"]
        assert event["service_instance_id"] == body["service_instance_id"]
        assert event["source_identity"] == body["source_identity"]
        assert event["source_version"] == body["source_version"]
        assert event["epoch"] == body["epoch"]
    assert body["events"][2]["data"] == body["checkpoint"]
    assert body["events"][3]["data"] == {
        "checkpoint_id": body["checkpoint"]["current_checkpoint_id"]
    }
    encoded = output_path.read_bytes().lower()
    assert b"postgresql://" not in encoded and b"secret" not in encoded and b"token" not in encoded
    assert "tunnel.stop" not in events and "postgres.stop" not in events


def test_checkpoint_retry_uses_reserved_time_and_does_not_reopen_writes(tmp_path: Path) -> None:
    events: list[str] = []

    class RetryCoordinator(_Coordinator):
        def create_and_publish(self, **kwargs):  # type: ignore[no-untyped-def]
            if self.fail:
                self.events.append("checkpoint.publish")
                self.fail = False
                raise RuntimeError("transient provider failure")
            return super().create_and_publish(**kwargs)

    sleeps: list[float] = []
    receipt = _checkpoint_until_deadline(
        gate=_Gate(events),
        runtime=_Runtime(events),
        tunnel=_Process(events, "tunnel"),
        supervisor=_Process(events, "postgres"),
        coordinator=RetryCoordinator(events, fail=True),
        database_url="postgresql:///postgres",
        package_directory=tmp_path,
        identity=IDENTITY,
        deadline=200,
        publication_attempt_seconds=100,
        retry_seconds=60,
        monotonic=lambda: 0,
        sleep=sleeps.append,
    )
    assert receipt.current_checkpoint_id == receipt.checkpoint_id
    assert sleeps == [60]
    assert events.count("gate.drain") == 1
    assert events.count("runtime.draining") == 1
    assert events.count("checkpoint.started") == 2
    assert events.count("checkpoint.failed") == 1


def test_checkpoint_does_not_claim_a_second_attempt_without_another_full_budget(tmp_path: Path) -> None:
    events: list[str] = []
    observed = iter([0.0, CHECKPOINT_ATTEMPT_BUDGET_SECONDS + 1.0])
    with pytest.raises(CheckpointShutdownError):
        _checkpoint_until_deadline(
            gate=_Gate(events),
            runtime=_Runtime(events),
            tunnel=_Process(events, "tunnel"),
            supervisor=_Process(events, "postgres"),
            coordinator=_Coordinator(events, fail=True),
            database_url="postgresql:///postgres",
            package_directory=tmp_path,
            identity=IDENTITY,
            deadline=MIN_CHECKPOINT_RESERVE_SECONDS,
            monotonic=lambda: next(observed),
            sleep=lambda _seconds: pytest.fail("a second attempt must not be scheduled"),
        )
    assert events.count("checkpoint.publish") == 1
    assert events.count("checkpoint.started") == 1


@pytest.mark.parametrize("lost_event", ["checkpoint_verified", "terminal"])
def test_terminal_ack_loss_replays_callbacks_without_creating_second_checkpoint(
    tmp_path: Path,
    lost_event: str,
) -> None:
    events: list[str] = []
    sleeps: list[float] = []
    coordinator = _Coordinator(events)
    receipt = _checkpoint_until_deadline(
        gate=_Gate(events),
        runtime=_Runtime(
            events,
            terminal_status="queued" if lost_event == "terminal" else "delivered",
            verified_status="queued" if lost_event == "checkpoint_verified" else "delivered",
            flush_results=[False, True],
        ),
        tunnel=_Process(events, "tunnel"),
        supervisor=_Process(events, "postgres"),
        coordinator=coordinator,
        database_url="postgresql:///postgres",
        package_directory=tmp_path,
        identity=IDENTITY,
        deadline=200,
        publication_attempt_seconds=100,
        retry_seconds=60,
        monotonic=lambda: 0,
        sleep=sleeps.append,
    )
    assert receipt.current_checkpoint_id == receipt.checkpoint_id
    assert sleeps == [60]
    assert events.count("checkpoint.publish") == 1
    assert events.count("checkpoint.started") == 1
    assert events.count("runtime.flush") == 2
    assert events[-2:] == ["tunnel.stop", "postgres.stop"]


def test_first_checkpoint_attempt_requires_its_conservative_admission_without_overrun_claim(
    tmp_path: Path,
) -> None:
    events: list[str] = []
    with pytest.raises(CheckpointAdmissionError, match="not started"):
        _checkpoint_until_deadline(
            gate=_Gate(events),
            runtime=_Runtime(events),
            tunnel=_Process(events, "tunnel"),
            supervisor=_Process(events, "postgres"),
            coordinator=_Coordinator(events),
            database_url="postgresql:///postgres",
            package_directory=tmp_path,
            identity=IDENTITY,
            deadline=CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
            monotonic=lambda: 1.0,
        )
    assert events == []


def test_checkpoint_attempt_is_admitted_at_the_exact_attempt_budget(tmp_path: Path) -> None:
    events: list[str] = []
    receipt = _checkpoint_until_deadline(
        gate=_Gate(events),
        runtime=_Runtime(events),
        tunnel=_Process(events, "tunnel"),
        supervisor=_Process(events, "postgres"),
        coordinator=_Coordinator(events),
        database_url="postgresql:///postgres",
        package_directory=tmp_path,
        identity=IDENTITY,
        deadline=CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
        monotonic=lambda: 0.0,
    )
    assert receipt.current_checkpoint_id == receipt.checkpoint_id
    assert events.count("checkpoint.publish") == 1


def test_checkpoint_errors_identify_the_only_permitted_retry_stage(tmp_path: Path) -> None:
    events: list[str] = []
    with pytest.raises(CheckpointShutdownError) as failure:
        _checkpoint_before_stop(
            gate=_Gate(events),
            runtime=_Runtime(events),
            tunnel=_Process(events, "tunnel"),
            supervisor=_Process(events, "postgres"),
            coordinator=_Coordinator(events, fail=True),
            database_url="postgresql:///postgres",
            package_directory=tmp_path,
            identity=IDENTITY,
        )
    assert failure.value.retry_stage is CheckpointRetryStage.PUBLICATION


@pytest.mark.parametrize("has_head", [False, True])
def test_notebook_main_resolves_exact_durable_head_and_always_wires_checkpoint_coordinator(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, has_head: bool
) -> None:
    payload = _payload()
    payload.update(
        {
            "run_id": "11111111-1111-4111-8111-111111111111",
            "attempt_id": "22222222-2222-4222-8222-222222222222",
        }
    )
    config_path = tmp_path / "master.json"
    config_path.write_text(json.dumps(payload))
    monkeypatch.setenv("MY_DATA_HUB_MASTER_CONFIG", str(config_path))
    working = tmp_path / "kaggle" / "working"
    working.mkdir(parents=True)
    monkeypatch.setenv("KAGGLE_WORKING_DIR", str(working))
    observed: dict[str, object] = {}

    class Coordinator:
        def resolve_boot_checkpoint(self, destination: Path) -> Path | None:
            observed["destination"] = destination
            return working / "exact-v7" if has_head else None

    coordinator = Coordinator()
    monkeypatch.setattr(
        "my_data_hub.checkpoints.kaggle_runtime.build_runtime_checkpoint_coordinator_from_environment",
        lambda **kwargs: observed.update(factory=kwargs) or coordinator,
    )

    def run(
        config: NotebookMasterConfig,
        *,
        checkpoint_coordinator: object,
        process_started_at: float,
    ) -> int:
        observed["config"] = config
        observed["coordinator"] = checkpoint_coordinator
        observed["process_started_at"] = process_started_at
        return 17

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.run_master", run)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.time.monotonic", lambda: 123.0)
    assert main() == 17
    config = observed["config"]
    assert isinstance(config, NotebookMasterConfig)
    assert config.boot_source is (BootSource.VERIFIED_CHECKPOINT if has_head else BootSource.EMPTY_BASELINE)
    assert config.checkpoint_directory == (working / "exact-v7" if has_head else None)
    assert observed["coordinator"] is coordinator
    assert observed["process_started_at"] == 123.0
