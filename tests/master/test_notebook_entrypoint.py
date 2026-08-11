from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.master_runtime.contracts import BootSource, MasterIdentity
from my_data_hub.master_runtime.notebook_entrypoint import (
    CheckpointShutdownError,
    NotebookMasterConfig,
    _activation_url,
    _checkpoint_before_stop,
    _checkpoint_until_deadline,
    _credential_registration_url,
    _register_reader_credential,
    main,
)
from my_data_hub.runtime_sdk import RuntimeEventType


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
        "maximum_runtime_seconds": 3600,
        "checkpoint_reserve_seconds": 900,
        "source_identity": "owner/postgres-master",
        "source_version": "1",
    }


def test_master_notebook_config_requires_exact_fields_and_source_binding(tmp_path: Path) -> None:
    path = tmp_path / "master.json"
    path.write_text(json.dumps(_payload()))
    config = NotebookMasterConfig.load(path)
    assert config.boot_source is BootSource.EMPTY_BASELINE
    assert config.epoch == 1

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
        maximum_runtime_seconds=3600,
        checkpoint_reserve_seconds=900,
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
    def __init__(self, events: list[str], *, terminal_status: str = "delivered") -> None:
        self.events = events
        self.terminal_status = terminal_status

    def emit(self, event_type, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append(event_type.value)
        return _Delivery(self.terminal_status if event_type is RuntimeEventType.RUNTIME_TERMINAL else "delivered")


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
    runtime = _Runtime(events, terminal_status="queued" if failure == "terminal" else "delivered")
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

    def run(config: NotebookMasterConfig, *, checkpoint_coordinator: object) -> int:
        observed["config"] = config
        observed["coordinator"] = checkpoint_coordinator
        return 17

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.run_master", run)
    assert main() == 17
    config = observed["config"]
    assert isinstance(config, NotebookMasterConfig)
    assert config.boot_source is (BootSource.VERIFIED_CHECKPOINT if has_head else BootSource.EMPTY_BASELINE)
    assert config.checkpoint_directory == (working / "exact-v7" if has_head else None)
    assert observed["coordinator"] is coordinator
