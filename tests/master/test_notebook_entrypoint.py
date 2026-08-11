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
    _credential_registration_url,
    _register_reader_credential,
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


def test_activation_url_is_https_and_exact() -> None:
    assert _activation_url(
        "https://control.example/internal/runtime/events", "run-1", "attempt-1"
    ) == "https://control.example/internal/runtime/activation/run-1/attempt-1"
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

    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner", Provisioner
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.urllib.request.urlopen", open_request
    )
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
    with pytest.raises(CheckpointShutdownError, match="remains"):
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
