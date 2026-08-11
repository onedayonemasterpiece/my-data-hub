from __future__ import annotations

import json
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from my_data_hub.acceptance.master_production import ProductionMasterAcceptanceEffectsFactory
from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.master_runtime.contracts import BootSource, MasterIdentity
from my_data_hub.master_runtime.notebook_entrypoint import (
    BloggerReceiptDeliveryError,
    CallbackLeaseClosingError,
    CheckpointAdmissionError,
    CheckpointRetryStage,
    CheckpointShutdownError,
    NotebookMasterConfig,
    _activation_url,
    _checkpoint_before_stop,
    _checkpoint_until_deadline,
    _cleanup_epoch_principals,
    _credential_registration_url,
    _EmbeddingLeaseMaintainer,
    _emit_service_ready,
    _fresh_canonical_committer_connection,
    _register_reader_credential,
    _register_session_credentials,
    _runtime_deadlines,
    _wait_for_activation,
    main,
    run_master,
)
from my_data_hub.runtime_sdk import (
    CHECKPOINT_ARCHIVE_COMMAND_COUNT,
    CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS,
    CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
    CHECKPOINT_PROVIDER_IO_BUDGET_SECONDS,
    CHECKPOINT_TRANSITION_GUARD_SECONDS,
    CHECKPOINT_VERIFIER_TIMEOUT_SECONDS,
    KAGGLE_PROVIDER_TIMEOUT_SECONDS,
    MIN_CHECKPOINT_RESERVE_SECONDS,
    RetryPolicy,
    RuntimeClient,
    RuntimeEventType,
)
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
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


def test_checkpoint_component_allocations_fit_one_admitted_attempt() -> None:
    assert CHECKPOINT_ARCHIVE_COMMAND_COUNT == 2
    assert CHECKPOINT_ATTEMPT_BUDGET_SECONDS == (
        CHECKPOINT_ARCHIVE_COMMAND_COUNT * CHECKPOINT_ARCHIVE_COMMAND_TIMEOUT_SECONDS
        + CHECKPOINT_VERIFIER_TIMEOUT_SECONDS
        + CHECKPOINT_PROVIDER_IO_BUDGET_SECONDS
    )
    assert MIN_CHECKPOINT_RESERVE_SECONDS == 2 * CHECKPOINT_ATTEMPT_BUDGET_SECONDS


def test_runtime_deadlines_are_fixed_at_process_entry_and_charge_boot_time(tmp_path: Path) -> None:
    path = tmp_path / "master.json"
    path.write_text(json.dumps(_payload()))
    config = NotebookMasterConfig.load(path)
    active_deadline, session_deadline = _runtime_deadlines(config, 100.0)
    assert active_deadline == 10_840.0
    assert session_deadline == 21_700.0
    assert session_deadline - active_deadline == (MIN_CHECKPOINT_RESERVE_SECONDS + CHECKPOINT_TRANSITION_GUARD_SECONDS)
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
        _activation_url("https://mcp-datahub.kenigevents.ru/internal/runtime/events", "run-1", "attempt-1")
        == "https://mcp-datahub.kenigevents.ru/internal/runtime/activation/run-1/attempt-1"
    )
    with pytest.raises(ValueError, match="owner-pinned"):
        _activation_url("http://control.example/internal/runtime/events", "run", "attempt")
    with pytest.raises(ValueError, match="owner-pinned"):
        _activation_url("https://attacker.example/internal/runtime/events", "run", "attempt")


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
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        run_secret="runtime-secret-long-enough",
        expires_at=now + timedelta(minutes=3),
        now=now,
    )
    assert principal.startswith("mdh_e7_reader_") and expiry == now + timedelta(minutes=3)
    assert captured["url"] == _credential_registration_url(
        "https://mcp-datahub.kenigevents.ru/internal/runtime/events", "run-1", "attempt-1"
    )
    assert captured["authorization"] == "Bearer runtime-secret-long-enough"
    body = captured["body"]
    assert isinstance(body, dict) and body["epoch"] == 7
    database_url = body["credentials"][0]["database_url"]
    assert "sslmode=verify-ca" in database_url
    assert "sslrootcert=%2Fstate%2Fmaster-tls%2Fca.pem" in database_url
    assert "connect_timeout=5" in database_url
    assert "runtime-secret-long-enough" not in json.dumps(body)


def test_activation_authorized_operator_is_issued_with_reader_in_one_bounded_envelope(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: dict[str, object] = {"creates": []}

    class Provisioner:
        def __init__(self, _connection, _gate) -> None:  # type: ignore[no-untyped-def]
            pass

        def create(self, **kwargs) -> str:  # type: ignore[no-untyped-def]
            captured["creates"].append(kwargs)  # type: ignore[union-attr]
            return kwargs["principal"]

        def drop(self, principal: str) -> None:
            captured.setdefault("drops", []).append(principal)  # type: ignore[union-attr]

    class Response:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args) -> None:  # type: ignore[no-untyped-def]
            return None

        @staticmethod
        def read(_limit: int) -> bytes:
            return b'{"registered":2,"credential_refs":["reader.json","operator.json"]}'

    def open_request(request, timeout):  # type: ignore[no-untyped-def]
        assert timeout == 10
        captured["body"] = json.loads(request.data)
        return Response()

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner", Provisioner)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.urllib.request.urlopen", open_request)
    config = NotebookMasterConfig(
        master_instance_id=IDENTITY.master_instance_id,
        run_id=IDENTITY.run_id,
        attempt_id="attempt-1",
        service_instance_id="service-1",
        epoch=IDENTITY.epoch,
        boot_source=BootSource.EMPTY_BASELINE,
        checkpoint_directory=None,
        lease_seconds=120,
        postgres_bin=Path("/postgres/bin"),
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
    now = datetime(2026, 8, 11, tzinfo=UTC)
    principals, _expiry = _register_session_credentials(
        connection=object(),
        gate=object(),  # type: ignore[arg-type]
        config=config,
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        run_secret="runtime-secret-long-enough",
        roles=("reader", "operator"),
        expires_at=now + timedelta(minutes=3),
        now=now,
    )
    creates = captured["creates"]
    assert isinstance(creates, list)
    assert [item["group"] for item in creates] == ["mdh_mcp_reader", "mdh_mcp_editor"]
    assert principals[0].startswith("mdh_e1_reader_")
    assert principals[1].startswith("mdh_e1_operator_")
    body = captured["body"]
    assert isinstance(body, dict)
    assert [item["role"] for item in body["credentials"]] == ["reader", "operator"]
    assert all(set(item) == {"role", "database_url", "expires_at"} for item in body["credentials"])


def test_activation_rejects_unrequested_or_reordered_credential_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class Response:
        def __init__(self, roles: list[str]) -> None:
            self.roles = roles

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args) -> None:  # type: ignore[no-untyped-def]
            return None

        def read(self, _limit: int) -> bytes:
            return json.dumps(
                {
                    "active": True,
                    "master_instance_id": str(IDENTITY.master_instance_id),
                    "epoch": IDENTITY.epoch,
                    "credential_roles": self.roles,
                }
            ).encode()

    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.urllib.request.urlopen",
        lambda *_args, **_kwargs: Response(["reader", "operator"]),
    )
    assert _wait_for_activation("https://example.test", "secret", IDENTITY) == (
        "reader",
        "operator",
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.urllib.request.urlopen",
        lambda *_args, **_kwargs: Response(["operator", "reader"]),
    )
    with pytest.raises(RuntimeError, match="exceed the bounded contract"):
        _wait_for_activation("https://example.test", "secret", IDENTITY)


def test_embedding_lease_maintainer_renews_past_original_lease_and_fences_on_control_loss(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed_now = [datetime(2026, 8, 11, tzinfo=UTC)]
    renewals: list[datetime] = []
    fences: list[str] = []
    tunnel_polls: list[datetime] = []
    delivery_status = ["delivered"]

    class Gate:
        def __init__(self, _connection) -> None:  # type: ignore[no-untyped-def]
            pass

        def renew(self, identity, lease_until) -> None:  # type: ignore[no-untyped-def]
            assert identity == IDENTITY
            renewals.append(lease_until)

        def fence(self, identity, reason) -> None:  # type: ignore[no-untyped-def]
            assert identity == IDENTITY
            fences.append(reason)

    class Runtime:
        def emit(self, event_type, **kwargs):  # type: ignore[no-untyped-def]
            assert event_type is RuntimeEventType.RUNTIME_HEARTBEAT
            assert kwargs["data"]["lease_until"].endswith("Z")
            if delivery_status[0] == "queued":
                observed_now[0] += timedelta(seconds=2)
            return _Delivery(delivery_status[0])

    class Tunnel:
        def poll(self, *, now) -> None:  # type: ignore[no-untyped-def]
            tunnel_polls.append(now)

    @contextmanager
    def connection_factory():  # type: ignore[no-untyped-def]
        yield object()

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.DatabaseGate", Gate)
    maintainer = _EmbeddingLeaseMaintainer(
        identity=IDENTITY,
        lease_seconds=60,
        initial_lease_until=observed_now[0] + timedelta(seconds=60),
        runtime=Runtime(),
        tunnel=Tunnel(),
        connection_factory=connection_factory,
        now=lambda: observed_now[0],
    )
    observed_now[0] += timedelta(seconds=20)
    maintainer.maintain_once()
    observed_now[0] += timedelta(seconds=40)  # at the original 60-second lease boundary
    maintainer.maintain_once()
    assert renewals == [
        datetime(2026, 8, 11, 0, 1, 20, tzinfo=UTC),
        datetime(2026, 8, 11, 0, 2, 0, tzinfo=UTC),
    ]
    assert maintainer.lease_until == renewals[-1]
    assert tunnel_polls == [
        datetime(2026, 8, 11, 0, 0, 20, tzinfo=UTC),
        datetime(2026, 8, 11, 0, 1, 0, tzinfo=UTC),
    ]

    delivery_status[0] = "queued"
    observed_now[0] = maintainer.lease_until - timedelta(seconds=16)
    with pytest.raises(CallbackLeaseClosingError, match="write lease is closing"):
        maintainer.maintain_once()
    assert fences == ["embedding_control_heartbeat_lost"]
    with pytest.raises(CallbackLeaseClosingError):
        maintainer.check()


def test_fresh_embedding_committer_is_epoch_bound_non_superuser_and_always_removed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[object] = []

    class Provisioner:
        def __init__(self, connection, gate) -> None:  # type: ignore[no-untyped-def]
            events.append(("provisioner", connection, gate))

        def create(self, **kwargs) -> str:  # type: ignore[no-untyped-def]
            events.append(("create", kwargs))
            return kwargs["principal"]

        def drop(self, principal: str) -> None:
            events.append(("drop", principal))

    class Cursor:
        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args) -> None:  # type: ignore[no-untyped-def]
            return None

        def execute(self, statement, _params=None):  # type: ignore[no-untyped-def]
            events.append(("verify", statement))
            return self

        @staticmethod
        def fetchone():  # type: ignore[no-untyped-def]
            create = next(item[1] for item in events if item[0] == "create")
            return (create["principal"], False)

    class Connection:
        def cursor(self):  # type: ignore[no-untyped-def]
            return Cursor()

        def close(self) -> None:
            events.append("connection.close")

    class Gate:
        def revoke_credential(self, credential_id, reason) -> None:  # type: ignore[no-untyped-def]
            events.append(("revoke", credential_id, reason))

        def fence(self, identity, reason) -> None:  # type: ignore[no-untyped-def]
            events.append(("fence", identity, reason))

    def connect(database_url, **kwargs):  # type: ignore[no-untyped-def]
        events.append(("connect", database_url, kwargs))
        return Connection()

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner", Provisioner)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.psycopg.connect", connect)
    gate = Gate()
    now = datetime(2026, 8, 11, tzinfo=UTC)
    with pytest.raises(RuntimeError, match="import failed"), _fresh_canonical_committer_connection(
        owner_connection=object(),
        gate=gate,  # type: ignore[arg-type]
        identity=IDENTITY,
        database_url="postgresql:///postgres",
        lease_until=lambda: now + timedelta(minutes=5),
        now=lambda: now,
    ):
        raise RuntimeError("import failed")

    create = next(item[1] for item in events if item[0] == "create")
    assert create["group"] == "mdh_canonical_committer"
    assert create["identity"] == IDENTITY
    assert create["principal"].startswith("mdh_e1_embed_")
    connect_event = next(item for item in events if item[0] == "connect")
    assert connect_event[2]["user"] == create["principal"]
    assert connect_event[2]["autocommit"] is True
    assert "connection.close" in events
    assert any(item[0] == "revoke" for item in events if isinstance(item, tuple))
    assert any(item == ("drop", create["principal"]) for item in events)


def test_reader_cleanup_failure_fences_before_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    events: list[str] = []

    class Provisioner:
        def __init__(self, _connection, _gate) -> None:  # type: ignore[no-untyped-def]
            pass

        def drop(self, principal: str) -> None:
            events.append(f"drop:{principal}")
            if principal == "reader-b":
                raise RuntimeError("drop failed")

    class Gate:
        def fence(self, identity, reason) -> None:  # type: ignore[no-untyped-def]
            assert identity == IDENTITY
            events.append(f"fence:{reason}")

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner", Provisioner)
    with pytest.raises(RuntimeError, match="epoch credential cleanup failed"):
        _cleanup_epoch_principals(
            connection=object(),
            gate=Gate(),  # type: ignore[arg-type]
            identity=IDENTITY,
            principals={"reader-a", "reader-b"},
        )
    assert events[-1] == "fence:epoch_credential_cleanup_failed"


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

    def start(self, **kwargs):  # type: ignore[no-untyped-def]
        self.events.append(f"{self.name}.start")


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
    assert (
        raw_output
        == json.dumps(
            body,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode()
    )
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
    assert body["events"][3]["data"] == {"checkpoint_id": body["checkpoint"]["current_checkpoint_id"]}
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


def test_persistent_terminal_callback_outage_keeps_spool_and_exits_cleanly_for_output_recovery(
    tmp_path: Path,
) -> None:
    events: list[str] = []

    class OfflineTransport:
        @staticmethod
        def post(url: str, body: bytes, headers: dict[str, str], timeout_seconds: float):  # type: ignore[no-untyped-def]
            raise ConnectionError("persistent callback outage")

    runtime = RuntimeClient(
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        run_secret="terminal-outage-secret-long-enough",
        run_id="run-1",
        attempt_id="attempt-1",
        service_instance_id="service-1",
        source_identity="owner/postgres-master",
        source_version="1",
        epoch=1,
        spool_path=tmp_path / "runtime" / "events.jsonl",
        transport=OfflineTransport(),
        retry_policy=RetryPolicy(max_attempts=1),
        sleep=lambda _seconds: None,
        now=lambda: datetime(2026, 8, 11, tzinfo=UTC),
    )
    observed = iter([0.0, 0.0, 111.0])
    output_path = tmp_path / "my-data-hub-master-terminal.json"
    receipt = _checkpoint_until_deadline(
        gate=_Gate(events),
        runtime=runtime,
        tunnel=_Process(events, "tunnel"),
        supervisor=_Process(events, "postgres"),
        coordinator=_Coordinator(events),
        database_url="postgresql:///postgres",
        package_directory=tmp_path / "checkpoints",
        identity=IDENTITY,
        deadline=200.0,
        terminal_output_path=output_path,
        publication_attempt_seconds=100.0,
        retry_seconds=60.0,
        monotonic=lambda: next(observed),
        sleep=lambda seconds: events.append(f"sleep:{seconds}"),
    )
    assert receipt.current_checkpoint_id == receipt.checkpoint_id
    assert output_path.is_file()
    assert [event["event_type"] for event in json.loads(output_path.read_bytes())["events"]] == [
        "runtime.draining",
        "checkpoint.started",
        "checkpoint.verified",
        "runtime.terminal",
    ]
    assert [event["event_type"] for event in runtime.spool.pending()] == [
        "runtime.draining",
        "checkpoint.started",
        "checkpoint.verified",
        "runtime.terminal",
    ]
    assert not any(record["record"] == "delivered" for record in runtime.spool.records())
    assert events.count("checkpoint.publish") == 1
    assert events[-2:] == ["tunnel.stop", "postgres.stop"]


def test_persistent_terminal_outage_without_exact_output_still_fails_closed(tmp_path: Path) -> None:
    events: list[str] = []
    observed = iter([0.0, 0.0, 111.0])
    with pytest.raises(CheckpointShutdownError, match="queued"):
        _checkpoint_until_deadline(
            gate=_Gate(events),
            runtime=_Runtime(events, terminal_status="queued", flush_results=[False, False]),
            tunnel=_Process(events, "tunnel"),
            supervisor=_Process(events, "postgres"),
            coordinator=_Coordinator(events),
            database_url="postgresql:///postgres",
            package_directory=tmp_path / "checkpoints",
            identity=IDENTITY,
            deadline=200.0,
            publication_attempt_seconds=100.0,
            retry_seconds=60.0,
            monotonic=lambda: next(observed),
            sleep=lambda _seconds: None,
        )
    assert events.count("checkpoint.publish") == 1
    assert "tunnel.stop" not in events and "postgres.stop" not in events


@pytest.mark.parametrize(
    ("active_error", "expected_error"),
    [
        (CallbackLeaseClosingError("callback unavailable; write lease is closing"), None),
        (RuntimeError("unrelated tunnel failure"), RuntimeError),
    ],
)
def test_run_master_suppresses_only_callback_lease_closure_after_exact_terminal_recovery(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    active_error: BaseException,
    expected_error: type[BaseException] | None,
) -> None:
    events: list[str] = []
    working = tmp_path / "kaggle" / "working"
    working.mkdir(parents=True)
    monkeypatch.setenv("KAGGLE_WORKING_DIR", str(working))
    for name, value in {
        "MY_DATA_HUB_CALLBACK_URL": "https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        "MY_DATA_HUB_RUN_SECRET": "run-secret-long-enough",
        "MY_DATA_HUB_POSTGRES_TLS_CERT": str(tmp_path / "tls.crt"),
        "MY_DATA_HUB_POSTGRES_TLS_KEY": str(tmp_path / "tls.key"),
        "MY_DATA_HUB_TUNNEL_KNOWN_HOSTS": str(tmp_path / "known_hosts"),
    }.items():
        monkeypatch.setenv(name, value)

    class Runtime(_Runtime):
        def emit(self, event_type, **kwargs):  # type: ignore[no-untyped-def]
            if event_type is RuntimeEventType.RUNTIME_HEARTBEAT:
                raise active_error
            return super().emit(event_type, **kwargs)

    runtime = Runtime(events, terminal_status="queued", flush_results=[False, False])

    class Connection:
        closed = False

        def close(self) -> None:
            self.closed = True

    connection = Connection()

    class Gate(_Gate):
        def __init__(self, _connection) -> None:  # type: ignore[no-untyped-def]
            super().__init__(events)

        def activate(self, identity) -> None:  # type: ignore[no-untyped-def]
            events.append("gate.activate")

        def fence(self, identity, reason) -> None:  # type: ignore[no-untyped-def]
            events.append("gate.fence")

    tunnel = _Process(events, "tunnel")
    tunnel.poll = lambda **kwargs: events.append("tunnel.poll")  # type: ignore[attr-defined]
    postgres = _Process(events, "postgres")

    class Bootstrap:
        def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
            self.announce_ready = kwargs["announce_ready"]
            self.tunnel = kwargs["tunnel"]

        def run(self, request):  # type: ignore[no-untyped-def]
            self.tunnel.start(now=datetime.now(UTC))
            ready = SimpleNamespace(
                lease_until=datetime.now(UTC) + timedelta(seconds=120),
                event_payload=lambda: {"epoch": 1},
            )
            self.announce_ready(ready)
            return ready

    config = NotebookMasterConfig(
        master_instance_id=IDENTITY.master_instance_id,
        run_id=IDENTITY.run_id,
        attempt_id="attempt-1",
        service_instance_id="service-1",
        epoch=IDENTITY.epoch,
        boot_source=BootSource.EMPTY_BASELINE,
        checkpoint_directory=None,
        lease_seconds=120,
        postgres_bin=tmp_path,
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
    original_checkpoint_until = _checkpoint_until_deadline

    def checkpoint_until(**kwargs):  # type: ignore[no-untyped-def]
        kwargs.update(
            deadline=200.0,
            publication_attempt_seconds=100.0,
            retry_seconds=60.0,
            monotonic=iter([0.0, 0.0, 111.0]).__next__,
            sleep=lambda _seconds: None,
        )
        return original_checkpoint_until(**kwargs)

    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.RuntimeClient", lambda **kwargs: runtime)
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.PostgresBinaries.discover",
        lambda path: object(),
    )
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.PostgresSupervisor", lambda **kwargs: postgres)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.TunnelSupervisor", lambda spec: tunnel)
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._issue_ephemeral_tunnel_identity",
        lambda **kwargs: SimpleNamespace(
            private_key=tmp_path / "ephemeral-tunnel-key",
            certificate=tmp_path / "ephemeral-tunnel-key-cert.pub",
        ),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._renew_registration_tunnel_lease",
        lambda **kwargs: None,
    )
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.MasterBootstrap", Bootstrap)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.DatabaseGate", Gate)
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.psycopg.connect",
        lambda *args, **kwargs: connection,
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._wait_for_activation",
        lambda *args: ("reader",),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._register_session_credentials",
        lambda **kwargs: (("reader",), kwargs["expires_at"]),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._cleanup_epoch_principals",
        lambda **kwargs: events.append("credentials.drop"),
    )
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint._checkpoint_until_deadline", checkpoint_until)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.time.sleep", lambda _seconds: None)

    started_at = time.monotonic()
    if expected_error is None:
        assert run_master(config, checkpoint_coordinator=_Coordinator(events), process_started_at=started_at) == 0
    else:
        with pytest.raises(expected_error, match="unrelated tunnel failure"):
            run_master(config, checkpoint_coordinator=_Coordinator(events), process_started_at=started_at)
    assert (working / "my-data-hub-master-terminal.json").is_file()
    assert events.count("checkpoint.publish") == 1


def test_run_master_never_checkpoints_unacknowledged_blogger_import_receipt(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    events: list[str] = []
    working = tmp_path / "kaggle" / "working"
    working.mkdir(parents=True)
    monkeypatch.setenv("KAGGLE_WORKING_DIR", str(working))
    for name, value in {
        "MY_DATA_HUB_CALLBACK_URL": "https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        "MY_DATA_HUB_RUN_SECRET": "run-secret-long-enough",
        "MY_DATA_HUB_POSTGRES_TLS_CERT": str(tmp_path / "tls.crt"),
        "MY_DATA_HUB_POSTGRES_TLS_KEY": str(tmp_path / "tls.key"),
        "MY_DATA_HUB_TUNNEL_KNOWN_HOSTS": str(tmp_path / "known_hosts"),
    }.items():
        monkeypatch.setenv(name, value)

    class Connection:
        closed = False

        def close(self) -> None:
            self.closed = True

    connection = Connection()

    class Gate(_Gate):
        def __init__(self, _connection) -> None:  # type: ignore[no-untyped-def]
            super().__init__(events)

        def activate(self, identity) -> None:  # type: ignore[no-untyped-def]
            events.append("gate.activate")

        def fence(self, identity, reason) -> None:  # type: ignore[no-untyped-def]
            events.append(f"gate.fence:{reason}")

    tunnel = _Process(events, "tunnel")
    postgres = _Process(events, "postgres")

    class Bootstrap:
        def __init__(self, **kwargs) -> None:  # type: ignore[no-untyped-def]
            self.announce_ready = kwargs["announce_ready"]
            self.tunnel = kwargs["tunnel"]

        def run(self, request):  # type: ignore[no-untyped-def]
            self.tunnel.start(now=datetime.now(UTC))
            ready = SimpleNamespace(
                lease_until=datetime.now(UTC) + timedelta(seconds=120),
                event_payload=lambda: {"epoch": 1},
            )
            self.announce_ready(ready)
            return ready

    migration_request = BloggerMigrationRequest(
        request_id=UUID("22222222-2222-4222-8222-222222222222"),
        operation_id=UUID("33333333-3333-4333-8333-333333333333"),
        project_id=UUID("44444444-4444-4444-8444-444444444444"),
        snapshot_at=datetime(2026, 8, 10, tzinfo=UTC),
        source_revision="b" * 40,
    )
    import_receipt = BloggerImportStageReceipt(
        request_id=migration_request.request_id,
        operation_id=migration_request.operation_id,
        master_instance_id=IDENTITY.master_instance_id,
        run_id=IDENTITY.run_id,
        epoch=IDENTITY.epoch,
        request_sha256=migration_request.request_sha256,
        export_batch_id=UUID("55555555-5555-4555-8555-555555555555"),
        row_count=266,
        distinct_record_ids=266,
        source_file_count=14,
        dispositions={"imported": 266, "quarantined": 0},
        record_id_set_sha256="a" * 64,
        logical_sha256="b" * 64,
        canonical_outcome_sha256="c" * 64,
        actor_count=266,
        account_count=210,
        duplicate_group_count=0,
        replayed_count=0,
        canonical_revision=9,
    )
    config = NotebookMasterConfig(
        master_instance_id=IDENTITY.master_instance_id,
        run_id=IDENTITY.run_id,
        attempt_id="attempt-1",
        service_instance_id="service-1",
        epoch=IDENTITY.epoch,
        boot_source=BootSource.EMPTY_BASELINE,
        checkpoint_directory=None,
        lease_seconds=120,
        postgres_bin=tmp_path,
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

    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.RuntimeClient",
        lambda **kwargs: _Runtime(events),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.PostgresBinaries.discover",
        lambda path: object(),
    )
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.PostgresSupervisor", lambda **kwargs: postgres)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.TunnelSupervisor", lambda spec: tunnel)
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._issue_ephemeral_tunnel_identity",
        lambda **kwargs: SimpleNamespace(
            private_key=tmp_path / "ephemeral-tunnel-key",
            certificate=tmp_path / "ephemeral-tunnel-key-cert.pub",
        ),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._renew_registration_tunnel_lease",
        lambda **kwargs: None,
    )
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.MasterBootstrap", Bootstrap)
    monkeypatch.setattr("my_data_hub.master_runtime.notebook_entrypoint.DatabaseGate", Gate)
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.psycopg.connect",
        lambda *args, **kwargs: connection,
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._wait_for_activation",
        lambda *args: ("reader",),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._register_session_credentials",
        lambda **kwargs: (("reader",), kwargs["expires_at"]),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._cleanup_epoch_principals",
        lambda **kwargs: events.append("credentials.drop"),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._claim_blogger_migration",
        lambda **kwargs: migration_request,
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.execute_blogger_migration_stage",
        lambda *args, **kwargs: import_receipt,
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._post_blogger_runtime_receipt",
        lambda **kwargs: (_ for _ in ()).throw(BloggerReceiptDeliveryError("persistent receipt outage")),
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint._checkpoint_until_deadline",
        lambda **kwargs: pytest.fail("unacknowledged blogger receipt must never be checkpointed"),
    )

    with pytest.raises(BloggerReceiptDeliveryError, match="persistent receipt outage"):
        run_master(config, checkpoint_coordinator=_Coordinator(events), process_started_at=time.monotonic())

    assert "checkpoint.publish" not in events
    assert "gate.fence:blogger_import_receipt_unacknowledged" in events
    assert events[-2:] == ["tunnel.stop", "postgres.stop"]
    assert connection.closed is True


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
        acceptance_effects_factory: ProductionMasterAcceptanceEffectsFactory,
        process_started_at: float,
    ) -> int:
        observed["config"] = config
        observed["coordinator"] = checkpoint_coordinator
        observed["acceptance_effects_factory"] = acceptance_effects_factory
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
    assert isinstance(observed["acceptance_effects_factory"], ProductionMasterAcceptanceEffectsFactory)
    assert observed["process_started_at"] == 123.0
