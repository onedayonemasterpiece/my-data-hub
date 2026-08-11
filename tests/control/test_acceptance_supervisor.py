from __future__ import annotations

import hashlib
import json
import os
import socket
import stat
import threading
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import NAMESPACE_URL, UUID, uuid5

import pytest

from my_data_hub.acceptance.master_lifecycle import (
    MasterAcceptanceBinding,
    MasterAcceptanceCommand,
    MasterAcceptanceCommandKind,
    MasterAcceptanceScenario,
)
from my_data_hub.acceptance.master_production import ControlRestartReceipt
from my_data_hub.control_plane.acceptance_supervisor import (
    ALLOWED_CALLBACK_EVENT_TYPES,
    CallbackCapture,
    CallbackLossDirective,
    CallbackSupervisorBlocked,
    ComposeControlPlaneRestartRunner,
    ControlLedgerCallbackLossPort,
    HostRestartController,
    HostRestartJournal,
    HostRestartRequest,
    LedgerCallbackLossSupervisor,
    UnixHostRestartClient,
    UnixHostRestartServer,
    _parse_signed_envelope,
    _signed_envelope,
    callback_loss_supervisor_from_environment,
)
from my_data_hub.hashing import canonical_json_bytes

NOW = datetime(2026, 8, 11, 10, 0, tzinfo=UTC)
BEFORE = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
AFTER = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
BODY_HASH = "c" * 64
KEY = b"k" * 32


def command() -> MasterAcceptanceCommand:
    task_id = UUID("11111111-1111-4111-8111-111111111111")
    return MasterAcceptanceCommand(
        command_id=uuid5(NAMESPACE_URL, f"master-acceptance:{task_id}:FM08"),
        task_id=task_id,
        scenario=MasterAcceptanceScenario.FM08,
        command_kind=MasterAcceptanceCommandKind.CALLBACK_LOSS_RECOVERY,
        source_revision="d" * 40,
        binding=MasterAcceptanceBinding(
            operation_id=UUID("22222222-2222-4222-8222-222222222222"),
            run_id=UUID("33333333-3333-4333-8333-333333333333"),
            attempt_id=UUID("44444444-4444-4444-8444-444444444444"),
            service_instance_id="master-service-1",
            master_instance_id=UUID("55555555-5555-4555-8555-555555555555"),
            epoch=7,
        ),
    )


def directive() -> CallbackLossDirective:
    value = command()
    fields = {
        "task_id": str(value.task_id),
        "command_id": str(value.command_id),
        "command_sha256": value.command_sha256,
        "run_id": str(value.binding.run_id),
        "attempt_id": str(value.binding.attempt_id),
        "master_instance_id": str(value.binding.master_instance_id),
        "epoch": value.binding.epoch,
        "event_type": "runtime.heartbeat",
        "maximum_callbacks": 1,
        "schema_version": "my-data-hub-fm08-callback-directive.v1",
        "expires_at": (NOW + timedelta(seconds=180)).isoformat().replace("+00:00", "Z"),
        "before_boot_id": str(BEFORE),
    }
    receipt_hash = hashlib.sha256(canonical_json_bytes(fields)).hexdigest()
    return CallbackLossDirective(
        task_id=value.task_id,
        command_id=value.command_id,
        command_sha256=value.command_sha256,
        operation_id=value.binding.operation_id,
        run_id=value.binding.run_id,
        attempt_id=value.binding.attempt_id,
        master_instance_id=value.binding.master_instance_id,
        epoch=value.binding.epoch,
        allowed_event_types=ALLOWED_CALLBACK_EVENT_TYPES,
        max_callbacks=1,
        armed_at=NOW,
        expires_at=NOW + timedelta(seconds=180),
        before_boot_id=BEFORE,
        directive_receipt_sha256=receipt_hash,
        acknowledged=True,
    )


def private_key(tmp_path: Path) -> Path:
    path = tmp_path / "supervisor.key"
    path.write_bytes(KEY)
    path.chmod(0o600)
    return path


@dataclass
class SequenceHealth:
    values: list[UUID]

    def current_boot_id(self) -> UUID:
        if len(self.values) > 1:
            return self.values.pop(0)
        return self.values[0]


@dataclass
class InspectingRunner:
    journal: HostRestartJournal
    command_id: UUID
    calls: int = 0

    def restart_control_plane(self) -> None:
        self.calls += 1
        assert self.journal.load()[str(self.command_id)]["state"] == "INTENT"


def test_host_request_is_task_derived_signed_and_has_no_command_selector() -> None:
    request = HostRestartRequest.from_directive(directive())
    assert request.request_id == uuid5(
        NAMESPACE_URL, f"my-data-hub:fm08:restart:{request.command_id}"
    )
    decoded = json.loads(_signed_envelope(request, KEY))
    assert decoded["action"] == "RESTART_CONTROL_PLANE"
    assert not ({"argv", "service", "path", "url", "body", "event"} & decoded.keys())
    assert _parse_signed_envelope(_signed_envelope(request, KEY), KEY) == request

    decoded["service"] = "remote-mcp"
    unsigned = decoded.pop("auth_hmac_sha256")
    decoded["auth_hmac_sha256"] = unsigned
    with pytest.raises(PermissionError):
        _parse_signed_envelope(canonical_json_bytes(decoded) + b"\n", KEY)


def test_host_controller_persists_before_restart_and_replays_without_second_effect(
    tmp_path: Path,
) -> None:
    request = HostRestartRequest.from_directive(directive())
    journal = HostRestartJournal(tmp_path / "journal.json")
    runner = InspectingRunner(journal, request.command_id)
    controller = HostRestartController(
        journal=journal,
        runner=runner,
        health=SequenceHealth([BEFORE, AFTER]),
        now=lambda: NOW + timedelta(seconds=1),
        sleep=lambda _seconds: None,
    )

    first = controller.execute(request)
    second = controller.execute(request)

    assert first == second
    assert first.before_boot_id == BEFORE
    assert first.after_boot_id == AFTER
    assert runner.calls == 1
    stored = journal.load()[str(request.command_id)]
    assert stored["state"] == "COMPLETE"
    assert "directive_receipt_sha256" not in json.dumps(stored["receipt"])


def test_host_controller_recovers_lost_response_from_changed_healthy_boot(tmp_path: Path) -> None:
    request = HostRestartRequest.from_directive(directive())
    journal = HostRestartJournal(tmp_path / "journal.json")
    journal.record(
        request.command_id,
        {
            "state": "INTENT",
            "request_sha256": request.request_sha256,
            "request_id": str(request.request_id),
            "before_boot_id": str(BEFORE),
            "directive_receipt_sha256": request.directive_receipt_sha256,
        },
    )
    runner = InspectingRunner(journal, request.command_id)
    receipt = HostRestartController(
        journal=journal,
        runner=runner,
        health=SequenceHealth([AFTER]),
        now=lambda: NOW + timedelta(seconds=10),
    ).execute(request)
    assert receipt.after_boot_id == AFTER
    assert runner.calls == 0


def test_expired_directive_blocks_before_restart(tmp_path: Path) -> None:
    request = HostRestartRequest.from_directive(directive())
    journal = HostRestartJournal(tmp_path / "journal.json")
    runner = InspectingRunner(journal, request.command_id)
    controller = HostRestartController(
        journal=journal,
        runner=runner,
        health=SequenceHealth([BEFORE]),
        now=lambda: request.expires_at,
    )
    with pytest.raises(CallbackSupervisorBlocked, match="FM08_DIRECTIVE_EXPIRED"):
        controller.execute(request)
    assert runner.calls == 0
    assert journal.load()[str(request.command_id)]["state"] == "BLOCKED"


def test_compose_runner_has_one_immutable_target(tmp_path: Path) -> None:
    docker = tmp_path / "docker"
    docker.write_text("#!/bin/sh\n", encoding="utf-8")
    docker.chmod(0o700)
    env = tmp_path / "compose.env"
    env.write_text("TAG=x\n", encoding="utf-8")
    compose = tmp_path / "compose.control-plane.yaml"
    compose.write_text("services: {}\n", encoding="utf-8")
    runner = ComposeControlPlaneRestartRunner(
        docker_path=docker,
        compose_env=env,
        project_directory=tmp_path,
        compose_files=(compose,),
    )
    assert runner.argv[-3:] == ("restart", "--no-deps", "control-plane")
    assert "remote-mcp" not in runner.argv[-3:]
    assert "oauth-server" not in runner.argv[-3:]


@dataclass
class FakeControl:
    directive: CallbackLossDirective | None = None
    captured: CallbackCapture | None = None
    dispositions: list[Literal["accepted", "duplicate"] | None] = field(default_factory=list)
    arm_calls: int = 0
    disarmed: int = 0
    restart_ids: tuple[UUID, UUID] | None = None

    def arm_callback_loss(self, value: MasterAcceptanceCommand, **kwargs: Any) -> CallbackLossDirective:
        self.arm_calls += 1
        assert value == command()
        assert kwargs["allowed_event_types"] == ALLOWED_CALLBACK_EVENT_TYPES
        assert kwargs["max_callbacks"] == 1
        self.directive = directive()
        return self.directive

    def callback_loss_directive(self, value: MasterAcceptanceCommand) -> CallbackLossDirective | None:
        assert value == command()
        return self.directive

    def captured_callback(
        self, value: MasterAcceptanceCommand, armed: CallbackLossDirective
    ) -> CallbackCapture | None:
        assert value == command()
        assert armed == self.directive
        return self.captured

    def replay_disposition(
        self,
        value: MasterAcceptanceCommand,
        armed: CallbackLossDirective,
        event_id: UUID,
    ) -> Literal["accepted", "duplicate"] | None:
        assert value == command()
        assert armed == self.directive
        assert self.captured is not None and event_id == self.captured.event_id
        return self.dispositions.pop(0) if self.dispositions else None

    def disarm_expired_callback_loss(
        self, value: MasterAcceptanceCommand, armed: CallbackLossDirective
    ) -> None:
        assert value == command()
        assert armed == self.directive
        self.disarmed += 1

    def record_control_restart(
        self,
        value: MasterAcceptanceCommand,
        armed: CallbackLossDirective,
        *,
        before_boot_id: UUID,
        after_boot_id: UUID,
    ) -> None:
        assert value == command()
        assert armed == self.directive
        self.restart_ids = (before_boot_id, after_boot_id)

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool:
        return binding == command().binding


@dataclass
class FakeHost:
    available: bool = True
    checked: int = 0

    def assert_available(self) -> None:
        self.checked += 1
        if not self.available:
            raise CallbackSupervisorBlocked("FM08_HOST_PERMISSION_UNAVAILABLE")

    def restart(self, request: HostRestartRequest):  # type: ignore[no-untyped-def]
        from my_data_hub.control_plane.acceptance_supervisor import HostRestartReceipt

        assert request.before_boot_id == BEFORE
        return HostRestartReceipt(
            request_id=request.request_id,
            request_sha256=request.request_sha256,
            before_boot_id=BEFORE,
            after_boot_id=AFTER,
            healthy=True,
        )


def test_missing_host_permission_blocks_before_directive_effect() -> None:
    control = FakeControl()
    supervisor = LedgerCallbackLossSupervisor(
        control=control,
        host=FakeHost(available=False),  # type: ignore[arg-type]
        health=SequenceHealth([BEFORE]),
        now=lambda: NOW,
    )
    with pytest.raises(CallbackSupervisorBlocked, match="FM08_HOST_PERMISSION_UNAVAILABLE"):
        supervisor.suppress_next_task_callback(command())
    assert control.arm_calls == 0


def test_supervisor_returns_only_captured_identity_and_waits_for_replay(tmp_path: Path) -> None:
    del tmp_path
    event_id = UUID("66666666-6666-4666-8666-666666666666")
    control = FakeControl(
        directive=directive(),
        captured=CallbackCapture(event_id=event_id, body_sha256=BODY_HASH),
        dispositions=[None, "duplicate"],
    )
    monotonic_value = 0.0

    def monotonic() -> float:
        nonlocal monotonic_value
        monotonic_value += 0.25
        return monotonic_value

    supervisor = LedgerCallbackLossSupervisor(
        control=control,
        host=FakeHost(),  # type: ignore[arg-type]
        health=SequenceHealth([BEFORE]),
        now=lambda: NOW,
        monotonic=monotonic,
        sleep=lambda _seconds: None,
    )
    stored = supervisor.suppress_next_task_callback(command())
    assert stored.event_id == event_id
    assert stored.body_sha256 == BODY_HASH
    assert not hasattr(stored, "body")
    restart = supervisor.restart_control_process(command())
    assert isinstance(restart, ControlRestartReceipt)
    assert (restart.before_boot_id, restart.after_boot_id) == (BEFORE, AFTER)
    assert control.restart_ids == (BEFORE, AFTER)
    assert supervisor.replay_stored_callback(command(), event_id) == "duplicate"
    assert supervisor.exact_service_active(command().binding)


def test_private_unix_socket_auth_and_mode(tmp_path: Path) -> None:
    request = HostRestartRequest.from_directive(directive())
    journal = HostRestartJournal(tmp_path / "journal.json")
    runner = InspectingRunner(journal, request.command_id)
    socket_path = tmp_path / "supervisor" / "control.sock"
    server = UnixHostRestartServer(
        socket_path=socket_path,
        key_path=private_key(tmp_path),
        allowed_uid=os.getuid(),
        controller=HostRestartController(
            journal=journal,
            runner=runner,
            health=SequenceHealth([BEFORE, AFTER]),
            now=lambda: NOW + timedelta(seconds=1),
            sleep=lambda _seconds: None,
        ),
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    for _ in range(100):
        if socket_path.exists():
            break
        threading.Event().wait(0.01)
    assert stat.S_IMODE(socket_path.stat().st_mode) == 0o600
    client = UnixHostRestartClient(socket_path, server.key_path, response_timeout_seconds=5)
    receipt = client.restart(request)
    assert receipt.after_boot_id == AFTER
    server.stop.set()
    # Wake accept() so shutdown does not wait for the timeout.
    with suppress(FileNotFoundError), socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as wake:
        wake.connect(str(socket_path))
    thread.join(timeout=2)
    assert not thread.is_alive()


@dataclass
class FakeLedger:
    row: dict[str, Any] | None = None

    def arm_master_acceptance_callback_loss(self, **values: Any) -> dict[str, Any]:
        expires = (NOW + timedelta(seconds=120)).isoformat().replace("+00:00", "Z")
        receipt = {
            "schema_version": "my-data-hub-fm08-callback-directive.v1",
            "task_id": values["task_id"],
            "command_id": values["command_id"],
            "command_sha256": values["command_sha256"],
            "run_id": values["run_id"],
            "attempt_id": values["attempt_id"],
            "master_instance_id": values["master_instance_id"],
            "epoch": values["epoch"],
            "event_type": "runtime.heartbeat",
            "maximum_callbacks": 1,
            "before_boot_id": values["before_boot_id"],
            "expires_at": expires,
        }
        self.row = {
            **values,
            "scenario_id": "FM08",
            "callback_state": "ARMED",
            "callback_count": 0,
            "armed_at": NOW.isoformat().replace("+00:00", "Z"),
            "expires_at": expires,
            "directive_receipt_sha256": hashlib.sha256(
                canonical_json_bytes(receipt)
            ).hexdigest(),
            "restart_from_id": None,
            "restart_to_id": None,
        }
        return self.row

    def master_acceptance_runtime_control(self, task_id: str) -> dict[str, Any] | None:
        assert self.row is None or task_id == self.row["task_id"]
        return self.row

    def armed_master_acceptance_callback_loss(self, **identity: Any) -> dict[str, Any] | None:
        assert self.row is not None
        assert identity["run_id"] == self.row["run_id"]
        return self.row

    def record_master_acceptance_restart(self, **values: Any) -> dict[str, Any]:
        assert self.row is not None and values["task_id"] == self.row["task_id"]
        self.row["restart_from_id"] = values["restart_from_id"]
        self.row["restart_to_id"] = values["restart_to_id"]
        return self.row


def test_control_ledger_adapter_reconciles_exact_directive_and_restart() -> None:
    ledger = FakeLedger()
    adapter = ControlLedgerCallbackLossPort(ledger)
    value = command()
    armed = adapter.arm_callback_loss(
        value,
        allowed_event_types=ALLOWED_CALLBACK_EVENT_TYPES,
        max_callbacks=1,
        armed_at=NOW,
        expires_at=NOW + timedelta(seconds=180),
        before_boot_id=BEFORE,
    )
    assert armed.command_sha256 == value.command_sha256
    assert armed.expires_at == NOW + timedelta(seconds=120)
    assert adapter.callback_loss_directive(value) == armed

    assert ledger.row is not None
    ledger.row.update(
        callback_state="CAPTURED",
        callback_count=1,
        callback_event_id=str(UUID(int=99)),
        callback_body_sha256=BODY_HASH,
    )
    assert adapter.captured_callback(value, armed) == CallbackCapture(
        event_id=UUID(int=99), body_sha256=BODY_HASH
    )
    adapter.record_control_restart(
        value, armed, before_boot_id=BEFORE, after_boot_id=AFTER
    )
    ledger.row["callback_state"] = "REPLAYED"
    assert adapter.replay_disposition(value, armed, UUID(int=99)) == "duplicate"


def test_environment_factory_is_default_off_and_rejects_partial_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_SOCKET", raising=False)
    monkeypatch.delenv("MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_KEY_FILE", raising=False)
    assert callback_loss_supervisor_from_environment(FakeLedger()) is None
    monkeypatch.setenv("MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_SOCKET", "/run/mdh/control.sock")
    with pytest.raises(ValueError, match="configured together"):
        callback_loss_supervisor_from_environment(FakeLedger())
