"""Task-bound FM08 callback-loss and control-container restart supervisor.

The public control API is intentionally absent.  A control process talks to the
host daemon through a private Unix socket.  The daemon accepts one closed
``RESTART_CONTROL_PLANE`` message and invokes one immutable compose command; no
request field can select an executable, compose file, service, URL, or callback
body.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import socket
import struct
import subprocess
import tempfile
import threading
import time
import urllib.request
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.hashing import canonical_json_bytes

if TYPE_CHECKING:
    from my_data_hub.acceptance.master_lifecycle import (
        MasterAcceptanceBinding,
        MasterAcceptanceCommand,
    )

MAX_MESSAGE_BYTES = 16 * 1024
MAX_RESPONSE_BYTES = 16 * 1024
CALLBACK_CAPTURE_SECONDS = 90
CALLBACK_REPLAY_SECONDS = 120
DIRECTIVE_TTL_SECONDS = 180
HEALTH_TIMEOUT_SECONDS = 120
ALLOWED_CALLBACK_EVENT_TYPES = ("runtime.heartbeat",)
RESTART_ACTION = "RESTART_CONTROL_PLANE"
_UUID_PATTERN = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
_SHA_PATTERN = re.compile(r"^[0-9a-f]{64}$")


class CallbackSupervisorBlocked(RuntimeError):
    """FM08 could not safely begin or reconcile a host effect."""

    def __init__(self, code: str) -> None:
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{2,119}", code):
            raise ValueError("callback supervisor blocker code is invalid")
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class CallbackCapture:
    event_id: UUID
    body_sha256: str

    def __post_init__(self) -> None:
        if not _SHA_PATTERN.fullmatch(self.body_sha256):
            raise ValueError("captured callback body hash is invalid")


@dataclass(frozen=True, slots=True)
class CallbackLossDirective:
    """Persisted, owner-claim-bound instruction; contains no callback body."""

    task_id: UUID
    command_id: UUID
    command_sha256: str
    operation_id: UUID
    run_id: UUID
    attempt_id: UUID
    master_instance_id: UUID
    epoch: int
    allowed_event_types: tuple[Literal["runtime.heartbeat"], ...]
    max_callbacks: Literal[1]
    armed_at: datetime
    expires_at: datetime
    before_boot_id: UUID
    directive_receipt_sha256: str
    acknowledged: Literal[True]

    def __post_init__(self) -> None:
        receipt_payload = {
            "schema_version": "my-data-hub-fm08-callback-directive.v1",
            "task_id": str(self.task_id),
            "command_id": str(self.command_id),
            "command_sha256": self.command_sha256,
            "run_id": str(self.run_id),
            "attempt_id": str(self.attempt_id),
            "master_instance_id": str(self.master_instance_id),
            "epoch": self.epoch,
            "event_type": "runtime.heartbeat",
            "maximum_callbacks": 1,
            "expires_at": self.expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "before_boot_id": str(self.before_boot_id),
        }
        expected_receipt = hashlib.sha256(canonical_json_bytes(receipt_payload)).hexdigest()
        if (
            self.epoch < 1
            or not _SHA_PATTERN.fullmatch(self.command_sha256)
            or self.allowed_event_types != ALLOWED_CALLBACK_EVENT_TYPES
            or self.max_callbacks != 1
            or not self.acknowledged
            or self.armed_at.tzinfo is None
            or self.expires_at.tzinfo is None
            or not self.armed_at < self.expires_at <= self.armed_at + timedelta(seconds=DIRECTIVE_TTL_SECONDS)
            or not _SHA_PATTERN.fullmatch(self.directive_receipt_sha256)
            or not hmac.compare_digest(self.directive_receipt_sha256, expected_receipt)
        ):
            raise ValueError("callback-loss directive violates its fixed bounds")

    def assert_command(self, command: MasterAcceptanceCommand) -> None:
        binding = command.binding
        if (
            self.task_id != command.task_id
            or self.command_id != command.command_id
            or self.command_sha256 != command.command_sha256
            or self.operation_id != binding.operation_id
            or self.run_id != binding.run_id
            or self.attempt_id != binding.attempt_id
            or self.master_instance_id != binding.master_instance_id
            or self.epoch != binding.epoch
        ):
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_BINDING_MISMATCH")


class CallbackLossControlPort(Protocol):
    """Narrow adapter over migration-019 state; implementations store hashes only."""

    def arm_callback_loss(
        self,
        command: MasterAcceptanceCommand,
        *,
        allowed_event_types: tuple[Literal["runtime.heartbeat"], ...],
        max_callbacks: Literal[1],
        armed_at: datetime,
        expires_at: datetime,
        before_boot_id: UUID,
    ) -> CallbackLossDirective: ...

    def callback_loss_directive(self, command: MasterAcceptanceCommand) -> CallbackLossDirective | None: ...

    def captured_callback(
        self, command: MasterAcceptanceCommand, directive: CallbackLossDirective
    ) -> CallbackCapture | None: ...

    def replay_disposition(
        self,
        command: MasterAcceptanceCommand,
        directive: CallbackLossDirective,
        event_id: UUID,
    ) -> Literal["accepted", "duplicate"] | None: ...

    def record_control_restart(
        self,
        command: MasterAcceptanceCommand,
        directive: CallbackLossDirective,
        *,
        before_boot_id: UUID,
        after_boot_id: UUID,
    ) -> None: ...

    def disarm_expired_callback_loss(
        self, command: MasterAcceptanceCommand, directive: CallbackLossDirective
    ) -> None: ...

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool: ...


class ControlHealthProbe(Protocol):
    def current_boot_id(self) -> UUID: ...


@dataclass(frozen=True, slots=True)
class HttpControlHealthProbe:
    """Read only the loopback control health document and its per-process UUID."""

    url: str = "http://127.0.0.1:8080/health/ready"
    timeout_seconds: float = 3.0

    def __post_init__(self) -> None:
        if self.url != "http://127.0.0.1:8080/health/ready" or not 0 < self.timeout_seconds <= 5:
            raise ValueError("control health probe must use the fixed loopback endpoint")

    def current_boot_id(self) -> UUID:
        try:
            with urllib.request.urlopen(self.url, timeout=self.timeout_seconds) as response:
                body = response.read(MAX_RESPONSE_BYTES + 1)
        except OSError as exc:
            raise CallbackSupervisorBlocked("FM08_CONTROL_HEALTH_UNAVAILABLE") from exc
        if len(body) > MAX_RESPONSE_BYTES:
            raise CallbackSupervisorBlocked("FM08_CONTROL_HEALTH_OVERSIZE")
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict) or payload.get("ok") is not True:
                raise ValueError("not healthy")
            return UUID(str(payload["control_boot_id"]))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CallbackSupervisorBlocked("FM08_CONTROL_BOOT_ID_INVALID") from exc


@dataclass(frozen=True, slots=True)
class HostRestartRequest:
    schema_version: Literal["my-data-hub-fm08-host-restart.v1"]
    action: Literal["RESTART_CONTROL_PLANE"]
    request_id: UUID
    task_id: UUID
    command_id: UUID
    command_sha256: str
    operation_id: UUID
    run_id: UUID
    attempt_id: UUID
    master_instance_id: UUID
    epoch: int
    before_boot_id: UUID
    directive_receipt_sha256: str
    issued_at: datetime
    expires_at: datetime

    def __post_init__(self) -> None:
        expected_id = uuid5(NAMESPACE_URL, f"my-data-hub:fm08:restart:{self.command_id}")
        if (
            self.schema_version != "my-data-hub-fm08-host-restart.v1"
            or self.action != RESTART_ACTION
            or self.request_id != expected_id
            or self.epoch < 1
            or not _SHA_PATTERN.fullmatch(self.command_sha256)
            or not _SHA_PATTERN.fullmatch(self.directive_receipt_sha256)
            or self.issued_at.tzinfo is None
            or self.expires_at.tzinfo is None
            or not self.issued_at < self.expires_at <= self.issued_at + timedelta(seconds=DIRECTIVE_TTL_SECONDS)
        ):
            raise ValueError("host restart request violates its fixed contract")

    @classmethod
    def from_directive(cls, directive: CallbackLossDirective) -> HostRestartRequest:
        return cls(
            schema_version="my-data-hub-fm08-host-restart.v1",
            action=RESTART_ACTION,
            request_id=uuid5(NAMESPACE_URL, f"my-data-hub:fm08:restart:{directive.command_id}"),
            task_id=directive.task_id,
            command_id=directive.command_id,
            command_sha256=directive.command_sha256,
            operation_id=directive.operation_id,
            run_id=directive.run_id,
            attempt_id=directive.attempt_id,
            master_instance_id=directive.master_instance_id,
            epoch=directive.epoch,
            before_boot_id=directive.before_boot_id,
            directive_receipt_sha256=directive.directive_receipt_sha256,
            issued_at=directive.armed_at,
            expires_at=directive.expires_at,
        )

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "action": self.action,
            "request_id": str(self.request_id),
            "task_id": str(self.task_id),
            "command_id": str(self.command_id),
            "command_sha256": self.command_sha256,
            "operation_id": str(self.operation_id),
            "run_id": str(self.run_id),
            "attempt_id": str(self.attempt_id),
            "master_instance_id": str(self.master_instance_id),
            "epoch": self.epoch,
            "before_boot_id": str(self.before_boot_id),
            "directive_receipt_sha256": self.directive_receipt_sha256,
            "issued_at": self.issued_at.astimezone(UTC).isoformat(),
            "expires_at": self.expires_at.astimezone(UTC).isoformat(),
        }

    @classmethod
    def parse(cls, payload: Mapping[str, Any]) -> HostRestartRequest:
        expected = {
            "schema_version", "action", "request_id", "task_id", "command_id", "command_sha256", "operation_id",
            "run_id", "attempt_id", "master_instance_id", "epoch", "before_boot_id",
            "directive_receipt_sha256", "issued_at", "expires_at",
        }
        if set(payload) != expected:
            raise ValueError("host restart request fields differ from the fixed contract")
        try:
            return cls(
                schema_version=cast(Any, payload["schema_version"]),
                action=cast(Any, payload["action"]),
                request_id=UUID(str(payload["request_id"])),
                task_id=UUID(str(payload["task_id"])),
                command_id=UUID(str(payload["command_id"])),
                command_sha256=str(payload["command_sha256"]),
                operation_id=UUID(str(payload["operation_id"])),
                run_id=UUID(str(payload["run_id"])),
                attempt_id=UUID(str(payload["attempt_id"])),
                master_instance_id=UUID(str(payload["master_instance_id"])),
                epoch=int(payload["epoch"]),
                before_boot_id=UUID(str(payload["before_boot_id"])),
                directive_receipt_sha256=str(payload["directive_receipt_sha256"]),
                issued_at=datetime.fromisoformat(str(payload["issued_at"])).astimezone(UTC),
                expires_at=datetime.fromisoformat(str(payload["expires_at"])).astimezone(UTC),
            )
        except (TypeError, ValueError) as exc:
            raise ValueError("host restart request values are invalid") from exc

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.payload())).hexdigest()


@dataclass(frozen=True, slots=True)
class HostRestartReceipt:
    request_id: UUID
    request_sha256: str
    before_boot_id: UUID
    after_boot_id: UUID
    healthy: Literal[True]

    def __post_init__(self) -> None:
        if (
            not _SHA_PATTERN.fullmatch(self.request_sha256)
            or self.before_boot_id == self.after_boot_id
            or not self.healthy
        ):
            raise ValueError("host restart receipt does not prove a healthy process replacement")

    def payload(self) -> dict[str, Any]:
        return {
            "schema_version": "my-data-hub-fm08-host-restart-receipt.v1",
            "request_id": str(self.request_id),
            "request_sha256": self.request_sha256,
            "before_boot_id": str(self.before_boot_id),
            "after_boot_id": str(self.after_boot_id),
            "healthy": True,
        }

    @classmethod
    def parse(cls, payload: Mapping[str, Any]) -> HostRestartReceipt:
        expected = {
            "schema_version", "request_id", "request_sha256", "before_boot_id", "after_boot_id", "healthy"
        }
        if set(payload) != expected or payload.get("schema_version") != "my-data-hub-fm08-host-restart-receipt.v1":
            raise ValueError("host restart receipt fields are invalid")
        return cls(
            request_id=UUID(str(payload["request_id"])),
            request_sha256=str(payload["request_sha256"]),
            before_boot_id=UUID(str(payload["before_boot_id"])),
            after_boot_id=UUID(str(payload["after_boot_id"])),
            healthy=cast(Any, payload["healthy"]),
        )


def _signed_envelope(request: HostRestartRequest, key: bytes) -> bytes:
    payload = request.payload()
    signature = hmac.new(key, canonical_json_bytes(payload), hashlib.sha256).hexdigest()
    encoded = canonical_json_bytes({**payload, "auth_hmac_sha256": signature}) + b"\n"
    if len(encoded) > MAX_MESSAGE_BYTES:
        raise ValueError("host restart request is oversized")
    return encoded


def _parse_signed_envelope(raw: bytes, key: bytes) -> HostRestartRequest:
    if not raw or len(raw) > MAX_MESSAGE_BYTES or not raw.endswith(b"\n"):
        raise ValueError("host restart envelope framing is invalid")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("host restart envelope is not JSON") from exc
    if not isinstance(payload, dict):
        raise ValueError("host restart envelope must be an object")
    signature = payload.pop("auth_hmac_sha256", None)
    if not isinstance(signature, str) or not _SHA_PATTERN.fullmatch(signature):
        raise PermissionError("host restart envelope signature is absent")
    expected = hmac.new(key, canonical_json_bytes(payload), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(signature, expected):
        raise PermissionError("host restart envelope signature is invalid")
    return HostRestartRequest.parse(payload)


def _read_private_key(path: Path) -> bytes:
    if not path.is_absolute() or path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ValueError("acceptance supervisor key must be an absolute private regular file")
    key = path.read_bytes().strip()
    if not 32 <= len(key) <= 256:
        raise ValueError("acceptance supervisor key must contain 32..256 bytes")
    return key


def _atomic_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        directory_fd = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


@dataclass(slots=True)
class HostRestartJournal:
    path: Path

    def __post_init__(self) -> None:
        if not self.path.is_absolute() or self.path.is_symlink():
            raise ValueError("host restart journal must be an absolute non-symlink path")
        self.path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        if self.path.exists() and (not self.path.is_file() or self.path.stat().st_mode & 0o077):
            raise ValueError("host restart journal must be a private regular file")

    def load(self) -> dict[str, dict[str, Any]]:
        if not self.path.exists():
            return {}
        raw = self.path.read_bytes()
        if len(raw) > 1024 * 1024:
            raise ValueError("host restart journal exceeds 1 MiB")
        value = json.loads(raw)
        if not isinstance(value, dict) or not all(isinstance(item, dict) for item in value.values()):
            raise ValueError("host restart journal is malformed")
        return cast(dict[str, dict[str, Any]], value)

    def record(self, command_id: UUID, entry: Mapping[str, Any]) -> None:
        records = self.load()
        records[str(command_id)] = dict(entry)
        if len(records) > 1_000:
            raise ValueError("host restart journal entry bound exceeded")
        _atomic_json(self.path, records)


class FixedRestartRunner(Protocol):
    def restart_control_plane(self) -> None: ...


@dataclass(frozen=True, slots=True)
class ComposeControlPlaneRestartRunner:
    """Execute an installer-pinned argv; callers cannot choose its target."""

    docker_path: Path
    compose_env: Path
    project_directory: Path
    compose_files: tuple[Path, ...]

    def __post_init__(self) -> None:
        paths = (self.docker_path, self.compose_env, self.project_directory, *self.compose_files)
        if any(not path.is_absolute() or path.is_symlink() for path in paths):
            raise ValueError("compose restart configuration requires absolute non-symlink paths")
        if (
            not self.docker_path.is_file()
            or not os.access(self.docker_path, os.X_OK)
            or not self.compose_env.is_file()
            or not self.project_directory.is_dir()
            or not self.compose_files
            or any(not path.is_file() for path in self.compose_files)
            or self.compose_files != (self.project_directory / "compose.control-plane.yaml",)
        ):
            raise ValueError("compose restart configuration is unavailable")

    @property
    def argv(self) -> tuple[str, ...]:
        result = [
            str(self.docker_path), "compose", "--env-file", str(self.compose_env),
            "--profile", "remote-mcp", "--project-directory", str(self.project_directory),
        ]
        for compose_file in self.compose_files:
            result.extend(("-f", str(compose_file)))
        result.extend(("restart", "--no-deps", "control-plane"))
        return tuple(result)

    def restart_control_plane(self) -> None:
        subprocess.run(
            self.argv,
            check=True,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=60,
            env={"PATH": "/usr/bin:/bin", "HOME": str(Path.home())},
        )


@dataclass(slots=True)
class HostRestartController:
    journal: HostRestartJournal
    runner: FixedRestartRunner
    health: ControlHealthProbe
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    sleep: Callable[[float], None] = time.sleep

    def execute(self, request: HostRestartRequest) -> HostRestartReceipt:
        records = self.journal.load()
        prior = records.get(str(request.command_id))
        if prior is not None:
            if prior.get("request_sha256") != request.request_sha256:
                raise PermissionError("host restart command identity was reused")
            if prior.get("state") == "COMPLETE":
                return HostRestartReceipt.parse(cast(Mapping[str, Any], prior["receipt"]))
            if prior.get("state") != "INTENT":
                raise CallbackSupervisorBlocked("FM08_HOST_RESTART_TERMINAL")
            current = self._healthy_boot_id()
            if current != request.before_boot_id:
                return self._complete(request, current)
        if self.now().astimezone(UTC) >= request.expires_at.astimezone(UTC):
            self.journal.record(
                request.command_id,
                {"state": "BLOCKED", "request_sha256": request.request_sha256, "code": "FM08_DIRECTIVE_EXPIRED"},
            )
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_EXPIRED")
        current = self._healthy_boot_id()
        if current != request.before_boot_id:
            raise CallbackSupervisorBlocked("FM08_CONTROL_BOOT_CHANGED_BEFORE_EFFECT")
        # Persist-before-effect is the recovery boundary.
        self.journal.record(
            request.command_id,
            {
                "state": "INTENT",
                "request_sha256": request.request_sha256,
                "request_id": str(request.request_id),
                "before_boot_id": str(request.before_boot_id),
                "directive_receipt_sha256": request.directive_receipt_sha256,
            },
        )
        try:
            self.runner.restart_control_plane()
        except (OSError, subprocess.SubprocessError) as exc:
            raise CallbackSupervisorBlocked("FM08_CONTROL_RESTART_FAILED") from exc
        after = self._await_changed_boot(request.before_boot_id, request.expires_at)
        return self._complete(request, after)

    def _complete(self, request: HostRestartRequest, after: UUID) -> HostRestartReceipt:
        receipt = HostRestartReceipt(
            request_id=request.request_id,
            request_sha256=request.request_sha256,
            before_boot_id=request.before_boot_id,
            after_boot_id=after,
            healthy=True,
        )
        self.journal.record(
            request.command_id,
            {"state": "COMPLETE", "request_sha256": request.request_sha256, "receipt": receipt.payload()},
        )
        return receipt

    def _healthy_boot_id(self) -> UUID:
        return self.health.current_boot_id()

    def _await_changed_boot(self, before: UUID, expires_at: datetime) -> UUID:
        deadline = min(
            time.monotonic() + HEALTH_TIMEOUT_SECONDS,
            time.monotonic() + max(0, (expires_at - self.now()).total_seconds()),
        )
        while time.monotonic() < deadline:
            try:
                observed = self._healthy_boot_id()
            except CallbackSupervisorBlocked:
                self.sleep(0.5)
                continue
            if observed != before:
                return observed
            self.sleep(0.5)
        raise CallbackSupervisorBlocked("FM08_CONTROL_RESTART_HEALTH_TIMEOUT")


@dataclass(frozen=True, slots=True)
class UnixHostRestartClient:
    socket_path: Path
    key_path: Path
    connect_timeout_seconds: float = 3.0
    response_timeout_seconds: float = 130.0

    def __post_init__(self) -> None:
        if (
            not self.socket_path.is_absolute()
            or not self.key_path.is_absolute()
            or self.socket_path.is_symlink()
            or not 0 < self.connect_timeout_seconds <= 5
            or not 5 <= self.response_timeout_seconds <= 180
        ):
            raise ValueError("host restart client configuration is invalid")

    def assert_available(self) -> None:
        if not self.socket_path.exists() or not self.socket_path.is_socket():
            raise CallbackSupervisorBlocked("FM08_HOST_PERMISSION_UNAVAILABLE")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(self.connect_timeout_seconds)
                client.connect(str(self.socket_path))
        except OSError as exc:
            raise CallbackSupervisorBlocked("FM08_HOST_PERMISSION_UNAVAILABLE") from exc

    def restart(self, request: HostRestartRequest) -> HostRestartReceipt:
        key = _read_private_key(self.key_path)
        encoded = _signed_envelope(request, key)
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(self.connect_timeout_seconds)
                client.connect(str(self.socket_path))
                client.settimeout(self.response_timeout_seconds)
                client.sendall(encoded)
                raw = _receive_line(client, MAX_RESPONSE_BYTES)
        except (OSError, TimeoutError) as exc:
            # The control container is expected to lose this connection while it
            # is restarted.  The same deterministic request reconciles the host
            # journal when the new process resumes.
            raise CallbackSupervisorBlocked("FM08_RESTART_RESPONSE_PENDING") from exc
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise CallbackSupervisorBlocked("FM08_HOST_RESPONSE_INVALID") from exc
        if not isinstance(payload, dict):
            raise CallbackSupervisorBlocked("FM08_HOST_RESPONSE_INVALID")
        if payload.get("ok") is not True:
            code = payload.get("code")
            if isinstance(code, str) and re.fullmatch(r"FM08_[A-Z0-9_]{2,100}", code):
                raise CallbackSupervisorBlocked(code)
            raise CallbackSupervisorBlocked("FM08_HOST_RESTART_REJECTED")
        try:
            receipt = HostRestartReceipt.parse(cast(Mapping[str, Any], payload["receipt"]))
        except (KeyError, TypeError, ValueError) as exc:
            raise CallbackSupervisorBlocked("FM08_HOST_RESPONSE_INVALID") from exc
        if receipt.request_id != request.request_id or receipt.request_sha256 != request.request_sha256:
            raise CallbackSupervisorBlocked("FM08_HOST_RESPONSE_BINDING_MISMATCH")
        return receipt


def _receive_line(connection: socket.socket, limit: int) -> bytes:
    chunks: list[bytes] = []
    size = 0
    while True:
        chunk = connection.recv(min(4096, limit + 1 - size))
        if not chunk:
            break
        chunks.append(chunk)
        size += len(chunk)
        if size > limit:
            raise ValueError("Unix IPC message exceeds its bound")
        if b"\n" in chunk:
            break
    raw = b"".join(chunks)
    if not raw.endswith(b"\n") or raw.count(b"\n") != 1:
        raise ValueError("Unix IPC message framing is invalid")
    return raw


@dataclass(slots=True)
class UnixHostRestartServer:
    socket_path: Path
    key_path: Path
    allowed_uid: int
    controller: HostRestartController
    stop: threading.Event = field(default_factory=threading.Event)

    def serve_forever(self) -> None:
        key = _read_private_key(self.key_path)
        if not self.socket_path.is_absolute() or self.socket_path.is_symlink():
            raise ValueError("acceptance supervisor socket path is invalid")
        self.socket_path.parent.mkdir(parents=True, mode=0o700, exist_ok=True)
        os.chmod(self.socket_path.parent, 0o700)
        if self.socket_path.exists():
            if self.socket_path.is_socket():
                self.socket_path.unlink()
            else:
                raise ValueError("acceptance supervisor socket path is occupied")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
            server.bind(str(self.socket_path))
            os.chmod(self.socket_path, 0o600)
            server.listen(8)
            server.settimeout(0.5)
            while not self.stop.is_set():
                try:
                    connection, _address = server.accept()
                except TimeoutError:
                    continue
                with connection:
                    self._serve_one(connection, key)
        self.socket_path.unlink(missing_ok=True)

    def _serve_one(self, connection: socket.socket, key: bytes) -> None:
        try:
            credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
            peer_pid, peer_uid, _peer_gid = struct.unpack("3i", credentials)
            if peer_pid < 1 or peer_uid != self.allowed_uid:
                raise PermissionError("acceptance supervisor peer is not authorized")
            raw = _receive_line(connection, MAX_MESSAGE_BYTES)
            request = _parse_signed_envelope(raw, key)
            receipt = self.controller.execute(request)
            response: dict[str, Any] = {"ok": True, "receipt": receipt.payload()}
        except CallbackSupervisorBlocked as exc:
            response = {"ok": False, "code": exc.code}
        except (OSError, PermissionError, ValueError, json.JSONDecodeError):
            response = {"ok": False, "code": "FM08_HOST_REQUEST_REJECTED"}
        encoded = canonical_json_bytes(response) + b"\n"
        try:
            connection.sendall(encoded)
        except OSError:
            # Expected when the requesting control container is replaced.
            return


@dataclass(slots=True)
class LedgerCallbackLossSupervisor:
    """Concrete ``CallbackLossSupervisorPort`` over ledger metadata and host IPC."""

    control: CallbackLossControlPort
    host: UnixHostRestartClient
    health: ControlHealthProbe
    now: Callable[[], datetime] = field(default=lambda: datetime.now(UTC))
    monotonic: Callable[[], float] = time.monotonic
    sleep: Callable[[float], None] = time.sleep

    def suppress_next_task_callback(self, command: MasterAcceptanceCommand) -> Any:
        self._assert_fm08(command)
        # Missing socket/peer permission is detected before the directive can
        # suppress a callback.
        self.host.assert_available()
        directive = self.control.callback_loss_directive(command)
        if directive is None:
            armed_at = self.now().astimezone(UTC)
            directive = self.control.arm_callback_loss(
                command,
                allowed_event_types=ALLOWED_CALLBACK_EVENT_TYPES,
                max_callbacks=1,
                armed_at=armed_at,
                expires_at=armed_at + timedelta(seconds=DIRECTIVE_TTL_SECONDS),
                before_boot_id=self.health.current_boot_id(),
            )
        directive.assert_command(command)
        if not directive.acknowledged:
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_NOT_ACKNOWLEDGED")
        capture = self._await_capture(command, directive)
        # Avoid importing StoredCallbackRef at module load so this host module
        # remains independent of the production composition cycle.
        from my_data_hub.acceptance.master_production import StoredCallbackRef

        return StoredCallbackRef(event_id=capture.event_id, body_sha256=capture.body_sha256)

    def restart_control_process(self, command: MasterAcceptanceCommand) -> Any:
        self._assert_fm08(command)
        directive = self.control.callback_loss_directive(command)
        if directive is None:
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_MISSING")
        directive.assert_command(command)
        capture = self.control.captured_callback(command, directive)
        if capture is None:
            raise CallbackSupervisorBlocked("FM08_CALLBACK_NOT_CAPTURED")
        receipt = self.host.restart(HostRestartRequest.from_directive(directive))
        self.control.record_control_restart(
            command,
            directive,
            before_boot_id=receipt.before_boot_id,
            after_boot_id=receipt.after_boot_id,
        )
        from my_data_hub.acceptance.master_production import ControlRestartReceipt

        return ControlRestartReceipt(
            before_boot_id=receipt.before_boot_id,
            after_boot_id=receipt.after_boot_id,
        )

    def replay_stored_callback(
        self, command: MasterAcceptanceCommand, event_id: UUID
    ) -> Literal["accepted", "duplicate"]:
        self._assert_fm08(command)
        directive = self.control.callback_loss_directive(command)
        if directive is None:
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_MISSING")
        directive.assert_command(command)
        capture = self.control.captured_callback(command, directive)
        if capture is None or capture.event_id != event_id:
            raise CallbackSupervisorBlocked("FM08_CALLBACK_CAPTURE_MISMATCH")
        deadline = min(
            self.monotonic() + CALLBACK_REPLAY_SECONDS,
            self.monotonic() + max(0, (directive.expires_at - self.now()).total_seconds()),
        )
        while self.monotonic() < deadline:
            disposition = self.control.replay_disposition(command, directive, event_id)
            if disposition is not None:
                return disposition
            self.sleep(0.5)
        self.control.disarm_expired_callback_loss(command, directive)
        raise CallbackSupervisorBlocked("FM08_CALLBACK_REPLAY_TIMEOUT")

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool:
        return self.control.exact_service_active(binding)

    def _await_capture(
        self, command: MasterAcceptanceCommand, directive: CallbackLossDirective
    ) -> CallbackCapture:
        deadline = min(
            self.monotonic() + CALLBACK_CAPTURE_SECONDS,
            self.monotonic() + max(0, (directive.expires_at - self.now()).total_seconds()),
        )
        while self.monotonic() < deadline:
            capture = self.control.captured_callback(command, directive)
            if capture is not None:
                return capture
            self.sleep(0.5)
        self.control.disarm_expired_callback_loss(command, directive)
        raise CallbackSupervisorBlocked("FM08_CALLBACK_CAPTURE_TIMEOUT")

    @staticmethod
    def _assert_fm08(command: MasterAcceptanceCommand) -> None:
        if command.command_kind.value != "CALLBACK_LOSS_RECOVERY":
            from my_data_hub.acceptance.master_lifecycle import MasterLifecycleAcceptanceError

            raise MasterLifecycleAcceptanceError("callback supervisor received a non-FM08 command")


@dataclass(frozen=True, slots=True)
class ControlLedgerCallbackLossPort:
    """Exact migration-019/020 adapter used by production composition.

    The adapter supplies the operation ID from the already validated command;
    the runtime-control row itself remains a hash/identity-only journal.
    """

    ledger: Any

    def arm_callback_loss(
        self,
        command: MasterAcceptanceCommand,
        *,
        allowed_event_types: tuple[Literal["runtime.heartbeat"], ...],
        max_callbacks: Literal[1],
        armed_at: datetime,
        expires_at: datetime,
        before_boot_id: UUID,
    ) -> CallbackLossDirective:
        if (
            allowed_event_types != ALLOWED_CALLBACK_EVENT_TYPES
            or max_callbacks != 1
            or expires_at > armed_at + timedelta(seconds=DIRECTIVE_TTL_SECONDS)
        ):
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_BOUNDS_INVALID")
        binding = command.binding
        row = self.ledger.arm_master_acceptance_callback_loss(
            task_id=str(command.task_id),
            command_id=str(command.command_id),
            command_sha256=command.command_sha256,
            run_id=str(binding.run_id),
            attempt_id=str(binding.attempt_id),
            master_instance_id=str(binding.master_instance_id),
            epoch=binding.epoch,
            before_boot_id=str(before_boot_id),
        )
        directive = self._directive(command, row)
        if directive.armed_at < armed_at - timedelta(seconds=1) or directive.expires_at > expires_at:
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_CLOCK_BOUNDS_INVALID")
        return directive

    def callback_loss_directive(
        self, command: MasterAcceptanceCommand
    ) -> CallbackLossDirective | None:
        row = self.ledger.master_acceptance_runtime_control(str(command.task_id))
        if row is None:
            return None
        if row.get("callback_state") in {"ARMED", "CAPTURED"} and row.get("restart_to_id") is None:
            self.ledger.armed_master_acceptance_callback_loss(
                run_id=str(command.binding.run_id),
                attempt_id=str(command.binding.attempt_id),
                epoch=command.binding.epoch,
            )
            row = self.ledger.master_acceptance_runtime_control(str(command.task_id))
        if row is None or row.get("callback_state") == "DISARMED":
            return None
        return self._directive(command, row)

    def captured_callback(
        self, command: MasterAcceptanceCommand, directive: CallbackLossDirective
    ) -> CallbackCapture | None:
        directive.assert_command(command)
        row = self.ledger.master_acceptance_runtime_control(str(command.task_id))
        if row is None or row.get("callback_state") not in {"CAPTURED", "REPLAYED"}:
            return None
        if row.get("callback_count") != 1:
            raise CallbackSupervisorBlocked("FM08_CALLBACK_COUNT_INVALID")
        try:
            return CallbackCapture(
                event_id=UUID(str(row["callback_event_id"])),
                body_sha256=str(row["callback_body_sha256"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CallbackSupervisorBlocked("FM08_CALLBACK_CAPTURE_INVALID") from exc

    def record_control_restart(
        self,
        command: MasterAcceptanceCommand,
        directive: CallbackLossDirective,
        *,
        before_boot_id: UUID,
        after_boot_id: UUID,
    ) -> None:
        directive.assert_command(command)
        if before_boot_id != directive.before_boot_id or after_boot_id == before_boot_id:
            raise CallbackSupervisorBlocked("FM08_RESTART_RECEIPT_MISMATCH")
        row = self.ledger.record_master_acceptance_restart(
            task_id=str(command.task_id),
            restart_from_id=str(before_boot_id),
            restart_to_id=str(after_boot_id),
        )
        if row.get("restart_from_id") != str(before_boot_id) or row.get("restart_to_id") != str(after_boot_id):
            raise CallbackSupervisorBlocked("FM08_RESTART_JOURNAL_MISMATCH")

    def replay_disposition(
        self,
        command: MasterAcceptanceCommand,
        directive: CallbackLossDirective,
        event_id: UUID,
    ) -> Literal["accepted", "duplicate"] | None:
        capture = self.captured_callback(command, directive)
        if capture is None or capture.event_id != event_id:
            return None
        row = self.ledger.master_acceptance_runtime_control(str(command.task_id))
        if row is None or row.get("callback_state") != "REPLAYED":
            return None
        return "duplicate"

    def disarm_expired_callback_loss(
        self, command: MasterAcceptanceCommand, directive: CallbackLossDirective
    ) -> None:
        directive.assert_command(command)
        self.ledger.armed_master_acceptance_callback_loss(
            run_id=str(command.binding.run_id),
            attempt_id=str(command.binding.attempt_id),
            epoch=command.binding.epoch,
        )
        row = self.ledger.master_acceptance_runtime_control(str(command.task_id))
        if row is not None and row.get("callback_state") != "DISARMED":
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_NOT_DISARMED")

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool:
        operation = self.ledger.get_operation(str(binding.operation_id))
        service = self.ledger.resolve_service("postgres-master")
        return bool(
            operation is not None
            and operation.state == "ACTIVE"
            and service is not None
            and service.state == "ACTIVE"
            and service.run_id == str(binding.run_id)
            and service.attempt_id == str(binding.attempt_id)
            and service.service_instance_id == binding.service_instance_id
            and service.master_instance_id == str(binding.master_instance_id)
            and service.epoch == binding.epoch
            and self.ledger.current_epoch("postgres-master") == binding.epoch
        )

    @staticmethod
    def _directive(
        command: MasterAcceptanceCommand, row: Mapping[str, Any]
    ) -> CallbackLossDirective:
        exact = {
            "task_id": str(command.task_id),
            "command_id": str(command.command_id),
            "command_sha256": command.command_sha256,
            "scenario_id": "FM08",
            "run_id": str(command.binding.run_id),
            "attempt_id": str(command.binding.attempt_id),
            "master_instance_id": str(command.binding.master_instance_id),
            "epoch": command.binding.epoch,
        }
        if any(str(row.get(key)) != str(value) for key, value in exact.items()):
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_BINDING_MISMATCH")
        try:
            return CallbackLossDirective(
                task_id=command.task_id,
                command_id=command.command_id,
                command_sha256=command.command_sha256,
                operation_id=command.binding.operation_id,
                run_id=command.binding.run_id,
                attempt_id=command.binding.attempt_id,
                master_instance_id=command.binding.master_instance_id,
                epoch=command.binding.epoch,
                allowed_event_types=ALLOWED_CALLBACK_EVENT_TYPES,
                max_callbacks=1,
                armed_at=datetime.fromisoformat(str(row["armed_at"]).replace("Z", "+00:00")),
                expires_at=datetime.fromisoformat(str(row["expires_at"]).replace("Z", "+00:00")),
                before_boot_id=UUID(str(row["before_boot_id"])),
                directive_receipt_sha256=str(row["directive_receipt_sha256"]),
                acknowledged=True,
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise CallbackSupervisorBlocked("FM08_DIRECTIVE_RECEIPT_INVALID") from exc


def callback_loss_supervisor_from_environment(ledger: Any) -> LedgerCallbackLossSupervisor | None:
    """Build the private supervisor only when both installer-owned paths exist."""

    socket_value = os.getenv("MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_SOCKET", "").strip()
    key_value = os.getenv("MY_DATA_HUB_ACCEPTANCE_SUPERVISOR_KEY_FILE", "").strip()
    if not socket_value and not key_value:
        return None
    if not socket_value or not key_value:
        raise ValueError("acceptance supervisor socket and key must be configured together")
    socket_path = Path(socket_value)
    key_path = Path(key_value)
    return LedgerCallbackLossSupervisor(
        control=ControlLedgerCallbackLossPort(ledger),
        host=UnixHostRestartClient(socket_path=socket_path, key_path=key_path),
        health=HttpControlHealthProbe(),
    )


def _parse_paths(raw: str) -> tuple[Path, ...]:
    paths = tuple(Path(value) for value in raw.split(":"))
    if not paths or any(not str(path) for path in paths):
        raise ValueError("compose file list is invalid")
    return paths


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="fixed my-data-hub FM08 host supervisor")
    parser.add_argument("--socket", required=True, type=Path)
    parser.add_argument("--key-file", required=True, type=Path)
    parser.add_argument("--journal", required=True, type=Path)
    parser.add_argument("--docker", required=True, type=Path)
    parser.add_argument("--compose-env", required=True, type=Path)
    parser.add_argument("--project-directory", required=True, type=Path)
    parser.add_argument("--compose-files", required=True)
    parser.add_argument("--allowed-uid", required=True, type=int)
    values = parser.parse_args(argv)
    runner = ComposeControlPlaneRestartRunner(
        docker_path=values.docker,
        compose_env=values.compose_env,
        project_directory=values.project_directory,
        compose_files=_parse_paths(values.compose_files),
    )
    server = UnixHostRestartServer(
        socket_path=values.socket,
        key_path=values.key_file,
        allowed_uid=values.allowed_uid,
        controller=HostRestartController(
            journal=HostRestartJournal(values.journal),
            runner=runner,
            health=HttpControlHealthProbe(),
        ),
    )
    server.serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
