"""Operational entrypoint for the protected Kaggle PostgreSQL master notebook."""

from __future__ import annotations

import json
import os
import secrets
import stat
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Callable, Iterator
from contextlib import contextmanager, suppress
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from functools import partial
from importlib.resources import as_file, files
from itertools import pairwise
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

import psycopg
from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.acceptance.master_lifecycle import (
    MasterAcceptanceCommand,
    MasterAcceptanceRuntimeEffects,
    execute_master_acceptance_command,
)
from my_data_hub.acceptance.master_production import ProductionMasterAcceptanceEffectsFactory
from my_data_hub.checkpoints import load_and_verify, restore_physical_archive
from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.db.migrations import migrate
from my_data_hub.embeddings.master_stage import EmbeddingStageContext, execute_embedding_production_stage
from my_data_hub.embeddings.production import EmbeddingProductionRequest, EmbeddingProductionStageReceipt
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.runtime_sdk import (
    CANONICAL_RUNTIME_CALLBACK_URL,
    CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
    CHECKPOINT_TRANSITION_GUARD_SECONDS,
    KAGGLE_PROVIDER_TIMEOUT_SECONDS,
    MIN_CHECKPOINT_RESERVE_SECONDS,
    RuntimeClient,
    RuntimeEvent,
    RuntimeEventType,
)
from my_data_hub.workloads.bloggers.master_stage import (
    MAX_REQUEST_BYTES,
    BloggerImportStageReceipt,
    BloggerMigrationQuarantined,
    BloggerMigrationRequest,
    BloggerStageContext,
    execute_blogger_migration_stage,
)

from .bootstrap import BootstrapRequest, MasterBootstrap
from .contracts import BootSource, MasterIdentity, MasterPaths
from .credentials import CredentialProvisioner
from .database_gate import DatabaseGate
from .postgres import PostgresBinaries, PostgresConfig, PostgresSupervisor
from .tunnel import ReverseTunnelSpec, TunnelSupervisor

MASTER_TERMINAL_OUTPUT_NAME = "my-data-hub-master-terminal.json"
MASTER_TERMINAL_SCHEMA_VERSION = "my-data-hub-master-terminal.v1"
MASTER_TERMINAL_MAX_BYTES = 256 * 1024
_MASTER_TERMINAL_EVENT_TYPES = (
    RuntimeEventType.RUNTIME_DRAINING,
    RuntimeEventType.CHECKPOINT_STARTED,
    RuntimeEventType.CHECKPOINT_VERIFIED,
    RuntimeEventType.RUNTIME_TERMINAL,
)


class MasterTerminalCheckpoint(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    checkpoint_id: str = Field(pattern=r"^[0-9a-fA-F-]{36}$")
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    current_checkpoint_id: str = Field(pattern=r"^[0-9a-fA-F-]{36}$")

    @model_validator(mode="after")
    def checkpoint_is_current(self) -> MasterTerminalCheckpoint:
        UUID(self.checkpoint_id)
        UUID(self.current_checkpoint_id)
        if self.checkpoint_id != self.current_checkpoint_id:
            raise ValueError("terminal checkpoint must be the exact current checkpoint")
        return self


class MasterTerminalRecord(BaseModel):
    """Bounded secret-free recovery evidence written after durable promotion."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-master-terminal.v1"] = MASTER_TERMINAL_SCHEMA_VERSION
    run_id: str = Field(min_length=1, max_length=200)
    attempt_id: str = Field(min_length=1, max_length=200)
    service_instance_id: str = Field(min_length=1, max_length=200)
    master_instance_id: str = Field(pattern=r"^[0-9a-fA-F-]{36}$")
    source_identity: str = Field(min_length=1, max_length=500)
    source_version: str = Field(min_length=1, max_length=200)
    epoch: int = Field(ge=1)
    status: Literal["succeeded"] = "succeeded"
    checkpoint: MasterTerminalCheckpoint
    events: tuple[dict[str, Any], ...] = Field(min_length=4, max_length=4)
    blogger_import_receipt: BloggerImportStageReceipt | None = None

    @model_validator(mode="after")
    def exact_event_chain(self) -> MasterTerminalRecord:
        UUID(self.master_instance_id)
        parsed = tuple(RuntimeEvent.model_validate(body) for body in self.events)
        if tuple(event.event_type for event in parsed) != _MASTER_TERMINAL_EVENT_TYPES:
            raise ValueError("master terminal events differ from the exact lifecycle chain")
        if any(
            (
                event.run_id,
                event.attempt_id,
                event.service_instance_id,
                event.source_identity,
                event.source_version,
                event.epoch,
            )
            != (
                self.run_id,
                self.attempt_id,
                self.service_instance_id,
                self.source_identity,
                self.source_version,
                self.epoch,
            )
            for event in parsed
        ):
            raise ValueError("master terminal event identity differs from its envelope")
        sequences = tuple(event.local_sequence for event in parsed)
        if any(left >= right for left, right in pairwise(sequences)):
            raise ValueError("master terminal events are not in strictly increasing sequence order")
        if (
            (parsed[0].phase, parsed[0].status, parsed[0].data) != ("draining", "closed", {})
            or (parsed[1].phase, parsed[1].status, parsed[1].data) != ("checkpointing", "started", {})
            or any(event.artifact_refs or event.metrics for event in parsed)
        ):
            raise ValueError("master terminal lifecycle bodies contain unexpected payload data")
        checkpoint = self.checkpoint.model_dump(mode="json")
        if (parsed[2].phase, parsed[2].status, parsed[2].data) != (
            "checkpointing",
            "verified",
            checkpoint,
        ):
            raise ValueError("checkpoint.verified does not bind the exact terminal checkpoint")
        if (parsed[3].phase, parsed[3].status, parsed[3].data) != (
            "stopped",
            "succeeded",
            {"checkpoint_id": self.checkpoint.current_checkpoint_id},
        ):
            raise ValueError("runtime.terminal does not bind the exact current checkpoint")
        return self


@dataclass(frozen=True, slots=True)
class NotebookMasterConfig:
    master_instance_id: UUID
    run_id: str
    attempt_id: str
    service_instance_id: str
    epoch: int
    boot_source: BootSource
    checkpoint_directory: Path | None
    lease_seconds: int
    postgres_bin: Path
    postgres_port: int
    tunnel_gateway_host: str
    tunnel_gateway_port: int
    tunnel_gateway_user: str
    tunnel_remote_port: int
    maximum_runtime_seconds: int
    checkpoint_reserve_seconds: int
    source_identity: str
    source_version: str

    @classmethod
    def load(cls, path: Path) -> NotebookMasterConfig:
        if path.is_symlink() or not path.is_file() or path.stat().st_size > 64 * 1024:
            raise ValueError("master config must be a bounded regular file")
        raw = json.loads(path.read_text(encoding="utf-8"))
        required = {
            "master_instance_id",
            "run_id",
            "attempt_id",
            "service_instance_id",
            "epoch",
            "boot_source",
            "checkpoint_directory",
            "lease_seconds",
            "postgres_bin",
            "postgres_port",
            "tunnel_gateway_host",
            "tunnel_gateway_port",
            "tunnel_gateway_user",
            "tunnel_remote_port",
            "maximum_runtime_seconds",
            "checkpoint_reserve_seconds",
            "source_identity",
            "source_version",
        }
        if not isinstance(raw, dict) or set(raw) != required:
            raise ValueError("master config fields differ from the exact contract")
        source = BootSource(str(raw["boot_source"]))
        checkpoint = Path(str(raw["checkpoint_directory"])) if raw["checkpoint_directory"] else None
        config = cls(
            master_instance_id=UUID(str(raw["master_instance_id"])),
            run_id=str(raw["run_id"]),
            attempt_id=str(raw["attempt_id"]),
            service_instance_id=str(raw["service_instance_id"]),
            epoch=int(raw["epoch"]),
            boot_source=source,
            checkpoint_directory=checkpoint,
            lease_seconds=int(raw["lease_seconds"]),
            postgres_bin=Path(str(raw["postgres_bin"])),
            postgres_port=int(raw["postgres_port"]),
            tunnel_gateway_host=str(raw["tunnel_gateway_host"]),
            tunnel_gateway_port=int(raw["tunnel_gateway_port"]),
            tunnel_gateway_user=str(raw["tunnel_gateway_user"]),
            tunnel_remote_port=int(raw["tunnel_remote_port"]),
            maximum_runtime_seconds=int(raw["maximum_runtime_seconds"]),
            checkpoint_reserve_seconds=int(raw["checkpoint_reserve_seconds"]),
            source_identity=str(raw["source_identity"]),
            source_version=str(raw["source_version"]),
        )
        if not 60 <= config.lease_seconds <= 600:
            raise ValueError("master lease must be 60..600 seconds")
        if config.maximum_runtime_seconds < 1_800:
            raise ValueError("maximum runtime must be at least 1800 seconds")
        if config.checkpoint_reserve_seconds != MIN_CHECKPOINT_RESERVE_SECONDS:
            raise ValueError("checkpoint reserve must be exactly 10800 seconds")
        if config.checkpoint_reserve_seconds + CHECKPOINT_TRANSITION_GUARD_SECONDS >= config.maximum_runtime_seconds:
            raise ValueError("checkpoint reserve and transition guard must be below maximum runtime")
        if config.maximum_runtime_seconds + config.checkpoint_reserve_seconds > KAGGLE_PROVIDER_TIMEOUT_SECONDS:
            raise ValueError("process runtime plus exit reserve exceeds the declared Kaggle provider timeout")
        if (source is BootSource.EMPTY_BASELINE) != (checkpoint is None):
            raise ValueError("checkpoint source/config mismatch")
        return config


class RuntimeCheckpointCoordinator(Protocol):
    """Generate, upload, exact-readback, restore-verify, and promote one checkpoint."""

    def create_and_publish(
        self,
        *,
        database_url: str,
        package_directory: Path,
        identity: MasterIdentity,
    ) -> PublishReceipt: ...


class CheckpointRetryStage(StrEnum):
    PUBLICATION = "publication"
    TERMINAL_DELIVERY = "terminal_delivery"


class CheckpointShutdownError(RuntimeError):
    """The drained master was intentionally left running because durability failed."""

    def __init__(
        self,
        message: str,
        *,
        retry_stage: CheckpointRetryStage,
        receipt: PublishReceipt | None = None,
    ) -> None:
        super().__init__(message)
        self.retry_stage = retry_stage
        self.receipt = receipt


class CheckpointAdmissionError(RuntimeError):
    """A publication attempt was not started because its full budget was absent."""


class BloggerReceiptDeliveryError(RuntimeError):
    """Exact metadata delivery was not acknowledged after bounded retries."""


class EmbeddingReceiptDeliveryError(RuntimeError):
    """Committed embedding imports were not acknowledged by control metadata."""


class EmbeddingStageExecutionError(RuntimeError):
    """Gate K failed before an acknowledged complete stage receipt."""


class CallbackLeaseClosingError(TimeoutError):
    """The control callback outage closed the write lease as designed."""


class _EmbeddingLeaseMaintainer:
    """Keep the ACTIVE epoch renewed while Gate K waits on remote providers.

    The worker owns a separate PostgreSQL connection per transition; it never
    shares the owner connection concurrently with a canonical import.  A failed
    control heartbeat is allowed only while the already-acknowledged lease has
    more than the safety margin remaining.  At the margin it fences immediately
    and exposes the same failure to the foreground before any import can start.
    """

    _SAFETY_MARGIN = timedelta(seconds=15)

    def __init__(
        self,
        *,
        identity: MasterIdentity,
        lease_seconds: int,
        initial_lease_until: datetime,
        runtime: Any,
        tunnel: Any,
        connection_factory: Callable[[], Any],
        now: Callable[[], datetime] = lambda: datetime.now(UTC),
        interval_seconds: float | None = None,
    ) -> None:
        if lease_seconds < 60:
            raise ValueError("embedding lease maintenance requires a lease of at least 60 seconds")
        interval = interval_seconds if interval_seconds is not None else min(30.0, lease_seconds / 3)
        if interval <= 0 or interval >= lease_seconds - self._SAFETY_MARGIN.total_seconds():
            raise ValueError("embedding lease maintenance interval exceeds the safe lease window")
        self.identity = identity
        self.lease_seconds = lease_seconds
        self.runtime = runtime
        self.tunnel = tunnel
        self.connection_factory = connection_factory
        self.now = now
        self.interval_seconds = interval
        self._lease_until = initial_lease_until
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._error: BaseException | None = None

    @property
    def lease_until(self) -> datetime:
        with self._lock:
            return self._lease_until

    def check(self) -> None:
        with self._lock:
            error = self._error
            lease_until = self._lease_until
        if error is not None:
            raise error
        if self.now() + self._SAFETY_MARGIN >= lease_until:
            failure = CallbackLeaseClosingError("callback unavailable; write lease is closing")
            self._fail(failure, "embedding_lease_safety_margin")
            raise failure

    def current_lease_until(self) -> datetime:
        return self.lease_until

    def maintain_once(self) -> None:
        self.check()
        observed = self.now()
        try:
            self.tunnel.poll(now=observed)
            proposed = observed + timedelta(seconds=self.lease_seconds)
            delivery = self.runtime.emit(
                RuntimeEventType.RUNTIME_HEARTBEAT,
                phase="active",
                status="healthy",
                data={"lease_until": proposed.isoformat().replace("+00:00", "Z")},
            )
            if delivery.status == "delivered":
                with self.connection_factory() as connection:
                    DatabaseGate(connection).renew(self.identity, proposed)
                with self._lock:
                    self._lease_until = proposed
                return
            if self.now() + self._SAFETY_MARGIN >= self.lease_until:
                failure = CallbackLeaseClosingError("callback unavailable; write lease is closing")
                self._fail(failure, "embedding_control_heartbeat_lost")
                raise failure
        except CallbackLeaseClosingError:
            raise
        except BaseException as exc:
            failure = CallbackLeaseClosingError(
                "embedding lease maintenance lost control or database reachability"
            )
            self._fail(failure, "embedding_lease_maintenance_failed")
            raise failure from exc

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("embedding lease maintainer is single-use")
        # Prove control + gate reachability synchronously before provider work.
        self.maintain_once()
        self._thread = threading.Thread(
            target=self._run,
            name="mdh-embedding-lease-maintainer",
            daemon=True,
        )
        self._thread.start()

    def stop(self, timeout_seconds: float = 60.0) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None:
            thread.join(timeout_seconds)
            if thread.is_alive():
                failure = CallbackLeaseClosingError("embedding lease maintainer did not stop")
                self._fail(failure, "embedding_lease_maintainer_stuck")
        self.check()

    def _run(self) -> None:
        try:
            while not self._stop.wait(self._next_delay_seconds()):
                self.maintain_once()
        except BaseException as exc:
            if self._error is None:
                self._fail(exc, "embedding_lease_maintenance_failed")

    def _next_delay_seconds(self) -> float:
        safe_remaining = max(
            0.0,
            (self.lease_until - self.now() - self._SAFETY_MARGIN).total_seconds(),
        )
        return min(self.interval_seconds, safe_remaining)

    def _fail(self, error: BaseException, reason: str) -> None:
        with self._lock:
            if self._error is not None:
                return
            self._error = error
        self._stop.set()
        with suppress(Exception), self.connection_factory() as connection:
            DatabaseGate(connection).fence(self.identity, reason)


def _runtime_deadlines(config: NotebookMasterConfig, process_started_at: float) -> tuple[float, float]:
    """Return fixed active/session deadlines anchored before all boot work."""

    if process_started_at < 0:
        raise ValueError("process monotonic start must be non-negative")
    session_deadline = process_started_at + config.maximum_runtime_seconds
    return (
        session_deadline - config.checkpoint_reserve_seconds - CHECKPOINT_TRANSITION_GUARD_SECONDS,
        session_deadline,
    )


def _require_active_window(*, active_deadline: float, monotonic: Any = time.monotonic) -> None:
    """Refuse readiness/write activation once boot consumed the ACTIVE window."""

    if monotonic() >= active_deadline:
        raise RuntimeError("boot consumed the ACTIVE window reserved before checkpoint admission")


def _emit_service_ready(
    *,
    runtime: Any,
    ready: Any,
    active_deadline: float,
    monotonic: Any = time.monotonic,
) -> None:
    """Check the fixed process deadline before readiness can authorize writes."""

    _require_active_window(active_deadline=active_deadline, monotonic=monotonic)
    receipt = runtime.emit(
        RuntimeEventType.SERVICE_READY,
        phase="registering",
        status="ready",
        data=ready.event_payload(),
    )
    if receipt.status != "delivered":
        raise RuntimeError("service.ready was not acknowledged by the control plane")


def _write_master_terminal(
    *,
    output_path: Path,
    runtime: Any,
    identity: MasterIdentity,
    receipt: PublishReceipt,
    blogger_import_receipt: BloggerImportStageReceipt | None = None,
) -> None:
    """Atomically persist the exact spooled terminal lifecycle for recovery."""

    if output_path.is_symlink() or not output_path.parent.is_dir() or output_path.parent.is_symlink():
        raise RuntimeError("master terminal output requires a real existing directory")
    event_bodies = runtime.durable_event_bodies(_MASTER_TERMINAL_EVENT_TYPES)
    record = MasterTerminalRecord(
        run_id=runtime.run_id,
        attempt_id=runtime.attempt_id,
        service_instance_id=runtime.service_instance_id,
        master_instance_id=str(identity.master_instance_id),
        source_identity=runtime.source_identity,
        source_version=runtime.source_version,
        epoch=identity.epoch,
        checkpoint=MasterTerminalCheckpoint(
            checkpoint_id=receipt.checkpoint_id,
            manifest_sha256=receipt.manifest_sha256,
            current_checkpoint_id=receipt.current_checkpoint_id,
        ),
        events=event_bodies,
        blogger_import_receipt=blogger_import_receipt,
    )
    if record.run_id != identity.run_id or runtime.epoch != identity.epoch:
        raise RuntimeError("master terminal runtime identity differs from the fenced master")
    encoded = json.dumps(
        record.model_dump(mode="json", exclude_none=True),
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    if len(encoded) > MASTER_TERMINAL_MAX_BYTES:
        raise RuntimeError("master terminal output exceeds 256 KiB")
    lowered = encoded.lower()
    if any(marker in lowered for marker in (b"postgresql://", b"postgres://", b"password=", b"sslkey=")):
        raise RuntimeError("master terminal output contains a forbidden database credential marker")
    temporary = output_path.with_name(f".{output_path.name}.{secrets.token_hex(8)}.tmp")
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            descriptor = -1
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, output_path)
        os.chmod(output_path, 0o600)
        directory_descriptor = os.open(output_path.parent, os.O_RDONLY)
        try:
            os.fsync(directory_descriptor)
        finally:
            os.close(directory_descriptor)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        with suppress(OSError):
            temporary.unlink(missing_ok=True)


def _master_terminal_supports_output_recovery(
    output_path: Path,
    receipt: PublishReceipt,
) -> bool:
    """Revalidate the exact durable artifact before allowing a clean provider exit."""

    try:
        if output_path.is_symlink() or not output_path.is_file():
            return False
        metadata = output_path.stat()
        if stat.S_IMODE(metadata.st_mode) != 0o600 or metadata.st_size > MASTER_TERMINAL_MAX_BYTES:
            return False
        encoded = output_path.read_bytes()
        record = MasterTerminalRecord.model_validate_json(encoded)
        canonical = json.dumps(
            record.model_dump(mode="json", exclude_none=True),
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        return (
            encoded == canonical
            and record.checkpoint.checkpoint_id == receipt.checkpoint_id
            and record.checkpoint.manifest_sha256 == receipt.manifest_sha256
            and record.checkpoint.current_checkpoint_id == receipt.current_checkpoint_id
        )
    except (OSError, ValueError):
        return False


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required Kaggle secret/runtime value is absent: {name}")
    return value


def _local_url(paths: MasterPaths, port: int) -> str:
    from urllib.parse import quote

    return f"postgresql://postgres@/postgres?host={quote(str(paths.socket), safe='')}&port={port}"


def _activation_url(callback_url: str, run_id: str, attempt_id: str) -> str:
    suffix = "/internal/runtime/events"
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    return f"{callback_url.removesuffix(suffix)}/internal/runtime/activation/{run_id}/{attempt_id}"


def _tunnel_certificate_url(callback_url: str, run_id: str, attempt_id: str) -> str:
    suffix = "/internal/runtime/events"
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    return f"{callback_url.removesuffix(suffix)}/internal/runtime/tunnel-certificates/{run_id}/{attempt_id}"


def _tunnel_lease_url(callback_url: str, run_id: str, attempt_id: str) -> str:
    suffix = "/internal/runtime/events"
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    return f"{callback_url.removesuffix(suffix)}/internal/runtime/tunnel-leases/{run_id}/{attempt_id}"


def _renew_registration_tunnel_lease(
    *,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    lease_until: datetime,
    timeout_seconds: int = 90,
) -> None:
    payload = canonical_json_bytes(
        {
            "master_instance_id": str(config.master_instance_id),
            "epoch": config.epoch,
            "lease_until": lease_until.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
    )
    deadline = time.monotonic() + timeout_seconds
    while True:
        request = urllib.request.Request(
            _tunnel_lease_url(callback_url, config.run_id, config.attempt_id),
            data=payload,
            headers={"Authorization": f"Bearer {run_secret}", "Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=min(20, timeout_seconds)) as response:
                body = json.loads(response.read(8 * 1024))
            break
        except urllib.error.HTTPError as exc:
            if exc.code not in {409, 503} or time.monotonic() + 2 >= deadline:
                raise
        except (OSError, TimeoutError, ValueError):
            if time.monotonic() + 2 >= deadline:
                raise
        time.sleep(2)
    try:
        observed = datetime.fromisoformat(str(body.get("lease_until", "")).replace("Z", "+00:00")).astimezone(UTC)
    except (AttributeError, TypeError, ValueError) as exc:
        raise RuntimeError("tunnel lease renewal response is malformed") from exc
    if body.get("renewed") is not True or set(body) != {"renewed", "lease_until"} or observed < lease_until:
        raise RuntimeError("tunnel lease renewal response differs from the exact contract")


@dataclass(frozen=True, slots=True)
class EphemeralTunnelIdentity:
    root: Path
    private_key: Path
    certificate: Path

    def cleanup(self) -> None:
        for path in (self.certificate, self.private_key.with_suffix(".pub"), self.private_key):
            with suppress(OSError):
                path.unlink()
        with suppress(OSError):
            self.root.rmdir()


def _issue_ephemeral_tunnel_identity(
    *,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    valid_before: datetime,
) -> EphemeralTunnelIdentity:
    """Create a task-local SSH key and exchange only its public half.

    The private key lives under the notebook process temporary directory rather
    than ``/kaggle/working`` so Kaggle output collection cannot publish it.  It
    is never sent to the control plane or stored in a Dataset/checkpoint.
    """

    root = Path(tempfile.mkdtemp(prefix="my-data-hub-tunnel-", dir=os.getenv("TMPDIR", "/tmp")))
    os.chmod(root, 0o700)
    private_key = root / "identity"
    certificate = root / "identity-cert.pub"
    identity = EphemeralTunnelIdentity(root, private_key, certificate)
    try:
        completed = subprocess.run(
            ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(private_key)],
            check=False,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            timeout=15,
        )
        if completed.returncode != 0:
            raise RuntimeError("ephemeral tunnel key generation failed")
        os.chmod(private_key, 0o600)
        public_key_path = private_key.with_suffix(".pub")
        if public_key_path.is_symlink() or not public_key_path.is_file():
            raise RuntimeError("ephemeral tunnel public key is absent")
        public_key = public_key_path.read_text(encoding="ascii").strip()
        # Strip the optional ssh-keygen comment; the control endpoint accepts
        # exactly one algorithm/blob pair and never receives private material.
        public_parts = public_key.split()
        if len(public_parts) < 2:
            raise RuntimeError("ephemeral tunnel public key is malformed")
        public_key = " ".join(public_parts[:2])
        payload = canonical_json_bytes(
            {
                "master_instance_id": str(config.master_instance_id),
                "epoch": config.epoch,
                "public_key": public_key,
                "valid_before": valid_before.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            }
        )
        request = urllib.request.Request(
            _tunnel_certificate_url(callback_url, config.run_id, config.attempt_id),
            data=payload,
            headers={"Authorization": f"Bearer {run_secret}", "Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=20) as response:
            body = json.loads(response.read(32 * 1024))
        required = {"certificate", "serial", "principal", "valid_before", "listen_host", "listen_port"}
        if not isinstance(body, dict) or set(body) != required:
            raise RuntimeError("tunnel certificate response differs from the exact contract")
        observed_expiry = datetime.fromisoformat(str(body["valid_before"]).replace("Z", "+00:00")).astimezone(UTC)
        if (
            observed_expiry != valid_before
            or body["listen_host"] != "127.0.0.1"
            or int(body["listen_port"]) != config.tunnel_remote_port
            or not str(body["principal"]).startswith("mdh-")
            or int(body["serial"]) < 1
        ):
            raise RuntimeError("tunnel certificate authority returned a mismatched epoch binding")
        encoded = str(body["certificate"]).encode("ascii")
        if not encoded.startswith(b"ssh-ed25519-cert-v01@openssh.com ") or len(encoded) > 16 * 1024:
            raise RuntimeError("tunnel certificate is malformed or oversized")
        descriptor = os.open(certificate, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(encoded.rstrip(b"\n") + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        public_key_path.unlink()
        return identity
    except Exception:
        identity.cleanup()
        raise


def _blogger_migration_url(callback_url: str, run_id: str, attempt_id: str, suffix: str = "") -> str:
    events_suffix = "/internal/runtime/events"
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    base = callback_url.removesuffix(events_suffix)
    return f"{base}/internal/runtime/blogger-migration/{run_id}/{attempt_id}{suffix}"


def _embedding_production_url(callback_url: str, run_id: str, attempt_id: str, suffix: str = "") -> str:
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    base = callback_url.removesuffix("/internal/runtime/events")
    return f"{base}/internal/runtime/embedding-production/{run_id}/{attempt_id}{suffix}"


def _master_acceptance_url(callback_url: str, run_id: str, attempt_id: str, suffix: str = "") -> str:
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    base = callback_url.removesuffix("/internal/runtime/events")
    return f"{base}/internal/runtime/master-acceptance/{run_id}/{attempt_id}{suffix}"


def _runtime_metadata_headers(config: NotebookMasterConfig, run_secret: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {run_secret}",
        "Content-Type": "application/json",
        "X-MDH-Master-Instance-ID": str(config.master_instance_id),
        "X-MDH-Epoch": str(config.epoch),
    }


def _claim_blogger_migration(
    *, config: NotebookMasterConfig, callback_url: str, run_secret: str
) -> BloggerMigrationRequest | None:
    request = urllib.request.Request(
        _blogger_migration_url(callback_url, config.run_id, config.attempt_id),
        headers=_runtime_metadata_headers(config, run_secret),
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(MAX_REQUEST_BYTES + 4097)
    except Exception:
        # Claim observation is optional while no request exists, but an actual
        # claimed request is durable and will be returned on the next heartbeat.
        return None
    if len(raw) > MAX_REQUEST_BYTES + 4096:
        raise RuntimeError("blogger claim response exceeds 256 KiB")
    body = json.loads(raw)
    if body.get("available") is not True:
        return None
    migration = BloggerMigrationRequest.model_validate(body.get("request"))
    if body.get("request_sha256") != migration.request_sha256:
        raise RuntimeError("blogger work request hash differs from its exact body")
    return migration


def _claim_embedding_production(
    *, config: NotebookMasterConfig, callback_url: str, run_secret: str
) -> EmbeddingProductionRequest | None:
    request = urllib.request.Request(
        _embedding_production_url(callback_url, config.run_id, config.attempt_id),
        headers=_runtime_metadata_headers(config, run_secret),
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(64 * 1024)
    except urllib.error.HTTPError as exc:
        if exc.code in {502, 503, 504}:
            return None
        raise
    except OSError:
        return None
    body = json.loads(raw)
    if not isinstance(body, dict):
        raise RuntimeError("embedding claim response is not an object")
    if body.get("available") is not True:
        return None
    production = EmbeddingProductionRequest.model_validate(body.get("request"))
    if body.get("request_sha256") != production.request_sha256:
        raise RuntimeError("embedding work request hash differs from its exact body")
    return production


def _claim_master_acceptance(
    *, config: NotebookMasterConfig, callback_url: str, run_secret: str
) -> MasterAcceptanceCommand | None:
    """Poll only the fixed command envelope; no arbitrary task body is accepted."""

    request = urllib.request.Request(
        _master_acceptance_url(callback_url, config.run_id, config.attempt_id),
        headers=_runtime_metadata_headers(config, run_secret),
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            raw = response.read(64 * 1024 + 1)
    except urllib.error.HTTPError as exc:
        if exc.code in {502, 503, 504}:
            return None
        raise
    except OSError:
        return None
    if len(raw) > 64 * 1024:
        raise RuntimeError("master acceptance command exceeds 64 KiB")
    body = json.loads(raw)
    if not isinstance(body, dict) or body.get("available") is not True:
        return None
    return MasterAcceptanceCommand.model_validate(body.get("command"))


def _post_master_acceptance_receipt(
    *,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    receipt: Any,
    attempts: int = 3,
    sleep: Any = time.sleep,
) -> None:
    encoded = canonical_json_bytes(receipt.model_dump(mode="json"))
    if len(encoded) > 64 * 1024 or not 1 <= attempts <= 5:
        raise RuntimeError("master acceptance receipt transport bounds are invalid")
    for attempt in range(attempts):
        request = urllib.request.Request(
            _master_acceptance_url(callback_url, config.run_id, config.attempt_id, "/receipt"),
            data=encoded,
            headers=_runtime_metadata_headers(config, run_secret),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = json.loads(response.read(16 * 1024))
            if body.get("accepted") is True and body.get("receipt_sha256") == receipt.receipt_sha256:
                return
        except Exception:
            pass
        if attempt + 1 < attempts:
            sleep(min(2**attempt, 2))
    raise RuntimeError("control did not acknowledge exact master acceptance receipt")


def _post_blogger_runtime_receipt(
    *,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    suffix: str,
    payload: dict[str, Any],
    attempts: int = 3,
    sleep: Any = time.sleep,
) -> None:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > MAX_REQUEST_BYTES:
        raise RuntimeError("blogger runtime metadata receipt exceeds 256 KiB")
    if not 1 <= attempts <= 5:
        raise ValueError("blogger receipt delivery attempts must be 1..5")
    for attempt in range(attempts):
        request = urllib.request.Request(
            _blogger_migration_url(callback_url, config.run_id, config.attempt_id, suffix),
            data=encoded,
            headers=_runtime_metadata_headers(config, run_secret),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = json.loads(response.read(16 * 1024))
            if body.get("accepted") is True:
                return
        except Exception:
            pass
        if attempt + 1 < attempts:
            sleep(min(2**attempt, 2))
    raise BloggerReceiptDeliveryError("control plane did not acknowledge exact blogger runtime metadata")


def _post_embedding_runtime_receipt(
    *,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    receipt: EmbeddingProductionStageReceipt,
    attempts: int = 3,
) -> None:
    encoded = canonical_json_bytes(receipt.model_dump(mode="json"))
    if len(encoded) > 256 * 1024:
        raise RuntimeError("embedding runtime receipt exceeds 256 KiB")
    for attempt in range(attempts):
        request = urllib.request.Request(
            _embedding_production_url(
                callback_url, config.run_id, config.attempt_id, "/stage-receipt"
            ),
            data=encoded,
            headers=_runtime_metadata_headers(config, run_secret),
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = json.loads(response.read(16 * 1024))
            if body.get("accepted") is True and body.get("receipt_sha256") == receipt.receipt_sha256:
                return
        except Exception:
            pass
        if attempt + 1 < attempts:
            time.sleep(min(2**attempt, 2))
    raise EmbeddingReceiptDeliveryError("control plane did not acknowledge exact embedding receipt")


def _credential_registration_url(callback_url: str, run_id: str, attempt_id: str) -> str:
    suffix = "/internal/runtime/events"
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise ValueError("callback URL does not match the owner-pinned HTTPS runtime endpoint")
    return f"{callback_url.removesuffix(suffix)}/internal/runtime/session-credentials/{run_id}/{attempt_id}"


def _register_reader_credential(
    *,
    connection: Any,
    gate: DatabaseGate,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    expires_at: datetime,
    now: datetime,
) -> tuple[str, datetime]:
    """Create and hand off one epoch-bound reader without logging its secret."""

    principals, expiry = _register_session_credentials(
        connection=connection,
        gate=gate,
        config=config,
        callback_url=callback_url,
        run_secret=run_secret,
        roles=("reader",),
        expires_at=expires_at,
        now=now,
    )
    return principals[0], expiry


def _register_session_credentials(
    *,
    connection: Any,
    gate: DatabaseGate,
    config: NotebookMasterConfig,
    callback_url: str,
    run_secret: str,
    roles: tuple[Literal["reader", "operator"], ...],
    expires_at: datetime,
    now: datetime,
) -> tuple[tuple[str, ...], datetime]:
    """Issue only the roles explicitly authorized by activation, in one envelope."""

    from urllib.parse import quote, urlencode

    if expires_at <= now or expires_at - now > timedelta(minutes=5):
        raise ValueError("session credential expiry is outside the broker bound")
    if roles not in {("reader",), ("reader", "operator")}:
        raise ValueError("activation credential roles differ from the bounded contract")
    identity = MasterIdentity(config.master_instance_id, config.run_id, config.epoch)
    provisioner = CredentialProvisioner(connection, gate)
    query = urlencode(
        {
            "sslmode": "verify-ca",
            # Fixed path in the remote MCP container, not a Kaggle filesystem path.
            "sslrootcert": "/state/master-tls/ca.pem",
            "connect_timeout": "5",
        }
    )
    principals: list[str] = []
    credentials: list[dict[str, str]] = []
    try:
        for role in roles:
            credential_id = UUID(bytes=secrets.token_bytes(16), version=4)
            principal = f"mdh_e{config.epoch}_{role}_{credential_id.hex[:8]}"
            password = secrets.token_urlsafe(36)
            provisioner.create(
                principal=principal,
                password=password,
                group="mdh_mcp_reader" if role == "reader" else "mdh_mcp_editor",
                identity=identity,
                credential_id=credential_id,
                expires_at=expires_at,
                now=now,
            )
            principals.append(principal)
            database_url = (
                f"postgresql://{quote(principal, safe='')}:{quote(password, safe='')}@"
                f"127.0.0.1:{config.tunnel_remote_port}/postgres?{query}"
            )
            credentials.append(
                {
                    "role": role,
                    "database_url": database_url,
                    "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
                }
            )
    except Exception:
        cleanup_failed = False
        for principal in reversed(principals):
            try:
                provisioner.drop(principal)
            except Exception:
                cleanup_failed = True
        if cleanup_failed:
            with suppress(Exception):
                gate.fence(identity, "credential_provisioning_cleanup_failed")
        raise
    body = json.dumps(
        {
            "master_instance_id": str(config.master_instance_id),
            "epoch": config.epoch,
            "credentials": credentials,
        },
        separators=(",", ":"),
    ).encode()
    request = urllib.request.Request(
        _credential_registration_url(callback_url, config.run_id, config.attempt_id),
        data=body,
        headers={"Authorization": f"Bearer {run_secret}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            receipt = json.loads(response.read(16 * 1024))
        if receipt.get("registered") != len(roles):
            raise RuntimeError("control plane did not accept all requested session credentials")
    except Exception:
        # No usable login may survive a failed broker handoff.
        try:
            for principal in reversed(principals):
                provisioner.drop(principal)
        except Exception:
            gate.fence(identity, "credential_handoff_failed")
        raise
    return tuple(principals), expires_at


@contextmanager
def _fresh_canonical_committer_connection(
    *,
    owner_connection: Any,
    gate: DatabaseGate,
    identity: MasterIdentity,
    database_url: str,
    lease_until: Callable[[], datetime],
    now: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> Iterator[Any]:
    """Yield one fresh non-superuser committer and always make it unusable.

    The credential expires inside the currently acknowledged lease.  The
    importer opens its transaction with ``session_user`` fixed to this LOGIN;
    ``SET LOCAL ROLE mdh_canonical_committer`` grants only the bounded write
    surface, while migration 0011's deferred trigger rejects a commit after a
    lease expiry, revocation, or epoch fence.
    """

    observed = now()
    expiry = min(observed + timedelta(minutes=4), lease_until())
    if expiry <= observed + timedelta(seconds=15):
        raise CallbackLeaseClosingError("ACTIVE lease is too short for a canonical committer")
    credential_id = UUID(bytes=secrets.token_bytes(16), version=4)
    principal = f"mdh_e{identity.epoch}_embed_{credential_id.hex[:8]}"
    password = secrets.token_urlsafe(36)
    provisioner = CredentialProvisioner(owner_connection, gate)
    provisioner.create(
        principal=principal,
        password=password,
        group="mdh_canonical_committer",
        identity=identity,
        credential_id=credential_id,
        expires_at=expiry,
        now=observed,
    )
    connection: Any | None = None
    cleanup_error: BaseException | None = None
    try:
        connection = psycopg.connect(
            database_url,
            user=principal,
            password=password,
            autocommit=True,
        )
        with connection.cursor() as cursor:
            authenticated = cursor.execute(
                "SELECT rolname,rolsuper FROM pg_roles WHERE rolname=session_user"
            ).fetchone()
        if authenticated != (principal, False):
            raise RuntimeError("canonical committer LOGIN is absent or unexpectedly privileged")
        yield connection
    finally:
        if connection is not None:
            with suppress(Exception):
                connection.close()
        cleanup_errors: list[BaseException] = []
        try:
            gate.revoke_credential(credential_id, "embedding_committer_complete")
        except BaseException as exc:
            cleanup_errors.append(exc)
        try:
            provisioner.drop(principal)
        except BaseException as exc:
            cleanup_errors.append(exc)
        if cleanup_errors:
            cleanup_error = cleanup_errors[0]
            with suppress(Exception):
                gate.fence(identity, "embedding_committer_cleanup_failed")
        if cleanup_error is not None:
            raise RuntimeError("embedding committer cleanup failed; epoch was fenced") from cleanup_error


def _cleanup_epoch_principals(
    *,
    connection: Any,
    gate: DatabaseGate,
    identity: MasterIdentity,
    principals: set[str],
) -> None:
    """Remove handed-off LOGINs before physical checkpoint publication."""

    provisioner = CredentialProvisioner(connection, gate)
    try:
        for principal in sorted(principals):
            provisioner.drop(principal)
    except BaseException as exc:
        with suppress(Exception):
            gate.fence(identity, "epoch_credential_cleanup_failed")
        raise RuntimeError("epoch credential cleanup failed; epoch was fenced") from exc


def _wait_for_activation(
    url: str,
    token: str,
    identity: MasterIdentity,
    timeout_seconds: int = 90,
) -> tuple[Literal["reader", "operator"], ...]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                body = json.loads(response.read(16 * 1024))
        except (OSError, TimeoutError, ValueError):
            time.sleep(2)
            continue
        if (
            body.get("active") is True
            and body.get("master_instance_id") == str(identity.master_instance_id)
            and int(body.get("epoch", 0)) == identity.epoch
        ):
            raw_roles = body.get("credential_roles", ["reader"])
            if not isinstance(raw_roles, list) or any(not isinstance(role, str) for role in raw_roles):
                raise RuntimeError("activation credential roles are malformed")
            roles = tuple(raw_roles)
            if roles not in {("reader",), ("reader", "operator")}:
                raise RuntimeError("activation credential roles exceed the bounded contract")
            return roles  # type: ignore[return-value]
        time.sleep(2)
    raise TimeoutError("control plane did not activate the exact master epoch")


def _bootstrap_owner(database_url: str) -> None:
    with psycopg.connect(database_url, autocommit=True) as connection, connection.cursor() as cursor:
        cursor.execute(
            "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='mdh_owner') THEN "
            "CREATE ROLE mdh_owner NOLOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOINHERIT "
            "NOREPLICATION NOBYPASSRLS; END IF; END $$"
        )
        cursor.execute("GRANT mdh_owner TO postgres")
        cursor.execute("GRANT CREATE,TEMPORARY ON DATABASE postgres TO mdh_owner")


def run_master(
    config: NotebookMasterConfig,
    *,
    checkpoint_coordinator: RuntimeCheckpointCoordinator | None = None,
    acceptance_effects: MasterAcceptanceRuntimeEffects | None = None,
    acceptance_effects_factory: ProductionMasterAcceptanceEffectsFactory | None = None,
    process_started_at: float | None = None,
) -> int:
    # Direct callers get the same entry-bound budget as ``main``.  ``main``
    # passes a timestamp captured before config/head resolution so all Python
    # boot work is charged to the provider lifetime.
    if process_started_at is None:
        process_started_at = time.monotonic()
    active_deadline, session_deadline = _runtime_deadlines(config, process_started_at)
    working = Path(os.environ.get("KAGGLE_WORKING_DIR", "/kaggle/working"))
    paths = MasterPaths.under(working)
    identity = MasterIdentity(config.master_instance_id, config.run_id, config.epoch)
    now = datetime.now(UTC)
    lease_until = now + timedelta(seconds=config.lease_seconds)
    callback_url = _required("MY_DATA_HUB_CALLBACK_URL")
    if callback_url != CANONICAL_RUNTIME_CALLBACK_URL:
        raise RuntimeError("runtime callback URL does not match the owner-pinned audience")
    run_secret = _required("MY_DATA_HUB_RUN_SECRET")
    runtime = RuntimeClient(
        callback_url=callback_url,
        run_secret=run_secret,
        run_id=config.run_id,
        attempt_id=config.attempt_id,
        service_instance_id=config.service_instance_id,
        source_identity=config.source_identity,
        source_version=config.source_version,
        epoch=config.epoch,
        spool_path=paths.runtime_events,
        heartbeat_interval_seconds=30,
    )
    tls_certificate = Path(_required("MY_DATA_HUB_POSTGRES_TLS_CERT"))
    tls_key = Path(_required("MY_DATA_HUB_POSTGRES_TLS_KEY"))
    supervisor = PostgresSupervisor(
        paths=paths,
        binaries=PostgresBinaries.discover(config.postgres_bin),
        config=PostgresConfig(config.postgres_port, tls_certificate, tls_key),
    )
    initial_registration_lease = now + timedelta(minutes=4)
    _renew_registration_tunnel_lease(
        config=config,
        callback_url=callback_url,
        run_secret=run_secret,
        lease_until=initial_registration_lease,
    )
    class LazyCertifiedTunnel:
        def __init__(self) -> None:
            self.supervisor: TunnelSupervisor | None = None

        def start(self, *, now: datetime) -> None:
            del now  # Bootstrap time may be stale after a long restore.
            observed = datetime.now(UTC)
            valid_before = min(lease_until, observed + timedelta(minutes=4))
            if valid_before <= observed + timedelta(seconds=15):
                raise RuntimeError("REGISTERING lease is too short for an epoch-bound tunnel certificate")
            tunnel_identity = _issue_ephemeral_tunnel_identity(
                config=config,
                callback_url=callback_url,
                run_secret=run_secret,
                valid_before=valid_before,
            )
            self.supervisor = TunnelSupervisor(
                ReverseTunnelSpec(
                    gateway_host=config.tunnel_gateway_host,
                    gateway_port=config.tunnel_gateway_port,
                    gateway_user=config.tunnel_gateway_user,
                    remote_bind_host="127.0.0.1",
                    remote_bind_port=config.tunnel_remote_port,
                    local_postgres_port=config.postgres_port,
                    identity_file=tunnel_identity.private_key,
                    certificate_file=tunnel_identity.certificate,
                    known_hosts_file=Path(_required("MY_DATA_HUB_TUNNEL_KNOWN_HOSTS")),
                    expires_at=observed
                    + timedelta(seconds=max(1.0, session_deadline - time.monotonic())),
                    delete_identity_on_stop=True,
                )
            )
            try:
                self.supervisor.start(now=observed)
            except Exception:
                self.supervisor.stop()
                raise

        def poll(self, *, now: datetime) -> None:
            if self.supervisor is None:
                raise RuntimeError("certified tunnel is not started")
            self.supervisor.poll(now=now)

        def stop(self) -> None:
            if self.supervisor is not None:
                self.supervisor.stop()
                self.supervisor = None

    tunnel = LazyCertifiedTunnel()
    registration_lease_stop = threading.Event()
    registration_lease_errors: list[BaseException] = []

    def keep_registration_tunnel_lease() -> None:
        while not registration_lease_stop.wait(30):
            try:
                _renew_registration_tunnel_lease(
                    config=config,
                    callback_url=callback_url,
                    run_secret=run_secret,
                    lease_until=datetime.now(UTC) + timedelta(minutes=4),
                )
            except BaseException as exc:
                registration_lease_errors.append(exc)
                return

    registration_lease_thread = threading.Thread(
        target=keep_registration_tunnel_lease,
        name="mdh-tunnel-registration-lease",
        daemon=True,
    )
    registration_lease_thread.start()
    database_url = _local_url(paths, config.postgres_port)
    gate_connection: Any | None = None

    def restore(checkpoint: Path) -> None:
        manifest = load_and_verify(checkpoint / "checkpoint-manifest.json", checkpoint)
        restore_physical_archive(checkpoint, manifest, paths.pgdata)
        supervisor.write_configuration()

    def apply_migrations() -> None:
        _bootstrap_owner(database_url)
        migration_resource = files("my_data_hub.master_runtime").joinpath("sql/migrations")
        with as_file(migration_resource) as directory:
            migrate(database_url, directory)

    def reconcile_roles() -> None:
        role_resource = files("my_data_hub.master_runtime").joinpath("sql/admin/role_contract.sql")
        role_sql = role_resource.read_text(encoding="utf-8")
        with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
            cursor.execute(role_sql)
            connection.commit()

    def verify_database() -> tuple[int, int, str]:
        import hashlib

        with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
            schema_version = int(cursor.execute("SELECT max(version) FROM hub_meta.schema_migration").fetchone()[0])
            canonical_revision = int(
                cursor.execute("SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true").fetchone()[0]
            )
            extensions = {
                str(row[0])
                for row in cursor.execute(
                    "SELECT extname FROM pg_extension WHERE extname IN ('vector','pgcrypto','pg_trgm','citext')"
                ).fetchall()
            }
            if extensions != {"vector", "pgcrypto", "pg_trgm", "citext"}:
                raise RuntimeError("required PostgreSQL extensions are absent")
        return schema_version, canonical_revision, hashlib.sha256(tls_certificate.read_bytes()).hexdigest()

    def announce(ready) -> None:  # type: ignore[no-untyped-def]
        _emit_service_ready(
            runtime=runtime,
            ready=ready,
            active_deadline=active_deadline,
        )

    def endpoint() -> str:
        return f"tunnel://127.0.0.1:{config.tunnel_remote_port}"

    try:
        gate_connection = psycopg.connect(database_url)
    except Exception:
        gate_connection = None

    # Gate connection can exist only after PostgreSQL starts.  Use a lazy seam.
    class LazyGate:
        def _gate(self) -> DatabaseGate:
            nonlocal gate_connection
            if gate_connection is None or gate_connection.closed:
                gate_connection = psycopg.connect(database_url)
            return DatabaseGate(gate_connection)

        def acquire(self, target: MasterIdentity, until: datetime) -> None:
            self._gate().acquire(target, until)

        def fence(self, target: MasterIdentity, reason: str) -> None:
            self._gate().fence(target, reason)

    bootstrap = MasterBootstrap(
        postgres=supervisor,
        gate=LazyGate(),
        tunnel=tunnel,
        restore=restore,
        migrate=apply_migrations,
        reconcile_roles=reconcile_roles,
        verify_database=verify_database,
        announce_ready=announce,
        endpoint=endpoint,
    )
    try:
        ready = bootstrap.run(
            BootstrapRequest(
                identity=identity,
                source=config.boot_source,
                checkpoint_directory=config.checkpoint_directory,
                lease_until=lease_until,
                now=now,
            )
        )
    except Exception:
        # Bootstrap may fail before it starts the SSH subprocess, but the
        # task-local private key must still be destroyed.
        tunnel.stop()
        raise
    finally:
        registration_lease_stop.set()
        registration_lease_thread.join(timeout=5)
    if registration_lease_errors:
        tunnel.stop()
        supervisor.stop(immediate=True)
        raise RuntimeError("tunnel registration lease renewal failed") from registration_lease_errors[0]
    try:
        credential_roles = _wait_for_activation(
            _activation_url(callback_url, config.run_id, config.attempt_id), run_secret, identity
        )
        assert gate_connection is not None
        gate = DatabaseGate(gate_connection)
        _require_active_window(active_deadline=active_deadline)
        gate.activate(identity)
    except Exception:
        if gate_connection is not None:
            with suppress(Exception):
                DatabaseGate(gate_connection).fence(identity, "activation_window_exhausted")
            with suppress(Exception):
                gate_connection.close()
        with suppress(Exception):
            tunnel.stop()
        with suppress(Exception):
            supervisor.stop(immediate=True)
        raise
    credential_now = datetime.now(UTC)
    reader_expires_at = min(credential_now + timedelta(minutes=4), ready.lease_until)
    if reader_expires_at <= credential_now + timedelta(seconds=15):
        raise RuntimeError("ACTIVE lease is too short to issue a bounded reader credential")
    try:
        session_principals, reader_expires_at = _register_session_credentials(
            connection=gate_connection,
            gate=gate,
            config=config,
            callback_url=callback_url,
            run_secret=run_secret,
            roles=credential_roles,
            expires_at=reader_expires_at,
            now=credential_now,
        )
    except Exception:
        gate.fence(identity, "reader_credential_registration_failed")
        tunnel.stop()
        supervisor.stop(immediate=True)
        gate_connection.close()
        raise
    issued_principals = set(session_principals)
    if acceptance_effects is not None and acceptance_effects_factory is not None:
        raise ValueError("provide either acceptance effects or its production factory, not both")
    if acceptance_effects_factory is not None:
        # Build only after the exact epoch is ACTIVE and its local PostgreSQL
        # connection exists.  This prevents a preboot request from receiving a
        # generic connection/clock/fault payload and keeps the ordinary launch
        # path free of test-only effects.
        acceptance_effects = acceptance_effects_factory.build(
            connection=gate_connection,
            boot_source=config.boot_source.value,
        )
    current_lease = ready.lease_until
    blogger_receipt: BloggerImportStageReceipt | None = None
    embedding_receipt: EmbeddingProductionStageReceipt | None = None
    active_error: BaseException | None = None
    try:
        while True:
            remaining_active = active_deadline - time.monotonic()
            if remaining_active <= 0:
                break
            if acceptance_effects is not None:
                acceptance_command = _claim_master_acceptance(
                    config=config, callback_url=callback_url, run_secret=run_secret
                )
                if acceptance_command is not None:
                    acceptance_receipt = execute_master_acceptance_command(
                        acceptance_command, acceptance_effects
                    )
                    _post_master_acceptance_receipt(
                        config=config,
                        callback_url=callback_url,
                        run_secret=run_secret,
                        receipt=acceptance_receipt,
                    )
                    break
            migration_request = _claim_blogger_migration(
                config=config, callback_url=callback_url, run_secret=run_secret
            )
            if migration_request is not None:
                if remaining_active < 300:
                    raise RuntimeError("blogger stage was not admitted without its bounded active-time allocation")
                try:
                    blogger_receipt = execute_blogger_migration_stage(
                        BloggerStageContext(
                            identity=identity,
                            request=migration_request,
                            local_database_url=database_url,
                            lease_until=current_lease,
                            attempt_id=config.attempt_id,
                        ),
                        owner_connection=gate_connection,
                    )
                except BloggerMigrationQuarantined as exc:
                    with suppress(Exception):
                        _post_blogger_runtime_receipt(
                            config=config,
                            callback_url=callback_url,
                            run_secret=run_secret,
                            suffix="/failed",
                            payload={
                                "request_id": str(migration_request.request_id),
                                "failure_code": type(exc).__name__,
                                "quarantine_receipt": exc.receipt.model_dump(mode="json"),
                                "receipt_sha256": exc.receipt.receipt_sha256,
                            },
                        )
                    raise
                except Exception as exc:
                    with suppress(Exception):
                        _post_blogger_runtime_receipt(
                            config=config,
                            callback_url=callback_url,
                            run_secret=run_secret,
                            suffix="/failed",
                            payload={
                                "request_id": str(migration_request.request_id),
                                "failure_code": type(exc).__name__[:100],
                            },
                        )
                    raise
                _post_blogger_runtime_receipt(
                    config=config,
                    callback_url=callback_url,
                    run_secret=run_secret,
                    suffix="/import-receipt",
                    payload=blogger_receipt.model_dump(mode="json"),
                )
                break
            embedding_request = _claim_embedding_production(
                config=config, callback_url=callback_url, run_secret=run_secret
            )
            if embedding_request is not None:
                if checkpoint_coordinator is None:
                    raise RuntimeError("embedding stage requires the production checkpoint/provider coordinator")
                provider = getattr(checkpoint_coordinator.coordinator.provider, "adapter", None)
                if provider is None:
                    raise RuntimeError("embedding stage cannot resolve the single Kaggle adapter")
                maintainer = _EmbeddingLeaseMaintainer(
                    identity=identity,
                    lease_seconds=config.lease_seconds,
                    initial_lease_until=current_lease,
                    runtime=runtime,
                    tunnel=tunnel,
                    connection_factory=lambda: psycopg.connect(database_url),
                )
                canonical_connection_factory = partial(
                    _fresh_canonical_committer_connection,
                    owner_connection=gate_connection,
                    gate=gate,
                    identity=identity,
                    database_url=database_url,
                    lease_until=maintainer.current_lease_until,
                )
                try:
                    maintainer.start()
                    try:
                        embedding_receipt = execute_embedding_production_stage(
                            EmbeddingStageContext(
                                identity=identity,
                                operation_id=UUID(_required("MY_DATA_HUB_OPERATION_ID")),
                                request=embedding_request,
                                database_url=database_url,
                                wheel_path=Path(_required("MY_DATA_HUB_WHEEL_PATH")),
                                wheel_sha256=_required("MY_DATA_HUB_WHEEL_SHA256"),
                                provider_owner=provider.identity.username,
                                remaining_seconds=remaining_active,
                            ),
                            connection=gate_connection,
                            adapter=provider,
                            canonical_connection_factory=canonical_connection_factory,
                            lease_guard=maintainer.check,
                        )
                    except Exception as exc:
                        raise EmbeddingStageExecutionError("embedding stage did not complete") from exc
                    _post_embedding_runtime_receipt(
                        config=config,
                        callback_url=callback_url,
                        run_secret=run_secret,
                        receipt=embedding_receipt,
                    )
                finally:
                    maintainer.stop()
                    current_lease = maintainer.lease_until
                break
            time.sleep(min(30.0, remaining_active))
            if time.monotonic() >= active_deadline:
                break
            tunnel.poll(now=datetime.now(UTC))
            proposed = datetime.now(UTC) + timedelta(seconds=config.lease_seconds)
            delivery = runtime.emit(
                RuntimeEventType.RUNTIME_HEARTBEAT,
                phase="active",
                status="healthy",
                data={"lease_until": proposed.isoformat().replace("+00:00", "Z")},
            )
            if delivery.status == "delivered":
                gate.renew(identity, proposed)
                current_lease = proposed
                observed_now = datetime.now(UTC)
                if reader_expires_at <= observed_now + timedelta(seconds=75):
                    next_expiry = min(observed_now + timedelta(minutes=4), proposed)
                    if next_expiry <= observed_now + timedelta(seconds=15):
                        raise TimeoutError("renewed lease is too short for a broker credential")
                    session_principals, reader_expires_at = _register_session_credentials(
                        connection=gate_connection,
                        gate=gate,
                        config=config,
                        callback_url=callback_url,
                        run_secret=run_secret,
                        roles=credential_roles,
                        expires_at=next_expiry,
                        now=observed_now,
                    )
                    issued_principals.update(session_principals)
            if datetime.now(UTC) + timedelta(seconds=15) >= current_lease:
                raise CallbackLeaseClosingError("callback unavailable; write lease is closing")
    except BaseException as exc:
        active_error = exc

    try:
        _cleanup_epoch_principals(
            connection=gate_connection,
            gate=gate,
            identity=identity,
            principals=issued_principals,
        )
    except BaseException:
        tunnel.stop()
        supervisor.stop(immediate=True)
        gate_connection.close()
        raise

    if isinstance(active_error, BloggerReceiptDeliveryError):
        # Never promote a checkpoint containing the committed workload until
        # the metadata ledger has acknowledged the exact import receipt.  A
        # persistent callback outage therefore loses only this ephemeral
        # attempt; the previous verified HEAD remains authoritative.
        gate.fence(identity, "blogger_import_receipt_unacknowledged")
        tunnel.stop()
        supervisor.stop(immediate=True)
        gate_connection.close()
        raise active_error

    if isinstance(active_error, (EmbeddingReceiptDeliveryError, EmbeddingStageExecutionError)):
        gate.fence(identity, "embedding_stage_receipt_unacknowledged")
        tunnel.stop()
        supervisor.stop(immediate=True)
        gate_connection.close()
        raise active_error

    terminal_output_path = paths.working / MASTER_TERMINAL_OUTPUT_NAME
    checkpoint_receipt = _checkpoint_until_deadline(
        gate=gate,
        runtime=runtime,
        tunnel=tunnel,
        supervisor=supervisor,
        coordinator=checkpoint_coordinator,
        database_url=database_url,
        package_directory=paths.checkpoints,
        identity=identity,
        deadline=session_deadline,
        terminal_output_path=terminal_output_path,
        blogger_import_receipt=blogger_receipt,
    )
    if blogger_receipt is not None:
        checkpoint_payload = {
            "request_id": str(blogger_receipt.request_id),
            "checkpoint_id": checkpoint_receipt.checkpoint_id,
            "manifest_sha256": checkpoint_receipt.manifest_sha256,
            "current_checkpoint_id": checkpoint_receipt.current_checkpoint_id,
            "canonical_revision": blogger_receipt.canonical_revision,
        }
        with suppress(Exception):
            # Promotion is already durable.  A callback outage must not turn a
            # recoverable COMPLETE provider run into FAILED; the command can
            # reconcile the exact operation/checkpoint from ledger metadata.
            _post_blogger_runtime_receipt(
                config=config,
                callback_url=callback_url,
                run_secret=run_secret,
                suffix="/checkpoint-receipt",
                payload=checkpoint_payload,
            )

    if active_error is not None:
        callback_closure_recovered = isinstance(
            active_error, CallbackLeaseClosingError
        ) and _master_terminal_supports_output_recovery(terminal_output_path, checkpoint_receipt)
        if not callback_closure_recovered:
            if gate_connection is not None:
                gate_connection.close()
            raise active_error
    if gate_connection is not None:
        gate_connection.close()
    return 0


def _checkpoint_before_stop(
    *,
    gate: Any,
    runtime: Any,
    tunnel: Any,
    supervisor: Any,
    coordinator: RuntimeCheckpointCoordinator | None,
    database_url: str,
    package_directory: Path,
    identity: MasterIdentity,
    terminal_output_path: Path | None = None,
    blogger_import_receipt: BloggerImportStageReceipt | None = None,
    retry: bool = False,
) -> PublishReceipt:
    """Close writes and stop processes only after durable checkpoint success.

    On any candidate failure the gate remains draining, the previous durable
    HEAD remains authoritative, and no runtime-terminal/stop effect is emitted.
    """

    if not retry:
        gate.drain(identity, "runtime_checkpoint")
        runtime.emit(RuntimeEventType.RUNTIME_DRAINING, phase="draining", status="closed")
    runtime.emit(RuntimeEventType.CHECKPOINT_STARTED, phase="checkpointing", status="started")
    try:
        if coordinator is None:
            raise RuntimeError("verified checkpoint coordinator is not configured")
        receipt = coordinator.create_and_publish(
            database_url=database_url,
            package_directory=package_directory,
            identity=identity,
        )
    except Exception as exc:
        runtime.emit(
            RuntimeEventType.CHECKPOINT_FAILED,
            phase="checkpointing",
            status="checkpoint_failed",
            data={"failure_code": type(exc).__name__},
        )
        raise CheckpointShutdownError(
            "checkpoint failed; old HEAD is preserved and the drained master requires retry",
            retry_stage=CheckpointRetryStage.PUBLICATION,
        ) from exc

    verified = runtime.emit(
        RuntimeEventType.CHECKPOINT_VERIFIED,
        phase="checkpointing",
        status="verified",
        data={
            "checkpoint_id": receipt.checkpoint_id,
            "manifest_sha256": receipt.manifest_sha256,
            "current_checkpoint_id": receipt.current_checkpoint_id,
        },
    )
    terminal = runtime.emit(
        RuntimeEventType.RUNTIME_TERMINAL,
        phase="stopped",
        status="succeeded",
        data={"checkpoint_id": receipt.current_checkpoint_id},
    )
    if terminal_output_path is not None:
        _write_master_terminal(
            output_path=terminal_output_path,
            runtime=runtime,
            identity=identity,
            receipt=receipt,
            blogger_import_receipt=blogger_import_receipt,
        )
    if verified.status != "delivered" or terminal.status != "delivered":
        # ``emit(terminal)`` already auto-replays older callbacks.  Check the
        # durable spool rather than trusting the earlier receipt snapshot.
        if runtime.flush_pending(max_events=100):
            tunnel.stop()
            supervisor.stop(immediate=False)
            return receipt
        raise CheckpointShutdownError(
            "checkpoint is durable but terminal state was not acknowledged; master remains drained",
            retry_stage=CheckpointRetryStage.TERMINAL_DELIVERY,
            receipt=receipt,
        )
    tunnel.stop()
    supervisor.stop(immediate=False)
    return receipt


def _checkpoint_until_deadline(
    *,
    gate: Any,
    runtime: Any,
    tunnel: Any,
    supervisor: Any,
    coordinator: RuntimeCheckpointCoordinator | None,
    database_url: str,
    package_directory: Path,
    identity: MasterIdentity,
    deadline: float,
    terminal_output_path: Path | None = None,
    blogger_import_receipt: BloggerImportStageReceipt | None = None,
    publication_attempt_seconds: float = CHECKPOINT_ATTEMPT_BUDGET_SECONDS,
    retry_seconds: float = 60.0,
    monotonic: Any = time.monotonic,
    sleep: Any = time.sleep,
) -> PublishReceipt:
    """Admit only publication attempts whose entire declared allocation remains.

    This is a conservative start/resume gate, not a claim that an already
    running third-party provider call can be interrupted at ``deadline``.
    """

    if publication_attempt_seconds <= 0:
        raise ValueError("checkpoint publication attempt budget must be positive")

    retry = False
    terminal_receipt: PublishReceipt | None = None
    while True:
        try:
            if terminal_receipt is not None:
                if runtime.flush_pending(max_events=100):
                    tunnel.stop()
                    supervisor.stop(immediate=False)
                    return terminal_receipt
                raise CheckpointShutdownError(
                    "terminal callbacks remain queued; master remains drained",
                    retry_stage=CheckpointRetryStage.TERMINAL_DELIVERY,
                    receipt=terminal_receipt,
                )
            remaining = deadline - monotonic()
            if remaining < publication_attempt_seconds:
                raise CheckpointAdmissionError(
                    "checkpoint publication was not started because its full attempt budget is absent"
                )
            return _checkpoint_before_stop(
                gate=gate,
                runtime=runtime,
                tunnel=tunnel,
                supervisor=supervisor,
                coordinator=coordinator,
                database_url=database_url,
                package_directory=package_directory,
                identity=identity,
                terminal_output_path=terminal_output_path,
                blogger_import_receipt=blogger_import_receipt,
                retry=retry,
            )
        except CheckpointShutdownError as exc:
            if exc.retry_stage is CheckpointRetryStage.TERMINAL_DELIVERY:
                if exc.receipt is None:
                    raise AssertionError("terminal delivery retry lost its durable checkpoint receipt") from exc
                terminal_receipt = exc.receipt
            elif terminal_receipt is not None:
                raise AssertionError("checkpoint publication cannot restart after durable promotion") from exc
            remaining = deadline - monotonic()
            if exc.retry_stage is CheckpointRetryStage.PUBLICATION:
                required_after_sleep = publication_attempt_seconds + retry_seconds
            else:
                required_after_sleep = retry_seconds + 30
            if remaining < required_after_sleep:
                if (
                    exc.retry_stage is CheckpointRetryStage.TERMINAL_DELIVERY
                    and exc.receipt is not None
                    and terminal_output_path is not None
                    and _master_terminal_supports_output_recovery(terminal_output_path, exc.receipt)
                ):
                    # Callback delivery remains unacknowledged and the local
                    # spool remains queued.  A clean provider exit makes the
                    # exact private output artifact available to the recovery
                    # projector; it does not convert those callbacks to ACKed.
                    tunnel.stop()
                    supervisor.stop(immediate=False)
                    return exc.receipt
                raise
            sleep(retry_seconds)
            retry = True


def main() -> int:
    process_started_at = time.monotonic()
    config = NotebookMasterConfig.load(Path(_required("MY_DATA_HUB_MASTER_CONFIG")))
    identity = MasterIdentity(config.master_instance_id, config.run_id, config.epoch)
    from my_data_hub.checkpoints.kaggle_runtime import (
        build_runtime_checkpoint_coordinator_from_environment,
    )

    coordinator = build_runtime_checkpoint_coordinator_from_environment(
        identity=identity,
        attempt_id=UUID(config.attempt_id),
        postgres_bin=config.postgres_bin,
    )
    paths = MasterPaths.under(Path(os.environ.get("KAGGLE_WORKING_DIR", "/kaggle/working")))
    boot_checkpoint = coordinator.resolve_boot_checkpoint(paths.checkpoints / "verified-head-boot")
    config = replace(
        config,
        boot_source=(BootSource.VERIFIED_CHECKPOINT if boot_checkpoint is not None else BootSource.EMPTY_BASELINE),
        checkpoint_directory=boot_checkpoint,
    )
    return run_master(
        config,
        checkpoint_coordinator=coordinator,
        process_started_at=process_started_at,
    )
