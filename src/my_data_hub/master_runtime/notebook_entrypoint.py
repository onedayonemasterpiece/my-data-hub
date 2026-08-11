"""Operational entrypoint for the protected Kaggle PostgreSQL master notebook."""

from __future__ import annotations

import json
import os
import secrets
import time
import urllib.request
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any, Protocol
from uuid import UUID

import psycopg

from my_data_hub.checkpoints import load_and_verify, restore_physical_archive
from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.db.migrations import migrate
from my_data_hub.runtime_sdk import RuntimeClient, RuntimeEventType

from .bootstrap import BootstrapRequest, MasterBootstrap
from .contracts import BootSource, MasterIdentity, MasterPaths
from .credentials import CredentialProvisioner
from .database_gate import DatabaseGate
from .postgres import PostgresBinaries, PostgresConfig, PostgresSupervisor
from .tunnel import ReverseTunnelSpec, TunnelSupervisor


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
        if not 1_800 <= config.maximum_runtime_seconds <= 43_200:
            raise ValueError("maximum runtime must be 1800..43200 seconds")
        if (
            not 900 <= config.checkpoint_reserve_seconds <= 10_800
            or config.checkpoint_reserve_seconds >= config.maximum_runtime_seconds
        ):
            raise ValueError("checkpoint reserve must be 900..10800 seconds and below maximum runtime")
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


class CheckpointShutdownError(RuntimeError):
    """The drained master was intentionally left running because durability failed."""


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
    if not callback_url.startswith("https://") or not callback_url.endswith(suffix):
        raise ValueError("callback URL does not match the exact HTTPS runtime endpoint")
    return f"{callback_url.removesuffix(suffix)}/internal/runtime/activation/{run_id}/{attempt_id}"


def _credential_registration_url(callback_url: str, run_id: str, attempt_id: str) -> str:
    suffix = "/internal/runtime/events"
    if not callback_url.startswith("https://") or not callback_url.endswith(suffix):
        raise ValueError("callback URL does not match the exact HTTPS runtime endpoint")
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

    from urllib.parse import quote, urlencode

    if expires_at <= now or expires_at - now > timedelta(minutes=5):
        raise ValueError("reader credential expiry is outside the broker bound")
    credential_id = UUID(bytes=secrets.token_bytes(16), version=4)
    principal = f"mdh_e{config.epoch}_reader_{credential_id.hex[:8]}"
    password = secrets.token_urlsafe(36)
    identity = MasterIdentity(config.master_instance_id, config.run_id, config.epoch)
    CredentialProvisioner(connection, gate).create(
        principal=principal,
        password=password,
        group="mdh_mcp_reader",
        identity=identity,
        credential_id=credential_id,
        expires_at=expires_at,
        now=now,
    )
    query = urlencode(
        {
            "sslmode": "verify-ca",
            # Fixed path in the remote MCP container, not a Kaggle filesystem path.
            "sslrootcert": "/state/master-tls/ca.pem",
            "connect_timeout": "5",
        }
    )
    database_url = (
        f"postgresql://{quote(principal, safe='')}:{quote(password, safe='')}@"
        f"127.0.0.1:{config.tunnel_remote_port}/postgres?{query}"
    )
    body = json.dumps(
        {
            "master_instance_id": str(config.master_instance_id),
            "epoch": config.epoch,
            "credentials": [
                {
                    "role": "reader",
                    "database_url": database_url,
                    "expires_at": expires_at.isoformat().replace("+00:00", "Z"),
                }
            ],
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
        if receipt.get("registered") != 1:
            raise RuntimeError("control plane did not accept the reader credential")
    except Exception:
        # No usable login may survive a failed broker handoff.
        try:
            CredentialProvisioner(connection, gate).drop(principal)
        except Exception:
            gate.fence(identity, "credential_handoff_failed")
        raise
    return principal, expires_at


def _wait_for_activation(url: str, token: str, identity: MasterIdentity, timeout_seconds: int = 90) -> None:
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
            return
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
) -> int:
    working = Path(os.environ.get("KAGGLE_WORKING_DIR", "/kaggle/working"))
    paths = MasterPaths.under(working)
    identity = MasterIdentity(config.master_instance_id, config.run_id, config.epoch)
    now = datetime.now(UTC)
    lease_until = now + timedelta(seconds=config.lease_seconds)
    callback_url = _required("MY_DATA_HUB_CALLBACK_URL")
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
    tunnel = TunnelSupervisor(
        ReverseTunnelSpec(
            gateway_host=config.tunnel_gateway_host,
            gateway_port=config.tunnel_gateway_port,
            gateway_user=config.tunnel_gateway_user,
            remote_bind_host="127.0.0.1",
            remote_bind_port=config.tunnel_remote_port,
            local_postgres_port=config.postgres_port,
            identity_file=Path(_required("MY_DATA_HUB_TUNNEL_IDENTITY_FILE")),
            known_hosts_file=Path(_required("MY_DATA_HUB_TUNNEL_KNOWN_HOSTS")),
            expires_at=now + timedelta(seconds=config.maximum_runtime_seconds + 300),
        )
    )
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
        receipt = runtime.emit(
            RuntimeEventType.SERVICE_READY,
            phase="registering",
            status="ready",
            data=ready.event_payload(),
        )
        if receipt.status != "delivered":
            raise RuntimeError("service.ready was not acknowledged by the control plane")

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
    ready = bootstrap.run(
        BootstrapRequest(
            identity=identity,
            source=config.boot_source,
            checkpoint_directory=config.checkpoint_directory,
            lease_until=lease_until,
            now=now,
        )
    )
    _wait_for_activation(_activation_url(callback_url, config.run_id, config.attempt_id), run_secret, identity)
    assert gate_connection is not None
    gate = DatabaseGate(gate_connection)
    gate.activate(identity)
    credential_now = datetime.now(UTC)
    reader_expires_at = min(credential_now + timedelta(minutes=4), ready.lease_until)
    if reader_expires_at <= credential_now + timedelta(seconds=15):
        raise RuntimeError("ACTIVE lease is too short to issue a bounded reader credential")
    try:
        _reader_principal, reader_expires_at = _register_reader_credential(
            connection=gate_connection,
            gate=gate,
            config=config,
            callback_url=callback_url,
            run_secret=run_secret,
            expires_at=reader_expires_at,
            now=credential_now,
        )
    except Exception:
        gate.fence(identity, "reader_credential_registration_failed")
        tunnel.stop()
        supervisor.stop(immediate=True)
        gate_connection.close()
        raise
    # The provider hard-stop applies to the whole Notebook, not only the active
    # service loop.  Reserve a declared, testable window for drain, basebackup,
    # exact readback, independent restore and durable HEAD promotion.
    deadline = time.monotonic() + (config.maximum_runtime_seconds - config.checkpoint_reserve_seconds)
    current_lease = ready.lease_until
    active_error: BaseException | None = None
    try:
        while time.monotonic() < deadline:
            time.sleep(30)
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
                    _reader_principal, reader_expires_at = _register_reader_credential(
                        connection=gate_connection,
                        gate=gate,
                        config=config,
                        callback_url=callback_url,
                        run_secret=run_secret,
                        expires_at=next_expiry,
                        now=observed_now,
                    )
            if datetime.now(UTC) + timedelta(seconds=15) >= current_lease:
                raise TimeoutError("callback unavailable; write lease is closing")
    except BaseException as exc:
        active_error = exc

    _checkpoint_before_stop(
        gate=gate,
        runtime=runtime,
        tunnel=tunnel,
        supervisor=supervisor,
        coordinator=checkpoint_coordinator,
        database_url=database_url,
        package_directory=paths.checkpoints,
        identity=identity,
    )
    if active_error is not None:
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
) -> PublishReceipt:
    """Close writes and stop processes only after durable checkpoint success.

    On any candidate failure the gate remains draining, the previous durable
    HEAD remains authoritative, and no runtime-terminal/stop effect is emitted.
    """

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
            "checkpoint failed; drained master remains nonterminal and old HEAD is preserved"
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
    if verified.status != "delivered" or terminal.status != "delivered":
        raise CheckpointShutdownError(
            "checkpoint is durable but terminal state was not acknowledged; master remains drained"
        )
    tunnel.stop()
    supervisor.stop(immediate=False)
    return receipt


def main() -> int:
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
    return run_master(config, checkpoint_coordinator=coordinator)
