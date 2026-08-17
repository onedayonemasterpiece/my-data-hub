"""PostgreSQL 18 process configuration and bounded command supervision."""

from __future__ import annotations

import os
import shutil
import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol

from .contracts import MasterPaths

KAGGLE_POSTGRES_UID = 65534
KAGGLE_POSTGRES_GID = 65534
SETPRIV = Path("/usr/bin/setpriv")


class PostgresRuntimeError(RuntimeError):
    """PostgreSQL runtime precondition or command failure."""


class CommandRunner(Protocol):
    def run(
        self,
        arguments: Sequence[str],
        *,
        timeout_seconds: int,
        environment: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]: ...


class SubprocessRunner:
    """No-shell runner whose errors never include the process environment."""

    def run(
        self,
        arguments: Sequence[str],
        *,
        timeout_seconds: int,
        environment: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        command = list(arguments)
        if os.geteuid() == 0:
            if not SETPRIV.is_file() or not os.access(SETPRIV, os.X_OK):
                raise PostgresRuntimeError("root Kaggle runtime requires exact /usr/bin/setpriv")
            command = [
                str(SETPRIV),
                f"--reuid={KAGGLE_POSTGRES_UID}",
                f"--regid={KAGGLE_POSTGRES_GID}",
                "--clear-groups",
                "--",
                *command,
            ]
        result = subprocess.run(
            command,
            check=False,
            capture_output=True,
            text=True,
            timeout=timeout_seconds,
            env=dict(environment) if environment is not None else None,
        )
        if result.returncode:
            command = Path(arguments[0]).name if arguments else "<empty>"
            detail = (result.stderr or result.stdout).splitlines()[:1]
            raise PostgresRuntimeError(
                f"{command} failed with exit {result.returncode}"
                + (f": {detail[0][:500]}" if detail else "")
            )
        return result


@dataclass(frozen=True, slots=True)
class PostgresBinaries:
    initdb: Path
    pg_ctl: Path
    postgres: Path
    pg_isready: Path
    pg_basebackup: Path
    pg_dump: Path
    pg_restore: Path

    @classmethod
    def discover(cls, bin_directory: Path | None = None) -> PostgresBinaries:
        names = ("initdb", "pg_ctl", "postgres", "pg_isready", "pg_basebackup", "pg_dump", "pg_restore")
        resolved: dict[str, Path] = {}
        for name in names:
            candidate = bin_directory / name if bin_directory is not None else None
            value = str(candidate) if candidate is not None and candidate.is_file() else shutil.which(name)
            if value is None:
                raise PostgresRuntimeError(f"required PostgreSQL binary is missing: {name}")
            resolved[name] = Path(value).resolve()
        return cls(**resolved)  # type: ignore[arg-type]


@dataclass(frozen=True, slots=True)
class PostgresConfig:
    port: int
    tls_certificate: Path
    tls_private_key: Path
    tls_ca: Path | None = None
    max_connections: int = 40
    statement_timeout_ms: int = 30_000
    lock_timeout_ms: int = 5_000
    idle_transaction_timeout_ms: int = 30_000

    def validate(self) -> None:
        if not 1024 <= self.port <= 65535:
            raise ValueError("PostgreSQL port must be unprivileged")
        if not 4 <= self.max_connections <= 200:
            raise ValueError("max_connections must remain bounded")
        for value in (
            self.statement_timeout_ms,
            self.lock_timeout_ms,
            self.idle_transaction_timeout_ms,
        ):
            if not 100 <= value <= 300_000:
                raise ValueError("PostgreSQL timeout is outside the bounded contract")
        for path in (self.tls_certificate, self.tls_private_key):
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"TLS material must be a regular file: {path}")
        if self.tls_private_key.stat().st_mode & 0o077:
            raise ValueError("TLS private key must not be accessible by group/other")
        if self.tls_ca is not None and (not self.tls_ca.is_file() or self.tls_ca.is_symlink()):
            raise ValueError("TLS CA must be a regular file")

    def render(self, paths: MasterPaths) -> str:
        self.validate()
        paths.validate()
        values = {
            "port": self.port,
            "socket": str(paths.socket),
            "max_connections": self.max_connections,
            "cert": str(self.tls_certificate.resolve()),
            "key": str(self.tls_private_key.resolve()),
            "statement_timeout": self.statement_timeout_ms,
            "lock_timeout": self.lock_timeout_ms,
            "idle_timeout": self.idle_transaction_timeout_ms,
        }
        ca_line = ""
        if self.tls_ca is not None:
            ca_line = f"ssl_ca_file = '{_quote_setting(str(self.tls_ca.resolve()))}'\n"
        return (
            "# Generated by my_data_hub.master_runtime; do not place secrets here.\n"
            "listen_addresses = '127.0.0.1'\n"
            f"port = {values['port']}\n"
            f"unix_socket_directories = '{_quote_setting(values['socket'])}'\n"
            f"max_connections = {values['max_connections']}\n"
            "ssl = on\n"
            "ssl_min_protocol_version = 'TLSv1.3'\n"
            f"ssl_cert_file = '{_quote_setting(values['cert'])}'\n"
            f"ssl_key_file = '{_quote_setting(values['key'])}'\n"
            f"{ca_line}"
            "password_encryption = 'scram-sha-256'\n"
            "shared_preload_libraries = ''\n"
            "wal_level = replica\n"
            "max_wal_senders = 3\n"
            "archive_mode = off\n"
            "log_connections = on\n"
            "log_disconnections = on\n"
            "log_statement = 'ddl'\n"
            "log_min_duration_statement = 1000\n"
            "log_parameter_max_length = 0\n"
            "log_parameter_max_length_on_error = 0\n"
            f"statement_timeout = {values['statement_timeout']}\n"
            f"lock_timeout = {values['lock_timeout']}\n"
            f"idle_in_transaction_session_timeout = {values['idle_timeout']}\n"
        )

    @staticmethod
    def render_hba() -> str:
        return (
            "# Local bootstrap is socket-only and owned by the isolated notebook runtime; "
            "the tunnel reaches loopback TLS.\n"
            # Physical base backups open a replication connection, which has
            # no database name and therefore does not match ``local all``.
            # Keep this authority socket-local and restricted to the bootstrap
            # superuser used by the in-Notebook checkpoint creator.
            "local replication postgres trust\n"
            "local all postgres trust\n"
            "local all all scram-sha-256\n"
            "hostnossl all all 0.0.0.0/0 reject\n"
            "hostnossl all all ::0/0 reject\n"
            "hostssl all all 127.0.0.1/32 scram-sha-256\n"
            "hostssl all all ::1/128 scram-sha-256\n"
        )


def _quote_setting(value: object) -> str:
    return str(value).replace("'", "''")


class PostgresSupervisor:
    """Initialize and supervise one PostgreSQL cluster below a Kaggle working dir."""

    def __init__(
        self,
        *,
        paths: MasterPaths,
        binaries: PostgresBinaries,
        config: PostgresConfig,
        runner: CommandRunner | None = None,
    ) -> None:
        paths.validate()
        self.paths = paths
        self.binaries = binaries
        self.config = config
        self.runner = runner or SubprocessRunner()

    def prepare_directories(self) -> None:
        for path in (self.paths.pgdata, self.paths.socket, self.paths.logs, self.paths.runtime_events.parent):
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.chmod(0o700)
        self._give_tree_to_postgres(self.paths.pgdata)
        self._give_to_postgres(self.paths.socket)
        self._give_to_postgres(self.paths.logs)
        for path in (self.config.tls_certificate, self.config.tls_private_key, self.config.tls_ca):
            if path is not None:
                self._give_to_postgres(path)

    def initialize_empty(self) -> None:
        self.prepare_directories()
        if any(self.paths.pgdata.iterdir()):
            raise PostgresRuntimeError("refusing empty bootstrap into non-empty PGDATA")
        self.runner.run(
            [
                str(self.binaries.initdb),
                "--pgdata",
                str(self.paths.pgdata),
                "--username=postgres",
                "--auth-local=peer",
                "--auth-host=scram-sha-256",
                "--encoding=UTF8",
                "--locale=C.UTF-8",
                "--no-instructions",
            ],
            timeout_seconds=180,
        )
        self.write_configuration()

    def write_configuration(self) -> None:
        self.prepare_directories()
        auto_conf = self.paths.pgdata / "postgresql.auto.conf"
        hba = self.paths.pgdata / "pg_hba.conf"
        _atomic_write(auto_conf, self.config.render(self.paths), mode=0o600)
        _atomic_write(hba, self.config.render_hba(), mode=0o600)
        self._give_to_postgres(auto_conf)
        self._give_to_postgres(hba)

    @staticmethod
    def _give_to_postgres(path: Path) -> None:
        if os.geteuid() == 0:
            os.chown(path, KAGGLE_POSTGRES_UID, KAGGLE_POSTGRES_GID)

    @classmethod
    def _give_tree_to_postgres(cls, root: Path) -> None:
        if os.geteuid() != 0:
            return
        if root.is_symlink():
            raise PostgresRuntimeError("PostgreSQL runtime tree root cannot be a symlink")
        cls._give_to_postgres(root)
        for directory, names, files in os.walk(root, followlinks=False):
            base = Path(directory)
            for name in (*names, *files):
                child = base / name
                if child.is_symlink():
                    raise PostgresRuntimeError("PostgreSQL runtime tree cannot contain symlinks")
                cls._give_to_postgres(child)

    def start(self) -> None:
        log_path = self.paths.logs / "postgres.log"
        self.runner.run(
            [
                str(self.binaries.pg_ctl),
                "--pgdata",
                str(self.paths.pgdata),
                "--log",
                str(log_path),
                "--wait",
                "--timeout=60",
                "start",
            ],
            timeout_seconds=75,
        )
        self.runner.run(
            [
                str(self.binaries.pg_isready),
                "--host",
                str(self.paths.socket),
                "--port",
                str(self.config.port),
                "--timeout=5",
            ],
            timeout_seconds=10,
        )

    def stop(self, *, immediate: bool = False) -> None:
        mode = "immediate" if immediate else "fast"
        self.runner.run(
            [
                str(self.binaries.pg_ctl),
                "--pgdata",
                str(self.paths.pgdata),
                "--wait",
                "--timeout=60",
                "--mode",
                mode,
                "stop",
            ],
            timeout_seconds=75,
        )


def _atomic_write(path: Path, content: str, *, mode: int) -> None:
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    temporary.chmod(mode)
    temporary.replace(path)
