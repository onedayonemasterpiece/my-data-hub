"""Production isolated PostgreSQL restore verifier for exact checkpoint bytes."""

from __future__ import annotations

import shutil
import subprocess
import tempfile
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol

from .manifest import CheckpointManifest
from .publisher import assert_restore_equality
from .restore import restore_physical_archive
from .restore_probe import collect_restore_probe


class RestoreVerifierError(RuntimeError):
    """The exact checkpoint could not prove an isolated PostgreSQL restore."""


class RestoreCommandRunner(Protocol):
    def run(self, arguments: list[str], *, timeout_seconds: int) -> None: ...


class _SubprocessRestoreRunner:
    def run(self, arguments: list[str], *, timeout_seconds: int) -> None:
        result = subprocess.run(arguments, check=False, capture_output=True, text=True, timeout=timeout_seconds)
        if result.returncode:
            raise RestoreVerifierError(f"{Path(arguments[0]).name} failed with exit {result.returncode}")


class IsolatedPostgresRestoreVerifier:
    """Extract, start, probe, and stop a disposable PostgreSQL restore.

    A verifier never accepts a URL for a pre-existing database.  The only
    PostgreSQL it connects to is the process started from ``package`` below a
    newly-created private temporary directory.
    """

    def __init__(
        self,
        *,
        pg_ctl: Path,
        working_directory: Path,
        port: int,
        runner: RestoreCommandRunner | None = None,
        connect: Any | None = None,
        timeout_seconds: int = 180,
    ) -> None:
        if not 1024 <= port <= 65535:
            raise ValueError("isolated restore port must be unprivileged")
        if not 60 <= timeout_seconds <= 900:
            raise ValueError("isolated restore timeout is outside the bounded contract")
        if not working_directory.is_dir() or working_directory.is_symlink():
            raise ValueError("isolated restore working directory must be a real directory")
        self.pg_ctl = pg_ctl
        self.working_directory = working_directory
        self.port = port
        self.runner = runner or _SubprocessRestoreRunner()
        if connect is None:
            import psycopg

            connect = psycopg.connect
        self.connect = connect
        self.timeout_seconds = timeout_seconds

    def verify_restore(self, package: Path, manifest: CheckpointManifest) -> dict[str, object]:
        root = Path(tempfile.mkdtemp(prefix="checkpoint-restore-", dir=self.working_directory))
        root.chmod(0o700)
        pgdata = root / "pgdata"
        socket = root / "socket"
        log = root / "postgres.log"
        started = False
        try:
            restore_physical_archive(package, manifest, pgdata)
            socket.mkdir(mode=0o700)
            auto_conf = pgdata / "postgresql.auto.conf"
            with auto_conf.open("a", encoding="utf-8") as stream:
                stream.write(
                    "\n# isolated checkpoint restore verifier\n"
                    "listen_addresses = ''\n"
                    f"port = {self.port}\n"
                    f"unix_socket_directories = '{_quote_setting(socket)}'\n"
                    "ssl = off\n"
                    "archive_mode = off\n"
                )
            auto_conf.chmod(0o600)
            self.runner.run(
                [
                    str(self.pg_ctl),
                    "--pgdata",
                    str(pgdata),
                    "--log",
                    str(log),
                    "--wait",
                    "--timeout=60",
                    "start",
                ],
                timeout_seconds=self.timeout_seconds,
            )
            started = True
            with self.connect(
                dbname="postgres",
                user="postgres",
                host=str(socket),
                port=self.port,
                connect_timeout=15,
            ) as connection:
                observed = collect_restore_probe(
                    connection,
                    tuple(sorted(manifest.restore_probe.row_counts)),
                )
            assert_restore_equality(manifest, observed)
            return {
                "ok": True,
                "mode": "isolated_physical_restore",
                "checkpoint_id": str(manifest.checkpoint_id),
                "manifest_sha256": manifest.manifest_sha256,
                **observed,
            }
        except Exception as exc:
            if isinstance(exc, RestoreVerifierError):
                raise
            raise RestoreVerifierError("isolated checkpoint restore verification failed") from exc
        finally:
            if started:
                with suppress(Exception):
                    self.runner.run(
                        [
                            str(self.pg_ctl),
                            "--pgdata",
                            str(pgdata),
                            "--wait",
                            "--timeout=60",
                            "--mode=immediate",
                            "stop",
                        ],
                        timeout_seconds=self.timeout_seconds,
                    )
            shutil.rmtree(root, ignore_errors=True)


def _quote_setting(value: object) -> str:
    return str(value).replace("'", "''")
