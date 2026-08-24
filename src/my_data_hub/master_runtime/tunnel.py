"""Restricted SSH reverse-tunnel process contract for the direct PostgreSQL plane."""

from __future__ import annotations

import ipaddress
import subprocess
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from .contracts import require_utc


class TunnelError(RuntimeError):
    """Tunnel configuration or process failed closed."""


@dataclass(frozen=True, slots=True)
class ReverseTunnelSpec:
    gateway_host: str
    gateway_port: int
    gateway_user: str
    remote_bind_host: str
    remote_bind_port: int
    local_postgres_port: int
    identity_file: Path
    certificate_file: Path
    known_hosts_file: Path
    expires_at: datetime
    delete_identity_on_stop: bool = False

    def validate(self, *, now: datetime) -> None:
        if not self.gateway_host or any(char.isspace() for char in self.gateway_host):
            raise ValueError("gateway host is invalid")
        if not self.gateway_user or not self.gateway_user.replace("-", "").replace("_", "").isalnum():
            raise ValueError("gateway user is invalid")
        if ipaddress.ip_address(self.remote_bind_host) not in {
            ipaddress.ip_address("127.0.0.1"),
            ipaddress.ip_address("::1"),
        }:
            raise ValueError("reverse bind must be loopback-only")
        for port in (self.gateway_port, self.remote_bind_port, self.local_postgres_port):
            if not 1 <= port <= 65535:
                raise ValueError("tunnel port is outside 1..65535")
        for path in (self.identity_file, self.certificate_file, self.known_hosts_file):
            if not path.is_file() or path.is_symlink():
                raise ValueError(f"tunnel trust material must be a regular file: {path}")
        if self.identity_file.stat().st_mode & 0o077:
            raise ValueError("SSH identity must not be accessible by group/other")
        if self.certificate_file.stat().st_mode & 0o077:
            raise ValueError("SSH certificate must not be accessible by group/other")
        if require_utc(self.expires_at, "expires_at") <= require_utc(now, "now"):
            raise ValueError("tunnel credential is expired")

    def arguments(self, *, now: datetime) -> list[str]:
        self.validate(now=now)
        reverse = f"{self.remote_bind_host}:{self.remote_bind_port}:127.0.0.1:{self.local_postgres_port}"
        return [
            "ssh",
            "-F",
            "/dev/null",
            "-N",
            "-T",
            "-p",
            str(self.gateway_port),
            "-i",
            str(self.identity_file.resolve()),
            "-o",
            f"CertificateFile={self.certificate_file.resolve()}",
            "-o",
            "BatchMode=yes",
            "-o",
            "IdentitiesOnly=yes",
            "-o",
            "StrictHostKeyChecking=yes",
            "-o",
            f"UserKnownHostsFile={self.known_hosts_file.resolve()}",
            "-o",
            "ExitOnForwardFailure=yes",
            "-o",
            "ForwardAgent=no",
            "-o",
            "PermitLocalCommand=no",
            "-o",
            "ServerAliveInterval=10",
            "-o",
            "ServerAliveCountMax=2",
            "-R",
            reverse,
            f"{self.gateway_user}@{self.gateway_host}",
        ]


class TunnelSupervisor:
    def __init__(self, spec: ReverseTunnelSpec) -> None:
        self.spec = spec
        self._process: subprocess.Popen[bytes] | None = None

    def start(self, *, now: datetime) -> None:
        if self._process is not None and self._process.poll() is None:
            raise TunnelError("tunnel is already running")
        arguments = self.spec.arguments(now=now)
        self._process = subprocess.Popen(
            arguments,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            start_new_session=True,
            close_fds=True,
        )

    def poll(self, *, now: datetime) -> None:
        process = self._process
        if process is None:
            raise TunnelError("tunnel is not started")
        if require_utc(now, "now") >= require_utc(self.spec.expires_at, "expires_at"):
            self.stop()
            raise TunnelError("tunnel credential expired")
        code = process.poll()
        if code is not None:
            raise TunnelError(f"tunnel exited with status {code}")

    def stop(self) -> None:
        process = self._process
        if process is not None and process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
        self._process = None
        if self.spec.delete_identity_on_stop:
            for path in (self.spec.certificate_file, self.spec.identity_file):
                with suppress(FileNotFoundError):
                    path.unlink()
            with suppress(OSError):
                self.spec.identity_file.parent.rmdir()
