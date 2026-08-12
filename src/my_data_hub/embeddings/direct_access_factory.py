"""JIT direct-access assembly for centrally launched embedding workers.

This module does not expose a listener and does not proxy PostgreSQL bytes.  It
adapts a task-bound credential minted by the ACTIVE master to a separately
deployed, worker-reachable TLS forwarding endpoint.  Absence of either primitive
is represented by a stable fail-closed reason code.
"""

from __future__ import annotations

import ipaddress
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Protocol
from urllib.parse import parse_qsl, quote, urlencode, urlsplit, urlunsplit
from uuid import UUID

from my_data_hub.embeddings.central_launcher import EmbeddingWorkerDirectAccess
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata


class EmbeddingDirectAccessUnavailable(RuntimeError):
    """One exact missing production primitive; safe to expose as a code."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(code)


@dataclass(frozen=True, slots=True)
class TaskBoundEmbeddingCredential:
    """Secret result of the ACTIVE master's existing JIT role provisioner."""

    master_instance_id: UUID
    epoch: int
    task_run_id: UUID
    credential_id: UUID
    role: str
    database_url: str = field(repr=False)
    expires_at: datetime


class EmbeddingCredentialAuthority(Protocol):
    """Task-token authenticated source backed by the master JIT registrar."""

    def issue(
        self, metadata: EmbeddingLaunchMetadata, task_token: str
    ) -> TaskBoundEmbeddingCredential: ...

    def revoke(self, credential_id: UUID, *, task_run_id: UUID) -> None: ...


@dataclass(frozen=True, slots=True)
class WorkerReachableTunnel:
    """Deploy-owned TLS TCP forward; never an HTTP/control-plane proxy."""

    host: str
    port: int
    tls_ca_path: Path

    def validate(self) -> None:
        if not self.host or any(character.isspace() for character in self.host):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_ENDPOINT_INVALID")
        try:
            address = ipaddress.ip_address(self.host)
        except ValueError:
            address = None
        if self.host in {"localhost", "postgres-master.internal"} or (
            address is not None
            and (address.is_loopback or address.is_private or address.is_link_local or address.is_unspecified)
        ):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_NOT_WORKER_REACHABLE")
        if not 1 <= self.port <= 65535:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_ENDPOINT_INVALID")
        path = self.tls_ca_path
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_CA_UNAVAILABLE")
        if path.stat().st_mode & 0o022 or not 1 <= path.stat().st_size <= 64 * 1024:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_CA_UNSAFE")


@dataclass(slots=True)
class ExistingEpochEmbeddingAccessFactory:
    """Smallest safe adapter over JIT credentials plus a direct TLS endpoint."""

    authority: EmbeddingCredentialAuthority | None
    tunnel: WorkerReachableTunnel | None
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    minimum_remaining: timedelta = timedelta(seconds=45)

    @property
    def ready(self) -> bool:
        try:
            self._prerequisites()
        except EmbeddingDirectAccessUnavailable:
            return False
        return True

    def missing_component(self) -> str | None:
        try:
            self._prerequisites()
        except EmbeddingDirectAccessUnavailable as exc:
            return exc.code
        return None

    def __call__(
        self, metadata: EmbeddingLaunchMetadata, task_token: str
    ) -> EmbeddingWorkerDirectAccess:
        authority, tunnel = self._prerequisites()
        if len(task_token) < 32 or len(task_token) > 256:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TASK_TOKEN_INVALID")
        credential = authority.issue(metadata, task_token)
        now = self.clock().astimezone(UTC)
        expiry = credential.expires_at
        if expiry.tzinfo is None:
            self._revoke(authority, credential)
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_INVALID")
        expiry = expiry.astimezone(UTC)
        if (
            credential.task_run_id != metadata.task_run_id
            or credential.epoch != metadata.epoch
            or credential.role != "embedding_worker"
            or expiry <= now + self.minimum_remaining
            or expiry > now + timedelta(minutes=5)
        ):
            self._revoke(authority, credential)
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_BINDING_INVALID")
        try:
            database_url = self._external_url(credential.database_url, tunnel)
            ca_pem = tunnel.tls_ca_path.read_text(encoding="ascii")
        except Exception as exc:
            self._revoke(authority, credential)
            if isinstance(exc, EmbeddingDirectAccessUnavailable):
                raise
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_CA_UNAVAILABLE") from exc
        if "BEGIN CERTIFICATE" not in ca_pem or "PRIVATE KEY" in ca_pem:
            self._revoke(authority, credential)
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TUNNEL_CA_INVALID")
        return EmbeddingWorkerDirectAccess(
            database_url=database_url,
            tls_ca_pem=ca_pem,
            expires_at=expiry,
            epoch=credential.epoch,
            tunnel_endpoint=f"{tunnel.host}:{tunnel.port}",
            credential_id=credential.credential_id,
        )

    def revoke(self, credential_id: UUID, *, task_run_id: UUID) -> None:
        authority, _ = self._prerequisites()
        authority.revoke(credential_id, task_run_id=task_run_id)

    def _prerequisites(self) -> tuple[EmbeddingCredentialAuthority, WorkerReachableTunnel]:
        if self.authority is None:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_JIT_CREDENTIAL_AUTHORITY_UNAVAILABLE")
        if self.tunnel is None:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_WORKER_TLS_FORWARD_UNAVAILABLE")
        self.tunnel.validate()
        return self.authority, self.tunnel

    @staticmethod
    def _revoke(authority: EmbeddingCredentialAuthority, credential: TaskBoundEmbeddingCredential) -> None:
        authority.revoke(credential.credential_id, task_run_id=credential.task_run_id)

    @staticmethod
    def _external_url(value: str, tunnel: WorkerReachableTunnel) -> str:
        parsed = urlsplit(value)
        query = dict(parse_qsl(parsed.query, keep_blank_values=True))
        if (
            parsed.scheme not in {"postgres", "postgresql"}
            or not parsed.username
            or not parsed.password
            or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
            or query.get("sslmode") not in {"verify-ca", "verify-full"}
            or query.get("connect_timeout") != "5"
        ):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_JIT_DATABASE_URL_INVALID")
        query["sslmode"] = "verify-ca"
        query["sslrootcert"] = "/kaggle/working/mdh-worker-ca.pem"
        netloc = f"{quote(parsed.username, safe='')}:{quote(parsed.password, safe='')}@{tunnel.host}:{tunnel.port}"
        return urlunsplit((parsed.scheme, netloc, parsed.path, urlencode(query), ""))
