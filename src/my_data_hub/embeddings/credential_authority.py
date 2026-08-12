"""Private-file registrar for task-bound embedding worker JIT credentials.

The ACTIVE master publishes bounded secret envelopes.  The central launcher
claims only the exact task/epoch credential and writes revocation commands for
the master to consume.  Files are the same 0700-directory/0600-file primitive
already used by MCP session credentials, but task and credential identities are
part of the contract so two concurrent workers never overwrite one another.
"""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import parse_qs, urlsplit
from uuid import UUID

from my_data_hub.embeddings.direct_access_factory import (
    EmbeddingDirectAccessUnavailable,
    TaskBoundEmbeddingCredential,
)
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata
from my_data_hub.hashing import canonical_json_bytes

_MAX_FILE_BYTES = 16 * 1024


@dataclass(frozen=True, slots=True)
class EmbeddingCredentialRegistration:
    master_instance_id: UUID
    epoch: int
    task_run_id: UUID
    credential_id: UUID
    role: str
    database_url: str
    expires_at: datetime
    task_token_sha256: str

    def credential(self) -> TaskBoundEmbeddingCredential:
        return TaskBoundEmbeddingCredential(
            master_instance_id=self.master_instance_id,
            epoch=self.epoch,
            task_run_id=self.task_run_id,
            credential_id=self.credential_id,
            role=self.role,
            database_url=self.database_url,
            expires_at=self.expires_at,
        )


class DirectoryEmbeddingCredentialAuthority:
    """Atomic registrar/source plus durable, idempotent revocation mailbox."""

    def __init__(
        self,
        root: Path,
        *,
        clock=lambda: datetime.now(UTC),  # type: ignore[no-untyped-def]
        credential_wait_seconds: float = 30.0,
        poll_seconds: float = 0.25,
    ) -> None:
        if not 0 <= credential_wait_seconds <= 120 or not 0.01 <= poll_seconds <= 5:
            raise ValueError("embedding credential wait policy is outside the bounded contract")
        self.root = root
        self.clock = clock
        self.credential_wait_seconds = credential_wait_seconds
        self.poll_seconds = poll_seconds

    def store(self, registration: EmbeddingCredentialRegistration) -> Path:
        self._validate_registration(registration)
        self._private_root(create=True)
        path = self.root / self._credential_name(registration.task_run_id)
        payload = {
            "schema_version": "embedding-worker-credential.v1",
            "master_instance_id": str(registration.master_instance_id),
            "epoch": registration.epoch,
            "task_run_id": str(registration.task_run_id),
            "credential_id": str(registration.credential_id),
            "role": registration.role,
            "database_url": registration.database_url,
            "expires_at": registration.expires_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "task_token_sha256": registration.task_token_sha256,
        }
        self._atomic(path, canonical_json_bytes(payload) + b"\n")
        request_path = self.root / "requests" / f"{registration.task_run_id.hex}.json"
        if request_path.is_file() and not request_path.is_symlink():
            request_path.unlink()
        return path

    def issue(
        self, metadata: EmbeddingLaunchMetadata, task_token: str
    ) -> TaskBoundEmbeddingCredential:
        self._private_root(create=False)
        path = self.root / self._credential_name(metadata.task_run_id)
        if not path.is_file():
            self._request(metadata, task_token)
        deadline = time.monotonic() + self.credential_wait_seconds
        while not path.is_file() and time.monotonic() < deadline:
            time.sleep(min(self.poll_seconds, max(0.0, deadline - time.monotonic())))
        if not path.is_file():
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_JIT_CREDENTIAL_PENDING")
        payload = self._read(path)
        expected = {
            "schema_version", "master_instance_id", "epoch", "task_run_id", "credential_id",
            "role", "database_url", "expires_at", "task_token_sha256",
        }
        if set(payload) != expected or payload.get("schema_version") != "embedding-worker-credential.v1":
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_ENVELOPE_INVALID")
        observed_token = hashlib.sha256(task_token.encode()).hexdigest()
        if not hmac.compare_digest(str(payload["task_token_sha256"]), observed_token):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TASK_TOKEN_INVALID")
        try:
            registration = EmbeddingCredentialRegistration(
                master_instance_id=UUID(str(payload["master_instance_id"])),
                epoch=int(str(payload["epoch"])), task_run_id=UUID(str(payload["task_run_id"])),
                credential_id=UUID(str(payload["credential_id"])), role=str(payload["role"]),
                database_url=str(payload["database_url"]),
                expires_at=datetime.fromisoformat(str(payload["expires_at"]).replace("Z", "+00:00")),
                task_token_sha256=str(payload["task_token_sha256"]),
            )
            self._validate_registration(registration)
        except (TypeError, ValueError) as exc:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_ENVELOPE_INVALID") from exc
        if registration.task_run_id != metadata.task_run_id or registration.epoch != metadata.epoch:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_BINDING_INVALID")
        return registration.credential()

    def pending_requests(self, *, limit: int = 100) -> tuple[dict[str, object], ...]:
        """Master-facing metadata commands; never contains the plaintext token."""

        self._private_root(create=False)
        directory = self.root / "requests"
        if not directory.exists():
            return ()
        if directory.is_symlink() or not directory.is_dir() or directory.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_REQUEST_MAILBOX_UNSAFE")
        values: list[dict[str, object]] = []
        for path in sorted(directory.iterdir()):
            if len(values) >= limit:
                break
            payload = self._read(path)
            if payload.get("schema_version") != "embedding-worker-credential-request.v1":
                raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_REQUEST_INVALID")
            values.append(payload)
        return tuple(values)

    def revoke(self, credential_id: UUID, *, task_run_id: UUID) -> None:
        self._private_root(create=True)
        directory = self.root / "revocations"
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.is_symlink() or directory.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_REVOCATION_MAILBOX_UNSAFE")
        path = directory / f"{task_run_id.hex}.{credential_id.hex}.json"
        self._atomic(path, canonical_json_bytes({
            "schema_version": "embedding-worker-revocation.v1",
            "task_run_id": str(task_run_id), "credential_id": str(credential_id),
            "requested_at": self.clock().astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }) + b"\n")

    def pending_revocations(self, *, limit: int = 100) -> tuple[tuple[UUID, UUID, Path], ...]:
        self._private_root(create=False)
        directory = self.root / "revocations"
        if not directory.exists():
            return ()
        if directory.is_symlink() or not directory.is_dir() or directory.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_REVOCATION_MAILBOX_UNSAFE")
        values: list[tuple[UUID, UUID, Path]] = []
        for path in sorted(directory.iterdir()):
            if len(values) >= limit:
                break
            payload = self._read(path)
            if payload.get("schema_version") != "embedding-worker-revocation.v1":
                raise EmbeddingDirectAccessUnavailable("EMBEDDING_REVOCATION_COMMAND_INVALID")
            values.append((UUID(str(payload["task_run_id"])), UUID(str(payload["credential_id"])), path))
        return tuple(values)

    @staticmethod
    def acknowledge_revocation(path: Path) -> None:
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_REVOCATION_COMMAND_INVALID")
        path.unlink()

    def _validate_registration(self, value: EmbeddingCredentialRegistration) -> None:
        now = self.clock().astimezone(UTC)
        if value.role != "embedding_worker" or value.epoch < 1:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_BINDING_INVALID")
        if value.expires_at.tzinfo is None or value.expires_at.astimezone(UTC) <= now:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_EXPIRED")
        if len(value.task_token_sha256) != 64 or any(c not in "0123456789abcdef" for c in value.task_token_sha256):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_TASK_TOKEN_INVALID")
        parsed = urlsplit(value.database_url)
        query = parse_qs(parsed.query)
        if (parsed.scheme not in {"postgres", "postgresql"} or not parsed.username or not parsed.password
                or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
                or query.get("sslmode", [""])[0] not in {"verify-ca", "verify-full"}
                or query.get("connect_timeout", [""])[0] != "5"):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_JIT_DATABASE_URL_INVALID")

    def _request(self, metadata: EmbeddingLaunchMetadata, task_token: str) -> None:
        token_sha = hashlib.sha256(task_token.encode()).hexdigest()
        directory = self.root / "requests"
        directory.mkdir(mode=0o700, exist_ok=True)
        if directory.is_symlink() or directory.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_REQUEST_MAILBOX_UNSAFE")
        path = directory / f"{metadata.task_run_id.hex}.json"
        payload = {
            "schema_version": "embedding-worker-credential-request.v1",
            "request_id": str(metadata.request_id),
            "request_sha256": metadata.request_sha256,
            "task_run_id": str(metadata.task_run_id),
            "input_jobs_sha256": metadata.input_jobs_sha256,
            "epoch": metadata.epoch,
            "task_token_sha256": token_sha,
            "requested_at": self.clock().astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
        encoded = canonical_json_bytes(payload) + b"\n"
        if path.exists():
            existing = path.read_bytes()
            if existing != encoded:
                raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_REQUEST_CONFLICT")
            return
        self._atomic(path, encoded)

    def _private_root(self, *, create: bool) -> None:
        if create:
            self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
            self.root.chmod(0o700)
        if self.root.is_symlink() or not self.root.is_dir() or self.root.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_DIRECTORY_UNSAFE")

    @staticmethod
    def _credential_name(task_run_id: UUID) -> str:
        return f"{task_run_id.hex}.embedding-worker.json"

    @staticmethod
    def _atomic(path: Path, content: bytes) -> None:
        descriptor, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        temporary = Path(raw)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, path)
        finally:
            temporary.unlink(missing_ok=True)

    @staticmethod
    def _read(path: Path) -> dict[str, object]:
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_ENVELOPE_UNAVAILABLE")
        if path.stat().st_size > _MAX_FILE_BYTES:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_ENVELOPE_INVALID")
        try:
            payload = json.loads(path.read_text())
        except (OSError, json.JSONDecodeError) as exc:
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_ENVELOPE_INVALID") from exc
        if not isinstance(payload, dict):
            raise EmbeddingDirectAccessUnavailable("EMBEDDING_CREDENTIAL_ENVELOPE_INVALID")
        return payload
