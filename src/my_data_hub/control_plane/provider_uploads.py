"""Restart-safe bounded staging for MCP Dataset uploads.

Raw file chunks live only below a private staging root.  The durable SQLite
control ledger receives provider intents/receipts only when finalization calls
the existing central Kaggle adapter; it never receives staged bytes.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import re
import shutil
import stat
import time
from base64 import b64decode, b64encode
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.oauth import AccessIdentity

UPLOAD_SCHEMA = "my-data-hub-mcp-chunked-upload.v1"
RECEIPT_SCHEMA = "my-data-hub-mcp-chunked-upload-receipt.v1"
MAX_UPLOAD_FILES = 100
MAX_UPLOAD_FILE_BYTES = 64 * 1024 * 1024
MAX_UPLOAD_TOTAL_BYTES = 256 * 1024 * 1024
MAX_UPLOAD_CHUNK_BYTES = 24 * 1024
MIN_UPLOAD_TTL_SECONDS = 300
MAX_UPLOAD_TTL_SECONDS = 86_400
RECEIPT_RETENTION_SECONDS = 7 * 86_400
MAX_ACTIVE_UPLOADS = 32
MAX_ACTIVE_DECLARED_BYTES = 1024 * 1024 * 1024
MAX_PRINCIPAL_ACTIVE_UPLOADS = 8
MAX_PRINCIPAL_DECLARED_BYTES = 512 * 1024 * 1024
MIN_UPLOAD_DISK_RESERVE_BYTES = 512 * 1024 * 1024


class ProviderUploadError(ValueError):
    """A bounded caller-visible upload contract failure."""


class ProviderUploadConflict(ProviderUploadError):
    """An exact replay binding differs or staged content was tampered with."""


class ProviderUploadExpired(ProviderUploadError):
    """The bounded staging lease expired and its raw bytes were removed."""


def _safe_path(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 1000:
        raise ProviderUploadError("upload file path is invalid")
    path = PurePosixPath(value)
    if (
        path.is_absolute()
        or value != path.as_posix()
        or "\\" in value
        or any(part in {"", ".", ".."} for part in path.parts)
    ):
        raise ProviderUploadError("upload file path must be normalized and traversal-free")
    lowered = value.casefold()
    forbidden = {
        "my-data-hub-resource.json",
        "dataset-metadata.json",
        "pg_version",
        "backup_manifest",
        "backup_label",
        "tablespace_map",
        "postmaster.pid",
        "postmaster.opts",
    }
    if (
        "checkpoint" in lowered
        or "postgres" in lowered
        or any(part.casefold() in forbidden or part.casefold().startswith("pg_wal") for part in path.parts)
        or lowered.endswith((".dump", ".sql", ".backup"))
    ):
        raise ProviderUploadError("upload file path is reserved")
    return value


def _private_directory(path: Path) -> None:
    if not path.is_absolute():
        raise ValueError("provider upload staging root must be absolute")
    if path.is_symlink() or (path.exists() and not path.is_dir()):
        raise ValueError("provider upload staging root must be a real directory")
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.chmod(0o700)
    if path.stat().st_mode & 0o077:
        raise ValueError("provider upload staging root must be private")


def _fsync_directory(path: Path) -> None:
    descriptor = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    raw = canonical_json_bytes(dict(value))
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        path.chmod(0o600)
        _fsync_directory(path.parent)
    finally:
        temporary.unlink(missing_ok=True)


def _read_json(path: Path) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
        raise ProviderUploadConflict("upload metadata is not a private regular file")
    try:
        value = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProviderUploadConflict("upload metadata is invalid") from exc
    if not isinstance(value, dict):
        raise ProviderUploadConflict("upload metadata is invalid")
    return value


def _require_private_real_directory(path: Path) -> None:
    try:
        metadata = path.lstat()
    except OSError as exc:
        raise ProviderUploadConflict("upload staging directory is missing") from exc
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_mode & 0o077:
        raise ProviderUploadConflict("upload staging directory is not private and real")


def _read_private_file(path: Path) -> bytes:
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or metadata.st_mode & 0o077:
            raise ProviderUploadConflict("upload chunk is not a private regular file")
        with os.fdopen(descriptor, "rb", closefd=False) as stream:
            return stream.read(MAX_UPLOAD_CHUNK_BYTES + 1)
    finally:
        os.close(descriptor)


class ProviderChunkedUploadStore:
    """Private filesystem staging with exact replay and terminal tombstones."""

    def __init__(
        self,
        root: Path,
        *,
        clock: Callable[[], float] = time.time,
        disk_usage: Callable[[Path], Any] = shutil.disk_usage,
    ) -> None:
        _private_directory(root)
        self.root = root
        self.uploads = root / "uploads"
        self.receipts = root / "receipts"
        _private_directory(self.uploads)
        _private_directory(self.receipts)
        self.clock = clock
        self.disk_usage = disk_usage
        self.root_lock_path = root / ".root.lock"
        descriptor = os.open(
            self.root_lock_path,
            os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
            0o600,
        )
        os.fchmod(descriptor, 0o600)
        os.close(descriptor)

    @contextmanager
    def _root_lock(self):  # type: ignore[no-untyped-def]
        descriptor = os.open(
            self.root_lock_path,
            os.O_RDWR | os.O_NOFOLLOW,
        )
        try:
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _upload_id(value: object) -> str:
        try:
            parsed = UUID(str(value))
        except (TypeError, ValueError) as exc:
            raise ProviderUploadError("upload_id must be an exact UUID") from exc
        return str(parsed)

    def _directory(self, upload_id: str) -> Path:
        return self.uploads / upload_id

    def _receipt_path(self, upload_id: str) -> Path:
        return self.receipts / f"{upload_id}.json"

    def _terminal_receipt(self, upload_id: str) -> dict[str, Any] | None:
        path = self._receipt_path(upload_id)
        if path.is_symlink():
            raise ProviderUploadConflict("upload receipt contract is invalid")
        if not path.exists():
            return None
        receipt = _read_json(path)
        if (
            receipt.get("schema_version") != RECEIPT_SCHEMA
            or receipt.get("upload_id") != upload_id
            or receipt.get("state") not in {
                "FINALIZED",
                "ABORTED",
                "EXPIRED",
                "QUARANTINED",
            }
        ):
            raise ProviderUploadConflict("upload receipt contract is invalid")
        return receipt

    def _remove_orphan_active_directory(self, upload_id: str) -> None:
        directory = self._directory(upload_id)
        if not directory.exists() and not directory.is_symlink():
            return
        _require_private_real_directory(directory)
        shutil.rmtree(directory)
        _fsync_directory(self.uploads)

    def _active_usage(self) -> tuple[int, int, dict[tuple[str, str], tuple[int, int]]]:
        active_count = 0
        active_bytes = 0
        principals: dict[tuple[str, str], tuple[int, int]] = {}
        for directory in list(self.uploads.iterdir()):
            if directory.is_symlink() or not directory.is_dir():
                raise ProviderUploadConflict("upload staging contains an invalid entry")
            try:
                upload_id = self._upload_id(directory.name)
            except ProviderUploadError as exc:
                raise ProviderUploadConflict("upload staging contains an invalid entry") from exc
            if self._terminal_receipt(upload_id) is not None:
                self._remove_orphan_active_directory(upload_id)
                continue
            try:
                state = _read_json(directory / "state.json")
            except ProviderUploadConflict:
                # A terminal operation may have removed the directory after
                # this root-wide snapshot. It can only reduce quota usage.
                if (
                    self._terminal_receipt(upload_id) is not None
                    or (not directory.exists() and not directory.is_symlink())
                ):
                    continue
                raise
            if (
                state.get("schema_version") != UPLOAD_SCHEMA
                or state.get("upload_id") != upload_id
                or state.get("state") not in {"OPEN", "READY", "FINALIZING"}
            ):
                raise ProviderUploadConflict("upload staging state is invalid")
            declared = state.get("total_bytes")
            subject = state.get("principal")
            client_id = state.get("client_id")
            if (
                not isinstance(declared, int)
                or isinstance(declared, bool)
                or not 0 <= declared <= MAX_UPLOAD_TOTAL_BYTES
                or not isinstance(subject, str)
                or not subject
                or not isinstance(client_id, str)
                or not client_id
            ):
                raise ProviderUploadConflict("upload staging quota metadata is invalid")
            active_count += 1
            active_bytes += declared
            key = (subject, client_id)
            prior_count, prior_bytes = principals.get(key, (0, 0))
            principals[key] = (prior_count + 1, prior_bytes + declared)
        return active_count, active_bytes, principals

    def _admit_upload(self, *, total: int, principal: AccessIdentity) -> None:
        count, declared, principals = self._active_usage()
        principal_count, principal_bytes = principals.get(
            (principal.subject, principal.client_id), (0, 0)
        )
        if count + 1 > MAX_ACTIVE_UPLOADS or declared + total > MAX_ACTIVE_DECLARED_BYTES:
            raise ProviderUploadError("global active upload quota is exhausted")
        if (
            principal_count + 1 > MAX_PRINCIPAL_ACTIVE_UPLOADS
            or principal_bytes + total > MAX_PRINCIPAL_DECLARED_BYTES
        ):
            raise ProviderUploadError("principal active upload quota is exhausted")
        free = int(self.disk_usage(self.root).free)
        if free - declared - total < MIN_UPLOAD_DISK_RESERVE_BYTES:
            raise ProviderUploadError("provider upload staging disk reserve would be violated")

    @contextmanager
    def _lock(self, upload_id: str, *, create: bool = False):  # type: ignore[no-untyped-def]
        directory = self._directory(upload_id)
        if create:
            if directory.exists() and (directory.is_symlink() or not directory.is_dir()):
                raise ProviderUploadConflict("upload staging directory is invalid")
            directory.mkdir(mode=0o700, parents=False, exist_ok=True)
            directory.chmod(0o700)
            _fsync_directory(self.uploads)
        if not directory.is_dir() or directory.is_symlink():
            raise ProviderUploadError("upload was not found")
        lock_path = directory / ".lock"
        descriptor = os.open(lock_path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
        try:
            os.fchmod(descriptor, 0o600)
            fcntl.flock(descriptor, fcntl.LOCK_EX)
            yield directory
        finally:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
            os.close(descriptor)

    @staticmethod
    def _binding(arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, str | bool]:
        return {
            "resource_ref": str(arguments.get("resource_ref", "")),
            "control_class": str(arguments.get("control_class", "")),
            "private": arguments.get("private") is True,
            "principal": principal.subject,
            "client_id": principal.client_id,
        }

    @staticmethod
    def _assert_binding(
        state: Mapping[str, Any], arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> None:
        binding = ProviderChunkedUploadStore._binding(arguments, principal)
        if any(state.get(key) != value for key, value in binding.items()):
            raise PermissionError("upload is not bound to this resource, principal, and client")
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping) or state.get("task_id") != str(payload.get("task_id", "")):
            raise PermissionError("upload is not bound to this task")

    @staticmethod
    def _public(state: Mapping[str, Any]) -> dict[str, Any]:
        files = []
        for item in state.get("files", []):
            chunks = item.get("chunks", [])
            received = int(
                item.get(
                    "received_bytes",
                    sum(int(chunk["byte_size"]) for chunk in chunks),
                )
            )
            files.append(
                {
                    "path": item["path"],
                    "byte_size": item["byte_size"],
                    "sha256": item["sha256"],
                    "received_bytes": received,
                    "complete": received == item["byte_size"],
                }
            )
        result = {
            "upload_id": state["upload_id"],
            "task_id": state["task_id"],
            "resource_ref": state["resource_ref"],
            "state": state["state"],
            "created_at": state["created_at"],
            "expires_at": state["expires_at"],
            "file_count": len(files),
            "total_bytes": state["total_bytes"],
            "files": files,
        }
        if isinstance(state.get("result"), Mapping):
            result["result"] = dict(state["result"])
        return result

    def _read_receipt(
        self, upload_id: str, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> dict[str, Any] | None:
        receipt = self._terminal_receipt(upload_id)
        if receipt is None:
            return None
        self._assert_binding(receipt, arguments, principal)
        self._remove_orphan_active_directory(upload_id)
        return receipt

    def start(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping):
            raise ProviderUploadError("upload start payload is invalid")
        expected_payload = {
            "kind",
            "upload_id",
            "task_id",
            "effect_id",
            "idempotency_key",
            "title",
            "disposable",
            "files",
            "ttl_seconds",
        }
        if set(payload) != expected_payload or payload.get("kind") != "dataset":
            raise ProviderUploadError("upload start payload is not exact")
        upload_id = self._upload_id(payload.get("upload_id"))
        task_id = str(payload.get("task_id", ""))
        effect_id = str(payload.get("effect_id", ""))
        try:
            UUID(task_id)
            UUID(effect_id)
        except ValueError as exc:
            raise ProviderUploadError("upload task_id and effect_id must be exact UUIDs") from exc
        resource_ref = arguments.get("resource_ref")
        idempotency_key = payload.get("idempotency_key")
        title = payload.get("title")
        disposable = payload.get("disposable")
        if (
            not isinstance(resource_ref, str)
            or re.fullmatch(r"[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{6,50}", resource_ref)
            is None
            or not isinstance(idempotency_key, str)
            or not 8 <= len(idempotency_key) <= 300
            or not isinstance(title, str)
            or not 6 <= len(title) <= 50
            or not isinstance(disposable, bool)
        ):
            raise ProviderUploadError("upload Dataset identity is invalid")
        if arguments.get("control_class") != "mcp_managed" or arguments.get("private") is not True:
            raise PermissionError("chunked upload is limited to private mcp_managed datasets")
        declarations = payload.get("files")
        if not isinstance(declarations, list) or not 1 <= len(declarations) <= MAX_UPLOAD_FILES:
            raise ProviderUploadError("upload file manifest is outside the bounded contract")
        files: list[dict[str, Any]] = []
        observed: set[str] = set()
        total = 0
        for declaration in declarations:
            if not isinstance(declaration, Mapping) or set(declaration) != {"path", "byte_size", "sha256"}:
                raise ProviderUploadError("upload file declaration is invalid")
            path = _safe_path(declaration["path"])
            size = declaration["byte_size"]
            digest = declaration["sha256"]
            if (
                path in observed
                or not isinstance(size, int)
                or isinstance(size, bool)
                or not 0 <= size <= MAX_UPLOAD_FILE_BYTES
                or not isinstance(digest, str)
                or len(digest) != 64
                or any(character not in "0123456789abcdef" for character in digest)
            ):
                raise ProviderUploadError("upload file declaration is invalid")
            observed.add(path)
            total += size
            files.append({"path": path, "byte_size": size, "sha256": digest, "chunks": []})
        if total > MAX_UPLOAD_TOTAL_BYTES:
            raise ProviderUploadError("upload total size exceeds the bounded contract")
        ttl = payload.get("ttl_seconds")
        if (
            not isinstance(ttl, int)
            or isinstance(ttl, bool)
            or not MIN_UPLOAD_TTL_SECONDS <= ttl <= MAX_UPLOAD_TTL_SECONDS
        ):
            raise ProviderUploadError("upload TTL is outside the bounded contract")
        now = int(self.clock())
        request = {
            **self._binding(arguments, principal),
            "upload_id": upload_id,
            "task_id": task_id,
            "effect_id": effect_id,
            "idempotency_key": idempotency_key,
            "title": title,
            "disposable": disposable,
            "files": [{key: item[key] for key in ("path", "byte_size", "sha256")} for item in files],
            "ttl_seconds": ttl,
        }
        request_sha256 = hashlib.sha256(canonical_json_bytes(request)).hexdigest()
        state = {
            "schema_version": UPLOAD_SCHEMA,
            **request,
            "files": files,
            "request_sha256": request_sha256,
            "state": "READY" if total == 0 else "OPEN",
            "total_bytes": total,
            "created_at": now,
            "updated_at": now,
            "expires_at": now + ttl,
        }
        with self._root_lock():
            receipt = self._terminal_receipt(upload_id)
            if receipt is not None:
                if receipt.get("request_sha256") != request_sha256:
                    raise ProviderUploadConflict(
                        "upload_id replay differs from its exact start request"
                    )
                self._assert_binding(receipt, arguments, principal)
                self._remove_orphan_active_directory(upload_id)
                return self._public(receipt)
            directory = self._directory(upload_id)
            if directory.exists() or directory.is_symlink():
                with self._lock(upload_id) as locked:
                    prior = _read_json(locked / "state.json")
                    if prior.get("request_sha256") != request_sha256:
                        raise ProviderUploadConflict(
                            "upload_id replay differs from its exact start request"
                        )
                    self._assert_binding(prior, arguments, principal)
                    return self._public(prior)
            self._admit_upload(total=total, principal=principal)
            with self._lock(upload_id, create=True) as locked:
                chunks = locked / "chunks"
                chunks.mkdir(mode=0o700)
                chunks.chmod(0o700)
                _atomic_json(locked / "state.json", state)
        return self._public(state)

    def _load_active(
        self, directory: Path, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]:
        state = _read_json(directory / "state.json")
        if state.get("schema_version") != UPLOAD_SCHEMA:
            raise ProviderUploadConflict("upload state contract is invalid")
        self._assert_binding(state, arguments, principal)
        if int(state.get("expires_at", 0)) <= int(self.clock()):
            self._terminal(directory, state, "EXPIRED")
            raise ProviderUploadExpired("upload expired and staged bytes were removed")
        if state.get("state") not in {"OPEN", "READY", "FINALIZING"}:
            raise ProviderUploadConflict("upload is terminal")
        return state

    @staticmethod
    def _find_file(state: Mapping[str, Any], path: str) -> dict[str, Any]:
        for item in state.get("files", []):
            if item.get("path") == path:
                return item
        raise ProviderUploadError("chunk path is absent from the upload manifest")

    def put_chunk(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping):
            raise ProviderUploadError("upload chunk payload is invalid")
        if set(payload) != {
            "upload_id",
            "task_id",
            "path",
            "offset",
            "encoding",
            "content_base64",
            "byte_size",
            "sha256",
        } or payload.get("encoding") != "base64":
            raise ProviderUploadError("upload chunk payload is not exact")
        upload_id = self._upload_id(payload.get("upload_id"))
        path = _safe_path(payload.get("path"))
        offset = payload.get("offset")
        size = payload.get("byte_size")
        digest = payload.get("sha256")
        armored = payload.get("content_base64")
        if (
            not isinstance(offset, int)
            or isinstance(offset, bool)
            or offset < 0
            or not isinstance(size, int)
            or isinstance(size, bool)
            or not 1 <= size <= MAX_UPLOAD_CHUNK_BYTES
            or not isinstance(digest, str)
            or len(digest) != 64
            or any(character not in "0123456789abcdef" for character in digest)
            or not isinstance(armored, str)
        ):
            raise ProviderUploadError("upload chunk declaration is invalid")
        try:
            content = b64decode(armored, validate=True)
        except (TypeError, ValueError) as exc:
            raise ProviderUploadError("upload chunk is not canonical base64") from exc
        if (
            b64encode(content).decode("ascii") != armored
            or len(content) != size
            or not hmac.compare_digest(hashlib.sha256(content).hexdigest(), digest)
        ):
            raise ProviderUploadError("upload chunk size or sha256 differs from its bytes")
        if self._read_receipt(upload_id, arguments, principal) is not None:
            raise ProviderUploadConflict("upload is already terminal")
        with self._lock(upload_id) as directory:
            state = self._load_active(directory, arguments, principal)
            item = self._find_file(state, path)
            received = sum(int(chunk["byte_size"]) for chunk in item["chunks"])
            if offset > received or offset + size > int(item["byte_size"]):
                raise ProviderUploadConflict("upload chunk offset is not the next bounded offset")
            key = hashlib.sha256(path.encode()).hexdigest()
            chunks_root = directory / "chunks"
            _require_private_real_directory(directory)
            _require_private_real_directory(chunks_root)
            chunk_directory = chunks_root / key
            if chunk_directory.is_symlink() or (
                chunk_directory.exists() and not chunk_directory.is_dir()
            ):
                raise ProviderUploadConflict("upload chunk directory is invalid")
            chunk_directory.mkdir(mode=0o700, exist_ok=True)
            chunk_directory.chmod(0o700)
            _require_private_real_directory(chunk_directory)
            chunk_path = chunk_directory / f"{offset:020d}-{digest}.part"
            if offset < received:
                expected = next(
                    (
                        chunk
                        for chunk in item["chunks"]
                        if int(chunk["offset"]) == offset
                    ),
                    None,
                )
                if (
                    expected is None
                    or expected.get("byte_size") != size
                    or expected.get("sha256") != digest
                    or not chunk_path.is_file()
                    or chunk_path.is_symlink()
                    or not hmac.compare_digest(_read_private_file(chunk_path), content)
                ):
                    self._terminal(directory, state, "QUARANTINED")
                    raise ProviderUploadConflict("upload chunk replay conflicts with staged bytes")
                return {
                    "upload_id": upload_id,
                    "task_id": state["task_id"],
                    "path": path,
                    "offset": offset,
                    "accepted_bytes": size,
                    "next_offset": received,
                    "replayed": True,
                    "file_complete": received == item["byte_size"],
                }
            conflicting = list(chunk_directory.glob(f"{offset:020d}-*.part"))
            if conflicting and chunk_path not in conflicting:
                self._terminal(directory, state, "QUARANTINED")
                raise ProviderUploadConflict("upload chunk replay conflicts with staged bytes")
            if chunk_path.exists():
                if chunk_path.is_symlink() or not hmac.compare_digest(
                    _read_private_file(chunk_path), content
                ):
                    self._terminal(directory, state, "QUARANTINED")
                    raise ProviderUploadConflict("upload chunk replay conflicts with staged bytes")
            else:
                if chunk_path.is_symlink():
                    self._terminal(directory, state, "QUARANTINED")
                    raise ProviderUploadConflict("upload chunk replay conflicts with staged bytes")
                if int(self.disk_usage(self.root).free) - size < MIN_UPLOAD_DISK_RESERVE_BYTES:
                    raise ProviderUploadError("provider upload staging disk reserve would be violated")
                _require_private_real_directory(chunks_root)
                _require_private_real_directory(chunk_directory)
                temporary = chunk_path.with_suffix(f".tmp-{os.getpid()}")
                descriptor = os.open(
                    temporary,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                )
                try:
                    with os.fdopen(descriptor, "wb") as stream:
                        stream.write(content)
                        stream.flush()
                        os.fsync(stream.fileno())
                    os.replace(temporary, chunk_path)
                    chunk_path.chmod(0o600)
                    _fsync_directory(chunk_directory)
                finally:
                    temporary.unlink(missing_ok=True)
            item["chunks"].append(
                {"offset": offset, "byte_size": size, "sha256": digest}
            )
            received += size
            state["state"] = (
                "READY"
                if all(
                    sum(int(chunk["byte_size"]) for chunk in current["chunks"])
                    == int(current["byte_size"])
                    for current in state["files"]
                )
                else "OPEN"
            )
            state["updated_at"] = int(self.clock())
            _atomic_json(directory / "state.json", state)
            return {
                "upload_id": upload_id,
                "task_id": state["task_id"],
                "path": path,
                "offset": offset,
                "accepted_bytes": size,
                "next_offset": received,
                "replayed": False,
                "file_complete": received == item["byte_size"],
            }

    def status(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping):
            raise ProviderUploadError("upload status payload is invalid")
        if set(payload) != {"upload_id", "task_id"}:
            raise ProviderUploadError("upload status payload is not exact")
        upload_id = self._upload_id(payload.get("upload_id"))
        receipt = self._read_receipt(upload_id, arguments, principal)
        if receipt is not None:
            return self._public(receipt)
        with self._lock(upload_id) as directory:
            state = self._load_active(directory, arguments, principal)
            return self._public(state)

    def _assemble(self, directory: Path, state: Mapping[str, Any]) -> Path:
        _require_private_real_directory(directory)
        chunks_root = directory / "chunks"
        _require_private_real_directory(chunks_root)
        assembled = directory / "assembled"
        if assembled.is_symlink() or assembled.exists():
            if assembled.is_symlink() or not assembled.is_dir():
                raise ProviderUploadConflict("assembled upload directory is invalid")
            shutil.rmtree(assembled)
        assembled.mkdir(mode=0o700)
        assembled.chmod(0o700)
        _require_private_real_directory(assembled)
        for item in state["files"]:
            path = _safe_path(item["path"])
            chunks = item["chunks"]
            expected_offset = 0
            parts = PurePosixPath(path).parts
            parent = assembled
            for part in parts[:-1]:
                parent = parent / part
                if parent.is_symlink() or (parent.exists() and not parent.is_dir()):
                    raise ProviderUploadConflict("assembled upload path is invalid")
                parent.mkdir(mode=0o700, exist_ok=True)
                parent.chmod(0o700)
                _require_private_real_directory(parent)
            destination = parent / parts[-1]
            digest = hashlib.sha256()
            _require_private_real_directory(parent)
            descriptor = os.open(
                destination,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                0o600,
            )
            try:
                with os.fdopen(descriptor, "wb") as stream:
                    for chunk in chunks:
                        if int(chunk["offset"]) != expected_offset:
                            raise ProviderUploadConflict("upload chunks are not contiguous")
                        key = hashlib.sha256(path.encode()).hexdigest()
                        chunk_directory = chunks_root / key
                        _require_private_real_directory(chunks_root)
                        _require_private_real_directory(chunk_directory)
                        chunk_path = chunk_directory / (
                            f"{expected_offset:020d}-{chunk['sha256']}.part"
                        )
                        if chunk_path.is_symlink() or not chunk_path.is_file():
                            raise ProviderUploadConflict("upload chunk is missing")
                        content = _read_private_file(chunk_path)
                        if (
                            len(content) != int(chunk["byte_size"])
                            or hashlib.sha256(content).hexdigest() != chunk["sha256"]
                        ):
                            raise ProviderUploadConflict("upload chunk was tampered with")
                        stream.write(content)
                        digest.update(content)
                        expected_offset += len(content)
                    stream.flush()
                    os.fsync(stream.fileno())
            except Exception:
                destination.unlink(missing_ok=True)
                raise
            if expected_offset != int(item["byte_size"]) or digest.hexdigest() != item["sha256"]:
                raise ProviderUploadConflict("assembled upload size or sha256 differs from its manifest")
        return assembled

    def finalize(
        self,
        arguments: Mapping[str, Any],
        principal: AccessIdentity,
        callback: Callable[[Mapping[str, Any], Path, AccessIdentity], Mapping[str, Any]],
    ) -> dict[str, Any]:
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping):
            raise ProviderUploadError("upload finalize payload is invalid")
        if set(payload) != {"upload_id", "task_id"}:
            raise ProviderUploadError("upload finalize payload is not exact")
        upload_id = self._upload_id(payload.get("upload_id"))
        receipt = self._read_receipt(upload_id, arguments, principal)
        if receipt is not None:
            if receipt.get("state") != "FINALIZED":
                raise ProviderUploadConflict("upload is already terminal")
            return self._public(receipt)
        with self._lock(upload_id) as directory:
            state = self._load_active(directory, arguments, principal)
            if state.get("state") not in {"READY", "FINALIZING"}:
                raise ProviderUploadError("upload is incomplete")
            try:
                assembled = self._assemble(directory, state)
                state["state"] = "FINALIZING"
                state["updated_at"] = int(self.clock())
                _atomic_json(directory / "state.json", state)
                result = dict(callback(state, assembled, principal))
            except ProviderUploadConflict:
                if directory.exists() and (directory / "state.json").exists():
                    self._terminal(directory, state, "QUARANTINED")
                raise
            except Exception:
                if directory.exists() and (directory / "state.json").exists():
                    state["state"] = "READY"
                    state["updated_at"] = int(self.clock())
                    _atomic_json(directory / "state.json", state)
                    shutil.rmtree(directory / "assembled", ignore_errors=True)
                raise
            state["state"] = "FINALIZED"
            state["updated_at"] = int(self.clock())
            state["result"] = result
            receipt_value = {**state, "schema_version": RECEIPT_SCHEMA}
            receipt_value["files"] = [
                {
                    **{key: item[key] for key in ("path", "byte_size", "sha256")},
                    "received_bytes": item["byte_size"],
                }
                for item in state["files"]
            ]
            _atomic_json(self._receipt_path(upload_id), receipt_value)
        self._remove_orphan_active_directory(upload_id)
        return self._public(receipt_value)

    def abort(self, arguments: Mapping[str, Any], principal: AccessIdentity) -> dict[str, Any]:
        payload = arguments.get("payload")
        if not isinstance(payload, Mapping):
            raise ProviderUploadError("upload abort payload is invalid")
        if set(payload) != {"upload_id", "task_id"}:
            raise ProviderUploadError("upload abort payload is not exact")
        upload_id = self._upload_id(payload.get("upload_id"))
        receipt = self._read_receipt(upload_id, arguments, principal)
        if receipt is not None:
            if receipt.get("state") == "FINALIZED":
                raise ProviderUploadConflict("finalized upload cannot be aborted")
            return self._public(receipt)
        with self._lock(upload_id) as directory:
            state = self._load_active(directory, arguments, principal)
            self._terminal(directory, state, "ABORTED")
            receipt_value = _read_json(self._receipt_path(upload_id))
        return self._public(receipt_value)

    def _terminal(self, directory: Path, state: Mapping[str, Any], terminal: str) -> None:
        upload_id = str(state["upload_id"])
        if self._terminal_receipt(upload_id) is not None:
            self._remove_orphan_active_directory(upload_id)
            return
        value = dict(state)
        value["schema_version"] = RECEIPT_SCHEMA
        value["state"] = terminal
        value["updated_at"] = int(self.clock())
        value["files"] = [
            {
                **{key: item[key] for key in ("path", "byte_size", "sha256")},
                "received_bytes": sum(
                    int(chunk["byte_size"]) for chunk in item.get("chunks", [])
                ),
            }
            for item in state.get("files", [])
        ]
        _atomic_json(self._receipt_path(upload_id), value)
        shutil.rmtree(directory, ignore_errors=True)
        _fsync_directory(self.uploads)

    def reap_expired(self) -> dict[str, int]:
        now = int(self.clock())
        expired = 0
        receipts_removed = 0
        with self._root_lock():
            for directory in list(self.uploads.iterdir()):
                if directory.is_symlink() or not directory.is_dir():
                    continue
                try:
                    upload_id = self._upload_id(directory.name)
                    if self._terminal_receipt(upload_id) is not None:
                        self._remove_orphan_active_directory(upload_id)
                        continue
                    with self._lock(upload_id) as locked:
                        if self._terminal_receipt(upload_id) is not None:
                            self._remove_orphan_active_directory(upload_id)
                            continue
                        state = _read_json(locked / "state.json")
                        if int(state.get("expires_at", 0)) <= now:
                            self._terminal(locked, state, "EXPIRED")
                            expired += 1
                except (FileNotFoundError, ProviderUploadError):
                    continue
            for receipt in list(self.receipts.glob("*.json")):
                if receipt.is_symlink() or not receipt.is_file():
                    continue
                try:
                    value = _read_json(receipt)
                    if int(value.get("updated_at", 0)) + RECEIPT_RETENTION_SECONDS <= now:
                        receipt.unlink()
                        _fsync_directory(self.receipts)
                        receipts_removed += 1
                except ProviderUploadError:
                    continue
        return {"expired_uploads": expired, "receipts_removed": receipts_removed}
