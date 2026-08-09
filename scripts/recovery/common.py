from __future__ import annotations

import hashlib
import json
import os
import re
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit
from uuid import UUID

SHA256_RE = re.compile(r"^[a-f0-9]{64}$")
SAFE_LABEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/-]{0,254}$")


class RecoveryContractError(ValueError):
    """Raised when recovery evidence is incomplete or internally inconsistent."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def canonical_sha256(value: dict[str, Any], *, omit: str) -> str:
    unsigned = {key: item for key, item in value.items() if key != omit}
    encoded = json.dumps(
        unsigned, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def load_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RecoveryContractError(f"cannot read JSON object {path.name}: {exc}") from exc
    if not isinstance(value, dict):
        raise RecoveryContractError(f"{path.name} must contain a JSON object")
    return value


def atomic_write_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd, temporary_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(temporary_name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        # link(2) is atomic and, unlike replace(), fails if immutable evidence already
        # exists. The temporary file is created in the same directory/filesystem.
        os.link(temporary, path)
        temporary.unlink()
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def require_sha256(value: object, field: str) -> str:
    if not isinstance(value, str) or SHA256_RE.fullmatch(value) is None:
        raise RecoveryContractError(f"{field} must be a lowercase SHA-256 digest")
    return value


def require_timestamp(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise RecoveryContractError(f"{field} must be an RFC 3339 timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise RecoveryContractError(f"{field} must be an RFC 3339 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise RecoveryContractError(f"{field} must include a UTC offset")
    return value


def require_uuid(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise RecoveryContractError(f"{field} must be a UUID")
    try:
        UUID(value)
    except ValueError as exc:
        raise RecoveryContractError(f"{field} must be a UUID") from exc
    return value


def require_safe_label(value: object, field: str) -> str:
    if not isinstance(value, str) or SAFE_LABEL_RE.fullmatch(value) is None:
        raise RecoveryContractError(f"{field} contains unsupported characters")
    return value


def require_safe_locator(value: object) -> str:
    locator = require_safe_label(value, "object_locator")
    parsed = urlsplit(locator)
    if parsed.username or parsed.password or parsed.query or parsed.fragment:
        raise RecoveryContractError(
            "object_locator must not contain credentials, query parameters, or fragments"
        )
    return locator


def utc_now() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def validate_manifest(manifest: dict[str, Any], artifact: Path | None = None) -> None:
    required = {
        "schema_version",
        "backup_id",
        "started_at",
        "completed_at",
        "source_instance",
        "source_environment",
        "repository_commit",
        "artifact",
        "dump",
    }
    if set(manifest) != required:
        raise RecoveryContractError("backup manifest fields do not match the v2 contract")
    if manifest["schema_version"] != "my-data-hub-postgres-backup.v2":
        raise RecoveryContractError("unsupported backup manifest schema_version")
    require_uuid(manifest["backup_id"], "backup_id")
    started = require_timestamp(manifest["started_at"], "started_at")
    completed = require_timestamp(manifest["completed_at"], "completed_at")
    if datetime.fromisoformat(completed.replace("Z", "+00:00")) < datetime.fromisoformat(
        started.replace("Z", "+00:00")
    ):
        raise RecoveryContractError("completed_at precedes started_at")
    require_safe_label(manifest["source_instance"], "source_instance")
    require_safe_label(manifest["source_environment"], "source_environment")
    commit = manifest["repository_commit"]
    if not isinstance(commit, str) or re.fullmatch(r"[a-f0-9]{40,64}", commit) is None:
        raise RecoveryContractError("repository_commit must be a 40-64 character hex digest")

    artifact_data = manifest["artifact"]
    if not isinstance(artifact_data, dict) or set(artifact_data) != {
        "file_name",
        "sha256",
        "byte_size",
        "format",
        "encryption",
        "recipient_fingerprint_sha256",
    }:
        raise RecoveryContractError("manifest artifact fields do not match the v2 contract")
    require_safe_label(artifact_data["file_name"], "artifact.file_name")
    require_sha256(artifact_data["sha256"], "artifact.sha256")
    if not isinstance(artifact_data["byte_size"], int) or artifact_data["byte_size"] <= 0:
        raise RecoveryContractError("artifact.byte_size must be positive")
    if artifact_data["format"] != "pg_dump-custom":
        raise RecoveryContractError("artifact.format must be pg_dump-custom")
    if artifact_data["encryption"] != "age":
        raise RecoveryContractError("artifact.encryption must be age")
    require_sha256(
        artifact_data["recipient_fingerprint_sha256"],
        "artifact.recipient_fingerprint_sha256",
    )

    dump = manifest["dump"]
    if not isinstance(dump, dict) or set(dump) != {"pg_dump_version", "options"}:
        raise RecoveryContractError("manifest dump fields do not match the v2 contract")
    if not isinstance(dump["pg_dump_version"], str) or not dump["pg_dump_version"].strip():
        raise RecoveryContractError("dump.pg_dump_version is required")
    expected_options = ["--format=custom", "--compress=9", "--no-owner", "--no-privileges"]
    if dump["options"] != expected_options:
        raise RecoveryContractError("dump.options do not match the supported restore contract")

    if artifact is not None:
        if not artifact.is_file() or artifact.is_symlink():
            raise RecoveryContractError("encrypted artifact must be a regular, non-symlink file")
        if artifact.name != artifact_data["file_name"]:
            raise RecoveryContractError("encrypted artifact filename does not match manifest")
        if artifact.stat().st_size != artifact_data["byte_size"]:
            raise RecoveryContractError("encrypted artifact byte size does not match manifest")
        if sha256_file(artifact) != artifact_data["sha256"]:
            raise RecoveryContractError("encrypted artifact SHA-256 does not match manifest")
