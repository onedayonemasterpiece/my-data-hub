"""Immutable checkpoint manifest creation and exact package verification."""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from typing import Any
from uuid import UUID

SCHEMA_VERSION = "my-data-hub-checkpoint-manifest.v1"
_SHA256 = re.compile(r"^[a-f0-9]{64}$")


class ManifestError(RuntimeError):
    """Checkpoint metadata or exact package bytes are invalid."""


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ManifestError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class CheckpointFile:
    path: str
    kind: str
    byte_size: int
    sha256: str

    def validate(self) -> None:
        candidate = PurePosixPath(self.path)
        if candidate.is_absolute() or ".." in candidate.parts or not candidate.parts:
            raise ManifestError(f"unsafe checkpoint path: {self.path!r}")
        if self.kind not in {"physical", "logical", "verification_receipt", "restore_smoke_receipt"}:
            raise ManifestError(f"unsupported checkpoint file kind: {self.kind}")
        if self.byte_size < 0 or not _SHA256.fullmatch(self.sha256):
            raise ManifestError(f"invalid size/hash for {self.path}")


@dataclass(frozen=True, slots=True)
class RestoreProbe:
    schema_version: int
    canonical_revision: int
    logical_hash_sha256: str
    row_counts: dict[str, int]

    def validate(self) -> None:
        if self.schema_version < 1 or self.canonical_revision < 0:
            raise ManifestError("restore probe revisions are invalid")
        if not _SHA256.fullmatch(self.logical_hash_sha256):
            raise ManifestError("restore probe logical hash is invalid")
        if any(not name or count < 0 for name, count in self.row_counts.items()):
            raise ManifestError("restore probe row counts are invalid")


@dataclass(frozen=True, slots=True)
class CheckpointManifest:
    checkpoint_id: UUID
    master_instance_id: UUID
    epoch: int
    parent_checkpoint_id: UUID | None
    postgres_version: str
    pgvector_version: str
    schema_version: int
    canonical_revision: int
    source_run_id: str
    source_identity: str
    created_at: datetime
    checkpoint_lsn: str
    files: tuple[CheckpointFile, ...]
    restore_probe: RestoreProbe
    manifest_sha256: str
    contract: str = SCHEMA_VERSION

    def unsigned_payload(self) -> dict[str, Any]:
        return {
            "contract": self.contract,
            "checkpoint_id": str(self.checkpoint_id),
            "master_instance_id": str(self.master_instance_id),
            "epoch": self.epoch,
            "parent_checkpoint_id": str(self.parent_checkpoint_id) if self.parent_checkpoint_id else None,
            "postgres_version": self.postgres_version,
            "pgvector_version": self.pgvector_version,
            "schema_version": self.schema_version,
            "canonical_revision": self.canonical_revision,
            "source_run_id": self.source_run_id,
            "source_identity": self.source_identity,
            "created_at": _utc(self.created_at, "created_at").isoformat().replace("+00:00", "Z"),
            "checkpoint_lsn": self.checkpoint_lsn,
            "files": [asdict(item) for item in self.files],
            "restore_probe": asdict(self.restore_probe),
        }

    def payload(self) -> dict[str, Any]:
        return {**self.unsigned_payload(), "manifest_sha256": self.manifest_sha256}

    def validate(self) -> None:
        if self.contract != SCHEMA_VERSION:
            raise ManifestError("unsupported checkpoint manifest contract")
        if self.epoch < 1 or self.schema_version < 1 or self.canonical_revision < 0:
            raise ManifestError("checkpoint revision/epoch is invalid")
        if not self.postgres_version.startswith("18."):
            raise ManifestError("checkpoint must originate from PostgreSQL 18")
        if not self.pgvector_version or len(self.pgvector_version) > 64:
            raise ManifestError("pgvector version is missing")
        if not self.source_run_id or not self.source_identity:
            raise ManifestError("source/run identities are required")
        if not re.fullmatch(r"[0-9A-F]+/[0-9A-F]+", self.checkpoint_lsn):
            raise ManifestError("checkpoint LSN is invalid")
        _utc(self.created_at, "created_at")
        if len(self.files) < 4:
            raise ManifestError("physical, logical, verification and restore receipts are required")
        kinds = {item.kind for item in self.files}
        required = {"physical", "logical", "verification_receipt", "restore_smoke_receipt"}
        if not required.issubset(kinds):
            raise ManifestError(f"checkpoint file kinds missing: {sorted(required - kinds)}")
        paths = [item.path for item in self.files]
        if len(paths) != len(set(paths)) or paths != sorted(paths):
            raise ManifestError("checkpoint file paths must be unique and sorted")
        for item in self.files:
            item.validate()
        self.restore_probe.validate()
        expected = digest_payload(self.unsigned_payload())
        if self.manifest_sha256 != expected:
            raise ManifestError("manifest self-hash mismatch")

    @classmethod
    def from_payload(cls, value: dict[str, Any]) -> CheckpointManifest:
        allowed = {
            "contract",
            "checkpoint_id",
            "master_instance_id",
            "epoch",
            "parent_checkpoint_id",
            "postgres_version",
            "pgvector_version",
            "schema_version",
            "canonical_revision",
            "source_run_id",
            "source_identity",
            "created_at",
            "checkpoint_lsn",
            "files",
            "restore_probe",
            "manifest_sha256",
        }
        if set(value) != allowed:
            raise ManifestError(f"manifest fields differ: {sorted(set(value) ^ allowed)}")
        try:
            probe_value = value["restore_probe"]
            manifest = cls(
                contract=str(value["contract"]),
                checkpoint_id=UUID(str(value["checkpoint_id"])),
                master_instance_id=UUID(str(value["master_instance_id"])),
                epoch=int(value["epoch"]),
                parent_checkpoint_id=(
                    UUID(str(value["parent_checkpoint_id"])) if value["parent_checkpoint_id"] else None
                ),
                postgres_version=str(value["postgres_version"]),
                pgvector_version=str(value["pgvector_version"]),
                schema_version=int(value["schema_version"]),
                canonical_revision=int(value["canonical_revision"]),
                source_run_id=str(value["source_run_id"]),
                source_identity=str(value["source_identity"]),
                created_at=datetime.fromisoformat(str(value["created_at"]).replace("Z", "+00:00")),
                checkpoint_lsn=str(value["checkpoint_lsn"]),
                files=tuple(CheckpointFile(**item) for item in value["files"]),
                restore_probe=RestoreProbe(
                    schema_version=int(probe_value["schema_version"]),
                    canonical_revision=int(probe_value["canonical_revision"]),
                    logical_hash_sha256=str(probe_value["logical_hash_sha256"]),
                    row_counts={str(key): int(count) for key, count in probe_value["row_counts"].items()},
                ),
                manifest_sha256=str(value["manifest_sha256"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ManifestError("manifest shape or value is invalid") from exc
        manifest.validate()
        return manifest


def canonical_json(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")


def digest_payload(value: object) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    *,
    package_directory: Path,
    checkpoint_id: UUID,
    master_instance_id: UUID,
    epoch: int,
    parent_checkpoint_id: UUID | None,
    postgres_version: str,
    pgvector_version: str,
    schema_version: int,
    canonical_revision: int,
    source_run_id: str,
    source_identity: str,
    created_at: datetime,
    checkpoint_lsn: str,
    file_kinds: dict[str, str],
    restore_probe: RestoreProbe,
) -> CheckpointManifest:
    root = package_directory.resolve(strict=True)
    files: list[CheckpointFile] = []
    for relative, kind in sorted(file_kinds.items()):
        pure = PurePosixPath(relative)
        if pure.is_absolute() or ".." in pure.parts:
            raise ManifestError(f"unsafe checkpoint path: {relative!r}")
        path = root.joinpath(*pure.parts)
        if path.is_symlink() or not path.is_file():
            raise ManifestError(f"checkpoint artifact must be a regular non-symlink file: {relative}")
        if path.resolve().parent != root and root not in path.resolve().parents:
            raise ManifestError(f"checkpoint artifact escapes package: {relative}")
        files.append(CheckpointFile(relative, kind, path.stat().st_size, sha256_file(path)))
    unsigned = CheckpointManifest(
        checkpoint_id=checkpoint_id,
        master_instance_id=master_instance_id,
        epoch=epoch,
        parent_checkpoint_id=parent_checkpoint_id,
        postgres_version=postgres_version,
        pgvector_version=pgvector_version,
        schema_version=schema_version,
        canonical_revision=canonical_revision,
        source_run_id=source_run_id,
        source_identity=source_identity,
        created_at=created_at,
        checkpoint_lsn=checkpoint_lsn,
        files=tuple(files),
        restore_probe=restore_probe,
        manifest_sha256="",
    )
    manifest = CheckpointManifest(
        checkpoint_id=unsigned.checkpoint_id,
        master_instance_id=unsigned.master_instance_id,
        epoch=unsigned.epoch,
        parent_checkpoint_id=unsigned.parent_checkpoint_id,
        postgres_version=unsigned.postgres_version,
        pgvector_version=unsigned.pgvector_version,
        schema_version=unsigned.schema_version,
        canonical_revision=unsigned.canonical_revision,
        source_run_id=unsigned.source_run_id,
        source_identity=unsigned.source_identity,
        created_at=unsigned.created_at,
        checkpoint_lsn=unsigned.checkpoint_lsn,
        files=unsigned.files,
        restore_probe=unsigned.restore_probe,
        manifest_sha256=digest_payload(unsigned.unsigned_payload()),
    )
    manifest.validate()
    return manifest


def write_manifest(path: Path, manifest: CheckpointManifest) -> None:
    manifest.validate()
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_bytes(canonical_json(manifest.payload()) + b"\n")
    temporary.chmod(0o600)
    temporary.replace(path)


def load_and_verify(manifest_path: Path, package_directory: Path) -> CheckpointManifest:
    if manifest_path.is_symlink() or not manifest_path.is_file():
        raise ManifestError("manifest must be a regular non-symlink file")
    try:
        value = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ManifestError("manifest is not valid JSON") from exc
    if not isinstance(value, dict):
        raise ManifestError("manifest root must be an object")
    manifest = CheckpointManifest.from_payload(value)
    root = package_directory.resolve(strict=True)
    for item in manifest.files:
        path = root.joinpath(*PurePosixPath(item.path).parts)
        resolved = path.resolve()
        if path.is_symlink() or not path.is_file() or (resolved.parent != root and root not in resolved.parents):
            raise ManifestError(f"checkpoint artifact is absent or unsafe: {item.path}")
        if path.stat().st_size != item.byte_size or sha256_file(path) != item.sha256:
            raise ManifestError(f"checkpoint artifact hash/size mismatch: {item.path}")
    return manifest
