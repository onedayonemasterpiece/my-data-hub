"""Validated owner-only artifacts for the bounded Region Talk blogger snapshot.

These contracts are intentionally filesystem-only.  The control plane receives
only the existing metadata request/receipts; source bytes may be consumed only by
the ACTIVE master from an owner-only directory.
"""

from __future__ import annotations

import hashlib
import json
import os
import stat
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import sha256_file

from .importer import batch_identity
from .schema import (
    SOURCE_COLUMNS,
    SOURCE_DATABASE_ID,
    SOURCE_DATABASE_PATH,
    SOURCE_QUERY,
    SOURCE_QUERY_SHA256,
    SOURCE_SCHEMA_SHA256,
    SOURCE_TABLE,
    BloggerSourceRow,
)

PROTECTED_MANIFEST_SCHEMA = "my-data-hub.region-talk-ydb-protected-export-manifest.v1"
PROTECTED_RECEIPT_SCHEMA = "my-data-hub.region-talk-ydb-protected-export-receipt.v1"
SHA256_PATTERN = r"^[0-9a-f]{64}$"
REVISION_PATTERN = r"^[0-9a-f]{40}$"
READER_SERVICE_ACCOUNT_NAME = "my-data-hub-ydb-reader"
MANIFEST_NAME = "manifest.json"
RECEIPT_NAME = "receipt.json"
DATA_NAME = "rows-000001.jsonl"


class ProtectedArtifactError(ValueError):
    """The protected export is unsafe, incomplete, stale, or tampered with."""


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _set_sha256(values: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for value in sorted(set(values)):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


@dataclass(frozen=True, slots=True)
class ScanReceipt:
    row_count: int
    distinct_record_ids: int
    logical_sha256: str
    record_id_set_sha256: str
    batch_count: int
    batch_id_set_sha256: str
    source_file_count: int
    source_file_set_sha256: str
    confirmation_status_counts: dict[str, int]
    min_updated_at: str | None
    max_updated_at: str | None

    def as_mapping(self) -> dict[str, Any]:
        return asdict(self)


class _ScanAccumulator:
    def __init__(self) -> None:
        self.logical = hashlib.sha256()
        self.record_ids: set[str] = set()
        self.batch_ids: set[str] = set()
        self.source_files: set[str] = set()
        self.statuses: Counter[str] = Counter()
        self.previous_id: str | None = None
        self.row_count = 0
        self.min_updated: datetime | None = None
        self.max_updated: datetime | None = None

    def add(self, row: BloggerSourceRow) -> bytes:
        if self.previous_id is not None and row.record_id <= self.previous_id:
            raise ProtectedArtifactError("source rows are not strictly ordered by record_id")
        if row.record_id in self.record_ids:
            raise ProtectedArtifactError("duplicate record_id in exact source snapshot")
        encoded = row.canonical_bytes()
        self.logical.update(len(encoded).to_bytes(8, "big"))
        self.logical.update(encoded)
        self.record_ids.add(row.record_id)
        self.batch_ids.add(row.batch_id)
        self.source_files.add(row.source_file_sha256)
        self.statuses[row.confirmation_status] += 1
        self.previous_id = row.record_id
        self.row_count += 1
        self.min_updated = row.updated_at if self.min_updated is None else min(self.min_updated, row.updated_at)
        self.max_updated = row.updated_at if self.max_updated is None else max(self.max_updated, row.updated_at)
        return encoded

    def finish(self) -> ScanReceipt:
        def timestamp(value: datetime | None) -> str | None:
            return value.astimezone(UTC).isoformat().replace("+00:00", "Z") if value else None

        return ScanReceipt(
            row_count=self.row_count,
            distinct_record_ids=len(self.record_ids),
            logical_sha256=self.logical.hexdigest(),
            record_id_set_sha256=_set_sha256(self.record_ids),
            batch_count=len(self.batch_ids),
            batch_id_set_sha256=_set_sha256(self.batch_ids),
            source_file_count=len(self.source_files),
            source_file_set_sha256=_set_sha256(self.source_files),
            confirmation_status_counts=dict(sorted(self.statuses.items())),
            min_updated_at=timestamp(self.min_updated),
            max_updated_at=timestamp(self.max_updated),
        )


def scan_rows(rows: Iterable[Mapping[str, Any]], output: Any | None = None) -> ScanReceipt:
    """Validate/hash one exact ordered scan, optionally writing canonical JSONL."""

    accumulator = _ScanAccumulator()
    for raw in rows:
        encoded = accumulator.add(BloggerSourceRow.from_mapping(dict(raw)))
        if output is not None:
            output.write(encoded + b"\n")
    return accumulator.finish()


class SourceBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    system: Literal["ydb"] = "ydb"
    database_id: Literal[SOURCE_DATABASE_ID] = SOURCE_DATABASE_ID
    database_path: Literal[SOURCE_DATABASE_PATH] = SOURCE_DATABASE_PATH
    table: Literal[SOURCE_TABLE] = SOURCE_TABLE
    columns: tuple[Annotated[str, Field(min_length=1, max_length=128)], ...]
    query: Literal[SOURCE_QUERY] = SOURCE_QUERY
    query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256
    schema_sha256: Literal[SOURCE_SCHEMA_SHA256] = SOURCE_SCHEMA_SHA256
    source_revision: str = Field(pattern=REVISION_PATTERN)

    @model_validator(mode="after")
    def exact_columns(self) -> SourceBinding:
        if self.columns != SOURCE_COLUMNS:
            raise ValueError("protected source columns differ from the exact contract")
        return self


class ObservedInventoryBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipt_schema_version: str = Field(min_length=1, max_length=200)
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    observed_at: datetime
    row_count: int = Field(gt=0)
    distinct_record_ids: int = Field(gt=0)
    batch_count: int = Field(gt=0)
    source_file_count: int = Field(gt=0)
    confirmation_status_counts: dict[str, int] = Field(min_length=1, max_length=32)

    @model_validator(mode="after")
    def exact_accounting(self) -> ObservedInventoryBinding:
        _utc(self.observed_at, "inventory observed_at")
        if self.distinct_record_ids != self.row_count:
            raise ValueError("inventory contains duplicate or missing record identities")
        if any(not key or value < 0 for key, value in self.confirmation_status_counts.items()):
            raise ValueError("inventory confirmation accounting is invalid")
        if sum(self.confirmation_status_counts.values()) != self.row_count:
            raise ValueError("inventory confirmation accounting differs from row_count")
        return self


class ReaderPrincipalBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    service_account_name: Literal[READER_SERVICE_ACCOUNT_NAME] = READER_SERVICE_ACCOUNT_NAME
    service_account_id: str = Field(pattern=r"^[a-z0-9]{20}$")
    access_bindings_observed_at: datetime
    access_bindings_sha256: str = Field(pattern=SHA256_PATTERN)
    database_roles: tuple[str, ...]
    credential_mode: Literal["ephemeral_iam_token_impersonation"] = (
        "ephemeral_iam_token_impersonation"
    )
    write_denial_verified: Literal[True] = True
    write_denial_verified_at: datetime

    @model_validator(mode="after")
    def viewer_only(self) -> ReaderPrincipalBinding:
        _utc(self.access_bindings_observed_at, "access binding observed_at")
        _utc(self.write_denial_verified_at, "write denial verified_at")
        if self.database_roles != ("ydb.viewer",):
            raise ValueError("protected export principal must have exactly ydb.viewer")
        return self


class ScanEvidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    started_at: datetime
    completed_at: datetime
    row_count: int = Field(gt=0)
    distinct_record_ids: int = Field(gt=0)
    file_sha256: str = Field(pattern=SHA256_PATTERN)
    byte_size: int = Field(gt=0)
    logical_sha256: str = Field(pattern=SHA256_PATTERN)
    record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    batch_count: int = Field(gt=0)
    batch_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    source_file_count: int = Field(gt=0)
    source_file_set_sha256: str = Field(pattern=SHA256_PATTERN)
    confirmation_status_counts: dict[str, int] = Field(min_length=1, max_length=32)
    min_updated_at: datetime
    max_updated_at: datetime

    @model_validator(mode="after")
    def exact_accounting(self) -> ScanEvidence:
        start = _utc(self.started_at, "scan started_at")
        end = _utc(self.completed_at, "scan completed_at")
        minimum = _utc(self.min_updated_at, "scan min_updated_at")
        maximum = _utc(self.max_updated_at, "scan max_updated_at")
        if end < start or maximum < minimum:
            raise ValueError("scan timestamps are reversed")
        if self.byte_size <= self.row_count:
            raise ValueError("scan byte size cannot contain the declared rows")
        if self.distinct_record_ids != self.row_count:
            raise ValueError("scan source identities are not exact")
        if sum(self.confirmation_status_counts.values()) != self.row_count:
            raise ValueError("scan confirmation accounting differs from row_count")
        return self

    @property
    def content_binding(self) -> dict[str, Any]:
        return self.model_dump(exclude={"started_at", "completed_at"})


class DataFileBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    path: Literal[DATA_NAME] = DATA_NAME
    mode: Literal["0600"] = "0600"
    row_count: int = Field(gt=0)
    byte_size: int = Field(gt=0)
    sha256: str = Field(pattern=SHA256_PATTERN)


class ProtectedExportManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[PROTECTED_MANIFEST_SCHEMA] = PROTECTED_MANIFEST_SCHEMA
    export_batch_id: UUID
    snapshot_at: datetime
    created_at: datetime
    source: SourceBinding
    inventory: ObservedInventoryBinding
    principal: ReaderPrincipalBinding
    consistency_mode: Literal["two_independent_QuerySnapshotReadOnly"] = (
        "two_independent_QuerySnapshotReadOnly"
    )
    ordering: tuple[Literal["record_id"], ...] = ("record_id",)
    primary_scan: ScanEvidence
    verification_scan: ScanEvidence
    data_file: DataFileBinding
    raw_data_committed_to_git: Literal[False] = False
    status: Literal["COMPLETE_PROTECTED_EXPORT"] = "COMPLETE_PROTECTED_EXPORT"

    @model_validator(mode="after")
    def exact_binding(self) -> ProtectedExportManifest:
        snapshot = _utc(self.snapshot_at, "snapshot_at")
        _utc(self.created_at, "created_at")
        if self.export_batch_id != batch_identity(snapshot, self.inventory.row_count):
            raise ValueError("export batch id differs from deterministic snapshot identity")
        if self.primary_scan.content_binding != self.verification_scan.content_binding:
            raise ValueError("independent ordered scans differ")
        if self.verification_scan.started_at < self.primary_scan.completed_at:
            raise ValueError("independent ordered scans overlap")
        scan = self.primary_scan
        if scan.row_count != self.inventory.row_count:
            raise ValueError("export count differs from the observed inventory")
        if scan.confirmation_status_counts != self.inventory.confirmation_status_counts:
            raise ValueError("export status accounting differs from the observed inventory")
        if scan.batch_count != self.inventory.batch_count:
            raise ValueError("export batch accounting differs from the observed inventory")
        if scan.source_file_count != self.inventory.source_file_count:
            raise ValueError("export source-file accounting differs from the observed inventory")
        if (
            self.data_file.row_count != scan.row_count
            or self.data_file.byte_size != scan.byte_size
            or self.data_file.sha256 != scan.file_sha256
        ):
            raise ValueError("protected data file differs from scan evidence")
        return self


class ProtectedExportReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[PROTECTED_RECEIPT_SCHEMA] = PROTECTED_RECEIPT_SCHEMA
    created_at: datetime
    manifest_path: Literal[MANIFEST_NAME] = MANIFEST_NAME
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    export_batch_id: UUID
    snapshot_at: datetime
    source_revision: str = Field(pattern=REVISION_PATTERN)
    row_count: int = Field(gt=0)
    logical_sha256: str = Field(pattern=SHA256_PATTERN)
    record_id_set_sha256: str = Field(pattern=SHA256_PATTERN)
    data_file_path: Literal[DATA_NAME] = DATA_NAME
    data_file_sha256: str = Field(pattern=SHA256_PATTERN)
    artifact_directory_mode: Literal["0700"] = "0700"
    manifest_mode: Literal["0600"] = "0600"
    receipt_mode: Literal["0600"] = "0600"
    data_file_mode: Literal["0600"] = "0600"
    raw_data_committed_to_git: Literal[False] = False
    complete: Literal[True] = True

    @model_validator(mode="after")
    def timestamps_are_aware(self) -> ProtectedExportReceipt:
        _utc(self.created_at, "receipt created_at")
        _utc(self.snapshot_at, "receipt snapshot_at")
        return self


def scan_evidence(
    receipt: ScanReceipt,
    *,
    path: Path,
    started_at: datetime,
    completed_at: datetime,
) -> ScanEvidence:
    if receipt.min_updated_at is None or receipt.max_updated_at is None:
        raise ProtectedArtifactError("a protected export must contain at least one source row")
    values = receipt.as_mapping()
    values.pop("min_updated_at")
    values.pop("max_updated_at")
    return ScanEvidence(
        started_at=started_at,
        completed_at=completed_at,
        file_sha256=sha256_file(path),
        byte_size=path.stat().st_size,
        min_updated_at=receipt.min_updated_at,
        max_updated_at=receipt.max_updated_at,
        **values,
    )


def _exact_mode(path: Path, expected: int, description: str) -> None:
    if path.is_symlink() or not path.is_file():
        raise ProtectedArtifactError(f"{description} must be a real regular file")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode != expected:
        raise ProtectedArtifactError(
            f"{description} mode must be {expected:04o}, observed {mode:04o}"
        )


def _json_object(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ProtectedArtifactError(f"invalid protected JSON file: {path.name}") from exc
    if not isinstance(value, dict):
        raise ProtectedArtifactError(f"protected JSON must be an object: {path.name}")
    return value


@dataclass(frozen=True, slots=True)
class ValidatedProtectedArtifact:
    directory: Path
    manifest_path: Path
    receipt_path: Path
    data_path: Path
    manifest: ProtectedExportManifest
    receipt: ProtectedExportReceipt

    def destroy_source_bytes(self) -> None:
        """Remove the provider-side bundle after a terminal in-master result.

        This is deliberately strict and non-recursive: an undeclared file or a
        changed path blocks deletion rather than widening the cleanup scope.
        """

        observed_names = {item.name for item in self.directory.iterdir()}
        if observed_names != {MANIFEST_NAME, RECEIPT_NAME, DATA_NAME}:
            raise ProtectedArtifactError("refusing to destroy an inexact protected artifact")
        for path in (self.data_path, self.manifest_path, self.receipt_path):
            _exact_mode(path, 0o600, f"protected cleanup file {path.name}")
            if path == self.data_path:
                with path.open("r+b", buffering=0) as handle:
                    size = path.stat().st_size
                    block = b"\0" * (1024 * 1024)
                    while size:
                        chunk = min(size, len(block))
                        handle.write(block[:chunk])
                        size -= chunk
                    handle.flush()
                    os.fsync(handle.fileno())
            path.unlink()
        self.directory.rmdir()

    def assert_import_binding(
        self, *, snapshot_at: datetime, expected_row_count: int, source_revision: str
    ) -> None:
        snapshot = _utc(snapshot_at, "import snapshot_at")
        if self.manifest.snapshot_at.astimezone(UTC) != snapshot:
            raise ProtectedArtifactError("protected artifact snapshot differs from request")
        if self.manifest.inventory.row_count != expected_row_count:
            raise ProtectedArtifactError("protected artifact count differs from request")
        if self.manifest.source.source_revision != source_revision:
            raise ProtectedArtifactError("protected artifact source revision differs from request")
        if self.manifest.export_batch_id != batch_identity(snapshot, expected_row_count):
            raise ProtectedArtifactError("protected artifact batch differs from request")

    def iter_rows(self) -> Iterator[dict[str, object]]:
        """Stream canonical rows and verify the open file before transaction exit.

        The importer consumes the iterator to exhaustion inside its PostgreSQL
        transaction.  Any concurrent or post-validation tamper changes the final
        digest/accounting and raises, causing that transaction to roll back.
        """

        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        descriptor = os.open(self.data_path, flags)
        accumulator = _ScanAccumulator()
        file_digest = hashlib.sha256()
        byte_size = 0
        try:
            opened = os.fstat(descriptor)
            if not stat.S_ISREG(opened.st_mode) or stat.S_IMODE(opened.st_mode) != 0o600:
                raise ProtectedArtifactError("opened protected data file is not mode-0600 regular data")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                for line in handle:
                    byte_size += len(line)
                    file_digest.update(line)
                    if not line.endswith(b"\n") or line == b"\n":
                        raise ProtectedArtifactError("protected JSONL has a blank or unterminated row")
                    encoded = line[:-1]
                    try:
                        value = json.loads(encoded)
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise ProtectedArtifactError("protected JSONL row is invalid") from exc
                    if not isinstance(value, dict):
                        raise ProtectedArtifactError("protected JSONL row must be an object")
                    row = BloggerSourceRow.from_mapping(value)
                    canonical = accumulator.add(row)
                    if canonical != encoded:
                        raise ProtectedArtifactError("protected JSONL row is not canonical")
                    yield row.payload()
        finally:
            if descriptor >= 0:
                os.close(descriptor)
        observed = accumulator.finish()
        expected = self.manifest.primary_scan
        if (
            file_digest.hexdigest() != expected.file_sha256
            or byte_size != expected.byte_size
            or observed.as_mapping()
            != {
                key: value
                for key, value in expected.model_dump(mode="json").items()
                if key not in {"started_at", "completed_at", "file_sha256", "byte_size"}
            }
        ):
            raise ProtectedArtifactError("protected data changed after validation")


def load_protected_artifact(manifest_path: Path) -> ValidatedProtectedArtifact:
    """Validate a sealed artifact without copying source bytes into any database."""

    expanded = manifest_path.expanduser()
    if expanded.is_symlink():
        raise ProtectedArtifactError("protected manifest must not be a symlink")
    manifest = expanded.resolve(strict=True)
    directory = manifest.parent
    if manifest.name != MANIFEST_NAME:
        raise ProtectedArtifactError(f"protected manifest must be named {MANIFEST_NAME}")
    if directory.is_symlink() or not directory.is_dir():
        raise ProtectedArtifactError("protected artifact directory must be a real directory")
    directory_mode = stat.S_IMODE(directory.stat().st_mode)
    if directory_mode != 0o700:
        raise ProtectedArtifactError(
            f"protected artifact directory mode must be 0700, observed {directory_mode:04o}"
        )
    receipt_path = directory / RECEIPT_NAME
    data_path = directory / DATA_NAME
    observed_names = {item.name for item in directory.iterdir()}
    expected_names = {MANIFEST_NAME, RECEIPT_NAME, DATA_NAME}
    if observed_names != expected_names:
        raise ProtectedArtifactError("protected artifact must contain exactly manifest, receipt, and data")
    _exact_mode(manifest, 0o600, "protected manifest")
    _exact_mode(receipt_path, 0o600, "protected receipt")
    _exact_mode(data_path, 0o600, "protected data file")
    try:
        parsed_manifest = ProtectedExportManifest.model_validate(_json_object(manifest))
        parsed_receipt = ProtectedExportReceipt.model_validate(_json_object(receipt_path))
    except ValueError as exc:
        raise ProtectedArtifactError("protected artifact contract validation failed") from exc
    manifest_sha256 = sha256_file(manifest)
    if (
        parsed_receipt.manifest_sha256 != manifest_sha256
        or parsed_receipt.export_batch_id != parsed_manifest.export_batch_id
        or parsed_receipt.snapshot_at != parsed_manifest.snapshot_at
        or parsed_receipt.source_revision != parsed_manifest.source.source_revision
        or parsed_receipt.row_count != parsed_manifest.primary_scan.row_count
        or parsed_receipt.logical_sha256 != parsed_manifest.primary_scan.logical_sha256
        or parsed_receipt.record_id_set_sha256
        != parsed_manifest.primary_scan.record_id_set_sha256
        or parsed_receipt.data_file_sha256 != parsed_manifest.data_file.sha256
        or parsed_receipt.data_file_sha256 != sha256_file(data_path)
        or parsed_manifest.data_file.byte_size != data_path.stat().st_size
    ):
        raise ProtectedArtifactError("detached receipt or protected bytes differ from manifest")
    artifact = ValidatedProtectedArtifact(
        directory=directory,
        manifest_path=manifest,
        receipt_path=receipt_path,
        data_path=data_path,
        manifest=parsed_manifest,
        receipt=parsed_receipt,
    )
    # Consume once during admission.  The import consumes and verifies again so
    # a later filesystem change fails the in-master transaction closed.
    for _row in artifact.iter_rows():
        pass
    return artifact
