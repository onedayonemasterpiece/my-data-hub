"""Read-only YDB snapshot reader and metadata-only two-scan evidence."""

from __future__ import annotations

import hashlib
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

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
    assert_query_identity,
)

ZERO_ROW_WRITE_DENIAL_PROBE = (
    "UPDATE `region_talk_external_blogger_evidence` SET blogger_name = blogger_name "
    'WHERE record_id = "__my_data_hub_permission_probe_never_matches__";'
)
DENIAL_REQUEST_TIMEOUT_SECONDS = 10
SNAPSHOT_REQUEST_TIMEOUT_SECONDS = 30
SOURCE_READ_RECEIPT_SCHEMA = "my-data-hub.region-talk-ydb-source-read-receipt.v1"
MAX_BLOGGER_SOURCE_ROWS = 100_000


def _utc(value: datetime, field: str) -> datetime:
    if value.tzinfo is None:
        raise ValueError(f"{field} must be timezone-aware")
    return value.astimezone(UTC)


def _set_sha256(values: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for value in sorted(set(values)):
        encoded = value.encode()
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


class BloggerYdbScanEvidence(BaseModel):
    """Bounded metadata from one ordered scan; never contains a source value."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    started_at: datetime
    completed_at: datetime
    row_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    distinct_record_ids: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    logical_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    record_id_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    batch_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    batch_id_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    source_file_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    source_file_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    confirmation_status_counts: dict[str, int] = Field(min_length=1, max_length=32)
    min_updated_at: datetime
    max_updated_at: datetime

    @model_validator(mode="after")
    def exact_accounting(self) -> BloggerYdbScanEvidence:
        if _utc(self.completed_at, "completed_at") < _utc(self.started_at, "started_at"):
            raise ValueError("scan timestamps are reversed")
        if _utc(self.max_updated_at, "max_updated_at") < _utc(self.min_updated_at, "min_updated_at"):
            raise ValueError("source timestamps are reversed")
        if self.distinct_record_ids != self.row_count:
            raise ValueError("scan record identities are not exact")
        if self.batch_count > self.row_count or self.source_file_count > self.row_count:
            raise ValueError("scan source-set accounting exceeds row count")
        if sum(self.confirmation_status_counts.values()) != self.row_count:
            raise ValueError("scan confirmation accounting differs from row count")
        if any(not key or value < 0 for key, value in self.confirmation_status_counts.items()):
            raise ValueError("scan confirmation accounting is invalid")
        return self

    @property
    def consistency_binding(self) -> dict[str, Any]:
        return self.model_dump(exclude={"started_at", "completed_at"}, mode="json")


class BloggerYdbSourceReadReceipt(BaseModel):
    """Detached, row-free evidence from an exact provider-side YDB preflight."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[SOURCE_READ_RECEIPT_SCHEMA] = SOURCE_READ_RECEIPT_SCHEMA
    export_batch_id: UUID
    snapshot_at: datetime
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    database_id: Literal[SOURCE_DATABASE_ID] = SOURCE_DATABASE_ID
    database_path: Literal[SOURCE_DATABASE_PATH] = SOURCE_DATABASE_PATH
    table: Literal[SOURCE_TABLE] = SOURCE_TABLE
    schema_sha256: Literal[SOURCE_SCHEMA_SHA256] = SOURCE_SCHEMA_SHA256
    query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256
    reader_service_account_id: str = Field(pattern=r"^[a-z0-9]{20}$")
    database_roles: tuple[Literal["ydb.viewer"], ...]
    access_bindings_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    write_denial_verified: Literal[True] = True
    first_scan: BloggerYdbScanEvidence
    repeat_scan: BloggerYdbScanEvidence

    @model_validator(mode="after")
    def exact_binding(self) -> BloggerYdbSourceReadReceipt:
        snapshot = _utc(self.snapshot_at, "snapshot_at")
        if self.database_roles != ("ydb.viewer",):
            raise ValueError("source reader must have exactly ydb.viewer")
        if self.first_scan.consistency_binding != self.repeat_scan.consistency_binding:
            raise ValueError("independent ordered YDB scans differ")
        if self.repeat_scan.started_at < self.first_scan.completed_at:
            raise ValueError("independent ordered YDB scans overlap")
        if self.export_batch_id != batch_identity(snapshot, self.first_scan.row_count):
            raise ValueError("source read batch differs from snapshot/count binding")
        return self

    @property
    def row_count(self) -> int:
        return self.first_scan.row_count

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


def scan_ydb_rows(
    rows: Iterable[Mapping[str, Any]], *, started_at: datetime, completed_at: datetime | None = None
) -> BloggerYdbScanEvidence:
    """Hash one exact ordered scan without retaining or writing source payloads."""

    logical = hashlib.sha256()
    record_ids: set[str] = set()
    batch_ids: set[str] = set()
    source_files: set[str] = set()
    statuses: Counter[str] = Counter()
    previous: str | None = None
    row_count = 0
    minimum: datetime | None = None
    maximum: datetime | None = None
    for raw in rows:
        row = BloggerSourceRow.from_mapping(dict(raw))
        if previous is not None and row.record_id <= previous:
            raise YdbSnapshotError("source rows are not strictly ordered by record_id")
        if row_count >= MAX_BLOGGER_SOURCE_ROWS:
            raise YdbSnapshotError("source row count exceeds the bounded contract")
        encoded = row.canonical_bytes()
        logical.update(len(encoded).to_bytes(8, "big"))
        logical.update(encoded)
        record_ids.add(row.record_id)
        batch_ids.add(row.batch_id)
        source_files.add(row.source_file_sha256)
        statuses[row.confirmation_status] += 1
        previous = row.record_id
        row_count += 1
        minimum = row.updated_at if minimum is None else min(minimum, row.updated_at)
        maximum = row.updated_at if maximum is None else max(maximum, row.updated_at)
    if minimum is None or maximum is None:
        raise YdbSnapshotError("source snapshot is empty")
    return BloggerYdbScanEvidence(
        started_at=started_at,
        completed_at=completed_at or datetime.now(UTC),
        row_count=row_count,
        distinct_record_ids=len(record_ids),
        logical_sha256=logical.hexdigest(),
        record_id_set_sha256=_set_sha256(record_ids),
        batch_count=len(batch_ids),
        batch_id_set_sha256=_set_sha256(batch_ids),
        source_file_count=len(source_files),
        source_file_set_sha256=_set_sha256(source_files),
        confirmation_status_counts=dict(sorted(statuses.items())),
        min_updated_at=minimum,
        max_updated_at=maximum,
    )


class YdbSnapshotError(RuntimeError):
    """The exact read-only snapshot could not be established."""


class YdbBloggerSnapshot:
    """Consumes one QuerySnapshotReadOnly result without writing it to disk.

    The caller supplies an already-authenticated YDB driver whose principal has
    been independently verified to have only database-scoped ``ydb.viewer``.
    """

    def __init__(self, driver: Any, *, acquire_timeout_seconds: float = 20.0) -> None:
        if acquire_timeout_seconds <= 0 or acquire_timeout_seconds > 60:
            raise ValueError("session acquire timeout is outside the bounded contract")
        self.driver = driver
        self.acquire_timeout_seconds = acquire_timeout_seconds

    def assert_write_denied(self) -> None:
        """Prove the live principal cannot execute even a zero-row UPDATE.

        Only the SDK's exact UNAUTHORIZED status is accepted as evidence.
        Connectivity, syntax, timeout, and generic failures fail closed.
        """

        import ydb

        pool = ydb.QuerySessionPool(self.driver, size=1)
        try:
            with pool.checkout(timeout=self.acquire_timeout_seconds) as session:
                try:
                    responses = session.transaction(ydb.QuerySerializableReadWrite()).execute(
                        ZERO_ROW_WRITE_DENIAL_PROBE,
                        commit_tx=True,
                        settings=self._request_settings(ydb, DENIAL_REQUEST_TIMEOUT_SECONDS),
                    )
                    # Query Service responses are streaming.  Consume the
                    # iterator so a deferred UNAUTHORIZED cannot be mistaken
                    # for a successful denial probe.
                    for _response in responses:
                        pass
                except ydb.issues.Unauthorized:
                    return
                raise YdbSnapshotError("YDB viewer write-denial probe unexpectedly succeeded")
        finally:
            pool.stop(timeout=5)

    @contextmanager
    def iter_rows(self) -> Iterator[Iterator[dict[str, object]]]:
        import ydb
        from ydb import convert

        assert_query_identity(SOURCE_QUERY, SOURCE_QUERY_SHA256)
        pool = ydb.QuerySessionPool(self.driver, size=1)
        try:
            with pool.checkout(timeout=self.acquire_timeout_seconds) as session:
                tx = session.transaction(ydb.QuerySnapshotReadOnly())
                responses = tx.execute(
                    SOURCE_QUERY,
                    commit_tx=True,
                    settings=self._request_settings(ydb, SNAPSHOT_REQUEST_TIMEOUT_SECONDS),
                )
                result_sets = convert.aggregate_result_sets_by_index(responses)
                if len(result_sets) != 1:
                    raise YdbSnapshotError("blogger query returned an unexpected result-set count")

                def rows() -> Iterator[dict[str, object]]:
                    for raw in result_sets[0].rows:
                        value = raw if isinstance(raw, dict) else {name: getattr(raw, name) for name in SOURCE_COLUMNS}
                        if set(value) != set(SOURCE_COLUMNS):
                            raise YdbSnapshotError("YDB result shape differs from exact 27-column contract")
                        yield value

                yield rows()
        finally:
            pool.stop(timeout=5)

    @staticmethod
    def _request_settings(ydb: Any, timeout_seconds: int) -> Any:
        return (
            ydb.BaseRequestSettings()
            .with_timeout(timeout_seconds)
            .with_operation_timeout(timeout_seconds)
            .with_cancel_after(timeout_seconds)
        )
