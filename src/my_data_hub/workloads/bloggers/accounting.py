"""Streaming hashes and exact terminal accounting without retaining source payloads."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from .schema import SOURCE_QUERY_SHA256, BloggerSourceRow
from .transform import BloggerDisposition, BloggerProjection


@dataclass(frozen=True, slots=True)
class BloggerExportReceipt:
    export_batch_id: UUID
    exported_at: datetime
    query_sha256: str
    row_count: int
    distinct_record_ids: int
    record_id_set_sha256: str
    logical_sha256: str
    dispositions: dict[str, int]
    undispositioned: int
    source_file_count: int

    @property
    def complete(self) -> bool:
        return (
            self.row_count == self.distinct_record_ids
            and self.undispositioned == 0
            and self.dispositions.get(BloggerDisposition.QUARANTINED.value, 0) == 0
        )


class BloggerExportAccumulator:
    """Consumes rows once; stores identifiers/counters but never raw source rows."""

    def __init__(self, export_batch_id: UUID, exported_at: datetime) -> None:
        if exported_at.tzinfo is None:
            raise ValueError("exported_at must be timezone-aware")
        self.export_batch_id = export_batch_id
        self.exported_at = exported_at.astimezone(UTC)
        self._logical = hashlib.sha256()
        self._ids: set[str] = set()
        self._source_files: set[str] = set()
        self._dispositions = {item.value: 0 for item in BloggerDisposition}
        self._row_count = 0

    def add(self, row: BloggerSourceRow, projection: BloggerProjection) -> None:
        if row.record_id != projection.record_id:
            raise ValueError("projection does not belong to source row")
        if row.record_id in self._ids:
            raise ValueError("duplicate source record_id in exact snapshot")
        self._ids.add(row.record_id)
        self._source_files.add(row.source_file_sha256)
        self._logical.update(len(row.canonical_bytes()).to_bytes(8, "big"))
        self._logical.update(row.canonical_bytes())
        self._dispositions[projection.disposition.value] += 1
        self._row_count += 1

    def finish(self, *, expected_row_count: int) -> BloggerExportReceipt:
        if self._row_count != expected_row_count:
            raise ValueError(f"source row count mismatch: expected {expected_row_count}, observed {self._row_count}")
        id_digest = hashlib.sha256()
        for record_id in sorted(self._ids):
            encoded = record_id.encode("utf-8")
            id_digest.update(len(encoded).to_bytes(8, "big"))
            id_digest.update(encoded)
        dispositioned = sum(self._dispositions.values())
        return BloggerExportReceipt(
            export_batch_id=self.export_batch_id,
            exported_at=self.exported_at,
            query_sha256=SOURCE_QUERY_SHA256,
            row_count=self._row_count,
            distinct_record_ids=len(self._ids),
            record_id_set_sha256=id_digest.hexdigest(),
            logical_sha256=self._logical.hexdigest(),
            dispositions=dict(self._dispositions),
            undispositioned=self._row_count - dispositioned,
            source_file_count=len(self._source_files),
        )
