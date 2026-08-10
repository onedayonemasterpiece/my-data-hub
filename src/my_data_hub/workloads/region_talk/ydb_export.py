from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections import Counter
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from my_data_hub.hashing import sha256_file, sha256_value
from my_data_hub.workloads.region_talk.constants import KNOWN_YDB_ROW_KINDS
from my_data_hub.workloads.region_talk.contracts import YdbExportManifest, YdbExportRow
from my_data_hub.workloads.region_talk.migration import logical_row_bytes, validate_export

_SAFE_TABLE_PATH = re.compile(r"^[A-Za-z0-9_./:-]+$")
_SAFE_KIND = re.compile(r"^[A-Za-z0-9_./:-]+$")


class YdbExportError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class LegacyYdbRecord:
    source_table: str
    source_pk: str
    payload: Any
    source_updated_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class ExportBundle:
    directory: Path
    manifest_path: Path
    export_batch_id: UUID
    row_count: int
    row_kind_counts: dict[str, int]
    unknown_row_kinds: tuple[str, ...]
    logical_sha256: str


def row_kind_from_pk(source_pk: str) -> str:
    prefix, separator, _remainder = source_pk.partition(":")
    if not separator:
        return "unknown_unprefixed"
    if not prefix or not _SAFE_KIND.fullmatch(prefix):
        return "unknown_invalid_prefix"
    return prefix


def _normalise_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value
    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            parsed = datetime.fromisoformat(candidate)
        except ValueError as exc:
            raise YdbExportError(f"unsupported updated_at value: {value!r}") from exc
        return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed
    raise YdbExportError(f"unsupported updated_at type: {type(value).__name__}")


def _decode_text(value: Any, *, field: str) -> str:
    if isinstance(value, bytes):
        return value.decode("utf-8")
    if isinstance(value, str):
        return value
    raise YdbExportError(f"{field} must be Utf8/String, got {type(value).__name__}")


def _decode_payload(value: Any) -> Any:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if isinstance(value, str):
        try:
            return json.loads(value)
        except json.JSONDecodeError as exc:
            raise YdbExportError("payload_json is not valid JSON") from exc
    # Some SDK/query combinations may already decode Json into Python values.
    return value


def _safe_table_path(table: str) -> str:
    if not table or not _SAFE_TABLE_PATH.fullmatch(table) or "`" in table:
        raise YdbExportError(f"unsafe YDB table path: {table!r}")
    return table


class _JsonlAttemptWriter:
    def __init__(self, path: Path, *, export_batch_id: UUID, source_table: str) -> None:
        self.path = path
        self.export_batch_id = export_batch_id
        self.source_table = source_table
        self.counts: Counter[str] = Counter()
        self.logical_digest = hashlib.sha256()
        self.previous_pk: str | None = None
        self.row_count = 0
        self._handle = path.open("w", encoding="utf-8", newline="\n")

    def append(self, record: LegacyYdbRecord) -> None:
        if record.source_table != self.source_table:
            raise YdbExportError(
                f"writer for {self.source_table} received row from {record.source_table}"
            )
        if self.previous_pk is not None and record.source_pk <= self.previous_pk:
            raise YdbExportError(
                f"YDB rows are not strictly ordered: {record.source_pk!r} after "
                f"{self.previous_pk!r}"
            )
        row_kind = row_kind_from_pk(record.source_pk)
        row = YdbExportRow(
            schema_version="region-talk-ydb-export-row.v1",
            export_batch_id=self.export_batch_id,
            source_table=record.source_table,
            source_pk=record.source_pk,
            row_kind=row_kind,
            source_updated_at=record.source_updated_at,
            payload=record.payload,
            payload_sha256=sha256_value(record.payload),
        )
        self._handle.write(
            json.dumps(
                row.model_dump(mode="json"),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            + "\n"
        )
        self.logical_digest.update(logical_row_bytes(row))
        self.counts[row_kind] += 1
        self.row_count += 1
        self.previous_pk = record.source_pk

    def close(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())
        self._handle.close()


def _create_bundle_directory(output_root: Path, export_batch_id: UUID) -> Path:
    output_root = output_root.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)
    directory = output_root / f"region-talk-ydb-{export_batch_id}"
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError as exc:
        raise YdbExportError(f"export directory already exists: {directory}") from exc
    return directory


def _finalize_bundle(
    *,
    directory: Path,
    export_batch_id: UUID,
    database: str,
    table: str,
    scope: str,
    consistency_mode: str,
    source_revision: str | None,
    source_code_revision: str | None,
    watermark_start: datetime,
    watermark_end: datetime,
    data_path: Path,
    writer: _JsonlAttemptWriter,
    metadata: dict[str, Any] | None = None,
) -> ExportBundle:
    known = set(KNOWN_YDB_ROW_KINDS)
    unknown = tuple(sorted(kind for kind in writer.counts if kind not in known))
    manifest = YdbExportManifest(
        schema_version="region-talk-ydb-export-manifest.v1",
        export_batch_id=export_batch_id,
        source={
            "system": "ydb",
            "database": database,
            "tables": [table],
            "scope": scope,
            "source_revision": source_revision,
            "source_code_revision": source_code_revision,
        },
        consistency={
            "mode": consistency_mode,
            "ordering": ["source_table", "source_pk"],
            "watermark_start": watermark_start,
            "watermark_end": watermark_end,
        },
        expected_row_count=writer.row_count,
        row_kind_counts=dict(sorted(writer.counts.items())),
        files=[
            {
                "path": data_path.name,
                "source_table": table,
                "sha256": sha256_file(data_path),
                "row_count": writer.row_count,
                "byte_size": data_path.stat().st_size,
            }
        ],
        logical_sha256=writer.logical_digest.hexdigest(),
        created_at=datetime.now(UTC),
        metadata={
            "known_row_kinds_at_export": list(KNOWN_YDB_ROW_KINDS),
            "unknown_row_kinds": list(unknown),
            **(metadata or {}),
        },
    )
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            manifest.model_dump(mode="json"),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )
    validated = validate_export(manifest_path)
    return ExportBundle(
        directory=directory,
        manifest_path=manifest_path,
        export_batch_id=export_batch_id,
        row_count=validated.row_count,
        row_kind_counts=validated.row_kind_counts,
        unknown_row_kinds=validated.unknown_row_kinds,
        logical_sha256=validated.logical_sha256,
    )


def export_records(
    records: Iterable[LegacyYdbRecord],
    *,
    output_root: Path,
    database: str,
    table: str,
    scope: str = "region-talk",
    consistency_mode: str = "consistent_snapshot",
    source_revision: str | None = None,
    source_code_revision: str | None = None,
    export_batch_id: UUID | None = None,
    metadata: dict[str, Any] | None = None,
) -> ExportBundle:
    """Create the same lossless bundle from a deterministic record stream.

    This path is used by tests and by alternative read-only exporters. The caller
    must supply rows ordered by primary key.
    """
    table = _safe_table_path(table)
    batch_id = export_batch_id or uuid4()
    directory = _create_bundle_directory(output_root, batch_id)
    data_path = directory / "rows-000001.jsonl"
    watermark_start = datetime.now(UTC)
    writer = _JsonlAttemptWriter(data_path, export_batch_id=batch_id, source_table=table)
    try:
        for record in records:
            writer.append(record)
        writer.close()
        return _finalize_bundle(
            directory=directory,
            export_batch_id=batch_id,
            database=database,
            table=table,
            scope=scope,
            consistency_mode=consistency_mode,
            source_revision=source_revision,
            source_code_revision=source_code_revision,
            watermark_start=watermark_start,
            watermark_end=datetime.now(UTC),
            data_path=data_path,
            writer=writer,
            metadata=metadata,
        )
    except Exception:
        if not writer._handle.closed:
            writer._handle.close()
        shutil.rmtree(directory, ignore_errors=True)
        raise


def export_ydb_table(
    *,
    endpoint: str,
    database: str,
    table: str,
    output_root: Path,
    page_size: int = 1000,
    scope: str = "region-talk",
    source_revision: str | None = None,
    source_code_revision: str | None = None,
    export_batch_id: UUID | None = None,
    connect_timeout_seconds: int = 20,
) -> ExportBundle:
    """Export one legacy YDB table inside one SnapshotReadOnly transaction.

    The transaction is retried as a whole by the official QuerySessionPool. Each
    retry truncates and rewrites the attempt file, so a successful manifest never
    references bytes from a failed attempt. No source mutation statement exists in
    this module.
    """
    try:
        import ydb
    except ImportError as exc:  # pragma: no cover
        raise YdbExportError("install my-data-hub[ydb] to export YDB") from exc

    if not endpoint.strip() or not database.strip():
        raise YdbExportError("YDB endpoint and database are required")
    if not 1 <= page_size <= 10000:
        raise YdbExportError("page_size must be between 1 and 10000")
    table = _safe_table_path(table)
    batch_id = export_batch_id or uuid4()
    directory = _create_bundle_directory(output_root, batch_id)
    data_path = directory / "rows-000001.jsonl"
    watermark_start = datetime.now(UTC)
    successful_writer: _JsonlAttemptWriter | None = None

    query = f"""
        DECLARE $after_pk AS Utf8;
        DECLARE $page_size AS Uint64;
        SELECT pk, payload_json, updated_at
        FROM `{table}`
        WHERE pk > $after_pk
        ORDER BY pk
        LIMIT $page_size;
    """

    try:
        with ydb.Driver(
            endpoint=endpoint,
            database=database,
            credentials=ydb.credentials_from_env_variables(),
        ) as driver:
            driver.wait(timeout=connect_timeout_seconds, fail_fast=True)
            with ydb.QuerySessionPool(driver) as pool:

                def read_snapshot(tx):  # type: ignore[no-untyped-def]
                    nonlocal successful_writer
                    if data_path.exists():
                        data_path.unlink()
                    writer = _JsonlAttemptWriter(
                        data_path,
                        export_batch_id=batch_id,
                        source_table=table,
                    )
                    try:
                        after_pk = ""
                        while True:
                            page: list[Any] = []
                            with tx.execute(
                                query,
                                parameters={"$after_pk": after_pk, "$page_size": page_size},
                            ) as results:
                                for result_set in results:
                                    page.extend(result_set.rows)
                            if not page:
                                break
                            for raw in page:
                                source_pk = _decode_text(raw["pk"], field="pk")
                                writer.append(
                                    LegacyYdbRecord(
                                        source_table=table,
                                        source_pk=source_pk,
                                        payload=_decode_payload(raw["payload_json"]),
                                        source_updated_at=_normalise_datetime(raw["updated_at"]),
                                    )
                                )
                            after_pk = writer.previous_pk or after_pk
                            if len(page) < page_size:
                                break
                        writer.close()
                        successful_writer = writer
                    except Exception:
                        if not writer._handle.closed:
                            writer._handle.close()
                        raise

                pool.retry_tx_sync(
                    read_snapshot,
                    tx_mode=ydb.QuerySnapshotReadOnly(),
                )
        if successful_writer is None:
            raise YdbExportError("YDB snapshot completed without an export writer")
        return _finalize_bundle(
            directory=directory,
            export_batch_id=batch_id,
            database=database,
            table=table,
            scope=scope,
            consistency_mode="consistent_snapshot",
            source_revision=source_revision,
            source_code_revision=source_code_revision,
            watermark_start=watermark_start,
            watermark_end=datetime.now(UTC),
            data_path=data_path,
            writer=successful_writer,
            metadata={
                "exporter": "my_data_hub.workloads.region_talk.ydb_export",
                "transaction_mode": "QuerySnapshotReadOnly",
                "page_size": page_size,
            },
        )
    except Exception:
        shutil.rmtree(directory, ignore_errors=True)
        raise
