from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.hashing import canonical_json_bytes, sha256_file, sha256_value
from my_data_hub.workloads.region_talk.constants import KNOWN_YDB_ROW_KINDS
from my_data_hub.workloads.region_talk.contracts import (
    ExportFile,
    MigrationReconciliationReport,
    YdbExportManifest,
    YdbExportRow,
)


class RegionTalkMigrationError(RuntimeError):
    pass


TERMINAL_DISPOSITIONS = frozenset(
    {
        "normalized",
        "deduplicated",
        "intentionally_excluded",
        "retained_raw",
        "quarantined",
    }
)


@dataclass(frozen=True, slots=True)
class ResolvedExportFile:
    contract: ExportFile
    path: Path


@dataclass(frozen=True, slots=True)
class ValidatedExport:
    manifest: YdbExportManifest
    manifest_path: Path
    files: tuple[Path, ...]
    resolved_files: tuple[ResolvedExportFile, ...]
    row_count: int
    row_kind_counts: dict[str, int]
    logical_sha256: str
    unknown_row_kinds: tuple[str, ...]


def load_manifest(path: Path) -> YdbExportManifest:
    try:
        return YdbExportManifest.model_validate_json(path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RegionTalkMigrationError(f"invalid export manifest {path}: {exc}") from exc


def resolve_export_files(
    manifest_path: Path, manifest: YdbExportManifest
) -> tuple[ResolvedExportFile, ...]:
    base = manifest_path.parent.resolve()
    resolved: list[ResolvedExportFile] = []
    for entry in manifest.files:
        path = (base / entry.path).resolve()
        try:
            path.relative_to(base)
        except ValueError as exc:
            raise RegionTalkMigrationError(
                f"manifest file escapes export directory: {entry.path}"
            ) from exc
        if not path.is_file():
            raise RegionTalkMigrationError(f"manifest file does not exist: {entry.path}")
        if path.stat().st_size != entry.byte_size:
            raise RegionTalkMigrationError(f"byte-size mismatch for {entry.path}")
        if sha256_file(path) != entry.sha256:
            raise RegionTalkMigrationError(f"SHA-256 mismatch for {entry.path}")
        resolved.append(ResolvedExportFile(contract=entry, path=path))
    return tuple(resolved)


def iter_rows(
    files: Iterable[ResolvedExportFile],
) -> Iterator[tuple[ResolvedExportFile, int, YdbExportRow]]:
    for item in files:
        with item.path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                try:
                    row = YdbExportRow.model_validate_json(line)
                except Exception as exc:
                    raise RegionTalkMigrationError(
                        f"invalid export row at {item.path}:{line_number}: {exc}"
                    ) from exc
                if row.source_table != item.contract.source_table:
                    raise RegionTalkMigrationError(
                        f"source-table mismatch at {item.path}:{line_number}: "
                        f"file={item.contract.source_table}, row={row.source_table}"
                    )
                yield item, line_number, row


def logical_row_bytes(row: YdbExportRow) -> bytes:
    identity = {
        "source_table": row.source_table,
        "source_pk": row.source_pk,
        "row_kind": row.row_kind,
        "source_updated_at": row.source_updated_at.isoformat()
        if row.source_updated_at
        else None,
        "payload_sha256": row.payload_sha256,
    }
    return canonical_json_bytes(identity) + b"\n"


def validate_export(manifest_path: Path) -> ValidatedExport:
    manifest_path = manifest_path.resolve()
    manifest = load_manifest(manifest_path)
    resolved = resolve_export_files(manifest_path, manifest)
    counts: Counter[str] = Counter()
    file_counts: Counter[Path] = Counter()
    seen_keys: set[tuple[str, str]] = set()
    total = 0
    logical_digest = hashlib.sha256()
    previous_sort_key: tuple[str, str] | None = None

    for item, line_number, row in iter_rows(resolved):
        if row.export_batch_id != manifest.export_batch_id:
            raise RegionTalkMigrationError(
                f"batch mismatch at {item.path}:{line_number}: {row.export_batch_id}"
            )
        if sha256_value(row.payload) != row.payload_sha256:
            raise RegionTalkMigrationError(
                f"payload hash mismatch at {item.path}:{line_number}"
            )
        key = (row.source_table, row.source_pk)
        if key in seen_keys:
            raise RegionTalkMigrationError(
                f"duplicate source identity in export: {row.source_table}/{row.source_pk}"
            )
        seen_keys.add(key)

        if previous_sort_key is not None and key <= previous_sort_key:
            raise RegionTalkMigrationError(
                f"export is not strictly ordered at {item.path}:{line_number}: {key}"
            )
        previous_sort_key = key

        logical_digest.update(logical_row_bytes(row))
        counts[row.row_kind] += 1
        file_counts[item.path] += 1
        total += 1

    if total != manifest.expected_row_count:
        raise RegionTalkMigrationError(
            f"row count mismatch: manifest={manifest.expected_row_count}, actual={total}"
        )
    expected_counts = dict(sorted(manifest.row_kind_counts.items()))
    actual_counts = dict(sorted(counts.items()))
    if actual_counts != expected_counts:
        raise RegionTalkMigrationError(
            f"row-kind count mismatch: manifest={expected_counts}, actual={actual_counts}"
        )
    for item in resolved:
        if file_counts[item.path] != item.contract.row_count:
            raise RegionTalkMigrationError(
                f"file row count mismatch for {item.contract.path}: "
                f"manifest={item.contract.row_count}, actual={file_counts[item.path]}"
            )
    logical_sha256 = logical_digest.hexdigest()
    if logical_sha256 != manifest.logical_sha256:
        raise RegionTalkMigrationError(
            f"logical SHA-256 mismatch: manifest={manifest.logical_sha256}, "
            f"actual={logical_sha256}"
        )
    known = set(KNOWN_YDB_ROW_KINDS)
    unknown = tuple(sorted(kind for kind in counts if kind not in known))
    return ValidatedExport(
        manifest=manifest,
        manifest_path=manifest_path,
        files=tuple(item.path for item in resolved),
        resolved_files=resolved,
        row_count=total,
        row_kind_counts=actual_counts,
        logical_sha256=logical_sha256,
        unknown_row_kinds=unknown,
    )


def raw_record_id(row: YdbExportRow) -> UUID:
    value = (
        f"my-data-hub:region-talk-ydb:{row.export_batch_id}:"
        f"{row.source_table}:{row.source_pk}"
    )
    return uuid5(NAMESPACE_URL, value)


def import_raw_export(database_url: str, validated: ValidatedExport) -> int:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RegionTalkMigrationError("psycopg is required for import") from exc

    manifest = validated.manifest
    manifest_hash = sha256_file(validated.manifest_path)
    inserted = 0
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cursor.execute(
                """
                INSERT INTO migration.export_batch (
                    export_batch_id, source_system, source_database, source_tables,
                    source_scope, schema_version, source_revision, source_code_revision,
                    consistency_mode, watermark_start, watermark_end,
                    expected_row_count, manifest_sha256, logical_sha256, status, metadata
                ) VALUES (
                    %s, %s, %s, %s::jsonb, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, 'landing', %s::jsonb
                )
                ON CONFLICT (export_batch_id) DO NOTHING
                """,
                (
                    manifest.export_batch_id,
                    manifest.source.system,
                    manifest.source.database,
                    json.dumps(manifest.source.tables, ensure_ascii=False),
                    manifest.source.scope,
                    manifest.schema_version,
                    manifest.source.source_revision,
                    manifest.source.source_code_revision,
                    manifest.consistency.mode,
                    manifest.consistency.watermark_start,
                    manifest.consistency.watermark_end,
                    manifest.expected_row_count,
                    manifest_hash,
                    manifest.logical_sha256,
                    json.dumps(manifest.metadata, ensure_ascii=False),
                ),
            )
            cursor.execute(
                """
                SELECT manifest_sha256, logical_sha256, expected_row_count, source_tables
                FROM migration.export_batch WHERE export_batch_id = %s
                """,
                (manifest.export_batch_id,),
            )
            existing_batch = cursor.fetchone()
            if existing_batch is None:
                raise RegionTalkMigrationError("export_batch insert/readback failed")
            expected_batch = (
                manifest_hash,
                manifest.logical_sha256,
                manifest.expected_row_count,
                manifest.source.tables,
            )
            actual_batch = (
                str(existing_batch[0]),
                str(existing_batch[1]),
                int(existing_batch[2]),
                list(existing_batch[3]),
            )
            if actual_batch != expected_batch:
                raise RegionTalkMigrationError(
                    "conflicting export_batch replay: manifest identity changed"
                )

            for row_kind, count in manifest.row_kind_counts.items():
                cursor.execute(
                    """
                    INSERT INTO migration.export_batch_kind (
                        export_batch_id, row_kind, expected_row_count
                    ) VALUES (%s, %s, %s)
                    ON CONFLICT (export_batch_id, row_kind) DO NOTHING
                    """,
                    (manifest.export_batch_id, row_kind, count),
                )
            cursor.execute(
                """
                SELECT row_kind, expected_row_count
                FROM migration.export_batch_kind
                WHERE export_batch_id = %s
                ORDER BY row_kind
                """,
                (manifest.export_batch_id,),
            )
            actual_kinds = {
                str(row_kind): int(count) for row_kind, count in cursor.fetchall()
            }
            if actual_kinds != manifest.row_kind_counts:
                raise RegionTalkMigrationError(
                    "conflicting export_batch_kind replay: row-kind counts changed"
                )

            for entry in manifest.files:
                cursor.execute(
                    """
                    INSERT INTO migration.export_file (
                        export_batch_id, relative_path, source_table,
                        sha256, row_count, byte_size
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (export_batch_id, relative_path) DO NOTHING
                    """,
                    (
                        manifest.export_batch_id,
                        entry.path,
                        entry.source_table,
                        entry.sha256,
                        entry.row_count,
                        entry.byte_size,
                    ),
                )
            cursor.execute(
                """
                SELECT relative_path, source_table, sha256, row_count, byte_size
                FROM migration.export_file
                WHERE export_batch_id = %s
                ORDER BY relative_path
                """,
                (manifest.export_batch_id,),
            )
            actual_files = [
                (str(path), str(table), str(digest), int(rows), int(size))
                for path, table, digest, rows, size in cursor.fetchall()
            ]
            expected_files = sorted(
                (
                    entry.path,
                    entry.source_table,
                    entry.sha256,
                    entry.row_count,
                    entry.byte_size,
                )
                for entry in manifest.files
            )
            if actual_files != expected_files:
                raise RegionTalkMigrationError(
                    "conflicting export_file replay: file manifest changed"
                )

            for _item, _line_number, row in iter_rows(validated.resolved_files):
                record_id = raw_record_id(row)
                cursor.execute(
                    """
                    INSERT INTO migration.raw_record (
                        raw_record_id, export_batch_id, source_table, source_pk,
                        row_kind, source_updated_at, payload, payload_sha256
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                    ON CONFLICT (export_batch_id, source_table, source_pk) DO NOTHING
                    RETURNING raw_record_id
                    """,
                    (
                        record_id,
                        row.export_batch_id,
                        row.source_table,
                        row.source_pk,
                        row.row_kind,
                        row.source_updated_at,
                        json.dumps(row.payload, ensure_ascii=False),
                        row.payload_sha256,
                    ),
                )
                if cursor.fetchone() is not None:
                    inserted += 1
                    continue
                cursor.execute(
                    """
                    SELECT row_kind, payload_sha256
                    FROM migration.raw_record
                    WHERE export_batch_id = %s AND source_table = %s AND source_pk = %s
                    """,
                    (row.export_batch_id, row.source_table, row.source_pk),
                )
                existing = cursor.fetchone()
                if existing is None or tuple(existing) != (row.row_kind, row.payload_sha256):
                    raise RegionTalkMigrationError(
                        f"conflicting replay for {row.source_table}/{row.source_pk}"
                    )

            cursor.execute(
                "SELECT count(*) FROM migration.raw_record WHERE export_batch_id = %s",
                (manifest.export_batch_id,),
            )
            raw_count = int(cursor.fetchone()[0])
            if raw_count != manifest.expected_row_count:
                raise RegionTalkMigrationError(
                    f"raw database count mismatch: expected={manifest.expected_row_count}, "
                    f"actual={raw_count}"
                )
            cursor.execute(
                """
                UPDATE migration.export_batch
                SET status = 'landed', completed_at = now()
                WHERE export_batch_id = %s
                """,
                (manifest.export_batch_id,),
            )
        connection.commit()
    return inserted


def build_reconciliation_accounting(
    *, expected_by_kind: dict[str, int], actual_rows: Iterable[tuple[str, str | None]]
) -> list[dict[str, int | str | bool]]:
    """Build deterministic row-kind accounting for reports and MCP output."""
    actual: dict[str, Counter[str]] = {}
    normalized_expected: dict[str, int] = {}
    for row_kind, count in expected_by_kind.items():
        if not row_kind:
            raise RegionTalkMigrationError("expected row kind must not be empty")
        parsed_count = int(count)
        if parsed_count < 0:
            raise RegionTalkMigrationError("expected row count must not be negative")
        normalized_expected[str(row_kind)] = parsed_count
    for row_kind, disposition in actual_rows:
        if not row_kind:
            raise RegionTalkMigrationError("actual row kind must not be empty")
        key = disposition or "undispositioned"
        if key != "undispositioned" and key not in TERMINAL_DISPOSITIONS:
            raise RegionTalkMigrationError(f"unknown migration disposition: {key}")
        actual.setdefault(str(row_kind), Counter())[key] += 1
    result: list[dict[str, int | str | bool]] = []
    for row_kind in sorted(set(normalized_expected) | set(actual)):
        counts = actual.get(row_kind, Counter())
        raw = sum(counts.values())
        expected = normalized_expected.get(row_kind, 0)
        undispositioned = counts["undispositioned"]
        quarantined = counts["quarantined"]
        fully_accounted = raw == expected and undispositioned == 0
        result.append(
            {
                "row_kind": row_kind,
                "expected": expected,
                "raw": raw,
                "normalized": counts["normalized"],
                "deduplicated": counts["deduplicated"],
                "intentionally_excluded": counts["intentionally_excluded"],
                "retained_raw": counts["retained_raw"],
                "quarantined": quarantined,
                "undispositioned": undispositioned,
                "raw_matches_expected": raw == expected,
                "fully_accounted": fully_accounted,
                "cutover_ready": fully_accounted and quarantined == 0,
            }
        )
    return result


def reconciliation_blocking_findings(
    accounting: Iterable[dict[str, int | str | bool]],
) -> list[dict[str, Any]]:
    findings: list[dict[str, Any]] = []
    for row in accounting:
        reasons: list[str] = []
        if not bool(row["raw_matches_expected"]):
            reasons.append("raw_count_mismatch")
        if int(row["undispositioned"]) > 0:
            reasons.append("undispositioned_rows")
        if int(row["quarantined"]) > 0:
            reasons.append("quarantined_rows")
        if reasons:
            findings.append(
                {
                    "row_kind": str(row["row_kind"]),
                    "reasons": reasons,
                    "accounting": dict(row),
                }
            )
    return findings


def reconciliation_report_from_database(
    database_url: str, export_batch_id: UUID
) -> dict[str, Any]:
    try:
        import psycopg
    except ImportError as exc:  # pragma: no cover
        raise RegionTalkMigrationError("psycopg is required for reconciliation") from exc

    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
                SELECT row_kind, expected_row_count
                FROM migration.export_batch_kind
                WHERE export_batch_id = %s
                ORDER BY row_kind
                """,
            (export_batch_id,),
        )
        expected = {str(kind): int(count) for kind, count in cursor.fetchall()}
        cursor.execute(
            """
                SELECT raw.row_kind, disp.disposition
                FROM migration.raw_record raw
                LEFT JOIN migration.row_disposition disp
                  ON disp.raw_record_id = raw.raw_record_id
                WHERE raw.export_batch_id = %s
                ORDER BY raw.row_kind, raw.source_table, raw.source_pk
                """,
            (export_batch_id,),
        )
        accounting = build_reconciliation_accounting(
            expected_by_kind=expected,
            actual_rows=(
                (str(kind), str(disposition) if disposition else None)
                for kind, disposition in cursor.fetchall()
            ),
        )
        cursor.execute(
            """
                SELECT status, expected_row_count, manifest_sha256, logical_sha256,
                       source_database, source_tables, completed_at
                FROM migration.export_batch
                WHERE export_batch_id = %s
                """,
            (export_batch_id,),
        )
        batch = cursor.fetchone()
    if batch is None:
        raise RegionTalkMigrationError(f"unknown export batch: {export_batch_id}")
    blocking = reconciliation_blocking_findings(accounting)
    if batch[3] is None:
        raise RegionTalkMigrationError(
            f"export batch has no logical SHA-256: {export_batch_id}"
        )
    report = MigrationReconciliationReport.model_validate(
        {
            "schema_version": "migration-reconciliation-report.v1",
            "workload": "region-talk",
            "export_batch_id": export_batch_id,
            "source": {
                "database": str(batch[4]),
                "tables": list(batch[5]),
            },
            "batch_status": str(batch[0]),
            "expected_row_count": int(batch[1]),
            "manifest_sha256": str(batch[2]),
            "logical_sha256": str(batch[3]),
            "completed_at": batch[6],
            "accounting": accounting,
            "blocking_findings": blocking,
            "passed": not blocking,
        }
    )
    return report.model_dump(mode="json")
