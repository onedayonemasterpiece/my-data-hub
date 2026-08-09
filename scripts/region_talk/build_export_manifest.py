#!/usr/bin/env python3
"""Seal already extracted Region Talk YDB JSONL files into a v1 export manifest.

This is an offline helper for data extracted by another read-only mechanism. The
preferred path is ``my-data-hub region-talk export-ydb``, which creates the rows
and manifest together inside one snapshot transaction.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

from my_data_hub.hashing import sha256_file, sha256_value
from my_data_hub.workloads.region_talk.contracts import (
    ExportConsistency,
    ExportFile,
    YdbExportManifest,
    YdbExportRow,
    YdbExportSource,
)
from my_data_hub.workloads.region_talk.migration import logical_row_bytes


def _parse_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--export-batch-id", required=True, type=UUID)
    parser.add_argument("--database", required=True)
    parser.add_argument("--scope", default="region-talk")
    parser.add_argument(
        "--mode",
        choices=["consistent_snapshot", "bounded_scan", "final_delta"],
        required=True,
    )
    parser.add_argument("--source-revision")
    parser.add_argument("--source-code-revision")
    parser.add_argument("--watermark-start")
    parser.add_argument("--watermark-end")
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("files", nargs="+", type=Path)
    args = parser.parse_args()

    output = args.output.expanduser().resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    base = output.parent
    counts: Counter[str] = Counter()
    files: list[ExportFile] = []
    logical = hashlib.sha256()
    total = 0
    seen: set[tuple[str, str]] = set()
    source_tables: set[str] = set()
    previous_identity: tuple[str, str] | None = None

    for source_path in sorted(args.files, key=lambda value: str(value)):
        path = source_path.expanduser().resolve()
        try:
            relative = path.relative_to(base)
        except ValueError as exc:
            raise SystemExit(f"input file must be below manifest directory: {path}") from exc
        row_count = 0
        file_table: str | None = None
        with path.open("r", encoding="utf-8") as handle:
            for line_number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                row = YdbExportRow.model_validate_json(line)
                if row.export_batch_id != args.export_batch_id:
                    raise SystemExit(f"batch mismatch at {path}:{line_number}")
                if sha256_value(row.payload) != row.payload_sha256:
                    raise SystemExit(f"payload hash mismatch at {path}:{line_number}")
                identity = (row.source_table, row.source_pk)
                if identity in seen:
                    raise SystemExit(f"duplicate source identity: {identity}")
                if previous_identity is not None and identity <= previous_identity:
                    raise SystemExit(
                        f"rows must be globally ordered by source_table/source_pk: "
                        f"{identity!r} after {previous_identity!r}"
                    )
                if file_table is None:
                    file_table = row.source_table
                elif row.source_table != file_table:
                    raise SystemExit(
                        f"one JSONL file may contain only one source table: {path}"
                    )
                seen.add(identity)
                previous_identity = identity
                source_tables.add(row.source_table)
                counts[row.row_kind] += 1
                row_count += 1
                total += 1
                logical.update(logical_row_bytes(row))
        if file_table is None:
            raise SystemExit(f"empty export file is not supported by v1 helper: {path}")
        files.append(
            ExportFile(
                path=relative.as_posix(),
                source_table=file_table,
                sha256=sha256_file(path),
                row_count=row_count,
                byte_size=path.stat().st_size,
            )
        )

    manifest = YdbExportManifest(
        schema_version="region-talk-ydb-export-manifest.v1",
        export_batch_id=args.export_batch_id,
        source=YdbExportSource(
            system="ydb",
            database=args.database,
            tables=sorted(source_tables),
            scope=args.scope,
            source_revision=args.source_revision,
            source_code_revision=args.source_code_revision,
        ),
        consistency=ExportConsistency(
            mode=args.mode,
            ordering=["source_table", "source_pk"],
            watermark_start=_parse_datetime(args.watermark_start),
            watermark_end=_parse_datetime(args.watermark_end),
        ),
        expected_row_count=total,
        row_kind_counts=dict(sorted(counts.items())),
        files=files,
        logical_sha256=logical.hexdigest(),
        created_at=datetime.now(UTC),
        metadata={"sealed_by": "scripts/region_talk/build_export_manifest.py"},
    )
    output.write_text(
        json.dumps(manifest.model_dump(mode="json"), ensure_ascii=False, indent=2, sort_keys=True)
        + "\n",
        encoding="utf-8",
    )
    print(
        json.dumps(
            {
                "manifest": str(output),
                "rows": total,
                "tables": sorted(source_tables),
                "logical_sha256": logical.hexdigest(),
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
