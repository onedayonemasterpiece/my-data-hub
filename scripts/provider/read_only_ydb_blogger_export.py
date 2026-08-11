#!/usr/bin/env python3
"""Create an owner-only, read-only export of the exact Region Talk blogger table.

The command never accepts a write credential or a PostgreSQL destination.  It first
proves that the supplied YDB principal cannot execute the repository's zero-row
write-denial probe, then reads the full ordered source query twice in independent
``QuerySnapshotReadOnly`` transactions.  Only matching snapshots are sealed.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import stat
import sys
import time
from collections import Counter
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import uuid4

from my_data_hub.workloads.bloggers.schema import (
    SOURCE_COLUMNS,
    SOURCE_DATABASE_ID,
    SOURCE_DATABASE_PATH,
    SOURCE_QUERY,
    SOURCE_QUERY_SHA256,
    SOURCE_SCHEMA_SHA256,
    SOURCE_TABLE,
    BloggerSourceRow,
)
from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot


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


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _iso(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _set_sha256(values: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for value in sorted(set(values)):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


def scan_rows(rows: Iterable[dict[str, Any]], output: Any | None = None) -> ScanReceipt:
    """Validate/hash one exact ordered scan, optionally writing canonical JSONL."""

    logical = hashlib.sha256()
    record_ids: set[str] = set()
    batch_ids: set[str] = set()
    source_files: set[str] = set()
    statuses: Counter[str] = Counter()
    previous_id: str | None = None
    row_count = 0
    min_updated: datetime | None = None
    max_updated: datetime | None = None
    for raw in rows:
        row = BloggerSourceRow.from_mapping(dict(raw))
        if previous_id is not None and row.record_id <= previous_id:
            raise ValueError("source rows are not strictly ordered by record_id")
        if row.record_id in record_ids:
            raise ValueError("duplicate record_id in exact source snapshot")
        encoded = row.canonical_bytes()
        if output is not None:
            output.write(encoded + b"\n")
        logical.update(len(encoded).to_bytes(8, "big"))
        logical.update(encoded)
        record_ids.add(row.record_id)
        batch_ids.add(row.batch_id)
        source_files.add(row.source_file_sha256)
        statuses[row.confirmation_status] += 1
        previous_id = row.record_id
        row_count += 1
        min_updated = row.updated_at if min_updated is None else min(min_updated, row.updated_at)
        max_updated = row.updated_at if max_updated is None else max(max_updated, row.updated_at)
    return ScanReceipt(
        row_count=row_count,
        distinct_record_ids=len(record_ids),
        logical_sha256=logical.hexdigest(),
        record_id_set_sha256=_set_sha256(record_ids),
        batch_count=len(batch_ids),
        batch_id_set_sha256=_set_sha256(batch_ids),
        source_file_count=len(source_files),
        source_file_set_sha256=_set_sha256(source_files),
        confirmation_status_counts=dict(sorted(statuses.items())),
        min_updated_at=_iso(min_updated) if min_updated else None,
        max_updated_at=_iso(max_updated) if max_updated else None,
    )


def _protected_root(path: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise ValueError("output root must not be a symlink")
    root = expanded.resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise ValueError("output root must be a real directory")
    mode = stat.S_IMODE(root.stat().st_mode)
    if mode & 0o077:
        raise ValueError("output root must not grant group/other permissions")
    return root


def _token(path: Path) -> str:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise ValueError("IAM token source must not be a symlink")
    source = expanded.resolve(strict=True)
    if not source.is_file():
        raise ValueError("IAM token source must be a real file")
    if stat.S_IMODE(source.stat().st_mode) & 0o077:
        raise ValueError("IAM token file must be owner-only")
    value = source.read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError("IAM token file is empty")
    return value


def _retryable(error: BaseException) -> bool:
    # Import only at execution time so pure unit tests do not need the YDB extra.
    import ydb

    return isinstance(
        error,
        (
            ydb.issues.Overloaded,
            ydb.issues.Unavailable,
            ydb.issues.Timeout,
            ydb.issues.ConnectionError,
        ),
    )


def _scan_once(driver: Any, rows_path: Path | None) -> ScanReceipt:
    snapshot = YdbBloggerSnapshot(driver)
    if rows_path is None:
        with snapshot.iter_rows() as rows:
            return scan_rows(rows)
    with rows_path.open("wb") as handle:
        os.chmod(rows_path, 0o600)
        with snapshot.iter_rows() as rows:
            receipt = scan_rows(rows, handle)
        handle.flush()
        os.fsync(handle.fileno())
    return receipt


def _export(args: argparse.Namespace, token: str, directory: Path) -> dict[str, Any]:
    import ydb

    rows_path = directory / "rows-000001.jsonl"
    started = _utc_now()
    with ydb.Driver(
        endpoint=args.endpoint,
        database=args.database,
        credentials=ydb.AccessTokenCredentials(token),
    ) as driver:
        driver.wait(timeout=args.connect_timeout_seconds, fail_fast=True)
        snapshot = YdbBloggerSnapshot(driver)
        snapshot.assert_write_denied()
        first = _scan_once(driver, rows_path)
        first_completed = _utc_now()
        second = _scan_once(driver, None)
    completed = _utc_now()
    if first != second:
        raise RuntimeError("independent ordered source snapshots differ")
    file_digest = hashlib.sha256(rows_path.read_bytes()).hexdigest()
    manifest: dict[str, Any] = {
        "schema_version": "my-data-hub.region-talk-ydb-readonly-export-manifest/v1",
        "export_batch_id": str(uuid4()),
        "source": {
            "system": "ydb",
            "database_id": SOURCE_DATABASE_ID,
            "database_path": args.database,
            "endpoint": args.endpoint,
            "tables": [SOURCE_TABLE],
            "query": SOURCE_QUERY,
            "query_sha256": SOURCE_QUERY_SHA256,
            "contract_schema_sha256": SOURCE_SCHEMA_SHA256,
            "columns": list(SOURCE_COLUMNS),
            "reader_service_account_id": args.reader_service_account_id,
        },
        "consistency": {
            "transaction_mode": "QuerySnapshotReadOnly",
            "ordering": ["record_id"],
            "read_started_at": _iso(started),
            "first_snapshot_completed_at": _iso(first_completed),
            "repeat_snapshot_completed_at": _iso(completed),
            "repeat_snapshot_equal": True,
        },
        "write_denial_probe": {
            "classification": "permission_denied",
            "zero_row_predicate": True,
            "principal_write_capability": False,
        },
        "accounting": asdict(first),
        "files": [
            {
                "path": rows_path.name,
                "row_count": first.row_count,
                "byte_size": rows_path.stat().st_size,
                "sha256": file_digest,
                "mode": "0600",
            }
        ],
        "logical_sha256": first.logical_sha256,
        "raw_data_committed_to_git": False,
        "status": "COMPLETE_READONLY_EXPORT",
    }
    manifest_path = directory / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.chmod(manifest_path, 0o600)
    manifest["manifest_sha256"] = hashlib.sha256(manifest_path.read_bytes()).hexdigest()
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--database", default=SOURCE_DATABASE_PATH)
    parser.add_argument("--reader-service-account-id", required=True)
    parser.add_argument("--iam-token-file", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--max-attempts", type=int, default=4)
    parser.add_argument("--initial-backoff-seconds", type=float, default=30.0)
    parser.add_argument("--connect-timeout-seconds", type=int, default=20)
    args = parser.parse_args()
    if args.database != SOURCE_DATABASE_PATH:
        parser.error("database must equal the pinned Region Talk source database")
    if not 1 <= args.max_attempts <= 8:
        parser.error("max-attempts must be between 1 and 8")
    if not 0 <= args.initial_backoff_seconds <= 300:
        parser.error("initial backoff must be between 0 and 300 seconds")
    os.umask(0o077)
    root = _protected_root(args.output_root)
    token = _token(args.iam_token_file)
    final_error: Exception | None = None
    for attempt in range(1, args.max_attempts + 1):
        directory = root / f"region-talk-ydb-{uuid4()}"
        directory.mkdir(mode=0o700)
        try:
            manifest = _export(args, token, directory)
            print(
                json.dumps(
                    {
                        "status": manifest["status"],
                        "directory": str(directory),
                        "row_count": manifest["accounting"]["row_count"],
                        "logical_sha256": manifest["logical_sha256"],
                        "manifest_sha256": manifest["manifest_sha256"],
                    },
                    sort_keys=True,
                )
            )
            return 0
        except Exception as error:
            final_error = error
            shutil.rmtree(directory, ignore_errors=True)
            if not _retryable(error) or attempt == args.max_attempts:
                break
            delay = args.initial_backoff_seconds * (2 ** (attempt - 1))
            print(
                json.dumps(
                    {
                        "status": "RETRYING_READONLY_EXPORT",
                        "attempt": attempt,
                        "error_class": type(error).__name__,
                        "backoff_seconds": delay,
                    },
                    sort_keys=True,
                ),
                file=sys.stderr,
            )
            time.sleep(delay)
    assert final_error is not None
    print(
        json.dumps(
            {
                "status": "BLOCKED_READONLY_EXPORT",
                "attempts": args.max_attempts,
                "error_class": type(final_error).__name__,
            },
            sort_keys=True,
        ),
        file=sys.stderr,
    )
    return 75


if __name__ == "__main__":
    raise SystemExit(main())
