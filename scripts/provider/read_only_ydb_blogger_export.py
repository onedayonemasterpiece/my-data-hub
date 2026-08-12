#!/usr/bin/env python3
"""Produce metadata-only two-scan YDB evidence without exporting source rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import stat
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.schema import SOURCE_DATABASE_PATH
from my_data_hub.workloads.bloggers.ydb_reader import (
    BloggerYdbSourceReadReceipt,
    YdbBloggerSnapshot,
    scan_ydb_rows,
)


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _private_file(path: Path, label: str) -> Path:
    source = path.expanduser()
    if source.is_symlink():
        raise ValueError(f"{label} must not be a symlink")
    resolved = source.resolve(strict=True)
    if not resolved.is_file() or stat.S_IMODE(resolved.stat().st_mode) != 0o600:
        raise ValueError(f"{label} must be a mode-0600 regular file")
    return resolved


def _token(path: Path) -> str:
    value = _private_file(path, "IAM token source").read_text(encoding="utf-8").strip()
    if not value:
        raise ValueError("IAM token file is empty")
    return value


def _reader_roles(path: Path, service_account_id: str) -> tuple[str, ...]:
    source = _private_file(path, "access binding evidence")
    raw = json.loads(source.read_bytes())
    if not isinstance(raw, list):
        raise ValueError("access binding evidence must be the yc JSON list")
    roles = tuple(
        sorted(
            item.get("role_id")
            for item in raw
            if isinstance(item, dict)
            and isinstance(item.get("subject"), dict)
            and item["subject"].get("id") == service_account_id
        )
    )
    if roles != ("ydb.viewer",):
        raise ValueError("YDB source principal must have exactly ydb.viewer")
    return roles


def _scan(driver: Any) -> Any:
    started = _utc_now()
    snapshot = YdbBloggerSnapshot(driver)
    with snapshot.iter_rows() as rows:
        return scan_ydb_rows(rows, started_at=started)


def _observe(args: argparse.Namespace, token: str) -> BloggerYdbSourceReadReceipt:
    import ydb

    roles = _reader_roles(args.access_bindings_json, args.reader_service_account_id)
    access_sha256 = hashlib.sha256(
        _private_file(args.access_bindings_json, "access binding evidence").read_bytes()
    ).hexdigest()
    with ydb.Driver(
        endpoint=args.endpoint,
        database=args.database,
        credentials=ydb.AccessTokenCredentials(token),
    ) as driver:
        driver.wait(timeout=args.connect_timeout_seconds, fail_fast=True)
        YdbBloggerSnapshot(driver).assert_write_denied()
        first = _scan(driver)
        repeat = _scan(driver)
    snapshot_at = first.started_at
    return BloggerYdbSourceReadReceipt(
        export_batch_id=batch_identity(snapshot_at, first.row_count),
        snapshot_at=snapshot_at,
        source_revision=args.source_revision,
        reader_service_account_id=args.reader_service_account_id,
        database_roles=roles,
        access_bindings_sha256=access_sha256,
        write_denial_verified=True,
        first_scan=first,
        repeat_scan=repeat,
    )


def _write_receipt(path: Path, receipt: BloggerYdbSourceReadReceipt) -> None:
    destination = path.expanduser().resolve()
    destination.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if destination.exists() or destination.is_symlink():
        raise ValueError("fresh detached receipt path is required")
    temporary = destination.with_suffix(destination.suffix + ".tmp")
    if temporary.exists() or temporary.is_symlink():
        raise ValueError("fresh detached receipt temporary path is required")
    temporary.write_bytes(canonical_json_bytes(receipt.model_dump(mode="json")) + b"\n")
    temporary.chmod(0o600)
    temporary.replace(destination)


def _retryable(error: BaseException) -> bool:
    import ydb

    return isinstance(
        error,
        (
            ydb.issues.Aborted,
            ydb.issues.Overloaded,
            ydb.issues.Unavailable,
            ydb.issues.Timeout,
            ydb.issues.ConnectionError,
        ),
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--database", default=SOURCE_DATABASE_PATH)
    parser.add_argument("--reader-service-account-id", required=True)
    parser.add_argument("--iam-token-file", required=True, type=Path)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--access-bindings-json", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
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
    if len(args.source_revision) != 40 or any(c not in "0123456789abcdef" for c in args.source_revision):
        parser.error("source-revision must be an exact lowercase 40-character Git revision")
    os.umask(0o077)
    token = _token(args.iam_token_file)
    final_error: Exception | None = None
    for attempt in range(1, args.max_attempts + 1):
        try:
            receipt = _observe(args, token)
            _write_receipt(args.receipt, receipt)
            print(
                json.dumps(
                    {
                        "status": "COMPLETE_METADATA_ONLY_YDB_READ",
                        "receipt": str(args.receipt.expanduser().resolve()),
                        "receipt_sha256": receipt.receipt_sha256,
                        "export_batch_id": str(receipt.export_batch_id),
                        "row_count": receipt.row_count,
                    },
                    sort_keys=True,
                )
            )
            return 0
        except Exception as error:
            final_error = error
            if not _retryable(error) or attempt == args.max_attempts:
                break
            delay = args.initial_backoff_seconds * (2 ** (attempt - 1))
            print(
                json.dumps(
                    {
                        "status": "RETRYING_METADATA_ONLY_YDB_READ",
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
                "status": "BLOCKED_METADATA_ONLY_YDB_READ",
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
