#!/usr/bin/env python3
"""Seal two exact read-only YDB scans into an owner-only protected artifact."""

from __future__ import annotations

import argparse
import json
import os
import shutil
import stat
import sys
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

from my_data_hub.hashing import sha256_file
from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.protected_artifact import (
    DATA_NAME,
    MANIFEST_NAME,
    RECEIPT_NAME,
    ObservedInventoryBinding,
    ProtectedArtifactError,
    ProtectedExportManifest,
    ProtectedExportReceipt,
    ReaderPrincipalBinding,
    ScanReceipt,
    SourceBinding,
    load_protected_artifact,
    scan_evidence,
    scan_rows,
)
from my_data_hub.workloads.bloggers.schema import (
    SOURCE_COLUMNS,
    SOURCE_DATABASE_ID,
    SOURCE_DATABASE_PATH,
    SOURCE_TABLE,
)
from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot


def _utc_now() -> datetime:
    return datetime.now(UTC)


def _datetime(value: str, field: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProtectedArtifactError(f"{field} is not an ISO timestamp") from exc
    if parsed.tzinfo is None:
        raise ProtectedArtifactError(f"{field} must be timezone-aware")
    return parsed.astimezone(UTC)


def _json_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ProtectedArtifactError(f"{path.name} must contain one JSON object")
    return value


def load_observed_inventory(path: Path) -> ObservedInventoryBinding:
    """Bind the full scan to independently observed, source-identified accounting."""

    source_path = path.expanduser().resolve(strict=True)
    raw = _json_object(source_path)
    source = raw.get("source")
    aggregate = raw.get("successful_live_aggregate")
    if not isinstance(source, dict) or not isinstance(aggregate, dict):
        raise ProtectedArtifactError("inventory receipt lacks source/full aggregate evidence")
    if (
        source.get("database_id") != SOURCE_DATABASE_ID
        or source.get("database_path") != SOURCE_DATABASE_PATH
        or source.get("table") != SOURCE_TABLE
        or aggregate.get("sample_or_limit_clause_used") is not False
    ):
        raise ProtectedArtifactError("inventory receipt is not the exact unbounded Region Talk source")
    statuses = aggregate.get("confirmation_status_counts")
    if not isinstance(statuses, dict):
        raise ProtectedArtifactError("inventory receipt lacks confirmation accounting")
    return ObservedInventoryBinding(
        receipt_schema_version=raw.get("schema_version"),
        receipt_sha256=sha256_file(source_path),
        observed_at=aggregate.get("read_at"),
        row_count=aggregate.get("row_count"),
        distinct_record_ids=aggregate.get("distinct_record_ids"),
        batch_count=aggregate.get("batch_count"),
        source_file_count=aggregate.get("source_file_count"),
        confirmation_status_counts=statuses,
    )


def load_reader_binding(
    path: Path,
    *,
    service_account_id: str,
    observed_at: datetime,
    write_denial_verified_at: datetime,
) -> ReaderPrincipalBinding:
    """Require the live database binding snapshot to contain viewer and nothing else."""

    source_path = path.expanduser().resolve(strict=True)
    raw = json.loads(source_path.read_bytes())
    if not isinstance(raw, list):
        raise ProtectedArtifactError("access binding evidence must be the yc JSON list")
    roles = tuple(
        sorted(
            item.get("role_id")
            for item in raw
            if isinstance(item, dict)
            and isinstance(item.get("subject"), dict)
            and item["subject"].get("id") == service_account_id
        )
    )
    return ReaderPrincipalBinding(
        service_account_id=service_account_id,
        access_bindings_observed_at=observed_at,
        access_bindings_sha256=sha256_file(source_path),
        database_roles=roles,
        write_denial_verified=True,
        write_denial_verified_at=write_denial_verified_at,
    )


def _protected_root(path: Path) -> Path:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise ProtectedArtifactError("output root must not be a symlink")
    root = expanded.resolve()
    root.mkdir(parents=True, exist_ok=True, mode=0o700)
    if root.is_symlink() or not root.is_dir():
        raise ProtectedArtifactError("output root must be a real directory")
    mode = stat.S_IMODE(root.stat().st_mode)
    if mode != 0o700:
        raise ProtectedArtifactError(f"output root mode must be 0700, observed {mode:04o}")
    return root


def _token(path: Path) -> str:
    expanded = path.expanduser()
    if expanded.is_symlink():
        raise ProtectedArtifactError("IAM token source must not be a symlink")
    source = expanded.resolve(strict=True)
    if not source.is_file() or stat.S_IMODE(source.stat().st_mode) != 0o600:
        raise ProtectedArtifactError("IAM token file must be a mode-0600 regular file")
    value = source.read_text(encoding="utf-8").strip()
    if not value:
        raise ProtectedArtifactError("IAM token file is empty")
    return value


def _retryable(error: BaseException) -> bool:
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


def _scan_once(driver: Any, rows_path: Path) -> tuple[ScanReceipt, datetime, datetime]:
    started_at = _utc_now()
    snapshot = YdbBloggerSnapshot(driver)
    with rows_path.open("xb") as handle:
        os.chmod(rows_path, 0o600)
        with snapshot.iter_rows() as rows:
            receipt = scan_rows(rows, handle)
        handle.flush()
        os.fsync(handle.fileno())
    return receipt, started_at, _utc_now()


def _write_json(path: Path, value: dict[str, Any]) -> None:
    with path.open("x", encoding="utf-8", newline="\n") as handle:
        os.chmod(path, 0o600)
        handle.write(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n")
        handle.flush()
        os.fsync(handle.fileno())


def _export(
    args: argparse.Namespace,
    token: str,
    directory: Path,
    inventory: ObservedInventoryBinding,
) -> ProtectedExportReceipt:
    import ydb

    primary_path = directory / DATA_NAME
    verification_path = directory / "rows-verification.jsonl"
    with ydb.Driver(
        endpoint=args.endpoint,
        database=args.database,
        credentials=ydb.AccessTokenCredentials(token),
    ) as driver:
        driver.wait(timeout=args.connect_timeout_seconds, fail_fast=True)
        snapshot = YdbBloggerSnapshot(driver)
        snapshot.assert_write_denied()
        write_denial_at = _utc_now()
        primary, primary_started, primary_completed = _scan_once(driver, primary_path)
        verification, verification_started, verification_completed = _scan_once(
            driver, verification_path
        )

    primary_evidence = scan_evidence(
        primary,
        path=primary_path,
        started_at=primary_started,
        completed_at=primary_completed,
    )
    verification_evidence = scan_evidence(
        verification,
        path=verification_path,
        started_at=verification_started,
        completed_at=verification_completed,
    )
    if primary_evidence.content_binding != verification_evidence.content_binding:
        raise ProtectedArtifactError("independent ordered source scans differ")
    reader = load_reader_binding(
        args.access_bindings_json,
        service_account_id=args.reader_service_account_id,
        observed_at=args.access_bindings_observed_at,
        write_denial_verified_at=write_denial_at,
    )
    snapshot_at = primary_started
    export_batch_id = batch_identity(snapshot_at, inventory.row_count)
    manifest = ProtectedExportManifest(
        export_batch_id=export_batch_id,
        snapshot_at=snapshot_at,
        created_at=_utc_now(),
        source=SourceBinding(columns=SOURCE_COLUMNS, source_revision=args.source_revision),
        inventory=inventory,
        principal=reader,
        primary_scan=primary_evidence,
        verification_scan=verification_evidence,
        data_file={
            "row_count": primary_evidence.row_count,
            "byte_size": primary_evidence.byte_size,
            "sha256": primary_evidence.file_sha256,
        },
    )
    verification_path.unlink()
    manifest_path = directory / MANIFEST_NAME
    _write_json(manifest_path, manifest.model_dump(mode="json"))
    receipt = ProtectedExportReceipt(
        created_at=_utc_now(),
        manifest_sha256=sha256_file(manifest_path),
        export_batch_id=export_batch_id,
        snapshot_at=snapshot_at,
        source_revision=args.source_revision,
        row_count=primary_evidence.row_count,
        logical_sha256=primary_evidence.logical_sha256,
        record_id_set_sha256=primary_evidence.record_id_set_sha256,
        data_file_sha256=primary_evidence.file_sha256,
    )
    _write_json(directory / RECEIPT_NAME, receipt.model_dump(mode="json"))
    validated = load_protected_artifact(manifest_path)
    if validated.receipt != receipt:
        raise ProtectedArtifactError("sealed receipt changed during final validation")
    return receipt


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--endpoint", required=True)
    parser.add_argument("--database", default=SOURCE_DATABASE_PATH)
    parser.add_argument("--reader-service-account-id", required=True)
    parser.add_argument("--iam-token-file", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--inventory-receipt", required=True, type=Path)
    parser.add_argument("--access-bindings-json", required=True, type=Path)
    parser.add_argument(
        "--access-bindings-observed-at",
        required=True,
        type=lambda value: _datetime(value, "access binding observed_at"),
    )
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
    root = _protected_root(args.output_root)
    token = _token(args.iam_token_file)
    inventory = load_observed_inventory(args.inventory_receipt)
    final_error: Exception | None = None
    for attempt in range(1, args.max_attempts + 1):
        export_batch_hint = UUID(bytes=os.urandom(16), version=4)
        directory = root / f"region-talk-ydb-attempt-{export_batch_hint}"
        directory.mkdir(mode=0o700)
        try:
            receipt = _export(args, token, directory, inventory)
            final_directory = root / f"region-talk-ydb-{receipt.export_batch_id}"
            directory.rename(final_directory)
            print(
                json.dumps(
                    {
                        "status": "COMPLETE_PROTECTED_EXPORT",
                        "directory": str(final_directory),
                        "row_count": receipt.row_count,
                        "logical_sha256": receipt.logical_sha256,
                        "manifest_sha256": receipt.manifest_sha256,
                        "export_batch_id": str(receipt.export_batch_id),
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
