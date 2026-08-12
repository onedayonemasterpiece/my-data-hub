"""Primary source for the bounded read-only YDB blogger import notebook."""

from __future__ import annotations

import os
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import psycopg
import ydb

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.importer import BloggerSnapshotImporter
from my_data_hub.workloads.bloggers.ydb_reader import YdbBloggerSnapshot


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required runtime value is absent: {name}")
    return value


def main() -> int:
    endpoint = _required("MY_DATA_HUB_YDB_ENDPOINT")
    database = _required("MY_DATA_HUB_YDB_DATABASE")
    # Credentials are supplied through Kaggle User Secrets/federation.  They are
    # consumed by the official SDK and never serialized into the receipt.
    driver = ydb.Driver(endpoint=endpoint, database=database, credentials=ydb.credentials_from_env_variables())
    driver.wait(timeout=20, fail_fast=True)
    snapshot_at = datetime.fromisoformat(_required("MY_DATA_HUB_YDB_SNAPSHOT_AT").replace("Z", "+00:00"))
    expected = int(_required("MY_DATA_HUB_YDB_EXPECTED_ROWS"))
    project_id = UUID(_required("MY_DATA_HUB_REGION_TALK_PROJECT_ID"))
    try:
        with psycopg.connect(_required("MY_DATA_HUB_MASTER_MIGRATION_URL"), connect_timeout=15) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET statement_timeout='30min'")
                cursor.execute("SET lock_timeout='5s'")
                cursor.execute("SET idle_in_transaction_session_timeout='30s'")
            with YdbBloggerSnapshot(driver).iter_rows() as rows:
                receipt = BloggerSnapshotImporter().import_rows(
                    connection,
                    project_id=project_id,
                    snapshot_at=snapshot_at.astimezone(UTC),
                    expected_row_count=expected,
                    rows=rows,
                    source_code_revision=_required("MY_DATA_HUB_SOURCE_REVISION"),
                )
    finally:
        driver.stop(timeout=5)
    quarantined = receipt.export.dispositions.get("quarantined", 0)
    if not receipt.accounting_complete or quarantined or receipt.export.undispositioned:
        raise RuntimeError("blogger import accounting is not complete")
    public = {
        "schema_version": "region-talk-ydb-bloggers-import-receipt.v1",
        "export_batch_id": str(receipt.export.export_batch_id),
        "row_count": receipt.export.row_count,
        "record_id_set_sha256": receipt.export.record_id_set_sha256,
        "logical_sha256": receipt.export.logical_sha256,
        "canonical_outcome_sha256": receipt.canonical_outcome_sha256,
        "actor_count": receipt.actor_count,
        "account_count": receipt.account_count,
        "duplicate_group_count": receipt.duplicate_group_count,
        "undispositioned": receipt.export.undispositioned,
        "quarantined": quarantined,
        "canonical_revision": receipt.canonical_revision,
        "durability_state": receipt.durability_state,
    }
    Path("/kaggle/working/blogger-import-receipt.json").write_bytes(canonical_json_bytes(public))
    return 0
