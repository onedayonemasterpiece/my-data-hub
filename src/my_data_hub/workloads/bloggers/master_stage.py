"""Bounded FINAL-BLOGGER migration stage executed only inside the ACTIVE master.

The stage streams the exact YDB snapshot directly into the local PostgreSQL
primary.  Source rows never cross the control plane or touch a local artifact.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Literal
from urllib.parse import quote
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner, LoginPolicy

from .importer import BloggerSnapshotImporter, ImportReceipt
from .schema import SOURCE_QUERY_SHA256
from .ydb_reader import YdbBloggerSnapshot

EXPECTED_BLOGGER_ROWS = 266
BLOGGER_STAGE_SCHEMA = "my-data-hub-blogger-migration-request.v1"
BLOGGER_IMPORT_RECEIPT_SCHEMA = "region-talk-ydb-bloggers-import-receipt.v2"
MAX_REQUEST_BYTES = 16 * 1024
MAX_RECEIPT_BYTES = 64 * 1024


class BloggerMigrationRequest(BaseModel):
    """Secret-free exact request stored in the metadata-only control ledger."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-blogger-migration-request.v1"] = BLOGGER_STAGE_SCHEMA
    request_id: UUID
    operation_id: UUID
    project_id: UUID
    snapshot_at: datetime
    expected_rows: Literal[266] = EXPECTED_BLOGGER_ROWS
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256

    @model_validator(mode="after")
    def exact_snapshot(self) -> BloggerMigrationRequest:
        if self.snapshot_at.tzinfo is None:
            raise ValueError("blogger snapshot timestamp must be timezone-aware")
        return self

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class BloggerImportStageReceipt(BaseModel):
    """Sanitized commit receipt; contains accounting and identities, never rows."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-ydb-bloggers-import-receipt.v2"] = BLOGGER_IMPORT_RECEIPT_SCHEMA
    request_id: UUID
    operation_id: UUID
    master_instance_id: UUID
    run_id: str = Field(min_length=1, max_length=200)
    epoch: int = Field(ge=1)
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    export_batch_id: UUID
    source_query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256
    row_count: Literal[266]
    distinct_record_ids: Literal[266]
    source_file_count: int = Field(ge=1, le=266)
    dispositions: dict[str, int]
    record_id_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    logical_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_outcome_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    actor_count: Literal[266]
    account_count: int = Field(ge=0, le=266 * 8)
    duplicate_group_count: Literal[0] = 0
    duplicate_groups_pending: Literal[0] = 0
    undispositioned: Literal[0] = 0
    quarantined: Literal[0] = 0
    replayed_count: int = Field(ge=0, le=266)
    canonical_revision: int = Field(ge=1)
    transaction_committed: Literal[True] = True
    ydb_write_denial_verified: Literal[True] = True
    durability_state: Literal["COMMITTED_PENDING_CHECKPOINT"] = "COMMITTED_PENDING_CHECKPOINT"

    @model_validator(mode="after")
    def lossless_accounting(self) -> BloggerImportStageReceipt:
        if sum(self.dispositions.values()) != EXPECTED_BLOGGER_ROWS:
            raise ValueError("blogger dispositions do not account for all 266 rows")
        if any(not isinstance(key, str) or value < 0 for key, value in self.dispositions.items()):
            raise ValueError("blogger dispositions are invalid")
        if self.replayed_count not in {0, EXPECTED_BLOGGER_ROWS}:
            raise ValueError("blogger import is a partial replay")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True, slots=True)
class BloggerStageContext:
    identity: MasterIdentity
    request: BloggerMigrationRequest
    local_database_url: str
    lease_until: datetime


def _migration_url(base_url: str, principal: str, password: str) -> str:
    """Bind a short-lived LOGIN to the master's existing local socket URL."""

    if not base_url.startswith("postgresql://postgres@/"):
        raise ValueError("blogger stage requires the in-master local PostgreSQL socket")
    return base_url.replace(
        "postgresql://postgres@/",
        f"postgresql://{quote(principal, safe='')}:{quote(password, safe='')}@/",
        1,
    )


def _to_receipt(
    context: BloggerStageContext,
    imported: ImportReceipt,
) -> BloggerImportStageReceipt:
    export = imported.export
    quarantined = export.dispositions.get("quarantined", 0)
    return BloggerImportStageReceipt(
        request_id=context.request.request_id,
        operation_id=context.request.operation_id,
        master_instance_id=context.identity.master_instance_id,
        run_id=context.identity.run_id,
        epoch=context.identity.epoch,
        request_sha256=context.request.request_sha256,
        export_batch_id=export.export_batch_id,
        row_count=export.row_count,
        distinct_record_ids=export.distinct_record_ids,
        source_file_count=export.source_file_count,
        dispositions=export.dispositions,
        record_id_set_sha256=export.record_id_set_sha256,
        logical_sha256=export.logical_sha256,
        canonical_outcome_sha256=imported.canonical_outcome_sha256,
        actor_count=imported.actor_count,
        account_count=imported.account_count,
        duplicate_group_count=imported.duplicate_group_count,
        duplicate_groups_pending=0,
        undispositioned=export.undispositioned,
        quarantined=quarantined,
        replayed_count=imported.replayed_count,
        canonical_revision=imported.canonical_revision,
        transaction_committed=True,
        ydb_write_denial_verified=True,
    )


def execute_blogger_migration_stage(
    context: BloggerStageContext,
    *,
    owner_connection: Any,
    driver: Any | None = None,
    importer: BloggerSnapshotImporter | None = None,
    now: datetime | None = None,
) -> BloggerImportStageReceipt:
    """Execute denial probe + exact 266-row import under one epoch-bound login.

    The temporary LOGIN is created and consumed only within the master process.
    It is always dropped; no DSN is returned, persisted, logged, or handed to a
    second Kaggle notebook.
    """

    import psycopg
    import ydb

    request = context.request
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    expiry = min(observed + timedelta(minutes=5), context.lease_until.astimezone(UTC))
    if expiry <= observed + timedelta(seconds=270):
        raise RuntimeError("ACTIVE epoch lease is too short for the bounded blogger stage")
    credential_id = UUID(bytes=secrets.token_bytes(16), version=4)
    principal = f"mdh_e{context.identity.epoch}_migration_{credential_id.hex[:8]}"
    password = secrets.token_urlsafe(36)
    provisioner = CredentialProvisioner(owner_connection)
    provisioner.create(
        principal=principal,
        password=password,
        group="mdh_migration_operator",
        identity=context.identity,
        credential_id=credential_id,
        expires_at=expiry,
        now=observed,
        policy=LoginPolicy(
            statement_timeout_ms=180_000,
            lock_timeout_ms=5_000,
            idle_transaction_timeout_ms=30_000,
            connection_limit=1,
        ),
    )
    owns_driver = driver is None
    if driver is None:
        endpoint = os.environ.get("MY_DATA_HUB_YDB_ENDPOINT", "").strip()
        database = os.environ.get("MY_DATA_HUB_YDB_DATABASE", "").strip()
        if not endpoint or not database:
            provisioner.drop(principal)
            raise RuntimeError("bounded YDB endpoint/database configuration is absent")
        driver = ydb.Driver(
            endpoint=endpoint,
            database=database,
            credentials=ydb.credentials_from_env_variables(),
        )
        driver.wait(timeout=20, fail_fast=True)
    try:
        snapshot = YdbBloggerSnapshot(driver)
        snapshot.assert_write_denied()
        with psycopg.connect(
            _migration_url(context.local_database_url, principal, password), connect_timeout=5
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET ROLE mdh_migration_operator")
                cursor.execute("SET statement_timeout='180s'")
                cursor.execute("SET lock_timeout='5s'")
                cursor.execute("SET idle_in_transaction_session_timeout='30s'")
                cursor.execute("SET transaction_timeout='180s'")
            with snapshot.iter_rows() as rows:
                imported = (importer or BloggerSnapshotImporter()).import_rows(
                    connection,
                    project_id=request.project_id,
                    snapshot_at=request.snapshot_at.astimezone(UTC),
                    expected_row_count=EXPECTED_BLOGGER_ROWS,
                    rows=rows,
                    source_code_revision=request.source_revision,
                )
        return _to_receipt(context, imported)
    finally:
        if owns_driver:
            driver.stop(timeout=5)
        provisioner.drop(principal)
