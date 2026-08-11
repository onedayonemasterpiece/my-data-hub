"""One-transaction bounded import orchestration inside the ACTIVE master."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Iterable
from dataclasses import dataclass, replace
from datetime import datetime
from typing import Any
from uuid import UUID, uuid5

from psycopg.types.json import Jsonb

from .accounting import BloggerExportAccumulator, BloggerExportReceipt
from .postgres import PostgresBloggerWriter, WriteOutcome, canonical_outcome_hash
from .schema import (
    SOURCE_DATABASE_PATH,
    SOURCE_QUERY_SHA256,
    SOURCE_SCHEMA_SHA256,
    SOURCE_TABLE,
    BloggerSourceRow,
)
from .transform import transform_row

_BATCH_NAMESPACE = UUID("fa5115d2-39c3-5eab-b849-df13bf06cbb0")


@dataclass(frozen=True, slots=True)
class ImportReceipt:
    export: BloggerExportReceipt
    canonical_outcome_sha256: str
    actor_count: int
    account_count: int
    duplicate_group_count: int
    replayed_count: int
    canonical_revision: int
    durability_state: str = "COMMITTED_PENDING_CHECKPOINT"

    @property
    def accounting_complete(self) -> bool:
        return self.export.complete and self.duplicate_group_count == 0

    @property
    def durable_complete(self) -> bool:
        return self.durability_state == "DURABLE_COMPLETE"


def batch_identity(snapshot_at: datetime, expected_count: int) -> UUID:
    if snapshot_at.tzinfo is None or expected_count < 0:
        raise ValueError("snapshot identity is invalid")
    return uuid5(
        _BATCH_NAMESPACE,
        f"{SOURCE_DATABASE_PATH}\0{SOURCE_TABLE}\0{SOURCE_QUERY_SHA256}\0"
        f"{snapshot_at.isoformat()}\0{expected_count}",
    )


def _manifest_hash(batch_id: UUID, snapshot_at: datetime, expected_count: int) -> str:
    value = {
        "batch_id": str(batch_id),
        "consistency": "QuerySnapshotReadOnly",
        "expected_count": expected_count,
        "query_sha256": SOURCE_QUERY_SHA256,
        "schema_sha256": SOURCE_SCHEMA_SHA256,
        "snapshot_at": snapshot_at.isoformat(),
        "sort_key": "record_id",
        "source_database": SOURCE_DATABASE_PATH,
        "source_table": SOURCE_TABLE,
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class BloggerSnapshotImporter:
    """Imports the iterable without materializing it or committing partial rows."""

    def __init__(self, writer: PostgresBloggerWriter | None = None) -> None:
        self.writer = writer or PostgresBloggerWriter()

    def import_rows(
        self,
        connection: Any,
        *,
        project_id: UUID,
        snapshot_at: datetime,
        expected_row_count: int,
        rows: Iterable[dict[str, object]],
        source_code_revision: str,
    ) -> ImportReceipt:
        batch_id = batch_identity(snapshot_at, expected_row_count)
        manifest_sha = _manifest_hash(batch_id, snapshot_at, expected_row_count)
        accumulator = BloggerExportAccumulator(batch_id, snapshot_at)
        outcomes: list[WriteOutcome] = []
        try:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO migration.export_batch(
                        export_batch_id,source_system,source_database,source_tables,source_scope,
                        schema_version,source_revision,source_code_revision,consistency_mode,
                        watermark_start,watermark_end,expected_row_count,manifest_sha256,status,metadata
                    ) VALUES (%s,'ydb',%s,%s,'region-talk-bloggers-v1',%s,%s,%s,
                              'QuerySnapshotReadOnly',%s,%s,%s,%s,'landing',%s)
                    ON CONFLICT (export_batch_id) DO NOTHING
                    """,
                    (
                        batch_id,
                        SOURCE_DATABASE_PATH,
                        Jsonb([SOURCE_TABLE]),
                        SOURCE_SCHEMA_SHA256,
                        snapshot_at.isoformat(),
                        source_code_revision,
                        snapshot_at,
                        snapshot_at,
                        expected_row_count,
                        manifest_sha,
                        Jsonb({"query_sha256": SOURCE_QUERY_SHA256, "sort_key": "record_id"}),
                    ),
                )
                observed = cursor.execute(
                    "SELECT manifest_sha256,expected_row_count FROM migration.export_batch "
                    "WHERE export_batch_id=%s",
                    (batch_id,),
                ).fetchone()
                if observed != (manifest_sha, expected_row_count):
                    raise ValueError("export batch idempotency conflict")
                cursor.execute(
                    """
                    INSERT INTO migration.export_batch_kind(export_batch_id,row_kind,expected_row_count)
                    VALUES (%s,'region_talk_external_blogger_evidence',%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (batch_id, expected_row_count),
                )
                for raw in rows:
                    row = BloggerSourceRow.from_mapping(raw)
                    projection = transform_row(row)
                    outcome = self.writer.write_row(
                        cursor,
                        export_batch_id=batch_id,
                        project_id=project_id,
                        row=row,
                        projection=projection,
                    )
                    accumulator.add(row, replace(projection, disposition=outcome.disposition))
                    outcomes.append(outcome)
                export = accumulator.finish(expected_row_count=expected_row_count)
                accounting = cursor.execute(
                    """
                    SELECT raw_count,undispositioned_count,quarantined_count
                    FROM migration.batch_accounting WHERE export_batch_id=%s
                    """,
                    (batch_id,),
                ).fetchone()
                if accounting != (expected_row_count, 0, 0):
                    raise ValueError(f"canonical accounting failed: {accounting!r}")
                duplicate_count = cursor.execute(
                    "SELECT count(*) FROM migration.duplicate_group "
                    "WHERE export_batch_id=%s AND decision_status='pending'",
                    (batch_id,),
                ).fetchone()[0]
                actor_count = cursor.execute(
                    "SELECT count(*) FROM region_talk.blogger_profile WHERE export_batch_id=%s",
                    (batch_id,),
                ).fetchone()[0]
                account_count = cursor.execute(
                    """
                    SELECT count(*) FROM hub.external_account account
                    JOIN region_talk.blogger_profile profile ON profile.actor_id=account.actor_id
                    WHERE profile.export_batch_id=%s
                    """,
                    (batch_id,),
                ).fetchone()[0]
                replayed_count = sum(item.replayed for item in outcomes)
                if 0 < replayed_count < expected_row_count:
                    raise ValueError("batch is partially replayed; exact all-or-nothing import required")
                if replayed_count == expected_row_count:
                    stored_receipt = cursor.execute(
                        "SELECT metadata->>'canonical_revision', "
                        "metadata->>'canonical_outcome_sha256' FROM migration.export_batch "
                        "WHERE export_batch_id=%s",
                        (batch_id,),
                    ).fetchone()
                    if stored_receipt is None or None in stored_receipt:
                        raise ValueError("replayed batch lacks canonical revision receipt")
                    canonical_revision = int(stored_receipt[0])
                    canonical_hash = stored_receipt[1]
                else:
                    canonical_hash = canonical_outcome_hash(outcomes)
                    previous_revision = cursor.execute(
                        "SELECT canonical_revision FROM hub.canonical_state "
                        "WHERE singleton=true"
                    ).fetchone()[0]
                    canonical_revision = cursor.execute(
                        "SELECT hub.advance_canonical_revision(%s)", (previous_revision,)
                    ).fetchone()[0]
                    cursor.execute(
                        """
                        INSERT INTO sync.external_outbox(
                            aggregate_type,aggregate_id,effect_kind,idempotency_key,payload,required_revision
                        ) VALUES ('blogger_import',%s,'verified_checkpoint_required',%s,%s,%s)
                        """,
                        (
                            batch_id,
                            f"blogger-import-checkpoint:{batch_id}:{canonical_revision}",
                            Jsonb(
                                {
                                    "export_batch_id": str(batch_id),
                                    "durability_state": "COMMITTED_PENDING_CHECKPOINT",
                                }
                            ),
                            canonical_revision,
                        ),
                    )
                    cursor.execute(
                        """
                        INSERT INTO sync.audit_event(
                            actor_id,client_id,action,outcome,subject_type,subject_id,details
                        ) VALUES ('migration-operator','region-talk-ydb-bloggers-v1',
                                  'blogger_import_commit','pending_checkpoint','export_batch',%s,%s)
                        """,
                        (
                            batch_id,
                            Jsonb({"canonical_revision": canonical_revision, "row_count": expected_row_count}),
                        ),
                    )
                    cursor.execute(
                        """
                        UPDATE migration.export_batch
                        SET logical_sha256=%s,status='accepted',completed_at=clock_timestamp(),
                            metadata=metadata || %s
                        WHERE export_batch_id=%s
                        """,
                        (
                            export.logical_sha256,
                            Jsonb(
                                {
                                    "record_id_set_sha256": export.record_id_set_sha256,
                                    "canonical_outcome_sha256": canonical_hash,
                                    "duplicate_groups_pending": duplicate_count,
                                    "canonical_revision": canonical_revision,
                                }
                            ),
                            batch_id,
                        ),
                    )
        except Exception:
            connection.rollback()
            raise
        return ImportReceipt(
            export=export,
            canonical_outcome_sha256=canonical_hash,
            actor_count=actor_count,
            account_count=account_count,
            duplicate_group_count=duplicate_count,
            replayed_count=replayed_count,
            canonical_revision=canonical_revision,
        )
