from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from my_data_hub.connectors.contracts import ConnectorReceipt, ReceiptStatus
from my_data_hub.connectors.repository import (
    AcceptanceDisposition,
    AcceptanceSubmission,
    ExistingAcceptance,
    QuarantineEvidence,
    ReplayDisposition,
    RepositoryDecision,
    classify_replay,
)


@dataclass(frozen=True, slots=True)
class CommitReceipt:
    batch_id: UUID
    canonical_revision: int
    outbox_id: UUID
    duplicate: bool


class PostgresConnectorAcceptanceRepository:
    """Atomic PostgreSQL intake boundary; never mutates shared canonical tables."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def _connect(self):  # type: ignore[no-untyped-def]
        import psycopg

        return psycopg.connect(self.database_url)

    def accept(self, submission: AcceptanceSubmission) -> RepositoryDecision:
        envelope = submission.validated.envelope
        source_cursor = envelope.source_cursor.model_dump(mode="json") if envelope.source_cursor else None
        correction = envelope.correction
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cursor.execute(
                """
                SELECT c.service_principal, c.status, p.enabled, p.schema_version
                FROM integration.connector c
                JOIN integration.data_product p ON p.connector_id = c.connector_id
                WHERE c.connector_id = %s AND p.data_product = %s
                FOR SHARE OF c, p
                """,
                (submission.identity.connector_id, envelope.data_product),
            )
            registration = cursor.fetchone()
            if registration is None:
                raise PermissionError("connector or data product is not registered")
            if str(registration[0]) != submission.authenticated_principal:
                raise PermissionError("authenticated service principal is not bound to connector")
            if str(registration[1]) != "active" or not bool(registration[2]):
                raise PermissionError("connector or data product is disabled")
            if str(registration[3]) != envelope.schema_version:
                raise ValueError("data product schema version is not registered")

            cursor.execute(
                """
                SELECT b.batch_id, b.payload_sha256, b.envelope_sha256, r.receipt
                FROM integration.batch b
                LEFT JOIN LATERAL (
                    SELECT receipt
                    FROM integration.receipt
                    WHERE batch_id = b.batch_id AND receipt_type = 'accepted'
                    ORDER BY created_at
                    LIMIT 1
                ) r ON true
                WHERE b.connector_id = %s AND b.idempotency_key = %s
                FOR UPDATE OF b
                """,
                (submission.identity.connector_id, submission.identity.idempotency_key),
            )
            row = cursor.fetchone()
            if row is not None:
                existing = ExistingAcceptance(
                    identity=submission.identity,
                    batch_id=UUID(str(row[0])),
                    payload_sha256=str(row[1]),
                    envelope_sha256=str(row[2]),
                    receipt=ConnectorReceipt.model_validate(row[3]),
                )
                if classify_replay(existing, submission) is ReplayDisposition.EXACT_REPLAY:
                    return RepositoryDecision(AcceptanceDisposition.REPLAYED, receipt=existing.receipt)
                quarantine_id = uuid4()
                cursor.execute(
                    """
                    INSERT INTO integration.quarantine (
                        quarantine_id, batch_id, connector_id, idempotency_key, reason_code,
                        expected_sha256, observed_sha256, exact_envelope
                    ) VALUES (%s, %s, %s, %s, 'conflicting_replay', %s, %s, %s)
                    """,
                    (
                        quarantine_id,
                        existing.batch_id,
                        submission.identity.connector_id,
                        submission.identity.idempotency_key,
                        existing.payload_sha256,
                        submission.payload_sha256,
                        submission.exact_bytes,
                    ),
                )
                cursor.execute(
                    """
                    INSERT INTO integration.batch_event (
                        batch_id, event_type, actor_principal, correlation_id, details
                    ) VALUES (%s, 'conflicting_replay', %s, %s, %s::jsonb)
                    """,
                    (
                        existing.batch_id,
                        submission.authenticated_principal,
                        submission.correlation_id,
                        json.dumps(
                            {
                                "quarantine_id": str(quarantine_id),
                                "incoming_batch_id": str(submission.batch_id),
                                "incoming_envelope_sha256": submission.envelope_sha256,
                            }
                        ),
                    ),
                )
                evidence = QuarantineEvidence(
                    quarantine_id=quarantine_id,
                    reason="conflicting_replay",
                    identity=submission.identity,
                    incoming_batch_id=submission.batch_id,
                    existing_batch_id=existing.batch_id,
                    incoming_payload_sha256=submission.payload_sha256,
                    existing_payload_sha256=existing.payload_sha256,
                    incoming_envelope_sha256=submission.envelope_sha256,
                    existing_envelope_sha256=existing.envelope_sha256,
                )
                return RepositoryDecision(AcceptanceDisposition.QUARANTINED, quarantine=evidence)

            accepted_at = datetime.now(UTC)
            receipt = ConnectorReceipt(
                receipt_id=uuid4(),
                status=ReceiptStatus.ACCEPTED,
                connector_id=submission.identity.connector_id,
                batch_id=submission.batch_id,
                idempotency_key=submission.identity.idempotency_key,
                payload_sha256=submission.payload_sha256,
                envelope_sha256=submission.envelope_sha256,
                accepted_at=accepted_at,
            )
            cursor.execute(
                """
                INSERT INTO integration.batch (
                    batch_id, connector_id, data_product, idempotency_key, contract_version,
                    schema_version, payload_sha256, envelope_sha256, record_count,
                    delivery_mode, producer_partition, period_start, period_end, source_cursor,
                    authenticated_principal, correlation_id, supersedes_batch_id,
                    correction_reason, status, accepted_at
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb,
                    %s, %s, %s, %s, 'accepted', %s
                )
                """,
                (
                    submission.batch_id,
                    submission.identity.connector_id,
                    envelope.data_product,
                    submission.identity.idempotency_key,
                    envelope.contract_version,
                    envelope.schema_version,
                    submission.payload_sha256,
                    submission.envelope_sha256,
                    envelope.record_count,
                    "inline" if envelope.inline_records is not None else "artifact",
                    envelope.source_cursor.partition if envelope.source_cursor else None,
                    envelope.observed_period.start,
                    envelope.observed_period.end,
                    json.dumps(source_cursor) if source_cursor is not None else None,
                    submission.authenticated_principal,
                    submission.correlation_id,
                    correction.supersedes_batch_id if correction else None,
                    correction.reason if correction else None,
                    accepted_at,
                ),
            )
            cursor.execute(
                """
                INSERT INTO integration.batch_payload (
                    batch_id, exact_envelope, inline_payload, artifact_reference, byte_size
                ) VALUES (%s, %s, %s::jsonb, %s::jsonb, %s)
                """,
                (
                    submission.batch_id,
                    submission.exact_bytes,
                    json.dumps(envelope.inline_records) if envelope.inline_records is not None else None,
                    json.dumps(envelope.artifact.model_dump(mode="json")) if envelope.artifact else None,
                    len(submission.exact_bytes),
                ),
            )
            cursor.execute(
                """
                INSERT INTO integration.batch_event (
                    batch_id, event_type, actor_principal, correlation_id, details
                ) VALUES (%s, 'accepted', %s, %s, %s::jsonb)
                """,
                (
                    submission.batch_id,
                    submission.authenticated_principal,
                    submission.correlation_id,
                    json.dumps(
                        {
                            "payload_sha256": submission.payload_sha256,
                            "envelope_sha256": submission.envelope_sha256,
                            "exact_bytes_sha256": submission.exact_bytes_sha256,
                        }
                    ),
                ),
            )
            cursor.execute(
                """
                INSERT INTO integration.receipt (
                    receipt_id, batch_id, receipt_type, connector_id, idempotency_key,
                    payload_sha256, correlation_id, receipt, created_at
                ) VALUES (%s, %s, 'accepted', %s, %s, %s, %s, %s::jsonb, %s)
                """,
                (
                    receipt.receipt_id,
                    submission.batch_id,
                    submission.identity.connector_id,
                    submission.identity.idempotency_key,
                    submission.payload_sha256,
                    submission.correlation_id,
                    json.dumps(receipt.model_dump(mode="json")),
                    accepted_at,
                ),
            )
        return RepositoryDecision(AcceptanceDisposition.ACCEPTED, receipt=receipt)

    def get_receipt(self, batch_id: UUID) -> ConnectorReceipt | None:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT receipt FROM integration.receipt
                WHERE batch_id = %s AND receipt_type IN ('accepted', 'duplicate')
                ORDER BY created_at LIMIT 1
                """,
                (batch_id,),
            )
            row = cursor.fetchone()
        return ConnectorReceipt.model_validate(row[0]) if row else None

    def health(self, connector_id: str) -> dict[str, Any]:
        with self._connect() as connection, connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT c.status, max(b.accepted_at), max(b.committed_at),
                       count(*) FILTER (WHERE b.status = 'accepted'),
                       count(*) FILTER (WHERE b.status = 'canonical_committed')
                FROM integration.connector c
                LEFT JOIN integration.batch b ON b.connector_id = c.connector_id
                WHERE c.connector_id = %s
                GROUP BY c.status
                """,
                (connector_id,),
            )
            row = cursor.fetchone()
        if row is None:
            raise LookupError("connector is not registered")
        return {
            "connector_id": connector_id,
            "status": str(row[0]),
            "last_accepted_at": row[1].isoformat() if row[1] else None,
            "last_committed_at": row[2].isoformat() if row[2] else None,
            "accepted_uncommitted": int(row[3]),
            "committed": int(row[4]),
        }


class PostgresDailyStatisticsCommitter:
    """Local canonical committer for the bounded R1 daily-statistics products."""

    def __init__(self, database_url: str) -> None:
        self.database_url = database_url

    def commit(self, batch_id: UUID) -> CommitReceipt:
        import psycopg

        with psycopg.connect(self.database_url) as connection, connection.cursor() as cursor:
            cursor.execute("SET TRANSACTION ISOLATION LEVEL SERIALIZABLE")
            cursor.execute(
                """
                SELECT b.data_product, b.status, b.correlation_id, p.inline_payload,
                       b.source_cursor, b.period_end
                FROM integration.batch b
                JOIN integration.batch_payload p ON p.batch_id = b.batch_id
                WHERE b.batch_id = %s
                FOR UPDATE OF b
                """,
                (batch_id,),
            )
            row = cursor.fetchone()
            if row is None:
                raise LookupError("accepted connector batch was not found")
            cursor.execute(
                "SELECT canonical_revision FROM integration.daily_statistic WHERE batch_id = %s",
                (batch_id,),
            )
            existing = cursor.fetchone()
            if existing is not None:
                cursor.execute(
                    "SELECT outbox_id FROM sync.external_outbox WHERE idempotency_key = %s",
                    (f"connector-commit:{batch_id}",),
                )
                outbox = cursor.fetchone()
                return CommitReceipt(batch_id, int(existing[0]), UUID(str(outbox[0])), True)
            if str(row[1]) != "accepted":
                raise ValueError(f"batch is not committable from status {row[1]}")
            if str(row[0]) not in {
                "synthetic.daily-statistics.v1",
                "events-bot.daily-statistics.v1",
            }:
                raise ValueError("no bounded R1 normalizer is registered for data product")
            records = row[3]
            if not isinstance(records, list) or len(records) != 1 or not isinstance(records[0], dict):
                raise ValueError("daily-statistics payload must contain exactly one object")
            record = records[0]
            counters = record.get("counts")
            if not isinstance(counters, dict) or not all(
                isinstance(key, str) and isinstance(value, int) and value >= 0
                for key, value in counters.items()
            ):
                raise ValueError("daily-statistics counts must be non-negative integer values")
            reporting_date = record.get("reporting_date")
            timezone = record.get("timezone")
            source_revision = record.get("source_revision")
            if not all(isinstance(value, str) and value for value in (reporting_date, timezone, source_revision)):
                raise ValueError("daily-statistics identity fields are required")

            cursor.execute(
                """
                UPDATE hub.canonical_state
                SET canonical_revision = canonical_revision + 1, updated_at = now()
                WHERE singleton = true
                RETURNING canonical_revision
                """
            )
            canonical_revision = int(cursor.fetchone()[0])
            cursor.execute(
                """
                INSERT INTO integration.daily_statistic (
                    batch_id, data_product, reporting_date, timezone, source_revision,
                    counters, canonical_revision
                ) VALUES (%s, %s, %s::date, %s, %s, %s::jsonb, %s)
                """,
                (
                    batch_id,
                    str(row[0]),
                    reporting_date,
                    timezone,
                    source_revision,
                    json.dumps(counters, sort_keys=True),
                    canonical_revision,
                ),
            )
            cursor.execute(
                """
                UPDATE integration.batch
                SET status = 'canonical_committed', committed_at = now()
                WHERE batch_id = %s
                """,
                (batch_id,),
            )
            source_cursor = row[4] if isinstance(row[4], dict) else {}
            partition = str(source_cursor.get("partition") or "")
            cursor.execute(
                """
                INSERT INTO integration.watermark (
                    data_product, producer_partition, source_cursor, period_end,
                    committed_batch_id, canonical_revision
                ) VALUES (%s, %s, %s::jsonb, %s, %s, %s)
                ON CONFLICT (data_product, producer_partition) DO UPDATE
                SET source_cursor = EXCLUDED.source_cursor,
                    period_end = EXCLUDED.period_end,
                    committed_batch_id = EXCLUDED.committed_batch_id,
                    canonical_revision = EXCLUDED.canonical_revision,
                    updated_at = now()
                WHERE integration.watermark.canonical_revision < EXCLUDED.canonical_revision
                """,
                (str(row[0]), partition, json.dumps(source_cursor), row[5], batch_id, canonical_revision),
            )
            outbox_id = uuid4()
            cursor.execute(
                """
                INSERT INTO sync.external_outbox (
                    outbox_id, aggregate_type, aggregate_id, effect_kind,
                    idempotency_key, payload, required_revision
                ) VALUES (%s, 'connector.batch', %s, 'connector.batch.committed', %s, %s::jsonb, %s)
                """,
                (
                    outbox_id,
                    batch_id,
                    f"connector-commit:{batch_id}",
                    json.dumps(
                        {
                            "batch_id": str(batch_id),
                            "data_product": str(row[0]),
                            "canonical_revision": canonical_revision,
                        }
                    ),
                    canonical_revision,
                ),
            )
            cursor.execute(
                """
                INSERT INTO integration.batch_event (
                    batch_id, event_type, actor_principal, correlation_id, details
                ) VALUES (%s, 'canonical_committed', 'my-data-hub-committer', %s, %s::jsonb)
                """,
                (
                    batch_id,
                    str(row[2]),
                    json.dumps({"canonical_revision": canonical_revision, "outbox_id": str(outbox_id)}),
                ),
            )
        return CommitReceipt(batch_id, canonical_revision, outbox_id, False)
