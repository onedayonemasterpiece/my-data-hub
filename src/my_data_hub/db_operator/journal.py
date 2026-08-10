"""Durable operator preview/apply journal adapters."""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping
from typing import Any, Protocol
from uuid import UUID

from my_data_hub.hashing import sha256_value


class JournalCursor(Protocol):
    def execute(self, query: str, params: tuple[object, ...] | None = None) -> Any: ...
    def fetchone(self) -> tuple[object, ...] | None: ...


class OperatorJournal(Protocol):
    def record_preview(self, payload: Mapping[str, Any], receipt: str) -> None: ...

    def find_apply(
        self, cursor: JournalCursor, *, principal: str, idempotency_key: str
    ) -> tuple[str, str, int, int, int] | None: ...

    def record_apply(
        self,
        cursor: JournalCursor,
        *,
        preview: Mapping[str, Any],
        principal: str,
        idempotency_key: str,
        request_fingerprint: str,
        receipt: str,
        affected_rows: int,
        revision_before: int,
        revision_after: int,
    ) -> None: ...


class InMemoryOperatorJournal:
    """Test-only journal. Production construction must inject PostgresOperatorJournal."""

    durable = False

    def __init__(self) -> None:
        self._values: dict[tuple[str, str], tuple[str, str, int, int, int]] = {}

    def record_preview(self, payload: Mapping[str, Any], receipt: str) -> None:
        return None

    def find_apply(
        self, cursor: JournalCursor, *, principal: str, idempotency_key: str
    ) -> tuple[str, str, int, int, int] | None:
        return self._values.get((principal, idempotency_key))

    def record_apply(
        self,
        cursor: JournalCursor,
        *,
        preview: Mapping[str, Any],
        principal: str,
        idempotency_key: str,
        request_fingerprint: str,
        receipt: str,
        affected_rows: int,
        revision_before: int,
        revision_after: int,
    ) -> None:
        self._values[(principal, idempotency_key)] = (
            request_fingerprint,
            receipt,
            affected_rows,
            revision_before,
            revision_after,
        )


class PostgresOperatorJournal:
    """Journal whose apply receipt is inserted in the same transaction as DML."""

    durable = True

    def __init__(self, connection_factory: Callable[[], Any]) -> None:
        self._connection_factory = connection_factory

    def record_preview(self, payload: Mapping[str, Any], receipt: str) -> None:
        # A returned preview must already be durable. The preview DML itself was
        # rolled back, so this append-only receipt is intentionally a new tx.
        try:
            evidence_id = UUID(str(payload["backup_evidence_revision"]))
            preview_id = UUID(str(payload["receipt_id"]))
        except (KeyError, ValueError) as exc:
            raise ValueError("durable operator journal requires UUID recovery evidence") from exc
        connection = self._connection_factory()
        cursor = connection.cursor()
        committed = False
        try:
            cursor.execute("BEGIN TRANSACTION READ WRITE")
            cursor.execute("SET LOCAL search_path = pg_catalog")
            cursor.execute(
                """
                INSERT INTO operator_control.preview_receipt (
                    preview_id, principal, correlation_id, sql_sha256, params_sha256,
                    allowed_targets, expected_revision, expected_min_rows,
                    expected_max_rows, backup_evidence_id, expires_at, receipt_sha256
                ) VALUES (%s, %s, %s, %s, %s, ARRAY[%s], %s, %s, %s, %s, %s, %s)
                """,
                (
                    preview_id,
                    str(payload["principal"]),
                    str(payload["correlation_id"]),
                    str(payload["sql_fingerprint"]),
                    str(payload["params_fingerprint"]),
                    str(payload["target"]),
                    int(payload["expected_revision"]),
                    int(payload["expected_row_min"]),
                    int(payload["expected_row_max"]),
                    evidence_id,
                    str(payload["expires_at"]),
                    sha256_value(receipt),
                ),
            )
            connection.commit()
            committed = True
        finally:
            if not committed:
                connection.rollback()
            cursor.close()
            connection.close()

    def find_apply(
        self, cursor: JournalCursor, *, principal: str, idempotency_key: str
    ) -> tuple[str, str, int, int, int] | None:
        cursor.execute(
            """
            SELECT audit_receipt->>'request_fingerprint', audit_receipt->>'token',
                   affected_rows, revision_before, revision_after
            FROM operator_control.apply_receipt
            WHERE principal = %s AND idempotency_key = %s
            """,
            (principal, idempotency_key),
        )
        row = cursor.fetchone()
        if row is None:
            return None
        return (str(row[0]), str(row[1]), int(row[2]), int(row[3]), int(row[4]))

    def record_apply(
        self,
        cursor: JournalCursor,
        *,
        preview: Mapping[str, Any],
        principal: str,
        idempotency_key: str,
        request_fingerprint: str,
        receipt: str,
        affected_rows: int,
        revision_before: int,
        revision_after: int,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO operator_control.apply_receipt (
                preview_id, principal, idempotency_key, status, affected_rows,
                revision_before, revision_after, sql_sha256, params_sha256,
                audit_receipt
            ) VALUES (%s, %s, %s, 'committed', %s, %s, %s, %s, %s, %s::jsonb)
            """,
            (
                UUID(str(preview["receipt_id"])),
                principal,
                idempotency_key,
                affected_rows,
                revision_before,
                revision_after,
                str(preview["sql_fingerprint"]),
                str(preview["params_fingerprint"]),
                json.dumps(
                    {"request_fingerprint": request_fingerprint, "token": receipt},
                    sort_keys=True,
                ),
            ),
        )
