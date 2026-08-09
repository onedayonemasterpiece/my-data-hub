"""Bounded reader/editor execution with preview, apply, and idempotency."""

from __future__ import annotations

import base64
import json
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import UUID

from my_data_hub.hashing import sha256_value

from .errors import EffectBoundsError, IdempotencyConflict, ReceiptError, RevisionConflict
from .journal import OperatorJournal
from .policy import (
    BackupFreshnessPolicy,
    BackupState,
    DatabaseAllowlist,
    OperatorLimits,
)
from .receipts import ReceiptSigner, parameter_fingerprint
from .sql import analyze_editor_sql, analyze_reader_sql, compile_psycopg_parameters


class Cursor(Protocol):
    description: Sequence[Any] | None
    rowcount: int

    def execute(self, query: str, params: Sequence[object] | None = None) -> Any: ...

    def fetchone(self) -> Sequence[object] | None: ...

    def close(self) -> None: ...


class Connection(Protocol):
    def cursor(self) -> Cursor: ...

    def commit(self) -> None: ...

    def rollback(self) -> None: ...

    def close(self) -> None: ...


ConnectionFactory = Callable[[], Connection]
RevisionReader = Callable[[Cursor], int]
BackupStateProvider = Callable[[], BackupState]
Clock = Callable[[], datetime]


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _json_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, bytes):
        return {"base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, (Decimal, UUID)):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _json_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    return str(value)


def _description_name(item: Any) -> str:
    if hasattr(item, "name"):
        return str(item.name)
    return str(item[0])


@dataclass(frozen=True, slots=True)
class ReadResult:
    columns: tuple[str, ...]
    rows: tuple[tuple[object, ...], ...]
    row_count: int
    serialized_bytes: int
    truncated: bool
    truncation_reasons: tuple[str, ...]
    max_rows: int
    max_bytes: int


@dataclass(frozen=True, slots=True)
class PreviewResult:
    receipt: str
    affected_rows: int
    target: str
    expected_revision: int
    expires_at: str


@dataclass(frozen=True, slots=True)
class ApplyResult:
    receipt: str
    affected_rows: int
    revision_before: int
    revision_after: int
    idempotency_key: str
    replayed: bool = False


class DatabaseOperator:
    def __init__(
        self,
        *,
        connection_factory: ConnectionFactory,
        allowlist: DatabaseAllowlist,
        revision_reader: RevisionReader,
        backup_state_provider: BackupStateProvider,
        schema_revision: int,
        signer: ReceiptSigner,
        limits: OperatorLimits | None = None,
        freshness: BackupFreshnessPolicy | None = None,
        clock: Clock = _utcnow,
        journal: OperatorJournal,
    ) -> None:
        if schema_revision < 0:
            raise ValueError("schema_revision must not be negative")
        relation_schemas = {
            relation.schema
            for relation in allowlist.readable_relations | frozenset(allowlist.writable_columns)
        }
        if relation_schemas and relation_schemas != {allowlist.rollout_disposable_schema}:
            raise ValueError("R1 execution is restricted to its declared disposable schema")
        self._connection_factory = connection_factory
        self._allowlist = allowlist
        self._revision_reader = revision_reader
        self._backup_state_provider = backup_state_provider
        self._schema_revision = schema_revision
        self._signer = signer
        self._limits = limits or OperatorLimits()
        self._freshness = freshness or BackupFreshnessPolicy()
        self._clock = clock
        self._journal = journal

    def _begin(self, cursor: Cursor, *, read_only: bool) -> None:
        mode = "READ ONLY" if read_only else "READ WRITE"
        cursor.execute(f"BEGIN TRANSACTION {mode}")
        cursor.execute("SET LOCAL search_path = pg_catalog")
        cursor.execute("SET LOCAL row_security = on")
        cursor.execute(f"SET LOCAL statement_timeout = '{self._limits.statement_timeout_ms}ms'")
        cursor.execute(f"SET LOCAL transaction_timeout = '{self._limits.transaction_timeout_ms}ms'")
        cursor.execute(f"SET LOCAL lock_timeout = '{self._limits.lock_timeout_ms}ms'")
        cursor.execute(
            "SET LOCAL idle_in_transaction_session_timeout = "
            f"'{self._limits.idle_transaction_timeout_ms}ms'"
        )

    def read(self, sql: str, *, params: Sequence[object] = ()) -> ReadResult:
        analysis = analyze_reader_sql(sql, allowlist=self._allowlist, params=params)
        query, bound = compile_psycopg_parameters(analysis.normalized_sql, params)
        connection = self._connection_factory()
        cursor = connection.cursor()
        rows: list[tuple[object, ...]] = []
        reasons: list[str] = []
        serialized_bytes = 2  # []
        columns: tuple[str, ...] = ()
        try:
            self._begin(cursor, read_only=True)
            cursor.execute(query, bound)
            columns = tuple(_description_name(item) for item in (cursor.description or ()))
            while True:
                row = cursor.fetchone()
                if row is None:
                    break
                if len(rows) >= self._limits.max_rows:
                    reasons.append("row_limit")
                    break
                normalized_row = tuple(_json_value(value) for value in row)
                encoded = json.dumps(
                    normalized_row,
                    ensure_ascii=False,
                    separators=(",", ":"),
                    allow_nan=False,
                ).encode("utf-8")
                added_bytes = len(encoded) + (1 if rows else 0)
                if serialized_bytes + added_bytes > self._limits.max_bytes:
                    reasons.append("byte_limit")
                    break
                rows.append(normalized_row)
                serialized_bytes += added_bytes
        finally:
            connection.rollback()
            cursor.close()
            connection.close()
        return ReadResult(
            columns=columns,
            rows=tuple(rows),
            row_count=len(rows),
            serialized_bytes=serialized_bytes,
            truncated=bool(reasons),
            truncation_reasons=tuple(reasons),
            max_rows=self._limits.max_rows,
            max_bytes=self._limits.max_bytes,
        )

    def _open_gate(
        self, state: BackupState, *, now: datetime, require_checkpoint: bool = False
    ) -> None:
        self._freshness.require_open(
            state,
            now=now,
            expected_schema_revision=self._schema_revision,
            require_checkpoint=require_checkpoint,
        )

    @staticmethod
    def _validate_identity(value: str, name: str) -> None:
        if not value or len(value) > 300:
            raise ValueError(f"{name} must contain 1..300 characters")

    def preview(
        self,
        sql: str,
        *,
        params: Sequence[object],
        principal: str,
        session_id: str,
        correlation_id: str,
        expected_revision: int,
        expected_row_min: int,
        expected_row_max: int,
        impact_tier: str = "low",
    ) -> PreviewResult:
        for name, value in (
            ("principal", principal),
            ("session_id", session_id),
            ("correlation_id", correlation_id),
        ):
            self._validate_identity(value, name)
        if expected_revision < 0:
            raise ValueError("expected_revision must not be negative")
        if not 0 <= expected_row_min <= expected_row_max <= self._limits.max_write_rows:
            raise EffectBoundsError("expected row bounds exceed the bounded editor policy")
        if impact_tier not in {"low", "medium", "high", "bulk"}:
            raise ValueError("impact_tier must be low, medium, high, or bulk")
        analysis = analyze_editor_sql(sql, allowlist=self._allowlist, params=params)
        assert analysis.target is not None
        query, bound = compile_psycopg_parameters(analysis.normalized_sql, params)
        now = self._clock()
        backup = self._backup_state_provider()
        self._open_gate(
            backup,
            now=now,
            require_checkpoint=impact_tier in {"high", "bulk"} or expected_row_max > 10,
        )
        connection = self._connection_factory()
        cursor = connection.cursor()
        try:
            self._begin(cursor, read_only=False)
            actual_revision = int(self._revision_reader(cursor))
            if actual_revision != expected_revision:
                raise RevisionConflict(
                    f"canonical revision changed: expected={expected_revision}, actual={actual_revision}"
                )
            cursor.execute(query, bound)
            affected = int(cursor.rowcount)
            if not expected_row_min <= affected <= expected_row_max:
                raise EffectBoundsError(
                    f"preview affected {affected} rows outside "
                    f"[{expected_row_min}, {expected_row_max}]"
                )
        finally:
            # R1 preview is restricted to a disposable schema and is always rolled
            # back. Application-schema rollout needs a dedicated effect estimator.
            connection.rollback()
            cursor.close()
            connection.close()
        token = self._signer.issue_preview(
            now=now,
            principal=principal,
            session_id=session_id,
            correlation_id=correlation_id,
            sql_fingerprint=analysis.sql_fingerprint,
            params_fingerprint=parameter_fingerprint(params),
            target=analysis.target.qualified_name,
            expected_revision=expected_revision,
            expected_row_min=expected_row_min,
            expected_row_max=expected_row_max,
            preview_affected_rows=affected,
            backup_evidence_revision=backup.evidence_revision,
            backup_fingerprint=backup.fingerprint,
            impact_tier=impact_tier,
        )
        payload = self._signer.verify_preview(token, now=now)
        self._journal.record_preview(payload, token)
        return PreviewResult(
            receipt=token,
            affected_rows=affected,
            target=analysis.target.qualified_name,
            expected_revision=expected_revision,
            expires_at=str(payload["expires_at"]),
        )

    @staticmethod
    def _require_binding(payload: Mapping[str, Any], name: str, actual: Any) -> None:
        if payload.get(name) != actual:
            raise ReceiptError(f"preview receipt {name} does not match this apply request")

    def apply(
        self,
        sql: str,
        *,
        params: Sequence[object],
        principal: str,
        session_id: str,
        correlation_id: str,
        preview_receipt: str,
        idempotency_key: str,
    ) -> ApplyResult:
        for name, value in (
            ("principal", principal),
            ("session_id", session_id),
            ("correlation_id", correlation_id),
            ("idempotency_key", idempotency_key),
        ):
            self._validate_identity(value, name)
        analysis = analyze_editor_sql(sql, allowlist=self._allowlist, params=params)
        assert analysis.target is not None
        now = self._clock()
        preview = self._signer.verify_preview(preview_receipt, now=now)
        bindings = {
            "principal": principal,
            "session_id": session_id,
            "correlation_id": correlation_id,
            "sql_fingerprint": analysis.sql_fingerprint,
            "params_fingerprint": parameter_fingerprint(params),
            "target": analysis.target.qualified_name,
        }
        for name, value in bindings.items():
            self._require_binding(preview, name, value)
        request_fingerprint = sha256_value(
            {
                "preview_receipt": preview_receipt,
                "idempotency_key": idempotency_key,
                **bindings,
            }
        )

        backup = self._backup_state_provider()
        impact_tier = str(preview.get("impact_tier", ""))
        if impact_tier not in {"low", "medium", "high", "bulk"}:
            raise ReceiptError("preview receipt contains an invalid impact tier")
        self._open_gate(
            backup,
            now=now,
            require_checkpoint=impact_tier in {"high", "bulk"}
            or int(preview["expected_row_max"]) > 10,
        )
        self._require_binding(preview, "backup_evidence_revision", backup.evidence_revision)
        self._require_binding(preview, "backup_fingerprint", backup.fingerprint)
        query, bound = compile_psycopg_parameters(analysis.normalized_sql, params)
        expected_revision = int(preview["expected_revision"])
        expected_min = int(preview["expected_row_min"])
        expected_max = int(preview["expected_row_max"])
        preview_affected = int(preview["preview_affected_rows"])
        if not 0 <= expected_min <= expected_max <= self._limits.max_write_rows:
            raise ReceiptError("preview receipt contains invalid effect bounds")

        connection = self._connection_factory()
        cursor = connection.cursor()
        committed = False
        try:
            self._begin(cursor, read_only=False)
            existing = self._journal.find_apply(
                cursor, principal=principal, idempotency_key=idempotency_key
            )
            if existing is not None:
                previous_fingerprint, receipt, affected, before, after = existing
                if previous_fingerprint != request_fingerprint:
                    raise IdempotencyConflict(
                        "idempotency key was already used for another apply request"
                    )
                return ApplyResult(
                    receipt=receipt,
                    affected_rows=affected,
                    revision_before=before,
                    revision_after=after,
                    idempotency_key=idempotency_key,
                    replayed=True,
                )
            revision_before = int(self._revision_reader(cursor))
            if revision_before != expected_revision:
                raise RevisionConflict(
                    "canonical revision no longer matches the preview: "
                    f"expected={expected_revision}, actual={revision_before}"
                )
            cursor.execute(query, bound)
            affected = int(cursor.rowcount)
            if not expected_min <= affected <= expected_max or affected != preview_affected:
                raise EffectBoundsError(
                    f"apply affected {affected} rows; preview={preview_affected}, "
                    f"bounds=[{expected_min}, {expected_max}]"
                )
            revision_after = int(self._revision_reader(cursor))
            apply_payload = {
                "receipt_id": str(preview["receipt_id"]),
                "preview_receipt_fingerprint": sha256_value(preview_receipt),
                "principal": principal,
                "session_id": session_id,
                "correlation_id": correlation_id,
                "sql_fingerprint": analysis.sql_fingerprint,
                "params_fingerprint": parameter_fingerprint(params),
                "target": analysis.target.qualified_name,
                "affected_rows": affected,
                "revision_before": revision_before,
                "revision_after": revision_after,
                "backup_evidence_revision": backup.evidence_revision,
                "backup_fingerprint": backup.fingerprint,
                "idempotency_key": idempotency_key,
                "committed_at": now.astimezone(UTC).isoformat(),
            }
            apply_receipt = self._signer.issue_apply(apply_payload)
            self._journal.record_apply(
                cursor,
                preview=preview,
                principal=principal,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                receipt=apply_receipt,
                affected_rows=affected,
                revision_before=revision_before,
                revision_after=revision_after,
            )
            connection.commit()
            committed = True
        finally:
            if not committed:
                connection.rollback()
            cursor.close()
            connection.close()

        return ApplyResult(
            receipt=apply_receipt,
            affected_rows=affected,
            revision_before=revision_before,
            revision_after=revision_after,
            idempotency_key=idempotency_key,
        )
