from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import uuid4

from my_data_hub.control_plane.clock import Clock, SystemClock

from .errors import EventRejected, IdempotencyConflict, LeaseRejected, StaleRuntimeEvent
from .migrations import apply_control_migrations, default_migration_directory
from .models import (
    CheckpointHead,
    EffectRecord,
    EffectState,
    EventDisposition,
    EventReceipt,
    OperationRecord,
    ResourceLeaseRecord,
    ServiceRecord,
)

MAX_EVENT_BYTES = 64 * 1024
MAX_METADATA_BYTES = 16 * 1024
TERMINAL_EVENT_TYPES = frozenset(
    {
        "runtime.terminal",
        "runtime.failed",
        "job.completed",
        "job.failed",
        "checkpoint.verified",
        "checkpoint.failed",
    }
)
_SECRET_FRAGMENTS = ("password", "secret", "token", "authorization", "credential", "private_key")


def _format_time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("control-ledger timestamps must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _json_default(value: object) -> object:
    if isinstance(value, datetime):
        return _format_time(value)
    raise TypeError(f"unsupported JSON value: {type(value).__name__}")


def _canonical_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=_json_default)


def _safe_json(value: Mapping[str, Any] | None, *, max_bytes: int = MAX_METADATA_BYTES) -> str:
    value = value or {}

    def inspect(candidate: object) -> None:
        if isinstance(candidate, Mapping):
            for key, nested in candidate.items():
                lowered = str(key).lower()
                if any(fragment in lowered for fragment in _SECRET_FRAGMENTS):
                    raise EventRejected(f"secret-bearing field is forbidden in the control ledger: {key}")
                inspect(nested)
        elif isinstance(candidate, (list, tuple)):
            for nested in candidate:
                inspect(nested)

    inspect(value)
    encoded = _canonical_json(value)
    if len(encoded.encode("utf-8")) > max_bytes:
        raise EventRejected("structured control metadata exceeds its bounded size")
    return encoded


class ControlLedger:
    """Small non-canonical SQLite ledger with explicit writer serialization."""

    def __init__(
        self,
        path: Path,
        *,
        clock: Clock | None = None,
        busy_timeout_ms: int = 5_000,
        migration_directory: Path | None = None,
        heartbeat_coalesce_seconds: float = 30.0,
    ) -> None:
        if busy_timeout_ms < 1:
            raise ValueError("busy_timeout_ms must be positive")
        self.path = path.resolve()
        self.clock = clock or SystemClock()
        self.busy_timeout_ms = busy_timeout_ms
        self.heartbeat_coalesce_seconds = heartbeat_coalesce_seconds
        self.migration_directory = migration_directory or default_migration_directory()
        self._permission_lock = threading.Lock()
        self._prepare_path()
        with self._connect() as connection:
            apply_control_migrations(connection, self.migration_directory)
        self._tighten_file_modes()

    def _prepare_path(self) -> None:
        if self.path.exists() and self.path.is_symlink():
            raise ValueError("control ledger may not be a symbolic link")
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)

    def _tighten_file_modes(self) -> None:
        with self._permission_lock:
            for suffix in ("", "-wal", "-shm"):
                candidate = Path(f"{self.path}{suffix}")
                if candidate.exists():
                    os.chmod(candidate, 0o600)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(
            self.path,
            timeout=self.busy_timeout_ms / 1_000,
            isolation_level=None,
            check_same_thread=False,
        )
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {self.busy_timeout_ms}")
        mode = str(connection.execute("PRAGMA journal_mode = WAL").fetchone()[0]).lower()
        if mode != "wal":
            connection.close()
            raise RuntimeError(f"control ledger requires SQLite WAL mode, got {mode}")
        connection.execute("PRAGMA synchronous = FULL")
        self._tighten_file_modes()
        return connection

    @contextmanager
    def _transaction(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            yield connection
            connection.commit()
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()
            self._tighten_file_modes()

    @contextmanager
    def _reader(self) -> Iterator[sqlite3.Connection]:
        connection = self._connect()
        try:
            yield connection
        finally:
            connection.close()
            self._tighten_file_modes()

    def sqlite_pragmas(self) -> dict[str, int | str]:
        with self._reader() as connection:
            return {
                "journal_mode": str(connection.execute("PRAGMA journal_mode").fetchone()[0]).lower(),
                "foreign_keys": int(connection.execute("PRAGMA foreign_keys").fetchone()[0]),
                "busy_timeout": int(connection.execute("PRAGMA busy_timeout").fetchone()[0]),
            }

    def ensure_operation(
        self,
        *,
        operation_id: str,
        idempotency_key: str,
        operation_kind: str,
        intent: Mapping[str, Any],
        initial_state: str,
        identity: Mapping[str, Any],
        allocate_epoch_for: str | None = None,
    ) -> tuple[OperationRecord, bool]:
        intent_json = _safe_json(intent)
        intent_hash = hashlib.sha256(intent_json.encode()).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            if row is not None:
                if row["intent_hash"] != intent_hash or row["operation_kind"] != operation_kind:
                    raise IdempotencyConflict("idempotency key was reused for a different control intent")
                return self._operation_from_row(row), False
            durable_identity = dict(identity)
            if allocate_epoch_for is not None:
                epoch_row = connection.execute(
                    "SELECT current_epoch FROM service_epochs WHERE service_kind=?", (allocate_epoch_for,)
                ).fetchone()
                epoch = (int(epoch_row[0]) if epoch_row else 0) + 1
                connection.execute(
                    "INSERT INTO service_epochs(service_kind,current_epoch,updated_at) VALUES (?,?,?) "
                    "ON CONFLICT(service_kind) DO UPDATE SET current_epoch=excluded.current_epoch,"
                    "updated_at=excluded.updated_at",
                    (allocate_epoch_for, epoch, now),
                )
                connection.execute(
                    "UPDATE services SET state='FENCED',updated_at=? WHERE service_kind=? AND epoch<? "
                    "AND state IN ('ACTIVE','DRAINING')",
                    (now, allocate_epoch_for, epoch),
                )
                durable_identity["epoch"] = epoch
            identity_json = _safe_json(durable_identity)
            connection.execute(
                "INSERT INTO operations(operation_id,idempotency_key,operation_kind,intent_hash,state,identity_json,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?)",
                (operation_id, idempotency_key, operation_kind, intent_hash, initial_state, identity_json, now, now),
            )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,NULL,?,?,?)",
                (operation_id, initial_state, now, _safe_json({"reason": "created"})),
            )
            created = True
            row = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
            assert row is not None
            return self._operation_from_row(row), created

    def get_operation(self, operation_id: str) -> OperationRecord | None:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM operations WHERE operation_id = ?", (operation_id,)).fetchone()
        return self._operation_from_row(row) if row else None

    def incomplete_operations(self, operation_kind: str | None = None) -> list[OperationRecord]:
        if operation_kind == "ensure_master":
            # ACTIVE masters still require provider observation.  A STOPPED
            # operation whose service remains DRAINING represents a verified
            # checkpoint with a lost terminal callback and must also resume.
            query = (
                "SELECT DISTINCT operations.* FROM operations "
                "LEFT JOIN run_attempts ON run_attempts.operation_id=operations.operation_id "
                "LEFT JOIN services ON services.service_instance_id=run_attempts.service_instance_id "
                "WHERE operations.operation_kind=? "
                "AND operations.state NOT IN ('FAILED','FENCED','ORPHANED') "
                "AND (operations.state!='STOPPED' OR services.state='DRAINING') "
                "ORDER BY operations.created_at,operations.operation_id"
            )
            with self._reader() as connection:
                return [self._operation_from_row(row) for row in connection.execute(query, (operation_kind,))]
        terminal = ("ACTIVE", "STOPPED", "FAILED", "FENCED", "ORPHANED", "DURABLE_COMPLETE")
        placeholders = ",".join("?" for _ in terminal)
        query = f"SELECT * FROM operations WHERE state NOT IN ({placeholders})"
        params: tuple[str, ...] = terminal
        if operation_kind is not None:
            query += " AND operation_kind=?"
            params += (operation_kind,)
        query += " ORDER BY created_at, operation_id"
        with self._reader() as connection:
            return [self._operation_from_row(row) for row in connection.execute(query, params)]

    def transition_operation(
        self,
        operation_id: str,
        *,
        expected_state: str,
        new_state: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> OperationRecord:
        now = _format_time(self.clock.now())
        metadata_json = _safe_json(metadata)
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE operations SET state = ?, updated_at = ? WHERE operation_id = ? AND state = ?",
                (new_state, now, operation_id, expected_state),
            ).rowcount
            if changed != 1:
                current = connection.execute(
                    "SELECT state FROM operations WHERE operation_id = ?", (operation_id,)
                ).fetchone()
                actual = current[0] if current else "missing"
                raise StaleRuntimeEvent(f"operation transition expected {expected_state}, found {actual}")
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,?,?,?,?)",
                (operation_id, expected_state, new_state, now, metadata_json),
            )
            row = connection.execute("SELECT * FROM operations WHERE operation_id = ?", (operation_id,)).fetchone()
            assert row is not None
            return self._operation_from_row(row)

    def plan_effect(
        self,
        *,
        effect_id: str,
        operation_id: str,
        idempotency_key: str,
        effect_kind: str,
        exact_identity: Mapping[str, Any],
    ) -> tuple[EffectRecord, bool]:
        exact_json = _safe_json(exact_identity)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            try:
                connection.execute(
                    "INSERT INTO effects(effect_id,operation_id,idempotency_key,effect_kind,exact_identity_json,state,"
                    "planned_at,updated_at) VALUES (?,?,?,?,?,'PLANNED',?,?)",
                    (effect_id, operation_id, idempotency_key, effect_kind, exact_json, now, now),
                )
                connection.execute(
                    "INSERT INTO effect_log(effect_id,operation_id,state,recorded_at,metadata_json) VALUES (?,?,?,?,?)",
                    (effect_id, operation_id, EffectState.PLANNED.value, now, _safe_json({"reason": "planned"})),
                )
                created = True
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT * FROM effects WHERE idempotency_key = ?", (idempotency_key,)
                ).fetchone()
                if (
                    row is None
                    or row["operation_id"] != operation_id
                    or row["effect_kind"] != effect_kind
                    or row["exact_identity_json"] != exact_json
                ):
                    raise IdempotencyConflict(
                        "effect idempotency key was reused with different exact identity"
                    ) from None
                created = False
            row = connection.execute("SELECT * FROM effects WHERE idempotency_key = ?", (idempotency_key,)).fetchone()
            assert row is not None
            return self._effect_from_row(row), created

    def claim_effect(self, effect_id: str) -> EffectRecord | None:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE effects SET state='IN_PROGRESS', updated_at=? WHERE effect_id=? AND state='PLANNED'",
                (now, effect_id),
            ).rowcount
            if changed != 1:
                return None
            row = connection.execute("SELECT * FROM effects WHERE effect_id = ?", (effect_id,)).fetchone()
            assert row is not None
            connection.execute(
                "INSERT INTO effect_log(effect_id,operation_id,state,recorded_at,metadata_json) VALUES (?,?,?,?,?)",
                (effect_id, row["operation_id"], EffectState.IN_PROGRESS.value, now, _safe_json({"reason": "claimed"})),
            )
            return self._effect_from_row(row)

    def complete_effect(self, effect_id: str, receipt: Mapping[str, Any]) -> EffectRecord:
        receipt_json = _safe_json(receipt)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM effects WHERE effect_id = ?", (effect_id,)).fetchone()
            if row is None:
                raise KeyError(effect_id)
            if row["state"] == EffectState.APPLIED.value:
                if row["receipt_json"] != receipt_json:
                    raise IdempotencyConflict("effect already completed with a different receipt")
                return self._effect_from_row(row)
            if row["state"] != EffectState.IN_PROGRESS.value:
                raise StaleRuntimeEvent(f"effect {effect_id} is not in progress")
            connection.execute(
                "UPDATE effects SET state='APPLIED', receipt_json=?, updated_at=? WHERE effect_id=?",
                (receipt_json, now, effect_id),
            )
            connection.execute(
                "INSERT INTO effect_log(effect_id,operation_id,state,recorded_at,metadata_json) VALUES (?,?,?,?,?)",
                (effect_id, row["operation_id"], EffectState.APPLIED.value, now, receipt_json),
            )
            completed = connection.execute("SELECT * FROM effects WHERE effect_id = ?", (effect_id,)).fetchone()
            assert completed is not None
            return self._effect_from_row(completed)

    def pending_effects(self, operation_id: str | None = None) -> list[EffectRecord]:
        query = "SELECT * FROM effects WHERE state IN ('PLANNED','IN_PROGRESS')"
        params: tuple[str, ...] = ()
        if operation_id is not None:
            query += " AND operation_id = ?"
            params = (operation_id,)
        query += " ORDER BY planned_at, effect_id"
        with self._reader() as connection:
            return [self._effect_from_row(row) for row in connection.execute(query, params)]

    def get_effect_by_idempotency_key(self, idempotency_key: str) -> EffectRecord | None:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM effects WHERE idempotency_key=?", (idempotency_key,)).fetchone()
        return self._effect_from_row(row) if row else None

    def record_attempt(
        self,
        *,
        attempt_id: str,
        run_id: str,
        operation_id: str,
        source_identity: str,
        source_version: str,
        service_instance_id: str,
        master_instance_id: str | None,
        epoch: int,
        state: str,
    ) -> None:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO run_attempts(attempt_id,run_id,operation_id,source_identity,source_version,"
                "service_instance_id,master_instance_id,epoch,state,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(attempt_id) DO NOTHING",
                (
                    attempt_id,
                    run_id,
                    operation_id,
                    source_identity,
                    source_version,
                    service_instance_id,
                    master_instance_id,
                    epoch,
                    state,
                    now,
                    now,
                ),
            )
            row = connection.execute("SELECT * FROM run_attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
            assert row is not None
            expected = (
                run_id,
                operation_id,
                source_identity,
                source_version,
                service_instance_id,
                master_instance_id,
                epoch,
            )
            actual = (
                row["run_id"],
                row["operation_id"],
                row["source_identity"],
                row["source_version"],
                row["service_instance_id"],
                row["master_instance_id"],
                int(row["epoch"]),
            )
            if actual != expected:
                raise IdempotencyConflict("attempt ID was reused with different exact identities")

    def set_attempt_provider_run(self, attempt_id: str, provider_run_ref: str, state: str) -> None:
        with self._transaction() as connection:
            connection.execute(
                "UPDATE run_attempts SET provider_run_ref=?, state=?, updated_at=? WHERE attempt_id=?",
                (provider_run_ref, state, _format_time(self.clock.now()), attempt_id),
            )

    def operation_for_attempt(self, run_id: str, attempt_id: str) -> OperationRecord | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT operation_id FROM run_attempts WHERE run_id=? AND attempt_id=?", (run_id, attempt_id)
            ).fetchone()
        return self.get_operation(str(row[0])) if row else None

    def allocate_epoch(self, service_kind: str) -> int:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind=?", (service_kind,)
            ).fetchone()
            epoch = (int(row[0]) if row else 0) + 1
            connection.execute(
                "INSERT INTO service_epochs(service_kind,current_epoch,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(service_kind) DO UPDATE SET current_epoch=excluded.current_epoch, "
                "updated_at=excluded.updated_at",
                (service_kind, epoch, now),
            )
            connection.execute(
                "UPDATE services SET state='FENCED', updated_at=? WHERE service_kind=? AND epoch<? "
                "AND state IN ('ACTIVE','DRAINING')",
                (now, service_kind, epoch),
            )
            return epoch

    def current_epoch(self, service_kind: str) -> int:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind=?", (service_kind,)
            ).fetchone()
        return int(row[0]) if row else 0

    def store_runtime_token_hash(self, run_id: str, attempt_id: str, token: str) -> None:
        token_sha256 = hashlib.sha256(token.encode()).hexdigest()
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO runtime_token_hashes(run_id,attempt_id,token_sha256,created_at) VALUES (?,?,?,?) "
                "ON CONFLICT(run_id,attempt_id) DO NOTHING",
                (run_id, attempt_id, token_sha256, _format_time(self.clock.now())),
            )
            row = connection.execute(
                "SELECT token_sha256 FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?", (run_id, attempt_id)
            ).fetchone()
            if row is None or not hmac.compare_digest(str(row[0]), token_sha256):
                raise IdempotencyConflict("runtime attempt already has a different per-run token hash")

    def revoke_runtime_token(self, run_id: str, attempt_id: str) -> None:
        with self._transaction() as connection:
            connection.execute(
                "UPDATE runtime_token_hashes SET revoked_at=? WHERE run_id=? AND attempt_id=?",
                (_format_time(self.clock.now()), run_id, attempt_id),
            )

    def runtime_token_valid(self, run_id: str, attempt_id: str, token: str) -> bool:
        """Validate one per-run bearer without disclosing the persisted hash."""

        if not token or len(token) > 4096:
            return False
        with self._reader() as connection:
            row = connection.execute(
                "SELECT token_sha256,revoked_at FROM runtime_token_hashes "
                "WHERE run_id=? AND attempt_id=?",
                (run_id, attempt_id),
            ).fetchone()
        if row is None or row["revoked_at"] is not None:
            return False
        observed = hashlib.sha256(token.encode()).hexdigest()
        return hmac.compare_digest(str(row["token_sha256"]), observed)

    def ingest_runtime_event(self, raw_body: bytes, *, header_token: str) -> EventReceipt:
        if len(raw_body) > MAX_EVENT_BYTES:
            raise EventRejected("runtime event body exceeds 64 KiB")
        try:
            from my_data_hub.runtime_sdk.events import RuntimeEvent

            event = RuntimeEvent.model_validate_json(raw_body)
        except Exception as exc:
            raise EventRejected(f"invalid content-runtime-event/v1 envelope: {exc}") from exc
        raw = event.model_dump(mode="json", by_alias=True, exclude_none=True)
        if any(key in raw for key in ("token", "authorization", "credential", "password")):
            raise EventRejected("runtime credentials are forbidden in the event body")
        sanitized_json = _safe_json(raw, max_bytes=MAX_EVENT_BYTES)
        body_sha256 = hashlib.sha256(raw_body).hexdigest()
        now_dt = self.clock.now()
        now = _format_time(now_dt)
        with self._transaction() as connection:
            expected = connection.execute(
                "SELECT * FROM run_attempts WHERE run_id=? AND attempt_id=?",
                (event.run_id, event.attempt_id),
            ).fetchone()
            if expected is None:
                raise StaleRuntimeEvent("unknown run/attempt identity")
            token_row = connection.execute(
                "SELECT token_sha256, revoked_at FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
                (event.run_id, event.attempt_id),
            ).fetchone()
            candidate_hash = hashlib.sha256(header_token.encode()).hexdigest()
            if (
                event.service_instance_id != expected["service_instance_id"]
                or event.source_identity != expected["source_identity"]
                or event.source_version != expected["source_version"]
                or event.epoch != expected["epoch"]
            ):
                raise StaleRuntimeEvent("event exact identity does not match the durable attempt")
            duplicate = connection.execute(
                "SELECT body_sha256 FROM runtime_event_dedup WHERE event_id=?", (event.event_id,)
            ).fetchone()
            if duplicate is not None:
                if duplicate[0] != body_sha256:
                    raise EventRejected("event ID was reused with a different body")
                # A terminal callback can be committed before the HTTP response
                # reaches the Notebook.  Its projection revokes the token, but
                # an exact body replay under the same former token is still a
                # read-only acknowledgement.  No altered token/body/attempt is
                # allowed through this narrow response-loss exception.
                if token_row is None or not hmac.compare_digest(token_row["token_sha256"], candidate_hash):
                    raise EventRejected("invalid runtime token for duplicate event")
                return EventReceipt(event.event_id, EventDisposition.DUPLICATE, body_sha256)
            if (
                token_row is None
                or token_row["revoked_at"] is not None
                or not hmac.compare_digest(token_row["token_sha256"], candidate_hash)
            ):
                raise EventRejected("invalid or revoked runtime token")
            current_epoch = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
            ).fetchone()
            if current_epoch is not None and event.epoch < int(current_epoch[0]):
                connection.execute(
                    "UPDATE run_attempts SET state='FENCED', updated_at=? WHERE attempt_id=?",
                    (now, event.attempt_id),
                )
                return EventReceipt(event.event_id, EventDisposition.FENCED, body_sha256)
            connection.execute(
                "INSERT INTO runtime_event_dedup(event_id,body_sha256,first_seen_at) VALUES (?,?,?)",
                (event.event_id, body_sha256, now),
            )
            projection = connection.execute(
                "SELECT * FROM runtime_projection WHERE run_id=? AND attempt_id=?",
                (event.run_id, event.attempt_id),
            ).fetchone()
            coalesced = False
            if event.event_type == "runtime.heartbeat" and projection is not None:
                elapsed = (now_dt - _parse_time(projection["latest_seen_at"])).total_seconds()
                coalesced = (
                    projection["latest_event_type"] == "runtime.heartbeat" and elapsed < self.heartbeat_coalesce_seconds
                )
            terminal = event.event_type in TERMINAL_EVENT_TYPES
            if not coalesced:
                connection.execute(
                    "INSERT INTO runtime_events(event_id,schema_version,run_id,attempt_id,service_instance_id,"
                    "source_identity,source_version,epoch,event_type,emitted_at,received_at,local_sequence,"
                    "body_sha256,body_bytes,sanitized_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        event.event_id,
                        event.schema_version,
                        event.run_id,
                        event.attempt_id,
                        event.service_instance_id,
                        event.source_identity,
                        event.source_version,
                        event.epoch,
                        event.event_type,
                        _format_time(event.emitted_at),
                        now,
                        event.local_sequence,
                        body_sha256,
                        len(raw_body),
                        sanitized_json,
                    ),
                )
            if projection is None or event.local_sequence > int(projection["latest_sequence"]):
                connection.execute(
                    "INSERT INTO runtime_projection(run_id,attempt_id,latest_event_id,latest_event_type,"
                    "latest_sequence,latest_epoch,latest_seen_at,terminal) VALUES (?,?,?,?,?,?,?,?) "
                    "ON CONFLICT(run_id,attempt_id) DO UPDATE SET latest_event_id=excluded.latest_event_id,"
                    "latest_event_type=excluded.latest_event_type,latest_sequence=excluded.latest_sequence,"
                    "latest_epoch=excluded.latest_epoch,latest_seen_at=excluded.latest_seen_at,"
                    "terminal=max(runtime_projection.terminal,excluded.terminal)",
                    (
                        event.run_id,
                        event.attempt_id,
                        event.event_id,
                        event.event_type,
                        event.local_sequence,
                        event.epoch,
                        now,
                        int(terminal),
                    ),
                )
            return EventReceipt(
                event.event_id,
                EventDisposition.COALESCED if coalesced else EventDisposition.ACCEPTED,
                body_sha256,
            )

    def announce_service(
        self,
        *,
        service_instance_id: str,
        service_kind: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str | None,
        epoch: int,
        endpoint: str,
        protocol: str,
        tls_fingerprint: str | None,
        capabilities: tuple[str, ...],
        canonical_revision: int | None,
        schema_version: str | None,
        lease_until: datetime,
        latest_event_id: str,
    ) -> ServiceRecord:
        now = self.clock.now()
        if lease_until <= now:
            raise LeaseRejected("service lease is already expired")
        capabilities_json = _canonical_json(sorted(set(capabilities)))
        with self._transaction() as connection:
            current = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind=?", (service_kind,)
            ).fetchone()
            if current is None or int(current[0]) != epoch:
                raise LeaseRejected("service announcement is fenced by a newer epoch")
            connection.execute(
                "INSERT INTO services(service_instance_id,service_kind,run_id,attempt_id,master_instance_id,epoch,"
                "endpoint,protocol,tls_fingerprint,capabilities_json,canonical_revision,schema_version,lease_until,"
                "state,latest_event_id,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,?) "
                "ON CONFLICT(service_instance_id) DO UPDATE SET endpoint=excluded.endpoint,protocol=excluded.protocol,"
                "tls_fingerprint=excluded.tls_fingerprint,capabilities_json=excluded.capabilities_json,"
                "canonical_revision=excluded.canonical_revision,schema_version=excluded.schema_version,"
                "lease_until=excluded.lease_until,state='ACTIVE',latest_event_id=excluded.latest_event_id,"
                "updated_at=excluded.updated_at WHERE services.epoch=excluded.epoch",
                (
                    service_instance_id,
                    service_kind,
                    run_id,
                    attempt_id,
                    master_instance_id,
                    epoch,
                    endpoint,
                    protocol,
                    tls_fingerprint,
                    capabilities_json,
                    canonical_revision,
                    schema_version,
                    _format_time(lease_until),
                    latest_event_id,
                    _format_time(now),
                ),
            )
            row = connection.execute(
                "SELECT * FROM services WHERE service_instance_id=?", (service_instance_id,)
            ).fetchone()
            assert row is not None
            return self._service_from_row(row)

    def activate_service_operation(
        self,
        *,
        operation_id: str,
        expected_state: str,
        service_instance_id: str,
        service_kind: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str | None,
        epoch: int,
        endpoint: str,
        protocol: str,
        tls_fingerprint: str | None,
        capabilities: tuple[str, ...],
        canonical_revision: int | None,
        schema_version: str | None,
        lease_until: datetime,
        latest_event_id: str,
    ) -> ServiceRecord:
        """Atomically project service readiness and advance its lifecycle operation."""

        now_dt = self.clock.now()
        if lease_until <= now_dt:
            raise LeaseRejected("service lease is already expired")
        now = _format_time(now_dt)
        with self._transaction() as connection:
            current = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind=?", (service_kind,)
            ).fetchone()
            if current is None or int(current[0]) != epoch:
                raise LeaseRejected("service activation is fenced by a newer epoch")
            operation = connection.execute(
                "SELECT state FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if operation is None:
                raise KeyError(operation_id)
            if operation["state"] == "ACTIVE":
                existing = connection.execute(
                    "SELECT * FROM services WHERE service_instance_id=?", (service_instance_id,)
                ).fetchone()
                if existing is None:
                    raise StaleRuntimeEvent("active operation is missing its atomic service projection")
                return self._service_from_row(existing)
            if operation["state"] != expected_state:
                raise StaleRuntimeEvent(
                    f"service activation expected operation {expected_state}, found {operation['state']}"
                )
            connection.execute(
                "UPDATE operations SET state='ACTIVE',updated_at=? WHERE operation_id=?",
                (now, operation_id),
            )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,?,?,?,?)",
                (operation_id, expected_state, "ACTIVE", now, _safe_json({"event_id": latest_event_id})),
            )
            connection.execute(
                "INSERT INTO services(service_instance_id,service_kind,run_id,attempt_id,master_instance_id,epoch,"
                "endpoint,protocol,tls_fingerprint,capabilities_json,canonical_revision,schema_version,lease_until,"
                "state,latest_event_id,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,'ACTIVE',?,?)",
                (
                    service_instance_id,
                    service_kind,
                    run_id,
                    attempt_id,
                    master_instance_id,
                    epoch,
                    endpoint,
                    protocol,
                    tls_fingerprint,
                    _canonical_json(sorted(set(capabilities))),
                    canonical_revision,
                    schema_version,
                    _format_time(lease_until),
                    latest_event_id,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM services WHERE service_instance_id=?", (service_instance_id,)
            ).fetchone()
            assert row is not None
            return self._service_from_row(row)

    def project_master_lifecycle(
        self,
        *,
        operation_id: str,
        service_instance_id: str,
        epoch: int,
        expected_operation_state: str,
        operation_state: str,
        service_state: str,
        event_id: str,
    ) -> None:
        """Atomically project one authenticated runtime lifecycle transition."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            operation = connection.execute(
                "SELECT state FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            service = connection.execute(
                "SELECT state,epoch FROM services WHERE service_instance_id=?", (service_instance_id,)
            ).fetchone()
            if operation is None or service is None or int(service["epoch"]) != epoch:
                raise StaleRuntimeEvent("runtime lifecycle identity is absent or fenced")
            if operation["state"] == operation_state and service["state"] == service_state:
                return
            if operation["state"] != expected_operation_state:
                raise StaleRuntimeEvent(
                    f"runtime lifecycle expected {expected_operation_state}, found {operation['state']}"
                )
            connection.execute(
                "UPDATE operations SET state=?,updated_at=? WHERE operation_id=? AND state=?",
                (operation_state, now, operation_id, expected_operation_state),
            )
            connection.execute(
                "UPDATE services SET state=?,latest_event_id=?,updated_at=? "
                "WHERE service_instance_id=? AND epoch=?",
                (service_state, event_id, now, service_instance_id, epoch),
            )
            connection.execute(
                "UPDATE run_attempts SET state=?,updated_at=? WHERE operation_id=? AND epoch=?",
                (operation_state, now, operation_id, epoch),
            )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,?,?,?,?)",
                (
                    operation_id,
                    expected_operation_state,
                    operation_state,
                    now,
                    _safe_json({"event_id": event_id}),
                ),
            )

    def renew_service(self, service_instance_id: str, epoch: int, lease_until: datetime, event_id: str) -> None:
        now = self.clock.now()
        if lease_until <= now:
            raise LeaseRejected("renewed lease must be in the future")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT s.*, e.current_epoch FROM services s JOIN service_epochs e USING(service_kind) "
                "WHERE service_instance_id=?",
                (service_instance_id,),
            ).fetchone()
            if (
                row is None
                or row["state"] != "ACTIVE"
                or int(row["epoch"]) != epoch
                or int(row["current_epoch"]) != epoch
                or _parse_time(row["lease_until"]) <= now
            ):
                raise LeaseRejected("service renewal was fenced or arrived after lease expiry")
            connection.execute(
                "UPDATE services SET lease_until=?,latest_event_id=?,updated_at=? WHERE service_instance_id=?",
                (_format_time(lease_until), event_id, _format_time(now), service_instance_id),
            )

    def resolve_service(self, service_kind: str, *, now: datetime | None = None) -> ServiceRecord | None:
        now = now or self.clock.now()
        with self._reader() as connection:
            row = connection.execute(
                "SELECT s.* FROM services s JOIN service_epochs e USING(service_kind) "
                "WHERE s.service_kind=? AND s.state='ACTIVE' AND s.epoch=e.current_epoch AND s.lease_until>? "
                "ORDER BY s.epoch DESC LIMIT 1",
                (service_kind, _format_time(now)),
            ).fetchone()
        return self._service_from_row(row) if row else None

    def register_provider_resource(
        self,
        *,
        provider: str,
        resource_ref: str,
        resource_kind: str,
        source_identity: str,
        source_version: str,
        control_class: str,
        private: bool | None,
        state: str,
        metadata: Mapping[str, Any] | None = None,
    ) -> None:
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO provider_resources(provider,resource_ref,resource_kind,source_identity,source_version,"
                "control_class,private,state,metadata_json,observed_at) VALUES (?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(provider,resource_ref,source_version) DO UPDATE SET state=excluded.state,"
                "metadata_json=excluded.metadata_json,observed_at=excluded.observed_at",
                (
                    provider,
                    resource_ref,
                    resource_kind,
                    source_identity,
                    source_version,
                    control_class,
                    None if private is None else int(private),
                    state,
                    _safe_json(metadata),
                    _format_time(self.clock.now()),
                ),
            )

    def list_provider_resources(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500:
            raise ValueError("provider resource limit must be between 1 and 500")
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT provider,resource_ref,resource_kind,source_identity,source_version,control_class,"
                "private,state,metadata_json,observed_at FROM provider_resources "
                "ORDER BY observed_at DESC,provider,resource_ref LIMIT ?", (limit,)
            ).fetchall()
        return [
            {
                "provider": row["provider"],
                "resource_ref": row["resource_ref"],
                "resource_kind": row["resource_kind"],
                "source_identity": row["source_identity"],
                "source_version": row["source_version"],
                "control_class": row["control_class"],
                "private": None if row["private"] is None else bool(row["private"]),
                "state": row["state"],
                "metadata": json.loads(row["metadata_json"]),
                "observed_at": row["observed_at"],
            }
            for row in rows
        ]

    def record_connector_coverage(
        self,
        *,
        connector_kind: str,
        contract_version: str,
        state: str,
        observed_at: datetime,
    ) -> None:
        """Persist bounded connector progress metadata, never connector rows."""

        if (
            not connector_kind
            or len(connector_kind) > 100
            or not contract_version
            or len(contract_version) > 100
            or state not in {"PENDING", "COMPLETE", "FAILED"}
            or observed_at.tzinfo is None
        ):
            raise ValueError("connector coverage metadata is invalid")
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO connector_coverage_metadata(connector_kind,contract_version,state,observed_at,updated_at) "
                "VALUES (?,?,?,?,?) ON CONFLICT(connector_kind) DO UPDATE SET "
                "contract_version=excluded.contract_version,state=excluded.state,"
                "observed_at=excluded.observed_at,updated_at=excluded.updated_at",
                (
                    connector_kind,
                    contract_version,
                    state,
                    _format_time(observed_at),
                    _format_time(self.clock.now()),
                ),
            )

    def connector_coverage_metadata(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("connector coverage limit must be between 1 and 100")
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT connector_kind,contract_version,state,observed_at "
                "FROM connector_coverage_metadata ORDER BY connector_kind LIMIT ?",
                (limit,),
            ).fetchall()
        return [dict(row) for row in rows]

    def register_oauth_client(
        self,
        *,
        issuer: str,
        client_id: str,
        principal_id: str,
        allowed_scopes: frozenset[str],
        profile_kind: str,
        enabled: bool = True,
    ) -> None:
        if profile_kind not in {"reader", "owner_operator"} or not allowed_scopes:
            raise ValueError("OAuth client profile/scopes are invalid")
        scopes_json = _safe_json({"scopes": sorted(allowed_scopes)})
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO oauth_clients(issuer,client_id,enabled,allowed_scopes_json,principal_id,profile_kind,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(issuer,client_id) DO UPDATE SET "
                "enabled=excluded.enabled,allowed_scopes_json=excluded.allowed_scopes_json,"
                "principal_id=excluded.principal_id,profile_kind=excluded.profile_kind,updated_at=excluded.updated_at",
                (issuer, client_id, int(enabled), scopes_json, principal_id, profile_kind, now, now),
            )

    def register_configured_oauth_client(
        self,
        *,
        issuer: str,
        client_id: str,
        principal_id: str,
        allowed_scopes: frozenset[str],
        profile_kind: str,
    ) -> None:
        """Register static config without re-enabling an administratively disabled client.

        Startup reconciliation is one atomic statement.  On first registration
        the configured client is enabled; on conflict its policy metadata is
        refreshed while the durable security-state bit is deliberately absent
        from the update set.  This avoids a read/upsert race with revocation or
        an administrative disable.
        """

        if profile_kind not in {"reader", "owner_operator"} or not allowed_scopes:
            raise ValueError("OAuth client profile/scopes are invalid")
        scopes_json = _safe_json({"scopes": sorted(allowed_scopes)})
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO oauth_clients(issuer,client_id,enabled,allowed_scopes_json,principal_id,profile_kind,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?) ON CONFLICT(issuer,client_id) DO UPDATE SET "
                "allowed_scopes_json=excluded.allowed_scopes_json,principal_id=excluded.principal_id,"
                "profile_kind=excluded.profile_kind,updated_at=excluded.updated_at",
                (issuer, client_id, 1, scopes_json, principal_id, profile_kind, now, now),
            )

    def oauth_client(self, issuer: str, client_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM oauth_clients WHERE issuer=? AND client_id=?", (issuer, client_id)
            ).fetchone()
        if row is None:
            return None
        return {
            "issuer": row["issuer"], "client_id": row["client_id"],
            "enabled": bool(row["enabled"]),
            "allowed_scopes": frozenset(json.loads(row["allowed_scopes_json"])["scopes"]),
            "principal_id": row["principal_id"], "profile_kind": row["profile_kind"],
        }

    def request_master(
        self,
        *,
        request_id: str,
        idempotency_key: str,
        requested_by: str,
        intent: str,
        operation_id: str,
    ) -> tuple[dict[str, Any], bool]:
        if not all((request_id, idempotency_key, requested_by, intent, operation_id)):
            raise ValueError("master request identity is incomplete")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_requests WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if row is not None:
                expected = (request_id, requested_by, intent, operation_id)
                actual = (row["request_id"], row["requested_by"], row["intent"], row["operation_id"])
                if actual != expected:
                    raise IdempotencyConflict("master request key was reused with different identity")
                return dict(row), False
            connection.execute(
                "INSERT INTO master_requests(request_id,idempotency_key,requested_by,intent,operation_id,state,"
                "attempts,claim_until,created_at,updated_at) VALUES (?,?,?,?,?,'PENDING',0,NULL,?,?)",
                (request_id, idempotency_key, requested_by, intent, operation_id, now, now),
            )
            row = connection.execute(
                "SELECT * FROM master_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            assert row is not None
            return dict(row), True

    def latest_master_request(self) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_requests WHERE state<>'DONE' ORDER BY created_at DESC,request_id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def claim_master_request(self, *, claim_seconds: int = 900) -> dict[str, Any] | None:
        if not 30 <= claim_seconds <= 3_600:
            raise ValueError("master request claim lifetime is outside policy")
        now = self.clock.now()
        claim_until = _format_time(now + timedelta(seconds=claim_seconds))
        now_text = _format_time(now)
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_requests WHERE state='PENDING' OR "
                "(state='IN_PROGRESS' AND claim_until<=?) ORDER BY created_at,request_id LIMIT 1",
                (now_text,),
            ).fetchone()
            if row is None:
                return None
            changed = connection.execute(
                "UPDATE master_requests SET state='IN_PROGRESS',attempts=attempts+1,claim_until=?,updated_at=? "
                "WHERE request_id=? AND (state='PENDING' OR (state='IN_PROGRESS' AND claim_until<=?))",
                (claim_until, now_text, row["request_id"], now_text),
            ).rowcount
            if changed != 1:
                return None
            claimed = connection.execute(
                "SELECT * FROM master_requests WHERE request_id=?", (row["request_id"],)
            ).fetchone()
            assert claimed is not None
            return dict(claimed)

    def complete_master_request(self, request_id: str, operation_id: str) -> None:
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE master_requests SET state='DONE',claim_until=NULL,updated_at=? "
                "WHERE request_id=? AND operation_id=? AND state='IN_PROGRESS'",
                (_format_time(self.clock.now()), request_id, operation_id),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT state,operation_id FROM master_requests WHERE request_id=?", (request_id,)
                ).fetchone()
                if row is None or row["state"] != "DONE" or row["operation_id"] != operation_id:
                    raise StaleRuntimeEvent("master request completion lost its claim")

    def release_master_request(self, request_id: str) -> None:
        with self._transaction() as connection:
            connection.execute(
                "UPDATE master_requests SET state='PENDING',claim_until=NULL,updated_at=? "
                "WHERE request_id=? AND state='IN_PROGRESS'",
                (_format_time(self.clock.now()), request_id),
            )

    def create_oauth_authorization_grant(self, grant: Mapping[str, Any]) -> bool:
        scopes_json = _safe_json({"scopes": list(grant["scopes"])})
        with self._transaction() as connection:
            try:
                connection.execute(
                    "INSERT INTO oauth_authorization_grants(code_digest,code_challenge,client_id,redirect_uri,"
                    "resource,scopes_json,subject,nonce,authenticated_at,expires_at,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        grant["code_digest"], grant["code_challenge"], grant["client_id"],
                        grant["redirect_uri"], grant["resource"], scopes_json, grant["subject"],
                        grant.get("nonce"), int(grant["authenticated_at"]), int(grant["expires_at"]),
                        _format_time(self.clock.now()),
                    ),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def consume_oauth_authorization_grant(
        self, code_digest: str, *, now: int
    ) -> dict[str, Any] | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM oauth_authorization_grants WHERE code_digest=?", (code_digest,)
            ).fetchone()
            if row is None:
                return None
            connection.execute(
                "DELETE FROM oauth_authorization_grants WHERE code_digest=?", (code_digest,)
            )
            if int(row["expires_at"]) <= now:
                return None
            return {
                "code_digest": row["code_digest"], "code_challenge": row["code_challenge"],
                "client_id": row["client_id"], "redirect_uri": row["redirect_uri"],
                "resource": row["resource"],
                "scopes": tuple(json.loads(row["scopes_json"])["scopes"]),
                "subject": row["subject"], "nonce": row["nonce"],
                "authenticated_at": int(row["authenticated_at"]),
                "expires_at": int(row["expires_at"]),
            }

    def create_oauth_refresh_grant(self, grant: Mapping[str, Any]) -> bool:
        scopes_json = _safe_json({"scopes": list(grant["scopes"])})
        with self._transaction() as connection:
            try:
                connection.execute(
                    "INSERT INTO oauth_refresh_grants(credential_digest,family_id,client_id,resource,scopes_json,"
                    "subject,authenticated_at,expires_at,consumed_at,revoked_at,created_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        grant["credential_digest"], grant["family_id"], grant["client_id"],
                        grant["resource"], scopes_json, grant["subject"], int(grant["authenticated_at"]),
                        int(grant["expires_at"]), grant.get("consumed_at"), grant.get("revoked_at"),
                        _format_time(self.clock.now()),
                    ),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def rotate_oauth_refresh_grant(
        self,
        *,
        presented_digest: str,
        successor_digest: str,
        client_id: str,
        resource: str,
        requested_scopes: tuple[str, ...] | None,
        successor_expires_at: int,
        now: int,
    ) -> tuple[str, dict[str, Any] | None]:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM oauth_refresh_grants WHERE credential_digest=?", (presented_digest,)
            ).fetchone()
            if row is None:
                return "invalid", None
            if row["consumed_at"] is not None:
                connection.execute(
                    "UPDATE oauth_refresh_grants SET revoked_at=coalesce(revoked_at,?) WHERE family_id=?",
                    (now, row["family_id"]),
                )
                return "replayed", None
            current_scopes = tuple(json.loads(row["scopes_json"])["scopes"])
            successor_scopes = current_scopes if requested_scopes is None else requested_scopes
            conflict = connection.execute(
                "SELECT 1 FROM oauth_refresh_grants WHERE credential_digest=?", (successor_digest,)
            ).fetchone()
            if (
                row["revoked_at"] is not None
                or int(row["expires_at"]) <= now
                or row["client_id"] != client_id
                or row["resource"] != resource
                or not set(successor_scopes).issubset(current_scopes)
                or conflict is not None
            ):
                return "invalid", None
            connection.execute(
                "UPDATE oauth_refresh_grants SET consumed_at=? WHERE credential_digest=? AND consumed_at IS NULL",
                (now, presented_digest),
            )
            expires_at = min(int(row["expires_at"]), successor_expires_at)
            connection.execute(
                "INSERT INTO oauth_refresh_grants(credential_digest,family_id,client_id,resource,scopes_json,"
                "subject,authenticated_at,expires_at,consumed_at,revoked_at,created_at) "
                "VALUES (?,?,?,?,?,?,?,?,NULL,NULL,?)",
                (
                    successor_digest, row["family_id"], row["client_id"], row["resource"],
                    _safe_json({"scopes": list(successor_scopes)}), row["subject"],
                    int(row["authenticated_at"]), expires_at, _format_time(self.clock.now()),
                ),
            )
            return "rotated", {
                "credential_digest": successor_digest, "family_id": row["family_id"],
                "client_id": row["client_id"], "resource": row["resource"],
                "scopes": successor_scopes, "subject": row["subject"],
                "authenticated_at": int(row["authenticated_at"]), "expires_at": expires_at,
                "consumed_at": None, "revoked_at": None,
            }

    def revoke_oauth_refresh_grant(
        self, credential_digest: str, *, client_id: str, now: int
    ) -> None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT family_id,client_id FROM oauth_refresh_grants WHERE credential_digest=?",
                (credential_digest,),
            ).fetchone()
            if row is not None and row["client_id"] == client_id:
                connection.execute(
                    "UPDATE oauth_refresh_grants SET revoked_at=coalesce(revoked_at,?) WHERE family_id=?",
                    (now, row["family_id"]),
                )

    def persist_provider_effect_intent(self, payload: Mapping[str, Any]) -> None:
        """Persist the exact provider intent before any external mutation."""

        intent_json = _safe_json(payload)
        required = {
            "effect_id", "operation_id", "idempotency_key", "task_id",
            "action", "provider_ref", "request_sha256",
        }
        if not required.issubset(payload):
            raise ValueError("provider effect intent is missing durable identity fields")
        with self._transaction() as connection:
            try:
                connection.execute(
                    "INSERT INTO provider_effect_intents(effect_id,operation_id,idempotency_key,task_id,action,"
                    "provider_ref,request_sha256,intent_json,recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        str(payload["effect_id"]), str(payload["operation_id"]),
                        str(payload["idempotency_key"]), str(payload["task_id"]),
                        str(payload["action"]), str(payload["provider_ref"]),
                        str(payload["request_sha256"]), intent_json,
                        _format_time(self.clock.now()),
                    ),
                )
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT intent_json FROM provider_effect_intents WHERE effect_id=? OR idempotency_key=?",
                    (str(payload["effect_id"]), str(payload["idempotency_key"])),
                ).fetchone()
                if row is None or row["intent_json"] != intent_json:
                    raise IdempotencyConflict("provider effect identity was reused for a different intent") from None

    def persist_provider_effect_receipt(self, effect_id: str, payload: Mapping[str, Any]) -> None:
        receipt_json = _safe_json(payload)
        receipt_sha256 = hashlib.sha256(receipt_json.encode()).hexdigest()
        with self._transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM provider_effect_intents WHERE effect_id=?", (effect_id,)
            ).fetchone() is None:
                raise StaleRuntimeEvent("provider receipt has no persist-before-effect intent")
            # Receipts form an append-only reconciliation history.  An
            # UNCERTAIN observation may later become exactly APPLIED/ABSENT;
            # repeated identical observations collapse by the unique hash.
            connection.execute(
                "INSERT OR IGNORE INTO provider_effect_receipts(effect_id,receipt_json,receipt_sha256,recorded_at) "
                "VALUES (?,?,?,?)",
                (effect_id, receipt_json, receipt_sha256, _format_time(self.clock.now())),
            )

    def persist_provider_resource_claim(self, payload: Mapping[str, Any]) -> None:
        claim_json = _safe_json(payload)
        required = {
            "claim_sha256", "task_id", "effect_id", "provider_ref", "kind",
            "control_class", "disposable", "provider_version",
        }
        if not required.issubset(payload):
            raise ValueError("provider resource claim is missing durable identity fields")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT claim_json FROM provider_resource_claims "
                "WHERE effect_id=? OR (provider_ref=? AND resource_kind=? AND provider_version=?)",
                (
                    str(payload["effect_id"]),
                    str(payload["provider_ref"]),
                    str(payload["kind"]),
                    int(payload["provider_version"]),
                ),
            ).fetchall()
            if existing:
                if any(hmac.compare_digest(str(row["claim_json"]), claim_json) for row in existing):
                    return
                raise IdempotencyConflict(
                    "provider effect/resource version already has different claim authority"
                )
            try:
                connection.execute(
                    "INSERT INTO provider_resource_claims(claim_sha256,task_id,effect_id,provider_ref,resource_kind,"
                    "control_class,disposable,provider_version,claim_json,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        str(payload["claim_sha256"]), str(payload["task_id"]), str(payload["effect_id"]),
                        str(payload["provider_ref"]), str(payload["kind"]), str(payload["control_class"]),
                        int(bool(payload["disposable"])), int(payload["provider_version"]), claim_json,
                        _format_time(self.clock.now()),
                    ),
                )
            except sqlite3.IntegrityError:
                row = connection.execute(
                    "SELECT claim_json FROM provider_resource_claims WHERE claim_sha256=?",
                    (str(payload["claim_sha256"]),),
                ).fetchone()
                if row is None or row["claim_json"] != claim_json:
                    raise IdempotencyConflict("provider claim hash was reused for different authority") from None

    def assert_provider_resource_claim(self, claim_sha256: str, payload: Mapping[str, Any]) -> None:
        claim_json = _safe_json(payload)
        with self._reader() as connection:
            row = connection.execute(
                "SELECT claim_json FROM provider_resource_claims WHERE claim_sha256=?", (claim_sha256,)
            ).fetchone()
        if row is None or not hmac.compare_digest(row["claim_json"], claim_json):
            raise PermissionError("resource has no exact task-created claim in the durable control ledger")

    def provider_effect_authority(self, effect_id: str) -> dict[str, str] | None:
        """Return only the bounded identity needed to authorize a remote journal call."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT effect_id,operation_id,task_id,provider_ref,action "
                "FROM provider_effect_intents WHERE effect_id=?",
                (effect_id,),
            ).fetchone()
        if row is None:
            return None
        return {key: str(value) for key, value in dict(row).items()}

    def latest_provider_resource_claim(
        self,
        *,
        provider_ref: str,
        resource_kind: str,
        control_class: str,
    ) -> dict[str, Any] | None:
        """Resolve the exact highest-version durable claim for a protected resource."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT claim_json FROM provider_resource_claims WHERE provider_ref=? "
                "AND resource_kind=? AND control_class=? ORDER BY provider_version DESC LIMIT 1",
                (provider_ref, resource_kind, control_class),
            ).fetchone()
        return json.loads(str(row["claim_json"])) if row else None

    def acquire_resource_lease(
        self,
        *,
        lease_id: str,
        resource_kind: str,
        resource_ref: str,
        holder_id: str,
        lease_until: datetime,
    ) -> ResourceLeaseRecord:
        now = self.clock.now()
        if lease_until <= now:
            raise LeaseRejected("resource lease must expire in the future")
        with self._transaction() as connection:
            active = connection.execute(
                "SELECT * FROM resource_leases WHERE resource_kind=? AND resource_ref=? "
                "AND released_at IS NULL AND lease_until>? ORDER BY epoch DESC LIMIT 1",
                (resource_kind, resource_ref, _format_time(now)),
            ).fetchone()
            if active is not None:
                if active["lease_id"] == lease_id and active["holder_id"] == holder_id:
                    return self._resource_lease_from_row(active)
                raise LeaseRejected("resource already has an active lease")
            last = connection.execute(
                "SELECT max(epoch) FROM resource_leases WHERE resource_kind=? AND resource_ref=?",
                (resource_kind, resource_ref),
            ).fetchone()
            epoch = int(last[0] or 0) + 1
            connection.execute(
                "INSERT INTO resource_leases(lease_id,resource_kind,resource_ref,holder_id,epoch,acquired_at,"
                "lease_until) VALUES (?,?,?,?,?,?,?)",
                (lease_id, resource_kind, resource_ref, holder_id, epoch, _format_time(now), _format_time(lease_until)),
            )
            row = connection.execute("SELECT * FROM resource_leases WHERE lease_id=?", (lease_id,)).fetchone()
            assert row is not None
            return self._resource_lease_from_row(row)

    def renew_resource_lease(self, lease_id: str, holder_id: str, epoch: int, lease_until: datetime) -> None:
        now = self.clock.now()
        if lease_until <= now:
            raise LeaseRejected("resource lease renewal must expire in the future")
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE resource_leases SET lease_until=? WHERE lease_id=? AND holder_id=? AND epoch=? "
                "AND released_at IS NULL AND lease_until>?",
                (_format_time(lease_until), lease_id, holder_id, epoch, _format_time(now)),
            ).rowcount
            if changed != 1:
                raise LeaseRejected("resource lease renewal is stale, expired, or fenced")

    def release_resource_lease(self, lease_id: str, holder_id: str, epoch: int) -> None:
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE resource_leases SET released_at=? WHERE lease_id=? AND holder_id=? AND epoch=? "
                "AND released_at IS NULL",
                (_format_time(self.clock.now()), lease_id, holder_id, epoch),
            ).rowcount
            if changed != 1:
                raise LeaseRejected("resource lease release is stale or fenced")

    def prune_runtime_events(
        self,
        *,
        nonterminal_before: datetime,
        terminal_before: datetime | None = None,
    ) -> tuple[int, int]:
        """Apply explicit retention while preserving each attempt's latest projection event."""

        terminal_types = tuple(sorted(TERMINAL_EVENT_TYPES))
        terminal_placeholders = ",".join("?" for _ in terminal_types)
        with self._transaction() as connection:
            query = (
                "DELETE FROM runtime_events WHERE event_id NOT IN (SELECT latest_event_id FROM runtime_projection) "
                f"AND ((event_type NOT IN ({terminal_placeholders}) AND received_at<?)"
            )
            params: tuple[Any, ...] = (*terminal_types, _format_time(nonterminal_before))
            if terminal_before is not None:
                query += f" OR (event_type IN ({terminal_placeholders}) AND received_at<?)"
                params += (*terminal_types, _format_time(terminal_before))
            query += ")"
            events_deleted = connection.execute(query, params).rowcount
            dedupe_deleted = connection.execute(
                "DELETE FROM runtime_event_dedup WHERE event_id NOT IN (SELECT event_id FROM runtime_events) "
                "AND first_seen_at<?",
                (_format_time(terminal_before or nonterminal_before),),
            ).rowcount
            connection.execute(
                "INSERT INTO retention_runs(retention_run_id,nonterminal_before,terminal_before,events_deleted,"
                "dedupe_keys_deleted,recorded_at) VALUES (?,?,?,?,?,?)",
                (
                    str(uuid4()),
                    _format_time(nonterminal_before),
                    _format_time(terminal_before) if terminal_before else None,
                    events_deleted,
                    dedupe_deleted,
                    _format_time(self.clock.now()),
                ),
            )
            return events_deleted, dedupe_deleted

    def add_checkpoint_candidate(
        self,
        *,
        checkpoint_id: str,
        service_kind: str = "postgres-master",
        operation_id: str,
        dataset_ref: str,
        version_ref: str | None,
        manifest_sha256: str,
        source_checkpoint_id: str | None,
        source_head_generation: int | None = None,
        master_instance_id: str,
        epoch: int,
        manifest_payload: Mapping[str, Any] | None = None,
    ) -> None:
        if len(manifest_sha256) != 64:
            raise ValueError("manifest_sha256 must be an exact SHA-256")
        with self._transaction() as connection:
            head = connection.execute(
                "SELECT generation,current_checkpoint_id FROM checkpoint_heads WHERE service_kind=?",
                (service_kind,),
            ).fetchone()
            current_generation = int(head["generation"]) if head else 0
            current_checkpoint_id = (
                str(head["current_checkpoint_id"]) if head and head["current_checkpoint_id"] else None
            )
            expected_generation = current_generation if source_head_generation is None else source_head_generation
            if expected_generation != current_generation or source_checkpoint_id != current_checkpoint_id:
                raise StaleRuntimeEvent("checkpoint candidate source is not current HEAD")
            values = (
                checkpoint_id,
                service_kind,
                operation_id,
                dataset_ref,
                version_ref,
                manifest_sha256,
                source_checkpoint_id,
                expected_generation,
                master_instance_id,
                epoch,
                _format_time(self.clock.now()),
            )
            changed = connection.execute(
                "INSERT INTO checkpoint_candidates(checkpoint_id,service_kind,operation_id,dataset_ref,version_ref,"
                "manifest_sha256,source_checkpoint_id,source_head_generation,master_instance_id,epoch,status,"
                "created_at,manifest_json) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,'CANDIDATE',?,?) ON CONFLICT(checkpoint_id) DO NOTHING",
                (*values, _safe_json(manifest_payload) if manifest_payload is not None else None),
            ).rowcount
            if changed == 0:
                existing = connection.execute(
                    "SELECT service_kind,operation_id,dataset_ref,manifest_sha256,source_checkpoint_id,"
                    "source_head_generation,master_instance_id,epoch "
                    "FROM checkpoint_candidates WHERE checkpoint_id=?",
                    (checkpoint_id,),
                ).fetchone()
                immutable = (
                    service_kind,
                    operation_id,
                    dataset_ref,
                    manifest_sha256,
                    source_checkpoint_id,
                    expected_generation,
                    master_instance_id,
                    epoch,
                )
                if existing is None or tuple(existing) != immutable:
                    raise StaleRuntimeEvent("checkpoint idempotency identity conflicts with durable candidate")

    def mark_checkpoint_uploaded(self, checkpoint_id: str, version_ref: str) -> None:
        if not version_ref or len(version_ref) > 512:
            raise ValueError("checkpoint exact version ref is invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE checkpoint_candidates SET status='UPLOADED',version_ref=? "
                "WHERE checkpoint_id=? AND status='CANDIDATE' AND version_ref IS NULL",
                (version_ref, checkpoint_id),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT status,version_ref FROM checkpoint_candidates WHERE checkpoint_id=?", (checkpoint_id,)
                ).fetchone()
                if (
                    row is None
                    or row["status"] not in {"UPLOADED", "READBACK_VERIFIED", "RESTORE_VERIFIED", "VERIFIED"}
                    or row["version_ref"] != version_ref
                ):
                    raise StaleRuntimeEvent("checkpoint candidate cannot be uploaded from its current state")

    def record_checkpoint_package_sha256(self, checkpoint_id: str, package_sha256: str) -> None:
        if len(package_sha256) != 64 or any(char not in "0123456789abcdef" for char in package_sha256):
            raise ValueError("checkpoint package hash is invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE checkpoint_candidates SET package_sha256=coalesce(package_sha256,?) "
                "WHERE checkpoint_id=? AND (package_sha256 IS NULL OR package_sha256=?)",
                (package_sha256, checkpoint_id, package_sha256),
            ).rowcount
            if changed != 1:
                raise StaleRuntimeEvent("checkpoint package hash conflicts with durable identity")

    def mark_checkpoint_readback_verified(self, checkpoint_id: str) -> None:
        self._checkpoint_transition(
            checkpoint_id,
            "UPLOADED",
            "READBACK_VERIFIED",
            later_statuses={"RESTORE_VERIFIED", "VERIFIED"},
        )

    def mark_checkpoint_restore_verified(self, checkpoint_id: str) -> None:
        self._checkpoint_transition(
            checkpoint_id,
            "READBACK_VERIFIED",
            "RESTORE_VERIFIED",
            later_statuses={"VERIFIED"},
        )

    def mark_checkpoint_verified(self, checkpoint_id: str) -> None:
        self._checkpoint_transition(
            checkpoint_id,
            "RESTORE_VERIFIED",
            "VERIFIED",
            verified_at=_format_time(self.clock.now()),
        )

    def _checkpoint_transition(
        self,
        checkpoint_id: str,
        expected: str,
        target: str,
        *,
        verified_at: str | None = None,
        later_statuses: set[str] | None = None,
    ) -> None:
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE checkpoint_candidates SET status=?,verified_at=coalesce(?,verified_at) "
                "WHERE checkpoint_id=? AND status=?",
                (target, verified_at, checkpoint_id, expected),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT status FROM checkpoint_candidates WHERE checkpoint_id=?", (checkpoint_id,)
                ).fetchone()
                if row is None or row["status"] not in ({target} | (later_statuses or set())):
                    raise StaleRuntimeEvent(
                        f"checkpoint candidate cannot advance {expected}->{target} from its current state"
                    )

    def fail_checkpoint(self, checkpoint_id: str, failure_code: str) -> None:
        with self._transaction() as connection:
            connection.execute(
                "UPDATE checkpoint_candidates SET status='FAILED',failure_code=? "
                "WHERE checkpoint_id=? AND status<>'VERIFIED'",
                (failure_code, checkpoint_id),
            )

    def promote_checkpoint(
        self,
        service_kind: str,
        checkpoint_id: str,
        *,
        expected_generation: int,
        expected_parent_checkpoint_id: str | None,
    ) -> CheckpointHead:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            candidate = connection.execute(
                "SELECT status,service_kind,source_checkpoint_id,source_head_generation "
                "FROM checkpoint_candidates WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            if candidate is None or candidate["service_kind"] != service_kind:
                raise StaleRuntimeEvent("checkpoint candidate is absent or belongs to another service")
            current = connection.execute(
                "SELECT generation,current_checkpoint_id FROM checkpoint_heads WHERE service_kind=?", (service_kind,)
            ).fetchone()
            generation = int(current["generation"]) if current else 0
            old_current = current["current_checkpoint_id"] if current else None
            if old_current == checkpoint_id and candidate["status"] == "VERIFIED":
                row = connection.execute(
                    "SELECT * FROM checkpoint_heads WHERE service_kind=?", (service_kind,)
                ).fetchone()
                assert row is not None
                return self._checkpoint_head_from_row(row)
            if candidate["status"] not in {"RESTORE_VERIFIED", "VERIFIED"}:
                raise StaleRuntimeEvent("only a restore-verified checkpoint can advance HEAD")
            if (
                generation != expected_generation
                or old_current != expected_parent_checkpoint_id
                or int(candidate["source_head_generation"]) != expected_generation
                or candidate["source_checkpoint_id"] != expected_parent_checkpoint_id
            ):
                raise StaleRuntimeEvent("checkpoint HEAD generation or parent changed concurrently")
            if current is None:
                connection.execute(
                    "INSERT INTO checkpoint_heads(service_kind,generation,current_checkpoint_id,"
                    "previous_checkpoint_id,updated_at) VALUES (?,?,?,?,?)",
                    (service_kind, 1, checkpoint_id, old_current, now),
                )
            else:
                changed = connection.execute(
                    "UPDATE checkpoint_heads SET generation=generation+1,previous_checkpoint_id=current_checkpoint_id,"
                    "current_checkpoint_id=?,updated_at=? WHERE service_kind=? AND generation=? "
                    "AND current_checkpoint_id IS ?",
                    (checkpoint_id, now, service_kind, expected_generation, expected_parent_checkpoint_id),
                ).rowcount
                if changed != 1:
                    raise StaleRuntimeEvent("checkpoint HEAD compare-and-swap lost")
            if candidate["status"] == "RESTORE_VERIFIED":
                verified = connection.execute(
                    "UPDATE checkpoint_candidates SET status='VERIFIED',verified_at=? "
                    "WHERE checkpoint_id=? AND status='RESTORE_VERIFIED'",
                    (now, checkpoint_id),
                ).rowcount
                if verified != 1:
                    raise StaleRuntimeEvent("checkpoint verification state changed during promotion")
            row = connection.execute("SELECT * FROM checkpoint_heads WHERE service_kind=?", (service_kind,)).fetchone()
            assert row is not None
            return self._checkpoint_head_from_row(row)

    def checkpoint_head(self, service_kind: str) -> CheckpointHead | None:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM checkpoint_heads WHERE service_kind=?", (service_kind,)).fetchone()
        return self._checkpoint_head_from_row(row) if row else None

    def checkpoint_candidate(self, checkpoint_id: str) -> dict[str, Any] | None:
        """Return a metadata-only checkpoint projection; archive bytes never enter the ledger."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT checkpoint_id,service_kind,operation_id,dataset_ref,version_ref,manifest_sha256,"
                "source_checkpoint_id,source_head_generation,master_instance_id,epoch,status,verified_at,"
                "manifest_json,package_sha256 "
                "FROM checkpoint_candidates WHERE checkpoint_id=?",
                (checkpoint_id,),
            ).fetchone()
        if row is None:
            return None
        result = dict(row)
        result["epoch"] = int(result["epoch"])
        result["source_head_generation"] = int(result["source_head_generation"])
        result["manifest"] = json.loads(result.pop("manifest_json")) if result["manifest_json"] else None
        return result

    def ensure_blogger_migration_request(
        self, *, request_id: str, operation_id: str, request_sha256: str, request: Mapping[str, Any]
    ) -> tuple[dict[str, Any], bool]:
        """Persist one secret-free importer request before the master may claim it."""

        if len(request_sha256) != 64:
            raise ValueError("blogger request hash must be an exact SHA-256")
        request_json = _safe_json(request)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "INSERT INTO blogger_migration_requests(request_id,operation_id,request_sha256,request_json,state,"
                "created_at,updated_at) VALUES (?,?,?,?,'REQUESTED',?,?) ON CONFLICT(request_id) DO NOTHING",
                (request_id, operation_id, request_sha256, request_json, now, now),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (row["operation_id"], row["request_sha256"], row["request_json"]) != (
                operation_id, request_sha256, request_json
            ):
                raise IdempotencyConflict("blogger request identity was reused for different metadata")
            return self._blogger_request_from_row(row), bool(changed)

    def claim_blogger_migration_request(
        self, *, operation_id: str, run_id: str, attempt_id: str, master_instance_id: str, epoch: int
    ) -> dict[str, Any] | None:
        """Atomically bind the pending request to the exact ACTIVE runtime attempt."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                return None
            if row["state"] == "REQUESTED":
                connection.execute(
                    "UPDATE blogger_migration_requests SET state='CLAIMED',claimed_run_id=?,claimed_attempt_id=?,"
                    "claimed_master_instance_id=?,claimed_epoch=?,updated_at=? WHERE request_id=? AND state='REQUESTED'",
                    (run_id, attempt_id, master_instance_id, epoch, now, row["request_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM blogger_migration_requests WHERE request_id=?", (row["request_id"],)
                ).fetchone()
            assert row is not None
            claimed = (row["claimed_run_id"], row["claimed_attempt_id"], row["claimed_master_instance_id"], row["claimed_epoch"])
            if claimed != (run_id, attempt_id, master_instance_id, epoch):
                raise StaleRuntimeEvent("blogger migration request belongs to another runtime epoch")
            return self._blogger_request_from_row(row)

    def record_blogger_import_receipt(
        self, *, request_id: str, run_id: str, attempt_id: str, receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        receipt_json = _safe_json(receipt)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE blogger_migration_requests SET state='IMPORT_COMMITTED',import_receipt_json=?,updated_at=? "
                "WHERE request_id=? AND state='CLAIMED' AND claimed_run_id=? AND claimed_attempt_id=?",
                (receipt_json, now, request_id, run_id, attempt_id),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (changed != 1 and row["import_receipt_json"] != receipt_json):
                raise StaleRuntimeEvent("blogger import receipt does not bind the claimed runtime")
            return self._blogger_request_from_row(row)

    def record_blogger_checkpoint_receipt(
        self, *, request_id: str, run_id: str, attempt_id: str, receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        receipt_json = _safe_json(receipt)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE blogger_migration_requests SET state='CHECKPOINT_VERIFIED',checkpoint_receipt_json=?,updated_at=? "
                "WHERE request_id=? AND state='IMPORT_COMMITTED' AND claimed_run_id=? AND claimed_attempt_id=?",
                (receipt_json, now, request_id, run_id, attempt_id),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (changed != 1 and row["checkpoint_receipt_json"] != receipt_json):
                raise StaleRuntimeEvent("blogger checkpoint receipt does not follow committed import")
            return self._blogger_request_from_row(row)

    def fail_blogger_migration_request(
        self, *, request_id: str, run_id: str, attempt_id: str, failure_code: str
    ) -> None:
        if not failure_code or len(failure_code) > 100:
            raise ValueError("blogger failure code is invalid")
        with self._transaction() as connection:
            connection.execute(
                "UPDATE blogger_migration_requests SET state='FAILED',failure_code=?,updated_at=? "
                "WHERE request_id=? AND claimed_run_id=? AND claimed_attempt_id=? "
                "AND state IN ('CLAIMED','IMPORT_COMMITTED')",
                (failure_code, _format_time(self.clock.now()), request_id, run_id, attempt_id),
            )

    def verified_checkpoint_for_operation(self, operation_id: str) -> dict[str, Any] | None:
        """Return the exact current VERIFIED candidate owned by one master operation."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT c.checkpoint_id,c.manifest_sha256,c.version_ref FROM checkpoint_candidates c "
                "JOIN checkpoint_heads h ON h.service_kind=c.service_kind "
                "AND h.current_checkpoint_id=c.checkpoint_id "
                "WHERE c.operation_id=? AND c.status='VERIFIED' AND c.version_ref IS NOT NULL",
                (operation_id,),
            ).fetchone()
        return dict(row) if row else None

    def blogger_migration_request(self, request_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
        return self._blogger_request_from_row(row) if row else None

    @staticmethod
    def _blogger_request_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["request"] = json.loads(value.pop("request_json"))
        import_receipt_json = value.pop("import_receipt_json")
        checkpoint_receipt_json = value.pop("checkpoint_receipt_json")
        value["import_receipt"] = json.loads(import_receipt_json) if import_receipt_json else None
        value["checkpoint_receipt"] = (
            json.loads(checkpoint_receipt_json) if checkpoint_receipt_json else None
        )
        if value["claimed_epoch"] is not None:
            value["claimed_epoch"] = int(value["claimed_epoch"])
        return value

    def revoke_oauth_reference(
        self,
        *,
        token_reference: str,
        client_id: str,
        principal_id: str | None,
        reason_code: str,
        audit_ref: str,
    ) -> str:
        digest = hashlib.sha256(token_reference.encode()).hexdigest()
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO oauth_revocations("
                "token_ref_sha256,client_id,principal_id,reason_code,revoked_at,audit_ref) "
                "VALUES (?,?,?,?,?,?) ON CONFLICT(token_ref_sha256) DO NOTHING",
                (digest, client_id, principal_id, reason_code, _format_time(self.clock.now()), audit_ref),
            )
        return digest

    def is_oauth_reference_revoked(self, token_reference: str) -> bool:
        digest = hashlib.sha256(token_reference.encode()).hexdigest()
        with self._reader() as connection:
            row = connection.execute("SELECT 1 FROM oauth_revocations WHERE token_ref_sha256=?", (digest,)).fetchone()
        return row is not None

    def append_audit(
        self,
        *,
        action: str,
        audit_ref: str,
        principal_id: str | None = None,
        client_id: str | None = None,
        operation_id: str | None = None,
        epoch: int | None = None,
        revision: int | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> str:
        audit_id = str(uuid4())
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO audit_log(audit_id,principal_id,client_id,action,operation_id,epoch,revision,audit_ref,"
                "recorded_at,metadata_json) VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    audit_id,
                    principal_id,
                    client_id,
                    action,
                    operation_id,
                    epoch,
                    revision,
                    audit_ref,
                    _format_time(self.clock.now()),
                    _safe_json(metadata),
                ),
            )
        return audit_id

    def record_master_terminal_recovery_evidence(
        self,
        *,
        operation_id: str,
        epoch: int,
        output_receipt_sha256: str,
        provider_status: str,
        metadata: Mapping[str, Any],
    ) -> str:
        """Persist one idempotent, metadata-only provider recovery receipt."""

        if (
            epoch < 1
            or len(output_receipt_sha256) != 64
            or any(character not in "0123456789abcdef" for character in output_receipt_sha256)
            or provider_status not in {"queued", "running", "complete", "error", "unknown"}
        ):
            raise ValueError("master terminal recovery evidence identity is invalid")
        expected_keys = {
            "schema_version",
            "run_id",
            "attempt_id",
            "service_instance_id",
            "master_instance_id",
            "source_identity",
            "source_version",
            "checkpoint_id",
            "manifest_sha256",
            "output_tree_sha256",
            "output_receipt_sha256",
            "provider_status",
            "events",
        }
        events = metadata.get("events")
        if (
            set(metadata) != expected_keys
            or metadata.get("schema_version") != "my-data-hub-master-terminal-recovery-evidence.v1"
            or metadata.get("provider_status") != provider_status
            or metadata.get("output_receipt_sha256") != output_receipt_sha256
            or not isinstance(events, list)
            or len(events) != 4
            or any(
                not isinstance(event, Mapping)
                or set(event) != {"event_id", "body_sha256"}
                or not isinstance(event.get("event_id"), str)
                or not 1 <= len(str(event["event_id"])) <= 200
                or not isinstance(event.get("body_sha256"), str)
                or len(str(event["body_sha256"])) != 64
                or any(character not in "0123456789abcdef" for character in str(event["body_sha256"]))
                for event in events
            )
            or len({str(event["event_id"]) for event in events}) != 4
            or any(
                not isinstance(metadata[key], str) or not metadata[key] or len(str(metadata[key])) > 500
                for key in expected_keys - {"events"}
            )
        ):
            raise ValueError("master terminal recovery metadata contract is invalid")
        metadata_json = _safe_json(metadata, max_bytes=MAX_METADATA_BYTES)
        audit_ref = f"kaggle-terminal-output-sha256:{output_receipt_sha256}"
        audit_id = hashlib.sha256(
            f"master-terminal-recovery-v1:{operation_id}:{epoch}:{output_receipt_sha256}".encode()
        ).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO audit_log(audit_id,principal_id,client_id,action,operation_id,epoch,revision,audit_ref,"
                "recorded_at,metadata_json) VALUES (?,NULL,NULL,'master.terminal_recovery',?,?,NULL,?,?,?) "
                "ON CONFLICT(audit_id) DO NOTHING",
                (audit_id, operation_id, epoch, audit_ref, now, metadata_json),
            )
            row = connection.execute(
                "SELECT action,operation_id,epoch,audit_ref,metadata_json FROM audit_log WHERE audit_id=?",
                (audit_id,),
            ).fetchone()
            if row is None or (
                row["action"],
                row["operation_id"],
                int(row["epoch"]),
                row["audit_ref"],
                row["metadata_json"],
            ) != (
                "master.terminal_recovery",
                operation_id,
                epoch,
                audit_ref,
                metadata_json,
            ):
                raise IdempotencyConflict("master terminal recovery evidence identity collision")
        return audit_id

    @staticmethod
    def _operation_from_row(row: sqlite3.Row) -> OperationRecord:
        return OperationRecord(
            operation_id=row["operation_id"],
            idempotency_key=row["idempotency_key"],
            operation_kind=row["operation_kind"],
            intent_hash=row["intent_hash"],
            state=row["state"],
            identity=json.loads(row["identity_json"]),
            created_at=_parse_time(row["created_at"]),
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _effect_from_row(row: sqlite3.Row) -> EffectRecord:
        return EffectRecord(
            effect_id=row["effect_id"],
            operation_id=row["operation_id"],
            idempotency_key=row["idempotency_key"],
            effect_kind=row["effect_kind"],
            exact_identity=json.loads(row["exact_identity_json"]),
            state=EffectState(row["state"]),
            receipt=json.loads(row["receipt_json"]) if row["receipt_json"] else None,
            planned_at=_parse_time(row["planned_at"]),
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _service_from_row(row: sqlite3.Row) -> ServiceRecord:
        return ServiceRecord(
            service_instance_id=row["service_instance_id"],
            service_kind=row["service_kind"],
            run_id=row["run_id"],
            attempt_id=row["attempt_id"],
            master_instance_id=row["master_instance_id"],
            epoch=int(row["epoch"]),
            endpoint=row["endpoint"],
            protocol=row["protocol"],
            tls_fingerprint=row["tls_fingerprint"],
            capabilities=tuple(json.loads(row["capabilities_json"])),
            canonical_revision=row["canonical_revision"],
            schema_version=row["schema_version"],
            lease_until=_parse_time(row["lease_until"]),
            state=row["state"],
            latest_event_id=row["latest_event_id"],
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _checkpoint_head_from_row(row: sqlite3.Row) -> CheckpointHead:
        return CheckpointHead(
            service_kind=row["service_kind"],
            generation=int(row["generation"]),
            current_checkpoint_id=row["current_checkpoint_id"],
            previous_checkpoint_id=row["previous_checkpoint_id"],
            updated_at=_parse_time(row["updated_at"]),
        )

    @staticmethod
    def _resource_lease_from_row(row: sqlite3.Row) -> ResourceLeaseRecord:
        return ResourceLeaseRecord(
            lease_id=row["lease_id"],
            resource_kind=row["resource_kind"],
            resource_ref=row["resource_ref"],
            holder_id=row["holder_id"],
            epoch=int(row["epoch"]),
            acquired_at=_parse_time(row["acquired_at"]),
            lease_until=_parse_time(row["lease_until"]),
            released_at=_parse_time(row["released_at"]) if row["released_at"] else None,
        )
