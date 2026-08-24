from __future__ import annotations

import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
import threading
from collections.abc import Iterator, Mapping
from contextlib import contextmanager, suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Literal
from uuid import UUID, uuid4

from my_data_hub.control_plane.clock import Clock, SystemClock

from .errors import EventRejected, IdempotencyConflict, LeaseRejected, MasterAdmissionRejected, StaleRuntimeEvent
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
                safe_boolean_observation = lowered == "credentials_invalidated" and nested is True
                safe_authorization_identifier = False
                if lowered == "authorization_id" and isinstance(nested, str):
                    with suppress(ValueError):
                        safe_authorization_identifier = str(UUID(nested)) == nested
                if (
                    any(fragment in lowered for fragment in _SECRET_FRAGMENTS)
                    and not safe_boolean_observation
                    and not safe_authorization_identifier
                ):
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
                try:
                    before = os.stat(candidate, follow_symlinks=False)
                except FileNotFoundError:
                    # SQLite may unlink WAL/SHM while a connection is closing.
                    # Absence is safe; each connection tightens new sidecars.
                    continue
                except OSError as exc:
                    raise ValueError("control ledger files must be regular non-symlinks") from exc

                if not stat.S_ISREG(before.st_mode):
                    raise ValueError("control ledger files must be regular non-symlinks")
                try:
                    # Do not open and close the database, WAL or SHM paths here.  On
                    # POSIX, closing any descriptor for a file releases every advisory
                    # lock that this process holds for that inode.  Doing so while a
                    # sibling SQLite connection is active can invalidate WAL locking
                    # and crash a process with SIGBUS.  Path-based chmod preserves the
                    # SQLite descriptors and their locks.
                    os.chmod(candidate, 0o600, follow_symlinks=False)
                    after = os.stat(candidate, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                except OSError as exc:
                    raise ValueError("control ledger files must be regular non-symlinks") from exc
                if (
                    not stat.S_ISREG(after.st_mode)
                    or (after.st_dev, after.st_ino) != (before.st_dev, before.st_ino)
                ):
                    raise ValueError("control ledger files must be stable regular non-symlinks")

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

    def ensure_master_operation(
        self,
        *,
        operation_id: str,
        idempotency_key: str,
        intent: Mapping[str, Any],
        identity: Mapping[str, Any],
        service_kind: str = "postgres-master",
    ) -> tuple[OperationRecord, bool]:
        """Atomically admit one master epoch or replay its exact request.

        Epoch allocation is the admission decision. It shares the same
        ``BEGIN IMMEDIATE`` transaction with every lifecycle, service and
        checkpoint check so distinct requests cannot race and fence each
        other before either provider effect is durable.
        """

        terminal_states = {"STOPPED", "FAILED", "FENCED", "ORPHANED"}
        intent_json = _safe_json(intent)
        intent_hash = hashlib.sha256(intent_json.encode()).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key=?", (idempotency_key,)
            ).fetchone()
            if existing is not None:
                if existing["intent_hash"] != intent_hash or existing["operation_kind"] != "ensure_master":
                    raise IdempotencyConflict("idempotency key was reused for a different control intent")
                return self._operation_from_row(existing), False

            predecessors = connection.execute(
                "SELECT * FROM operations WHERE operation_kind='ensure_master' ORDER BY created_at,operation_id"
            ).fetchall()
            incomplete = next((row for row in predecessors if row["state"] not in terminal_states), None)
            if incomplete is not None:
                raise MasterAdmissionRejected(
                    "master admission rejected: lifecycle operation "
                    f"{incomplete['operation_id']} is {incomplete['state']}"
                )

            live_service = connection.execute(
                "SELECT service_instance_id,state FROM services "
                "WHERE service_kind=? AND state IN ('ACTIVE','DRAINING') "
                "ORDER BY epoch DESC LIMIT 1",
                (service_kind,),
            ).fetchone()
            if live_service is not None:
                raise MasterAdmissionRejected(
                    "master admission rejected: service "
                    f"{live_service['service_instance_id']} is {live_service['state']}"
                )

            epoch_row = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind=?", (service_kind,)
            ).fetchone()
            current_epoch = int(epoch_row["current_epoch"]) if epoch_row is not None else 0
            predecessor: sqlite3.Row | None = None
            predecessor_identity: dict[str, Any] | None = None
            predecessor_epoch = 0
            for row in predecessors:
                try:
                    candidate_identity = json.loads(str(row["identity_json"]))
                    candidate_epoch = int(candidate_identity["epoch"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise MasterAdmissionRejected(
                        "master admission rejected: predecessor epoch identity is invalid"
                    ) from exc
                if candidate_epoch > predecessor_epoch:
                    predecessor = row
                    predecessor_identity = candidate_identity
                    predecessor_epoch = candidate_epoch
            if (predecessor is None and current_epoch != 0) or (
                predecessor is not None and predecessor_epoch != current_epoch
            ):
                raise MasterAdmissionRejected("master admission rejected: lifecycle and epoch ledgers are inconsistent")

            if predecessor is not None and predecessor["state"] == "STOPPED":
                assert predecessor_identity is not None
                checkpoint = connection.execute(
                    "SELECT c.operation_id,c.master_instance_id,c.epoch,c.status "
                    "FROM checkpoint_heads h JOIN checkpoint_candidates c "
                    "ON c.checkpoint_id=h.current_checkpoint_id WHERE h.service_kind=?",
                    (service_kind,),
                ).fetchone()
                if (
                    checkpoint is None
                    or checkpoint["status"] != "VERIFIED"
                    or checkpoint["operation_id"] != predecessor["operation_id"]
                    or checkpoint["master_instance_id"] != predecessor_identity.get("master_instance_id")
                    or int(checkpoint["epoch"]) != predecessor_epoch
                ):
                    raise MasterAdmissionRejected(
                        "master admission rejected: stopped predecessor lacks its verified checkpoint"
                    )

            rotation_prefix = "forced-rotation:"
            if idempotency_key.startswith(rotation_prefix):
                request_id = idempotency_key.removeprefix(rotation_prefix)
                rotation = connection.execute(
                    "SELECT operation_kind,state,identity_json FROM operations WHERE operation_id=?",
                    (request_id,),
                ).fetchone()
                if (
                    rotation is None
                    or rotation["operation_kind"] != "forced_master_rotation"
                    or rotation["state"] != "REQUESTED"
                ):
                    raise MasterAdmissionRejected("master admission rejected: forced rotation binding is invalid")
                try:
                    rotation_identity = json.loads(str(rotation["identity_json"]))
                    expected_epoch = int(rotation_identity["expected_active_epoch"])
                    expected_generation = int(rotation_identity["head_generation"])
                    expected_checkpoint = str(rotation_identity["checkpoint_id"])
                except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise MasterAdmissionRejected(
                        "master admission rejected: forced rotation binding is invalid"
                    ) from exc
                head = connection.execute(
                    "SELECT generation,current_checkpoint_id FROM checkpoint_heads WHERE service_kind=?",
                    (service_kind,),
                ).fetchone()
                candidate = connection.execute(
                    "SELECT operation_id,master_instance_id,epoch,status "
                    "FROM checkpoint_candidates WHERE checkpoint_id=?",
                    (expected_checkpoint,),
                ).fetchone()
                source = (
                    connection.execute(
                        "SELECT state,identity_json FROM operations WHERE operation_id=?",
                        (candidate["operation_id"],),
                    ).fetchone()
                    if candidate is not None
                    else None
                )
                if source is None:
                    raise MasterAdmissionRejected("master admission rejected: forced rotation source is invalid")
                try:
                    source_identity = json.loads(str(source["identity_json"]))
                except (TypeError, ValueError, json.JSONDecodeError) as exc:
                    raise MasterAdmissionRejected(
                        "master admission rejected: forced rotation source is invalid"
                    ) from exc
                if (
                    predecessor is None
                    or current_epoch != expected_epoch
                    or head is None
                    or int(head["generation"]) != expected_generation
                    or head["current_checkpoint_id"] != expected_checkpoint
                    or candidate is None
                    or predecessor["operation_id"] != candidate["operation_id"]
                    or predecessor["state"] != "STOPPED"
                    or candidate["status"] != "VERIFIED"
                    or int(candidate["epoch"]) != expected_epoch
                    or source["state"] != "STOPPED"
                    or int(source_identity.get("epoch", 0)) != expected_epoch
                    or candidate["master_instance_id"] != source_identity.get("master_instance_id")
                ):
                    raise MasterAdmissionRejected(
                        "master admission rejected: forced rotation lacks the exact verified STOPPED handoff"
                    )

            epoch = current_epoch + 1
            connection.execute(
                "INSERT INTO service_epochs(service_kind,current_epoch,updated_at) VALUES (?,?,?) "
                "ON CONFLICT(service_kind) DO UPDATE SET current_epoch=excluded.current_epoch,"
                "updated_at=excluded.updated_at",
                (service_kind, epoch, now),
            )
            durable_identity = {**identity, "epoch": epoch}
            connection.execute(
                "INSERT INTO operations(operation_id,idempotency_key,operation_kind,intent_hash,state,identity_json,"
                "created_at,updated_at) VALUES (?,?,'ensure_master',?,'REQUESTED',?,?,?)",
                (operation_id, idempotency_key, intent_hash, _safe_json(durable_identity), now, now),
            )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,NULL,'REQUESTED',?,?)",
                (operation_id, now, _safe_json({"reason": "master_admitted", "epoch": epoch})),
            )
            row = connection.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            assert row is not None
            return self._operation_from_row(row), True

    def get_operation(self, operation_id: str) -> OperationRecord | None:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM operations WHERE operation_id = ?", (operation_id,)).fetchone()
        return self._operation_from_row(row) if row else None

    def get_operation_by_idempotency_key(self, idempotency_key: str) -> OperationRecord | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM operations WHERE idempotency_key = ?", (idempotency_key,)
            ).fetchone()
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

    def fail_unstarted_master_after_tunnel_expiry(
        self,
        *,
        operation_id: str,
        effect_id: str,
        run_id: str,
        attempt_id: str,
        service_instance_id: str,
        epoch: int,
    ) -> OperationRecord:
        """Atomically terminalize an ABSENT trigger after its broker lease expired."""

        now = _format_time(self.clock.now())
        metadata = _safe_json({"code": "TRIGGER_ABSENT_AFTER_TUNNEL_LEASE_EXPIRY", "epoch": epoch})
        with self._transaction() as connection:
            operation = connection.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            effect = connection.execute(
                "SELECT * FROM effects WHERE effect_id=? AND operation_id=? AND effect_kind='trigger_run'",
                (effect_id, operation_id),
            ).fetchone()
            attempt = connection.execute(
                "SELECT * FROM run_attempts WHERE operation_id=? AND run_id=? AND attempt_id=? "
                "AND service_instance_id=? AND epoch=?",
                (operation_id, run_id, attempt_id, service_instance_id, epoch),
            ).fetchone()
            current = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
            ).fetchone()
            if operation is None or effect is None or attempt is None or current is None:
                raise StaleRuntimeEvent("expired unstarted master identity is incomplete")
            if operation["state"] == "FAILED" and effect["state"] == EffectState.FAILED.value:
                return self._operation_from_row(operation)
            if (
                operation["state"] != "RESTORING"
                or effect["state"] != EffectState.IN_PROGRESS.value
                or int(current[0]) != epoch
            ):
                raise StaleRuntimeEvent("expired unstarted master is no longer terminalizable")
            connection.execute(
                "UPDATE effects SET state='FAILED',updated_at=? WHERE effect_id=? AND state='IN_PROGRESS'",
                (now, effect_id),
            )
            connection.execute(
                "INSERT INTO effect_log(effect_id,operation_id,state,recorded_at,metadata_json) VALUES (?,?,?,?,?)",
                (effect_id, operation_id, EffectState.FAILED.value, now, metadata),
            )
            connection.execute(
                "UPDATE operations SET state='FAILED',updated_at=? WHERE operation_id=? AND state='RESTORING'",
                (now, operation_id),
            )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,'RESTORING','FAILED',?,?)",
                (operation_id, now, metadata),
            )
            connection.execute(
                "UPDATE run_attempts SET state='FENCED',updated_at=? WHERE attempt_id=? AND run_id=? AND epoch=?",
                (now, attempt_id, run_id, epoch),
            )
            connection.execute(
                "UPDATE services SET state='FENCED',updated_at=? WHERE service_instance_id=? AND epoch=? "
                "AND state IN ('REGISTERING','ACTIVE','DRAINING')",
                (now, service_instance_id, epoch),
            )
            connection.execute(
                "UPDATE runtime_token_hashes SET revoked_at=? WHERE run_id=? AND attempt_id=?",
                (now, run_id, attempt_id),
            )
            terminal = connection.execute("SELECT * FROM operations WHERE operation_id=?", (operation_id,)).fetchone()
            assert terminal is not None
            return self._operation_from_row(terminal)

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

    def ensure_master_status_dataset_authority(
        self,
        *,
        operation_id: str,
        run_id: str,
        attempt_id: str,
        token: str,
        creator_claim_until: datetime,
        expected_content_tree_sha256: str,
        resource_lease: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        """Atomically bind one random callback token hash to one master attempt."""

        token_sha256 = hashlib.sha256(token.encode()).hexdigest()
        if (
            creator_claim_until.tzinfo is None
            or len(token) != 64
            or len(expected_content_tree_sha256) != 64
            or any(value not in "0123456789abcdef" for value in expected_content_tree_sha256)
        ):
            raise ValueError("master status Dataset authority is invalid")
        now = _format_time(self.clock.now())
        claim_until = _format_time(creator_claim_until)
        resource_lease_json = _safe_json(resource_lease, max_bytes=8 * 1024)
        with self._transaction() as connection:
            operation = connection.execute(
                "SELECT identity_json FROM operations WHERE operation_id=? AND operation_kind='ensure_master'",
                (operation_id,),
            ).fetchone()
            if operation is None:
                raise KeyError(operation_id)
            identity = json.loads(str(operation["identity_json"]))
            if str(identity.get("run_id")) != run_id or str(identity.get("attempt_id")) != attempt_id:
                raise StaleRuntimeEvent("master status authority differs from admitted operation")
            existing = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if existing is not None:
                return self._master_status_dataset_from_row(existing), False
            connection.execute(
                "INSERT INTO master_status_dataset_authorities(operation_id,run_id,attempt_id,"
                "token_sha256,creator_claim_until,expected_content_tree_sha256,resource_lease_json,"
                "state,created_at,updated_at) VALUES (?,?,?,?,?,?,?,'CREATING',?,?)",
                (
                    operation_id,
                    run_id,
                    attempt_id,
                    token_sha256,
                    claim_until,
                    expected_content_tree_sha256,
                    resource_lease_json,
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO runtime_token_hashes(run_id,attempt_id,token_sha256,created_at) "
                "VALUES (?,?,?,?) ON CONFLICT(run_id,attempt_id) DO NOTHING",
                (run_id, attempt_id, token_sha256, now),
            )
            token_row = connection.execute(
                "SELECT token_sha256 FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
                (run_id, attempt_id),
            ).fetchone()
            if token_row is None or not hmac.compare_digest(str(token_row[0]), token_sha256):
                raise IdempotencyConflict("master attempt already has another callback token hash")
            row = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            assert row is not None
            return self._master_status_dataset_from_row(row), True

    def master_status_dataset_authority(self, operation_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._master_status_dataset_from_row(row) if row is not None else None

    def record_master_status_dataset(self, *, operation_id: str, status_dataset: Mapping[str, Any]) -> dict[str, Any]:
        payload = _safe_json(status_dataset, max_bytes=32 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            if row["status_dataset_json"] is not None and row["status_dataset_json"] != payload:
                raise IdempotencyConflict("master status Dataset claim changed")
            connection.execute(
                "UPDATE master_status_dataset_authorities SET status_dataset_json=?,state='READY',"
                "updated_at=? WHERE operation_id=? AND state IN ('CREATING','READY')",
                (payload, now, operation_id),
            )
            current = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            assert current is not None
            return self._master_status_dataset_from_row(current)

    def complete_master_status_dataset_cleanup(
        self, *, operation_id: str, cleanup_receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        payload = _safe_json(cleanup_receipt, max_bytes=32 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None or row["status_dataset_json"] is None:
                raise StaleRuntimeEvent("master status Dataset cleanup lacks an exact claim")
            if row["cleanup_receipt_json"] is not None and row["cleanup_receipt_json"] != payload:
                raise IdempotencyConflict("master status Dataset cleanup receipt changed")
            connection.execute(
                "UPDATE master_status_dataset_authorities SET cleanup_receipt_json=?,state='CLEANED',"
                "updated_at=? WHERE operation_id=? AND state IN ('CLEANING','CLEANED')",
                (payload, now, operation_id),
            )
            current = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            assert current is not None
            return self._master_status_dataset_from_row(current)

    def complete_ambiguous_master_status_dataset_cleanup(
        self, *, operation_id: str, cleanup_receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        payload = _safe_json(cleanup_receipt, max_bytes=32 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None or row["state"] != "AMBIGUOUS" or row["status_dataset_json"] is not None:
                raise StaleRuntimeEvent("ambiguous master status cleanup binding changed")
            if row["cleanup_receipt_json"] is not None and row["cleanup_receipt_json"] != payload:
                raise IdempotencyConflict("ambiguous master status cleanup receipt changed")
            connection.execute(
                "UPDATE master_status_dataset_authorities SET cleanup_receipt_json=?,updated_at=? "
                "WHERE operation_id=? AND state='AMBIGUOUS'",
                (payload, now, operation_id),
            )
            current = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            assert current is not None
            return self._master_status_dataset_from_row(current)

    def claim_master_status_dataset_cleanup(self, operation_id: str, *, claim_until: datetime) -> bool:
        now = self.clock.now()
        if claim_until <= now:
            raise ValueError("master status cleanup claim must expire in the future")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state,cleanup_claim_until FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            reclaim = row["state"] == "CLEANING" and (
                row["cleanup_claim_until"] is not None and _parse_time(str(row["cleanup_claim_until"])) <= now
            )
            if row["state"] != "READY" and not reclaim:
                return False
            changed = connection.execute(
                "UPDATE master_status_dataset_authorities SET state='CLEANING',cleanup_claim_until=?,"
                "updated_at=? WHERE operation_id=? AND state=?",
                (_format_time(claim_until), _format_time(now), operation_id, row["state"]),
            ).rowcount
            return changed == 1

    def terminal_master_status_dataset_authorities(self, *, limit: int = 10) -> list[dict[str, Any]]:
        if not 1 <= limit <= 100:
            raise ValueError("master status cleanup limit is invalid")
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT a.* FROM master_status_dataset_authorities a JOIN operations o "
                "ON o.operation_id=a.operation_id WHERE o.state IN ('STOPPED','FAILED','FENCED','ORPHANED') "
                "AND a.status_dataset_json IS NOT NULL AND a.cleanup_receipt_json IS NULL "
                "AND a.state IN ('READY','CLEANING') ORDER BY a.updated_at,a.operation_id LIMIT ?",
                (limit,),
            ).fetchall()
        return [self._master_status_dataset_from_row(row) for row in rows]

    def fail_ambiguous_master_status_dataset(self, operation_id: str) -> dict[str, Any]:
        """Fence a creator that outlived its fixed deadline without an exact claim."""

        now = self.clock.now()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None:
                raise KeyError(operation_id)
            if row["status_dataset_json"] is not None:
                return self._master_status_dataset_from_row(row)
            if _parse_time(str(row["creator_claim_until"])) > now:
                return self._master_status_dataset_from_row(row)
            connection.execute(
                "UPDATE master_status_dataset_authorities SET state='AMBIGUOUS',updated_at=? "
                "WHERE operation_id=? AND state='CREATING'",
                (_format_time(now), operation_id),
            )
            operation = connection.execute(
                "SELECT state FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if operation is not None and operation["state"] in {"REQUESTED", "STARTING"}:
                connection.execute(
                    "UPDATE operations SET state='FAILED',updated_at=? WHERE operation_id=?",
                    (_format_time(now), operation_id),
                )
                connection.execute(
                    "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                    "VALUES (?,?,?,?,?)",
                    (
                        operation_id,
                        operation["state"],
                        "FAILED",
                        _format_time(now),
                        _safe_json({"code": "MASTER_STATUS_DATASET_RESPONSE_AMBIGUOUS"}),
                    ),
                )
            connection.execute(
                "UPDATE runtime_token_hashes SET revoked_at=? WHERE run_id=? AND attempt_id=?",
                (_format_time(now), row["run_id"], row["attempt_id"]),
            )
            current = connection.execute(
                "SELECT * FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            assert current is not None
            return self._master_status_dataset_from_row(current)

    @staticmethod
    def _master_status_dataset_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["resource_lease"] = json.loads(str(value.pop("resource_lease_json")))
        status = value.pop("status_dataset_json")
        cleanup = value.pop("cleanup_receipt_json")
        value["status_dataset"] = json.loads(str(status)) if status is not None else None
        value["cleanup_receipt"] = json.loads(str(cleanup)) if cleanup is not None else None
        return value

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
                "SELECT token_sha256,revoked_at FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
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
                "UPDATE services SET state=?,latest_event_id=?,updated_at=? WHERE service_instance_id=? AND epoch=?",
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

    def project_master_terminal_failure(
        self,
        *,
        operation_id: str,
        run_id: str,
        attempt_id: str,
        service_instance_id: str,
        epoch: int,
        expected_operation_state: str,
        event_id: str,
    ) -> None:
        """Fence a provider-terminal master even before service registration.

        Kaggle can fail after the run was durably triggered but before the
        Notebook emits its first authenticated service event.  In that state
        there is intentionally no ``services`` row yet; requiring one would
        strand the operation in REGISTERING forever.
        """

        now = _format_time(self.clock.now())
        metadata = _safe_json({"event_id": event_id, "code": "PROVIDER_TERMINAL_FAILED"})
        with self._transaction() as connection:
            operation = connection.execute(
                "SELECT state FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            attempt = connection.execute(
                "SELECT state,service_instance_id,epoch FROM run_attempts "
                "WHERE operation_id=? AND run_id=? AND attempt_id=?",
                (operation_id, run_id, attempt_id),
            ).fetchone()
            current = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
            ).fetchone()
            service = connection.execute(
                "SELECT state,epoch FROM services WHERE service_instance_id=?", (service_instance_id,)
            ).fetchone()
            status_authority = connection.execute(
                "SELECT resource_lease_json FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if operation is None or attempt is None or current is None:
                raise StaleRuntimeEvent("provider-terminal master identity is incomplete")
            if operation["state"] == "FAILED" and attempt["state"] == "FAILED":
                return
            if (
                operation["state"] != expected_operation_state
                or attempt["service_instance_id"] != service_instance_id
                or int(attempt["epoch"]) != epoch
                or int(current[0]) != epoch
                or (service is not None and int(service["epoch"]) != epoch)
            ):
                raise StaleRuntimeEvent("provider-terminal master identity is stale or fenced")
            connection.execute(
                "UPDATE operations SET state='FAILED',updated_at=? WHERE operation_id=? AND state=?",
                (now, operation_id, expected_operation_state),
            )
            connection.execute(
                "UPDATE run_attempts SET state='FAILED',updated_at=? "
                "WHERE operation_id=? AND run_id=? AND attempt_id=? AND epoch=?",
                (now, operation_id, run_id, attempt_id, epoch),
            )
            if service is not None:
                connection.execute(
                    "UPDATE services SET state='FENCED',latest_event_id=?,updated_at=? "
                    "WHERE service_instance_id=? AND epoch=?",
                    (event_id, now, service_instance_id, epoch),
                )
            connection.execute(
                "UPDATE runtime_token_hashes SET revoked_at=? WHERE run_id=? AND attempt_id=?",
                (now, run_id, attempt_id),
            )
            if status_authority is not None:
                lease = json.loads(str(status_authority["resource_lease_json"]))
                try:
                    lease_id = str(lease["lease_id"])
                    holder_id = str(lease["holder_id"])
                    lease_epoch = int(lease["epoch"])
                except (KeyError, TypeError, ValueError) as exc:
                    raise StaleRuntimeEvent("provider-terminal master resource lease is invalid") from exc
                if holder_id != run_id:
                    raise StaleRuntimeEvent("provider-terminal master resource lease differs")
                connection.execute(
                    "UPDATE resource_leases SET released_at=? WHERE lease_id=? AND holder_id=? AND epoch=? "
                    "AND released_at IS NULL",
                    (now, lease_id, holder_id, lease_epoch),
                )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,?,'FAILED',?,?)",
                (operation_id, expected_operation_state, now, metadata),
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

    def fence_checkpoint_failed_master(
        self,
        *,
        operation_id: str,
        run_id: str,
        attempt_id: str,
        service_instance_id: str,
        epoch: int,
        event_id: str,
    ) -> None:
        """Fence an unrecoverable failed checkpoint after tunnel deactivation.

        This operator boundary is intentionally narrower than a generic state
        edit: the exact current runtime must already be draining, its
        publication must be terminally failed, and no candidate from the
        operation may be the verified HEAD.  Provider termination/tunnel
        deactivation remains an external precondition; this transaction only
        retires the control authority so a replacement epoch can be admitted.
        """

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            operation = connection.execute(
                "SELECT state FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            attempt = connection.execute(
                "SELECT state,service_instance_id,epoch FROM run_attempts "
                "WHERE operation_id=? AND run_id=? AND attempt_id=?",
                (operation_id, run_id, attempt_id),
            ).fetchone()
            service = connection.execute(
                "SELECT state,run_id,attempt_id,master_instance_id,epoch FROM services "
                "WHERE service_instance_id=?",
                (service_instance_id,),
            ).fetchone()
            current = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
            ).fetchone()
            token = connection.execute(
                "SELECT revoked_at FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
                (run_id, attempt_id),
            ).fetchone()
            if (
                operation is not None
                and operation["state"] == "FENCED"
                and attempt is not None
                and attempt["state"] == "FENCED"
                and service is not None
                and service["state"] == "FENCED"
                and token is not None
                and token["revoked_at"] is not None
            ):
                return
            terminal_publications = connection.execute(
                "SELECT count(*) FROM checkpoint_blob_publications "
                "WHERE operation_id=? AND state IN ('FAILED','QUARANTINED')",
                (operation_id,),
            ).fetchone()
            nonterminal_publications = connection.execute(
                "SELECT count(*) FROM checkpoint_blob_publications "
                "WHERE operation_id=? AND state NOT IN ('FAILED','QUARANTINED')",
                (operation_id,),
            ).fetchone()
            verified_authority = connection.execute(
                "SELECT count(*) FROM checkpoint_candidates c "
                "LEFT JOIN checkpoint_heads h ON h.current_checkpoint_id=c.checkpoint_id "
                "WHERE c.operation_id=? AND (c.status='VERIFIED' OR h.current_checkpoint_id IS NOT NULL)",
                (operation_id,),
            ).fetchone()
            if not (
                operation is not None
                and operation["state"] == "CHECKPOINT_FAILED"
                and attempt is not None
                and attempt["service_instance_id"] == service_instance_id
                and int(attempt["epoch"]) == epoch
                and service is not None
                and service["state"] == "DRAINING"
                and service["run_id"] == run_id
                and service["attempt_id"] == attempt_id
                and int(service["epoch"]) == epoch
                and current is not None
                and int(current["current_epoch"]) == epoch
                and terminal_publications is not None
                and int(terminal_publications[0]) >= 1
                and nonterminal_publications is not None
                and int(nonterminal_publications[0]) == 0
                and verified_authority is not None
                and int(verified_authority[0]) == 0
            ):
                raise StaleRuntimeEvent("checkpoint-failed master is not safely fenceable")
            connection.execute(
                "UPDATE operations SET state='FENCED',updated_at=? "
                "WHERE operation_id=? AND state='CHECKPOINT_FAILED'",
                (now, operation_id),
            )
            connection.execute(
                "UPDATE run_attempts SET state='FENCED',updated_at=? "
                "WHERE operation_id=? AND run_id=? AND attempt_id=? AND epoch=?",
                (now, operation_id, run_id, attempt_id, epoch),
            )
            connection.execute(
                "UPDATE services SET state='FENCED',latest_event_id=?,updated_at=? "
                "WHERE service_instance_id=? AND epoch=? AND state='DRAINING'",
                (event_id, now, service_instance_id, epoch),
            )
            connection.execute(
                "UPDATE runtime_token_hashes SET revoked_at=? WHERE run_id=? AND attempt_id=?",
                (now, run_id, attempt_id),
            )
            status_authority = connection.execute(
                "SELECT resource_lease_json FROM master_status_dataset_authorities WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if status_authority is not None:
                lease = json.loads(str(status_authority["resource_lease_json"]))
                if str(lease.get("holder_id")) != run_id:
                    raise StaleRuntimeEvent("checkpoint-failed resource lease differs")
                connection.execute(
                    "UPDATE resource_leases SET released_at=? WHERE lease_id=? AND holder_id=? AND epoch=? "
                    "AND released_at IS NULL",
                    (now, str(lease["lease_id"]), run_id, int(lease["epoch"])),
                )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,'CHECKPOINT_FAILED','FENCED',?,?)",
                (
                    operation_id,
                    now,
                    _safe_json(
                        {
                            "code": "OPERATOR_CHECKPOINT_FAILURE_RECOVERY",
                            "event_id": event_id,
                        }
                    ),
                ),
            )

    def list_provider_resources(self, *, limit: int = 100) -> list[dict[str, Any]]:
        if not 1 <= limit <= 500:
            raise ValueError("provider resource limit must be between 1 and 500")
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT provider,resource_ref,resource_kind,source_identity,source_version,control_class,"
                "private,state,metadata_json,observed_at FROM provider_resources "
                "ORDER BY observed_at DESC,provider,resource_ref LIMIT ?",
                (limit,),
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

    def record_acceptance_consumer_heartbeat(self, available: bool) -> None:
        with self._transaction() as connection:
            connection.execute(
                "INSERT INTO acceptance_consumer_heartbeat VALUES (1,?,?) ON CONFLICT(singleton) "
                "DO UPDATE SET available=excluded.available,observed_at=excluded.observed_at",
                (int(available), _format_time(self.clock.now())),
            )

    def acceptance_consumer_available(self, max_age_seconds: int = 30) -> bool:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM acceptance_consumer_heartbeat WHERE singleton=1").fetchone()
        return bool(
            row
            and row["available"]
            and (self.clock.now() - _parse_time(row["observed_at"])).total_seconds() <= max_age_seconds
        )

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
            "issuer": row["issuer"],
            "client_id": row["client_id"],
            "enabled": bool(row["enabled"]),
            "allowed_scopes": frozenset(json.loads(row["allowed_scopes_json"])["scopes"]),
            "principal_id": row["principal_id"],
            "profile_kind": row["profile_kind"],
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
            row = connection.execute("SELECT * FROM master_requests WHERE request_id=?", (request_id,)).fetchone()
            assert row is not None
            return dict(row), True

    def latest_master_request(self) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_requests WHERE state<>'DONE' ORDER BY created_at DESC,request_id DESC LIMIT 1"
            ).fetchone()
        return dict(row) if row else None

    def master_request_by_operation_id(self, operation_id: str) -> dict[str, Any] | None:
        if not operation_id:
            return None
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_requests WHERE operation_id=? ORDER BY created_at DESC,request_id DESC LIMIT 1",
                (operation_id,),
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
                        grant["code_digest"],
                        grant["code_challenge"],
                        grant["client_id"],
                        grant["redirect_uri"],
                        grant["resource"],
                        scopes_json,
                        grant["subject"],
                        grant.get("nonce"),
                        int(grant["authenticated_at"]),
                        int(grant["expires_at"]),
                        _format_time(self.clock.now()),
                    ),
                )
            except sqlite3.IntegrityError:
                return False
        return True

    def consume_oauth_authorization_grant(self, code_digest: str, *, now: int) -> dict[str, Any] | None:
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM oauth_authorization_grants WHERE code_digest=?", (code_digest,)
            ).fetchone()
            if row is None:
                return None
            connection.execute("DELETE FROM oauth_authorization_grants WHERE code_digest=?", (code_digest,))
            if int(row["expires_at"]) <= now:
                return None
            return {
                "code_digest": row["code_digest"],
                "code_challenge": row["code_challenge"],
                "client_id": row["client_id"],
                "redirect_uri": row["redirect_uri"],
                "resource": row["resource"],
                "scopes": tuple(json.loads(row["scopes_json"])["scopes"]),
                "subject": row["subject"],
                "nonce": row["nonce"],
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
                        grant["credential_digest"],
                        grant["family_id"],
                        grant["client_id"],
                        grant["resource"],
                        scopes_json,
                        grant["subject"],
                        int(grant["authenticated_at"]),
                        int(grant["expires_at"]),
                        grant.get("consumed_at"),
                        grant.get("revoked_at"),
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
                    successor_digest,
                    row["family_id"],
                    row["client_id"],
                    row["resource"],
                    _safe_json({"scopes": list(successor_scopes)}),
                    row["subject"],
                    int(row["authenticated_at"]),
                    expires_at,
                    _format_time(self.clock.now()),
                ),
            )
            return "rotated", {
                "credential_digest": successor_digest,
                "family_id": row["family_id"],
                "client_id": row["client_id"],
                "resource": row["resource"],
                "scopes": successor_scopes,
                "subject": row["subject"],
                "authenticated_at": int(row["authenticated_at"]),
                "expires_at": expires_at,
                "consumed_at": None,
                "revoked_at": None,
            }

    def revoke_oauth_refresh_grant(self, credential_digest: str, *, client_id: str, now: int) -> None:
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
            "effect_id",
            "operation_id",
            "idempotency_key",
            "task_id",
            "action",
            "provider_ref",
            "request_sha256",
        }
        if not required.issubset(payload):
            raise ValueError("provider effect intent is missing durable identity fields")
        with self._transaction() as connection:
            try:
                connection.execute(
                    "INSERT INTO provider_effect_intents(effect_id,operation_id,idempotency_key,task_id,action,"
                    "provider_ref,request_sha256,intent_json,recorded_at) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        str(payload["effect_id"]),
                        str(payload["operation_id"]),
                        str(payload["idempotency_key"]),
                        str(payload["task_id"]),
                        str(payload["action"]),
                        str(payload["provider_ref"]),
                        str(payload["request_sha256"]),
                        intent_json,
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

    def ensure_mcp_write_operation(
        self,
        *,
        operation_id: str,
        idempotency_key: str,
        principal_id: str,
        client_id: str,
        master_instance_id: str,
        epoch: int,
        expected_revision: int,
        request_sha256: str,
        pre_change_checkpoint_id: str,
    ) -> tuple[dict[str, Any], bool]:
        """Persist the exact preview intent before an operator session is opened."""

        if (
            not operation_id
            or not 8 <= len(idempotency_key) <= 300
            or not principal_id
            or not client_id
            or not master_instance_id
            or epoch < 1
            or expected_revision < 0
            or len(request_sha256) != 64
        ):
            raise ValueError("MCP write identity is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=? OR "
                "(principal_id=? AND client_id=? AND idempotency_key=?)",
                (operation_id, principal_id, client_id, idempotency_key),
            ).fetchone()
            if row is not None:
                existing = self._mcp_write_from_row(row)
                expected = {
                    "operation_id": operation_id,
                    "idempotency_key": idempotency_key,
                    "principal_id": principal_id,
                    "client_id": client_id,
                    "master_instance_id": master_instance_id,
                    "epoch": epoch,
                    "expected_revision": expected_revision,
                    "request_sha256": request_sha256,
                    "pre_change_checkpoint_id": pre_change_checkpoint_id,
                }
                if any(existing[key] != value for key, value in expected.items()):
                    raise IdempotencyConflict("MCP write identity was reused for different intent")
                return existing, False
            connection.execute(
                "INSERT INTO mcp_write_operations(operation_id,idempotency_key,tool,principal_id,client_id,"
                "master_instance_id,epoch,expected_revision,request_sha256,state,pre_change_checkpoint_id,"
                "created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,'REQUESTED',?,?,?)",
                (
                    operation_id,
                    idempotency_key,
                    "data.change.preview",
                    principal_id,
                    client_id,
                    master_instance_id,
                    epoch,
                    expected_revision,
                    request_sha256,
                    pre_change_checkpoint_id,
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO mcp_write_events(operation_id,state,metadata_json,recorded_at) VALUES (?,'REQUESTED',?,?)",
                (operation_id, _safe_json({"request_sha256": request_sha256}), now),
            )
            row = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert row is not None
            return self._mcp_write_from_row(row), True

    def ensure_blogger_import_operation(
        self,
        *,
        operation_id: str,
        batch_id: str,
        idempotency_key: str,
        principal_id: str,
        client_id: str,
        master_instance_id: str | None,
        epoch: int | None,
        expected_revision: int,
        request_sha256: str,
        pre_change_checkpoint_id: str | None,
    ) -> tuple[dict[str, Any], bool]:
        """Persist only the identity/hash metadata for one typed blogger preview."""

        try:
            exact_batch_id = str(UUID(batch_id))
        except ValueError as exc:
            raise ValueError("blogger import batch_id is invalid") from exc
        if (
            not re.fullmatch(r"[a-f0-9]{64}", operation_id)
            or not re.fullmatch(r"[a-f0-9]{64}", request_sha256)
            or not 8 <= len(idempotency_key) <= 300
            or not principal_id
            or not client_id
            or expected_revision < 0
        ):
            raise ValueError("blogger import operation identity is invalid")
        binding = (master_instance_id, epoch, pre_change_checkpoint_id)
        if not (
            all(value is None for value in binding)
            or (
                isinstance(master_instance_id, str)
                and bool(master_instance_id)
                and isinstance(epoch, int)
                and epoch >= 1
                and isinstance(pre_change_checkpoint_id, str)
                and bool(pre_change_checkpoint_id)
            )
        ):
            raise ValueError("blogger import master binding must be wholly absent or complete")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=? OR "
                "(principal_id=? AND client_id=? AND idempotency_key=?) OR "
                "(principal_id=? AND client_id=? AND batch_id=?)",
                (
                    operation_id,
                    principal_id,
                    client_id,
                    idempotency_key,
                    principal_id,
                    client_id,
                    exact_batch_id,
                ),
            ).fetchone()
            expected = {
                "operation_id": operation_id,
                "batch_id": exact_batch_id,
                "idempotency_key": idempotency_key,
                "principal_id": principal_id,
                "client_id": client_id,
                "expected_revision": expected_revision,
                "request_sha256": request_sha256,
            }
            if row is not None:
                existing = self._blogger_import_from_row(row)
                if any(existing[key] != value for key, value in expected.items()):
                    raise IdempotencyConflict(
                        "blogger import identity was reused for a different batch or request"
                    )
                existing_binding = (
                    existing["master_instance_id"],
                    existing["epoch"],
                    existing["pre_change_checkpoint_id"],
                )
                if any(value is not None for value in binding) and existing_binding != binding:
                    raise IdempotencyConflict(
                        "blogger import identity was reused with a different master binding"
                    )
                return existing, False
            if master_instance_id is not None:
                assert epoch is not None and pre_change_checkpoint_id is not None
                self._require_blogger_checkpoint_authority(
                    connection,
                    checkpoint_id=pre_change_checkpoint_id,
                    master_instance_id=master_instance_id,
                    epoch=epoch,
                )
            connection.execute(
                "INSERT INTO mcp_blogger_import_operations("
                "operation_id,batch_id,idempotency_key,principal_id,client_id,master_instance_id,"
                "epoch,expected_revision,request_sha256,state,pre_change_checkpoint_id,created_at,updated_at"
                ") VALUES (?,?,?,?,?,?,?,?,?,'REQUESTED',?,?,?)",
                (
                    operation_id,
                    exact_batch_id,
                    idempotency_key,
                    principal_id,
                    client_id,
                    master_instance_id,
                    epoch,
                    expected_revision,
                    request_sha256,
                    pre_change_checkpoint_id,
                    now,
                    now,
                ),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,'REQUESTED',?,?)",
                (
                    operation_id,
                    _safe_json(
                        {
                            "batch_id": exact_batch_id,
                            "request_sha256": request_sha256,
                            "expected_revision": expected_revision,
                        }
                    ),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current), True

    def bind_blogger_import_active_master(
        self,
        operation_id: str,
        *,
        master_instance_id: str,
        epoch: int,
        pre_change_checkpoint_id: str,
    ) -> dict[str, Any]:
        """Bind a persisted cold-start request to one ACTIVE epoch and checkpoint."""

        if not master_instance_id or epoch < 1 or not pre_change_checkpoint_id:
            raise ValueError("blogger import ACTIVE-master binding is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None or str(row["state"]) not in {"REQUESTED", "WAITING_MASTER"}:
                raise StaleRuntimeEvent("blogger import cannot bind an ACTIVE master in this state")
            existing = self._blogger_import_from_row(row)
            current_binding = (
                existing["master_instance_id"],
                existing["epoch"],
                existing["pre_change_checkpoint_id"],
            )
            requested_binding = (master_instance_id, epoch, pre_change_checkpoint_id)
            if all(value is not None for value in current_binding):
                if current_binding == requested_binding:
                    return existing
                raise IdempotencyConflict("blogger import is already bound to a different epoch")
            self._require_blogger_checkpoint_authority(
                connection,
                checkpoint_id=pre_change_checkpoint_id,
                master_instance_id=master_instance_id,
                epoch=epoch,
            )
            connection.execute(
                "UPDATE mcp_blogger_import_operations SET master_instance_id=?,epoch=?,"
                "pre_change_checkpoint_id=?,updated_at=? WHERE operation_id=?",
                (master_instance_id, epoch, pre_change_checkpoint_id, now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,?,?,?)",
                (
                    operation_id,
                    str(row["state"]),
                    _safe_json(
                        {
                            "active_master_bound": True,
                            "master_instance_id": master_instance_id,
                            "epoch": epoch,
                            "pre_change_checkpoint_id": pre_change_checkpoint_id,
                        }
                    ),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    def record_blogger_import_preview(
        self,
        operation_id: str,
        *,
        preview_receipt: str,
        plan_sha256: str,
        summary: Mapping[str, int],
    ) -> dict[str, Any]:
        allowed = {
            "create_actor_count",
            "link_existing_count",
            "quarantine_count",
            "account_count",
        }
        if (
            not preview_receipt
            or not re.fullmatch(r"[a-f0-9]{64}", plan_sha256)
            or set(summary) != allowed
            or any(isinstance(value, bool) or not isinstance(value, int) or value < 0 for value in summary.values())
        ):
            raise ValueError("blogger preview receipt or bounded summary is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None or any(
                row[column] is None
                for column in ("master_instance_id", "epoch", "pre_change_checkpoint_id")
            ):
                raise StaleRuntimeEvent("blogger preview requires an ACTIVE-master binding")
            existing = self._blogger_import_from_row(row)
            if str(row["state"]) == "PREVIEWED":
                if (
                    existing["preview_receipt"] == preview_receipt
                    and existing["plan_sha256"] == plan_sha256
                    and existing["preview_summary"] == dict(summary)
                ):
                    return existing
                raise IdempotencyConflict("blogger preview replay differs from immutable plan")
            if str(row["state"]) not in {"REQUESTED", "WAITING_MASTER"}:
                raise StaleRuntimeEvent("blogger import lifecycle transition is stale")
            connection.execute(
                "UPDATE mcp_blogger_import_operations SET state='PREVIEWED',preview_receipt=?,"
                "plan_sha256=?,preview_summary_json=?,updated_at=? WHERE operation_id=?",
                (preview_receipt, plan_sha256, _safe_json(dict(summary)), now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,'PREVIEWED',?,?)",
                (operation_id, _safe_json({"plan_sha256": plan_sha256, **summary}), now),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    def mark_blogger_import_waiting_master(self, operation_id: str) -> dict[str, Any]:
        """Checkpoint a cold-start wait without terminalizing the owner request."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
        if row is not None and str(row["state"]) == "WAITING_MASTER":
            return self._blogger_import_from_row(row)
        return self._transition_blogger_import(
            operation_id,
            expected_states={"REQUESTED"},
            new_state="WAITING_MASTER",
            updates={},
            metadata={"continuation": "resume_preview_after_active_master"},
        )

    def restart_blogger_import_after_preview_epoch_loss(
        self,
        operation_id: str,
        *,
        failed_master_instance_id: str,
        failed_epoch: int,
    ) -> dict[str, Any]:
        """Discard only an un-applied dead-epoch preview and retain request identity."""

        if not failed_master_instance_id or failed_epoch < 1:
            raise ValueError("failed blogger preview epoch identity is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if (
                row is None
                or str(row["state"]) != "PREVIEWED"
                or row["master_instance_id"] != failed_master_instance_id
                or int(row["epoch"] or 0) != failed_epoch
            ):
                raise StaleRuntimeEvent("blogger preview restart does not match the dead epoch")
            self._require_blogger_preview_epoch_dead(
                connection,
                master_instance_id=failed_master_instance_id,
                epoch=failed_epoch,
            )
            old_plan_sha256 = str(row["plan_sha256"])
            connection.execute(
                "UPDATE mcp_blogger_import_operations SET state='WAITING_MASTER',"
                "master_instance_id=NULL,epoch=NULL,pre_change_checkpoint_id=NULL,"
                "preview_receipt=NULL,plan_sha256=NULL,preview_summary_json=NULL,"
                "preview_generation=preview_generation+1,updated_at=? WHERE operation_id=?",
                (now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,'WAITING_MASTER',?,?)",
                (
                    operation_id,
                    _safe_json(
                        {
                            "continuation": "rebind_and_repreview_after_epoch_loss",
                            "failed_master_instance_id": failed_master_instance_id,
                            "failed_epoch": failed_epoch,
                            "discarded_plan_sha256": old_plan_sha256,
                        }
                    ),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    @staticmethod
    def _require_blogger_preview_epoch_dead(
        connection: sqlite3.Connection,
        *,
        master_instance_id: str,
        epoch: int,
    ) -> None:
        authority = connection.execute(
            "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
        ).fetchone()
        if authority is None:
            raise StaleRuntimeEvent(
                "blogger preview reset lacks authoritative PostgreSQL epoch state"
            )
        current_epoch = int(authority["current_epoch"])
        if current_epoch > epoch:
            return
        service = connection.execute(
            "SELECT state FROM services WHERE service_kind='postgres-master' "
            "AND master_instance_id=? AND epoch=?",
            (master_instance_id, epoch),
        ).fetchone()
        if (
            current_epoch == epoch
            and service is not None
            and str(service["state"]) in {"FENCED", "STOPPED"}
        ):
            return
        raise StaleRuntimeEvent(
            "blogger preview reset requires an authoritatively stopped or fenced PostgreSQL epoch"
        )

    def begin_blogger_import_apply(
        self, operation_id: str, *, preview_receipt: str, plan_sha256: str
    ) -> dict[str, Any]:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT preview_receipt,plan_sha256 FROM mcp_blogger_import_operations "
                "WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        if (
            row is None
            or not hmac.compare_digest(str(row["preview_receipt"] or ""), preview_receipt)
            or not hmac.compare_digest(str(row["plan_sha256"] or ""), plan_sha256)
        ):
            raise PermissionError("blogger apply does not bind the durable preview and plan")
        return self._transition_blogger_import(
            operation_id,
            expected_states={"PREVIEWED"},
            new_state="APPLYING",
            updates={},
            metadata={"plan_sha256": plan_sha256},
        )

    def record_blogger_import_commit(
        self, operation_id: str, *, affected_rows: int, committed_revision: int
    ) -> dict[str, Any]:
        if affected_rows < 1 or committed_revision < 1:
            raise ValueError("blogger commit result is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is not None and str(row["state"]) in {
                "COMMITTED_PENDING_CHECKPOINT",
                "CHECKPOINTING",
                "CHECKPOINT_VERIFIED",
                "DURABLE_COMPLETE",
            }:
                existing = self._blogger_import_from_row(row)
                if (
                    existing["affected_rows"] == affected_rows
                    and existing["committed_revision"] == committed_revision
                ):
                    return existing
                raise IdempotencyConflict("blogger commit replay differs from immutable receipt")
            if row is None or str(row["state"]) != "APPLYING":
                raise StaleRuntimeEvent("blogger import lifecycle transition is stale")
            connection.execute(
                "UPDATE mcp_blogger_import_operations SET state='COMMITTED_PENDING_CHECKPOINT',"
                "affected_rows=?,committed_revision=?,committed_at=?,updated_at=? WHERE operation_id=?",
                (affected_rows, committed_revision, now, now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,'COMMITTED_PENDING_CHECKPOINT',?,?)",
                (
                    operation_id,
                    _safe_json(
                        {"affected_rows": affected_rows, "committed_revision": committed_revision}
                    ),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    def reconcile_blogger_import_commit(
        self,
        operation_id: str,
        *,
        request_sha256: str,
        plan_sha256: str,
        master_instance_id: str,
        epoch: int,
        expected_revision: int,
        principal_id: str,
        client_id: str,
        affected_rows: int,
        committed_revision: int,
        committed_at: str,
    ) -> dict[str, Any]:
        if (
            not re.fullmatch(r"[a-f0-9]{64}", request_sha256)
            or not re.fullmatch(r"[a-f0-9]{64}", plan_sha256)
            or not master_instance_id
            or epoch < 1
            or expected_revision < 0
            or not principal_id
            or not client_id
            or affected_rows < 1
            or committed_revision != expected_revision + 1
            or not committed_at
        ):
            raise ValueError("canonical blogger reconciliation receipt is invalid")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                raise StaleRuntimeEvent("blogger reconciliation has no durable preview intent")
            record = self._blogger_import_from_row(row)
            expected = {
                "request_sha256": request_sha256,
                "plan_sha256": plan_sha256,
                "master_instance_id": master_instance_id,
                "epoch": epoch,
                "expected_revision": expected_revision,
                "principal_id": principal_id,
                "client_id": client_id,
            }
            if any(record[key] != value for key, value in expected.items()):
                raise StaleRuntimeEvent(
                    "canonical blogger receipt differs from durable preview identity"
                )
            if record["state"] != "APPLYING":
                if (
                    record["state"]
                    in {
                        "COMMITTED_PENDING_CHECKPOINT",
                        "CHECKPOINTING",
                        "CHECKPOINT_VERIFIED",
                        "DURABLE_COMPLETE",
                    }
                    and record["affected_rows"] == affected_rows
                    and record["committed_revision"] == committed_revision
                ):
                    return record
                raise StaleRuntimeEvent("blogger reconciliation is stale or conflicts")
            now = _format_time(self.clock.now())
            connection.execute(
                "UPDATE mcp_blogger_import_operations SET state='COMMITTED_PENDING_CHECKPOINT',"
                "affected_rows=?,committed_revision=?,committed_at=?,updated_at=? WHERE operation_id=?",
                (affected_rows, committed_revision, committed_at, now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,'COMMITTED_PENDING_CHECKPOINT',?,?)",
                (
                    operation_id,
                    _safe_json(
                        {
                            "affected_rows": affected_rows,
                            "committed_revision": committed_revision,
                            "reconciled_from": "canonical_postgres_receipt",
                        }
                    ),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    def blogger_import_operation(self, operation_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
        return self._blogger_import_from_row(row) if row else None

    def advance_blogger_import_checkpoint(
        self,
        operation_id: str,
        *,
        state: str,
        post_change_checkpoint_id: str | None = None,
    ) -> dict[str, Any]:
        allowed = {
            "CHECKPOINTING": {"COMMITTED_PENDING_CHECKPOINT"},
            "CHECKPOINT_VERIFIED": {"CHECKPOINTING"},
            "DURABLE_COMPLETE": {"CHECKPOINT_VERIFIED"},
        }
        if state not in allowed or (state != "CHECKPOINTING" and not post_change_checkpoint_id):
            raise ValueError("blogger checkpoint transition is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                raise StaleRuntimeEvent("blogger checkpoint operation does not exist")
            existing = self._blogger_import_from_row(row)
            if str(row["state"]) == state:
                if (
                    post_change_checkpoint_id is None
                    or existing["post_change_checkpoint_id"] == post_change_checkpoint_id
                ):
                    return existing
                raise IdempotencyConflict(
                    "blogger checkpoint replay differs from immutable identity"
                )
            if str(row["state"]) not in allowed[state]:
                raise StaleRuntimeEvent("blogger import lifecycle transition is stale")
            stored_checkpoint_id = existing["post_change_checkpoint_id"]
            if (
                stored_checkpoint_id is not None
                and post_change_checkpoint_id is not None
                and stored_checkpoint_id != post_change_checkpoint_id
            ):
                raise IdempotencyConflict("blogger durable checkpoint identity cannot be replaced")
            effective_checkpoint_id = post_change_checkpoint_id or stored_checkpoint_id
            if state in {"CHECKPOINT_VERIFIED", "DURABLE_COMPLETE"}:
                if (
                    effective_checkpoint_id is None
                    or existing["committed_revision"] is None
                ):
                    raise StaleRuntimeEvent("blogger durable checkpoint lacks request authority")
                self._require_blogger_post_checkpoint_authority(
                    connection,
                    operation_id=operation_id,
                    batch_id=str(existing["batch_id"]),
                    committed_revision=int(existing["committed_revision"]),
                    checkpoint_id=str(effective_checkpoint_id),
                )
            connection.execute(
                "UPDATE mcp_blogger_import_operations SET state=?,post_change_checkpoint_id=?,"
                "updated_at=? WHERE operation_id=?",
                (state, effective_checkpoint_id, now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,?,?,?)",
                (
                    operation_id,
                    state,
                    _safe_json({"post_change_checkpoint_id": effective_checkpoint_id}),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    @staticmethod
    def _require_blogger_post_checkpoint_authority(
        connection: sqlite3.Connection,
        *,
        operation_id: str,
        batch_id: str,
        committed_revision: int,
        checkpoint_id: str,
    ) -> None:
        request_identity = {
            "kind": "mcp-blogger-import-checkpoint-v1",
            "operation_id": operation_id,
            "batch_id": batch_id,
            "canonical_revision": committed_revision,
        }
        request_id = hashlib.sha256(_canonical_json(request_identity).encode()).hexdigest()
        request = connection.execute(
            "SELECT * FROM connector_checkpoint_requests WHERE operation_id=? "
            "AND idempotency_key=?",
            (f"connector-checkpoint:{request_id}", request_id),
        ).fetchone()
        checkpoint = connection.execute(
            "SELECT candidate.operation_id,candidate.status,candidate.master_instance_id,"
            "candidate.epoch,candidate.service_kind,candidate.manifest_json,"
            "candidate.version_ref,candidate.verified_at,head.current_checkpoint_id "
            "FROM checkpoint_candidates candidate "
            "LEFT JOIN checkpoint_heads head ON head.service_kind=candidate.service_kind "
            "WHERE candidate.checkpoint_id=?",
            (checkpoint_id,),
        ).fetchone()
        try:
            manifest = json.loads(str(checkpoint["manifest_json"])) if checkpoint is not None else None
        except (TypeError, ValueError, json.JSONDecodeError):
            manifest = None
        protected_revision = (
            manifest.get("canonical_revision") if isinstance(manifest, dict) else None
        )
        if (
            request is None
            or request["state"] != "DURABLE_COMPLETE"
            or request["checkpoint_id"] != checkpoint_id
            or int(request["canonical_revision"]) != committed_revision
            or checkpoint is None
            or checkpoint["status"] != "VERIFIED"
            or checkpoint["service_kind"] != "postgres-master"
            or checkpoint["operation_id"] != request["master_operation_id"]
            or checkpoint["master_instance_id"] != request["master_instance_id"]
            or int(checkpoint["epoch"]) != int(request["epoch"])
            or not checkpoint["version_ref"]
            or not checkpoint["verified_at"]
            or checkpoint["current_checkpoint_id"] != checkpoint_id
            or not isinstance(manifest, dict)
            or not isinstance(protected_revision, int)
            or isinstance(protected_revision, bool)
            or protected_revision < committed_revision
        ):
            raise StaleRuntimeEvent(
                "blogger checkpoint lacks its exact request-bound VERIFIED HEAD authority"
            )

    def _require_blogger_checkpoint_authority(
        self,
        connection: sqlite3.Connection,
        *,
        checkpoint_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> None:
        """Require the exact VERIFIED HEAD restored by the write epoch.

        A normal cold start restores a checkpoint produced by the preceding
        draining epoch.  The producer identity therefore cannot be required
        to equal the successor that is about to write.  For that normal case,
        bind the successor through its ACTIVE service operation and the
        immutable ``boot_checkpoint`` recorded before its provider launch.
        The same-epoch branch is retained for a master that checkpoints and
        resumes without a replacement.
        """

        checkpoint = connection.execute(
            "SELECT candidate.status,candidate.master_instance_id,candidate.epoch,"
            "candidate.service_kind,candidate.version_ref,candidate.manifest_sha256,"
            "head.current_checkpoint_id,head.generation "
            "FROM checkpoint_candidates candidate "
            "LEFT JOIN checkpoint_heads head ON head.service_kind=candidate.service_kind "
            "WHERE candidate.checkpoint_id=?",
            (checkpoint_id,),
        ).fetchone()
        exact_head = (
            checkpoint is not None
            and checkpoint["status"] == "VERIFIED"
            and checkpoint["service_kind"] == "postgres-master"
            and checkpoint["current_checkpoint_id"] == checkpoint_id
            and isinstance(checkpoint["version_ref"], str)
            and bool(checkpoint["version_ref"])
        )
        if not exact_head:
            raise StaleRuntimeEvent(
                "blogger checkpoint is not the exact verified PostgreSQL HEAD for its epoch"
            )
        assert checkpoint is not None
        if (
            checkpoint["master_instance_id"] == master_instance_id
            and int(checkpoint["epoch"]) == epoch
        ):
            return

        successor = connection.execute(
            "SELECT operation.operation_id,operation.state AS operation_state,"
            "service.state AS service_state,service.lease_until,"
            "authority.state AS authority_state,authority.status_dataset_json,"
            "service_epoch.current_epoch "
            "FROM services service "
            "JOIN service_epochs service_epoch ON service_epoch.service_kind=service.service_kind "
            "JOIN run_attempts attempt ON attempt.run_id=service.run_id "
            "AND attempt.attempt_id=service.attempt_id "
            "JOIN operations operation ON operation.operation_id=attempt.operation_id "
            "JOIN master_status_dataset_authorities authority "
            "ON authority.operation_id=operation.operation_id "
            "WHERE service.service_kind='postgres-master' "
            "AND service.master_instance_id=? AND service.epoch=?",
            (master_instance_id, epoch),
        ).fetchone()
        boot_checkpoint: object = None
        if successor is not None and successor["status_dataset_json"] is not None:
            with suppress(json.JSONDecodeError, TypeError):
                status_dataset = json.loads(str(successor["status_dataset_json"]))
                if isinstance(status_dataset, dict):
                    boot_checkpoint = status_dataset.get("boot_checkpoint")
        if (
            successor is None
            or successor["operation_state"] != "ACTIVE"
            or successor["service_state"] != "ACTIVE"
            or int(successor["current_epoch"]) != epoch
            or _parse_time(str(successor["lease_until"])) <= self.clock.now()
            or successor["authority_state"] != "READY"
            or not isinstance(boot_checkpoint, dict)
            or boot_checkpoint.get("kind") != "VERIFIED"
            or boot_checkpoint.get("checkpoint_id") != checkpoint_id
            or boot_checkpoint.get("generation") != int(checkpoint["generation"])
            or boot_checkpoint.get("exact_version_ref") != checkpoint["version_ref"]
            or boot_checkpoint.get("manifest_sha256") != checkpoint["manifest_sha256"]
        ):
            raise StaleRuntimeEvent(
                "blogger checkpoint is not the exact verified PostgreSQL HEAD restored by the ACTIVE epoch"
            )

    def _transition_blogger_import(
        self,
        operation_id: str,
        *,
        expected_states: set[str],
        new_state: str,
        updates: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        allowed_columns = {
            "preview_receipt",
            "plan_sha256",
            "preview_summary_json",
            "affected_rows",
            "committed_revision",
            "committed_at",
            "post_change_checkpoint_id",
        }
        if not set(updates) <= allowed_columns:
            raise ValueError("unsupported blogger import projection update")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None or str(row["state"]) not in expected_states:
                raise StaleRuntimeEvent("blogger import lifecycle transition is stale")
            assignments = ["state=?", "updated_at=?", *(f"{column}=?" for column in updates)]
            values = [new_state, now, *updates.values(), operation_id]
            connection.execute(
                f"UPDATE mcp_blogger_import_operations SET {','.join(assignments)} "
                "WHERE operation_id=?",
                values,
            )
            connection.execute(
                "INSERT INTO mcp_blogger_import_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,?,?,?)",
                (operation_id, new_state, _safe_json(dict(metadata)), now),
            )
            current = connection.execute(
                "SELECT * FROM mcp_blogger_import_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._blogger_import_from_row(current)

    @staticmethod
    def _blogger_import_from_row(row: sqlite3.Row) -> dict[str, Any]:
        result = dict(zip(row.keys(), row, strict=True))
        if result.get("preview_summary_json"):
            result["preview_summary"] = json.loads(result["preview_summary_json"])
        return result

    def record_mcp_write_preview(
        self,
        operation_id: str,
        *,
        preview_receipt: str,
        affected_rows: int,
    ) -> dict[str, Any]:
        if not preview_receipt or affected_rows < 0:
            raise ValueError("MCP preview result is invalid")
        return self._transition_mcp_write(
            operation_id,
            expected_states={"REQUESTED", "PREVIEWED"},
            new_state="PREVIEWED",
            updates={"preview_receipt": preview_receipt, "affected_rows": affected_rows},
            metadata={"affected_rows": affected_rows},
        )

    def begin_mcp_write_apply(self, operation_id: str, *, preview_receipt: str) -> dict[str, Any]:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT preview_receipt FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
        if row is None or not hmac.compare_digest(str(row["preview_receipt"] or ""), preview_receipt):
            raise PermissionError("apply does not bind the durable preview receipt")
        return self._transition_mcp_write(
            operation_id,
            # APPLYING is deliberately not retryable.  A lost post-commit
            # acknowledgement is ambiguous and must be reconciled instead of
            # executing the DML twice.
            expected_states={"PREVIEWED"},
            new_state="APPLYING",
            updates={},
            metadata={},
        )

    def record_mcp_write_commit(
        self,
        operation_id: str,
        *,
        affected_rows: int,
        committed_revision: int,
    ) -> dict[str, Any]:
        if affected_rows < 0 or committed_revision < 0:
            raise ValueError("MCP commit result is invalid")
        return self._transition_mcp_write(
            operation_id,
            expected_states={"APPLYING", "COMMITTED_PENDING_CHECKPOINT"},
            new_state="COMMITTED_PENDING_CHECKPOINT",
            updates={
                "affected_rows": affected_rows,
                "committed_revision": committed_revision,
                "committed_at": _format_time(self.clock.now()),
            },
            metadata={"affected_rows": affected_rows, "committed_revision": committed_revision},
        )

    def reconcile_mcp_write_commit(
        self,
        operation_id: str,
        *,
        request_sha256: str,
        master_instance_id: str,
        epoch: int,
        expected_revision: int,
        principal_id: str,
        client_id: str,
        affected_rows: int,
        committed_revision: int,
        committed_at: str,
    ) -> dict[str, Any]:
        """Idempotently project one exact canonical PostgreSQL receipt.

        PostgreSQL is authoritative for whether the DML committed.  This one
        IMMEDIATE SQLite transaction validates every durable intent binding
        before moving an ambiguous APPLYING projection forward; it never
        admits another DML execution.
        """

        if (
            len(request_sha256) != 64
            or not master_instance_id
            or epoch < 1
            or expected_revision < 0
            or not principal_id
            or not client_id
            or affected_rows < 1
            or committed_revision != expected_revision + 1
            or not committed_at
        ):
            raise ValueError("canonical MCP reconciliation receipt is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                raise StaleRuntimeEvent("MCP reconciliation has no durable preview intent")
            record = self._mcp_write_from_row(row)
            expected = {
                "request_sha256": request_sha256,
                "master_instance_id": master_instance_id,
                "epoch": epoch,
                "expected_revision": expected_revision,
                "principal_id": principal_id,
                "client_id": client_id,
            }
            if any(record[key] != value for key, value in expected.items()):
                raise StaleRuntimeEvent("canonical receipt differs from the durable MCP write intent")
            if record["state"] != "APPLYING":
                if (
                    record["state"]
                    in {
                        "COMMITTED_PENDING_CHECKPOINT",
                        "CHECKPOINTING",
                        "CHECKPOINT_VERIFIED",
                        "DURABLE_COMPLETE",
                    }
                    and record["affected_rows"] == affected_rows
                    and record["committed_revision"] == committed_revision
                ):
                    return record
                raise StaleRuntimeEvent("MCP reconciliation is stale or differs from its projection")
            connection.execute(
                "UPDATE mcp_write_operations SET state='COMMITTED_PENDING_CHECKPOINT',"
                "affected_rows=?,committed_revision=?,committed_at=?,updated_at=? WHERE operation_id=?",
                (affected_rows, committed_revision, committed_at, now, operation_id),
            )
            connection.execute(
                "INSERT INTO mcp_write_events(operation_id,state,metadata_json,recorded_at) "
                "VALUES (?,'COMMITTED_PENDING_CHECKPOINT',?,?)",
                (
                    operation_id,
                    _safe_json(
                        {
                            "affected_rows": affected_rows,
                            "committed_revision": committed_revision,
                            "reconciled_from": "canonical_postgres_receipt",
                        }
                    ),
                    now,
                ),
            )
            current = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._mcp_write_from_row(current)

    def advance_mcp_write_checkpoint(
        self,
        operation_id: str,
        *,
        state: str,
        post_change_checkpoint_id: str | None = None,
    ) -> dict[str, Any]:
        allowed = {
            "CHECKPOINTING": {"COMMITTED_PENDING_CHECKPOINT", "CHECKPOINTING"},
            "CHECKPOINT_VERIFIED": {"CHECKPOINTING", "CHECKPOINT_VERIFIED"},
            "DURABLE_COMPLETE": {"CHECKPOINT_VERIFIED", "DURABLE_COMPLETE"},
        }
        if state not in allowed or (state != "CHECKPOINTING" and not post_change_checkpoint_id):
            raise ValueError("MCP checkpoint transition is invalid")
        updates: dict[str, Any] = {}
        if post_change_checkpoint_id is not None:
            updates["post_change_checkpoint_id"] = post_change_checkpoint_id
        return self._transition_mcp_write(
            operation_id,
            expected_states=allowed[state],
            new_state=state,
            updates=updates,
            metadata={"post_change_checkpoint_id": post_change_checkpoint_id},
        )

    def mcp_write_operation(self, operation_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
        return self._mcp_write_from_row(row) if row else None

    def _transition_mcp_write(
        self,
        operation_id: str,
        *,
        expected_states: set[str],
        new_state: str,
        updates: Mapping[str, Any],
        metadata: Mapping[str, Any],
    ) -> dict[str, Any]:
        now = _format_time(self.clock.now())
        allowed_columns = {
            "preview_receipt",
            "affected_rows",
            "committed_revision",
            "committed_at",
            "post_change_checkpoint_id",
        }
        if not set(updates) <= allowed_columns:
            raise ValueError("unsupported MCP write projection update")
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None or str(row["state"]) not in expected_states:
                raise StaleRuntimeEvent("MCP write lifecycle transition is stale")
            assignments = ["state=?", "updated_at=?", *(f"{column}=?" for column in updates)]
            values = [new_state, now, *updates.values(), operation_id]
            connection.execute(
                f"UPDATE mcp_write_operations SET {','.join(assignments)} WHERE operation_id=?",
                values,
            )
            connection.execute(
                "INSERT INTO mcp_write_events(operation_id,state,metadata_json,recorded_at) VALUES (?,?,?,?)",
                (operation_id, new_state, _safe_json(metadata), now),
            )
            current = connection.execute(
                "SELECT * FROM mcp_write_operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert current is not None
            return self._mcp_write_from_row(current)

    @staticmethod
    def _mcp_write_from_row(row: sqlite3.Row) -> dict[str, Any]:
        return dict(zip(row.keys(), row, strict=True))

    def persist_provider_effect_receipt(self, effect_id: str, payload: Mapping[str, Any]) -> None:
        receipt_json = _safe_json(payload)
        receipt_sha256 = hashlib.sha256(receipt_json.encode()).hexdigest()
        with self._transaction() as connection:
            if (
                connection.execute("SELECT 1 FROM provider_effect_intents WHERE effect_id=?", (effect_id,)).fetchone()
                is None
            ):
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
            "claim_sha256",
            "task_id",
            "effect_id",
            "provider_ref",
            "kind",
            "control_class",
            "disposable",
            "provider_version",
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
                raise IdempotencyConflict("provider effect/resource version already has different claim authority")
            try:
                connection.execute(
                    "INSERT INTO provider_resource_claims(claim_sha256,task_id,effect_id,provider_ref,resource_kind,"
                    "control_class,disposable,provider_version,claim_json,recorded_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    (
                        str(payload["claim_sha256"]),
                        str(payload["task_id"]),
                        str(payload["effect_id"]),
                        str(payload["provider_ref"]),
                        str(payload["kind"]),
                        str(payload["control_class"]),
                        int(bool(payload["disposable"])),
                        int(payload["provider_version"]),
                        claim_json,
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

    def provider_effect_idempotency_key(self, effect_id: str) -> str | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT idempotency_key FROM provider_effect_intents WHERE effect_id=?", (effect_id,)
            ).fetchone()
        return str(row["idempotency_key"]) if row else None

    def provider_effect_arguments_sha256(self, effect_id: str) -> str | None:
        """Return the immutable argument digest without projecting provider payloads."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT intent_json FROM provider_effect_intents WHERE effect_id=?", (effect_id,)
            ).fetchone()
        if row is None:
            return None
        value = json.loads(str(row["intent_json"])).get("arguments_sha256")
        return str(value) if isinstance(value, str) and len(value) == 64 else None

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

    def provider_resource_claim_for_task(
        self,
        *,
        task_id: str,
        resource_kind: str,
        control_class: str,
        disposable: bool,
    ) -> dict[str, Any] | None:
        """Resolve one exact task-owned resource claim without guessing a latest version."""

        with self._reader() as connection:
            rows = connection.execute(
                "SELECT claim_json FROM provider_resource_claims WHERE task_id=? "
                "AND resource_kind=? AND control_class=? AND disposable=? "
                "ORDER BY provider_version,claim_sha256",
                (task_id, resource_kind, control_class, int(disposable)),
            ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise IdempotencyConflict("task has more than one matching provider resource claim")
        payload = json.loads(str(rows[0]["claim_json"]))
        if not isinstance(payload, dict):
            raise IdempotencyConflict("task provider resource claim is not an object")
        return payload

    def provider_resource_claim_exact(
        self,
        *,
        provider_ref: str,
        provider_version: int,
        resource_kind: str,
        control_class: str,
        disposable: bool,
    ) -> dict[str, Any] | None:
        """Resolve one immutable provider version without falling back to latest.

        Release-scoped master assets may be reused by a later task.  Such a
        task must not be credited with creating the Dataset, but its durable
        orchestration receipt can still bind the exact original claim by
        provider ref and numeric version.
        """

        if provider_version < 1:
            return None
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT claim_json FROM provider_resource_claims WHERE provider_ref=? "
                "AND provider_version=? AND resource_kind=? AND control_class=? AND disposable=? "
                "ORDER BY claim_sha256",
                (
                    provider_ref,
                    provider_version,
                    resource_kind,
                    control_class,
                    int(disposable),
                ),
            ).fetchall()
        if not rows:
            return None
        if len(rows) != 1:
            raise IdempotencyConflict("provider version has more than one durable resource claim")
        payload = json.loads(str(rows[0]["claim_json"]))
        if not isinstance(payload, dict):
            raise IdempotencyConflict("provider resource claim is not an object")
        return payload

    def provider_resource_claim(self, claim_sha256: str) -> dict[str, Any] | None:
        if len(claim_sha256) != 64:
            return None
        with self._reader() as connection:
            row = connection.execute(
                "SELECT claim_json FROM provider_resource_claims WHERE claim_sha256=?",
                (claim_sha256,),
            ).fetchone()
        return json.loads(str(row["claim_json"])) if row else None

    def provider_resource_claim_for_effect(self, effect_id: str) -> dict[str, Any] | None:
        """Return the exact claim committed by one deterministic provider effect."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT claim_json FROM provider_resource_claims WHERE effect_id=?",
                (effect_id,),
            ).fetchone()
        return json.loads(str(row["claim_json"])) if row else None

    def latest_provider_effect_receipt(self, effect_id: str) -> dict[str, Any] | None:
        """Return bounded reconciliation metadata; provider bytes never enter this journal."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT receipt_json FROM provider_effect_receipts WHERE effect_id=? ORDER BY sequence DESC LIMIT 1",
                (effect_id,),
            ).fetchone()
        return json.loads(str(row["receipt_json"])) if row else None

    def latest_successful_provider_effect_receipt(self, effect_id: str) -> dict[str, Any] | None:
        """Return the newest conclusive success for one immutable provider effect.

        A transport-loss reconciliation can append ``uncertain`` after an earlier
        exact readback was durably recorded.  Such a later observation must not
        erase the successful receipt used to bind an immutable resource claim.
        """

        with self._reader() as connection:
            rows = connection.execute(
                "SELECT receipt_json FROM provider_effect_receipts WHERE effect_id=? "
                "ORDER BY sequence DESC",
                (effect_id,),
            ).fetchall()
        for row in rows:
            receipt = json.loads(str(row["receipt_json"]))
            if receipt.get("outcome") in {"applied", "already_applied"}:
                return receipt
        return None

    def ensure_acceptance_evidence_task(
        self,
        *,
        scenario_id: str,
        task_id: str,
        idempotency_key: str,
        principal_id: str,
        client_id: str,
        request_sha256: str,
    ) -> tuple[dict[str, Any], bool]:
        """Claim an exact acceptance task before any provider side effect starts."""

        if (
            scenario_id not in {"FM01", "FM02", "FM03", "FM06", "FM22", "FM23"}
            or not task_id
            or not 8 <= len(idempotency_key) <= 300
            or not principal_id
            or not client_id
            or len(request_sha256) != 64
        ):
            raise ValueError("acceptance evidence task identity is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM acceptance_evidence_tasks WHERE (scenario_id=? AND task_id=?) OR "
                "(scenario_id=? AND principal_id=? AND client_id=? AND idempotency_key=?)",
                (scenario_id, task_id, scenario_id, principal_id, client_id, idempotency_key),
            ).fetchone()
            if row is not None:
                current = dict(row)
                expected = {
                    "scenario_id": scenario_id,
                    "task_id": task_id,
                    "idempotency_key": idempotency_key,
                    "principal_id": principal_id,
                    "client_id": client_id,
                    "request_sha256": request_sha256,
                }
                if any(current[key] != value for key, value in expected.items()):
                    raise IdempotencyConflict("acceptance task identity was reused for different evidence intent")
                return current, False
            connection.execute(
                "INSERT INTO acceptance_evidence_tasks(task_id,scenario_id,idempotency_key,principal_id,client_id,"
                "request_sha256,state,created_at,updated_at) VALUES (?,?,?,?,?,?,'CLAIMED',?,?)",
                (task_id, scenario_id, idempotency_key, principal_id, client_id, request_sha256, now, now),
            )
            evidence = _safe_json({"request_sha256": request_sha256})
            connection.execute(
                "INSERT INTO acceptance_evidence_events(scenario_id,task_id,event_type,evidence_json,"
                "evidence_sha256,recorded_at) VALUES (?,?, 'CLAIMED',?,?,?)",
                (scenario_id, task_id, evidence, hashlib.sha256(evidence.encode()).hexdigest(), now),
            )
            created = connection.execute(
                "SELECT * FROM acceptance_evidence_tasks WHERE scenario_id=? AND task_id=?",
                (scenario_id, task_id),
            ).fetchone()
            assert created is not None
            return dict(created), True

    def begin_acceptance_evidence_task(self, *, scenario_id: str, task_id: str) -> dict[str, Any]:
        """Durably mark that a provider mutation may follow this transaction."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM acceptance_evidence_tasks WHERE scenario_id=? AND task_id=?",
                (scenario_id, task_id),
            ).fetchone()
            if row is None or str(row["state"]) not in {"CLAIMED", "RUNNING"}:
                raise StaleRuntimeEvent("acceptance evidence task cannot begin from its current state")
            if str(row["state"]) == "CLAIMED":
                connection.execute(
                    "UPDATE acceptance_evidence_tasks SET state='RUNNING',mutation_started=1,updated_at=? "
                    "WHERE scenario_id=? AND task_id=?",
                    (now, scenario_id, task_id),
                )
                evidence = _safe_json({"mutation_started": True})
                connection.execute(
                    "INSERT INTO acceptance_evidence_events(scenario_id,task_id,event_type,evidence_json,"
                    "evidence_sha256,recorded_at) VALUES (?,?,'RUNNING',?,?,?)",
                    (scenario_id, task_id, evidence, hashlib.sha256(evidence.encode()).hexdigest(), now),
                )
            current = connection.execute(
                "SELECT * FROM acceptance_evidence_tasks WHERE scenario_id=? AND task_id=?",
                (scenario_id, task_id),
            ).fetchone()
            assert current is not None
            return dict(current)

    def append_acceptance_evidence(
        self,
        *,
        scenario_id: str,
        task_id: str,
        event_type: str,
        evidence: Mapping[str, Any],
    ) -> dict[str, Any]:
        allowed = {"PROVIDER_DATASET", "PROVIDER_NOTEBOOK", "OUTPUT_READ", "CLEANUP"}
        if event_type not in allowed:
            raise ValueError("acceptance evidence type is invalid")
        evidence_json = _safe_json(evidence)
        evidence_sha256 = hashlib.sha256(evidence_json.encode()).hexdigest()
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT state FROM acceptance_evidence_tasks WHERE scenario_id=? AND task_id=?",
                (scenario_id, task_id),
            ).fetchone()
            if row is None or str(row["state"]) not in {"RUNNING", "SUCCEEDED"}:
                raise StaleRuntimeEvent("acceptance evidence has no running or successful task claim")
            connection.execute(
                "INSERT OR IGNORE INTO acceptance_evidence_events(scenario_id,task_id,event_type,evidence_json,"
                "evidence_sha256,recorded_at) VALUES (?,?,?,?,?,?)",
                (scenario_id, task_id, event_type, evidence_json, evidence_sha256, _format_time(self.clock.now())),
            )
        return {"event_type": event_type, "evidence_sha256": evidence_sha256, **dict(evidence)}

    def terminalize_acceptance_evidence_task(
        self,
        *,
        scenario_id: str,
        task_id: str,
        state: str,
        evidence: Mapping[str, Any],
        failure_code: str | None = None,
    ) -> dict[str, Any]:
        if state not in {"SUCCEEDED", "FAILED"} or (state == "FAILED") != bool(failure_code):
            raise ValueError("acceptance terminal state is invalid")
        evidence_json = _safe_json(evidence)
        evidence_sha256 = hashlib.sha256(evidence_json.encode()).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM acceptance_evidence_tasks WHERE scenario_id=? AND task_id=?",
                (scenario_id, task_id),
            ).fetchone()
            if row is None:
                raise StaleRuntimeEvent("acceptance evidence terminal has no task claim")
            if str(row["state"]) in {"SUCCEEDED", "FAILED"}:
                if str(row["state"]) != state or (row["failure_code"] or None) != failure_code:
                    raise IdempotencyConflict("acceptance task is already terminal with a different result")
            else:
                connection.execute(
                    "UPDATE acceptance_evidence_tasks SET state=?,failure_code=?,updated_at=? "
                    "WHERE scenario_id=? AND task_id=?",
                    (state, failure_code, now, scenario_id, task_id),
                )
            connection.execute(
                "INSERT OR IGNORE INTO acceptance_evidence_events(scenario_id,task_id,event_type,evidence_json,"
                "evidence_sha256,recorded_at) VALUES (?,?,?,?,?,?)",
                (scenario_id, task_id, state, evidence_json, evidence_sha256, now),
            )
        result = self.acceptance_evidence_task(scenario_id=scenario_id, task_id=task_id)
        assert result is not None
        return result

    def acceptance_evidence_task(self, *, scenario_id: str, task_id: str) -> dict[str, Any] | None:
        """Read an exact metadata-only task and its bounded append-only evidence."""

        with self._reader() as connection:
            task = connection.execute(
                "SELECT * FROM acceptance_evidence_tasks WHERE scenario_id=? AND task_id=?",
                (scenario_id, task_id),
            ).fetchone()
            if task is None:
                return None
            events = connection.execute(
                "SELECT sequence,event_type,evidence_json,evidence_sha256,recorded_at "
                "FROM acceptance_evidence_events WHERE scenario_id=? AND task_id=? "
                "ORDER BY sequence ASC LIMIT 100",
                (scenario_id, task_id),
            ).fetchall()
        value = dict(task)
        value["mutation_started"] = bool(value["mutation_started"])
        value["evidence"] = [
            {
                "sequence": int(event["sequence"]),
                "event_type": str(event["event_type"]),
                "evidence": json.loads(str(event["evidence_json"])),
                "evidence_sha256": str(event["evidence_sha256"]),
                "recorded_at": str(event["recorded_at"]),
            }
            for event in events
        ]
        value["bounded"] = True
        return value

    def ensure_master_acceptance_task(
        self,
        *,
        task_id: str,
        scenario_id: str,
        idempotency_key: str,
        request_sha256: str,
        principal_id: str,
        client_id: str,
        source_revision: str,
        target_operation_id: str | None,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically persist one fixed lifecycle task before any fault/effect.

        FM04/FM07 intentionally start unbound.  All other scenarios must bind
        the exact currently ACTIVE master in this same transaction.
        """

        scenarios = {"FM04", "FM07", "FM08", "FM09", "FM10", "FM11", "FM12", "FM24"}
        try:
            UUID(task_id)
            if target_operation_id is not None:
                UUID(target_operation_id)
        except ValueError as exc:
            raise ValueError("master acceptance task identities must be UUIDs") from exc
        if (
            scenario_id not in scenarios
            or not 8 <= len(idempotency_key) <= 200
            or len(request_sha256) != 64
            or len(source_revision) != 40
            or not principal_id
            or not client_id
            or ((scenario_id in {"FM04", "FM07"}) != (target_operation_id is None))
        ):
            raise ValueError("master acceptance request violates the fixed contract")
        now = _format_time(self.clock.now())
        timeout_seconds = 5700 if scenario_id == "FM24" else 1800
        deadline_at = _format_time(self.clock.now() + timedelta(seconds=timeout_seconds))
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_tasks WHERE task_id=? OR idempotency_key=?",
                (task_id, idempotency_key),
            ).fetchone()
            if row is not None:
                expected = {
                    "task_id": task_id,
                    "scenario_id": scenario_id,
                    "idempotency_key": idempotency_key,
                    "request_sha256": request_sha256,
                    "principal_id": principal_id,
                    "client_id": client_id,
                    "source_revision": source_revision,
                    "target_operation_id": target_operation_id,
                }
                if any(row[key] != value for key, value in expected.items()):
                    raise IdempotencyConflict("master acceptance identity was reused for another request")
                return self._master_acceptance_task_from_connection(connection, row), False

            binding: dict[str, Any] | None = None
            if target_operation_id is not None:
                binding = self._active_master_binding(connection, target_operation_id)
            else:
                live = connection.execute(
                    "SELECT 1 FROM services WHERE service_kind='postgres-master' "
                    "AND state IN ('ACTIVE','DRAINING') LIMIT 1"
                ).fetchone()
                if live is not None:
                    raise StaleRuntimeEvent("FM04/FM07 require an ABSENT or terminal master")
                if scenario_id == "FM04":
                    head = connection.execute(
                        "SELECT current_checkpoint_id FROM checkpoint_heads WHERE service_kind='postgres-master'"
                    ).fetchone()
                    if head is not None and head["current_checkpoint_id"] is not None:
                        raise StaleRuntimeEvent("FM04 empty bootstrap requires no checkpoint HEAD")
            values = binding or {}
            state = "BOUND" if binding else "PENDING"
            connection.execute(
                "INSERT INTO master_acceptance_tasks(task_id,scenario_id,idempotency_key,request_sha256,"
                "principal_id,client_id,source_revision,target_operation_id,target_run_id,target_attempt_id,"
                "target_service_instance_id,target_master_instance_id,target_epoch,state,timeout_seconds,"
                "deadline_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    task_id,
                    scenario_id,
                    idempotency_key,
                    request_sha256,
                    principal_id,
                    client_id,
                    source_revision,
                    values.get("operation_id"),
                    values.get("run_id"),
                    values.get("attempt_id"),
                    values.get("service_instance_id"),
                    values.get("master_instance_id"),
                    values.get("epoch"),
                    state,
                    timeout_seconds,
                    deadline_at,
                    now,
                    now,
                ),
            )
            self._append_master_acceptance_event(
                connection,
                task_id=task_id,
                event_type="PENDING",
                evidence={"request_sha256": request_sha256, "scenario_id": scenario_id},
                now=now,
            )
            if binding is not None:
                self._insert_master_acceptance_command(
                    connection,
                    task_id=task_id,
                    scenario_id=scenario_id,
                    source_revision=source_revision,
                    binding=binding,
                    now=now,
                )
            created = connection.execute("SELECT * FROM master_acceptance_tasks WHERE task_id=?", (task_id,)).fetchone()
            assert created is not None
            return self._master_acceptance_task_from_connection(connection, created), True

    def bind_master_acceptance_task(self, *, task_id: str, operation_id: str) -> dict[str, Any]:
        """Bind a pre-boot FM04/FM07 task to the one observed ACTIVE result."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute("SELECT * FROM master_acceptance_tasks WHERE task_id=?", (task_id,)).fetchone()
            if row is None or row["scenario_id"] not in {"FM04", "FM07"}:
                raise StaleRuntimeEvent("only a claimed pre-boot acceptance task can be bound")
            binding = self._active_master_binding(connection, operation_id)
            if row["state"] == "PENDING":
                connection.execute(
                    "UPDATE master_acceptance_tasks SET target_operation_id=?,target_run_id=?,target_attempt_id=?,"
                    "target_service_instance_id=?,target_master_instance_id=?,target_epoch=?,state='BOUND',"
                    "updated_at=? "
                    "WHERE task_id=? AND state='PENDING'",
                    (
                        binding["operation_id"],
                        binding["run_id"],
                        binding["attempt_id"],
                        binding["service_instance_id"],
                        binding["master_instance_id"],
                        binding["epoch"],
                        now,
                        task_id,
                    ),
                )
                self._insert_master_acceptance_command(
                    connection,
                    task_id=task_id,
                    scenario_id=str(row["scenario_id"]),
                    source_revision=str(row["source_revision"]),
                    binding=binding,
                    now=now,
                )
            elif any(
                row[key] != binding[key.removeprefix("target_")]
                for key in (
                    "target_operation_id",
                    "target_run_id",
                    "target_attempt_id",
                    "target_service_instance_id",
                    "target_master_instance_id",
                    "target_epoch",
                )
            ):
                raise IdempotencyConflict("pre-boot task was already bound to another master")
            current = connection.execute("SELECT * FROM master_acceptance_tasks WHERE task_id=?", (task_id,)).fetchone()
            assert current is not None
            return self._master_acceptance_task_from_connection(connection, current)

    def claim_master_acceptance_command(self, *, run_id: str, attempt_id: str, epoch: int) -> dict[str, Any] | None:
        """Lease the sole fixed command only to its exact authenticated runtime."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT c.*,t.source_revision,t.deadline_at,t.target_operation_id,t.target_master_instance_id,"
                "t.target_service_instance_id FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id "
                "WHERE t.target_run_id=? AND t.target_attempt_id=? AND t.target_epoch=? "
                "AND t.scenario_id IN ('FM04','FM24') AND t.state IN ('BOUND','CLAIMED') "
                "AND (c.state='PENDING' OR (c.state='CLAIMED' AND c.claim_authority='runtime')) "
                "ORDER BY t.created_at LIMIT 1",
                (run_id, attempt_id, epoch),
            ).fetchone()
            if row is None:
                return None
            if _parse_time(str(row["deadline_at"])) <= self.clock.now():
                connection.execute(
                    "UPDATE master_acceptance_tasks SET state='FAILED',failure_code='ACCEPTANCE_TIMEOUT',"
                    "updated_at=? WHERE task_id=? AND state IN ('BOUND','CLAIMED')",
                    (now, row["task_id"]),
                )
                self._append_master_acceptance_event(
                    connection,
                    task_id=str(row["task_id"]),
                    event_type="FAILED",
                    evidence={"failure_code": "ACCEPTANCE_TIMEOUT"},
                    now=now,
                )
                return None
            binding = self._active_master_binding(connection, str(row["target_operation_id"]))
            if (binding["run_id"], binding["attempt_id"], binding["epoch"]) != (
                run_id,
                attempt_id,
                epoch,
            ):
                raise StaleRuntimeEvent("acceptance command target is no longer the ACTIVE runtime")
            if row["state"] == "PENDING":
                connection.execute(
                    "UPDATE master_acceptance_commands SET state='CLAIMED',claimed_run_id=?,claimed_attempt_id=?,"
                    "claimed_epoch=?,claimed_at=?,claim_authority='runtime' "
                    "WHERE command_id=? AND state='PENDING' AND claim_authority IS NULL",
                    (run_id, attempt_id, epoch, now, row["command_id"]),
                )
                connection.execute(
                    "UPDATE master_acceptance_tasks SET state='CLAIMED',updated_at=? WHERE task_id=? AND state='BOUND'",
                    (now, row["task_id"]),
                )
                self._append_master_acceptance_event(
                    connection,
                    task_id=str(row["task_id"]),
                    event_type="CLAIMED",
                    evidence={"command_sha256": str(row["command_sha256"]), "epoch": epoch},
                    now=now,
                )
            return self._master_acceptance_command_from_row(row, binding)

    def claim_master_acceptance_host_command(
        self,
        *,
        task_id: str,
        expected_scenario: str,
        principal_id: str,
        client_id: str,
    ) -> dict[str, Any] | None:
        """Owner-bound CAS for fixed control-host scenarios.

        This is deliberately separate from the runtime-token claim.  It never
        consults or issues a runtime secret and can only replay the same
        principal/client claim.
        """

        host_scenarios = {"FM07", "FM08", "FM09", "FM10", "FM11", "FM12"}
        if expected_scenario not in host_scenarios or not principal_id or not client_id:
            raise ValueError("master acceptance host claim identity is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT c.*,t.scenario_id,t.source_revision,t.deadline_at,t.principal_id,t.client_id,"
                "t.target_operation_id,t.target_run_id,t.target_attempt_id,t.target_service_instance_id,"
                "t.target_master_instance_id,t.target_epoch FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id WHERE c.task_id=?",
                (task_id,),
            ).fetchone()
            if row is None:
                return None
            if (
                row["scenario_id"] != expected_scenario
                or row["principal_id"] != principal_id
                or row["client_id"] != client_id
            ):
                raise StaleRuntimeEvent("acceptance host claim differs from its task owner")
            if _parse_time(str(row["deadline_at"])) <= self.clock.now():
                connection.execute(
                    "UPDATE master_acceptance_tasks SET state='FAILED',failure_code='ACCEPTANCE_TIMEOUT',"
                    "updated_at=? WHERE task_id=? AND state IN ('BOUND','CLAIMED')",
                    (now, task_id),
                )
                self._append_master_acceptance_event(
                    connection,
                    task_id=task_id,
                    event_type="FAILED",
                    evidence={"failure_code": "ACCEPTANCE_TIMEOUT"},
                    now=now,
                )
                return None
            if row["state"] in {"SUCCEEDED", "FAILED"}:
                return None
            if row["state"] == "PENDING":
                changed = connection.execute(
                    "UPDATE master_acceptance_commands SET state='CLAIMED',claimed_run_id=?,claimed_attempt_id=?,"
                    "claimed_epoch=?,claimed_at=?,claim_authority='owner_host',claimed_principal_id=?,"
                    "claimed_client_id=? WHERE command_id=? AND state='PENDING' AND claim_authority IS NULL",
                    (
                        row["target_run_id"],
                        row["target_attempt_id"],
                        row["target_epoch"],
                        now,
                        principal_id,
                        client_id,
                        row["command_id"],
                    ),
                ).rowcount
                if changed != 1:
                    raise StaleRuntimeEvent("acceptance host command claim lost its CAS")
                connection.execute(
                    "UPDATE master_acceptance_tasks SET state='CLAIMED',updated_at=? WHERE task_id=? AND state='BOUND'",
                    (now, task_id),
                )
                self._append_master_acceptance_event(
                    connection,
                    task_id=task_id,
                    event_type="HOST_CLAIMED",
                    evidence={
                        "command_sha256": str(row["command_sha256"]),
                        "principal_id": principal_id,
                        "client_id": client_id,
                    },
                    now=now,
                )
            elif (
                row["state"] != "CLAIMED"
                or row["claim_authority"] != "owner_host"
                or row["claimed_principal_id"] != principal_id
                or row["claimed_client_id"] != client_id
            ):
                raise StaleRuntimeEvent("acceptance command is claimed by another authority")
            binding = {
                "operation_id": row["target_operation_id"],
                "run_id": row["target_run_id"],
                "attempt_id": row["target_attempt_id"],
                "service_instance_id": row["target_service_instance_id"],
                "master_instance_id": row["target_master_instance_id"],
                "epoch": int(row["target_epoch"]),
            }
            return self._master_acceptance_command_from_row(row, binding)

    def complete_master_acceptance_host_command(
        self,
        *,
        command_id: str,
        command_sha256: str,
        principal_id: str,
        client_id: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        """CAS one exact owner-host claim to PASSED with metadata-only receipt."""

        from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceReceipt

        validated_receipt = MasterAcceptanceReceipt.model_validate(receipt)
        receipt_json = _safe_json(validated_receipt.model_dump(mode="json"), max_bytes=64 * 1024)
        receipt_sha256 = hashlib.sha256(receipt_json.encode()).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT c.*,t.principal_id,t.client_id,t.target_operation_id,t.deadline_at "
                "FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id WHERE c.command_id=?",
                (command_id,),
            ).fetchone()
            if row is None:
                raise StaleRuntimeEvent("acceptance command is unknown")
            if _parse_time(str(row["deadline_at"])) <= self.clock.now():
                raise StaleRuntimeEvent("acceptance host receipt arrived after its absolute deadline")
            if (
                row["command_sha256"] != command_sha256
                or str(validated_receipt.command_id) != command_id
                or validated_receipt.command_sha256 != command_sha256
                or str(validated_receipt.binding.run_id) != row["claimed_run_id"]
                or str(validated_receipt.binding.attempt_id) != row["claimed_attempt_id"]
                or validated_receipt.binding.epoch != row["claimed_epoch"]
                or str(validated_receipt.binding.operation_id) != row["target_operation_id"]
                or row["claim_authority"] != "owner_host"
                or row["principal_id"] != principal_id
                or row["client_id"] != client_id
                or row["claimed_principal_id"] != principal_id
                or row["claimed_client_id"] != client_id
            ):
                raise StaleRuntimeEvent("acceptance host receipt differs from its exact owner claim")
            if row["state"] == "SUCCEEDED":
                if not hmac.compare_digest(str(row["receipt_sha256"]), receipt_sha256):
                    raise IdempotencyConflict("acceptance host command already has another receipt")
            elif row["state"] != "CLAIMED":
                raise StaleRuntimeEvent("acceptance host command was not claimed")
            else:
                changed = connection.execute(
                    "UPDATE master_acceptance_commands SET state='SUCCEEDED',receipt_json=?,receipt_sha256=?,"
                    "completed_at=? WHERE command_id=? AND state='CLAIMED' AND claim_authority='owner_host'",
                    (receipt_json, receipt_sha256, now, command_id),
                ).rowcount
                if changed != 1:
                    raise StaleRuntimeEvent("acceptance host completion lost its CAS")
                connection.execute(
                    "UPDATE master_acceptance_tasks SET state='PASSED',failure_code=NULL,updated_at=? "
                    "WHERE task_id=? AND state='CLAIMED'",
                    (now, row["task_id"]),
                )
                self._append_master_acceptance_event(
                    connection,
                    task_id=str(row["task_id"]),
                    event_type="SUCCEEDED",
                    evidence={"receipt_sha256": receipt_sha256, "claim_authority": "owner_host"},
                    now=now,
                )
            task = connection.execute(
                "SELECT * FROM master_acceptance_tasks WHERE task_id=?", (row["task_id"],)
            ).fetchone()
            assert task is not None
            return self._master_acceptance_task_from_connection(connection, task)

    def ensure_fm08_abrupt_recovery(
        self,
        *,
        task_id: str,
        command_id: str,
        command_sha256: str,
        old_operation_id: str,
        old_run: Mapping[str, Any],
        old_epoch: int,
        replacement_idempotency_key: str,
        replacement_notebook_ref: str,
    ) -> tuple[dict[str, Any], bool]:
        """Persist the exact old/recovery run plan before terminating Kaggle."""

        for value in (task_id, command_id, old_operation_id):
            try:
                UUID(value)
            except ValueError as exc:
                raise ValueError("FM08 abrupt recovery requires exact UUID identities") from exc
        if (
            len(command_sha256) != 64
            or old_epoch < 1
            or not 8 <= len(replacement_idempotency_key) <= 200
            or len(replacement_notebook_ref.split("/")) != 2
            or any(not part for part in replacement_notebook_ref.split("/"))
        ):
            raise ValueError("FM08 abrupt recovery identity is invalid")
        old_run_json = _safe_json(old_run, max_bytes=16 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            command = connection.execute(
                "SELECT c.command_id,c.command_sha256,c.state,c.claim_authority,t.scenario_id,"
                "t.target_operation_id,t.target_epoch FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id WHERE c.task_id=?",
                (task_id,),
            ).fetchone()
            operation = connection.execute(
                "SELECT state,identity_json FROM operations WHERE operation_id=?",
                (old_operation_id,),
            ).fetchone()
            if (
                command is None
                or operation is None
                or command["command_id"] != command_id
                or command["command_sha256"] != command_sha256
                or command["state"] != "CLAIMED"
                or command["claim_authority"] != "owner_host"
                or command["scenario_id"] != "FM08"
                or command["target_operation_id"] != old_operation_id
                or int(command["target_epoch"]) != old_epoch
            ):
                raise StaleRuntimeEvent("FM08 abrupt recovery differs from its owner-host claim")
            existing = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if existing is not None:
                value = self._fm08_abrupt_recovery_from_row(existing)
                expected = {
                    "command_id": command_id,
                    "command_sha256": command_sha256,
                    "old_operation_id": old_operation_id,
                    "old_epoch": old_epoch,
                    "replacement_idempotency_key": replacement_idempotency_key,
                    "replacement_notebook_ref": replacement_notebook_ref,
                }
                if any(value[key] != item for key, item in expected.items()) or value["old_run"] != json.loads(
                    old_run_json
                ):
                    raise IdempotencyConflict("FM08 abrupt recovery plan changed")
                return value, False
            identity = json.loads(str(operation["identity_json"]))
            if operation["state"] != "ACTIVE" or int(identity.get("epoch", 0)) != old_epoch:
                raise StaleRuntimeEvent("FM08 abrupt recovery requires the exact ACTIVE old master")
            connection.execute(
                "INSERT INTO master_acceptance_abrupt_recoveries(task_id,command_id,command_sha256,"
                "old_operation_id,old_run_json,old_epoch,replacement_idempotency_key,"
                "replacement_notebook_ref,state,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,'INTENT',?,?)",
                (
                    task_id,
                    command_id,
                    command_sha256,
                    old_operation_id,
                    old_run_json,
                    old_epoch,
                    replacement_idempotency_key,
                    replacement_notebook_ref,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?",
                (task_id,),
            ).fetchone()
            assert row is not None
            return self._fm08_abrupt_recovery_from_row(row), True

    def begin_fm11_old_epoch_context(
        self,
        *,
        task_id: str,
        command_id: str,
        command_sha256: str,
        credential_handle: str,
        expires_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        """Persist the exact pre-STOPPED capture intent before opening H1."""

        try:
            UUID(task_id)
            UUID(command_id)
            UUID(credential_handle)
        except ValueError as exc:
            raise ValueError("FM11 context requires exact UUID identities") from exc
        if (
            len(command_sha256) != 64
            or expires_at.tzinfo is None
            or not self.clock.now() < expires_at <= self.clock.now() + timedelta(seconds=900)
        ):
            raise ValueError("FM11 context deadline or command hash is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            command = connection.execute(
                "SELECT c.command_id,c.command_sha256,c.state,c.claim_authority,t.scenario_id,"
                "t.target_operation_id FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id WHERE c.task_id=?",
                (task_id,),
            ).fetchone()
            if (
                command is None
                or command["command_id"] != command_id
                or command["command_sha256"] != command_sha256
                or command["state"] != "CLAIMED"
                or command["claim_authority"] != "owner_host"
                or command["scenario_id"] != "FM11"
            ):
                raise StaleRuntimeEvent("FM11 context differs from its owner-host claim")
            existing = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if existing is not None:
                value = self._fm11_old_epoch_context_from_row(existing)
                if (
                    value["command_id"] != command_id
                    or value["command_sha256"] != command_sha256
                    or value["credential_handle"] != credential_handle
                ):
                    raise IdempotencyConflict("FM11 context capture identity changed")
                return value, False
            operation = connection.execute(
                "SELECT state,identity_json FROM operations WHERE operation_id=?",
                (command["target_operation_id"],),
            ).fetchone()
            if operation is None or operation["state"] != "ACTIVE":
                raise StaleRuntimeEvent("FM11 context must be captured while the old master is ACTIVE")
            identity = json.loads(str(operation["identity_json"]))
            token = connection.execute(
                "SELECT token_sha256,revoked_at FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
                (identity["run_id"], identity["attempt_id"]),
            ).fetchone()
            if token is None or token["revoked_at"] is not None:
                raise StaleRuntimeEvent("FM11 old runtime token is unavailable before capture")
            binding = {
                "operation_id": str(command["target_operation_id"]),
                "run_id": str(identity["run_id"]),
                "attempt_id": str(identity["attempt_id"]),
                "service_instance_id": str(identity["service_instance_id"]),
                "master_instance_id": str(identity["master_instance_id"]),
                "epoch": int(identity["epoch"]),
            }
            connection.execute(
                "INSERT INTO master_acceptance_old_epoch_contexts(task_id,command_id,command_sha256,"
                "old_operation_id,old_binding_json,runtime_token_sha256,credential_handle,state,"
                "expires_at,created_at,updated_at) VALUES (?,?,?,?,?,?,?,'INTENT',?,?,?)",
                (
                    task_id,
                    command_id,
                    command_sha256,
                    command["target_operation_id"],
                    _safe_json(binding),
                    token["token_sha256"],
                    credential_handle,
                    _format_time(expires_at),
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?", (task_id,)
            ).fetchone()
            assert row is not None
            return self._fm11_old_epoch_context_from_row(row), True

    def complete_fm11_old_epoch_context_capture(
        self,
        *,
        task_id: str,
        tunnel_certificate: Mapping[str, Any],
        context_sha256: str,
    ) -> dict[str, Any]:
        tunnel_json = _safe_json(tunnel_certificate, max_bytes=8 * 1024)
        if len(context_sha256) != 64:
            raise ValueError("FM11 context digest is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?", (task_id,)
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["state"] == "CAPTURED":
                if row["tunnel_certificate_json"] != tunnel_json or row["context_sha256"] != context_sha256:
                    raise IdempotencyConflict("FM11 captured context changed")
            elif row["state"] != "INTENT" or str(row["expires_at"]) <= now:
                raise StaleRuntimeEvent("FM11 context capture intent is no longer current")
            else:
                connection.execute(
                    "UPDATE master_acceptance_old_epoch_contexts SET state='CAPTURED',"
                    "tunnel_certificate_json=?,context_sha256=?,captured_at=?,updated_at=? "
                    "WHERE task_id=? AND state='INTENT'",
                    (tunnel_json, context_sha256, now, now, task_id),
                )
            current = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?", (task_id,)
            ).fetchone()
            assert current is not None
            return self._fm11_old_epoch_context_from_row(current)

    def release_fm11_old_epoch_context(self, *, task_id: str, context_sha256: str, result_receipt_sha256: str) -> None:
        for value in (context_sha256, result_receipt_sha256):
            if len(value) != 64:
                raise ValueError("FM11 context release digest is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?", (task_id,)
            ).fetchone()
            if row is None or row["context_sha256"] != context_sha256:
                raise StaleRuntimeEvent("FM11 released context differs from its captured identity")
            if row["state"] == "RELEASED":
                if row["result_receipt_sha256"] != result_receipt_sha256:
                    raise IdempotencyConflict("FM11 released context receipt changed")
                return
            if row["state"] != "CAPTURED":
                raise StaleRuntimeEvent("FM11 context was not captured before release")
            connection.execute(
                "UPDATE master_acceptance_old_epoch_contexts SET state='RELEASED',released_at=?,"
                "result_receipt_sha256=?,updated_at=? WHERE task_id=? AND state='CAPTURED'",
                (now, result_receipt_sha256, now, task_id),
            )

    def fm11_old_epoch_context(self, task_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?", (task_id,)
            ).fetchone()
        return self._fm11_old_epoch_context_from_row(row) if row is not None else None

    def fm11_retired_admission_observation(self, *, task_id: str, replacement_operation_id: str) -> dict[str, Any]:
        """Project fixed runtime/register denial facts from canonical admission state."""

        with self._reader() as connection:
            context = connection.execute(
                "SELECT * FROM master_acceptance_old_epoch_contexts WHERE task_id=?", (task_id,)
            ).fetchone()
            if context is None or context["state"] != "CAPTURED":
                raise StaleRuntimeEvent("FM11 retired admission has no captured context")
            old_binding = json.loads(str(context["old_binding_json"]))
            old_operation = connection.execute(
                "SELECT state FROM operations WHERE operation_id=?", (context["old_operation_id"],)
            ).fetchone()
            replacement = connection.execute(
                "SELECT state,identity_json FROM operations WHERE operation_id=?",
                (replacement_operation_id,),
            ).fetchone()
            token = connection.execute(
                "SELECT token_sha256,revoked_at FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
                (old_binding["run_id"], old_binding["attempt_id"]),
            ).fetchone()
            service = connection.execute(
                "SELECT * FROM services WHERE service_kind='postgres-master' AND state='ACTIVE' "
                "ORDER BY epoch DESC LIMIT 1"
            ).fetchone()
            epoch = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
            ).fetchone()
        if (
            old_operation is None
            or old_operation["state"] != "STOPPED"
            or replacement is None
            or replacement["state"] != "ACTIVE"
            or token is None
            or token["revoked_at"] is None
            or service is None
            or epoch is None
        ):
            raise StaleRuntimeEvent("FM11 retired runtime is not durably denied/current")
        replacement_identity = json.loads(str(replacement["identity_json"]))
        if (
            int(replacement_identity["epoch"]) != int(old_binding["epoch"]) + 1
            or int(epoch["current_epoch"]) != int(replacement_identity["epoch"])
            or service["run_id"] != replacement_identity["run_id"]
            or service["attempt_id"] != replacement_identity["attempt_id"]
            or service["master_instance_id"] != replacement_identity["master_instance_id"]
        ):
            raise StaleRuntimeEvent("FM11 replacement is not the current admission authority")
        return {
            "runtime_token_sha256": str(token["token_sha256"]),
            "heartbeat_denied": True,
            "lease_renewal_denied": True,
            "runtime_token_revoked": True,
            "registration_denied": True,
            "old_credential_bind_denied": True,
        }

    @staticmethod
    def _fm11_old_epoch_context_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["old_binding"] = json.loads(str(value.pop("old_binding_json")))
        tunnel = value.pop("tunnel_certificate_json")
        value["tunnel_certificate"] = json.loads(str(tunnel)) if tunnel is not None else None
        return value

    def fence_fm08_abrupt_master(
        self,
        *,
        task_id: str,
        termination_receipt: Mapping[str, Any],
        termination_receipt_sha256: str,
    ) -> dict[str, Any]:
        """Fence the exact old epoch only after its provider run is absent."""

        payload = _safe_json(termination_receipt, max_bytes=32 * 1024)
        if hashlib.sha256(payload.encode()).hexdigest() != termination_receipt_sha256:
            raise ValueError("FM08 termination receipt hash is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?",
                (task_id,),
            ).fetchone()
            if row is None:
                raise KeyError(task_id)
            if row["termination_receipt_json"] is not None:
                if (
                    row["termination_receipt_json"] != payload
                    or row["termination_receipt_sha256"] != termination_receipt_sha256
                ):
                    raise IdempotencyConflict("FM08 termination receipt changed")
                return self._fm08_abrupt_recovery_from_row(row)
            operation = connection.execute(
                "SELECT state,identity_json FROM operations WHERE operation_id=?",
                (row["old_operation_id"],),
            ).fetchone()
            if operation is None or operation["state"] != "ACTIVE":
                raise StaleRuntimeEvent("FM08 old master is no longer atomically fenceable")
            identity = json.loads(str(operation["identity_json"]))
            epoch = int(row["old_epoch"])
            connection.execute(
                "UPDATE operations SET state='FENCED',updated_at=? WHERE operation_id=? AND state='ACTIVE'",
                (now, row["old_operation_id"]),
            )
            connection.execute(
                "INSERT INTO operation_log(operation_id,from_state,to_state,recorded_at,metadata_json) "
                "VALUES (?,'ACTIVE','FENCED',?,?)",
                (
                    row["old_operation_id"],
                    now,
                    _safe_json({"code": "FM08_ABRUPT_MASTER_TERMINATED", "task_id": task_id}),
                ),
            )
            connection.execute(
                "UPDATE services SET state='FENCED',updated_at=? WHERE service_instance_id=? "
                "AND epoch=? AND state IN ('ACTIVE','DRAINING','REGISTERING')",
                (now, identity["service_instance_id"], epoch),
            )
            connection.execute(
                "UPDATE run_attempts SET state='FENCED',updated_at=? WHERE run_id=? AND attempt_id=? AND epoch=?",
                (now, identity["run_id"], identity["attempt_id"], epoch),
            )
            connection.execute(
                "UPDATE runtime_token_hashes SET revoked_at=? WHERE run_id=? AND attempt_id=?",
                (now, identity["run_id"], identity["attempt_id"]),
            )
            connection.execute(
                "UPDATE master_acceptance_abrupt_recoveries SET state='TERMINATED',"
                "termination_receipt_json=?,termination_receipt_sha256=?,updated_at=? WHERE task_id=?",
                (payload, termination_receipt_sha256, now, task_id),
            )
            current = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?",
                (task_id,),
            ).fetchone()
            assert current is not None
            return self._fm08_abrupt_recovery_from_row(current)

    def bind_fm08_replacement(
        self,
        *,
        task_id: str,
        replacement_operation_id: str,
        replacement_run: Mapping[str, Any] | None,
        recovery_receipt_sha256: str | None,
        active: bool,
    ) -> dict[str, Any]:
        run_json = _safe_json(replacement_run, max_bytes=16 * 1024) if replacement_run is not None else None
        if active and (run_json is None or recovery_receipt_sha256 is None):
            raise ValueError("FM08 active recovery requires its exact provider receipt")
        if recovery_receipt_sha256 is not None and (
            len(recovery_receipt_sha256) != 64
            or any(character not in "0123456789abcdef" for character in recovery_receipt_sha256)
        ):
            raise ValueError("FM08 recovery receipt hash is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?",
                (task_id,),
            ).fetchone()
            operation = connection.execute(
                "SELECT state,identity_json,idempotency_key FROM operations WHERE operation_id=?",
                (replacement_operation_id,),
            ).fetchone()
            if row is None or operation is None or row["state"] not in {"TERMINATED", "RECOVERING", "ACTIVE"}:
                raise StaleRuntimeEvent("FM08 replacement has no terminated old master")
            identity = json.loads(str(operation["identity_json"]))
            if (
                operation["idempotency_key"] != row["replacement_idempotency_key"]
                or int(identity.get("epoch", 0)) != int(row["old_epoch"]) + 1
                or (active and operation["state"] != "ACTIVE")
            ):
                raise StaleRuntimeEvent("FM08 replacement identity is not consecutive/current")
            if row["replacement_operation_id"] not in {None, replacement_operation_id}:
                raise IdempotencyConflict("FM08 replacement operation changed")
            if run_json is not None and row["replacement_run_json"] not in {None, run_json}:
                raise IdempotencyConflict("FM08 replacement provider run changed")
            if recovery_receipt_sha256 is not None and row["recovery_receipt_sha256"] not in {
                None,
                recovery_receipt_sha256,
            }:
                raise IdempotencyConflict("FM08 recovery receipt changed")
            connection.execute(
                "UPDATE master_acceptance_abrupt_recoveries SET replacement_operation_id=?,"
                "replacement_run_json=COALESCE(?,replacement_run_json),"
                "recovery_receipt_sha256=COALESCE(?,recovery_receipt_sha256),"
                "state=CASE WHEN state='ACTIVE' THEN 'ACTIVE' ELSE ? END,updated_at=? WHERE task_id=?",
                (
                    replacement_operation_id,
                    run_json,
                    recovery_receipt_sha256,
                    "ACTIVE" if active else "RECOVERING",
                    now,
                    task_id,
                ),
            )
            current = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?",
                (task_id,),
            ).fetchone()
            assert current is not None
            return self._fm08_abrupt_recovery_from_row(current)

    def fm08_abrupt_recovery(self, task_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE task_id=?", (task_id,)
            ).fetchone()
        return self._fm08_abrupt_recovery_from_row(row) if row is not None else None

    def fm08_recovery_for_operation(self, operation_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_abrupt_recoveries WHERE replacement_operation_id=?",
                (operation_id,),
            ).fetchone()
        return self._fm08_abrupt_recovery_from_row(row) if row is not None else None

    @staticmethod
    def _fm08_abrupt_recovery_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["old_run"] = json.loads(str(value.pop("old_run_json")))
        replacement = value.pop("replacement_run_json")
        termination = value.pop("termination_receipt_json")
        value["replacement_run"] = json.loads(str(replacement)) if replacement is not None else None
        value["termination_receipt"] = json.loads(str(termination)) if termination is not None else None
        return value

    def arm_master_acceptance_callback_loss(
        self,
        *,
        task_id: str,
        command_id: str,
        command_sha256: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
        before_boot_id: str,
    ) -> dict[str, Any]:
        """Arm exactly one task-bound FM08 heartbeat response loss."""

        return self._ensure_master_acceptance_runtime_control(
            task_id=task_id,
            command_id=command_id,
            command_sha256=command_sha256,
            scenario_id="FM08",
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            callback_state="ARMED",
            renewal_suspended=False,
            before_boot_id=before_boot_id,
        )

    def suspend_master_acceptance_renewal(
        self,
        *,
        task_id: str,
        command_id: str,
        command_sha256: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> dict[str, Any]:
        """Suspend both heartbeat and database-gate renewal for exact FM10."""

        return self._ensure_master_acceptance_runtime_control(
            task_id=task_id,
            command_id=command_id,
            command_sha256=command_sha256,
            scenario_id="FM10",
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            callback_state="DISARMED",
            renewal_suspended=True,
            before_boot_id=None,
        )

    def _ensure_master_acceptance_runtime_control(
        self,
        *,
        task_id: str,
        command_id: str,
        command_sha256: str,
        scenario_id: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
        callback_state: str,
        renewal_suspended: bool,
        before_boot_id: str | None,
    ) -> dict[str, Any]:
        try:
            for value in (task_id, command_id, run_id, attempt_id, master_instance_id):
                UUID(value)
            if before_boot_id is not None:
                UUID(before_boot_id)
        except ValueError as exc:
            raise ValueError("acceptance runtime control requires exact UUID identities") from exc
        if len(command_sha256) != 64 or epoch < 1:
            raise ValueError("acceptance runtime control binding is invalid")
        now = _format_time(self.clock.now())
        armed_at = now if callback_state == "ARMED" else None
        expires_at = _format_time(self.clock.now() + timedelta(seconds=900)) if callback_state == "ARMED" else None
        directive_receipt_sha256 = (
            hashlib.sha256(
                _safe_json(
                    {
                        "schema_version": "my-data-hub-fm08-callback-directive.v1",
                        "task_id": task_id,
                        "command_id": command_id,
                        "command_sha256": command_sha256,
                        "run_id": run_id,
                        "attempt_id": attempt_id,
                        "master_instance_id": master_instance_id,
                        "epoch": epoch,
                        "event_type": "runtime.heartbeat",
                        "maximum_callbacks": 1,
                        "before_boot_id": before_boot_id,
                        "expires_at": expires_at,
                    }
                ).encode()
            ).hexdigest()
            if callback_state == "ARMED"
            else None
        )
        with self._transaction() as connection:
            command = connection.execute(
                "SELECT c.command_id,c.command_sha256,c.state,c.claim_authority,t.task_id,t.scenario_id,"
                "t.target_run_id,t.target_attempt_id,t.target_master_instance_id,t.target_epoch "
                "FROM master_acceptance_commands c JOIN master_acceptance_tasks t ON t.task_id=c.task_id "
                "WHERE t.task_id=?",
                (task_id,),
            ).fetchone()
            if (
                command is None
                or command["command_id"] != command_id
                or command["command_sha256"] != command_sha256
                or command["state"] != "CLAIMED"
                or command["claim_authority"] != "owner_host"
                or command["scenario_id"] != scenario_id
                or command["target_run_id"] != run_id
                or command["target_attempt_id"] != attempt_id
                or command["target_master_instance_id"] != master_instance_id
                or int(command["target_epoch"]) != epoch
            ):
                raise StaleRuntimeEvent("acceptance runtime control differs from the owner-host claim")
            existing = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=?", (task_id,)
            ).fetchone()
            exact = {
                "task_id": task_id,
                "command_id": command_id,
                "command_sha256": command_sha256,
                "scenario_id": scenario_id,
                "run_id": run_id,
                "attempt_id": attempt_id,
                "master_instance_id": master_instance_id,
                "epoch": epoch,
            }
            if existing is None:
                connection.execute(
                    "INSERT INTO master_acceptance_runtime_controls(task_id,command_id,command_sha256,"
                    "scenario_id,run_id,attempt_id,master_instance_id,epoch,callback_state,renewal_suspended,"
                    "armed_at,expires_at,before_boot_id,directive_receipt_sha256,updated_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        task_id,
                        command_id,
                        command_sha256,
                        scenario_id,
                        run_id,
                        attempt_id,
                        master_instance_id,
                        epoch,
                        callback_state,
                        int(renewal_suspended),
                        armed_at,
                        expires_at,
                        before_boot_id,
                        directive_receipt_sha256,
                        now,
                    ),
                )
            else:
                if any(existing[key] != value for key, value in exact.items()):
                    raise StaleRuntimeEvent("acceptance runtime control identity was reused")
                if callback_state == "ARMED" and existing["callback_state"] == "DISARMED":
                    connection.execute(
                        "UPDATE master_acceptance_runtime_controls SET callback_state='ARMED',armed_at=?,"
                        "expires_at=?,before_boot_id=?,directive_receipt_sha256=?,updated_at=? "
                        "WHERE task_id=? AND callback_state='DISARMED'",
                        (armed_at, expires_at, before_boot_id, directive_receipt_sha256, now, task_id),
                    )
                if renewal_suspended and not bool(existing["renewal_suspended"]):
                    connection.execute(
                        "UPDATE master_acceptance_runtime_controls SET renewal_suspended=1,updated_at=? "
                        "WHERE task_id=?",
                        (now, task_id),
                    )
            row = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=?", (task_id,)
            ).fetchone()
            assert row is not None
            return dict(row)

    def master_acceptance_runtime_directive(
        self,
        *,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> dict[str, Any]:
        """Return only fixed booleans/counters for the exact runtime."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT task_id,command_id,command_sha256,scenario_id,renewal_suspended,"
                "soak_requested_step,soak_completed_step FROM master_acceptance_runtime_controls "
                "WHERE run_id=? AND attempt_id=? AND master_instance_id=? AND epoch=?",
                (run_id, attempt_id, master_instance_id, epoch),
            ).fetchone()
        return (
            {"available": False, "renewal_suspended": False, "soak_requested_step": 0, "soak_completed_step": 0}
            if row is None
            else {"available": True, **dict(row)}
        )

    def acknowledge_master_acceptance_renewal_suspension(
        self,
        *,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> None:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE master_acceptance_runtime_controls SET renewal_acknowledged=1,updated_at=? "
                "WHERE run_id=? AND attempt_id=? AND master_instance_id=? AND epoch=? "
                "AND scenario_id='FM10' AND renewal_suspended=1",
                (now, run_id, attempt_id, master_instance_id, epoch),
            ).rowcount
            if changed != 1:
                raise StaleRuntimeEvent("FM10 renewal suspension acknowledgement is stale")

    def armed_master_acceptance_callback_loss(
        self,
        *,
        run_id: str,
        attempt_id: str,
        epoch: int,
    ) -> dict[str, Any] | None:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE run_id=? AND attempt_id=? "
                "AND epoch=? AND scenario_id='FM08' AND callback_state IN ('ARMED','CAPTURED') "
                "AND restart_to_id IS NULL",
                (run_id, attempt_id, epoch),
            ).fetchone()
            if row is not None and (row["expires_at"] is None or str(row["expires_at"]) <= now):
                connection.execute(
                    "UPDATE master_acceptance_runtime_controls SET callback_state='DISARMED',"
                    "armed_at=NULL,expires_at=NULL,before_boot_id=NULL,directive_receipt_sha256=NULL,"
                    "callback_event_id=NULL,callback_body_sha256=NULL,callback_count=0,updated_at=? "
                    "WHERE task_id=? AND callback_state IN ('ARMED','CAPTURED')",
                    (now, row["task_id"]),
                )
                return None
        return dict(row) if row is not None else None

    def restarted_master_acceptance_callback(
        self,
        *,
        run_id: str,
        attempt_id: str,
        epoch: int,
        event_id: str,
        body_sha256: str,
    ) -> dict[str, Any] | None:
        """Find one exact captured callback after its process restart is durable."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE run_id=? AND attempt_id=? "
                "AND epoch=? AND scenario_id='FM08' AND callback_state='CAPTURED' "
                "AND restart_to_id IS NOT NULL AND callback_event_id=? AND callback_body_sha256=?",
                (run_id, attempt_id, epoch, event_id, body_sha256),
            ).fetchone()
        return dict(row) if row is not None else None

    def capture_master_acceptance_callback(
        self,
        *,
        task_id: str,
        event_id: str,
        body_sha256: str,
    ) -> dict[str, Any]:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "UPDATE master_acceptance_runtime_controls SET callback_state='CAPTURED',"
                "callback_event_id=?,callback_body_sha256=?,callback_count=1,updated_at=? "
                "WHERE task_id=? AND scenario_id='FM08' AND callback_state='ARMED' "
                "AND callback_count=0 AND expires_at>?",
                (event_id, body_sha256, now, task_id, now),
            )
            row = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=?", (task_id,)
            ).fetchone()
            if (
                row is None
                or row["callback_state"] != "CAPTURED"
                or row["callback_event_id"] != event_id
                or row["callback_body_sha256"] != body_sha256
            ):
                raise StaleRuntimeEvent("FM08 callback capture lost its exact CAS")
            return dict(row)

    def master_acceptance_runtime_control(self, task_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=?", (task_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def record_master_acceptance_restart(
        self,
        *,
        task_id: str,
        restart_from_id: str,
        restart_to_id: str | None,
    ) -> dict[str, Any]:
        try:
            UUID(restart_from_id)
            if restart_to_id is not None:
                UUID(restart_to_id)
        except ValueError as exc:
            raise ValueError("control process invocation identity is invalid") from exc
        if restart_to_id == restart_from_id:
            raise ValueError("control process restart must change invocation identity")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=? AND scenario_id='FM08'",
                (task_id,),
            ).fetchone()
            if row is None or row["callback_state"] not in {"CAPTURED", "REPLAYED"}:
                raise StaleRuntimeEvent("FM08 restart is not bound to a captured callback")
            if row["before_boot_id"] != restart_from_id:
                raise StaleRuntimeEvent("FM08 restart origin differs from its armed process")
            if row["restart_from_id"] not in {None, restart_from_id}:
                raise StaleRuntimeEvent("FM08 restart origin changed")
            if row["restart_to_id"] not in {None, restart_to_id}:
                raise StaleRuntimeEvent("FM08 restart result changed")
            connection.execute(
                "UPDATE master_acceptance_runtime_controls SET restart_from_id=?,restart_to_id=?,updated_at=? "
                "WHERE task_id=?",
                (restart_from_id, restart_to_id, now, task_id),
            )
            updated = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=?", (task_id,)
            ).fetchone()
            assert updated is not None
            return dict(updated)

    def mark_master_acceptance_callback_replayed(
        self,
        *,
        task_id: str,
        event_id: str,
        body_sha256: str,
    ) -> None:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE master_acceptance_runtime_controls SET callback_state='REPLAYED',updated_at=? "
                "WHERE task_id=? AND callback_state='CAPTURED' AND callback_event_id=? "
                "AND callback_body_sha256=? AND restart_to_id IS NOT NULL",
                (now, task_id, event_id, body_sha256),
            ).rowcount
            if changed != 1:
                row = connection.execute(
                    "SELECT callback_state,callback_event_id,callback_body_sha256 "
                    "FROM master_acceptance_runtime_controls WHERE task_id=?",
                    (task_id,),
                ).fetchone()
                if (
                    row is None
                    or row["callback_state"] != "REPLAYED"
                    or row["callback_event_id"] != event_id
                    or row["callback_body_sha256"] != body_sha256
                ):
                    raise StaleRuntimeEvent("FM08 callback replay lost its exact CAS")

    def project_master_acceptance_callback_replay(
        self, *, task_id: str, event_id: str, body_sha256: str
    ) -> Literal["duplicate"]:
        """Acknowledge an already-authenticated captured body by durable identity.

        FM08 deliberately kills and fences the old runtime after its callback
        was ingested, so neither its raw Bearer nor body is reconstructable.
        This fixed internal replay verifies the immutable event row and the
        task-bound capture before projecting only the duplicate disposition.
        """

        try:
            UUID(task_id)
            UUID(event_id)
        except ValueError as exc:
            raise ValueError("FM08 callback replay requires exact UUID identities") from exc
        if len(body_sha256) != 64 or any(character not in "0123456789abcdef" for character in body_sha256):
            raise ValueError("FM08 callback replay body hash is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            control = connection.execute(
                "SELECT * FROM master_acceptance_runtime_controls WHERE task_id=? AND scenario_id='FM08'",
                (task_id,),
            ).fetchone()
            event = connection.execute(
                "SELECT event_id,run_id,attempt_id,epoch,body_sha256 FROM runtime_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            if (
                control is None
                or event is None
                or control["callback_state"] not in {"CAPTURED", "REPLAYED"}
                or control["restart_to_id"] is None
                or control["callback_event_id"] != event_id
                or control["callback_body_sha256"] != body_sha256
                or event["body_sha256"] != body_sha256
                or event["run_id"] != control["run_id"]
                or event["attempt_id"] != control["attempt_id"]
                or int(event["epoch"]) != int(control["epoch"])
            ):
                raise StaleRuntimeEvent("FM08 stored callback replay binding is invalid")
            if control["callback_state"] == "CAPTURED":
                changed = connection.execute(
                    "UPDATE master_acceptance_runtime_controls SET callback_state='REPLAYED',updated_at=? "
                    "WHERE task_id=? AND callback_state='CAPTURED' AND callback_event_id=? "
                    "AND callback_body_sha256=? AND restart_to_id IS NOT NULL",
                    (now, task_id, event_id, body_sha256),
                ).rowcount
                if changed != 1:
                    raise StaleRuntimeEvent("FM08 stored callback replay lost its exact CAS")
        return "duplicate"

    def master_acceptance_drain_directive(self, *, run_id: str, attempt_id: str, epoch: int) -> dict[str, Any] | None:
        """Return only the fixed FM11/FM12 drain directive for an owner claim."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT c.command_id,c.command_sha256,t.task_id,t.scenario_id FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id "
                "WHERE t.target_run_id=? AND t.target_attempt_id=? AND t.target_epoch=? "
                "AND t.scenario_id IN ('FM11','FM12') AND t.state='CLAIMED' "
                "AND c.state='CLAIMED' AND c.claim_authority='owner_host'",
                (run_id, attempt_id, epoch),
            ).fetchone()
        return dict(row) if row else None

    def complete_master_acceptance_command(
        self,
        *,
        command_id: str,
        command_sha256: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        state: str,
        receipt: Mapping[str, Any],
    ) -> dict[str, Any]:
        if state not in {"SUCCEEDED", "FAILED"}:
            raise ValueError("master acceptance terminal state is invalid")
        receipt_json = _safe_json(receipt, max_bytes=64 * 1024)
        receipt_sha256 = hashlib.sha256(receipt_json.encode()).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT c.*,t.deadline_at FROM master_acceptance_commands c "
                "JOIN master_acceptance_tasks t ON t.task_id=c.task_id WHERE c.command_id=?",
                (command_id,),
            ).fetchone()
            if row is None:
                raise StaleRuntimeEvent("acceptance command is unknown")
            if _parse_time(str(row["deadline_at"])) <= self.clock.now():
                raise StaleRuntimeEvent("acceptance runtime receipt arrived after its absolute deadline")
            if (
                row["command_sha256"] != command_sha256
                or row["claim_authority"] != "runtime"
                or row["claimed_run_id"] != run_id
                or row["claimed_attempt_id"] != attempt_id
                or row["claimed_epoch"] != epoch
            ):
                raise StaleRuntimeEvent("acceptance receipt does not bind its exact command/runtime")
            if row["state"] in {"SUCCEEDED", "FAILED"}:
                if row["state"] != state or not hmac.compare_digest(str(row["receipt_sha256"]), receipt_sha256):
                    raise IdempotencyConflict("acceptance command already has another terminal receipt")
            elif row["state"] != "CLAIMED":
                raise StaleRuntimeEvent("acceptance command was not claimed")
            else:
                connection.execute(
                    "UPDATE master_acceptance_commands SET state=?,receipt_json=?,receipt_sha256=?,completed_at=? "
                    "WHERE command_id=? AND state='CLAIMED'",
                    (state, receipt_json, receipt_sha256, now, command_id),
                )
                connection.execute(
                    "UPDATE master_acceptance_tasks SET state=?,failure_code=?,updated_at=? WHERE task_id=?",
                    (
                        "PASSED" if state == "SUCCEEDED" else "FAILED",
                        None if state == "SUCCEEDED" else "RUNTIME_SCENARIO_FAILED",
                        now,
                        row["task_id"],
                    ),
                )
                self._append_master_acceptance_event(
                    connection,
                    task_id=str(row["task_id"]),
                    event_type=state,
                    evidence={"receipt_sha256": receipt_sha256},
                    now=now,
                )
            task = connection.execute(
                "SELECT * FROM master_acceptance_tasks WHERE task_id=?", (row["task_id"],)
            ).fetchone()
            assert task is not None
            return self._master_acceptance_task_from_connection(connection, task)

    def master_acceptance_task(self, task_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM master_acceptance_tasks WHERE task_id=?", (task_id,)).fetchone()
            return self._master_acceptance_task_from_connection(connection, row) if row else None

    def _active_master_binding(self, connection: sqlite3.Connection, operation_id: str) -> dict[str, Any]:
        operation = connection.execute(
            "SELECT * FROM operations WHERE operation_id=? AND operation_kind='ensure_master' AND state='ACTIVE'",
            (operation_id,),
        ).fetchone()
        if operation is None:
            raise StaleRuntimeEvent("master acceptance target is not an ACTIVE master operation")
        identity = json.loads(str(operation["identity_json"]))
        service = connection.execute(
            "SELECT * FROM services WHERE service_kind='postgres-master' AND state='ACTIVE' AND "
            "run_id=? AND attempt_id=? AND service_instance_id=? AND master_instance_id=? AND epoch=?",
            (
                identity.get("run_id"),
                identity.get("attempt_id"),
                identity.get("service_instance_id"),
                identity.get("master_instance_id"),
                identity.get("epoch"),
            ),
        ).fetchone()
        current_epoch = connection.execute(
            "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
        ).fetchone()
        if service is None or current_epoch is None or int(current_epoch[0]) != int(identity.get("epoch", 0)):
            raise StaleRuntimeEvent("master acceptance target differs from current ACTIVE service")
        return {
            "operation_id": operation_id,
            "run_id": str(identity["run_id"]),
            "attempt_id": str(identity["attempt_id"]),
            "service_instance_id": str(identity["service_instance_id"]),
            "master_instance_id": str(identity["master_instance_id"]),
            "epoch": int(identity["epoch"]),
        }

    def _insert_master_acceptance_command(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        scenario_id: str,
        source_revision: str,
        binding: Mapping[str, Any],
        now: str,
    ) -> None:
        from my_data_hub.acceptance.master_lifecycle import (
            MasterAcceptanceBinding,
            MasterAcceptanceRequest,
            MasterAcceptanceScenario,
            command_for,
        )

        request = MasterAcceptanceRequest(
            task_id=task_id,
            scenario=MasterAcceptanceScenario(scenario_id),
            idempotency_key="internal-binding-only",
            source_revision=source_revision,
            target_operation_id=None if scenario_id in {"FM04", "FM07"} else binding["operation_id"],
        )
        command = command_for(request, MasterAcceptanceBinding.model_validate(binding))
        command_json = _safe_json(command.model_dump(mode="json"))
        command_sha256 = hashlib.sha256(command_json.encode()).hexdigest()
        connection.execute(
            "INSERT INTO master_acceptance_commands(command_id,task_id,scenario_id,command_kind,command_sha256,state) "
            "VALUES (?,?,?,?,?,'PENDING')",
            (str(command.command_id), task_id, scenario_id, command.command_kind.value, command_sha256),
        )
        self._append_master_acceptance_event(
            connection,
            task_id=task_id,
            event_type="BOUND",
            evidence={"command_sha256": command_sha256, "epoch": binding["epoch"]},
            now=now,
        )

    def _master_acceptance_command_from_row(self, row: sqlite3.Row, binding: Mapping[str, Any]) -> dict[str, Any]:
        from my_data_hub.acceptance.master_lifecycle import (
            MasterAcceptanceBinding,
            MasterAcceptanceRequest,
            MasterAcceptanceScenario,
            command_for,
        )

        request = MasterAcceptanceRequest(
            task_id=row["task_id"],
            scenario=MasterAcceptanceScenario(str(row["scenario_id"])),
            idempotency_key="internal-binding-only",
            source_revision=str(row["source_revision"]),
            target_operation_id=(None if row["scenario_id"] in {"FM04", "FM07"} else binding["operation_id"]),
        )
        command = command_for(request, MasterAcceptanceBinding.model_validate(binding))
        if command.command_sha256 != row["command_sha256"]:
            raise StaleRuntimeEvent("stored acceptance command hash is inconsistent")
        return command.model_dump(mode="json")

    def _master_acceptance_task_from_connection(
        self, connection: sqlite3.Connection, row: sqlite3.Row
    ) -> dict[str, Any]:
        value = dict(row)
        command = connection.execute(
            "SELECT command_id,command_kind,command_sha256,state,claim_authority,receipt_json,receipt_sha256,"
            "claimed_at,completed_at "
            "FROM master_acceptance_commands WHERE task_id=?",
            (row["task_id"],),
        ).fetchone()
        events = connection.execute(
            "SELECT sequence,event_type,evidence_json,evidence_sha256,recorded_at "
            "FROM master_acceptance_events WHERE task_id=? ORDER BY sequence LIMIT 100",
            (row["task_id"],),
        ).fetchall()
        if command is not None:
            command_value = dict(command)
            raw_receipt = command_value.pop("receipt_json")
            if raw_receipt is not None:
                from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceReceipt

                receipt = MasterAcceptanceReceipt.model_validate_json(str(raw_receipt))
                command_value["receipt"] = receipt.model_dump(mode="json")
            else:
                command_value["receipt"] = None
            value["command"] = command_value
        else:
            value["command"] = None
        value["operation_id"] = value.get("target_operation_id")
        value["provider_carrier"] = None
        operation_id = value.get("target_operation_id")
        if operation_id is not None:
            launch = connection.execute(
                "SELECT receipt_json FROM effects WHERE operation_id=? AND effect_kind='trigger_run' "
                "AND state='APPLIED' AND receipt_json IS NOT NULL ORDER BY planned_at LIMIT 1",
                (operation_id,),
            ).fetchone()
            if launch is not None:
                from my_data_hub.providers.kaggle.contracts import KaggleKernelRunIdentity

                launch_receipt = json.loads(str(launch["receipt_json"]))
                run_identity = launch_receipt.get("exact_identity")
                if isinstance(run_identity, dict):
                    run = KaggleKernelRunIdentity.model_validate(run_identity)
                    terminal = connection.execute(
                        "SELECT metadata_json FROM audit_log WHERE action='master.terminal_recovery' "
                        "AND operation_id=? ORDER BY recorded_at DESC LIMIT 1",
                        (operation_id,),
                    ).fetchone()
                    output: dict[str, Any] = {
                        "output_file_name": None,
                        "output_file_sha256": None,
                        "output_tree_sha256": None,
                        "output_receipt_sha256": None,
                    }
                    if terminal is not None:
                        terminal_metadata = json.loads(str(terminal["metadata_json"]))
                        if (
                            terminal_metadata.get("run_id") != str(run.task_run_id)
                            or terminal_metadata.get("output_receipt_sha256") is None
                        ):
                            raise StaleRuntimeEvent("terminal carrier differs from the exact provider run")
                        output = {
                            "output_file_name": "my-data-hub-master-terminal.json",
                            "output_file_sha256": terminal_metadata["output_receipt_sha256"],
                            "output_tree_sha256": terminal_metadata["output_tree_sha256"],
                            "output_receipt_sha256": terminal_metadata["output_receipt_sha256"],
                        }
                    from my_data_hub.acceptance.master_lifecycle import MasterProviderCarrierObservation

                    value["provider_carrier"] = MasterProviderCarrierObservation(
                        provider_ref=run.provider_ref,
                        provider_run_ref=run.provider_run_ref,
                        provider_kernel_id=run.provider_kernel_id,
                        source_version=run.source_version,
                        source_sha256=run.source_sha256,
                        **output,
                    ).model_dump(mode="json")
        value["events"] = [
            {
                **{key: event[key] for key in ("sequence", "event_type", "evidence_sha256", "recorded_at")},
                "evidence": json.loads(str(event["evidence_json"])),
            }
            for event in events
        ]
        value["bounded"] = True
        return value

    def _append_master_acceptance_event(
        self,
        connection: sqlite3.Connection,
        *,
        task_id: str,
        event_type: str,
        evidence: Mapping[str, Any],
        now: str,
    ) -> None:
        evidence_json = _safe_json(evidence)
        connection.execute(
            "INSERT OR IGNORE INTO master_acceptance_events(task_id,event_type,evidence_json,evidence_sha256,"
            "recorded_at) VALUES (?,?,?,?,?)",
            (task_id, event_type, evidence_json, hashlib.sha256(evidence_json.encode()).hexdigest(), now),
        )

    def ensure_checkpoint_acceptance_launch(
        self,
        *,
        request: Mapping[str, Any],
        request_sha256: str,
        principal_id: str,
        client_id: str,
        token_sha256: str,
        expires_at: datetime,
        config: Mapping[str, Any],
        config_sha256: str,
        expected_source_sha256: str,
    ) -> tuple[dict[str, Any], bool]:
        """Persist one owner-task checkpoint launch before provider mutation."""

        required = {
            "request_id",
            "scenario",
            "operation_id",
            "task_run_id",
            "idempotency_key",
            "control_identity",
        }
        if not required.issubset(request) or request["scenario"] not in {"FM05", "FM14", "FM15"}:
            raise ValueError("checkpoint acceptance launch request is incomplete")
        identity = request["control_identity"]
        if not isinstance(identity, Mapping):
            raise ValueError("checkpoint acceptance control identity is invalid")
        values = {
            "request_id": str(UUID(str(request["request_id"]))),
            "operation_id": str(UUID(str(request["operation_id"]))),
            "task_run_id": str(UUID(str(request["task_run_id"]))),
            "attempt_id": str(UUID(str(identity["attempt_id"]))),
        }
        if (
            values["request_id"] != values["task_run_id"]
            or str(identity.get("request_id")) != values["request_id"]
            or str(identity.get("task_run_id")) != values["task_run_id"]
            or identity.get("scope") != "acceptance:operate"
            or any(
                len(value) != 64 or any(c not in "0123456789abcdef" for c in value)
                for value in (request_sha256, token_sha256, config_sha256, expected_source_sha256)
            )
            or expires_at.tzinfo is None
        ):
            raise ValueError("checkpoint acceptance launch binding is invalid")
        request_json = _safe_json(request, max_bytes=64 * 1024)
        config_json = _safe_json(config, max_bytes=64 * 1024)
        now = _format_time(self.clock.now())
        expiry = _format_time(expires_at)
        creator_claim_until = expiry
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=? OR idempotency_key=?",
                (values["request_id"], str(request["idempotency_key"])),
            ).fetchone()
            if existing is not None:
                row = dict(existing)
                exact = (
                    row["request_id"] == values["request_id"]
                    and row["scenario_id"] == request["scenario"]
                    and row["operation_id"] == values["operation_id"]
                    and row["task_run_id"] == values["task_run_id"]
                    and row["attempt_id"] == values["attempt_id"]
                    and row["request_sha256"] == request_sha256
                    and row["request_json"] == request_json
                    and row["principal_id"] == principal_id
                    and row["client_id"] == client_id
                )
                if not exact:
                    raise IdempotencyConflict("checkpoint acceptance launch identity changed")
                return self._checkpoint_acceptance_launch_from_row(existing), False
            connection.execute(
                "INSERT INTO checkpoint_acceptance_launches("
                "request_id,scenario_id,operation_id,task_run_id,attempt_id,idempotency_key,"
                "request_sha256,request_json,principal_id,client_id,token_sha256,expires_at,state,"
                "creator_claim_until,config_sha256,config_json,expected_source_sha256,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    values["request_id"],
                    request["scenario"],
                    values["operation_id"],
                    values["task_run_id"],
                    values["attempt_id"],
                    request["idempotency_key"],
                    request_sha256,
                    request_json,
                    principal_id,
                    client_id,
                    token_sha256,
                    expiry,
                    "REQUESTED",
                    creator_claim_until,
                    config_sha256,
                    config_json,
                    expected_source_sha256,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?",
                (values["request_id"],),
            ).fetchone()
            assert row is not None
            return self._checkpoint_acceptance_launch_from_row(row), True

    def checkpoint_acceptance_launch(self, request_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?",
                (str(UUID(request_id)),),
            ).fetchone()
        return self._checkpoint_acceptance_launch_from_row(row) if row is not None else None

    def attest_checkpoint_acceptance_source(
        self, *, request_id: str, attempt_id: str, observed_source_sha256: str
    ) -> dict[str, Any]:
        if len(observed_source_sha256) != 64 or any(
            value not in "0123456789abcdef" for value in observed_source_sha256
        ):
            raise ValueError("checkpoint acceptance source SHA-256 is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=? AND attempt_id=?",
                (request_id, attempt_id),
            ).fetchone()
            if row is None or row["state"] not in {"REQUESTED", "RUNNING"}:
                raise StaleRuntimeEvent("checkpoint acceptance source authority is stale")
            state = (
                "MATCHED"
                if hmac.compare_digest(str(row["expected_source_sha256"]), observed_source_sha256)
                else "MISMATCH"
            )
            if row["observed_source_sha256"] is not None and (
                row["observed_source_sha256"] != observed_source_sha256 or row["source_attestation_state"] != state
            ):
                raise IdempotencyConflict("checkpoint acceptance source attestation changed")
            connection.execute(
                "UPDATE checkpoint_acceptance_launches SET observed_source_sha256=?,"
                "source_attestation_state=?,updated_at=? WHERE request_id=?",
                (observed_source_sha256, state, now, request_id),
            )
            current = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            assert current is not None
            return self._checkpoint_acceptance_launch_from_row(current)

    def authenticate_checkpoint_acceptance(
        self, *, request_id: str, attempt_id: str, token: str
    ) -> dict[str, Any] | None:
        digest = hashlib.sha256(token.encode()).hexdigest()
        now = self.clock.now()
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=? AND attempt_id=?",
                (str(UUID(request_id)), str(UUID(attempt_id))),
            ).fetchone()
        if row is None or not hmac.compare_digest(str(row["token_sha256"]), digest):
            return None
        if _parse_time(str(row["expires_at"])) <= now or row["state"] not in {"REQUESTED", "RUNNING"}:
            return None
        return self._checkpoint_acceptance_launch_from_row(row)

    def record_checkpoint_acceptance_event(
        self,
        *,
        request_id: str,
        attempt_id: str,
        event: Mapping[str, Any],
    ) -> dict[str, Any]:
        """Append one exact donor-style event under the owner-task authority."""

        allowed = {
            "runtime.started",
            "runtime.progress",
            "runtime.heartbeat",
            "runtime.failed",
            "runtime.terminal",
            "resource.acquire",
            "resource.release",
            "job.result_available",
        }
        data = event.get("data")
        if not isinstance(data, Mapping):
            raise ValueError("checkpoint acceptance event data is invalid")
        event_uid = data.get("donor_event_uid")
        progress = data.get("progress", {})
        event_type = str(event.get("event_type", ""))
        phase = event.get("phase")
        status = event.get("status")
        local_sequence = event.get("local_sequence")
        if (
            str(event.get("run_id")) != str(UUID(request_id))
            or str(event.get("attempt_id")) != str(UUID(attempt_id))
            or str(event.get("service_instance_id")) != str(UUID(request_id))
            or event_type not in allowed
            or not isinstance(event_uid, str)
            or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:-]{7,199}", event_uid) is None
            or not isinstance(local_sequence, int)
            or isinstance(local_sequence, bool)
            or local_sequence < 1
            or (phase is not None and (not isinstance(phase, str) or not 1 <= len(phase) <= 100))
            or (status is not None and (not isinstance(status, str) or not 1 <= len(status) <= 100))
            or not isinstance(progress, Mapping)
        ):
            raise ValueError("checkpoint acceptance event binding is invalid")
        body_json = _safe_json(event, max_bytes=64 * 1024)
        progress_json = _safe_json(progress, max_bytes=8 * 1024)
        body_sha256 = hashlib.sha256(body_json.encode()).hexdigest()
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            launch = connection.execute(
                "SELECT attempt_id,state FROM checkpoint_acceptance_launches WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if launch is None or launch["attempt_id"] != attempt_id or launch["state"] not in {"REQUESTED", "RUNNING"}:
                raise StaleRuntimeEvent("checkpoint acceptance event authority is terminal or stale")
            existing = connection.execute(
                "SELECT body_sha256,body_json FROM checkpoint_acceptance_events WHERE request_id=? AND event_uid=?",
                (request_id, event_uid),
            ).fetchone()
            if existing is not None:
                if existing["body_sha256"] != body_sha256 or existing["body_json"] != body_json:
                    raise IdempotencyConflict("checkpoint acceptance event_uid changed body")
                return {"event_uid": event_uid, "body_sha256": body_sha256, "duplicate": True}
            try:
                connection.execute(
                    "INSERT INTO checkpoint_acceptance_events(request_id,attempt_id,event_uid,event_type,"
                    "phase,status,progress_json,body_sha256,body_json,local_sequence,received_at) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
                    (
                        request_id,
                        attempt_id,
                        event_uid,
                        event_type,
                        phase,
                        status,
                        progress_json,
                        body_sha256,
                        body_json,
                        local_sequence,
                        now,
                    ),
                )
            except sqlite3.IntegrityError as exc:
                raise IdempotencyConflict("checkpoint acceptance local sequence changed event body") from exc
            connection.execute(
                "UPDATE checkpoint_acceptance_launches SET state='RUNNING',updated_at=? "
                "WHERE request_id=? AND state='REQUESTED'",
                (now, request_id),
            )
        return {"event_uid": event_uid, "body_sha256": body_sha256, "duplicate": False}

    def checkpoint_acceptance_event_observation(self, request_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT event_uid,event_type,phase,status,progress_json,body_sha256,local_sequence "
                "FROM checkpoint_acceptance_events WHERE request_id=? ORDER BY local_sequence",
                (str(UUID(request_id)),),
            ).fetchall()
        if not rows:
            return None
        counts: dict[str, int] = {}
        event_uids: list[str] = []
        receipt_sha256s: list[str] = []
        runtime_source_sha256: str | None = None
        for row in rows:
            counts[str(row["event_type"])] = counts.get(str(row["event_type"]), 0) + 1
            event_uids.append(str(row["event_uid"]))
            receipt_sha256s.append(str(row["body_sha256"]))
            if row["event_type"] == "runtime.started":
                progress = json.loads(str(row["progress_json"]))
                observed = progress.get("runtime_source_sha256")
                if isinstance(observed, str):
                    runtime_source_sha256 = observed
        latest = rows[-1]
        return {
            "latest_phase": latest["phase"],
            "latest_status": latest["status"],
            "latest_progress": json.loads(str(latest["progress_json"])),
            "event_counts": counts,
            "event_uids": event_uids,
            "event_receipt_sha256s": receipt_sha256s,
            "last_local_sequence": int(latest["local_sequence"]),
            "runtime_source_sha256": runtime_source_sha256,
        }

    def record_checkpoint_acceptance_provider_run(
        self, *, request_id: str, provider_run: Mapping[str, Any]
    ) -> dict[str, Any]:
        payload = _safe_json(provider_run, max_bytes=32 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                raise KeyError(request_id)
            if provider_run.get("source_sha256") != row["expected_source_sha256"]:
                raise StaleRuntimeEvent("checkpoint provider run source differs from push intent")
            if row["provider_run_json"] is not None and row["provider_run_json"] != payload:
                raise IdempotencyConflict("checkpoint acceptance provider run changed")
            connection.execute(
                "UPDATE checkpoint_acceptance_launches SET provider_run_json=?,state='RUNNING',updated_at=? "
                "WHERE request_id=? AND state IN ('REQUESTED','RUNNING')",
                (payload, now, request_id),
            )
            current = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            assert current is not None
            return self._checkpoint_acceptance_launch_from_row(current)

    def record_checkpoint_acceptance_status_dataset(
        self, *, request_id: str, status_dataset: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Bind the exact disposable status input before the Notebook push."""

        payload = _safe_json(status_dataset, max_bytes=32 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                raise KeyError(request_id)
            if row["status_dataset_json"] is not None and row["status_dataset_json"] != payload:
                raise IdempotencyConflict("checkpoint acceptance status Dataset changed")
            if row["provider_run_json"] is not None and row["status_dataset_json"] is None:
                raise StaleRuntimeEvent("checkpoint acceptance Notebook was launched without status authority")
            connection.execute(
                "UPDATE checkpoint_acceptance_launches SET status_dataset_json=?,updated_at=? "
                "WHERE request_id=? AND state='REQUESTED'",
                (payload, now, request_id),
            )
            current = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            assert current is not None
            return self._checkpoint_acceptance_launch_from_row(current)

    def record_checkpoint_acceptance_cleanup(
        self, *, request_id: str, cleanup_receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        """Persist exact absence proof for the task status Dataset."""

        payload = _safe_json(cleanup_receipt, max_bytes=32 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                raise KeyError(request_id)
            if row["status_dataset_json"] is None:
                raise StaleRuntimeEvent("checkpoint acceptance has no status Dataset to clean")
            if row["cleanup_receipt_json"] is not None and row["cleanup_receipt_json"] != payload:
                raise IdempotencyConflict("checkpoint acceptance cleanup receipt changed")
            connection.execute(
                "UPDATE checkpoint_acceptance_launches SET cleanup_receipt_json=?,updated_at=? WHERE request_id=?",
                (payload, now, request_id),
            )
            current = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            assert current is not None
            return self._checkpoint_acceptance_launch_from_row(current)

    def complete_checkpoint_acceptance_launch(
        self,
        *,
        request_id: str,
        state: str,
        result: Mapping[str, Any],
        result_sha256: str,
    ) -> dict[str, Any]:
        if state not in {"LIVE_EVIDENCE_READY", "BLOCKED", "FAIL"}:
            raise ValueError("checkpoint acceptance terminal state is invalid")
        if len(result_sha256) != 64 or any(c not in "0123456789abcdef" for c in result_sha256):
            raise ValueError("checkpoint acceptance result hash is invalid")
        payload = _safe_json(result, max_bytes=256 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None:
                raise KeyError(request_id)
            if row["result_json"] is not None:
                if (row["state"], row["result_json"], row["result_sha256"]) != (state, payload, result_sha256):
                    raise IdempotencyConflict("checkpoint acceptance terminal result changed")
                return self._checkpoint_acceptance_launch_from_row(row)
            connection.execute(
                "UPDATE checkpoint_acceptance_launches SET state=?,result_json=?,result_sha256=?,updated_at=? "
                "WHERE request_id=? AND state IN ('REQUESTED','RUNNING')",
                (state, payload, result_sha256, now, request_id),
            )
            current = connection.execute(
                "SELECT * FROM checkpoint_acceptance_launches WHERE request_id=?", (request_id,)
            ).fetchone()
            assert current is not None
            if current["result_json"] is None:
                raise StaleRuntimeEvent("checkpoint acceptance launch was already terminal")
            return self._checkpoint_acceptance_launch_from_row(current)

    @staticmethod
    def _checkpoint_acceptance_launch_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["request"] = json.loads(str(value.pop("request_json")))
        value["config"] = json.loads(str(value.pop("config_json")))
        provider = value.pop("provider_run_json")
        status_dataset = value.pop("status_dataset_json")
        cleanup = value.pop("cleanup_receipt_json")
        result = value.pop("result_json")
        value["status_dataset"] = json.loads(str(status_dataset)) if status_dataset is not None else None
        value["provider_run"] = json.loads(str(provider)) if provider is not None else None
        value["cleanup_receipt"] = json.loads(str(cleanup)) if cleanup is not None else None
        value["result"] = json.loads(str(result)) if result is not None else None
        return value

    def runtime_event_history(
        self, *, run_id: str, attempt_id: str, epoch: int, limit: int = 100
    ) -> list[dict[str, Any]]:
        """Return event envelope metadata only, never sanitized event payloads."""

        try:
            UUID(run_id)
            UUID(attempt_id)
        except ValueError as exc:
            raise ValueError("runtime event history requires exact UUID identities") from exc
        if epoch < 1 or not 1 <= limit <= 200:
            raise ValueError("runtime event history identity or limit is invalid")
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT event_id,schema_version,run_id,attempt_id,service_instance_id,source_identity,"
                "source_version,epoch,event_type,emitted_at,received_at,local_sequence,body_sha256,body_bytes "
                "FROM runtime_events WHERE run_id=? AND attempt_id=? AND epoch=? "
                "ORDER BY local_sequence ASC LIMIT ?",
                (run_id, attempt_id, epoch, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def exact_stored_runtime_event_identity(self, *, run_id: str, attempt_id: str, epoch: int) -> dict[str, str] | None:
        """Select one immutable ACKed event without returning its stored body."""

        try:
            UUID(run_id)
            UUID(attempt_id)
        except ValueError as exc:
            raise ValueError("stored runtime replay requires exact UUID identities") from exc
        if epoch < 1:
            raise ValueError("stored runtime replay epoch is invalid")
        with self._reader() as connection:
            rows = connection.execute(
                "SELECT event_id,body_sha256,sanitized_json FROM runtime_events "
                "WHERE run_id=? AND attempt_id=? AND epoch=? ORDER BY local_sequence DESC LIMIT 20",
                (run_id, attempt_id, epoch),
            ).fetchall()
        for row in rows:
            canonical_sha256 = hashlib.sha256(str(row["sanitized_json"]).encode()).hexdigest()
            if hmac.compare_digest(canonical_sha256, str(row["body_sha256"])):
                return {
                    "event_id": str(row["event_id"]),
                    "body_sha256": str(row["body_sha256"]),
                }
        return None

    def replay_stored_runtime_event_identity(
        self,
        *,
        event_id: str,
        body_sha256: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
    ) -> Literal["duplicate"]:
        """Return the dedup result for one already-authenticated immutable row."""

        try:
            UUID(event_id)
            UUID(run_id)
            UUID(attempt_id)
        except ValueError as exc:
            raise ValueError("FM09 replay requires exact UUID identities") from exc
        with self._reader() as connection:
            event = connection.execute(
                "SELECT event_id,run_id,attempt_id,epoch,body_sha256 FROM runtime_events WHERE event_id=?",
                (event_id,),
            ).fetchone()
            dedup = connection.execute(
                "SELECT body_sha256 FROM runtime_event_dedup WHERE event_id=?", (event_id,)
            ).fetchone()
        if (
            event is None
            or dedup is None
            or event["run_id"] != run_id
            or event["attempt_id"] != attempt_id
            or int(event["epoch"]) != epoch
            or event["body_sha256"] != body_sha256
            or dedup["body_sha256"] != body_sha256
        ):
            raise StaleRuntimeEvent("FM09 exact stored replay identity changed")
        return "duplicate"

    def fm09_runtime_replay_denials(
        self,
        *,
        event_id: str,
        body_sha256: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
    ) -> dict[str, bool]:
        """Prove retired-token and stale-epoch rejection without raw bearers."""

        self.replay_stored_runtime_event_identity(
            event_id=event_id,
            body_sha256=body_sha256,
            run_id=run_id,
            attempt_id=attempt_id,
            epoch=epoch,
        )
        with self._reader() as connection:
            current = connection.execute(
                "SELECT token_sha256,revoked_at FROM runtime_token_hashes WHERE run_id=? AND attempt_id=?",
                (run_id, attempt_id),
            ).fetchone()
            retired = connection.execute(
                "SELECT token_sha256 FROM runtime_token_hashes WHERE revoked_at IS NOT NULL "
                "AND NOT (run_id=? AND attempt_id=?) ORDER BY revoked_at DESC LIMIT 1",
                (run_id, attempt_id),
            ).fetchone()
            attempt = connection.execute(
                "SELECT operation_id,epoch FROM run_attempts WHERE run_id=? AND attempt_id=?",
                (run_id, attempt_id),
            ).fetchone()
            operation = (
                connection.execute(
                    "SELECT state FROM operations WHERE operation_id=?", (attempt["operation_id"],)
                ).fetchone()
                if attempt is not None
                else None
            )
            current_epoch = connection.execute(
                "SELECT current_epoch FROM service_epochs WHERE service_kind='postgres-master'"
            ).fetchone()
        if (
            current is None
            or current["revoked_at"] is not None
            or retired is None
            or hmac.compare_digest(str(current["token_sha256"]), str(retired["token_sha256"]))
            or attempt is None
            or operation is None
            or operation["state"] != "ACTIVE"
            or int(attempt["epoch"]) != epoch
            or current_epoch is None
            or int(current_epoch["current_epoch"]) != epoch
        ):
            raise StaleRuntimeEvent("FM09 runtime authentication state is not exact/current")
        if epoch <= 1:
            raise StaleRuntimeEvent("FM09 has no older epoch to submit")
        return {"retired_runtime_auth_rejected": True, "stale_epoch_rejected": True}

    def latest_revoked_runtime_identity(self, *, exclude_run_id: str, exclude_attempt_id: str) -> dict[str, str] | None:
        """Return a retired token identity only; no token hash or secret leaves."""

        with self._reader() as connection:
            row = connection.execute(
                "SELECT run_id,attempt_id FROM runtime_token_hashes "
                "WHERE revoked_at IS NOT NULL AND NOT (run_id=? AND attempt_id=?) "
                "ORDER BY revoked_at DESC LIMIT 1",
                (exclude_run_id, exclude_attempt_id),
            ).fetchone()
        return dict(row) if row else None

    def provider_resource(self, provider_ref: str, source_version: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT provider,resource_ref,resource_kind,source_identity,source_version,control_class,"
                "private,state,metadata_json,observed_at FROM provider_resources "
                "WHERE resource_ref=? AND source_version=? ORDER BY observed_at DESC LIMIT 1",
                (provider_ref, source_version),
            ).fetchone()
        if row is None:
            return None
        value = dict(row)
        value["private"] = None if value["private"] is None else bool(value["private"])
        value["metadata"] = json.loads(str(value.pop("metadata_json")))
        return value

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

    def resource_lease(self, lease_id: str) -> ResourceLeaseRecord | None:
        with self._reader() as connection:
            row = connection.execute("SELECT * FROM resource_leases WHERE lease_id=?", (lease_id,)).fetchone()
        return self._resource_lease_from_row(row) if row is not None else None

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

    def release_resource_lease_exact(self, lease_id: str, holder_id: str, epoch: int) -> None:
        """Idempotently release one already-bound owner-task lease."""

        with self._transaction() as connection:
            row = connection.execute(
                "SELECT holder_id,epoch,released_at FROM resource_leases WHERE lease_id=?",
                (lease_id,),
            ).fetchone()
            if row is None or row["holder_id"] != holder_id or int(row["epoch"]) != epoch:
                raise LeaseRejected("resource lease release is stale or fenced")
            if row["released_at"] is None:
                connection.execute(
                    "UPDATE resource_leases SET released_at=? WHERE lease_id=? AND released_at IS NULL",
                    (_format_time(self.clock.now()), lease_id),
                )

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
            existing = connection.execute(
                "SELECT service_kind,operation_id,dataset_ref,manifest_sha256,source_checkpoint_id,"
                "source_head_generation,master_instance_id,epoch "
                "FROM checkpoint_candidates WHERE checkpoint_id=?",
                (checkpoint_id,),
            ).fetchone()
            if existing is not None:
                replay_generation = (
                    int(existing["source_head_generation"])
                    if source_head_generation is None
                    else source_head_generation
                )
                immutable = (
                    service_kind,
                    operation_id,
                    dataset_ref,
                    manifest_sha256,
                    source_checkpoint_id,
                    replay_generation,
                    master_instance_id,
                    epoch,
                )
                if tuple(existing) != immutable:
                    raise StaleRuntimeEvent("checkpoint idempotency identity conflicts with durable candidate")
                return
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
                raise StaleRuntimeEvent("checkpoint candidate creation lost its exact insert")

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

    def retire_failed_checkpoint_version(
        self,
        checkpoint_id: str,
        *,
        version_ref: str,
        deletion_effect_id: str,
        deletion_receipt_sha256: str,
    ) -> dict[str, Any]:
        """Release a deleted failed provider version without losing its audit identity.

        The caller must first persist an exact DELETE_DATASET intent and an
        APPLIED/ABSENT receipt after provider read-back.  Only a terminal
        failed candidate that is not either durable HEAD may be retired.
        """

        if not version_ref or len(version_ref) > 512:
            raise ValueError("retired checkpoint exact version ref is invalid")
        if len(deletion_receipt_sha256) != 64 or any(
            character not in "0123456789abcdef" for character in deletion_receipt_sha256
        ):
            raise ValueError("checkpoint deletion receipt hash is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM retired_checkpoint_versions WHERE checkpoint_id=?",
                (checkpoint_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["version_ref"] != version_ref
                    or existing["deletion_effect_id"] != deletion_effect_id
                    or existing["deletion_receipt_sha256"] != deletion_receipt_sha256
                ):
                    raise StaleRuntimeEvent("retired checkpoint version replay differs")
                return dict(existing)

            candidate = connection.execute(
                "SELECT dataset_ref,version_ref,status FROM checkpoint_candidates WHERE checkpoint_id=?",
                (checkpoint_id,),
            ).fetchone()
            publication = connection.execute(
                "SELECT state,exact_version_ref FROM checkpoint_blob_publications WHERE checkpoint_id=?",
                (checkpoint_id,),
            ).fetchone()
            head = connection.execute(
                "SELECT current_checkpoint_id,previous_checkpoint_id FROM checkpoint_heads "
                "WHERE current_checkpoint_id=? OR previous_checkpoint_id=? LIMIT 1",
                (checkpoint_id, checkpoint_id),
            ).fetchone()
            authority = connection.execute(
                "SELECT i.action,i.provider_ref,r.receipt_json,r.receipt_sha256 "
                "FROM provider_effect_intents i JOIN provider_effect_receipts r ON r.effect_id=i.effect_id "
                "WHERE i.effect_id=? AND r.receipt_sha256=? ORDER BY r.sequence DESC LIMIT 1",
                (deletion_effect_id, deletion_receipt_sha256),
            ).fetchone()
            if candidate is None or candidate["status"] != "FAILED" or head is not None:
                raise StaleRuntimeEvent("only a non-HEAD failed checkpoint version may be retired")
            exact_ref = candidate["version_ref"] or (
                publication["exact_version_ref"] if publication is not None else None
            )
            if (
                exact_ref != version_ref
                or publication is None
                or publication["state"] not in {"FAILED", "QUARANTINED"}
            ):
                raise StaleRuntimeEvent("failed checkpoint retirement version binding differs")
            if authority is None or authority["action"] != "delete_dataset":
                raise StaleRuntimeEvent("checkpoint retirement lacks its exact deletion receipt")
            if authority["provider_ref"] != candidate["dataset_ref"]:
                raise StaleRuntimeEvent("checkpoint deletion receipt targets another Dataset")
            receipt = json.loads(str(authority["receipt_json"]))
            if (
                receipt.get("action") != "delete_dataset"
                or receipt.get("outcome") not in {"applied", "already_applied", "not_found"}
                or "absent" not in str(receipt.get("detail_code", ""))
            ):
                raise StaleRuntimeEvent("checkpoint deletion receipt is not terminal absence evidence")
            connection.execute(
                "INSERT INTO retired_checkpoint_versions(checkpoint_id,dataset_ref,version_ref,"
                "deletion_effect_id,deletion_receipt_sha256,retired_at) VALUES (?,?,?,?,?,?)",
                (
                    checkpoint_id,
                    candidate["dataset_ref"],
                    version_ref,
                    deletion_effect_id,
                    deletion_receipt_sha256,
                    now,
                ),
            )
            connection.execute(
                "UPDATE checkpoint_candidates SET version_ref=NULL WHERE checkpoint_id=? AND status='FAILED'",
                (checkpoint_id,),
            )
            row = connection.execute(
                "SELECT * FROM retired_checkpoint_versions WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            assert row is not None
            return dict(row)

    def checkpoint_dataset_incarnation_retirement(
        self,
        replacement_checkpoint_id: str,
    ) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_dataset_incarnation_retirements "
                "WHERE replacement_checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def prepare_missing_checkpoint_dataset_incarnation(
        self,
        replacement_checkpoint_id: str,
        *,
        dataset_ref: str,
        source_head_checkpoint_id: str,
        source_head_generation: int,
    ) -> dict[str, Any]:
        """Persist an exact reset intent before recreating an absent Dataset.

        This is a disaster-recovery boundary for a provider Dataset that is
        independently absent while the ledger still has a durable HEAD.  The
        append-only intent retains every historical binding before the
        provider effect, so response-loss replay cannot confuse this recovery
        with the ordinary first checkpoint (which also starts at version 1).
        """

        if not dataset_ref or len(dataset_ref) > 300 or source_head_generation < 1:
            raise ValueError("checkpoint Dataset incarnation identity is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM checkpoint_dataset_incarnation_retirements "
                "WHERE replacement_checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            if existing is not None:
                if (
                    existing["dataset_ref"] != dataset_ref
                    or existing["source_head_checkpoint_id"] != source_head_checkpoint_id
                    or int(existing["source_head_generation"]) != source_head_generation
                ):
                    raise StaleRuntimeEvent("checkpoint Dataset incarnation retirement replay differs")
                return dict(existing)

            head = connection.execute(
                "SELECT generation,current_checkpoint_id FROM checkpoint_heads "
                "WHERE service_kind='postgres-master'",
            ).fetchone()
            replacement = connection.execute(
                "SELECT dataset_ref,source_checkpoint_id,source_head_generation,status "
                "FROM checkpoint_candidates WHERE checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            publication = connection.execute(
                "SELECT state,expected_provider_version FROM "
                "checkpoint_blob_publications WHERE checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            if (
                head is None
                or int(head["generation"]) != source_head_generation
                or head["current_checkpoint_id"] != source_head_checkpoint_id
                or replacement is None
                or replacement["dataset_ref"] != dataset_ref
                or replacement["source_checkpoint_id"] != source_head_checkpoint_id
                or int(replacement["source_head_generation"]) != source_head_generation
                or replacement["status"] != "CANDIDATE"
                or publication is None
                or publication["state"] not in {"READY_TO_FINALIZE", "FINALIZING"}
                or (
                    publication["expected_provider_version"] is not None
                    and int(publication["expected_provider_version"]) != 1
                )
            ):
                raise StaleRuntimeEvent("checkpoint Dataset reset intent is not exact")
            rows = connection.execute(
                "SELECT checkpoint_id,version_ref FROM checkpoint_candidates "
                "WHERE dataset_ref=? AND version_ref IS NOT NULL ORDER BY checkpoint_id",
                (dataset_ref,),
            ).fetchall()
            retired = [
                {"checkpoint_id": str(row["checkpoint_id"]), "version_ref": str(row["version_ref"])}
                for row in rows
            ]
            if not retired or not any(
                item["checkpoint_id"] == source_head_checkpoint_id for item in retired
            ):
                raise StaleRuntimeEvent("checkpoint Dataset retirement lacks the durable HEAD binding")
            retired_json = _safe_json(retired)
            retired_sha256 = hashlib.sha256(retired_json.encode()).hexdigest()
            connection.execute(
                "INSERT INTO checkpoint_dataset_incarnation_retirements("
                "replacement_checkpoint_id,dataset_ref,source_head_checkpoint_id,"
                "source_head_generation,retired_versions_json,retired_versions_sha256,"
                "observed_absent_at) VALUES (?,?,?,?,?,?,?)",
                (
                    replacement_checkpoint_id,
                    dataset_ref,
                    source_head_checkpoint_id,
                    source_head_generation,
                    retired_json,
                    retired_sha256,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM checkpoint_dataset_incarnation_retirements "
                "WHERE replacement_checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            assert row is not None
            return dict(row)

    def retire_missing_checkpoint_dataset_incarnation(
        self,
        replacement_checkpoint_id: str,
        *,
        dataset_ref: str,
        source_head_checkpoint_id: str,
        source_head_generation: int,
    ) -> dict[str, Any]:
        """Release old exact refs after the replacement resolves as version 1."""

        with self._transaction() as connection:
            retirement = connection.execute(
                "SELECT * FROM checkpoint_dataset_incarnation_retirements "
                "WHERE replacement_checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            head = connection.execute(
                "SELECT generation,current_checkpoint_id FROM checkpoint_heads "
                "WHERE service_kind='postgres-master'",
            ).fetchone()
            replacement = connection.execute(
                "SELECT dataset_ref,source_checkpoint_id,source_head_generation,status "
                "FROM checkpoint_candidates WHERE checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            publication = connection.execute(
                "SELECT state,exact_version_ref,expected_provider_version FROM "
                "checkpoint_blob_publications WHERE checkpoint_id=?",
                (replacement_checkpoint_id,),
            ).fetchone()
            if (
                retirement is None
                or retirement["dataset_ref"] != dataset_ref
                or retirement["source_head_checkpoint_id"] != source_head_checkpoint_id
                or int(retirement["source_head_generation"]) != source_head_generation
                or head is None
                or int(head["generation"]) != source_head_generation
                or head["current_checkpoint_id"] != source_head_checkpoint_id
                or replacement is None
                or replacement["dataset_ref"] != dataset_ref
                or replacement["source_checkpoint_id"] != source_head_checkpoint_id
                or int(replacement["source_head_generation"]) != source_head_generation
                or replacement["status"] != "CANDIDATE"
                or publication is None
                or publication["state"] != "DATASET_RESOLVED"
                or publication["exact_version_ref"] != f"{dataset_ref}/1"
                or int(publication["expected_provider_version"] or 0) != 1
            ):
                raise StaleRuntimeEvent("checkpoint Dataset replacement is not exact and resolved")
            connection.execute(
                "UPDATE checkpoint_candidates SET version_ref=NULL "
                "WHERE dataset_ref=? AND version_ref IS NOT NULL AND checkpoint_id<>?",
                (dataset_ref, replacement_checkpoint_id),
            )
            return dict(retirement)

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
                "FROM checkpoint_candidates WHERE checkpoint_id=?",
                (checkpoint_id,),
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
                operation_id,
                request_sha256,
                request_json,
            ):
                raise IdempotencyConflict("blogger request identity was reused for different metadata")
            return self._blogger_request_from_row(row), bool(changed)

    def admit_blogger_migration_request(
        self,
        *,
        request_id: str,
        operation_id: str,
        request_sha256: str,
        request: Mapping[str, Any],
        replay_source_request_id: str | None = None,
        replay_source_receipt_sha256: str | None = None,
    ) -> tuple[dict[str, Any], bool]:
        """CAS ACTIVE authority and insertion in one ``BEGIN IMMEDIATE``.

        An exact existing request is replayable after drain.  A distinct request
        can be inserted only while its operation, runtime, epoch and lease still
        describe the same ACTIVE master.  Replay additionally binds the immutable
        quarantine receipt observed by the owner.
        """

        if len(request_sha256) != 64:
            raise ValueError("blogger request hash must be an exact SHA-256")
        request_json = _safe_json(request)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if existing is not None:
                if (existing["operation_id"], existing["request_sha256"], existing["request_json"]) != (
                    operation_id,
                    request_sha256,
                    request_json,
                ):
                    raise IdempotencyConflict("blogger request identity was reused for different metadata")
                return self._blogger_request_from_row(existing), False
            authority = connection.execute(
                "SELECT o.operation_kind,o.state AS operation_state,o.identity_json,s.run_id,s.attempt_id,"
                "s.master_instance_id,s.epoch,s.state AS service_state,s.lease_until,e.current_epoch "
                "FROM operations o JOIN run_attempts r ON r.operation_id=o.operation_id "
                "JOIN services s ON s.run_id=r.run_id AND s.attempt_id=r.attempt_id "
                "JOIN service_epochs e ON e.service_kind=s.service_kind "
                "WHERE o.operation_id=? AND s.service_kind='postgres-master'",
                (operation_id,),
            ).fetchone()
            if authority is None:
                raise MasterAdmissionRejected("master operation invalid")
            identity = json.loads(authority["identity_json"])
            if (
                authority["operation_kind"] != "ensure_master"
                or authority["operation_state"] != "ACTIVE"
                or authority["service_state"] != "ACTIVE"
                or int(authority["epoch"]) != int(authority["current_epoch"])
                or authority["lease_until"] <= now
                or str(identity.get("run_id")) != authority["run_id"]
                or str(identity.get("attempt_id")) != authority["attempt_id"]
                or str(identity.get("master_instance_id")) != authority["master_instance_id"]
                or int(identity.get("epoch", 0)) != int(authority["epoch"])
            ):
                raise MasterAdmissionRejected("master not active at blogger admission CAS")
            if replay_source_request_id is not None:
                source = connection.execute(
                    "SELECT request_id,state,failure_code,quarantine_receipt_sha256 "
                    "FROM blogger_migration_requests WHERE request_id=?",
                    (replay_source_request_id,),
                ).fetchone()
                if (
                    source is None
                    or source["state"] != "FAILED"
                    or source["failure_code"] != "BloggerMigrationQuarantined"
                    or replay_source_receipt_sha256 is None
                    or source["quarantine_receipt_sha256"] != replay_source_receipt_sha256
                ):
                    raise MasterAdmissionRejected("blogger replay quarantine evidence changed")
            connection.execute(
                "INSERT INTO blogger_migration_requests(request_id,operation_id,request_sha256,request_json,state,"
                "created_at,updated_at) VALUES (?,?,?,?,'REQUESTED',?,?)",
                (request_id, operation_id, request_sha256, request_json, now, now),
            )
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            assert row is not None
            return self._blogger_request_from_row(row), True

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
                    "claimed_master_instance_id=?,claimed_epoch=?,updated_at=? WHERE request_id=? "
                    "AND state='REQUESTED'",
                    (run_id, attempt_id, master_instance_id, epoch, now, row["request_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM blogger_migration_requests WHERE request_id=?", (row["request_id"],)
                ).fetchone()
            assert row is not None
            claimed = (
                row["claimed_run_id"],
                row["claimed_attempt_id"],
                row["claimed_master_instance_id"],
                row["claimed_epoch"],
            )
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
                "UPDATE blogger_migration_requests SET state='CHECKPOINT_VERIFIED',"
                "checkpoint_receipt_json=?,updated_at=? "
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
            changed = connection.execute(
                "UPDATE blogger_migration_requests SET state='FAILED',failure_code=?,updated_at=? "
                "WHERE request_id=? AND claimed_run_id=? AND claimed_attempt_id=? "
                "AND state='CLAIMED' AND import_receipt_json IS NULL",
                (failure_code, _format_time(self.clock.now()), request_id, run_id, attempt_id),
            ).rowcount
            row = connection.execute(
                "SELECT state,claimed_run_id,claimed_attempt_id FROM blogger_migration_requests WHERE request_id=?",
                (request_id,),
            ).fetchone()
            if changed != 1 and (
                row is None
                or row["state"] != "FAILED"
                or row["claimed_run_id"] != run_id
                or row["claimed_attempt_id"] != attempt_id
            ):
                raise StaleRuntimeEvent("committed blogger import cannot be downgraded to failed")

    def record_blogger_quarantine_receipt(
        self,
        *,
        request_id: str,
        run_id: str,
        attempt_id: str,
        receipt: Mapping[str, Any],
        receipt_sha256: str,
    ) -> dict[str, Any]:
        """Persist the immutable durable-quarantine projection, exactly once."""

        if len(receipt_sha256) != 64:
            raise ValueError("blogger quarantine receipt hash must be an exact SHA-256")
        receipt_json = _safe_json(receipt, max_bytes=256 * 1024)
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE blogger_migration_requests SET state='FAILED',failure_code=?,"
                "quarantine_receipt_json=?,quarantine_receipt_sha256=?,updated_at=? "
                "WHERE request_id=? AND claimed_run_id=? AND claimed_attempt_id=? "
                "AND state='CLAIMED' AND import_receipt_json IS NULL "
                "AND quarantine_receipt_json IS NULL",
                (
                    "BloggerMigrationQuarantined",
                    receipt_json,
                    receipt_sha256,
                    now,
                    request_id,
                    run_id,
                    attempt_id,
                ),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (
                changed != 1
                and (
                    row["state"] != "FAILED"
                    or row["failure_code"] != "BloggerMigrationQuarantined"
                    or row["claimed_run_id"] != run_id
                    or row["claimed_attempt_id"] != attempt_id
                    or row["quarantine_receipt_json"] != receipt_json
                    or row["quarantine_receipt_sha256"] != receipt_sha256
                )
            ):
                raise StaleRuntimeEvent("blogger quarantine evidence differs from the durable receipt")
            return self._blogger_request_from_row(row)

    def reconcile_abandoned_blogger_migration_request(self, request_id: str) -> dict[str, Any] | None:
        """Terminalize a claim only after its exact master ended without import evidence."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT b.*,o.state AS operation_state FROM blogger_migration_requests b "
                "JOIN operations o ON o.operation_id=b.operation_id WHERE b.request_id=?",
                (request_id,),
            ).fetchone()
            if row is None:
                return None
            if row["state"] == "REQUESTED" and row["operation_state"] in {"STOPPED", "FAILED", "FENCED", "ORPHANED"}:
                connection.execute(
                    "UPDATE blogger_migration_requests SET state='FAILED',failure_code=?,updated_at=? "
                    "WHERE request_id=? AND state='REQUESTED'",
                    ("ADMISSION_RUNTIME_TERMINAL_BEFORE_CLAIM", now, request_id),
                )
            elif (
                row["state"] == "CLAIMED"
                and row["import_receipt_json"] is None
                and row["operation_state"] in {"FAILED", "FENCED", "ORPHANED"}
            ):
                connection.execute(
                    "UPDATE blogger_migration_requests SET state='FAILED',failure_code=?,updated_at=? "
                    "WHERE request_id=? AND state='CLAIMED' AND import_receipt_json IS NULL",
                    ("CLAIMED_RUNTIME_TERMINAL_WITHOUT_RECEIPT", now, request_id),
                )
            elif row["state"] == "IMPORT_COMMITTED" and row["operation_state"] in {
                "FAILED",
                "FENCED",
                "ORPHANED",
            }:
                verified = connection.execute(
                    "SELECT 1 FROM checkpoint_candidates c JOIN checkpoint_heads h "
                    "ON h.service_kind=c.service_kind AND h.current_checkpoint_id=c.checkpoint_id "
                    "WHERE c.operation_id=? AND c.status='VERIFIED' LIMIT 1",
                    (row["operation_id"],),
                ).fetchone()
                if verified is None:
                    connection.execute(
                        "UPDATE blogger_migration_requests SET state='FAILED',failure_code=?,updated_at=? "
                        "WHERE request_id=? AND state='IMPORT_COMMITTED' AND checkpoint_receipt_json IS NULL",
                        ("IMPORT_COMMITTED_WITHOUT_DURABLE_CHECKPOINT", now, request_id),
                    )
            current = connection.execute(
                "SELECT * FROM blogger_migration_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            return self._blogger_request_from_row(current) if current else None

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

    def ensure_connector_checkpoint_request(
        self,
        *,
        operation_id: str,
        idempotency_key: str,
        canonical_revision: int,
    ) -> dict[str, Any]:
        """Bind one connector durability request to the exact current ACTIVE master."""

        if (
            not operation_id.startswith("connector-checkpoint:")
            or len(idempotency_key) != 64
            or any(character not in "0123456789abcdef" for character in idempotency_key)
            or operation_id != f"connector-checkpoint:{idempotency_key}"
            or canonical_revision < 1
        ):
            raise ValueError("connector checkpoint request identity is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            service = connection.execute(
                "SELECT a.operation_id,s.master_instance_id,s.epoch FROM services s "
                "JOIN service_epochs e USING(service_kind) "
                "JOIN run_attempts a ON a.run_id=s.run_id AND a.attempt_id=s.attempt_id "
                "WHERE s.service_kind='postgres-master' AND s.state='ACTIVE' "
                "AND s.epoch=e.current_epoch AND s.lease_until>?",
                (now,),
            ).fetchone()
            if service is None:
                raise LeaseRejected("connector checkpoint request requires the exact ACTIVE master")
            expected = (
                idempotency_key,
                canonical_revision,
                str(service["operation_id"]),
                str(service["master_instance_id"]),
                int(service["epoch"]),
            )
            connection.execute(
                "INSERT INTO connector_checkpoint_requests(operation_id,idempotency_key,canonical_revision,"
                "master_operation_id,master_instance_id,epoch,state,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,'REQUESTED',?,?) ON CONFLICT DO NOTHING",
                (operation_id, *expected, now, now),
            )
            row = connection.execute(
                "SELECT * FROM connector_checkpoint_requests WHERE operation_id=? OR idempotency_key=?",
                (operation_id, idempotency_key),
            ).fetchone()
            keys = (
                "idempotency_key",
                "canonical_revision",
                "master_operation_id",
                "master_instance_id",
                "epoch",
            )
            if row is None or tuple(row[key] for key in keys) != expected:
                raise IdempotencyConflict("connector checkpoint request identity changed")
            return dict(row)

    def claim_connector_checkpoint_request(
        self,
        *,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> dict[str, Any] | None:
        """Return one exact drain request only to its task/epoch-bound runtime."""

        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            operation = connection.execute(
                "SELECT o.operation_id,o.state FROM operations o JOIN run_attempts a "
                "ON a.operation_id=o.operation_id WHERE a.run_id=? AND a.attempt_id=? "
                "AND json_extract(o.identity_json,'$.master_instance_id')=? "
                "AND CAST(json_extract(o.identity_json,'$.epoch') AS INTEGER)=?",
                (run_id, attempt_id, master_instance_id, epoch),
            ).fetchone()
            if operation is None or operation["state"] != "ACTIVE":
                raise LeaseRejected("connector checkpoint claim runtime is not ACTIVE")
            row = connection.execute(
                "SELECT * FROM connector_checkpoint_requests WHERE master_operation_id=? "
                "AND master_instance_id=? AND epoch=? AND state IN ('REQUESTED','CLAIMED') "
                "ORDER BY canonical_revision DESC,created_at LIMIT 1",
                (operation["operation_id"], master_instance_id, epoch),
            ).fetchone()
            if row is None:
                return None
            if row["state"] == "REQUESTED":
                connection.execute(
                    "UPDATE connector_checkpoint_requests SET state='CLAIMED',updated_at=? "
                    "WHERE operation_id=? AND state='REQUESTED'",
                    (now, row["operation_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM connector_checkpoint_requests WHERE operation_id=?",
                    (row["operation_id"],),
                ).fetchone()
            return dict(row) if row is not None else None

    def connector_checkpoint_request(self, operation_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM connector_checkpoint_requests WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
        return dict(row) if row is not None else None

    def complete_connector_checkpoint_request(
        self,
        operation_id: str,
        *,
        checkpoint_id: str,
        manifest_sha256: str,
        verified_at: str,
    ) -> dict[str, Any]:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "UPDATE connector_checkpoint_requests SET state='DURABLE_COMPLETE',checkpoint_id=?,"
                "manifest_sha256=?,verified_at=?,updated_at=? WHERE operation_id=? "
                "AND state IN ('REQUESTED','CLAIMED')",
                (checkpoint_id, manifest_sha256, verified_at, now, operation_id),
            )
            row = connection.execute(
                "SELECT * FROM connector_checkpoint_requests WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None or (
                row["state"] != "DURABLE_COMPLETE"
                or row["checkpoint_id"] != checkpoint_id
                or row["manifest_sha256"] != manifest_sha256
                or row["verified_at"] != verified_at
            ):
                raise IdempotencyConflict("connector checkpoint completion changed")
            return dict(row)

    def fail_connector_checkpoint_request(
        self, operation_id: str, *, failure_code: str
    ) -> dict[str, Any]:
        code = re.sub(r"[^A-Za-z0-9_.-]", "_", failure_code)[:120]
        if not code:
            raise ValueError("connector checkpoint failure code is invalid")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            connection.execute(
                "UPDATE connector_checkpoint_requests SET state='FAILED',failure_code=?,updated_at=? "
                "WHERE operation_id=? AND state IN ('REQUESTED','CLAIMED')",
                (code, now, operation_id),
            )
            row = connection.execute(
                "SELECT * FROM connector_checkpoint_requests WHERE operation_id=?",
                (operation_id,),
            ).fetchone()
            if row is None or row["state"] != "FAILED" or row["failure_code"] != code:
                raise IdempotencyConflict("connector checkpoint failure changed")
            return dict(row)

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
        quarantine_receipt_json = value.pop("quarantine_receipt_json")
        value["import_receipt"] = json.loads(import_receipt_json) if import_receipt_json else None
        value["checkpoint_receipt"] = json.loads(checkpoint_receipt_json) if checkpoint_receipt_json else None
        value["quarantine_receipt"] = json.loads(quarantine_receipt_json) if quarantine_receipt_json else None
        if value["claimed_epoch"] is not None:
            value["claimed_epoch"] = int(value["claimed_epoch"])
        return value

    def ensure_embedding_production_request(
        self,
        *,
        request_id: str,
        operation_id: str,
        idempotency_key_sha256: str,
        request_sha256: str,
        request: Mapping[str, Any],
    ) -> tuple[dict[str, Any], bool]:
        """Append one secret-free Gate-K request, or return its exact replay."""

        request_json = _safe_json(request)
        if any(len(value) != 64 for value in (idempotency_key_sha256, request_sha256)):
            raise ValueError("embedding request hashes must be exact SHA-256 values")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            changed = connection.execute(
                "INSERT INTO embedding_production_requests(request_id,operation_id,idempotency_key_sha256,"
                "request_sha256,request_json,state,created_at,updated_at) VALUES (?,?,?,?,?,'REQUESTED',?,?) "
                "ON CONFLICT(request_id) DO NOTHING",
                (request_id, operation_id, idempotency_key_sha256, request_sha256, request_json, now, now),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (
                row["operation_id"],
                row["idempotency_key_sha256"],
                row["request_sha256"],
                row["request_json"],
            ) != (operation_id, idempotency_key_sha256, request_sha256, request_json):
                raise IdempotencyConflict("embedding request identity was reused for different metadata")
            return self._embedding_request_from_row(row), bool(changed)

    def admit_embedding_production_request(
        self,
        *,
        request_id: str,
        idempotency_key_sha256: str,
        request_sha256: str,
        request: Mapping[str, Any],
        canonical_revision: int,
        checkpoint_id: str,
    ) -> tuple[dict[str, Any], bool]:
        """Atomically bind an embedding request to ACTIVE master + exact HEAD."""

        request_json = _safe_json(request)
        if any(len(value) != 64 for value in (idempotency_key_sha256, request_sha256)):
            raise ValueError("embedding request hashes must be exact SHA-256 values")
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if existing is not None:
                if (
                    existing["idempotency_key_sha256"],
                    existing["request_sha256"],
                    existing["request_json"],
                ) != (idempotency_key_sha256, request_sha256, request_json):
                    raise IdempotencyConflict("embedding request identity was reused for different metadata")
                return self._embedding_request_from_row(existing), False
            authority = connection.execute(
                "SELECT o.operation_id,o.operation_kind,o.state AS operation_state,o.identity_json,"
                "s.run_id,s.attempt_id,s.master_instance_id,s.epoch,s.state AS service_state,"
                "s.lease_until,s.canonical_revision,e.current_epoch,h.current_checkpoint_id,c.status "
                "FROM services s JOIN service_epochs e USING(service_kind) "
                "JOIN run_attempts r ON r.run_id=s.run_id AND r.attempt_id=s.attempt_id "
                "JOIN operations o ON o.operation_id=r.operation_id "
                "JOIN checkpoint_heads h ON h.service_kind=s.service_kind "
                "LEFT JOIN checkpoint_candidates c ON c.checkpoint_id=h.current_checkpoint_id "
                "WHERE s.service_kind='postgres-master' AND s.epoch=e.current_epoch",
            ).fetchone()
            if authority is None:
                raise MasterAdmissionRejected("embedding prerequisite not active")
            identity = json.loads(authority["identity_json"])
            if (
                authority["operation_kind"] != "ensure_master"
                or authority["operation_state"] != "ACTIVE"
                or authority["service_state"] != "ACTIVE"
                or authority["lease_until"] <= now
                or int(authority["epoch"]) != int(authority["current_epoch"])
                or int(authority["canonical_revision"] or -1) != canonical_revision
                or authority["current_checkpoint_id"] != checkpoint_id
                or authority["status"] != "VERIFIED"
                or str(identity.get("run_id")) != authority["run_id"]
                or str(identity.get("attempt_id")) != authority["attempt_id"]
                or str(identity.get("master_instance_id")) != authority["master_instance_id"]
                or int(identity.get("epoch", 0)) != int(authority["epoch"])
            ):
                raise MasterAdmissionRejected("embedding prerequisite changed during admission CAS")
            connection.execute(
                "INSERT INTO embedding_production_requests(request_id,operation_id,idempotency_key_sha256,"
                "request_sha256,request_json,state,created_at,updated_at) VALUES (?,?,?,?,?,'REQUESTED',?,?)",
                (
                    request_id,
                    authority["operation_id"],
                    idempotency_key_sha256,
                    request_sha256,
                    request_json,
                    now,
                    now,
                ),
            )
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            assert row is not None
            return self._embedding_request_from_row(row), True

    def claim_embedding_production_request(
        self, *, operation_id: str, run_id: str, attempt_id: str, master_instance_id: str, epoch: int
    ) -> dict[str, Any] | None:
        now = _format_time(self.clock.now())
        with self._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if row is None:
                return None
            if row["state"] == "REQUESTED":
                connection.execute(
                    "UPDATE embedding_production_requests SET state='CLAIMED',claimed_run_id=?,"
                    "claimed_attempt_id=?,claimed_master_instance_id=?,claimed_epoch=?,updated_at=? "
                    "WHERE request_id=? AND state='REQUESTED'",
                    (run_id, attempt_id, master_instance_id, epoch, now, row["request_id"]),
                )
                row = connection.execute(
                    "SELECT * FROM embedding_production_requests WHERE request_id=?", (row["request_id"],)
                ).fetchone()
            assert row is not None
            if (
                row["claimed_run_id"],
                row["claimed_attempt_id"],
                row["claimed_master_instance_id"],
                row["claimed_epoch"],
            ) != (run_id, attempt_id, master_instance_id, epoch):
                raise StaleRuntimeEvent("embedding request belongs to another runtime epoch")
            return self._embedding_request_from_row(row)

    def record_embedding_stage_receipt(
        self, *, request_id: str, run_id: str, attempt_id: str, receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        receipt_json = _safe_json(receipt)
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE embedding_production_requests SET state='STAGE_COMMITTED',stage_receipt_json=?,updated_at=? "
                "WHERE request_id=? AND state='CLAIMED' AND claimed_run_id=? AND claimed_attempt_id=?",
                (receipt_json, _format_time(self.clock.now()), request_id, run_id, attempt_id),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (changed != 1 and row["stage_receipt_json"] != receipt_json):
                raise StaleRuntimeEvent("embedding receipt does not bind the claimed runtime")
            return self._embedding_request_from_row(row)

    def record_embedding_checkpoint_receipt(
        self, *, request_id: str, run_id: str, attempt_id: str, receipt: Mapping[str, Any]
    ) -> dict[str, Any]:
        receipt_json = _safe_json(receipt)
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE embedding_production_requests SET state='CHECKPOINT_VERIFIED',checkpoint_receipt_json=?,"
                "updated_at=? WHERE request_id=? AND state='STAGE_COMMITTED' AND claimed_run_id=? "
                "AND claimed_attempt_id=?",
                (receipt_json, _format_time(self.clock.now()), request_id, run_id, attempt_id),
            ).rowcount
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if row is None or (changed != 1 and row["checkpoint_receipt_json"] != receipt_json):
                raise StaleRuntimeEvent("embedding checkpoint does not follow committed imports")
            return self._embedding_request_from_row(row)

    def fail_embedding_production_request(
        self, *, request_id: str, run_id: str, attempt_id: str, failure_code: str
    ) -> None:
        if not failure_code or len(failure_code) > 100:
            raise ValueError("embedding failure code is invalid")
        with self._transaction() as connection:
            changed = connection.execute(
                "UPDATE embedding_production_requests SET state='FAILED',failure_code=?,updated_at=? "
                "WHERE request_id=? AND state='CLAIMED' AND stage_receipt_json IS NULL "
                "AND claimed_run_id=? AND claimed_attempt_id=?",
                (failure_code, _format_time(self.clock.now()), request_id, run_id, attempt_id),
            ).rowcount
            row = connection.execute(
                "SELECT state,failure_code FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            if changed != 1 and (row is None or row["state"] != "FAILED" or row["failure_code"] != failure_code):
                raise StaleRuntimeEvent("committed embedding imports cannot be downgraded to failed")

    def embedding_production_request(self, request_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
        return self._embedding_request_from_row(row) if row else None

    def embedding_production_request_for_operation(self, operation_id: str) -> dict[str, Any] | None:
        with self._reader() as connection:
            row = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE operation_id=?", (operation_id,)
            ).fetchone()
        return self._embedding_request_from_row(row) if row else None

    def reconcile_abandoned_embedding_production_request(self, request_id: str) -> dict[str, Any] | None:
        """Fail a claim only after its exact runtime is durably terminal."""

        with self._transaction() as connection:
            row = connection.execute(
                "SELECT e.*,o.state AS operation_state FROM embedding_production_requests e "
                "JOIN operations o ON o.operation_id=e.operation_id WHERE e.request_id=?",
                (request_id,),
            ).fetchone()
            if (
                row is not None
                and row["state"] == "REQUESTED"
                and row["operation_state"] in {"STOPPED", "FAILED", "FENCED", "ORPHANED"}
            ):
                connection.execute(
                    "UPDATE embedding_production_requests SET state='FAILED',failure_code=?,updated_at=? "
                    "WHERE request_id=? AND state='REQUESTED'",
                    (
                        "ADMISSION_RUNTIME_TERMINAL_BEFORE_CLAIM",
                        _format_time(self.clock.now()),
                        request_id,
                    ),
                )
            elif (
                row is not None
                and row["state"] == "CLAIMED"
                and row["stage_receipt_json"] is None
                and row["operation_state"] in {"FAILED", "FENCED", "ORPHANED"}
            ):
                connection.execute(
                    "UPDATE embedding_production_requests SET state='FAILED',failure_code=?,updated_at=? "
                    "WHERE request_id=? AND state='CLAIMED' AND stage_receipt_json IS NULL",
                    (
                        "CLAIMED_RUNTIME_TERMINAL_WITHOUT_STAGE_RECEIPT",
                        _format_time(self.clock.now()),
                        request_id,
                    ),
                )
            current = connection.execute(
                "SELECT * FROM embedding_production_requests WHERE request_id=?", (request_id,)
            ).fetchone()
            return self._embedding_request_from_row(current) if current else None

    @staticmethod
    def _embedding_request_from_row(row: sqlite3.Row) -> dict[str, Any]:
        value = dict(row)
        value["request"] = json.loads(value.pop("request_json"))
        for source, target in (
            ("stage_receipt_json", "stage_receipt"),
            ("checkpoint_receipt_json", "checkpoint_receipt"),
        ):
            raw = value.pop(source)
            value[target] = json.loads(raw) if raw else None
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
        optional_keys = {"blogger_import_receipt_sha256"}
        blogger_receipt_sha = metadata.get("blogger_import_receipt_sha256")
        events = metadata.get("events")
        if (
            not expected_keys.issubset(metadata)
            or not set(metadata).issubset(expected_keys | optional_keys)
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
            or (
                blogger_receipt_sha is not None
                and (
                    not isinstance(blogger_receipt_sha, str)
                    or len(blogger_receipt_sha) != 64
                    or any(character not in "0123456789abcdef" for character in blogger_receipt_sha)
                )
            )
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

    def record_master_security_evidence(
        self, *, operation_id: str, evidence: Mapping[str, Any]
    ) -> dict[str, Any]:
        required = {
            "contract",
            "source_commit",
            "master_instance_id",
            "epoch",
            "schema_version",
            "canonical_revision",
            "outcome",
            "role_probe_count",
            "security_probe_count",
            "role_verification_sha256",
            "security_test_receipt_sha256",
            "observed_at",
        }
        if set(evidence) != required or evidence.get("contract") != "my-data-hub-master-security-evidence.v1":
            raise ValueError("master security evidence differs from its exact contract")
        if evidence.get("outcome") != "PASSED":
            raise ValueError("master security evidence is not successful")
        for key in ("source_commit", "role_verification_sha256", "security_test_receipt_sha256"):
            expected = 40 if key == "source_commit" else 64
            value = evidence.get(key)
            if not isinstance(value, str) or len(value) != expected or not re.fullmatch(r"[a-f0-9]+", value):
                raise ValueError(f"master security evidence {key} is invalid")
        for key, minimum in (("epoch", 1), ("schema_version", 1), ("canonical_revision", 0),
                             ("role_probe_count", 1), ("security_probe_count", 1)):
            value = evidence.get(key)
            if not isinstance(value, int) or isinstance(value, bool) or value < minimum:
                raise ValueError(f"master security evidence {key} is invalid")
        try:
            master_instance_id = str(UUID(str(evidence["master_instance_id"])))
            observed_at = _format_time(_parse_time(str(evidence["observed_at"])))
        except (TypeError, ValueError) as exc:
            raise ValueError("master security evidence identity or time is invalid") from exc
        evidence_json = _safe_json(evidence)
        evidence_sha256 = hashlib.sha256(evidence_json.encode("utf-8")).hexdigest()
        with self._transaction() as connection:
            operation = connection.execute(
                "SELECT operation_id,state,identity_json FROM operations WHERE operation_id=?", (operation_id,)
            ).fetchone()
            if operation is None:
                raise StaleRuntimeEvent("master security evidence operation is absent")
            identity = json.loads(operation["identity_json"])
            if (
                operation["state"] != "ACTIVE"
                or str(identity.get("run_id", "")) == ""
                or str(identity.get("attempt_id", "")) == ""
                or str(identity.get("master_instance_id")) != master_instance_id
                or int(identity.get("epoch", 0)) != int(evidence["epoch"])
            ):
                raise StaleRuntimeEvent("master security evidence differs from the ACTIVE operation")
            values = (
                operation_id,
                str(identity["run_id"]),
                str(identity["attempt_id"]),
                master_instance_id,
                int(evidence["epoch"]),
                str(evidence["source_commit"]),
                int(evidence["schema_version"]),
                int(evidence["canonical_revision"]),
                int(evidence["role_probe_count"]),
                int(evidence["security_probe_count"]),
                str(evidence["role_verification_sha256"]),
                str(evidence["security_test_receipt_sha256"]),
                observed_at,
                evidence_sha256,
            )
            try:
                connection.execute(
                    "INSERT INTO master_security_evidence(operation_id,run_id,attempt_id,master_instance_id,epoch,"
                    "source_commit,schema_version,canonical_revision,role_probe_count,security_probe_count,"
                    "role_verification_sha256,security_test_receipt_sha256,observed_at,evidence_sha256) "
                    "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    values,
                )
            except sqlite3.IntegrityError:
                stored = connection.execute(
                    "SELECT * FROM master_security_evidence WHERE operation_id=?", (operation_id,)
                ).fetchone()
                if stored is None or tuple(stored) != values:
                    raise IdempotencyConflict("master security evidence identity collision") from None
            stored = connection.execute(
                "SELECT * FROM master_security_evidence WHERE operation_id=?", (operation_id,)
            ).fetchone()
            assert stored is not None
            return dict(stored)

    def latest_master_security_evidence(self, *, source_commit: str | None = None) -> dict[str, Any] | None:
        with self._connect() as connection:
            if source_commit is None:
                row = connection.execute(
                    "SELECT * FROM master_security_evidence ORDER BY observed_at DESC LIMIT 1"
                ).fetchone()
            else:
                row = connection.execute(
                    "SELECT * FROM master_security_evidence WHERE source_commit=? ORDER BY observed_at DESC LIMIT 1",
                    (source_commit,),
                ).fetchone()
            return dict(row) if row is not None else None

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
