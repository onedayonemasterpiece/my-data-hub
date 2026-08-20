"""Durable control-core and bounded cycle runner for Region Talk.

This module intentionally depends only on the devstand SQLite control ledger
for scheduling metadata.  It never connects to the canonical PostgreSQL
database and never stores task credentials or Region Talk business rows.
"""

from __future__ import annotations

import hashlib
import sqlite3
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field

from my_data_hub.control_plane.ledger.migrations import apply_control_migrations
from my_data_hub.hashing import canonical_json_bytes

from .pipeline_contracts import (
    ActiveMasterBinding,
    RegionTalkAccessBinding,
    RegionTalkCleanupReceipt,
    RegionTalkLaunchMetadata,
    RegionTalkLaunchReceipt,
    RegionTalkRunRequest,
    RegionTalkRunSnapshot,
    RegionTalkRunState,
    RegionTalkRuntimeAttestation,
    RegionTalkTerminalReceipt,
    RegionTalkTerminalStatus,
    RegionTalkTrigger,
)


class RegionTalkPipelineError(RuntimeError):
    pass


class RegionTalkLaunchAmbiguity(RegionTalkPipelineError):
    pass


class RegionTalkEpochFenced(RegionTalkPipelineError):
    pass


class LaunchObservationKind(StrEnum):
    ABSENT = "ABSENT"
    PRESENT = "PRESENT"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True, slots=True)
class LaunchObservation:
    kind: LaunchObservationKind
    receipt: RegionTalkLaunchReceipt | None = None

    def __post_init__(self) -> None:
        if (self.kind is LaunchObservationKind.PRESENT) != (self.receipt is not None):
            raise ValueError("PRESENT launch observation must contain exactly one receipt")


class RegionTalkNotebookLaunchPort(Protocol):
    """Single central provider-adapter boundary; effects must be idempotent."""

    def observe(self, metadata: RegionTalkLaunchMetadata) -> LaunchObservation: ...

    def launch(self, metadata: RegionTalkLaunchMetadata) -> RegionTalkLaunchReceipt: ...


class RegionTalkNotebookCleanupPort(Protocol):
    """Revoke exact task access before deleting exact private resources."""

    def cleanup(self, run: RegionTalkRunSnapshot) -> RegionTalkCleanupReceipt: ...


@dataclass(frozen=True, slots=True)
class RegionTalkRuntimePins:
    runtime_dataset_exact_ref: str
    runtime_image_identity: str
    runtime_image_source_commit: str
    wheel_relative_path: str
    wheel_sha256: str
    ydb_endpoint: str
    ydb_database: str
    ydb_viewer_secret_label: str
    max_cycles: int = 24
    max_runtime_seconds: int = 7_200


def _iso(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat()


def _dt(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _safe_code(value: str) -> str:
    if not 1 <= len(value) <= 80 or any(not (char.isupper() or char.isdigit() or char == "_") for char in value):
        raise ValueError("error code must be a bounded uppercase identifier")
    return value


class RegionTalkPipelineStore:
    """SQLite projection with atomic claim/replay semantics.

    A connection is opened per operation so two timer/service processes share
    SQLite's ``BEGIN IMMEDIATE`` serialization rather than a Python lock.
    """

    _ACTIVE_STATES = (
        RegionTalkRunState.LAUNCHING,
        RegionTalkRunState.PENDING_ATTESTATION,
        RegionTalkRunState.ATTESTED,
        RegionTalkRunState.RUNNING,
    )
    _BLOCKING_STATES = (
        *_ACTIVE_STATES,
        RegionTalkRunState.TERMINAL,
        RegionTalkRunState.TIMED_OUT,
        RegionTalkRunState.FENCED,
        RegionTalkRunState.CLEANUP_PENDING,
    )

    def __init__(self, path: Path) -> None:
        if not path.is_absolute():
            raise ValueError("Region Talk control ledger path must be absolute")
        self.path = path
        path.parent.mkdir(parents=True, exist_ok=True)
        connection = self._connect()
        try:
            apply_control_migrations(connection)
        finally:
            connection.close()

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=30000")
        return connection

    def ensure_request(self, request: RegionTalkRunRequest) -> tuple[RegionTalkRunSnapshot, bool]:
        connection = self._connect()
        created = False
        now = request.requested_at
        try:
            connection.execute("BEGIN IMMEDIATE")
            existing = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE request_id=?",
                (str(request.request_id),),
            ).fetchone()
            if existing is None:
                collision = connection.execute(
                    "SELECT request_id FROM region_talk_pipeline_requests "
                    "WHERE schedule_slot=? OR idempotency_key_sha256=?",
                    (request.schedule_slot, request.idempotency_key_sha256),
                ).fetchone()
                if collision is not None:
                    raise RegionTalkPipelineError("Region Talk request identity conflicts with a durable slot")
                stamp = _iso(now)
                connection.execute(
                    "INSERT INTO region_talk_pipeline_requests("
                    "request_id,project_slug,trigger_kind,schedule_slot,idempotency_key_sha256,"
                    "source_revision,publication_dispatch,state,requested_at,created_at,updated_at"
                    ") VALUES (?,?,?,?,?,?,0,'WAITING_MASTER',?,?,?)",
                    (
                        str(request.request_id), request.project_slug, request.trigger.value,
                        request.schedule_slot, request.idempotency_key_sha256, request.source_revision,
                        stamp, stamp, stamp,
                    ),
                )
                self._event(
                    connection, request.request_id, None, RegionTalkRunState.WAITING_MASTER,
                    "REQUEST_ACCEPTED", now,
                    {"trigger": request.trigger.value, "slot": request.schedule_slot},
                )
                created = True
            else:
                durable = self._snapshot(existing).request
                if durable.model_dump(exclude={"requested_at"}) != request.model_dump(
                    exclude={"requested_at"}
                ):
                    raise RegionTalkPipelineError("Region Talk request replay differs from durable intent")
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE request_id=?",
                (str(request.request_id),),
            ).fetchone()
            connection.commit()
            assert row is not None
            return self._snapshot(row), created
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def status(self, request_id: UUID) -> RegionTalkRunSnapshot | None:
        """Pure read: this method never schedules, claims, launches, or wakes."""

        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
            return self._snapshot(row) if row is not None else None
        finally:
            connection.close()

    def latest(self) -> RegionTalkRunSnapshot | None:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests ORDER BY requested_at DESC, request_id DESC LIMIT 1"
            ).fetchone()
            return self._snapshot(row) if row is not None else None
        finally:
            connection.close()

    @classmethod
    def latest_read_only(cls, path: Path) -> RegionTalkRunSnapshot | None:
        """Read status through SQLite `mode=ro` without applying migrations."""

        if not path.is_absolute() or not path.is_file() or path.is_symlink():
            raise ValueError("Region Talk control ledger must be an existing absolute file")
        connection = sqlite3.connect(
            f"{path.as_uri()}?mode=ro",
            uri=True,
            timeout=30,
            isolation_level=None,
        )
        connection.row_factory = sqlite3.Row
        try:
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests "
                "ORDER BY requested_at DESC, request_id DESC LIMIT 1"
            ).fetchone()
            return cls._snapshot(row) if row else None
        finally:
            connection.close()

    def claim_launch(
        self,
        *,
        master: ActiveMasterBinding,
        owner: str,
        now: datetime,
        lease_seconds: int,
        timeout_seconds: int,
    ) -> RegionTalkRunSnapshot | None:
        if not 15 <= lease_seconds <= 300:
            raise ValueError("launch lease must be 15..300 seconds")
        if not 60 <= timeout_seconds <= 10_800:
            raise ValueError("run timeout must be 60..10800 seconds")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            now_iso = _iso(now)
            # Restart replay owns an expired LAUNCHING lease before admitting a
            # newer slot. The task/master identity remains unchanged.
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests "
                "WHERE state='LAUNCHING' AND (lease_until IS NULL OR lease_until<=?) "
                "ORDER BY requested_at,request_id LIMIT 1",
                (now_iso,),
            ).fetchone()
            if row is not None:
                snapshot = self._snapshot(row)
                if snapshot.master != master:
                    connection.commit()
                    return None
                connection.execute(
                    "UPDATE region_talk_pipeline_requests SET lease_owner=?,lease_until=?,updated_at=? "
                    "WHERE request_id=? AND state='LAUNCHING'",
                    (
                        owner, _iso(now + timedelta(seconds=lease_seconds)), now_iso,
                        str(snapshot.request.request_id),
                    ),
                )
                connection.commit()
                return self.status(snapshot.request.request_id)

            placeholders = ",".join("?" for _ in self._BLOCKING_STATES)
            blocking = connection.execute(
                f"SELECT 1 FROM region_talk_pipeline_requests WHERE state IN ({placeholders}) LIMIT 1",
                tuple(state.value for state in self._BLOCKING_STATES),
            ).fetchone()
            if blocking is not None:
                connection.commit()
                return None
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE state='WAITING_MASTER' "
                "ORDER BY requested_at,request_id LIMIT 1"
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            request_id = UUID(str(row["request_id"]))
            task_run_id = uuid5(
                NAMESPACE_URL,
                f"region-talk:{request_id}:{master.master_instance_id}:{master.epoch}",
            )
            updated = connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='LAUNCHING',task_run_id=?,"
                "master_run_id=?,master_attempt_id=?,master_instance_id=?,epoch=?,lease_owner=?,"
                "lease_until=?,timeout_at=?,updated_at=?,error_code=NULL "
                "WHERE request_id=? AND state='WAITING_MASTER'",
                (
                    str(task_run_id), str(master.run_id), str(master.attempt_id),
                    str(master.master_instance_id), master.epoch, owner,
                    _iso(now + timedelta(seconds=lease_seconds)),
                    _iso(now + timedelta(seconds=timeout_seconds)), now_iso, str(request_id),
                ),
            ).rowcount
            if updated != 1:
                raise RegionTalkPipelineError("Region Talk launch claim lost atomic ownership")
            self._event(
                connection, request_id, RegionTalkRunState.WAITING_MASTER,
                RegionTalkRunState.LAUNCHING, "ACTIVE_MASTER_BOUND", now,
                {"task_run_id": str(task_run_id), "master_instance_id": str(master.master_instance_id),
                 "epoch": master.epoch},
            )
            claimed = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE request_id=?", (str(request_id),)
            ).fetchone()
            connection.commit()
            assert claimed is not None
            return self._snapshot(claimed)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def record_launch(
        self,
        *,
        metadata: RegionTalkLaunchMetadata,
        receipt: RegionTalkLaunchReceipt,
        owner: str,
        now: datetime,
    ) -> RegionTalkRunSnapshot:
        self._assert_launch_receipt(metadata, receipt)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, metadata.task_run_id)
            snapshot = self._snapshot(row)
            if snapshot.state is RegionTalkRunState.PENDING_ATTESTATION:
                if (
                    snapshot.source_sha256 != receipt.source_sha256
                    or snapshot.provider_run_ref != receipt.provider_run_ref
                    or snapshot.status_dataset_exact_ref != receipt.status_dataset_exact_ref
                    or snapshot.access != receipt.access
                ):
                    raise RegionTalkPipelineError("launch replay differs from durable receipt")
                connection.commit()
                return snapshot
            self._require_state_owner(row, RegionTalkRunState.LAUNCHING, owner, now)
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='PENDING_ATTESTATION',"
                "provider_run_ref=?,status_dataset_exact_ref=?,source_sha256=?,credential_id=?,"
                "credential_generation=?,credential_command_sha256=?,credential_task_token_sha256=?,"
                "credential_expires_at=?,"
                "ssh_certificate_serial=?,"
                "runtime_image_identity=?,runtime_image_source_commit=?,lease_owner=NULL,lease_until=NULL,"
                "updated_at=?,error_code=NULL WHERE task_run_id=?",
                (
                    receipt.provider_run_ref, receipt.status_dataset_exact_ref, receipt.source_sha256,
                    str(receipt.access.credential_id), receipt.access.generation,
                    receipt.access.command_sha256, receipt.access.task_token_sha256,
                    _iso(receipt.access.expires_at),
                    receipt.access.ssh_certificate_serial,
                    metadata.runtime_image_identity, metadata.runtime_image_source_commit,
                    _iso(now), str(metadata.task_run_id),
                ),
            )
            self._event(
                connection, metadata.request_id, RegionTalkRunState.LAUNCHING,
                RegionTalkRunState.PENDING_ATTESTATION, "NOTEBOOK_LAUNCHED", now,
                {"provider_run_ref": receipt.provider_run_ref, "source_sha256": receipt.source_sha256},
            )
            result = self._bound_row(connection, metadata.task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def release_launch(
        self, *, task_run_id: UUID, owner: str, now: datetime, error_code: str
    ) -> RegionTalkRunSnapshot:
        code = _safe_code(error_code)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, task_run_id)
            self._require_state_owner(row, RegionTalkRunState.LAUNCHING, owner, now, allow_expired=True)
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET lease_owner=NULL,lease_until=NULL,error_code=?,"
                "updated_at=? WHERE task_run_id=?",
                (code, _iso(now), str(task_run_id)),
            )
            self._event(
                connection, UUID(str(row["request_id"])), RegionTalkRunState.LAUNCHING,
                RegionTalkRunState.LAUNCHING, code, now, {"error_code": code},
            )
            result = self._bound_row(connection, task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def attest(
        self, attestation: RegionTalkRuntimeAttestation
    ) -> RegionTalkRunSnapshot:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, attestation.task_run_id)
            snapshot = self._snapshot(row)
            if snapshot.state in {RegionTalkRunState.ATTESTED, RegionTalkRunState.RUNNING}:
                self._assert_attestation(row, attestation)
                connection.commit()
                return snapshot
            if snapshot.state is not RegionTalkRunState.PENDING_ATTESTATION:
                raise RegionTalkPipelineError("runtime attestation is not expected in the current state")
            self._assert_attestation(row, attestation)
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='ATTESTED',updated_at=? WHERE task_run_id=?",
                (_iso(attestation.attested_at), str(attestation.task_run_id)),
            )
            self._event(
                connection, attestation.request_id, RegionTalkRunState.PENDING_ATTESTATION,
                RegionTalkRunState.ATTESTED, "RUNTIME_ATTESTED", attestation.attested_at,
                {"source_sha256": attestation.source_sha256, "epoch": attestation.epoch},
            )
            result = self._bound_row(connection, attestation.task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def mark_running(self, *, task_run_id: UUID, master: ActiveMasterBinding, now: datetime) -> RegionTalkRunSnapshot:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, task_run_id)
            snapshot = self._snapshot(row)
            if snapshot.master != master:
                raise RegionTalkEpochFenced("running event belongs to another ACTIVE epoch")
            if snapshot.state is RegionTalkRunState.RUNNING:
                connection.commit()
                return snapshot
            if snapshot.state is not RegionTalkRunState.ATTESTED:
                raise RegionTalkPipelineError("runtime cannot start before exact attestation")
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='RUNNING',updated_at=? WHERE task_run_id=?",
                (_iso(now), str(task_run_id)),
            )
            self._event(
                connection, snapshot.request.request_id, RegionTalkRunState.ATTESTED,
                RegionTalkRunState.RUNNING, "RUNTIME_STARTED", now, {"epoch": master.epoch},
            )
            result = self._bound_row(connection, task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def record_terminal(self, receipt: RegionTalkTerminalReceipt) -> RegionTalkRunSnapshot:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, receipt.task_run_id)
            snapshot = self._snapshot(row)
            if (
                snapshot.request.request_id != receipt.request_id
                or snapshot.master is None
                or snapshot.master.master_instance_id != receipt.master_instance_id
                or snapshot.master.epoch != receipt.epoch
            ):
                raise RegionTalkEpochFenced("terminal receipt belongs to another task or ACTIVE epoch")
            receipt_sha = hashlib.sha256(canonical_json_bytes(receipt.model_dump(mode="json"))).hexdigest()
            status = RegionTalkTerminalStatus(receipt.status)
            if snapshot.state is RegionTalkRunState.TERMINAL:
                if snapshot.terminal_status is not status or snapshot.terminal_receipt_sha256 != receipt_sha:
                    raise RegionTalkPipelineError("terminal replay differs from durable receipt")
                connection.commit()
                return snapshot
            if snapshot.state not in {RegionTalkRunState.ATTESTED, RegionTalkRunState.RUNNING}:
                raise RegionTalkPipelineError("terminal receipt is not expected in the current state")
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='TERMINAL',terminal_status=?,"
                "terminal_receipt_sha256=?,updated_at=? WHERE task_run_id=?",
                (status.value, receipt_sha, _iso(receipt.completed_at), str(receipt.task_run_id)),
            )
            self._event(
                connection, receipt.request_id, snapshot.state, RegionTalkRunState.TERMINAL,
                "RUNTIME_TERMINAL", receipt.completed_at,
                {"status": status.value, "receipt_sha256": receipt_sha},
            )
            result = self._bound_row(connection, receipt.task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def expire_and_fence(
        self, *, now: datetime, active_master: ActiveMasterBinding | None
    ) -> tuple[UUID, ...]:
        connection = self._connect()
        transitioned: list[UUID] = []
        try:
            connection.execute("BEGIN IMMEDIATE")
            placeholders = ",".join("?" for _ in self._ACTIVE_STATES)
            rows = connection.execute(
                f"SELECT * FROM region_talk_pipeline_requests WHERE state IN ({placeholders})",
                tuple(state.value for state in self._ACTIVE_STATES),
            ).fetchall()
            for row in rows:
                snapshot = self._snapshot(row)
                target: RegionTalkRunState | None = None
                status: RegionTalkTerminalStatus | None = None
                code: str | None = None
                if snapshot.timeout_at is not None and snapshot.timeout_at <= now.astimezone(UTC):
                    target = RegionTalkRunState.TIMED_OUT
                    status = RegionTalkTerminalStatus.TIMED_OUT
                    code = "RUN_TIMEOUT"
                elif active_master is not None and snapshot.master != active_master:
                    target = RegionTalkRunState.FENCED
                    status = RegionTalkTerminalStatus.EPOCH_FENCED
                    code = "ACTIVE_EPOCH_CHANGED"
                if target is None or status is None or code is None:
                    continue
                connection.execute(
                    "UPDATE region_talk_pipeline_requests SET state=?,terminal_status=?,error_code=?,"
                    "lease_owner=NULL,lease_until=NULL,updated_at=? WHERE request_id=?",
                    (target.value, status.value, code, _iso(now), str(snapshot.request.request_id)),
                )
                self._event(
                    connection, snapshot.request.request_id, snapshot.state, target, code, now,
                    {"terminal_status": status.value},
                )
                transitioned.append(snapshot.request.request_id)
            connection.commit()
            return tuple(transitioned)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def claim_cleanup(
        self, *, owner: str, now: datetime, lease_seconds: int = 120
    ) -> RegionTalkRunSnapshot | None:
        if not 15 <= lease_seconds <= 300:
            raise ValueError("cleanup lease must be 15..300 seconds")
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            now_iso = _iso(now)
            row = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE "
                "state IN ('TERMINAL','TIMED_OUT','FENCED') OR "
                "(state='CLEANUP_PENDING' AND (lease_until IS NULL OR lease_until<=?)) "
                "ORDER BY updated_at,request_id LIMIT 1",
                (now_iso,),
            ).fetchone()
            if row is None:
                connection.commit()
                return None
            snapshot = self._snapshot(row)
            previous = snapshot.state
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='CLEANUP_PENDING',lease_owner=?,"
                "lease_until=?,updated_at=? WHERE request_id=?",
                (
                    owner, _iso(now + timedelta(seconds=lease_seconds)), now_iso,
                    str(snapshot.request.request_id),
                ),
            )
            if previous is not RegionTalkRunState.CLEANUP_PENDING:
                self._event(
                    connection, snapshot.request.request_id, previous,
                    RegionTalkRunState.CLEANUP_PENDING, "CLEANUP_CLAIMED", now,
                    {"terminal_status": snapshot.terminal_status.value if snapshot.terminal_status else None},
                )
            result = connection.execute(
                "SELECT * FROM region_talk_pipeline_requests WHERE request_id=?",
                (str(snapshot.request.request_id),),
            ).fetchone()
            connection.commit()
            assert result is not None
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def complete_cleanup(
        self, *, receipt: RegionTalkCleanupReceipt, owner: str
    ) -> RegionTalkRunSnapshot:
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, receipt.task_run_id)
            snapshot = self._snapshot(row)
            if snapshot.state is RegionTalkRunState.CLEANED:
                if snapshot.cleanup_receipt_sha256 != receipt.receipt_sha256:
                    raise RegionTalkPipelineError("cleanup replay differs from durable receipt")
                connection.commit()
                return snapshot
            self._require_state_owner(row, RegionTalkRunState.CLEANUP_PENDING, owner, receipt.cleaned_at)
            if snapshot.access is not None and (
                snapshot.access.credential_id != receipt.credential_id
                or snapshot.access.generation != receipt.generation
                or snapshot.access.command_sha256 != receipt.command_sha256
                or snapshot.access.task_token_sha256 != receipt.task_token_sha256
                or snapshot.access.ssh_certificate_serial != receipt.ssh_certificate_serial
            ):
                raise RegionTalkPipelineError("cleanup did not revoke the exact task credential")
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET state='CLEANED',cleanup_receipt_sha256=?,"
                "lease_owner=NULL,lease_until=NULL,updated_at=?,error_code=NULL WHERE task_run_id=?",
                (receipt.receipt_sha256, _iso(receipt.cleaned_at), str(receipt.task_run_id)),
            )
            self._event(
                connection, snapshot.request.request_id, RegionTalkRunState.CLEANUP_PENDING,
                RegionTalkRunState.CLEANED, "CLEANUP_COMPLETE", receipt.cleaned_at,
                {"receipt_sha256": receipt.receipt_sha256, "resources_deleted": receipt.resources_deleted},
            )
            result = self._bound_row(connection, receipt.task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def release_cleanup(
        self, *, task_run_id: UUID, owner: str, now: datetime, error_code: str
    ) -> RegionTalkRunSnapshot:
        code = _safe_code(error_code)
        connection = self._connect()
        try:
            connection.execute("BEGIN IMMEDIATE")
            row = self._bound_row(connection, task_run_id)
            self._require_state_owner(row, RegionTalkRunState.CLEANUP_PENDING, owner, now, allow_expired=True)
            connection.execute(
                "UPDATE region_talk_pipeline_requests SET lease_owner=NULL,lease_until=NULL,error_code=?,"
                "updated_at=? WHERE task_run_id=?",
                (code, _iso(now), str(task_run_id)),
            )
            result = self._bound_row(connection, task_run_id)
            connection.commit()
            return self._snapshot(result)
        except Exception:
            connection.rollback()
            raise
        finally:
            connection.close()

    def event_count(self, request_id: UUID) -> int:
        connection = self._connect()
        try:
            row = connection.execute(
                "SELECT count(*) FROM region_talk_pipeline_events WHERE request_id=?",
                (str(request_id),),
            ).fetchone()
            return int(row[0])
        finally:
            connection.close()

    @staticmethod
    def _assert_launch_receipt(
        metadata: RegionTalkLaunchMetadata, receipt: RegionTalkLaunchReceipt
    ) -> None:
        if (
            receipt.task_run_id != metadata.task_run_id
            or receipt.master_instance_id != metadata.master.master_instance_id
            or receipt.epoch != metadata.master.epoch
        ):
            raise RegionTalkEpochFenced("launch receipt differs from exact task/master binding")

    @staticmethod
    def _assert_attestation(row: sqlite3.Row, value: RegionTalkRuntimeAttestation) -> None:
        if (
            str(row["request_id"]) != str(value.request_id)
            or str(row["master_instance_id"]) != str(value.master_instance_id)
            or int(row["epoch"]) != value.epoch
            or str(row["source_sha256"]) != value.source_sha256
            or str(row["runtime_image_identity"]) != value.image_identity
            or str(row["runtime_image_source_commit"]) != value.image_source_commit
        ):
            raise RegionTalkEpochFenced("runtime attestation differs from exact source/image/epoch binding")

    @staticmethod
    def _require_state_owner(
        row: sqlite3.Row,
        state: RegionTalkRunState,
        owner: str,
        now: datetime,
        *,
        allow_expired: bool = False,
    ) -> None:
        if str(row["state"]) != state.value or str(row["lease_owner"]) != owner:
            raise RegionTalkPipelineError("Region Talk transition lacks the exact lease owner")
        lease_until = _dt(row["lease_until"])
        if not allow_expired and (lease_until is None or lease_until < now.astimezone(UTC)):
            raise RegionTalkPipelineError("Region Talk transition lease expired")

    @staticmethod
    def _bound_row(connection: sqlite3.Connection, task_run_id: UUID) -> sqlite3.Row:
        row = connection.execute(
            "SELECT * FROM region_talk_pipeline_requests WHERE task_run_id=?", (str(task_run_id),)
        ).fetchone()
        if row is None:
            raise RegionTalkPipelineError("Region Talk task binding is unknown")
        return row

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        request_id: UUID,
        from_state: RegionTalkRunState | None,
        to_state: RegionTalkRunState,
        event_kind: str,
        now: datetime,
        metadata: dict[str, object],
    ) -> None:
        digest = hashlib.sha256(canonical_json_bytes(metadata)).hexdigest()
        connection.execute(
            "INSERT INTO region_talk_pipeline_events("
            "request_id,from_state,to_state,event_kind,event_metadata_sha256,recorded_at"
            ") VALUES (?,?,?,?,?,?)",
            (
                str(request_id), from_state.value if from_state else None, to_state.value,
                event_kind, digest, _iso(now),
            ),
        )

    @staticmethod
    def _snapshot(row: sqlite3.Row) -> RegionTalkRunSnapshot:
        requested_at = _dt(row["requested_at"])
        created_at = _dt(row["created_at"])
        updated_at = _dt(row["updated_at"])
        assert requested_at is not None and created_at is not None and updated_at is not None
        request = RegionTalkRunRequest(
            request_id=UUID(str(row["request_id"])),
            trigger=RegionTalkTrigger(str(row["trigger_kind"])),
            schedule_slot=str(row["schedule_slot"]),
            idempotency_key_sha256=str(row["idempotency_key_sha256"]),
            source_revision=row["source_revision"],
            requested_at=requested_at,
        )
        master = None
        if row["master_instance_id"] is not None:
            master = ActiveMasterBinding(
                run_id=UUID(str(row["master_run_id"])),
                attempt_id=UUID(str(row["master_attempt_id"])),
                master_instance_id=UUID(str(row["master_instance_id"])),
                epoch=int(row["epoch"]),
            )
        access = None
        if row["credential_id"] is not None:
            expires = _dt(row["credential_expires_at"])
            assert expires is not None
            access = RegionTalkAccessBinding(
                credential_id=UUID(str(row["credential_id"])),
                generation=int(row["credential_generation"]),
                command_sha256=str(row["credential_command_sha256"]),
                task_token_sha256=str(row["credential_task_token_sha256"]),
                expires_at=expires,
                ssh_certificate_serial=int(row["ssh_certificate_serial"]),
            )
        return RegionTalkRunSnapshot(
            request=request,
            state=RegionTalkRunState(str(row["state"])),
            task_run_id=UUID(str(row["task_run_id"])) if row["task_run_id"] else None,
            master=master,
            provider_run_ref=row["provider_run_ref"],
            status_dataset_exact_ref=row["status_dataset_exact_ref"],
            source_sha256=row["source_sha256"],
            access=access,
            timeout_at=_dt(row["timeout_at"]),
            terminal_status=(
                RegionTalkTerminalStatus(str(row["terminal_status"]))
                if row["terminal_status"] else None
            ),
            terminal_receipt_sha256=row["terminal_receipt_sha256"],
            cleanup_receipt_sha256=row["cleanup_receipt_sha256"],
            error_code=row["error_code"],
            created_at=created_at,
            updated_at=updated_at,
        )


@dataclass(slots=True)
class RegionTalkPipelineCoordinator:
    """One lightweight scheduler/reconciler around the central provider port."""

    store: RegionTalkPipelineStore
    launcher: RegionTalkNotebookLaunchPort
    cleanup: RegionTalkNotebookCleanupPort
    pins: RegionTalkRuntimePins
    instance_id: str
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    def schedule(self, slot: datetime, *, source_revision: str | None = None) -> tuple[RegionTalkRunSnapshot, bool]:
        return self.store.ensure_request(
            RegionTalkRunRequest.scheduled(
                schedule_slot=slot,
                source_revision=source_revision,
                requested_at=self.clock(),
            )
        )

    def request_supervised(
        self, *, idempotency_key: str, source_revision: str | None = None
    ) -> tuple[RegionTalkRunSnapshot, bool]:
        return self.store.ensure_request(
            RegionTalkRunRequest.supervised(
                idempotency_key=idempotency_key,
                source_revision=source_revision,
                requested_at=self.clock(),
            )
        )

    def status(self, request_id: UUID | None = None) -> RegionTalkRunSnapshot | None:
        return self.store.status(request_id) if request_id else self.store.latest()

    def tick(self, active_master: ActiveMasterBinding | None) -> RegionTalkRunSnapshot | None:
        now = self.clock().astimezone(UTC)
        self.store.expire_and_fence(now=now, active_master=active_master)
        cleanup_claim = self.store.claim_cleanup(owner=self.instance_id, now=now)
        if cleanup_claim is not None:
            try:
                receipt = self.cleanup.cleanup(cleanup_claim)
            except Exception:
                self.store.release_cleanup(
                    task_run_id=cleanup_claim.task_run_id,  # type: ignore[arg-type]
                    owner=self.instance_id,
                    now=now,
                    error_code="CLEANUP_RETRY_REQUIRED",
                )
                raise
            return self.store.complete_cleanup(receipt=receipt, owner=self.instance_id)
        if active_master is None:
            return None
        claim = self.store.claim_launch(
            master=active_master,
            owner=self.instance_id,
            now=now,
            lease_seconds=120,
            timeout_seconds=self.pins.max_runtime_seconds,
        )
        if claim is None:
            return None
        assert claim.task_run_id is not None and claim.master is not None
        metadata = RegionTalkLaunchMetadata(
            request_id=claim.request.request_id,
            task_run_id=claim.task_run_id,
            trigger=claim.request.trigger,
            schedule_slot=claim.request.schedule_slot,
            source_revision=claim.request.source_revision,
            master=claim.master,
            runtime_dataset_exact_ref=self.pins.runtime_dataset_exact_ref,
            runtime_image_identity=self.pins.runtime_image_identity,
            runtime_image_source_commit=self.pins.runtime_image_source_commit,
            wheel_relative_path=self.pins.wheel_relative_path,
            wheel_sha256=self.pins.wheel_sha256,
            ydb_endpoint=self.pins.ydb_endpoint,
            ydb_database=self.pins.ydb_database,
            ydb_viewer_secret_label=self.pins.ydb_viewer_secret_label,
            max_cycles=self.pins.max_cycles,
            max_runtime_seconds=self.pins.max_runtime_seconds,
        )
        try:
            observation = self.launcher.observe(metadata)
            if observation.kind is LaunchObservationKind.AMBIGUOUS:
                self.store.release_launch(
                    task_run_id=metadata.task_run_id,
                    owner=self.instance_id,
                    now=now,
                    error_code="PROVIDER_LAUNCH_AMBIGUOUS",
                )
                raise RegionTalkLaunchAmbiguity("provider launch identity is ambiguous; no second launch was issued")
            receipt = (
                observation.receipt
                if observation.kind is LaunchObservationKind.PRESENT
                else self.launcher.launch(metadata)
            )
            assert receipt is not None
            return self.store.record_launch(
                metadata=metadata, receipt=receipt, owner=self.instance_id, now=now
            )
        except RegionTalkLaunchAmbiguity:
            raise
        except Exception:
            self.store.release_launch(
                task_run_id=metadata.task_run_id,
                owner=self.instance_id,
                now=now,
                error_code="PROVIDER_LAUNCH_RETRY_REQUIRED",
            )
            raise


class RegionTalkCycleDisposition(StrEnum):
    PROGRESSED = "PROGRESSED"
    IDLE = "IDLE"
    COMPLETE = "COMPLETE"
    RETRYABLE = "RETRYABLE"


class RegionTalkCycleRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    cycle_number: int = Field(ge=1, le=96)
    publication_dispatch: bool = Field(default=False)

    def model_post_init(self, __context: object) -> None:
        if self.publication_dispatch:
            raise ValueError("Region Talk publication dispatch is disabled during supervised cutover")


class RegionTalkCycleResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    disposition: RegionTalkCycleDisposition
    rows_observed: int = Field(ge=0)
    rows_changed: int = Field(ge=0)
    receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    queue_revision: int | None = Field(default=None, ge=0)


class RegionTalkCycleExecutor(Protocol):
    """Implemented in the private Notebook against task-bound direct access."""

    def execute_cycle(self, request: RegionTalkCycleRequest) -> RegionTalkCycleResult: ...


@dataclass(frozen=True, slots=True)
class BoundedSupervisorResult:
    cycles_completed: int
    rows_observed: int
    rows_changed: int
    completed: bool
    aggregate_receipt_sha256: str
    queue_revision: int | None = None


def run_bounded_supervisor(
    *,
    executor: RegionTalkCycleExecutor,
    task_run_id: UUID,
    master_instance_id: UUID,
    epoch: int,
    max_cycles: int,
    max_runtime_seconds: int,
    max_idle_cycles: int = 2,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> BoundedSupervisorResult:
    """Run finite pipeline cycles; never publish and never become a daemon."""

    if not 1 <= max_cycles <= 96:
        raise ValueError("max_cycles must be 1..96")
    if not 60 <= max_runtime_seconds <= 10_800:
        raise ValueError("max_runtime_seconds must be 60..10800")
    if not 1 <= max_idle_cycles <= 5:
        raise ValueError("max_idle_cycles must be 1..5")
    started = monotonic()
    observed = changed = idle = completed_cycles = 0
    receipt_hashes: list[str] = []
    queue_revision: int | None = None
    complete = False
    for cycle_number in range(1, max_cycles + 1):
        if monotonic() - started >= max_runtime_seconds:
            break
        result = executor.execute_cycle(
            RegionTalkCycleRequest(
                task_run_id=task_run_id,
                master_instance_id=master_instance_id,
                epoch=epoch,
                cycle_number=cycle_number,
                publication_dispatch=False,
            )
        )
        completed_cycles += 1
        observed += result.rows_observed
        changed += result.rows_changed
        receipt_hashes.append(result.receipt_sha256)
        if result.queue_revision is not None:
            queue_revision = result.queue_revision
        if result.disposition is RegionTalkCycleDisposition.COMPLETE:
            complete = True
            break
        if result.disposition is RegionTalkCycleDisposition.IDLE:
            idle += 1
            if idle >= max_idle_cycles:
                complete = True
                break
        else:
            idle = 0
        if result.disposition is RegionTalkCycleDisposition.RETRYABLE:
            sleep(min(2**completed_cycles, 30))
    aggregate = hashlib.sha256(canonical_json_bytes({
        "task_run_id": str(task_run_id),
        "master_instance_id": str(master_instance_id),
        "epoch": epoch,
        "cycles_completed": completed_cycles,
        "rows_observed": observed,
        "rows_changed": changed,
        "cycle_receipts": receipt_hashes,
        "queue_revision": queue_revision,
        "publication_dispatch": False,
    })).hexdigest()
    return BoundedSupervisorResult(
        cycles_completed=completed_cycles,
        rows_observed=observed,
        rows_changed=changed,
        completed=complete,
        aggregate_receipt_sha256=aggregate,
        queue_revision=queue_revision,
    )
