"""Durable metadata-only ledger for brokered direct checkpoint uploads."""

from __future__ import annotations

import hashlib
import hmac
import json
import re
import sqlite3
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any

from my_data_hub.control_plane.ledger.errors import IdempotencyConflict, LeaseRejected
from my_data_hub.control_plane.ledger.store import ControlLedger

_SHA256 = re.compile(r"^[a-f0-9]{64}$")


def _time(value: datetime) -> str:
    if value.tzinfo is None:
        raise ValueError("checkpoint upload timestamp must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)


def _evidence(value: Mapping[str, Any]) -> str:
    encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    if len(encoded.encode()) > 16 * 1024:
        raise ValueError("checkpoint upload evidence is too large")
    lowered = encoded.casefold()
    if any(marker in lowered for marker in ("create_url", "blob_token", "authorization", "credential")):
        raise ValueError("checkpoint upload evidence contains secret-bearing metadata")
    return hashlib.sha256(encoded.encode()).hexdigest()


class CheckpointUploadLedger:
    """Narrow projection over the 0600 control ledger.

    Raw provider blob tokens and signed URLs appear only as encrypted BLOBs in
    this projection. Public methods never place them into evidence JSON.
    """

    def __init__(self, ledger: ControlLedger) -> None:
        self.ledger = ledger

    def ensure_publication(
        self,
        *,
        checkpoint_id: str,
        operation_id: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        service_instance_id: str,
        master_run_ref: str,
        epoch: int,
        dataset_ref: str,
        manifest_sha256: str,
        source_head_generation: int,
        expected_file_count: int,
        expected_total_bytes: int,
        authority_kind: str = "master",
        acceptance_scenario: str | None = None,
        source_previous_checkpoint_id: str | None = None,
    ) -> dict[str, Any]:
        if (
            not all(
                (
                    checkpoint_id,
                    operation_id,
                    run_id,
                    attempt_id,
                    master_instance_id,
                    service_instance_id,
                    master_run_ref,
                    dataset_ref,
                )
            )
            or epoch < 1
            or source_head_generation < 0
            or expected_file_count < 1
            or expected_total_bytes < 1
            or not _SHA256.fullmatch(manifest_sha256)
            or authority_kind not in {"master", "acceptance"}
            or acceptance_scenario not in {None, "FM05", "FM14", "FM15"}
            or (authority_kind == "master") != (acceptance_scenario is None)
        ):
            raise ValueError("checkpoint upload publication identity is invalid")
        now = _time(self.ledger.clock.now())
        expected = (
            operation_id,
            run_id,
            attempt_id,
            master_instance_id,
            service_instance_id,
            master_run_ref,
            epoch,
            dataset_ref,
            manifest_sha256,
            source_head_generation,
            expected_file_count,
            expected_total_bytes,
            authority_kind,
            acceptance_scenario,
            source_previous_checkpoint_id,
        )
        with self.ledger._transaction() as connection:
            connection.execute(
                "INSERT INTO checkpoint_blob_publications("
                "checkpoint_id,operation_id,run_id,attempt_id,master_instance_id,service_instance_id,master_run_ref,"
                "epoch,dataset_ref,"
                "manifest_sha256,source_head_generation,expected_file_count,expected_total_bytes,"
                "authority_kind,acceptance_scenario,source_previous_checkpoint_id,state,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'PREPARING',?,?) ON CONFLICT(checkpoint_id) DO NOTHING",
                (checkpoint_id, *expected, now, now),
            )
            row = connection.execute(
                "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            keys = (
                "operation_id",
                "run_id",
                "attempt_id",
                "master_instance_id",
                "service_instance_id",
                "master_run_ref",
                "epoch",
                "dataset_ref",
                "manifest_sha256",
                "source_head_generation",
                "expected_file_count",
                "expected_total_bytes",
                "authority_kind",
                "acceptance_scenario",
                "source_previous_checkpoint_id",
            )
            if row is None or tuple(row[key] for key in keys) != expected:
                raise IdempotencyConflict("checkpoint upload publication identity collision")
            self._event(
                connection,
                checkpoint_id,
                None,
                "publication.created",
                {"operation_id": operation_id, "epoch": epoch, "manifest_sha256": manifest_sha256},
                now,
            )
            return dict(row)

    def publication(self, checkpoint_id: str) -> dict[str, Any] | None:
        with self.ledger._reader() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
        return dict(row) if row is not None else None

    def runtime_service_authorized(
        self,
        *,
        service_instance_id: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> bool:
        return (
            self.runtime_service_snapshot(
                service_instance_id=service_instance_id,
                run_id=run_id,
                attempt_id=attempt_id,
                master_instance_id=master_instance_id,
                epoch=epoch,
            )
            is not None
        )

    def runtime_service_snapshot(
        self,
        *,
        service_instance_id: str,
        run_id: str,
        attempt_id: str,
        master_instance_id: str,
        epoch: int,
    ) -> dict[str, Any] | None:
        now = _time(self.ledger.clock.now())
        with self.ledger._reader() as connection:
            row = connection.execute(
                "SELECT s.run_id,s.attempt_id,s.master_instance_id,s.epoch,s.state,s.lease_until,"
                "e.current_epoch FROM services s JOIN service_epochs e USING(service_kind) "
                "WHERE s.service_kind='postgres-master' AND s.service_instance_id=?",
                (service_instance_id,),
            ).fetchone()
        if not (
            row is not None
            and row["run_id"] == run_id
            and row["attempt_id"] == attempt_id
            and row["master_instance_id"] == master_instance_id
            and int(row["epoch"]) == epoch
            and int(row["current_epoch"]) == epoch
            and row["state"] in {"ACTIVE", "DRAINING"}
            and str(row["lease_until"]) > now
        ):
            return None
        return dict(row)

    def publication_runtime_authority(self, checkpoint_id: str) -> dict[str, Any] | None:
        now = _time(self.ledger.clock.now())
        with self.ledger._reader() as connection:
            row = connection.execute(
                "SELECT p.operation_id,p.run_id,p.attempt_id,p.master_instance_id,p.service_instance_id,"
                "p.master_run_ref,p.epoch,s.lease_until FROM checkpoint_blob_publications p "
                "JOIN services s ON s.service_instance_id=p.service_instance_id "
                "JOIN service_epochs e ON e.service_kind=s.service_kind "
                "WHERE p.checkpoint_id=? AND s.service_kind='postgres-master' "
                "AND s.run_id=p.run_id AND s.attempt_id=p.attempt_id "
                "AND s.master_instance_id=p.master_instance_id AND s.epoch=p.epoch "
                "AND e.current_epoch=p.epoch AND s.state IN ('ACTIVE','DRAINING') AND s.lease_until>?",
                (checkpoint_id, now),
            ).fetchone()
        return dict(row) if row is not None else None

    def pending_publications(self, *, limit: int = 10) -> list[str]:
        if not 1 <= limit <= 100:
            raise ValueError("checkpoint publication scan limit is invalid")
        with self.ledger._reader() as connection:
            rows = connection.execute(
                "SELECT checkpoint_id FROM checkpoint_blob_publications WHERE authority_kind='master' AND state IN ("
                "'READY_TO_FINALIZE','FINALIZING','DATASET_RESOLVED','VERIFYING','VERIFIED') "
                "ORDER BY updated_at,checkpoint_id LIMIT ?",
                (limit,),
            ).fetchall()
        return [str(row[0]) for row in rows]

    def ensure_claim(
        self,
        *,
        claim_id: str,
        checkpoint_id: str,
        operation_id: str,
        epoch: int,
        file_name: str,
        content_length: int,
        content_type: str,
        content_sha256: str,
        manifest_sha256: str,
        intent_sha256: str,
        expires_at: datetime,
    ) -> tuple[dict[str, Any], bool]:
        if (
            not all((claim_id, checkpoint_id, operation_id, file_name, content_type))
            or epoch < 1
            or content_length < 1
            or not all(
                _SHA256.fullmatch(value)
                for value in (
                    content_sha256,
                    manifest_sha256,
                    intent_sha256,
                )
            )
        ):
            raise ValueError("checkpoint upload claim identity is invalid")
        now = _time(self.ledger.clock.now())
        expiry = _time(expires_at)
        expected = (
            checkpoint_id,
            operation_id,
            epoch,
            file_name,
            content_length,
            content_type,
            content_sha256,
            manifest_sha256,
            intent_sha256,
            expiry,
        )
        conflict = False
        result: dict[str, Any] | None = None
        with self.ledger._transaction() as connection:
            created = connection.execute(
                "INSERT INTO checkpoint_blob_upload_claims("
                "claim_id,checkpoint_id,operation_id,epoch,file_name,content_length,content_type,"
                "content_sha256,manifest_sha256,intent_sha256,state,expires_at,created_at,updated_at) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,'PREPARING',?,?,?) "
                "ON CONFLICT(checkpoint_id,file_name) DO NOTHING",
                (claim_id, *expected, now, now),
            )
            row = connection.execute(
                "SELECT * FROM checkpoint_blob_upload_claims WHERE checkpoint_id=? AND file_name=?",
                (checkpoint_id, file_name),
            ).fetchone()
            keys = (
                "checkpoint_id",
                "operation_id",
                "epoch",
                "file_name",
                "content_length",
                "content_type",
                "content_sha256",
                "manifest_sha256",
                "intent_sha256",
                "expires_at",
            )
            if row is None or tuple(row[key] for key in keys) != expected:
                if row is not None and row["state"] not in {"CONFLICT", "REVOKED"}:
                    connection.execute(
                        "UPDATE checkpoint_blob_upload_claims SET state='CONFLICT',updated_at=? WHERE claim_id=?",
                        (now, row["claim_id"]),
                    )
                    connection.execute(
                        "UPDATE checkpoint_blob_publications SET state='QUARANTINED',"
                        "failure_code='BLOB_PREPARE_CONFLICT',updated_at=? "
                        "WHERE checkpoint_id=? AND state NOT IN ('PROMOTED','FAILED','QUARANTINED')",
                        (now, checkpoint_id),
                    )
                    self._event(
                        connection,
                        checkpoint_id,
                        str(row["claim_id"]),
                        "blob.upload.conflict",
                        {"file_name": file_name},
                        now,
                    )
                conflict = True
            elif row["state"] == "PREPARING" and created.rowcount == 1:
                self._event(
                    connection,
                    checkpoint_id,
                    str(row["claim_id"]),
                    "blob.prepare.started",
                    {"intent_sha256": intent_sha256},
                    now,
                )
            if row is not None:
                result = dict(row)
        if conflict or result is None:
            raise IdempotencyConflict("checkpoint upload claim conflicts with its exact replay")
        return result, created.rowcount == 1

    def claim_start(self, claim_id: str) -> dict[str, Any]:
        """Persist the non-repeatable provider-effect boundary before calling Kaggle."""

        now = _time(self.ledger.clock.now())
        with self.ledger._transaction() as connection:
            row = self._claim(connection, claim_id)
            if row["state"] != "PREPARING":
                raise IdempotencyConflict("checkpoint blob start was already claimed")
            changed = connection.execute(
                "UPDATE checkpoint_blob_upload_claims SET state='STARTING',updated_at=? "
                "WHERE claim_id=? AND state='PREPARING'",
                (now, claim_id),
            ).rowcount
            if changed != 1:
                raise IdempotencyConflict("checkpoint blob start claim lost its CAS")
            self._event(
                connection,
                str(row["checkpoint_id"]),
                claim_id,
                "blob.prepare.claimed",
                {"intent_sha256": str(row["intent_sha256"])},
                now,
            )
            return dict(self._claim(connection, claim_id))

    def mark_ready(self, claim_id: str, *, sealed_blob_token: bytes, sealed_create_url: bytes) -> dict[str, Any]:
        if not sealed_blob_token or not sealed_create_url:
            raise ValueError("sealed upload authority is absent")
        now = _time(self.ledger.clock.now())
        with self.ledger._transaction() as connection:
            row = self._claim(connection, claim_id)
            if row["state"] == "STARTING":
                connection.execute(
                    "UPDATE checkpoint_blob_upload_claims SET state='READY',sealed_blob_token=?,"
                    "sealed_create_url=?,updated_at=? WHERE claim_id=? AND state='STARTING'",
                    (sealed_blob_token, sealed_create_url, now, claim_id),
                )
                connection.execute(
                    "UPDATE checkpoint_blob_publications SET state='UPLOADING',updated_at=? "
                    "WHERE checkpoint_id=? AND state='PREPARING'",
                    (now, row["checkpoint_id"]),
                )
                self._event(
                    connection,
                    str(row["checkpoint_id"]),
                    claim_id,
                    "blob.prepare.ready",
                    {"intent_sha256": row["intent_sha256"]},
                    now,
                )
            elif row["state"] not in {"READY", "UPLOADED"}:
                raise IdempotencyConflict("checkpoint upload claim cannot become ready")
            return dict(self._claim(connection, claim_id))

    def mark_start_ambiguous(self, claim_id: str) -> None:
        now = _time(self.ledger.clock.now())
        with self.ledger._transaction() as connection:
            row = self._claim(connection, claim_id)
            if row["state"] == "STARTING":
                connection.execute(
                    "UPDATE checkpoint_blob_upload_claims SET state='START_AMBIGUOUS',updated_at=? WHERE claim_id=?",
                    (now, claim_id),
                )
                connection.execute(
                    "UPDATE checkpoint_blob_publications SET state='QUARANTINED',"
                    "failure_code='BLOB_START_AMBIGUOUS',updated_at=? "
                    "WHERE checkpoint_id=? AND state NOT IN ('PROMOTED','FAILED','QUARANTINED')",
                    (now, row["checkpoint_id"]),
                )
                self._event(
                    connection,
                    str(row["checkpoint_id"]),
                    claim_id,
                    "blob.prepare.ambiguous",
                    {"intent_sha256": row["intent_sha256"]},
                    now,
                )

    def complete_claim(self, claim_id: str, *, bytes_sent: int, content_sha256: str) -> dict[str, Any]:
        now_dt = self.ledger.clock.now()
        now = _time(now_dt)
        conflict = False
        result: dict[str, Any] | None = None
        with self.ledger._transaction() as connection:
            row = self._claim(connection, claim_id)
            if bytes_sent != int(row["content_length"]) or not hmac.compare_digest(
                content_sha256, str(row["content_sha256"])
            ):
                if row["state"] not in {"CONFLICT", "REVOKED"}:
                    connection.execute(
                        "UPDATE checkpoint_blob_upload_claims SET state='CONFLICT',"
                        "sealed_create_url=NULL,updated_at=? WHERE claim_id=?",
                        (now, claim_id),
                    )
                    connection.execute(
                        "UPDATE checkpoint_blob_publications SET state='QUARANTINED',"
                        "failure_code='BLOB_COMPLETION_MISMATCH',updated_at=? "
                        "WHERE checkpoint_id=? AND state NOT IN ('PROMOTED','FAILED','QUARANTINED')",
                        (now, row["checkpoint_id"]),
                    )
                    self._event(
                        connection,
                        str(row["checkpoint_id"]),
                        claim_id,
                        "blob.upload.conflict",
                        {"intent_sha256": row["intent_sha256"]},
                        now,
                    )
                conflict = True
            elif _parse(str(row["expires_at"])) <= now_dt:
                raise LeaseRejected("checkpoint upload claim expired")
            elif row["state"] == "READY":
                connection.execute(
                    "UPDATE checkpoint_blob_upload_claims SET state='UPLOADED',sealed_create_url=NULL,"
                    "completed_at=?,updated_at=? "
                    "WHERE claim_id=? AND state='READY'",
                    (now, now, claim_id),
                )
                self._event(
                    connection,
                    str(row["checkpoint_id"]),
                    claim_id,
                    "blob.upload.completed",
                    {"intent_sha256": row["intent_sha256"], "content_sha256": content_sha256},
                    now,
                )
            elif row["state"] != "UPLOADED":
                raise IdempotencyConflict("checkpoint upload claim cannot be completed")
            if not conflict:
                counts = connection.execute(
                    "SELECT count(*) AS total,coalesce(sum(content_length),0) AS total_bytes,"
                    "sum(CASE WHEN state='UPLOADED' THEN 1 ELSE 0 END) AS uploaded "
                    "FROM checkpoint_blob_upload_claims WHERE checkpoint_id=?",
                    (row["checkpoint_id"],),
                ).fetchone()
                publication = connection.execute(
                    "SELECT expected_file_count,expected_total_bytes FROM checkpoint_blob_publications "
                    "WHERE checkpoint_id=?",
                    (row["checkpoint_id"],),
                ).fetchone()
                assert counts is not None and publication is not None
                complete = (
                    int(counts["total"]) == int(publication["expected_file_count"])
                    and int(counts["uploaded"]) == int(publication["expected_file_count"])
                    and int(counts["total_bytes"]) == int(publication["expected_total_bytes"])
                )
                if complete:
                    connection.execute(
                        "UPDATE checkpoint_blob_publications SET state='READY_TO_FINALIZE',updated_at=? "
                        "WHERE checkpoint_id=? AND state='UPLOADING'",
                        (now, row["checkpoint_id"]),
                    )
                result = dict(self._claim(connection, claim_id))
        if conflict or result is None:
            raise IdempotencyConflict("checkpoint upload completion differs from its claim")
        return result

    def claims(self, checkpoint_id: str) -> list[dict[str, Any]]:
        with self.ledger._reader() as connection:
            rows = connection.execute(
                "SELECT * FROM checkpoint_blob_upload_claims WHERE checkpoint_id=? ORDER BY file_name",
                (checkpoint_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim(self, claim_id: str) -> dict[str, Any]:
        with self.ledger._reader() as connection:
            return dict(self._claim(connection, claim_id))

    def begin_finalize(self, checkpoint_id: str, *, expected_provider_version: int) -> dict[str, Any]:
        if expected_provider_version < 1:
            raise ValueError("expected provider version must be positive")
        now = _time(self.ledger.clock.now())
        with self.ledger._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            if row is None:
                raise KeyError(checkpoint_id)
            if row["state"] == "READY_TO_FINALIZE":
                changed = connection.execute(
                    "UPDATE checkpoint_blob_publications SET state='FINALIZING',"
                    "expected_provider_version=?,finalize_attempts=finalize_attempts+1,updated_at=? "
                    "WHERE checkpoint_id=? AND state='READY_TO_FINALIZE'",
                    (expected_provider_version, now, checkpoint_id),
                ).rowcount
                if changed != 1:
                    raise IdempotencyConflict("checkpoint finalization claim lost its CAS")
                self._event(
                    connection,
                    checkpoint_id,
                    None,
                    "dataset.finalize.started",
                    {"expected_provider_version": expected_provider_version},
                    now,
                )
                row = connection.execute(
                    "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
                ).fetchone()
                assert row is not None
            elif row["state"] == "FINALIZING" and int(row["finalize_attempts"]) < 3:
                connection.execute(
                    "UPDATE checkpoint_blob_publications SET finalize_attempts=finalize_attempts+1,updated_at=? "
                    "WHERE checkpoint_id=? AND state='FINALIZING' AND finalize_attempts<3",
                    (now, checkpoint_id),
                )
                row = connection.execute(
                    "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
                ).fetchone()
                assert row is not None
                self._event(
                    connection,
                    checkpoint_id,
                    None,
                    "dataset.finalize.started",
                    {
                        "expected_provider_version": expected_provider_version,
                        "attempt": int(row["finalize_attempts"]),
                    },
                    now,
                )
            if (
                row["state"] not in {"FINALIZING", "DATASET_RESOLVED", "VERIFYING", "VERIFIED", "PROMOTED"}
                or int(row["expected_provider_version"] or 0) != expected_provider_version
            ):
                raise IdempotencyConflict("checkpoint finalization identity changed")
            return dict(row)

    def resolve_dataset(self, checkpoint_id: str, *, exact_version_ref: str) -> dict[str, Any]:
        if not exact_version_ref or len(exact_version_ref) > 512:
            raise ValueError("exact dataset version is invalid")
        now = _time(self.ledger.clock.now())
        with self.ledger._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            if row is None:
                raise KeyError(checkpoint_id)
            if row["state"] == "FINALIZING":
                connection.execute(
                    "UPDATE checkpoint_blob_publications SET state='DATASET_RESOLVED',exact_version_ref=?,"
                    "updated_at=? WHERE checkpoint_id=? AND state='FINALIZING'",
                    (exact_version_ref, now, checkpoint_id),
                )
                connection.execute(
                    "UPDATE checkpoint_blob_upload_claims SET state='CONSUMED',sealed_blob_token=NULL,"
                    "sealed_create_url=NULL,updated_at=? WHERE checkpoint_id=? AND state='UPLOADED'",
                    (now, checkpoint_id),
                )
                self._event(
                    connection,
                    checkpoint_id,
                    None,
                    "dataset.version.resolved",
                    {"exact_version_ref": exact_version_ref},
                    now,
                )
                row = connection.execute(
                    "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
                ).fetchone()
                assert row is not None
            if row["state"] not in {"DATASET_RESOLVED", "VERIFYING", "VERIFIED", "PROMOTED"}:
                raise IdempotencyConflict("checkpoint dataset cannot be resolved from its current state")
            if row["exact_version_ref"] != exact_version_ref:
                raise IdempotencyConflict("checkpoint dataset exact version changed")
            return dict(row)

    def transition(
        self,
        checkpoint_id: str,
        *,
        expected_states: frozenset[str],
        state: str,
        event_type: str,
        evidence: Mapping[str, Any],
        exact_version_ref: str | None = None,
        verifier_run_ref: str | None = None,
        verifier_receipt_sha256: str | None = None,
        verifier_evidence: Mapping[str, Any] | None = None,
        failure_code: str | None = None,
    ) -> dict[str, Any]:
        now = _time(self.ledger.clock.now())
        verifier_json = None
        if verifier_evidence is not None:
            # Validate the same bounded, secret-free shape used for event evidence,
            # but retain the typed body for acceptance reconstruction.
            _evidence(verifier_evidence)
            verifier_json = json.dumps(verifier_evidence, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        with self.ledger._transaction() as connection:
            row = connection.execute(
                "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            if row is None:
                raise KeyError(checkpoint_id)
            if row["state"] == state:
                return dict(row)
            if str(row["state"]) not in expected_states:
                raise IdempotencyConflict("checkpoint publication transition is stale")
            connection.execute(
                "UPDATE checkpoint_blob_publications SET state=?,exact_version_ref=COALESCE(?,exact_version_ref),"
                "verifier_run_ref=COALESCE(?,verifier_run_ref),verifier_receipt_sha256=COALESCE(?,verifier_receipt_sha256),"
                "verifier_evidence_json=COALESCE(?,verifier_evidence_json),"
                "failure_code=COALESCE(?,failure_code),updated_at=? WHERE checkpoint_id=? AND state=?",
                (
                    state,
                    exact_version_ref,
                    verifier_run_ref,
                    verifier_receipt_sha256,
                    verifier_json,
                    failure_code,
                    now,
                    checkpoint_id,
                    row["state"],
                ),
            )
            self._event(connection, checkpoint_id, None, event_type, evidence, now)
            refreshed = connection.execute(
                "SELECT * FROM checkpoint_blob_publications WHERE checkpoint_id=?", (checkpoint_id,)
            ).fetchone()
            assert refreshed is not None
            return dict(refreshed)

    @staticmethod
    def _claim(connection: sqlite3.Connection, claim_id: str) -> sqlite3.Row:
        row = connection.execute("SELECT * FROM checkpoint_blob_upload_claims WHERE claim_id=?", (claim_id,)).fetchone()
        if row is None:
            raise KeyError(claim_id)
        return row

    @staticmethod
    def _event(
        connection: sqlite3.Connection,
        checkpoint_id: str,
        claim_id: str | None,
        event_type: str,
        evidence: Mapping[str, Any],
        created_at: str,
    ) -> None:
        digest = _evidence(evidence)
        exists = connection.execute(
            "SELECT 1 FROM checkpoint_blob_upload_events WHERE checkpoint_id=? AND "
            "COALESCE(claim_id,'')=COALESCE(?,'') AND event_type=? AND evidence_sha256=?",
            (checkpoint_id, claim_id, event_type, digest),
        ).fetchone()
        if exists is None:
            connection.execute(
                "INSERT INTO checkpoint_blob_upload_events("
                "checkpoint_id,claim_id,event_type,evidence_sha256,created_at) "
                "VALUES (?,?,?,?,?)",
                (checkpoint_id, claim_id, event_type, digest, created_at),
            )
