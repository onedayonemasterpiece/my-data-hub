"""Serializable current/previous/candidate checkpoint promotion model."""

from __future__ import annotations

from dataclasses import dataclass, replace
from enum import StrEnum
from threading import RLock
from uuid import UUID

from .manifest import CheckpointManifest


class CheckpointStatus(StrEnum):
    CANDIDATE = "candidate"
    UPLOADED = "uploaded"
    READBACK_VERIFIED = "readback_verified"
    RESTORE_VERIFIED = "restore_verified"
    VERIFIED = "verified"
    REJECTED = "rejected"


@dataclass(frozen=True, slots=True)
class CheckpointRecord:
    manifest: CheckpointManifest
    status: CheckpointStatus
    exact_version_ref: str | None = None
    rejection_reason: str | None = None


@dataclass(frozen=True, slots=True)
class CheckpointHead:
    generation: int = 0
    current: UUID | None = None
    previous: UUID | None = None


class CheckpointRegistry:
    """Thread-safe promotion logic suitable for a transactional ledger adapter.

    This object does not persist business data.  Integration supplies the durable
    SQLite transaction around ``head`` and the serialized records.
    """

    def __init__(self) -> None:
        self._lock = RLock()
        self._head = CheckpointHead()
        self._records: dict[UUID, CheckpointRecord] = {}

    @property
    def head(self) -> CheckpointHead:
        with self._lock:
            return self._head

    def record(self, checkpoint_id: UUID) -> CheckpointRecord:
        with self._lock:
            return self._records[checkpoint_id]

    def add_candidate(self, manifest: CheckpointManifest) -> CheckpointRecord:
        manifest.validate()
        with self._lock:
            existing = self._records.get(manifest.checkpoint_id)
            if existing is not None:
                if existing.manifest.manifest_sha256 != manifest.manifest_sha256:
                    raise ValueError("checkpoint id reused with a different manifest")
                return existing
            expected_parent = self._head.current
            if manifest.parent_checkpoint_id != expected_parent:
                raise ValueError("candidate parent is not current HEAD")
            record = CheckpointRecord(manifest, CheckpointStatus.CANDIDATE)
            self._records[manifest.checkpoint_id] = record
            return record

    def uploaded(self, checkpoint_id: UUID, exact_version_ref: str) -> CheckpointRecord:
        if not exact_version_ref or len(exact_version_ref) > 512:
            raise ValueError("exact version ref is invalid")
        return self._transition(
            checkpoint_id,
            expected=CheckpointStatus.CANDIDATE,
            status=CheckpointStatus.UPLOADED,
            exact_version_ref=exact_version_ref,
        )

    def readback_verified(self, checkpoint_id: UUID) -> CheckpointRecord:
        return self._transition(
            checkpoint_id,
            expected=CheckpointStatus.UPLOADED,
            status=CheckpointStatus.READBACK_VERIFIED,
        )

    def restore_verified(self, checkpoint_id: UUID) -> CheckpointRecord:
        return self._transition(
            checkpoint_id,
            expected=CheckpointStatus.READBACK_VERIFIED,
            status=CheckpointStatus.RESTORE_VERIFIED,
        )

    def reject(self, checkpoint_id: UUID, reason: str) -> CheckpointRecord:
        if not reason or len(reason) > 512:
            raise ValueError("rejection reason is invalid")
        with self._lock:
            record = self._records[checkpoint_id]
            if record.status is CheckpointStatus.VERIFIED:
                raise ValueError("verified checkpoint cannot be rejected")
            rejected = replace(record, status=CheckpointStatus.REJECTED, rejection_reason=reason)
            self._records[checkpoint_id] = rejected
            return rejected

    def promote(self, checkpoint_id: UUID, *, expected_generation: int) -> CheckpointHead:
        with self._lock:
            record = self._records[checkpoint_id]
            if self._head.current == checkpoint_id and record.status is CheckpointStatus.VERIFIED:
                return self._head
            if expected_generation != self._head.generation:
                raise ValueError("checkpoint HEAD generation changed concurrently")
            if record.status is not CheckpointStatus.RESTORE_VERIFIED:
                raise ValueError("candidate has not passed independent restore verification")
            if record.manifest.parent_checkpoint_id != self._head.current:
                raise ValueError("candidate parent no longer matches current HEAD")
            self._records[checkpoint_id] = replace(record, status=CheckpointStatus.VERIFIED)
            self._head = CheckpointHead(
                generation=self._head.generation + 1,
                current=checkpoint_id,
                previous=self._head.current,
            )
            return self._head

    def _transition(
        self,
        checkpoint_id: UUID,
        *,
        expected: CheckpointStatus,
        status: CheckpointStatus,
        exact_version_ref: str | None = None,
    ) -> CheckpointRecord:
        with self._lock:
            record = self._records[checkpoint_id]
            if record.status is status:
                return record
            if record.status is not expected:
                raise ValueError(f"invalid checkpoint transition {record.status.value}->{status.value}")
            updated = replace(
                record,
                status=status,
                exact_version_ref=exact_version_ref or record.exact_version_ref,
            )
            self._records[checkpoint_id] = updated
            return updated
