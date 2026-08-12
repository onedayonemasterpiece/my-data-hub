"""Resumable, metadata-only production port for FM24 session rotation soak.

The adapter is deliberately hook-driven: it never sleeps and it does not own the
normal runtime heartbeat loop.  A production caller invokes the five
``SoakSessionPort`` methods at the fixed cadence from ``master_production``.  Each
method records an exact action intent before crossing a side-effect boundary and
an ACK hash afterwards.  Repeating an intent after process loss is safe because
every injected production port is required to reconcile its task/step-derived
idempotency hash.

Secrets, DSNs, principals, certificate material and query rows cannot be
represented by the durable models in this module.
"""

from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from typing import Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity, require_utc
from my_data_hub.master_runtime.database_gate import DatabaseGate
from my_data_hub.runtime_sdk import RuntimeClient, RuntimeEventType
from my_data_hub.tunnel_broker_ipc import TunnelBrokerClient

from .master_lifecycle import MasterAcceptanceBinding

SOAK_STEP_COUNT = 12
SOAK_STEP_SECONDS = 300
SOAK_MIN_SECONDS = 3600
SOAK_MAX_SECONDS = 5400
SOAK_LEASE_EXTENSION_SECONDS = 540
SOAK_CREDENTIAL_TTL_SECONDS = 240
MAX_SOAK_STATE_BYTES = 64 * 1024
_NS_PER_SECOND = 1_000_000_000

EvidenceClass = Literal["injected", "live"]


class SoakSessionError(RuntimeError):
    """The fixed FM24 production contract was violated."""


class SoakSessionNotDue(SoakSessionError):
    """The next hook arrived before its real monotonic schedule."""


class SoakSessionCancelled(SoakSessionError):
    """The owner/runtime cancelled this exact task."""


class SoakSessionDeadlineExceeded(SoakSessionError):
    """The original absolute 90-minute deadline elapsed."""


class SystemSoakClock:
    """Non-accelerated clock allowed for live evidence."""

    @staticmethod
    def monotonic_ns() -> int:
        return time.monotonic_ns()

    @staticmethod
    def utc_now() -> datetime:
        return datetime.now(UTC)


class SoakClock(Protocol):
    def monotonic_ns(self) -> int: ...

    def utc_now(self) -> datetime: ...


class StepAction(StrEnum):
    HEARTBEAT_ACK = "heartbeat_ack"
    DATABASE_LEASE_RENEW = "database_lease_renew"
    TUNNEL_LEASE_RENEW = "tunnel_lease_renew"
    CREDENTIAL_ROTATION = "credential_rotation"
    BOUNDED_READ = "bounded_read"
    PRIOR_CREDENTIAL_EXPIRY = "prior_credential_expiry"
    STALE_RECONNECT_DENIAL = "stale_reconnect_denial"


ACTION_ORDER = tuple(StepAction)


class ActionRecord(BaseModel):
    """One persisted intent/ACK pair; receipt contents stay at the authority."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    action: StepAction
    intent_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    state: Literal["INTENT_COMMITTED", "ACKED"]
    receipt_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    acknowledged_monotonic_ns: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def ack_shape(self) -> ActionRecord:
        acknowledged = self.receipt_sha256 is not None and self.acknowledged_monotonic_ns is not None
        if (self.state == "ACKED") != acknowledged:
            raise ValueError("FM24 action ACK fields differ from action state")
        return self


class SoakStepRecord(BaseModel):
    """Durable progress for one of the twelve fixed rotation steps."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    step: int = Field(ge=1, le=SOAK_STEP_COUNT)
    not_before_monotonic_ns: int = Field(ge=0)
    lease_until: datetime
    credential_expires_at: datetime
    actions: tuple[ActionRecord, ...] = Field(default=(), max_length=len(ACTION_ORDER))

    @model_validator(mode="after")
    def ordered_actions(self) -> SoakStepRecord:
        require_utc(self.lease_until, "lease_until")
        require_utc(self.credential_expires_at, "credential_expires_at")
        if self.credential_expires_at >= self.lease_until:
            raise ValueError("FM24 credential expiry must precede the renewed lease")
        if tuple(item.action for item in self.actions) != ACTION_ORDER[: len(self.actions)]:
            raise ValueError("FM24 step actions are not an exact ordered prefix")
        for index, item in enumerate(self.actions[:-1]):
            if item.state != "ACKED":
                raise ValueError(f"FM24 action {index + 1} lacks ACK before a later intent")
        return self

    @property
    def complete(self) -> bool:
        return len(self.actions) == len(ACTION_ORDER) and all(item.state == "ACKED" for item in self.actions)


class SoakSessionState(BaseModel):
    """Bounded task-owned journal projection containing metadata only."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-fm24-soak-state.v1"] = "my-data-hub-fm24-soak-state.v1"
    task_id_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    binding_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    evidence_class: EvidenceClass
    started_monotonic_ns: int = Field(ge=0)
    deadline_monotonic_ns: int = Field(ge=0)
    status: Literal["RUNNING", "COMPLETE", "CANCELLED", "FAILED"] = "RUNNING"
    terminal_code: Literal["FM24_CANCELLED", "FM24_DEADLINE_EXCEEDED"] | None = None
    completed_steps: int = Field(default=0, ge=0, le=SOAK_STEP_COUNT)
    lease_renewals: int = Field(default=0, ge=0, le=SOAK_STEP_COUNT)
    tunnel_renewals: int = Field(default=0, ge=0, le=SOAK_STEP_COUNT)
    session_rotations: int = Field(default=0, ge=0, le=SOAK_STEP_COUNT)
    bounded_reads: int = Field(default=0, ge=0, le=SOAK_STEP_COUNT)
    rejected_stale_sessions: int = Field(default=0, ge=0, le=SOAK_STEP_COUNT)
    steps: tuple[SoakStepRecord, ...] = Field(default=(), max_length=SOAK_STEP_COUNT)
    checkpoint_recovery: CheckpointRecoveryRecord | None = None
    finished_monotonic_ns: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def exact_projection(self) -> SoakSessionState:
        if self.deadline_monotonic_ns - self.started_monotonic_ns != SOAK_MAX_SECONDS * _NS_PER_SECOND:
            raise ValueError("FM24 deadline is not the fixed 90-minute absolute bound")
        if tuple(item.step for item in self.steps) != tuple(range(1, len(self.steps) + 1)):
            raise ValueError("FM24 journal steps are not contiguous")
        if any(not item.complete for item in self.steps[:-1]):
            raise ValueError("FM24 journal has a partial step before a later step")
        completed = sum(item.complete for item in self.steps)
        counters = (
            self.completed_steps,
            self.lease_renewals,
            self.tunnel_renewals,
            self.session_rotations,
            self.bounded_reads,
            self.rejected_stale_sessions,
        )
        if any(value != completed for value in counters):
            raise ValueError("FM24 durable counters differ from fully ACKed steps")
        checkpoint_acked = (
            self.checkpoint_recovery is not None
            and self.checkpoint_recovery.state == "ACKED"
        )
        if self.checkpoint_recovery is not None and completed != SOAK_STEP_COUNT:
            raise ValueError("FM24 checkpoint/recovery started before twelve durable steps")
        is_complete = completed == SOAK_STEP_COUNT and checkpoint_acked
        if (self.status == "COMPLETE") != is_complete:
            raise ValueError("FM24 COMPLETE state differs from its twelve durable steps")
        if self.status == "RUNNING" and (self.terminal_code is not None or self.finished_monotonic_ns is not None):
            raise ValueError("running FM24 state has terminal metadata")
        if self.status == "COMPLETE":
            if self.terminal_code is not None or self.finished_monotonic_ns is None:
                raise ValueError("completed FM24 state has invalid terminal metadata")
            if self.finished_monotonic_ns - self.started_monotonic_ns < SOAK_MIN_SECONDS * _NS_PER_SECOND:
                raise ValueError("completed FM24 state is shorter than one real hour")
            if self.finished_monotonic_ns > self.deadline_monotonic_ns:
                raise ValueError("completed FM24 state exceeded its absolute deadline")
        if self.status in {"CANCELLED", "FAILED"}:
            expected = "FM24_CANCELLED" if self.status == "CANCELLED" else "FM24_DEADLINE_EXCEEDED"
            if self.terminal_code != expected or self.finished_monotonic_ns is None:
                raise ValueError("terminal FM24 state lacks its bounded terminal code/time")
        return self


class CredentialRotationReceipt(BaseModel):
    """Secret-free result of an idempotent broker credential rotation."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_class: EvidenceClass
    step: int = Field(ge=1, le=SOAK_STEP_COUNT)
    current_credential_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    prior_credential_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    registration_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expires_at: datetime

    @model_validator(mode="after")
    def different_credentials(self) -> CredentialRotationReceipt:
        require_utc(self.expires_at, "expires_at")
        if self.current_credential_sha256 == self.prior_credential_sha256:
            raise ValueError("FM24 rotation retained the prior credential")
        return self


class CredentialExpiryReceipt(BaseModel):
    """Proof that the preceding step credential is explicitly unusable."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_class: EvidenceClass
    step: int = Field(ge=1, le=SOAK_STEP_COUNT)
    prior_credential_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expiry_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    expired: Literal[True]


class BoundedReadReceipt(BaseModel):
    """Metadata-only receipt for the fixed one-row ACTIVE-epoch SELECT."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_class: EvidenceClass
    step: int = Field(ge=1, le=SOAK_STEP_COUNT)
    query_contract: Literal["fm24_active_epoch_read.v1"]
    observed_rows: Literal[1]
    active_epoch: int = Field(ge=1)
    read_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class StaleReconnectReceipt(BaseModel):
    """Broker/session-binding denial, never a synthetic password check."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_class: EvidenceClass
    step: int = Field(ge=1, le=SOAK_STEP_COUNT)
    denied: Literal[True]
    denial_code: Literal["MDH_CREDENTIAL_EXPIRED_OR_REVOKED"]
    broker_binding_verified: Literal[True]
    denial_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class ActiveServiceReceipt(BaseModel):
    """Final fixed read/status proof for the exact unchanged epoch."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_class: EvidenceClass
    active: Literal[True]
    epoch: int = Field(ge=1)
    service_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class FM24CheckpointRecoveryEvidence(BaseModel):
    """Exact metadata proving the post-soak checkpoint and fixed recovery."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    evidence_class: EvidenceClass
    checkpoint_verified: Literal[True]
    recovery_succeeded: Literal[True]
    checkpoint_id: UUID
    exact_version_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    checkpoint_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    recovery_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class FM24SoakCompletionEvidence(FM24CheckpointRecoveryEvidence):
    """Durable continuity/read ACKs plus the exact checkpoint/recovery proof."""

    heartbeats_continuous: Literal[True]
    heartbeat_count: Literal[12]
    heartbeat_receipt_sha256s: tuple[str, ...] = Field(
        min_length=SOAK_STEP_COUNT,
        max_length=SOAK_STEP_COUNT,
    )
    reads_succeeded: Literal[True]
    read_query_count: Literal[12]
    bounded_read_receipt_sha256s: tuple[str, ...] = Field(
        min_length=SOAK_STEP_COUNT,
        max_length=SOAK_STEP_COUNT,
    )

    @model_validator(mode="after")
    def exact_receipt_hashes(self) -> FM24SoakCompletionEvidence:
        hashes = (*self.heartbeat_receipt_sha256s, *self.bounded_read_receipt_sha256s)
        if any(not _is_sha256(value) for value in hashes):
            raise ValueError("FM24 heartbeat/read receipt list contains a non-SHA-256 value")
        return self


class CheckpointRecoveryRecord(BaseModel):
    """Persisted intent/ACK projection for the single post-soak effect."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    intent_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    state: Literal["INTENT_COMMITTED", "ACKED"]
    evidence: FM24CheckpointRecoveryEvidence | None = None
    acknowledged_monotonic_ns: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def ack_shape(self) -> CheckpointRecoveryRecord:
        acknowledged = self.evidence is not None and self.acknowledged_monotonic_ns is not None
        if (self.state == "ACKED") != acknowledged:
            raise ValueError("FM24 checkpoint/recovery ACK fields differ from action state")
        return self


class SoakCheckpointRecoveryPort(Protocol):
    """Runtime-local fixed checkpoint/recovery effect; no SQL, bytes, or clock input."""

    def ensure_checkpoint_recovery(
        self,
        binding: MasterAcceptanceBinding,
        *,
        intent_sha256: str,
    ) -> FM24CheckpointRecoveryEvidence: ...


class SoakCredentialRegistrar(Protocol):
    """Production broker seam; implementations reconcile the exact intent hash."""

    def ensure_rotation(
        self,
        binding: MasterAcceptanceBinding,
        *,
        step: int,
        intent_sha256: str,
        expires_at: datetime,
    ) -> CredentialRotationReceipt: ...

    def ensure_prior_expired(
        self,
        binding: MasterAcceptanceBinding,
        *,
        step: int,
        intent_sha256: str,
    ) -> CredentialExpiryReceipt: ...


class SoakReadProbe(Protocol):
    """Fixed production read and production broker/session denial probes."""

    def bounded_read(
        self,
        binding: MasterAcceptanceBinding,
        *,
        step: int,
        intent_sha256: str,
    ) -> BoundedReadReceipt: ...

    def stale_reconnect_denied(
        self,
        binding: MasterAcceptanceBinding,
        *,
        step: int,
        intent_sha256: str,
    ) -> StaleReconnectReceipt: ...

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> ActiveServiceReceipt: ...


def _never_cancel() -> bool:
    return False


@dataclass(slots=True)
class SoakStateJournal:
    """Atomic mode-0600 state file below a task-owned private directory."""

    path: Path

    def __post_init__(self) -> None:
        self.path = Path(os.path.abspath(self.path))
        if not self.path.is_absolute() or _has_symlink_ancestor(self.path.parent):
            raise ValueError("FM24 state path must be absolute with a non-symlink parent")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.path.parent.stat().st_mode & 0o077:
            raise ValueError("FM24 task state directory must have mode 0700")
        if self.path.is_symlink():
            raise ValueError("FM24 state path must not be a symbolic link")

    def load(self) -> SoakSessionState | None:
        if not self.path.exists():
            return None
        if not self.path.is_file() or self.path.is_symlink() or self.path.stat().st_mode & 0o077:
            raise SoakSessionError("FM24 state file permissions are not mode 0600")
        if self.path.stat().st_size > MAX_SOAK_STATE_BYTES:
            raise SoakSessionError("FM24 state file exceeds 64 KiB")
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise SoakSessionError("FM24 state file is unreadable or malformed") from exc
        return SoakSessionState.model_validate(raw)

    def save(self, state: SoakSessionState) -> None:
        state = SoakSessionState.model_validate(state.model_dump(mode="python"))
        encoded = canonical_json_bytes(state.model_dump(mode="json"))
        if len(encoded) > MAX_SOAK_STATE_BYTES:
            raise SoakSessionError("FM24 state exceeds 64 KiB")
        temporary = self.path.with_name(f".{self.path.name}.tmp")
        if temporary.is_symlink():
            raise SoakSessionError("FM24 temporary state path is a symbolic link")
        descriptor = os.open(
            temporary,
            os.O_WRONLY | os.O_CREAT | os.O_TRUNC | getattr(os, "O_NOFOLLOW", 0),
            0o600,
        )
        try:
            with os.fdopen(descriptor, "wb", closefd=True) as handle:
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, self.path)
            os.chmod(self.path, 0o600)
            directory = os.open(self.path.parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            if temporary.exists():
                temporary.unlink()


@dataclass(slots=True)
class ProductionSoakSessionPort:
    """Concrete hook-driven implementation of ``master_production.SoakSessionPort``.

    ``credential_registrar`` and ``read_probe`` are narrow production adapters:
    their methods receive no credential, DSN, SQL or row payload.  They must
    reconcile repeated calls by ``intent_sha256``.  ``DatabaseGate.renew`` and
    ``TunnelBrokerClient.renew`` are themselves exact-identity, same-expiry
    idempotent transitions.
    """

    task_id: UUID
    binding: MasterAcceptanceBinding
    journal: SoakStateJournal
    runtime_client: RuntimeClient
    database_gate: DatabaseGate
    tunnel_authority: TunnelBrokerClient
    credential_registrar: SoakCredentialRegistrar
    read_probe: SoakReadProbe
    evidence_class: EvidenceClass
    checkpoint_recovery: SoakCheckpointRecoveryPort | None = None
    clock: SoakClock = field(default_factory=SystemSoakClock)
    cancelled: Callable[[], bool] = _never_cancel
    _lock: threading.RLock = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._lock = threading.RLock()
        self._assert_binding(self.binding)
        if self.evidence_class == "live" and type(self.clock) is not SystemSoakClock:
            raise ValueError("live FM24 evidence requires the non-accelerated system monotonic clock")
        if self.evidence_class == "live":
            runtime_root = Path("/kaggle/working").resolve()
            try:
                self.journal.path.relative_to(runtime_root)
            except ValueError as exc:
                raise ValueError("live FM24 journal must remain below /kaggle/working") from exc
        existing = self.journal.load()
        if existing is None:
            started = self.clock.monotonic_ns()
            existing = SoakSessionState(
                task_id_sha256=_sha_text(str(self.task_id)),
                binding_sha256=_binding_sha(self.binding),
                evidence_class=self.evidence_class,
                started_monotonic_ns=started,
                deadline_monotonic_ns=started + SOAK_MAX_SECONDS * _NS_PER_SECOND,
            )
            self.journal.save(existing)
        self._assert_state_binding(existing)

    def renew_lease_and_tunnel(self, binding: MasterAcceptanceBinding) -> None:
        """ACK a real heartbeat before renewing gate and tunnel leases."""

        with self._lock:
            step = self._step(binding)
            if step is None:
                return
            self._action(step.step, StepAction.HEARTBEAT_ACK, self._heartbeat)
            self._action(step.step, StepAction.DATABASE_LEASE_RENEW, self._database_renew)
            self._action(step.step, StepAction.TUNNEL_LEASE_RENEW, self._tunnel_renew)

    def rotate_credentials(self, binding: MasterAcceptanceBinding) -> None:
        with self._lock:
            step = self._step(binding)
            if step is None:
                return
            self._require_acked(step.step, StepAction.TUNNEL_LEASE_RENEW)
            self._action(step.step, StepAction.CREDENTIAL_ROTATION, self._credential_rotation)

    def bounded_read(self, binding: MasterAcceptanceBinding) -> None:
        with self._lock:
            step = self._step(binding)
            if step is None:
                return
            self._require_acked(step.step, StepAction.CREDENTIAL_ROTATION)
            self._action(step.step, StepAction.BOUNDED_READ, self._bounded_read)

    def stale_session_reconnect_denied(self, binding: MasterAcceptanceBinding) -> bool:
        with self._lock:
            step = self._step(binding)
            if step is None:
                return True
            self._require_acked(step.step, StepAction.BOUNDED_READ)
            self._action(step.step, StepAction.PRIOR_CREDENTIAL_EXPIRY, self._expire_prior)
            self._action(step.step, StepAction.STALE_RECONNECT_DENIAL, self._stale_denial)
            self._complete_step(step.step)
            return True

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool:
        with self._lock:
            self._assert_binding(binding)
            state = self._running_state(allow_complete=True)
            if state.completed_steps != SOAK_STEP_COUNT:
                return False
            now_ns = self.clock.monotonic_ns()
            if not (
                state.started_monotonic_ns + SOAK_MIN_SECONDS * _NS_PER_SECOND
                <= now_ns
                <= state.deadline_monotonic_ns
            ):
                return False
            self._ensure_checkpoint_recovery(binding)
            receipt = self.read_probe.exact_service_active(binding)
            self._receipt_class(receipt.evidence_class)
            return receipt.active and receipt.epoch == binding.epoch

    def checkpoint_recovery_evidence(
        self, binding: MasterAcceptanceBinding
    ) -> FM24SoakCompletionEvidence:
        """Return only a durable ACK from the real fixed post-soak effect."""

        with self._lock:
            self._assert_binding(binding)
            state = self._state()
            record = state.checkpoint_recovery
            if state.status != "COMPLETE" or record is None or record.state != "ACKED":
                raise SoakSessionError("FM24_CHECKPOINT_RECOVERY_ACK_REQUIRED")
            assert record.evidence is not None
            heartbeat_receipts = tuple(
                step.actions[ACTION_ORDER.index(StepAction.HEARTBEAT_ACK)].receipt_sha256
                for step in state.steps
            )
            bounded_read_receipts = tuple(
                step.actions[ACTION_ORDER.index(StepAction.BOUNDED_READ)].receipt_sha256
                for step in state.steps
            )
            if any(value is None for value in (*heartbeat_receipts, *bounded_read_receipts)):
                raise SoakSessionError("FM24_HEARTBEAT_OR_READ_ACK_REQUIRED")
            return FM24SoakCompletionEvidence(
                **record.evidence.model_dump(mode="python"),
                heartbeats_continuous=True,
                heartbeat_count=SOAK_STEP_COUNT,
                heartbeat_receipt_sha256s=heartbeat_receipts,
                reads_succeeded=True,
                read_query_count=SOAK_STEP_COUNT,
                bounded_read_receipt_sha256s=bounded_read_receipts,
            )

    def completed_steps(self, binding: MasterAcceptanceBinding) -> int:
        """Composition hook allowing the controller to resume at durable step N."""

        with self._lock:
            self._assert_binding(binding)
            return self._state().completed_steps

    def session_started_monotonic_ns(self, binding: MasterAcceptanceBinding) -> int:
        with self._lock:
            self._assert_binding(binding)
            return self._state().started_monotonic_ns

    def session_deadline_monotonic_ns(self, binding: MasterAcceptanceBinding) -> int:
        with self._lock:
            self._assert_binding(binding)
            return self._state().deadline_monotonic_ns

    def durable_state(self, binding: MasterAcceptanceBinding) -> SoakSessionState:
        """Return metadata-only progress for composition and tests."""

        with self._lock:
            self._assert_binding(binding)
            return self._state()

    def _step(self, binding: MasterAcceptanceBinding) -> SoakStepRecord | None:
        self._assert_binding(binding)
        state = self._running_state(allow_complete=True)
        if state.status == "COMPLETE":
            return None
        if state.completed_steps == SOAK_STEP_COUNT:
            # The step schedule is complete; only the fixed checkpoint/recovery
            # hook may advance the remaining RUNNING state.
            return None
        if state.steps and not state.steps[-1].complete:
            return state.steps[-1]
        step_number = state.completed_steps + 1
        not_before = state.started_monotonic_ns + step_number * SOAK_STEP_SECONDS * _NS_PER_SECOND
        now_ns = self.clock.monotonic_ns()
        if now_ns < not_before:
            raise SoakSessionNotDue("FM24_NEXT_STEP_NOT_DUE")
        now = require_utc(self.clock.utc_now(), "FM24 clock")
        step = SoakStepRecord(
            step=step_number,
            not_before_monotonic_ns=not_before,
            lease_until=now + timedelta(seconds=SOAK_LEASE_EXTENSION_SECONDS),
            credential_expires_at=now + timedelta(seconds=SOAK_CREDENTIAL_TTL_SECONDS),
        )
        state = state.model_copy(update={"steps": (*state.steps, step)})
        self.journal.save(state)
        return step

    def _action(
        self,
        step_number: int,
        action: StepAction,
        effect: Callable[[MasterAcceptanceBinding, SoakStepRecord, str], str],
    ) -> None:
        state = self._running_state()
        step = self._exact_step(state, step_number)
        index = ACTION_ORDER.index(action)
        if len(step.actions) > index:
            record = step.actions[index]
            if record.action is not action:
                raise SoakSessionError("FM24 journal action differs from fixed order")
            if record.state == "ACKED":
                return
        elif len(step.actions) != index:
            raise SoakSessionError("FM24 action called before its fixed predecessor")
        else:
            intent_sha256 = self._action_intent(state, step, action)
            record = ActionRecord(
                action=action,
                intent_sha256=intent_sha256,
                state="INTENT_COMMITTED",
            )
            self._replace_step(state, step.model_copy(update={"actions": (*step.actions, record)}))
        # Re-load the exact persisted intent before crossing the effect boundary.
        state = self._running_state()
        step = self._exact_step(state, step_number)
        record = step.actions[index]
        receipt_sha256 = effect(self.binding, step, record.intent_sha256)
        if not _is_sha256(receipt_sha256):
            raise SoakSessionError("FM24 side-effect receipt is not SHA-256")
        # An effect may use its own bounded network timeout.  Re-check the
        # original task cancellation/deadline before accepting its ACK.
        state = self._running_state()
        step = self._exact_step(state, step_number)
        record = step.actions[index]
        acknowledged = record.model_copy(
            update={
                "state": "ACKED",
                "receipt_sha256": receipt_sha256,
                "acknowledged_monotonic_ns": self.clock.monotonic_ns(),
            }
        )
        actions = (*step.actions[:index], acknowledged, *step.actions[index + 1 :])
        updated_step = step.model_copy(update={"actions": actions})
        updated_steps = tuple(updated_step if item.step == updated_step.step else item for item in state.steps)
        update: dict[str, object] = {"steps": updated_steps}
        if action is StepAction.STALE_RECONNECT_DENIAL:
            completed = sum(item.complete for item in updated_steps)
            update.update(
                {
                    "completed_steps": completed,
                    "lease_renewals": completed,
                    "tunnel_renewals": completed,
                    "session_rotations": completed,
                    "bounded_reads": completed,
                    "rejected_stale_sessions": completed,
                }
            )
            if completed == SOAK_STEP_COUNT:
                observed = self.clock.monotonic_ns() - state.started_monotonic_ns
                if observed < SOAK_MIN_SECONDS * _NS_PER_SECOND:
                    raise SoakSessionError("FM24_REAL_HOUR_NOT_OBSERVED")
        self.journal.save(state.model_copy(update=update))

    def _ensure_checkpoint_recovery(self, binding: MasterAcceptanceBinding) -> None:
        state = self._running_state(allow_complete=True)
        if state.status == "COMPLETE":
            return
        if state.completed_steps != SOAK_STEP_COUNT:
            raise SoakSessionError("FM24_CHECKPOINT_BEFORE_TWELVE_STEPS")
        if self.checkpoint_recovery is None:
            raise SoakSessionError("FM24_CHECKPOINT_RECOVERY_PORT_UNAVAILABLE")
        record = state.checkpoint_recovery
        if record is None:
            intent_sha256 = _sha(
                {
                    "schema_version": "my-data-hub-fm24-checkpoint-recovery-intent.v1",
                    "task_id_sha256": state.task_id_sha256,
                    "binding_sha256": state.binding_sha256,
                    "step_receipt_sha256s": tuple(
                        action.receipt_sha256
                        for step in state.steps
                        for action in step.actions
                    ),
                }
            )
            record = CheckpointRecoveryRecord(
                intent_sha256=intent_sha256,
                state="INTENT_COMMITTED",
            )
            self.journal.save(state.model_copy(update={"checkpoint_recovery": record}))
        elif record.state == "ACKED":
            return

        # Re-load the persisted intent before the checkpoint provider boundary.
        state = self._running_state()
        record = state.checkpoint_recovery
        assert record is not None
        evidence = self.checkpoint_recovery.ensure_checkpoint_recovery(
            binding,
            intent_sha256=record.intent_sha256,
        )
        self._receipt_class(evidence.evidence_class)
        state = self._running_state()
        record = state.checkpoint_recovery
        if record is None or record.state != "INTENT_COMMITTED":
            raise SoakSessionError("FM24 checkpoint/recovery durable intent changed")
        finished = self.clock.monotonic_ns()
        acknowledged = record.model_copy(
            update={
                "state": "ACKED",
                "evidence": evidence,
                "acknowledged_monotonic_ns": finished,
            }
        )
        self.journal.save(
            state.model_copy(
                update={
                    "checkpoint_recovery": acknowledged,
                    "status": "COMPLETE",
                    "finished_monotonic_ns": finished,
                }
            )
        )

    def _heartbeat(self, _binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str) -> str:
        receipt = self.runtime_client.emit(
            RuntimeEventType.RUNTIME_HEARTBEAT,
            phase="active",
            status="healthy",
            data={
                "acceptance_scenario": "FM24",
                "step": step.step,
                "step_intent_sha256": intent,
                "task_id_sha256": _sha_text(str(self.task_id)),
                "lease_until": step.lease_until.isoformat().replace("+00:00", "Z"),
            },
        )
        if receipt.status != "delivered" or not receipt.durable_local or receipt.event_id is None:
            raise SoakSessionError("FM24_HEARTBEAT_NOT_ACKED")
        return _sha(
            {
                "action": StepAction.HEARTBEAT_ACK,
                "intent_sha256": intent,
                "event_id_sha256": _sha_text(receipt.event_id),
                "status": receipt.status,
                "attempts": receipt.attempts,
            }
        )

    def _database_renew(self, binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str) -> str:
        self.database_gate.renew(_master_identity(binding), step.lease_until)
        return _sha({"action": StepAction.DATABASE_LEASE_RENEW, "intent_sha256": intent, "acked": True})

    def _tunnel_renew(self, binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str) -> str:
        lease = self.tunnel_authority.renew(
            master_instance_id=str(binding.master_instance_id),
            run_id=str(binding.run_id),
            attempt_id=str(binding.attempt_id),
            epoch=binding.epoch,
            lease_until=step.lease_until,
            now=require_utc(self.clock.utc_now(), "FM24 clock"),
        )
        if (
            lease.master_instance_id != str(binding.master_instance_id)
            or lease.run_id != str(binding.run_id)
            or lease.attempt_id != str(binding.attempt_id)
            or lease.epoch != binding.epoch
            or lease.lease_until != step.lease_until
        ):
            raise SoakSessionError("FM24 tunnel authority returned another binding")
        return _sha(
            {
                "action": StepAction.TUNNEL_LEASE_RENEW,
                "intent_sha256": intent,
                "lease_sha256": _sha(lease.to_json()),
            }
        )

    def _credential_rotation(
        self, binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str
    ) -> str:
        receipt = self.credential_registrar.ensure_rotation(
            binding,
            step=step.step,
            intent_sha256=intent,
            expires_at=step.credential_expires_at,
        )
        self._receipt_class(receipt.evidence_class)
        if receipt.step != step.step or receipt.expires_at != step.credential_expires_at:
            raise SoakSessionError("FM24 credential rotation receipt differs from its intent")
        return _sha(receipt.model_dump(mode="json"))

    def _bounded_read(self, binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str) -> str:
        receipt = self.read_probe.bounded_read(binding, step=step.step, intent_sha256=intent)
        self._receipt_class(receipt.evidence_class)
        if receipt.step != step.step or receipt.active_epoch != binding.epoch:
            raise SoakSessionError("FM24 bounded read did not observe the exact ACTIVE epoch")
        return _sha(receipt.model_dump(mode="json"))

    def _expire_prior(self, binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str) -> str:
        receipt = self.credential_registrar.ensure_prior_expired(
            binding,
            step=step.step,
            intent_sha256=intent,
        )
        self._receipt_class(receipt.evidence_class)
        if receipt.step != step.step or receipt.expired is not True:
            raise SoakSessionError("FM24 prior credential was not explicitly expired")
        return _sha(receipt.model_dump(mode="json"))

    def _stale_denial(self, binding: MasterAcceptanceBinding, step: SoakStepRecord, intent: str) -> str:
        receipt = self.read_probe.stale_reconnect_denied(
            binding,
            step=step.step,
            intent_sha256=intent,
        )
        self._receipt_class(receipt.evidence_class)
        if receipt.step != step.step or not receipt.denied or not receipt.broker_binding_verified:
            raise SoakSessionError("FM24 stale reconnect was not denied by production session binding")
        return _sha(receipt.model_dump(mode="json"))

    def _complete_step(self, step_number: int) -> None:
        state = self._state()
        if not state.steps or state.steps[-1].step != step_number or not state.steps[-1].complete:
            raise SoakSessionError("FM24 step cannot complete before every side effect ACK")
        if state.completed_steps != step_number:
            raise SoakSessionError("FM24 durable step counter was not advanced with its final ACK")

    def _require_acked(self, step_number: int, action: StepAction) -> None:
        state = self._running_state()
        step = self._exact_step(state, step_number)
        index = ACTION_ORDER.index(action)
        if len(step.actions) <= index or step.actions[index].state != "ACKED":
            raise SoakSessionError(f"FM24_{action.value.upper()}_ACK_REQUIRED")

    def _running_state(self, *, allow_complete: bool = False) -> SoakSessionState:
        state = self._state()
        now_ns = self.clock.monotonic_ns()
        if state.status == "CANCELLED":
            raise SoakSessionCancelled("FM24_CANCELLED")
        if state.status == "FAILED":
            raise SoakSessionDeadlineExceeded("FM24_DEADLINE_EXCEEDED")
        if state.status == "COMPLETE":
            if allow_complete:
                return state
            raise SoakSessionError("FM24 session is already complete")
        if self.cancelled():
            terminal = state.model_copy(
                update={
                    "status": "CANCELLED",
                    "terminal_code": "FM24_CANCELLED",
                    "finished_monotonic_ns": now_ns,
                }
            )
            self.journal.save(terminal)
            raise SoakSessionCancelled("FM24_CANCELLED")
        if now_ns > state.deadline_monotonic_ns:
            terminal = state.model_copy(
                update={
                    "status": "FAILED",
                    "terminal_code": "FM24_DEADLINE_EXCEEDED",
                    "finished_monotonic_ns": now_ns,
                }
            )
            self.journal.save(terminal)
            raise SoakSessionDeadlineExceeded("FM24_DEADLINE_EXCEEDED")
        return state

    def _state(self) -> SoakSessionState:
        state = self.journal.load()
        if state is None:
            raise SoakSessionError("FM24 durable state disappeared")
        self._assert_state_binding(state)
        return state

    def _replace_step(self, state: SoakSessionState, replacement: SoakStepRecord) -> None:
        steps = tuple(replacement if item.step == replacement.step else item for item in state.steps)
        if steps == state.steps:
            raise SoakSessionError("FM24 durable step is absent")
        self.journal.save(state.model_copy(update={"steps": steps}))

    @staticmethod
    def _exact_step(state: SoakSessionState, step_number: int) -> SoakStepRecord:
        if not state.steps or state.steps[-1].step != step_number or state.steps[-1].complete:
            raise SoakSessionError("FM24 method call does not target the current partial step")
        return state.steps[-1]

    def _action_intent(self, state: SoakSessionState, step: SoakStepRecord, action: StepAction) -> str:
        return _sha(
            {
                "schema_version": "my-data-hub-fm24-soak-action-intent.v1",
                "task_id_sha256": state.task_id_sha256,
                "binding_sha256": state.binding_sha256,
                "evidence_class": state.evidence_class,
                "step": step.step,
                "action": action.value,
                "not_before_monotonic_ns": step.not_before_monotonic_ns,
                "lease_until": step.lease_until.isoformat().replace("+00:00", "Z"),
                "credential_expires_at": step.credential_expires_at.isoformat().replace("+00:00", "Z"),
            }
        )

    def _assert_binding(self, binding: MasterAcceptanceBinding) -> None:
        if binding != self.binding:
            raise SoakSessionError("FM24 method received another master binding")
        runtime_binding = (
            str(self.runtime_client.run_id),
            str(self.runtime_client.attempt_id),
            self.runtime_client.service_instance_id,
            self.runtime_client.epoch,
        )
        expected = (
            str(binding.run_id),
            str(binding.attempt_id),
            binding.service_instance_id,
            binding.epoch,
        )
        if runtime_binding != expected:
            raise SoakSessionError("FM24 RuntimeClient differs from the exact bound runtime")

    def _assert_state_binding(self, state: SoakSessionState) -> None:
        if (
            state.task_id_sha256 != _sha_text(str(self.task_id))
            or state.binding_sha256 != _binding_sha(self.binding)
            or state.evidence_class != self.evidence_class
        ):
            raise SoakSessionError("FM24 durable state belongs to another task, binding, or evidence class")

    def _receipt_class(self, evidence_class: EvidenceClass) -> None:
        if evidence_class != self.evidence_class:
            raise SoakSessionError("FM24 effect receipt overstates or changes its evidence class")


def _master_identity(binding: MasterAcceptanceBinding) -> MasterIdentity:
    return MasterIdentity(
        master_instance_id=binding.master_instance_id,
        run_id=str(binding.run_id),
        epoch=binding.epoch,
    )


def _binding_sha(binding: MasterAcceptanceBinding) -> str:
    return _sha(binding.model_dump(mode="json"))


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _sha_text(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()


def _is_sha256(value: str) -> bool:
    return len(value) == 64 and all(character in "0123456789abcdef" for character in value)


def _has_symlink_ancestor(path: Path) -> bool:
    current = path
    while True:
        if current.is_symlink():
            return True
        if current == current.parent:
            return False
        current = current.parent


__all__ = [
    "SOAK_MAX_SECONDS",
    "SOAK_MIN_SECONDS",
    "SOAK_STEP_COUNT",
    "SOAK_STEP_SECONDS",
    "ActiveServiceReceipt",
    "BoundedReadReceipt",
    "CheckpointRecoveryRecord",
    "CredentialExpiryReceipt",
    "CredentialRotationReceipt",
    "EvidenceClass",
    "FM24CheckpointRecoveryEvidence",
    "FM24SoakCompletionEvidence",
    "ProductionSoakSessionPort",
    "SoakCheckpointRecoveryPort",
    "SoakCredentialRegistrar",
    "SoakReadProbe",
    "SoakSessionCancelled",
    "SoakSessionDeadlineExceeded",
    "SoakSessionError",
    "SoakSessionNotDue",
    "SoakSessionState",
    "SoakStateJournal",
    "StaleReconnectReceipt",
    "SystemSoakClock",
]
