"""Fixed, task-owned live acceptance protocol for the PostgreSQL master lifecycle.

The request contains identities only.  It deliberately has no SQL, payload bytes,
fault parameters, clocks, durations, or provider resource names.  The command kind
and all safety assertions are derived from the closed scenario enum.
"""

from __future__ import annotations

import hashlib
from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Any, Literal, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from my_data_hub.hashing import canonical_json_bytes

ACCEPTANCE_OPERATE_SCOPE = "acceptance:operate"
MASTER_ACCEPTANCE_SCHEMA = "my-data-hub-master-lifecycle-acceptance.v1"
MAX_ACCEPTANCE_RECEIPT_BYTES = 64 * 1024
MIN_SOAK_SECONDS = 3600
MAX_SOAK_SECONDS = 5400
CONCURRENT_ENSURE_COUNT = 20


class MasterAcceptanceScenario(StrEnum):
    FM04 = "FM04"
    FM07 = "FM07"
    FM08 = "FM08"
    FM09 = "FM09"
    FM10 = "FM10"
    FM11 = "FM11"
    FM12 = "FM12"
    FM24 = "FM24"


class MasterAcceptanceCommandKind(StrEnum):
    EMPTY_MASTER_BOOTSTRAP = "EMPTY_MASTER_BOOTSTRAP"
    CONCURRENT_ENSURE_SINGLE_RUN = "CONCURRENT_ENSURE_SINGLE_RUN"
    CALLBACK_LOSS_RECOVERY = "CALLBACK_LOSS_RECOVERY"
    STALE_REPLAY_REJECTION = "STALE_REPLAY_REJECTION"
    LEASE_EXPIRY_DENIAL = "LEASE_EXPIRY_DENIAL"
    OLD_EPOCH_RETURN_DENIAL = "OLD_EPOCH_RETURN_DENIAL"
    CLEAN_DRAIN = "CLEAN_DRAIN"
    SESSION_ROTATION_SOAK = "SESSION_ROTATION_SOAK"


COMMAND_FOR_SCENARIO = {
    MasterAcceptanceScenario.FM04: MasterAcceptanceCommandKind.EMPTY_MASTER_BOOTSTRAP,
    MasterAcceptanceScenario.FM07: MasterAcceptanceCommandKind.CONCURRENT_ENSURE_SINGLE_RUN,
    MasterAcceptanceScenario.FM08: MasterAcceptanceCommandKind.CALLBACK_LOSS_RECOVERY,
    MasterAcceptanceScenario.FM09: MasterAcceptanceCommandKind.STALE_REPLAY_REJECTION,
    MasterAcceptanceScenario.FM10: MasterAcceptanceCommandKind.LEASE_EXPIRY_DENIAL,
    MasterAcceptanceScenario.FM11: MasterAcceptanceCommandKind.OLD_EPOCH_RETURN_DENIAL,
    MasterAcceptanceScenario.FM12: MasterAcceptanceCommandKind.CLEAN_DRAIN,
    MasterAcceptanceScenario.FM24: MasterAcceptanceCommandKind.SESSION_ROTATION_SOAK,
}


class MasterLifecycleAcceptanceError(RuntimeError):
    """The fixed live acceptance protocol was violated or terminally failed."""


class MasterAcceptanceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-master-lifecycle-acceptance.v1"] = MASTER_ACCEPTANCE_SCHEMA
    task_id: UUID
    scenario: MasterAcceptanceScenario
    idempotency_key: str = Field(min_length=8, max_length=200)
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    target_operation_id: UUID | None = None

    @model_validator(mode="after")
    def target_shape(self) -> MasterAcceptanceRequest:
        pre_boot = self.scenario in {MasterAcceptanceScenario.FM04, MasterAcceptanceScenario.FM07}
        if pre_boot == (self.target_operation_id is not None):
            raise ValueError("FM04/FM07 are admitted only before boot; every other scenario binds ACTIVE operation")
        return self

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class MasterAcceptanceBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    operation_id: UUID
    run_id: UUID
    attempt_id: UUID
    service_instance_id: str = Field(min_length=1, max_length=200)
    master_instance_id: UUID
    epoch: int = Field(ge=1)


class MasterAcceptanceCommand(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-master-lifecycle-command.v1"] = "my-data-hub-master-lifecycle-command.v1"
    command_id: UUID
    task_id: UUID
    scenario: MasterAcceptanceScenario
    command_kind: MasterAcceptanceCommandKind
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    binding: MasterAcceptanceBinding

    @model_validator(mode="after")
    def fixed_kind(self) -> MasterAcceptanceCommand:
        if COMMAND_FOR_SCENARIO[self.scenario] != self.command_kind:
            raise ValueError("acceptance command differs from its fixed scenario")
        expected = uuid5(NAMESPACE_URL, f"master-acceptance:{self.task_id}:{self.scenario.value}")
        if self.command_id != expected:
            raise ValueError("acceptance command id is not task-derived")
        return self

    @property
    def command_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class _Evidence(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EmptyBootstrapEvidence(_Evidence):
    kind: Literal["EMPTY_MASTER_BOOTSTRAP"]
    boot_source: Literal["empty_baseline"]
    canonical_revision: Literal[0]
    canonical_row_count: Literal[0]
    service_active: Literal[True]


class ConcurrentEnsureEvidence(_Evidence):
    kind: Literal["CONCURRENT_ENSURE_SINGLE_RUN"]
    request_count: Literal[20]
    operation_ids: tuple[UUID, ...] = Field(min_length=20, max_length=20)
    provider_run_refs: tuple[str, ...] = Field(min_length=20, max_length=20)
    provider_kernel_ids: tuple[int, ...] = Field(min_length=20, max_length=20)
    epochs: tuple[int, ...] = Field(min_length=20, max_length=20)

    @model_validator(mode="after")
    def single_effect(self) -> ConcurrentEnsureEvidence:
        if (
            len(set(self.operation_ids)) != 1
            or len(set(self.provider_run_refs)) != 1
            or len(set(self.provider_kernel_ids)) != 1
            or self.provider_kernel_ids[0] < 1
            or len(set(self.epochs)) != 1
        ):
            raise ValueError("20 same-key ensures did not converge to one operation, run, and epoch")
        return self


class CallbackLossEvidence(_Evidence):
    kind: Literal["CALLBACK_LOSS_RECOVERY"]
    callback_suppressed_once: Literal[True]
    exact_event_id: UUID
    exact_body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    control_boot_id_before: UUID
    control_boot_id_after: UUID
    replay_disposition: Literal["accepted", "duplicate"]
    service_active_after_recovery: Literal[True]

    @model_validator(mode="after")
    def real_restart(self) -> CallbackLossEvidence:
        if self.control_boot_id_before == self.control_boot_id_after:
            raise ValueError("FM08 requires a real control process restart")
        return self


class StaleReplayEvidence(_Evidence):
    kind: Literal["STALE_REPLAY_REJECTION"]
    exact_event_id: UUID
    exact_body_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    duplicate_disposition: Literal["duplicate"]
    stale_runtime_auth_rejected: Literal[True]
    stale_epoch_rejected: Literal[True]
    state_sha256_before: str = Field(pattern=r"^[0-9a-f]{64}$")
    state_sha256_after: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def state_unchanged(self) -> StaleReplayEvidence:
        if self.state_sha256_before != self.state_sha256_after:
            raise ValueError("stale replay changed current control state")
        return self


class LeaseExpiryEvidence(_Evidence):
    kind: Literal["LEASE_EXPIRY_DENIAL"]
    observed_wait_seconds: int = Field(ge=60, le=900)
    lease_expired: Literal[True]
    bounded_operator_dml_denied: Literal[True]
    transaction_state: Literal["rollback_only"]
    operator_operation_id: UUID
    operator_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    denial_code: Literal["MDH_EPOCH_LEASE_EXPIRED"]
    canonical_revision_before: int = Field(ge=0)
    canonical_revision_after: int = Field(ge=0)

    @model_validator(mode="after")
    def no_write(self) -> LeaseExpiryEvidence:
        if self.canonical_revision_before != self.canonical_revision_after:
            raise ValueError("expired lease denial advanced canonical revision")
        return self


class OldEpochEvidence(_Evidence):
    kind: Literal["OLD_EPOCH_RETURN_DENIAL"]
    old_epoch: int = Field(ge=1)
    new_epoch: int = Field(ge=2)
    old_runtime_draining_before_rotation: Literal[True]
    renew_denied: Literal[True]
    register_denied: Literal[True]
    bounded_write_denied: Literal[True]
    tunnel_denied: Literal[True]
    new_epoch_active: Literal[True]
    old_operation_id: UUID
    new_operation_id: UUID
    handoff_checkpoint_id: UUID
    write_denial_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    tunnel_denial_receipt_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @model_validator(mode="after")
    def monotonic_epoch(self) -> OldEpochEvidence:
        if self.new_epoch <= self.old_epoch or self.new_operation_id == self.old_operation_id:
            raise ValueError("replacement epoch did not advance")
        return self


class CleanDrainEvidence(_Evidence):
    kind: Literal["CLEAN_DRAIN"]
    write_gate_closed: Literal[True]
    checkpoint_id: UUID
    exact_version_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    manifest_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    exact_readback_verified: Literal[True]
    restore_smoke_verified: Literal[True]
    head_promoted: Literal[True]
    terminal_state: Literal["STOPPED"]


class RotationSoakEvidence(_Evidence):
    kind: Literal["SESSION_ROTATION_SOAK"]
    monotonic_started_ns: int = Field(ge=0)
    monotonic_finished_ns: int = Field(ge=0)
    observed_duration_seconds: int = Field(ge=MIN_SOAK_SECONDS, le=MAX_SOAK_SECONDS)
    session_rotations: int = Field(ge=12, le=200)
    lease_renewals: int = Field(ge=12, le=400)
    tunnel_renewals: int = Field(ge=12, le=400)
    rejected_stale_sessions: int = Field(ge=1, le=400)
    remained_single_epoch: Literal[True]
    service_active_at_end: Literal[True]

    @model_validator(mode="after")
    def monotonic_duration(self) -> RotationSoakEvidence:
        elapsed = (self.monotonic_finished_ns - self.monotonic_started_ns) // 1_000_000_000
        if elapsed != self.observed_duration_seconds:
            raise ValueError("FM24 duration does not match monotonic observations")
        return self


AcceptanceEvidence = Annotated[
    EmptyBootstrapEvidence
    | ConcurrentEnsureEvidence
    | CallbackLossEvidence
    | StaleReplayEvidence
    | LeaseExpiryEvidence
    | OldEpochEvidence
    | CleanDrainEvidence
    | RotationSoakEvidence,
    Field(discriminator="kind"),
]
_EVIDENCE_ADAPTER = TypeAdapter(AcceptanceEvidence)


class MasterAcceptanceReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-master-lifecycle-receipt.v1"] = "my-data-hub-master-lifecycle-receipt.v1"
    command_id: UUID
    command_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    task_id: UUID
    scenario: MasterAcceptanceScenario
    command_kind: MasterAcceptanceCommandKind
    binding: MasterAcceptanceBinding
    evidence_class: Literal["live"]
    outcome: Literal["succeeded", "failed"]
    evidence: AcceptanceEvidence
    completed_at: datetime

    @model_validator(mode="after")
    def exact_contract(self) -> MasterAcceptanceReceipt:
        if self.completed_at.tzinfo is None:
            raise ValueError("acceptance receipt completion must be timezone-aware")
        if COMMAND_FOR_SCENARIO[self.scenario] != self.command_kind:
            raise ValueError("receipt command differs from its scenario")
        if self.evidence.kind != self.command_kind.value:
            raise ValueError("receipt evidence differs from its command")
        if self.outcome != "succeeded":
            raise ValueError("failed runs are terminal evidence, never acceptance receipts")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_ACCEPTANCE_RECEIPT_BYTES:
            raise ValueError("master acceptance receipt exceeds 64 KiB")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class AcceptancePrincipal(Protocol):
    subject: str
    client_id: str
    scopes: frozenset[str]


def require_acceptance_operator(principal: AcceptancePrincipal) -> None:
    if ACCEPTANCE_OPERATE_SCOPE not in principal.scopes:
        raise PermissionError("master lifecycle acceptance requires acceptance:operate")


def parse_acceptance_evidence(value: Any) -> AcceptanceEvidence:
    return _EVIDENCE_ADAPTER.validate_python(value)


def command_for(request: MasterAcceptanceRequest, binding: MasterAcceptanceBinding) -> MasterAcceptanceCommand:
    return MasterAcceptanceCommand(
        command_id=uuid5(NAMESPACE_URL, f"master-acceptance:{request.task_id}:{request.scenario.value}"),
        task_id=request.task_id,
        scenario=request.scenario,
        command_kind=COMMAND_FOR_SCENARIO[request.scenario],
        source_revision=request.source_revision,
        binding=binding,
    )


class MasterAcceptanceRuntimeEffects(Protocol):
    """Task-owned implementations; no method accepts SQL, bytes, clocks, or fault knobs."""

    def empty_master_bootstrap(self, command: MasterAcceptanceCommand) -> EmptyBootstrapEvidence: ...

    def concurrent_ensure_single_run(self, command: MasterAcceptanceCommand) -> ConcurrentEnsureEvidence: ...

    def callback_loss_recovery(self, command: MasterAcceptanceCommand) -> CallbackLossEvidence: ...

    def stale_replay_rejection(self, command: MasterAcceptanceCommand) -> StaleReplayEvidence: ...

    def lease_expiry_denial(self, command: MasterAcceptanceCommand) -> LeaseExpiryEvidence: ...

    def old_epoch_return_denial(self, command: MasterAcceptanceCommand) -> OldEpochEvidence: ...

    def clean_drain(self, command: MasterAcceptanceCommand) -> CleanDrainEvidence: ...

    def session_rotation_soak(self, command: MasterAcceptanceCommand) -> RotationSoakEvidence: ...


def execute_master_acceptance_command(
    command: MasterAcceptanceCommand,
    effects: MasterAcceptanceRuntimeEffects,
    *,
    completed_at: datetime | None = None,
) -> MasterAcceptanceReceipt:
    """Dispatch only the code-defined operation and build a validated live receipt.

    This intentionally cannot execute a caller-selected method.  Implementations
    must perform the real database/tunnel/control operations and return the
    scenario-specific proof; a mock remains contract evidence only and must not
    be persisted through the authenticated live runtime endpoint.
    """

    calls = {
        MasterAcceptanceCommandKind.EMPTY_MASTER_BOOTSTRAP: effects.empty_master_bootstrap,
        MasterAcceptanceCommandKind.CONCURRENT_ENSURE_SINGLE_RUN: effects.concurrent_ensure_single_run,
        MasterAcceptanceCommandKind.CALLBACK_LOSS_RECOVERY: effects.callback_loss_recovery,
        MasterAcceptanceCommandKind.STALE_REPLAY_REJECTION: effects.stale_replay_rejection,
        MasterAcceptanceCommandKind.LEASE_EXPIRY_DENIAL: effects.lease_expiry_denial,
        MasterAcceptanceCommandKind.OLD_EPOCH_RETURN_DENIAL: effects.old_epoch_return_denial,
        MasterAcceptanceCommandKind.CLEAN_DRAIN: effects.clean_drain,
        MasterAcceptanceCommandKind.SESSION_ROTATION_SOAK: effects.session_rotation_soak,
    }
    evidence = calls[command.command_kind](command)
    return MasterAcceptanceReceipt(
        command_id=command.command_id,
        command_sha256=command.command_sha256,
        task_id=command.task_id,
        scenario=command.scenario,
        command_kind=command.command_kind,
        binding=command.binding,
        evidence_class="live",
        outcome="succeeded",
        evidence=evidence,
        completed_at=(completed_at or datetime.now(UTC)).astimezone(UTC),
    )
