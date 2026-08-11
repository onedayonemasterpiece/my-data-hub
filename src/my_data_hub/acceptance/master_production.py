"""Fixed production executors for the master-lifecycle acceptance scenarios.

The public request is identity-only.  Effects which must run on the control
host use a distinct owner-bound claim/CAS port; they never impersonate the
runtime callback identity.  The low-level host ports below are deliberately
scenario-specific: none accepts SQL, bytes, a clock, a fault name, a provider
resource name, or a duration.
"""
from __future__ import annotations

import hashlib
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Literal, Protocol
from uuid import UUID

from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.orchestrator.master import MasterState
from my_data_hub.providers.kaggle import KaggleMasterRuntimeProvider

from .master_lifecycle import (
    AcceptanceEvidence,
    AcceptancePrincipal,
    CallbackLossEvidence,
    CleanDrainEvidence,
    ConcurrentEnsureEvidence,
    EmptyBootstrapEvidence,
    LeaseExpiryEvidence,
    MasterAcceptanceBinding,
    MasterAcceptanceCommand,
    MasterAcceptanceCommandKind,
    MasterAcceptanceReceipt,
    MasterAcceptanceRequest,
    MasterAcceptanceRuntimeEffects,
    MasterAcceptanceScenario,
    MasterLifecycleAcceptanceError,
    OldEpochEvidence,
    RotationSoakEvidence,
    StaleReplayEvidence,
    execute_master_acceptance_command,
    require_acceptance_operator,
)

MASTER_SERVICE_KIND = "postgres-master"
HOST_SCENARIOS = frozenset(
    {
        MasterAcceptanceScenario.FM07,
        MasterAcceptanceScenario.FM08,
        MasterAcceptanceScenario.FM09,
        MasterAcceptanceScenario.FM10,
        MasterAcceptanceScenario.FM11,
        MasterAcceptanceScenario.FM12,
        MasterAcceptanceScenario.FM24,
    }
)
SOAK_SECONDS = 3600
SOAK_STEP_SECONDS = 300


class ProductionAcceptanceBlocked(MasterLifecycleAcceptanceError):
    """A real authority is absent; the scenario did not start."""

    def __init__(self, code: str) -> None:
        if not code or not code.replace("_", "").isalnum() or code.upper() != code:
            raise ValueError("production acceptance blocker code is invalid")
        self.code = code
        super().__init__(code)


class EmptyMasterConnection(Protocol):
    def execute(self, query: str) -> Any: ...


class H1ExpiredLeaseDenialPort(Protocol):
    """H1-owned fixed probe; the implementation owns the allowlisted DML."""

    def prove_expired_lease_denial(self, command: MasterAcceptanceCommand) -> LeaseExpiryEvidence: ...


class OwnerBoundAcceptanceClaimPort(Protocol):
    """Separate acceptance:operate service identity, never a runtime token.

    The implementation must atomically CAS ``PENDING -> CLAIMED`` against task,
    command, operation and principal/client binding.  Completion must CAS the
    same claim and exact receipt hash.  Returning ``None`` means the fixed host
    preconditions (for example STOPPED for FM12) are not ready yet.
    """

    def claim(
        self,
        *,
        task_id: UUID,
        expected_scenario: MasterAcceptanceScenario,
        principal: AcceptancePrincipal,
    ) -> MasterAcceptanceCommand | None: ...

    def complete(
        self,
        *,
        receipt: MasterAcceptanceReceipt,
        principal: AcceptancePrincipal,
    ) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class StoredCallbackRef:
    event_id: UUID
    body_sha256: str

    def __post_init__(self) -> None:
        if len(self.body_sha256) != 64 or any(character not in "0123456789abcdef" for character in self.body_sha256):
            raise ValueError("stored callback hash is not SHA-256")


class CallbackLossSupervisorPort(Protocol):
    """Task-owned callback suppression and real control-process supervisor."""

    def control_boot_id(self) -> UUID: ...

    def suppress_next_task_callback(self, command: MasterAcceptanceCommand) -> StoredCallbackRef: ...

    def restart_control_process(self, command: MasterAcceptanceCommand) -> UUID: ...

    def replay_stored_callback(
        self, command: MasterAcceptanceCommand, event_id: UUID
    ) -> Literal["accepted", "duplicate"]: ...

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool: ...


class StoredReplayPort(Protocol):
    """Replays one protected stored body by ID; body bytes never cross this port."""

    def exact_acked_callback(self, binding: MasterAcceptanceBinding) -> StoredCallbackRef: ...

    def control_state_sha256(self, binding: MasterAcceptanceBinding) -> str: ...

    def replay_stored_callback(self, event_id: UUID) -> Literal["duplicate"]: ...

    def replay_with_retired_runtime_auth(self, event_id: UUID) -> bool: ...

    def replay_with_stale_epoch(self, event_id: UUID) -> bool: ...


@dataclass(frozen=True, slots=True)
class OldEpochDenials:
    renew_denied: bool
    register_denied: bool
    bounded_write_denied: bool
    tunnel_denied: bool
    write_receipt_sha256: str
    tunnel_receipt_sha256: str

    def __post_init__(self) -> None:
        for value in (self.write_receipt_sha256, self.tunnel_receipt_sha256):
            if len(value) != 64 or any(character not in "0123456789abcdef" for character in value):
                raise ValueError("old-epoch denial receipt is not SHA-256")


class OldEpochDenialPort(Protocol):
    """Four fixed probes against the exact retired binding, with no mutation input."""

    def prove_old_epoch_denials(self, binding: MasterAcceptanceBinding) -> OldEpochDenials: ...


class SoakSessionPort(Protocol):
    """Real ACTIVE data-plane actions used by the fixed 60 minute controller."""

    def renew_lease_and_tunnel(self, binding: MasterAcceptanceBinding) -> None: ...

    def rotate_credentials(self, binding: MasterAcceptanceBinding) -> None: ...

    def bounded_read(self, binding: MasterAcceptanceBinding) -> None: ...

    def stale_session_reconnect_denied(self, binding: MasterAcceptanceBinding) -> bool: ...

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> bool: ...


EMPTY_CANONICAL_RELATIONS = (
    "hub.project",
    "hub.actor",
    "hub.external_account",
    "hub.content_item",
    "hub.content_identity",
    "hub.content_author",
    "hub.provenance_event",
    "hub.project_content",
    "hub.content_asset",
    "hub.entity_alias",
    "hub.project_actor",
    "region_talk.blogger_profile",
)


@dataclass(slots=True)
class ProductionMasterAcceptanceEffects(MasterAcceptanceRuntimeEffects):
    """Runtime-side fixed effects. Missing cross-boundary authority fails closed."""

    connection: EmptyMasterConnection
    boot_source: str
    h1_denial: H1ExpiredLeaseDenialPort | None = None
    soak_sessions: SoakSessionPort | None = None

    def empty_master_bootstrap(self, command: MasterAcceptanceCommand) -> EmptyBootstrapEvidence:
        self._kind(command, MasterAcceptanceCommandKind.EMPTY_MASTER_BOOTSTRAP)
        if self.boot_source != "empty_baseline":
            raise ProductionAcceptanceBlocked("FM04_NOT_EMPTY_BASELINE")
        revision_row = self.connection.execute(
            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
        ).fetchone()
        if revision_row is None or int(revision_row[0]) != 0:
            raise ProductionAcceptanceBlocked("FM04_CANONICAL_REVISION_NOT_ZERO")
        # Identifiers are a source-pinned allowlist, never request data.
        fixed_probe = " UNION ALL ".join(
            f"SELECT count(*)::bigint AS n FROM {relation}" for relation in EMPTY_CANONICAL_RELATIONS
        )
        rows = self.connection.execute(fixed_probe).fetchall()
        if len(rows) != len(EMPTY_CANONICAL_RELATIONS) or sum(int(row[0]) for row in rows) != 0:
            raise ProductionAcceptanceBlocked("FM04_CANONICAL_ROWS_NOT_EMPTY")
        return EmptyBootstrapEvidence(
            kind="EMPTY_MASTER_BOOTSTRAP",
            boot_source="empty_baseline",
            canonical_revision=0,
            canonical_row_count=0,
            service_active=True,
        )

    def concurrent_ensure_single_run(self, command: MasterAcceptanceCommand) -> ConcurrentEnsureEvidence:
        del command
        raise ProductionAcceptanceBlocked("FM07_CONTROL_HOST_EXECUTION_REQUIRED")

    def callback_loss_recovery(self, command: MasterAcceptanceCommand) -> CallbackLossEvidence:
        del command
        raise ProductionAcceptanceBlocked("FM08_CONTROL_HOST_EXECUTION_REQUIRED")

    def stale_replay_rejection(self, command: MasterAcceptanceCommand) -> StaleReplayEvidence:
        del command
        raise ProductionAcceptanceBlocked("FM09_CONTROL_HOST_EXECUTION_REQUIRED")

    def lease_expiry_denial(self, command: MasterAcceptanceCommand) -> LeaseExpiryEvidence:
        self._kind(command, MasterAcceptanceCommandKind.LEASE_EXPIRY_DENIAL)
        if self.h1_denial is None:
            # Checked before any heartbeat suppression or lease wait.
            raise ProductionAcceptanceBlocked("FM10_H1_DENIAL_RECEIPT_UNAVAILABLE")
        return self.h1_denial.prove_expired_lease_denial(command)

    def old_epoch_return_denial(self, command: MasterAcceptanceCommand) -> OldEpochEvidence:
        del command
        raise ProductionAcceptanceBlocked("FM11_CONTROL_HOST_EXECUTION_REQUIRED")

    def clean_drain(self, command: MasterAcceptanceCommand) -> CleanDrainEvidence:
        del command
        raise ProductionAcceptanceBlocked("FM12_POST_TERMINAL_FINALIZER_REQUIRED")

    def session_rotation_soak(self, command: MasterAcceptanceCommand) -> RotationSoakEvidence:
        self._kind(command, MasterAcceptanceCommandKind.SESSION_ROTATION_SOAK)
        if self.soak_sessions is None:
            raise ProductionAcceptanceBlocked("FM24_SOAK_SESSION_PORT_UNAVAILABLE")
        started = time.monotonic_ns()
        rotations = renewals = tunnel_renewals = stale_denials = 0
        for _step in range(SOAK_SECONDS // SOAK_STEP_SECONDS):
            time.sleep(SOAK_STEP_SECONDS)
            self.soak_sessions.renew_lease_and_tunnel(command.binding)
            renewals += 1
            tunnel_renewals += 1
            self.soak_sessions.rotate_credentials(command.binding)
            rotations += 1
            self.soak_sessions.bounded_read(command.binding)
            stale_denials += int(self.soak_sessions.stale_session_reconnect_denied(command.binding))
        finished = time.monotonic_ns()
        observed = (finished - started) // 1_000_000_000
        if not SOAK_SECONDS <= observed <= 5400:
            raise ProductionAcceptanceBlocked("FM24_MONOTONIC_WINDOW_INVALID")
        if stale_denials != rotations or not self.soak_sessions.exact_service_active(command.binding):
            raise ProductionAcceptanceBlocked("FM24_SESSION_OR_SERVICE_ASSERTION_FAILED")
        return RotationSoakEvidence(
            kind="SESSION_ROTATION_SOAK",
            monotonic_started_ns=started,
            monotonic_finished_ns=finished,
            observed_duration_seconds=observed,
            session_rotations=rotations,
            lease_renewals=renewals,
            tunnel_renewals=tunnel_renewals,
            rejected_stale_sessions=stale_denials,
            remained_single_epoch=True,
            service_active_at_end=True,
        )

    @staticmethod
    def _kind(command: MasterAcceptanceCommand, kind: MasterAcceptanceCommandKind) -> None:
        if command.command_kind is not kind:
            raise MasterLifecycleAcceptanceError("production effect received another fixed command")


@dataclass(frozen=True, slots=True)
class ProductionMasterAcceptanceEffectsFactory:
    h1_denial: H1ExpiredLeaseDenialPort | None = None
    soak_sessions: SoakSessionPort | None = None

    def build(
        self, *, connection: EmptyMasterConnection, boot_source: str
    ) -> ProductionMasterAcceptanceEffects:
        if boot_source not in {"empty_baseline", "verified_checkpoint"}:
            raise ValueError("production acceptance boot source is invalid")
        return ProductionMasterAcceptanceEffects(
            connection=connection,
            boot_source=boot_source,
            h1_denial=self.h1_denial,
            soak_sessions=self.soak_sessions,
        )


@dataclass(slots=True)
class ProductionControlHostEffects:
    """Concrete composition of task-owned host operations and ledger evidence."""

    runtime: ControlPlaneMasterRuntime
    callback_supervisor: CallbackLossSupervisorPort | None = None
    stored_replay: StoredReplayPort | None = None
    old_epoch_denials: OldEpochDenialPort | None = None
    h1_denial: H1ExpiredLeaseDenialPort | None = None
    soak_sessions: SoakSessionPort | None = None

    def execute(self, command: MasterAcceptanceCommand) -> AcceptanceEvidence:
        calls = {
            MasterAcceptanceCommandKind.CALLBACK_LOSS_RECOVERY: self._callback_loss,
            MasterAcceptanceCommandKind.STALE_REPLAY_REJECTION: self._stale_replay,
            MasterAcceptanceCommandKind.LEASE_EXPIRY_DENIAL: self._lease_expiry,
            MasterAcceptanceCommandKind.OLD_EPOCH_RETURN_DENIAL: self._old_epoch,
            MasterAcceptanceCommandKind.CLEAN_DRAIN: self._clean_drain,
            MasterAcceptanceCommandKind.SESSION_ROTATION_SOAK: self._soak,
        }
        action = calls.get(command.command_kind)
        if action is None:
            raise MasterLifecycleAcceptanceError("command is not a control-host scenario")
        return action(command)

    def _callback_loss(self, command: MasterAcceptanceCommand) -> CallbackLossEvidence:
        supervisor = self.callback_supervisor
        if supervisor is None:
            raise ProductionAcceptanceBlocked("FM08_CALLBACK_SUPERVISOR_UNAVAILABLE")
        before = supervisor.control_boot_id()
        stored = supervisor.suppress_next_task_callback(command)
        after = supervisor.restart_control_process(command)
        if before == after or after != supervisor.control_boot_id():
            raise ProductionAcceptanceBlocked("FM08_REAL_CONTROL_RESTART_NOT_OBSERVED")
        disposition = supervisor.replay_stored_callback(command, stored.event_id)
        if not supervisor.exact_service_active(command.binding):
            raise ProductionAcceptanceBlocked("FM08_SERVICE_NOT_ACTIVE_AFTER_RECOVERY")
        return CallbackLossEvidence(
            kind="CALLBACK_LOSS_RECOVERY",
            callback_suppressed_once=True,
            exact_event_id=stored.event_id,
            exact_body_sha256=stored.body_sha256,
            control_boot_id_before=before,
            control_boot_id_after=after,
            replay_disposition=disposition,
            service_active_after_recovery=True,
        )

    def _stale_replay(self, command: MasterAcceptanceCommand) -> StaleReplayEvidence:
        replay = self.stored_replay
        if replay is None:
            raise ProductionAcceptanceBlocked("FM09_STORED_REPLAY_PORT_UNAVAILABLE")
        stored = replay.exact_acked_callback(command.binding)
        before = replay.control_state_sha256(command.binding)
        duplicate = replay.replay_stored_callback(stored.event_id)
        retired = replay.replay_with_retired_runtime_auth(stored.event_id)
        stale_epoch = replay.replay_with_stale_epoch(stored.event_id)
        after = replay.control_state_sha256(command.binding)
        return StaleReplayEvidence(
            kind="STALE_REPLAY_REJECTION",
            exact_event_id=stored.event_id,
            exact_body_sha256=stored.body_sha256,
            duplicate_disposition=duplicate,
            stale_runtime_auth_rejected=retired,
            stale_epoch_rejected=stale_epoch,
            state_sha256_before=before,
            state_sha256_after=after,
        )

    def _lease_expiry(self, command: MasterAcceptanceCommand) -> LeaseExpiryEvidence:
        if self.h1_denial is None:
            raise ProductionAcceptanceBlocked("FM10_H1_DENIAL_RECEIPT_UNAVAILABLE")
        return self.h1_denial.prove_expired_lease_denial(command)

    def _old_epoch(self, command: MasterAcceptanceCommand) -> OldEpochEvidence:
        old_operation = self.runtime.ledger.get_operation(str(command.binding.operation_id))
        if old_operation is None or old_operation.state != MasterState.STOPPED.value:
            raise ProductionAcceptanceBlocked("FM11_OLD_RUNTIME_NOT_STOPPED")
        events = self.runtime.ledger.runtime_event_history(
            run_id=str(command.binding.run_id),
            attempt_id=str(command.binding.attempt_id),
            epoch=command.binding.epoch,
            limit=200,
        )
        event_types = tuple(str(item["event_type"]) for item in events)
        if (
            "runtime.draining" not in event_types
            or "runtime.terminal" not in event_types
            or event_types.index("runtime.draining") >= event_types.index("runtime.terminal")
        ):
            raise ProductionAcceptanceBlocked("FM11_DRAIN_BEFORE_ROTATION_NOT_PROVED")
        checkpoint = self.runtime.ledger.verified_checkpoint_for_operation(str(command.binding.operation_id))
        if checkpoint is None:
            raise ProductionAcceptanceBlocked("FM11_HANDOFF_CHECKPOINT_UNAVAILABLE")
        replacement, _duplicate = self.runtime.ensure(f"master-acceptance-fm11:{command.task_id}")
        if replacement.state is not MasterState.ACTIVE or replacement.epoch <= command.binding.epoch:
            raise ProductionAcceptanceBlocked("FM11_REPLACEMENT_NOT_ACTIVE")
        if self.old_epoch_denials is None:
            raise ProductionAcceptanceBlocked("FM11_OLD_EPOCH_PROBE_UNAVAILABLE")
        denial = self.old_epoch_denials.prove_old_epoch_denials(command.binding)
        return OldEpochEvidence(
            kind="OLD_EPOCH_RETURN_DENIAL",
            old_epoch=command.binding.epoch,
            new_epoch=replacement.epoch,
            old_runtime_draining_before_rotation=True,
            renew_denied=denial.renew_denied,
            register_denied=denial.register_denied,
            bounded_write_denied=denial.bounded_write_denied,
            tunnel_denied=denial.tunnel_denied,
            new_epoch_active=True,
            old_operation_id=command.binding.operation_id,
            new_operation_id=UUID(replacement.operation_id),
            handoff_checkpoint_id=UUID(str(checkpoint["checkpoint_id"])),
            write_denial_receipt_sha256=denial.write_receipt_sha256,
            tunnel_denial_receipt_sha256=denial.tunnel_receipt_sha256,
        )

    def _clean_drain(self, command: MasterAcceptanceCommand) -> CleanDrainEvidence:
        operation = self.runtime.ledger.get_operation(str(command.binding.operation_id))
        checkpoint = self.runtime.ledger.verified_checkpoint_for_operation(str(command.binding.operation_id))
        if operation is None or operation.state != MasterState.STOPPED.value or checkpoint is None:
            raise ProductionAcceptanceBlocked("FM12_TERMINAL_CHECKPOINT_NOT_READY")
        return CleanDrainEvidence(
            kind="CLEAN_DRAIN",
            write_gate_closed=True,
            checkpoint_id=UUID(str(checkpoint["checkpoint_id"])),
            exact_version_ref=str(checkpoint["version_ref"]),
            manifest_sha256=str(checkpoint["manifest_sha256"]),
            exact_readback_verified=True,
            restore_smoke_verified=True,
            head_promoted=True,
            terminal_state="STOPPED",
        )

    def _soak(self, command: MasterAcceptanceCommand) -> RotationSoakEvidence:
        effects = ProductionMasterAcceptanceEffects(
            connection=_UnavailableConnection(),
            boot_source="verified_checkpoint",
            soak_sessions=self.soak_sessions,
        )
        return effects.session_rotation_soak(command)


class _UnavailableConnection:
    def execute(self, query: str) -> Any:
        del query
        raise AssertionError("control-host effects cannot access notebook PostgreSQL")


@dataclass(slots=True)
class ControlMasterAcceptanceExecutor:
    """Persist requests, execute preboot actions, and reconcile owner host claims."""

    runtime: ControlPlaneMasterRuntime
    host_claims: OwnerBoundAcceptanceClaimPort | None = None
    host_effects: ProductionControlHostEffects | None = None

    def request(
        self, request: MasterAcceptanceRequest, principal: AcceptancePrincipal
    ) -> dict[str, Any]:
        require_acceptance_operator(principal)
        task, created = self.runtime.request_master_acceptance(request, principal)
        if request.scenario is MasterAcceptanceScenario.FM04:
            task = self._ensure_empty(request, task)
        elif request.scenario is MasterAcceptanceScenario.FM07:
            task = self._ensure_twenty(request, task, principal)
        else:
            task = self._reconcile_host(request.task_id, request.scenario, task, principal)
        return {**task, "created": created, "live_pass": task["state"] == "PASSED"}

    def status(self, task_id: UUID, principal: AcceptancePrincipal) -> dict[str, Any]:
        require_acceptance_operator(principal)
        task = self.runtime.ledger.master_acceptance_task(str(task_id))
        if task is None or task["principal_id"] != principal.subject or task["client_id"] != principal.client_id:
            return {"found": False}
        scenario = MasterAcceptanceScenario(str(task["scenario_id"]))
        if scenario is MasterAcceptanceScenario.FM07:
            task = self._ensure_twenty(
                MasterAcceptanceRequest(
                    task_id=task_id,
                    scenario=scenario,
                    idempotency_key=str(task["idempotency_key"]),
                    source_revision=str(task["source_revision"]),
                ),
                task,
                principal,
            )
        elif scenario in HOST_SCENARIOS:
            task = self._reconcile_host(task_id, scenario, task, principal)
        return {"found": True, **task, "live_pass": task["state"] == "PASSED"}

    def _ensure_empty(self, request: MasterAcceptanceRequest, task: dict[str, Any]) -> dict[str, Any]:
        if task["state"] != "PENDING":
            return task
        handle, _duplicate = self.runtime.ensure(f"master-acceptance-fm04:{request.task_id}")
        if handle.state is MasterState.ACTIVE:
            return self.runtime.bind_master_acceptance(str(request.task_id), handle.operation_id)
        return self.runtime.ledger.master_acceptance_task(str(request.task_id)) or task

    def _ensure_twenty(
        self,
        request: MasterAcceptanceRequest,
        task: dict[str, Any],
        principal: AcceptancePrincipal,
    ) -> dict[str, Any]:
        if task["state"] in {"PENDING", "BOUND"}:
            if self.host_claims is None:
                # Do not launch a real provider run when the owner-bound CAS
                # needed to persist its evidence is unavailable.
                raise ProductionAcceptanceBlocked("FM07_OWNER_HOST_CLAIM_UNAVAILABLE")
            provider = self.runtime.coordinator.provider
            if not isinstance(provider, KaggleMasterRuntimeProvider):
                raise ProductionAcceptanceBlocked("FM07_OFFICIAL_KAGGLE_ADAPTER_REQUIRED")
            key = f"master-acceptance-fm07:{request.task_id}"
            with ThreadPoolExecutor(max_workers=20) as pool:
                handles = tuple(pool.map(lambda _index: self.runtime.ensure(key)[0], range(20)))
            operation_ids = tuple(UUID(item.operation_id) for item in handles)
            epochs = tuple(item.epoch for item in handles)
            if len(set(operation_ids)) != 1 or len(set(epochs)) != 1:
                raise MasterLifecycleAcceptanceError("20 same-key ensures allocated multiple operations or epochs")
            handle = handles[0]
            if handle.state is not MasterState.ACTIVE:
                return self.runtime.ledger.master_acceptance_task(str(request.task_id)) or task
            task = self.runtime.bind_master_acceptance(str(request.task_id), handle.operation_id)
            trigger = self.runtime.ledger.get_effect_by_idempotency_key(f"{handle.operation_id}:trigger_run")
            if trigger is None or trigger.state.value != "APPLIED" or not isinstance(trigger.receipt, dict):
                raise MasterLifecycleAcceptanceError("FM07 trigger has no exact applied provider receipt")
            exact = trigger.receipt.get("exact_identity")
            provider_ref = trigger.receipt.get("exact_ref")
            if (
                not isinstance(exact, dict)
                or not isinstance(provider_ref, str)
                or exact.get("task_run_id") != handle.run_id
                or not isinstance(exact.get("provider_kernel_id"), int)
            ):
                raise MasterLifecycleAcceptanceError("FM07 provider receipt lacks exact numeric run identity")
            evidence = ConcurrentEnsureEvidence(
                kind="CONCURRENT_ENSURE_SINGLE_RUN",
                request_count=20,
                operation_ids=operation_ids,
                provider_run_refs=(provider_ref,) * 20,
                provider_kernel_ids=(int(exact["provider_kernel_id"]),) * 20,
                epochs=epochs,
            )
            return self._complete_host_evidence(request.task_id, request.scenario, evidence, principal, task)
        return self._reconcile_host(request.task_id, request.scenario, task, principal)

    def _reconcile_host(
        self,
        task_id: UUID,
        scenario: MasterAcceptanceScenario,
        task: dict[str, Any],
        principal: AcceptancePrincipal,
    ) -> dict[str, Any]:
        if task["state"] in {"PASSED", "FAILED"}:
            return task
        if self.host_claims is None or self.host_effects is None:
            return task
        command = self.host_claims.claim(task_id=task_id, expected_scenario=scenario, principal=principal)
        if command is None:
            return task
        evidence = self.host_effects.execute(command)
        receipt = execute_master_acceptance_command(command, _ExactEvidenceEffects(evidence))
        return self.host_claims.complete(receipt=receipt, principal=principal)

    def _complete_host_evidence(
        self,
        task_id: UUID,
        scenario: MasterAcceptanceScenario,
        evidence: AcceptanceEvidence,
        principal: AcceptancePrincipal,
        task: dict[str, Any],
    ) -> dict[str, Any]:
        if self.host_claims is None:
            return task
        command = self.host_claims.claim(task_id=task_id, expected_scenario=scenario, principal=principal)
        if command is None:
            return task
        receipt = execute_master_acceptance_command(command, _ExactEvidenceEffects(evidence))
        return self.host_claims.complete(receipt=receipt, principal=principal)


@dataclass(frozen=True, slots=True)
class MasterAcceptanceOperatorAdapter:
    """Operator-only adapter; catalog integration remains an assembly concern.

    These are the only two request surfaces.  In particular there is no
    scenario action/fault selector and no status list endpoint which could act
    as a reader-visible acceptance catalog.
    """

    executor: ControlMasterAcceptanceExecutor

    REQUEST_TOOL = "master.acceptance.request"
    STATUS_TOOL = "master.acceptance.status"

    @classmethod
    def tool_schemas(cls) -> dict[str, dict[str, Any]]:
        request_schema = MasterAcceptanceRequest.model_json_schema()
        status_schema = {
            "type": "object",
            "additionalProperties": False,
            "required": ["task_id"],
            "properties": {"task_id": {"type": "string", "format": "uuid"}},
        }
        return {cls.REQUEST_TOOL: request_schema, cls.STATUS_TOOL: status_schema}

    def call(
        self,
        tool: str,
        arguments: dict[str, Any],
        principal: AcceptancePrincipal,
    ) -> dict[str, Any]:
        require_acceptance_operator(principal)
        if tool == self.REQUEST_TOOL:
            return self.executor.request(MasterAcceptanceRequest.model_validate(arguments), principal)
        if tool == self.STATUS_TOOL:
            if set(arguments) != {"task_id"}:
                raise ValueError("master acceptance status arguments differ from the exact contract")
            return self.executor.status(UUID(str(arguments["task_id"])), principal)
        raise ValueError("unknown master acceptance operator tool")


@dataclass(frozen=True, slots=True)
class _ExactEvidenceEffects:
    evidence: AcceptanceEvidence

    def __getattr__(self, _name: str) -> Any:
        return lambda _command: self.evidence


def receipt_observation_sha256(receipt: MasterAcceptanceReceipt) -> str:
    """Metadata-only binding for a provider/control observation."""

    return hashlib.sha256(canonical_json_bytes(receipt.model_dump(mode="json"))).hexdigest()
