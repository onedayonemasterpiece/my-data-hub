from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from uuid import NAMESPACE_URL, uuid5

from my_data_hub.control_plane.ledger import ControlLedger, EffectState, EventDisposition, EventReceipt
from my_data_hub.runtime_sdk.events import RuntimeEvent, RuntimeEventType

from .evidence import TerminalDecision, decide_terminal
from .provider import (
    MasterRuntimeProvider,
    MasterTerminalEvidence,
    MasterTerminalQuery,
    PlannedProviderEffect,
    ProviderEffectReceipt,
    ReconciliationStatus,
)
from .state_machine import MasterSignal, MasterState, transition_master

MASTER_SERVICE_KIND = "postgres-master"


@dataclass(frozen=True, slots=True)
class MasterIntent:
    idempotency_key: str
    source_identity: str
    source_version: str
    checkpoint_ref: str
    dataset_ref: str
    notebook_ref: str

    def as_dict(self) -> dict[str, str]:
        return {
            "source_identity": self.source_identity,
            "source_version": self.source_version,
            "checkpoint_ref": self.checkpoint_ref,
            "dataset_ref": self.dataset_ref,
            "notebook_ref": self.notebook_ref,
        }


@dataclass(frozen=True, slots=True)
class MasterHandle:
    operation_id: str
    run_id: str
    attempt_id: str
    service_instance_id: str
    master_instance_id: str
    epoch: int
    state: MasterState


class MasterCoordinator:
    def __init__(
        self,
        ledger: ControlLedger,
        provider: MasterRuntimeProvider,
        *,
        lease_ttl: timedelta = timedelta(minutes=5),
    ) -> None:
        self.ledger = ledger
        self.provider = provider
        self.lease_ttl = lease_ttl

    def ensure_master(self, intent: MasterIntent, *, runtime_secret: str) -> MasterHandle:
        identity = self.identity_for(intent.idempotency_key)
        record, _ = self.ledger.ensure_operation(
            operation_id=identity["operation_id"],
            idempotency_key=intent.idempotency_key,
            operation_kind="ensure_master",
            intent=intent.as_dict(),
            initial_state=MasterState.REQUESTED.value,
            identity=identity,
            allocate_epoch_for=MASTER_SERVICE_KIND,
        )
        durable = record.identity
        self.ledger.record_attempt(
            attempt_id=str(durable["attempt_id"]),
            run_id=str(durable["run_id"]),
            operation_id=record.operation_id,
            source_identity=intent.source_identity,
            source_version=intent.source_version,
            service_instance_id=str(durable["service_instance_id"]),
            master_instance_id=str(durable["master_instance_id"]),
            epoch=int(durable["epoch"]),
            state=MasterState.REQUESTED.value,
        )
        self.ledger.store_runtime_token_hash(str(durable["run_id"]), str(durable["attempt_id"]), runtime_secret)
        self.reconcile_operation(record.operation_id, intent)
        current = self.ledger.get_operation(record.operation_id)
        assert current is not None
        return self._handle(current)

    def reconcile_operation(self, operation_id: str, intent: MasterIntent) -> MasterHandle:
        for _ in range(3):
            operation = self.ledger.get_operation(operation_id)
            if operation is None:
                raise KeyError(operation_id)
            state = MasterState(operation.state)
            step = {
                MasterState.REQUESTED: ("ensure_dataset", MasterSignal.DATASET_READY, intent.dataset_ref),
                MasterState.STARTING: ("push_notebook", MasterSignal.SOURCE_PUSHED, intent.notebook_ref),
                MasterState.RESTORING: (
                    "trigger_run",
                    MasterSignal.RUN_TRIGGERED,
                    f"{intent.notebook_ref}/run/{operation.identity['run_id']}",
                ),
            }.get(state)
            if step is None:
                evidence = self._observe_terminal(operation, intent)
                if evidence is None or not self._recover_terminal(operation, intent, evidence):
                    return self._handle(operation)
                recovered = self.ledger.get_operation(operation_id)
                assert recovered is not None
                return self._handle(recovered)
            effect_kind, signal, exact_ref = step
            receipt = self._apply_effect(operation_id, effect_kind, exact_ref, intent, operation.identity)
            if receipt is None:
                return self._handle(operation)
            transition = transition_master(state, signal)
            try:
                self.ledger.transition_operation(
                    operation_id,
                    expected_state=state.value,
                    new_state=transition.current.value,
                    metadata={"provider": receipt.provider, "effect_kind": receipt.effect_kind},
                )
            except Exception:
                latest = self.ledger.get_operation(operation_id)
                if latest is None or latest.state == state.value:
                    raise
        operation = self.ledger.get_operation(operation_id)
        assert operation is not None
        return self._handle(operation)

    def reconcile_all(self, intents: dict[str, MasterIntent]) -> list[MasterHandle]:
        handles: list[MasterHandle] = []
        operations = self.ledger.incomplete_operations("ensure_master")
        for operation in operations:
            intent = intents.get(operation.idempotency_key)
            if intent is not None:
                handles.append(self.reconcile_operation(operation.operation_id, intent))
        return handles

    def _observe_terminal(self, operation: Any, intent: MasterIntent) -> MasterTerminalEvidence | None:
        if MasterState(operation.state) not in {
            MasterState.REGISTERING,
            MasterState.ACTIVE,
            MasterState.DRAINING,
            MasterState.CHECKPOINTING,
            MasterState.CHECKPOINT_FAILED,
            MasterState.STOPPED,
        }:
            return None
        observe = getattr(self.provider, "observe_terminal", None)
        if not callable(observe):
            return None
        trigger = self.ledger.get_effect_by_idempotency_key(f"{operation.operation_id}:trigger_run")
        if trigger is None or trigger.state != EffectState.APPLIED or trigger.receipt is None:
            return None
        provider_run_identity = trigger.receipt.get("exact_identity")
        if not isinstance(provider_run_identity, dict):
            return None
        identity = operation.identity
        return observe(
            MasterTerminalQuery(
                operation_id=operation.operation_id,
                run_id=str(identity["run_id"]),
                attempt_id=str(identity["attempt_id"]),
                service_instance_id=str(identity["service_instance_id"]),
                master_instance_id=str(identity["master_instance_id"]),
                source_identity=intent.source_identity,
                source_version=intent.source_version,
                epoch=int(identity["epoch"]),
                checkpoint_ref=intent.checkpoint_ref,
                provider_run_identity=provider_run_identity,
            )
        )

    def _recover_terminal(
        self,
        operation: Any,
        intent: MasterIntent,
        evidence: MasterTerminalEvidence,
    ) -> bool:
        output = evidence.output
        decision = decide_terminal(
            platform_status=evidence.platform_status,
            output=output.exact_output() if output is not None else None,
            run_id=str(operation.identity["run_id"]),
            attempt_id=str(operation.identity["attempt_id"]),
            source_identity=intent.source_identity,
            source_version=intent.source_version,
            epoch=int(operation.identity["epoch"]),
        )
        if decision != TerminalDecision.SUCCEEDED or output is None:
            return False
        if output.service_instance_id != str(
            operation.identity["service_instance_id"]
        ) or output.master_instance_id != str(operation.identity["master_instance_id"]):
            return False
        self._require_verified_terminal_checkpoint(operation, intent, output)
        events = tuple(RuntimeEvent.model_validate_json(raw) for raw in output.recovered_events)
        expected_types = (
            RuntimeEventType.RUNTIME_DRAINING,
            RuntimeEventType.CHECKPOINT_STARTED,
            RuntimeEventType.CHECKPOINT_VERIFIED,
            RuntimeEventType.RUNTIME_TERMINAL,
        )
        if tuple(event.event_type for event in events) != expected_types:
            raise ValueError("recovered master terminal events are incomplete or out of order")
        previous_sequence = 0
        for event in events:
            if (
                event.run_id != output.run_id
                or event.attempt_id != output.attempt_id
                or event.service_instance_id != output.service_instance_id
                or event.source_identity != output.source_identity
                or event.source_version != output.source_version
                or event.epoch != output.epoch
                or event.local_sequence <= previous_sequence
            ):
                raise ValueError("recovered master terminal event identity is stale or reordered")
            previous_sequence = event.local_sequence
        draining, started, verified, terminal = events
        if (
            (draining.phase, draining.status, draining.data) != ("draining", "closed", {})
            or (started.phase, started.status, started.data) != ("checkpointing", "started", {})
            or any(event.artifact_refs or event.metrics for event in events)
            or verified.phase != "checkpointing"
            or verified.status != "verified"
            or verified.data.get("checkpoint_id") != output.checkpoint_id
            or verified.data.get("manifest_sha256") != output.manifest_sha256
            or verified.data.get("current_checkpoint_id") != output.current_checkpoint_id
            or set(verified.data) != {"checkpoint_id", "manifest_sha256", "current_checkpoint_id"}
            or terminal.phase != "stopped"
            or terminal.status != "succeeded"
            or terminal.data.get("checkpoint_id") != output.current_checkpoint_id
            or set(terminal.data) != {"checkpoint_id"}
        ):
            raise ValueError("recovered terminal events disagree with exact checkpoint output")
        for event in events:
            self._project_recovered_terminal_event(operation.operation_id, event)
        self.ledger.revoke_runtime_token(output.run_id, output.attempt_id)
        return True

    def _require_verified_terminal_checkpoint(self, operation: Any, intent: MasterIntent, output: Any) -> None:
        head = self.ledger.checkpoint_head(MASTER_SERVICE_KIND)
        candidate = self.ledger.checkpoint_candidate(output.checkpoint_id)
        if (
            head is None
            or head.current_checkpoint_id != output.current_checkpoint_id
            or candidate is None
            or candidate["status"] != "VERIFIED"
            or candidate["operation_id"] != operation.operation_id
            or candidate["dataset_ref"] != intent.checkpoint_ref
            or candidate["master_instance_id"] != output.master_instance_id
            or candidate["epoch"] != output.epoch
            or candidate["manifest_sha256"] != output.manifest_sha256
        ):
            raise ValueError("recovered terminal output is not bound to the durable verified HEAD")

    def _project_recovered_terminal_event(self, operation_id: str, event: RuntimeEvent) -> None:
        operation = self.ledger.get_operation(operation_id)
        if operation is None:
            raise KeyError(operation_id)
        state = MasterState(operation.state)
        if event.event_type == RuntimeEventType.RUNTIME_DRAINING:
            if state != MasterState.ACTIVE:
                if state in {
                    MasterState.DRAINING,
                    MasterState.CHECKPOINTING,
                    MasterState.CHECKPOINT_FAILED,
                    MasterState.STOPPED,
                }:
                    return
                raise ValueError("recovered drain evidence cannot advance this master state")
            expected, target = MasterState.ACTIVE, MasterState.DRAINING
        elif event.event_type == RuntimeEventType.CHECKPOINT_STARTED:
            if state == MasterState.CHECKPOINTING or state == MasterState.STOPPED:
                return
            if state not in {MasterState.DRAINING, MasterState.CHECKPOINT_FAILED}:
                raise ValueError("recovered checkpoint evidence cannot advance this master state")
            expected, target = state, MasterState.CHECKPOINTING
        elif event.event_type == RuntimeEventType.CHECKPOINT_VERIFIED:
            if state == MasterState.STOPPED:
                return
            if state != MasterState.CHECKPOINTING:
                raise ValueError("recovered verified evidence cannot advance this master state")
            expected, target = MasterState.CHECKPOINTING, MasterState.STOPPED
        elif event.event_type == RuntimeEventType.RUNTIME_TERMINAL:
            if state != MasterState.STOPPED:
                raise ValueError("recovered terminal event requires a stopped operation")
            expected = target = MasterState.STOPPED
        else:  # pragma: no cover - guarded by the exact event tuple above
            raise ValueError("unsupported recovered terminal event")
        service_state = (
            MasterState.STOPPED if event.event_type == RuntimeEventType.RUNTIME_TERMINAL else MasterState.DRAINING
        )
        self.ledger.project_master_lifecycle(
            operation_id=operation_id,
            service_instance_id=event.service_instance_id,
            epoch=event.epoch,
            expected_operation_state=expected.value,
            operation_state=target.value,
            service_state=service_state.value,
            event_id=event.event_id,
        )

    def accept_runtime_event(self, raw_body: bytes, *, header_token: str) -> EventReceipt:
        receipt = self.ledger.ingest_runtime_event(raw_body, header_token=header_token)
        if receipt.disposition in {EventDisposition.COALESCED, EventDisposition.FENCED}:
            return receipt
        event = RuntimeEvent.model_validate_json(raw_body)
        projected_events = {
            RuntimeEventType.SERVICE_READY,
            RuntimeEventType.RUNTIME_DRAINING,
            RuntimeEventType.CHECKPOINT_STARTED,
            RuntimeEventType.CHECKPOINT_VERIFIED,
            RuntimeEventType.CHECKPOINT_FAILED,
            RuntimeEventType.RUNTIME_TERMINAL,
        }
        if receipt.disposition == EventDisposition.DUPLICATE and event.event_type not in projected_events:
            return receipt
        if (
            receipt.disposition == EventDisposition.DUPLICATE
            and event.event_type == RuntimeEventType.RUNTIME_TERMINAL
            and not self.ledger.runtime_token_valid(event.run_id, event.attempt_id, header_token)
        ):
            # The original terminal projection committed and revoked the token,
            # but its HTTP response was lost.  The ledger authenticated this
            # exact deduplicated body against the former token hash; acknowledge
            # it without writing a second projection.
            return receipt
        operation = self._operation_for_attempt(event.run_id, event.attempt_id)
        if event.event_type == RuntimeEventType.SERVICE_READY:
            if operation.state != MasterState.REGISTERING.value:
                return receipt
            data = event.data
            required = {
                "service_kind",
                "endpoint",
                "protocol",
                "tls_fingerprint",
                "capabilities",
                "canonical_revision",
                "schema_version",
                "lease_until",
                "master_instance_id",
                "epoch",
            }
            if not required.issubset(data):
                raise ValueError("service.ready is missing required master announcement fields")
            if data["service_kind"] != MASTER_SERVICE_KIND or int(data["epoch"]) != event.epoch:
                raise ValueError("service.ready identity disagrees with its envelope")
            self.ledger.activate_service_operation(
                operation_id=operation.operation_id,
                expected_state=MasterState.REGISTERING.value,
                service_instance_id=event.service_instance_id,
                service_kind=MASTER_SERVICE_KIND,
                run_id=event.run_id,
                attempt_id=event.attempt_id,
                master_instance_id=str(data["master_instance_id"]),
                epoch=event.epoch,
                endpoint=str(data["endpoint"]),
                protocol=str(data["protocol"]),
                tls_fingerprint=str(data["tls_fingerprint"]),
                capabilities=tuple(str(value) for value in data["capabilities"]),
                canonical_revision=int(data["canonical_revision"]),
                schema_version=str(data["schema_version"]),
                lease_until=self._parse_time(str(data["lease_until"])),
                latest_event_id=event.event_id,
            )
        elif event.event_type == RuntimeEventType.RUNTIME_HEARTBEAT:
            lease_until_raw = event.data.get("lease_until")
            if lease_until_raw:
                self.ledger.renew_service(
                    event.service_instance_id,
                    event.epoch,
                    self._parse_time(str(lease_until_raw)),
                    event.event_id,
                )
        elif event.event_type == RuntimeEventType.RUNTIME_DRAINING:
            self.ledger.project_master_lifecycle(
                operation_id=operation.operation_id,
                service_instance_id=event.service_instance_id,
                epoch=event.epoch,
                expected_operation_state=MasterState.ACTIVE.value,
                operation_state=MasterState.DRAINING.value,
                service_state=MasterState.DRAINING.value,
                event_id=event.event_id,
            )
        elif event.event_type == RuntimeEventType.CHECKPOINT_STARTED:
            expected_state = (
                MasterState.CHECKPOINT_FAILED.value
                if operation.state == MasterState.CHECKPOINT_FAILED.value
                else MasterState.DRAINING.value
            )
            self.ledger.project_master_lifecycle(
                operation_id=operation.operation_id,
                service_instance_id=event.service_instance_id,
                epoch=event.epoch,
                expected_operation_state=expected_state,
                operation_state=MasterState.CHECKPOINTING.value,
                service_state=MasterState.DRAINING.value,
                event_id=event.event_id,
            )
        elif event.event_type == RuntimeEventType.CHECKPOINT_VERIFIED:
            checkpoint_id = str(event.data.get("checkpoint_id", ""))
            manifest_sha256 = str(event.data.get("manifest_sha256", ""))
            head = self.ledger.checkpoint_head(MASTER_SERVICE_KIND)
            candidate = self.ledger.checkpoint_candidate(checkpoint_id)
            if (
                head is None
                or head.current_checkpoint_id != checkpoint_id
                or candidate is None
                or candidate["status"] != "VERIFIED"
                or candidate["operation_id"] != operation.operation_id
                or candidate["master_instance_id"] != str(operation.identity["master_instance_id"])
                or candidate["epoch"] != event.epoch
                or candidate["manifest_sha256"] != manifest_sha256
                or event.data.get("current_checkpoint_id") != checkpoint_id
            ):
                raise ValueError("checkpoint.verified is not bound to the durable verified HEAD")
            self.ledger.project_master_lifecycle(
                operation_id=operation.operation_id,
                service_instance_id=event.service_instance_id,
                epoch=event.epoch,
                expected_operation_state=MasterState.CHECKPOINTING.value,
                operation_state=MasterState.STOPPED.value,
                service_state=MasterState.DRAINING.value,
                event_id=event.event_id,
            )
        elif event.event_type == RuntimeEventType.CHECKPOINT_FAILED:
            self.ledger.project_master_lifecycle(
                operation_id=operation.operation_id,
                service_instance_id=event.service_instance_id,
                epoch=event.epoch,
                expected_operation_state=MasterState.CHECKPOINTING.value,
                operation_state=MasterState.CHECKPOINT_FAILED.value,
                service_state=MasterState.DRAINING.value,
                event_id=event.event_id,
            )
        elif event.event_type == RuntimeEventType.RUNTIME_TERMINAL:
            self.ledger.project_master_lifecycle(
                operation_id=operation.operation_id,
                service_instance_id=event.service_instance_id,
                epoch=event.epoch,
                expected_operation_state=MasterState.STOPPED.value,
                operation_state=MasterState.STOPPED.value,
                service_state=MasterState.STOPPED.value,
                event_id=event.event_id,
            )
            self.ledger.revoke_runtime_token(event.run_id, event.attempt_id)
        return receipt

    def _apply_effect(
        self,
        operation_id: str,
        effect_kind: str,
        exact_ref: str,
        intent: MasterIntent,
        identity: dict[str, Any],
    ) -> ProviderEffectReceipt | None:
        key = f"{operation_id}:{effect_kind}"
        effect_id = str(uuid5(NAMESPACE_URL, key))
        exact_identity = {
            "operation_id": operation_id,
            "exact_ref": exact_ref,
            "run_id": identity["run_id"],
            "attempt_id": identity["attempt_id"],
            "source_identity": intent.source_identity,
            "source_version": intent.source_version,
            "epoch": identity["epoch"],
            "service_instance_id": identity["service_instance_id"],
            "master_instance_id": identity["master_instance_id"],
            "checkpoint_ref": intent.checkpoint_ref,
        }
        if effect_kind == "trigger_run":
            source_effect = self.ledger.get_effect_by_idempotency_key(f"{operation_id}:push_notebook")
            if source_effect is None or source_effect.state != EffectState.APPLIED:
                return None
            assert source_effect.receipt is not None
            exact_identity["notebook_launch"] = source_effect.receipt.get("exact_identity")
        effect, _ = self.ledger.plan_effect(
            effect_id=effect_id,
            operation_id=operation_id,
            idempotency_key=key,
            effect_kind=effect_kind,
            exact_identity=exact_identity,
        )
        planned = PlannedProviderEffect(
            effect.effect_id,
            effect.idempotency_key,
            effect.effect_kind,
            effect.exact_identity,
        )
        if effect.state == EffectState.APPLIED:
            assert effect.receipt is not None
            return ProviderEffectReceipt(**effect.receipt)
        if effect.state == EffectState.PLANNED:
            claimed = self.ledger.claim_effect(effect.effect_id)
            if claimed is None:
                return None
            receipt = self.provider.execute(planned)
        elif effect.state == EffectState.IN_PROGRESS:
            reconciliation = self.provider.reconcile(planned)
            if reconciliation.status == ReconciliationStatus.AMBIGUOUS:
                return None
            if reconciliation.status == ReconciliationStatus.ABSENT:
                receipt = self.provider.execute(planned)
            else:
                assert reconciliation.receipt is not None
                receipt = reconciliation.receipt
        else:
            return None
        self.ledger.complete_effect(effect.effect_id, receipt.as_dict())
        if effect_kind == "trigger_run":
            self.ledger.set_attempt_provider_run(
                str(identity["attempt_id"]), receipt.exact_ref, MasterState.REGISTERING.value
            )
        return receipt

    def _operation_for_attempt(self, run_id: str, attempt_id: str):  # type: ignore[no-untyped-def]
        operation = self.ledger.operation_for_attempt(run_id, attempt_id)
        if operation is None:
            raise KeyError((run_id, attempt_id))
        return operation

    @staticmethod
    def identity_for(idempotency_key: str) -> dict[str, str]:
        return {
            "operation_id": str(uuid5(NAMESPACE_URL, f"mdh:operation:{idempotency_key}")),
            "run_id": str(uuid5(NAMESPACE_URL, f"mdh:run:{idempotency_key}")),
            "attempt_id": str(uuid5(NAMESPACE_URL, f"mdh:attempt:{idempotency_key}:1")),
            "service_instance_id": str(uuid5(NAMESPACE_URL, f"mdh:service:{idempotency_key}:1")),
            "master_instance_id": str(uuid5(NAMESPACE_URL, f"mdh:master:{idempotency_key}:1")),
        }

    _identity = identity_for

    @staticmethod
    def _handle(operation) -> MasterHandle:  # type: ignore[no-untyped-def]
        return MasterHandle(
            operation_id=operation.operation_id,
            run_id=str(operation.identity["run_id"]),
            attempt_id=str(operation.identity["attempt_id"]),
            service_instance_id=str(operation.identity["service_instance_id"]),
            master_instance_id=str(operation.identity["master_instance_id"]),
            epoch=int(operation.identity["epoch"]),
            state=MasterState(operation.state),
        )

    @staticmethod
    def _parse_time(value: str) -> datetime:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
