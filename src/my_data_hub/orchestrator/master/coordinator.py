from __future__ import annotations

import hashlib
import hmac
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid5

from my_data_hub.control_plane.ledger import ControlLedger, EffectState, EventDisposition, EventReceipt
from my_data_hub.runtime_sdk.events import RuntimeEvent, RuntimeEventType
from my_data_hub.workloads.bloggers.master_stage import BloggerImportStageReceipt

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


class MasterTunnelAuthority(Protocol):
    """Host-side epoch authority; it never receives PostgreSQL or business bytes."""

    def activate(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        lease_until: datetime,
        listen_port: int,
        now: datetime,
    ) -> object: ...

    def renew(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        lease_until: datetime,
        now: datetime,
    ) -> object: ...

    def deactivate(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        reason: str,
    ) -> None: ...


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
        tunnel_authority: MasterTunnelAuthority | None = None,
        tunnel_listen_port: int = 25432,
    ) -> None:
        self.ledger = ledger
        self.provider = provider
        self.lease_ttl = lease_ttl
        self.tunnel_authority = tunnel_authority
        self.tunnel_listen_port = tunnel_listen_port

    def ensure_master(self, intent: MasterIntent, *, runtime_secret: str | None = None) -> MasterHandle:
        identity = self.identity_for(intent.idempotency_key)
        record, _ = self.ledger.ensure_master_operation(
            operation_id=identity["operation_id"],
            idempotency_key=intent.idempotency_key,
            intent=intent.as_dict(),
            identity=identity,
            service_kind=MASTER_SERVICE_KIND,
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
        if runtime_secret is not None:
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
                latest = self.ledger.get_operation(operation_id)
                assert latest is not None
                return self._handle(latest)
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
                try:
                    handle = self.reconcile_operation(operation.operation_id, intent)
                except Exception as exc:
                    # Provider failures are operation-scoped.  Persist bounded,
                    # non-secret evidence without terminalizing an ambiguous
                    # mutation, then continue reconciling independent work.
                    # Any claimed mutation remains IN_PROGRESS, which forces
                    # the next pass through exact provider reconciliation
                    # before another execution.
                    current = self.ledger.get_operation(operation.operation_id)
                    if current is None:
                        raise
                    self.ledger.transition_operation(
                        operation.operation_id,
                        expected_state=current.state,
                        new_state=current.state,
                        metadata={
                            "schema_version": "my-data-hub-master-reconciliation-failure.v1",
                            "code": "MASTER_RECONCILIATION_EXCEPTION",
                            "exception_type": type(exc).__name__,
                            "recovery": "EXACT_EFFECT_RECONCILIATION_REQUIRED",
                        },
                    )
                    latest = self.ledger.get_operation(operation.operation_id)
                    assert latest is not None
                    handle = self._handle(latest)
                handles.append(handle)
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
        if decision is TerminalDecision.FAILED:
            if MasterState(operation.state) in {MasterState.FAILED, MasterState.FENCED, MasterState.ORPHANED}:
                return True
            event_id = str(uuid5(NAMESPACE_URL, f"provider-terminal-error:{operation.operation_id}"))
            self._deactivate_tunnel_authority(operation.identity, "provider_terminal_failed")
            self.ledger.project_master_terminal_failure(
                operation_id=operation.operation_id,
                run_id=str(operation.identity["run_id"]),
                attempt_id=str(operation.identity["attempt_id"]),
                service_instance_id=str(operation.identity["service_instance_id"]),
                epoch=int(operation.identity["epoch"]),
                expected_operation_state=operation.state,
                event_id=event_id,
            )
            return True
        if decision != TerminalDecision.SUCCEEDED or output is None:
            return False
        if output.service_instance_id != str(
            operation.identity["service_instance_id"]
        ) or output.master_instance_id != str(operation.identity["master_instance_id"]):
            return False
        self._require_verified_terminal_checkpoint(operation, intent, output)
        if output.blogger_import_receipt is not None:
            blogger_receipt = BloggerImportStageReceipt.model_validate(output.blogger_import_receipt)
            if (
                str(blogger_receipt.operation_id) != operation.operation_id
                or blogger_receipt.run_id != output.run_id
                or blogger_receipt.epoch != output.epoch
                or str(blogger_receipt.master_instance_id) != output.master_instance_id
            ):
                raise ValueError("recovered blogger receipt differs from the exact master operation")
            self.ledger.record_blogger_import_receipt(
                request_id=str(blogger_receipt.request_id),
                run_id=output.run_id,
                attempt_id=output.attempt_id,
                receipt=blogger_receipt.model_dump(mode="json"),
            )
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
        if len({event.event_id for event in events}) != len(events):
            raise ValueError("recovered master terminal event IDs are not unique")
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
            or terminal.data.get("executed_source_sha256") != output.executed_source_sha256
            or set(terminal.data) != {"checkpoint_id", "executed_source_sha256"}
        ):
            raise ValueError("recovered terminal events disagree with exact checkpoint output")
        self.ledger.record_master_terminal_recovery_evidence(
            operation_id=operation.operation_id,
            epoch=output.epoch,
            output_receipt_sha256=output.output_receipt_sha256,
            provider_status=evidence.platform_status.value,
            metadata={
                "schema_version": "my-data-hub-master-terminal-recovery-evidence.v1",
                "run_id": output.run_id,
                "attempt_id": output.attempt_id,
                "service_instance_id": output.service_instance_id,
                "master_instance_id": output.master_instance_id,
                "source_identity": output.source_identity,
                "source_version": output.source_version,
                "checkpoint_id": output.checkpoint_id,
                "manifest_sha256": output.manifest_sha256,
                "output_tree_sha256": output.output_tree_sha256,
                "output_receipt_sha256": output.output_receipt_sha256,
                "provider_status": evidence.platform_status.value,
                "blogger_import_receipt_sha256": (
                    BloggerImportStageReceipt.model_validate(output.blogger_import_receipt).receipt_sha256
                    if output.blogger_import_receipt is not None
                    else None
                ),
                "events": [
                    {
                        "event_id": event.event_id,
                        "body_sha256": hashlib.sha256(raw).hexdigest(),
                    }
                    for event, raw in zip(events, output.recovered_events, strict=True)
                ],
            },
        )
        self._deactivate_tunnel_authority(operation.identity, "provider_terminal_recovered")
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
            RuntimeEventType.RUNTIME_HEARTBEAT,
            RuntimeEventType.RESOURCE_ACQUIRE,
            RuntimeEventType.RESOURCE_RENEW,
            RuntimeEventType.RESOURCE_RELEASE,
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
            expected_source_sha256 = self._expected_executed_source_sha256(operation.operation_id)
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
            observed_source_sha256 = str(data.get("executed_source_sha256", ""))
            if expected_source_sha256 is not None and not hmac.compare_digest(
                expected_source_sha256, observed_source_sha256
            ):
                raise ValueError("service.ready executed source differs from exact provider push")
            self._require_admitted_boot_checkpoint(operation.operation_id, data)
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
                lease_until = self._parse_time(str(lease_until_raw))
                self.ledger.renew_service(
                    event.service_instance_id,
                    event.epoch,
                    lease_until,
                    event.event_id,
                )
                self._renew_tunnel_authority(event, operation.identity, lease_until)
                if event.data.get("resource") is not None:
                    lease = self._exact_status_resource_lease(operation.operation_id, event)
                    self.ledger.renew_resource_lease(
                        str(lease["lease_id"]),
                        str(lease["holder_id"]),
                        int(lease["epoch"]),
                        lease_until,
                    )
        elif event.event_type in {
            RuntimeEventType.RESOURCE_ACQUIRE,
            RuntimeEventType.RESOURCE_RENEW,
            RuntimeEventType.RESOURCE_RELEASE,
        }:
            lease = self._exact_status_resource_lease(operation.operation_id, event)
            if event.event_type == RuntimeEventType.RESOURCE_RENEW:
                lease_until_raw = event.data.get("lease_until")
                if not isinstance(lease_until_raw, str):
                    raise ValueError("resource renewal lacks an exact deadline")
                self.ledger.renew_resource_lease(
                    str(lease["lease_id"]),
                    str(lease["holder_id"]),
                    int(lease["epoch"]),
                    self._parse_time(lease_until_raw),
                )
            elif event.event_type == RuntimeEventType.RESOURCE_RELEASE:
                self.ledger.release_resource_lease_exact(
                    str(lease["lease_id"]), str(lease["holder_id"]), int(lease["epoch"])
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
            expected_source_sha256 = self._expected_executed_source_sha256(operation.operation_id)
            observed_source_sha256 = str(event.data.get("executed_source_sha256", ""))
            if expected_source_sha256 is not None and not hmac.compare_digest(
                expected_source_sha256, observed_source_sha256
            ):
                raise ValueError("runtime.terminal executed source differs from exact provider push")
            self._deactivate_tunnel_authority(operation.identity, "runtime_terminal")
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

    def _require_admitted_boot_checkpoint(self, operation_id: str, data: dict[str, Any]) -> None:
        authority = self.ledger.master_status_dataset_authority(operation_id)
        if authority is None:
            return
        status_dataset = authority.get("status_dataset")
        expected = status_dataset.get("boot_checkpoint") if isinstance(status_dataset, dict) else None
        expected_tls_sha256 = (
            str(status_dataset.get("tls_certificate_sha256", "")) if isinstance(status_dataset, dict) else ""
        )
        observed_tls_sha256 = str(data.get("tls_fingerprint", ""))
        if observed_tls_sha256.startswith("sha256:"):
            observed_tls_sha256 = observed_tls_sha256.removeprefix("sha256:")
        if (
            len(expected_tls_sha256) != 64
            or len(observed_tls_sha256) != 64
            or not hmac.compare_digest(expected_tls_sha256, observed_tls_sha256)
        ):
            raise ValueError("service.ready TLS certificate differs from admitted status Dataset")
        observed = data.get("boot_checkpoint")
        if not isinstance(expected, dict) or observed != expected:
            raise ValueError("service.ready boot checkpoint differs from admitted status Dataset")
        head = self.ledger.checkpoint_head(MASTER_SERVICE_KIND)
        if expected.get("kind") == "EMPTY":
            if expected != {"kind": "EMPTY", "generation": 0} or (
                head is not None and head.current_checkpoint_id is not None
            ):
                raise ValueError("service.ready EMPTY checkpoint is no longer current")
            return
        checkpoint_id = str(expected.get("checkpoint_id", ""))
        candidate = self.ledger.checkpoint_candidate(checkpoint_id)
        if (
            expected.get("kind") != "VERIFIED"
            or head is None
            or head.generation != int(expected.get("generation", -1))
            or head.current_checkpoint_id != checkpoint_id
            or candidate is None
            or candidate.get("status") != "VERIFIED"
            or candidate.get("version_ref") != expected.get("exact_version_ref")
            or candidate.get("manifest_sha256") != expected.get("manifest_sha256")
        ):
            raise ValueError("service.ready verified checkpoint is no longer current")

    def _exact_status_resource_lease(self, operation_id: str, event: RuntimeEvent) -> dict[str, Any]:
        resource = event.data.get("resource")
        authority = self.ledger.master_status_dataset_authority(operation_id)
        if not isinstance(resource, dict) or authority is None:
            raise ValueError("runtime resource event lacks a durable status authority")
        expected = authority.get("resource_lease")
        if not isinstance(expected, dict):
            raise ValueError("runtime resource event lacks a durable resource lease")
        string_fields = ("lease_id", "resource_kind", "resource_ref", "holder_id")
        if any(str(resource.get(key, "")) != str(expected.get(key, "")) for key in string_fields):
            raise ValueError("runtime resource event differs from the owner-task lease")
        try:
            observed_epoch = int(resource.get("epoch"))
            expected_epoch = int(expected.get("epoch"))
        except (TypeError, ValueError) as exc:
            raise ValueError("runtime resource event has an invalid fencing epoch") from exc
        if observed_epoch != expected_epoch or str(expected["holder_id"]) != event.run_id:
            raise ValueError("runtime resource event is stale or cross-task")
        return expected

    def _expected_executed_source_sha256(self, operation_id: str) -> str | None:
        """Return the exact push response digest for an official Kaggle run.

        Legacy deterministic test providers predate source identities; only
        the official adapter is admitted to production and must always carry
        its exact push response through the trigger receipt.
        """

        trigger = self.ledger.get_effect_by_idempotency_key(f"{operation_id}:trigger_run")
        if trigger is None or trigger.receipt is None:
            raise ValueError("runtime callback has no durable trigger receipt")
        identity = trigger.receipt.get("exact_identity")
        expected = identity.get("source_sha256") if isinstance(identity, dict) else None
        if isinstance(expected, str) and len(expected) == 64 and set(expected) <= set("0123456789abcdef"):
            return expected
        if trigger.receipt.get("provider") == "kaggle":
            raise ValueError("official Kaggle trigger lacks exact source attestation")
        return None

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
        operation = self.ledger.get_operation(operation_id)
        if operation is None:
            raise KeyError(operation_id)
        exact_identity = {
            "operation_id": operation_id,
            "operation_requested_at": operation.created_at.isoformat(),
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
        if effect_kind == "push_notebook":
            dataset_effect = self.ledger.get_effect_by_idempotency_key(f"{operation_id}:ensure_dataset")
            if dataset_effect is None or dataset_effect.state != EffectState.APPLIED:
                return None
            assert dataset_effect.receipt is not None
            exact_identity["asset_dataset"] = dataset_effect.receipt.get("exact_identity")
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
            if effect_kind == "trigger_run":
                self._activate_tunnel_authority(identity)
            # A transport exception may occur after Kaggle accepted the run.
            # Keep the short broker lease until exact reconciliation or expiry;
            # deactivating here would strand a legitimately started notebook.
            receipt = self.provider.execute(planned)
        elif effect.state == EffectState.IN_PROGRESS:
            reconciliation = self.provider.reconcile(planned)
            if reconciliation.status == ReconciliationStatus.AMBIGUOUS:
                return None
            if reconciliation.status == ReconciliationStatus.ABSENT:
                if effect_kind == "trigger_run":
                    if self.ledger.clock.now() >= effect.updated_at + self.lease_ttl:
                        # The broker's same-epoch lease is a one-way safety
                        # boundary.  Retrying activation after this deadline
                        # can never be valid, so terminalize the ABSENT attempt
                        # instead of stranding admission behind IN_PROGRESS.
                        with suppress(Exception):
                            self._deactivate_tunnel_authority(identity, "trigger_absent_after_lease_expiry")
                        # An already-expired/reconciled broker may have no
                        # matching active lease to deactivate.  The durable
                        # high-water mark still prevents revival.
                        self.ledger.fail_unstarted_master_after_tunnel_expiry(
                            operation_id=operation_id,
                            effect_id=effect.effect_id,
                            run_id=str(identity["run_id"]),
                            attempt_id=str(identity["attempt_id"]),
                            service_instance_id=str(identity["service_instance_id"]),
                            epoch=int(identity["epoch"]),
                        )
                        return None
                    self._activate_tunnel_authority(identity)
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

    def _activate_tunnel_authority(self, identity: dict[str, Any]) -> None:
        authority = self.tunnel_authority
        if authority is None:
            return
        now = self.ledger.clock.now()
        authority.activate(
            master_instance_id=str(identity["master_instance_id"]),
            run_id=str(identity["run_id"]),
            attempt_id=str(identity["attempt_id"]),
            epoch=int(identity["epoch"]),
            lease_until=now + self.lease_ttl,
            listen_port=self.tunnel_listen_port,
            now=now,
        )

    def _renew_tunnel_authority(self, event: RuntimeEvent, identity: dict[str, Any], lease_until: datetime) -> None:
        authority = self.tunnel_authority
        if authority is None:
            return
        authority.renew(
            master_instance_id=str(identity["master_instance_id"]),
            run_id=event.run_id,
            attempt_id=event.attempt_id,
            epoch=event.epoch,
            lease_until=lease_until,
            now=self.ledger.clock.now(),
        )

    def deactivate_terminal_operation(self, operation_id: str, reason: str) -> None:
        """Revoke tunnel authority only for an already durable terminal operation."""

        if reason != "fm08_abrupt_master_terminated":
            raise ValueError("terminal tunnel deactivation reason is not task-owned")
        operation = self.ledger.get_operation(operation_id)
        if operation is None or MasterState(operation.state) not in {
            MasterState.FENCED,
            MasterState.FAILED,
            MasterState.ORPHANED,
            MasterState.STOPPED,
        }:
            raise ValueError("tunnel authority cannot be revoked before durable terminal fencing")
        self._deactivate_tunnel_authority(operation.identity, reason)

    def _deactivate_tunnel_authority(self, identity: dict[str, Any], reason: str) -> None:
        authority = self.tunnel_authority
        if authority is None:
            return
        authority.deactivate(
            master_instance_id=str(identity["master_instance_id"]),
            run_id=str(identity["run_id"]),
            attempt_id=str(identity["attempt_id"]),
            epoch=int(identity["epoch"]),
            reason=reason,
        )

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
