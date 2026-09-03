from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger, EffectState, EventDisposition, OperationRecord
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator, MasterIntent, MasterState
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType

RUNTIME_SECRET = "a" * 64
NOW = datetime(2026, 8, 25, 20, tzinfo=UTC)


def _intent() -> MasterIntent:
    return MasterIntent(
        idempotency_key="bootstrap-callback-before-trigger-receipt",
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="EMPTY_BASELINE",
        dataset_ref="private/checkpoint-dataset",
        notebook_ref="private/postgres-master",
    )


def _trigger_in_progress(
    tmp_path: Path,
) -> tuple[MasterCoordinator, ControlLedger, OperationRecord, dict[str, object]]:
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=DeterministicClock(NOW))
    coordinator = MasterCoordinator(
        ledger,
        FakeKaggleRuntime({"trigger_run": [RuntimeError("trigger response pending")]}),
    )
    request = _intent()
    with pytest.raises(RuntimeError, match="trigger response pending"):
        coordinator.ensure_master(request, runtime_secret=RUNTIME_SECRET)

    identity = MasterCoordinator.identity_for(request.idempotency_key)
    operation = ledger.get_operation(identity["operation_id"])
    assert operation is not None and operation.state == MasterState.RESTORING.value
    trigger = ledger.get_effect_by_idempotency_key(f"{operation.operation_id}:trigger_run")
    assert trigger is not None and trigger.state is EffectState.IN_PROGRESS and trigger.receipt is None

    lease = ledger.acquire_resource_lease(
        lease_id="bootstrap-notebook-lease",
        resource_kind="kaggle_notebook",
        resource_ref=request.notebook_ref,
        holder_id=str(operation.identity["run_id"]),
        lease_until=NOW + timedelta(minutes=5),
    )
    resource = {
        "lease_id": lease.lease_id,
        "resource_kind": lease.resource_kind,
        "resource_ref": lease.resource_ref,
        "holder_id": lease.holder_id,
        "epoch": lease.epoch,
        "lease_until": lease.lease_until.isoformat(),
    }
    ledger.ensure_master_status_dataset_authority(
        operation_id=operation.operation_id,
        run_id=str(operation.identity["run_id"]),
        attempt_id=str(operation.identity["attempt_id"]),
        token=RUNTIME_SECRET,
        creator_claim_until=NOW + timedelta(minutes=5),
        expected_content_tree_sha256="b" * 64,
        resource_lease=resource,
    )
    return coordinator, ledger, operation, resource


def _event(operation: OperationRecord, event_type: RuntimeEventType, sequence: int, **data: object) -> bytes:
    identity = operation.identity
    body: str = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=str(identity["run_id"]),
        attempt_id=str(identity["attempt_id"]),
        service_instance_id=str(identity["service_instance_id"]),
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        event_type=event_type,
        emitted_at=NOW,
        local_sequence=sequence,
        epoch=int(identity["epoch"]),
        data=dict(data),
    ).model_dump_json(by_alias=True, exclude_none=True)
    return body.encode()


def test_bootstrap_callbacks_ack_before_trigger_receipt_but_ready_requires_it(tmp_path: Path) -> None:
    coordinator, _ledger, operation, resource = _trigger_in_progress(tmp_path)

    for sequence, event_type, data in (
        (1, RuntimeEventType.RUNTIME_STARTED, {}),
        (2, RuntimeEventType.RUNTIME_PROGRESS, {"step": "postgres_bootstrap"}),
        (3, RuntimeEventType.RESOURCE_ACQUIRE, {"resource": resource}),
    ):
        receipt = coordinator.accept_runtime_event(
            _event(operation, event_type, sequence, **data),
            header_token=RUNTIME_SECRET,
        )
        assert receipt.disposition is EventDisposition.ACCEPTED

    with pytest.raises(ValueError, match="no durable trigger receipt"):
        coordinator.accept_runtime_event(
            _event(
                operation,
                RuntimeEventType.SERVICE_READY,
                4,
                service_kind="postgres-master",
                endpoint="tunnel://bootstrap",
                protocol="postgresql+tls",
                tls_fingerprint="sha256:" + "c" * 64,
                capabilities=["sql"],
                canonical_revision=0,
                schema_version="1",
                lease_until=(NOW + timedelta(minutes=5)).isoformat(),
                master_instance_id=str(operation.identity["master_instance_id"]),
                epoch=int(operation.identity["epoch"]),
                executed_source_sha256="d" * 64,
            ),
            header_token=RUNTIME_SECRET,
        )
