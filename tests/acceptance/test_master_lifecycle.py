from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from my_data_hub.acceptance.master_lifecycle import (
    ACCEPTANCE_OPERATE_SCOPE,
    CallbackLossEvidence,
    CleanDrainEvidence,
    ConcurrentEnsureEvidence,
    EmptyBootstrapEvidence,
    LeaseExpiryEvidence,
    MasterAcceptanceBinding,
    MasterAcceptanceCommand,
    MasterAcceptanceRequest,
    OldEpochEvidence,
    RotationSoakEvidence,
    StaleReplayEvidence,
    command_for,
    execute_master_acceptance_command,
    require_acceptance_operator,
)
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger, IdempotencyConflict, StaleRuntimeEvent
from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime, MasterRuntimeSettings
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator, MasterIntent
from my_data_hub.providers.kaggle import KaggleMasterLaunchAssets
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType

SECRET = "correct-horse-battery-staple"
SOURCE_REVISION = "a" * 40


def test_request_example_and_all_generated_schemas_validate() -> None:
    root = Path(__file__).resolve().parents[2]
    example = json.loads((root / "examples/acceptance/master-lifecycle-request-fm10.v1.example.json").read_text())
    schema = json.loads((root / "schemas/acceptance/master-lifecycle-request.v1.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)
    MasterAcceptanceRequest.model_validate(example)
    for path in sorted((root / "schemas/acceptance").glob("*.schema.json")):
        Draft202012Validator.check_schema(json.loads(path.read_text()))


@dataclass(frozen=True)
class Principal:
    subject: str = "owner"
    client_id: str = "acceptance-client"
    scopes: frozenset[str] = frozenset({ACCEPTANCE_OPERATE_SCOPE})


def _request(scenario: str, *, operation_id: str | None = None) -> MasterAcceptanceRequest:
    return MasterAcceptanceRequest(
        task_id=uuid4(),
        scenario=scenario,
        idempotency_key=f"acceptance-{scenario.lower()}-{uuid4()}",
        source_revision=SOURCE_REVISION,
        target_operation_id=operation_id,
    )


def _intent(key: str) -> MasterIntent:
    return MasterIntent(
        idempotency_key=key,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="EMPTY_BASELINE",
        dataset_ref="private/checkpoint-dataset",
        notebook_ref="private/postgres-master",
    )


def _active_ledger(tmp_path: Path) -> tuple[ControlLedger, object]:
    clock = DeterministicClock(datetime(2026, 8, 11, 11, 0, tzinfo=UTC))
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(_intent("acceptance-active"), runtime_secret=SECRET)
    event = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=clock.now(),
        local_sequence=1,
        epoch=handle.epoch,
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://acceptance",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + "a" * 64,
            "capabilities": ["sql", "fts", "pgvector"],
            "canonical_revision": 0,
            "schema_version": "1",
            "lease_until": (clock.now() + timedelta(minutes=5)).isoformat(),
            "master_instance_id": handle.master_instance_id,
            "epoch": handle.epoch,
        },
    )
    coordinator.accept_runtime_event(
        event.model_dump_json(by_alias=True, exclude_none=True).encode(), header_token=SECRET
    )
    return ledger, handle


def test_request_is_closed_and_scope_is_dedicated() -> None:
    with pytest.raises(ValidationError):
        MasterAcceptanceRequest.model_validate(
            {
                **_request("FM04").model_dump(mode="json"),
                "sql": "DROP TABLE hub.project",
            }
        )
    with pytest.raises(ValidationError):
        _request("FM10")
    with pytest.raises(PermissionError):
        require_acceptance_operator(Principal(scopes=frozenset({"data:read"})))
    require_acceptance_operator(Principal())


def test_ledger_claim_is_exact_epoch_bound_and_replay_safe(tmp_path: Path) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM10", operation_id=handle.operation_id)
    stored, created = ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id=request.scenario.value,
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    assert created and stored["state"] == "BOUND"
    replay, created = ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id=request.scenario.value,
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    assert not created and replay["command"] == stored["command"]
    with pytest.raises(IdempotencyConflict):
        ledger.ensure_master_acceptance_task(
            task_id=str(uuid4()),
            scenario_id="FM10",
            idempotency_key=request.idempotency_key,
            request_sha256="b" * 64,
            principal_id="owner",
            client_id="acceptance-client",
            source_revision=request.source_revision,
            target_operation_id=handle.operation_id,
        )
    assert (
        ledger.claim_master_acceptance_command(run_id=str(uuid4()), attempt_id=handle.attempt_id, epoch=handle.epoch)
        is None
    )
    command = ledger.claim_master_acceptance_command(
        run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch
    )
    assert command is not None and command["command_kind"] == "LEASE_EXPIRY_DENIAL"


def test_authenticated_runtime_endpoint_accepts_only_exact_live_receipt(tmp_path: Path) -> None:
    ledger, handle = _active_ledger(tmp_path)
    assets = KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-assets",
        notebook_ref="owner/master-runtime",
        dataset_files={"asset.txt": b"bounded", "checkpoint-verifier.ipynb": b"{}"},
        notebook_source=b"print('master')\n",
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        runtime_token_secret_name="MDH_RUNTIME_ROOT",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
        notebook_kernel_type="script",
    )
    runtime = ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, FakeKaggleRuntime()),
        MasterRuntimeSettings(assets=assets, runtime_token_root="runtime-root-secret-long-enough"),
    )
    request = _request("FM10", operation_id=handle.operation_id)
    runtime.request_master_acceptance(request, Principal())
    app = create_app(ControlPlaneSettings(ledger_path=ledger.path), ledger=ledger, master_runtime=runtime)
    headers = {
        "Authorization": f"Bearer {SECRET}",
        "X-MDH-Master-Instance-ID": handle.master_instance_id,
        "X-MDH-Epoch": str(handle.epoch),
    }
    with TestClient(app) as client:
        claimed = client.get(
            f"/internal/runtime/master-acceptance/{handle.run_id}/{handle.attempt_id}",
            headers=headers,
        )
        assert claimed.status_code == 200
        command = MasterAcceptanceCommand.model_validate(claimed.json()["command"])
        evidence = LeaseExpiryEvidence(
            kind="LEASE_EXPIRY_DENIAL",
            observed_wait_seconds=60,
            lease_expired=True,
            bounded_operator_dml_denied=True,
            transaction_state="rollback_only",
            operator_operation_id=uuid4(),
            operator_receipt_sha256="e" * 64,
            denial_code="MDH_EPOCH_LEASE_EXPIRED",
            canonical_revision_before=0,
            canonical_revision_after=0,
        )
        receipt = execute_master_acceptance_command(
            command,
            FixedEffects(evidence),
            completed_at=datetime(2026, 8, 11, 12, 0, tzinfo=UTC),
        )
        stale = receipt.model_copy(
            update={"binding": receipt.binding.model_copy(update={"epoch": receipt.binding.epoch + 1})}
        )
        rejected = client.post(
            f"/internal/runtime/master-acceptance/{handle.run_id}/{handle.attempt_id}/receipt",
            headers=headers,
            json=stale.model_dump(mode="json"),
        )
        assert rejected.status_code == 409
        accepted = client.post(
            f"/internal/runtime/master-acceptance/{handle.run_id}/{handle.attempt_id}/receipt",
            headers=headers,
            json=receipt.model_dump(mode="json"),
        )
        assert accepted.status_code == 200
        assert accepted.json()["state"] == "PASSED"


def test_preboot_tasks_require_absent_master_and_bind_one_active_result(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "preboot.sqlite3")
    request = _request("FM07")
    task, created = ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM07",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=None,
    )
    assert created and task["state"] == "PENDING" and task["command"] is None
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(_intent("fm07-same-key"), runtime_secret=SECRET)
    clock = ledger.clock.now()
    ready = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=clock,
        local_sequence=1,
        epoch=handle.epoch,
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://fm07",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + "b" * 64,
            "capabilities": ["sql"],
            "canonical_revision": 0,
            "schema_version": "1",
            "lease_until": (clock + timedelta(minutes=5)).isoformat(),
            "master_instance_id": handle.master_instance_id,
            "epoch": handle.epoch,
        },
    )
    coordinator.accept_runtime_event(
        ready.model_dump_json(by_alias=True, exclude_none=True).encode(), header_token=SECRET
    )
    bound = ledger.bind_master_acceptance_task(task_id=str(request.task_id), operation_id=handle.operation_id)
    assert bound["state"] == "BOUND" and bound["target_epoch"] == handle.epoch
    with pytest.raises(StaleRuntimeEvent):
        second = _request("FM04")
        ledger.ensure_master_acceptance_task(
            task_id=str(second.task_id),
            scenario_id="FM04",
            idempotency_key=second.idempotency_key,
            request_sha256=second.request_sha256,
            principal_id="owner",
            client_id="acceptance-client",
            source_revision=second.source_revision,
            target_operation_id=None,
        )


def test_unclaimed_command_reaches_fixed_terminal_timeout(tmp_path: Path) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM09", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM09",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    assert isinstance(ledger.clock, DeterministicClock)
    ledger.clock.advance(1801)
    assert (
        ledger.claim_master_acceptance_command(run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch)
        is None
    )
    task = ledger.master_acceptance_task(str(request.task_id))
    assert task is not None
    assert task["state"] == "FAILED" and task["failure_code"] == "ACCEPTANCE_TIMEOUT"


class FixedEffects:
    def __init__(self, evidence: object) -> None:
        self.evidence = evidence

    def __getattr__(self, _name: str):
        return lambda _command: self.evidence


@pytest.mark.parametrize(
    ("scenario", "evidence"),
    [
        (
            "FM04",
            EmptyBootstrapEvidence(
                kind="EMPTY_MASTER_BOOTSTRAP",
                boot_source="empty_baseline",
                canonical_revision=0,
                canonical_row_count=0,
                service_active=True,
            ),
        ),
        (
            "FM07",
            ConcurrentEnsureEvidence(
                kind="CONCURRENT_ENSURE_SINGLE_RUN",
                request_count=20,
                operation_ids=(UUID(int=1),) * 20,
                provider_run_refs=("owner/run/1",) * 20,
                provider_kernel_ids=(1,) * 20,
                epochs=(1,) * 20,
            ),
        ),
        (
            "FM08",
            CallbackLossEvidence(
                kind="CALLBACK_LOSS_RECOVERY",
                callback_suppressed_once=True,
                exact_event_id=UUID(int=2),
                exact_body_sha256="a" * 64,
                control_boot_id_before=UUID(int=3),
                control_boot_id_after=UUID(int=4),
                replay_disposition="accepted",
                service_active_after_recovery=True,
            ),
        ),
        (
            "FM09",
            StaleReplayEvidence(
                kind="STALE_REPLAY_REJECTION",
                exact_event_id=UUID(int=5),
                exact_body_sha256="b" * 64,
                duplicate_disposition="duplicate",
                stale_runtime_auth_rejected=True,
                stale_epoch_rejected=True,
                state_sha256_before="c" * 64,
                state_sha256_after="c" * 64,
            ),
        ),
        (
            "FM10",
            LeaseExpiryEvidence(
                kind="LEASE_EXPIRY_DENIAL",
                observed_wait_seconds=60,
                lease_expired=True,
                bounded_operator_dml_denied=True,
                transaction_state="rollback_only",
                operator_operation_id=UUID(int=7),
                operator_receipt_sha256="e" * 64,
                denial_code="MDH_EPOCH_LEASE_EXPIRED",
                canonical_revision_before=4,
                canonical_revision_after=4,
            ),
        ),
        (
            "FM11",
            OldEpochEvidence(
                kind="OLD_EPOCH_RETURN_DENIAL",
                old_epoch=1,
                new_epoch=2,
                old_runtime_draining_before_rotation=True,
                renew_denied=True,
                register_denied=True,
                bounded_write_denied=True,
                tunnel_denied=True,
                new_epoch_active=True,
                old_operation_id=UUID(int=8),
                new_operation_id=UUID(int=9),
                handoff_checkpoint_id=UUID(int=10),
                write_denial_receipt_sha256="f" * 64,
                tunnel_denial_receipt_sha256="1" * 64,
            ),
        ),
        (
            "FM12",
            CleanDrainEvidence(
                kind="CLEAN_DRAIN",
                write_gate_closed=True,
                checkpoint_id=UUID(int=6),
                exact_version_ref="owner/checkpoint/1",
                manifest_sha256="d" * 64,
                exact_readback_verified=True,
                restore_smoke_verified=True,
                head_promoted=True,
                terminal_state="STOPPED",
            ),
        ),
        (
            "FM24",
            RotationSoakEvidence(
                kind="SESSION_ROTATION_SOAK",
                monotonic_started_ns=10,
                monotonic_finished_ns=3_600_000_000_010,
                observed_duration_seconds=3600,
                session_rotations=12,
                lease_renewals=12,
                tunnel_renewals=12,
                rejected_stale_sessions=1,
                remained_single_epoch=True,
                service_active_at_end=True,
            ),
        ),
    ],
)
def test_all_fixed_receipts_require_scenario_specific_live_proof(scenario: str, evidence: object) -> None:
    operation = uuid4()
    request = _request(scenario, operation_id=None if scenario in {"FM04", "FM07"} else str(operation))
    binding = MasterAcceptanceBinding(
        operation_id=operation,
        run_id=uuid4(),
        attempt_id=uuid4(),
        service_instance_id="service-1",
        master_instance_id=uuid4(),
        epoch=1,
    )
    command = command_for(request, binding)
    receipt = execute_master_acceptance_command(
        command, FixedEffects(evidence), completed_at=datetime(2026, 8, 11, tzinfo=UTC)
    )
    assert receipt.outcome == "succeeded"
    assert receipt.evidence_class == "live"
    assert len(json.dumps(receipt.model_dump(mode="json"))) < 64 * 1024


def test_fm10_fm11_and_fm24_cannot_overstate_partial_checks() -> None:
    with pytest.raises(ValidationError):
        LeaseExpiryEvidence(
            kind="LEASE_EXPIRY_DENIAL",
            observed_wait_seconds=59,
            lease_expired=True,
            bounded_operator_dml_denied=True,
            transaction_state="rollback_only",
            operator_operation_id=uuid4(),
            operator_receipt_sha256="e" * 64,
            denial_code="MDH_EPOCH_LEASE_EXPIRED",
            canonical_revision_before=1,
            canonical_revision_after=1,
        )
    with pytest.raises(ValidationError):
        OldEpochEvidence(
            kind="OLD_EPOCH_RETURN_DENIAL",
            old_epoch=2,
            new_epoch=2,
            old_runtime_draining_before_rotation=True,
            renew_denied=True,
            register_denied=True,
            bounded_write_denied=True,
            tunnel_denied=True,
            new_epoch_active=True,
            old_operation_id=UUID(int=8),
            new_operation_id=UUID(int=9),
            handoff_checkpoint_id=UUID(int=10),
            write_denial_receipt_sha256="f" * 64,
            tunnel_denial_receipt_sha256="1" * 64,
        )
    with pytest.raises(ValidationError):
        RotationSoakEvidence(
            kind="SESSION_ROTATION_SOAK",
            monotonic_started_ns=0,
            monotonic_finished_ns=3_599_000_000_000,
            observed_duration_seconds=3599,
            session_rotations=12,
            lease_renewals=12,
            tunnel_renewals=12,
            rejected_stale_sessions=1,
            remained_single_epoch=True,
            service_active_at_end=True,
        )
