from __future__ import annotations

import hashlib
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
    MasterAcceptanceReceipt,
    MasterAcceptanceRequest,
    OldEpochEvidence,
    RotationSoakEvidence,
    StaleReplayEvidence,
    command_for,
    execute_master_acceptance_command,
    require_acceptance_operator,
)
from my_data_hub.acceptance.master_production import ControlLedgerStoredReplay
from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger, IdempotencyConflict, StaleRuntimeEvent
from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime, MasterRuntimeSettings
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator, MasterIntent
from my_data_hub.orchestrator.master.provider import ProviderEffectReceipt
from my_data_hub.providers.kaggle import KaggleMasterLaunchAssets
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType
from my_data_hub.runtime_sdk.transport import json_body

SECRET = "a" * 64
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


def test_fm24_live_receipt_example_requires_exact_checkpoint_recovery_hashes() -> None:
    root = Path(__file__).resolve().parents[2]
    example = json.loads((root / "examples/acceptance/master-lifecycle-receipt-fm24.v1.example.json").read_text())
    schema = json.loads((root / "schemas/acceptance/master-lifecycle-receipt.v1.schema.json").read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)
    receipt = MasterAcceptanceReceipt.model_validate(example)
    assert receipt.evidence.checkpoint_verified is True
    assert receipt.evidence.recovery_succeeded is True


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


def _control_runtime(ledger: ControlLedger) -> ControlPlaneMasterRuntime:
    assets = KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-assets",
        notebook_ref="owner/master-runtime",
        dataset_files={
            "asset.txt": b"bounded",
            "checkpoint-verifier.ipynb": b"{}",
            "postgresql-18-runtime.tar.gz": b"fake-postgresql-18-runtime",
            "postgresql-18-runtime.json": b"""{"archive_sha256":"63a988449f3d37c9c9fd2658b14f9254918e0b0f8ac600f9b98f15ede09e912f","build_recipe_sha256":"3fbcf52450dd44e3eb0eb7b826ebdb84a4293fbc54b713408083f10b44964d61","builder_image":"ubuntu:22.04@sha256:3b06811b2afd352be909dd088a004166d665dc76d38b13eada33522a9d915c6f","pgvector_source_sha256":"10bf9938906e5d643bbc4a7eea104b6f57ba4898e5b76b20e60484ea1d5a7f8f","pgvector_source_url":"https://github.com/pgvector/pgvector/archive/refs/tags/v0.8.6.tar.gz","pgvector_version":"0.8.6","platform":"linux-x86_64","postgresql_source_sha256":"81a81ec695fb0c7901407defaa1d2f7973617154cf27ba74e3a7ab8e64436094","postgresql_source_url":"https://ftp.postgresql.org/pub/source/v18.4/postgresql-18.4.tar.bz2","postgresql_version":"18.4","schema_version":"my-data-hub-postgresql-runtime.v1"}""",
            "tunnel-known-hosts": b"|1|aaaa|bbbb ssh-ed25519 AAAA\n",
        },
        notebook_source=b"print('master')\n",
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
        tunnel_gateway_host="gateway.example.test",
        tunnel_gateway_port=22,
        tunnel_gateway_user="mdh_tunnel",
        tunnel_remote_port=25432,
        notebook_kernel_type="script",
    )
    return ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, FakeKaggleRuntime()),
        MasterRuntimeSettings(assets=assets),
    )


class _ExactRunIdentityFake(FakeKaggleRuntime):
    def execute(self, effect) -> ProviderEffectReceipt:
        receipt = super().execute(effect)
        if effect.effect_kind != "trigger_run":
            return receipt
        source_version = 7
        provider_ref = "private/postgres-master"
        return ProviderEffectReceipt(
            provider=receipt.provider,
            effect_kind=receipt.effect_kind,
            exact_ref=f"{provider_ref}/{source_version}",
            source_identity=receipt.source_identity,
            source_version=receipt.source_version,
            exact_identity={
                "task_run_id": effect.exact_identity["run_id"],
                "provider_ref": provider_ref,
                "source_version": source_version,
                "source_sha256": "f" * 64,
                "provider_kernel_id": 71,
                "provider_run_ref": f"{provider_ref}/{source_version}",
                "started_at": "2026-08-11T11:00:00Z",
            },
        )


def _activate_ledger(
    ledger: ControlLedger,
    key: str = "acceptance-active",
    *,
    exact_run_identity: bool = False,
) -> object:
    provider = _ExactRunIdentityFake() if exact_run_identity else FakeKaggleRuntime()
    coordinator = MasterCoordinator(ledger, provider)
    handle = coordinator.ensure_master(_intent(key), runtime_secret=SECRET)
    event = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=ledger.clock.now(),
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
            "lease_until": (ledger.clock.now() + timedelta(minutes=5)).isoformat(),
            "master_instance_id": handle.master_instance_id,
            "epoch": handle.epoch,
            **({"executed_source_sha256": "f" * 64} if exact_run_identity else {}),
        },
    )
    coordinator.accept_runtime_event(
        event.model_dump_json(by_alias=True, exclude_none=True).encode(), header_token=SECRET
    )
    return handle


def _active_ledger(tmp_path: Path) -> tuple[ControlLedger, object]:
    clock = DeterministicClock(datetime(2026, 8, 11, 11, 0, tzinfo=UTC))
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=clock)
    handle = _activate_ledger(ledger, exact_run_identity=True)
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
    with pytest.raises(StaleRuntimeEvent):
        ledger.claim_master_acceptance_host_command(
            task_id=str(request.task_id),
            expected_scenario="FM10",
            principal_id="another-owner",
            client_id="acceptance-client",
        )
    command = ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario="FM10",
        principal_id="owner",
        client_id="acceptance-client",
    )
    assert command is not None and command["command_kind"] == "LEASE_EXPIRY_DENIAL"
    assert (
        ledger.claim_master_acceptance_command(run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch)
        is None
    )
    parsed = MasterAcceptanceCommand.model_validate(command)
    receipt = execute_master_acceptance_command(
        parsed,
        FixedEffects(
            LeaseExpiryEvidence(
                kind="LEASE_EXPIRY_DENIAL",
                observed_wait_seconds=60,
                lease_expired=True,
                credentials_invalidated=True,
                bounded_operator_dml_denied=True,
                transaction_state="rollback_only",
                operator_operation_id=uuid4(),
                operator_receipt_sha256="e" * 64,
                denial_code="MDH_EPOCH_LEASE_EXPIRED",
                canonical_revision_before=0,
                canonical_revision_after=0,
            )
        ),
    )
    with pytest.raises(StaleRuntimeEvent):
        ledger.complete_master_acceptance_host_command(
            command_id=str(receipt.command_id),
            command_sha256=receipt.command_sha256,
            principal_id="another-owner",
            client_id="acceptance-client",
            receipt=receipt.model_dump(mode="json"),
        )
    completed = ledger.complete_master_acceptance_host_command(
        command_id=str(receipt.command_id),
        command_sha256=receipt.command_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        receipt=receipt.model_dump(mode="json"),
    )
    assert completed["state"] == "PASSED"
    assert completed["operation_id"] == handle.operation_id
    assert completed["command"]["receipt"] == receipt.model_dump(mode="json")
    assert completed["command"]["receipt_sha256"] == receipt.receipt_sha256
    assert completed["provider_carrier"] == {
        "provider_ref": "private/postgres-master",
        "provider_run_ref": "private/postgres-master/7",
        "provider_kernel_id": 71,
        "source_version": 7,
        "source_sha256": "f" * 64,
        "output_file_name": None,
        "output_file_sha256": None,
        "output_tree_sha256": None,
        "output_receipt_sha256": None,
    }
    ledger.record_master_terminal_recovery_evidence(
        operation_id=handle.operation_id,
        epoch=handle.epoch,
        output_receipt_sha256="9" * 64,
        provider_status="complete",
        metadata={
            "schema_version": "my-data-hub-master-terminal-recovery-evidence.v1",
            "run_id": handle.run_id,
            "attempt_id": handle.attempt_id,
            "service_instance_id": handle.service_instance_id,
            "master_instance_id": handle.master_instance_id,
            "source_identity": "my-data-hub/postgres-master",
            "source_version": "git:0123456789abcdef",
            "checkpoint_id": str(UUID(int=99)),
            "manifest_sha256": "7" * 64,
            "output_tree_sha256": "8" * 64,
            "output_receipt_sha256": "9" * 64,
            "provider_status": "complete",
            "events": [
                {"event_id": f"terminal-event-{index}", "body_sha256": str(index) * 64} for index in range(1, 5)
            ],
        },
    )
    observed = ledger.master_acceptance_task(str(request.task_id))
    assert observed is not None
    assert observed["provider_carrier"] == {
        "provider_ref": "private/postgres-master",
        "provider_run_ref": "private/postgres-master/7",
        "provider_kernel_id": 71,
        "source_version": 7,
        "source_sha256": "f" * 64,
        "output_file_name": "my-data-hub-master-terminal.json",
        "output_file_sha256": "9" * 64,
        "output_tree_sha256": "8" * 64,
        "output_receipt_sha256": "9" * 64,
    }


def test_authenticated_runtime_endpoint_accepts_only_exact_live_receipt(tmp_path: Path) -> None:
    ledger = ControlLedger(
        tmp_path / "runtime-fm04.sqlite3",
        clock=DeterministicClock(datetime(2026, 8, 11, 11, 0, tzinfo=UTC)),
    )
    runtime = _control_runtime(ledger)
    request = _request("FM04")
    runtime.request_master_acceptance(request, Principal())
    handle = _activate_ledger(ledger, f"master-acceptance-fm04:{request.task_id}")
    runtime.bind_master_acceptance(str(request.task_id), handle.operation_id)
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
        evidence = EmptyBootstrapEvidence(
            kind="EMPTY_MASTER_BOOTSTRAP",
            boot_source="empty_baseline",
            canonical_revision=0,
            canonical_row_count=0,
            service_active=True,
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
        ledger.claim_master_acceptance_host_command(
            task_id=str(request.task_id),
            expected_scenario="FM09",
            principal_id="owner",
            client_id="acceptance-client",
        )
        is None
    )
    task = ledger.master_acceptance_task(str(request.task_id))
    assert task is not None
    assert task["state"] == "FAILED" and task["failure_code"] == "ACCEPTANCE_TIMEOUT"


def test_owner_claim_exposes_only_exact_fm11_fm12_drain_directive(tmp_path: Path) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM12", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM12",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    assert (
        ledger.master_acceptance_drain_directive(run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch)
        is None
    )
    ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario="FM12",
        principal_id="owner",
        client_id="acceptance-client",
    )
    directive = ledger.master_acceptance_drain_directive(
        run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch
    )
    assert directive is not None
    assert directive["scenario_id"] == "FM12" and directive["task_id"] == str(request.task_id)
    assert (
        ledger.master_acceptance_drain_directive(
            run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch + 1
        )
        is None
    )
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=_control_runtime(ledger),
    )
    headers = {
        "Authorization": f"Bearer {SECRET}",
        "X-MDH-Master-Instance-ID": handle.master_instance_id,
        "X-MDH-Epoch": str(handle.epoch),
    }
    with TestClient(app) as client:
        response = client.get(
            f"/internal/runtime/master-acceptance/{handle.run_id}/{handle.attempt_id}/drain-directive",
            headers=headers,
        )
        assert response.status_code == 200
        assert response.json()["drain"] is True
        assert response.json()["directive"]["task_id"] == str(request.task_id)


@pytest.mark.parametrize("scenario", ["FM08", "FM10"])
def test_runtime_control_directive_is_exact_owner_claim_bound(tmp_path: Path, scenario: str) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request(scenario, operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id=scenario,
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    payload = ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario=scenario,
        principal_id="owner",
        client_id="acceptance-client",
    )
    assert payload is not None
    command = MasterAcceptanceCommand.model_validate(payload)
    arguments = {
        "task_id": str(command.task_id),
        "command_id": str(command.command_id),
        "command_sha256": command.command_sha256,
        "run_id": str(command.binding.run_id),
        "attempt_id": str(command.binding.attempt_id),
        "master_instance_id": str(command.binding.master_instance_id),
        "epoch": command.binding.epoch,
    }
    if scenario == "FM10":
        control = ledger.suspend_master_acceptance_renewal(**arguments)
        assert control["renewal_suspended"] == 1 and control["renewal_acknowledged"] == 0
        ledger.acknowledge_master_acceptance_renewal_suspension(
            run_id=arguments["run_id"],
            attempt_id=arguments["attempt_id"],
            master_instance_id=arguments["master_instance_id"],
            epoch=command.binding.epoch,
        )
        assert ledger.master_acceptance_runtime_control(str(command.task_id))["renewal_acknowledged"] == 1  # type: ignore[index]
    else:
        before_boot_id = str(uuid4())
        control = ledger.arm_master_acceptance_callback_loss(**arguments, before_boot_id=before_boot_id)
        assert control["callback_state"] == "ARMED"
        event_id = str(uuid4())
        ledger.capture_master_acceptance_callback(task_id=str(command.task_id), event_id=event_id, body_sha256="b" * 64)
        before, after = before_boot_id, str(uuid4())
        ledger.record_master_acceptance_restart(
            task_id=str(command.task_id), restart_from_id=before, restart_to_id=after
        )
        ledger.mark_master_acceptance_callback_replayed(
            task_id=str(command.task_id), event_id=event_id, body_sha256="b" * 64
        )
        assert ledger.master_acceptance_runtime_control(str(command.task_id))["callback_state"] == "REPLAYED"  # type: ignore[index]
    exact = ledger.master_acceptance_runtime_directive(
        run_id=arguments["run_id"],
        attempt_id=arguments["attempt_id"],
        master_instance_id=arguments["master_instance_id"],
        epoch=command.binding.epoch,
    )
    assert exact["available"] is True and exact["scenario_id"] == scenario
    stale = ledger.master_acceptance_runtime_directive(
        run_id=arguments["run_id"],
        attempt_id=arguments["attempt_id"],
        master_instance_id=arguments["master_instance_id"],
        epoch=command.binding.epoch + 1,
    )
    assert stale == {
        "available": False,
        "renewal_suspended": False,
        "soak_requested_step": 0,
        "soak_completed_step": 0,
    }


def test_fm08_app_persists_exact_heartbeat_but_suppresses_projection_and_ack(
    tmp_path: Path,
) -> None:
    ledger, handle = _active_ledger(tmp_path)
    runtime = _control_runtime(ledger)
    request = _request("FM08", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM08",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    payload = ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario="FM08",
        principal_id="owner",
        client_id="acceptance-client",
    )
    assert payload is not None
    command = MasterAcceptanceCommand.model_validate(payload)
    ledger.arm_master_acceptance_callback_loss(
        task_id=str(command.task_id),
        command_id=str(command.command_id),
        command_sha256=command.command_sha256,
        run_id=str(command.binding.run_id),
        attempt_id=str(command.binding.attempt_id),
        master_instance_id=str(command.binding.master_instance_id),
        epoch=command.binding.epoch,
        before_boot_id=str(uuid4()),
    )
    service_before = ledger.resolve_service("postgres-master")
    assert service_before is not None
    heartbeat = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        event_type=RuntimeEventType.RUNTIME_HEARTBEAT,
        emitted_at=ledger.clock.now(),
        local_sequence=2,
        epoch=handle.epoch,
        data={"lease_until": (ledger.clock.now() + timedelta(minutes=6)).isoformat()},
    )
    app = create_app(ControlPlaneSettings(ledger_path=ledger.path), ledger=ledger, master_runtime=runtime)
    with TestClient(app) as client:
        response = client.post(
            "/internal/runtime/events",
            content=json_body(heartbeat.model_dump(mode="json", by_alias=True, exclude_none=True)),
            headers={"Authorization": f"Bearer {SECRET}"},
        )
        retry = client.post(
            "/internal/runtime/events",
            content=json_body(heartbeat.model_dump(mode="json", by_alias=True, exclude_none=True)),
            headers={"Authorization": f"Bearer {SECRET}"},
        )
        control = ledger.master_acceptance_runtime_control(str(command.task_id))
        assert control is not None
        service_suppressed = ledger.resolve_service("postgres-master")
        assert service_suppressed is not None
        assert service_suppressed.latest_event_id == service_before.latest_event_id
        ledger.record_master_acceptance_restart(
            task_id=str(command.task_id),
            restart_from_id=str(control["before_boot_id"]),
            restart_to_id=str(uuid4()),
        )
    assert response.status_code == 503
    assert retry.status_code == 503
    assert response.json()["detail"]["code"] == "acceptance_callback_ack_suppressed"
    ledger.revoke_runtime_token(handle.run_id, handle.attempt_id)
    assert (
        ledger.project_master_acceptance_callback_replay(
            task_id=str(command.task_id),
            event_id=str(heartbeat.event_id),
            body_sha256=hashlib.sha256(
                json_body(heartbeat.model_dump(mode="json", by_alias=True, exclude_none=True))
            ).hexdigest(),
        )
        == "duplicate"
    )
    control = ledger.master_acceptance_runtime_control(str(command.task_id))
    assert control is not None and control["callback_state"] == "REPLAYED"
    assert control["callback_event_id"] == heartbeat.event_id
    service_after = ledger.resolve_service("postgres-master")
    assert service_after is not None and service_after.latest_event_id == service_before.latest_event_id


def test_fm08_recovery_plan_survives_old_epoch_fencing_without_second_intent(
    tmp_path: Path,
) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM08", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM08",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    payload = ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario="FM08",
        principal_id="owner",
        client_id="acceptance-client",
    )
    assert payload is not None
    command = MasterAcceptanceCommand.model_validate(payload)
    old_run = {
        "schema_version": "my-data-hub-kaggle-kernel-run-identity.v1",
        "provider_ref": "owner/old-master",
        "provider_run_ref": "owner/old-master/1",
        "source_version": 1,
        "provider_kernel_id": 123,
        "source_sha256": "e" * 64,
        "task_run_id": str(command.binding.run_id),
    }
    kwargs = {
        "task_id": str(command.task_id),
        "command_id": str(command.command_id),
        "command_sha256": command.command_sha256,
        "old_operation_id": str(command.binding.operation_id),
        "old_run": old_run,
        "old_epoch": command.binding.epoch,
        "replacement_idempotency_key": f"fm08-recovery:{command.task_id}",
        "replacement_notebook_ref": f"owner/mdh-master-fm08-{command.task_id.hex}",
    }
    plan, created = ledger.ensure_fm08_abrupt_recovery(**kwargs)
    assert created and plan["state"] == "INTENT"
    receipt = {"schema_version": "my-data-hub-fm08-termination-test.v1", "outcome": "APPLIED"}
    receipt_sha256 = hashlib.sha256(json.dumps(receipt, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    fenced = ledger.fence_fm08_abrupt_master(
        task_id=str(command.task_id),
        termination_receipt=receipt,
        termination_receipt_sha256=receipt_sha256,
    )
    assert fenced["state"] == "TERMINATED"
    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == "FENCED"
    replay, created_again = ledger.ensure_fm08_abrupt_recovery(**kwargs)
    assert not created_again and replay == fenced


def test_fm11_context_intent_is_task_keyed_and_persisted_only_while_active(
    tmp_path: Path,
) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM11", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM11",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    payload = ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario="FM11",
        principal_id="owner",
        client_id="acceptance-client",
    )
    assert payload is not None
    command = MasterAcceptanceCommand.model_validate(payload)
    handle_id = str(uuid4())
    values = {
        "task_id": str(command.task_id),
        "command_id": str(command.command_id),
        "command_sha256": command.command_sha256,
        "credential_handle": handle_id,
        "expires_at": ledger.clock.now() + timedelta(minutes=15),
    }
    intent, created = ledger.begin_fm11_old_epoch_context(**values)
    assert created and intent["state"] == "INTENT"
    assert intent["old_binding"] == command.binding.model_dump(mode="json")
    assert intent["runtime_token_sha256"] == hashlib.sha256(SECRET.encode()).hexdigest()
    replay, created_again = ledger.begin_fm11_old_epoch_context(**values)
    assert not created_again and replay == intent
    with pytest.raises(IdempotencyConflict):
        ledger.begin_fm11_old_epoch_context(**{**values, "credential_handle": str(uuid4())})


def test_fm10_runtime_control_endpoint_acknowledges_exact_suspension(tmp_path: Path) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM10", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM10",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    payload = ledger.claim_master_acceptance_host_command(
        task_id=str(request.task_id),
        expected_scenario="FM10",
        principal_id="owner",
        client_id="acceptance-client",
    )
    assert payload is not None
    command = MasterAcceptanceCommand.model_validate(payload)
    ledger.suspend_master_acceptance_renewal(
        task_id=str(command.task_id),
        command_id=str(command.command_id),
        command_sha256=command.command_sha256,
        run_id=str(command.binding.run_id),
        attempt_id=str(command.binding.attempt_id),
        master_instance_id=str(command.binding.master_instance_id),
        epoch=command.binding.epoch,
    )
    app = create_app(
        ControlPlaneSettings(ledger_path=ledger.path),
        ledger=ledger,
        master_runtime=_control_runtime(ledger),
    )
    headers = {
        "Authorization": f"Bearer {SECRET}",
        "X-MDH-Master-Instance-ID": handle.master_instance_id,
        "X-MDH-Epoch": str(handle.epoch),
    }
    url = f"/internal/runtime/master-acceptance/{handle.run_id}/{handle.attempt_id}"
    with TestClient(app) as client:
        directive = client.get(f"{url}/control-directive", headers=headers)
        assert directive.status_code == 200
        assert directive.json()["renewal_suspended"] is True
        acknowledged = client.post(f"{url}/renewal-suspended", headers=headers)
        assert acknowledged.json() == {"accepted": True, "renewal_suspended": True}
    control = ledger.master_acceptance_runtime_control(str(command.task_id))
    assert control is not None and control["renewal_acknowledged"] == 1


def test_fm24_is_claimed_only_by_the_exact_active_runtime(tmp_path: Path) -> None:
    ledger, handle = _active_ledger(tmp_path)
    request = _request("FM24", operation_id=handle.operation_id)
    ledger.ensure_master_acceptance_task(
        task_id=str(request.task_id),
        scenario_id="FM24",
        idempotency_key=request.idempotency_key,
        request_sha256=request.request_sha256,
        principal_id="owner",
        client_id="acceptance-client",
        source_revision=request.source_revision,
        target_operation_id=handle.operation_id,
    )
    with pytest.raises(ValueError, match="host claim identity"):
        ledger.claim_master_acceptance_host_command(
            task_id=str(request.task_id),
            expected_scenario="FM24",
            principal_id="owner",
            client_id="acceptance-client",
        )
    payload = ledger.claim_master_acceptance_command(
        run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch
    )
    assert payload is not None
    assert payload["command_kind"] == "SESSION_ROTATION_SOAK"
    assert payload["task_id"] == str(request.task_id)
    assert (
        ledger.claim_master_acceptance_command(
            run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch + 1
        )
        is None
    )


def test_protected_ledger_replays_one_exact_acked_body_without_state_change(tmp_path: Path) -> None:
    ledger = ControlLedger(
        tmp_path / "stored-replay.sqlite3",
        clock=DeterministicClock(datetime(2026, 8, 11, 11, 0, tzinfo=UTC)),
    )
    old = _activate_ledger(ledger, key="fm09-retired-predecessor")
    ledger.project_master_lifecycle(
        operation_id=old.operation_id,
        service_instance_id=old.service_instance_id,
        epoch=old.epoch,
        expected_operation_state="ACTIVE",
        operation_state="FENCED",
        service_state="FENCED",
        event_id=str(uuid4()),
    )
    ledger.revoke_runtime_token(old.run_id, old.attempt_id)
    runtime = _control_runtime(ledger)
    handle, _duplicate = runtime.ensure("fm09-stored-replay")
    assert handle.epoch == 2
    token = "c" * 64
    ledger.store_runtime_token_hash(handle.run_id, handle.attempt_id, token)
    event = RuntimeEvent(
        event_id=str(uuid4()),
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity="owner/postgres-master",
        source_version="git:0123456789abcdef",
        event_type=RuntimeEventType.SERVICE_READY,
        emitted_at=ledger.clock.now(),
        local_sequence=1,
        epoch=handle.epoch,
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://fm09",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + "9" * 64,
            "capabilities": ["sql"],
            "canonical_revision": 0,
            "schema_version": "1",
            "lease_until": (ledger.clock.now() + timedelta(minutes=5)).isoformat(),
            "master_instance_id": handle.master_instance_id,
            "epoch": handle.epoch,
        },
    )
    runtime.coordinator.accept_runtime_event(
        json_body(event.model_dump(mode="json", by_alias=True, exclude_none=True)), header_token=token
    )
    retired_run = str(UUID(int=91))
    retired_attempt = str(UUID(int=92))
    retired_token = "b" * 64
    ledger.store_runtime_token_hash(retired_run, retired_attempt, retired_token)
    ledger.revoke_runtime_token(retired_run, retired_attempt)
    binding = MasterAcceptanceBinding(
        operation_id=UUID(handle.operation_id),
        run_id=UUID(handle.run_id),
        attempt_id=UUID(handle.attempt_id),
        service_instance_id=handle.service_instance_id,
        master_instance_id=UUID(handle.master_instance_id),
        epoch=handle.epoch,
    )
    replay = ControlLedgerStoredReplay(runtime)
    stored = replay.exact_acked_callback(binding)
    protected = ledger.exact_stored_runtime_event_identity(
        run_id=handle.run_id, attempt_id=handle.attempt_id, epoch=handle.epoch
    )
    assert protected == {"event_id": str(stored.event_id), "body_sha256": stored.body_sha256}
    assert "body" not in protected
    before = replay.control_state_sha256(binding)
    assert replay.replay_stored_callback(stored.event_id) == "duplicate"
    assert replay.replay_with_retired_runtime_auth(stored.event_id)
    assert replay.replay_with_stale_epoch(stored.event_id)
    assert replay.control_state_sha256(binding) == before
    assert "runtime_token" not in replay.__dataclass_fields__
    assert "retired_runtime_token" not in replay.__dataclass_fields__
    with pytest.raises(StaleRuntimeEvent):
        ledger.replay_stored_runtime_event_identity(
            event_id=str(stored.event_id),
            body_sha256="f" * 64,
            run_id=handle.run_id,
            attempt_id=handle.attempt_id,
            epoch=handle.epoch,
        )


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
                old_master_abruptly_terminated=True,
                old_operation_id=UUID(int=10),
                new_operation_id=UUID(int=11),
                old_epoch=1,
                new_epoch=2,
                old_provider_run_ref="owner/old/1",
                old_provider_kernel_id=10,
                new_provider_run_ref="owner/new/1",
                new_provider_kernel_id=11,
                termination_receipt_sha256="c" * 64,
                recovery_receipt_sha256="d" * 64,
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
                credentials_invalidated=True,
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
                registry_resolves_new=True,
                old_operation_id=UUID(int=8),
                new_operation_id=UUID(int=9),
                old_provider_run_ref="owner/old-master/1",
                old_provider_kernel_id=81,
                new_provider_run_ref="owner/new-master/2",
                new_provider_kernel_id=82,
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
                heartbeats_continuous=True,
                heartbeat_count=12,
                heartbeat_receipt_sha256s=("5" * 64,) * 12,
                reads_succeeded=True,
                read_query_count=12,
                bounded_read_receipt_sha256s=("6" * 64,) * 12,
                checkpoint_verified=True,
                recovery_succeeded=True,
                checkpoint_id=UUID(int=11),
                exact_version_ref="owner/checkpoints/7",
                manifest_sha256="2" * 64,
                checkpoint_receipt_sha256="3" * 64,
                recovery_receipt_sha256="4" * 64,
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
            credentials_invalidated=True,
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
            registry_resolves_new=True,
            old_operation_id=UUID(int=8),
            new_operation_id=UUID(int=9),
            old_provider_run_ref="owner/old-master/1",
            old_provider_kernel_id=81,
            new_provider_run_ref="owner/new-master/2",
            new_provider_kernel_id=82,
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


def test_fm24_live_receipt_rejects_soak_without_real_checkpoint_recovery() -> None:
    operation = uuid4()
    command = command_for(
        _request("FM24", operation_id=str(operation)),
        MasterAcceptanceBinding(
            operation_id=operation,
            run_id=uuid4(),
            attempt_id=uuid4(),
            service_instance_id="service-1",
            master_instance_id=uuid4(),
            epoch=1,
        ),
    )
    evidence = RotationSoakEvidence(
        kind="SESSION_ROTATION_SOAK",
        monotonic_started_ns=0,
        monotonic_finished_ns=3_600_000_000_000,
        observed_duration_seconds=3600,
        session_rotations=12,
        lease_renewals=12,
        tunnel_renewals=12,
        rejected_stale_sessions=12,
        remained_single_epoch=True,
        service_active_at_end=True,
    )
    with pytest.raises(ValidationError, match="real checkpoint and recovery evidence"):
        execute_master_acceptance_command(command, FixedEffects(evidence))
