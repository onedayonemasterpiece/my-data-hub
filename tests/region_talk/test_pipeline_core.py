from __future__ import annotations

import hashlib
import json
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.workloads.region_talk.central_launcher import (
    RegionTalkBootstrapConfig,
    RegionTalkSupervisorCapability,
    render_region_talk_supervisor_source,
)
from my_data_hub.workloads.region_talk.pipeline_contracts import (
    ActiveMasterBinding,
    RegionTalkAccessBinding,
    RegionTalkCleanupReceipt,
    RegionTalkDirectMasterAccess,
    RegionTalkLaunchMetadata,
    RegionTalkLaunchReceipt,
    RegionTalkRunRequest,
    RegionTalkRunState,
    RegionTalkRuntimeAttestation,
    RegionTalkTerminalReceipt,
    TaskWorkerCredentialBatch,
    TaskWorkerCredentialCommand,
    TaskWorkerCredentialRegistration,
    TaskWorkerCredentialRegistrationResponse,
    TaskWorkerCredentialRevocation,
    task_worker_credentials_endpoint,
)
from my_data_hub.workloads.region_talk.pipeline_runtime import (
    LaunchObservation,
    LaunchObservationKind,
    RegionTalkCycleDisposition,
    RegionTalkCycleResult,
    RegionTalkEpochFenced,
    RegionTalkLaunchAmbiguity,
    RegionTalkPipelineCoordinator,
    RegionTalkPipelineStore,
    RegionTalkRuntimePins,
    run_bounded_supervisor,
)

NOW = datetime(2026, 8, 19, 12, tzinfo=UTC)
MASTER = ActiveMasterBinding(
    run_id=UUID("11111111-1111-4111-8111-111111111111"),
    attempt_id=UUID("22222222-2222-4222-8222-222222222222"),
    master_instance_id=UUID("33333333-3333-4333-8333-333333333333"),
    epoch=47,
)
NEW_MASTER = ActiveMasterBinding(
    run_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    attempt_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    master_instance_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
    epoch=48,
)
ACCESS_ID = UUID("44444444-4444-4444-8444-444444444444")


def _pins() -> RegionTalkRuntimePins:
    return RegionTalkRuntimePins(
        runtime_dataset_exact_ref="owner/mdh-runtime/12",
        runtime_image_identity="runtime@sha256:" + "d" * 64,
        runtime_image_source_commit="e" * 40,
        wheel_relative_path="dist/my_data_hub.whl",
        wheel_sha256="f" * 64,
        ydb_endpoint="grpcs://ydb.serverless.yandexcloud.net:2135",
        ydb_database="/ru-central1/example/region-talk",
        ydb_viewer_secret_label="REGION_TALK_YDB_VIEWER_SA_JSON",
        max_cycles=4,
        max_runtime_seconds=600,
    )


def _receipt(metadata: RegionTalkLaunchMetadata) -> RegionTalkLaunchReceipt:
    return RegionTalkLaunchReceipt(
        task_run_id=metadata.task_run_id,
        master_instance_id=metadata.master.master_instance_id,
        epoch=metadata.master.epoch,
        source_sha256="1" * 64,
        status_dataset_exact_ref=f"owner/region-talk-{metadata.task_run_id.hex[:12]}/1",
        provider_run_ref=f"owner/region-talk-supervisor/runs/{metadata.task_run_id.hex[:12]}",
        access=RegionTalkAccessBinding(
            credential_id=ACCESS_ID,
            generation=1,
            command_sha256="7" * 64,
            task_token_sha256="6" * 64,
            expires_at=NOW + timedelta(minutes=10),
            ssh_certificate_serial=7001,
        ),
    )


class _Launcher:
    def __init__(self) -> None:
        self.calls = 0
        self.observations: dict[UUID, RegionTalkLaunchReceipt] = {}
        self.ambiguous = False
        self.raise_after_effect = False
        self.lock = threading.Lock()

    def observe(self, metadata: RegionTalkLaunchMetadata) -> LaunchObservation:
        with self.lock:
            if self.ambiguous:
                return LaunchObservation(LaunchObservationKind.AMBIGUOUS)
            receipt = self.observations.get(metadata.task_run_id)
            return LaunchObservation(
                LaunchObservationKind.PRESENT if receipt else LaunchObservationKind.ABSENT,
                receipt,
            )

    def launch(self, metadata: RegionTalkLaunchMetadata) -> RegionTalkLaunchReceipt:
        with self.lock:
            self.calls += 1
            receipt = _receipt(metadata)
            self.observations[metadata.task_run_id] = receipt
        if self.raise_after_effect:
            self.raise_after_effect = False
            raise RuntimeError("provider response was lost")
        return receipt


class _Cleanup:
    def __init__(self) -> None:
        self.calls: list[object] = []

    def cleanup(self, run):  # type: ignore[no-untyped-def]
        self.calls.append(run)
        assert run.access is not None and run.task_run_id is not None
        return RegionTalkCleanupReceipt(
            task_run_id=run.task_run_id,
            credential_id=run.access.credential_id,
            generation=run.access.generation,
            command_sha256=run.access.command_sha256,
            task_token_sha256=run.access.task_token_sha256,
            ssh_certificate_serial=run.access.ssh_certificate_serial,
            resources_deleted=2,
            receipt_sha256="9" * 64,
            cleaned_at=run.updated_at + timedelta(seconds=1),
        )


def _coordinator(
    path: Path,
    launcher: _Launcher,
    cleanup: _Cleanup,
    *,
    instance: str = "scheduler-a",
    clock=lambda: NOW,
) -> RegionTalkPipelineCoordinator:
    return RegionTalkPipelineCoordinator(
        store=RegionTalkPipelineStore(path),
        launcher=launcher,
        cleanup=cleanup,
        pins=_pins(),
        instance_id=instance,
        clock=clock,
    )


def test_duplicate_supervised_key_and_schedule_slot_are_exactly_idempotent(tmp_path: Path) -> None:
    store = RegionTalkPipelineStore((tmp_path / "control.sqlite3").absolute())
    first_request = RegionTalkRunRequest.supervised(
        idempotency_key="region-talk-first-supervised-run",
        source_revision="donor@74feca4",
        requested_at=NOW,
    )
    replay_request = RegionTalkRunRequest.supervised(
        idempotency_key="region-talk-first-supervised-run",
        source_revision="donor@74feca4",
        requested_at=NOW + timedelta(minutes=5),
    )
    first, created = store.ensure_request(first_request)
    replay, replay_created = store.ensure_request(replay_request)
    assert created is True and replay_created is False
    assert replay == first
    assert replay.request.project_slug == "region-talk"
    assert replay.request.publication_dispatch is False
    assert replay.state is RegionTalkRunState.WAITING_MASTER
    assert store.event_count(first.request.request_id) == 1
    scheduled = RegionTalkRunRequest.scheduled(
        schedule_slot=NOW + timedelta(hours=1),
        source_revision="donor@74feca4",
        requested_at=NOW,
    )
    scheduled_replay = RegionTalkRunRequest.scheduled(
        schedule_slot=NOW + timedelta(hours=1),
        source_revision="donor@74feca4",
        requested_at=NOW + timedelta(minutes=30),
    )
    slot, slot_created = store.ensure_request(scheduled)
    slot_replay, slot_replay_created = store.ensure_request(scheduled_replay)
    assert slot_created is True and slot_replay_created is False
    assert slot_replay == slot and store.event_count(slot.request.request_id) == 1


def test_concurrent_ticks_launch_only_one_waiting_slot(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    launcher = _Launcher()
    cleanup = _Cleanup()
    first = _coordinator(path, launcher, cleanup, instance="scheduler-a")
    second = _coordinator(path, launcher, cleanup, instance="scheduler-b")
    first.schedule(NOW)

    barrier = threading.Barrier(2)

    def tick(coordinator: RegionTalkPipelineCoordinator):
        barrier.wait(timeout=5)
        return coordinator.tick(MASTER)

    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(tick, (first, second)))
    assert launcher.calls == 1
    assert sum(result is not None for result in results) == 1
    current = first.status()
    assert current is not None and current.state is RegionTalkRunState.PENDING_ATTESTATION
    assert current.master == MASTER


def test_waiting_master_never_launches_or_embeds_data_plane_state(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    launcher = _Launcher()
    coordinator = _coordinator(path, launcher, _Cleanup())
    request, _ = coordinator.request_supervised(idempotency_key="supervised-region-talk-001")
    assert coordinator.tick(None) is None
    current = coordinator.status(request.request.request_id)
    assert current is not None and current.state is RegionTalkRunState.WAITING_MASTER
    assert launcher.calls == 0
    raw = path.read_bytes().lower()
    assert b"postgresql://" not in raw and b"private key" not in raw
    assert b"task-token-secret-value" not in raw and b"authorization: bearer" not in raw


def test_lost_launch_response_is_reconciled_after_restart_without_second_push(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    launcher = _Launcher()
    launcher.raise_after_effect = True
    first = _coordinator(path, launcher, _Cleanup(), instance="scheduler-a")
    first.request_supervised(idempotency_key="supervised-region-talk-002")
    with pytest.raises(RuntimeError, match="response was lost"):
        first.tick(MASTER)
    current = first.status()
    assert current is not None and current.state is RegionTalkRunState.LAUNCHING
    assert current.error_code == "PROVIDER_LAUNCH_RETRY_REQUIRED"

    restarted = _coordinator(path, launcher, _Cleanup(), instance="scheduler-b")
    result = restarted.tick(MASTER)
    assert result is not None and result.state is RegionTalkRunState.PENDING_ATTESTATION
    assert launcher.calls == 1


def test_ambiguous_provider_observation_fails_closed_without_launch(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    launcher = _Launcher()
    launcher.ambiguous = True
    coordinator = _coordinator(path, launcher, _Cleanup())
    coordinator.request_supervised(idempotency_key="supervised-region-talk-003")
    with pytest.raises(RegionTalkLaunchAmbiguity, match="no second launch"):
        coordinator.tick(MASTER)
    current = coordinator.status()
    assert current is not None and current.state is RegionTalkRunState.LAUNCHING
    assert current.error_code == "PROVIDER_LAUNCH_AMBIGUOUS"
    assert launcher.calls == 0


def test_partial_launch_timeout_can_cleanup_discovered_exact_credential(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    now = NOW

    def clock() -> datetime:
        return now

    launcher = _Launcher()
    launcher.raise_after_effect = True

    class RecoveryCleanup:
        def __init__(self) -> None:
            self.calls = 0

        def cleanup(self, run):  # type: ignore[no-untyped-def]
            self.calls += 1
            assert run.task_run_id is not None and run.access is None
            discovered = launcher.observations[run.task_run_id].access
            return RegionTalkCleanupReceipt(
                task_run_id=run.task_run_id,
                credential_id=discovered.credential_id,
                generation=discovered.generation,
                command_sha256=discovered.command_sha256,
                task_token_sha256=discovered.task_token_sha256,
                ssh_certificate_serial=discovered.ssh_certificate_serial,
                resources_deleted=2,
                receipt_sha256="5" * 64,
                cleaned_at=now,
            )

    cleanup = RecoveryCleanup()
    coordinator = RegionTalkPipelineCoordinator(
        store=RegionTalkPipelineStore(path),
        launcher=launcher,
        cleanup=cleanup,
        pins=_pins(),
        instance_id="scheduler-a",
        clock=clock,
    )
    coordinator.request_supervised(idempotency_key="supervised-region-talk-partial-timeout")
    with pytest.raises(RuntimeError, match="response was lost"):
        coordinator.tick(MASTER)
    now = NOW + timedelta(seconds=601)
    result = coordinator.tick(MASTER)
    assert result is not None and result.state is RegionTalkRunState.CLEANED
    assert result.terminal_status is not None and result.terminal_status.value == "TIMED_OUT"
    assert result.cleanup_receipt_sha256 == "5" * 64 and cleanup.calls == 1


def test_source_image_and_epoch_attestation_is_fenced(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    coordinator = _coordinator(path, _Launcher(), _Cleanup())
    coordinator.request_supervised(idempotency_key="supervised-region-talk-004")
    launched = coordinator.tick(MASTER)
    assert launched is not None and launched.task_run_id is not None
    with pytest.raises(RegionTalkEpochFenced, match="source/image/epoch"):
        coordinator.store.attest(
            RegionTalkRuntimeAttestation(
                request_id=launched.request.request_id,
                task_run_id=launched.task_run_id,
                master_instance_id=MASTER.master_instance_id,
                epoch=46,
                source_sha256="1" * 64,
                image_identity=_pins().runtime_image_identity,
                image_source_commit=_pins().runtime_image_source_commit,
                attested_at=NOW + timedelta(seconds=1),
            )
        )
    attested = coordinator.store.attest(
        RegionTalkRuntimeAttestation(
            request_id=launched.request.request_id,
            task_run_id=launched.task_run_id,
            master_instance_id=MASTER.master_instance_id,
            epoch=47,
            source_sha256="1" * 64,
            image_identity=_pins().runtime_image_identity,
            image_source_commit=_pins().runtime_image_source_commit,
            attested_at=NOW + timedelta(seconds=1),
        )
    )
    assert attested.state is RegionTalkRunState.ATTESTED
    assert coordinator.store.mark_running(
        task_run_id=launched.task_run_id,
        master=MASTER,
        now=NOW + timedelta(seconds=2),
    ).state is RegionTalkRunState.RUNNING
    transitioned = coordinator.store.expire_and_fence(
        now=NOW + timedelta(seconds=3), active_master=NEW_MASTER
    )
    assert transitioned == (launched.request.request_id,)
    fenced = coordinator.status(launched.request.request_id)
    assert fenced is not None and fenced.state is RegionTalkRunState.FENCED
    assert fenced.terminal_status is not None and fenced.terminal_status.value == "EPOCH_FENCED"


def test_timeout_claims_exact_cleanup_and_allows_next_slot_only_after_cleanup(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    now = NOW

    def clock() -> datetime:
        return now

    cleanup = _Cleanup()
    coordinator = _coordinator(path, _Launcher(), cleanup, clock=clock)
    first, _ = coordinator.request_supervised(idempotency_key="supervised-region-talk-005")
    launched = coordinator.tick(MASTER)
    assert launched is not None
    coordinator.request_supervised(idempotency_key="supervised-region-talk-006")
    now = NOW + timedelta(seconds=601)
    cleaned = coordinator.tick(MASTER)
    assert cleaned is not None and cleaned.state is RegionTalkRunState.CLEANED
    assert cleaned.terminal_status is not None and cleaned.terminal_status.value == "TIMED_OUT"
    assert len(cleanup.calls) == 1
    assert cleanup.calls[0].request.request_id == first.request.request_id
    second = coordinator.tick(MASTER)
    assert second is not None and second.state is RegionTalkRunState.PENDING_ATTESTATION


def test_terminal_then_cleanup_is_restart_safe_and_exactly_bound(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    cleanup = _Cleanup()
    coordinator = _coordinator(path, _Launcher(), cleanup)
    coordinator.request_supervised(idempotency_key="supervised-region-talk-007")
    launched = coordinator.tick(MASTER)
    assert launched is not None and launched.task_run_id is not None
    coordinator.store.attest(
        RegionTalkRuntimeAttestation(
            request_id=launched.request.request_id,
            task_run_id=launched.task_run_id,
            master_instance_id=MASTER.master_instance_id,
            epoch=MASTER.epoch,
            source_sha256="1" * 64,
            image_identity=_pins().runtime_image_identity,
            image_source_commit=_pins().runtime_image_source_commit,
            attested_at=NOW + timedelta(seconds=1),
        )
    )
    coordinator.store.mark_running(
        task_run_id=launched.task_run_id,
        master=MASTER,
        now=NOW + timedelta(seconds=2),
    )
    terminal = coordinator.store.record_terminal(
        RegionTalkTerminalReceipt(
            request_id=launched.request.request_id,
            task_run_id=launched.task_run_id,
            master_instance_id=MASTER.master_instance_id,
            epoch=MASTER.epoch,
            status="SUCCEEDED",
            cycles_completed=3,
            rows_observed=120,
            rows_changed=17,
            queue_revision=4,
            aggregate_receipt_sha256="8" * 64,
            completed_at=NOW + timedelta(seconds=3),
        )
    )
    assert terminal.state is RegionTalkRunState.TERMINAL
    restarted = _coordinator(path, _Launcher(), cleanup, instance="scheduler-b")
    cleaned = restarted.tick(MASTER)
    assert cleaned is not None and cleaned.state is RegionTalkRunState.CLEANED
    assert cleaned.cleanup_receipt_sha256 == "9" * 64
    assert restarted.tick(MASTER) is None


def test_task_worker_command_is_exact_endpoint_bound_and_tamper_evident() -> None:
    command = TaskWorkerCredentialCommand.create(
        task_run_id=UUID("55555555-5555-4555-8555-555555555555"),
        epoch=MASTER.epoch,
        generation=1,
        task_token_sha256="a" * 64,
    )
    assert task_worker_credentials_endpoint(MASTER) == (
        f"/internal/runtime/task-worker-credentials/{MASTER.run_id}/{MASTER.attempt_id}/commands"
    )
    assert command.worker_kind == "region_talk"
    assert TaskWorkerCredentialBatch.rolling_upgrade_empty().model_dump(mode="json") == {
        "schema_version": "my-data-hub-task-credential-batch.v1",
        "commands": [],
        "revocations": [],
    }
    changed = command.model_dump(mode="json")
    changed["epoch"] = 48
    with pytest.raises(ValidationError, match="command_sha256"):
        TaskWorkerCredentialCommand.model_validate(changed)
    registration = TaskWorkerCredentialRegistration(
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        task_run_id=command.task_run_id,
        generation=command.generation,
        credential_id=ACCESS_ID,
        database_url="postgresql://region_talk:secret@127.0.0.1:25432/postgres",
        expires_at=NOW + timedelta(minutes=4),
        task_token_sha256=command.task_token_sha256,
        command_sha256=command.command_sha256,
    )
    response = TaskWorkerCredentialRegistrationResponse(
        task_run_id=command.task_run_id,
        epoch=command.epoch,
        generation=command.generation,
        credential_id=registration.credential_id,
        command_sha256=command.command_sha256,
    )
    assert set(response.model_dump(mode="json")) == {
        "registered", "worker_kind", "task_run_id", "epoch", "generation",
        "credential_id", "command_sha256",
    }
    revocation = TaskWorkerCredentialRevocation(
        task_run_id=command.task_run_id,
        epoch=command.epoch,
        generation=command.generation,
        task_token_sha256=command.task_token_sha256,
        command_sha256=command.command_sha256,
        credential_id=registration.credential_id,
        reason="region_talk_terminal",
    )
    batch = TaskWorkerCredentialBatch(commands=(command,), revocations=(revocation,))
    assert batch.commands[0].command_sha256 == batch.revocations[0].command_sha256


def test_private_capability_is_separate_and_bootstrap_attests_before_database_access() -> None:
    request = RegionTalkRunRequest.supervised(
        idempotency_key="supervised-region-talk-008", requested_at=NOW
    )
    metadata = RegionTalkLaunchMetadata(
        request_id=request.request_id,
        task_run_id=UUID("55555555-5555-4555-8555-555555555555"),
        trigger=request.trigger,
        schedule_slot=request.schedule_slot,
        master=MASTER,
        runtime_dataset_exact_ref=_pins().runtime_dataset_exact_ref,
        runtime_image_identity=_pins().runtime_image_identity,
        runtime_image_source_commit=_pins().runtime_image_source_commit,
        wheel_relative_path=_pins().wheel_relative_path,
        wheel_sha256=_pins().wheel_sha256,
        ydb_endpoint=_pins().ydb_endpoint,
        ydb_database=_pins().ydb_database,
        ydb_viewer_secret_label=_pins().ydb_viewer_secret_label,
        max_cycles=4,
        max_runtime_seconds=600,
    )
    token = "task-token-with-at-least-twenty-four-characters"
    access = RegionTalkDirectMasterAccess(
        credential_id=ACCESS_ID,
        task_run_id=metadata.task_run_id,
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        generation=1,
        command_sha256="7" * 64,
        task_token_sha256=hashlib.sha256(token.encode()).hexdigest(),
        database_url="postgresql://region_talk:secret@tunnel:25432/hub",
        tls_ca_pem="-----BEGIN CERTIFICATE-----\nca\n-----END CERTIFICATE-----",
        expires_at=NOW + timedelta(minutes=5),
        tunnel_endpoint="tunnel:25432",
        ssh_private_key="-----BEGIN OPENSSH PRIVATE KEY-----\nkey",
        ssh_certificate="ssh-ed25519-cert-v01@openssh.com certificate",
        ssh_known_hosts="host ssh-ed25519 public-key",
        ssh_gateway_host="gateway.example",
        ssh_gateway_port=22,
        ssh_account="mdh-task-worker",
        ssh_certificate_serial=7001,
    )
    capability = RegionTalkSupervisorCapability(
        launch=metadata,
        direct_access=access,
        callback_base_url="https://control.example/internal",
        task_token=token,
        task_token_sha256=hashlib.sha256(token.encode()).hexdigest(),
    )
    private_body = capability.private_dataset_bytes()
    assert b"postgresql://region_talk:secret" in private_body
    assert token.encode() in private_body
    source = render_region_talk_supervisor_source(
        metadata, config=RegionTalkBootstrapConfig(cycle_executor_factory="package.module:factory")
    )
    compile(source, "region-talk-supervisor.py", "exec")
    attest_offset = source.index(b"post_metadata(ATTESTATION_PATH")
    refresh_offset = source.index(b"access=refresh_with_replay(previous_access)")
    materialize_offset = source.index(b"executor,tunnel=materialize(access)")
    activation_offset = source.index(b"activate_with_replay(previous_access,access)")
    assert attest_offset < refresh_offset < materialize_offset < activation_offset
    assert b"sslrootcert" in source
    assert b"region-talk-credential-refresh.v1" in source
    assert b"region-talk-credential-activation.v1" in source
    assert b"UserSecretsClient" in source
    assert b"YDB_SERVICE_ACCOUNT_KEY_FILE_CREDENTIALS" in source
    assert b"REGION_TALK_YDB_VIEWER_SA_JSON" in source
    assert b"post_metadata_with_replay(TERMINAL_PATH" in source
    assert b"set_transport_refresher" in source
    assert b"publication_dispatch=False" in source
    assert b"exact Region Talk capability input is absent or ambiguous" in source
    assert b"postgresql://region_talk:secret" not in source
    assert token.encode() not in source


def test_bounded_supervisor_stops_after_idle_and_never_dispatches() -> None:
    class Executor:
        def __init__(self) -> None:
            self.calls = []

        def execute_cycle(self, request):  # type: ignore[no-untyped-def]
            self.calls.append(request)
            return RegionTalkCycleResult(
                disposition=RegionTalkCycleDisposition.IDLE,
                rows_observed=1,
                rows_changed=0,
                receipt_sha256=f"{request.cycle_number:064x}",
            )

    executor = Executor()
    ticks = iter((0.0, 1.0, 2.0))
    result = run_bounded_supervisor(
        executor=executor,
        task_run_id=UUID("55555555-5555-4555-8555-555555555555"),
        master_instance_id=MASTER.master_instance_id,
        epoch=MASTER.epoch,
        max_cycles=20,
        max_runtime_seconds=600,
        monotonic=lambda: next(ticks),
        sleep=lambda _seconds: None,
    )
    assert result.completed is True and result.cycles_completed == 2
    assert result.rows_observed == 2 and result.rows_changed == 0
    assert all(call.publication_dispatch is False for call in executor.calls)


def test_control_journal_contains_only_fixed_metadata_columns(tmp_path: Path) -> None:
    path = (tmp_path / "control.sqlite3").absolute()
    coordinator = _coordinator(path, _Launcher(), _Cleanup())
    coordinator.request_supervised(idempotency_key="supervised-region-talk-009")
    coordinator.tick(MASTER)
    connection = sqlite3.connect(path)
    try:
        row = connection.execute("SELECT * FROM region_talk_pipeline_requests").fetchone()
        columns = [item[0] for item in connection.execute(
            "SELECT name FROM pragma_table_info('region_talk_pipeline_requests') ORDER BY cid"
        )]
    finally:
        connection.close()
    assert row is not None
    assert not {"database_url", "dsn", "password", "task_token", "content", "payload"} & set(columns)
    serialized = json.dumps(row)
    assert "postgresql://" not in serialized and "PRIVATE KEY" not in serialized
