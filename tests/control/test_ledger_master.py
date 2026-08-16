from __future__ import annotations

import json
import os
import random
import sqlite3
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import (
    ControlLedger,
    EffectState,
    EventDisposition,
    EventRejected,
    MasterAdmissionRejected,
    StaleRuntimeEvent,
    discover_control_migrations,
)
from my_data_hub.control_plane.ledger import migrations as control_migration_module
from my_data_hub.orchestrator.master import (
    ExactOutput,
    FakeKaggleRuntime,
    MasterCoordinator,
    MasterIntent,
    MasterSignal,
    MasterState,
    PlatformStatus,
    SimulatedProcessCrash,
    TerminalDecision,
    decide_terminal,
    transition_master,
)
from my_data_hub.runtime_sdk import (
    JsonlEventSpool,
    RetryPolicy,
    RuntimeClient,
    RuntimeEvent,
    RuntimeEventType,
    TransportResponse,
)

SECRET = "correct-horse-battery-staple"


def intent(key: str = "ensure-master-v1") -> MasterIntent:
    return MasterIntent(
        idempotency_key=key,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="EMPTY_BASELINE",
        dataset_ref="private/checkpoint-dataset",
        notebook_ref="private/postgres-master",
    )


def ledger_at(tmp_path: Path, clock: DeterministicClock | None = None) -> ControlLedger:
    return ControlLedger(tmp_path / "private-control" / "ledger.sqlite3", clock=clock)


def active_admission_ledger(tmp_path: Path) -> tuple[ControlLedger, str, str]:
    now = datetime(2026, 8, 11, 12, tzinfo=UTC)
    ledger = ledger_at(tmp_path, DeterministicClock(now))
    operation_id = "admission-operation"
    master_id = "33333333-3333-4333-8333-333333333333"
    identity = {
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "service_instance_id": "service-1",
        "master_instance_id": master_id,
        "epoch": 1,
    }
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key="atomic-admission",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="READY",
        identity=identity,
    )
    ledger.record_attempt(
        attempt_id="attempt-1",
        run_id="run-1",
        operation_id=operation_id,
        source_identity="source",
        source_version="git:" + "a" * 40,
        service_instance_id="service-1",
        master_instance_id=master_id,
        epoch=1,
        state="RUNNING",
    )
    assert ledger.allocate_epoch("postgres-master") == 1
    ledger.activate_service_operation(
        operation_id=operation_id,
        expected_state="READY",
        service_instance_id="service-1",
        service_kind="postgres-master",
        run_id="run-1",
        attempt_id="attempt-1",
        master_instance_id=master_id,
        epoch=1,
        endpoint="tunnel://127.0.0.1:5432",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "b" * 64,
        capabilities=("sql",),
        canonical_revision=12,
        schema_version="1",
        lease_until=now + timedelta(minutes=10),
        latest_event_id="event-1",
    )
    checkpoint_id = "55555555-5555-4555-8555-555555555555"
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="c" * 64,
        source_checkpoint_id=None,
        master_instance_id=master_id,
        epoch=1,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "owner/checkpoints:17")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    return ledger, operation_id, checkpoint_id


def runtime_event(handle, event_type: RuntimeEventType, sequence: int, now: datetime, **data):  # type: ignore[no-untyped-def]
    return (
        RuntimeEvent(
            event_id=str(uuid4()),
            run_id=handle.run_id,
            attempt_id=handle.attempt_id,
            service_instance_id=handle.service_instance_id,
            source_identity="my-data-hub/postgres-master",
            source_version="git:0123456789abcdef",
            event_type=event_type,
            emitted_at=now,
            local_sequence=sequence,
            epoch=handle.epoch,
            data=data,
        )
        .model_dump_json(by_alias=True, exclude_none=True)
        .encode()
    )


def test_sqlite_pragmas_permissions_and_append_only_logs(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    assert ledger.sqlite_pragmas() == {"journal_mode": "wal", "foreign_keys": 1, "busy_timeout": 5000}
    assert ledger.path.parent.stat().st_mode & 0o777 == 0o700
    assert ledger.path.stat().st_mode & 0o777 == 0o600
    migrations = (
        sqlite3.connect(ledger.path)
        .execute("SELECT version FROM control_schema_migrations ORDER BY version")
        .fetchall()
    )
    assert migrations == [(version,) for version in range(1, 29)]

    operation, _ = ledger.ensure_operation(
        operation_id=str(uuid4()),
        idempotency_key="append-only-test",
        operation_kind="test",
        intent={"kind": "safe"},
        initial_state="REQUESTED",
        identity={"run_id": "run"},
    )
    connection = sqlite3.connect(ledger.path)
    with pytest.raises(sqlite3.IntegrityError, match="append-only"):
        connection.execute("UPDATE operation_log SET to_state='BROKEN'")
    connection.close()
    assert ledger.get_operation(operation.operation_id) is not None


def test_embedding_request_is_durable_exactly_once_and_epoch_bound(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    request_id = str(uuid4())
    operation_id = str(uuid4())
    body = {"schema_version": "my-data-hub-embedding-production-request.v1", "request_id": request_id}
    stored, created = ledger.ensure_embedding_production_request(
        request_id=request_id,
        operation_id=operation_id,
        idempotency_key_sha256="a" * 64,
        request_sha256="b" * 64,
        request=body,
    )
    assert created and stored["state"] == "REQUESTED"
    replay, created = ledger.ensure_embedding_production_request(
        request_id=request_id,
        operation_id=operation_id,
        idempotency_key_sha256="a" * 64,
        request_sha256="b" * 64,
        request=body,
    )
    assert not created and replay == stored
    claimed = ledger.claim_embedding_production_request(
        operation_id=operation_id,
        run_id="run-1",
        attempt_id="attempt-1",
        master_instance_id=str(uuid4()),
        epoch=9,
    )
    assert claimed is not None and claimed["state"] == "CLAIMED"
    with pytest.raises(StaleRuntimeEvent):
        ledger.claim_embedding_production_request(
            operation_id=operation_id,
            run_id="run-2",
            attempt_id="attempt-2",
            master_instance_id=str(uuid4()),
            epoch=10,
        )
    receipt = {"request_id": request_id, "canonical_revision": 12}
    committed = ledger.record_embedding_stage_receipt(
        request_id=request_id, run_id="run-1", attempt_id="attempt-1", receipt=receipt
    )
    assert committed["state"] == "STAGE_COMMITTED"
    replay = ledger.record_embedding_stage_receipt(
        request_id=request_id, run_id="run-1", attempt_id="attempt-1", receipt=receipt
    )
    assert replay["stage_receipt"] == receipt
    with pytest.raises(StaleRuntimeEvent):
        ledger.fail_embedding_production_request(
            request_id=request_id, run_id="run-1", attempt_id="attempt-1", failure_code="late"
        )


def test_mode_tightening_tolerates_a_wal_unlinked_during_open(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    ledger = ledger_at(tmp_path)
    real_open = os.open

    def racy_open(path: object, flags: int, *args: object, **kwargs: object) -> int:
        if str(path).endswith("-wal"):
            raise FileNotFoundError(str(path))
        return real_open(path, flags, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(os, "open", racy_open)
    ledger._tighten_file_modes()
    assert ledger.path.stat().st_mode & 0o777 == 0o600


def test_packaged_and_repository_control_migrations_are_identical() -> None:
    root = Path(__file__).resolve().parents[2]
    repository = discover_control_migrations(root / "control_migrations")
    packaged = discover_control_migrations(Path(control_migration_module.__file__).with_name("sql"))
    assert [(item.version, item.sha256) for item in repository] == [(item.version, item.sha256) for item in packaged]


def test_atomic_data_admission_rejects_drain_without_stranding_requests(tmp_path: Path) -> None:
    ledger, operation_id, checkpoint_id = active_admission_ledger(tmp_path)
    blogger, created = ledger.admit_blogger_migration_request(
        request_id="blogger-request",
        operation_id=operation_id,
        request_sha256="d" * 64,
        request={"schema_version": "blogger-test"},
    )
    assert created and blogger["state"] == "REQUESTED"
    embedding, created = ledger.admit_embedding_production_request(
        request_id="embedding-request",
        idempotency_key_sha256="e" * 64,
        request_sha256="f" * 64,
        request={"schema_version": "embedding-test"},
        canonical_revision=12,
        checkpoint_id=checkpoint_id,
    )
    assert created and embedding["operation_id"] == operation_id

    ledger.transition_operation(
        operation_id,
        expected_state="ACTIVE",
        new_state="STOPPED",
        metadata={"reason": "drain_won_after_admission"},
    )
    assert ledger.reconcile_abandoned_blogger_migration_request("blogger-request")["failure_code"] == (
        "ADMISSION_RUNTIME_TERMINAL_BEFORE_CLAIM"
    )
    assert (
        ledger.reconcile_abandoned_embedding_production_request("embedding-request")["failure_code"]
        == "ADMISSION_RUNTIME_TERMINAL_BEFORE_CLAIM"
    )

    replay, created = ledger.admit_blogger_migration_request(
        request_id="blogger-request",
        operation_id=operation_id,
        request_sha256="d" * 64,
        request={"schema_version": "blogger-test"},
    )
    assert not created and replay["state"] == "FAILED"
    replay, created = ledger.admit_embedding_production_request(
        request_id="embedding-request",
        idempotency_key_sha256="e" * 64,
        request_sha256="f" * 64,
        request={"schema_version": "embedding-test"},
        canonical_revision=12,
        checkpoint_id=checkpoint_id,
    )
    assert not created and replay["state"] == "FAILED"
    with pytest.raises(MasterAdmissionRejected, match="not active"):
        ledger.admit_blogger_migration_request(
            request_id="new-after-drain",
            operation_id=operation_id,
            request_sha256="1" * 64,
            request={"schema_version": "blogger-test"},
        )
    with pytest.raises(MasterAdmissionRejected, match=r"not active|admission CAS"):
        ledger.admit_embedding_production_request(
            request_id="new-embedding-after-drain",
            idempotency_key_sha256="2" * 64,
            request_sha256="3" * 64,
            request={"schema_version": "embedding-test"},
            canonical_revision=12,
            checkpoint_id=checkpoint_id,
        )


def test_quarantine_receipt_is_idempotent_and_cannot_be_altered(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    operation, _ = ledger.ensure_operation(
        operation_id="quarantine-operation",
        idempotency_key="quarantine-operation",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="ACTIVE",
        identity={"run_id": "run-1", "attempt_id": "attempt-1"},
    )
    ledger.ensure_blogger_migration_request(
        request_id="quarantine-request",
        operation_id=operation.operation_id,
        request_sha256="a" * 64,
        request={"schema_version": "test"},
    )
    ledger.claim_blogger_migration_request(
        operation_id=operation.operation_id,
        run_id="run-1",
        attempt_id="attempt-1",
        master_instance_id="master-1",
        epoch=1,
    )
    receipt = {"schema_version": "quarantine-test", "request_id": "quarantine-request"}
    first = ledger.record_blogger_quarantine_receipt(
        request_id="quarantine-request",
        run_id="run-1",
        attempt_id="attempt-1",
        receipt=receipt,
        receipt_sha256="b" * 64,
    )
    replay = ledger.record_blogger_quarantine_receipt(
        request_id="quarantine-request",
        run_id="run-1",
        attempt_id="attempt-1",
        receipt=receipt,
        receipt_sha256="b" * 64,
    )
    assert replay == first
    with pytest.raises(StaleRuntimeEvent, match="differs"):
        ledger.record_blogger_quarantine_receipt(
            request_id="quarantine-request",
            run_id="run-1",
            attempt_id="attempt-1",
            receipt={**receipt, "request_id": "altered"},
            receipt_sha256="c" * 64,
        )


def test_import_commit_without_verified_checkpoint_terminalizes_after_provider_failure(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    operation, _ = ledger.ensure_operation(
        operation_id="blogger-master-operation",
        idempotency_key="blogger-master-operation",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="ACTIVE",
        identity={"run_id": "run-1", "attempt_id": "attempt-1"},
    )
    ledger.ensure_blogger_migration_request(
        request_id="request-1",
        operation_id=operation.operation_id,
        request_sha256="a" * 64,
        request={"schema_version": "test"},
    )
    ledger.claim_blogger_migration_request(
        operation_id=operation.operation_id,
        run_id="run-1",
        attempt_id="attempt-1",
        master_instance_id="master-1",
        epoch=1,
    )
    ledger.record_blogger_import_receipt(
        request_id="request-1",
        run_id="run-1",
        attempt_id="attempt-1",
        receipt={"canonical_revision": 9},
    )
    ledger.transition_operation(
        operation.operation_id,
        expected_state="ACTIVE",
        new_state="FAILED",
        metadata={"reason": "provider_error_before_checkpoint"},
    )

    reconciled = ledger.reconcile_abandoned_blogger_migration_request("request-1")

    assert reconciled is not None
    assert reconciled["state"] == "FAILED"
    assert reconciled["failure_code"] == "IMPORT_COMMITTED_WITHOUT_DURABLE_CHECKPOINT"


def test_twenty_concurrent_ensure_requests_collapse_to_one_physical_run(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    fake = FakeKaggleRuntime()
    coordinator = MasterCoordinator(ledger, fake)
    request = intent()

    with ThreadPoolExecutor(max_workers=20) as pool:
        handles = list(pool.map(lambda _: coordinator.ensure_master(request, runtime_secret=SECRET), range(20)))

    final = coordinator.reconcile_operation(handles[0].operation_id, request)
    assert final.state == MasterState.REGISTERING
    assert {handle.operation_id for handle in handles} == {final.operation_id}
    assert {handle.epoch for handle in handles} == {1}
    assert fake.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }


def test_distinct_concurrent_ensure_requests_admit_only_one_master(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    fake = FakeKaggleRuntime()
    coordinator = MasterCoordinator(ledger, fake)
    barrier = threading.Barrier(2)

    def ensure(key: str):  # type: ignore[no-untyped-def]
        barrier.wait()
        return coordinator.ensure_master(intent(key), runtime_secret=SECRET)

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(ensure, key) for key in ("distinct-master-a", "distinct-master-b")]
    successes = []
    failures = []
    for future in futures:
        try:
            successes.append(future.result())
        except MasterAdmissionRejected as exc:
            failures.append(exc)

    assert len(successes) == 1
    assert len(failures) == 1
    assert "master admission" in str(failures[0]).lower()
    assert successes[0].epoch == 1
    assert ledger.current_epoch("postgres-master") == 1
    assert len(ledger.incomplete_operations("ensure_master")) == 1
    assert fake.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }


def test_distinct_ensure_cannot_fence_an_active_master_or_create_attempt(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    fake = FakeKaggleRuntime()
    coordinator = MasterCoordinator(ledger, fake)
    active = coordinator.ensure_master(intent("active-master"), runtime_secret=SECRET)
    coordinator.accept_runtime_event(
        runtime_event(
            active,
            RuntimeEventType.SERVICE_READY,
            1,
            clock.now(),
            service_kind="postgres-master",
            endpoint="tunnel://active-master",
            protocol="postgresql+tls",
            tls_fingerprint="sha256:" + "a" * 64,
            capabilities=["sql"],
            canonical_revision=1,
            schema_version="15",
            lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
            master_instance_id=active.master_instance_id,
            epoch=active.epoch,
        ),
        header_token=SECRET,
    )
    before_attempts = sqlite3.connect(ledger.path).execute("SELECT count(*) FROM run_attempts").fetchone()
    before_effects = sqlite3.connect(ledger.path).execute("SELECT count(*) FROM effects").fetchone()

    with pytest.raises(MasterAdmissionRejected, match="master admission"):
        coordinator.ensure_master(intent("replacement-too-early"), runtime_secret=SECRET)

    assert ledger.current_epoch("postgres-master") == active.epoch
    service = ledger.resolve_service("postgres-master")
    assert service is not None
    assert service.service_instance_id == active.service_instance_id
    assert service.state == MasterState.ACTIVE.value
    assert sqlite3.connect(ledger.path).execute("SELECT count(*) FROM run_attempts").fetchone() == before_attempts
    assert sqlite3.connect(ledger.path).execute("SELECT count(*) FROM effects").fetchone() == before_effects
    assert fake.physical_effect_counts == {
        "ensure_dataset": 1,
        "push_notebook": 1,
        "trigger_run": 1,
    }


def test_stopped_master_requires_its_verified_checkpoint_before_new_epoch(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    fake = FakeKaggleRuntime()
    coordinator = MasterCoordinator(ledger, fake)
    stopped = coordinator.ensure_master(intent("stopped-master"), runtime_secret=SECRET)
    ledger.transition_operation(
        stopped.operation_id,
        expected_state=MasterState.REGISTERING.value,
        new_state=MasterState.STOPPED.value,
        metadata={"reason": "test-stop-before-checkpoint"},
    )

    with pytest.raises(MasterAdmissionRejected, match="verified checkpoint"):
        coordinator.ensure_master(intent("rotation-without-checkpoint"), runtime_secret=SECRET)
    assert ledger.current_epoch("postgres-master") == stopped.epoch

    checkpoint_id = "stopped-master-checkpoint"
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=stopped.operation_id,
        dataset_ref="private/checkpoint-dataset",
        version_ref=None,
        manifest_sha256="d" * 64,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=stopped.master_instance_id,
        epoch=stopped.epoch,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "private/checkpoint-dataset/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )

    replacement = coordinator.ensure_master(intent("rotation-with-checkpoint"), runtime_secret=SECRET)
    assert replacement.epoch == stopped.epoch + 1
    assert replacement.state == MasterState.REGISTERING


@pytest.mark.parametrize("intermediate_failed_epoch", [False, True])
def test_forced_rotation_atomically_binds_the_latest_stopped_checkpoint_handoff(
    tmp_path: Path, intermediate_failed_epoch: bool
) -> None:
    ledger = ledger_at(tmp_path)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    source = coordinator.ensure_master(intent("rotation-source"), runtime_secret=SECRET)
    ledger.transition_operation(
        source.operation_id,
        expected_state=MasterState.REGISTERING.value,
        new_state=MasterState.STOPPED.value,
        metadata={"reason": "rotation-source-stopped"},
    )
    checkpoint_id = "rotation-source-checkpoint"
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=source.operation_id,
        dataset_ref="private/checkpoint-dataset",
        version_ref=None,
        manifest_sha256="c" * 64,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=source.master_instance_id,
        epoch=source.epoch,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "private/checkpoint-dataset/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master", checkpoint_id, expected_generation=0, expected_parent_checkpoint_id=None
    )
    rotation_request_id = "a" * 64
    ledger.ensure_operation(
        operation_id=rotation_request_id,
        idempotency_key="acceptance-rotation-request",
        operation_kind="forced_master_rotation",
        intent={"checkpoint_id": checkpoint_id},
        initial_state="REQUESTED",
        identity={
            "checkpoint_id": checkpoint_id,
            "expected_active_epoch": source.epoch,
            "head_generation": 1,
        },
    )
    if intermediate_failed_epoch:
        intermediate = coordinator.ensure_master(intent("intermediate-failure"), runtime_secret=SECRET)
        ledger.transition_operation(
            intermediate.operation_id,
            expected_state=MasterState.REGISTERING.value,
            new_state=MasterState.FAILED.value,
            metadata={"reason": "intermediate-provider-failure"},
        )

    forced = intent(f"forced-rotation:{rotation_request_id}")
    if intermediate_failed_epoch:
        with pytest.raises(MasterAdmissionRejected, match="exact verified STOPPED handoff"):
            coordinator.ensure_master(forced, runtime_secret=SECRET)
        assert ledger.current_epoch("postgres-master") == 2
        assert ledger.get_operation(MasterCoordinator.identity_for(forced.idempotency_key)["operation_id"]) is None
    else:
        replacement = coordinator.ensure_master(forced, runtime_secret=SECRET)
        assert replacement.epoch == source.epoch + 1


@pytest.mark.parametrize(
    "state",
    [
        MasterState.REQUESTED,
        MasterState.STARTING,
        MasterState.RESTORING,
        MasterState.REGISTERING,
        MasterState.ACTIVE,
        MasterState.DRAINING,
        MasterState.CHECKPOINTING,
        MasterState.CHECKPOINT_FAILED,
    ],
)
def test_every_nonterminal_master_state_blocks_distinct_epoch_allocation(tmp_path: Path, state: MasterState) -> None:
    ledger = ledger_at(tmp_path)
    first_identity = MasterCoordinator.identity_for(f"blocked-{state.value}")
    first, created = ledger.ensure_master_operation(
        operation_id=first_identity["operation_id"],
        idempotency_key=f"blocked-{state.value}",
        intent=intent(f"blocked-{state.value}").as_dict(),
        identity=first_identity,
    )
    assert created
    if state is not MasterState.REQUESTED:
        first = ledger.transition_operation(
            first.operation_id,
            expected_state=MasterState.REQUESTED.value,
            new_state=state.value,
            metadata={"reason": "admission-state-test"},
        )
    second_identity = MasterCoordinator.identity_for(f"blocked-{state.value}-replacement")

    with pytest.raises(MasterAdmissionRejected, match=state.value):
        ledger.ensure_master_operation(
            operation_id=second_identity["operation_id"],
            idempotency_key=f"blocked-{state.value}-replacement",
            intent=intent(f"blocked-{state.value}-replacement").as_dict(),
            identity=second_identity,
        )

    assert ledger.current_epoch("postgres-master") == 1
    assert ledger.get_operation(second_identity["operation_id"]) is None


@pytest.mark.parametrize("terminal_state", [MasterState.FAILED, MasterState.FENCED, MasterState.ORPHANED])
def test_failed_fenced_or_orphaned_master_permits_exact_next_epoch(tmp_path: Path, terminal_state: MasterState) -> None:
    ledger = ledger_at(tmp_path)
    first_identity = MasterCoordinator.identity_for(f"terminal-{terminal_state.value}")
    first, _created = ledger.ensure_master_operation(
        operation_id=first_identity["operation_id"],
        idempotency_key=f"terminal-{terminal_state.value}",
        intent=intent(f"terminal-{terminal_state.value}").as_dict(),
        identity=first_identity,
    )
    ledger.transition_operation(
        first.operation_id,
        expected_state=MasterState.REQUESTED.value,
        new_state=terminal_state.value,
        metadata={"reason": "terminal-admission-test"},
    )
    second_identity = MasterCoordinator.identity_for(f"after-{terminal_state.value}")

    second, created = ledger.ensure_master_operation(
        operation_id=second_identity["operation_id"],
        idempotency_key=f"after-{terminal_state.value}",
        intent=intent(f"after-{terminal_state.value}").as_dict(),
        identity=second_identity,
    )

    assert created
    assert second.identity["epoch"] == 2
    assert ledger.current_epoch("postgres-master") == 2


def test_checkpoint_verified_operation_still_waits_for_terminal_service_handoff(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(intent("terminal-handoff"), runtime_secret=SECRET)
    coordinator.accept_runtime_event(
        runtime_event(
            handle,
            RuntimeEventType.SERVICE_READY,
            1,
            clock.now(),
            service_kind="postgres-master",
            endpoint="tunnel://terminal-handoff",
            protocol="postgresql+tls",
            tls_fingerprint="sha256:" + "e" * 64,
            capabilities=["sql"],
            canonical_revision=1,
            schema_version="15",
            lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
            master_instance_id=handle.master_instance_id,
            epoch=handle.epoch,
        ),
        header_token=SECRET,
    )
    checkpoint_id = "terminal-handoff-checkpoint"
    manifest_sha256 = "f" * 64
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=handle.operation_id,
        dataset_ref="private/checkpoint-dataset",
        version_ref=None,
        manifest_sha256=manifest_sha256,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "private/checkpoint-dataset/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master", checkpoint_id, expected_generation=0, expected_parent_checkpoint_id=None
    )
    shutdown = (
        (RuntimeEventType.RUNTIME_DRAINING, {}),
        (RuntimeEventType.CHECKPOINT_STARTED, {}),
        (
            RuntimeEventType.CHECKPOINT_VERIFIED,
            {
                "checkpoint_id": checkpoint_id,
                "manifest_sha256": manifest_sha256,
                "current_checkpoint_id": checkpoint_id,
            },
        ),
    )
    for sequence, (event_type, data) in enumerate(shutdown, start=2):
        coordinator.accept_runtime_event(
            runtime_event(handle, event_type, sequence, clock.now(), **data), header_token=SECRET
        )
    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == MasterState.STOPPED.value
    replacement_intent = intent("after-terminal-handoff")

    with pytest.raises(MasterAdmissionRejected, match=r"service .* DRAINING"):
        coordinator.ensure_master(replacement_intent, runtime_secret=SECRET)
    assert ledger.current_epoch("postgres-master") == handle.epoch

    coordinator.accept_runtime_event(
        runtime_event(handle, RuntimeEventType.RUNTIME_TERMINAL, 5, clock.now()), header_token=SECRET
    )
    replacement = coordinator.ensure_master(replacement_intent, runtime_secret=SECRET)
    assert replacement.epoch == handle.epoch + 1


def test_effect_is_durable_before_provider_is_called(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)

    class InspectingProvider(FakeKaggleRuntime):
        def execute(self, effect):  # type: ignore[no-untyped-def]
            durable = ledger.get_effect_by_idempotency_key(effect.idempotency_key)
            assert durable is not None
            assert durable.state.value == "IN_PROGRESS"
            return super().execute(effect)

    provider = InspectingProvider()
    handle = MasterCoordinator(ledger, provider).ensure_master(intent("persist-first"), runtime_secret=SECRET)
    assert handle.state == MasterState.REGISTERING


@pytest.mark.parametrize("crash_effect", ["ensure_dataset", "push_notebook", "trigger_run"])
def test_crash_after_provider_effect_reconciles_by_exact_identity(tmp_path: Path, crash_effect: str) -> None:
    ledger = ledger_at(tmp_path)
    fake = FakeKaggleRuntime({crash_effect: [SimulatedProcessCrash(crash_effect)]})
    first = MasterCoordinator(ledger, fake)
    request = intent(f"crash-{crash_effect}")
    with pytest.raises(SimulatedProcessCrash):
        first.ensure_master(request, runtime_secret=SECRET)

    restarted_ledger = ControlLedger(ledger.path)
    restarted = MasterCoordinator(restarted_ledger, fake)
    handle = restarted.ensure_master(request, runtime_secret=SECRET)
    handle = restarted.reconcile_operation(handle.operation_id, request)
    assert handle.state == MasterState.REGISTERING
    assert all(count == 1 for count in fake.physical_effect_counts.values())
    assert fake.physical_effect_counts[crash_effect] == 1


def test_crash_before_provider_effect_retries_only_after_exact_absence(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    fake = FakeKaggleRuntime({"ensure_dataset": [RuntimeError("crash-before-call")]})
    request = intent("crash-before-effect")
    with pytest.raises(RuntimeError, match="crash-before-call"):
        MasterCoordinator(ledger, fake).ensure_master(request, runtime_secret=SECRET)
    recovered = MasterCoordinator(ControlLedger(ledger.path), fake).ensure_master(request, runtime_secret=SECRET)
    assert recovered.state == MasterState.REGISTERING
    assert fake.physical_effect_counts["ensure_dataset"] == 1


def test_absent_trigger_after_tunnel_expiry_fences_attempt_and_allows_next_epoch(
    tmp_path: Path,
) -> None:
    clock = DeterministicClock(datetime(2026, 8, 11, 13, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    fake = FakeKaggleRuntime({"trigger_run": [RuntimeError("response lost before provider mutation")]})

    class ExpiringAuthority:
        def __init__(self) -> None:
            self.highest_epoch = 0
            self.active: dict[str, object] | None = None
            self.overlap = False

        def activate(self, **kwargs):  # type: ignore[no-untyped-def]
            epoch = int(kwargs["epoch"])
            if self.active is not None and self.active["epoch"] != epoch:
                self.overlap = True
            if epoch <= self.highest_epoch:
                raise RuntimeError("expired tunnel activation cannot be revived at the same epoch")
            self.highest_epoch = epoch
            self.active = dict(kwargs)
            return object()

        def renew(self, **_kwargs):  # type: ignore[no-untyped-def]
            return object()

        def deactivate(self, **kwargs):  # type: ignore[no-untyped-def]
            if self.active is None or self.active["epoch"] != kwargs["epoch"]:
                raise RuntimeError("already absent")
            self.active = None

    authority = ExpiringAuthority()
    first = MasterCoordinator(ledger, fake, tunnel_authority=authority)
    first_intent = intent("expired-trigger-first")
    with pytest.raises(RuntimeError, match="response lost"):
        first.ensure_master(first_intent, runtime_secret=SECRET)
    identity = MasterCoordinator.identity_for(first_intent.idempotency_key)
    old = ledger.get_operation(identity["operation_id"])
    assert old is not None and old.state == MasterState.RESTORING.value
    trigger = ledger.get_effect_by_idempotency_key(f"{old.operation_id}:trigger_run")
    assert trigger is not None and trigger.state == EffectState.IN_PROGRESS
    assert authority.active is not None and authority.active["epoch"] == 1

    clock.advance(301)
    restarted = MasterCoordinator(ledger, fake, tunnel_authority=authority)
    recovered = restarted.reconcile_operation(old.operation_id, first_intent)
    assert recovered.state == MasterState.FAILED
    assert ledger.get_effect_by_idempotency_key(f"{old.operation_id}:trigger_run").state == EffectState.FAILED  # type: ignore[union-attr]
    assert not ledger.runtime_token_valid(identity["run_id"], identity["attempt_id"], SECRET)
    with sqlite3.connect(ledger.path) as connection:
        attempt_state = connection.execute(
            "SELECT state FROM run_attempts WHERE attempt_id=?", (identity["attempt_id"],)
        ).fetchone()
    assert attempt_state == (MasterState.FENCED.value,)
    assert authority.active is None

    replacement = restarted.ensure_master(intent("expired-trigger-replacement"), runtime_secret=SECRET)
    assert replacement.epoch == 2
    assert replacement.state == MasterState.REGISTERING
    assert authority.active is not None and authority.active["epoch"] == 2
    assert not authority.overlap


def test_callbacks_dedupe_coalesce_size_and_fence_stale_epoch(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle_a = coordinator.ensure_master(intent("master-a"), runtime_secret=SECRET)
    ready = runtime_event(
        handle_a,
        RuntimeEventType.SERVICE_READY,
        1,
        clock.now(),
        service_kind="postgres-master",
        endpoint="tunnel://master-a",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "a" * 64,
        capabilities=["sql", "fts", "pgvector"],
        canonical_revision=1,
        schema_version="1",
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
        master_instance_id=handle_a.master_instance_id,
        epoch=handle_a.epoch,
    )
    assert coordinator.accept_runtime_event(ready, header_token=SECRET).disposition == EventDisposition.ACCEPTED
    assert coordinator.accept_runtime_event(ready, header_token=SECRET).disposition == EventDisposition.DUPLICATE
    assert ledger.resolve_service("postgres-master") is not None

    heartbeat = runtime_event(
        handle_a,
        RuntimeEventType.RUNTIME_HEARTBEAT,
        2,
        clock.now(),
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
    )
    assert coordinator.accept_runtime_event(heartbeat, header_token=SECRET).disposition == EventDisposition.ACCEPTED
    next_heartbeat = runtime_event(
        handle_a,
        RuntimeEventType.RUNTIME_HEARTBEAT,
        3,
        clock.now(),
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
    )
    assert (
        coordinator.accept_runtime_event(next_heartbeat, header_token=SECRET).disposition == EventDisposition.COALESCED
    )

    with pytest.raises(EventRejected, match="64 KiB"):
        coordinator.accept_runtime_event(b"{" + b"x" * (64 * 1024), header_token=SECRET)
    with pytest.raises(EventRejected, match="token"):
        coordinator.accept_runtime_event(ready, header_token="wrong-secret-credential")

    # A distinct ensure may advance only after the old lifecycle is terminal.
    # Keep its runtime token valid so the next assertion still exercises the
    # stale-epoch disposition rather than token revocation.
    ledger.project_master_lifecycle(
        operation_id=handle_a.operation_id,
        service_instance_id=handle_a.service_instance_id,
        epoch=handle_a.epoch,
        expected_operation_state=MasterState.ACTIVE.value,
        operation_state=MasterState.FENCED.value,
        service_state=MasterState.FENCED.value,
        event_id="test-explicit-fence-before-replacement",
    )
    handle_b = coordinator.ensure_master(intent("master-b"), runtime_secret=SECRET)
    assert handle_b.epoch == 2
    stale = runtime_event(
        handle_a,
        RuntimeEventType.RUNTIME_HEARTBEAT,
        4,
        clock.now(),
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
    )
    assert coordinator.accept_runtime_event(stale, header_token=SECRET).disposition == EventDisposition.FENCED
    assert ledger.resolve_service("postgres-master") is None


def test_tunnel_authority_activates_before_run_and_renews_on_exact_heartbeat(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)

    class Authority:
        def __init__(self) -> None:
            self.activated: list[dict[str, object]] = []
            self.renewed: list[dict[str, object]] = []

        def activate(self, **kwargs):  # type: ignore[no-untyped-def]
            self.activated.append(kwargs)
            return object()

        def renew(self, **kwargs):  # type: ignore[no-untyped-def]
            self.renewed.append(kwargs)
            return object()

        def deactivate(self, **kwargs):  # type: ignore[no-untyped-def]
            return None

    authority = Authority()
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime(), tunnel_authority=authority)
    handle = coordinator.ensure_master(intent("tunnel-authority"), runtime_secret=SECRET)
    assert len(authority.activated) == 1
    assert authority.activated[0]["epoch"] == handle.epoch
    ready = runtime_event(
        handle,
        RuntimeEventType.SERVICE_READY,
        1,
        clock.now(),
        service_kind="postgres-master",
        endpoint="tunnel://master",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "a" * 64,
        capabilities=["sql"],
        canonical_revision=1,
        schema_version="1",
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    coordinator.accept_runtime_event(ready, header_token=SECRET)
    heartbeat = runtime_event(
        handle,
        RuntimeEventType.RUNTIME_HEARTBEAT,
        2,
        clock.now(),
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
    )
    assert coordinator.accept_runtime_event(heartbeat, header_token=SECRET).disposition == EventDisposition.ACCEPTED
    assert coordinator.accept_runtime_event(heartbeat, header_token=SECRET).disposition == EventDisposition.DUPLICATE
    assert len(authority.renewed) == 2
    assert all(call["master_instance_id"] == handle.master_instance_id for call in authority.renewed)


def test_duplicate_ready_callback_repairs_crash_between_event_and_projection(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(intent("ready-crash-window"), runtime_secret=SECRET)
    ready = runtime_event(
        handle,
        RuntimeEventType.SERVICE_READY,
        1,
        clock.now(),
        service_kind="postgres-master",
        endpoint="tunnel://recovered",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "a" * 64,
        capabilities=["sql"],
        canonical_revision=1,
        schema_version="1",
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    assert ledger.ingest_runtime_event(ready, header_token=SECRET).disposition == EventDisposition.ACCEPTED
    registering = ledger.get_operation(handle.operation_id)
    assert registering is not None and registering.state == MasterState.REGISTERING.value
    assert coordinator.accept_runtime_event(ready, header_token=SECRET).disposition == EventDisposition.DUPLICATE
    active = ledger.get_operation(handle.operation_id)
    service = ledger.resolve_service("postgres-master")
    assert active is not None and active.state == MasterState.ACTIVE.value
    assert service is not None and service.endpoint == "tunnel://recovered"


def test_checkpoint_callbacks_project_drain_stop_and_revoke_runtime_token(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(intent("checkpoint-lifecycle"), runtime_secret=SECRET)
    ready = runtime_event(
        handle,
        RuntimeEventType.SERVICE_READY,
        1,
        clock.now(),
        service_kind="postgres-master",
        endpoint="tunnel://checkpoint",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "a" * 64,
        capabilities=["sql"],
        canonical_revision=1,
        schema_version="13",
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    coordinator.accept_runtime_event(ready, header_token=SECRET)
    checkpoint_id = "checkpoint-lifecycle-verified"
    manifest_sha256 = "c" * 64
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        service_kind="postgres-master",
        operation_id=handle.operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256=manifest_sha256,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "owner/checkpoints/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    for sequence, event_type in enumerate(
        (
            RuntimeEventType.RUNTIME_DRAINING,
            RuntimeEventType.CHECKPOINT_STARTED,
            RuntimeEventType.CHECKPOINT_VERIFIED,
            RuntimeEventType.RUNTIME_TERMINAL,
        ),
        start=2,
    ):
        data = (
            {
                "checkpoint_id": checkpoint_id,
                "manifest_sha256": manifest_sha256,
                "current_checkpoint_id": checkpoint_id,
            }
            if event_type == RuntimeEventType.CHECKPOINT_VERIFIED
            else {}
        )
        event = runtime_event(handle, event_type, sequence, clock.now(), **data)
        coordinator.accept_runtime_event(event, header_token=SECRET)
    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == MasterState.STOPPED.value
    row = (
        sqlite3.connect(ledger.path)
        .execute("SELECT state FROM services WHERE service_instance_id=?", (handle.service_instance_id,))
        .fetchone()
    )
    assert row == (MasterState.STOPPED.value,)
    assert not ledger.runtime_token_valid(handle.run_id, handle.attempt_id, SECRET)


def test_stale_output_never_completes_attempt() -> None:
    expected = {
        "run_id": "run-a",
        "attempt_id": "attempt-a",
        "source_identity": "source",
        "source_version": "v1",
        "epoch": 2,
    }
    stale = ExactOutput("run-old", "attempt-a", "source", "v1", 2, "succeeded")
    assert (
        decide_terminal(platform_status=PlatformStatus.COMPLETE, output=stale, **expected) == TerminalDecision.AMBIGUOUS
    )
    exact = ExactOutput("run-a", "attempt-a", "source", "v1", 2, "succeeded")
    assert (
        decide_terminal(platform_status=PlatformStatus.UNKNOWN, output=exact, **expected) == TerminalDecision.AMBIGUOUS
    )
    assert (
        decide_terminal(platform_status=PlatformStatus.COMPLETE, output=exact, **expected) == TerminalDecision.SUCCEEDED
    )


def test_lost_terminal_response_replays_exact_duplicate_and_empties_spool(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(intent("terminal-response-loss"), runtime_secret=SECRET)
    ready = runtime_event(
        handle,
        RuntimeEventType.SERVICE_READY,
        1,
        clock.now(),
        service_kind="postgres-master",
        endpoint="tunnel://terminal-response-loss",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "a" * 64,
        capabilities=["sql"],
        canonical_revision=1,
        schema_version="13",
        lease_until=(clock.now() + timedelta(minutes=5)).isoformat(),
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    coordinator.accept_runtime_event(ready, header_token=SECRET)
    delivered_events = [ready]
    checkpoint_id = "checkpoint-terminal-response-loss"
    manifest_sha256 = "e" * 64
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        service_kind="postgres-master",
        operation_id=handle.operation_id,
        dataset_ref="private/checkpoint-dataset",
        version_ref=None,
        manifest_sha256=manifest_sha256,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "private/checkpoint-dataset/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master", checkpoint_id, expected_generation=0, expected_parent_checkpoint_id=None
    )
    for sequence, event_type in enumerate(
        (
            RuntimeEventType.RUNTIME_DRAINING,
            RuntimeEventType.CHECKPOINT_STARTED,
            RuntimeEventType.CHECKPOINT_VERIFIED,
        ),
        start=2,
    ):
        data = (
            {
                "checkpoint_id": checkpoint_id,
                "manifest_sha256": manifest_sha256,
                "current_checkpoint_id": checkpoint_id,
            }
            if event_type == RuntimeEventType.CHECKPOINT_VERIFIED
            else {}
        )
        event = runtime_event(handle, event_type, sequence, clock.now(), **data)
        coordinator.accept_runtime_event(event, header_token=SECRET)
        delivered_events.append(event)

    class CommitThenLoseResponse:
        def __init__(self) -> None:
            self.calls = 0
            self.body = b""

        def post(self, url, body, headers, timeout_seconds):  # type: ignore[no-untyped-def]
            del url, headers, timeout_seconds
            self.calls += 1
            self.body = body
            receipt = coordinator.accept_runtime_event(body, header_token=SECRET)
            if self.calls == 1:
                assert receipt.disposition == EventDisposition.ACCEPTED
                raise OSError("response lost after durable commit")
            assert receipt.disposition == EventDisposition.DUPLICATE
            return TransportResponse(200)

    transport = CommitThenLoseResponse()
    spool_path = tmp_path / "terminal-spool.jsonl"
    prior_spool = JsonlEventSpool(spool_path)
    for raw_event in delivered_events:
        payload = json.loads(raw_event)
        prior_spool.append_event(payload)
        prior_spool.acknowledge(str(payload["event_id"]), clock.now().isoformat())
    client = RuntimeClient(
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        run_secret=SECRET,
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        epoch=handle.epoch,
        spool_path=spool_path,
        transport=transport,
        retry_policy=RetryPolicy(
            max_attempts=1,
            base_seconds=0,
            max_seconds=0,
            jitter_ratio=0,
            timeout_seconds=1,
        ),
        now=clock.now,
        sleep=lambda _: None,
    )
    queued = client.emit(
        RuntimeEventType.RUNTIME_TERMINAL,
        status="succeeded",
        data={"checkpoint_id": checkpoint_id},
    )
    assert queued.status == "queued" and len(client.spool.pending()) == 1
    operation_log_before = sqlite3.connect(ledger.path).execute("SELECT count(*) FROM operation_log").fetchone()
    assert client.flush_pending()
    assert client.spool.pending() == []
    operation_log_after = sqlite3.connect(ledger.path).execute("SELECT count(*) FROM operation_log").fetchone()
    assert operation_log_after == operation_log_before
    assert not ledger.runtime_token_valid(handle.run_id, handle.attempt_id, SECRET)

    with pytest.raises(EventRejected, match="token"):
        coordinator.accept_runtime_event(transport.body, header_token="wrong-former-token")
    altered = json.loads(transport.body)
    altered["status"] = "failed"
    with pytest.raises(EventRejected, match="different body"):
        coordinator.accept_runtime_event(
            json.dumps(altered, sort_keys=True, separators=(",", ":")).encode(), header_token=SECRET
        )
    altered["run_id"] = str(uuid4())
    with pytest.raises(StaleRuntimeEvent, match="unknown run/attempt"):
        coordinator.accept_runtime_event(
            json.dumps(altered, sort_keys=True, separators=(",", ":")).encode(), header_token=SECRET
        )


def test_checkpoint_promotion_keeps_previous_and_failed_candidate_cannot_advance(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    operation, _ = ledger.ensure_operation(
        operation_id=str(uuid4()),
        idempotency_key="checkpoint-operation",
        operation_kind="checkpoint",
        intent={"service_kind": "postgres-master"},
        initial_state="CHECKPOINTING",
        identity={"epoch": 1},
    )
    for checkpoint_id, version in (("cp-1", "v1"), ("cp-2", "v2")):
        ledger.add_checkpoint_candidate(
            checkpoint_id=checkpoint_id,
            operation_id=operation.operation_id,
            dataset_ref="private/checkpoints",
            version_ref=None,
            manifest_sha256=("a" if version == "v1" else "b") * 64,
            source_checkpoint_id=None if version == "v1" else "cp-1",
            master_instance_id="master-1",
            epoch=1,
        )
        with pytest.raises(StaleRuntimeEvent):
            ledger.mark_checkpoint_verified(checkpoint_id)
        ledger.mark_checkpoint_uploaded(checkpoint_id, f"private/checkpoints/{version}")
        ledger.mark_checkpoint_readback_verified(checkpoint_id)
        ledger.mark_checkpoint_restore_verified(checkpoint_id)
        # Persisted forward stages are replay-safe after a process crash, but
        # none of the required gates can be skipped.
        ledger.mark_checkpoint_uploaded(checkpoint_id, f"private/checkpoints/{version}")
        ledger.mark_checkpoint_readback_verified(checkpoint_id)
        ledger.mark_checkpoint_restore_verified(checkpoint_id)
        current = ledger.checkpoint_head("postgres-master")
        ledger.promote_checkpoint(
            "postgres-master",
            checkpoint_id,
            expected_generation=current.generation if current else 0,
            expected_parent_checkpoint_id=current.current_checkpoint_id if current else None,
        )
    head = ledger.checkpoint_head("postgres-master")
    assert head is not None
    assert (head.current_checkpoint_id, head.previous_checkpoint_id) == ("cp-2", "cp-1")

    ledger.add_checkpoint_candidate(
        checkpoint_id="cp-bad",
        operation_id=operation.operation_id,
        dataset_ref="private/checkpoints",
        version_ref="v-bad",
        manifest_sha256="c" * 64,
        source_checkpoint_id="cp-2",
        master_instance_id="master-1",
        epoch=1,
    )
    ledger.fail_checkpoint("cp-bad", "readback_hash_mismatch")
    with pytest.raises(StaleRuntimeEvent):
        ledger.promote_checkpoint(
            "postgres-master",
            "cp-bad",
            expected_generation=head.generation,
            expected_parent_checkpoint_id=head.current_checkpoint_id,
        )
    assert ledger.checkpoint_head("postgres-master") == head


def test_oauth_revocation_stores_only_hash_and_audit_reference(tmp_path: Path) -> None:
    ledger = ledger_at(tmp_path)
    raw_reference = "opaque-token-reference-never-store"
    digest = ledger.revoke_oauth_reference(
        token_reference=raw_reference,
        client_id="chatgpt-client",
        principal_id="owner",
        reason_code="owner_revoked",
        audit_ref="audit://revocations/1",
    )
    assert ledger.is_oauth_reference_revoked(raw_reference)
    stored = sqlite3.connect(ledger.path).execute("SELECT token_ref_sha256 FROM oauth_revocations").fetchone()
    assert stored == (digest,)
    database_bytes = b"".join(path.read_bytes() for path in (ledger.path, Path(f"{ledger.path}-wal")) if path.exists())
    assert raw_reference.encode() not in database_bytes


def test_resource_leases_are_monotonic_fenced_and_releasable(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    first = ledger.acquire_resource_lease(
        lease_id="lease-a",
        resource_kind="physical-kernel",
        resource_ref="owner/kernel",
        holder_id="attempt-a",
        lease_until=clock.now() + timedelta(minutes=2),
    )
    assert first.epoch == 1
    with pytest.raises(Exception, match="active lease"):
        ledger.acquire_resource_lease(
            lease_id="lease-b",
            resource_kind="physical-kernel",
            resource_ref="owner/kernel",
            holder_id="attempt-b",
            lease_until=clock.now() + timedelta(minutes=2),
        )
    ledger.renew_resource_lease("lease-a", "attempt-a", 1, clock.now() + timedelta(minutes=3))
    ledger.release_resource_lease("lease-a", "attempt-a", 1)
    second = ledger.acquire_resource_lease(
        lease_id="lease-b",
        resource_kind="physical-kernel",
        resource_ref="owner/kernel",
        holder_id="attempt-b",
        lease_until=clock.now() + timedelta(minutes=2),
    )
    assert second.epoch == 2
    with pytest.raises(Exception, match="stale"):
        ledger.renew_resource_lease("lease-a", "attempt-a", 1, clock.now() + timedelta(minutes=3))


def test_runtime_retention_is_explicit_and_preserves_latest_projection(tmp_path: Path) -> None:
    clock = DeterministicClock(datetime(2026, 8, 10, 18, 0, tzinfo=UTC))
    ledger = ledger_at(tmp_path, clock)
    coordinator = MasterCoordinator(ledger, FakeKaggleRuntime())
    handle = coordinator.ensure_master(intent("retention-master"), runtime_secret=SECRET)
    ready = runtime_event(
        handle,
        RuntimeEventType.SERVICE_READY,
        1,
        clock.now(),
        service_kind="postgres-master",
        endpoint="tunnel://retention",
        protocol="postgresql+tls",
        tls_fingerprint="sha256:" + "a" * 64,
        capabilities=["sql"],
        canonical_revision=1,
        schema_version="1",
        lease_until=(clock.now() + timedelta(hours=2)).isoformat(),
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    coordinator.accept_runtime_event(ready, header_token=SECRET)
    clock.advance(60)
    heartbeat = runtime_event(
        handle,
        RuntimeEventType.RUNTIME_HEARTBEAT,
        2,
        clock.now(),
        lease_until=(clock.now() + timedelta(hours=2)).isoformat(),
    )
    coordinator.accept_runtime_event(heartbeat, header_token=SECRET)
    clock.advance(60)
    deleted, deduped = ledger.prune_runtime_events(nonterminal_before=clock.now())
    assert (deleted, deduped) == (1, 1)
    connection = sqlite3.connect(ledger.path)
    assert connection.execute("SELECT event_type FROM runtime_events").fetchall() == [("runtime.heartbeat",)]
    assert connection.execute("SELECT count(*) FROM retention_runs").fetchone() == (1,)
    connection.close()


def test_ten_thousand_deterministic_transition_sequences_preserve_invariants() -> None:
    rng = random.Random(20260810)
    normal = {
        MasterState.ABSENT: MasterSignal.REQUEST,
        MasterState.REQUESTED: MasterSignal.DATASET_READY,
        MasterState.STARTING: MasterSignal.SOURCE_PUSHED,
        MasterState.RESTORING: MasterSignal.RUN_TRIGGERED,
        MasterState.REGISTERING: MasterSignal.SERVICE_READY,
        MasterState.ACTIVE: MasterSignal.DRAIN,
        MasterState.DRAINING: MasterSignal.DRAINED,
        MasterState.CHECKPOINTING: MasterSignal.CHECKPOINT_VERIFIED,
    }
    terminal = {
        MasterState.STOPPED,
        MasterState.FAILED,
        MasterState.FENCED,
        MasterState.ORPHANED,
    }
    for _ in range(10_000):
        state = MasterState.ABSENT
        active_epochs = 0
        for _step in range(12):
            if state in terminal:
                break
            signal = rng.choice(
                [normal.get(state, MasterSignal.FAIL), MasterSignal.FAIL, MasterSignal.FENCE, MasterSignal.ORPHAN]
            )
            result = transition_master(state, signal)
            state = result.current
            if state == MasterState.ACTIVE:
                active_epochs += 1
            assert active_epochs <= 1
            if state == MasterState.FENCED:
                assert result.next_effect.value == "stop_runtime"
