from __future__ import annotations

import random
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import (
    ControlLedger,
    EventDisposition,
    EventRejected,
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
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType

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
    assert migrations == [(1,), (2,), (3,), (4,), (5,), (6,), (7,), (8,)]

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


def test_packaged_and_repository_control_migrations_are_identical() -> None:
    root = Path(__file__).resolve().parents[2]
    repository = discover_control_migrations(root / "control_migrations")
    packaged = discover_control_migrations(Path(control_migration_module.__file__).with_name("sql"))
    assert [(item.version, item.sha256) for item in repository] == [(item.version, item.sha256) for item in packaged]


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
    row = sqlite3.connect(ledger.path).execute(
        "SELECT state FROM services WHERE service_instance_id=?", (handle.service_instance_id,)
    ).fetchone()
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
        decide_terminal(platform_status=PlatformStatus.UNKNOWN, output=exact, **expected) == TerminalDecision.SUCCEEDED
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
