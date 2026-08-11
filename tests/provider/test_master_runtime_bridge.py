from __future__ import annotations

import json
import sqlite3
from collections import Counter
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.orchestrator.master import MasterCoordinator, MasterIntent, MasterState
from my_data_hub.providers.kaggle import (
    KaggleKernelRunIdentity,
    KaggleMasterLaunchAssets,
    KaggleMasterRuntimeProvider,
    MasterLaunchContractError,
)
from my_data_hub.providers.kaggle.source_attestation import executable_source_sha256
from my_data_hub.runtime_sdk import (
    KAGGLE_HARD_CAP_SECONDS,
    KAGGLE_PROVIDER_TIMEOUT_SECONDS,
    RuntimeEvent,
    RuntimeEventType,
)
from my_data_hub.workloads.bloggers.master_stage import BloggerImportStageReceipt, BloggerMigrationRequest

SECRET = "runtime-secret-long-enough"


class FakeKaggleAdapter:
    """Duck-typed fake for the one KaggleProviderAdapter boundary."""

    def __init__(self) -> None:
        self.calls: Counter[str] = Counter()
        self.run: KaggleKernelRunIdentity | None = None
        self.last_notebook_kwargs: dict[str, object] | None = None
        self.status_state = "running"
        self.terminal_payload: dict[str, object] | None = None

    def create_private_dataset(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls["dataset"] += 1
        return SimpleNamespace(
            identity=SimpleNamespace(
                provider_ref=kwargs["intent"].provider_ref,
                version=1,
                package_sha256="a" * 64,
            )
        )

    def push_private_notebook(self, **kwargs):  # type: ignore[no-untyped-def]
        self.calls["notebook_run"] += 1
        self.last_notebook_kwargs = kwargs
        source = kwargs["source"]
        source_sha = executable_source_sha256(source, kernel_type="notebook")
        self.run = KaggleKernelRunIdentity(
            task_run_id=kwargs["task_run_id"],
            provider_ref=kwargs["intent"].provider_ref,
            source_version=1,
            source_sha256=source_sha,
            provider_kernel_id=42,
            provider_run_ref=f"{kwargs['intent'].provider_ref}/1",
            started_at=datetime.now(UTC),
        )
        return SimpleNamespace(run=self.run)

    def push_private_master_notebook_pending_attestation(self, **kwargs):  # type: ignore[no-untyped-def]
        return self.push_private_notebook(**kwargs)

    def read_run_status(self, run):  # type: ignore[no-untyped-def]
        assert self.run == run
        self.calls["run_reconcile"] += 1
        return SimpleNamespace(state=self.status_state)

    def download_exact_run_output_file(  # type: ignore[no-untyped-def]
        self, run, *, destination, file_name, max_bytes
    ):
        assert self.run == run
        assert self.terminal_payload is not None
        assert file_name == "my-data-hub-master-terminal.json"
        assert max_bytes == 256 * 1024
        self.calls["output_download"] += 1
        (destination / file_name).write_text(json.dumps(self.terminal_payload, sort_keys=True, separators=(",", ":")))
        return SimpleNamespace(output_tree_sha256="d" * 64, file_count=1)

    def reconcile_private_notebook_run(self, **kwargs):  # type: ignore[no-untyped-def]
        if self.run is None:
            return None
        if self.run.task_run_id != kwargs["task_run_id"]:
            return None
        if self.run.source_sha256 != kwargs["expected_source_sha256"]:
            return None
        return self.run


def test_concrete_bridge_launches_dataset_notebook_and_run_once(tmp_path: Path) -> None:
    launch = KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:exact",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-launch",
        notebook_ref="owner/postgres-master",
        dataset_files={"config.json": b'{"run":"{{MY_DATA_HUB_RUN_ID}}"}', "checkpoint-verifier.ipynb": b"{}"},
        notebook_source=b'{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}',
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        runtime_token_secret_name="master-runtime-root",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
    )
    adapter = FakeKaggleAdapter()
    provider = KaggleMasterRuntimeProvider(adapter, launch)  # type: ignore[arg-type]
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    coordinator = MasterCoordinator(ledger, provider)
    intent = MasterIntent(
        idempotency_key="bridge-master",
        source_identity=launch.source_identity,
        source_version=launch.source_version,
        checkpoint_ref=launch.checkpoint_ref,
        dataset_ref=launch.dataset_ref,
        notebook_ref=launch.notebook_ref,
    )
    first = coordinator.ensure_master(intent, runtime_secret="runtime-secret-long-enough")
    second = coordinator.ensure_master(intent, runtime_secret="runtime-secret-long-enough")
    assert first.state == second.state == MasterState.REGISTERING
    assert first.run_id == second.run_id == str(UUID(first.run_id))
    assert adapter.calls == {"dataset": 1, "notebook_run": 1, "run_reconcile": 2}
    assert launch.notebook_timeout_seconds == KAGGLE_PROVIDER_TIMEOUT_SECONDS
    assert KAGGLE_PROVIDER_TIMEOUT_SECONDS < KAGGLE_HARD_CAP_SECONDS
    assert adapter.last_notebook_kwargs is not None
    assert adapter.last_notebook_kwargs["timeout_seconds"] == KAGGLE_PROVIDER_TIMEOUT_SECONDS
    with pytest.raises(MasterLaunchContractError, match="reserve"):
        replace(launch, notebook_timeout_seconds=KAGGLE_HARD_CAP_SECONDS)
    with pytest.raises(MasterLaunchContractError, match="owner-pinned"):
        replace(launch, callback_url="https://attacker.example/internal/runtime/events")


def _launch() -> KaggleMasterLaunchAssets:
    return KaggleMasterLaunchAssets(
        source_identity="owner/postgres-master",
        source_version="git:exact",
        checkpoint_ref="owner/checkpoints",
        dataset_ref="owner/master-launch",
        notebook_ref="owner/postgres-master",
        dataset_files={"checkpoint-verifier.ipynb": b"{}"},
        notebook_source=b'{"cells":[],"metadata":{},"nbformat":4,"nbformat_minor":5}',
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        runtime_token_secret_name="master-runtime-root",
        checkpoint_verifier_ref="owner/checkpoint-verifier",
        checkpoint_verifier_source_file="checkpoint-verifier.ipynb",
        checkpoint_probe_relations=("hub.canonical_state",),
    )


def _runtime_event(handle, launch, event_type, sequence, *, phase=None, status=None, data=None):  # type: ignore[no-untyped-def]
    return RuntimeEvent(
        event_id=str(uuid4()),
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        service_instance_id=handle.service_instance_id,
        source_identity=launch.source_identity,
        source_version=launch.source_version,
        event_type=event_type,
        emitted_at=datetime.now(UTC),
        local_sequence=sequence,
        epoch=handle.epoch,
        phase=phase,
        status=status,
        data=data or {},
    )


def _active_master_with_terminal_output(tmp_path: Path):  # type: ignore[no-untyped-def]
    launch = _launch()
    adapter = FakeKaggleAdapter()
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    coordinator = MasterCoordinator(ledger, KaggleMasterRuntimeProvider(adapter, launch))  # type: ignore[arg-type]
    request = MasterIntent(
        idempotency_key="terminal-recovery",
        source_identity=launch.source_identity,
        source_version=launch.source_version,
        checkpoint_ref=launch.checkpoint_ref,
        dataset_ref=launch.dataset_ref,
        notebook_ref=launch.notebook_ref,
    )
    handle = coordinator.ensure_master(request, runtime_secret=SECRET)
    assert adapter.run is not None
    source_sha256 = adapter.run.source_sha256
    ready = _runtime_event(
        handle,
        launch,
        RuntimeEventType.SERVICE_READY,
        1,
        data={
            "service_kind": "postgres-master",
            "endpoint": "tunnel://terminal-recovery",
            "protocol": "postgresql+tls",
            "tls_fingerprint": "sha256:" + "a" * 64,
            "capabilities": ["sql"],
            "canonical_revision": 1,
            "schema_version": "13",
            "lease_until": "2099-01-01T00:00:00+00:00",
            "master_instance_id": handle.master_instance_id,
            "epoch": handle.epoch,
            "executed_source_sha256": source_sha256,
        },
    )
    coordinator.accept_runtime_event(ready.model_dump_json(exclude_none=True).encode(), header_token=SECRET)
    checkpoint_id = str(uuid4())
    manifest_sha256 = "c" * 64
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        service_kind="postgres-master",
        operation_id=handle.operation_id,
        dataset_ref=launch.checkpoint_ref,
        version_ref=None,
        manifest_sha256=manifest_sha256,
        source_checkpoint_id=None,
        source_head_generation=0,
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, f"{launch.checkpoint_ref}/1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    events = [
        _runtime_event(
            handle,
            launch,
            RuntimeEventType.RUNTIME_DRAINING,
            2,
            phase="draining",
            status="closed",
        ),
        _runtime_event(
            handle,
            launch,
            RuntimeEventType.CHECKPOINT_STARTED,
            3,
            phase="checkpointing",
            status="started",
        ),
        _runtime_event(
            handle,
            launch,
            RuntimeEventType.CHECKPOINT_VERIFIED,
            4,
            phase="checkpointing",
            status="verified",
            data={
                "checkpoint_id": checkpoint_id,
                "manifest_sha256": manifest_sha256,
                "current_checkpoint_id": checkpoint_id,
            },
        ),
        _runtime_event(
            handle,
            launch,
            RuntimeEventType.RUNTIME_TERMINAL,
            5,
            phase="stopped",
            status="succeeded",
            data={
                "checkpoint_id": checkpoint_id,
                "executed_source_sha256": source_sha256,
            },
        ),
    ]
    adapter.status_state = "complete"
    adapter.terminal_payload = {
        "schema_version": "my-data-hub-master-terminal.v1",
        "run_id": handle.run_id,
        "attempt_id": handle.attempt_id,
        "service_instance_id": handle.service_instance_id,
        "master_instance_id": handle.master_instance_id,
        "source_identity": launch.source_identity,
        "source_version": launch.source_version,
        "executed_source_sha256": source_sha256,
        "epoch": handle.epoch,
        "status": "succeeded",
        "checkpoint": {
            "checkpoint_id": checkpoint_id,
            "manifest_sha256": manifest_sha256,
            "current_checkpoint_id": checkpoint_id,
        },
        "events": [event.model_dump(mode="json", exclude_none=True) for event in events],
    }
    return launch, adapter, ledger, coordinator, request, handle


def test_service_ready_requires_runtime_computed_exact_push_source_and_replays_loss(
    tmp_path: Path,
) -> None:
    launch = _launch()
    adapter = FakeKaggleAdapter()
    ledger = ControlLedger(tmp_path / "source-attestation.sqlite3")
    coordinator = MasterCoordinator(ledger, KaggleMasterRuntimeProvider(adapter, launch))  # type: ignore[arg-type]
    request = MasterIntent(
        idempotency_key="source-attestation",
        source_identity=launch.source_identity,
        source_version=launch.source_version,
        checkpoint_ref=launch.checkpoint_ref,
        dataset_ref=launch.dataset_ref,
        notebook_ref=launch.notebook_ref,
    )
    handle = coordinator.ensure_master(request, runtime_secret=SECRET)
    assert adapter.run is not None
    common = {
        "service_kind": "postgres-master",
        "endpoint": "tunnel://source-attestation",
        "protocol": "postgresql+tls",
        "tls_fingerprint": "sha256:" + "a" * 64,
        "capabilities": ["sql"],
        "canonical_revision": 1,
        "schema_version": "13",
        "lease_until": "2099-01-01T00:00:00+00:00",
        "master_instance_id": handle.master_instance_id,
        "epoch": handle.epoch,
    }
    mismatch = _runtime_event(
        handle,
        launch,
        RuntimeEventType.SERVICE_READY,
        1,
        data={**common, "executed_source_sha256": "0" * 64},
    )
    with pytest.raises(ValueError, match="exact provider push"):
        coordinator.accept_runtime_event(
            mismatch.model_dump_json(exclude_none=True).encode(), header_token=SECRET
        )
    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == MasterState.REGISTERING.value
    assert ledger.resolve_service("postgres-master") is None

    ready = _runtime_event(
        handle,
        launch,
        RuntimeEventType.SERVICE_READY,
        2,
        data={**common, "executed_source_sha256": adapter.run.source_sha256},
    )
    body = ready.model_dump_json(exclude_none=True).encode()
    accepted = coordinator.accept_runtime_event(body, header_token=SECRET)
    replayed = coordinator.accept_runtime_event(body, header_token=SECRET)
    assert accepted.disposition.value == "accepted"
    assert replayed.disposition.value == "duplicate"
    assert ledger.resolve_service("postgres-master") is not None


def test_exact_terminal_output_recovers_callbacks_lost_through_process_exit(tmp_path: Path) -> None:
    _, adapter, ledger, coordinator, request, handle = _active_master_with_terminal_output(tmp_path)

    restarted = MasterCoordinator(ControlLedger(ledger.path), coordinator.provider)
    recovered_handles = restarted.reconcile_all({request.idempotency_key: request})
    assert len(recovered_handles) == 1
    recovered = recovered_handles[0]

    assert recovered.state == MasterState.STOPPED
    row = (
        sqlite3.connect(ledger.path)
        .execute("SELECT state FROM services WHERE service_instance_id=?", (handle.service_instance_id,))
        .fetchone()
    )
    assert row == (MasterState.STOPPED.value,)
    assert not ledger.runtime_token_valid(handle.run_id, handle.attempt_id, SECRET)
    assert adapter.calls["output_download"] == 1
    audit = (
        sqlite3.connect(ledger.path)
        .execute(
            "SELECT action,audit_ref,metadata_json FROM audit_log WHERE operation_id=?",
            (handle.operation_id,),
        )
        .fetchone()
    )
    assert audit is not None and audit[0] == "master.terminal_recovery"
    metadata = json.loads(audit[2])
    assert audit[1] == f"kaggle-terminal-output-sha256:{metadata['output_receipt_sha256']}"
    assert metadata["provider_status"] == "complete"
    assert metadata["checkpoint_id"] == adapter.terminal_payload["checkpoint"]["checkpoint_id"]  # type: ignore[index]
    recovered_event_ids = tuple(event["event_id"] for event in adapter.terminal_payload["events"])  # type: ignore[index]
    assert tuple(item["event_id"] for item in metadata["events"]) == recovered_event_ids
    assert all(set(item) == {"event_id", "body_sha256"} for item in metadata["events"])
    placeholders = ",".join("?" for _ in recovered_event_ids)
    stored_recovered_bodies = (
        sqlite3.connect(ledger.path)
        .execute(f"SELECT count(*) FROM runtime_events WHERE event_id IN ({placeholders})", recovered_event_ids)
        .fetchone()
    )
    assert stored_recovered_bodies == (0,)
    service_latest = (
        sqlite3.connect(ledger.path)
        .execute("SELECT latest_event_id FROM services WHERE service_instance_id=?", (handle.service_instance_id,))
        .fetchone()
    )
    assert service_latest == (recovered_event_ids[-1],)
    assert restarted.reconcile_all({request.idempotency_key: request}) == []


def test_terminal_output_recovers_committed_blogger_receipt_after_all_callback_acks_are_lost(
    tmp_path: Path,
) -> None:
    _, adapter, ledger, coordinator, request, handle = _active_master_with_terminal_output(tmp_path)
    blogger_request = BloggerMigrationRequest(
        request_id=uuid4(),
        operation_id=UUID(handle.operation_id),
        project_id=uuid4(),
        snapshot_at=datetime.now(UTC),
        source_revision="a" * 40,
    )
    ledger.ensure_blogger_migration_request(
        request_id=str(blogger_request.request_id),
        operation_id=handle.operation_id,
        request_sha256=blogger_request.request_sha256,
        request=blogger_request.model_dump(mode="json"),
    )
    ledger.claim_blogger_migration_request(
        operation_id=handle.operation_id,
        run_id=handle.run_id,
        attempt_id=handle.attempt_id,
        master_instance_id=handle.master_instance_id,
        epoch=handle.epoch,
    )
    blogger_receipt = BloggerImportStageReceipt(
        request_id=blogger_request.request_id,
        operation_id=UUID(handle.operation_id),
        master_instance_id=UUID(handle.master_instance_id),
        run_id=handle.run_id,
        epoch=handle.epoch,
        request_sha256=blogger_request.request_sha256,
        export_batch_id=uuid4(),
        row_count=266,
        distinct_record_ids=266,
        source_file_count=14,
        dispositions={"imported": 266},
        record_id_set_sha256="b" * 64,
        logical_sha256="c" * 64,
        canonical_outcome_sha256="d" * 64,
        actor_count=266,
        account_count=250,
        replayed_count=0,
        canonical_revision=9,
    )
    assert adapter.terminal_payload is not None
    adapter.terminal_payload["blogger_import_receipt"] = blogger_receipt.model_dump(mode="json")

    restarted_ledger = ControlLedger(ledger.path)
    restarted = MasterCoordinator(restarted_ledger, coordinator.provider)
    recovered = restarted.reconcile_all({request.idempotency_key: request})

    assert len(recovered) == 1 and recovered[0].state is MasterState.STOPPED
    stored = restarted_ledger.blogger_migration_request(str(blogger_request.request_id))
    assert stored is not None
    assert stored["state"] == "IMPORT_COMMITTED"
    assert stored["import_receipt"]["canonical_revision"] == 9


def test_provider_error_terminalizes_active_master_without_accepting_stale_output(tmp_path: Path) -> None:
    _, adapter, ledger, coordinator, request, handle = _active_master_with_terminal_output(tmp_path)
    adapter.status_state = "failed"
    adapter.terminal_payload = None

    recovered = coordinator.reconcile_operation(handle.operation_id, request)

    assert recovered.state is MasterState.FAILED
    service = (
        sqlite3.connect(ledger.path)
        .execute("SELECT state FROM services WHERE service_instance_id=?", (handle.service_instance_id,))
        .fetchone()
    )
    assert service == (MasterState.FENCED.value,)
    assert not ledger.runtime_token_valid(handle.run_id, handle.attempt_id, SECRET)


@pytest.mark.parametrize(
    ("delivered_count", "intermediate_state"),
    (
        (1, MasterState.DRAINING),
        (2, MasterState.CHECKPOINTING),
        (3, MasterState.STOPPED),
    ),
)
def test_terminal_reconciliation_completes_from_each_durable_shutdown_state(
    tmp_path: Path, delivered_count: int, intermediate_state: MasterState
) -> None:
    _, adapter, ledger, coordinator, request, handle = _active_master_with_terminal_output(tmp_path)
    assert adapter.terminal_payload is not None
    events = adapter.terminal_payload["events"]
    assert isinstance(events, list)
    for event in events[:delivered_count]:
        coordinator.accept_runtime_event(
            json.dumps(event, sort_keys=True, separators=(",", ":")).encode(),
            header_token=SECRET,
        )
    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == intermediate_state.value

    restarted = MasterCoordinator(ControlLedger(ledger.path), coordinator.provider)
    recovered_handles = restarted.reconcile_all({request.idempotency_key: request})
    assert len(recovered_handles) == 1
    recovered = recovered_handles[0]

    assert recovered.state == MasterState.STOPPED
    row = (
        sqlite3.connect(ledger.path)
        .execute("SELECT state FROM services WHERE service_instance_id=?", (handle.service_instance_id,))
        .fetchone()
    )
    assert row == (MasterState.STOPPED.value,)
    assert not ledger.runtime_token_valid(handle.run_id, handle.attempt_id, SECRET)


def test_terminal_recovery_evidence_is_durable_before_any_lifecycle_projection(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    _, _, ledger, coordinator, request, handle = _active_master_with_terminal_output(tmp_path)

    def reject_projection(**kwargs):  # type: ignore[no-untyped-def]
        del kwargs
        raise RuntimeError("projection unavailable after evidence commit")

    project = ledger.project_master_lifecycle
    monkeypatch.setattr(ledger, "project_master_lifecycle", reject_projection)
    with pytest.raises(RuntimeError, match="after evidence commit"):
        coordinator.reconcile_operation(handle.operation_id, request)

    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == MasterState.ACTIVE.value
    audit = (
        sqlite3.connect(ledger.path)
        .execute("SELECT action,metadata_json FROM audit_log WHERE operation_id=?", (handle.operation_id,))
        .fetchone()
    )
    assert audit is not None and audit[0] == "master.terminal_recovery"
    metadata = json.loads(audit[1])
    assert metadata["provider_status"] == "complete"
    assert len(metadata["events"]) == 4
    monkeypatch.setattr(ledger, "project_master_lifecycle", project)
    assert coordinator.reconcile_operation(handle.operation_id, request).state == MasterState.STOPPED
    audit_count = (
        sqlite3.connect(ledger.path)
        .execute(
            "SELECT count(*) FROM audit_log WHERE operation_id=? AND action='master.terminal_recovery'",
            (handle.operation_id,),
        )
        .fetchone()
    )
    assert audit_count == (1,)


@pytest.mark.parametrize(
    ("field", "stale_value"),
    (
        ("run_id", str(UUID(int=0))),
        ("source_version", "git:stale"),
        ("master_instance_id", str(UUID(int=1))),
    ),
)
def test_stale_or_mismatched_exact_terminal_output_is_denied(tmp_path: Path, field: str, stale_value: str) -> None:
    _, adapter, ledger, coordinator, request, handle = _active_master_with_terminal_output(tmp_path)
    assert adapter.terminal_payload is not None
    adapter.terminal_payload[field] = stale_value

    with pytest.raises(MasterLaunchContractError, match="exact master attempt"):
        coordinator.reconcile_operation(handle.operation_id, request)

    operation = ledger.get_operation(handle.operation_id)
    assert operation is not None and operation.state == MasterState.ACTIVE.value
    assert ledger.runtime_token_valid(handle.run_id, handle.attempt_id, SECRET)
