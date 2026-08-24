from __future__ import annotations

import hashlib
import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import uuid4

import pytest

from my_data_hub.control_plane.ledger import (
    ControlLedger,
    IdempotencyConflict,
    StaleRuntimeEvent,
)


def _ledger_with_checkpoint(tmp_path: Path) -> tuple[ControlLedger, str]:
    ledger = ControlLedger(tmp_path / "control" / "ledger.sqlite3")
    source_operation = str(uuid4())
    ledger.ensure_operation(
        operation_id=source_operation,
        idempotency_key="blogger-pre-change-checkpoint",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="READY",
        identity={"master_instance_id": "master-1", "epoch": 7},
    )
    checkpoint_id = str(uuid4())
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=source_operation,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256="c" * 64,
        source_checkpoint_id=None,
        master_instance_id="master-1",
        epoch=7,
        manifest_payload={"canonical_revision": 12},
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, "owner/checkpoints:1")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=0,
        expected_parent_checkpoint_id=None,
    )
    return ledger, checkpoint_id


def _identity(checkpoint_id: str) -> dict[str, object]:
    return {
        "operation_id": "a" * 64,
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "idempotency_key": "blogger-discovery-exact-replay",
        "principal_id": "owner:one",
        "client_id": "client:chatgpt",
        "master_instance_id": "master-1",
        "epoch": 7,
        "expected_revision": 12,
        "request_sha256": "b" * 64,
        "pre_change_checkpoint_id": checkpoint_id,
    }


def _record_service_authority(
    ledger: ControlLedger,
    *,
    master_instance_id: str,
    epoch: int,
    state: str,
) -> None:
    connection = sqlite3.connect(ledger.path)
    try:
        connection.execute(
            "INSERT INTO service_epochs(service_kind,current_epoch,updated_at) "
            "VALUES ('postgres-master',?,?)",
            (epoch, "2026-08-16T12:00:00Z"),
        )
        connection.execute(
            "INSERT INTO services(service_instance_id,service_kind,run_id,attempt_id,"
            "master_instance_id,epoch,endpoint,protocol,tls_fingerprint,capabilities_json,"
            "canonical_revision,schema_version,lease_until,state,latest_event_id,updated_at) "
            "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                f"postgres-{epoch}",
                "postgres-master",
                f"run-{epoch}",
                f"attempt-{epoch}",
                master_instance_id,
                epoch,
                "tunnel://postgres",
                "postgresql+tls",
                None,
                "[]",
                12,
                "20",
                "2099-01-01T00:00:00Z",
                state,
                f"event-{epoch}",
                "2026-08-16T12:00:00Z",
            ),
        )
        connection.commit()
    finally:
        connection.close()


def _checkpoint(
    ledger: ControlLedger,
    *,
    parent_id: str,
    parent_generation: int,
    checkpoint_id: str,
    master_instance_id: str,
    epoch: int,
    promote: bool = True,
    operation_id: str | None = None,
) -> str:
    operation_id = operation_id or str(uuid4())
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key=f"checkpoint-{checkpoint_id}",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="READY",
        identity={"master_instance_id": master_instance_id, "epoch": epoch},
    )
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/checkpoints",
        version_ref=None,
        manifest_sha256=checkpoint_id.replace("-", "")[:1] * 64,
        source_checkpoint_id=parent_id,
        source_head_generation=parent_generation,
        master_instance_id=master_instance_id,
        epoch=epoch,
        manifest_payload={"canonical_revision": 13},
    )
    if promote:
        ledger.mark_checkpoint_uploaded(
            checkpoint_id, f"owner/checkpoints:{parent_generation + 1}"
        )
        ledger.mark_checkpoint_readback_verified(checkpoint_id)
        ledger.mark_checkpoint_restore_verified(checkpoint_id)
        ledger.promote_checkpoint(
            "postgres-master",
            checkpoint_id,
            expected_generation=parent_generation,
            expected_parent_checkpoint_id=parent_id,
        )
    return checkpoint_id


def test_blogger_import_lifecycle_is_metadata_only_and_exactly_replayable(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    identity = _identity(checkpoint_id)

    first, created = ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]
    replay, replay_created = ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]
    assert created is True
    assert replay_created is False
    assert replay == first
    waiting = ledger.mark_blogger_import_waiting_master(str(identity["operation_id"]))
    assert waiting["state"] == "WAITING_MASTER"
    assert ledger.mark_blogger_import_waiting_master(str(identity["operation_id"])) == waiting

    summary = {
        "create_actor_count": 2,
        "link_existing_count": 1,
        "quarantine_count": 0,
        "account_count": 4,
    }
    preview = ledger.record_blogger_import_preview(
        str(identity["operation_id"]),
        preview_receipt="preview-receipt-1",
        plan_sha256="d" * 64,
        summary=summary,
    )
    assert preview["state"] == "PREVIEWED"
    assert preview["preview_summary"] == summary
    serialized = json.dumps(preview)
    assert "source_record_id" not in serialized
    assert "display_name" not in serialized
    assert "accounts" not in serialized
    assert ledger.record_blogger_import_preview(
        str(identity["operation_id"]),
        preview_receipt="preview-receipt-1",
        plan_sha256="d" * 64,
        summary=summary,
    ) == preview
    with pytest.raises(IdempotencyConflict, match="immutable plan"):
        ledger.record_blogger_import_preview(
            str(identity["operation_id"]),
            preview_receipt="changed-preview",
            plan_sha256="d" * 64,
            summary=summary,
        )

    with pytest.raises(PermissionError, match="durable preview"):
        ledger.begin_blogger_import_apply(
            str(identity["operation_id"]),
            preview_receipt="wrong",
            plan_sha256="d" * 64,
        )
    applying = ledger.begin_blogger_import_apply(
        str(identity["operation_id"]),
        preview_receipt="preview-receipt-1",
        plan_sha256="d" * 64,
    )
    assert applying["state"] == "APPLYING"

    committed = ledger.record_blogger_import_commit(
        str(identity["operation_id"]), affected_rows=3, committed_revision=13
    )
    assert committed["state"] == "COMMITTED_PENDING_CHECKPOINT"
    assert committed["committed_revision"] == 13
    assert ledger.record_blogger_import_commit(
        str(identity["operation_id"]), affected_rows=3, committed_revision=13
    ) == committed
    with pytest.raises(IdempotencyConflict, match="immutable receipt"):
        ledger.record_blogger_import_commit(
            str(identity["operation_id"]), affected_rows=4, committed_revision=13
        )


def test_blogger_import_rejects_identity_conflicts_and_stale_reconciliation(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    identity = _identity(checkpoint_id)
    ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]

    with pytest.raises(IdempotencyConflict, match="different batch or request"):
        ledger.ensure_blogger_import_operation(  # type: ignore[arg-type]
            **{**identity, "request_sha256": "e" * 64}
        )

    ledger.record_blogger_import_preview(
        str(identity["operation_id"]),
        preview_receipt="preview-receipt-1",
        plan_sha256="d" * 64,
        summary={
            "create_actor_count": 1,
            "link_existing_count": 0,
            "quarantine_count": 0,
            "account_count": 1,
        },
    )
    ledger.begin_blogger_import_apply(
        str(identity["operation_id"]),
        preview_receipt="preview-receipt-1",
        plan_sha256="d" * 64,
    )
    receipt = {
        "operation_id": str(identity["operation_id"]),
        "request_sha256": str(identity["request_sha256"]),
        "plan_sha256": "d" * 64,
        "master_instance_id": str(identity["master_instance_id"]),
        "epoch": int(identity["epoch"]),
        "expected_revision": int(identity["expected_revision"]),
        "principal_id": str(identity["principal_id"]),
        "client_id": str(identity["client_id"]),
        "affected_rows": 1,
        "committed_revision": 13,
        "committed_at": "2026-08-16T12:00:00Z",
    }
    projected = ledger.reconcile_blogger_import_commit(**receipt)  # type: ignore[arg-type]
    replay = ledger.reconcile_blogger_import_commit(**receipt)  # type: ignore[arg-type]
    assert projected["state"] == "COMMITTED_PENDING_CHECKPOINT"
    assert replay == projected

    with pytest.raises(StaleRuntimeEvent, match="differs"):
        ledger.reconcile_blogger_import_commit(  # type: ignore[arg-type]
            **{**receipt, "principal_id": "owner:other"}
        )


def test_blogger_import_events_are_append_only(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    identity = _identity(checkpoint_id)
    ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]

    connection = sqlite3.connect(ledger.path)
    try:
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute("DELETE FROM mcp_blogger_import_events")
        with pytest.raises(sqlite3.IntegrityError, match="append-only"):
            connection.execute(
                "UPDATE mcp_blogger_import_events SET state='FAILED'"
            )
    finally:
        connection.close()


def test_cold_master_request_is_persisted_then_bound_and_resumed(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    cold_identity = {
        **_identity(checkpoint_id),
        "operation_id": "9" * 64,
        "batch_id": "99999999-9999-4999-8999-999999999999",
        "idempotency_key": "blogger-discovery-cold-master",
        "master_instance_id": None,
        "epoch": None,
        "pre_change_checkpoint_id": None,
    }
    persisted, created = ledger.ensure_blogger_import_operation(  # type: ignore[arg-type]
        **cold_identity
    )
    assert created is True
    assert persisted["state"] == "REQUESTED"
    assert persisted["master_instance_id"] is None
    waiting = ledger.mark_blogger_import_waiting_master(str(cold_identity["operation_id"]))
    assert waiting["state"] == "WAITING_MASTER"

    bound = ledger.bind_blogger_import_active_master(
        str(cold_identity["operation_id"]),
        master_instance_id="master-1",
        epoch=7,
        pre_change_checkpoint_id=checkpoint_id,
    )
    assert bound["state"] == "WAITING_MASTER"
    assert bound["epoch"] == 7
    assert ledger.bind_blogger_import_active_master(
        str(cold_identity["operation_id"]),
        master_instance_id="master-1",
        epoch=7,
        pre_change_checkpoint_id=checkpoint_id,
    ) == bound
    replay, replay_created = ledger.ensure_blogger_import_operation(  # type: ignore[arg-type]
        **cold_identity
    )
    assert replay_created is False
    assert replay == bound
    resumed = ledger.record_blogger_import_preview(
        str(cold_identity["operation_id"]),
        preview_receipt="resumed-preview",
        plan_sha256="8" * 64,
        summary={
            "create_actor_count": 1,
            "link_existing_count": 0,
            "quarantine_count": 0,
            "account_count": 1,
        },
    )
    assert resumed["state"] == "PREVIEWED"


def test_pre_change_checkpoint_requires_verified_matching_current_head(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    candidate_id = _checkpoint(
        ledger,
        parent_id=checkpoint_id,
        parent_generation=1,
        checkpoint_id="77777777-7777-4777-8777-777777777777",
        master_instance_id="master-1",
        epoch=7,
        promote=False,
    )
    cold = {
        **_identity(checkpoint_id),
        "operation_id": "7" * 64,
        "batch_id": "77777777-7777-4777-8777-777777777778",
        "idempotency_key": "candidate-checkpoint-must-fail",
        "master_instance_id": None,
        "epoch": None,
        "pre_change_checkpoint_id": None,
    }
    ledger.ensure_blogger_import_operation(**cold)  # type: ignore[arg-type]
    with pytest.raises(StaleRuntimeEvent, match="exact verified PostgreSQL HEAD"):
        ledger.bind_blogger_import_active_master(
            str(cold["operation_id"]),
            master_instance_id="master-1",
            epoch=7,
            pre_change_checkpoint_id=candidate_id,
        )


def test_pre_change_checkpoint_accepts_exact_successor_boot_checkpoint(tmp_path: Path) -> None:
    """A restored ACTIVE epoch must write against the checkpoint it booted from.

    Checkpoints are produced by the preceding draining epoch.  Requiring the
    candidate's producer identity to equal the restored successor identity
    makes every cold-started operator write impossible.
    """

    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    head = ledger.checkpoint_head("postgres-master")
    candidate = ledger.checkpoint_candidate(checkpoint_id)
    assert head is not None and candidate is not None

    successor_operation = "successor-operation"
    successor_identity = {
        "run_id": "run-8",
        "attempt_id": "attempt-8",
        "service_instance_id": "postgres-8",
        "master_instance_id": "master-2",
        "epoch": 8,
    }
    ledger.ensure_operation(
        operation_id=successor_operation,
        idempotency_key="successor-restores-current-head",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="ACTIVE",
        identity=successor_identity,
    )
    ledger.record_attempt(
        attempt_id="attempt-8",
        run_id="run-8",
        operation_id=successor_operation,
        source_identity="git:" + "a" * 40,
        source_version="a" * 40,
        service_instance_id="postgres-8",
        master_instance_id="master-2",
        epoch=8,
        state="REGISTERING",
    )
    _record_service_authority(
        ledger,
        master_instance_id="master-2",
        epoch=8,
        state="ACTIVE",
    )
    ledger.ensure_master_status_dataset_authority(
        operation_id=successor_operation,
        run_id="run-8",
        attempt_id="attempt-8",
        token="t" * 64,
        creator_claim_until=datetime.now(UTC) + timedelta(minutes=5),
        expected_content_tree_sha256="e" * 64,
        resource_lease={"lease_id": "successor-status-lease"},
    )
    ledger.record_master_status_dataset(
        operation_id=successor_operation,
        status_dataset={
            "provider_ref": "owner/status",
            "exact_version_ref": "owner/status/1",
            "boot_checkpoint": {
                "kind": "VERIFIED",
                "generation": head.generation,
                "checkpoint_id": checkpoint_id,
                "exact_version_ref": candidate["version_ref"],
                "manifest_sha256": candidate["manifest_sha256"],
            },
        },
    )

    cold = {
        **_identity(checkpoint_id),
        "operation_id": "8" * 64,
        "batch_id": "88888888-8888-4888-8888-888888888888",
        "idempotency_key": "successor-boot-checkpoint",
        "master_instance_id": None,
        "epoch": None,
        "pre_change_checkpoint_id": None,
    }
    ledger.ensure_blogger_import_operation(**cold)  # type: ignore[arg-type]

    bound = ledger.bind_blogger_import_active_master(
        str(cold["operation_id"]),
        master_instance_id="master-2",
        epoch=8,
        pre_change_checkpoint_id=checkpoint_id,
    )

    assert bound["master_instance_id"] == "master-2"
    assert bound["epoch"] == 8
    assert bound["pre_change_checkpoint_id"] == checkpoint_id


def test_checkpoint_identity_cannot_swap_between_verified_and_durable(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    identity = _identity(checkpoint_id)
    ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]
    ledger.record_blogger_import_preview(
        str(identity["operation_id"]),
        preview_receipt="preview",
        plan_sha256="d" * 64,
        summary={
            "create_actor_count": 1,
            "link_existing_count": 0,
            "quarantine_count": 0,
            "account_count": 1,
        },
    )
    ledger.begin_blogger_import_apply(
        str(identity["operation_id"]), preview_receipt="preview", plan_sha256="d" * 64
    )
    ledger.record_blogger_import_commit(
        str(identity["operation_id"]), affected_rows=2, committed_revision=13
    )
    ledger.advance_blogger_import_checkpoint(str(identity["operation_id"]), state="CHECKPOINTING")
    candidate_unverified = _checkpoint(
        ledger,
        parent_id=checkpoint_id,
        parent_generation=1,
        checkpoint_id="cccccccc-cccc-4ccc-8ccc-cccccccccccc",
        master_instance_id="master-1",
        epoch=7,
        promote=False,
    )
    with pytest.raises(StaleRuntimeEvent, match="exact request-bound VERIFIED HEAD"):
        ledger.advance_blogger_import_checkpoint(
            str(identity["operation_id"]),
            state="CHECKPOINT_VERIFIED",
            post_change_checkpoint_id=candidate_unverified,
        )
    checkpoint_operation_id = str(uuid4())
    verified_a = _checkpoint(
        ledger,
        parent_id=checkpoint_id,
        parent_generation=1,
        checkpoint_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        master_instance_id="master-1",
        epoch=7,
        operation_id=checkpoint_operation_id,
    )
    request_identity = {
        "kind": "mcp-blogger-import-checkpoint-v1",
        "operation_id": str(identity["operation_id"]),
        "batch_id": str(identity["batch_id"]),
        "canonical_revision": 13,
    }
    request_id = hashlib.sha256(
        json.dumps(
            request_identity, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode()
    ).hexdigest()
    with sqlite3.connect(ledger.path) as connection:
        connection.execute(
            "INSERT INTO connector_checkpoint_requests(operation_id,idempotency_key,"
            "canonical_revision,master_operation_id,master_instance_id,epoch,state,"
            "checkpoint_id,manifest_sha256,verified_at,created_at,updated_at) "
            "VALUES (?,?,?,?,?,?,'DURABLE_COMPLETE',?,?,?,?,?)",
            (
                f"connector-checkpoint:{request_id}",
                request_id,
                13,
                checkpoint_operation_id,
                "master-1",
                7,
                verified_a,
                "a" * 64,
                "2026-08-16T12:00:00Z",
                "2026-08-16T12:00:00Z",
                "2026-08-16T12:00:00Z",
            ),
        )
    ledger.advance_blogger_import_checkpoint(
        str(identity["operation_id"]),
        state="CHECKPOINT_VERIFIED",
        post_change_checkpoint_id=verified_a,
    )
    candidate_b = _checkpoint(
        ledger,
        parent_id=verified_a,
        parent_generation=2,
        checkpoint_id="bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
        master_instance_id="master-1",
        epoch=7,
        promote=False,
    )
    with pytest.raises(IdempotencyConflict, match="cannot be replaced"):
        ledger.advance_blogger_import_checkpoint(
            str(identity["operation_id"]),
            state="DURABLE_COMPLETE",
            post_change_checkpoint_id=candidate_b,
        )
    durable = ledger.advance_blogger_import_checkpoint(
        str(identity["operation_id"]),
        state="DURABLE_COMPLETE",
        post_change_checkpoint_id=verified_a,
    )
    assert durable["post_change_checkpoint_id"] == verified_a


def test_dead_epoch_preview_can_rebind_and_repreview_but_applying_cannot(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    _record_service_authority(
        ledger,
        master_instance_id="master-1",
        epoch=7,
        state="ACTIVE",
    )
    identity = _identity(checkpoint_id)
    ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]
    ledger.record_blogger_import_preview(
        str(identity["operation_id"]),
        preview_receipt="old-preview",
        plan_sha256="d" * 64,
        summary={
            "create_actor_count": 1,
            "link_existing_count": 0,
            "quarantine_count": 0,
            "account_count": 1,
        },
    )
    with pytest.raises(StaleRuntimeEvent, match="stopped or fenced"):
        ledger.restart_blogger_import_after_preview_epoch_loss(
            str(identity["operation_id"]),
            failed_master_instance_id="master-1",
            failed_epoch=7,
        )
    connection = sqlite3.connect(ledger.path)
    try:
        connection.execute(
            "UPDATE services SET state='DRAINING' WHERE service_kind='postgres-master' AND epoch=7"
        )
        connection.commit()
    finally:
        connection.close()
    with pytest.raises(StaleRuntimeEvent, match="stopped or fenced"):
        ledger.restart_blogger_import_after_preview_epoch_loss(
            str(identity["operation_id"]),
            failed_master_instance_id="master-1",
            failed_epoch=7,
        )
    assert ledger.allocate_epoch("postgres-master") == 8
    restarted = ledger.restart_blogger_import_after_preview_epoch_loss(
        str(identity["operation_id"]),
        failed_master_instance_id="master-1",
        failed_epoch=7,
    )
    assert restarted["state"] == "WAITING_MASTER"
    assert restarted["preview_generation"] == 1
    assert restarted["plan_sha256"] is None
    next_head = _checkpoint(
        ledger,
        parent_id=checkpoint_id,
        parent_generation=1,
        checkpoint_id="88888888-8888-4888-8888-888888888888",
        master_instance_id="master-2",
        epoch=8,
    )
    ledger.bind_blogger_import_active_master(
        str(identity["operation_id"]),
        master_instance_id="master-2",
        epoch=8,
        pre_change_checkpoint_id=next_head,
    )
    repreviewed = ledger.record_blogger_import_preview(
        str(identity["operation_id"]),
        preview_receipt="new-preview",
        plan_sha256="e" * 64,
        summary={
            "create_actor_count": 0,
            "link_existing_count": 1,
            "quarantine_count": 0,
            "account_count": 1,
        },
    )
    assert repreviewed["state"] == "PREVIEWED"
    assert repreviewed["master_instance_id"] == "master-2"
    ledger.begin_blogger_import_apply(
        str(identity["operation_id"]),
        preview_receipt="new-preview",
        plan_sha256="e" * 64,
    )
    with pytest.raises(StaleRuntimeEvent, match="dead epoch"):
        ledger.restart_blogger_import_after_preview_epoch_loss(
            str(identity["operation_id"]),
            failed_master_instance_id="master-2",
            failed_epoch=8,
        )


def test_concurrent_exact_preview_and_commit_retries_converge(tmp_path: Path) -> None:
    ledger, checkpoint_id = _ledger_with_checkpoint(tmp_path)
    identity = _identity(checkpoint_id)
    operation_id = str(identity["operation_id"])
    ledger.ensure_blogger_import_operation(**identity)  # type: ignore[arg-type]
    summary = {
        "create_actor_count": 1,
        "link_existing_count": 0,
        "quarantine_count": 0,
        "account_count": 1,
    }

    def preview():  # type: ignore[no-untyped-def]
        return ledger.record_blogger_import_preview(
            operation_id,
            preview_receipt="concurrent-preview",
            plan_sha256="d" * 64,
            summary=summary,
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        previews = list(pool.map(lambda _: preview(), range(4)))
    assert all(item == previews[0] for item in previews)
    ledger.begin_blogger_import_apply(
        operation_id,
        preview_receipt="concurrent-preview",
        plan_sha256="d" * 64,
    )

    def commit():  # type: ignore[no-untyped-def]
        return ledger.record_blogger_import_commit(
            operation_id, affected_rows=2, committed_revision=13
        )

    with ThreadPoolExecutor(max_workers=4) as pool:
        commits = list(pool.map(lambda _: commit(), range(4)))
    assert all(item == commits[0] for item in commits)
