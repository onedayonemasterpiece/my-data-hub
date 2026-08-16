from __future__ import annotations

import json
import sqlite3
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
