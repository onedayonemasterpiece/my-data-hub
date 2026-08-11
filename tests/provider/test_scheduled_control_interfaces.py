from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from my_data_hub.control_plane.adapters import LedgerControlReader
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.control_plane.runtime import ControlPlaneMasterRuntime
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator


def _identity() -> AccessIdentity:
    return AccessIdentity(
        subject="scheduled-operator",
        client_id="scheduled-operator",
        scopes=frozenset({"checkpoint:read", "recovery:request", "acceptance:probe"}),
        audience="mcp",
        token_id="token-id",
        expires_at=2_000_000_000,
        issuer="https://issuer.example",
        issued_at=1_999_999_000,
        resource="https://mcp.example/mcp",
    )


def _checkpoint(ledger: ControlLedger, checkpoint_id: str, parent: str | None, generation: int) -> None:
    operation_id = f"operation-{checkpoint_id}"
    ledger.ensure_operation(
        operation_id=operation_id,
        idempotency_key=operation_id,
        operation_kind="checkpoint",
        intent={"checkpoint_id": checkpoint_id},
        initial_state="CHECKPOINTING",
        identity={"checkpoint_id": checkpoint_id},
    )
    ledger.add_checkpoint_candidate(
        checkpoint_id=checkpoint_id,
        operation_id=operation_id,
        dataset_ref="owner/private-checkpoints",
        version_ref=None,
        manifest_sha256=("a" if checkpoint_id == "cp-1" else "b") * 64,
        source_checkpoint_id=parent,
        source_head_generation=generation,
        master_instance_id="master-1",
        epoch=1,
    )
    ledger.mark_checkpoint_uploaded(checkpoint_id, f"owner/private-checkpoints/{generation + 10}")
    ledger.mark_checkpoint_readback_verified(checkpoint_id)
    ledger.mark_checkpoint_restore_verified(checkpoint_id)
    ledger.promote_checkpoint(
        "postgres-master",
        checkpoint_id,
        expected_generation=generation,
        expected_parent_checkpoint_id=parent,
    )


def test_checkpoint_status_exposes_exact_current_previous_metadata(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    _checkpoint(ledger, "cp-1", None, 0)
    _checkpoint(ledger, "cp-2", "cp-1", 1)

    status = LedgerControlReader(ledger).invoke_control("checkpoint.status", {}, _identity())

    assert status["generation"] == 2
    assert status["current_exact_version_ref"] == "owner/private-checkpoints/11"
    assert status["previous_exact_version_ref"] == "owner/private-checkpoints/10"
    assert status["verified_at"] == status["current"]["verified_at"]
    assert status["current"]["manifest_sha256"] == "b" * 64


def test_restore_request_is_exact_durable_and_idempotent_for_consumer(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    _checkpoint(ledger, "cp-1", None, 0)
    reader = LedgerControlReader(ledger, acceptance_consumer_available=True)
    arguments = {
        "idempotency_key": "workflow-123:current",
        "target": "current",
        "checkpoint_id": "cp-1",
        "exact_version_ref": "owner/private-checkpoints/10",
        "timeout_seconds": 1200,
    }

    first = reader.invoke_control("checkpoint.restore.request", arguments, _identity())
    second = reader.invoke_control("checkpoint.restore.request", arguments, _identity())

    assert first["accepted"] is True and first["duplicate"] is False
    assert second["accepted"] is True and second["duplicate"] is True
    assert first["operation_id"] == second["operation_id"]
    assert first["state"] == "REQUESTED"
    assert first["execution_supported"] is True
    operation = ledger.get_operation(str(first["operation_id"]))
    assert operation is not None and operation.operation_kind == "checkpoint_restore_smoke"

    class RestoreExecutor:
        def restore(self, operation_id: str, candidate: dict[str, object]) -> dict[str, object]:
            assert operation_id == first["operation_id"]
            assert candidate["checkpoint_id"] == "cp-1"
            return {"ok": True, "checkpoint_id": "cp-1"}

    runtime = ControlPlaneMasterRuntime(
        ledger,
        MasterCoordinator(ledger, FakeKaggleRuntime()),
        object(),  # type: ignore[arg-type]
        RestoreExecutor(),  # type: ignore[arg-type]
    )
    terminal = runtime.reconcile_acceptance_once()
    assert terminal == {"operation_id": first["operation_id"], "state": "DURABLE_COMPLETE"}
    assert ledger.incomplete_operations("checkpoint_restore_smoke") == []


def test_protected_resource_probe_exercises_policy_without_provider_mutation(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    ledger.register_provider_resource(
        provider="kaggle",
        resource_ref="owner/protected",
        resource_kind="dataset",
        source_identity="git:sha",
        source_version="1",
        control_class="orchestrator_protected",
        private=True,
        state="ready",
    )

    result = LedgerControlReader(ledger).invoke_control(
        "provider.protected_resource.probe",
        {"resource_ref": "owner/protected"},
        _identity(),
    )

    assert result["evaluated"] is True
    assert result["protected"] is True
    assert result["denied"] is True
    assert result["mutation_attempted"] is False
    assert result["reason_code"] == "PROTECTED_RESOURCE_DENIED"


def test_connector_coverage_fails_closed_without_business_rows(tmp_path: Path) -> None:
    result = LedgerControlReader(ControlLedger(tmp_path / "control.sqlite3")).invoke_control(
        "connector.coverage", {}, _identity()
    )

    assert result["available"] is False
    assert result["bounded"] is True
    assert result["blocker_code"] == "CONNECTOR_METADATA_HEARTBEAT_ABSENT"
    assert "rows" not in result


def test_connector_coverage_reads_only_bounded_heartbeat_metadata(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    for connector in ("ydb-bloggers", "joplin-notes"):
        ledger.record_connector_coverage(
            connector_kind=connector,
            contract_version="v1",
            state="COMPLETE",
            observed_at=datetime(2026, 8, 12, 12, tzinfo=UTC),
        )

    result = LedgerControlReader(ledger).invoke_control(
        "connector.coverage", {}, _identity()
    )

    assert result["available"] is True
    assert result["connector_count"] == 2
    assert result["complete_count"] == 2
    assert "rows" not in result


def test_active_runtime_produces_authenticated_connector_heartbeat(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "control.sqlite3")
    identity = {
        "run_id": "run-1",
        "attempt_id": "attempt-1",
        "service_instance_id": "service-1",
        "master_instance_id": "master-1",
    }
    operation, _ = ledger.ensure_operation(
        operation_id="master-operation-1",
        idempotency_key="master-operation-1",
        operation_kind="ensure_master",
        intent={"source": "test"},
        initial_state="REGISTERING",
        identity=identity,
        allocate_epoch_for="postgres-master",
    )
    ledger.record_attempt(
        attempt_id="attempt-1",
        run_id="run-1",
        operation_id=operation.operation_id,
        source_identity="owner/master",
        source_version="git:exact",
        service_instance_id="service-1",
        master_instance_id="master-1",
        epoch=1,
        state="REGISTERING",
    )
    ledger.store_runtime_token_hash("run-1", "attempt-1", "runtime-token-long-enough")
    ledger.activate_service_operation(
        operation_id=operation.operation_id,
        expected_state="REGISTERING",
        service_instance_id="service-1",
        service_kind="postgres-master",
        run_id="run-1",
        attempt_id="attempt-1",
        master_instance_id="master-1",
        epoch=1,
        endpoint="postgres-master.internal:5432",
        protocol="postgresql+tls",
        tls_fingerprint="a" * 64,
        capabilities=("connector-intake",),
        canonical_revision=1,
        schema_version="13",
        lease_until=datetime.now(UTC) + timedelta(minutes=5),
        latest_event_id="ready-1",
    )
    runtime = ControlPlaneMasterRuntime(
        ledger, MasterCoordinator(ledger, FakeKaggleRuntime()), object()  # type: ignore[arg-type]
    )
    observed_at = datetime.now(UTC)

    runtime.record_connector_heartbeat(
        run_id="run-1",
        attempt_id="attempt-1",
        runtime_token="runtime-token-long-enough",
        connector_kind="ydb-bloggers",
        contract_version="my-data-hub-data-connector.v1",
        state="COMPLETE",
        observed_at=observed_at,
    )

    assert ledger.connector_coverage_metadata()[0]["connector_kind"] == "ydb-bloggers"
