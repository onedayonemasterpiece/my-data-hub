from __future__ import annotations

import hashlib
import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

import scripts.provider.operational_kaggle_matrix as matrix_module
from my_data_hub.acceptance.scenario_operator import CheckpointAcceptanceOperationalResult
from my_data_hub.checkpoints.acceptance import CheckpointAcceptanceReceipt
from my_data_hub.hashing import canonical_json_bytes
from scripts.provider.operational_kaggle_matrix import (
    EXTERNAL_BLOCKED,
    MAXIMUM_SOAK_SECONDS,
    MINIMUM_DISTINCT_PROVIDER_RUNS,
    MINIMUM_SOAK_SECONDS,
    SCENARIOS,
    DriverResult,
    LifecycleEvent,
    _summary,
    build_plan,
    run_operational_matrix,
)


def _plan() -> dict[str, object]:
    return build_plan(
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        created_at=datetime(2026, 8, 11, tzinfo=UTC),
    )


def _fm14_checkpoint_result(plan: dict[str, object]) -> CheckpointAcceptanceOperationalResult:
    root = Path(__file__).resolve().parents[2]
    rows = plan["scenarios"]
    assert isinstance(rows, list)
    row = rows[13]
    result = json.loads(
        (root / "examples/provider/checkpoint-acceptance-operational-result.v1.example.json").read_text()
    )
    result["task_run_id"] = row["planned_task_run_id"]
    result["source_revision"] = plan["commit_sha"]
    result["receipt"]["task_run_id"] = row["planned_task_run_id"]
    receipt = CheckpointAcceptanceReceipt.model_validate(result["receipt"])
    result["receipt_sha256"] = receipt.receipt_sha256
    return CheckpointAcceptanceOperationalResult.model_validate(result)


def test_operational_plan_is_exact_24_scenario_source_matrix() -> None:
    plan = _plan()
    rows = plan["scenarios"]
    assert isinstance(rows, list)
    assert len(rows) == len(SCENARIOS) == 24
    assert [row["requirement_id"] for row in rows] == [f"FM{i:02d}" for i in range(1, 25)]
    assert len({row["planned_task_run_id"] for row in rows}) == 24
    assert plan["minimum_distinct_provider_runs"] == MINIMUM_DISTINCT_PROVIDER_RUNS == 15
    names = {row["name"] for row in rows}
    assert {
        "empty-postgresql-master-bootstrap",
        "concurrent-ensure-master-single-run",
        "full-ydb-blogger-import-checkpoint",
        "e5-corpus-worker-transactional-import",
        "bge-m3-corpus-worker-transactional-import",
        "remote-mcp-cold-start-blogger-search",
        "accelerated-session-rotation-soak",
    } <= names


def test_lifecycle_contract_encodes_all_mandatory_real_gates() -> None:
    plan = _plan()
    rows = plan["scenarios"]
    assert isinstance(rows, list)
    gates = [gate for row in rows for gate in row["lifecycle_gates"]]
    assert gates.count("master_boot") >= 3
    assert gates.count("clean_rotation") >= 2
    assert gates.count("abrupt_master_termination") == 1
    assert gates.count("control_plane_restart") == 1
    assert gates.count("host_reboot") == 1
    assert gates.count("soak") == 1
    assert MINIMUM_SOAK_SECONDS == 3600
    assert MAXIMUM_SOAK_SECONDS == 5400
    rows_by_id = {row["requirement_id"]: row for row in rows}
    assert rows_by_id["FM11"]["lifecycle_gates"] == ["clean_rotation"]
    assert rows_by_id["FM24"]["lifecycle_gates"] == ["soak"]


def test_soak_and_rotation_events_fail_closed() -> None:
    base = {
        "event_id": "11111111-1111-4111-8111-111111111111",
        "started_at": "2026-08-11T00:00:00Z",
        "completed_at": "2026-08-11T00:30:00Z",
    }
    with pytest.raises(ValidationError, match="60-90 minutes"):
        LifecycleEvent.model_validate(
            {
                **base,
                "gate": "soak",
                "duration_seconds": 1800,
                "heartbeat_count": 1,
                "read_query_count": 1,
                "checkpoint_count": 1,
                "recovery_count": 1,
            }
        )
    with pytest.raises(ValidationError, match="consecutive epochs"):
        LifecycleEvent.model_validate(
            {
                **base,
                "gate": "clean_rotation",
                "old_provider_run_ref": "owner/old/1",
                "new_provider_run_ref": "owner/new/1",
                "old_epoch": 1,
                "new_epoch": 3,
            }
        )
    with pytest.raises(ValidationError, match="before/after identities"):
        LifecycleEvent.model_validate(
            {
                **base,
                "gate": "control_plane_restart",
                "operation_id": "unfinished-operation",
                "before_identity": "same-boot",
                "after_identity": "same-boot",
            }
        )


def test_credential_free_run_exits_78_before_plan_ledger_adapter_or_driver(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "no-token"))
    called = False

    def adapter_factory(**_: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("adapter must not be constructed")

    receipt = tmp_path / "summary.json"
    result = run_operational_matrix(
        ledger_path=tmp_path / "ledger.sqlite3",
        plan_path=tmp_path / "plan.json",
        receipt_path=receipt,
        scenario_directory=tmp_path / "scenarios",
        commit_sha="a" * 40,
        root=tmp_path,
    )
    assert result == EXTERNAL_BLOCKED
    assert json.loads(receipt.read_text())["passed_scenarios"] == 0
    assert not called
    assert (tmp_path / "plan.json").exists()
    assert not (tmp_path / "ledger.sqlite3").exists()


def test_missing_operational_interface_writes_24_typed_blockers_without_adapter(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "present-but-not-live-unit-test-token")
    called = False

    def adapter_factory(**_: object) -> object:
        nonlocal called
        called = True
        raise AssertionError("adapter must not be constructed")

    receipt = tmp_path / "summary.json"
    result = run_operational_matrix(
        ledger_path=tmp_path / "ledger.sqlite3",
        plan_path=tmp_path / "plan.json",
        receipt_path=receipt,
        scenario_directory=tmp_path / "scenarios",
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        root=tmp_path,
    )
    assert result == EXTERNAL_BLOCKED
    assert not called
    assert not (tmp_path / "ledger.sqlite3").exists()
    paths = sorted((tmp_path / "scenarios").glob("*.json"))
    assert len(paths) == 24
    assert all(json.loads(path.read_text())["outcome"] == "BLOCKED" for path in paths)
    assert all(json.loads(path.read_text())["live_evidence"] is False for path in paths)
    summary = json.loads(receipt.read_text())
    assert summary["outcome"] == "BLOCKED"
    assert summary["passed_scenarios"] == 0
    assert summary["blocked_scenarios"] == 24
    assert summary["distinct_provider_run_refs"] == []
    assert summary["distinct_provider_kernel_ids"] == []


def test_injected_fake_path_can_never_produce_live_pass(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "fake-token")
    result = run_operational_matrix(
        ledger_path=tmp_path / "ledger.sqlite3",
        plan_path=tmp_path / "plan.json",
        receipt_path=tmp_path / "summary.json",
        scenario_directory=tmp_path / "scenarios",
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        root=tmp_path,
    )
    assert result == EXTERNAL_BLOCKED
    summary = json.loads((tmp_path / "summary.json").read_text())
    assert summary["outcome"] == "BLOCKED"
    assert summary["live_evidence"] is False
    assert all(
        json.loads(path.read_text())["blocker"]["code"] == "TEST_INJECTION_CANNOT_CREATE_LIVE_EVIDENCE"
        for path in (tmp_path / "scenarios").glob("*.json")
    )


def test_summary_never_counts_task_uuid_as_real_provider_run() -> None:
    plan = _plan()
    rows = plan["scenarios"]
    assert isinstance(rows, list)
    receipts = []
    for row in rows:
        receipts.append(
            {
                "ordinal": row["ordinal"],
                "requirement_id": row["requirement_id"],
                "scenario": row["name"],
                "outcome": "PASS",
                "real_run_identity": {"provider_run_ref": "owner/same-run/1", "provider_kernel_id": 7},
                "lifecycle_events": [],
            }
        )
    summary = _summary(plan, receipts)
    assert len({row["planned_task_run_id"] for row in rows}) == 24
    assert summary["distinct_provider_run_refs"] == ["owner/same-run/1"]
    assert summary["distinct_provider_kernel_ids"] == [7]
    assert summary["outcome"] == "BLOCKED"


def test_driver_command_is_hard_pinned_to_checked_in_sibling(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_DRIVER_JSON", '["/tmp/untrusted"]')
    command = matrix_module._trusted_driver_command()
    assert command == (sys.executable, str(matrix_module.TRUSTED_DRIVER_PATH.resolve()))
    assert "/tmp/untrusted" not in command


def _trusted_output_locator(plan: dict[str, object], row: dict[str, object]) -> DriverResult:
    output = {
        "schema_version": "my-data-hub-operational-kaggle-output.v1",
        "matrix_id": plan["matrix_id"],
        "scenario": row["name"],
        "task_run_id": row["planned_task_run_id"],
        "outcome": "PASS",
        "assertions": [
            {"name": name, "outcome": "PASS", "evidence_sha256": hashlib.sha256(str(name).encode()).hexdigest()}
            for name in row["required_assertions"]
        ],
        "lifecycle_events": [],
        "operation_ids": [],
        "completed_at": "2026-08-11T00:00:00Z",
    }
    return DriverResult.model_validate(
        {
            "schema_version": "my-data-hub-operational-kaggle-driver-result.v2",
            "phase": "EXECUTE",
            "outcome": "READY",
            "scenario": row["name"],
            "task_run_id": row["planned_task_run_id"],
            "provider_ref": "owner/evidence",
            "provider_run_ref": "owner/evidence/7",
            "provider_kernel_id": 77,
            "source_version": 7,
            "source_sha256": "a" * 64,
            "mutations_started": 1,
            "capability_checks": [],
            "observation_sha256": "b" * 64,
            "claim_task_id": "11111111-1111-4111-8111-111111111111",
            "claim_sha256": "c" * 64,
            "output_receipt_sha256": "d" * 64,
            "output_file_sha256": hashlib.sha256(canonical_json_bytes(output)).hexdigest(),
            "output_tree_sha256": "f" * 64,
            "cleanup_state": "PENDING",
            "scenario_output": output,
        }
    )


def test_control_owned_scenario_output_binds_exact_receipt_without_local_adapter() -> None:
    plan = _plan()
    row = plan["scenarios"][1]
    assert isinstance(row, dict)
    locator = _trusted_output_locator(plan, row)
    receipt, binding = matrix_module._trusted_control_receipt(
        plan=plan, row=row, locator=locator, started_at=datetime(2026, 8, 11, tzinfo=UTC)
    )
    assert receipt["outcome"] == "PASS"
    assert receipt["real_run_identity"]["provider_status"] == "control_reconciled"
    assert binding is not None and binding.provider_run_ref == "owner/evidence/7"


def test_control_owned_scenario_output_rejects_hash_drift() -> None:
    plan = _plan()
    row = plan["scenarios"][1]
    assert isinstance(row, dict)
    locator = _trusted_output_locator(plan, row).model_copy(update={"output_file_sha256": "0" * 64})
    with pytest.raises(RuntimeError, match="control-owned output receipt"):
        matrix_module._trusted_control_receipt(
            plan=plan, row=row, locator=locator, started_at=datetime(2026, 8, 11, tzinfo=UTC)
        )


def test_reconciliation_fence_resumes_cleanup_after_notebook_was_deleted(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    plan = _plan()
    row = plan["scenarios"][0]
    assert isinstance(row, dict)
    output = {
        "schema_version": "my-data-hub-operational-kaggle-output.v1",
        "matrix_id": plan["matrix_id"],
        "scenario": row["name"],
        "task_run_id": row["planned_task_run_id"],
        "outcome": "PASS",
        "assertions": [
            {"name": name, "outcome": "PASS", "evidence_sha256": hashlib.sha256(name.encode()).hexdigest()}
            for name in row["required_assertions"]
        ],
        "lifecycle_events": [],
        "operation_ids": [],
        "completed_at": "2026-08-11T00:00:00Z",
    }
    raw = canonical_json_bytes(output)
    output_sha = hashlib.sha256(raw).hexdigest()
    tree_sha = "f" * 64
    ready = DriverResult.model_validate(
        {
            "schema_version": "my-data-hub-operational-kaggle-driver-result.v2",
            "phase": "EXECUTE",
            "outcome": "READY",
            "scenario": row["name"],
            "task_run_id": row["planned_task_run_id"],
            "provider_ref": "owner/evidence",
            "provider_run_ref": "owner/evidence/7",
            "provider_kernel_id": 77,
            "source_version": 7,
            "source_sha256": "a" * 64,
            "blocker_code": None,
            "integration_dependency": None,
            "mutations_started": 2,
            "capability_checks": [],
            "observation_sha256": "b" * 64,
            "claim_task_id": "11111111-1111-4111-8111-111111111111",
            "claim_sha256": "c" * 64,
            "output_receipt_sha256": "d" * 64,
            "output_file_sha256": output_sha,
            "output_tree_sha256": tree_sha,
            "cleanup_state": "PENDING",
            "scenario_output": output,
        }
    )

    cleanup_attempts = 0

    def invoke(request: dict[str, object], *, timeout_seconds: int) -> DriverResult:
        nonlocal cleanup_attempts
        assert timeout_seconds == 7200
        if request["phase"] == "EXECUTE":
            if request["requirement_id"] == "FM01":
                return ready
            return DriverResult.model_validate(
                {
                    "schema_version": "my-data-hub-operational-kaggle-driver-result.v2",
                    "phase": "EXECUTE",
                    "outcome": "BLOCKED",
                    "scenario": request["scenario"],
                    "task_run_id": request["task_run_id"],
                    "provider_ref": None,
                    "provider_run_ref": None,
                    "provider_kernel_id": None,
                    "source_version": None,
                    "source_sha256": None,
                    "blocker_code": "TEST_REMAINDER_BLOCKED",
                    "integration_dependency": "unit-test remainder",
                    "mutations_started": 0,
                    "capability_checks": [],
                    "observation_sha256": None,
                    "claim_task_id": None,
                    "claim_sha256": None,
                    "output_receipt_sha256": None,
                    "output_file_sha256": None,
                    "output_tree_sha256": None,
                    "cleanup_state": "NOT_REQUIRED",
                }
            )
        if request["phase"] == "RECONCILE":
            cleanup = dict(request["cleanup"])  # type: ignore[arg-type]
            return DriverResult.model_validate(
                {
                    "schema_version": "my-data-hub-operational-kaggle-driver-result.v2",
                    "phase": "RECONCILE",
                    "outcome": "PASS",
                    "scenario": request["scenario"],
                    "task_run_id": request["task_run_id"],
                    "mutations_started": 1,
                    "capability_checks": [],
                    "observation_sha256": "e" * 64,
                    **cleanup,
                    "cleanup_state": "PENDING",
                }
            )
        cleanup_attempts += 1
        if cleanup_attempts == 1:
            raise RuntimeError("cleanup response lost after durable delete")
        cleanup = dict(request["cleanup"])  # type: ignore[arg-type]
        return DriverResult.model_validate(
            {
                "schema_version": "my-data-hub-operational-kaggle-driver-result.v2",
                "phase": "CLEANUP",
                "outcome": "PASS",
                "scenario": request["scenario"],
                "task_run_id": request["task_run_id"],
                "provider_ref": cleanup["provider_ref"],
                "provider_run_ref": cleanup["provider_run_ref"],
                "provider_kernel_id": cleanup["provider_kernel_id"],
                "source_version": cleanup["source_version"],
                "source_sha256": cleanup["source_sha256"],
                "blocker_code": None,
                "integration_dependency": None,
                "mutations_started": 3,
                "capability_checks": [],
                "observation_sha256": "e" * 64,
                **cleanup,
                "cleanup_state": "COMPLETE",
            }
        )

    monkeypatch.setattr(matrix_module, "_exact_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(matrix_module, "_invoke_driver", invoke)
    kwargs = {
        "ledger_path": tmp_path / "ledger.sqlite3",
        "plan_path": tmp_path / "plan.json",
        "receipt_path": tmp_path / "summary.json",
        "scenario_directory": tmp_path / "scenarios",
        "matrix_id": UUID(str(plan["matrix_id"])),
    }
    with pytest.raises(RuntimeError, match="cleanup response lost"):
        run_operational_matrix(**kwargs)  # type: ignore[arg-type]
    assert run_operational_matrix(**kwargs) == EXTERNAL_BLOCKED  # type: ignore[arg-type]
    assert cleanup_attempts == 2
    fm01 = json.loads((tmp_path / "scenarios/01-private-dataset-create-readback-delete.json").read_text())
    assert fm01["outcome"] == "PASS"


def test_fm16_owner_pause_resumes_same_launch_and_stops_dependents_until_authorized(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    fm16_requests: list[dict[str, object]] = []

    def blocked(request: dict[str, object], code: str) -> DriverResult:
        return DriverResult.model_validate(
            {
                "schema_version": "my-data-hub-operational-kaggle-driver-result.v2",
                "phase": "EXECUTE",
                "outcome": "BLOCKED",
                "scenario": request["scenario"],
                "task_run_id": request["task_run_id"],
                "blocker_code": code,
                "integration_dependency": "unit-test exact dependency",
                "mutations_started": 0,
                "capability_checks": [],
                "cleanup_state": "NOT_REQUIRED",
            }
        )

    def invoke(request: dict[str, object], *, timeout_seconds: int) -> DriverResult:
        assert timeout_seconds == 7200
        if request["requirement_id"] == "FM16":
            fm16_requests.append(dict(request))
            code = (
                "FM16_AWAITING_OWNER_AUTHORIZATION"
                if len(fm16_requests) == 1
                else "FM16_OWNER_ENVELOPE_STILL_REQUIRED"
            )
            return blocked(request, code)
        return blocked(request, "UNIT_TEST_REMAINDER_BLOCKED")

    monkeypatch.setattr(matrix_module, "_exact_commit", lambda _root: "a" * 40)
    monkeypatch.setattr(matrix_module, "_invoke_driver", invoke)
    kwargs = {
        "ledger_path": tmp_path / "ledger.sqlite3",
        "plan_path": tmp_path / "plan.json",
        "receipt_path": tmp_path / "summary.json",
        "scenario_directory": tmp_path / "scenarios",
        "matrix_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    }

    assert run_operational_matrix(**kwargs) == EXTERNAL_BLOCKED  # type: ignore[arg-type]
    assert len(fm16_requests) == 1
    assert fm16_requests[0]["resume_only"] is False
    assert not (tmp_path / "scenarios/16-full-ydb-blogger-import-checkpoint.json").exists()
    pause_path = tmp_path / "scenarios/16-full-ydb-blogger-import-checkpoint.owner-pause.json"
    assert json.loads(pause_path.read_text())["blocker"]["code"] == "FM16_AWAITING_OWNER_AUTHORIZATION"
    assert not (tmp_path / "scenarios/17-post-import-cold-restore-equality.json").exists()

    assert run_operational_matrix(**kwargs) == EXTERNAL_BLOCKED  # type: ignore[arg-type]
    assert len(fm16_requests) == 2
    assert fm16_requests[1]["resume_only"] is True
    fm16 = json.loads((tmp_path / "scenarios/16-full-ydb-blogger-import-checkpoint.json").read_text())
    assert fm16["blocker"]["code"] == "FM16_OWNER_ENVELOPE_STILL_REQUIRED"
    assert json.loads(pause_path.read_text())["blocker"]["code"] == "FM16_AWAITING_OWNER_AUTHORIZATION"


@pytest.mark.parametrize(
    ("schema_name", "example_name"),
    [
        ("operational-kaggle-matrix-plan.v1.schema.json", "operational-kaggle-matrix-plan.v1.example.json"),
        ("operational-kaggle-scenario-receipt.v1.schema.json", "operational-kaggle-scenario-receipt.v1.example.json"),
        ("operational-kaggle-matrix-receipt.v1.schema.json", "operational-kaggle-matrix-receipt.v1.example.json"),
        ("operational-kaggle-output.v1.schema.json", "operational-kaggle-output.v1.example.json"),
        ("operational-kaggle-driver-result.v1.schema.json", "operational-kaggle-driver-result.v1.example.json"),
        ("operational-kaggle-driver-request.v1.schema.json", "operational-kaggle-driver-request.v1.example.json"),
        ("operational-kaggle-driver-request.v2.schema.json", "operational-kaggle-driver-request.v2.example.json"),
        ("operational-kaggle-driver-result.v2.schema.json", "operational-kaggle-driver-result.v2.example.json"),
        ("operational-kaggle-evidence-driver.v1.schema.json", "operational-kaggle-evidence-driver.v1.example.json"),
        (
            "operational-kaggle-evidence-claims.v1.schema.json",
            "operational-kaggle-evidence-claims.v1.example.json",
        ),
    ],
)
def test_operational_contract_examples_validate(schema_name: str, example_name: str) -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas" / "provider" / schema_name).read_text())
    example = json.loads((root / "examples" / "provider" / example_name).read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)


def test_provider_workflow_runs_operational_matrix_not_smoke_surrogate() -> None:
    root = Path(__file__).resolve().parents[2]
    workflow = (root / ".github" / "workflows" / "provider-real.yml").read_text()
    assert "operational_kaggle_matrix.py preflight" in workflow
    assert "operational_kaggle_matrix.py run" in workflow
    assert "real_kaggle_matrix.py matrix" not in workflow
    assert "MY_DATA_HUB_OPERATIONAL_DRIVER_JSON" not in workflow
    assert "KAGGLE_API_TOKEN" not in workflow
    assert "timeout-minutes: 360" in workflow
