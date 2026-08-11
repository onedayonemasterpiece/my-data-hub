from __future__ import annotations

import json
import sys
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from pydantic import ValidationError

from scripts.provider.operational_kaggle_matrix import (
    EXTERNAL_BLOCKED,
    MAXIMUM_SOAK_SECONDS,
    MINIMUM_DISTINCT_PROVIDER_RUNS,
    MINIMUM_SOAK_SECONDS,
    SCENARIOS,
    LifecycleEvent,
    _driver_request,
    _invoke_driver,
    _parse_driver_command,
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
        driver_command=("must-not-run",),
        commit_sha="a" * 40,
        adapter_factory=adapter_factory,  # type: ignore[arg-type]
        root=tmp_path,
    )
    assert result == EXTERNAL_BLOCKED
    assert json.loads(receipt.read_text())["mutations_started"] == 0
    assert not called
    assert not (tmp_path / "plan.json").exists()
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
        driver_command=None,
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        adapter_factory=adapter_factory,  # type: ignore[arg-type]
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
        driver_command=("fake-driver",),
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        adapter_factory=lambda **_: pytest.fail("fake adapter must not be called"),  # type: ignore[arg-type]
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


def test_driver_command_is_json_argv_not_shell_text() -> None:
    assert _parse_driver_command('["python", "/opt/mdh/driver.py"]') == (
        "python",
        "/opt/mdh/driver.py",
    )
    with pytest.raises(ValueError, match="JSON argv"):
        _parse_driver_command("python /opt/mdh/driver.py")


def test_typed_driver_fail_exit_is_preserved_as_scenario_failure(tmp_path: Path) -> None:
    script = tmp_path / "typed_fail_driver.py"
    script.write_text(
        "import json, sys\n"
        "result = sys.argv[sys.argv.index('--result') + 1]\n"
        "request = json.load(open(sys.argv[sys.argv.index('--request') + 1]))\n"
        "json.dump({\n"
        " 'schema_version': 'my-data-hub-operational-kaggle-driver-result.v1',\n"
        " 'outcome': 'FAIL', 'scenario': request['scenario'],\n"
        " 'task_run_id': request['task_run_id'], 'provider_ref': None,\n"
        " 'provider_run_ref': None, 'provider_kernel_id': None,\n"
        " 'source_version': None, 'source_sha256': None,\n"
        " 'blocker_code': None, 'integration_dependency': None,\n"
        " 'mutations_started': 1, 'capability_checks': [],\n"
        " 'observation_sha256': None}, open(result, 'w'))\n"
        "raise SystemExit(1)\n"
    )
    plan = _plan()
    rows = plan["scenarios"]
    assert isinstance(rows, list)
    request = _driver_request(plan, rows[5], resume_only=False)

    result = _invoke_driver((sys.executable, str(script)), request, timeout_seconds=10)

    assert result.outcome == "FAIL"
    assert result.mutations_started == 1


@pytest.mark.parametrize(
    ("schema_name", "example_name"),
    [
        ("operational-kaggle-matrix-plan.v1.schema.json", "operational-kaggle-matrix-plan.v1.example.json"),
        ("operational-kaggle-scenario-receipt.v1.schema.json", "operational-kaggle-scenario-receipt.v1.example.json"),
        ("operational-kaggle-matrix-receipt.v1.schema.json", "operational-kaggle-matrix-receipt.v1.example.json"),
        ("operational-kaggle-output.v1.schema.json", "operational-kaggle-output.v1.example.json"),
        ("operational-kaggle-driver-result.v1.schema.json", "operational-kaggle-driver-result.v1.example.json"),
        ("operational-kaggle-driver-request.v1.schema.json", "operational-kaggle-driver-request.v1.example.json"),
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
    assert "MY_DATA_HUB_OPERATIONAL_DRIVER_JSON" in workflow
    assert "timeout-minutes: 360" in workflow
