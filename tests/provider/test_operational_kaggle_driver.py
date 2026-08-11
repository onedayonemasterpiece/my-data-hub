from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from scripts.provider.operational_kaggle_driver import (
    EXECUTORS,
    DriverRequest,
    MissingCredential,
    execute,
)
from scripts.provider.operational_kaggle_matrix import DriverResult, _driver_request, build_plan


class FakeGateway:
    def __init__(self, *, missing_profile: str | None = None, missing_tool: str | None = None) -> None:
        self.missing_profile = missing_profile
        self.missing_tool = missing_tool
        self.calls: list[tuple[str, str, dict[str, Any]]] = []

    async def catalog(self, profile: str) -> frozenset[str]:
        if profile == self.missing_profile:
            raise MissingCredential(profile)
        tools = {tool for executor in EXECUTORS for tool_profile, tool in executor.tools if tool_profile == profile}
        if self.missing_tool:
            tools.discard(self.missing_tool)
        return frozenset(tools)

    async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((profile, tool, dict(arguments)))
        if tool == "master.status":
            return {
                "state": "ACTIVE",
                "master_epoch": 2,
                "provider_run_ref": "owner/master/2",
            }
        if tool == "checkpoint.status":
            return {
                "current_checkpoint_id": "11111111-1111-4111-8111-111111111111",
                "current_exact_version_ref": "owner/checkpoint/2",
            }
        if tool == "provider.resources.status":
            return {
                "bounded": True,
                "resources": [
                    {
                        "resource_ref": "owner/protected-master",
                        "control_class": "orchestrator_protected",
                    }
                ],
            }
        if tool == "runtime.stale_epoch.probe":
            return {"evaluated": True, "denied": True, "mutation_attempted": False}
        if tool == "provider.protected_resource.probe":
            return {"evaluated": True, "denied": True, "mutation_attempted": False}
        if tool == "bloggers.migration.accounting":
            return {"bounded": True, "accounted": True}
        if tool == "bloggers.statistics":
            return {"bounded": True, "count": 10}
        if tool == "embedding.coverage":
            return {"e5": {"coverage": 1.0}, "bge_m3": {"coverage": 1.0}}
        raise AssertionError(f"unexpected mutating/unsupported tool call: {tool}")


def _request(ordinal: int) -> DriverRequest:
    plan = build_plan(
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        created_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    return DriverRequest.model_validate(_driver_request(plan, plan["scenarios"][ordinal - 1], resume_only=False))


def test_driver_has_one_named_executor_and_internal_gap_per_scenario() -> None:
    assert len(EXECUTORS) == 24
    assert [item.requirement_id for item in EXECUTORS] == [f"FM{i:02d}" for i in range(1, 25)]
    assert len({item.gap_code for item in EXECUTORS}) == 24
    assert all(item.gap_code != "OPERATIONAL_DRIVER_INTERFACE_MISSING" for item in EXECUTORS)
    assert all("missing" in item.gap_code.casefold() for item in EXECUTORS)
    assert all(item.gap_dependency and len(item.gap_dependency) <= 500 for item in EXECUTORS)


def test_missing_modern_token_blocks_before_mcp_or_mutation(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "no-token"))
    gateway = FakeGateway()
    result = asyncio.run(execute(_request(1), gateway))
    assert result.outcome == "BLOCKED"
    assert result.blocker_code == "KAGGLE_MODERN_API_TOKEN_REQUIRED"
    assert result.mutations_started == 0
    assert not gateway.calls
    assert DriverResult.model_validate(result.model_dump(mode="json")).outcome == "BLOCKED"


@pytest.mark.parametrize("ordinal", range(1, 25))
def test_each_executor_runs_only_safe_existing_probes_then_names_exact_gap(
    ordinal: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_BLOGGER_BATCH_ID", "batch-for-safe-accounting")
    gateway = FakeGateway()
    result = asyncio.run(execute(_request(ordinal), gateway))
    spec = EXECUTORS[ordinal - 1]
    assert result.outcome == "BLOCKED"
    assert result.blocker_code == spec.gap_code
    assert result.integration_dependency == spec.gap_dependency
    assert result.mutations_started == 0
    assert result.provider_run_ref is None
    assert all(check.detail_code != "OPERATIONAL_DRIVER_INTERFACE_MISSING" for check in result.capability_checks)
    called_tools = {tool for _profile, tool, _arguments in gateway.calls}
    assert called_tools <= {tool for _profile, tool in spec.tools}
    assert not called_tools & {
        "master.ensure",
        "master.rotation.request",
        "checkpoint.restore.request",
        "data.change.preview",
        "data.change.apply",
        "bloggers.import.preview",
        "bloggers.import.apply",
        "provider.resources.create",
        "provider.resources.run",
        "provider.resources.delete",
    }


def test_missing_profile_is_specific_and_precedes_safe_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    gateway = FakeGateway(missing_profile="operator")
    result = asyncio.run(execute(_request(13), gateway))
    assert result.blocker_code == "OPERATOR_MCP_TOKEN_MISSING"
    assert result.mutations_started == 0
    assert not gateway.calls


def test_missing_catalog_tool_is_scenario_specific(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    gateway = FakeGateway(missing_tool="provider.protected_resource.probe")
    result = asyncio.run(execute(_request(23), gateway))
    assert result.blocker_code == "FM23_MCP_TOOLSET_INCOMPLETE"
    assert "provider.protected_resource.probe" in str(result.integration_dependency)
    assert result.mutations_started == 0
    assert not gateway.calls


def test_driver_request_rejects_scenario_assertion_drift() -> None:
    payload = _driver_request(
        build_plan(
            matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            commit_sha="a" * 40,
            created_at=datetime(2026, 8, 11, tzinfo=UTC),
        ),
        build_plan(
            matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
            commit_sha="a" * 40,
            created_at=datetime(2026, 8, 11, tzinfo=UTC),
        )["scenarios"][0],
        resume_only=False,
    )
    payload["required_assertions"] = ["synthetic_pass", "not_allowed"]
    with pytest.raises(ValueError, match="exact FM01-FM24 catalog"):
        DriverRequest.model_validate(payload)


def test_extended_driver_result_example_validates() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas/provider/operational-kaggle-driver-result.v1.schema.json").read_text())
    example = json.loads((root / "examples/provider/operational-kaggle-driver-result.v1.example.json").read_text())
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)


def test_provider_workflow_selects_trusted_repository_driver_by_default() -> None:
    workflow = (Path(__file__).resolve().parents[2] / ".github/workflows/provider-real.yml").read_text()
    assert '["python","scripts/provider/operational_kaggle_driver.py"]' in workflow


def test_direct_driver_cli_emits_typed_exit_78_before_network_without_token(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    root = Path(__file__).resolve().parents[2]
    request = tmp_path / "request.json"
    result = tmp_path / "result.json"
    request.write_bytes(
        json.dumps(
            _driver_request(
                build_plan(
                    matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                    commit_sha="a" * 40,
                    created_at=datetime(2026, 8, 11, tzinfo=UTC),
                ),
                build_plan(
                    matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                    commit_sha="a" * 40,
                    created_at=datetime(2026, 8, 11, tzinfo=UTC),
                )["scenarios"][0],
                resume_only=False,
            )
        ).encode()
    )
    environment = os.environ.copy()
    environment.pop("KAGGLE_API_TOKEN", None)
    environment["KAGGLE_CONFIG_DIR"] = str(tmp_path / "no-token")
    completed = subprocess.run(
        [
            sys.executable,
            "scripts/provider/operational_kaggle_driver.py",
            "--request",
            str(request),
            "--result",
            str(result),
        ],
        cwd=root,
        env=environment,
        check=False,
        timeout=20,
    )
    assert completed.returncode == 78
    value = DriverResult.model_validate_json(result.read_bytes())
    assert value.outcome == "BLOCKED"
    assert value.blocker_code == "KAGGLE_MODERN_API_TOKEN_REQUIRED"
    assert value.mutations_started == 0
