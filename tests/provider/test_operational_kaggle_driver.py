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


class ClaimedActionGateway(FakeGateway):
    def __init__(self, *, rotation_ready: bool = False, action_state: str = "DURABLE_COMPLETE") -> None:
        super().__init__()
        self.rotation_ready = rotation_ready
        self.action_state = action_state

    async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((profile, tool, dict(arguments)))
        if tool == "master.status":
            return {
                "master_state": "STOPPED" if self.rotation_ready else "ACTIVE",
                "master_epoch": 2,
                "canonical_revision": 9,
            }
        if tool == "checkpoint.status":
            return {
                "current_checkpoint_id": "checkpoint-current",
                "current_exact_version_ref": "owner/checkpoints/12",
                "generation": 3,
                "current": {
                    "source_state": "STOPPED" if self.rotation_ready else "ACTIVE",
                    "source_epoch": 2,
                    "canonical_revision": 9,
                },
            }
        if tool == "provider.resources.read":
            resource_ref = str(arguments["resource_ref"])
            ordinal = 13 if "fm13" in resource_ref else 6
            return {
                "claim_sha256": "c" * 64,
                "task_id": "22222222-2222-4222-8222-222222222222",
                "task_run_id": str(_request(ordinal).task_run_id),
                "provider_ref": resource_ref,
                "provider_run_ref": f"{resource_ref}/7",
                "provider_kernel_id": 701,
                "source_version": 4,
                "source_sha256": "d" * 64,
                "fingerprint": {"algorithm": "sha256", "value": "e" * 64},
                "private": True,
            }
        if tool in {"checkpoint.restore.request", "master.rotation.request"}:
            from scripts.provider.operational_kaggle_driver import _action_request

            ordinal = 6 if tool == "checkpoint.restore.request" else 13
            request = _request(ordinal)
            spec = EXECUTORS[ordinal - 1]
            observations = {
                "checkpoint": await self.call("reader", "checkpoint.status", {}),
                "master": await self.call("reader", "master.status", {}),
            }
            _arguments, operation_id, _timeout = _action_request(request, spec, observations)
            return {
                "accepted": True,
                "duplicate": False,
                "execution_supported": True,
                "operation_id": operation_id,
                "state": "REQUESTED",
            }
        if tool == "operation.get":
            return {
                "found": True,
                "operation_id": arguments["operation_id"],
                "operation_kind": "acceptance-action",
                "state": self.action_state,
                "updated_at": "2026-08-11T00:00:00Z",
            }
        return await super().call(profile, tool, arguments)


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


def test_restore_runs_only_after_exact_provider_evidence_claim_and_durable_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(6)
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
                "claims": {
                    "FM06": {
                        "requirement_id": "FM06",
                        "task_id": "22222222-2222-4222-8222-222222222222",
                        "resource_ref": "owner/fm06-evidence",
                        "claim_sha256": "c" * 64,
                    }
                },
            }
        ),
    )
    gateway = ClaimedActionGateway()

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert result.mutations_started == 1
    assert result.provider_run_ref == "owner/fm06-evidence/7"
    calls = [tool for _profile, tool, _arguments in gateway.calls]
    assert calls.index("provider.resources.read") < calls.index("checkpoint.restore.request")
    assert "operation.get" in calls


def test_contradictory_action_acceptance_is_fail_not_zero_mutation_blocker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
                "claims": {
                    "FM06": {
                        "requirement_id": "FM06",
                        "task_id": "22222222-2222-4222-8222-222222222222",
                        "resource_ref": "owner/fm06-evidence",
                        "claim_sha256": "c" * 64,
                    }
                },
            }
        ),
    )

    class ContradictoryGateway(ClaimedActionGateway):
        async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "checkpoint.restore.request":
                self.calls.append((profile, tool, dict(arguments)))
                return {"accepted": True, "execution_supported": False, "operation_id": None}
            return await super().call(profile, tool, arguments)

    result = asyncio.run(execute(_request(6), ContradictoryGateway()))

    assert result.outcome == "FAIL"
    assert result.mutations_started == 1
    assert result.blocker_code is None


def test_rotation_refuses_action_until_checkpoint_source_is_durably_stopped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
                "claims": {
                    "FM13": {
                        "requirement_id": "FM13",
                        "task_id": "22222222-2222-4222-8222-222222222222",
                        "resource_ref": "owner/fm13-evidence",
                        "claim_sha256": "c" * 64,
                    }
                },
            }
        ),
    )
    gateway = ClaimedActionGateway(rotation_ready=False)

    result = asyncio.run(execute(_request(13), gateway))

    assert result.outcome == "BLOCKED"
    assert result.mutations_started == 0
    assert result.blocker_code == "FM13_EVIDENCE_ACTION_PRECONDITION_UNMET"
    assert "master.rotation.request" not in {tool for _profile, tool, _arguments in gateway.calls}


def test_rotation_uses_exact_stopped_checkpoint_binding_and_polls_durable_operation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
                "claims": {
                    "FM13": {
                        "requirement_id": "FM13",
                        "task_id": "22222222-2222-4222-8222-222222222222",
                        "resource_ref": "owner/fm13-evidence",
                        "claim_sha256": "c" * 64,
                    }
                },
            }
        ),
    )
    gateway = ClaimedActionGateway(rotation_ready=True)

    result = asyncio.run(execute(_request(13), gateway))

    assert result.outcome == "PASS"
    rotation = next(arguments for _profile, tool, arguments in gateway.calls if tool == "master.rotation.request")
    assert rotation["expected_active_epoch"] == 2
    assert rotation["expected_canonical_revision"] == 9
    assert rotation["checkpoint_id"] == "checkpoint-current"
    assert "operation.get" in {tool for _profile, tool, _arguments in gateway.calls}


def test_resume_only_never_creates_a_missing_durable_action(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
                "claims": {
                    "FM06": {
                        "requirement_id": "FM06",
                        "task_id": "22222222-2222-4222-8222-222222222222",
                        "resource_ref": "owner/fm06-evidence",
                        "claim_sha256": "c" * 64,
                        "operation_id": "f" * 64,
                    }
                },
            }
        ),
    )

    class MissingResumeGateway(ClaimedActionGateway):
        async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "operation.get":
                self.calls.append((profile, tool, dict(arguments)))
                return {"found": False}
            return await super().call(profile, tool, arguments)

    gateway = MissingResumeGateway()
    request = _request(6).model_copy(update={"resume_only": True})

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "FAIL"
    assert result.mutations_started == 0
    assert result.blocker_code is None
    assert "provider.resources.read" not in {tool for _profile, tool, _arguments in gateway.calls}
    assert "checkpoint.restore.request" not in {tool for _profile, tool, _arguments in gateway.calls}


def test_resume_uses_claimed_operation_id_without_recomputing_or_creating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    operation_id = "f" * 64
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
                "claims": {
                    "FM06": {
                        "requirement_id": "FM06",
                        "task_id": "22222222-2222-4222-8222-222222222222",
                        "resource_ref": "owner/fm06-evidence",
                        "claim_sha256": "c" * 64,
                        "operation_id": operation_id,
                    }
                },
            }
        ),
    )

    class ResumeGateway(ClaimedActionGateway):
        async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "operation.get":
                self.calls.append((profile, tool, dict(arguments)))
                return {
                    "found": True,
                    "operation_id": arguments["operation_id"],
                    "operation_kind": "checkpoint_restore_smoke",
                    "state": "DURABLE_COMPLETE",
                    "updated_at": "2026-08-11T00:00:00Z",
                }
            return await super().call(profile, tool, arguments)

    gateway = ResumeGateway()
    request = _request(6).model_copy(update={"resume_only": True})

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert result.mutations_started == 1
    operation_calls = [arguments for _profile, tool, arguments in gateway.calls if tool == "operation.get"]
    assert operation_calls and all(arguments["operation_id"] == operation_id for arguments in operation_calls)
    assert "checkpoint.restore.request" not in {tool for _profile, tool, _arguments in gateway.calls}


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


def test_evidence_claim_schema_and_runtime_share_keyed_requirement_identity() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas/provider/operational-kaggle-evidence-claims.v1.schema.json").read_text())
    payload = {
        "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
        "claims": {
            "FM06": {
                "requirement_id": "FM13",
                "task_id": "22222222-2222-4222-8222-222222222222",
                "resource_ref": "owner/evidence",
                "claim_sha256": "c" * 64,
            }
        },
    }
    assert list(Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(payload))
    from scripts.provider.operational_kaggle_driver import EvidenceClaimsDocument

    with pytest.raises(ValueError, match="key differs"):
        EvidenceClaimsDocument.model_validate(payload)


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
