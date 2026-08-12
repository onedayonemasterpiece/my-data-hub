from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from my_data_hub.acceptance.master_lifecycle import ACCEPTANCE_OPERATE_SCOPE
from my_data_hub.acceptance.scenario_operator import (
    AcceptanceScenarioOperatorAdapter,
    AcceptanceScenarioRequest,
    CheckpointAcceptanceLaunchCatalog,
    CheckpointAcceptanceLaunchRequest,
    CheckpointAcceptanceLaunchStatus,
    CheckpointDatasetInputClaim,
    CheckpointVerifierInputClaim,
    UnifiedAcceptanceScenarioExecutor,
)


@dataclass(frozen=True)
class Principal:
    subject: str = "owner"
    client_id: str = "acceptance-client"
    scopes: frozenset[str] = frozenset({ACCEPTANCE_OPERATE_SCOPE})


@dataclass
class Master:
    requested: list[Any] = field(default_factory=list)
    statuses: list[UUID] = field(default_factory=list)

    def request(self, request, principal) -> dict[str, Any]:
        self.requested.append((request, principal))
        return {"task_id": str(request.task_id), "scenario_id": request.scenario.value, "state": "PENDING"}

    def status(self, task_id: UUID, principal) -> dict[str, Any]:
        self.statuses.append(task_id)
        return {"found": False}


@dataclass
class Launcher:
    requests: list[CheckpointAcceptanceLaunchRequest] = field(default_factory=list)
    statuses: dict[UUID, CheckpointAcceptanceLaunchStatus] = field(default_factory=dict)

    def launch_checkpoint_acceptance(
        self, request: CheckpointAcceptanceLaunchRequest
    ) -> CheckpointAcceptanceLaunchStatus:
        self.requests.append(request)
        status = CheckpointAcceptanceLaunchStatus(
            request_id=request.request_id,
            scenario=request.scenario,
            operation_id=request.operation_id,
            task_run_id=request.task_run_id,
            principal_id=request.control_identity.principal_id,
            client_id=request.control_identity.client_id,
            state="REQUESTED",
            request_sha256=request.request_sha256,
            config_sha256="6" * 64,
        )
        self.statuses[request.request_id] = status
        return status

    def checkpoint_acceptance_status(
        self, request_id: UUID
    ) -> CheckpointAcceptanceLaunchStatus | None:
        return self.statuses.get(request_id)


def _catalog() -> CheckpointAcceptanceLaunchCatalog:
    template = CheckpointDatasetInputClaim(
        provider_ref="owner/empty-template",
        exact_version_ref="owner/empty-template/17",
        claim_sha256="1" * 64,
        manifest_sha256="2" * 64,
        content_sha256="3" * 64,
    )
    verifier = CheckpointVerifierInputClaim(
        provider_ref="owner/restore-verifier",
        exact_version_ref="owner/restore-verifier/23",
        claim_sha256="4" * 64,
        source_sha256="5" * 64,
    )
    return CheckpointAcceptanceLaunchCatalog(
        provider_owner="owner",
        evidence_notebook_ref="owner/checkpoint-evidence",
        candidate_dataset_refs={
            "FM05": "owner/fm05-candidate",
            "FM14": "owner/fm14-candidate",
            "FM15": "owner/fm15-candidate",
        },
        template_input=template,
        verifier_inputs={"FM05": verifier, "FM15": verifier},
        verifier_notebook_refs={
            "FM05": "owner/fm05-restore-verifier",
            "FM15": "owner/fm15-restore-verifier",
        },
    )


def _request(scenario: str, task_id: UUID | None = None) -> dict[str, Any]:
    value = {
        "task_id": str(task_id or uuid4()),
        "scenario": scenario,
        "idempotency_key": f"acceptance-fixed-{scenario.lower()}-request",
        "source_revision": "a" * 40,
    }
    if scenario not in {"FM04", "FM05", "FM07", "FM14", "FM15"}:
        value["target_operation_id"] = str(uuid4())
    return value


def test_checkpoint_request_builds_exact_provider_only_launch_metadata() -> None:
    launcher = Launcher()
    master = Master()
    executor = UnifiedAcceptanceScenarioExecutor(
        master=master,  # type: ignore[arg-type]
        checkpoint=launcher,
        checkpoint_catalog=_catalog(),
    )
    response = executor.request(AcceptanceScenarioRequest.model_validate(_request("FM05")), Principal())
    launch = launcher.requests[0]
    assert response["state"] == "REQUESTED"
    assert launch.timeout_seconds == 900
    assert launch.status_config_file == "kaggle_run.json"
    assert launch.status_helper_file == "kaggle_status_client.py"
    assert launch.template_input.exact_version_ref.endswith("/17")
    assert launch.verifier_input is not None and launch.verifier_input.exact_version_ref.endswith("/23")
    assert launch.control_identity.scope == "acceptance:operate"
    assert launch.control_identity.principal_id == "owner"
    serialized = launch.model_dump_json()
    assert "/kaggle/" not in serialized
    assert "secret_value" not in serialized
    assert not master.requested


def test_fm14_has_no_verifier_and_master_scenario_uses_master_executor() -> None:
    launcher = Launcher()
    master = Master()
    executor = UnifiedAcceptanceScenarioExecutor(
        master=master,  # type: ignore[arg-type]
        checkpoint=launcher,
        checkpoint_catalog=_catalog(),
    )
    executor.request(AcceptanceScenarioRequest.model_validate(_request("FM14")), Principal())
    assert launcher.requests[-1].verifier_input is None
    assert launcher.requests[-1].verifier_notebook_ref is None
    response = executor.request(AcceptanceScenarioRequest.model_validate(_request("FM07")), Principal())
    assert response["found"] is True and response["scenario_id"] == "FM07"
    assert len(master.requested) == 1


def test_unified_adapter_has_only_request_status_and_requires_owner_scope() -> None:
    launcher = Launcher()
    adapter = AcceptanceScenarioOperatorAdapter(
        UnifiedAcceptanceScenarioExecutor(
            master=Master(),  # type: ignore[arg-type]
            checkpoint=launcher,
            checkpoint_catalog=_catalog(),
        )
    )
    assert set(adapter.tool_schemas()) == {
        "acceptance.scenario.request",
        "acceptance.scenario.status",
    }
    with pytest.raises(PermissionError, match="acceptance:operate"):
        adapter.call(
            "acceptance.scenario.request",
            _request("FM05"),
            Principal(scopes=frozenset({"data:read"})),
        )
    with pytest.raises(ValueError, match="unknown"):
        adapter.call("acceptance.scenario.list", {}, Principal())
    with pytest.raises(ValidationError):
        AcceptanceScenarioRequest.model_validate({**_request("FM05"), "clock": "now"})
    with pytest.raises(ValidationError, match="exact target operation"):
        AcceptanceScenarioRequest.model_validate(
            {key: value for key, value in _request("FM10").items() if key != "target_operation_id"}
        )
    with pytest.raises(ValidationError, match="cannot select"):
        AcceptanceScenarioRequest.model_validate(
            {**_request("FM05"), "target_operation_id": str(uuid4())}
        )


def test_status_prefers_exact_checkpoint_request_and_never_lists_tasks() -> None:
    task_id = uuid4()
    launcher = Launcher()
    master = Master()
    executor = UnifiedAcceptanceScenarioExecutor(
        master=master,  # type: ignore[arg-type]
        checkpoint=launcher,
        checkpoint_catalog=_catalog(),
    )
    executor.request(
        AcceptanceScenarioRequest.model_validate(_request("FM15", task_id)), Principal()
    )
    response = executor.status(task_id, Principal())
    assert response["request_id"] == str(task_id) and response["scenario"] == "FM15"
    assert not master.statuses
    with pytest.raises(ValueError, match="exact contract"):
        AcceptanceScenarioOperatorAdapter(executor).call(
            "acceptance.scenario.status",
            {"task_id": str(task_id), "scenario": "FM15"},
            Principal(),
        )
