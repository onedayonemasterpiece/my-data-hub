from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource

import scripts.provider.operational_kaggle_driver as driver_module
from my_data_hub.acceptance.data_production import ProductionDataWorkloadReceipt
from my_data_hub.acceptance.data_workloads import (
    BloggerQuarantineEvidence,
    DataPhase,
    DataWorkloadEvidenceBundle,
    DataWorkloadPlan,
    DataWorkloadState,
    DuplicateReviewEvidence,
    RequirementEvidence,
)
from my_data_hub.acceptance.scenario_operator import (
    CheckpointAcceptanceLaunchStatus,
    CheckpointAcceptanceOperationalResult,
)
from my_data_hub.checkpoints.acceptance import CheckpointAcceptanceReceipt
from my_data_hub.hashing import canonical_json_bytes
from scripts.provider.operational_kaggle_driver import (
    EXECUTORS,
    CleanupBinding,
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
        if tool == "runtime.events.history":
            events = []
            for sequence, event_type in ((1, "runtime.heartbeat"), (2, "runtime.terminal")):
                events.append(
                    {
                        "event_id": str(UUID(int=sequence)),
                        "schema_version": "my-data-hub-runtime-event.v1",
                        "run_id": arguments["run_id"],
                        "attempt_id": arguments["attempt_id"],
                        "service_instance_id": "33333333-3333-4333-8333-333333333333",
                        "source_identity": "owner/runtime",
                        "source_version": 1,
                        "epoch": arguments["epoch"],
                        "event_type": event_type,
                        "emitted_at": f"2026-08-11T00:00:0{sequence}Z",
                        "received_at": f"2026-08-11T00:00:0{sequence}Z",
                        "local_sequence": sequence,
                        "body_sha256": str(sequence) * 64,
                        "body_bytes": 100 + sequence,
                    }
                )
            return {"events": events, "count": 2, "bounded": True}
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
            return {
                "evaluated": True,
                "protected": True,
                "denied": True,
                "reason_code": "PROTECTED_RESOURCE_DENIED",
                "mutation_attempted": False,
            }
        if tool == "bloggers.migration.accounting":
            return {"bounded": True, "accounted": True}
        if tool == "bloggers.statistics":
            return {"bounded": True, "count": 10}
        if tool == "embedding.coverage":
            return {"e5": {"coverage": 1.0}, "bge_m3": {"coverage": 1.0}}
        raise AssertionError(f"unexpected mutating/unsupported tool call: {tool}")


class EvidenceLifecycleGateway(FakeGateway):
    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.claims: dict[tuple[str, str], dict[str, Any]] = {}

    @staticmethod
    def _event(sequence: int, event_type: str, evidence: dict[str, Any]) -> dict[str, Any]:
        return {
            "sequence": sequence,
            "event_type": event_type,
            "evidence": evidence,
            "evidence_sha256": hashlib.sha256(canonical_json_bytes(evidence)).hexdigest(),
            "recorded_at": "2026-08-11T00:00:00Z",
        }

    async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((profile, tool, dict(arguments)))
        if tool == "provider.acceptance.claim.get":
            return self.claims.get(
                (str(arguments["scenario_id"]), str(arguments["task_id"])),
                {"found": False, "scenario_id": arguments["scenario_id"], "task_id": arguments["task_id"]},
            )
        if tool == "provider.acceptance.dataset.lifecycle":
            provider = {
                "provider_ref": arguments["resource_ref"],
                "provider_version": 2,
                "package_sha256": "a" * 64,
                "fingerprint": {"algorithm": "sha256", "value": "b" * 64},
                "claim_sha256": "c" * 64,
                "create_effect_id": "11111111-1111-4111-8111-111111111111",
                "version_effect_id": "22222222-2222-4222-8222-222222222222",
            }
            cleanup = {
                "provider_ref": arguments["resource_ref"],
                "claim_sha256": "c" * 64,
                "cleanup_effect_id": "33333333-3333-4333-8333-333333333333",
                "cleanup_outcome": "applied",
            }
            claim = {
                "found": True,
                "scenario_id": arguments["scenario_id"],
                "task_id": arguments["task_id"],
                "state": "SUCCEEDED",
                "failure_code": None,
                "mutation_started": True,
                "cleanup_state": "COMPLETE",
                "evidence": [self._event(1, "PROVIDER_DATASET", provider), self._event(2, "CLEANUP", cleanup)],
                "bounded": True,
            }
            self.claims[(arguments["scenario_id"], arguments["task_id"])] = claim
            return claim
        if tool == "provider.acceptance.notebook.lifecycle":
            provider_ref = str(arguments["resource_ref"])
            run_ref = f"{provider_ref}/7"
            notebook = {
                "provider_ref": provider_ref,
                "provider_version": 4,
                "source_version": 4,
                "source_sha256": hashlib.sha256(str(arguments["source_utf8"]).encode()).hexdigest(),
                "fingerprint": {"algorithm": "sha256", "value": "e" * 64},
                "provider_kernel_id": 701,
                "provider_run_ref": run_ref,
                "task_run_id": arguments["task_run_id"],
                "claim_sha256": "d" * 64,
                "run_effect_id": "44444444-4444-4444-8444-444444444444",
                "terminal_state": "complete",
            }
            output = {
                "provider_run_ref": run_ref,
                "output_file_name": "operational-result.json",
                "output_file_sha256": arguments["expected_output_sha256"],
                "output_tree_sha256": "f" * 64,
                "file_count": 1,
            }
            output["output_receipt_sha256"] = hashlib.sha256(canonical_json_bytes(output)).hexdigest()
            claim = {
                "found": True,
                "scenario_id": arguments["scenario_id"],
                "task_id": arguments["task_id"],
                "state": "SUCCEEDED",
                "failure_code": None,
                "mutation_started": True,
                "cleanup_state": "PENDING",
                "evidence": [self._event(1, "PROVIDER_NOTEBOOK", notebook), self._event(2, "OUTPUT_READ", output)],
                "bounded": True,
            }
            self.claims[(arguments["scenario_id"], arguments["task_id"])] = claim
            return claim
        if tool == "provider.acceptance.claim.cleanup":
            key = (str(arguments["scenario_id"]), str(arguments["task_id"]))
            claim = self.claims[key]
            notebook = next(
                item["evidence"]
                for item in claim["evidence"]
                if item["event_type"] == "PROVIDER_NOTEBOOK"
            )
            cleanup = {
                "provider_ref": notebook["provider_ref"],
                "claim_sha256": arguments["claim_sha256"],
                "cleanup_effect_id": "55555555-5555-4555-8555-555555555555",
                "cleanup_outcome": "applied",
            }
            claim = {
                **claim,
                "cleanup_state": "COMPLETE",
                "evidence": [*claim["evidence"], self._event(3, "CLEANUP", cleanup)],
            }
            self.claims[key] = claim
            return claim
        return await super().call(profile, tool, arguments)


class ClaimedActionGateway(EvidenceLifecycleGateway):
    def __init__(self, *, rotation_ready: bool = False, action_state: str = "DURABLE_COMPLETE") -> None:
        super().__init__()
        self.rotation_ready = rotation_ready
        self.action_state = action_state

    async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((profile, tool, dict(arguments)))
        if tool == "master.status":
            return {
                "master_state": "STOPPED" if self.rotation_ready else "ACTIVE",
                "instance_id": None if self.rotation_ready else "restored-master",
                "master_epoch": 2,
                "canonical_revision": 9,
                "lease_expires_at": None if self.rotation_ready else "2026-08-11T01:00:00Z",
                "provider_run_ref": None if self.rotation_ready else "owner/master/8",
                "provider_kernel_id": None if self.rotation_ready else 808,
                "capabilities": [] if self.rotation_ready else ["bloggers:read"],
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


class FM20Gateway(FakeGateway):
    def __init__(self, *, ensure_response: dict[str, Any] | None = None, empty_search: bool = False) -> None:
        super().__init__()
        self.ensure_response = ensure_response
        self.empty_search = empty_search
        self.ensure_started = False

    async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((profile, tool, dict(arguments)))
        if tool == "provider.resources.read":
            return {
                "claim_sha256": "c" * 64,
                "task_id": "22222222-2222-4222-8222-222222222222",
                "task_run_id": str(_request(20).task_run_id),
                "provider_ref": "owner/fm20-evidence",
                "provider_run_ref": "owner/fm20-evidence/7",
                "provider_kernel_id": 702,
                "source_version": 5,
                "source_sha256": "d" * 64,
                "fingerprint": {"algorithm": "sha256", "value": "e" * 64},
                "private": True,
            }
        if tool == "master.status":
            if not self.ensure_started:
                return {
                    "master_state": "ABSENT",
                    "operation_id": None,
                    "instance_id": None,
                    "master_epoch": None,
                    "canonical_revision": None,
                    "lease_expires_at": None,
                    "capabilities": [],
                }
            return {
                "master_state": "ACTIVE",
                "operation_id": None,
                "instance_id": "fm20-master-instance",
                "master_epoch": 8,
                "canonical_revision": 41,
                "lease_expires_at": "2026-08-11T06:00:00Z",
                "capabilities": ["bloggers:read"],
            }
        if tool == "master.ensure":
            self.ensure_started = True
            return self.ensure_response or {
                "operation_id": "11111111-1111-4111-8111-111111111111",
                "master_state": "REQUESTED",
                "duplicate": False,
                "intent": "explicit-mcp-request",
                "terminal": False,
            }
        if tool == "operation.get":
            self.ensure_started = True
            return {
                "found": True,
                "operation_id": arguments["operation_id"],
                "operation_kind": "ensure_master",
                "state": "ACTIVE",
                "updated_at": "2026-08-11T05:00:00Z",
            }
        if tool == "bloggers.search":
            return {
                "items": [] if self.empty_search else [{"blogger_id": "bounded-result"}],
                "cursor": None,
                "master_epoch": 8,
                "canonical_revision": 41,
                "complete": True,
            }
        raise AssertionError(f"unexpected FM20 tool call: {tool}")


def _request(ordinal: int) -> DriverRequest:
    plan = build_plan(
        matrix_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        commit_sha="a" * 40,
        created_at=datetime(2026, 8, 11, tzinfo=UTC),
    )
    return DriverRequest.model_validate(_driver_request(plan, plan["scenarios"][ordinal - 1], resume_only=False))


def _fm14_checkpoint_status(request: DriverRequest) -> dict[str, Any]:
    root = Path(__file__).resolve().parents[2]
    result = json.loads(
        (root / "examples/provider/checkpoint-acceptance-operational-result.v1.example.json").read_text()
    )
    result["task_run_id"] = str(request.task_run_id)
    result["source_revision"] = request.commit_sha
    result["receipt"]["task_run_id"] = str(request.task_run_id)
    receipt = CheckpointAcceptanceReceipt.model_validate(result["receipt"])
    result["receipt_sha256"] = receipt.receipt_sha256
    typed_result = CheckpointAcceptanceOperationalResult.model_validate(result)
    result = typed_result.model_dump(mode="json")
    result_sha256 = hashlib.sha256(canonical_json_bytes(result)).hexdigest()
    provider_ref = result["locator"]["evidence_notebook_ref"]
    status = {
        "schema_version": "my-data-hub-checkpoint-acceptance-launch-status.v1",
        "found": True,
        "request_id": str(request.task_run_id),
        "scenario": "FM14",
        "operation_id": result["operation_id"],
        "task_run_id": str(request.task_run_id),
        "principal_id": "unit-test-owner",
        "client_id": "unit-test-driver",
        "request_persisted": True,
        "state": "LIVE_EVIDENCE_READY",
        "request_sha256": "1" * 64,
        "config_sha256": result["config_sha256"],
        "result_sha256": result_sha256,
        "provider_output": {
            "provider_ref": provider_ref,
            "provider_run_ref": f"{provider_ref}/7",
            "provider_kernel_id": 707,
            "source_version": 7,
            "source_sha256": "e" * 64,
            "provider_claim_sha256": "f" * 64,
            "output_file_name": "operational-result.json",
            "output_file_sha256": result_sha256,
            "output_tree_sha256": "a" * 64,
            "output_receipt_sha256": "b" * 64,
            "private": True,
        },
        "result": result,
        "blocker_code": None,
        "failure_code": None,
        "official_adapter_observed": True,
    }
    return CheckpointAcceptanceLaunchStatus.model_validate(status).model_dump(mode="json")


class CheckpointAcceptanceGateway(FakeGateway):
    def __init__(
        self,
        request: DriverRequest,
        *,
        existing_status: dict[str, Any] | None = None,
        lose_request_response: bool = False,
    ) -> None:
        super().__init__()
        self.request = request
        self.status = existing_status
        self.lose_request_response = lose_request_response

    async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((profile, tool, dict(arguments)))
        if tool == "acceptance.scenario.status":
            return self.status or {"found": False}
        if tool == "acceptance.scenario.request":
            assert arguments == {
                "task_id": str(self.request.task_run_id),
                "scenario": "FM14",
                "idempotency_key": (
                    f"operational:{self.request.matrix_id}:FM14:checkpoint"
                ),
                "source_revision": self.request.commit_sha,
            }
            self.status = _fm14_checkpoint_status(self.request)
            if self.lose_request_response:
                raise TimeoutError("unit-test lost response after durable request")
            return self.status
        raise AssertionError(f"unexpected checkpoint tool call: {tool}")


def _set_evidence_config(monkeypatch: pytest.MonkeyPatch, *, fm03: bool = False) -> None:
    value: dict[str, Any] = {
        "schema_version": "my-data-hub-operational-kaggle-evidence-driver.v1",
        "provider_owner": "evidence-owner",
        "fm03_runtime": None,
    }
    if fm03:
        value["fm03_runtime"] = {
            "run_id": "11111111-1111-4111-8111-111111111111",
            "attempt_id": "22222222-2222-4222-8222-222222222222",
            "epoch": 7,
        }
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_EVIDENCE_DRIVER_JSON", json.dumps(value))


def _fm20_environment(request: DriverRequest) -> tuple[str, str]:
    now = datetime.now(UTC)
    image = "sha256:" + "9" * 64
    receipt: dict[str, Any] = {
        "schema_version": "my-data-hub-deployment-evidence.v2",
        "source_identity": "onedayonemasterpiece/my-data-hub",
        "deployed_commit": request.commit_sha,
        "source_tree_sha256": "8" * 64,
        "installed_release_tree_sha256": "8" * 64,
        "host_id_sha256": "1" * 64,
        "issued_at": (now - timedelta(minutes=1)).isoformat().replace("+00:00", "Z"),
        "expires_at": (now + timedelta(minutes=30)).isoformat().replace("+00:00", "Z"),
        "checks": {
            "services": {
                "control-plane": "running",
                "oauth-server": "running",
                "remote-mcp": "running",
            },
            "service_image_ids": {
                "control-plane": image,
                "oauth-server": image,
                "remote-mcp": image,
            },
            "database_process_present": False,
            "pgdata_present": False,
            "database_environment_present": False,
            "my_data_hub_public_listener_ports": [],
            "my_data_hub_loopback_listener_ports": [8080, 8765, 8780],
            "process_kill": {
                "target_service": "remote-mcp",
                "killed_at": (now - timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
                "recovered_at": (now - timedelta(minutes=4)).isoformat().replace("+00:00", "Z"),
                "before_process_sha256": "2" * 64,
                "after_process_sha256": "3" * 64,
                "recovered": True,
            },
            "reboot_autostart": {
                "rebooted_at": (now - timedelta(minutes=3)).isoformat().replace("+00:00", "Z"),
                "verified_at": (now - timedelta(minutes=2)).isoformat().replace("+00:00", "Z"),
                "before_boot_id_sha256": "4" * 64,
                "after_boot_id_sha256": "5" * 64,
                "systemd_unit": "my-data-hub-control-plane.service",
                "unit_enabled": True,
                "linger_enabled": True,
                "autostart_services": ["control-plane", "oauth-server", "remote-mcp"],
            },
        },
    }
    private = Ed25519PrivateKey.generate()
    canonical = json.dumps(receipt, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    receipt["signature"] = {
        "algorithm": "Ed25519",
        "key_id": "fm20-test-key",
        "value": base64.urlsafe_b64encode(private.sign(canonical)).rstrip(b"=").decode(),
    }
    public_pem = private.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    bundle = {
        "schema_version": "my-data-hub-operational-kaggle-fm20-evidence.v1",
        "deployment_evidence": receipt,
        "public_key_pem": public_pem,
        "expected_key_id": "fm20-test-key",
        "expected_source_identity": "onedayonemasterpiece/my-data-hub",
        "expected_source_tree_sha256": "8" * 64,
        "expected_service_image_ids": {
            "control-plane": image,
            "oauth-server": image,
            "remote-mcp": image,
        },
        "blogger_query": "Калининград",
    }
    claims = {
        "schema_version": "my-data-hub-operational-kaggle-evidence-claims.v1",
        "claims": {
            "FM20": {
                "requirement_id": "FM20",
                "task_id": "22222222-2222-4222-8222-222222222222",
                "resource_ref": "owner/fm20-evidence",
                "claim_sha256": "c" * 64,
            }
        },
        "fm20_evidence": bundle,
    }
    return json.dumps(bundle), json.dumps(claims)


def test_driver_has_one_named_executor_and_only_unclosed_scenarios_retain_missing_gaps() -> None:
    assert len(EXECUTORS) == 24
    assert [item.requirement_id for item in EXECUTORS] == [f"FM{i:02d}" for i in range(1, 25)]
    assert len({item.gap_code for item in EXECUTORS}) == 24
    assert all(item.gap_code != "OPERATIONAL_DRIVER_INTERFACE_MISSING" for item in EXECUTORS)
    closed = {"FM01", "FM02", "FM03", "FM06", "FM16", "FM17", "FM18", "FM19", "FM21", "FM22", "FM23"}
    assert all(
        "missing" in item.gap_code.casefold()
        for item in EXECUTORS
        if item.requirement_id not in closed | {"FM20"}
    )
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


def test_fm14_checkpoint_executor_requests_then_returns_exact_evidence_locator(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    request = _request(14)
    gateway = CheckpointAcceptanceGateway(request)

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert result.phase == "EXECUTE"
    assert result.cleanup_state == "NOT_REQUIRED"
    assert result.mutations_started == 2
    assert result.provider_run_ref == "owner/checkpoint-acceptance-evidence-fm14/7"
    assert result.claim_task_id == request.task_run_id
    assert result.claim_sha256 == "f" * 64
    assert result.output_receipt_sha256 == "b" * 64
    assert result.output_file_sha256 is not None
    assert result.output_tree_sha256 == "a" * 64
    assert [tool for _profile, tool, _arguments in gateway.calls] == [
        "acceptance.scenario.status",
        "acceptance.scenario.request",
    ]
    DriverResult.model_validate(result.model_dump(mode="json"))


def test_fm14_checkpoint_resume_reconciles_only_existing_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    request = _request(14).model_copy(update={"resume_only": True})
    gateway = CheckpointAcceptanceGateway(request, existing_status=_fm14_checkpoint_status(request))

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert [tool for _profile, tool, _arguments in gateway.calls] == ["acceptance.scenario.status"]


def test_fm14_checkpoint_preflight_blocker_is_non_mutating(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    request = _request(14)
    blocked = {
        "schema_version": "my-data-hub-checkpoint-acceptance-launch-status.v1",
        "found": True,
        "request_id": str(request.task_run_id),
        "scenario": "FM14",
        "operation_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "task_run_id": str(request.task_run_id),
        "principal_id": "unit-test-owner",
        "client_id": "unit-test-driver",
        "request_persisted": True,
        "state": "BLOCKED",
        "request_sha256": "1" * 64,
        "config_sha256": "2" * 64,
        "result_sha256": None,
        "provider_output": None,
        "result": None,
        "blocker_code": "CHECKPOINT_OWNER_ASSETS_MISSING",
        "failure_code": None,
        "official_adapter_observed": False,
    }
    gateway = CheckpointAcceptanceGateway(request, existing_status=blocked)

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "BLOCKED"
    assert result.blocker_code == "CHECKPOINT_OWNER_ASSETS_MISSING"
    assert result.mutations_started == 0
    assert [tool for _profile, tool, _arguments in gateway.calls] == ["acceptance.scenario.status"]


@pytest.mark.parametrize("tool", ["acceptance.scenario.request", "acceptance.scenario.status"])
def test_fm14_checkpoint_missing_owner_tool_blocks_before_status_or_mutation(
    tool: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    gateway = FakeGateway(missing_tool=tool)

    result = asyncio.run(execute(_request(14), gateway))

    assert result.outcome == "BLOCKED"
    assert result.blocker_code == "FM14_MCP_TOOLSET_INCOMPLETE"
    assert result.mutations_started == 0
    assert not gateway.calls


def test_fm14_checkpoint_lost_request_response_reconciles_same_task(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    request = _request(14)
    gateway = CheckpointAcceptanceGateway(request, lose_request_response=True)

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert [tool for _profile, tool, _arguments in gateway.calls] == [
        "acceptance.scenario.status",
        "acceptance.scenario.request",
        "acceptance.scenario.status",
    ]


def test_fm14_checkpoint_invalid_post_action_evidence_fails_not_blocks(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    request = _request(14)
    status = _fm14_checkpoint_status(request)
    status["result"]["source_revision"] = "f" * 40
    gateway = CheckpointAcceptanceGateway(request, existing_status=status)

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "FAIL"
    assert result.mutations_started == 1
    assert result.blocker_code is None


def test_checkpoint_driver_json_schema_requires_exact_output_receipts(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    request = _request(14)
    result = asyncio.run(execute(request, CheckpointAcceptanceGateway(request))).model_dump(mode="json")
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas/provider/operational-kaggle-driver-result.v2.schema.json").read_text())
    validator = Draft202012Validator(schema, format_checker=FormatChecker())

    validator.validate(result)
    result["output_file_sha256"] = None
    assert list(validator.iter_errors(result))


def test_missing_profile_is_specific_and_precedes_safe_probe(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    gateway = FakeGateway(missing_profile="operator")
    result = asyncio.run(execute(_request(13), gateway))
    assert result.blocker_code == "OPERATOR_MCP_TOKEN_MISSING"
    assert result.mutations_started == 0
    assert not gateway.calls


@pytest.mark.parametrize("ordinal", [1, 2, 3, 22, 23])
def test_exact_evidence_scenarios_return_ready_not_pass_before_cleanup(
    ordinal: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_evidence_config(monkeypatch, fm03=ordinal == 3)
    gateway = EvidenceLifecycleGateway()

    result = asyncio.run(execute(_request(ordinal), gateway))

    assert result.outcome == "READY"
    assert result.phase == "EXECUTE"
    assert result.cleanup_state == "PENDING"
    assert result.mutations_started >= 1
    assert result.claim_task_id is not None
    assert result.output_receipt_sha256 is not None
    assert result.provider_run_ref is not None
    assert any(tool == "provider.acceptance.notebook.lifecycle" for _profile, tool, _args in gateway.calls)
    if ordinal in {1, 22}:
        task_ids = {
            str(args["task_id"])
            for _profile, tool, args in gateway.calls
            if tool in {
                "provider.acceptance.dataset.lifecycle",
                "provider.acceptance.notebook.lifecycle",
            }
        }
        assert len(task_ids) == 2
        notebook = next(
            args for _profile, tool, args in gateway.calls if tool == "provider.acceptance.notebook.lifecycle"
        )
        assert notebook["dataset_inputs"] == []
    if ordinal == 3:
        assert any(tool == "runtime.events.history" for _profile, tool, _args in gateway.calls)


class _FixedDataStateStore:
    def __init__(self, state: DataWorkloadState) -> None:
        self.state = state

    def load(self, _plan: DataWorkloadPlan) -> DataWorkloadState:
        return self.state


def _data_evidence(request: DriverRequest) -> tuple[DataWorkloadPlan, DataWorkloadState, ProductionDataWorkloadReceipt]:
    plan = DataWorkloadPlan(
        matrix_id=request.matrix_id,
        source_commit=request.commit_sha,
        blogger_project_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        blogger_snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        blogger_source_revision="b" * 40,
        embedding_probe_query_sha256="c" * 64,
    )
    assertion_names = {
        spec.requirement_id: spec.assertions
        for spec in driver_module.SCENARIOS
        if spec.requirement_id in {"FM16", "FM17", "FM18", "FM19", "FM21"}
    }
    shared_request = "11111111-1111-4111-8111-111111111111"
    requirements = tuple(
        RequirementEvidence(
            requirement_id=requirement_id,  # type: ignore[arg-type]
            assertion_evidence_sha256={name: str(index) * 64 for name in assertion_names[requirement_id]},
            operation_ids=(
                (shared_request, f"worker-{requirement_id}")
                if requirement_id in {"FM18", "FM19"}
                else (f"operation-{requirement_id}",)
            ),
        )
        for index, requirement_id in enumerate(("FM16", "FM17", "FM18", "FM19", "FM21"), start=1)
    )
    evidence = DataWorkloadEvidenceBundle(
        matrix_id=request.matrix_id,
        source_commit=request.commit_sha,
        requirements=requirements,  # type: ignore[arg-type]
    )
    state = DataWorkloadState.initial(plan).model_copy(
        update={"phase": DataPhase.EVIDENCE_READY, "mutations_started": 6}
    )
    receipt = ProductionDataWorkloadReceipt(
        matrix_id=request.matrix_id,
        outcome="EVIDENCE_READY",
        state_sha256=hashlib.sha256(
            canonical_json_bytes(state.model_dump(mode="json"))
        ).hexdigest(),
        evidence=evidence,
    )
    return plan, state, receipt


def _set_data_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(
        "MY_DATA_HUB_OPERATIONAL_EVIDENCE_DRIVER_JSON",
        json.dumps(
            {
                "schema_version": "my-data-hub-operational-kaggle-evidence-driver.v1",
                "provider_owner": "evidence-owner",
                "data_workload": {
                    "plan_path": "/owner/plan.json",
                    "production_config_path": "/owner/production.json",
                    "state_path": "/state/data.json",
                },
            }
        ),
    )


def test_data_workload_preflight_persists_exact_mode_0600_matrix_claim_before_action(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    request = _request(16)
    plan = DataWorkloadPlan(
        matrix_id=request.matrix_id,
        source_commit=request.commit_sha,
        blogger_project_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        blogger_snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        blogger_source_revision="b" * 40,
        embedding_probe_query_sha256=hashlib.sha256(b"probe").hexdigest(),
    )
    production = driver_module.ProductionDataWorkloadConfig(
        control_base_url="http://127.0.0.1:8080",
        mcp_endpoint="https://mcp.example.test/mcp",
        blogger_v1_operation_id=UUID("11111111-1111-4111-8111-111111111111"),
        blogger_v2_operation_id=UUID("22222222-2222-4222-8222-222222222222"),
        probe_query="probe",
    )
    plan_path = tmp_path / "plan.json"
    production_path = tmp_path / "production.json"
    state_path = tmp_path / "state" / "data.json"
    plan_path.write_text(plan.model_dump_json())
    production_path.write_text(production.model_dump_json())
    monkeypatch.setenv("MY_DATA_HUB_DATA_MCP_READER_TOKEN", "r" * 24)
    monkeypatch.setenv("MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN", "o" * 24)
    config = driver_module.DataWorkloadDriverConfig(
        plan_path=str(plan_path),
        production_config_path=str(production_path),
        state_path=str(state_path),
    )

    prepared = driver_module._prepare_data_workload(request, config)

    assert prepared.store.load(plan).phase is DataPhase.INITIAL
    assert state_path.stat().st_mode & 0o777 == 0o600
    assert state_path.read_bytes()

    missing_state = config.model_copy(update={"state_path": str(tmp_path / "missing-state.json")})
    with pytest.raises(driver_module.MissingPreActionEvidence):
        driver_module._prepare_data_workload(
            request.model_copy(update={"resume_only": True}), missing_state
        )


@pytest.mark.parametrize("ordinal", [16, 17, 18, 19, 21])
def test_data_workload_live_terminal_launches_distinct_reconciled_evidence_notebook(
    ordinal: int, monkeypatch: pytest.MonkeyPatch
) -> None:
    request = _request(ordinal)
    plan, state, receipt = _data_evidence(request)
    prepared = SimpleNamespace(plan=plan, store=_FixedDataStateStore(state))
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_data_config(monkeypatch)
    monkeypatch.setattr(driver_module, "_prepare_data_workload", lambda *_args: prepared)
    monkeypatch.setattr(driver_module, "_invoke_data_workload_entrypoint", lambda _prepared: receipt)
    selected = next(
        item for item in receipt.evidence.requirements if item.requirement_id == request.requirement_id  # type: ignore[union-attr]
    )
    monkeypatch.setattr(
        driver_module,
        "_data_requirement_proof",
        lambda *_args: (
            {name: {"production_evidence_sha256": value} for name, value in selected.assertion_evidence_sha256.items()},
            selected.operation_ids,
            receipt.evidence.bundle_sha256,  # type: ignore[union-attr]
        ),
    )
    gateway = EvidenceLifecycleGateway()

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "READY"
    assert result.cleanup_state == "PENDING"
    assert result.mutations_started == 7
    assert result.provider_run_ref is not None
    lifecycle = [
        arguments
        for _profile, tool, arguments in gateway.calls
        if tool == "provider.acceptance.notebook.lifecycle"
    ]
    assert len(lifecycle) == 1
    assert lifecycle[0]["scenario_id"] == request.requirement_id
    assert lifecycle[0]["task_run_id"] == str(request.task_run_id)


def test_data_bundle_cannot_pass_without_exact_durable_operation_and_fixture_state() -> None:
    request = _request(21)
    _plan, state, receipt = _data_evidence(request)
    with pytest.raises(ValueError, match="durable production state"):
        driver_module._data_requirement_proof(request, receipt, state)


def test_fm16_owner_authorization_pause_is_reconciled_blocked_zero_and_resumable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(16)
    plan, state, _receipt = _data_evidence(request)
    state = state.model_copy(
        update={
            "phase": DataPhase.AWAITING_OWNER_AUTHORIZATION,
            "mutations_started": 1,
            "quarantine": BloggerQuarantineEvidence(
                request_id=UUID("11111111-1111-4111-8111-111111111111"),
                request_sha256="1" * 64,
                operation_id=UUID("22222222-2222-4222-8222-222222222222"),
                export_batch_id=UUID("33333333-3333-4333-8333-333333333333"),
                failure_code="BloggerMigrationQuarantined",
                quarantined_count=2,
                logical_sha256="2" * 64,
                record_id_set_sha256="3" * 64,
                canonical_outcome_sha256="4" * 64,
                duplicate_group_count=1,
                duplicate_groups_pending=1,
            ),
            "duplicate_review": DuplicateReviewEvidence(
                export_batch_id=UUID("33333333-3333-4333-8333-333333333333"),
                source_request_id=UUID("11111111-1111-4111-8111-111111111111"),
                source_operation_id=UUID("22222222-2222-4222-8222-222222222222"),
                source_request_sha256="1" * 64,
                duplicate_group_count=1,
                duplicate_groups_pending=1,
                identity_set_sha256="5" * 64,
                member_record_id_set_sha256="6" * 64,
                review_projection_sha256="7" * 64,
            ),
        }
    )
    receipt = ProductionDataWorkloadReceipt(
        matrix_id=request.matrix_id,
        outcome="AWAITING_OWNER_AUTHORIZATION",
        state_sha256=hashlib.sha256(canonical_json_bytes(state.model_dump(mode="json"))).hexdigest(),
    )
    prepared = SimpleNamespace(plan=plan, store=_FixedDataStateStore(state))
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_data_config(monkeypatch)
    monkeypatch.setattr(driver_module, "_prepare_data_workload", lambda *_args: prepared)
    monkeypatch.setattr(driver_module, "_invoke_data_workload_entrypoint", lambda _prepared: receipt)
    gateway = EvidenceLifecycleGateway()

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "BLOCKED"
    assert result.blocker_code == "FM16_AWAITING_OWNER_AUTHORIZATION"
    assert result.mutations_started == 0
    assert not any(tool == "provider.acceptance.notebook.lifecycle" for _p, tool, _a in gateway.calls)


def test_data_workload_post_mutation_blocker_is_fail_not_blocked(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(17)
    plan, state, _receipt = _data_evidence(request)
    state = state.model_copy(update={"phase": DataPhase.FM17_RESTORE_AMBIGUOUS, "mutations_started": 3})
    receipt = ProductionDataWorkloadReceipt(
        matrix_id=request.matrix_id,
        outcome="BLOCKED",
        state_sha256=hashlib.sha256(canonical_json_bytes(state.model_dump(mode="json"))).hexdigest(),
        blocker_code="FM17_ROTATION_RESPONSE_AMBIGUOUS",
    )
    prepared = SimpleNamespace(plan=plan, store=_FixedDataStateStore(state))
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_data_config(monkeypatch)
    monkeypatch.setattr(driver_module, "_prepare_data_workload", lambda *_args: prepared)
    monkeypatch.setattr(driver_module, "_invoke_data_workload_entrypoint", lambda _prepared: receipt)

    result = asyncio.run(execute(request, EvidenceLifecycleGateway()))

    assert result.outcome == "FAIL"
    assert result.mutations_started == 3


def test_cleanup_phase_requires_exact_outer_binding_and_only_then_passes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_evidence_config(monkeypatch)
    gateway = EvidenceLifecycleGateway()
    request = _request(2)
    ready = asyncio.run(execute(request, gateway))
    assert ready.outcome == "READY"
    cleanup = {
        "claim_task_id": str(ready.claim_task_id),
        "claim_sha256": ready.claim_sha256,
        "provider_ref": ready.provider_ref,
        "provider_run_ref": ready.provider_run_ref,
        "provider_kernel_id": ready.provider_kernel_id,
        "source_version": ready.source_version,
        "source_sha256": ready.source_sha256,
        "output_receipt_sha256": ready.output_receipt_sha256,
        "output_file_sha256": ready.output_file_sha256,
        "output_tree_sha256": ready.output_tree_sha256,
    }
    cleanup_request = request.model_copy(
        update={
            "phase": "CLEANUP",
            "resume_only": True,
            "cleanup": CleanupBinding.model_validate(cleanup),
        }
    )
    cleanup_request = DriverRequest.model_validate(cleanup_request.model_dump(mode="json"))

    result = asyncio.run(execute(cleanup_request, gateway))

    assert result.outcome == "PASS"
    assert result.phase == "CLEANUP"
    assert result.cleanup_state == "COMPLETE"
    assert any(tool == "provider.acceptance.claim.cleanup" for _profile, tool, _args in gateway.calls)


def test_ambiguous_lifecycle_response_is_fail_never_blocked_zero(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_evidence_config(monkeypatch)

    class AmbiguousGateway(EvidenceLifecycleGateway):
        async def call(self, profile: str, tool: str, arguments: dict[str, Any]) -> dict[str, Any]:
            if tool == "provider.acceptance.notebook.lifecycle":
                self.calls.append((profile, tool, dict(arguments)))
                raise ConnectionError("lost after request")
            return await super().call(profile, tool, arguments)

    result = asyncio.run(execute(_request(2), AmbiguousGateway()))

    assert result.outcome == "FAIL"
    assert result.mutations_started == 1
    assert result.blocker_code is None


def test_restore_runs_only_after_exact_provider_evidence_claim_and_durable_poll(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(6)
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_evidence_config(monkeypatch)
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

    assert result.outcome == "READY"
    assert result.mutations_started == 2
    assert result.provider_run_ref and "mdh-fm06-no-" in result.provider_run_ref
    calls = [tool for _profile, tool, _arguments in gateway.calls]
    assert calls.index("provider.resources.read") < calls.index("checkpoint.restore.request")
    assert "operation.get" in calls


def test_contradictory_action_acceptance_is_fail_not_zero_mutation_blocker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    _set_evidence_config(monkeypatch)
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
    _set_evidence_config(monkeypatch)
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
    _set_evidence_config(monkeypatch)
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

    assert result.outcome == "READY"
    assert result.mutations_started == 2
    operation_calls = [arguments for _profile, tool, arguments in gateway.calls if tool == "operation.get"]
    assert operation_calls and all(arguments["operation_id"] == operation_id for arguments in operation_calls)
    assert "checkpoint.restore.request" not in {tool for _profile, tool, _arguments in gateway.calls}


def test_fm20_requires_signed_host_and_notebook_evidence_before_ensure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(20)
    _bundle, claims = _fm20_environment(request)
    claims_document = json.loads(claims)
    claims_document.pop("fm20_evidence")
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON", json.dumps(claims_document))
    gateway = FM20Gateway()

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "BLOCKED"
    assert result.mutations_started == 0
    assert result.blocker_code == "FM20_SIGNED_HOST_OR_EVIDENCE_NOTEBOOK_MISSING"
    assert "master.ensure" not in {tool for _profile, tool, _arguments in gateway.calls}


def test_fm20_verifies_reboot_then_ensures_and_binds_bounded_search(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(20)
    _bundle, claims = _fm20_environment(request)
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON", claims)
    gateway = FM20Gateway()

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert result.mutations_started == 1
    assert result.provider_run_ref == "owner/fm20-evidence/7"
    tools = [tool for _profile, tool, _arguments in gateway.calls]
    assert tools.index("provider.resources.read") < tools.index("master.status")
    assert tools.index("master.status") < tools.index("master.ensure")
    assert tools.index("master.ensure") < tools.index("bloggers.search")
    search_arguments = next(arguments for _profile, tool, arguments in gateway.calls if tool == "bloggers.search")
    assert search_arguments == {"query": "Калининград", "cursor": None, "limit": 1}


def test_fm20_ambiguous_ensure_or_invalid_search_fails_after_mutation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(20)
    _bundle, claims = _fm20_environment(request)
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON", claims)
    ambiguous = FM20Gateway(
        ensure_response={
            "operation_id": None,
            "master_state": "REQUESTED",
            "duplicate": False,
            "intent": "explicit-mcp-request",
            "terminal": False,
        }
    )

    result = asyncio.run(execute(request, ambiguous))

    assert result.outcome == "FAIL"
    assert result.mutations_started == 1

    empty = FM20Gateway(empty_search=True)
    result = asyncio.run(execute(request, empty))
    assert result.outcome == "FAIL"
    assert result.mutations_started == 1


def test_fm20_resume_reconciles_claimed_ensure_without_creating_another(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request(20).model_copy(update={"resume_only": True})
    _bundle, claims_raw = _fm20_environment(request)
    claims = json.loads(claims_raw)
    claims["claims"]["FM20"]["operation_id"] = "11111111-1111-4111-8111-111111111111"
    monkeypatch.setenv("KAGGLE_API_TOKEN", "configured-test-token")
    monkeypatch.setenv("MY_DATA_HUB_OPERATIONAL_EVIDENCE_CLAIMS_JSON", json.dumps(claims))
    gateway = FM20Gateway()

    result = asyncio.run(execute(request, gateway))

    assert result.outcome == "PASS"
    assert result.mutations_started == 1
    tools = [tool for _profile, tool, _arguments in gateway.calls]
    assert "operation.get" in tools
    assert "master.ensure" not in tools


def test_fm20_evidence_bundle_schema_and_runtime_validate() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "schemas/provider/operational-kaggle-fm20-evidence.v1.schema.json").read_text()
    )
    example = json.loads(
        (root / "examples/provider/operational-kaggle-fm20-evidence.v1.example.json").read_text()
    )
    deployment_schema = json.loads((root / "schemas/deployment-evidence.v2.schema.json").read_text())
    registry = Registry().with_resource(
        deployment_schema["$id"], Resource.from_contents(deployment_schema)
    )
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker(), registry=registry).validate(example)
    from scripts.provider.operational_kaggle_driver import FM20EvidenceBundle

    FM20EvidenceBundle.model_validate(example)


def test_data_workload_driver_config_schema_example_and_runtime_validate() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads(
        (root / "schemas/provider/operational-kaggle-evidence-driver.v1.schema.json").read_text()
    )
    example = json.loads(
        (root / "examples/provider/operational-kaggle-evidence-driver.v1.example.json").read_text()
    )
    Draft202012Validator.check_schema(schema)
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)
    driver_module.EvidenceDriverConfig.model_validate(example)


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

    claims_example = json.loads(
        (root / "examples/provider/operational-kaggle-evidence-claims.v1.example.json").read_text()
    )
    fm20_schema = json.loads(
        (root / "schemas/provider/operational-kaggle-fm20-evidence.v1.schema.json").read_text()
    )
    deployment_schema = json.loads((root / "schemas/deployment-evidence.v2.schema.json").read_text())
    registry = (
        Registry()
        .with_resource(fm20_schema["$id"], Resource.from_contents(fm20_schema))
        .with_resource(deployment_schema["$id"], Resource.from_contents(deployment_schema))
    )
    Draft202012Validator(schema, format_checker=FormatChecker(), registry=registry).validate(claims_example)
    EvidenceClaimsDocument.model_validate(claims_example)


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
