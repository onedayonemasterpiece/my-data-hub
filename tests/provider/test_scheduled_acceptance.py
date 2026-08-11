from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.provider import scheduled_acceptance as acceptance

NOW = datetime(2026, 8, 12, 12, tzinfo=UTC)
COMMIT = "a" * 40
READ_ONLY_TOOLS = acceptance.READ_ONLY_TOOLS


class SnakeCaseMCPResult:
    def __init__(self, value: dict[str, object], *, is_error: bool = False) -> None:
        self.structured_content = value
        self.content: list[object] = []
        self.is_error = is_error


class SnakeCaseMCPSession:
    def __init__(self, *, error_tool: str | None = None) -> None:
        self.error_tool = error_tool
        self.initialized = False

    async def initialize(self) -> None:
        self.initialized = True

    async def list_tools(self) -> object:
        assert self.initialized
        return SimpleNamespace(tools=[SimpleNamespace(name=name) for name in READ_ONLY_TOOLS])

    async def call_tool(self, name: str, arguments: dict[str, object]) -> object:
        assert arguments == ({"limit": 100} if name == "provider.resources.status" else {})
        return SnakeCaseMCPResult(
            {"tool": name},
            is_error=name == self.error_tool,
        )


@pytest.mark.asyncio
async def test_live_mcp_collection_accepts_pinned_sdk_snake_case_result_fields() -> None:
    values = await acceptance._collect_mcp_session(SnakeCaseMCPSession())

    assert values["platform"] == {"tool": "platform.status"}
    assert values["provider"] == {"tool": "provider.resources.status"}
    assert values["tools"] == READ_ONLY_TOOLS


@pytest.mark.asyncio
async def test_live_mcp_collection_rejects_pinned_sdk_snake_case_error() -> None:
    with pytest.raises(RuntimeError, match=r"checkpoint\.status returned an error"):
        await acceptance._collect_mcp_session(
            SnakeCaseMCPSession(error_tool="checkpoint.status")
        )


def complete_observations() -> acceptance.Observations:
    return acceptance.Observations(
        live_resources=[
            {"provider_ref": "owner/checkpoints", "kind": "dataset", "private": True},
            {"provider_ref": "owner/master", "kind": "notebook", "private": True},
        ],
        registered_resources=[
            {
                "resource_ref": "owner/checkpoints",
                "control_class": "orchestrator_protected",
                "private": True,
                "observed_at": NOW.isoformat(),
            },
            {
                "resource_ref": "owner/master",
                "control_class": "orchestrator_protected",
                "private": True,
                "observed_at": NOW.isoformat(),
            },
        ],
        platform_status={"deployed_commit": COMMIT},
        master_status={
            "master_state": "ACTIVE",
            "master_epoch": 7,
            "lease_expires_at": (NOW + timedelta(hours=1)).isoformat(),
        },
        checkpoint_status={
            "current_checkpoint_id": "current-id",
            "previous_checkpoint_id": "previous-id",
            "current_exact_version_ref": "owner/checkpoints/7",
            "previous_exact_version_ref": "owner/checkpoints/6",
            "verified_at": (NOW - timedelta(hours=1)).isoformat(),
        },
        embedding_status={"e5": {"coverage": 1.0}, "bge_m3": {"coverage": 1.0}},
        mcp_tools=set(READ_ONLY_TOOLS),
        unauthenticated_http_status=401,
        invalid_token_http_status=401,
        deployed_commit_matches=True,
        lifecycle_receipts={
            "dataset": {
                "privacy": "private",
                "cleanup_outcome": "complete",
                "gate_results": [{"name": "cleanup", "outcome": "PASS"}],
            },
            "notebook": {
                "privacy": "private",
                "terminal_state": "complete",
                "cleanup": "private_notebook_deleted",
            },
        },
    )


def by_name(checks: list[acceptance.Check]) -> dict[str, acceptance.Check]:
    return {check.name: check for check in checks}


def test_nightly_runs_observable_gates_and_blocks_only_missing_runtime_interfaces() -> None:
    checks = acceptance.evaluate(
        acceptance.Mode.NIGHTLY,
        complete_observations(),
        now=NOW,
        freshness=timedelta(hours=26),
    )
    values = by_name(checks)

    expected_passes = {
        "deployed_commit",
        "live_provider_inventory",
        "provider_public_scan",
        "provider_orphan_scan",
        "provider_registry_freshness",
        "master_active_epoch",
        "checkpoint_current_previous",
        "checkpoint_freshness",
        "embedding_coverage",
        "mcp_reader_catalog",
        "mcp_unauthenticated_denial",
        "mcp_invalid_token_denial",
    }
    assert all(values[name].outcome is acceptance.Outcome.PASS for name in expected_passes)
    assert values["connector_coverage"].blocker_code == "CONNECTOR_COVERAGE_API_MISSING"
    assert values["bounded_cold_restore_request"].blocker_code == "COLD_RESTORE_REQUEST_API_MISSING"
    assert values["stale_epoch_rejection"].blocker_code == "STALE_EPOCH_PROBE_API_MISSING"
    receipt = acceptance.build_receipt(
        mode=acceptance.Mode.NIGHTLY,
        checks=checks,
        commit_sha=COMMIT,
        started_at=NOW,
        completed_at=NOW,
        workflow_run_id="123",
    )
    assert receipt["outcome"] == "BLOCKED"
    assert acceptance._exit_code(receipt) == acceptance.EXTERNAL_BLOCKED


def test_weekly_adds_rotation_previous_restore_denial_and_real_lifecycle_evidence() -> None:
    checks = acceptance.evaluate(
        acceptance.Mode.WEEKLY,
        complete_observations(),
        now=NOW,
        freshness=timedelta(hours=26),
    )
    values = by_name(checks)

    assert values["forced_master_rotation"].blocker_code == "FORCED_ROTATION_API_MISSING"
    assert values["previous_checkpoint_restore"].blocker_code == "PREVIOUS_CHECKPOINT_RESTORE_API_MISSING"
    assert (
        values["protected_resource_mutation_denial"].blocker_code
        == "PROTECTED_RESOURCE_DENIAL_PROBE_API_MISSING"
    )
    assert values["mcp_managed_dataset_lifecycle_cleanup"].outcome is acceptance.Outcome.PASS
    assert values["mcp_managed_notebook_lifecycle_cleanup"].outcome is acceptance.Outcome.PASS


def test_failures_outrank_blockers_without_emitting_resource_or_business_rows() -> None:
    observations = complete_observations()
    observations.live_resources = [
        {"provider_ref": "owner/other", "kind": "dataset", "private": False},
        {"provider_ref": "owner/unregistered", "kind": "dataset", "private": False},
    ]
    observations.registered_resources = [
        {
            "resource_ref": "owner/other",
            "observed_at": (NOW - timedelta(days=3)).isoformat(),
        }
    ]
    observations.master_status = {
        "master_state": "ACTIVE",
        "master_epoch": 7,
        "lease_expires_at": (NOW - timedelta(minutes=1)).isoformat(),
    }
    observations.checkpoint_status = {
        "current_checkpoint_id": "only-current",
        "previous_checkpoint_id": None,
    }
    observations.embedding_status = {"e5": {"coverage": 0.5}, "bge_m3": {"coverage": 0.0}}
    observations.mcp_tools = {*READ_ONLY_TOOLS, "provider.resources.delete"}
    observations.unauthenticated_http_status = 200

    checks = acceptance.evaluate(
        acceptance.Mode.NIGHTLY,
        observations,
        now=NOW,
        freshness=timedelta(hours=26),
    )
    receipt = acceptance.build_receipt(
        mode=acceptance.Mode.NIGHTLY,
        checks=checks,
        commit_sha=COMMIT,
        started_at=NOW,
        completed_at=NOW,
        workflow_run_id="124",
    )
    encoded = json.dumps(receipt, sort_keys=True)

    assert receipt["outcome"] == "FAIL"
    assert acceptance._exit_code(receipt) == acceptance.FAIL
    assert "owner/unregistered" not in encoded
    assert '"rows":' not in encoded
    assert by_name(checks)["provider_public_scan"].outcome is acceptance.Outcome.FAIL


def test_unknown_account_resources_remain_external_read_only_not_system_orphans() -> None:
    observations = complete_observations()
    observations.live_resources = [
        {"provider_ref": "owner/managed", "kind": "dataset", "private": True},
        {"provider_ref": "owner/unrelated-public", "kind": "dataset", "private": False},
    ]
    observations.registered_resources = [
        {
            "resource_ref": "owner/managed",
            "observed_at": NOW.isoformat(),
        }
    ]

    checks = acceptance._resource_checks(
        observations,
        now=NOW,
        freshness=timedelta(hours=26),
    )

    assert by_name(checks)["provider_public_scan"].outcome is acceptance.Outcome.PASS
    orphan = by_name(checks)["provider_orphan_scan"]
    assert orphan.outcome is acceptance.Outcome.PASS
    assert orphan.observed["external_read_only_count"] == 1


def test_existing_checkpoint_status_blocks_when_exact_refs_and_freshness_interface_are_absent() -> None:
    observations = complete_observations()
    observations.checkpoint_status = {
        "current_checkpoint_id": "current-id",
        "previous_checkpoint_id": "previous-id",
        "freshness": "recorded",
    }
    values = by_name(
        acceptance.evaluate(
            acceptance.Mode.NIGHTLY,
            observations,
            now=NOW,
            freshness=timedelta(hours=26),
        )
    )
    assert values["checkpoint_current_previous"].blocker_code == "CHECKPOINT_EXACT_REF_API_MISSING"
    assert values["checkpoint_freshness"].blocker_code == "CHECKPOINT_VERIFIED_AT_API_MISSING"


def test_absent_master_is_healthy_and_does_not_invent_a_stale_epoch() -> None:
    observations = complete_observations()
    observations.master_status = {
        "master_state": "ABSENT",
        "master_epoch": None,
        "lease_expires_at": None,
    }

    check = acceptance._master_checks(observations, now=NOW)[0]

    assert check.outcome is acceptance.Outcome.PASS
    assert check.observed == {"state": "ABSENT", "stale_active_epoch": False}


def test_bad_lifecycle_cleanup_is_a_failure_not_a_success_claim() -> None:
    observations = complete_observations()
    observations.lifecycle_receipts["dataset"] = {
        "privacy": "private",
        "cleanup_outcome": "incomplete",
        "gate_results": [{"name": "cleanup", "outcome": "FAIL"}],
    }
    values = by_name(
        acceptance.evaluate(
            acceptance.Mode.MANUAL,
            observations,
            now=NOW,
            freshness=timedelta(hours=26),
        )
    )
    assert values["mcp_managed_dataset_lifecycle_cleanup"].outcome is acceptance.Outcome.FAIL


def test_receipt_sanitizer_rejects_secret_and_business_row_shapes() -> None:
    with pytest.raises(ValueError, match="forbidden key"):
        acceptance._assert_sanitized({"checks": [{"observed": {"rows": [{"name": "person"}]}}]})
    with pytest.raises(ValueError, match="credential-shaped"):
        acceptance._assert_sanitized({"detail": "Bearer should-never-appear"})


def test_cli_writes_blocked_receipt_when_live_credentials_are_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "scheduled.json"
    observations = acceptance.Observations(
        blockers={
            "kaggle_inventory": ("KAGGLE_MODERN_API_TOKEN_REQUIRED", "KAGGLE_API_TOKEN"),
            "mcp": ("MCP_SCHEDULED_CREDENTIAL_OR_ENDPOINT_MISSING", "exact HTTPS /mcp endpoint"),
        }
    )
    monkeypatch.setattr(acceptance, "collect_live", lambda **_kwargs: observations)
    monkeypatch.setenv("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT", COMMIT)
    monkeypatch.setattr(
        sys,
        "argv",
        ["scheduled_acceptance.py", "run", "--mode", "nightly", "--receipt", str(output)],
    )

    assert acceptance.main() == acceptance.EXTERNAL_BLOCKED
    receipt = json.loads(output.read_bytes())
    assert receipt["schema_version"] == acceptance.RECEIPT_SCHEMA
    assert receipt["outcome"] == "BLOCKED"
    assert receipt["blockers"]


def test_workflows_execute_scheduled_runner_and_upload_its_receipts() -> None:
    root = Path(__file__).resolve().parents[2]
    nightly = (root / ".github/workflows/nightly.yml").read_text()
    provider = (root / ".github/workflows/provider-real.yml").read_text()

    assert "scheduled_acceptance.py run" in nightly
    assert "--mode nightly" in nightly
    assert "scheduled-nightly.json" in nightly
    assert "scheduled_acceptance.py run" in provider
    assert "--dataset-lifecycle-receipt artifacts/dataset-canary.json" in provider
    assert "--notebook-lifecycle-receipt artifacts/notebook-canary.json" in provider
    assert "scheduled-provider-real.json" in provider
