from __future__ import annotations

import json
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace

import jsonschema
import pytest

from my_data_hub.auth.oauth_credentials import RotatingOAuthBearerSource
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


class TerminalOperatorSession:
    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, object]]] = []
        self.initialized = False

    async def initialize(self) -> None:
        self.initialized = True

    async def call_tool(self, name: str, arguments: dict[str, object]) -> object:
        assert self.initialized
        self.calls.append((name, arguments))
        if name == "connector.coverage":
            return SnakeCaseMCPResult({"available": True})
        if name == "runtime.stale_epoch.probe":
            return SnakeCaseMCPResult({"evaluated": True, "denied": True, "mutation_attempted": False})
        if name == "provider.protected_resource.probe":
            return SnakeCaseMCPResult({"evaluated": True, "denied": True, "mutation_attempted": False})
        if name in {"checkpoint.restore.request", "master.rotation.request"}:
            key = str(arguments["idempotency_key"])
            return SnakeCaseMCPResult(
                {
                    "accepted": True,
                    "duplicate": False,
                    "execution_supported": True,
                    "operation_id": f"operation:{key}",
                    "state": "REQUESTED",
                    "checkpoint_id": arguments["checkpoint_id"],
                    "exact_version_ref": arguments["exact_version_ref"],
                }
            )
        if name == "operation.get":
            return SnakeCaseMCPResult(
                {
                    "found": True,
                    "operation_id": arguments["operation_id"],
                    "state": "DURABLE_COMPLETE",
                }
            )
        raise AssertionError(name)


@pytest.mark.asyncio
async def test_live_mcp_collection_accepts_pinned_sdk_snake_case_result_fields() -> None:
    values = await acceptance._collect_mcp_session(SnakeCaseMCPSession())

    assert values["platform"] == {"tool": "platform.status"}
    assert values["provider"] == {"tool": "provider.resources.status"}
    assert values["tools"] == READ_ONLY_TOOLS


@pytest.mark.asyncio
async def test_live_mcp_collection_rejects_pinned_sdk_snake_case_error() -> None:
    with pytest.raises(RuntimeError, match=r"checkpoint\.status returned an error"):
        await acceptance._collect_mcp_session(SnakeCaseMCPSession(error_tool="checkpoint.status"))


@pytest.mark.asyncio
async def test_http_auth_rotates_refresh_family_after_simulated_300_seconds(
    tmp_path: Path,
) -> None:
    import httpx2

    clock = [1_000.0]
    credential = tmp_path / "oauth.json"
    credential.write_text(
        json.dumps(
            {
                "schema_version": "my-data-hub-mcp-oauth-credentials.v1",
                "token_endpoint": "https://identity.kenigevents.ru/token",
                "resource": "https://mcp-datahub.kenigevents.ru/mcp",
                "profiles": {
                    "reader": {
                        "client_id": "acceptance-reader",
                        "refresh_token": "refresh-initial-" + "r" * 32,
                        "access_token": None,
                        "access_expires_at": None,
                    }
                },
            }
        ),
        encoding="utf-8",
    )
    credential.chmod(0o600)
    calls = 0

    def exchange(_endpoint: str, _parameters: dict[str, str]) -> dict[str, object]:
        nonlocal calls
        calls += 1
        return {
            "access_token": f"access-{calls}-" + "a" * 32,
            "refresh_token": f"refresh-{calls}-" + "b" * 32,
            "token_type": "Bearer",
            "expires_in": 300,
        }

    source = RotatingOAuthBearerSource(credential, now=lambda: clock[0], exchange=exchange)
    auth = acceptance._bearer_auth(httpx2, source, "reader")

    first = httpx2.Request("POST", "https://mcp-datahub.kenigevents.ru/mcp")
    first_flow = auth.async_auth_flow(first)
    await anext(first_flow)
    await first_flow.aclose()
    clock[0] = 1_301.0
    second = httpx2.Request("POST", "https://mcp-datahub.kenigevents.ru/mcp")
    second_flow = auth.async_auth_flow(second)
    await anext(second_flow)
    await second_flow.aclose()

    assert calls == 2
    assert first.headers["Authorization"] != second.headers["Authorization"]
    assert json.loads(credential.read_text())["profiles"]["reader"]["refresh_token"].startswith(
        "refresh-2-"
    )


@pytest.mark.asyncio
async def test_operator_collection_sequences_and_awaits_each_durable_action() -> None:
    session = TerminalOperatorSession()
    snapshot = {
        "master": {"master_epoch": 8},
        "checkpoint": {
            "current_checkpoint_id": "cp-current",
            "current_exact_version_ref": "owner/checkpoints/8",
            "previous_checkpoint_id": "cp-previous",
            "previous_exact_version_ref": "owner/checkpoints/7",
            "current": {
                "source_state": "STOPPED",
                "source_epoch": 7,
                "canonical_revision": 11,
            },
        },
        "provider": {"resources": [{"resource_ref": "owner/protected", "control_class": "orchestrator_protected"}]},
    }

    values = await acceptance._collect_operator_session(
        session,
        mode=acceptance.Mode.WEEKLY,
        snapshot=snapshot,
        workflow_run_id="workflow-123",
    )

    assert values["current_restore"]["state"] == "DURABLE_COMPLETE"
    assert values["previous_restore"]["state"] == "DURABLE_COMPLETE"
    assert values["rotation"]["state"] == "DURABLE_COMPLETE"
    action_calls = [
        name
        for name, _arguments in session.calls
        if name in {"checkpoint.restore.request", "master.rotation.request", "operation.get"}
    ]
    assert action_calls == [
        "checkpoint.restore.request",
        "operation.get",
        "checkpoint.restore.request",
        "operation.get",
        "master.rotation.request",
        "operation.get",
    ]


@pytest.mark.asyncio
async def test_operator_collection_performs_final_status_read_at_deadline(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class DeadlineSession(TerminalOperatorSession):
        def __init__(self) -> None:
            super().__init__()
            self.status_reads = 0

        async def call_tool(self, name: str, arguments: dict[str, object]) -> object:
            if name != "operation.get":
                return await super().call_tool(name, arguments)
            self.calls.append((name, arguments))
            self.status_reads += 1
            return SnakeCaseMCPResult(
                {
                    "found": True,
                    "operation_id": arguments["operation_id"],
                    "state": "REQUESTED" if self.status_reads == 1 else "DURABLE_COMPLETE",
                }
            )

    observed = iter([0.0, 0.0, 1201.0, 1201.0])

    class Loop:
        @staticmethod
        def time() -> float:
            return next(observed)

    async def no_sleep(_seconds: float) -> None:
        return None

    monkeypatch.setattr(acceptance.asyncio, "get_running_loop", lambda: Loop())
    monkeypatch.setattr(acceptance.asyncio, "sleep", no_sleep)
    session = DeadlineSession()
    values = await acceptance._collect_operator_session(
        session,
        mode=acceptance.Mode.NIGHTLY,
        snapshot={
            "master": {"master_epoch": 1},
            "checkpoint": {
                "current_checkpoint_id": "cp-current",
                "current_exact_version_ref": "owner/checkpoints/8",
            },
            "provider": {"resources": []},
        },
        workflow_run_id="workflow-deadline",
    )

    assert session.status_reads == 2
    assert values["current_restore"]["state"] == "DURABLE_COMPLETE"


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
        connector_status={
            "available": True,
            "bounded": True,
            "connector_count": 2,
            "complete_count": 2,
            "oldest_observed_at": NOW.isoformat(),
        },
        stale_epoch_probe={"evaluated": True, "denied": True, "mutation_attempted": False},
        protected_resource_probe={
            "evaluated": True,
            "denied": True,
            "mutation_attempted": False,
        },
        mcp_tools=set(READ_ONLY_TOOLS),
        unauthenticated_http_status=401,
        invalid_token_http_status=401,
        deployed_commit_matches=True,
        deployed_commit_sha=COMMIT,
        action_requests={
            key: {
                "accepted": True,
                "state": "REQUESTED",
                "execution_supported": False,
                "blocker_code": "ISOLATED_RESTORE_OPERATION_CONSUMER_MISSING"
                if "restore" in key
                else "MASTER_ROTATION_OPERATION_CONSUMER_MISSING",
            }
            for key in ("current_restore", "previous_restore", "rotation")
        },
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
    assert values["connector_coverage"].outcome is acceptance.Outcome.PASS
    assert values["bounded_cold_restore_request"].blocker_code == "ISOLATED_RESTORE_OPERATION_CONSUMER_MISSING"
    assert values["stale_epoch_rejection"].outcome is acceptance.Outcome.PASS
    receipt = acceptance.build_receipt(
        mode=acceptance.Mode.NIGHTLY,
        checks=checks,
        source_commit_sha="b" * 40,
        deployed_commit_sha=COMMIT,
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

    assert values["forced_master_rotation"].blocker_code == "MASTER_ROTATION_OPERATION_CONSUMER_MISSING"
    assert values["previous_checkpoint_restore"].blocker_code == "ISOLATED_RESTORE_OPERATION_CONSUMER_MISSING"
    assert values["protected_resource_mutation_denial"].outcome is acceptance.Outcome.PASS
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
        source_commit_sha="b" * 40,
        deployed_commit_sha=COMMIT,
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


def test_registry_only_negative_probe_remains_blocked_without_real_admission_path() -> None:
    observations = complete_observations()
    observations.stale_epoch_probe = {
        "evaluated": False,
        "denied": False,
        "binding_valid": True,
        "mutation_attempted": False,
        "blocker_code": "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE",
    }

    check = acceptance._negative_policy_check(observations, kind="stale_epoch", name="stale_epoch_rejection")

    assert check.outcome is acceptance.Outcome.BLOCKED
    assert check.blocker_code == "STALE_EPOCH_ADMISSION_PATH_UNAVAILABLE"


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


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://mcp-datahub.kenigevents.ru/mcp",
        "https://evil.example/mcp",
        "https://mcp-datahub.kenigevents.ru:443/mcp",
        "https://user@mcp-datahub.kenigevents.ru/mcp",
        "https://mcp-datahub.kenigevents.ru/mcp?redirect=evil",
        "https://mcp-datahub.kenigevents.ru/mcp#fragment",
        "https://mcp-datahub.kenigevents.ru/other",
    ],
)
def test_scheduled_endpoint_rejects_bearer_exfiltration_shapes(endpoint: str) -> None:
    assert not acceptance._canonical_mcp_endpoint(endpoint)


def test_scheduled_endpoint_accepts_only_canonical_https_implicit_port() -> None:
    assert acceptance._canonical_mcp_endpoint("https://mcp-datahub.kenigevents.ru/mcp")


def test_receipt_distinguishes_source_from_deployed_commit() -> None:
    receipt = acceptance.build_receipt(
        mode=acceptance.Mode.NIGHTLY,
        checks=[],
        source_commit_sha="b" * 40,
        deployed_commit_sha=COMMIT,
        started_at=NOW,
        completed_at=NOW,
        workflow_run_id="125",
    )
    assert receipt["source_commit_sha"] == "b" * 40
    assert receipt["deployed_commit_sha"] == COMMIT
    assert "commit_sha" not in receipt


def test_cli_writes_blocked_receipt_when_live_credentials_are_unavailable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    output = tmp_path / "scheduled.json"
    observations = acceptance.Observations(
        blockers={
            "kaggle_inventory": (
                "KAGGLE_AUTOMATED_CREDENTIAL_REQUIRED",
                "one control-side SDK credential mode",
            ),
            "mcp": ("MCP_SCHEDULED_CREDENTIAL_OR_ENDPOINT_MISSING", "exact HTTPS /mcp endpoint"),
        }
    )
    monkeypatch.setattr(acceptance, "collect_live", lambda **_kwargs: observations)
    monkeypatch.setenv("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT", COMMIT)
    monkeypatch.setenv("GITHUB_SHA", "b" * 40)
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
    assert receipt["source_commit_sha"] == "b" * 40
    assert receipt["deployed_commit_sha"] == COMMIT


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
    assert "MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN" in nightly
    assert "MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN" not in provider
    assert "MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE:" not in provider
    assert "runs-on: [self-hosted, linux, my-data-hub-devstand]" in provider
    assert "MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN" in nightly
    assert "KAGGLE_API_TOKEN" not in nightly
    assert "KAGGLE_USERNAME" not in nightly
    assert "KAGGLE_KEY" not in nightly
    assert "MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT: ${{ vars.MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT }}" in nightly


def test_scheduled_receipt_schema_validates_sanitized_example() -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas/scheduled-acceptance-receipt.v1.schema.json").read_text())
    example = json.loads((root / "examples/contracts/scheduled-acceptance-receipt.v1.example.json").read_text())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(example)
