from __future__ import annotations

import copy
import hashlib
import json
import shutil
import subprocess
from pathlib import Path
from typing import Any

import yaml
from jsonschema import Draft202012Validator, FormatChecker

from scripts.validate_repository import (
    Report,
    validate_connector_intake_compose_service,
    validate_operational_mvp_receipt_semantics,
    validate_provider_real_workflow_auth_boundary,
)

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads((ROOT / "schemas/operational-mvp-acceptance-receipt.v1.schema.json").read_text())
MATRIX_ID = "11111111-1111-4111-8111-111111111111"
GATES = {
    "A": "donor_compatibility",
    "B": "control_ledger_and_orchestrator",
    "C": "notebook_runtime_sdk",
    "D": "kaggle_provider_adapter",
    "E": "deterministic_notebooks",
    "F": "postgresql_master",
    "G": "direct_data_plane",
    "H": "verified_checkpoints_and_recovery",
    "I": "remote_mcp_and_oauth",
    "J": "ydb_blogger_import",
    "K": "e5_and_bge_m3_embeddings",
    "L": "data_connector_plane",
    "M": "devstand_deployment",
    "N": "ci_scheduled_checks_and_receipts",
}


def schema_errors(receipt: dict[str, Any]) -> list[str]:
    return [
        error.message
        for error in Draft202012Validator(
            SCHEMA,
            format_checker=FormatChecker(),
        ).iter_errors(receipt)
    ]


def write_json(path: Path, value: object) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, sort_keys=True) + "\n", encoding="utf-8")
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(root: Path, *args: str) -> str:
    result = subprocess.run(["git", *args], cwd=root, text=True, capture_output=True, check=True)
    return result.stdout.strip()


def initialize_reviewed_merge(root: Path) -> tuple[str, str]:
    git(root, "init", "-b", "main")
    git(root, "config", "user.name", "Receipt Test")
    git(root, "config", "user.email", "receipt@example.invalid")
    (root / "seed.txt").write_text("seed\n", encoding="utf-8")
    git(root, "add", "seed.txt")
    git(root, "commit", "-m", "seed")
    git(root, "checkout", "-b", "reviewed")
    (root / "reviewed.txt").write_text("reviewed\n", encoding="utf-8")
    git(root, "add", "reviewed.txt")
    git(root, "commit", "-m", "reviewed head")
    reviewed = git(root, "rev-parse", "HEAD")
    git(root, "checkout", "main")
    (root / "integration.txt").write_text("integration\n", encoding="utf-8")
    git(root, "add", "integration.txt")
    git(root, "commit", "-m", "integration parent")
    git(root, "merge", "--no-ff", "reviewed", "-m", "merge reviewed head")
    return reviewed, git(root, "rev-parse", "HEAD")


def assertion(gate_id: str, requirements: list[str], ordinal: int) -> dict[str, Any]:
    return {
        "assertion_id": f"gate-{gate_id.lower()}-assertion-{ordinal}",
        "gate_id": gate_id,
        "requirement_ids": requirements,
        "outcome": "PASS",
        "evidence_sha256": f"{ordinal:x}" * 64,
    }


def semantic_evidence(
    evidence_class: str,
    commit: str,
    gate_ids: list[str],
    requirements: list[str],
    *,
    reviewed: str | None = None,
) -> dict[str, Any]:
    value: dict[str, Any] = {
        "schema_version": "my-data-hub-operational-mvp-evidence.v1",
        "evidence_class": evidence_class,
        "source_commit": commit,
        "outcome": "PASS",
        "live_evidence": True,
        "gate_ids": gate_ids,
        "requirement_ids": requirements,
        "assertions": [assertion(gate_id, requirements, index) for index, gate_id in enumerate(gate_ids, start=1)],
        "observed_at": "2026-08-11T01:00:00Z",
    }
    if evidence_class == "IMPLEMENTATION_REVIEW":
        assert reviewed is not None
        value.update(
            {
                "reviewed_head_commit": reviewed,
                "merge_commit": commit,
                "review_relationship": "PARENT",
                "pull_request": {
                    "repository": "owner/my-data-hub",
                    "number": 42,
                    "url": "https://github.com/owner/my-data-hub/pull/42",
                },
                "hosted_checks": [
                    {
                        "name": name,
                        "provider": "GITHUB_ACTIONS",
                        "runner": "ubuntu-latest",
                        "status": "COMPLETED",
                        "conclusion": "SUCCESS",
                        "head_commit": reviewed,
                        "run_url": f"https://github.com/owner/my-data-hub/actions/runs/{4200 + index}",
                    }
                    for index, name in enumerate(("contracts", "postgres-integration"), start=1)
                ],
            }
        )
    elif evidence_class == "DEPLOYMENT":
        value.update({"deployed_commit": commit, "deployment_tree_state": "CLEAN"})
    elif evidence_class == "POST_DEPLOY":
        value.update(
            {
                "deployed_commit": commit,
                "post_deploy_verified_commit": commit,
                "deployment_tree_state": "CLEAN",
                "hosted_checks": [
                    {
                        "name": "post-deploy",
                        "provider": "GITHUB_ACTIONS",
                        "runner": "ubuntu-latest",
                        "status": "COMPLETED",
                        "conclusion": "SUCCESS",
                        "head_commit": commit,
                        "run_url": "https://github.com/owner/my-data-hub/actions/runs/4300",
                    },
                    {
                        "name": "provider-real",
                        "provider": "GITHUB_ACTIONS",
                        "runner": ["self-hosted", "linux", "my-data-hub-devstand"],
                        "status": "COMPLETED",
                        "conclusion": "SUCCESS",
                        "head_commit": commit,
                        "run_url": "https://github.com/owner/my-data-hub/actions/runs/4301",
                    },
                ],
            }
        )
    return value


def build_complete_receipt(tmp_path: Path) -> dict[str, Any]:
    reviewed, commit = initialize_reviewed_merge(tmp_path)
    provider_schema_dir = tmp_path / "schemas/provider"
    provider_schema_dir.mkdir(parents=True)
    for name in (
        "operational-kaggle-matrix-receipt.v1.schema.json",
        "operational-kaggle-scenario-receipt.v1.schema.json",
    ):
        shutil.copy2(ROOT / "schemas/provider" / name, provider_schema_dir / name)
    for name in (
        "operational-mvp-evidence.v1.schema.json",
        "connector-durability-receipt.v1.schema.json",
    ):
        target = tmp_path / "schemas" / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(ROOT / "schemas" / name, target)

    run_refs = [f"owner/master/{index}" for index in range(1, 16)]
    kernel_ids = list(range(1, 16))
    scenario_summaries: list[dict[str, Any]] = []
    for ordinal in range(1, 25):
        identity_index = ((ordinal - 1) % 15) + 1
        receipt_name = f"{ordinal:02d}-scenario-{ordinal:02d}.json"
        scenario = f"scenario-{ordinal:02d}"
        scenario_summaries.append(
            {
                "ordinal": ordinal,
                "requirement_id": f"FM{ordinal:02d}",
                "scenario": scenario,
                "outcome": "PASS",
                "receipt": receipt_name,
            }
        )
        scenario_receipt = {
            "schema_version": "my-data-hub-operational-kaggle-scenario-receipt.v1",
            "matrix_id": MATRIX_ID,
            "commit_sha": commit,
            "ordinal": ordinal,
            "requirement_id": f"FM{ordinal:02d}",
            "scenario": scenario,
            "outcome": "PASS",
            "live_evidence": True,
            "planned_task_run_id": f"00000000-0000-4000-8000-{ordinal:012d}",
            "real_run_identity": {
                "provider_ref": "owner/master",
                "provider_run_ref": f"owner/master/{identity_index}",
                "provider_kernel_id": identity_index,
                "source_version": 1,
                "source_sha256": "1" * 64,
                "provider_status": "complete",
                "output_tree_sha256": "2" * 64,
                "result_sha256": "3" * 64,
                "provider_claim_sha256": "4" * 64,
            },
            "assertions": [
                {"name": "source_bound", "outcome": "PASS", "evidence_sha256": "5" * 64},
                {"name": "result_bound", "outcome": "PASS", "evidence_sha256": "6" * 64},
            ],
            "lifecycle_events": [],
            "operation_ids": [],
            "blocker": None,
            "started_at": "2026-08-11T00:00:00Z",
            "completed_at": "2026-08-11T01:00:00Z",
            "driver_mutations_started": 1,
            "driver_capability_checks": [],
            "driver_observation_sha256": "7" * 64,
        }
        write_json(tmp_path / "artifacts/matrix" / receipt_name, scenario_receipt)

    matrix_lifecycle = {
        "master_boots": 3,
        "clean_rotations": 2,
        "abrupt_master_terminations": 1,
        "control_plane_restarts": 1,
        "host_reboots": 1,
        "soak_runs": 1,
        "soak_duration_seconds": 3600,
    }
    matrix_receipt = {
        "schema_version": "my-data-hub-operational-kaggle-matrix-receipt.v1",
        "matrix_id": MATRIX_ID,
        "commit_sha": commit,
        "matrix_scope": "operational_24_scenario",
        "outcome": "PASS",
        "live_evidence": True,
        "minimum_distinct_provider_runs": 15,
        "planned_scenarios": 24,
        "passed_scenarios": 24,
        "failed_scenarios": 0,
        "blocked_scenarios": 0,
        "distinct_provider_run_refs": run_refs,
        "distinct_provider_kernel_ids": kernel_ids,
        "lifecycle_gates": matrix_lifecycle,
        "scenario_receipts": scenario_summaries,
        "blockers": [],
        "completed_at": "2026-08-11T01:00:00Z",
    }
    matrix_locator = "artifacts/matrix/matrix.json"
    matrix_sha = write_json(tmp_path / matrix_locator, matrix_receipt)

    artifact_specs = {
        "review": (
            "IMPLEMENTATION_REVIEW",
            "artifacts/review.json",
            ["N"],
            ["GATE-N-HOSTED-CHECKS"],
        ),
        "deployment": (
            "DEPLOYMENT",
            "artifacts/deployment.json",
            ["M"],
            ["GATE-M-DEPLOYMENT"],
        ),
        "post-deploy": (
            "POST_DEPLOY",
            "artifacts/post-deploy.json",
            ["M", "N"],
            ["GATE-M-POST-DEPLOY", "GATE-N-RECEIPT"],
        ),
        "security": (
            "SECURITY_AUDIT",
            "artifacts/security.json",
            ["I"],
            ["FM20", "FM21"],
        ),
        "data-integrity": (
            "DATA_INTEGRITY_AUDIT",
            "artifacts/data-integrity.json",
            ["J", "K"],
            ["FM16", "FM18", "FM19"],
        ),
    }
    evidence: list[dict[str, Any]] = []
    for evidence_id, (kind, locator, gate_ids, requirements) in artifact_specs.items():
        content = semantic_evidence(
            kind,
            commit,
            gate_ids,
            requirements,
            reviewed=reviewed if kind == "IMPLEMENTATION_REVIEW" else None,
        )
        digest = write_json(tmp_path / locator, content)
        evidence.append(
            {
                "evidence_id": evidence_id,
                "artifact_kind": kind,
                "storage": "REPOSITORY_FILE",
                "locator": locator,
                "sha256": digest,
                "source_commit": commit,
                "observed_at": "2026-08-11T01:00:00Z",
                "live_evidence": True,
                "schema_path": "schemas/operational-mvp-evidence.v1.schema.json",
                "gate_ids": gate_ids,
                "requirement_ids": requirements,
            }
        )
    evidence.append(
        {
            "evidence_id": "real-matrix",
            "artifact_kind": "REAL_KAGGLE_MATRIX",
            "storage": "REPOSITORY_FILE",
            "locator": matrix_locator,
            "sha256": matrix_sha,
            "source_commit": commit,
            "observed_at": "2026-08-11T01:00:00Z",
            "live_evidence": True,
            "schema_path": "schemas/provider/operational-kaggle-matrix-receipt.v1.schema.json",
            "gate_ids": list("ABCDEFGH"),
            "requirement_ids": [f"FM{ordinal:02d}" for ordinal in range(1, 25)],
        }
    )
    connector_locator = "artifacts/connector.json"
    connector = {
        "schema_version": "my-data-hub-connector-durability-receipt.v1",
        "state": "DURABLE_COMPLETE",
        "acceptance": {
            "receipt_id": "14844b31-ca44-5c91-9d09-985b2b62ea17",
            "status": "accepted",
            "connector_id": "events-bot.daily-statistics",
            "batch_id": "79cd25b7-f6bd-5952-a107-3a792c340578",
            "idempotency_key": "events-bot.daily-statistics:2026-08-11:1",
            "payload_sha256": "a" * 64,
            "envelope_sha256": "b" * 64,
            "accepted_at": "2026-08-11T00:00:00Z",
        },
        "canonical_revision": 18,
        "checkpoint_request_id": "c" * 64,
        "checkpoint_operation_id": "checkpoint:connector:79cd25b7-f6bd-5952-a107-3a792c340578",
        "checkpoint_receipt_sha256": "d" * 64,
        "checkpoint_id": "checkpoint-20260811-18",
        "updated_at": "2026-08-11T01:00:00Z",
    }
    connector_sha = write_json(tmp_path / connector_locator, connector)
    evidence.append(
        {
            "evidence_id": "connector-durability",
            "artifact_kind": "CONNECTOR_DURABILITY",
            "storage": "REPOSITORY_FILE",
            "locator": connector_locator,
            "sha256": connector_sha,
            "source_commit": commit,
            "observed_at": "2026-08-11T01:00:00Z",
            "live_evidence": True,
            "schema_path": "schemas/connector-durability-receipt.v1.schema.json",
            "gate_ids": ["L"],
            "requirement_ids": ["GATE-L-CONNECTOR-DURABILITY"],
        }
    )

    lifecycle = {
        **matrix_lifecycle,
        "soak_heartbeats": 12,
        "soak_read_queries": 12,
        "soak_checkpoints": 1,
        "soak_recoveries": 1,
    }
    gate_refs = {
        **{gate: ["real-matrix"] for gate in "ABCDEFGH"},
        "I": ["security"],
        "J": ["data-integrity"],
        "K": ["data-integrity"],
        "L": ["connector-durability"],
        "M": ["deployment", "post-deploy"],
        "N": ["review", "post-deploy"],
    }
    return {
        "schema_version": "my-data-hub-operational-mvp-acceptance.v1",
        "receipt_scope": "OBSERVED_OPERATIONAL",
        "verdict": "MY_DATA_HUB_OPERATIONAL_MVP_COMPLETE",
        "completion_criteria_met": True,
        "evaluated_source_commit": commit,
        "implementation_identity": {
            "reviewed_head_commit": reviewed,
            "merge_commit": commit,
            "deployed_commit": commit,
            "post_deploy_verified_commit": commit,
            "deployment_tree_state": "CLEAN",
        },
        "architecture_source_sha256": "a" * 64,
        "completed_at": "2026-08-11T01:00:00Z",
        "summary": "Observed operational evidence satisfies the fail-closed contract.",
        "counts": {
            "real_kaggle_run_ids": 15,
            "real_kaggle_kernel_ids": 15,
            "operational_scenarios_passed": 24,
            "ydb_bloggers_observed": 266,
            "bloggers_imported": 266,
            "undispositioned_rows": 0,
            "quarantined_rows": 0,
            "e5_coverage": 1,
            "bge_m3_coverage": 1,
        },
        "operational_matrix": {
            "receipt_evidence_id": "real-matrix",
            "planned_scenarios": 24,
            "passed_scenarios": 24,
            "failed_scenarios": 0,
            "blocked_scenarios": 0,
            "distinct_provider_run_refs": run_refs,
            "distinct_provider_kernel_ids": kernel_ids,
            "lifecycle_gates": lifecycle,
        },
        "gate_results": [
            {
                "gate_id": gate_id,
                "name": name,
                "outcome": "PASS",
                "evidence": "Exact live requirement-specific evidence is referenced by content hash.",
                "evidence_refs": gate_refs[gate_id],
            }
            for gate_id, name in GATES.items()
        ],
        "evidence": evidence,
        "required_evidence": {
            "implementation_review": ["review"],
            "deployment": ["deployment"],
            "post_deploy": ["post-deploy"],
            "security_audit": ["security"],
            "data_integrity_audit": ["data-integrity"],
            "operational_matrix": ["real-matrix"],
        },
        "blockers": [],
    }


def evidence_item(receipt: dict[str, Any], evidence_id: str) -> dict[str, Any]:
    return next(item for item in receipt["evidence"] if item["evidence_id"] == evidence_id)


def rewrite_evidence(tmp_path: Path, receipt: dict[str, Any], evidence_id: str, value: object) -> None:
    item = evidence_item(receipt, evidence_id)
    item["sha256"] = write_json(tmp_path / item["locator"], value)


def validate_complete(tmp_path: Path, receipt: dict[str, Any]) -> list[str]:
    return validate_operational_mvp_receipt_semantics(
        receipt,
        root=tmp_path,
        expected_source_commit=receipt["evaluated_source_commit"],
        allow_complete=True,
    )


def test_committed_blocked_receipt_and_synthetic_example_are_honest() -> None:
    for relative, allow_complete in (
        ("examples/contracts/operational-mvp-acceptance-receipt.v1.example.json", False),
        (
            "docs/operations/evidence/2026-08-11-operational-mvp/operational-mvp-acceptance-blocked.json",
            True,
        ),
    ):
        receipt = json.loads((ROOT / relative).read_text())
        assert schema_errors(receipt) == []
        assert receipt["verdict"] == "MY_DATA_HUB_OPERATIONAL_MVP_BLOCKED"
        assert receipt["completion_criteria_met"] is False
        assert receipt["blockers"]
        assert (
            validate_operational_mvp_receipt_semantics(
                receipt,
                root=ROOT,
                expected_source_commit="f" * 40,
                allow_complete=allow_complete,
            )
            == []
        )


def test_complete_receipt_requires_exact_live_evidence_bundle(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)

    assert schema_errors(receipt) == []
    assert validate_complete(tmp_path, receipt) == []


def test_complete_receipt_rejects_stale_commit_and_tampered_evidence(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    (tmp_path / "artifacts/security.json").write_text("tampered\n", encoding="utf-8")

    errors = validate_operational_mvp_receipt_semantics(
        receipt,
        root=tmp_path,
        expected_source_commit="e" * 40,
        allow_complete=True,
    )

    assert any("is stale" in error for error in errors)
    assert any("hash mismatch" in error for error in errors)


def test_complete_receipt_rejects_identity_and_count_inconsistency(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    receipt["implementation_identity"]["deployed_commit"] = "e" * 40
    receipt["counts"]["real_kaggle_run_ids"] = 16

    errors = validate_complete(tmp_path, receipt)

    assert any("deployed_commit" in error for error in errors)
    assert any("run refs" in error for error in errors)


def test_complete_receipt_rejects_missing_scenario_evidence(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    (tmp_path / "artifacts/matrix/24-scenario-24.json").unlink()

    errors = validate_complete(tmp_path, receipt)

    assert any("scenario evidence is absent" in error for error in errors)


def test_complete_receipt_rejects_generic_dummy_and_gate_l_misclassification(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    rewrite_evidence(
        tmp_path,
        receipt,
        "connector-durability",
        {"commit_sha": receipt["evaluated_source_commit"], "outcome": "PASS"},
    )
    gate_l = next(gate for gate in receipt["gate_results"] if gate["gate_id"] == "L")
    gate_l["evidence_refs"] = ["review"]

    errors = validate_complete(tmp_path, receipt)

    assert any("violates its semantic schema" in error for error in errors)
    assert any("Gate L lacks requirement-specific evidence kind" in error for error in errors)


def test_complete_receipt_rejects_unscoped_gate_evidence(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    gate_a = next(gate for gate in receipt["gate_results"] if gate["gate_id"] == "A")
    gate_a["evidence_refs"] = ["security"]

    errors = validate_complete(tmp_path, receipt)

    assert any("Gate A references evidence outside its declared gate scope" in error for error in errors)
    assert any("Gate A lacks requirement-specific evidence kind" in error for error in errors)


def test_complete_receipt_rejects_unrelated_review_head_and_hosted_check(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    review = json.loads((tmp_path / "artifacts/review.json").read_text())
    review["hosted_checks"][0]["head_commit"] = "e" * 40
    rewrite_evidence(tmp_path, receipt, "review", review)
    receipt["implementation_identity"]["reviewed_head_commit"] = "f" * 40

    errors = validate_complete(tmp_path, receipt)

    assert any("neither a parent nor ancestor" in error for error in errors)
    assert any("not bound to reviewed_head_commit" in error for error in errors)


def test_complete_receipt_rejects_stale_deploy_provenance(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    deployment = json.loads((tmp_path / "artifacts/deployment.json").read_text())
    deployment["deployed_commit"] = "e" * 40
    rewrite_evidence(tmp_path, receipt, "deployment", deployment)

    errors = validate_complete(tmp_path, receipt)

    assert any("deployment evidence deployed_commit differs" in error for error in errors)


def test_complete_receipt_rejects_provider_real_on_untrusted_runner(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    post_deploy = json.loads((tmp_path / "artifacts/post-deploy.json").read_text())
    provider_check = next(check for check in post_deploy["hosted_checks"] if check["name"] == "provider-real")
    provider_check["runner"] = "ubuntu-latest"
    rewrite_evidence(tmp_path, receipt, "post-deploy", post_deploy)

    errors = validate_complete(tmp_path, receipt)

    assert any("provider-real' uses an untrusted runner" in error for error in errors)


def test_synthetic_or_blocker_free_blocked_receipt_cannot_claim_completion() -> None:
    example = json.loads((ROOT / "examples/contracts/operational-mvp-acceptance-receipt.v1.example.json").read_text())
    fake_complete = copy.deepcopy(example)
    fake_complete["verdict"] = "MY_DATA_HUB_OPERATIONAL_MVP_COMPLETE"
    fake_complete["completion_criteria_met"] = True
    assert schema_errors(fake_complete)
    semantic_errors = validate_operational_mvp_receipt_semantics(
        fake_complete,
        root=ROOT,
        expected_source_commit=fake_complete["evaluated_source_commit"],
        allow_complete=False,
    )
    assert any("may not use the COMPLETE" in error for error in semantic_errors)

    fake_blocked = copy.deepcopy(example)
    fake_blocked["blockers"] = []
    assert schema_errors(fake_blocked)


def test_connector_intake_compose_service_is_bounded_and_default_off() -> None:
    report = Report()
    compose = yaml.safe_load((ROOT / "compose.control-plane.yaml").read_text(encoding="utf-8"))

    validate_connector_intake_compose_service(report, compose["services"]["connector-intake"])

    assert report.errors == []


def test_connector_intake_compose_service_rejects_data_plane_surfaces() -> None:
    report = Report()
    compose = yaml.safe_load((ROOT / "compose.control-plane.yaml").read_text(encoding="utf-8"))
    service = copy.deepcopy(compose["services"]["connector-intake"])
    service["ports"] = ["0.0.0.0:9999:9999"]
    service["expose"] = ["25432"]
    service["environment"]["MY_DATA_HUB_DB_HOST"] = "master.internal"
    service["environment"]["MY_DATA_HUB_DATA_PLANE_ENDPOINT"] = "postgresql://master.internal/db"

    validate_connector_intake_compose_service(report, service)

    assert any("reviewed loopback port" in error for error in report.errors)
    assert any("unbounded Compose port" in error for error in report.errors)
    assert any("reviewed lightweight control boundary" in error for error in report.errors)
    assert any("embeds a database/PGDATA/data-plane endpoint" in error for error in report.errors)


def test_provider_real_workflow_uses_private_rotating_oauth_boundary() -> None:
    source = """
    MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE: /srv/private/oauth.json
    validate_oauth_credential_file(path, required_profiles=frozenset(
        {"reader", "operator", "migration", "provider"}
    ))
    """
    workflow = {
        "jobs": {
            "private-notebook-canary": {
                "runs-on": ["self-hosted", "linux", "my-data-hub-devstand"],
                "env": {"MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE": "/srv/private/oauth.json"},
            }
        }
    }
    report = Report()

    validate_provider_real_workflow_auth_boundary(report, workflow, source)

    assert report.errors == []


def test_provider_real_workflow_rejects_static_bearers_and_hosted_runner() -> None:
    source = """
    MY_DATA_HUB_MCP_CANARY_TOKEN: ${{ secrets.MY_DATA_HUB_MCP_CANARY_TOKEN }}
    KAGGLE_API_TOKEN: ${{ secrets.KAGGLE_API_TOKEN }}
    """
    workflow = {
        "jobs": {
            "private-notebook-canary": {
                "runs-on": "ubuntu-latest",
                "env": {
                    "MY_DATA_HUB_MCP_CANARY_TOKEN": "${{ secrets.MY_DATA_HUB_MCP_CANARY_TOKEN }}",
                    "KAGGLE_API_TOKEN": "${{ secrets.KAGGLE_API_TOKEN }}",
                },
            }
        }
    }
    report = Report()

    validate_provider_real_workflow_auth_boundary(report, workflow, source)

    assert any("owner-controlled self-hosted runner" in error for error in report.errors)
    assert any("static MCP/data/Kaggle credential variables" in error for error in report.errors)
    assert any("rotating OAuth credential file" in error for error in report.errors)
    assert any("static MCP/data/Kaggle bearer secret" in error for error in report.errors)
    assert any("refresh-file preflight" in error for error in report.errors)
