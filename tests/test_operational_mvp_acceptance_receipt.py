from __future__ import annotations

import copy
import hashlib
import json
import shutil
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from scripts.validate_repository import validate_operational_mvp_receipt_semantics

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = json.loads(
    (ROOT / "schemas/operational-mvp-acceptance-receipt.v1.schema.json").read_text()
)
COMMIT = "c" * 40
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


def build_complete_receipt(tmp_path: Path) -> dict[str, Any]:
    provider_schema_dir = tmp_path / "schemas/provider"
    provider_schema_dir.mkdir(parents=True)
    for name in (
        "operational-kaggle-matrix-receipt.v1.schema.json",
        "operational-kaggle-scenario-receipt.v1.schema.json",
    ):
        shutil.copy2(ROOT / "schemas/provider" / name, provider_schema_dir / name)

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
            "commit_sha": COMMIT,
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
        "commit_sha": COMMIT,
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
        "review": ("IMPLEMENTATION_REVIEW", "artifacts/review.json"),
        "deployment": ("DEPLOYMENT", "artifacts/deployment.json"),
        "post-deploy": ("POST_DEPLOY", "artifacts/post-deploy.json"),
        "security": ("SECURITY_AUDIT", "artifacts/security.json"),
        "data-integrity": ("DATA_INTEGRITY_AUDIT", "artifacts/data-integrity.json"),
    }
    evidence = []
    for evidence_id, (kind, locator) in artifact_specs.items():
        digest = write_json(tmp_path / locator, {"commit_sha": COMMIT, "outcome": "PASS"})
        evidence.append(
            {
                "evidence_id": evidence_id,
                "artifact_kind": kind,
                "storage": "REPOSITORY_FILE",
                "locator": locator,
                "sha256": digest,
                "source_commit": COMMIT,
                "observed_at": "2026-08-11T01:00:00Z",
                "live_evidence": True,
            }
        )
    evidence.append(
        {
            "evidence_id": "real-matrix",
            "artifact_kind": "REAL_KAGGLE_MATRIX",
            "storage": "REPOSITORY_FILE",
            "locator": matrix_locator,
            "sha256": matrix_sha,
            "source_commit": COMMIT,
            "observed_at": "2026-08-11T01:00:00Z",
            "live_evidence": True,
        }
    )

    lifecycle = {
        **matrix_lifecycle,
        "soak_heartbeats": 12,
        "soak_read_queries": 12,
        "soak_checkpoints": 1,
        "soak_recoveries": 1,
    }
    return {
        "schema_version": "my-data-hub-operational-mvp-acceptance.v1",
        "receipt_scope": "OBSERVED_OPERATIONAL",
        "verdict": "MY_DATA_HUB_OPERATIONAL_MVP_COMPLETE",
        "completion_criteria_met": True,
        "evaluated_source_commit": COMMIT,
        "implementation_identity": {
            "reviewed_head_commit": "d" * 40,
            "merge_commit": COMMIT,
            "deployed_commit": COMMIT,
            "post_deploy_verified_commit": COMMIT,
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
                "evidence": "Exact live evidence is referenced by content hash.",
                "evidence_refs": ["review"],
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


def test_committed_blocked_receipt_and_synthetic_example_are_honest() -> None:
    for relative, allow_complete in (
        ("examples/contracts/operational-mvp-acceptance-receipt.v1.example.json", False),
        (
            "docs/operations/evidence/2026-08-11-operational-mvp/"
            "operational-mvp-acceptance-blocked.json",
            True,
        ),
    ):
        receipt = json.loads((ROOT / relative).read_text())
        assert schema_errors(receipt) == []
        assert receipt["verdict"] == "MY_DATA_HUB_OPERATIONAL_MVP_BLOCKED"
        assert receipt["completion_criteria_met"] is False
        assert receipt["blockers"]
        assert validate_operational_mvp_receipt_semantics(
            receipt,
            root=ROOT,
            expected_source_commit="f" * 40,
            allow_complete=allow_complete,
        ) == []


def test_complete_receipt_requires_exact_live_evidence_bundle(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)

    assert schema_errors(receipt) == []
    assert validate_operational_mvp_receipt_semantics(
        receipt,
        root=tmp_path,
        expected_source_commit=COMMIT,
        allow_complete=True,
    ) == []


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

    errors = validate_operational_mvp_receipt_semantics(
        receipt,
        root=tmp_path,
        expected_source_commit=COMMIT,
        allow_complete=True,
    )

    assert any("deployed_commit" in error for error in errors)
    assert any("run refs" in error for error in errors)


def test_complete_receipt_rejects_missing_scenario_evidence(tmp_path: Path) -> None:
    receipt = build_complete_receipt(tmp_path)
    (tmp_path / "artifacts/matrix/24-scenario-24.json").unlink()

    errors = validate_operational_mvp_receipt_semantics(
        receipt,
        root=tmp_path,
        expected_source_commit=COMMIT,
        allow_complete=True,
    )

    assert any("scenario evidence is absent" in error for error in errors)


def test_synthetic_or_blocker_free_blocked_receipt_cannot_claim_completion() -> None:
    example = json.loads(
        (ROOT / "examples/contracts/operational-mvp-acceptance-receipt.v1.example.json").read_text()
    )
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
