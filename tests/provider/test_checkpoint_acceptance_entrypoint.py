from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator, FormatChecker

import my_data_hub.checkpoints.acceptance_runtime as acceptance_runtime
import scripts.provider.checkpoint_acceptance_evidence as entrypoint
from my_data_hub.checkpoints.acceptance import (
    CheckpointAcceptanceHead,
    CheckpointAcceptanceReceipt,
    CheckpointAcceptanceStageReceipt,
)
from my_data_hub.checkpoints.acceptance_runtime import (
    CheckpointAcceptanceEntrypointBlocker,
    CheckpointAcceptanceProductionConfig,
    build_production_checkpoint_acceptance_runtime,
)

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
OPERATION_ID = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
TASK_RUN_ID = UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
CANDIDATE_ID = UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")


@pytest.fixture
def runtime_root(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "kaggle" / "working"
    root.mkdir(parents=True)
    monkeypatch.setattr(acceptance_runtime, "_RUNTIME_ROOT", root)
    monkeypatch.setattr(entrypoint, "RUNTIME_ROOT", root)
    return root


def _config(root: Path, *, scenario: str = "FM14") -> CheckpointAcceptanceProductionConfig:
    payload: dict[str, object] = {
        "schema_version": "my-data-hub-checkpoint-acceptance-production-config.v1",
        "scenario": scenario,
        "operation_id": str(OPERATION_ID),
        "task_run_id": str(TASK_RUN_ID),
        "source_revision": "2" * 40,
        "started_at": NOW.isoformat(),
        "provider_owner": "owner",
        "dataset_ref": "owner/checkpoint-acceptance-fm14",
        "evidence_notebook_ref": "owner/checkpoint-acceptance-evidence-fm14",
        "template_dataset_version_ref": "owner/checkpoint-empty-template/3",
        "template_claim_sha256": "1" * 64,
        "template_directory": str(root / "template"),
        "template_manifest_sha256": "3" * 64,
        "template_content_sha256": "4" * 64,
        "status_dataset_version_ref": "owner/checkpoint-status-task/1",
        "status_config_sha256": "a" * 64,
        "status_helper_sha256": "b" * 64,
        "working_directory": str(root / "state"),
        "control_base_url": "https://control.example.test",
        "control_identity": {
            "authority_kind": "acceptance-task",
            "request_id": str(TASK_RUN_ID),
            "task_run_id": str(TASK_RUN_ID),
            "attempt_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
            "scope": "acceptance:operate",
        },
        "timeout_seconds": 900,
    }
    if scenario in {"FM05", "FM15"}:
        payload["verifier_notebook_ref"] = f"owner/checkpoint-verifier-{scenario.lower()}"
        payload["verifier"] = {
            "dataset_version_ref": "owner/checkpoint-verifier-assets/5",
            "claim_sha256": "f" * 64,
            "path": str(root / "fixed-verifier.py"),
            "source_sha256": "5" * 64,
            "code_file": "worker.py",
        }
    return CheckpointAcceptanceProductionConfig.model_validate(payload)


def _live_receipt() -> CheckpointAcceptanceReceipt:
    initial = CheckpointAcceptanceHead(generation=0)
    stages = (
        CheckpointAcceptanceStageReceipt(
            stage="corrupted_candidate",
            candidate_checkpoint_id=CANDIDATE_ID,
            task_owned=True,
            disposable_candidate=True,
            outcome="succeeded",
            detail_code="TASK_OWNED_CORRUPTION_CANDIDATE_CREATED",
            provider_receipt_sha256="6" * 64,
            exact_version_ref="owner/checkpoint-acceptance-fm14/1",
        ),
        CheckpointAcceptanceStageReceipt(
            stage="hash_mismatch_rejection",
            candidate_checkpoint_id=CANDIDATE_ID,
            task_owned=True,
            disposable_candidate=True,
            outcome="rejected_expected",
            detail_code="EXACT_READBACK_HASH_MISMATCH_REJECTED",
            provider_receipt_sha256="7" * 64,
            exact_version_ref="owner/checkpoint-acceptance-fm14/1",
            expected_content_sha256="8" * 64,
            observed_content_sha256="9" * 64,
        ),
    )
    return CheckpointAcceptanceReceipt(
        scenario="FM14",
        verdict="LIVE_PASS",
        evidence_class="live",
        operation_id=OPERATION_ID,
        task_run_id=TASK_RUN_ID,
        candidate_checkpoint_id=CANDIDATE_ID,
        intent_sha256="a" * 64,
        initial_head=initial,
        final_head=initial,
        head_unchanged=True,
        stages=stages,
        completed_at=NOW,
    )


def test_config_loader_requires_mode_0600_and_fixed_runtime_paths(runtime_root: Path) -> None:
    config = _config(runtime_root)
    path = runtime_root / entrypoint.CONFIG_FILE_NAME
    path.write_bytes(json.dumps(config.model_dump(mode="json")).encode())
    os.chmod(path, 0o644)
    with pytest.raises(ValueError, match="mode-0600"):
        entrypoint.load_config(path)
    os.chmod(path, 0o600)
    assert entrypoint.load_config(path) == config

    outside = runtime_root.parent / entrypoint.CONFIG_FILE_NAME
    outside.write_bytes(path.read_bytes())
    os.chmod(outside, 0o600)
    with pytest.raises(ValueError, match="below /kaggle/working"):
        entrypoint.load_config(outside)


def test_modern_token_is_first_factory_blocker_before_control_or_ledger(runtime_root: Path) -> None:
    calls: list[str] = []

    class Transport:
        def request(self, **_kwargs: object) -> object:
            calls.append("control")
            raise AssertionError("missing token must block before control")

    def ledger(_path: Path) -> object:
        calls.append("ledger")
        raise AssertionError("missing token must block before ledger")

    with pytest.raises(
        CheckpointAcceptanceEntrypointBlocker,
        match="CHECKPOINT_CONTROL_RUNTIME_TOKEN_MISSING",
    ):
        build_production_checkpoint_acceptance_runtime(
            _config(runtime_root),
            environ={},
            clock=lambda: NOW,
            control_transport=Transport(),
            ledger_factory=ledger,  # type: ignore[arg-type]
        )
    assert calls == []


def test_execute_emits_exact_live_receipt_and_never_matrix_pass(runtime_root: Path) -> None:
    config = _config(runtime_root)
    receipt = _live_receipt()
    code, result = entrypoint.execute(
        config,
        factory=lambda _config: SimpleNamespace(run=lambda: receipt),  # type: ignore[arg-type]
        clock=lambda: NOW,
    )
    assert code == 0
    assert result.outcome == "LIVE_EVIDENCE_READY"
    assert result.live_evidence is True
    assert result.receipt == receipt
    assert result.receipt_sha256 == receipt.receipt_sha256
    assert result.candidate_checkpoint_id == CANDIDATE_ID
    assert result.locator.exact_version_refs == ("owner/checkpoint-acceptance-fm14/1",)
    assert "PASS" not in result.outcome


def test_execute_distinguishes_pre_mutation_blocker_from_post_action_failure(runtime_root: Path) -> None:
    config = _config(runtime_root)

    def blocked(_config: CheckpointAcceptanceProductionConfig) -> object:
        raise CheckpointAcceptanceEntrypointBlocker("CHECKPOINT_ACCEPTANCE_ASSET_PREFLIGHT_FAILED")

    blocked_code, blocked_result = entrypoint.execute(config, factory=blocked, clock=lambda: NOW)  # type: ignore[arg-type]
    assert blocked_code == 78
    assert blocked_result.outcome == "BLOCKED"
    assert blocked_result.mutations_started == 0
    assert blocked_result.receipt is None

    def failed(_config: CheckpointAcceptanceProductionConfig) -> object:
        return SimpleNamespace(run=lambda: (_ for _ in ()).throw(TimeoutError("secret text")))

    failed_code, failed_result = entrypoint.execute(config, factory=failed, clock=lambda: NOW)  # type: ignore[arg-type]
    assert failed_code == 1
    assert failed_result.outcome == "FAIL"
    assert failed_result.mutations_started == 1
    assert failed_result.failure_code == "TIMEOUT_ERROR"
    assert "secret" not in json.dumps(failed_result.model_dump(mode="json"))


def test_cli_writes_fixed_mode_0600_typed_operational_result(
    runtime_root: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = _config(runtime_root)
    config_path = runtime_root / entrypoint.CONFIG_FILE_NAME
    config_path.write_bytes(json.dumps(config.model_dump(mode="json")).encode())
    os.chmod(config_path, 0o600)
    output = runtime_root / entrypoint.RESULT_FILE_NAME

    monkeypatch.setattr(
        entrypoint,
        "execute",
        lambda value: (
            78,
            entrypoint._blocked_result(
                value,
                "CHECKPOINT_ACCEPTANCE_ASSET_PREFLIGHT_FAILED",
                completed_at=NOW,
            ),
        ),
    )
    assert entrypoint.main(["--config", str(config_path), "--output", str(output)]) == 78
    assert output.stat().st_mode & 0o777 == 0o600
    parsed = acceptance_runtime.CheckpointAcceptanceOperationalResult.model_validate_json(output.read_bytes())
    assert parsed.outcome == "BLOCKED"
    assert parsed.mutations_started == 0


@pytest.mark.parametrize(
    ("schema_name", "example_name"),
    [
        (
            "checkpoint-acceptance-production-config.v1.schema.json",
            "checkpoint-acceptance-production-config.v1.example.json",
        ),
        (
            "checkpoint-acceptance-operational-result.v1.schema.json",
            "checkpoint-acceptance-operational-result.v1.example.json",
        ),
    ],
)
def test_checkpoint_entrypoint_examples_validate(schema_name: str, example_name: str) -> None:
    root = Path(__file__).resolve().parents[2]
    schema = json.loads((root / "schemas" / schema_name).read_text())
    example = json.loads((root / "examples" / "provider" / example_name).read_text())
    Draft202012Validator(schema, format_checker=FormatChecker()).validate(example)
