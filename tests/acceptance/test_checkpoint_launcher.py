from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.acceptance.checkpoint_launcher import (
    CheckpointAcceptanceDeployment,
    ControlCheckpointAcceptanceLauncher,
)
from my_data_hub.acceptance.scenario_operator import (
    AcceptanceScenarioRequest,
    CheckpointAcceptanceLaunchCatalog,
    CheckpointAcceptanceServiceIdentity,
)
from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.providers.kaggle.contracts import (
    DatasetMutationResult,
    EffectOutcome,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleKernelSourceIdentity,
    KaggleKernelStatus,
    KernelState,
    NotebookMutationResult,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind

NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
TASK = UUID("11111111-1111-4111-8111-111111111111")
ATTEMPT = UUID("22222222-2222-4222-8222-222222222222")


class Principal:
    subject = "owner"
    client_id = "operator"
    scopes = frozenset({"acceptance:operate"})


def _deployment() -> CheckpointAcceptanceDeployment:
    return CheckpointAcceptanceDeployment.model_validate(
        {
            "schema_version": "my-data-hub-checkpoint-acceptance-deployment.v1",
            "provider_owner": "owner",
            "evidence_notebook_ref": "owner/checkpoint-evidence",
            "candidate_dataset_refs": {
                "FM05": "owner/candidate-fm05",
                "FM14": "owner/candidate-fm14",
                "FM15": "owner/candidate-fm15",
            },
            "template_input": {
                "provider_ref": "owner/template",
                "exact_version_ref": "owner/template/3",
                "claim_sha256": "1" * 64,
                "manifest_sha256": "2" * 64,
                "content_sha256": "3" * 64,
            },
            "verifier_inputs": {
                scenario: {
                    "provider_ref": f"owner/verifier-{scenario.lower()}",
                    "exact_version_ref": f"owner/verifier-{scenario.lower()}/4",
                    "claim_sha256": "4" * 64,
                    "source_sha256": "5" * 64,
                }
                for scenario in ("FM05", "FM15")
            },
            "verifier_notebook_refs": {
                "FM05": "owner/checkpoint-verifier-fm05",
                "FM15": "owner/checkpoint-verifier-fm15",
            },
            "runtime_input": {
                "provider_ref": "owner/checkpoint-runtime",
                "exact_version_ref": "owner/checkpoint-runtime/9",
                "claim_sha256": "6" * 64,
                "wheel_file": "my_data_hub.whl",
                "wheel_sha256": "7" * 64,
                "entrypoint_sha256": "8" * 64,
            },
            "control_base_url": "https://control.example.test",
            "kaggle_secret_bindings": {
                "KAGGLE_USERNAME": "MDH_KAGGLE_USERNAME",
                "KAGGLE_KEY": "MDH_KAGGLE_KEY",
            },
        }
    )


def _request():
    catalog: CheckpointAcceptanceLaunchCatalog = _deployment().catalog
    public = AcceptanceScenarioRequest(
        task_id=TASK,
        scenario="FM14",
        idempotency_key="checkpoint-fm14-owner-task",
        source_revision="a" * 40,
    )
    principal = Principal()
    launch = catalog.request(public, principal, started_at=NOW)
    assert isinstance(launch.control_identity, CheckpointAcceptanceServiceIdentity)
    return launch.model_copy(
        update={
            "control_identity": launch.control_identity.model_copy(update={"attempt_id": ATTEMPT})
        }
    )


class FakeAdapter:
    def __init__(self, ledger: ControlLedger, *, lose_push: bool = False) -> None:
        self.ledger = ledger
        self.lose_push = lose_push
        self.push_calls = 0
        self.dataset_sources: tuple[str, ...] = ()
        self.status_files: dict[str, bytes] = {}
        self.source = b""

    def create_private_dataset(self, *, intent, files, title, control_class, disposable):
        assert self.ledger.checkpoint_acceptance_launch(str(TASK)) is not None
        assert control_class is ControlClass.MCP_EXCHANGE and disposable is True
        self.status_files = dict(files)
        fingerprint = ProviderFingerprint(value="9" * 64)
        identity = KaggleDatasetIdentity(
            provider_ref=intent.provider_ref,
            version=1,
            privacy="private",
            package_sha256="a" * 64,
            fingerprint=fingerprint,
            observed_at=NOW,
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.DATASET,
            control_class=control_class,
            disposable=True,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=NOW,
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=fingerprint,
            provider_version=1,
            observed_at=NOW,
            detail_code="private_dataset_exact_readback",
        )
        return DatasetMutationResult(identity=identity, claim=claim, effect=receipt)

    def push_private_notebook(self, *, intent, task_run_id, source, dataset_sources, **kwargs):
        self.push_calls += 1
        self.source = source
        self.dataset_sources = tuple(dataset_sources)
        if self.lose_push:
            raise RuntimeError("simulated lost provider response")
        fingerprint = ProviderFingerprint(value="b" * 64)
        source_sha = hashlib.sha256(source).hexdigest()
        source_identity = KaggleKernelSourceIdentity(
            provider_ref=intent.provider_ref,
            source_version=1,
            privacy="private",
            source_sha256=source_sha,
            fingerprint=fingerprint,
            observed_at=NOW,
        )
        run = KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=intent.provider_ref,
            source_version=1,
            source_sha256=source_sha,
            provider_kernel_id=17,
            provider_run_ref=f"{intent.provider_ref}/1",
            started_at=NOW,
        )
        claim = TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=ProviderKind.NOTEBOOK,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=False,
            fingerprint=fingerprint,
            provider_version=1,
            registered_at=NOW,
        )
        receipt = ProviderEffectReceipt(
            operation_id=intent.operation_id,
            effect_id=intent.effect_id,
            action=intent.action,
            provider_ref=intent.provider_ref,
            outcome=EffectOutcome.APPLIED,
            attempts=1,
            observed_fingerprint=fingerprint,
            provider_version=1,
            observed_at=NOW,
            detail_code="private_notebook_exact_readback",
        )
        return NotebookMutationResult(source=source_identity, run=run, claim=claim, effect=receipt)

    def read_run_status(self, run):
        return KaggleKernelStatus(
            run=run, state=KernelState.RUNNING, provider_status="running", observed_at=NOW
        )


def test_launcher_persists_then_attaches_exact_private_status_dataset(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger)
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,  # type: ignore[arg-type]
        deployment=_deployment(),
        control_token_root="control-owned-root-value-that-is-not-a-kaggle-secret",
    )

    result = launcher.launch_checkpoint_acceptance(_request())

    assert result.state == "RUNNING"
    assert result.status_input is not None and not result.status_input.cleaned
    assert result.status_input.exact_version_ref in adapter.dataset_sources
    assert "owner/checkpoint-runtime/9" in adapter.dataset_sources
    assert "owner/template/3" in adapter.dataset_sources
    status = json.loads(adapter.status_files["kaggle_run.json"])
    token = status["token"]
    assert status["run_id"] == str(TASK) and status["attempt_id"] == str(ATTEMPT)
    assert status["resource_leases"] == []
    assert token not in adapter.source.decode()
    assert "control-owned-root-value" not in adapter.source.decode()
    assert "MDH_KAGGLE_USERNAME" in adapter.source.decode()
    row = ledger.checkpoint_acceptance_launch(str(TASK))
    assert row is not None
    assert token not in json.dumps(row, sort_keys=True)
    assert row["token_sha256"] == hashlib.sha256(token.encode()).hexdigest()
    assert ledger.authenticate_checkpoint_acceptance(
        request_id=str(TASK), attempt_id=str(ATTEMPT), token=token
    ) is not None
    assert ledger.authenticate_checkpoint_acceptance(
        request_id=str(TASK), attempt_id=str(ATTEMPT), token="wrong-token"
    ) is None
    ledger.clock.advance(901)  # type: ignore[attr-defined]
    assert ledger.authenticate_checkpoint_acceptance(
        request_id=str(TASK), attempt_id=str(ATTEMPT), token=token
    ) is None


def test_lost_push_response_is_not_retried_or_status_input_deleted(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger, lose_push=True)
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,  # type: ignore[arg-type]
        deployment=_deployment(),
        control_token_root="control-owned-root-value-that-is-not-a-kaggle-secret",
    )
    request = _request()

    with pytest.raises(RuntimeError, match="lost provider response"):
        launcher.launch_checkpoint_acceptance(request)
    replay = launcher.launch_checkpoint_acceptance(request)

    assert replay.state == "FAIL"
    assert replay.failure_code == "CHECKPOINT_PUSH_RESPONSE_AMBIGUOUS"
    assert adapter.push_calls == 1
    row = ledger.checkpoint_acceptance_launch(str(TASK))
    assert row is not None and row["cleanup_receipt"] is None
