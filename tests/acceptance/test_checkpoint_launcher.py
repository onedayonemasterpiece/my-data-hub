from __future__ import annotations

import hashlib
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
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
from my_data_hub.providers.kaggle import (
    KaggleContractError,
    KaggleProviderAdapter,
    KaggleProviderIdentity,
)
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
RUNTIME_IMAGE = "gcr.io/kaggle-images/python@sha256:" + "a" * 64
RUNTIME_IMAGE_COMMIT = "c" * 40


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
                "docker_image": RUNTIME_IMAGE,
                "docker_image_pinning_type": "original",
                "image_source_commit": RUNTIME_IMAGE_COMMIT,
                "python_series": "3.12",
            },
            "control_base_url": "https://control.example.test",
            "brokered_checkpoint_upload": True,
        }
    )


def _request(scenario: str = "FM14"):
    catalog: CheckpointAcceptanceLaunchCatalog = _deployment().catalog
    public = AcceptanceScenarioRequest(
        task_id=TASK,
        scenario=scenario,
        idempotency_key=f"checkpoint-{scenario.lower()}-owner-task",
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
    def __init__(
        self, ledger: ControlLedger, *, lose_push: bool = False, lose_status: bool = False
    ) -> None:
        self.ledger = ledger
        self.lose_push = lose_push
        self.lose_status = lose_status
        self.dataset_calls = 0
        self.push_calls = 0
        self.dataset_sources: tuple[str, ...] = ()
        self.status_files: dict[str, bytes] = {}
        self.source = b""
        self.push_kwargs: dict[str, object] = {}
        self.push_entered: threading.Event | None = None
        self.push_release: threading.Event | None = None

    def create_private_dataset(self, *, intent, files, title, control_class, disposable):
        self.dataset_calls += 1
        if self.lose_status:
            raise RuntimeError("simulated status-input crash")
        assert self.ledger.checkpoint_acceptance_launch(str(TASK)) is not None
        assert control_class is ControlClass.ORCHESTRATOR_PROTECTED and disposable is True
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

    def push_private_notebook_pending_runtime_attestation(
        self, *, intent, task_run_id, source, dataset_sources, **kwargs
    ):
        self.push_calls += 1
        self.source = source
        self.dataset_sources = tuple(dataset_sources)
        self.push_kwargs = dict(kwargs)
        assert intent.arguments["docker_image"] == kwargs["docker_image"]
        assert intent.arguments["docker_image_pinning_type"] == kwargs[
            "docker_image_pinning_type"
        ]
        if self.push_entered is not None:
            self.push_entered.set()
        if self.push_release is not None:
            assert self.push_release.wait(timeout=5)
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


class ContractJournal:
    def __init__(self) -> None:
        self.intents = []
        self.receipts = []
        self.claims = []

    def persist_intent(self, intent) -> None:  # type: ignore[no-untyped-def]
        self.intents.append(intent)

    def persist_receipt(self, receipt) -> None:  # type: ignore[no-untyped-def]
        self.receipts.append(receipt)

    def persist_resource_claim(self, claim) -> None:  # type: ignore[no-untyped-def]
        self.claims.append(claim)


class ContractKaggleApi:
    def __init__(self) -> None:
        self.calls = 0
        self.metadata: dict[str, object] | None = None

    def kernels_push(self, folder: str, timeout: str | None = None, acc: str | None = None):
        self.calls += 1
        self.metadata = json.loads((Path(folder) / "kernel-metadata.json").read_bytes())
        return SimpleNamespace(
            ref=self.metadata["id"], kernelId=991, versionNumber=7, error=""
        )


def test_production_default_blocks_before_status_dataset_or_notebook_mutation(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger)
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger, adapter=adapter, deployment=_deployment()  # type: ignore[arg-type]
    )

    result = launcher.launch_checkpoint_acceptance(_request())

    assert result.state == "BLOCKED"
    assert result.blocker_code == "CHECKPOINT_ACCEPTANCE_BROKERED_UPLOAD_NOT_ASSEMBLED"
    assert result.status_input is None
    assert adapter.dataset_calls == 0
    assert adapter.push_calls == 0
    replay = launcher.launch_checkpoint_acceptance(_request())
    assert replay == result


@pytest.mark.parametrize("scenario", ["FM05", "FM14", "FM15"])
def test_checkpoint_launch_contract_passes_real_adapter_image_preflight(
    tmp_path: Path, scenario: str
) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    journal = ContractJournal()
    api = ContractKaggleApi()
    adapter = KaggleProviderAdapter(
        api,  # type: ignore[arg-type]
        identity=KaggleProviderIdentity(username="owner"),
        journal=journal,  # type: ignore[arg-type]
        sleep=lambda _seconds: None,
        monotonic=lambda: 0.0,
        clock=lambda: NOW,
    )
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,
        deployment=_deployment(),
        brokered_upload_ready=True,
    )
    request = _request(scenario)
    pins = launcher._execution_pins(request)
    status_files = launcher._status_files(request, "f" * 64, execution_pins=pins)
    source = launcher._render_source(request, launcher._config(request, status_files))
    intent = launcher._notebook_intent(request, source)
    call = {
        "intent": intent,
        "task_run_id": request.task_run_id,
        "source": source,
        "title": request.evidence_notebook_ref.split("/", 1)[1],
        "code_file": "worker.py",
        "kernel_type": "script",
        "language": "python",
        "control_class": ControlClass.ORCHESTRATOR_PROTECTED,
        "disposable": False,
        "dataset_sources": launcher._dataset_sources(request),
        "enable_internet": True,
        "timeout_seconds": request.timeout_seconds,
    }

    with pytest.raises(KaggleContractError, match="exact original image digest"):
        adapter.push_private_notebook_pending_runtime_attestation(**call)
    assert api.calls == 0 and journal.intents == []

    result = adapter.push_private_notebook_pending_runtime_attestation(
        **call,
        docker_image=RUNTIME_IMAGE,
        docker_image_pinning_type="original",
    )

    assert result.run.task_run_id == TASK
    assert api.calls == 1
    assert api.metadata is not None
    assert api.metadata["is_private"] is True
    assert api.metadata["dataset_sources"] == list(launcher._dataset_sources(request))
    assert api.metadata["docker_image"] == RUNTIME_IMAGE
    assert api.metadata["docker_image_pinning_type"] == "original"
    assert journal.intents == [intent]
    assert len(journal.receipts) == 1 and len(journal.claims) == 1


def test_launcher_persists_then_attaches_exact_private_status_dataset(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger)
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,  # type: ignore[arg-type]
        deployment=_deployment(),
        brokered_upload_ready=True,
    )

    result = launcher.launch_checkpoint_acceptance(_request())

    assert result.state == "RUNNING"
    assert result.status_input is not None and not result.status_input.cleaned
    assert result.status_input.exact_version_ref in adapter.dataset_sources
    assert "owner/checkpoint-runtime/9" in adapter.dataset_sources
    assert "owner/template/3" in adapter.dataset_sources
    status = json.loads(adapter.status_files["kaggle_run.json"])
    pins_bytes = adapter.status_files["execution-pins.json"]
    pins = json.loads(pins_bytes)
    token = status["token"]
    assert status["run_id"] == str(TASK) and status["attempt_id"] == str(ATTEMPT)
    assert len(status["resource_leases"]) == 1
    assert status["execution_pins_sha256"] == hashlib.sha256(pins_bytes).hexdigest()
    assert pins == {
        "schema": "my-data-hub-checkpoint-acceptance-execution-pins/v1",
        "task_run_id": str(TASK),
        "notebook_ref": "owner/checkpoint-evidence",
        "python_series": "3.12",
        "image_source_commit": RUNTIME_IMAGE_COMMIT,
        "docker_image": RUNTIME_IMAGE,
        "docker_image_pinning_type": "original",
        "input_dataset_versions": list(adapter.dataset_sources),
        "privacy": "private",
        "source_attestation": "control_expected_source_sha256",
    }
    assert adapter.push_kwargs["docker_image"] == RUNTIME_IMAGE
    assert adapter.push_kwargs["docker_image_pinning_type"] == "original"
    assert status["resource_leases"][0]["resource_ref"] == "owner/checkpoint-evidence"
    assert status["resource_leases"][0]["holder_id"] == str(TASK)
    assert token not in adapter.source.decode()
    source_text = adapter.source.decode()
    assert "kaggle_secrets" not in source_text
    assert "KAGGLE_USERNAME" not in source_text
    assert "KAGGLE_KEY" not in source_text
    assert "KAGGLE_API_TOKEN" not in source_text
    for marker in (
        "kernel_started",
        "preflight_ok",
        "alive",
        "report_written",
        "resource_acquire",
        "resource_release",
        "kaggle_status_events.jsonl",
        "/internal/acceptance/events",
        "execution pins hash mismatch",
        "runtime image source commit mismatch",
        "runtime Python series mismatch",
        "attached private Dataset set differs from execution pins",
        "execution_pins_sha256",
    ):
        assert marker in adapter.source.decode()
    row = ledger.checkpoint_acceptance_launch(str(TASK))
    assert row is not None
    assert token not in json.dumps(row, sort_keys=True)
    assert row["token_sha256"] == hashlib.sha256(token.encode()).hexdigest()
    assert result.status_input.resource_lease.resource_ref == "owner/checkpoint-evidence"
    assert result.status_input.resource_lease.released is False
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
        brokered_upload_ready=True,
    )
    request = _request()

    with pytest.raises(RuntimeError, match="lost provider response"):
        launcher.launch_checkpoint_acceptance(request)
    replay = launcher.launch_checkpoint_acceptance(request)

    assert replay.state == "REQUESTED"
    ledger.clock.advance(901)  # type: ignore[attr-defined]
    replay = launcher.launch_checkpoint_acceptance(request)

    assert replay.state == "FAIL"
    assert replay.failure_code == "CHECKPOINT_PUSH_RESPONSE_AMBIGUOUS"
    assert adapter.push_calls == 1
    row = ledger.checkpoint_acceptance_launch(str(TASK))
    assert row is not None and row["cleanup_receipt"] is None


def test_crash_after_launch_intent_never_regenerates_token_or_mutates(tmp_path: Path) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger, lose_status=True)
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,  # type: ignore[arg-type]
        deployment=_deployment(),
        brokered_upload_ready=True,
    )
    request = _request()

    with pytest.raises(RuntimeError, match="status-input crash"):
        launcher.launch_checkpoint_acceptance(request)
    stored = ledger.checkpoint_acceptance_launch(str(TASK))
    assert stored is not None and stored["status_dataset"] is None
    token_hash = stored["token_sha256"]
    assert launcher.launch_checkpoint_acceptance(request).state == "REQUESTED"
    assert ledger.checkpoint_acceptance_launch(str(TASK))["token_sha256"] == token_hash  # type: ignore[index]
    assert adapter.dataset_calls == 1
    ledger.clock.advance(901)  # type: ignore[attr-defined]
    terminal = launcher.launch_checkpoint_acceptance(request)
    assert terminal.state == "FAIL"
    assert terminal.failure_code == "CHECKPOINT_STATUS_INPUT_RESPONSE_AMBIGUOUS"
    assert adapter.dataset_calls == 1


def test_twenty_concurrent_exact_requests_create_one_status_input_and_one_run(
    tmp_path: Path, monkeypatch
) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger)
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,  # type: ignore[arg-type]
        deployment=_deployment(),
        brokered_upload_ready=True,
    )
    request = _request()
    barrier = threading.Barrier(20)
    counter = iter(range(20))

    def candidate_token(_size: int) -> str:
        value = next(counter)
        barrier.wait(timeout=5)
        return f"{value:064x}"

    monkeypatch.setattr(
        "my_data_hub.acceptance.checkpoint_launcher.secrets.token_hex", candidate_token
    )
    with ThreadPoolExecutor(max_workers=20) as pool:
        states = list(pool.map(lambda _: launcher.launch_checkpoint_acceptance(request).state, range(20)))

    assert set(states) <= {"REQUESTED", "RUNNING"}
    assert adapter.dataset_calls == 1
    assert adapter.push_calls == 1


def test_follower_does_not_terminalize_creator_between_status_record_and_push(
    tmp_path: Path,
) -> None:
    ledger = ControlLedger(tmp_path / "ledger.sqlite3", clock=DeterministicClock(NOW))
    adapter = FakeAdapter(ledger)
    adapter.push_entered = threading.Event()
    adapter.push_release = threading.Event()
    launcher = ControlCheckpointAcceptanceLauncher(
        ledger=ledger,
        adapter=adapter,  # type: ignore[arg-type]
        deployment=_deployment(),
        brokered_upload_ready=True,
    )
    request = _request()
    with ThreadPoolExecutor(max_workers=1) as pool:
        creator = pool.submit(launcher.launch_checkpoint_acceptance, request)
        assert adapter.push_entered.wait(timeout=5)
        follower = launcher.launch_checkpoint_acceptance(request)
        assert follower.state == "REQUESTED"
        adapter.push_release.set()
        assert creator.result(timeout=5).state == "RUNNING"

    assert adapter.dataset_calls == 1
    assert adapter.push_calls == 1
