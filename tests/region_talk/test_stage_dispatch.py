from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from pydantic import SecretStr

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.contracts import (
    KaggleAmbiguousMutation,
    KaggleKernelRunIdentity,
    KaggleKernelStatus,
    KernelState,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.workloads.region_talk.central_launcher import (
    RegionTalkStageWorkerAttestation,
    RegionTalkStageWorkerTerminal,
    render_region_talk_stage_worker_source,
)
from my_data_hub.workloads.region_talk.notebook_stages import (
    RegionTalkStageRuntimeUnavailable,
    execute_direct_region_talk_stage_worker,
    process_region_talk_stage_item,
    stage_model_identity,
)
from my_data_hub.workloads.region_talk.pipeline_contracts import (
    RegionTalkDirectMasterAccess,
    TaskWorkerCredentialCommand,
)
from my_data_hub.workloads.region_talk.production_assembly import (
    CentralRegionTalkStageNotebookAdapter,
)
from my_data_hub.workloads.region_talk.stage_dispatch import (
    PostgresStageSupervisorFunctions,
    PostgresStageWorkerFunctions,
    PrivateSupervisorStageCoordinator,
    ProviderObservationKind,
    RegionTalkStageDispatcher,
    StageMetadataClaimRequest,
    StageProviderLaunchReceipt,
    StageProviderObservation,
    StageWorkerBindingReceipt,
    StageWorkerCredentialStatus,
    StageWorkerDirectResultReceipt,
    StageWorkerLaunch,
    StageWorkerPayloadFetchRequest,
    StageWorkerRotateRequest,
    StageWorkMetadataClaimReceipt,
    StageWorkPayloadReceipt,
    stage_dispatch_id,
    stage_effect_id,
    stage_worker_task_run_id,
)
from my_data_hub.workloads.region_talk.stage_execution import stage_run_id, work_item_id

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
SUPERVISOR = UUID("11111111-1111-4111-8111-111111111111")
BATCH = UUID("22222222-2222-4222-8222-222222222222")
MASTER = UUID("33333333-3333-4333-8333-333333333333")
SUBJECT = UUID("44444444-4444-4444-8444-444444444444")
CONTENT = UUID("55555555-5555-4555-8555-555555555555")
RUN = stage_run_id(SUPERVISOR, BATCH)
INPUT = hashlib.sha256(b"input").hexdigest()
WORK = work_item_id(
    run_id=RUN,
    candidate_id=SUBJECT,
    revision=1,
    stage="e5_embedding",
    input_fingerprint=INPUT,
)


def _sha(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _metadata_claim() -> dict[str, Any]:
    effect = stage_effect_id(work_item_id=WORK, attempt=1, input_fingerprint=INPUT)
    worker = stage_worker_task_run_id(supervisor_task_run_id=SUPERVISOR, work_item_id=WORK, attempt=1)
    dispatch = stage_dispatch_id(
        supervisor_task_run_id=SUPERVISOR,
        work_item_id=WORK,
        attempt=1,
        input_fingerprint=INPUT,
    )
    body = {
        "schema_version": "region-talk-stage-work-metadata-claim-receipt.v2",
        "status": "CLAIMED",
        "supervisor_task_run_id": str(SUPERVISOR),
        "export_batch_id": str(BATCH),
        "stage_run_id": str(RUN),
        "master_instance_id": str(MASTER),
        "epoch": 7,
        "work_item_id": str(WORK),
        "effect_id": str(effect),
        "dispatch_id": str(dispatch),
        "worker_task_run_id": str(worker),
        "stage": "e5_embedding",
        "contract_version": "e5_semantic_bank_scores_v1",
        "subject_type": "region_talk.candidate",
        "subject_id": str(SUBJECT),
        "input_fingerprint": INPUT,
        "attempt": 1,
        "max_attempts": 3,
        "timeout_seconds": 900,
        "lease_expires_at": (NOW + timedelta(seconds=900)).isoformat(),
        "lease_token_sha256": "a" * 64,
        "lease_capability_sha256": "b" * 64,
        "claim_receipt_sha256": "f" * 64,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _binding(claim: dict[str, Any]) -> dict[str, Any]:
    body = {
        "schema_version": "region-talk-stage-worker-bind-receipt.v1",
        "bound": True,
        "supervisor_task_run_id": claim["supervisor_task_run_id"],
        "export_batch_id": claim["export_batch_id"],
        "stage_run_id": claim["stage_run_id"],
        "work_item_id": claim["work_item_id"],
        "effect_id": claim["effect_id"],
        "dispatch_id": claim["dispatch_id"],
        "worker_task_run_id": claim["worker_task_run_id"],
        "master_instance_id": claim["master_instance_id"],
        "epoch": claim["epoch"],
        "worker_credential_id": "66666666-6666-4666-8666-666666666666",
        "worker_generation": 1,
        "lease_capability_sha256": claim["lease_capability_sha256"],
        "worker_binding_sha256": "c" * 64,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


class _Adapter:
    def __init__(self) -> None:
        self.launches = []
        self.receipt = None

    def observe(self, launch):  # type: ignore[no-untyped-def]
        if self.receipt is None:
            return StageProviderObservation(kind=ProviderObservationKind.ABSENT)
        return StageProviderObservation(kind=ProviderObservationKind.RUNNING, launch_receipt=self.receipt)

    def launch(self, launch):  # type: ignore[no-untyped-def]
        self.launches.append(launch)
        body = {
            "schema_version": "region-talk-stage-provider-launch-receipt.v1",
            "effect_id": str(launch.effect_id),
            "dispatch_id": str(launch.dispatch_id),
            "worker_task_run_id": str(launch.worker_task_run_id),
            "notebook_ref": launch.notebook_ref,
            "provider_run_ref": f"{launch.notebook_ref}/9",
            "source_sha256": "d" * 64,
            "launched_at": NOW.isoformat(),
        }
        self.receipt = StageProviderLaunchReceipt(**body, receipt_sha256=_sha(body))
        return self.receipt


def test_metadata_dispatch_restarts_without_duplicate_or_business_journal(tmp_path: Path) -> None:
    claim = _metadata_claim()
    binding = _binding(claim)
    adapter = _Adapter()
    path = tmp_path / "journal.json"
    first = RegionTalkStageDispatcher(adapter, "owner", path)
    observed = first.dispatch_bound(claim, binding)
    assert observed.kind is ProviderObservationKind.RUNNING
    assert len(adapter.launches) == 1
    launch = adapter.launches[0]
    dispatch_id = UUID(claim["dispatch_id"])
    assert launch.notebook_ref == f"owner/mdh-rt-run-{dispatch_id.hex[:24]}"
    assert not hasattr(launch, "payload")

    replay = RegionTalkStageDispatcher(adapter, "owner", path)
    replay.dispatch_bound(claim, binding)
    assert len(adapter.launches) == 1
    serialized = path.read_bytes()
    for forbidden in (b'"payload"', b'"input_data"', b'"text"', b'"lease_token"', b'"database_url"'):
        assert forbidden not in serialized
    assert b'"lease_token_sha256"' not in serialized


class _StageAuthority:
    def __init__(self, root: Path, claim: StageWorkMetadataClaimReceipt) -> None:
        self.root = root
        self.claim = claim
        self.token = "stage-private-" + "x" * 40
        self.command = TaskWorkerCredentialCommand.create(
            task_run_id=claim.worker_task_run_id,
            epoch=claim.epoch,
            generation=1,
            task_token_sha256=hashlib.sha256(self.token.encode()).hexdigest(),
        )
        self.source_sha256 = ""
        self.revoked = 0
        self.terminal = False
        self.access = RegionTalkDirectMasterAccess(
            credential_id=UUID("66666666-6666-4666-8666-666666666666"),
            task_run_id=claim.worker_task_run_id,
            master_instance_id=claim.master_instance_id,
            epoch=claim.epoch,
            generation=1,
            command_sha256=self.command.command_sha256,
            task_token_sha256=self.command.task_token_sha256,
            database_url=SecretStr("postgresql://worker:private@tunnel:25432/hub"),
            tls_ca_pem=SecretStr("ca"),
            expires_at=NOW + timedelta(hours=8),
            tunnel_endpoint="tunnel:25432",
            ssh_private_key=SecretStr("key"),
            ssh_certificate=SecretStr("cert"),
            ssh_known_hosts=SecretStr("known"),
            ssh_gateway_host="gateway.internal",
            ssh_gateway_port=2222,
            ssh_account="mdh-region-talk",
            ssh_certificate_serial=88,
        )

    def stage_worker_claim(self, _task):  # type: ignore[no-untyped-def]
        return self.claim

    def stage_worker_command(self, _task):  # type: ignore[no-untyped-def]
        return self.command

    def stage_worker_active_access(self, _task):  # type: ignore[no-untyped-def]
        return self.access

    def task_token(self, _task):  # type: ignore[no-untyped-def]
        return self.token

    def validate_token(self, _task, supplied):  # type: ignore[no-untyped-def]
        assert supplied == self.token
        return {
            "source_sha256": self.source_sha256,
            "image_identity": "runtime@sha256:" + "d" * 64,
            "image_source_commit": "e" * 40,
            "terminal": self.terminal,
        }

    def request_stage_worker_revocation(self, _task):  # type: ignore[no-untyped-def]
        self.revoked += 1
        self.terminal = True


class _LostStageProvider:
    def __init__(self) -> None:
        self.dataset_present = False
        self.notebook_present = False
        self.dataset_creates = 0
        self.notebook_pushes = 0
        self.deletes = 0
        self.run: KaggleKernelRunIdentity | None = None
        self.status_state = KernelState.RUNNING
        self.status_observed_at = NOW

    @staticmethod
    def claim(intent, kind):  # type: ignore[no-untyped-def]
        return TaskResourceClaim.create(
            task_id=intent.task_id,
            effect_id=intent.effect_id,
            provider_ref=intent.provider_ref,
            kind=kind,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED,
            disposable=True,
            fingerprint=ProviderFingerprint(value=("a" if kind is ProviderKind.DATASET else "b") * 64),
            provider_version=1,
            registered_at=intent.requested_at,
        )

    def current_private_dataset_version(self, **_kwargs):  # type: ignore[no-untyped-def]
        return 1 if self.dataset_present else None

    def create_private_dataset(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.dataset_creates += 1
        self.dataset_present = True
        raise KaggleAmbiguousMutation("lost dataset response")

    def reconcile_private_dataset_directory_mutation(self, *, intent, **_kwargs):  # type: ignore[no-untyped-def]
        return SimpleNamespace(claim=self.claim(intent, ProviderKind.DATASET))

    def reconcile_private_notebook_mutation(self, *, intent, **_kwargs):  # type: ignore[no-untyped-def]
        if not self.notebook_present:
            return None
        claim = self.claim(intent, ProviderKind.NOTEBOOK)
        self.run = self.run or KaggleKernelRunIdentity(
            task_run_id=intent.task_id,
            provider_ref=claim.provider_ref,
            source_version=1,
            source_sha256=_kwargs["expected_source_sha256"],
            provider_kernel_id=81,
            provider_run_ref=f"{claim.provider_ref}/1",
            started_at=NOW,
        )
        return SimpleNamespace(
            claim=claim,
            run=self.run,
        )

    def reconcile_private_notebook_run(self, *, task_run_id, provider_ref, expected_source_sha256):  # type: ignore[no-untyped-def]
        if not self.notebook_present:
            return None
        self.run = self.run or KaggleKernelRunIdentity(
            task_run_id=task_run_id,
            provider_ref=provider_ref,
            source_version=1,
            source_sha256=expected_source_sha256,
            provider_kernel_id=81,
            provider_run_ref=f"{provider_ref}/1",
            started_at=NOW,
        )
        return self.run

    def read_run_status(self, run):  # type: ignore[no-untyped-def]
        return KaggleKernelStatus(
            run=run,
            state=self.status_state,
            provider_status=self.status_state.value,
            observed_at=self.status_observed_at,
        )

    def push_private_worker_notebook_pending_attestation(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.notebook_pushes += 1
        self.notebook_present = True
        raise KaggleAmbiguousMutation("lost notebook response")

    def delete_task_created_resource(self, **_kwargs):  # type: ignore[no-untyped-def]
        self.deletes += 1


def test_concrete_stage_adapter_reconciles_each_lost_effect_with_original_intent(
    tmp_path: Path,
) -> None:
    claim = StageWorkMetadataClaimReceipt.model_validate(_metadata_claim())
    binding = StageWorkerBindingReceipt.model_validate(_binding(claim.model_dump(mode="json")))
    launch = StageWorkerLaunch(
        supervisor_task_run_id=claim.supervisor_task_run_id,
        export_batch_id=claim.export_batch_id,
        master_instance_id=claim.master_instance_id,
        epoch=claim.epoch,
        stage_run_id=claim.stage_run_id,
        work_item_id=claim.work_item_id,
        effect_id=claim.effect_id,
        dispatch_id=claim.dispatch_id,
        worker_task_run_id=claim.worker_task_run_id,
        attempt=claim.attempt,
        stage=claim.stage,
        contract_version=claim.contract_version,
        notebook_ref=f"owner/mdh-rt-run-{claim.dispatch_id.hex[:24]}",
        input_fingerprint=claim.input_fingerprint,
        timeout_seconds=claim.timeout_seconds,
        claim_receipt_sha256=claim.claim_receipt_sha256,
        worker_binding_sha256=binding.worker_binding_sha256,
        lease_capability_sha256=claim.lease_capability_sha256,
    )
    root = (tmp_path / "private").absolute()
    root.mkdir(mode=0o700)
    authority = _StageAuthority(root, claim)
    provider = _LostStageProvider()

    def adapter(now: datetime) -> CentralRegionTalkStageNotebookAdapter:
        value = CentralRegionTalkStageNotebookAdapter(
            adapter=provider,
            authority=authority,  # type: ignore[arg-type]
            credential_broker=SimpleNamespace(),  # type: ignore[arg-type]
            owner="owner",
            callback_base_url="https://control.example",
            runtime_dataset_exact_ref="owner/runtime/12",
            runtime_image_identity="runtime@sha256:" + "d" * 64,
            runtime_image_source_commit="e" * 40,
            wheel_relative_path="dist/my_data_hub.whl",
            wheel_sha256="f" * 64,
            dependency_manifest_sha256="9" * 64,
            clock=lambda: now,
        )
        authority.source_sha256 = hashlib.sha256(value.source_for_claim(claim)).hexdigest()
        return value

    with pytest.raises(KaggleAmbiguousMutation):
        adapter(NOW).launch(launch)
    journal_path = root / "stage-provider-metadata.json"
    original = json.loads(journal_path.read_bytes())["entries"][str(claim.dispatch_id)][
        "dataset_intent"
    ]["requested_at"]
    with pytest.raises(KaggleAmbiguousMutation):
        adapter(NOW + timedelta(hours=1)).launch(launch)
    completed_adapter = adapter(NOW + timedelta(hours=2))
    receipt = completed_adapter.launch(launch)
    assert receipt.worker_task_run_id == claim.worker_task_run_id
    assert provider.dataset_creates == 1
    assert provider.notebook_pushes == 1
    encoded = journal_path.read_bytes()
    assert json.loads(encoded)["entries"][str(claim.dispatch_id)]["dataset_intent"][
        "requested_at"
    ] == original
    for forbidden in (
        b'"payload"',
        b'"input_data"',
        b'"text"',
        b'"lease_token"',
        b'"database_url"',
        b'"task_token"',
    ):
        assert forbidden not in encoded
    model_sha = hashlib.sha256(
        canonical_json_bytes(stage_model_identity(claim.stage))
    ).hexdigest()
    completed_adapter.record_attestation(
        RegionTalkStageWorkerAttestation(
            worker_task_run_id=str(claim.worker_task_run_id),
            dispatch_id=str(claim.dispatch_id),
            effect_id=str(claim.effect_id),
            master_instance_id=str(claim.master_instance_id),
            epoch=claim.epoch,
            source_sha256=receipt.source_sha256,
            image_identity="runtime@sha256:" + "d" * 64,
            image_source_commit="e" * 40,
            wheel_sha256="f" * 64,
            dependency_manifest_sha256="9" * 64,
            model_inputs_sha256=model_sha,
            attested_at=NOW,
        )
    )
    terminal = RegionTalkStageWorkerTerminal(
        worker_task_run_id=str(claim.worker_task_run_id),
        dispatch_id=str(claim.dispatch_id),
        effect_id=str(claim.effect_id),
        master_instance_id=str(claim.master_instance_id),
        epoch=claim.epoch,
        result_status="SUCCEEDED",
        result_receipt_sha256="7" * 64,
        completed_at=NOW,
    )
    completed_adapter.complete(terminal)
    completed_adapter.complete(terminal)
    assert provider.deletes == 2
    assert authority.revoked == 1


def test_generated_direct_stage_worker_is_offline_pinned_and_compiles() -> None:
    claim = StageWorkMetadataClaimReceipt.model_validate(_metadata_claim())
    source = render_region_talk_stage_worker_source(
        claim,
        runtime_image_identity="runtime@sha256:" + "d" * 64,
        runtime_image_source_commit="e" * 40,
        wheel_relative_path="dist/my_data_hub.whl",
        wheel_sha256="f" * 64,
        dependency_manifest_sha256="9" * 64,
        model_inputs={"model_id": "exact", "model_revision": "abc"},
    )
    compile(source, "region_talk_stage_worker.py", "exec")
    for required in (
        b"--no-index",
        b"--no-deps",
        b"PostgresStageWorkerFunctions",
        b"execute_direct_region_talk_stage_worker",
        b"region-talk-stage-worker-rotation-checkpoint.v1",
        b"publication_dispatch':False",
        b"notification_dispatch':False",
    ):
        assert required in source
    install_offset = source.index(b"pip','install")
    attestation_offset = source.index(b"stage/attestation")
    forced_rotation_offset = source.index(b"force=True")
    access_offset = source.index(b"stage/rotation/access")
    materialize_offset = source.index(
        b"replacement_functions,replacement_tunnel,replacement_connection=materialize(replacement)"
    )
    worker_offset = source.index(b"execute_direct_region_talk_stage_worker")
    assert install_offset < attestation_offset < forced_rotation_offset < worker_offset
    assert access_offset < materialize_offset < forced_rotation_offset
    assert b"materialize(access)" not in source
    assert b"post_with_exact_replay('/internal/region-talk-pipeline/stage/terminal'" in source


@pytest.mark.parametrize(
    ("provider_state", "elapsed_seconds", "expected_status"),
    (
        (KernelState.FAILED, 10, "FAILED_RETRYABLE"),
        (KernelState.RUNNING, 901, "FAILED_RETRYABLE"),
        (KernelState.COMPLETE, 10, "SUCCEEDED"),
    ),
)
def test_stage_provider_terminal_reaper_is_restart_safe_and_metadata_only(
    tmp_path: Path,
    provider_state: KernelState,
    elapsed_seconds: int,
    expected_status: str,
) -> None:
    claim = StageWorkMetadataClaimReceipt.model_validate(_metadata_claim())
    binding = StageWorkerBindingReceipt.model_validate(_binding(claim.model_dump(mode="json")))
    launch = StageWorkerLaunch(
        supervisor_task_run_id=claim.supervisor_task_run_id,
        export_batch_id=claim.export_batch_id,
        master_instance_id=claim.master_instance_id,
        epoch=claim.epoch,
        stage_run_id=claim.stage_run_id,
        work_item_id=claim.work_item_id,
        effect_id=claim.effect_id,
        dispatch_id=claim.dispatch_id,
        worker_task_run_id=claim.worker_task_run_id,
        attempt=claim.attempt,
        stage=claim.stage,
        contract_version=claim.contract_version,
        notebook_ref=f"owner/mdh-rt-run-{claim.dispatch_id.hex[:24]}",
        input_fingerprint=claim.input_fingerprint,
        timeout_seconds=claim.timeout_seconds,
        claim_receipt_sha256=claim.claim_receipt_sha256,
        worker_binding_sha256=binding.worker_binding_sha256,
        lease_capability_sha256=claim.lease_capability_sha256,
    )
    root = (tmp_path / "private").absolute()
    root.mkdir(mode=0o700)
    authority = _StageAuthority(root, claim)
    provider = _LostStageProvider()
    provider.dataset_present = True
    provider.notebook_present = True

    def make(now: datetime) -> CentralRegionTalkStageNotebookAdapter:
        value = CentralRegionTalkStageNotebookAdapter(
            adapter=provider,
            authority=authority,  # type: ignore[arg-type]
            credential_broker=SimpleNamespace(),  # type: ignore[arg-type]
            owner="owner",
            callback_base_url="https://control.example",
            runtime_dataset_exact_ref="owner/runtime/12",
            runtime_image_identity="runtime@sha256:" + "d" * 64,
            runtime_image_source_commit="e" * 40,
            wheel_relative_path="dist/my_data_hub.whl",
            wheel_sha256="f" * 64,
            dependency_manifest_sha256="9" * 64,
            clock=lambda: now,
        )
        authority.source_sha256 = hashlib.sha256(value.source_for_claim(claim)).hexdigest()
        return value

    make(NOW).launch(launch)
    provider.status_state = provider_state
    provider.status_observed_at = NOW + timedelta(seconds=elapsed_seconds)
    restarted = make(NOW + timedelta(seconds=elapsed_seconds))
    assert restarted.reconcile_terminal_runs() == 1
    assert restarted.reconcile_terminal_runs() == 0
    assert restarted.observe(launch).kind is ProviderObservationKind.TERMINAL
    entry = json.loads((root / "stage-provider-metadata.json").read_bytes())["entries"][
        str(claim.dispatch_id)
    ]
    assert entry["terminal"]["result_status"] == expected_status
    assert entry["cleaned"] is True
    assert provider.deletes == 2
    assert authority.revoked == 1
    encoded = canonical_json_bytes(entry)
    for forbidden in (b'"payload"', b'"text"', b'"task_token"', b'"database_url"'):
        assert forbidden not in encoded


def test_claim_and_binding_tamper_fail_before_launch(tmp_path: Path) -> None:
    claim = _metadata_claim()
    binding = _binding(claim)
    adapter = _Adapter()
    claim["input_fingerprint"] = "0" * 64
    with pytest.raises(ValueError, match="receipt_sha256"):
        RegionTalkStageDispatcher(adapter, "owner", tmp_path / "a.json").dispatch_bound(claim, binding)
    assert adapter.launches == []


def test_worker_and_dispatch_ids_are_replay_stable() -> None:
    claim = StageWorkMetadataClaimReceipt.model_validate(_metadata_claim())
    assert claim.worker_task_run_id == stage_worker_task_run_id(
        supervisor_task_run_id=SUPERVISOR, work_item_id=WORK, attempt=1
    )
    assert claim.dispatch_id == stage_dispatch_id(
        supervisor_task_run_id=SUPERVISOR,
        work_item_id=WORK,
        attempt=1,
        input_fingerprint=INPUT,
    )


class _AttachedRuntime:
    producer_exact_id = "verified-runtime@example@sha256:" + "9" * 64

    def execute(self, **kwargs):  # type: ignore[no-untyped-def]
        payload = kwargs["payload"]
        text = payload.input_data["text"]
        assert payload.input_data["text_sha256"] == hashlib.sha256(text.encode()).hexdigest()
        return {"verified": True, "input_sha256": hashlib.sha256(text.encode()).hexdigest()}


def _worker_item() -> dict[str, Any]:
    text = "Калининградский музей"
    return {
        "work_item_id": str(WORK),
        "subject_type": "region_talk.candidate",
        "subject_id": str(SUBJECT),
        "input_fingerprint": INPUT,
        "payload": {
            "schema_version": "region-talk-stage-work-execution.v1",
            "stage_run_id": str(RUN),
            "candidate_id": str(SUBJECT),
            "candidate_revision": 1,
            "revision_fingerprint": "e" * 64,
            "content_id": str(CONTENT),
            "content_type": "article",
            "canonical_url": "https://example.org/a",
            "canonical_source_key": "web:example.org",
            "input_fingerprint": INPUT,
            "upstream_results": [],
            "input_data": {
                "schema_version": "region-talk-stage-text-input.v1",
                "text": text,
                "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
                "topics": ["museum"],
            },
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    }


def test_private_worker_executes_attached_runtime_but_missing_runtime_is_retryable() -> None:
    item = _worker_item()
    with pytest.raises(RegionTalkStageRuntimeUnavailable) as failure:
        process_region_talk_stage_item(
            item,
            stage="e5_embedding",
            contract_version="e5_semantic_bank_scores_v1",
        )
    assert failure.value.retryable is True
    result = process_region_talk_stage_item(
        item,
        stage="e5_embedding",
        contract_version="e5_semantic_bank_scores_v1",
        runtime=_AttachedRuntime(),
    )
    assert result["subject_id"] == str(SUBJECT)
    assert result["input_fingerprint"] == INPUT
    assert result["metrics"]["verified"] is True


def test_metadata_models_reject_raw_payload_and_lease() -> None:
    claim = _metadata_claim()
    claim["payload"] = {"text": "business"}
    with pytest.raises(ValueError):
        StageWorkMetadataClaimReceipt.model_validate(claim)
    binding = _binding(_metadata_claim())
    binding["lease_token"] = "secret"
    with pytest.raises(ValueError):
        StageWorkerBindingReceipt.model_validate(binding)


def _payload_receipt() -> StageWorkPayloadReceipt:
    claim = _metadata_claim()
    body = {
        "schema_version": "region-talk-stage-work-payload-receipt.v1",
        "master_instance_id": claim["master_instance_id"],
        "epoch": claim["epoch"],
        "supervisor_task_run_id": claim["supervisor_task_run_id"],
        "worker_task_run_id": claim["worker_task_run_id"],
        "export_batch_id": claim["export_batch_id"],
        "stage_run_id": claim["stage_run_id"],
        "dispatch_id": claim["dispatch_id"],
        "work_item_id": claim["work_item_id"],
        "effect_id": claim["effect_id"],
        "stage": claim["stage"],
        "contract_version": claim["contract_version"],
        "subject_type": claim["subject_type"],
        "subject_id": claim["subject_id"],
        "input_fingerprint": claim["input_fingerprint"],
        "attempt": claim["attempt"],
        "lease_token": "77777777-7777-4777-8777-777777777777",
        "lease_expires_at": claim["lease_expires_at"],
        "worker_binding_sha256": "c" * 64,
        "payload": _worker_item()["payload"],
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    return StageWorkPayloadReceipt.model_validate({**body, "receipt_sha256": _sha(body)})


class _DirectFunctions:
    def __init__(self) -> None:
        self.fetched = _payload_receipt()
        self.submissions = []

    def fetch_payload(self, **kwargs):  # type: ignore[no-untyped-def]
        assert kwargs["worker_task_run_id"] == self.fetched.worker_task_run_id
        assert kwargs["effect_id"] == self.fetched.effect_id
        return self.fetched

    def submit_result(self, **kwargs):  # type: ignore[no-untyped-def]
        request = kwargs["request"]
        self.submissions.append(request)
        body = {
            "schema_version": "region-talk-stage-worker-direct-result-receipt.v1",
            "accepted": True,
            "master_instance_id": str(self.fetched.master_instance_id),
            "epoch": self.fetched.epoch,
            "supervisor_task_run_id": str(self.fetched.supervisor_task_run_id),
            "worker_task_run_id": str(self.fetched.worker_task_run_id),
            "export_batch_id": str(self.fetched.export_batch_id),
            "stage_run_id": str(self.fetched.stage_run_id),
            "dispatch_id": str(self.fetched.dispatch_id),
            "work_item_id": str(self.fetched.work_item_id),
            "effect_id": str(self.fetched.effect_id),
            "stage": self.fetched.stage,
            "subject_id": str(self.fetched.subject_id),
            "input_fingerprint": self.fetched.input_fingerprint,
            "attempt": self.fetched.attempt,
            "result_status": request.result_status.value,
            "metadata_sha256": request.metadata_sha256,
            "result_sha256": request.result_sha256,
            "worker_binding_sha256": request.worker_binding_sha256,
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        return StageWorkerDirectResultReceipt.model_validate(
            {**body, "receipt_sha256": _sha(body)}
        )


def test_private_worker_fetches_and_submits_directly_and_unavailable_is_retryable() -> None:
    functions = _DirectFunctions()
    fetched = functions.fetched
    receipt = execute_direct_region_talk_stage_worker(
        functions,
        StageWorkerPayloadFetchRequest(
            worker_task_run_id=fetched.worker_task_run_id,
            dispatch_id=fetched.dispatch_id,
            effect_id=fetched.effect_id,
            worker_binding_sha256=fetched.worker_binding_sha256,
            requested_at=NOW,
        ),
        clock=lambda: NOW,
    )
    assert receipt.result_status.value == "FAILED_RETRYABLE"
    assert functions.submissions[0].result_metadata.metrics["failure_code"] == (
        "HEAVY_RUNTIME_NOT_ATTACHED"
    )
    assert functions.submissions[0].publication_dispatch is False
    assert functions.submissions[0].notification_dispatch is False


def test_private_worker_attached_runtime_lands_exact_success() -> None:
    functions = _DirectFunctions()
    fetched = functions.fetched
    receipt = execute_direct_region_talk_stage_worker(
        functions,
        StageWorkerPayloadFetchRequest(
            worker_task_run_id=fetched.worker_task_run_id,
            dispatch_id=fetched.dispatch_id,
            effect_id=fetched.effect_id,
            worker_binding_sha256=fetched.worker_binding_sha256,
            requested_at=NOW,
        ),
        runtime=_AttachedRuntime(),
        clock=lambda: NOW,
    )
    assert receipt.result_status.value == "SUCCEEDED"
    submission = functions.submissions[0]
    assert submission.result_sha256 == submission.metadata_sha256
    assert submission.result_metadata.metrics["verified"] is True


def test_private_worker_rotates_at_bounded_checkpoint_before_submit() -> None:
    functions = _DirectFunctions()
    fetched = functions.fetched
    initial = StageWorkerPayloadFetchRequest(
        worker_task_run_id=fetched.worker_task_run_id,
        dispatch_id=fetched.dispatch_id,
        effect_id=fetched.effect_id,
        worker_binding_sha256=fetched.worker_binding_sha256,
        requested_at=NOW,
    )
    phases: list[str] = []

    def checkpoint(current_functions, request, *, phase):  # type: ignore[no-untyped-def]
        phases.append(phase)
        if phase == "before_submit":
            request = request.model_copy(update={"worker_binding_sha256": "9" * 64})
        return current_functions, request

    receipt = execute_direct_region_talk_stage_worker(
        functions,
        initial,
        runtime=_AttachedRuntime(),
        credential_checkpoint=checkpoint,
        clock=lambda: NOW,
    )
    assert phases == ["before_fetch", "before_submit"]
    assert receipt.worker_binding_sha256 == "9" * 64
    assert functions.submissions[0].worker_binding_sha256 == "9" * 64


class _Cursor:
    def __init__(self, response: dict[str, Any]) -> None:
        self.response = response
        self.calls: list[tuple[str, Any]] = []

    def __enter__(self):  # type: ignore[no-untyped-def]
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return None

    def execute(self, sql, params=None):  # type: ignore[no-untyped-def]
        self.calls.append((sql, params))
        return self

    def fetchone(self):  # type: ignore[no-untyped-def]
        return (self.response,)


class _Connection:
    def __init__(self, response: dict[str, Any]) -> None:
        self.value = _Cursor(response)
        self.commits = 0

    def cursor(self):  # type: ignore[no-untyped-def]
        return self.value

    def commit(self):
        self.commits += 1

    def rollback(self):
        raise AssertionError("unexpected rollback")


def test_fixed_0028_ports_set_pipeline_role_before_exact_functions() -> None:
    claim_value = _metadata_claim()
    supervisor_connection = _Connection(claim_value)
    supervisor = PostgresStageSupervisorFunctions(supervisor_connection)
    parsed = supervisor.claim_metadata(
        supervisor_task_run_id=SUPERVISOR,
        export_batch_id=BATCH,
        request=StageMetadataClaimRequest(
            claim_request_id=UUID("88888888-8888-4888-8888-888888888888"),
            lease_owner="private-supervisor",
            requested_at=NOW,
        ),
    )
    assert parsed.status == "CLAIMED"
    assert supervisor_connection.value.calls[0][0] == "SET LOCAL ROLE mdh_region_talk_pipeline"
    assert supervisor_connection.value.calls[1][0] == (
        "SELECT migration.claim_region_talk_stage_work_metadata(%s,%s,%s::jsonb)"
    )

    payload = _payload_receipt()
    payload_response = payload.model_dump(mode="json", exclude={"receipt_sha256"})
    payload_response["receipt_sha256"] = _sha(payload_response)
    worker_connection = _Connection(payload_response)
    worker = PostgresStageWorkerFunctions(worker_connection)
    worker.fetch_payload(
        worker_task_run_id=payload.worker_task_run_id,
        effect_id=payload.effect_id,
        request=StageWorkerPayloadFetchRequest(
            worker_task_run_id=payload.worker_task_run_id,
            dispatch_id=payload.dispatch_id,
            effect_id=payload.effect_id,
            worker_binding_sha256=payload.worker_binding_sha256,
            requested_at=NOW,
        ),
    )
    assert worker_connection.value.calls[0][0] == "SET LOCAL ROLE mdh_region_talk_pipeline"
    assert worker_connection.value.calls[1][0] == (
        "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)"
    )


def test_fixed_0029_rotation_port_rejects_generation_skip_and_calls_exact_function() -> None:
    claim = _metadata_claim()
    body = {
        "schema_version": "region-talk-stage-worker-rotate-receipt.v1",
        "rotated": True,
        "master_instance_id": claim["master_instance_id"],
        "epoch": claim["epoch"],
        "supervisor_task_run_id": claim["supervisor_task_run_id"],
        "export_batch_id": claim["export_batch_id"],
        "stage_run_id": claim["stage_run_id"],
        "dispatch_id": claim["dispatch_id"],
        "work_item_id": claim["work_item_id"],
        "effect_id": claim["effect_id"],
        "worker_task_run_id": claim["worker_task_run_id"],
        "prior_worker_generation": 1,
        "prior_worker_binding_sha256": "c" * 64,
        "worker_credential_id": "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        "worker_generation": 2,
        "worker_binding_sha256": "d" * 64,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    connection = _Connection({**body, "receipt_sha256": _sha(body)})
    supervisor = PostgresStageSupervisorFunctions(connection)
    request = StageWorkerRotateRequest(
        dispatch_id=UUID(claim["dispatch_id"]),
        effect_id=UUID(claim["effect_id"]),
        work_item_id=UUID(claim["work_item_id"]),
        worker_task_run_id=UUID(claim["worker_task_run_id"]),
        prior_worker_generation=1,
        prior_worker_binding_sha256="c" * 64,
        new_worker_credential_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        new_worker_generation=2,
        new_worker_command_sha256="e" * 64,
        new_worker_task_token_sha256="f" * 64,
        requested_at=NOW,
    )
    receipt = supervisor.rotate_worker(
        supervisor_task_run_id=SUPERVISOR,
        export_batch_id=BATCH,
        request=request,
    )
    assert receipt.worker_generation == 2
    assert connection.value.calls[1][0] == (
        "SELECT migration.rotate_region_talk_stage_worker_credential(%s,%s,%s::jsonb)"
    )
    with pytest.raises(ValueError, match="advance exactly one"):
        StageWorkerRotateRequest.model_validate(
            {**request.model_dump(mode="json"), "new_worker_generation": 3}
        )


def test_private_supervisor_replays_claim_until_child_is_bound_then_advances() -> None:
    claim = StageWorkMetadataClaimReceipt.model_validate(_metadata_claim())
    binding = StageWorkerBindingReceipt.model_validate(_binding(_metadata_claim()))

    class Functions:
        def __init__(self) -> None:
            self.requests = []
            self.binds = []

        def claim_metadata(self, **kwargs):  # type: ignore[no-untyped-def]
            self.requests.append(kwargs["request"])
            return claim

        def bind_worker(self, **kwargs):  # type: ignore[no-untyped-def]
            self.binds.append(kwargs["request"])
            return binding

    class Bridge:
        def __init__(self) -> None:
            self.calls = 0
            self.dispatched = []

        def prepare_worker(self, current):  # type: ignore[no-untyped-def]
            self.calls += 1
            return StageWorkerCredentialStatus(
                status="PENDING" if self.calls == 1 else "READY",
                dispatch_id=current.dispatch_id,
                effect_id=current.effect_id,
                worker_task_run_id=current.worker_task_run_id,
                worker_credential_id=(
                    None
                    if self.calls == 1
                    else UUID("66666666-6666-4666-8666-666666666666")
                ),
                worker_generation=None if self.calls == 1 else 1,
                worker_command_sha256="d" * 64,
                worker_task_token_sha256="e" * 64,
            )

        def dispatch_bound(self, current, exact_binding):  # type: ignore[no-untyped-def]
            self.dispatched.append((current, exact_binding))
            return StageProviderObservation(kind=ProviderObservationKind.RUNNING)

    functions = Functions()
    bridge = Bridge()
    coordinator = PrivateSupervisorStageCoordinator(
        functions=functions,  # type: ignore[arg-type]
        bridge=bridge,
        supervisor_task_run_id=SUPERVISOR,
        export_batch_id=BATCH,
        lease_owner="private-supervisor",
        clock=lambda: NOW,
    )
    coordinator.reconcile_next()
    coordinator.reconcile_next()
    assert len(functions.requests) == 2
    assert functions.requests[0].claim_request_id == functions.requests[1].claim_request_id
    assert len(functions.binds) == 1
    assert functions.binds[0].claim_receipt_sha256 == claim.claim_receipt_sha256
    assert bridge.dispatched == [(claim, binding)]
