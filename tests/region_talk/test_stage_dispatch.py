from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.notebook_stages import (
    RegionTalkStageRuntimeUnavailable,
    execute_direct_region_talk_stage_worker,
    process_region_talk_stage_item,
)
from my_data_hub.workloads.region_talk.stage_dispatch import (
    PostgresStageSupervisorFunctions,
    PostgresStageWorkerFunctions,
    ProviderObservationKind,
    RegionTalkStageDispatcher,
    StageMetadataClaimRequest,
    StageProviderLaunchReceipt,
    StageProviderObservation,
    StageWorkerBindingReceipt,
    StageWorkerDirectResultReceipt,
    StageWorkerPayloadFetchRequest,
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
    assert launch.notebook_ref == "owner/20-region-talk-e5-enrichment"
    assert not hasattr(launch, "payload")

    replay = RegionTalkStageDispatcher(adapter, "owner", path)
    replay.dispatch_bound(claim, binding)
    assert len(adapter.launches) == 1
    serialized = path.read_bytes()
    for forbidden in (b'"payload"', b'"input_data"', b'"text"', b'"lease_token"', b'"database_url"'):
        assert forbidden not in serialized
    assert b'"lease_token_sha256"' not in serialized


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
            "worker_binding_sha256": self.fetched.worker_binding_sha256,
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
