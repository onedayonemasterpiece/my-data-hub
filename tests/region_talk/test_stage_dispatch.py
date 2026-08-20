from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.notebook_stages import (
    RegionTalkStageRuntimeUnavailable,
    process_region_talk_stage_item,
)
from my_data_hub.workloads.region_talk.stage_dispatch import (
    DispatchDisposition,
    PostgresStageWorkFunctions,
    ProviderObservationKind,
    RegionTalkStageDispatcher,
    StageProviderLaunchReceipt,
    StageProviderObservation,
    StageResultMetadata,
    StageWorkClaimReceipt,
    StageWorkerResult,
    StageWorkerStatus,
    reconcile_notebook_result,
    stage_effect_id,
)
from my_data_hub.workloads.region_talk.stage_execution import stage_run_id, work_item_id
from my_data_hub.workloads.region_talk.transforms.evidence import (
    ALL_LABELS,
    SEMANTIC_BANK_HASH,
    SEMANTIC_BANK_VERSION,
    vector_evidence_fingerprint,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
TASK = UUID("11111111-1111-4111-8111-111111111111")
BATCH = UUID("22222222-2222-4222-8222-222222222222")
MASTER = UUID("33333333-3333-4333-8333-333333333333")
SUBJECT = UUID("44444444-4444-4444-8444-444444444444")
CONTENT = UUID("55555555-5555-4555-8555-555555555555")
LEASE = UUID("66666666-6666-4666-8666-666666666666")
RUN = stage_run_id(TASK, BATCH)
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


def _claim(*, attempt: int = 1, epoch: int = 7, stage: str = "e5_embedding") -> dict[str, Any]:
    definition = {
        "e5_embedding": ("e5_semantic_bank_scores_v1", 3, 900),
        "vector_fusion": ("region-talk.vector-fusion.v1", 3, 300),
    }[stage]
    input_fingerprint = INPUT if stage == "e5_embedding" else "f" * 64
    item = (
        WORK
        if stage == "e5_embedding"
        else work_item_id(
            run_id=RUN,
            candidate_id=SUBJECT,
            revision=1,
            stage=stage,
            input_fingerprint=input_fingerprint,
        )
    )
    effect = stage_effect_id(
        work_item_id=item,
        attempt=attempt,
        input_fingerprint=input_fingerprint,
    )
    payload = {
        "schema_version": "region-talk-stage-work-execution.v1",
        "stage_run_id": str(RUN),
        "candidate_id": str(SUBJECT),
        "candidate_revision": 1,
        "revision_fingerprint": "a" * 64,
        "content_id": str(CONTENT),
        "content_type": "article",
        "canonical_url": "https://example.org/a",
        "canonical_source_key": "web:example.org",
        "input_fingerprint": input_fingerprint,
        "upstream_results": [],
        "input_data": {
            "schema_version": "region-talk-stage-text-input.v1",
            "text": "Калининградский музей",
            "text_sha256": hashlib.sha256("Калининградский музей".encode()).hexdigest(),
            "topics": ["museum"],
        },
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    body = {
        "schema_version": "region-talk-stage-work-claim-receipt.v1",
        "status": "CLAIMED",
        "master_instance_id": str(MASTER),
        "epoch": epoch,
        "task_run_id": str(TASK),
        "export_batch_id": str(BATCH),
        "stage_run_id": str(RUN),
        "work_item_id": str(item),
        "stage": stage,
        "contract_version": definition[0],
        "subject_type": "region_talk.candidate",
        "subject_id": str(SUBJECT),
        "input_fingerprint": input_fingerprint,
        "attempt": attempt,
        "max_attempts": definition[1],
        "timeout_seconds": definition[2],
        "effect_id": str(effect),
        "lease_token": str(LEASE),
        "lease_expires_at": (NOW + timedelta(seconds=definition[2])).isoformat(),
        "payload": payload,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _empty(status: str = "COMPLETE") -> dict[str, Any]:
    body = {
        "schema_version": "region-talk-stage-work-claim-receipt.v1",
        "status": status,
        "master_instance_id": str(MASTER),
        "epoch": 7,
        "task_run_id": str(TASK),
        "export_batch_id": str(BATCH),
        "stage_run_id": str(RUN),
        "work_item_id": None,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    return {**body, "receipt_sha256": _sha(body)}


def _worker_result(claim: dict[str, Any]) -> StageWorkerResult:
    metadata = StageResultMetadata(
        stage=claim["stage"],
        contract_version=claim["contract_version"],
        subject_type="region_talk.candidate",
        subject_id=SUBJECT,
        candidate_revision=1,
        revision_fingerprint="a" * 64,
        input_fingerprint=claim["input_fingerprint"],
        producer_exact_id="intfloat/multilingual-e5-base@d128750597153bb5987e10b1c3493a34e5a4502a",
        metrics={"text_sha256": hashlib.sha256("Калининградский музей".encode()).hexdigest(), "scores": []},
    )
    digest = _sha(metadata.model_dump(mode="json"))
    return StageWorkerResult(
        effect_id=claim["effect_id"],
        work_item_id=claim["work_item_id"],
        attempt=claim["attempt"],
        status=StageWorkerStatus.SUCCEEDED,
        result_metadata=metadata,
        metadata_sha256=digest,
        result_sha256=digest,
        completed_at=NOW + timedelta(seconds=5),
    )


class _Functions:
    def __init__(self, claims: list[dict[str, Any]], *, lose_submit_once: bool = False) -> None:
        self.claims = claims
        self.claim_requests: list[dict[str, Any]] = []
        self.submissions: list[dict[str, Any]] = []
        self.lose_submit_once = lose_submit_once

    def claim(self, *, task_run_id, export_batch_id, request):  # type: ignore[no-untyped-def]
        assert (task_run_id, export_batch_id) == (TASK, BATCH)
        self.claim_requests.append(request)
        return self.claims.pop(0)

    def submit(self, *, task_run_id, export_batch_id, request):  # type: ignore[no-untyped-def]
        assert (task_run_id, export_batch_id) == (TASK, BATCH)
        self.submissions.append(request)
        if self.lose_submit_once:
            self.lose_submit_once = False
            raise ConnectionError("response lost after durable commit")
        body = {
            "schema_version": "region-talk-stage-worker-result-receipt.v1",
            "accepted": True,
            "master_instance_id": str(MASTER),
            "epoch": 7,
            "task_run_id": str(TASK),
            "export_batch_id": str(BATCH),
            "stage_run_id": str(RUN),
            "work_item_id": request["work_item_id"],
            "effect_id": request["effect_id"],
            "attempt": request["attempt"],
            "stage": request["stage"],
            "subject_id": request["subject_id"],
            "input_fingerprint": request["input_fingerprint"],
            "result_status": request["result_status"],
            "metadata_sha256": request["metadata_sha256"],
            "result_sha256": request["result_sha256"],
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        return {**body, "receipt_sha256": _sha(body)}


class _Adapter:
    def __init__(self) -> None:
        self.launches = []
        self.terminal = False
        self.receipt: StageProviderLaunchReceipt | None = None
        self.result: StageWorkerResult | None = None

    def observe(self, launch):  # type: ignore[no-untyped-def]
        if self.receipt is None:
            return StageProviderObservation(kind=ProviderObservationKind.ABSENT)
        if self.terminal:
            return StageProviderObservation(
                kind=ProviderObservationKind.TERMINAL,
                launch_receipt=self.receipt,
                result=self.result,
            )
        return StageProviderObservation(
            kind=ProviderObservationKind.RUNNING,
            launch_receipt=self.receipt,
        )

    def launch(self, launch):  # type: ignore[no-untyped-def]
        self.launches.append(launch)
        body = {
            "schema_version": "region-talk-stage-provider-launch-receipt.v1",
            "effect_id": str(launch.effect_id),
            "notebook_ref": launch.notebook_ref,
            "provider_run_ref": f"{launch.notebook_ref}/9",
            "source_sha256": "c" * 64,
            "launched_at": NOW.isoformat(),
        }
        self.receipt = StageProviderLaunchReceipt(**body, receipt_sha256=_sha(body))
        return self.receipt


@dataclass
class _Clock:
    value: datetime = NOW

    def __call__(self) -> datetime:
        return self.value


def _dispatcher(
    path: Path,
    functions: _Functions,
    adapter: _Adapter,
    *,
    clock: _Clock | None = None,
):  # type: ignore[no-untyped-def]
    return RegionTalkStageDispatcher(
        functions=functions,
        adapter=adapter,
        notebook_owner="owner",
        lease_owner="central.region-talk",
        journal_path=path,
        refresh_stages=lambda *_: SimpleNamespace(status="WAITING_WORK", receipt_sha256="d" * 64),
        clock=clock or _Clock(),
        uuid_factory=lambda: LEASE,
    )


def test_waiting_dependency_never_becomes_complete(tmp_path: Path) -> None:
    dispatcher = _dispatcher(tmp_path / "journal.json", _Functions([_empty("WAITING_DEPENDENCY")]), _Adapter())
    receipt = dispatcher.execute_one(
        task_run_id=TASK,
        export_batch_id=BATCH,
        master_instance_id=MASTER,
        epoch=7,
    )
    assert receipt.disposition is DispatchDisposition.WAITING_WORK
    assert receipt.work_item_id is None
    assert receipt.publication_dispatch is False
    assert receipt.notification_dispatch is False


def test_launch_restart_reconciles_one_effect_and_submits_exact_result(tmp_path: Path) -> None:
    claim = _claim()
    functions = _Functions([claim, _empty("COMPLETE")])
    adapter = _Adapter()
    path = tmp_path / "journal.json"
    first = _dispatcher(path, functions, adapter).execute_one(
        task_run_id=TASK,
        export_batch_id=BATCH,
        master_instance_id=MASTER,
        epoch=7,
    )
    assert first.disposition is DispatchDisposition.WAITING_WORK
    assert len(adapter.launches) == 1
    assert adapter.launches[0].notebook_ref == "owner/20-region-talk-e5-enrichment"
    assert adapter.launches[0].effect_id == UUID(claim["effect_id"])

    adapter.result = _worker_result(claim)
    adapter.terminal = True
    second = _dispatcher(path, functions, adapter).execute_one(
        task_run_id=TASK,
        export_batch_id=BATCH,
        master_instance_id=MASTER,
        epoch=7,
    )
    assert second.disposition is DispatchDisposition.RESULT_ACCEPTED
    assert len(adapter.launches) == 1
    assert len(functions.claim_requests) == 1
    assert len(functions.submissions) == 1

    terminal = _dispatcher(path, functions, adapter).execute_one(
        task_run_id=TASK,
        export_batch_id=BATCH,
        master_instance_id=MASTER,
        epoch=7,
    )
    assert terminal.disposition is DispatchDisposition.COMPLETE


def test_submit_response_loss_replays_identical_result_without_relaunch(tmp_path: Path) -> None:
    claim = _claim()
    functions = _Functions([claim], lose_submit_once=True)
    adapter = _Adapter()
    path = tmp_path / "journal.json"
    dispatcher = _dispatcher(path, functions, adapter)
    dispatcher.execute_one(task_run_id=TASK, export_batch_id=BATCH, master_instance_id=MASTER, epoch=7)
    adapter.result = _worker_result(claim)
    adapter.terminal = True
    with pytest.raises(ConnectionError, match="response lost"):
        dispatcher.execute_one(task_run_id=TASK, export_batch_id=BATCH, master_instance_id=MASTER, epoch=7)
    replay = _dispatcher(path, functions, adapter).execute_one(
        task_run_id=TASK, export_batch_id=BATCH, master_instance_id=MASTER, epoch=7
    )
    assert replay.disposition is DispatchDisposition.RESULT_ACCEPTED
    assert functions.submissions[0] == functions.submissions[1]
    assert len(adapter.launches) == 1


def test_expired_lease_stays_waiting_and_next_claim_uses_new_effect(tmp_path: Path) -> None:
    first_claim = _claim(attempt=1)
    second_claim = _claim(attempt=2)
    functions = _Functions([first_claim, second_claim])
    adapter = _Adapter()
    clock = _Clock()
    dispatcher = _dispatcher(tmp_path / "journal.json", functions, adapter, clock=clock)
    dispatcher.execute_one(task_run_id=TASK, export_batch_id=BATCH, master_instance_id=MASTER, epoch=7)
    clock.value = NOW + timedelta(seconds=901)
    expired = dispatcher.execute_one(task_run_id=TASK, export_batch_id=BATCH, master_instance_id=MASTER, epoch=7)
    assert expired.disposition is DispatchDisposition.WAITING_WORK
    retry = dispatcher.execute_one(task_run_id=TASK, export_batch_id=BATCH, master_instance_id=MASTER, epoch=7)
    assert retry.disposition is DispatchDisposition.WAITING_WORK
    assert retry.effect_id == UUID(second_claim["effect_id"])
    assert retry.effect_id != expired.effect_id
    assert len(functions.submissions) == 0


def test_claim_from_different_epoch_is_rejected_before_launch(tmp_path: Path) -> None:
    adapter = _Adapter()
    with pytest.raises(ValueError, match="task/master/epoch"):
        _dispatcher(tmp_path / "journal.json", _Functions([_claim(epoch=8)]), adapter).execute_one(
            task_run_id=TASK,
            export_batch_id=BATCH,
            master_instance_id=MASTER,
            epoch=7,
        )
    assert adapter.launches == []


def test_database_claim_receipt_tamper_is_rejected_before_launch(tmp_path: Path) -> None:
    claim = _claim()
    claim["payload"]["canonical_url"] = "https://attacker.invalid/tampered"
    adapter = _Adapter()
    with pytest.raises(ValueError, match="receipt_sha256"):
        _dispatcher(tmp_path / "journal.json", _Functions([claim]), adapter).execute_one(
            task_run_id=TASK,
            export_batch_id=BATCH,
            master_instance_id=MASTER,
            epoch=7,
        )
    assert adapter.launches == []


class _AttachedRuntime:
    producer_exact_id = "verified-runtime@example@sha256:" + "9" * 64

    def execute(self, **kwargs):  # type: ignore[no-untyped-def]
        payload = kwargs["payload"]
        text = payload.input_data["text"]
        assert payload.input_data["text_sha256"] == hashlib.sha256(text.encode()).hexdigest()
        return {"verified": True, "input_sha256": hashlib.sha256(text.encode()).hexdigest()}


def test_attached_runtime_executes_and_missing_runtime_remains_retryable() -> None:
    claim = _claim()
    item = {
        "work_item_id": claim["work_item_id"],
        "subject_type": claim["subject_type"],
        "subject_id": claim["subject_id"],
        "input_fingerprint": claim["input_fingerprint"],
        "payload": claim["payload"],
    }
    with pytest.raises(RegionTalkStageRuntimeUnavailable) as failure:
        process_region_talk_stage_item(
            item,
            stage=claim["stage"],
            contract_version=claim["contract_version"],
        )
    assert failure.value.retryable is True
    result = process_region_talk_stage_item(
        item,
        stage=claim["stage"],
        contract_version=claim["contract_version"],
        runtime=_AttachedRuntime(),
    )
    assert result["subject_id"] == str(SUBJECT)
    assert result["input_fingerprint"] == INPUT
    assert result["producer_exact_id"].startswith("verified-runtime@")


def test_generic_notebook_result_is_hash_verified_and_bound_to_claim() -> None:
    claim = _claim()
    metadata = _worker_result(claim).result_metadata.model_dump(mode="json")
    raw = {
        "schema_version": "my-data-hub-notebook-result.v1",
        "result_id": "77777777-7777-4777-8777-777777777777",
        "run_id": claim["effect_id"],
        "workload": "region-talk",
        "stage": claim["stage"],
        "stage_contract_version": claim["contract_version"],
        "input_manifest_sha256": "8" * 64,
        "producer": {
            "code_revision": "exact",
            "runtime": "private-kaggle",
            "model": {"name": "e5", "version": "exact"},
        },
        "status": "succeeded",
        "items": [
            {
                "work_item_id": claim["work_item_id"],
                "input_fingerprint": claim["input_fingerprint"],
                "output_fingerprint": _sha(metadata),
                "status": "succeeded",
                "result": metadata,
                "evidence": {},
            }
        ],
        "failures": [],
        "metrics": {"accounted_items": 1},
        "provider_usage": [],
        "artifacts": [],
        "started_at": NOW.isoformat(),
        "completed_at": (NOW + timedelta(seconds=3)).isoformat(),
    }
    result = reconcile_notebook_result(
        StageWorkClaimReceipt.model_validate(claim),
        raw,
    )
    assert result.status is StageWorkerStatus.SUCCEEDED
    raw["items"][0]["output_fingerprint"] = "0" * 64
    with pytest.raises(ValueError, match="fingerprint"):
        reconcile_notebook_result(
            StageWorkClaimReceipt.model_validate(claim),
            raw,
        )


def test_pure_vector_fusion_executes_only_exact_current_upstream_hashes() -> None:
    text_hash = "1" * 64
    scores = {label: 0.2 for label in sorted(ALL_LABELS)}
    scores["ko_editorial_publication"] = 0.9
    rows = []
    upstream = []
    for stage, contract, model in (
        ("e5_embedding", "e5_semantic_bank_scores_v1", "intfloat/multilingual-e5-base"),
        ("bge_m3_embedding", "bge_m3_flagembedding_dense_v1", "BAAI/bge-m3"),
    ):
        fingerprint = vector_evidence_fingerprint(
            contract_version=contract,
            model_id=model,
            text_hash=text_hash,
            semantic_bank_version=SEMANTIC_BANK_VERSION,
            semantic_bank_hash=SEMANTIC_BANK_HASH,
            scores=scores,
        )
        metadata = StageResultMetadata(
            stage=stage,
            contract_version=contract,
            subject_type="region_talk.candidate",
            subject_id=SUBJECT,
            candidate_revision=1,
            revision_fingerprint="a" * 64,
            input_fingerprint="2" * 64,
            producer_exact_id=f"{model}@exact",
            metrics={
                "text_sha256": text_hash,
                "semantic_bank_version": SEMANTIC_BANK_VERSION,
                "semantic_bank_hash": SEMANTIC_BANK_HASH,
                "evidence_fingerprint": fingerprint,
            },
        )
        upstream.append(
            {
                "stage": stage,
                "contract_version": contract,
                "input_fingerprint": "2" * 64,
                "result_sha256": _sha(metadata.model_dump(mode="json")),
                "result_metadata": metadata.model_dump(mode="json"),
            }
        )
        rows.extend(
            {"stage": stage, "label": label, "value": value, "result_sha256": upstream[-1]["result_sha256"]}
            for label, value in sorted(scores.items())
        )
    claim = _claim(stage="vector_fusion")
    claim["payload"]["input_fingerprint"] = "f" * 64
    claim["payload"]["upstream_results"] = upstream
    claim["payload"]["input_data"] = {
        "schema_version": "region-talk-vector-fusion-input.v1",
        "scores": rows,
    }
    item = {
        "work_item_id": claim["work_item_id"],
        "subject_type": claim["subject_type"],
        "subject_id": claim["subject_id"],
        "input_fingerprint": claim["input_fingerprint"],
        "payload": claim["payload"],
    }
    result = process_region_talk_stage_item(
        item,
        stage="vector_fusion",
        contract_version="region-talk.vector-fusion.v1",
    )
    assert result["metrics"]["status"] == "fused_e5_bge_m3"
    assert result["metrics"]["positive_class"] == "ko_editorial_publication"


class _Cursor:
    def __init__(self) -> None:
        self.calls = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def execute(self, sql, parameters=None):  # type: ignore[no-untyped-def]
        self.calls.append((sql, parameters))
        return self

    def fetchone(self):
        return ({"schema_version": "receipt"},)


class _Connection:
    def __init__(self) -> None:
        self.value = _Cursor()
        self.commits = 0

    def cursor(self):
        return self.value

    def commit(self):
        self.commits += 1

    def rollback(self):
        raise AssertionError("rollback unexpected")


def test_postgres_stage_work_port_exposes_only_fixed_functions_and_role() -> None:
    connection = _Connection()
    port = PostgresStageWorkFunctions(connection)
    port.claim(task_run_id=TASK, export_batch_id=BATCH, request={"a": 1})
    port.submit(task_run_id=TASK, export_batch_id=BATCH, request={"b": 2})
    port.status(task_run_id=TASK, export_batch_id=BATCH, request={"c": 3})
    statements = connection.value.calls
    assert statements[0] == ("SET LOCAL ROLE mdh_region_talk_pipeline", None)
    assert statements[1][0] == "SELECT migration.claim_region_talk_stage_work(%s,%s,%s::jsonb)"
    assert statements[2] == ("SET LOCAL ROLE mdh_region_talk_pipeline", None)
    assert statements[3][0] == "SELECT migration.submit_region_talk_stage_result(%s,%s,%s::jsonb)"
    assert statements[4] == ("SET LOCAL ROLE mdh_region_talk_pipeline", None)
    assert statements[5][0] == "SELECT migration.region_talk_stage_work_status(%s,%s,%s::jsonb)"
    assert connection.commits == 3
