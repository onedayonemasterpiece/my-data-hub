from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.notebook_stages import (
    RegionTalkStageRuntimeUnavailable,
    process_region_talk_stage_item,
    stage_model_identity,
)
from my_data_hub.workloads.region_talk.stage_execution import (
    HEAVY_STAGES,
    ORDERED_STAGE_DAG,
    CandidateEvidenceSet,
    CandidateStageEvidence,
    PostgresPostImportStageFunction,
    RegionTalkPostImportSupervisor,
    StageEvidenceStatus,
    StagePreparation,
    StageRunStatus,
    form_stage_commit,
    stage_run_id,
)

NOW = datetime(2026, 8, 20, 12, tzinfo=UTC)
TASK = UUID("11111111-1111-4111-8111-111111111111")
EXPORT = UUID("22222222-2222-4222-8222-222222222222")
LEGACY = UUID("33333333-3333-4333-8333-333333333333")
FUTURE = UUID("44444444-4444-4444-8444-444444444444")


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _evidence(
    status: StageEvidenceStatus = StageEvidenceStatus.MISSING,
) -> CandidateEvidenceSet:
    return CandidateEvidenceSet(
        **{
            stage: CandidateStageEvidence(
                status=status,
                input_fingerprint=_sha({"stage": stage, "candidate": "fixture"}),
            )
            for stage in HEAVY_STAGES
        }
    )


def _preparation(*, current_future: bool = False) -> StagePreparation:
    run_id = stage_run_id(TASK, EXPORT)
    rows = [
        {
            "content_id": "55555555-5555-4555-8555-555555555555",
            "candidate_id": str(LEGACY),
            "candidate_revision": 2,
            "revision_fingerprint": "a" * 64,
            "canonical_url": "https://example.org/legacy-selected",
            "content_lane": "article",
            "canonical_source_key": "web:example.org",
            "topics": ["museum"],
            "content_type": "article",
            "quality_score": 0.82,
            "legacy_selected": True,
            "evidence": _evidence().model_dump(mode="json"),
        },
        {
            "content_id": "66666666-6666-4666-8666-666666666666",
            "candidate_id": str(FUTURE),
            "candidate_revision": 1,
            "revision_fingerprint": "b" * 64,
            "canonical_url": "https://example.net/future-candidate",
            "content_lane": "social",
            "canonical_source_key": "web:example.net",
            "topics": ["coast"],
            "content_type": "post",
            "quality_score": 0.91,
            "legacy_selected": False,
            "evidence": _evidence(
                StageEvidenceStatus.CURRENT
                if current_future
                else StageEvidenceStatus.MISSING
            ).model_dump(mode="json"),
        },
    ]
    return StagePreparation(
        schema_version="region-talk-post-import-stage-preparation.v1",
        stage_run_id=run_id,
        task_run_id=TASK,
        export_batch_id=EXPORT,
        canonical_revision=26,
        status="PREPARED",
        preparation_sha256=_sha(rows),
        candidates=rows,
    )


def test_legacy_selected_enters_review_queue_but_missing_evidence_is_not_passed() -> None:
    commit = form_stage_commit(_preparation(), now=NOW)
    legacy, future = commit.candidate_outcomes

    assert legacy.candidate_id == LEGACY
    assert legacy.disposition == "QUEUED_REVIEW"
    assert legacy.review_basis == "LEGACY_SELECTED"
    assert legacy.queue_rank == 1
    assert {item.stage for item in legacy.work_requests} == {
        "e5_embedding",
        "bge_m3_embedding",
    }
    assert future.candidate_id == FUTURE
    assert future.disposition == "WAITING_WORK"
    assert future.review_basis is None and future.queue_rank is None
    assert {item.stage for item in future.work_requests} == {
        "e5_embedding",
        "bge_m3_embedding",
    }
    assert all(item.publication_dispatch is False for item in legacy.work_requests)
    assert all(item.notification_dispatch is False for item in legacy.work_requests)

    receipts = {item.stage: item for item in commit.stage_receipts}
    assert receipts["canonical_import"].status == "SUCCEEDED"
    assert receipts["e5_embedding"].status == "WAITING_WORK"
    assert receipts["bge_m3_embedding"].status == "WAITING_WORK"
    assert receipts["vector_fusion"].status == "SKIPPED_BLOCKED"
    assert receipts["review_queue"].status == "SUCCEEDED"


def test_future_candidate_requires_all_current_heavy_evidence_before_queue() -> None:
    commit = form_stage_commit(_preparation(current_future=True), now=NOW)
    legacy, future = commit.candidate_outcomes

    assert legacy.review_basis == "LEGACY_SELECTED"
    assert future.disposition == "QUEUED_REVIEW"
    assert future.review_basis == "CURRENT_EVIDENCE"
    # The higher-quality future row ranks first; ordering is deterministic.
    assert future.queue_rank == 1 and legacy.queue_rank == 2
    assert future.work_requests == ()


def test_stage_dag_and_work_identities_are_replay_stable() -> None:
    first = form_stage_commit(_preparation(), now=NOW)
    second = form_stage_commit(_preparation(), now=NOW)
    assert first == second
    assert tuple(item.stage for item in first.ordered_stages) == (
        "canonical_import",
        "e5_embedding",
        "bge_m3_embedding",
        "vector_fusion",
        "image_scoring",
        "final_verifier",
        "writer",
        "review_queue",
    )
    assert first.ordered_stages == ORDERED_STAGE_DAG


def test_retryable_and_exhausted_evidence_have_distinct_terminal_receipts() -> None:
    base = _preparation()
    future = base.candidates[1]
    retrying = future.model_copy(
        update={
            "evidence": future.evidence.model_copy(
                update={
                    "e5_embedding": future.evidence.e5_embedding.model_copy(
                        update={
                            "status": StageEvidenceStatus.FAILED_RETRYABLE,
                            "attempt_count": 1,
                        }
                    )
                }
            )
        }
    )
    retry_preparation = base.model_copy(update={"candidates": (base.candidates[0], retrying)})
    retry_commit = form_stage_commit(retry_preparation, now=NOW)
    retry_receipt = retry_commit.stage_receipts[1]
    assert retry_receipt.stage == "e5_embedding"
    assert retry_receipt.status == "FAILED_RETRYABLE"
    assert retry_receipt.attempt == 2

    exhausted = retrying.model_copy(
        update={
            "evidence": retrying.evidence.model_copy(
                update={
                    "e5_embedding": retrying.evidence.e5_embedding.model_copy(
                        update={"attempt_count": 3}
                    )
                }
            )
        }
    )
    exhausted_preparation = base.model_copy(
        update={"candidates": (base.candidates[0], exhausted)}
    )
    exhausted_commit = form_stage_commit(exhausted_preparation, now=NOW)
    exhausted_outcome = exhausted_commit.candidate_outcomes[1]
    assert exhausted_outcome.disposition == "FAILED_TERMINAL"
    assert all(item.stage != "e5_embedding" for item in exhausted_outcome.work_requests)
    assert exhausted_commit.stage_receipts[1].status == "FAILED_TERMINAL"


def test_stale_future_evidence_is_queued_for_refresh_not_accepted() -> None:
    base = _preparation(current_future=True)
    future = base.candidates[1]
    stale = future.model_copy(
        update={
            "evidence": future.evidence.model_copy(
                update={
                    "e5_embedding": future.evidence.e5_embedding.model_copy(
                        update={"status": StageEvidenceStatus.STALE}
                    )
                }
            )
        }
    )
    preparation = base.model_copy(update={"candidates": (base.candidates[0], stale)})
    commit = form_stage_commit(preparation, now=NOW)
    outcome = commit.candidate_outcomes[1]
    assert outcome.disposition == "WAITING_WORK"
    assert outcome.review_basis is None and outcome.queue_rank is None
    assert [(item.stage, item.reason) for item in outcome.work_requests] == [
        ("e5_embedding", "stale_evidence")
    ]


class _Function:
    def __init__(self, preparation: StagePreparation) -> None:
        self.preparation = preparation
        self.requests: list[dict[str, Any]] = []

    def call(self, *, task_run_id, export_batch_id, request):  # type: ignore[no-untyped-def]
        assert task_run_id == TASK and export_batch_id == EXPORT
        self.requests.append(request)
        if request["operation"] == "prepare":
            return self.preparation.model_dump(mode="json")
        stage_receipts = request["stage_receipts"]
        work_count = sum(
            len(item["work_requests"]) for item in request["candidate_outcomes"]
        )
        queued = sum(
            item["disposition"] == "QUEUED_REVIEW"
            for item in request["candidate_outcomes"]
        )
        body = {
            "schema_version": "region-talk-post-import-stage-receipt.v1",
            "stage_run_id": request["stage_run_id"],
            "task_run_id": str(TASK),
            "export_batch_id": str(EXPORT),
            "canonical_revision": self.preparation.canonical_revision,
            "status": "WAITING_WORK" if work_count else "COMPLETE",
            "stage_receipts": stage_receipts,
            "queue_revision": 1,
            "queue_count": queued,
            "work_request_count": work_count,
            "rows_observed": len(request["candidate_outcomes"]),
            "rows_changed": queued + work_count,
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        return {**body, "receipt_sha256": _sha(body)}


def test_separate_supervisor_prepares_transforms_and_commits_typed_receipt() -> None:
    function = _Function(_preparation())
    receipt = RegionTalkPostImportSupervisor(function, clock=lambda: NOW).execute_after_import(
        task_run_id=TASK,
        export_batch_id=EXPORT,
    )

    assert [item["operation"] for item in function.requests] == ["prepare", "commit"]
    assert receipt.status is StageRunStatus.WAITING_WORK
    assert receipt.queue_count == 1
    assert receipt.work_request_count == 4
    assert receipt.publication_dispatch is False
    assert receipt.notification_dispatch is False


class _Cursor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, object]] = []

    def __enter__(self):
        return self

    def __exit__(self, *_args):  # type: ignore[no-untyped-def]
        return False

    def execute(self, sql: str, parameters: object = None):
        self.statements.append((sql, parameters))
        return self

    def fetchone(self):
        return ({"status": "PREPARED"},)


class _Connection:
    def __init__(self) -> None:
        self.cursor_value = _Cursor()
        self.commits = 0

    def cursor(self):
        return self.cursor_value

    def commit(self) -> None:
        self.commits += 1

    def rollback(self) -> None:
        raise AssertionError("rollback was not expected")


def test_postgres_port_calls_only_the_fixed_security_definer_seam() -> None:
    connection = _Connection()
    result = PostgresPostImportStageFunction(connection).call(
        task_run_id=TASK,
        export_batch_id=EXPORT,
        request={"operation": "prepare"},
    )

    assert result == {"status": "PREPARED"}
    assert connection.commits == 1
    assert connection.cursor_value.statements[0] == (
        "SET LOCAL ROLE mdh_region_talk_pipeline",
        None,
    )
    sql, parameters = connection.cursor_value.statements[1]
    assert sql == (
        "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)"
    )
    assert parameters is not None


def test_required_heavy_notebook_contract_validates_then_fails_without_fake_result() -> None:
    commit = form_stage_commit(_preparation(), now=NOW)
    request = commit.candidate_outcomes[0].work_requests[0]
    work_item = {
        "work_item_id": str(request.work_item_id),
        "subject_type": request.subject_type,
        "subject_id": str(request.subject_id),
        "input_fingerprint": request.input_fingerprint,
        "payload": {
            "schema_version": "region-talk-stage-work-payload.v1",
            "stage_run_id": str(commit.stage_run_id),
            "candidate_revision": commit.candidate_outcomes[0].candidate_revision,
            "revision_fingerprint": "a" * 64,
            "reason": request.reason,
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    }
    with pytest.raises(
        RegionTalkStageRuntimeUnavailable,
        match="no verified heavyweight runtime is attached",
    ):
        process_region_talk_stage_item(
            work_item,
            stage=request.stage,
            contract_version=request.contract_version,
        )
    model = stage_model_identity("e5_embedding")
    assert model == {
        "model_id": "intfloat/multilingual-e5-base",
        "model_revision": "d128750597153bb5987e10b1c3493a34e5a4502a",
        "encoder_contract": "e5-attention-mask-mean-l2-prefixes-max512.v1",
    }


def test_generated_required_stage_notebooks_have_no_notimplemented_shell() -> None:
    root = Path(__file__).resolve().parents[2]
    required = (
        "20-region-talk-e5-enrichment",
        "30-region-talk-bge-m3-enrichment",
        "35-region-talk-vector-fusion",
        "40-region-talk-image-diagnostic",
        "50-region-talk-final-verifier",
        "70-region-talk-writer",
    )
    for directory in required:
        notebook = json.loads((root / "notebooks" / directory / "worker.ipynb").read_text())
        source = "\n".join(
            "".join(cell.get("source", [])) for cell in notebook["cells"]
        )
        metadata = json.loads(
            (root / "notebooks" / directory / "kernel-metadata.example.json").read_text()
        )
        assert "NotImplementedError" not in source
        assert "process_region_talk_stage_item" in source
        assert metadata["my_data_hub"]["adapter_status"] == "contract_ready"
        assert metadata["my_data_hub"]["production_ready"] is False
