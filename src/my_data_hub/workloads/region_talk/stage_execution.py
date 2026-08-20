"""Post-import Region Talk stage supervisor for the private master data plane.

The supervisor is deliberately separate from the snapshot importer.  It reads
one typed preparation through a fixed ``SECURITY DEFINER`` function, applies a
deterministic pure queue-formation transform, and commits the typed result
through the same function.  It never accepts SQL, publishes, or notifies.
"""

from __future__ import annotations

import hashlib
import json
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.region_talk.transforms.models import ReviewCandidate
from my_data_hub.workloads.region_talk.transforms.ranking import rank_review_queue

SHA256_PATTERN = r"^[a-f0-9]{64}$"
STAGE_ID_NAMESPACE = UUID("54a0dba7-1e4b-4d56-a143-173304989e85")


class StageEvidenceStatus(StrEnum):
    CURRENT = "CURRENT"
    MISSING = "MISSING"
    STALE = "STALE"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class StageReceiptStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    WAITING_WORK = "WAITING_WORK"
    SKIPPED_BLOCKED = "SKIPPED_BLOCKED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class StageRunStatus(StrEnum):
    COMPLETE = "COMPLETE"
    WAITING_WORK = "WAITING_WORK"
    FAILED = "FAILED"


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StageDefinition(StrictModel):
    stage: str = Field(pattern=r"^[a-z][a-z0-9_]{0,79}$")
    contract_version: str = Field(min_length=1, max_length=200)
    dependencies: tuple[str, ...] = ()
    max_attempts: int = Field(ge=1, le=20)
    timeout_seconds: int = Field(ge=1, le=10_800)


ORDERED_STAGE_DAG: tuple[StageDefinition, ...] = (
    StageDefinition(
        stage="canonical_import",
        contract_version="region-talk-direct-snapshot-receipt.v2",
        max_attempts=1,
        timeout_seconds=3_600,
    ),
    StageDefinition(
        stage="e5_embedding",
        contract_version="e5_semantic_bank_scores_v1",
        dependencies=("canonical_import",),
        max_attempts=3,
        timeout_seconds=900,
    ),
    StageDefinition(
        stage="bge_m3_embedding",
        contract_version="bge_m3_flagembedding_dense_v1",
        dependencies=("canonical_import",),
        max_attempts=3,
        timeout_seconds=1_200,
    ),
    StageDefinition(
        stage="vector_fusion",
        contract_version="region-talk.vector-fusion.v1",
        dependencies=("e5_embedding", "bge_m3_embedding"),
        max_attempts=3,
        timeout_seconds=300,
    ),
    StageDefinition(
        stage="image_scoring",
        contract_version="region-talk.image-diagnostic.v1",
        dependencies=("vector_fusion",),
        max_attempts=3,
        timeout_seconds=1_200,
    ),
    StageDefinition(
        stage="final_verifier",
        contract_version="region-talk.final-verifier.v1",
        dependencies=("image_scoring",),
        max_attempts=3,
        timeout_seconds=600,
    ),
    StageDefinition(
        stage="writer",
        contract_version="region-talk.writer.v1",
        dependencies=("final_verifier",),
        max_attempts=3,
        timeout_seconds=900,
    ),
    StageDefinition(
        stage="review_queue",
        contract_version="region-talk.review-queue.v1",
        dependencies=("writer",),
        max_attempts=3,
        timeout_seconds=300,
    ),
)
STAGE_BY_KEY = {item.stage: item for item in ORDERED_STAGE_DAG}
HEAVY_STAGES = tuple(
    item.stage
    for item in ORDERED_STAGE_DAG
    if item.stage not in {"canonical_import", "review_queue"}
)


class CandidateStageEvidence(StrictModel):
    status: StageEvidenceStatus
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt_count: int = Field(default=0, ge=0, le=100)


class CandidateEvidenceSet(StrictModel):
    e5_embedding: CandidateStageEvidence
    bge_m3_embedding: CandidateStageEvidence
    vector_fusion: CandidateStageEvidence
    image_scoring: CandidateStageEvidence
    final_verifier: CandidateStageEvidence
    writer: CandidateStageEvidence


class PreparedCandidate(StrictModel):
    content_id: UUID
    candidate_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    canonical_url: str = Field(default="", max_length=4_000)
    content_lane: Literal["article", "social"]
    canonical_source_key: str = Field(min_length=1, max_length=1_000)
    topics: tuple[str, ...] = Field(default=(), max_length=100)
    content_type: str = Field(default="", max_length=200)
    quality_score: float = Field(ge=0, le=1)
    legacy_selected: bool
    evidence: CandidateEvidenceSet


class StagePreparation(StrictModel):
    schema_version: Literal["region-talk-post-import-stage-preparation.v1"]
    stage_run_id: UUID
    task_run_id: UUID
    export_batch_id: UUID
    canonical_revision: int = Field(ge=0)
    status: Literal["PREPARED"]
    preparation_sha256: str = Field(pattern=SHA256_PATTERN)
    candidates: tuple[PreparedCandidate, ...]
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def candidates_are_stably_ordered(self) -> StagePreparation:
        keys = [
            (str(item.candidate_id), item.candidate_revision)
            for item in self.candidates
        ]
        if keys != sorted(keys) or len(keys) != len(set(keys)):
            raise ValueError("prepared candidates must be unique and canonically ordered")
        return self


class StageWorkRequest(StrictModel):
    schema_version: Literal["region-talk-stage-work-request.v1"] = (
        "region-talk-stage-work-request.v1"
    )
    work_item_id: UUID
    stage: str
    contract_version: str
    subject_type: Literal["region_talk.candidate"] = "region_talk.candidate"
    subject_id: UUID
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    status: Literal["PENDING", "FAILED_RETRYABLE"]
    attempt_count: int = Field(ge=0)
    max_attempts: int = Field(ge=1, le=20)
    timeout_seconds: int = Field(ge=1, le=10_800)
    reason: Literal["missing_evidence", "stale_evidence", "retryable_failure"]
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def identity_and_policy_match_dag(self) -> StageWorkRequest:
        definition = STAGE_BY_KEY.get(self.stage)
        if definition is None or self.stage not in HEAVY_STAGES:
            raise ValueError("work request stage is not a bounded DAG worker stage")
        if (
            self.contract_version != definition.contract_version
            or self.max_attempts != definition.max_attempts
            or self.timeout_seconds != definition.timeout_seconds
        ):
            raise ValueError("work request policy differs from the fixed DAG")
        if self.attempt_count >= self.max_attempts:
            raise ValueError("retryable work request exhausted max_attempts")
        return self


class CandidateStageOutcome(StrictModel):
    candidate_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    disposition: Literal["QUEUED_REVIEW", "WAITING_WORK", "FAILED_TERMINAL"]
    review_basis: Literal["LEGACY_SELECTED", "CURRENT_EVIDENCE"] | None = None
    queue_rank: int | None = Field(default=None, ge=1)
    work_requests: tuple[StageWorkRequest, ...] = ()

    @model_validator(mode="after")
    def queue_fields_are_consistent(self) -> CandidateStageOutcome:
        queued = self.disposition == "QUEUED_REVIEW"
        if queued != (self.review_basis is not None and self.queue_rank is not None):
            raise ValueError("only queued review outcomes carry basis and rank")
        return self


class StageAttemptReceipt(StrictModel):
    stage: str
    contract_version: str
    status: StageReceiptStatus
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1, le=20)
    timeout_seconds: int = Field(ge=1, le=10_800)
    rows_observed: int = Field(ge=0)
    rows_changed: int = Field(ge=0)
    work_request_count: int = Field(ge=0)
    input_sha256: str = Field(pattern=SHA256_PATTERN)
    output_sha256: str = Field(pattern=SHA256_PATTERN)
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    started_at: datetime
    completed_at: datetime

    @field_validator("started_at", "completed_at")
    @classmethod
    def timestamps_are_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("stage receipt timestamp must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def policy_and_time_match(self) -> StageAttemptReceipt:
        definition = STAGE_BY_KEY.get(self.stage)
        if definition is None:
            raise ValueError("unknown stage receipt")
        if (
            self.contract_version != definition.contract_version
            or self.max_attempts != definition.max_attempts
            or self.timeout_seconds != definition.timeout_seconds
        ):
            raise ValueError("stage receipt policy differs from the fixed DAG")
        if self.attempt > self.max_attempts:
            raise ValueError("stage receipt attempt exceeds max_attempts")
        if self.completed_at < self.started_at:
            raise ValueError("stage completed_at precedes started_at")
        return self


class StageCommitRequest(StrictModel):
    schema_version: Literal["region-talk-post-import-stage-request.v1"] = (
        "region-talk-post-import-stage-request.v1"
    )
    operation: Literal["commit"] = "commit"
    stage_run_id: UUID
    task_run_id: UUID
    export_batch_id: UUID
    requested_at: datetime
    preparation_sha256: str = Field(pattern=SHA256_PATTERN)
    ordered_stages: tuple[StageDefinition, ...]
    candidate_outcomes: tuple[CandidateStageOutcome, ...]
    stage_receipts: tuple[StageAttemptReceipt, ...]
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @field_validator("requested_at")
    @classmethod
    def requested_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("requested_at must be timezone-aware")
        return value.astimezone(UTC)

    @model_validator(mode="after")
    def exact_dag_and_receipt_order(self) -> StageCommitRequest:
        if self.ordered_stages != ORDERED_STAGE_DAG:
            raise ValueError("ordered_stages differs from the fixed Region Talk DAG")
        if tuple(item.stage for item in self.stage_receipts) != tuple(
            item.stage for item in ORDERED_STAGE_DAG
        ):
            raise ValueError("stage receipts must cover the fixed DAG in order")
        return self


class StagePrepareRequest(StrictModel):
    schema_version: Literal["region-talk-post-import-stage-request.v1"] = (
        "region-talk-post-import-stage-request.v1"
    )
    operation: Literal["prepare"] = "prepare"
    stage_run_id: UUID
    task_run_id: UUID
    export_batch_id: UUID
    requested_at: datetime
    ordered_stages: tuple[StageDefinition, ...] = ORDERED_STAGE_DAG
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @field_validator("requested_at")
    @classmethod
    def requested_at_is_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("requested_at must be timezone-aware")
        return value.astimezone(UTC)


class PostImportStageReceipt(StrictModel):
    schema_version: Literal["region-talk-post-import-stage-receipt.v1"]
    stage_run_id: UUID
    task_run_id: UUID
    export_batch_id: UUID
    canonical_revision: int = Field(ge=0)
    status: StageRunStatus
    stage_receipts: tuple[StageAttemptReceipt, ...]
    queue_revision: int = Field(ge=0)
    queue_count: int = Field(ge=0)
    work_request_count: int = Field(ge=0)
    rows_observed: int = Field(ge=0)
    rows_changed: int = Field(ge=0)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_receipt_order(self) -> PostImportStageReceipt:
        if tuple(item.stage for item in self.stage_receipts) != tuple(
            item.stage for item in ORDERED_STAGE_DAG
        ):
            raise ValueError("terminal stage receipts differ from the fixed DAG")
        return self


def stage_run_id(task_run_id: UUID, export_batch_id: UUID) -> UUID:
    return uuid5(
        STAGE_ID_NAMESPACE,
        f"region-talk-stage-run:{task_run_id}:{export_batch_id}",
    )


def work_item_id(
    *,
    run_id: UUID,
    candidate_id: UUID,
    revision: int,
    stage: str,
    input_fingerprint: str,
) -> UUID:
    return uuid5(
        STAGE_ID_NAMESPACE,
        f"region-talk-work:{run_id}:{candidate_id}:{revision}:{stage}:{input_fingerprint}",
    )


class FixedPostImportStageFunction(Protocol):
    def call(
        self,
        *,
        task_run_id: UUID,
        export_batch_id: UUID,
        request: dict[str, Any],
    ) -> dict[str, Any]: ...


class PostgresPostImportStageFunction:
    """Only the fixed migration function is exposed to the private caller."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def call(
        self,
        *,
        task_run_id: UUID,
        export_batch_id: UUID,
        request: dict[str, Any],
    ) -> dict[str, Any]:
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (task_run_id, export_batch_id, json.dumps(request)),
            ).fetchone()
        if row is None:
            self.connection.rollback()
            raise RuntimeError("master did not return a Region Talk stage response")
        self.connection.commit()
        value = row[0]
        parsed = json.loads(value) if isinstance(value, str) else value
        if not isinstance(parsed, dict):
            raise RuntimeError("master returned a non-object Region Talk stage response")
        return parsed


def _reason(status: StageEvidenceStatus) -> str:
    return {
        StageEvidenceStatus.MISSING: "missing_evidence",
        StageEvidenceStatus.STALE: "stale_evidence",
        StageEvidenceStatus.FAILED_RETRYABLE: "retryable_failure",
    }[status]


def _request_for(
    run_id: UUID,
    candidate: PreparedCandidate,
    stage: str,
    evidence: CandidateStageEvidence,
) -> StageWorkRequest | None:
    if evidence.status not in {
        StageEvidenceStatus.MISSING,
        StageEvidenceStatus.STALE,
        StageEvidenceStatus.FAILED_RETRYABLE,
    }:
        return None
    definition = STAGE_BY_KEY[stage]
    if evidence.attempt_count >= definition.max_attempts:
        return None
    return StageWorkRequest(
        work_item_id=work_item_id(
            run_id=run_id,
            candidate_id=candidate.candidate_id,
            revision=candidate.candidate_revision,
            stage=stage,
            input_fingerprint=evidence.input_fingerprint,
        ),
        stage=stage,
        contract_version=definition.contract_version,
        subject_id=candidate.candidate_id,
        input_fingerprint=evidence.input_fingerprint,
        status=(
            "FAILED_RETRYABLE"
            if evidence.status is StageEvidenceStatus.FAILED_RETRYABLE
            else "PENDING"
        ),
        attempt_count=evidence.attempt_count,
        max_attempts=definition.max_attempts,
        timeout_seconds=definition.timeout_seconds,
        reason=_reason(evidence.status),  # type: ignore[arg-type]
    )


def _actionable_requests(
    run_id: UUID, candidate: PreparedCandidate
) -> tuple[StageWorkRequest, ...]:
    """Queue only dependency-ready stages; E5/BGE form the only parallel fork."""

    evidence = candidate.evidence
    requests: list[StageWorkRequest] = []
    for stage in ("e5_embedding", "bge_m3_embedding"):
        request = _request_for(run_id, candidate, stage, getattr(evidence, stage))
        if request is not None:
            requests.append(request)
    if any(
        getattr(evidence, stage).status is not StageEvidenceStatus.CURRENT
        for stage in ("e5_embedding", "bge_m3_embedding")
    ):
        return tuple(requests)
    for stage in ("vector_fusion", "image_scoring", "final_verifier", "writer"):
        current = getattr(evidence, stage)
        if current.status is StageEvidenceStatus.CURRENT:
            continue
        request = _request_for(run_id, candidate, stage, current)
        return tuple([*requests, request] if request is not None else requests)
    return tuple(requests)


def _review_candidate(value: PreparedCandidate) -> ReviewCandidate:
    return ReviewCandidate(
        candidate_id=str(value.candidate_id),
        canonical_url=(
            value.canonical_url or f"urn:region-talk:candidate:{value.candidate_id}"
        ),
        content_lane=value.content_lane,
        canonical_source_key=value.canonical_source_key,
        topics=value.topics,
        content_type=value.content_type,
        quality_score=value.quality_score,
        current_revision_fingerprint=value.revision_fingerprint,
        final_verifier_status="accept",
        writer_status="completed",
    )


def form_stage_commit(
    preparation: StagePreparation,
    *,
    now: datetime,
) -> StageCommitRequest:
    """Pure deterministic queue/work transform used by the production supervisor."""

    observed = len(preparation.candidates)
    requests_by_candidate = {
        item.candidate_id: _actionable_requests(preparation.stage_run_id, item)
        for item in preparation.candidates
    }
    current = [
        item
        for item in preparation.candidates
        if all(
            getattr(item.evidence, stage).status is StageEvidenceStatus.CURRENT
            for stage in HEAVY_STAGES
        )
    ]
    queueable = {
        item.candidate_id: item
        for item in preparation.candidates
        if item.legacy_selected or item in current
    }
    ranked = rank_review_queue(
        [_review_candidate(item) for item in queueable.values()], limit=20
    )
    rank_by_id = {UUID(item.candidate_id): item.queue_rank for item in ranked}
    outcomes: list[CandidateStageOutcome] = []
    for item in preparation.candidates:
        terminal = any(
            getattr(item.evidence, stage).status
            is StageEvidenceStatus.FAILED_TERMINAL
            or (
                getattr(item.evidence, stage).status
                is StageEvidenceStatus.FAILED_RETRYABLE
                and getattr(item.evidence, stage).attempt_count
                >= STAGE_BY_KEY[stage].max_attempts
            )
            for stage in HEAVY_STAGES
        )
        if item.candidate_id in rank_by_id:
            outcomes.append(
                CandidateStageOutcome(
                    candidate_id=item.candidate_id,
                    candidate_revision=item.candidate_revision,
                    revision_fingerprint=item.revision_fingerprint,
                    disposition="QUEUED_REVIEW",
                    review_basis=(
                        "LEGACY_SELECTED"
                        if item.legacy_selected
                        else "CURRENT_EVIDENCE"
                    ),
                    queue_rank=rank_by_id[item.candidate_id],
                    work_requests=requests_by_candidate[item.candidate_id],
                )
            )
        else:
            outcomes.append(
                CandidateStageOutcome(
                    candidate_id=item.candidate_id,
                    candidate_revision=item.candidate_revision,
                    revision_fingerprint=item.revision_fingerprint,
                    disposition="FAILED_TERMINAL" if terminal else "WAITING_WORK",
                    work_requests=requests_by_candidate[item.candidate_id],
                )
            )

    counter: Counter[str] = Counter(
        request.stage for outcome in outcomes for request in outcome.work_requests
    )
    all_current = {
        stage: sum(
            getattr(item.evidence, stage).status is StageEvidenceStatus.CURRENT
            for item in preparation.candidates
        )
        for stage in HEAVY_STAGES
    }
    terminal_by_stage = {
        stage: any(
            getattr(item.evidence, stage).status is StageEvidenceStatus.FAILED_TERMINAL
            or (
                getattr(item.evidence, stage).status
                is StageEvidenceStatus.FAILED_RETRYABLE
                and getattr(item.evidence, stage).attempt_count
                >= STAGE_BY_KEY[stage].max_attempts
            )
            for item in preparation.candidates
        )
        for stage in HEAVY_STAGES
    }
    retryable_by_stage = {
        stage: any(
            request.stage == stage and request.status == "FAILED_RETRYABLE"
            for outcome in outcomes
            for request in outcome.work_requests
        )
        for stage in HEAVY_STAGES
    }
    stage_receipts: list[StageAttemptReceipt] = []
    for definition in ORDERED_STAGE_DAG:
        stage = definition.stage
        if stage == "canonical_import":
            status = StageReceiptStatus.SUCCEEDED
            changed = observed
        elif stage == "review_queue":
            status = StageReceiptStatus.SUCCEEDED
            changed = len(ranked)
        elif terminal_by_stage.get(stage):
            status = StageReceiptStatus.FAILED_TERMINAL
            changed = 0
        elif retryable_by_stage.get(stage):
            status = StageReceiptStatus.FAILED_RETRYABLE
            changed = counter[stage]
        elif counter[stage]:
            status = StageReceiptStatus.WAITING_WORK
            changed = counter[stage]
        elif all_current.get(stage, 0) == observed:
            status = StageReceiptStatus.SUCCEEDED
            changed = observed
        else:
            status = StageReceiptStatus.SKIPPED_BLOCKED
            changed = 0
        input_payload = {
            "stage_run_id": str(preparation.stage_run_id),
            "preparation_sha256": preparation.preparation_sha256,
            "stage": stage,
            "candidate_revisions": [
                [str(item.candidate_id), item.candidate_revision, item.revision_fingerprint]
                for item in preparation.candidates
            ],
        }
        output_payload = {
            "status": status.value,
            "rows_observed": observed,
            "rows_changed": changed,
            "work_request_count": counter[stage],
        }
        input_sha = hashlib.sha256(canonical_json_bytes(input_payload)).hexdigest()
        output_sha = hashlib.sha256(canonical_json_bytes(output_payload)).hexdigest()
        evidence_attempt = max(
            (
                getattr(item.evidence, stage).attempt_count
                for item in preparation.candidates
                if stage in HEAVY_STAGES
            ),
            default=0,
        )
        receipt_payload = {
            **output_payload,
            "stage": stage,
            "contract_version": definition.contract_version,
            "attempt": min(definition.max_attempts, max(1, evidence_attempt + 1)),
            "max_attempts": definition.max_attempts,
            "timeout_seconds": definition.timeout_seconds,
            "input_sha256": input_sha,
            "output_sha256": output_sha,
            "started_at": now.astimezone(UTC).isoformat(),
            "completed_at": now.astimezone(UTC).isoformat(),
        }
        receipt_sha = hashlib.sha256(canonical_json_bytes(receipt_payload)).hexdigest()
        stage_receipts.append(
            StageAttemptReceipt(
                **receipt_payload,
                receipt_sha256=receipt_sha,
            )
        )
    return StageCommitRequest(
        stage_run_id=preparation.stage_run_id,
        task_run_id=preparation.task_run_id,
        export_batch_id=preparation.export_batch_id,
        requested_at=now,
        preparation_sha256=preparation.preparation_sha256,
        ordered_stages=ORDERED_STAGE_DAG,
        candidate_outcomes=tuple(outcomes),
        stage_receipts=tuple(stage_receipts),
    )


class RegionTalkPostImportSupervisor:
    """Execute the fixed prepare -> pure transform -> atomic commit protocol."""

    def __init__(
        self,
        function: FixedPostImportStageFunction,
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self.function = function
        self.clock = clock

    def execute_after_import(
        self, *, task_run_id: UUID, export_batch_id: UUID
    ) -> PostImportStageReceipt:
        run_id = stage_run_id(task_run_id, export_batch_id)
        requested_at = self.clock()
        prepare = StagePrepareRequest(
            stage_run_id=run_id,
            task_run_id=task_run_id,
            export_batch_id=export_batch_id,
            requested_at=requested_at,
        )
        prepared = StagePreparation.model_validate(
            self.function.call(
                task_run_id=task_run_id,
                export_batch_id=export_batch_id,
                request=prepare.model_dump(mode="json"),
            )
        )
        if (
            prepared.stage_run_id != run_id
            or prepared.task_run_id != task_run_id
            or prepared.export_batch_id != export_batch_id
        ):
            raise RuntimeError("stage preparation differs from exact import identity")
        commit = form_stage_commit(prepared, now=requested_at)
        receipt = PostImportStageReceipt.model_validate(
            self.function.call(
                task_run_id=task_run_id,
                export_batch_id=export_batch_id,
                request=commit.model_dump(mode="json"),
            )
        )
        if (
            receipt.stage_run_id != run_id
            or receipt.task_run_id != task_run_id
            or receipt.export_batch_id != export_batch_id
        ):
            raise RuntimeError("stage receipt differs from exact import identity")
        return receipt


__all__ = [
    "ORDERED_STAGE_DAG",
    "CandidateEvidenceSet",
    "CandidateStageEvidence",
    "CandidateStageOutcome",
    "PostImportStageReceipt",
    "PostgresPostImportStageFunction",
    "PreparedCandidate",
    "RegionTalkPostImportSupervisor",
    "StageAttemptReceipt",
    "StageCommitRequest",
    "StageEvidenceStatus",
    "StagePreparation",
    "StagePrepareRequest",
    "StageReceiptStatus",
    "StageRunStatus",
    "StageWorkRequest",
    "form_stage_commit",
    "stage_run_id",
    "work_item_id",
]
