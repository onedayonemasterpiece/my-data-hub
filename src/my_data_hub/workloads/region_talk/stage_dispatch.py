"""Task/epoch-bound dispatch of Region Talk post-import worker stages.

This is the lightweight control loop between the fixed PostgreSQL stage-work
functions and one injected private-Notebook adapter.  The loop persists only
bounded identities, hashes, provider references and worker result metadata;
canonical content and credentials never enter its journal.
"""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from typing import Any, ClassVar, Literal, Protocol
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.notebooks.contracts import NotebookResult

from .stage_execution import HEAVY_STAGES, STAGE_BY_KEY

SHA256_PATTERN = r"^[a-f0-9]{64}$"
STAGE_EFFECT_NAMESPACE = UUID("54a0dba7-1e4b-4d56-a143-173304989e85")
MAX_RESULT_METADATA_BYTES = 64 * 1024


def _verify_raw_receipt(value: Any, label: str) -> Any:
    if not isinstance(value, dict):
        return value
    expected = hashlib.sha256(
        canonical_json_bytes({key: item for key, item in value.items() if key != "receipt_sha256"})
    ).hexdigest()
    if value.get("receipt_sha256") != expected:
        raise ValueError(f"{label} receipt_sha256 differs")
    return value


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class StageClaimRequest(StrictModel):
    schema_version: Literal["region-talk-stage-work-claim.v1"] = "region-talk-stage-work-claim.v1"
    lease_token: UUID
    lease_owner: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,200}$")
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @field_validator("requested_at")
    @classmethod
    def utc_requested_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("claim requested_at must be timezone-aware")
        return value.astimezone(UTC)


class StageWorkStatusRequest(StrictModel):
    schema_version: Literal["region-talk-stage-work-status-request.v1"] = "region-talk-stage-work-status-request.v1"
    work_item_id: UUID | None = None
    requested_at: datetime


class UpstreamStageResult(StrictModel):
    stage: str
    contract_version: str
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    result_metadata: dict[str, Any]


class StageExecutionPayload(StrictModel):
    schema_version: Literal["region-talk-stage-work-execution.v1"]
    stage_run_id: UUID
    candidate_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content_id: UUID
    content_type: str = Field(max_length=200)
    canonical_url: str = Field(max_length=4_000)
    canonical_source_key: str = Field(max_length=1_000)
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    upstream_results: tuple[UpstreamStageResult, ...] = Field(max_length=20)
    # Stage-specific canonical values are optional because older accepted rows
    # may have only hashes.  A worker must return FAILED_RETRYABLE when the
    # values required by its attached runtime are absent.
    input_data: dict[str, Any]
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


def stage_effect_id(*, work_item_id: UUID, attempt: int, input_fingerprint: str) -> UUID:
    return uuid5(
        STAGE_EFFECT_NAMESPACE,
        f"region-talk-stage-effect:{work_item_id}:{attempt}:{input_fingerprint}",
    )


class StageWorkClaimReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-work-claim-receipt.v1"]
    status: Literal["CLAIMED"]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    work_item_id: UUID
    stage: str
    contract_version: str
    subject_type: Literal["region_talk.candidate"]
    subject_id: UUID
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1, le=20)
    timeout_seconds: int = Field(ge=1, le=10_800)
    effect_id: UUID
    lease_token: UUID
    lease_expires_at: datetime
    payload: StageExecutionPayload
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "claim")

    @model_validator(mode="after")
    def exact_fixed_identity(self) -> StageWorkClaimReceipt:
        definition = STAGE_BY_KEY.get(self.stage)
        if self.stage not in HEAVY_STAGES or definition is None:
            raise ValueError("claim is not for a fixed Region Talk worker stage")
        if (
            self.contract_version != definition.contract_version
            or self.max_attempts != definition.max_attempts
            or self.timeout_seconds != definition.timeout_seconds
        ):
            raise ValueError("claim policy differs from the fixed stage DAG")
        if self.attempt > self.max_attempts:
            raise ValueError("claim attempt exceeds max_attempts")
        if self.effect_id != stage_effect_id(
            work_item_id=self.work_item_id,
            attempt=self.attempt,
            input_fingerprint=self.input_fingerprint,
        ):
            raise ValueError("claim effect_id is not deterministic")
        if (
            self.payload.stage_run_id != self.stage_run_id
            or self.payload.candidate_id != self.subject_id
            or self.payload.input_fingerprint != self.input_fingerprint
        ):
            raise ValueError("claim payload differs from its exact identity")
        return self


class StageClaimEmptyReceipt(StrictModel):
    """Same fixed claim receipt when no executable item can be leased."""

    schema_version: Literal["region-talk-stage-work-claim-receipt.v1"]
    status: Literal["EMPTY", "WAITING_DEPENDENCY", "COMPLETE", "FAILED"]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    work_item_id: None = None
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "empty claim")


StageClaimResponse = StageWorkClaimReceipt | StageClaimEmptyReceipt


class StageResultMetadata(StrictModel):
    schema_version: Literal["region-talk-stage-result-metadata.v1"] = "region-talk-stage-result-metadata.v1"
    stage: str
    contract_version: str
    subject_type: Literal["region_talk.candidate"]
    subject_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    producer_exact_id: str = Field(min_length=1, max_length=500)
    metrics: dict[str, Any]
    artifact_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def bounded_metadata(self) -> StageResultMetadata:
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_RESULT_METADATA_BYTES:
            raise ValueError("stage result metadata exceeds 64 KiB")
        return self


class StageWorkerStatus(StrEnum):
    SUCCEEDED = "SUCCEEDED"
    FAILED_RETRYABLE = "FAILED_RETRYABLE"
    FAILED_TERMINAL = "FAILED_TERMINAL"


class StageWorkerResult(StrictModel):
    """Bounded output returned by an exact private Notebook run."""

    schema_version: Literal["region-talk-stage-worker-output.v1"] = "region-talk-stage-worker-output.v1"
    effect_id: UUID
    work_item_id: UUID
    attempt: int = Field(ge=1)
    status: StageWorkerStatus
    result_metadata: StageResultMetadata
    metadata_sha256: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    completed_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_metadata_hash(self) -> StageWorkerResult:
        expected = hashlib.sha256(canonical_json_bytes(self.result_metadata.model_dump(mode="json"))).hexdigest()
        if self.metadata_sha256 != expected:
            raise ValueError("worker metadata_sha256 differs from exact metadata")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("worker completed_at must be timezone-aware")
        if self.status is StageWorkerStatus.SUCCEEDED:
            # Pure transforms use metadata as their immutable artifact.  Heavy
            # runtimes may instead bind an exact external artifact digest.
            artifact = self.result_metadata.artifact_sha256
            if self.result_sha256 not in {expected, artifact}:
                raise ValueError("successful worker result has no verified output digest")
        return self


def reconcile_notebook_result(claim: StageWorkClaimReceipt, raw_result: dict[str, Any]) -> StageWorkerResult:
    """Verify an exact generic Notebook envelope and bind it to one DB claim."""

    result = NotebookResult.model_validate(raw_result)
    if (
        result.run_id != claim.effect_id
        or result.workload != "region-talk"
        or result.stage != claim.stage
        or result.stage_contract_version != claim.contract_version
    ):
        raise ValueError("Notebook result differs from exact stage launch")
    items = [item for item in result.items if item.work_item_id == claim.work_item_id]
    failures = [item for item in result.failures if item.work_item_id == claim.work_item_id]
    if len(items) + len(failures) != 1:
        raise ValueError("Notebook result must account for the claimed item exactly once")
    if items:
        item = items[0]
        metadata = StageResultMetadata.model_validate(item.result)
        if (
            item.input_fingerprint != claim.input_fingerprint
            or item.output_fingerprint != hashlib.sha256(canonical_json_bytes(item.result)).hexdigest()
        ):
            raise ValueError("Notebook item fingerprint differs from exact content")
        status = StageWorkerStatus.SUCCEEDED
    else:
        failure = failures[0]
        assert failure.work_item_id is not None
        status = StageWorkerStatus.FAILED_RETRYABLE if failure.retryable else StageWorkerStatus.FAILED_TERMINAL
        producer = result.producer.model
        producer_id = "@".join(str(producer.get(key, "unknown")) for key in ("name", "version"))
        metadata = StageResultMetadata(
            stage=claim.stage,
            contract_version=claim.contract_version,
            subject_type=claim.subject_type,
            subject_id=claim.subject_id,
            candidate_revision=claim.payload.candidate_revision,
            revision_fingerprint=claim.payload.revision_fingerprint,
            input_fingerprint=claim.input_fingerprint,
            producer_exact_id=producer_id,
            metrics={
                "failure_code": failure.code,
                "failure_message_sha256": hashlib.sha256(failure.message.encode("utf-8")).hexdigest(),
                "retryable": failure.retryable,
            },
        )
    metadata_hash = hashlib.sha256(canonical_json_bytes(metadata.model_dump(mode="json"))).hexdigest()
    return StageWorkerResult(
        effect_id=claim.effect_id,
        work_item_id=claim.work_item_id,
        attempt=claim.attempt,
        status=status,
        result_metadata=metadata,
        metadata_sha256=metadata_hash,
        result_sha256=metadata.artifact_sha256 or metadata_hash,
        completed_at=result.completed_at,
    )


class StageResultSubmission(StrictModel):
    schema_version: Literal["region-talk-stage-worker-result.v1"] = "region-talk-stage-worker-result.v1"
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    work_item_id: UUID
    stage: str
    contract_version: str
    subject_type: Literal["region_talk.candidate"]
    subject_id: UUID
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt: int = Field(ge=1)
    effect_id: UUID
    lease_token: UUID
    result_status: StageWorkerStatus
    result_metadata: StageResultMetadata
    metadata_sha256: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    completed_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @classmethod
    def from_claim(cls, claim: StageWorkClaimReceipt, result: StageWorkerResult) -> StageResultSubmission:
        if (
            result.effect_id != claim.effect_id
            or result.work_item_id != claim.work_item_id
            or result.attempt != claim.attempt
            or result.result_metadata.stage != claim.stage
            or result.result_metadata.contract_version != claim.contract_version
            or result.result_metadata.subject_id != claim.subject_id
            or result.result_metadata.input_fingerprint != claim.input_fingerprint
            or result.result_metadata.candidate_revision != claim.payload.candidate_revision
            or result.result_metadata.revision_fingerprint != claim.payload.revision_fingerprint
        ):
            raise ValueError("worker result differs from the exact claim")
        return cls(
            master_instance_id=claim.master_instance_id,
            epoch=claim.epoch,
            task_run_id=claim.task_run_id,
            export_batch_id=claim.export_batch_id,
            stage_run_id=claim.stage_run_id,
            work_item_id=claim.work_item_id,
            stage=claim.stage,
            contract_version=claim.contract_version,
            subject_type=claim.subject_type,
            subject_id=claim.subject_id,
            candidate_revision=claim.payload.candidate_revision,
            revision_fingerprint=claim.payload.revision_fingerprint,
            input_fingerprint=claim.input_fingerprint,
            attempt=claim.attempt,
            effect_id=claim.effect_id,
            lease_token=claim.lease_token,
            result_status=result.status,
            result_metadata=result.result_metadata,
            metadata_sha256=result.metadata_sha256,
            result_sha256=result.result_sha256,
            completed_at=result.completed_at,
        )


class StageResultReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-worker-result-receipt.v1"]
    accepted: Literal[True]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    work_item_id: UUID
    stage: str
    subject_id: UUID
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    effect_id: UUID
    attempt: int = Field(ge=1)
    result_status: StageWorkerStatus
    metadata_sha256: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "result")


class FixedStageWorkFunctions(Protocol):
    def claim(self, *, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]: ...

    def submit(self, *, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]: ...

    def status(self, *, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]: ...


class PostgresStageWorkFunctions:
    """PostgreSQL port exposing exactly the two migration 0027 functions."""

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def _call(self, function: str, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]:
        if function not in {
            "migration.claim_region_talk_stage_work",
            "migration.submit_region_talk_stage_result",
            "migration.region_talk_stage_work_status",
        }:
            raise ValueError("unapproved Region Talk stage-work function")
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                f"SELECT {function}(%s,%s,%s::jsonb)",
                (task_run_id, export_batch_id, json.dumps(request)),
            ).fetchone()
        if row is None:
            self.connection.rollback()
            raise RuntimeError("master returned no Region Talk stage-work response")
        self.connection.commit()
        value = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        if not isinstance(value, dict):
            raise RuntimeError("master returned a non-object stage-work response")
        return value

    def claim(self, *, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]:
        return self._call(
            "migration.claim_region_talk_stage_work",
            task_run_id,
            export_batch_id,
            request,
        )

    def submit(self, *, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]:
        return self._call(
            "migration.submit_region_talk_stage_result",
            task_run_id,
            export_batch_id,
            request,
        )

    def status(self, *, task_run_id: UUID, export_batch_id: UUID, request: dict[str, Any]) -> dict[str, Any]:
        return self._call(
            "migration.region_talk_stage_work_status",
            task_run_id,
            export_batch_id,
            request,
        )


class ProviderObservationKind(StrEnum):
    ABSENT = "ABSENT"
    RUNNING = "RUNNING"
    TERMINAL = "TERMINAL"
    AMBIGUOUS = "AMBIGUOUS"


class StageWorkMetadataClaimReceipt(StrictModel):
    """Business-payload-free supervisor callback from migration 0028."""

    schema_version: Literal["region-talk-stage-work-metadata-claim-receipt.v2"]
    status: Literal["CLAIMED"]
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    work_item_id: UUID
    effect_id: UUID
    dispatch_id: UUID
    worker_task_run_id: UUID
    stage: str
    contract_version: str
    subject_type: Literal["region_talk.candidate"]
    subject_id: UUID
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt: int = Field(ge=1)
    max_attempts: int = Field(ge=1, le=20)
    timeout_seconds: int = Field(ge=1, le=10_800)
    lease_expires_at: datetime
    lease_token_sha256: str = Field(pattern=SHA256_PATTERN)
    lease_capability_sha256: str = Field(pattern=SHA256_PATTERN)
    claim_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "metadata claim")

    @model_validator(mode="after")
    def deterministic_identities(self) -> StageWorkMetadataClaimReceipt:
        if self.worker_task_run_id != stage_worker_task_run_id(
            supervisor_task_run_id=self.supervisor_task_run_id,
            work_item_id=self.work_item_id,
            attempt=self.attempt,
        ):
            raise ValueError("worker task identity is not deterministic")
        if self.dispatch_id != stage_dispatch_id(
            supervisor_task_run_id=self.supervisor_task_run_id,
            work_item_id=self.work_item_id,
            attempt=self.attempt,
            input_fingerprint=self.input_fingerprint,
        ):
            raise ValueError("stage dispatch identity is not deterministic")
        if self.effect_id != stage_effect_id(
            work_item_id=self.work_item_id,
            attempt=self.attempt,
            input_fingerprint=self.input_fingerprint,
        ):
            raise ValueError("stage effect identity is not deterministic")
        definition = STAGE_BY_KEY.get(self.stage)
        if (
            definition is None
            or self.stage not in HEAVY_STAGES
            or definition.contract_version != self.contract_version
            or definition.max_attempts != self.max_attempts
            or definition.timeout_seconds != self.timeout_seconds
        ):
            raise ValueError("metadata claim differs from the fixed DAG")
        return self


class StageWorkMetadataEmptyReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-work-metadata-claim-receipt.v2"]
    status: Literal["EMPTY", "WAITING_DEPENDENCY", "COMPLETE", "FAILED"]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    work_item_id: None = None
    dispatch_id: None = None
    worker_task_run_id: None = None
    effect_id: None = None
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "metadata empty")


class StageMetadataClaimRequest(StrictModel):
    schema_version: Literal["region-talk-stage-work-metadata-claim.v2"] = (
        "region-talk-stage-work-metadata-claim.v2"
    )
    claim_request_id: UUID
    lease_owner: str = Field(min_length=1, max_length=200)
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @field_validator("requested_at")
    @classmethod
    def utc_requested_at(cls, value: datetime) -> datetime:
        if value.tzinfo is None or value.utcoffset() is None:
            raise ValueError("metadata claim requested_at must be timezone-aware")
        return value.astimezone(UTC)


class StageWorkerBindRequest(StrictModel):
    schema_version: Literal["region-talk-stage-worker-bind.v1"] = "region-talk-stage-worker-bind.v1"
    dispatch_id: UUID
    effect_id: UUID
    claim_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_task_run_id: UUID
    worker_credential_id: UUID
    worker_generation: int = Field(ge=1)
    worker_command_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_task_token_sha256: str = Field(pattern=SHA256_PATTERN)
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class StageWorkerRotateRequest(StrictModel):
    schema_version: Literal["region-talk-stage-worker-rotate.v1"] = (
        "region-talk-stage-worker-rotate.v1"
    )
    dispatch_id: UUID
    effect_id: UUID
    work_item_id: UUID
    worker_task_run_id: UUID
    prior_worker_generation: int = Field(ge=1)
    prior_worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    new_worker_credential_id: UUID
    new_worker_generation: int = Field(ge=2)
    new_worker_command_sha256: str = Field(pattern=SHA256_PATTERN)
    new_worker_task_token_sha256: str = Field(pattern=SHA256_PATTERN)
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def successive_generation(self) -> StageWorkerRotateRequest:
        if self.new_worker_generation != self.prior_worker_generation + 1:
            raise ValueError("stage worker rotation must advance exactly one generation")
        return self


class StageWorkerRotateReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-worker-rotate-receipt.v1"]
    rotated: Literal[True]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    dispatch_id: UUID
    work_item_id: UUID
    effect_id: UUID
    worker_task_run_id: UUID
    prior_worker_generation: int = Field(ge=1)
    prior_worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_credential_id: UUID
    worker_generation: int = Field(ge=2)
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "worker rotation")

    @model_validator(mode="after")
    def successive_generation(self) -> StageWorkerRotateReceipt:
        if self.worker_generation != self.prior_worker_generation + 1:
            raise ValueError("stage worker rotation receipt skips a generation")
        return self


class StageWorkerCredentialStatus(StrictModel):
    """Metadata-only control response while a child credential is registered."""

    schema_version: Literal["region-talk-stage-worker-credential-status.v1"] = (
        "region-talk-stage-worker-credential-status.v1"
    )
    status: Literal["PENDING", "READY"]
    dispatch_id: UUID
    effect_id: UUID
    worker_task_run_id: UUID
    worker_credential_id: UUID | None = None
    worker_generation: int | None = Field(default=None, ge=1)
    worker_command_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_task_token_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def complete_ready_identity(self) -> StageWorkerCredentialStatus:
        ready = self.status == "READY"
        if ready != (self.worker_credential_id is not None and self.worker_generation is not None):
            raise ValueError("worker credential status is incomplete")
        return self


class StageWorkerPayloadFetchRequest(StrictModel):
    schema_version: Literal["region-talk-stage-work-payload-fetch.v1"] = (
        "region-talk-stage-work-payload-fetch.v1"
    )
    worker_task_run_id: UUID
    dispatch_id: UUID
    effect_id: UUID
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    requested_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class StageSupervisorStatusRequest(StrictModel):
    schema_version: Literal["region-talk-stage-supervisor-status-request.v1"] = (
        "region-talk-stage-supervisor-status-request.v1"
    )
    requested_at: datetime


class StageSupervisorStatusItem(StrictModel):
    dispatch_id: UUID
    work_item_id: UUID
    effect_id: UUID
    worker_task_run_id: UUID
    stage: str
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt: int = Field(ge=1)
    lease_expires_at: datetime
    worker_binding_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    work_status: Literal[
        "pending", "leased", "succeeded", "failed_retryable", "failed_terminal"
    ]
    result_ref: dict[str, Any] | None = None


class StageSupervisorStatusReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-supervisor-status-receipt.v1"]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    status: Literal["WAITING_WORK", "COMPLETE", "FAILED"]
    items: tuple[StageSupervisorStatusItem, ...] = Field(max_length=10_000)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "supervisor status")


def stage_worker_task_run_id(*, supervisor_task_run_id: UUID, work_item_id: UUID, attempt: int) -> UUID:
    return uuid5(
        STAGE_EFFECT_NAMESPACE,
        f"region-talk-stage-worker:{supervisor_task_run_id}:{work_item_id}:{attempt}",
    )


def stage_dispatch_id(
    *,
    supervisor_task_run_id: UUID,
    work_item_id: UUID,
    attempt: int,
    input_fingerprint: str,
) -> UUID:
    return uuid5(
        STAGE_EFFECT_NAMESPACE,
        f"region-talk-stage-dispatch:{supervisor_task_run_id}:{work_item_id}:{attempt}:{input_fingerprint}",
    )


class StageWorkerBindingReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-worker-bind-receipt.v1"]
    bound: Literal[True]
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    work_item_id: UUID
    effect_id: UUID
    dispatch_id: UUID
    worker_task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    worker_credential_id: UUID
    worker_generation: int = Field(ge=1)
    lease_capability_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "worker binding")


class StageWorkPayloadReceipt(StrictModel):
    """Private worker-only response; the raw lease and payload never go central."""

    schema_version: Literal["region-talk-stage-work-payload-receipt.v1"]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    supervisor_task_run_id: UUID
    worker_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    dispatch_id: UUID
    work_item_id: UUID
    effect_id: UUID
    stage: str
    contract_version: str
    subject_type: Literal["region_talk.candidate"]
    subject_id: UUID
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt: int = Field(ge=1)
    lease_token: UUID
    lease_expires_at: datetime
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    payload: StageExecutionPayload
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "worker payload")

    @model_validator(mode="after")
    def exact_identity(self) -> StageWorkPayloadReceipt:
        if (
            self.worker_task_run_id
            != stage_worker_task_run_id(
                supervisor_task_run_id=self.supervisor_task_run_id,
                work_item_id=self.work_item_id,
                attempt=self.attempt,
            )
            or self.effect_id
            != stage_effect_id(
                work_item_id=self.work_item_id,
                attempt=self.attempt,
                input_fingerprint=self.input_fingerprint,
            )
            or self.dispatch_id
            != stage_dispatch_id(
                supervisor_task_run_id=self.supervisor_task_run_id,
                work_item_id=self.work_item_id,
                attempt=self.attempt,
                input_fingerprint=self.input_fingerprint,
            )
            or self.payload.stage_run_id != self.stage_run_id
            or self.payload.candidate_id != self.subject_id
            or self.payload.input_fingerprint != self.input_fingerprint
        ):
            raise ValueError("private worker payload differs from its exact binding")
        definition = STAGE_BY_KEY.get(self.stage)
        if (
            definition is None
            or self.stage not in HEAVY_STAGES
            or definition.contract_version != self.contract_version
        ):
            raise ValueError("private worker payload differs from fixed stage DAG")
        return self


class StageWorkerDirectResultRequest(StrictModel):
    schema_version: Literal["region-talk-stage-worker-direct-result.v1"] = (
        "region-talk-stage-worker-direct-result.v1"
    )
    worker_task_run_id: UUID
    dispatch_id: UUID
    effect_id: UUID
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    work_item_id: UUID
    attempt: int = Field(ge=1)
    result_status: StageWorkerStatus
    result_metadata: StageResultMetadata
    metadata_sha256: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    completed_at: datetime
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_result_hash(self) -> StageWorkerDirectResultRequest:
        expected = hashlib.sha256(
            canonical_json_bytes(self.result_metadata.model_dump(mode="json"))
        ).hexdigest()
        if self.metadata_sha256 != expected:
            raise ValueError("direct worker metadata_sha256 differs")
        if self.completed_at.tzinfo is None or self.completed_at.utcoffset() is None:
            raise ValueError("direct worker completed_at must be timezone-aware")
        if self.result_status is StageWorkerStatus.SUCCEEDED and self.result_sha256 not in {
            expected,
            self.result_metadata.artifact_sha256,
        }:
            raise ValueError("successful direct worker result has no verified output digest")
        return self


class StageWorkerCombinedResultRequest(StrictModel):
    """One atomic public-metadata/private-heavy submission to the master."""

    schema_version: Literal["region-talk-stage-worker-combined-result.v1"] = (
        "region-talk-stage-worker-combined-result.v1"
    )
    direct_result: StageWorkerDirectResultRequest
    private_result: dict[str, Any] | None = None
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_private_success(self) -> StageWorkerCombinedResultRequest:
        heavy = self.direct_result.result_metadata.stage in {"image_scoring", "final_verifier", "writer"}
        succeeded = self.direct_result.result_status is StageWorkerStatus.SUCCEEDED
        if heavy and succeeded and self.private_result is None:
            raise ValueError("successful heavy stage lacks private result")
        if (not succeeded or not heavy) and self.private_result is not None:
            raise ValueError("private result is only valid for a successful heavy stage")
        return self


class StageWorkerDirectResultReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-worker-direct-result-receipt.v1"]
    accepted: Literal[True]
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    supervisor_task_run_id: UUID
    worker_task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    dispatch_id: UUID
    work_item_id: UUID
    effect_id: UUID
    stage: str
    subject_id: UUID
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    attempt: int = Field(ge=1)
    result_status: StageWorkerStatus
    metadata_sha256: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "direct worker result")


class PostgresStageSupervisorFunctions:
    """Supervisor-only 0028 functions; responses never contain business payloads."""

    _ALLOWED: ClassVar[frozenset[str]] = frozenset({
        "migration.claim_region_talk_stage_work_metadata",
        "migration.bind_region_talk_stage_worker",
        "migration.region_talk_stage_supervisor_status",
        "migration.rotate_region_talk_stage_worker_credential",
    })

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def _call(
        self,
        function: str,
        supervisor_task_run_id: UUID,
        export_batch_id: UUID,
        request: StrictModel,
    ) -> dict[str, Any]:
        if function not in self._ALLOWED:
            raise ValueError("unapproved Region Talk supervisor function")
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                f"SELECT {function}(%s,%s,%s::jsonb)",
                (
                    supervisor_task_run_id,
                    export_batch_id,
                    json.dumps(request.model_dump(mode="json")),
                ),
            ).fetchone()
        if row is None:
            self.connection.rollback()
            raise RuntimeError("master returned no Region Talk supervisor response")
        self.connection.commit()
        value = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        if not isinstance(value, dict):
            raise RuntimeError("master returned a non-object supervisor response")
        return value

    def claim_metadata(
        self,
        *,
        supervisor_task_run_id: UUID,
        export_batch_id: UUID,
        request: StageMetadataClaimRequest,
    ) -> StageWorkMetadataClaimReceipt | StageWorkMetadataEmptyReceipt:
        value = self._call(
            "migration.claim_region_talk_stage_work_metadata",
            supervisor_task_run_id,
            export_batch_id,
            request,
        )
        if value.get("status") == "CLAIMED":
            return StageWorkMetadataClaimReceipt.model_validate(value)
        return StageWorkMetadataEmptyReceipt.model_validate(value)

    def bind_worker(
        self,
        *,
        supervisor_task_run_id: UUID,
        export_batch_id: UUID,
        request: StageWorkerBindRequest,
    ) -> StageWorkerBindingReceipt:
        return StageWorkerBindingReceipt.model_validate(
            self._call(
                "migration.bind_region_talk_stage_worker",
                supervisor_task_run_id,
                export_batch_id,
                request,
            )
        )

    def status(
        self,
        *,
        supervisor_task_run_id: UUID,
        export_batch_id: UUID,
        request: StageSupervisorStatusRequest,
    ) -> StageSupervisorStatusReceipt:
        return StageSupervisorStatusReceipt.model_validate(
            self._call(
                "migration.region_talk_stage_supervisor_status",
                supervisor_task_run_id,
                export_batch_id,
                request,
            )
        )

    def rotate_worker(
        self,
        *,
        supervisor_task_run_id: UUID,
        export_batch_id: UUID,
        request: StageWorkerRotateRequest,
    ) -> StageWorkerRotateReceipt:
        return StageWorkerRotateReceipt.model_validate(
            self._call(
                "migration.rotate_region_talk_stage_worker_credential",
                supervisor_task_run_id,
                export_batch_id,
                request,
            )
        )


class PostgresStageWorkerFunctions:
    """Private child-worker 0028 functions; never instantiate on devstand."""

    _ALLOWED: ClassVar[frozenset[str]] = frozenset({
        "migration.fetch_region_talk_stage_work_payload",
        "migration.fetch_region_talk_heavy_stage_input",
        "migration.submit_region_talk_heavy_stage_worker_result",
    })

    def __init__(self, connection: Any) -> None:
        self.connection = connection

    def _call(
        self,
        function: str,
        worker_task_run_id: UUID,
        effect_id: UUID,
        request: StrictModel,
    ) -> dict[str, Any]:
        if function not in self._ALLOWED:
            raise ValueError("unapproved Region Talk private worker function")
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                f"SELECT {function}(%s,%s,%s::jsonb)",
                (
                    worker_task_run_id,
                    effect_id,
                    json.dumps(request.model_dump(mode="json")),
                ),
            ).fetchone()
        if row is None:
            self.connection.rollback()
            raise RuntimeError("master returned no Region Talk worker response")
        self.connection.commit()
        value = json.loads(row[0]) if isinstance(row[0], str) else row[0]
        if not isinstance(value, dict):
            raise RuntimeError("master returned a non-object private worker response")
        return value

    def fetch_payload(
        self,
        *,
        worker_task_run_id: UUID,
        effect_id: UUID,
        request: StageWorkerPayloadFetchRequest,
    ) -> StageWorkPayloadReceipt:
        return StageWorkPayloadReceipt.model_validate(
            self._call(
                "migration.fetch_region_talk_stage_work_payload",
                worker_task_run_id,
                effect_id,
                request,
            )
        )

    def submit_result(
        self,
        *,
        worker_task_run_id: UUID,
        effect_id: UUID,
        request: StageWorkerDirectResultRequest,
        private_result: dict[str, Any] | None = None,
    ) -> StageWorkerDirectResultReceipt:
        combined = StageWorkerCombinedResultRequest(
            direct_result=request,
            private_result=private_result,
        )
        return StageWorkerDirectResultReceipt.model_validate(
            self._call(
                "migration.submit_region_talk_heavy_stage_worker_result",
                worker_task_run_id,
                effect_id,
                combined,
            )
        )

    def fetch_heavy_input(
        self,
        *,
        worker_task_run_id: UUID,
        effect_id: UUID,
        request: StageWorkerPayloadFetchRequest,
    ) -> Any:
        from .heavy_wiring import HeavyStageInputReceipt

        return HeavyStageInputReceipt.model_validate(
            self._call(
                "migration.fetch_region_talk_heavy_stage_input",
                worker_task_run_id,
                effect_id,
                request,
            )
        )


class StageWorkerLaunch(StrictModel):
    """Metadata-only launch; content and DB capability stay adapter-private."""

    schema_version: Literal["region-talk-stage-worker-launch.v2"] = "region-talk-stage-worker-launch.v2"
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    stage_run_id: UUID
    work_item_id: UUID
    effect_id: UUID
    dispatch_id: UUID
    worker_task_run_id: UUID
    attempt: int = Field(ge=1)
    stage: str
    contract_version: str
    notebook_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    timeout_seconds: int = Field(ge=1, le=10_800)
    claim_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    worker_binding_sha256: str = Field(pattern=SHA256_PATTERN)
    lease_capability_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class StageProviderLaunchReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-provider-launch-receipt.v1"] = (
        "region-talk-stage-provider-launch-receipt.v1"
    )
    effect_id: UUID
    dispatch_id: UUID
    worker_task_run_id: UUID
    notebook_ref: str = Field(max_length=300)
    provider_run_ref: str = Field(min_length=3, max_length=500)
    source_sha256: str = Field(pattern=SHA256_PATTERN)
    launched_at: datetime
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt_hash(cls, value: Any) -> Any:
        return _verify_raw_receipt(value, "provider launch")


class StageProviderObservation(StrictModel):
    kind: ProviderObservationKind
    launch_receipt: StageProviderLaunchReceipt | None = None
    result_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)


class RegionTalkStageNotebookAdapter(Protocol):
    """Only central provider boundary; it owns private capability sidecars."""

    def observe(self, launch: StageWorkerLaunch) -> StageProviderObservation: ...

    def launch(self, launch: StageWorkerLaunch) -> StageProviderLaunchReceipt: ...


class RegionTalkStageControlBridge(Protocol):
    """Metadata-only supervisor-to-central handshake."""

    def prepare_worker(
        self, claim: StageWorkMetadataClaimReceipt
    ) -> StageWorkerCredentialStatus: ...

    def dispatch_bound(
        self,
        claim: StageWorkMetadataClaimReceipt,
        binding: StageWorkerBindingReceipt,
    ) -> StageProviderObservation: ...


@dataclass(slots=True)
class PrivateSupervisorStageCoordinator:
    """One bounded master-side claim/bind/dispatch step; never sees provider secrets."""

    functions: PostgresStageSupervisorFunctions
    bridge: RegionTalkStageControlBridge
    supervisor_task_run_id: UUID
    export_batch_id: UUID
    lease_owner: str
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    _claim_sequence: int = 0

    def reconcile_next(
        self,
    ) -> StageWorkMetadataClaimReceipt | StageWorkMetadataEmptyReceipt:
        claim_request_id = uuid5(
            STAGE_EFFECT_NAMESPACE,
            "region-talk-stage-metadata-claim:"
            f"{self.supervisor_task_run_id}:{self.export_batch_id}:{self._claim_sequence}",
        )
        claimed = self.functions.claim_metadata(
            supervisor_task_run_id=self.supervisor_task_run_id,
            export_batch_id=self.export_batch_id,
            request=StageMetadataClaimRequest(
                claim_request_id=claim_request_id,
                lease_owner=self.lease_owner,
                requested_at=self.clock().astimezone(UTC),
            ),
        )
        if isinstance(claimed, StageWorkMetadataEmptyReceipt):
            return claimed
        credential = self.bridge.prepare_worker(claimed)
        if credential.status == "PENDING":
            return claimed
        assert credential.worker_credential_id is not None
        assert credential.worker_generation is not None
        if (
            credential.dispatch_id != claimed.dispatch_id
            or credential.effect_id != claimed.effect_id
            or credential.worker_task_run_id != claimed.worker_task_run_id
        ):
            raise ValueError("child credential metadata differs from stage claim")
        binding = self.functions.bind_worker(
            supervisor_task_run_id=self.supervisor_task_run_id,
            export_batch_id=self.export_batch_id,
            request=StageWorkerBindRequest(
                dispatch_id=claimed.dispatch_id,
                effect_id=claimed.effect_id,
                claim_receipt_sha256=claimed.claim_receipt_sha256,
                worker_task_run_id=claimed.worker_task_run_id,
                worker_credential_id=credential.worker_credential_id,
                worker_generation=credential.worker_generation,
                worker_command_sha256=credential.worker_command_sha256,
                worker_task_token_sha256=credential.worker_task_token_sha256,
                requested_at=self.clock().astimezone(UTC),
            ),
        )
        self.bridge.dispatch_bound(claimed, binding)
        self._claim_sequence += 1
        return claimed


STAGE_NOTEBOOK_SLUGS: dict[str, str] = {
    "e5_embedding": "20-region-talk-e5-enrichment",
    "bge_m3_embedding": "30-region-talk-bge-m3-enrichment",
    "vector_fusion": "35-region-talk-vector-fusion",
    "image_scoring": "40-region-talk-image-diagnostic",
    "final_verifier": "50-region-talk-final-verifier",
    "writer": "70-region-talk-writer",
}


@dataclass(slots=True)
class _DispatchJournal:
    path: Path

    def __post_init__(self) -> None:
        if not self.path.is_absolute() or self.path.is_symlink():
            raise ValueError("stage dispatch journal path is unsafe")

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": "region-talk-stage-dispatch-journal.v2", "entries": {}}
        if self.path.is_symlink() or self.path.stat().st_size > 1024 * 1024:
            raise ValueError("stage dispatch journal is unsafe or oversized")
        value = json.loads(self.path.read_bytes())
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != "region-talk-stage-dispatch-journal.v2"
            or not isinstance(value.get("entries"), dict)
        ):
            raise ValueError("stage dispatch journal contract differs")
        return value

    def write(self, value: dict[str, Any]) -> None:
        encoded = canonical_json_bytes(value) + b"\n"
        forbidden = (b'"payload"', b'"input_data"', b'"text"', b'"lease_token"', b'"database_url"')
        if any(token in encoded for token in forbidden):
            raise ValueError("stage dispatch journal contains forbidden business/capability data")
        if len(encoded) > 1024 * 1024:
            raise ValueError("stage dispatch journal exceeds 1 MiB")
        self.path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path.parent.chmod(0o700)
        descriptor, raw = tempfile.mkstemp(prefix=".stage-dispatch.", dir=self.path.parent)
        try:
            os.fchmod(descriptor, 0o600)
            with os.fdopen(descriptor, "wb") as stream:
                stream.write(encoded)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(raw, self.path)
        finally:
            Path(raw).unlink(missing_ok=True)


@dataclass(slots=True)
class RegionTalkStageDispatcher:
    """Central metadata-only launch/restart reconciler."""

    adapter: RegionTalkStageNotebookAdapter
    notebook_owner: str
    journal_path: Path
    _journal: _DispatchJournal = field(init=False)

    def __post_init__(self) -> None:
        self._journal = _DispatchJournal(self.journal_path)

    def dispatch_bound(
        self,
        claim_value: dict[str, Any] | StageWorkMetadataClaimReceipt,
        binding_value: dict[str, Any] | StageWorkerBindingReceipt,
    ) -> StageProviderObservation:
        claim = (
            claim_value
            if isinstance(claim_value, StageWorkMetadataClaimReceipt)
            else StageWorkMetadataClaimReceipt.model_validate(claim_value)
        )
        binding = (
            binding_value
            if isinstance(binding_value, StageWorkerBindingReceipt)
            else StageWorkerBindingReceipt.model_validate(binding_value)
        )
        if (
            binding.supervisor_task_run_id != claim.supervisor_task_run_id
            or binding.export_batch_id != claim.export_batch_id
            or binding.stage_run_id != claim.stage_run_id
            or binding.work_item_id != claim.work_item_id
            or binding.effect_id != claim.effect_id
            or binding.dispatch_id != claim.dispatch_id
            or binding.worker_task_run_id != claim.worker_task_run_id
            or binding.master_instance_id != claim.master_instance_id
            or binding.epoch != claim.epoch
            or binding.lease_capability_sha256 != claim.lease_capability_sha256
        ):
            raise ValueError("worker binding differs from metadata claim")
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
            # A capability-bearing worker is unique per deterministic
            # dispatch.  Response-loss replay therefore targets the same slug
            # while no later task can overwrite its private Dataset binding.
            notebook_ref=(
                f"{self.notebook_owner}/mdh-rt-run-{claim.dispatch_id.hex[:24]}"
            ),
            input_fingerprint=claim.input_fingerprint,
            timeout_seconds=claim.timeout_seconds,
            claim_receipt_sha256=claim.claim_receipt_sha256,
            worker_binding_sha256=binding.worker_binding_sha256,
            lease_capability_sha256=claim.lease_capability_sha256,
        )
        journal = self._journal.read()
        key = str(claim.dispatch_id)
        entry = journal["entries"].get(key)
        if entry is None:
            entry = {
                "state": "BOUND",
                "launch": launch.model_dump(mode="json"),
                "claim_receipt_sha256": claim.receipt_sha256,
                "binding_receipt_sha256": binding.receipt_sha256,
                "provider_receipt": None,
            }
            journal["entries"][key] = entry
            self._journal.write(journal)
        elif entry["launch"] != launch.model_dump(mode="json"):
            raise ValueError("dispatch identity replay differs")
        observation = self.adapter.observe(launch)
        if observation.kind is ProviderObservationKind.AMBIGUOUS:
            raise RuntimeError("stage provider effect is ambiguous")
        if observation.kind is ProviderObservationKind.ABSENT:
            receipt = self.adapter.launch(launch)
            if (
                receipt.effect_id != launch.effect_id
                or receipt.dispatch_id != launch.dispatch_id
                or receipt.worker_task_run_id != launch.worker_task_run_id
            ):
                raise ValueError("provider launch receipt differs")
            entry["provider_receipt"] = receipt.model_dump(mode="json")
            entry["state"] = "LAUNCHED"
            self._journal.write(journal)
            return StageProviderObservation(kind=ProviderObservationKind.RUNNING, launch_receipt=receipt)
        return observation


__all__ = [
    "STAGE_NOTEBOOK_SLUGS",
    "PostgresStageSupervisorFunctions",
    "PostgresStageWorkFunctions",
    "PostgresStageWorkerFunctions",
    "PrivateSupervisorStageCoordinator",
    "ProviderObservationKind",
    "RegionTalkStageDispatcher",
    "RegionTalkStageNotebookAdapter",
    "StageExecutionPayload",
    "StageMetadataClaimRequest",
    "StageProviderLaunchReceipt",
    "StageProviderObservation",
    "StageResultMetadata",
    "StageResultReceipt",
    "StageResultSubmission",
    "StageSupervisorStatusReceipt",
    "StageSupervisorStatusRequest",
    "StageWorkClaimReceipt",
    "StageWorkMetadataClaimReceipt",
    "StageWorkMetadataEmptyReceipt",
    "StageWorkPayloadReceipt",
    "StageWorkerBindRequest",
    "StageWorkerBindingReceipt",
    "StageWorkerCombinedResultRequest",
    "StageWorkerCredentialStatus",
    "StageWorkerDirectResultReceipt",
    "StageWorkerDirectResultRequest",
    "StageWorkerLaunch",
    "StageWorkerPayloadFetchRequest",
    "StageWorkerResult",
    "StageWorkerRotateReceipt",
    "StageWorkerRotateRequest",
    "StageWorkerStatus",
    "reconcile_notebook_result",
    "stage_dispatch_id",
    "stage_effect_id",
    "stage_worker_task_run_id",
]
