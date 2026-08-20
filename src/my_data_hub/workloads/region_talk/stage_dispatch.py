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
from typing import Any, Literal, Protocol
from uuid import UUID, uuid4, uuid5

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


class StageWorkerLaunch(StrictModel):
    schema_version: Literal["region-talk-stage-worker-launch.v1"] = "region-talk-stage-worker-launch.v1"
    task_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    stage_run_id: UUID
    work_item_id: UUID
    effect_id: UUID
    attempt: int = Field(ge=1)
    stage: str
    contract_version: str
    notebook_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=300)
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    timeout_seconds: int = Field(ge=1, le=10_800)
    claim_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    payload: StageExecutionPayload
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False


class StageProviderLaunchReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-provider-launch-receipt.v1"] = (
        "region-talk-stage-provider-launch-receipt.v1"
    )
    effect_id: UUID
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
    result: StageWorkerResult | None = None

    @model_validator(mode="after")
    def exact_observation_shape(self) -> StageProviderObservation:
        if self.kind is ProviderObservationKind.TERMINAL:
            if self.launch_receipt is None or self.result is None:
                raise ValueError("terminal observation requires launch and worker result")
        elif self.result is not None:
            raise ValueError("nonterminal observation cannot contain a worker result")
        return self


class RegionTalkStageNotebookAdapter(Protocol):
    """The only provider boundary used by the stage dispatcher."""

    def observe(self, launch: StageWorkerLaunch) -> StageProviderObservation: ...

    def launch(self, launch: StageWorkerLaunch) -> StageProviderLaunchReceipt: ...


STAGE_NOTEBOOK_SLUGS: dict[str, str] = {
    "e5_embedding": "20-region-talk-e5-enrichment",
    "bge_m3_embedding": "30-region-talk-bge-m3-enrichment",
    "vector_fusion": "35-region-talk-vector-fusion",
    "image_scoring": "40-region-talk-image-diagnostic",
    "final_verifier": "50-region-talk-final-verifier",
    "writer": "70-region-talk-writer",
}


class DispatchDisposition(StrEnum):
    WAITING_WORK = "WAITING_WORK"
    RESULT_ACCEPTED = "RESULT_ACCEPTED"
    COMPLETE = "COMPLETE"
    FAILED = "FAILED"


class StageDispatchReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-dispatch-receipt.v1"] = "region-talk-stage-dispatch-receipt.v1"
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    disposition: DispatchDisposition
    work_item_id: UUID | None = None
    effect_id: UUID | None = None
    provider_run_ref: str | None = Field(default=None, max_length=500)
    result_receipt_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    stage_receipt_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)


@dataclass(slots=True)
class _DispatchJournal:
    path: Path

    def __post_init__(self) -> None:
        if not self.path.is_absolute() or self.path.is_symlink():
            raise ValueError("stage dispatch journal path is unsafe")

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"schema_version": "region-talk-stage-dispatch-journal.v1", "entries": {}}
        if self.path.is_symlink() or self.path.stat().st_size > 1024 * 1024:
            raise ValueError("stage dispatch journal is unsafe or oversized")
        value = json.loads(self.path.read_bytes())
        if (
            not isinstance(value, dict)
            or value.get("schema_version") != "region-talk-stage-dispatch-journal.v1"
            or not isinstance(value.get("entries"), dict)
        ):
            raise ValueError("stage dispatch journal contract differs")
        return value

    def write(self, value: dict[str, Any]) -> None:
        encoded = canonical_json_bytes(value) + b"\n"
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
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        finally:
            Path(raw).unlink(missing_ok=True)


@dataclass(slots=True)
class RegionTalkStageDispatcher:
    functions: FixedStageWorkFunctions
    adapter: RegionTalkStageNotebookAdapter
    notebook_owner: str
    lease_owner: str
    journal_path: Path
    refresh_stages: Callable[[UUID, UUID], Any]
    clock: Callable[[], datetime] = lambda: datetime.now(UTC)
    uuid_factory: Callable[[], UUID] = uuid4
    _journal: _DispatchJournal = field(init=False)

    def __post_init__(self) -> None:
        if not self.notebook_owner or any(
            char not in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_.-"
            for char in self.notebook_owner
        ):
            raise ValueError("notebook_owner is not a Kaggle owner slug")
        self._journal = _DispatchJournal(self.journal_path)

    def execute_one(
        self,
        *,
        task_run_id: UUID,
        export_batch_id: UUID,
        master_instance_id: UUID,
        epoch: int,
    ) -> StageDispatchReceipt:
        journal = self._journal.read()
        entry = self._unfinished_entry(
            journal,
            task_run_id=task_run_id,
            export_batch_id=export_batch_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        if entry is None:
            now = self.clock().astimezone(UTC)
            request = StageClaimRequest(
                lease_token=self.uuid_factory(),
                lease_owner=self.lease_owner,
                requested_at=now,
            )
            raw = self.functions.claim(
                task_run_id=task_run_id,
                export_batch_id=export_batch_id,
                request=request.model_dump(mode="json"),
            )
            if raw.get("status") != "CLAIMED":
                empty = StageClaimEmptyReceipt.model_validate(raw)
                self._assert_binding(
                    empty.master_instance_id,
                    empty.epoch,
                    empty.task_run_id,
                    empty.export_batch_id,
                    master_instance_id,
                    epoch,
                    task_run_id,
                    export_batch_id,
                )
                return self._receipt_for_empty(empty)
            claim = StageWorkClaimReceipt.model_validate(raw)
            self._assert_binding(
                claim.master_instance_id,
                claim.epoch,
                claim.task_run_id,
                claim.export_batch_id,
                master_instance_id,
                epoch,
                task_run_id,
                export_batch_id,
            )
            launch = self._launch_for(claim)
            entry = {
                "state": "CLAIMED",
                # Preserve the database's exact timestamp strings because its
                # receipt hash covers the raw JSON object.
                "claim": raw,
                "launch": launch.model_dump(mode="json"),
                "provider_receipt": None,
                "submission": None,
                "result_receipt": None,
            }
            journal["entries"][str(claim.effect_id)] = entry
            self._journal.write(journal)  # persist exact effect before provider mutation
        claim = StageWorkClaimReceipt.model_validate(entry["claim"])
        launch = StageWorkerLaunch.model_validate(entry["launch"])

        if self.clock().astimezone(UTC) >= claim.lease_expires_at.astimezone(UTC):
            # The database owns retry transition and the next attempt number.
            # Never submit a late worker result under an expired lease and never
            # reinterpret expiration as success/terminal completion.
            entry["state"] = "LEASE_EXPIRED"
            self._journal.write(journal)
            return self._waiting_receipt(claim, entry)

        if entry.get("submission") is not None:
            submission = StageResultSubmission.model_validate(entry["submission"])
            result_receipt = self._submit_exact(
                journal,
                entry,
                claim,
                submission,
                task_run_id=task_run_id,
                export_batch_id=export_batch_id,
            )
            return self._accepted_receipt(claim, entry, result_receipt)

        observation = self.adapter.observe(launch)
        if observation.kind is ProviderObservationKind.AMBIGUOUS:
            raise RuntimeError("stage provider effect is ambiguous; refusing duplicate launch")
        if observation.kind is ProviderObservationKind.ABSENT:
            provider_receipt = self.adapter.launch(launch)
            if provider_receipt.effect_id != launch.effect_id:
                raise ValueError("provider launch receipt differs from deterministic effect")
            entry["provider_receipt"] = provider_receipt.model_dump(mode="json")
            entry["state"] = "LAUNCHED"
            self._journal.write(journal)
            observation = self.adapter.observe(launch)
        if observation.launch_receipt is not None:
            if observation.launch_receipt.effect_id != claim.effect_id:
                raise ValueError("provider observation differs from exact effect")
            entry["provider_receipt"] = observation.launch_receipt.model_dump(mode="json")
        if observation.kind is not ProviderObservationKind.TERMINAL:
            entry["state"] = "LAUNCHED"
            self._journal.write(journal)
            return self._waiting_receipt(claim, entry)

        assert observation.result is not None
        submission = StageResultSubmission.from_claim(claim, observation.result)
        entry["submission"] = submission.model_dump(mode="json")
        entry["state"] = "RESULT_READY"
        self._journal.write(journal)  # response-loss replay owns exact result bytes
        result_receipt = self._submit_exact(
            journal,
            entry,
            claim,
            submission,
            task_run_id=task_run_id,
            export_batch_id=export_batch_id,
        )
        return self._accepted_receipt(claim, entry, result_receipt)

    def _submit_exact(
        self,
        journal: dict[str, Any],
        entry: dict[str, Any],
        claim: StageWorkClaimReceipt,
        submission: StageResultSubmission,
        *,
        task_run_id: UUID,
        export_batch_id: UUID,
    ) -> StageResultReceipt:
        raw = self.functions.submit(
            task_run_id=task_run_id,
            export_batch_id=export_batch_id,
            request=submission.model_dump(mode="json"),
        )
        receipt = StageResultReceipt.model_validate(raw)
        if (
            receipt.master_instance_id != claim.master_instance_id
            or receipt.epoch != claim.epoch
            or receipt.task_run_id != claim.task_run_id
            or receipt.export_batch_id != claim.export_batch_id
            or receipt.stage_run_id != claim.stage_run_id
            or receipt.work_item_id != claim.work_item_id
            or receipt.effect_id != claim.effect_id
            or receipt.attempt != claim.attempt
            or receipt.stage != claim.stage
            or receipt.subject_id != claim.subject_id
            or receipt.input_fingerprint != claim.input_fingerprint
            or receipt.result_status is not submission.result_status
            or receipt.metadata_sha256 != submission.metadata_sha256
            or receipt.result_sha256 != submission.result_sha256
        ):
            raise ValueError("stage result receipt differs from exact submission")
        entry["result_receipt"] = receipt.model_dump(mode="json")
        entry["state"] = "RESULT_ACCEPTED"
        self._journal.write(journal)
        return receipt

    def _accepted_receipt(
        self,
        claim: StageWorkClaimReceipt,
        entry: dict[str, Any],
        result_receipt: StageResultReceipt,
    ) -> StageDispatchReceipt:
        stage_receipt = self.refresh_stages(claim.task_run_id, claim.export_batch_id)
        stage_status = str(getattr(stage_receipt, "status", "WAITING_WORK"))
        stage_hash = getattr(stage_receipt, "receipt_sha256", None)
        disposition = (
            DispatchDisposition.COMPLETE
            if stage_status in {"COMPLETE", "StageRunStatus.COMPLETE"}
            else DispatchDisposition.FAILED
            if stage_status in {"FAILED", "StageRunStatus.FAILED"}
            else DispatchDisposition.RESULT_ACCEPTED
        )
        body = {
            "schema_version": "region-talk-stage-dispatch-receipt.v1",
            "task_run_id": str(claim.task_run_id),
            "export_batch_id": str(claim.export_batch_id),
            "stage_run_id": str(claim.stage_run_id),
            "master_instance_id": str(claim.master_instance_id),
            "epoch": claim.epoch,
            "disposition": disposition.value,
            "work_item_id": str(claim.work_item_id),
            "effect_id": str(claim.effect_id),
            "provider_run_ref": self._provider_ref(entry),
            "result_receipt_sha256": result_receipt.receipt_sha256,
            "stage_receipt_sha256": stage_hash,
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        return StageDispatchReceipt(**body, receipt_sha256=self._hash(body))

    def _waiting_receipt(self, claim: StageWorkClaimReceipt, entry: dict[str, Any]) -> StageDispatchReceipt:
        body = {
            "schema_version": "region-talk-stage-dispatch-receipt.v1",
            "task_run_id": str(claim.task_run_id),
            "export_batch_id": str(claim.export_batch_id),
            "stage_run_id": str(claim.stage_run_id),
            "master_instance_id": str(claim.master_instance_id),
            "epoch": claim.epoch,
            "disposition": DispatchDisposition.WAITING_WORK.value,
            "work_item_id": str(claim.work_item_id),
            "effect_id": str(claim.effect_id),
            "provider_run_ref": self._provider_ref(entry),
            "result_receipt_sha256": None,
            "stage_receipt_sha256": None,
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        return StageDispatchReceipt(**body, receipt_sha256=self._hash(body))

    def _receipt_for_empty(self, empty: StageClaimEmptyReceipt) -> StageDispatchReceipt:
        disposition = {
            "COMPLETE": DispatchDisposition.COMPLETE,
            "FAILED": DispatchDisposition.FAILED,
            "EMPTY": DispatchDisposition.WAITING_WORK,
            "WAITING_DEPENDENCY": DispatchDisposition.WAITING_WORK,
        }[empty.status]
        body = {
            "schema_version": "region-talk-stage-dispatch-receipt.v1",
            "task_run_id": str(empty.task_run_id),
            "export_batch_id": str(empty.export_batch_id),
            "stage_run_id": str(empty.stage_run_id),
            "master_instance_id": str(empty.master_instance_id),
            "epoch": empty.epoch,
            "disposition": disposition.value,
            "work_item_id": None,
            "effect_id": None,
            "provider_run_ref": None,
            "result_receipt_sha256": None,
            "stage_receipt_sha256": None,
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        return StageDispatchReceipt(**body, receipt_sha256=self._hash(body))

    def _launch_for(self, claim: StageWorkClaimReceipt) -> StageWorkerLaunch:
        slug = STAGE_NOTEBOOK_SLUGS[claim.stage]
        return StageWorkerLaunch(
            task_run_id=claim.task_run_id,
            master_instance_id=claim.master_instance_id,
            epoch=claim.epoch,
            stage_run_id=claim.stage_run_id,
            work_item_id=claim.work_item_id,
            effect_id=claim.effect_id,
            attempt=claim.attempt,
            stage=claim.stage,
            contract_version=claim.contract_version,
            notebook_ref=f"{self.notebook_owner}/{slug}",
            input_fingerprint=claim.input_fingerprint,
            timeout_seconds=claim.timeout_seconds,
            claim_receipt_sha256=claim.receipt_sha256,
            payload=claim.payload,
        )

    @staticmethod
    def _unfinished_entry(
        journal: dict[str, Any],
        *,
        task_run_id: UUID,
        export_batch_id: UUID,
        master_instance_id: UUID,
        epoch: int,
    ) -> dict[str, Any] | None:
        matches: list[dict[str, Any]] = []
        for value in journal["entries"].values():
            if value.get("state") in {"RESULT_ACCEPTED", "LEASE_EXPIRED"}:
                continue
            claim = StageWorkClaimReceipt.model_validate(value.get("claim"))
            if (
                claim.task_run_id == task_run_id
                and claim.export_batch_id == export_batch_id
                and claim.master_instance_id == master_instance_id
                and claim.epoch == epoch
            ):
                matches.append(value)
        if len(matches) > 1:
            raise ValueError("journal contains multiple in-flight effects for one task")
        return matches[0] if matches else None

    @staticmethod
    def _assert_binding(
        observed_master: UUID,
        observed_epoch: int,
        observed_task: UUID,
        observed_batch: UUID,
        expected_master: UUID,
        expected_epoch: int,
        expected_task: UUID,
        expected_batch: UUID,
    ) -> None:
        if (
            observed_master != expected_master
            or observed_epoch != expected_epoch
            or observed_task != expected_task
            or observed_batch != expected_batch
        ):
            raise ValueError("stage-work response differs from task/master/epoch binding")

    @staticmethod
    def _provider_ref(entry: dict[str, Any]) -> str | None:
        value = entry.get("provider_receipt")
        return str(value["provider_run_ref"]) if isinstance(value, dict) else None

    @staticmethod
    def _hash(value: dict[str, Any]) -> str:
        return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


__all__ = [
    "STAGE_NOTEBOOK_SLUGS",
    "DispatchDisposition",
    "PostgresStageWorkFunctions",
    "ProviderObservationKind",
    "RegionTalkStageDispatcher",
    "RegionTalkStageNotebookAdapter",
    "StageClaimEmptyReceipt",
    "StageClaimRequest",
    "StageDispatchReceipt",
    "StageExecutionPayload",
    "StageProviderLaunchReceipt",
    "StageProviderObservation",
    "StageResultMetadata",
    "StageResultReceipt",
    "StageResultSubmission",
    "StageWorkClaimReceipt",
    "StageWorkStatusRequest",
    "StageWorkerLaunch",
    "StageWorkerResult",
    "StageWorkerStatus",
    "stage_effect_id",
]
