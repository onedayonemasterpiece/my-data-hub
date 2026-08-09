from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator


class InputArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    locator: str = Field(min_length=1, max_length=4000)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    byte_size: int | None = Field(default=None, ge=0)


class NotebookWorkItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    work_item_id: UUID
    subject_type: str = Field(min_length=1, max_length=200)
    subject_id: UUID
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    payload: dict[str, Any]


class NotebookModelSpec(BaseModel):
    model_config = ConfigDict(extra="forbid")
    provider: str = Field(min_length=1, max_length=300)
    name: str = Field(min_length=1, max_length=300)
    version: str = Field(min_length=1, max_length=300)
    task: str = Field(min_length=1, max_length=300)
    encoder_contract: str | None = Field(default=None, max_length=500)
    configuration: dict[str, Any] = Field(default_factory=dict)


class NotebookLimits(BaseModel):
    model_config = ConfigDict(extra="forbid")
    max_runtime_seconds: int = Field(ge=1, le=86_400)
    max_output_bytes: int = Field(ge=1)
    max_items: int | None = Field(default=None, ge=1, le=5000)


class NotebookInputManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["my-data-hub-notebook-input.v1"]
    run_id: UUID
    workload: str = Field(min_length=1, max_length=200)
    stage: str = Field(min_length=1, max_length=200)
    stage_contract_version: str = Field(min_length=1, max_length=300)
    canonical_revision: int = Field(ge=0)
    work_items: list[NotebookWorkItem] = Field(min_length=1, max_length=1000)
    artifacts: list[InputArtifactRef] = Field(default_factory=list, max_length=1000)
    model: NotebookModelSpec
    policy_versions: dict[str, str] = Field(default_factory=dict)
    limits: NotebookLimits
    created_at: datetime

    @model_validator(mode="after")
    def work_items_must_be_unique_and_bounded(self) -> "NotebookInputManifest":
        ids = [item.work_item_id for item in self.work_items]
        if len(ids) != len(set(ids)):
            raise ValueError("work_item_id values must be unique")
        if self.limits.max_items is not None and len(ids) > self.limits.max_items:
            raise ValueError("work item count exceeds limits.max_items")
        return self


class Producer(BaseModel):
    model_config = ConfigDict(extra="forbid")
    code_revision: str = Field(min_length=1, max_length=500)
    runtime: str = Field(min_length=1, max_length=500)
    model: dict[str, Any]


class ResultItem(BaseModel):
    model_config = ConfigDict(extra="forbid")
    work_item_id: UUID
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    output_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    status: Literal["succeeded", "failed", "skipped"]
    result: dict[str, Any]
    evidence: dict[str, Any] = Field(default_factory=dict)


class ResultFailure(BaseModel):
    model_config = ConfigDict(extra="forbid")
    work_item_id: UUID | None = None
    code: str = Field(min_length=1, max_length=300)
    message: str = Field(min_length=1, max_length=5000)
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid")
    artifact_type: str = Field(min_length=1, max_length=300)
    locator: str = Field(min_length=1, max_length=4000)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    byte_size: int | None = Field(default=None, ge=0)


class NotebookResult(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["my-data-hub-notebook-result.v1"]
    result_id: UUID
    run_id: UUID
    workload: str = Field(min_length=1, max_length=200)
    stage: str = Field(min_length=1, max_length=200)
    stage_contract_version: str = Field(min_length=1, max_length=300)
    input_manifest_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    producer: Producer
    status: Literal["succeeded", "partial", "failed"]
    items: list[ResultItem] = Field(max_length=5000)
    failures: list[ResultFailure] = Field(max_length=5000)
    metrics: dict[str, Any]
    provider_usage: list[dict[str, Any]] = Field(default_factory=list, max_length=5000)
    artifacts: list[ArtifactRef] = Field(max_length=1000)
    started_at: datetime
    completed_at: datetime

    @model_validator(mode="after")
    def every_work_item_is_accounted_once(self) -> "NotebookResult":
        completed = [item.work_item_id for item in self.items]
        failed = [item.work_item_id for item in self.failures if item.work_item_id is not None]
        identities = completed + failed
        if len(identities) != len(set(identities)):
            raise ValueError("a work_item_id may appear only once across items and failures")
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        if self.status == "succeeded" and self.failures:
            raise ValueError("succeeded result must not contain failures")
        if self.status == "failed" and self.items:
            raise ValueError("failed result must not contain successful items")
        return self


NotebookResultEnvelope = NotebookResult
