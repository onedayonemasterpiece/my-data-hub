from __future__ import annotations

import math
from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.models import EmbeddingModelContract
from my_data_hub.hashing import sha256_value

SHA256_PATTERN = r"^[a-f0-9]{64}$"
NORM_TOLERANCE = 1e-3


def embedding_input_hash(document: SearchDocument, model: EmbeddingModelContract) -> str:
    return sha256_value(
        {
            "document_hash": document.document_hash,
            "representation_kind": document.representation_kind,
            "model_key": model.model_key,
            "model_revision": model.revision,
            "encoder_contract_version": model.encoder_contract_version,
        }
    )


def embedding_job_key(
    *,
    document_id: UUID,
    representation_kind: str,
    model: EmbeddingModelContract,
    input_hash: str,
) -> str:
    return sha256_value(
        {
            "document_id": str(document_id),
            "representation_kind": representation_kind,
            "model_id": model.exact_id,
            "input_hash": input_hash,
        }
    )


class EmbeddingJob(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["embedding-job.v1"] = "embedding-job.v1"
    job_key: str = Field(pattern=SHA256_PATTERN)
    document: SearchDocument
    document_hash: str = Field(pattern=SHA256_PATTERN)
    input_hash: str = Field(pattern=SHA256_PATTERN)
    model: EmbeddingModelContract
    canonical_revision: int = Field(ge=0)

    @classmethod
    def create(
        cls,
        *,
        document: SearchDocument,
        model: EmbeddingModelContract,
        canonical_revision: int,
    ) -> EmbeddingJob:
        input_hash = embedding_input_hash(document, model)
        return cls(
            job_key=embedding_job_key(
                document_id=document.document_id,
                representation_kind=document.representation_kind,
                model=model,
                input_hash=input_hash,
            ),
            document=document,
            document_hash=document.document_hash,
            input_hash=input_hash,
            model=model,
            canonical_revision=canonical_revision,
        )

    @model_validator(mode="after")
    def hashes_match_payload(self) -> EmbeddingJob:
        if self.document_hash != self.document.document_hash:
            raise ValueError("document_hash does not match canonical document")
        expected_input = embedding_input_hash(self.document, self.model)
        if self.input_hash != expected_input:
            raise ValueError("input_hash does not match document and model contract")
        expected_job = embedding_job_key(
            document_id=self.document.document_id,
            representation_kind=self.document.representation_kind,
            model=self.model,
            input_hash=self.input_hash,
        )
        if self.job_key != expected_job:
            raise ValueError("job_key does not match deterministic job identity")
        return self


class EmbeddingVectorResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["embedding-vector-result.v1"] = "embedding-vector-result.v1"
    job_key: str = Field(pattern=SHA256_PATTERN)
    document_id: UUID
    representation_kind: str = Field(min_length=1, max_length=200)
    input_hash: str = Field(pattern=SHA256_PATTERN)
    model_key: str = Field(min_length=1, max_length=300)
    model_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    vector_space: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")
    canonical_revision: int = Field(ge=0)
    dimensions: int = Field(ge=1, le=65_536)
    vector: tuple[float, ...] = Field(min_length=1, max_length=65_536)
    vector_sha256: str = Field(pattern=SHA256_PATTERN)

    @classmethod
    def from_job(cls, job: EmbeddingJob, vector: tuple[float, ...]) -> EmbeddingVectorResult:
        if not all(math.isfinite(value) for value in vector):
            raise ValueError("vector contains NaN or infinity")
        return cls(
            job_key=job.job_key,
            document_id=job.document.document_id,
            representation_kind=job.document.representation_kind,
            input_hash=job.input_hash,
            model_key=job.model.model_key,
            model_revision=job.model.revision,
            vector_space=job.model.vector_space,
            canonical_revision=job.canonical_revision,
            dimensions=job.model.dimensions,
            vector=vector,
            vector_sha256=sha256_value(vector),
        )

    @model_validator(mode="after")
    def validate_vector(self) -> EmbeddingVectorResult:
        if len(self.vector) != self.dimensions:
            raise ValueError(f"vector dimension mismatch: {len(self.vector)} != {self.dimensions}")
        if not all(math.isfinite(value) for value in self.vector):
            raise ValueError("vector contains NaN or infinity")
        norm = math.sqrt(sum(value * value for value in self.vector))
        if norm == 0:
            raise ValueError("vector must be nonzero")
        if abs(norm - 1.0) > NORM_TOLERANCE:
            raise ValueError(f"vector norm {norm:.9f} exceeds l2 tolerance {NORM_TOLERANCE}")
        if self.vector_sha256 != sha256_value(self.vector):
            raise ValueError("vector_sha256 does not match vector payload")
        return self


class FailureCode(StrEnum):
    ENCODER_UNAVAILABLE = "encoder_unavailable"
    INVALID_VECTOR = "invalid_vector"
    MODEL_CONTRACT_MISMATCH = "model_contract_mismatch"
    TIMEOUT = "timeout"


class EmbeddingFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    job_key: str = Field(pattern=SHA256_PATTERN)
    code: FailureCode
    message: str = Field(min_length=1, max_length=2000)
    retryable: bool
    terminal: bool

    @model_validator(mode="after")
    def terminal_is_not_retryable(self) -> EmbeddingFailure:
        if self.terminal == self.retryable:
            raise ValueError("exactly one of terminal and retryable must be true")
        return self


class EmbeddingWorkerPolicy(BaseModel):
    """Bounded retry/timeout policy carried by every worker receipt."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["embedding-worker-policy.v1"] = "embedding-worker-policy.v1"
    timeout_seconds: int = Field(default=1_800, ge=1, le=86_400)
    max_attempts: int = Field(default=3, ge=1, le=20)
    batch_size: int = Field(default=64, ge=1, le=5_000)
    retry_backoff_seconds: tuple[int, ...] = (30, 120)
    retryable_failure_codes: tuple[FailureCode, ...] = (
        FailureCode.ENCODER_UNAVAILABLE,
        FailureCode.TIMEOUT,
    )
    terminal_failure_codes: tuple[FailureCode, ...] = (
        FailureCode.INVALID_VECTOR,
        FailureCode.MODEL_CONTRACT_MISMATCH,
    )

    @model_validator(mode="after")
    def codes_are_complete_and_disjoint(self) -> EmbeddingWorkerPolicy:
        retryable = set(self.retryable_failure_codes)
        terminal = set(self.terminal_failure_codes)
        if retryable & terminal:
            raise ValueError("retryable and terminal failure codes overlap")
        if retryable | terminal != set(FailureCode):
            raise ValueError("worker policy must disposition every failure code")
        if len(self.retry_backoff_seconds) != max(0, self.max_attempts - 1):
            raise ValueError("retry backoff count must equal max_attempts - 1")
        if any(value < 0 for value in self.retry_backoff_seconds):
            raise ValueError("retry backoff cannot be negative")
        return self


class EmbeddingArtifactManifest(BaseModel):
    """Immutable worker artifact descriptor accepted by the master importer."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["embedding-artifact-manifest.v1"] = "embedding-artifact-manifest.v1"
    artifact_id: UUID
    run_id: UUID
    model: EmbeddingModelContract
    execution_policy: EmbeddingWorkerPolicy
    input_jobs_sha256: str = Field(pattern=SHA256_PATTERN)
    payload_sha256: str = Field(pattern=SHA256_PATTERN)
    successful_results: tuple[EmbeddingVectorResult, ...]
    failures: tuple[EmbeddingFailure, ...]
    total_jobs: int = Field(ge=1)
    started_at: datetime
    completed_at: datetime

    @model_validator(mode="after")
    def validate_accounting_and_model(self) -> EmbeddingArtifactManifest:
        if self.completed_at < self.started_at:
            raise ValueError("completed_at precedes started_at")
        keys = [item.job_key for item in self.successful_results] + [item.job_key for item in self.failures]
        if len(keys) != self.total_jobs or len(keys) != len(set(keys)):
            raise ValueError("every input job must have exactly one result or failure")
        for result in self.successful_results:
            if (
                result.model_key != self.model.model_key
                or result.model_revision != self.model.revision
                or result.vector_space != self.model.vector_space
                or result.dimensions != self.model.dimensions
            ):
                raise ValueError("result does not belong to manifest model/vector space")
        expected_payload = sha256_value(
            [item.model_dump(mode="json") for item in self.successful_results]
        )
        if self.payload_sha256 != expected_payload:
            raise ValueError("payload_sha256 does not match immutable successful results")
        return self
