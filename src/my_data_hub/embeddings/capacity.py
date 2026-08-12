from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.models import EmbeddingModelContract


class RetrievalMetrics(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    exact_relevance_case_count: int = Field(ge=1)
    recall_at_k: float = Field(ge=0, le=1)
    k: int = Field(ge=1)
    latency_p50_ms: float = Field(ge=0)
    latency_p95_ms: float = Field(ge=0)

    @model_validator(mode="after")
    def latency_order(self) -> RetrievalMetrics:
        if self.latency_p95_ms < self.latency_p50_ms:
            raise ValueError("latency p95 cannot be below p50")
        return self


class EmbeddingBenchmarkReceipt(BaseModel):
    """Observed quality/capacity receipt; zero or estimated values are rejected."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["embedding-search-benchmark-receipt.v1"] = (
        "embedding-search-benchmark-receipt.v1"
    )
    receipt_id: UUID
    run_id: str = Field(min_length=1, max_length=500)
    observed_at: datetime
    code_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    corpus_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    corpus_document_count: int = Field(ge=1)
    model: EmbeddingModelContract
    exact_search: RetrievalMetrics
    hnsw_search: RetrievalMetrics | None = None
    vector_heap_bytes: int = Field(ge=1)
    index_bytes: int = Field(ge=0)
    index_build_seconds: float | None = Field(default=None, gt=0)
    peak_build_memory_bytes: int | None = Field(default=None, ge=1)
    available_memory_bytes: int = Field(ge=1)
    checkpoint_bytes_before: int = Field(ge=1)
    checkpoint_bytes_after: int = Field(ge=1)
    checkpoint_seconds_after: float = Field(gt=0)
    capacity_proven: bool
    evidence_artifact_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def proof_requires_hnsw_observations(self) -> EmbeddingBenchmarkReceipt:
        hnsw_measurements = (
            self.hnsw_search,
            self.index_build_seconds,
            self.peak_build_memory_bytes,
        )
        if self.capacity_proven and (
            any(value is None for value in hnsw_measurements) or self.index_bytes <= 0
        ):
            raise ValueError("capacity_proven requires observed HNSW quality, size, build, and memory")
        return self


class HnswGateAction(StrEnum):
    DISABLED_BY_DEFAULT = "disabled_by_default"
    DENIED_MISSING_PROOF = "denied_missing_proof"
    DENIED_FAILED_PROOF = "denied_failed_proof"
    ALLOWED = "allowed"


class HnswCapacityPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled_by_default: Literal[False] = False
    minimum_recall_at_k: float = Field(default=0.95, ge=0, le=1)
    minimum_free_memory_fraction_after_peak: float = Field(default=0.20, ge=0, lt=1)
    maximum_checkpoint_growth_fraction: float = Field(default=0.50, ge=0)


class HnswGateDecision(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    action: HnswGateAction
    allowed: bool
    reasons: tuple[str, ...]
    benchmark_receipt_id: UUID | None = None


def evaluate_hnsw_gate(
    *,
    requested: bool,
    model: EmbeddingModelContract,
    policy: HnswCapacityPolicy | None = None,
    receipt: EmbeddingBenchmarkReceipt | None = None,
) -> HnswGateDecision:
    policy = policy or HnswCapacityPolicy()
    if not requested:
        return HnswGateDecision(
            action=HnswGateAction.DISABLED_BY_DEFAULT,
            allowed=False,
            reasons=("HNSW is opt-in and disabled by default",),
        )
    if receipt is None:
        return HnswGateDecision(
            action=HnswGateAction.DENIED_MISSING_PROOF,
            allowed=False,
            reasons=("no observed benchmark receipt was supplied",),
        )

    reasons: list[str] = []
    if receipt.model != model:
        reasons.append("benchmark model/revision/vector space does not match request")
    if not receipt.capacity_proven or receipt.hnsw_search is None:
        reasons.append("benchmark does not assert observed bounded capacity")
    elif receipt.hnsw_search.recall_at_k < policy.minimum_recall_at_k:
        reasons.append("HNSW recall is below policy threshold")
    assert receipt.peak_build_memory_bytes is not None or not receipt.capacity_proven
    if receipt.peak_build_memory_bytes is not None:
        free_fraction = max(
            0.0,
            (receipt.available_memory_bytes - receipt.peak_build_memory_bytes)
            / receipt.available_memory_bytes,
        )
        if free_fraction < policy.minimum_free_memory_fraction_after_peak:
            reasons.append("HNSW build leaves insufficient measured memory headroom")
    checkpoint_growth = (
        receipt.checkpoint_bytes_after - receipt.checkpoint_bytes_before
    ) / receipt.checkpoint_bytes_before
    if checkpoint_growth > policy.maximum_checkpoint_growth_fraction:
        reasons.append("HNSW checkpoint growth exceeds policy threshold")

    if reasons:
        return HnswGateDecision(
            action=HnswGateAction.DENIED_FAILED_PROOF,
            allowed=False,
            reasons=tuple(reasons),
            benchmark_receipt_id=receipt.receipt_id,
        )
    return HnswGateDecision(
        action=HnswGateAction.ALLOWED,
        allowed=True,
        reasons=("observed model-specific capacity and quality gates passed",),
        benchmark_receipt_id=receipt.receipt_id,
    )
