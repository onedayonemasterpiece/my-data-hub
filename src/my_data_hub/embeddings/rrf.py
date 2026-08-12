from __future__ import annotations

from fractions import Fraction
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RankedRetrieverResult(BaseModel):
    """Rank-only retriever output: raw model scores are intentionally absent."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=100)
    document_ids: tuple[str, ...] = Field(max_length=10_000)
    index_revision: int | None = Field(default=None, ge=0)
    vector_space: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_]{0,99}$")

    @model_validator(mode="after")
    def documents_are_unique(self) -> RankedRetrieverResult:
        if len(self.document_ids) != len(set(self.document_ids)):
            raise ValueError("retriever ranking contains duplicate document IDs")
        return self


class UnavailableRetriever(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    name: str = Field(min_length=1, max_length=100)
    reason: str = Field(min_length=1, max_length=300)
    retryable: bool


class FusedHit(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    document_id: str
    rank: int = Field(ge=1)
    rrf_score: float = Field(gt=0)
    matched_by: tuple[str, ...]
    retriever_ranks: dict[str, int]


class FusionCoverage(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    requested_profile: Literal["recall_first_hybrid_v1"] = "recall_first_hybrid_v1"
    retrievers_requested: tuple[str, ...]
    retrievers_completed: tuple[str, ...]
    retrievers_unavailable: tuple[UnavailableRetriever, ...]
    is_complete: bool
    index_revisions: dict[str, int]


class FusionResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    rrf_k: int = Field(ge=1)
    hits: tuple[FusedHit, ...]
    coverage: FusionCoverage


def reciprocal_rank_fusion(
    *,
    requested_retrievers: tuple[str, ...],
    rankings: tuple[RankedRetrieverResult, ...],
    unavailable: tuple[UnavailableRetriever, ...] = (),
    k: int = 60,
) -> FusionResult:
    """Fuse ordinal ranks only; ties are stable by document ID."""

    if k < 1:
        raise ValueError("rrf k must be positive")
    if len(requested_retrievers) != len(set(requested_retrievers)):
        raise ValueError("requested retriever names must be unique")
    ranking_names = [item.name for item in rankings]
    unavailable_names = [item.name for item in unavailable]
    if len(ranking_names) != len(set(ranking_names)) or len(unavailable_names) != len(
        set(unavailable_names)
    ):
        raise ValueError("each retriever must be reported once")
    if set(ranking_names) & set(unavailable_names):
        raise ValueError("retriever cannot be both completed and unavailable")
    if set(ranking_names) | set(unavailable_names) != set(requested_retrievers):
        raise ValueError("every requested retriever must be completed or explicitly unavailable")

    totals: dict[str, Fraction] = {}
    ranks: dict[str, dict[str, int]] = {}
    for ranking in rankings:
        for rank, document_id in enumerate(ranking.document_ids, start=1):
            totals[document_id] = totals.get(document_id, Fraction()) + Fraction(1, k + rank)
            ranks.setdefault(document_id, {})[ranking.name] = rank

    ordered = sorted(totals, key=lambda document_id: (-totals[document_id], document_id))
    hits = tuple(
        FusedHit(
            document_id=document_id,
            rank=fusion_rank,
            rrf_score=round(float(totals[document_id]), 15),
            matched_by=tuple(sorted(ranks[document_id])),
            retriever_ranks=dict(sorted(ranks[document_id].items())),
        )
        for fusion_rank, document_id in enumerate(ordered, start=1)
    )
    revisions = {
        ranking.name: ranking.index_revision
        for ranking in rankings
        if ranking.index_revision is not None
    }
    return FusionResult(
        rrf_k=k,
        hits=hits,
        coverage=FusionCoverage(
            retrievers_requested=requested_retrievers,
            retrievers_completed=tuple(ranking_names),
            retrievers_unavailable=unavailable,
            is_complete=not unavailable,
            index_revisions=revisions,
        ),
    )
