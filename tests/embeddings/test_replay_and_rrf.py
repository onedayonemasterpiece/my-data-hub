from __future__ import annotations

from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.embeddings.contracts import EmbeddingJob, EmbeddingVectorResult
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE
from my_data_hub.embeddings.replay import (
    ReplayAction,
    VectorReplayState,
    apply_vector_result,
)
from my_data_hub.embeddings.routing import DenseSearchRoute
from my_data_hub.embeddings.rrf import (
    RankedRetrieverResult,
    UnavailableRetriever,
    reciprocal_rank_fusion,
)

DOCUMENT_ID = UUID("11111111-1111-4111-8111-111111111111")


def result(*, description: str, revision: int, model=E5_MULTILINGUAL_BASE):  # type: ignore[no-untyped-def]
    document = SearchDocument(
        document_id=DOCUMENT_ID,
        actor_kind="person",
        display_name="Анна",
        description=description,
    )
    job = EmbeddingJob.create(document=document, model=model, canonical_revision=revision)
    return EmbeddingVectorResult.from_job(
        job,
        (1.0, *([0.0] * (model.dimensions - 1))),
    )


def test_replay_exact_noop_conflict_and_stale_current_semantics() -> None:
    first = result(description="first", revision=7)
    inserted = apply_vector_result(VectorReplayState(), first)
    assert inserted.action == ReplayAction.INSERTED

    noop = apply_vector_result(inserted.state, first)
    assert noop.action == ReplayAction.EXACT_NOOP
    assert noop.state is inserted.state

    conflicting_payload = first.model_copy(update={"vector_sha256": "0" * 64})
    conflict = apply_vector_result(inserted.state, conflicting_payload)
    assert conflict.action == ReplayAction.CONFLICT
    assert conflict.state == inserted.state

    newer = result(description="new", revision=8)
    replaced = apply_vector_result(inserted.state, newer)
    assert replaced.action == ReplayAction.REPLACED_CURRENT
    assert replaced.stale_job_keys == (first.job_key,)
    assert sum(item.is_current for item in replaced.state.records) == 1

    late = result(description="late old data", revision=6)
    stale = apply_vector_result(replaced.state, late)
    assert stale.action == ReplayAction.STALE_RESULT
    assert stale.state == replaced.state


def test_replay_keeps_e5_and_bge_current_spaces_independent() -> None:
    state = apply_vector_result(VectorReplayState(), result(description="same", revision=1)).state
    decision = apply_vector_result(
        state,
        result(description="same", revision=1, model=BGE_M3),
    )
    assert decision.action == ReplayAction.INSERTED
    assert sum(item.is_current for item in decision.state.records) == 2


def test_dense_route_rejects_e5_query_against_bge_index() -> None:
    with pytest.raises(ValidationError, match="exact revisions differ"):
        DenseSearchRoute(
            query_model=E5_MULTILINGUAL_BASE,
            index_model=BGE_M3,
            index_vector_space=BGE_M3.vector_space,
        )
    route = DenseSearchRoute(
        query_model=E5_MULTILINGUAL_BASE,
        index_model=E5_MULTILINGUAL_BASE,
        index_vector_space=E5_MULTILINGUAL_BASE.vector_space,
    )
    assert route.index_vector_space == "e5_multilingual_base_768_v1"


def test_rrf_is_rank_only_deterministic_and_uses_stable_ties() -> None:
    rankings = (
        RankedRetrieverResult(name="e5", document_ids=("a", "b"), index_revision=9),
        RankedRetrieverResult(name="bge", document_ids=("b", "a"), index_revision=8),
    )
    first = reciprocal_rank_fusion(
        requested_retrievers=("e5", "bge"), rankings=rankings, k=60
    )
    second = reciprocal_rank_fusion(
        requested_retrievers=("e5", "bge"), rankings=tuple(reversed(rankings)), k=60
    )
    assert [item.document_id for item in first.hits] == ["a", "b"]
    assert first.hits == second.hits
    assert first.hits[0].rrf_score == first.hits[1].rrf_score
    with pytest.raises(ValidationError, match=r"[Ee]xtra inputs"):
        RankedRetrieverResult.model_validate(
            {"name": "e5", "document_ids": ["a"], "raw_scores": [0.9]}
        )


def test_rrf_discloses_unavailable_retrievers_without_silent_completeness() -> None:
    result = reciprocal_rank_fusion(
        requested_retrievers=("exact", "fts", "e5", "bge"),
        rankings=(
            RankedRetrieverResult(name="exact", document_ids=()),
            RankedRetrieverResult(name="fts", document_ids=("a",)),
            RankedRetrieverResult(
                name="e5",
                document_ids=("b",),
                vector_space=E5_MULTILINGUAL_BASE.vector_space,
            ),
        ),
        unavailable=(
            UnavailableRetriever(name="bge", reason="service_cold", retryable=True),
        ),
    )
    assert result.coverage.is_complete is False
    assert result.coverage.retrievers_completed == ("exact", "fts", "e5")
    assert result.coverage.retrievers_unavailable[0].reason == "service_cold"


def test_rrf_requires_every_requested_retriever_to_be_accounted() -> None:
    with pytest.raises(ValueError, match="explicitly unavailable"):
        reciprocal_rank_fusion(
            requested_retrievers=("e5", "bge"),
            rankings=(RankedRetrieverResult(name="e5", document_ids=("a",)),),
        )
