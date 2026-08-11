from __future__ import annotations

import math
from datetime import UTC, datetime
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.embeddings.contracts import EmbeddingJob, EmbeddingVectorResult
from my_data_hub.embeddings.documents import SearchDocument
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE
from my_data_hub.embeddings.worker import EmbeddingWorker

DOCUMENT_ID = UUID("11111111-1111-4111-8111-111111111111")
RUN_ID = UUID("22222222-2222-4222-8222-222222222222")
NOW = datetime(2026, 8, 10, tzinfo=UTC)


def document(*, description: str = "Путешествия по России") -> SearchDocument:
    return SearchDocument(
        document_id=DOCUMENT_ID,
        actor_kind="person",
        display_name="  Анна   Автор  ",
        description=description,
        accounts=("@anna", " https://example.test/anna ", "@anna"),
        geography_signals=("Россия", "Калининград"),
        project_memberships=("region-talk",),
    )


def unit_vector(dimensions: int) -> tuple[float, ...]:
    return (1.0, *([0.0] * (dimensions - 1)))


class FakeEncoder:
    def __init__(self, *, dimensions: int, value: float = 1.0) -> None:
        self.dimensions = dimensions
        self.value = value
        self.calls: list[dict[str, object]] = []

    def encode(self, texts, **kwargs):  # type: ignore[no-untyped-def]
        self.calls.append({"texts": tuple(texts), **kwargs})
        return [(self.value, *([0.0] * (self.dimensions - 1))) for _ in texts]


def test_exact_model_contracts_are_pinned_and_separate() -> None:
    assert E5_MULTILINGUAL_BASE.model_key == "intfloat/multilingual-e5-base"
    assert E5_MULTILINGUAL_BASE.revision == "d128750597153bb5987e10b1c3493a34e5a4502a"
    assert E5_MULTILINGUAL_BASE.dimensions == 768
    assert E5_MULTILINGUAL_BASE.max_tokens == 512
    assert E5_MULTILINGUAL_BASE.query_prefix == "query: "
    assert E5_MULTILINGUAL_BASE.document_prefix == "passage: "
    assert E5_MULTILINGUAL_BASE.pooling == "attention_mask_mean"
    assert BGE_M3.model_key == "BAAI/bge-m3"
    assert BGE_M3.revision == "5617a9f61b028005a4858fdac845db406aefb181"
    assert BGE_M3.dimensions == 1024
    assert BGE_M3.output_modes == ("dense",)
    assert BGE_M3.normalization == "l2"
    assert BGE_M3.vector_space != E5_MULTILINGUAL_BASE.vector_space


def test_compact_document_and_hash_are_deterministic() -> None:
    left = document()
    right = SearchDocument(
        document_id=DOCUMENT_ID,
        actor_kind="person",
        display_name="Анна Автор",
        description="Путешествия   по России",
        accounts=("https://example.test/anna", "@anna"),
        geography_signals=("Калининград", "Россия"),
        project_memberships=("region-talk",),
    )
    assert left.compact_text() == right.compact_text()
    assert left.document_hash == right.document_hash


def test_document_change_changes_input_and_job_hash() -> None:
    first = EmbeddingJob.create(
        document=document(), model=E5_MULTILINGUAL_BASE, canonical_revision=7
    )
    second = EmbeddingJob.create(
        document=document(description="Новый текст"),
        model=E5_MULTILINGUAL_BASE,
        canonical_revision=8,
    )
    assert first.document_hash != second.document_hash
    assert first.input_hash != second.input_hash
    assert first.job_key != second.job_key


def test_e5_worker_enforces_prefix_pool_normalization_and_dimension() -> None:
    job = EmbeddingJob.create(
        document=document(), model=E5_MULTILINGUAL_BASE, canonical_revision=7
    )
    encoder = FakeEncoder(dimensions=768)
    manifest = EmbeddingWorker(model=E5_MULTILINGUAL_BASE, encoder=encoder).run(
        run_id=RUN_ID,
        jobs=(job,),
        started_at=NOW,
        completed_at=NOW,
    )
    assert not manifest.failures
    assert len(manifest.successful_results[0].vector) == 768
    call = encoder.calls[0]
    assert call["texts"][0].startswith("passage: ")  # type: ignore[index,union-attr]
    assert call["max_tokens"] == 512
    assert call["pooling"] == "attention_mask_mean"
    assert call["normalize"] is True
    assert call["dense_only"] is True


def test_bge_worker_is_dense_only_normalized_and_has_no_e5_prefix() -> None:
    job = EmbeddingJob.create(document=document(), model=BGE_M3, canonical_revision=7)
    encoder = FakeEncoder(dimensions=1024)
    manifest = EmbeddingWorker(model=BGE_M3, encoder=encoder).run(
        run_id=RUN_ID,
        jobs=(job,),
        started_at=NOW,
        completed_at=NOW,
    )
    call = encoder.calls[0]
    assert call["texts"][0].startswith("name: ")  # type: ignore[index,union-attr]
    assert call["pooling"] == "model_native_dense"
    assert call["dense_only"] is True
    assert manifest.successful_results[0].vector_space == BGE_M3.vector_space


def test_query_representation_uses_exact_query_contract_not_document_text() -> None:
    query_document = SearchDocument(
        document_id=DOCUMENT_ID,
        representation_kind="blogger_search_query_v1",
        actor_kind="search_query",
        display_name="калининград культура",
    )
    job = EmbeddingJob.create(
        document=query_document, model=E5_MULTILINGUAL_BASE, canonical_revision=7
    )
    encoder = FakeEncoder(dimensions=768)
    EmbeddingWorker(model=E5_MULTILINGUAL_BASE, encoder=encoder).run(
        run_id=RUN_ID, jobs=(job,), started_at=NOW, completed_at=NOW
    )
    assert encoder.calls[0]["texts"] == ("query: калининград культура",)


@pytest.mark.parametrize(
    ("vector", "message"),
    [
        ((1.0,), "dimension mismatch"),
        ((math.nan, *([0.0] * 767)), "NaN or infinity"),
        ((0.0,) * 768, "nonzero"),
        ((2.0, *([0.0] * 767)), "l2 tolerance"),
    ],
)
def test_vector_result_rejects_invalid_artifacts(vector, message: str) -> None:  # type: ignore[no-untyped-def]
    job = EmbeddingJob.create(
        document=document(), model=E5_MULTILINGUAL_BASE, canonical_revision=7
    )
    with pytest.raises((ValidationError, ValueError), match=message):
        EmbeddingVectorResult.from_job(job, vector)


def test_worker_accounts_invalid_vector_as_terminal_failure() -> None:
    job = EmbeddingJob.create(
        document=document(), model=E5_MULTILINGUAL_BASE, canonical_revision=7
    )
    manifest = EmbeddingWorker(
        model=E5_MULTILINGUAL_BASE,
        encoder=FakeEncoder(dimensions=768, value=2.0),
    ).run(run_id=RUN_ID, jobs=(job,), started_at=NOW, completed_at=NOW)
    assert not manifest.successful_results
    assert manifest.failures[0].code == "invalid_vector"
    assert manifest.failures[0].terminal is True
    assert manifest.failures[0].retryable is False


def test_manifest_cannot_mix_e5_and_bge_spaces() -> None:
    e5_job = EmbeddingJob.create(
        document=document(), model=E5_MULTILINGUAL_BASE, canonical_revision=7
    )
    bge_vector = EmbeddingVectorResult.from_job(
        EmbeddingJob.create(document=document(), model=BGE_M3, canonical_revision=7),
        unit_vector(1024),
    )
    good = EmbeddingWorker(
        model=E5_MULTILINGUAL_BASE,
        encoder=FakeEncoder(dimensions=768),
    ).run(run_id=RUN_ID, jobs=(e5_job,), started_at=NOW, completed_at=NOW)
    payload = good.model_dump(mode="python")
    payload["successful_results"] = (bge_vector,)
    with pytest.raises(ValidationError, match="manifest model/vector space"):
        type(good).model_validate(payload)
