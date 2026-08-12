from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import jsonschema

from my_data_hub.embeddings.capacity import (
    EmbeddingBenchmarkReceipt,
    HnswGateAction,
    RetrievalMetrics,
    evaluate_hnsw_gate,
)
from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE

ROOT = Path(__file__).resolve().parents[2]


def benchmark(*, capacity_proven: bool = True) -> EmbeddingBenchmarkReceipt:
    exact = RetrievalMetrics(
        exact_relevance_case_count=25,
        recall_at_k=1.0,
        k=10,
        latency_p50_ms=2,
        latency_p95_ms=4,
    )
    return EmbeddingBenchmarkReceipt(
        receipt_id=UUID("33333333-3333-4333-8333-333333333333"),
        run_id="kaggle-run-observed-fixture",
        observed_at=datetime(2026, 8, 10, tzinfo=UTC),
        code_revision="a" * 40,
        corpus_sha256="b" * 64,
        corpus_document_count=100,
        model=E5_MULTILINGUAL_BASE,
        exact_search=exact,
        hnsw_search=(
            RetrievalMetrics(
                exact_relevance_case_count=25,
                recall_at_k=0.99,
                k=10,
                latency_p50_ms=1,
                latency_p95_ms=3,
            )
            if capacity_proven
            else None
        ),
        vector_heap_bytes=154_400,
        index_bytes=60_000 if capacity_proven else 0,
        index_build_seconds=2 if capacity_proven else None,
        peak_build_memory_bytes=500_000 if capacity_proven else None,
        available_memory_bytes=1_000_000,
        checkpoint_bytes_before=1_000_000,
        checkpoint_bytes_after=1_200_000,
        checkpoint_seconds_after=3,
        capacity_proven=capacity_proven,
        evidence_artifact_sha256="c" * 64,
    )


def test_hnsw_is_off_by_default_and_requires_matching_observed_proof() -> None:
    disabled = evaluate_hnsw_gate(requested=False, model=E5_MULTILINGUAL_BASE)
    assert disabled.action == HnswGateAction.DISABLED_BY_DEFAULT
    assert disabled.allowed is False

    missing = evaluate_hnsw_gate(requested=True, model=E5_MULTILINGUAL_BASE)
    assert missing.action == HnswGateAction.DENIED_MISSING_PROOF

    failed = evaluate_hnsw_gate(
        requested=True,
        model=E5_MULTILINGUAL_BASE,
        receipt=benchmark(capacity_proven=False),
    )
    assert failed.action == HnswGateAction.DENIED_FAILED_PROOF

    mismatch = evaluate_hnsw_gate(
        requested=True,
        model=BGE_M3,
        receipt=benchmark(),
    )
    assert mismatch.action == HnswGateAction.DENIED_FAILED_PROOF

    allowed = evaluate_hnsw_gate(
        requested=True,
        model=E5_MULTILINGUAL_BASE,
        receipt=benchmark(),
    )
    assert allowed.action == HnswGateAction.ALLOWED
    assert allowed.allowed is True


def test_embedding_notebook_sources_are_separate_and_db_free() -> None:
    source_dir = ROOT / "notebooks/templates/embedding_workers"
    e5 = (source_dir / "e5_worker.py").read_text(encoding="utf-8")
    bge = (source_dir / "bge_m3_worker.py").read_text(encoding="utf-8")
    assert "E5_MULTILINGUAL_BASE" in e5
    assert "BGE_M3" in bge
    for source in (e5, bge):
        assert "psycopg" not in source
        assert "import ydb" not in source
        assert "INSERT INTO" not in source
        compile(source, "worker.py", "exec")


def test_embedding_examples_validate_and_schemas_match_runtime_models() -> None:
    from my_data_hub.embeddings.contracts import EmbeddingArtifactManifest

    pairs = (
        (
            "embedding-artifact-manifest.v1",
            EmbeddingArtifactManifest,
        ),
        (
            "embedding-search-benchmark-receipt.v1",
            EmbeddingBenchmarkReceipt,
        ),
    )
    for stem, model in pairs:
        schema = json.loads(
            (ROOT / f"schemas/embeddings/{stem}.schema.json").read_text(encoding="utf-8")
        )
        example = json.loads(
            (ROOT / f"examples/embeddings/{stem}.example.json").read_text(encoding="utf-8")
        )
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.Draft202012Validator(
            schema,
            format_checker=jsonschema.FormatChecker(),
        ).validate(example)
        runtime_schema = model.model_json_schema(mode="validation")
        for key in ("$schema", "$id", "title"):
            schema.pop(key, None)
            runtime_schema.pop(key, None)
        assert schema == runtime_schema
