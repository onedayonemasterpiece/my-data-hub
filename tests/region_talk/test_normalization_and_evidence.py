from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from my_data_hub.workloads.region_talk.transforms.evidence import (
    BGE_M3_CONTRACT,
    BGE_M3_MODEL_ID,
    E5_CONTRACT,
    E5_MODEL_ID,
    SEMANTIC_BANK_HASH,
    fuse_vector_evidence,
    vector_evidence_fingerprint,
)
from my_data_hub.workloads.region_talk.transforms.models import (
    ExternalArticleInput,
    PostInput,
    SourceInput,
    VectorEvidence,
    VectorFusionRequest,
)
from my_data_hub.workloads.region_talk.transforms.normalization import (
    normalize_external_article,
    normalize_post,
    normalize_source,
)

FIXTURE = Path(__file__).parent / "fixtures" / "external_article_golden.v1.json"


def test_external_article_golden_is_deterministic_and_provenanced() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))
    assert fixture["provenance"]["commit"] == "5bbdb681623d5e4e0bff2133e487a6663c1a838a"
    value = ExternalArticleInput.model_validate(fixture["input"])
    first = normalize_external_article(value)
    second = normalize_external_article(value)
    assert first == second
    assert first.status == "normalized"
    assert first.article is not None
    for key, expected in fixture["expected"].items():
        actual = getattr(first.article, key)
        if isinstance(actual, tuple):
            actual = list(actual)
        assert actual == expected
    assert first.article.external_publication_id.startswith("extpub_")
    assert first.article.media_reuse_allowed is False


def test_article_rights_and_private_urls_fail_closed() -> None:
    fixture = json.loads(FIXTURE.read_text(encoding="utf-8"))["input"]
    bad_rights = ExternalArticleInput.model_validate(
        {**fixture, "media_reuse_allowed": True, "rights_policy": "unknown"}
    )
    result = normalize_external_article(bad_rights)
    assert result.status == "rejected"
    assert "media_reuse_requires_verified_rights" in result.errors

    bad_url = ExternalArticleInput.model_validate(
        {**fixture, "canonical_url": "http://127.0.0.1/private"}
    )
    result = normalize_external_article(bad_url)
    assert result.status == "rejected"
    assert any("private" in error for error in result.errors)


def test_source_and_post_normalization_are_stable() -> None:
    source = SourceInput(
        platform="telegram",
        handle="@Travel_Case",
        canonical_url="https://t.me/travel_case/",
        title="  Travel   Case ",
        scope="external",
    )
    normalized_source = normalize_source(source)
    assert normalized_source.canonical_source_key == "telegram:travel_case"
    assert normalized_source.canonical_url == "https://t.me/travel_case"
    value = PostInput(
        platform_post_id="42",
        canonical_url="https://t.me/travel_case/42?utm_source=x",
        text="Personal account of a Kaliningrad trip.  \n",
        published_at=datetime(2026, 7, 12, 10, tzinfo=UTC),
        source=source,
    )
    assert normalize_post(value) == normalize_post(value)
    assert normalize_post(value).platform_post_key == "telegram:travel_case:42"


def _evidence(model: str, contract: str, text_hash: str, scores: dict[str, float]) -> VectorEvidence:
    fingerprint = vector_evidence_fingerprint(
        contract_version=contract,
        model_id=model,
        text_hash=text_hash,
        semantic_bank_version="semantic_bank_v1",
        semantic_bank_hash=SEMANTIC_BANK_HASH,
        scores=scores,
    )
    return VectorEvidence(
        contract_version=contract,
        model_id=model,
        text_hash=text_hash,
        semantic_bank_version="semantic_bank_v1",
        semantic_bank_hash=SEMANTIC_BANK_HASH,
        evidence_fingerprint=fingerprint,
        scores=scores,
    )


def test_dual_vector_fusion_requires_both_current_fingerprints() -> None:
    text_hash = "a" * 64
    e5 = _evidence(
        E5_MODEL_ID,
        E5_CONTRACT,
        text_hash,
        {"ko_visit_impression": 0.78, "other_region_travel": 0.11},
    )
    bge = _evidence(
        BGE_M3_MODEL_ID,
        BGE_M3_CONTRACT,
        text_hash,
        {"ko_visit_impression": 0.82, "other_region_travel": 0.12},
    )
    request = VectorFusionRequest(
        schema_version="region-talk.vector-fusion.v1",
        text_hash=text_hash,
        expected_e5_fingerprint=e5.evidence_fingerprint,
        expected_bge_m3_fingerprint=bge.evidence_fingerprint,
        e5=e5,
        bge_m3=bge,
    )
    first = fuse_vector_evidence(request)
    assert first == fuse_vector_evidence(request)
    assert first.status == "fused_e5_bge_m3"
    assert first.positive_score == 0.8
    assert first.negative_score == 0.115

    stale = fuse_vector_evidence(
        request.model_copy(update={"expected_bge_m3_fingerprint": "f" * 64})
    )
    assert stale.status == "stale_evidence"
    assert "bge_m3_expected_fingerprint_mismatch" in stale.reasons
    missing = fuse_vector_evidence(request.model_copy(update={"bge_m3": None}))
    assert missing.status == "evidence_required"
    assert missing.fused_scores == {}
