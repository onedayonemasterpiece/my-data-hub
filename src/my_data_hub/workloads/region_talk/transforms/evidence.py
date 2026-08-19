"""Deterministic fusion of independently produced E5 and BGE-M3 evidence."""

from __future__ import annotations

from ._canonical import sha256_json
from .models import VectorEvidence, VectorFusionRequest, VectorFusionResult

E5_MODEL_ID = "intfloat/multilingual-e5-base"
BGE_M3_MODEL_ID = "BAAI/bge-m3"
E5_CONTRACT = "e5_semantic_bank_scores_v1"
BGE_M3_CONTRACT = "bge_m3_flagembedding_dense_v1"
SEMANTIC_BANK_VERSION = "semantic_bank_v1"
# Exact hash of semantic_bank_v1 at the pinned donor revision.
SEMANTIC_BANK_HASH = "4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f"

POSITIVE_LABELS = frozenset(
    {
        "ko_visit_impression",
        "ko_route_useful",
        "ko_visual_place_card",
        "ko_editorial_publication",
        "ko_academic_publication",
    }
)
NEGATIVE_LABELS = frozenset(
    {
        "other_region_travel",
        "multi_region_roundup",
        "news_report",
        "event_announcement",
        "ad_or_promo",
        "low_substance",
    }
)
ALL_LABELS = POSITIVE_LABELS | NEGATIVE_LABELS


def vector_evidence_fingerprint(
    *,
    contract_version: str,
    model_id: str,
    text_hash: str,
    semantic_bank_version: str,
    semantic_bank_hash: str,
    scores: dict[str, float],
) -> str:
    return sha256_json(
        {
            "contract_version": contract_version,
            "model_id": model_id,
            "text_hash": text_hash,
            "semantic_bank_version": semantic_bank_version,
            "semantic_bank_hash": semantic_bank_hash,
            "scores": {key: round(float(value), 6) for key, value in sorted(scores.items())},
        }
    )


def _reason_for_invalid(
    evidence: VectorEvidence | None,
    *,
    expected_model: str,
    expected_contract: str,
    expected_fingerprint: str,
    text_hash: str,
    prefix: str,
) -> list[str]:
    if evidence is None:
        return [prefix + "_evidence_missing"]
    reasons: list[str] = []
    if evidence.model_id != expected_model:
        reasons.append(prefix + "_model_not_current")
    if evidence.contract_version != expected_contract:
        reasons.append(prefix + "_contract_not_current")
    if evidence.text_hash != text_hash:
        reasons.append(prefix + "_text_hash_stale")
    if evidence.semantic_bank_version != SEMANTIC_BANK_VERSION:
        reasons.append(prefix + "_semantic_bank_version_stale")
    if evidence.semantic_bank_hash != SEMANTIC_BANK_HASH:
        reasons.append(prefix + "_semantic_bank_hash_stale")
    if evidence.evidence_fingerprint != expected_fingerprint:
        reasons.append(prefix + "_expected_fingerprint_mismatch")
    computed = vector_evidence_fingerprint(
        contract_version=evidence.contract_version,
        model_id=evidence.model_id,
        text_hash=evidence.text_hash,
        semantic_bank_version=evidence.semantic_bank_version,
        semantic_bank_hash=evidence.semantic_bank_hash,
        scores=evidence.scores,
    )
    if evidence.evidence_fingerprint != computed:
        reasons.append(prefix + "_self_fingerprint_invalid")
    if not (set(evidence.scores) & POSITIVE_LABELS):
        reasons.append(prefix + "_positive_scores_missing")
    if not (set(evidence.scores) & NEGATIVE_LABELS):
        reasons.append(prefix + "_negative_scores_missing")
    return reasons


def fuse_vector_evidence(value: VectorFusionRequest) -> VectorFusionResult:
    reasons = _reason_for_invalid(
        value.e5,
        expected_model=E5_MODEL_ID,
        expected_contract=E5_CONTRACT,
        expected_fingerprint=value.expected_e5_fingerprint,
        text_hash=value.text_hash,
        prefix="e5",
    ) + _reason_for_invalid(
        value.bge_m3,
        expected_model=BGE_M3_MODEL_ID,
        expected_contract=BGE_M3_CONTRACT,
        expected_fingerprint=value.expected_bge_m3_fingerprint,
        text_hash=value.text_hash,
        prefix="bge_m3",
    )
    if reasons:
        missing = any(reason.endswith("_missing") for reason in reasons)
        return VectorFusionResult(
            contract_version="region-talk.vector-fusion.v1",
            status="evidence_required" if missing else "stale_evidence",
            reasons=tuple(sorted(set(reasons))),
        )
    assert value.e5 is not None and value.bge_m3 is not None
    by_model = {
        E5_MODEL_ID: {key: round(float(score), 6) for key, score in value.e5.scores.items()},
        BGE_M3_MODEL_ID: {
            key: round(float(score), 6) for key, score in value.bge_m3.scores.items()
        },
    }
    fused = {
        label: round(
            (value.e5.scores.get(label, 0.0) + value.bge_m3.scores.get(label, 0.0))
            / 2,
            6,
        )
        for label in sorted(ALL_LABELS)
    }
    positive_class, positive_score = max(
        ((label, fused[label]) for label in POSITIVE_LABELS), key=lambda item: item[1]
    )
    negative_class, negative_score = max(
        ((label, fused[label]) for label in NEGATIVE_LABELS), key=lambda item: item[1]
    )
    fingerprint = sha256_json(
        {
            "contract_version": "region-talk.vector-fusion.v1",
            "text_hash": value.text_hash,
            "e5_fingerprint": value.e5.evidence_fingerprint,
            "bge_m3_fingerprint": value.bge_m3.evidence_fingerprint,
            "scores": fused,
        }
    )
    return VectorFusionResult(
        contract_version="region-talk.vector-fusion.v1",
        status="fused_e5_bge_m3",
        reasons=(),
        evidence_fingerprint=fingerprint,
        scores_by_model=by_model,
        fused_scores=fused,
        positive_class=positive_class,
        positive_score=round(positive_score, 3),
        negative_class=negative_class,
        negative_score=round(negative_score, 3),
        margin=round(positive_score - negative_score, 3),
    )
