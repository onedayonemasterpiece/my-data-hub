"""Fail-closed publication eligibility v5 without invoking any model."""

from __future__ import annotations

from ._canonical import sha256_json
from .models import EligibilityDecision, EligibilityInput, WorkerGate

GATE_VERSION = "region_talk_publication_eligibility_v5"


def _worker_gate(stage: str, status: str, input_fingerprint: str, reason: str) -> WorkerGate:
    return WorkerGate(
        stage=stage, status=status, input_fingerprint=input_fingerprint, reason=reason
    )


def image_worker_input_fingerprint(value: EligibilityInput) -> str:
    return sha256_json(
        {
            "stage": "image_scoring",
            "post_url": value.post.canonical_url,
            "text_hash": value.post.text_hash,
            "vector_fingerprint": value.vector_fusion.evidence_fingerprint,
        }
    )


def evaluate_publication_eligibility(value: EligibilityInput) -> EligibilityDecision:
    evidence = {
        "gate_version": GATE_VERSION,
        "post": value.post.model_dump(mode="json"),
        "source": (
            value.authoritative_source.model_dump(mode="json")
            if value.authoritative_source
            else None
        ),
        "vector_fusion": value.vector_fusion.model_dump(mode="json"),
        "kaliningrad_oblast_only_scope": value.kaliningrad_oblast_only_scope,
        "kaliningrad_mention_role": value.kaliningrad_mention_role,
        "is_ad_or_promo": value.is_ad_or_promo,
        "image_evidence": value.image_evidence.model_dump(mode="json"),
    }
    fingerprint = sha256_json(evidence)
    image_input = image_worker_input_fingerprint(value)
    finalizer_input = sha256_json(
        {"stage": "final_verifier", "eligibility_fingerprint": fingerprint}
    )
    writer_input = sha256_json(
        {"stage": "writer", "eligibility_fingerprint": fingerprint}
    )
    no_worker_gates: tuple[WorkerGate, ...] = ()

    source = value.authoritative_source
    if source is None:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="needs_source_review",
            reason="authoritative_source_not_found",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if source.scope == "local_region":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="confirmed_local_source",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if source.scope == "unknown":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="needs_source_review",
            reason="source_verdict_unknown",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if not value.kaliningrad_oblast_only_scope:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="not_confirmed_kaliningrad_oblast_scope",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if value.kaliningrad_mention_role not in {"main_subject", "unclear"}:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="kaliningrad_not_main_subject",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if value.is_ad_or_promo:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="ad_or_promo",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if value.vector_fusion.status != "fused_e5_bge_m3":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="needs_text_review",
            reason="fused_e5_bge_m3_required",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )
    if value.vector_fusion.positive_score <= value.vector_fusion.negative_score:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="vector_negative_class_dominates",
            evidence_fingerprint=fingerprint,
            worker_gates=no_worker_gates,
        )

    image = value.image_evidence
    if image.status in {"missing", "pending"}:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="pending_worker",
            reason="actual_image_evidence_required",
            evidence_fingerprint=fingerprint,
            worker_gates=(
                _worker_gate("image_scoring", "required", image_input, "heavy_worker_required"),
                _worker_gate("final_verifier", "pending", finalizer_input, "waits_for_image"),
                _worker_gate("writer", "pending", writer_input, "waits_for_final_verifier"),
            ),
        )
    if image.status == "failed":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="pending_worker",
            reason="image_scoring_failed_retry_required",
            evidence_fingerprint=fingerprint,
            worker_gates=(
                _worker_gate("image_scoring", "failed", image_input, "retry_required"),
                _worker_gate("final_verifier", "pending", finalizer_input, "waits_for_image"),
                _worker_gate("writer", "pending", writer_input, "waits_for_final_verifier"),
            ),
        )
    if image.input_fingerprint != image_input or not image.result_fingerprint:
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="pending_worker",
            reason="image_evidence_stale_or_unbound",
            evidence_fingerprint=fingerprint,
            worker_gates=(
                _worker_gate("image_scoring", "required", image_input, "fresh_result_required"),
                _worker_gate("final_verifier", "pending", finalizer_input, "waits_for_image"),
                _worker_gate("writer", "pending", writer_input, "waits_for_final_verifier"),
            ),
        )
    if image.decision == "reject":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="image_worker_rejected",
            evidence_fingerprint=fingerprint,
            worker_gates=(
                _worker_gate("image_scoring", "satisfied", image_input, "current_reject"),
            ),
        )
    if image.decision != "accept":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="pending_worker",
            reason="image_requires_visual_review",
            evidence_fingerprint=fingerprint,
            worker_gates=(
                _worker_gate("image_scoring", "satisfied", image_input, "needs_visual_review"),
                _worker_gate("final_verifier", "pending", finalizer_input, "waits_for_visual_review"),
                _worker_gate("writer", "pending", writer_input, "waits_for_final_verifier"),
            ),
        )
    if not image.actual_image and value.post.content_origin_type == "social_post":
        return EligibilityDecision(
            gate_version=GATE_VERSION,
            decision="reject",
            reason="actual_image_required_for_social_post",
            evidence_fingerprint=fingerprint,
            worker_gates=(
                _worker_gate("image_scoring", "satisfied", image_input, "non_actual_input"),
            ),
        )
    return EligibilityDecision(
        gate_version=GATE_VERSION,
        decision="eligible_for_final_verifier",
        reason="deterministic_gates_passed_model_verdict_required",
        evidence_fingerprint=fingerprint,
        worker_gates=(
            _worker_gate("image_scoring", "satisfied", image_input, "current_accept"),
            _worker_gate("final_verifier", "required", finalizer_input, "model_result_required"),
            _worker_gate("writer", "pending", writer_input, "waits_for_final_verifier"),
        ),
    )
