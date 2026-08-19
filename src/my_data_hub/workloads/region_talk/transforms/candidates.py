"""Pure candidate-memory and immutable current-revision formation."""

from __future__ import annotations

from ._canonical import sha256_json, stable_id
from .models import (
    CandidateFormationResult,
    CandidateMemory,
    CandidateRevision,
    EligibilityDecision,
    ModelResultEvidence,
    NormalizedPost,
)


def _gate_input(decision: EligibilityDecision, stage: str) -> str:
    for gate in decision.worker_gates:
        if gate.stage == stage:
            return gate.input_fingerprint
    return sha256_json(
        {"stage": stage, "eligibility_fingerprint": decision.evidence_fingerprint}
    )


def _pending(stage: str, input_fingerprint: str) -> ModelResultEvidence:
    return ModelResultEvidence(
        stage=stage, status="pending", input_fingerprint=input_fingerprint
    )


def form_candidate_revision(
    post: NormalizedPost,
    eligibility: EligibilityDecision,
    *,
    final_verifier: ModelResultEvidence | None = None,
    writer: ModelResultEvidence | None = None,
    previous_revision: CandidateRevision | None = None,
) -> CandidateFormationResult:
    final_input = _gate_input(eligibility, "final_verifier")
    writer_input = _gate_input(eligibility, "writer")
    final = final_verifier or _pending("final_verifier", final_input)
    staged_writer = writer or _pending("writer", writer_input)
    final_current = final.input_fingerprint == final_input
    writer_current = staged_writer.input_fingerprint == writer_input

    if eligibility.decision == "reject":
        lifecycle = "rejected"
    elif eligibility.decision in {"needs_source_review", "needs_text_review"}:
        lifecycle = "review_required"
    elif eligibility.decision == "pending_worker" or not final_current or final.status != "completed":
        lifecycle = "worker_pending"
    elif final.decision == "reject":
        lifecycle = "rejected"
    elif final.decision != "accept":
        lifecycle = "review_required"
    elif not writer_current or staged_writer.status != "completed":
        lifecycle = "worker_pending"
    else:
        lifecycle = "review_queue_ready"

    input_fingerprint = sha256_json(
        {
            "post": post.model_dump(mode="json"),
            "eligibility": eligibility.model_dump(mode="json"),
            "final_verifier": final.model_dump(mode="json"),
            "writer": staged_writer.model_dump(mode="json"),
        }
    )
    candidate_id = stable_id("rtcandidate_", post.canonical_url)
    replayed = bool(
        previous_revision
        and previous_revision.candidate_id == candidate_id
        and previous_revision.input_fingerprint == input_fingerprint
    )
    revision_number = (
        previous_revision.revision_number
        if replayed and previous_revision
        else (previous_revision.revision_number + 1 if previous_revision else 1)
    )
    revision_id = stable_id(
        "rtrevision_", candidate_id, revision_number, input_fingerprint
    )
    memory = CandidateMemory(
        contract_version="region-talk.candidate-memory.v1",
        candidate_memory_id=stable_id("rtmemory_", candidate_id),
        canonical_url=post.canonical_url,
        current_input_fingerprint=input_fingerprint,
        eligibility=eligibility,
        final_verifier=final,
        writer=staged_writer,
        lifecycle_status=lifecycle,
    )
    revision = CandidateRevision(
        contract_version="region-talk.candidate-revision.v1",
        candidate_id=candidate_id,
        revision_id=revision_id,
        revision_number=revision_number,
        input_fingerprint=input_fingerprint,
        is_current=True,
        status=lifecycle,
        replayed=replayed,
    )
    return CandidateFormationResult(memory=memory, revision=revision)
