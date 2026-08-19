from __future__ import annotations

from datetime import UTC, date, datetime

from my_data_hub.workloads.region_talk.transforms._canonical import sha256_json
from my_data_hub.workloads.region_talk.transforms.candidates import form_candidate_revision
from my_data_hub.workloads.region_talk.transforms.eligibility import (
    evaluate_publication_eligibility,
    image_worker_input_fingerprint,
)
from my_data_hub.workloads.region_talk.transforms.merge import (
    merge_publisher_profiles,
    merge_source_profiles,
)
from my_data_hub.workloads.region_talk.transforms.models import (
    ApprovedCandidate,
    DiversityVector,
    EligibilityInput,
    ImageEvidence,
    ModelResultEvidence,
    NormalizedPost,
    NormalizedSource,
    PublicationPlanRequest,
    PublicationPolicy,
    PublisherProfile,
    RankedReviewCandidate,
    ReviewCandidate,
    SourceProfile,
    VectorFusionResult,
)
from my_data_hub.workloads.region_talk.transforms.planning import build_publication_plan
from my_data_hub.workloads.region_talk.transforms.ranking import rank_review_queue

SHA_A = "a" * 64
SHA_B = "b" * 64
SHA_C = "c" * 64


def source(scope: str = "external") -> NormalizedSource:
    return NormalizedSource(
        contract_version="region-talk.source-normalization.v1",
        canonical_source_key="telegram:travelcase",
        platform="telegram",
        handle="travelcase",
        canonical_url="https://t.me/travelcase",
        title="Travel Case",
        scope=scope,
    )


def post() -> NormalizedPost:
    return NormalizedPost(
        contract_version="region-talk.post-normalization.v1",
        post_id="rtpost_test",
        platform_post_key="telegram:travelcase:42",
        canonical_url="https://t.me/travelcase/42",
        text="Personal account of a trip to Kaliningrad.",
        text_hash=SHA_A,
        published_at=datetime(2026, 7, 12, tzinfo=UTC),
        source=source(),
        content_origin_type="social_post",
    )


def fusion() -> VectorFusionResult:
    return VectorFusionResult(
        contract_version="region-talk.vector-fusion.v1",
        status="fused_e5_bge_m3",
        reasons=(),
        evidence_fingerprint=SHA_B,
        fused_scores={"ko_visit_impression": 0.8, "other_region_travel": 0.1},
        positive_class="ko_visit_impression",
        positive_score=0.8,
        negative_class="other_region_travel",
        negative_score=0.1,
        margin=0.7,
    )


def eligibility(image: ImageEvidence, authoritative: NormalizedSource | None = None) -> EligibilityInput:
    return EligibilityInput(
        schema_version="region_talk_publication_eligibility_v5",
        post=post(),
        authoritative_source=authoritative or source(),
        vector_fusion=fusion(),
        kaliningrad_oblast_only_scope=True,
        kaliningrad_mention_role="main_subject",
        is_ad_or_promo=False,
        image_evidence=image,
    )


def test_eligibility_requires_real_workers_and_rejects_stale_evidence() -> None:
    pending_input = eligibility(ImageEvidence(status="missing"))
    pending = evaluate_publication_eligibility(pending_input)
    assert pending.decision == "pending_worker"
    assert [gate.stage for gate in pending.worker_gates] == [
        "image_scoring",
        "final_verifier",
        "writer",
    ]

    expected = image_worker_input_fingerprint(pending_input)
    stale = evaluate_publication_eligibility(
        eligibility(
            ImageEvidence(
                status="completed",
                input_fingerprint=SHA_C,
                result_fingerprint=SHA_C,
                model_contract="region-talk.image-diagnostic.v1",
                decision="accept",
                actual_image=True,
            )
        )
    )
    assert stale.reason == "image_evidence_stale_or_unbound"
    current = evaluate_publication_eligibility(
        eligibility(
            ImageEvidence(
                status="completed",
                input_fingerprint=expected,
                result_fingerprint=SHA_C,
                model_contract="region-talk.image-diagnostic.v1",
                decision="accept",
                actual_image=True,
            )
        )
    )
    assert current.decision == "eligible_for_final_verifier"
    assert any(gate.stage == "final_verifier" and gate.status == "required" for gate in current.worker_gates)

    unknown = evaluate_publication_eligibility(
        eligibility(ImageEvidence(status="missing"), authoritative=source("unknown"))
    )
    assert unknown.decision == "needs_source_review"


def test_candidate_revision_is_idempotent_and_stale_model_result_never_advances() -> None:
    base_input = eligibility(ImageEvidence(status="missing"))
    expected = image_worker_input_fingerprint(base_input)
    decision = evaluate_publication_eligibility(
        eligibility(
            ImageEvidence(
                status="completed",
                input_fingerprint=expected,
                result_fingerprint=SHA_C,
                model_contract="region-talk.image-diagnostic.v1",
                decision="accept",
                actual_image=True,
            )
        )
    )
    gates = {gate.stage: gate.input_fingerprint for gate in decision.worker_gates}
    final = ModelResultEvidence(
        stage="final_verifier",
        status="completed",
        input_fingerprint=gates["final_verifier"],
        result_fingerprint=SHA_A,
        decision="accept",
    )
    writer = ModelResultEvidence(
        stage="writer",
        status="completed",
        input_fingerprint=gates["writer"],
        result_fingerprint=SHA_B,
        decision="accept",
    )
    first = form_candidate_revision(post(), decision, final_verifier=final, writer=writer)
    assert first.memory.lifecycle_status == "review_queue_ready"
    replay = form_candidate_revision(
        post(),
        decision,
        final_verifier=final,
        writer=writer,
        previous_revision=first.revision,
    )
    assert replay.revision.replayed is True
    assert replay.revision.revision_number == 1
    stale = form_candidate_revision(
        post(),
        decision,
        final_verifier=final.model_copy(update={"input_fingerprint": SHA_C}),
        writer=writer,
    )
    assert stale.memory.lifecycle_status == "worker_pending"


def test_publisher_and_source_merges_are_monotonic_and_conflicts_fail_closed() -> None:
    seed = PublisherProfile(
        canonical_source_key="web:example.org",
        publisher_profile_id="rtpublisher_x",
        source_domain="example.org",
        scope="unknown",
        profile_origin="external_research_seed",
        evidence=({"evidence_id": "seed"},),
    )
    dossier = seed.model_copy(
        update={
            "scope": "external",
            "profile_origin": "publisher_profile_sidecar",
            "profile_status": "reviewed",
            "profile_dimensions": {"outlet_identity": "Synthetic dossier"},
            "evidence": ({"evidence_id": "dossier"},),
        }
    )
    merged = merge_publisher_profiles(dossier, seed)
    assert merged.status == "merged"
    assert merged.publisher is not None
    assert merged.publisher.profile_origin == "publisher_profile_sidecar"
    assert len(merged.publisher.evidence) == 2
    conflict = merge_publisher_profiles(
        dossier, seed.model_copy(update={"scope": "regional"})
    )
    assert conflict.status == "conflict"

    existing = SourceProfile(source=source("external"), status="confirmed_external", posts_scanned=10)
    older = SourceProfile(source=source("unknown"), status="candidate", posts_scanned=5)
    assert merge_source_profiles(existing, older).source.status == "confirmed_external"  # type: ignore[union-attr]
    local = SourceProfile(source=source("local_region"), status="rejected_local")
    assert merge_source_profiles(existing, local).status == "conflict"


def candidate(
    candidate_id: str,
    url: str,
    score: float,
    source_key: str,
    values: tuple[float, ...] | None,
    lane: str = "social",
) -> ReviewCandidate:
    vector = (
        DiversityVector(
            model_id="BAAI/bge-m3",
            encoder_contract="bge_m3_flagembedding_dense_v1",
            evidence_fingerprint=sha256_json([candidate_id, values]),
            values=values,
        )
        if values
        else None
    )
    return ReviewCandidate(
        candidate_id=candidate_id,
        canonical_url=url,
        content_lane=lane,
        canonical_source_key=source_key,
        topics=("coast",),
        content_type="travel",
        quality_score=score,
        current_revision_fingerprint=sha256_json(candidate_id),
        diversity_vector=vector,
    )


def test_mmr_ranking_is_deterministic_and_discloses_fallback() -> None:
    rows = [
        candidate("a", "https://example.org/a", 0.9, "web:a.example", (1.0, 0.0)),
        candidate("b", "https://example.org/b", 0.89, "web:a.example", (0.99, 0.01)),
        candidate("c", "https://example.org/c", 0.8, "web:c.example", (0.0, 1.0)),
        candidate("d", "https://example.org/d", 0.7, "web:d.example", None),
    ]
    first = rank_review_queue(rows, limit=4)
    assert first == rank_review_queue(list(reversed(rows)), limit=4)
    assert first[0].candidate_id == "a"
    assert first[1].candidate_id == "c"
    assert any(row.diversity_mode == "heuristic_fallback" for row in first)


def ranked(candidate_id: str, lane: str, rank: int) -> RankedReviewCandidate:
    row = candidate(
        candidate_id,
        f"https://example.org/{candidate_id}",
        0.9,
        f"web:{candidate_id}.example",
        (1.0, 0.0),
        lane,
    )
    return RankedReviewCandidate(
        **row.model_dump(),
        queue_rank=rank,
        rank_score=0.9,
        max_similarity=0,
        diversity_mode="not_applicable",
        adjacency_relaxed=False,
    )


def test_publication_plan_disables_effects_and_time_ambiguity_fails_closed() -> None:
    article = ranked("article", "article", 1)
    social = ranked("social", "social", 1)
    approved = (
        ApprovedCandidate(
            candidate=article,
            operator_decision="approved",
            operator_review_fingerprint=article.current_revision_fingerprint,
        ),
        ApprovedCandidate(
            candidate=social,
            operator_decision="approved",
            operator_review_fingerprint=social.current_revision_fingerprint,
        ),
    )
    request = PublicationPlanRequest(
        schema_version="region-talk.publication-plan.v1",
        start_date=date(2026, 8, 20),
        days=1,
        policy=PublicationPolicy(
            policy_version="region_talk_publication_plan.v1",
            article_times=("12:00",),
            social_times=("18:00",),
            effects_enabled=False,
        ),
        candidates=approved,
    )
    plan = build_publication_plan(request)
    assert plan.status == "planned"
    assert len(plan.slots) == 2
    assert all(slot.dispatch_allowed is False for slot in plan.slots)
    assert plan.slots[0].scheduled_for.isoformat() == "2026-08-20T12:00:00+02:00"  # type: ignore[union-attr]

    ambiguous = build_publication_plan(
        request.model_copy(
            update={
                "policy": request.policy.model_copy(
                    update={"article_times": ("12:00", "11:30")}
                )
            }
        )
    )
    assert ambiguous.status == "blocked_policy_ambiguity"
    assert ambiguous.slots == ()
    assert any("11:30,12:00" in reason for reason in ambiguous.reasons)
