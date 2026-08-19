"""Typed, side-effect-free contracts for the Region Talk transformation slice."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

Sha256 = str
Platform = Literal["telegram", "vk", "web"]
ContentOrigin = Literal[
    "social_post", "editorial_publication", "academic_publication"
]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EvidenceItem(StrictModel):
    evidence_id: str = Field(min_length=1, max_length=160)
    url: str = Field(min_length=1, max_length=2000)
    paraphrase: str = Field(default="", max_length=600)
    quote_short: str = Field(default="", max_length=240)


class QualityScores(StrictModel):
    source_authority: int = Field(ge=0, le=4)
    evidence_depth: int = Field(ge=0, le=4)
    editorial_independence: int = Field(ge=0, le=4)
    originality: int = Field(ge=0, le=4)
    kaliningrad_centrality: int = Field(ge=0, le=4)
    public_interest: int = Field(ge=0, le=4)
    accessibility: int = Field(ge=0, le=4)


class ExternalArticleInput(StrictModel):
    schema_version: Literal["region-talk.external-article.v1"]
    canonical_url: str = Field(min_length=1, max_length=2000)
    doi: str = Field(default="", max_length=500)
    title: str = Field(min_length=1, max_length=2000)
    authors: tuple[str, ...] = Field(default=(), max_length=100)
    source_name: str = Field(min_length=1, max_length=500)
    published_at: date
    date_basis: Literal["primary_page", "issue", "doi", "search_snippet", "unknown"]
    access_status: Literal["full_text", "abstract_only", "paywalled", "unknown"]
    source_scope: Literal["external", "mixed", "regional", "unknown"]
    centrality: Literal["central", "substantial", "secondary", "episodic"]
    track: Literal[
        "scholarly",
        "professional_editorial",
        "popular_editorial",
        "reference_or_project_catalog",
    ]
    research_decision: Literal["candidate", "needs_review", "exclude"]
    downstream_readiness: Literal[
        "candidate_report", "manual_review_required", "blocked"
    ]
    research_match: bool
    product_policy_match: bool
    language_policy_match: bool
    newsiness: Literal["non_news", "news", "unknown"]
    commerciality: Literal[
        "independent", "institutional_noncommercial", "commercial", "unknown"
    ]
    hard_exclusion_codes: tuple[str, ...] = Field(default=(), max_length=100)
    quality_tier: Literal["strong", "credible", "weak", "unknown"]
    quality_scores: QualityScores
    peer_reviewed: bool | None = None
    correction_status: Literal["none_found", "found", "unknown"] | None = None
    original_reporting_or_analysis: bool | None = None
    source_externality_evidence_refs: tuple[str, ...] = Field(default=(), max_length=100)
    copy_support_evidence_refs: tuple[str, ...] = Field(default=(), max_length=100)
    evidence: tuple[EvidenceItem, ...] = Field(min_length=1, max_length=500)
    media_candidate_urls: tuple[str, ...] = Field(default=(), max_length=100)
    rights_policy: Literal["reuse_verified", "score_only", "unknown"] = "unknown"
    media_reuse_allowed: bool = False
    run_window_start: date
    run_window_end: date

    @model_validator(mode="after")
    def valid_window(self) -> ExternalArticleInput:
        if self.run_window_end < self.run_window_start:
            raise ValueError("run_window_end precedes run_window_start")
        if len({item.evidence_id for item in self.evidence}) != len(self.evidence):
            raise ValueError("evidence_id values must be unique")
        return self


class NormalizedExternalArticle(StrictModel):
    contract_version: Literal["region_talk_external_publication_import.v1"]
    external_publication_id: str
    canonical_url: str
    canonical_url_identity: str
    doi: str | None
    identity_keys: tuple[str, ...]
    title: str
    normalized_title: str
    authors: tuple[str, ...]
    normalized_authors: tuple[str, ...]
    source_name: str
    published_at: date
    content_origin_type: Literal["editorial_publication", "academic_publication"]
    normalized_quality_score: float = Field(ge=0, le=1)
    import_status: Literal[
        "ready_for_region_talk_scoring",
        "manual_review_required",
        "research_only_blocked",
    ]
    evidence: tuple[EvidenceItem, ...]
    canonical_evidence_urls: tuple[str, ...]
    rights_policy: Literal["reuse_verified", "score_only", "unknown"]
    media_reuse_allowed: bool
    media_use_policy: Literal["reuse_verified", "score_only_no_reuse"]
    media_candidate_urls: tuple[str, ...]
    source_scope: Literal["external", "mixed", "regional", "unknown"]


class ArticleNormalizationResult(StrictModel):
    status: Literal["normalized", "rejected"]
    article: NormalizedExternalArticle | None = None
    errors: tuple[str, ...] = ()

    @model_validator(mode="after")
    def result_shape(self) -> ArticleNormalizationResult:
        if (self.status == "normalized") != (self.article is not None):
            raise ValueError("normalized status and article presence disagree")
        if self.status == "rejected" and not self.errors:
            raise ValueError("rejected result requires errors")
        return self


class SourceInput(StrictModel):
    platform: Platform
    handle: str = Field(default="", max_length=500)
    canonical_url: str = Field(default="", max_length=2000)
    title: str = Field(default="", max_length=1000)
    scope: Literal["external", "mixed_external", "local_region", "unknown"] = "unknown"


class NormalizedSource(StrictModel):
    contract_version: Literal["region-talk.source-normalization.v1"]
    canonical_source_key: str
    platform: Platform
    handle: str
    canonical_url: str
    title: str
    scope: Literal["external", "mixed_external", "local_region", "unknown"]


class PostInput(StrictModel):
    platform_post_id: str = Field(min_length=1, max_length=500)
    canonical_url: str = Field(min_length=1, max_length=2000)
    text: str = Field(default="", max_length=200_000)
    published_at: datetime
    source: SourceInput
    content_origin_type: ContentOrigin = "social_post"


class NormalizedPost(StrictModel):
    contract_version: Literal["region-talk.post-normalization.v1"]
    post_id: str
    platform_post_key: str
    canonical_url: str
    text: str
    text_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    published_at: datetime
    source: NormalizedSource
    content_origin_type: ContentOrigin


class VectorEvidence(StrictModel):
    contract_version: Literal[
        "e5_semantic_bank_scores_v1", "bge_m3_flagembedding_dense_v1"
    ]
    model_id: Literal["intfloat/multilingual-e5-base", "BAAI/bge-m3"]
    text_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    semantic_bank_version: Literal["semantic_bank_v1"]
    semantic_bank_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    evidence_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    scores: dict[str, float]

    @model_validator(mode="after")
    def scores_are_bounded(self) -> VectorEvidence:
        if not self.scores or any(not 0 <= score <= 1 for score in self.scores.values()):
            raise ValueError("scores must be non-empty and bounded 0..1")
        return self


class VectorFusionRequest(StrictModel):
    schema_version: Literal["region-talk.vector-fusion.v1"]
    text_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_e5_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    expected_bge_m3_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    e5: VectorEvidence | None = None
    bge_m3: VectorEvidence | None = None


class VectorFusionResult(StrictModel):
    contract_version: Literal["region-talk.vector-fusion.v1"]
    status: Literal["fused_e5_bge_m3", "evidence_required", "stale_evidence"]
    reasons: tuple[str, ...]
    evidence_fingerprint: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    scores_by_model: dict[str, dict[str, float]] = Field(default_factory=dict)
    fused_scores: dict[str, float] = Field(default_factory=dict)
    positive_class: str = ""
    positive_score: float = 0
    negative_class: str = ""
    negative_score: float = 0
    margin: float = 0


class ImageEvidence(StrictModel):
    status: Literal["missing", "pending", "completed", "failed"]
    input_fingerprint: str = ""
    result_fingerprint: str = ""
    model_contract: str = ""
    decision: Literal["accept", "reject", "needs_review", ""] = ""
    actual_image: bool = False


class WorkerGate(StrictModel):
    stage: Literal["image_scoring", "final_verifier", "writer"]
    status: Literal["required", "pending", "satisfied", "failed"]
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    reason: str


class EligibilityInput(StrictModel):
    schema_version: Literal["region_talk_publication_eligibility_v5"]
    post: NormalizedPost
    authoritative_source: NormalizedSource | None
    vector_fusion: VectorFusionResult
    kaliningrad_oblast_only_scope: bool
    kaliningrad_mention_role: Literal["main_subject", "unclear", "one_item", "incidental"]
    is_ad_or_promo: bool
    image_evidence: ImageEvidence


class EligibilityDecision(StrictModel):
    gate_version: Literal["region_talk_publication_eligibility_v5"]
    decision: Literal[
        "reject",
        "needs_source_review",
        "needs_text_review",
        "pending_worker",
        "eligible_for_final_verifier",
    ]
    reason: str
    evidence_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    worker_gates: tuple[WorkerGate, ...]


class ModelResultEvidence(StrictModel):
    stage: Literal["final_verifier", "writer"]
    status: Literal["pending", "completed", "failed"]
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    result_fingerprint: str = Field(default="", pattern=r"^$|^[a-f0-9]{64}$")
    decision: Literal["accept", "reject", "needs_review", ""] = ""


class CandidateMemory(StrictModel):
    contract_version: Literal["region-talk.candidate-memory.v1"]
    candidate_memory_id: str
    canonical_url: str
    current_input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    eligibility: EligibilityDecision
    final_verifier: ModelResultEvidence
    writer: ModelResultEvidence
    lifecycle_status: Literal[
        "rejected", "review_required", "worker_pending", "review_queue_ready"
    ]


class CandidateRevision(StrictModel):
    contract_version: Literal["region-talk.candidate-revision.v1"]
    candidate_id: str
    revision_id: str
    revision_number: int = Field(ge=1)
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    is_current: Literal[True] = True
    status: Literal["rejected", "review_required", "worker_pending", "review_queue_ready"]
    replayed: bool


class CandidateFormationResult(StrictModel):
    memory: CandidateMemory
    revision: CandidateRevision


class PublisherProfile(StrictModel):
    canonical_source_key: str
    publisher_profile_id: str
    source_domain: str
    scope: Literal["external", "mixed", "regional", "unknown"] = "unknown"
    profile_origin: Literal["external_research_seed", "publisher_profile_sidecar"]
    profile_status: str = ""
    profile_dimensions: dict[str, Any] = Field(default_factory=dict)
    evidence: tuple[dict[str, Any], ...] = ()
    profile_hash: str = ""
    copy_projection: dict[str, Any] = Field(default_factory=dict)
    public_copy_eligibility: str = ""


class SourceProfile(StrictModel):
    source: NormalizedSource
    status: Literal["unknown", "candidate", "confirmed_external", "rejected_local", "rejected_spam"]
    posts_scanned: int = Field(default=0, ge=0)
    ko_posts_found: int = Field(default=0, ge=0)
    candidate_posts_found: int = Field(default=0, ge=0)
    evidence: tuple[dict[str, Any], ...] = ()


class ProfileMergeResult(StrictModel):
    status: Literal["merged", "conflict"]
    publisher: PublisherProfile | None = None
    source: SourceProfile | None = None
    reason: str = ""


class DiversityVector(StrictModel):
    model_id: str
    encoder_contract: str
    evidence_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    values: tuple[float, ...] = Field(min_length=1, max_length=8192)


class ReviewCandidate(StrictModel):
    candidate_id: str
    canonical_url: str
    content_lane: Literal["article", "social"]
    canonical_source_key: str
    topics: tuple[str, ...] = ()
    content_type: str = ""
    quality_score: float = Field(ge=0, le=1)
    current_revision_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    diversity_vector: DiversityVector | None = None
    final_verifier_status: Literal["accept", "needs_review"] = "accept"
    writer_status: Literal["completed", "pending"] = "completed"


class RankedReviewCandidate(ReviewCandidate):
    queue_rank: int = Field(ge=1)
    rank_score: float
    max_similarity: float
    diversity_mode: Literal["vector", "heuristic_fallback", "not_applicable"]
    adjacency_relaxed: bool


class PublicationPolicy(StrictModel):
    policy_version: Literal["region_talk_publication_plan.v1"]
    timezone: str = "Europe/Kaliningrad"
    article_times: tuple[str, ...]
    social_times: tuple[str, ...]
    effects_enabled: Literal[False] = False


class ApprovedCandidate(StrictModel):
    candidate: RankedReviewCandidate
    operator_decision: Literal["approved", "rejected", "rewrite_requested"]
    operator_review_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")


class PublicationPlanRequest(StrictModel):
    schema_version: Literal["region-talk.publication-plan.v1"]
    start_date: date
    days: int = Field(default=14, ge=1, le=90)
    policy: PublicationPolicy
    candidates: tuple[ApprovedCandidate, ...]


class PublicationPlanSlot(StrictModel):
    plan_date: date
    content_lane: Literal["article", "social"]
    status: Literal["planned", "vacant"]
    scheduled_for: datetime | None
    candidate_id: str = ""
    revision_fingerprint: str = ""
    dispatch_allowed: Literal[False] = False
    reason: str = ""


class PublicationPlanResult(StrictModel):
    contract_version: Literal["region-talk.publication-plan.v1"]
    status: Literal["planned", "blocked_policy_ambiguity"]
    effects_enabled: Literal[False] = False
    policy_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    slots: tuple[PublicationPlanSlot, ...] = ()
    reasons: tuple[str, ...] = ()
