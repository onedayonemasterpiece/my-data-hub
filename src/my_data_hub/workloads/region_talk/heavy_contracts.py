"""Closed contracts for the three Region Talk evidence-heavy stages.

The contracts deliberately contain no provider credential, SQL, or network-fetch
instruction.  Every semantic input is bound to the current candidate revision and
every nested evidence bundle carries a canonical SHA-256 fingerprint.  A private
worker may resolve a task-private ``object_ref`` through an injected capability,
but must never treat a source URL as an artifact locator.
"""

from __future__ import annotations

import hashlib
import ipaddress
import json
import re
from pathlib import PurePosixPath
from typing import Annotated, Any, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, model_validator

from my_data_hub.hashing import canonical_json_bytes

SHA256_PATTERN = r"^[a-f0-9]{64}$"
MAX_TEXT_BYTES = 256 * 1024
MAX_RESULT_BYTES = 64 * 1024


class HeavyContractError(ValueError):
    """A heavy-stage value differs from its exact, closed contract."""


class HeavyRuntimeUnavailable(RuntimeError):
    """A valid request cannot run until a reviewed private capability is attached."""

    code = "HEAVY_RUNTIME_NOT_ATTACHED"
    retryable = True


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def canonical_sha256(value: BaseModel | dict[str, Any]) -> str:
    payload = value.model_dump(mode="json") if isinstance(value, BaseModel) else value
    return hashlib.sha256(canonical_json_bytes(payload)).hexdigest()


def _without(value: BaseModel, *keys: str) -> dict[str, Any]:
    payload = value.model_dump(mode="json")
    for key in keys:
        payload.pop(key, None)
    return payload


def _validate_public_https_url(value: str, *, label: str) -> str:
    parsed = urlsplit(value)
    if parsed.scheme != "https" or not parsed.hostname or parsed.username or parsed.password:
        raise ValueError(f"{label} must be a credential-free HTTPS URL")
    if parsed.port not in {None, 443}:
        raise ValueError(f"{label} must use the default HTTPS port")
    host = parsed.hostname.rstrip(".").lower()
    if host == "localhost" or host.endswith(".localhost") or "." not in host:
        raise ValueError(f"{label} host is not public")
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        return value
    if not address.is_global:
        raise ValueError(f"{label} IP literal is not public")
    return value


class ContentEvidence(StrictModel):
    title: str = Field(default="", max_length=4_000)
    summary: str = Field(default="", max_length=20_000)
    body_text: str = Field(default="", max_length=200_000)
    text_sha256: str = Field(pattern=SHA256_PATTERN)
    canonical_url: str = Field(min_length=1, max_length=4_000)
    canonical_source_key: str = Field(min_length=1, max_length=1_000)
    content_type: str = Field(min_length=1, max_length=200)

    @model_validator(mode="after")
    def exact_text_and_url(self) -> ContentEvidence:
        text = self.body_text or "\n\n".join(part for part in (self.title, self.summary) if part)
        if len(text.encode("utf-8")) > MAX_TEXT_BYTES or sha256_text(text) != self.text_sha256:
            raise ValueError("content text_sha256 or size differs")
        _validate_public_https_url(self.canonical_url, label="canonical_url")
        return self


class MediaArtifact(StrictModel):
    asset_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,200}$")
    source_media_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,300}$")
    normalized_source_url: str = Field(min_length=1, max_length=4_000)
    source_url_sha256: str = Field(pattern=SHA256_PATTERN)
    object_ref: str = Field(min_length=1, max_length=500)
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    byte_size: int = Field(ge=1, le=25_000_000)
    content_type: Literal["image/jpeg", "image/png", "image/webp"]
    width: int | None = Field(default=None, ge=1, le=50_000)
    height: int | None = Field(default=None, ge=1, le=50_000)

    @model_validator(mode="after")
    def safe_exact_artifact(self) -> MediaArtifact:
        _validate_public_https_url(self.normalized_source_url, label="normalized_source_url")
        if sha256_text(self.normalized_source_url) != self.source_url_sha256:
            raise ValueError("source_url_sha256 differs")
        path = PurePosixPath(self.object_ref)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("object_ref must be a task-private relative path")
        if any(not re.fullmatch(r"[A-Za-z0-9_.-]+", part) for part in path.parts):
            raise ValueError("object_ref contains an unsafe path component")
        return self


class MediaAcquisitionReceipt(StrictModel):
    schema_version: Literal["region-talk-media-artifact-acquisition-receipt.v1"]
    registered: Literal[True]
    acquisition_id: UUID
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    canonical_revision: int = Field(ge=1)
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    candidate_id: UUID
    candidate_revision: int = Field(ge=1)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content_id: UUID
    asset_id: UUID
    source_media_id: str = Field(min_length=1, max_length=500)
    normalized_source_url: str = Field(min_length=1, max_length=4_000)
    source_url_sha256: str = Field(pattern=SHA256_PATTERN)
    object_ref: str = Field(min_length=1, max_length=2_000)
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    byte_size: int = Field(ge=1, le=25_000_000)
    content_type: Literal["image/jpeg", "image/png", "image/webp"]
    width: int | None = Field(default=None, ge=1, le=50_000)
    height: int | None = Field(default=None, ge=1, le=50_000)
    acquisition_evidence_sha256: str = Field(pattern=SHA256_PATTERN)
    task_readable: Literal[True]
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_receipt(self) -> MediaAcquisitionReceipt:
        if self.acquisition_id.version != 5:
            raise ValueError("acquisition_id must be server-derived UUIDv5")
        _validate_public_https_url(self.normalized_source_url, label="acquisition normalized_source_url")
        if sha256_text(self.normalized_source_url) != self.source_url_sha256:
            raise ValueError("acquisition source_url_sha256 differs")
        path = PurePosixPath(self.object_ref)
        if path.is_absolute() or ".." in path.parts or not path.parts:
            raise ValueError("acquisition object_ref is not task-private and relative")
        if canonical_sha256(_without(self, "receipt_sha256")) != self.receipt_sha256:
            raise ValueError("media acquisition receipt_sha256 differs")
        return self


class MediaArtifactManifest(StrictModel):
    schema_version: Literal["region-talk-media-artifact-manifest.v1"]
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    acquisition_receipts: tuple[MediaAcquisitionReceipt, ...] = Field(min_length=1, max_length=20)
    items: tuple[MediaArtifact, ...] = Field(min_length=1, max_length=20)
    manifest_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_manifest(self) -> MediaArtifactManifest:
        if len({item.asset_id for item in self.items}) != len(self.items):
            raise ValueError("media asset_id values must be unique")
        if len({item.source_media_id for item in self.items}) != len(self.items):
            raise ValueError("source_media_id values must be unique")
        receipts = {str(receipt.asset_id): receipt for receipt in self.acquisition_receipts}
        if len(receipts) != len(self.acquisition_receipts):
            raise ValueError("acquisition receipt asset IDs must be unique")
        if set(receipts) != {item.asset_id for item in self.items}:
            raise ValueError("acquisition receipt does not cover the exact media manifest")
        for item in self.items:
            receipt = receipts[item.asset_id]
            if (
                receipt.candidate_revision_fingerprint != self.candidate_revision_fingerprint
                or receipt.source_media_id != item.source_media_id
                or receipt.normalized_source_url != item.normalized_source_url
                or receipt.source_url_sha256 != item.source_url_sha256
                or receipt.object_ref != item.object_ref
                or receipt.artifact_sha256 != item.artifact_sha256
                or receipt.byte_size != item.byte_size
                or receipt.content_type != item.content_type
                or receipt.width != item.width
                or receipt.height != item.height
            ):
                raise ValueError("media artifact differs from its immutable acquisition receipt")
        if canonical_sha256(_without(self, "manifest_sha256")) != self.manifest_sha256:
            raise ValueError("media manifest_sha256 differs")
        return self


class ImagePolicy(StrictModel):
    decision_contract_version: Literal["region_talk_article_image_association_v4"]
    acquisition_version: Literal["region_talk_http_article_image_evidence_v4"]
    scorer_version: Literal["region_talk_cv_clip_laion_nima_legacy_v1"]
    vlm_prompt_version: Literal["region_talk_visual_article_association_v3"]
    model_bundle_sha256: str = Field(pattern=SHA256_PATTERN)
    vlm_model_id: str = Field(min_length=1, max_length=200)


class ImageScoringInput(StrictModel):
    schema_version: Literal["region-talk-image-input.v1"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content: ContentEvidence
    eligibility_fingerprint: str = Field(pattern=SHA256_PATTERN)
    availability: Literal["AVAILABLE", "UNAVAILABLE"]
    unavailable_reason: str = Field(default="", max_length=500)
    artifact_manifest: MediaArtifactManifest | None = None
    policy: ImagePolicy
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_image_input(self) -> ImageScoringInput:
        if self.availability == "AVAILABLE":
            if self.artifact_manifest is None or self.unavailable_reason:
                raise ValueError("AVAILABLE image input requires only an artifact manifest")
            if self.artifact_manifest.candidate_revision_fingerprint != self.candidate_revision_fingerprint:
                raise ValueError("media manifest is stale for the candidate revision")
        elif self.artifact_manifest is not None or not self.unavailable_reason:
            raise ValueError("UNAVAILABLE image input requires a reason and no artifact manifest")
        if canonical_sha256(_without(self, "input_fingerprint")) != self.input_fingerprint:
            raise ValueError("image input_fingerprint differs")
        return self


class ImageFrameScores(StrictModel):
    media_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,300}$")
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    content_text_sha256: str = Field(pattern=SHA256_PATTERN)
    scorer_request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    cv_overall_media_score: float = Field(ge=0, le=1)
    technical_quality_score: float = Field(ge=0, le=1)
    clip_visual_fit_score: float = Field(ge=0, le=1)
    laion_aesthetic_score: float = Field(ge=0, le=1)
    nima_quality_score: float = Field(ge=0, le=1)
    overall_media_score: float = Field(ge=0, le=1)
    model_bundle_sha256: str = Field(pattern=SHA256_PATTERN)


class VisualAdjudication(StrictModel):
    decision: Literal["accept", "reject", "review"]
    article_association_supported: bool
    selected_media_ids: tuple[str, ...] = Field(default=(), max_length=6)
    reason_code: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)
    producer_exact_id: str = Field(min_length=1, max_length=500)


class ImageScoringResult(StrictModel):
    schema_version: Literal["region-talk-image-scoring-result.v1"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    media_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    producer_exact_id: str = Field(min_length=1, max_length=500)
    decision: Literal["legacy_auto_accept", "vlm_visual_accept", "needs_visual_review"]
    reason_codes: tuple[str, ...] = Field(min_length=1, max_length=20)
    frames: tuple[ImageFrameScores, ...] = Field(min_length=1, max_length=20)
    selected_media_ids: tuple[str, ...] = Field(default=(), max_length=6)
    visual_adjudication: VisualAdjudication | None = None
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    result_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_image_result(self) -> ImageScoringResult:
        frame_ids = {frame.media_id for frame in self.frames}
        if len(frame_ids) != len(self.frames) or not set(self.selected_media_ids) <= frame_ids:
            raise ValueError("selected image result media IDs differ from scored frames")
        if self.decision == "vlm_visual_accept":
            if self.visual_adjudication is None or self.visual_adjudication.decision != "accept":
                raise ValueError("VLM accept lacks exact adjudication")
            if not self.visual_adjudication.article_association_supported:
                raise ValueError("VLM accept lacks article association")
            if self.selected_media_ids != self.visual_adjudication.selected_media_ids:
                raise ValueError("VLM result selection differs from adjudication")
        if self.decision == "legacy_auto_accept" and not self.selected_media_ids:
            raise ValueError("legacy accept requires a selected image")
        if self.decision == "needs_visual_review" and self.selected_media_ids:
            raise ValueError("unreviewed image result cannot select publication media")
        if canonical_sha256(_without(self, "result_sha256")) != self.result_sha256:
            raise ValueError("image result_sha256 differs")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_RESULT_BYTES:
            raise ValueError("image result exceeds 64 KiB")
        return self


class FactEvidence(StrictModel):
    fact_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,160}$")
    claim: str = Field(min_length=1, max_length=1_000)
    support_excerpt: str = Field(min_length=1, max_length=1_000)
    source_url: str = Field(min_length=1, max_length=4_000)
    support_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_support(self) -> FactEvidence:
        _validate_public_https_url(self.source_url, label="fact source_url")
        if sha256_text(self.support_excerpt) != self.support_sha256:
            raise ValueError("fact support_sha256 differs")
        return self


class FactPack(StrictModel):
    schema_version: Literal["region-talk-fact-pack.v1"]
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    facts: tuple[FactEvidence, ...] = Field(min_length=1, max_length=20)
    fact_pack_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_fact_pack(self) -> FactPack:
        if len({fact.fact_id for fact in self.facts}) != len(self.facts):
            raise ValueError("fact_id values must be unique")
        if canonical_sha256(_without(self, "fact_pack_sha256")) != self.fact_pack_sha256:
            raise ValueError("fact_pack_sha256 differs")
        return self


class SourceEvidence(StrictModel):
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    canonical_source_key: str = Field(min_length=1, max_length=1_000)
    externality_status: Literal["verified", "unknown", "local"]
    source_scope: Literal["external", "mixed", "regional", "unknown"]
    source_fingerprint: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_source(self) -> SourceEvidence:
        if canonical_sha256(_without(self, "source_fingerprint")) != self.source_fingerprint:
            raise ValueError("source_fingerprint differs")
        return self


class UpstreamResultReceipt(StrictModel):
    stage: Literal["vector_fusion", "image_scoring", "final_verifier"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    result_sha256: str = Field(pattern=SHA256_PATTERN)
    result_metadata_sha256: str = Field(pattern=SHA256_PATTERN)


class FinalVerifierPolicy(StrictModel):
    eligibility_gate_version: Literal["region_talk_publication_eligibility_v5"]
    prompt_version: Literal["region_talk_final_verifier_v7_grounded_draft"]
    model_id: str = Field(min_length=1, max_length=200)


class FinalVerifierInput(StrictModel):
    schema_version: Literal["region-talk-final-verifier-input.v1"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content: ContentEvidence
    fact_pack: FactPack
    source: SourceEvidence
    vector_result_sha256: str = Field(pattern=SHA256_PATTERN)
    image_result_sha256: str = Field(pattern=SHA256_PATTERN)
    image_result: ImageScoringResult
    upstream_results: tuple[UpstreamResultReceipt, ...] = Field(min_length=2, max_length=2)
    policy: FinalVerifierPolicy
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_final_input(self) -> FinalVerifierInput:
        by_stage = {item.stage: item for item in self.upstream_results}
        if set(by_stage) != {"vector_fusion", "image_scoring"}:
            raise ValueError("final verifier requires exact vector and image receipts")
        if by_stage["vector_fusion"].result_sha256 != self.vector_result_sha256:
            raise ValueError("vector result receipt differs")
        if by_stage["image_scoring"].result_sha256 != self.image_result_sha256:
            raise ValueError("image result receipt differs")
        if self.image_result.result_sha256 != self.image_result_sha256:
            raise ValueError("typed image result hash differs")
        if self.image_result.candidate_revision_fingerprint != self.candidate_revision_fingerprint:
            raise ValueError("typed image result is stale for the candidate revision")
        if self.fact_pack.candidate_revision_fingerprint != self.candidate_revision_fingerprint:
            raise ValueError("fact pack is stale for the candidate revision")
        if self.source.candidate_revision_fingerprint != self.candidate_revision_fingerprint:
            raise ValueError("source evidence is stale for the candidate revision")
        if by_stage["image_scoring"].input_fingerprint != self.image_result.input_fingerprint:
            raise ValueError("image result input receipt differs")
        if self.source.canonical_source_key != self.content.canonical_source_key:
            raise ValueError("source evidence differs from content source")
        if canonical_sha256(_without(self, "input_fingerprint")) != self.input_fingerprint:
            raise ValueError("final verifier input_fingerprint differs")
        return self


class GroundingReference(StrictModel):
    claim: str = Field(min_length=1, max_length=1_000)
    fact_ids: tuple[str, ...] = Field(min_length=1, max_length=20)


class FinalVerifierProviderResponse(StrictModel):
    decision: Literal["accept", "reject", "needs_review"]
    reason_codes: tuple[str, ...] = Field(min_length=1, max_length=20)
    grounding: tuple[GroundingReference, ...] = Field(default=(), max_length=20)
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)


class FinalVerifierResult(StrictModel):
    schema_version: Literal["region-talk-final-verifier-result.v1"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    fact_pack_sha256: str = Field(pattern=SHA256_PATTERN)
    source_fingerprint: str = Field(pattern=SHA256_PATTERN)
    image_result_sha256: str = Field(pattern=SHA256_PATTERN)
    producer_exact_id: str = Field(min_length=1, max_length=500)
    decision: Literal["accept", "reject", "needs_review"]
    reason_codes: tuple[str, ...] = Field(min_length=1, max_length=20)
    grounding: tuple[GroundingReference, ...] = Field(default=(), max_length=20)
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    result_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_final_result(self) -> FinalVerifierResult:
        if self.decision == "accept" and not self.grounding:
            raise ValueError("accepted verifier result requires grounding")
        if canonical_sha256(_without(self, "result_sha256")) != self.result_sha256:
            raise ValueError("final verifier result_sha256 differs")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_RESULT_BYTES:
            raise ValueError("final verifier result exceeds 64 KiB")
        return self


class PublisherDimensions(StrictModel):
    publisher_identity: str = Field(min_length=1, max_length=1_000)
    intended_audience: str = Field(min_length=1, max_length=1_000)
    distinctive_value: str = Field(min_length=1, max_length=1_000)


class SourceProfile(StrictModel):
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    canonical_source_key: str = Field(min_length=1, max_length=1_000)
    source_fingerprint: str = Field(pattern=SHA256_PATTERN)
    profile_fingerprint: str = Field(pattern=SHA256_PATTERN)
    entity_type: Literal[
        "person", "collective", "journal", "media_brand", "professional_platform", "thematic_channel"
    ]
    externality_status: Literal["verified", "unknown", "local"]
    dimensions: PublisherDimensions

    @model_validator(mode="after")
    def exact_profile(self) -> SourceProfile:
        if canonical_sha256(_without(self, "profile_fingerprint")) != self.profile_fingerprint:
            raise ValueError("profile_fingerprint differs")
        return self


class PublicationHistoryItem(StrictModel):
    history_id: str = Field(pattern=r"^[A-Za-z0-9_.:-]{1,160}$")
    published_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    draft_fingerprint: str = Field(pattern=SHA256_PATTERN)
    body_text: str = Field(min_length=1, max_length=4_000)


class WriterPolicy(StrictModel):
    writer_version: Literal["region_talk_editorial_writer_v12_publisher_reader_brief"]
    output_contract: Literal["region_talk_editorial_output_v6_publisher_reader_brief"]
    input_contract: Literal["region_talk_editorial_input_v3_source_profile"]
    stage_execution_version: Literal["region_talk_writer_v12_publisher_reader_brief_v2"]
    media_materialization_contract: Literal["region_talk_media_materialization_v1"]
    model_id: str = Field(min_length=1, max_length=200)


class WriterInput(StrictModel):
    schema_version: Literal["region-talk-writer-input.v1"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content: ContentEvidence
    fact_pack: FactPack
    source_profile: SourceProfile
    image_result_sha256: str = Field(pattern=SHA256_PATTERN)
    final_result_sha256: str = Field(pattern=SHA256_PATTERN)
    image_result: ImageScoringResult
    final_result: FinalVerifierResult
    upstream_results: tuple[UpstreamResultReceipt, ...] = Field(min_length=2, max_length=2)
    history: tuple[PublicationHistoryItem, ...] = Field(default=(), max_length=5)
    policy: WriterPolicy
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @model_validator(mode="after")
    def exact_writer_input(self) -> WriterInput:
        by_stage = {item.stage: item for item in self.upstream_results}
        if set(by_stage) != {"image_scoring", "final_verifier"}:
            raise ValueError("writer requires exact image and verifier receipts")
        if by_stage["image_scoring"].result_sha256 != self.image_result_sha256:
            raise ValueError("writer image result receipt differs")
        if by_stage["final_verifier"].result_sha256 != self.final_result_sha256:
            raise ValueError("writer final verifier receipt differs")
        if self.image_result.result_sha256 != self.image_result_sha256:
            raise ValueError("writer typed image result hash differs")
        if self.final_result.result_sha256 != self.final_result_sha256:
            raise ValueError("writer typed final verifier result hash differs")
        if (
            self.image_result.candidate_revision_fingerprint != self.candidate_revision_fingerprint
            or self.final_result.candidate_revision_fingerprint != self.candidate_revision_fingerprint
        ):
            raise ValueError("writer upstream result is stale for the candidate revision")
        if self.fact_pack.candidate_revision_fingerprint != self.candidate_revision_fingerprint:
            raise ValueError("writer fact pack is stale for the candidate revision")
        if self.source_profile.candidate_revision_fingerprint != self.candidate_revision_fingerprint:
            raise ValueError("writer source profile is stale for the candidate revision")
        if by_stage["image_scoring"].input_fingerprint != self.image_result.input_fingerprint:
            raise ValueError("writer image result input receipt differs")
        if by_stage["final_verifier"].input_fingerprint != self.final_result.input_fingerprint:
            raise ValueError("writer final result input receipt differs")
        if self.final_result.decision != "accept":
            raise ValueError("writer requires an accepted exact final verifier result")
        if self.source_profile.canonical_source_key != self.content.canonical_source_key:
            raise ValueError("writer source profile differs from content source")
        if len({item.history_id for item in self.history}) != len(self.history):
            raise ValueError("writer history IDs must be unique")
        if canonical_sha256(_without(self, "input_fingerprint")) != self.input_fingerprint:
            raise ValueError("writer input_fingerprint differs")
        return self


class EditorialStrategy(StrictModel):
    angle: str = Field(min_length=1, max_length=1_000)
    current_hook_fact_ids: tuple[str, ...] = Field(min_length=1, max_length=10)
    source_value_fact_ids: tuple[str, ...] = Field(min_length=1, max_length=10)
    visual_hook_media_ids: tuple[str, ...] = Field(default=(), max_length=6)


class WriterDraft(StrictModel):
    title: str = Field(min_length=1, max_length=500)
    paragraph_one: str = Field(min_length=1, max_length=2_000)
    paragraph_two: str = Field(min_length=1, max_length=2_000)
    grounding: tuple[GroundingReference, ...] = Field(min_length=1, max_length=30)


class CriticResponse(StrictModel):
    decision: Literal["pass", "rewrite", "reject"]
    defects: tuple[str, ...] = Field(default=(), max_length=20)

    @model_validator(mode="after")
    def defects_match_decision(self) -> CriticResponse:
        if self.decision in {"rewrite", "reject"} and not self.defects:
            raise ValueError("critic rewrite/reject requires defects")
        return self


class EditorialStrategyResponse(StrictModel):
    strategy: EditorialStrategy
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)


class WriterDraftResponse(StrictModel):
    draft: WriterDraft
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)


class CriticProviderResponse(StrictModel):
    critic: CriticResponse
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)


class WriterResult(StrictModel):
    schema_version: Literal["region-talk-writer-result.v1"]
    input_fingerprint: str = Field(pattern=SHA256_PATTERN)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    fact_pack_sha256: str = Field(pattern=SHA256_PATTERN)
    source_profile_fingerprint: str = Field(pattern=SHA256_PATTERN)
    final_result_sha256: str = Field(pattern=SHA256_PATTERN)
    producer_exact_id: str = Field(min_length=1, max_length=500)
    status: Literal[
        "ready_for_operator_review",
        "needs_facts",
        "needs_source_profile",
        "needs_grounding_review",
        "rejected",
    ]
    title: str = Field(default="", max_length=500)
    paragraph_one: str = Field(default="", max_length=2_000)
    paragraph_two: str = Field(default="", max_length=2_000)
    grounding: tuple[GroundingReference, ...] = Field(default=(), max_length=30)
    strategy: EditorialStrategy | None = None
    critic: CriticResponse | None = None
    rewrite_count: int = Field(ge=0, le=1)
    request_fingerprint: str = Field(pattern=SHA256_PATTERN)
    model_id: str = Field(min_length=1, max_length=200)
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False
    result_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_writer_result(self) -> WriterResult:
        ready = self.status == "ready_for_operator_review"
        if ready and (
            not self.paragraph_one
            or not self.title
            or not self.paragraph_two
            or not self.grounding
            or self.strategy is None
            or self.critic is None
            or self.critic.decision != "pass"
        ):
            raise ValueError("ready writer result lacks its exact draft audit")
        if not ready and (self.title or self.paragraph_one or self.paragraph_two):
            raise ValueError("non-ready writer result cannot expose publication copy")
        if canonical_sha256(_without(self, "result_sha256")) != self.result_sha256:
            raise ValueError("writer result_sha256 differs")
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_RESULT_BYTES:
            raise ValueError("writer result exceeds 64 KiB")
        return self


type HeavyStageInput = Annotated[
    ImageScoringInput | FinalVerifierInput | WriterInput,
    Field(discriminator="schema_version"),
]
type HeavyStageResult = Annotated[
    ImageScoringResult | FinalVerifierResult | WriterResult,
    Field(discriminator="schema_version"),
]
HEAVY_INPUT_ADAPTER = TypeAdapter(HeavyStageInput)
HEAVY_RESULT_ADAPTER = TypeAdapter(HeavyStageResult)


def validate_heavy_stage_input(
    stage: str,
    value: dict[str, Any],
    *,
    expected_input_fingerprint: str,
) -> HeavyStageInput:
    parsed = HEAVY_INPUT_ADAPTER.validate_python(value)
    expected_type = {
        "image_scoring": ImageScoringInput,
        "final_verifier": FinalVerifierInput,
        "writer": WriterInput,
    }.get(stage)
    if expected_type is None or not isinstance(parsed, expected_type):
        raise HeavyContractError("heavy input schema differs from stage")
    if parsed.input_fingerprint != expected_input_fingerprint:
        raise HeavyContractError("heavy input differs from work input fingerprint")
    return parsed


def validate_heavy_stage_result(
    stage: str,
    value: dict[str, Any],
    *,
    expected_input_fingerprint: str,
) -> HeavyStageResult:
    parsed = HEAVY_RESULT_ADAPTER.validate_python(value)
    expected_type = {
        "image_scoring": ImageScoringResult,
        "final_verifier": FinalVerifierResult,
        "writer": WriterResult,
    }.get(stage)
    if expected_type is None or not isinstance(parsed, expected_type):
        raise HeavyContractError("heavy result schema differs from stage")
    if parsed.input_fingerprint != expected_input_fingerprint:
        raise HeavyContractError("heavy result differs from work input fingerprint")
    return parsed


def validate_heavy_result_against_input(
    request: HeavyStageInput,
    result: HeavyStageResult,
) -> HeavyStageResult:
    """Reject a self-consistent result that is not evidence-bound to its exact input."""

    if result.input_fingerprint != request.input_fingerprint:
        raise HeavyContractError("heavy result differs from exact input")
    if result.candidate_revision_fingerprint != request.candidate_revision_fingerprint:
        raise HeavyContractError("heavy result is stale for the candidate revision")
    if isinstance(request, ImageScoringInput) and isinstance(result, ImageScoringResult):
        if request.artifact_manifest is None:
            raise HeavyContractError("image result cannot succeed without an artifact manifest")
        artifacts = {item.source_media_id: item for item in request.artifact_manifest.items}
        frames = {item.media_id: item for item in result.frames}
        if result.media_manifest_sha256 != request.artifact_manifest.manifest_sha256 or set(frames) != set(artifacts):
            raise HeavyContractError("image result differs from exact media manifest")
        for media_id, frame in frames.items():
            artifact = artifacts[media_id]
            if (
                frame.artifact_sha256 != artifact.artifact_sha256
                or frame.content_text_sha256 != request.content.text_sha256
                or frame.model_bundle_sha256 != request.policy.model_bundle_sha256
            ):
                raise HeavyContractError("image result score provenance differs from input")
        return result
    if isinstance(request, FinalVerifierInput) and isinstance(result, FinalVerifierResult):
        if (
            result.fact_pack_sha256 != request.fact_pack.fact_pack_sha256
            or result.source_fingerprint != request.source.source_fingerprint
            or result.image_result_sha256 != request.image_result_sha256
            or result.model_id != request.policy.model_id
        ):
            raise HeavyContractError("final verifier result provenance differs from input")
        fact_ids = {fact.fact_id for fact in request.fact_pack.facts}
        if result.decision == "accept" and any(not set(item.fact_ids) <= fact_ids for item in result.grounding):
            raise HeavyContractError("final verifier grounding is stale")
        return result
    if isinstance(request, WriterInput) and isinstance(result, WriterResult):
        if (
            result.fact_pack_sha256 != request.fact_pack.fact_pack_sha256
            or result.source_profile_fingerprint != request.source_profile.profile_fingerprint
            or result.final_result_sha256 != request.final_result_sha256
            or result.model_id != request.policy.model_id
        ):
            raise HeavyContractError("writer result provenance differs from input")
        fact_ids = {fact.fact_id for fact in request.fact_pack.facts}
        if any(not set(item.fact_ids) <= fact_ids for item in result.grounding):
            raise HeavyContractError("writer grounding is stale")
        if result.strategy is not None and (
            not set(result.strategy.current_hook_fact_ids) <= fact_ids
            or not set(result.strategy.source_value_fact_ids) <= fact_ids
            or not set(result.strategy.visual_hook_media_ids) <= set(request.image_result.selected_media_ids)
        ):
            raise HeavyContractError("writer strategy is stale")
        return result
    raise HeavyContractError("heavy result schema differs from exact input stage")


def canonical_json_object(path: str) -> dict[str, Any]:
    """Load a canonical JSON object for an injected, already-local asset."""

    from pathlib import Path

    body = Path(path).read_bytes()
    value = json.loads(body)
    if not isinstance(value, dict) or body != canonical_json_bytes(value):
        raise HeavyContractError("asset manifest is not a canonical JSON object")
    return value


__all__ = [
    "CriticProviderResponse",
    "CriticResponse",
    "EditorialStrategy",
    "EditorialStrategyResponse",
    "FactEvidence",
    "FactPack",
    "FinalVerifierInput",
    "FinalVerifierProviderResponse",
    "FinalVerifierResult",
    "HeavyContractError",
    "HeavyRuntimeUnavailable",
    "ImageFrameScores",
    "ImageScoringInput",
    "ImageScoringResult",
    "MediaAcquisitionReceipt",
    "MediaArtifact",
    "MediaArtifactManifest",
    "SourceEvidence",
    "SourceProfile",
    "VisualAdjudication",
    "WriterDraft",
    "WriterDraftResponse",
    "WriterInput",
    "WriterResult",
    "canonical_sha256",
    "sha256_text",
    "validate_heavy_result_against_input",
    "validate_heavy_stage_input",
    "validate_heavy_stage_result",
]
