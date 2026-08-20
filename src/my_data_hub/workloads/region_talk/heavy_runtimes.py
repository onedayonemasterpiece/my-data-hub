# ruff: noqa: RUF001
"""Pure orchestration for Region Talk heavy stages with injected side effects.

Donor provenance (all at ``events-bot-new@5bbdb681623d5e4e0bff2133e487a6663c1a838a``):

* image logic: ``kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py``
  (SHA-256 ``d5e3683f04bab191b05881e3167b61b4a27b64eeef92c112548d054cbb245162``);
* final verifier: ``scripts/region_talk_publication_finalizer.py``
  (SHA-256 ``1cf78a6ff6b2df21475587a83de1b4c4790080f55b84491939f96e9e8ab901fe``);
* writer: ``scripts/region_talk_publication_draft_backfill.py``
  (SHA-256 ``426d44396b04d7bff677497663a632219139ccfbe3354f31e99f4ecf38e0452a``).

The donor's YDB writes, dynamic package/model downloads, provider construction,
and publication surfaces are intentionally not ported.  Those capabilities are
represented only by narrow protocols supplied inside a private worker.
"""

from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping
from typing import Any, Protocol

from .heavy_contracts import (
    CriticProviderResponse,
    CriticResponse,
    EditorialStrategy,
    EditorialStrategyResponse,
    FinalVerifierInput,
    FinalVerifierProviderResponse,
    FinalVerifierResult,
    GroundingReference,
    HeavyRuntimeUnavailable,
    ImageFrameScores,
    ImageScoringInput,
    ImageScoringResult,
    MediaArtifact,
    VisualAdjudication,
    WriterDraft,
    WriterDraftResponse,
    WriterInput,
    WriterResult,
    canonical_sha256,
)


class MediaArtifactReader(Protocol):
    """Resolve one task-private object; source URLs are never accepted here."""

    @property
    def authorized_receipt_sha256s(self) -> frozenset[str]: ...

    def read(self, object_ref: str, *, maximum_bytes: int) -> bytes: ...


class ImageScoreEngine(Protocol):
    @property
    def producer_exact_id(self) -> str: ...

    @property
    def model_bundle_sha256(self) -> str: ...

    def score(
        self,
        artifact: MediaArtifact,
        body: bytes,
        *,
        content_text: str,
        request_fingerprint: str,
    ) -> ImageFrameScores: ...


class VisualAdjudicator(Protocol):
    @property
    def producer_exact_id(self) -> str: ...

    def adjudicate(
        self,
        request: ImageScoringInput,
        frames: tuple[ImageFrameScores, ...],
        *,
        request_fingerprint: str,
    ) -> VisualAdjudication: ...


class FinalVerifierProvider(Protocol):
    @property
    def producer_exact_id(self) -> str: ...

    def verify(
        self,
        request: FinalVerifierInput,
        *,
        request_fingerprint: str,
    ) -> FinalVerifierProviderResponse: ...


class EditorialProvider(Protocol):
    @property
    def producer_exact_id(self) -> str: ...

    def strategy(self, request: WriterInput, *, request_fingerprint: str) -> EditorialStrategyResponse: ...

    def write(
        self,
        request: WriterInput,
        strategy: EditorialStrategy,
        *,
        request_fingerprint: str,
        defects: tuple[str, ...] = (),
    ) -> WriterDraftResponse: ...

    def critic(
        self,
        request: WriterInput,
        strategy: EditorialStrategy,
        draft: WriterDraft,
        *,
        request_fingerprint: str,
    ) -> CriticProviderResponse: ...


def _result_payload(value: dict[str, Any]) -> dict[str, Any]:
    value["result_sha256"] = canonical_sha256(value)
    return value


def _content_text(request: ImageScoringInput) -> str:
    content = request.content
    return content.body_text or "\n\n".join(part for part in (content.title, content.summary) if part)


def _legacy_frame_passes(frame: ImageFrameScores) -> bool:
    primary = frame.overall_media_score >= 0.66 and frame.clip_visual_fit_score >= 0.55
    narrow_override = (
        frame.overall_media_score >= 0.63
        and frame.clip_visual_fit_score >= 0.85
        and frame.laion_aesthetic_score + 0.001 >= 0.52
        and frame.technical_quality_score >= 0.68
    )
    return primary or narrow_override


class DeterministicImageRuntime:
    """Album-safe donor transition without any implicit network/model fallback."""

    def __init__(
        self,
        *,
        reader: MediaArtifactReader | None,
        scorer: ImageScoreEngine | None,
        adjudicator: VisualAdjudicator | None = None,
    ) -> None:
        self.reader = reader
        self.scorer = scorer
        self.adjudicator = adjudicator

    @property
    def producer_exact_id(self) -> str:
        if self.scorer is None:
            return "region-talk:image-runtime@unattached"
        return self.scorer.producer_exact_id

    def execute(self, request: ImageScoringInput) -> ImageScoringResult:
        if request.availability != "AVAILABLE" or request.artifact_manifest is None:
            raise HeavyRuntimeUnavailable(request.unavailable_reason or "verified media is unavailable")
        if self.reader is None or self.scorer is None:
            raise HeavyRuntimeUnavailable("image artifact reader or exact score engine is unattached")
        if self.scorer.model_bundle_sha256 != request.policy.model_bundle_sha256:
            raise HeavyRuntimeUnavailable("image model bundle differs from the execution pin")
        frames: list[ImageFrameScores] = []
        content_text = _content_text(request)
        receipts = {str(receipt.asset_id): receipt for receipt in request.artifact_manifest.acquisition_receipts}
        for artifact in request.artifact_manifest.items:
            receipt = receipts[artifact.asset_id]
            if receipt.receipt_sha256 not in self.reader.authorized_receipt_sha256s:
                raise HeavyRuntimeUnavailable("media reader is not bound to the immutable acquisition receipt")
            body = self.reader.read(artifact.object_ref, maximum_bytes=artifact.byte_size)
            if len(body) != artifact.byte_size or hashlib.sha256(body).hexdigest() != artifact.artifact_sha256:
                raise ValueError("materialized image bytes differ from their exact manifest")
            score_request_fp = canonical_sha256(
                {
                    "schema_version": request.policy.scorer_version,
                    "input_fingerprint": request.input_fingerprint,
                    "artifact_sha256": artifact.artifact_sha256,
                    "source_url_sha256": artifact.source_url_sha256,
                    "content_text_sha256": hashlib.sha256(content_text.encode("utf-8")).hexdigest(),
                    "model_bundle_sha256": request.policy.model_bundle_sha256,
                }
            )
            score = self.scorer.score(
                artifact,
                body,
                content_text=content_text,
                request_fingerprint=score_request_fp,
            )
            if score.media_id != artifact.source_media_id:
                raise ValueError("image score media identity differs")
            if score.model_bundle_sha256 != request.policy.model_bundle_sha256:
                raise ValueError("image score model bundle differs")
            if (
                score.artifact_sha256 != artifact.artifact_sha256
                or score.content_text_sha256 != hashlib.sha256(content_text.encode("utf-8")).hexdigest()
                or score.scorer_request_fingerprint != score_request_fp
            ):
                raise ValueError("image score provenance differs from exact request")
            frames.append(score)

        anchor = frames[0]
        decision = "legacy_auto_accept" if _legacy_frame_passes(anchor) else "needs_visual_review"
        reasons = ["legacy_anchor_passed" if decision == "legacy_auto_accept" else "uncalibrated_score_abstained"]
        selected = (anchor.media_id,) if decision == "legacy_auto_accept" else ()
        adjudication: VisualAdjudication | None = None
        if decision != "legacy_auto_accept" and self.adjudicator is not None:
            request_fp = canonical_sha256(
                {
                    "schema_version": request.policy.vlm_prompt_version,
                    "input_fingerprint": request.input_fingerprint,
                    "media_manifest_sha256": request.artifact_manifest.manifest_sha256,
                    "model_id": request.policy.vlm_model_id,
                    "frames": [frame.model_dump(mode="json") for frame in frames],
                }
            )
            adjudication = self.adjudicator.adjudicate(request, tuple(frames), request_fingerprint=request_fp)
            if (
                adjudication.request_fingerprint != request_fp
                or adjudication.model_id != request.policy.vlm_model_id
                or adjudication.producer_exact_id != self.adjudicator.producer_exact_id
            ):
                raise ValueError("visual adjudication differs from exact request")
            if adjudication.decision == "accept" and adjudication.article_association_supported:
                frame_ids = {item.media_id for item in frames}
                if not adjudication.selected_media_ids or not set(adjudication.selected_media_ids) <= frame_ids:
                    raise ValueError("visual adjudication selected unknown media")
                decision = "vlm_visual_accept"
                reasons = ["current_vlm_article_association_accept"]
                selected = adjudication.selected_media_ids
            else:
                reasons = ["current_vlm_abstained_or_rejected"]

        return ImageScoringResult.model_validate(
            _result_payload(
                {
                    "schema_version": "region-talk-image-scoring-result.v1",
                    "input_fingerprint": request.input_fingerprint,
                    "candidate_revision_fingerprint": request.candidate_revision_fingerprint,
                    "media_manifest_sha256": request.artifact_manifest.manifest_sha256,
                    "producer_exact_id": self.producer_exact_id,
                    "decision": decision,
                    "reason_codes": reasons,
                    "frames": [frame.model_dump(mode="json") for frame in frames],
                    "selected_media_ids": selected,
                    "visual_adjudication": (
                        adjudication.model_dump(mode="json") if adjudication is not None else None
                    ),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
            )
        )


def _grounding_is_current(grounding: tuple[GroundingReference, ...], fact_ids: set[str]) -> bool:
    return bool(grounding) and all(set(item.fact_ids) <= fact_ids for item in grounding)


class DeterministicFinalVerifierRuntime:
    def __init__(self, provider: FinalVerifierProvider | None) -> None:
        self.provider = provider

    @property
    def producer_exact_id(self) -> str:
        return self.provider.producer_exact_id if self.provider is not None else "region-talk:verifier@unattached"

    def execute(self, request: FinalVerifierInput) -> FinalVerifierResult:
        if request.source.externality_status != "verified" or request.source.source_scope != "external":
            return self._closed_result(request, "needs_review", ("externality_not_verified",), ())
        if request.image_result.decision == "needs_visual_review":
            return self._closed_result(request, "needs_review", ("visual_review_required",), ())
        if self.provider is None:
            raise HeavyRuntimeUnavailable("final verifier provider is unattached")
        request_fp = canonical_sha256(
            {
                "schema_version": request.policy.prompt_version,
                "input_fingerprint": request.input_fingerprint,
                "model_id": request.policy.model_id,
                "fact_pack_sha256": request.fact_pack.fact_pack_sha256,
                "source_fingerprint": request.source.source_fingerprint,
                "image_result_sha256": request.image_result_sha256,
                "vector_result_sha256": request.vector_result_sha256,
            }
        )
        response = self.provider.verify(request, request_fingerprint=request_fp)
        if response.request_fingerprint != request_fp or response.model_id != request.policy.model_id:
            raise ValueError("final verifier response differs from exact request")
        fact_ids = {fact.fact_id for fact in request.fact_pack.facts}
        if response.decision == "accept" and not _grounding_is_current(response.grounding, fact_ids):
            raise ValueError("accepted final verifier response has stale or absent grounding")
        return self._closed_result(
            request,
            response.decision,
            response.reason_codes,
            response.grounding,
            request_fingerprint=request_fp,
            model_id=response.model_id,
        )

    def _closed_result(
        self,
        request: FinalVerifierInput,
        decision: str,
        reasons: tuple[str, ...],
        grounding: tuple[GroundingReference, ...],
        *,
        request_fingerprint: str | None = None,
        model_id: str | None = None,
    ) -> FinalVerifierResult:
        request_fingerprint = request_fingerprint or canonical_sha256(
            {"input_fingerprint": request.input_fingerprint, "closed_reason": list(reasons)}
        )
        return FinalVerifierResult.model_validate(
            _result_payload(
                {
                    "schema_version": "region-talk-final-verifier-result.v1",
                    "input_fingerprint": request.input_fingerprint,
                    "candidate_revision_fingerprint": request.candidate_revision_fingerprint,
                    "fact_pack_sha256": request.fact_pack.fact_pack_sha256,
                    "source_fingerprint": request.source.source_fingerprint,
                    "image_result_sha256": request.image_result_sha256,
                    "producer_exact_id": self.producer_exact_id,
                    "decision": decision,
                    "reason_codes": reasons,
                    "grounding": [item.model_dump(mode="json") for item in grounding],
                    "request_fingerprint": request_fingerprint,
                    "model_id": model_id or request.policy.model_id,
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
            )
        )


_FIRST_PERSON = re.compile(r"(?iu)(?:^|\W)(?:я|мы|мой|моя|моё|наши|наш|нам|нас|мне)(?:$|\W)")
_BANNED_COPY = tuple(
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\bуникальн\w*\b",
        r"\bневероятн\w*\b",
        r"\bобязательно\s+посмотр\w*\b",
        r"\bв\s+данной\s+статье\b",
        r"\bв\s+рамках\b",
    )
)


def _sentence_count(value: str) -> int:
    return len([part for part in re.split(r"(?<=[.!?])\s+", value.strip()) if part.strip()])


def deterministic_draft_defects(draft: WriterDraft, fact_ids: set[str]) -> tuple[str, ...]:
    defects: list[str] = []
    if _sentence_count(draft.paragraph_one) != 2:
        defects.append("paragraph_one_requires_two_sentences")
    if not 1 <= _sentence_count(draft.paragraph_two) <= 2:
        defects.append("paragraph_two_requires_one_or_two_sentences")
    visible = draft.paragraph_one + "\n\n" + draft.paragraph_two
    if not 260 <= len(visible) <= 900:
        defects.append("visible_copy_length_out_of_bounds")
    if _FIRST_PERSON.search(visible):
        defects.append("third_person_contract_failed")
    if any(pattern.search(visible) for pattern in _BANNED_COPY):
        defects.append("banned_ai_cliche")
    if not _grounding_is_current(draft.grounding, fact_ids):
        defects.append("grounding_is_missing_or_stale")
    return tuple(dict.fromkeys(defects))


class DeterministicWriterRuntime:
    def __init__(self, provider: EditorialProvider | None) -> None:
        self.provider = provider

    @property
    def producer_exact_id(self) -> str:
        return self.provider.producer_exact_id if self.provider is not None else "region-talk:writer@unattached"

    def execute(self, request: WriterInput) -> WriterResult:
        if request.source_profile.externality_status != "verified":
            return self._closed_result(request, "needs_source_profile", ("source_profile_not_verified",))
        if self.provider is None:
            raise HeavyRuntimeUnavailable("editorial provider is unattached")
        request_fp = canonical_sha256(
            {
                "schema_version": request.policy.stage_execution_version,
                "input_fingerprint": request.input_fingerprint,
                "model_id": request.policy.model_id,
                "fact_pack_sha256": request.fact_pack.fact_pack_sha256,
                "source_profile_fingerprint": request.source_profile.profile_fingerprint,
                "image_result_sha256": request.image_result_sha256,
                "final_result_sha256": request.final_result_sha256,
                "history": [item.draft_fingerprint for item in request.history],
            }
        )
        strategy_response = self.provider.strategy(request, request_fingerprint=request_fp)
        self._validate_provider_envelope(
            strategy_response.request_fingerprint,
            strategy_response.model_id,
            request_fp,
            request.policy.model_id,
        )
        strategy = strategy_response.strategy
        fact_ids = {fact.fact_id for fact in request.fact_pack.facts}
        if not set(strategy.current_hook_fact_ids) <= fact_ids or not set(strategy.source_value_fact_ids) <= fact_ids:
            raise ValueError("writer strategy references unknown facts")
        if not set(strategy.visual_hook_media_ids) <= set(request.image_result.selected_media_ids):
            raise ValueError("writer strategy references unselected media")
        draft_response = self.provider.write(request, strategy, request_fingerprint=request_fp)
        self._validate_provider_envelope(
            draft_response.request_fingerprint,
            draft_response.model_id,
            request_fp,
            request.policy.model_id,
        )
        draft = draft_response.draft
        defects = deterministic_draft_defects(draft, fact_ids)
        critic_response = self.provider.critic(request, strategy, draft, request_fingerprint=request_fp)
        self._validate_provider_envelope(
            critic_response.request_fingerprint,
            critic_response.model_id,
            request_fp,
            request.policy.model_id,
        )
        critic = critic_response.critic
        combined = tuple(dict.fromkeys((*defects, *critic.defects)))
        rewrite_count = 0
        if defects or critic.decision == "rewrite":
            rewrite_count = 1
            draft_response = self.provider.write(
                request,
                strategy,
                request_fingerprint=request_fp,
                defects=combined or ("critic_requested_rewrite",),
            )
            self._validate_provider_envelope(
                draft_response.request_fingerprint,
                draft_response.model_id,
                request_fp,
                request.policy.model_id,
            )
            draft = draft_response.draft
            defects = deterministic_draft_defects(draft, fact_ids)
            critic_response = self.provider.critic(request, strategy, draft, request_fingerprint=request_fp)
            self._validate_provider_envelope(
                critic_response.request_fingerprint,
                critic_response.model_id,
                request_fp,
                request.policy.model_id,
            )
            critic = critic_response.critic
            combined = tuple(dict.fromkeys((*defects, *critic.defects)))
        if defects or critic.decision != "pass":
            return self._closed_result(
                request,
                "rejected" if critic.decision == "reject" else "needs_grounding_review",
                combined or ("critic_did_not_pass",),
                request_fingerprint=request_fp,
                rewrite_count=rewrite_count,
            )
        return WriterResult.model_validate(
            _result_payload(
                {
                    "schema_version": "region-talk-writer-result.v1",
                    "input_fingerprint": request.input_fingerprint,
                    "candidate_revision_fingerprint": request.candidate_revision_fingerprint,
                    "fact_pack_sha256": request.fact_pack.fact_pack_sha256,
                    "source_profile_fingerprint": request.source_profile.profile_fingerprint,
                    "final_result_sha256": request.final_result_sha256,
                    "producer_exact_id": self.producer_exact_id,
                    "status": "ready_for_operator_review",
                    "title": draft.title,
                    "paragraph_one": draft.paragraph_one,
                    "paragraph_two": draft.paragraph_two,
                    "grounding": [item.model_dump(mode="json") for item in draft.grounding],
                    "strategy": strategy.model_dump(mode="json"),
                    "critic": critic.model_dump(mode="json"),
                    "rewrite_count": rewrite_count,
                    "request_fingerprint": request_fp,
                    "model_id": request.policy.model_id,
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
            )
        )

    @staticmethod
    def _validate_provider_envelope(
        observed_fp: str,
        observed_model: str,
        expected_fp: str,
        expected_model: str,
    ) -> None:
        if observed_fp != expected_fp or observed_model != expected_model:
            raise ValueError("editorial provider response differs from exact request")

    def _closed_result(
        self,
        request: WriterInput,
        status: str,
        reasons: tuple[str, ...],
        *,
        request_fingerprint: str | None = None,
        rewrite_count: int = 0,
    ) -> WriterResult:
        # Reasons are retained in the critic audit, never rendered as copy.
        critic = CriticResponse(decision="reject", defects=reasons)
        return WriterResult.model_validate(
            _result_payload(
                {
                    "schema_version": "region-talk-writer-result.v1",
                    "input_fingerprint": request.input_fingerprint,
                    "candidate_revision_fingerprint": request.candidate_revision_fingerprint,
                    "fact_pack_sha256": request.fact_pack.fact_pack_sha256,
                    "source_profile_fingerprint": request.source_profile.profile_fingerprint,
                    "final_result_sha256": request.final_result_sha256,
                    "producer_exact_id": self.producer_exact_id,
                    "status": status,
                    "title": "",
                    "paragraph_one": "",
                    "paragraph_two": "",
                    "grounding": [],
                    "strategy": None,
                    "critic": critic.model_dump(mode="json"),
                    "rewrite_count": rewrite_count,
                    "request_fingerprint": request_fingerprint
                    or canonical_sha256({"input_fingerprint": request.input_fingerprint, "closed_reason": reasons}),
                    "model_id": request.policy.model_id,
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
            )
        )


DONOR_PROVENANCE: Mapping[str, Mapping[str, str]] = {
    "image_scoring": {
        "revision": "5bbdb681623d5e4e0bff2133e487a6663c1a838a",
        "path": "kaggle/RegionTalkImageDiagnostic/region_talk_image_diagnostic.py",
        "sha256": "d5e3683f04bab191b05881e3167b61b4a27b64eeef92c112548d054cbb245162",
    },
    "final_verifier": {
        "revision": "5bbdb681623d5e4e0bff2133e487a6663c1a838a",
        "path": "scripts/region_talk_publication_finalizer.py",
        "sha256": "1cf78a6ff6b2df21475587a83de1b4c4790080f55b84491939f96e9e8ab901fe",
    },
    "writer": {
        "revision": "5bbdb681623d5e4e0bff2133e487a6663c1a838a",
        "path": "scripts/region_talk_publication_draft_backfill.py",
        "sha256": "426d44396b04d7bff677497663a632219139ccfbe3354f31e99f4ecf38e0452a",
    },
}


__all__ = [
    "DONOR_PROVENANCE",
    "DeterministicFinalVerifierRuntime",
    "DeterministicImageRuntime",
    "DeterministicWriterRuntime",
    "EditorialProvider",
    "FinalVerifierProvider",
    "ImageScoreEngine",
    "MediaArtifactReader",
    "VisualAdjudicator",
    "deterministic_draft_defects",
]
