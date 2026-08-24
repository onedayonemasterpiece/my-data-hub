# ruff: noqa: RUF001
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from my_data_hub.workloads.region_talk.heavy_assets import load_asset_manifest
from my_data_hub.workloads.region_talk.heavy_contracts import (
    CriticProviderResponse,
    CriticResponse,
    EditorialStrategy,
    EditorialStrategyResponse,
    FactPack,
    FinalVerifierInput,
    FinalVerifierResult,
    HeavyContractError,
    HeavyRuntimeUnavailable,
    ImageFrameScores,
    ImageScoringInput,
    ImageScoringResult,
    MediaAcquisitionReceipt,
    MediaArtifactManifest,
    SourceEvidence,
    SourceProfile,
    WriterDraft,
    WriterDraftResponse,
    WriterInput,
    canonical_sha256,
    sha256_text,
    validate_heavy_result_against_input,
    validate_heavy_stage_input,
    validate_heavy_stage_result,
)
from my_data_hub.workloads.region_talk.heavy_dag_bridge import (
    DagImageWorkInput,
    image_guard_metrics,
)
from my_data_hub.workloads.region_talk.heavy_runtimes import (
    DeterministicFinalVerifierRuntime,
    DeterministicImageRuntime,
    DeterministicWriterRuntime,
)
from my_data_hub.workloads.region_talk.heavy_wiring import (
    HeavyAttachedStageRuntime,
    HeavyStageInputReceipt,
    validate_heavy_private_result_contract,
)

ZERO = "0" * 64
REVISION = "1" * 64
MODEL_BUNDLE = "2" * 64


def _exact(model: type[Any], hash_field: str, value: dict[str, Any]) -> Any:
    value = dict(value)
    value[hash_field] = canonical_sha256(value)
    return model.model_validate(value)


def _heavy_exact(model: type[Any], value: dict[str, Any]) -> Any:
    value = dict(value)
    value["work_input_fingerprint"] = "3" * 64
    value["enrichment_sha256"] = canonical_sha256(value)
    value["input_fingerprint"] = canonical_sha256(value)
    return model.model_validate(value)


def _content() -> dict[str, Any]:
    body = "Проверенный региональный материал содержит факты и контекст."
    return {
        "title": "Проверенный материал",
        "summary": "Краткое содержание",
        "body_text": body,
        "text_sha256": sha256_text(body),
        "canonical_url": "https://publisher.example/articles/current",
        "canonical_source_key": "publisher:example",
        "content_type": "article",
    }


def _manifest(body: bytes = b"reviewed-image") -> MediaArtifactManifest:
    url = "https://media.example/images/current.jpg"
    asset_id = "22222222-2222-4222-8222-222222222222"
    receipt = _exact(
        MediaAcquisitionReceipt,
        "receipt_sha256",
        {
            "schema_version": "region-talk-media-artifact-acquisition-receipt.v2",
            "registered": True,
            "acquisition_id": "11111111-1111-5111-8111-111111111111",
            "task_run_id": "33333333-3333-4333-8333-333333333333",
            "export_batch_id": "44444444-4444-4444-8444-444444444444",
            "stage_run_id": "55555555-5555-4555-8555-555555555555",
            "canonical_revision": 1,
            "master_instance_id": "66666666-6666-4666-8666-666666666666",
            "epoch": 1,
            "candidate_id": "77777777-7777-4777-8777-777777777777",
            "candidate_revision": 1,
            "candidate_revision_fingerprint": REVISION,
            "content_id": "88888888-8888-4888-8888-888888888888",
            "asset_id": asset_id,
            "source_media_id": "media-1",
            "normalized_source_url": url,
            "source_url_sha256": sha256_text(url),
            "object_ref": "task-1/media-1.jpg",
            "artifact_sha256": hashlib.sha256(body).hexdigest(),
            "byte_size": len(body),
            "content_type": "image/jpeg",
            "width": 640,
            "height": 480,
            "acquisition_evidence_sha256": "a" * 64,
            "legacy_receipt_sha256": "b" * 64,
            "task_readable": True,
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    )
    return _exact(
        MediaArtifactManifest,
        "manifest_sha256",
        {
            "schema_version": "region-talk-media-artifact-manifest.v1",
            "candidate_revision_fingerprint": REVISION,
            "acquisition_receipts": [receipt.model_dump(mode="json")],
            "items": [
                {
                    "asset_id": asset_id,
                    "source_media_id": "media-1",
                    "normalized_source_url": url,
                    "source_url_sha256": sha256_text(url),
                    "object_ref": "task-1/media-1.jpg",
                    "artifact_sha256": hashlib.sha256(body).hexdigest(),
                    "byte_size": len(body),
                    "content_type": "image/jpeg",
                    "width": 640,
                    "height": 480,
                }
            ],
        },
    )


def image_input(*, body: bytes = b"reviewed-image") -> ImageScoringInput:
    return _heavy_exact(
        ImageScoringInput,
        {
            "schema_version": "region-talk-image-input.v1",
            "candidate_revision_fingerprint": REVISION,
            "content": _content(),
            "eligibility_fingerprint": "4" * 64,
            "availability": "AVAILABLE",
            "unavailable_reason": "",
            "artifact_manifest": _manifest(body).model_dump(mode="json"),
            "policy": {
                "decision_contract_version": "region_talk_article_image_association_v4",
                "acquisition_version": "region_talk_http_article_image_evidence_v4",
                "scorer_version": "region_talk_cv_clip_laion_nima_legacy_v1",
                "vlm_prompt_version": "region_talk_visual_article_association_v3",
                "model_bundle_sha256": MODEL_BUNDLE,
                "vlm_model_id": "reviewed-vlm@2026-08",
            },
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    )


def image_result(request: ImageScoringInput | None = None) -> ImageScoringResult:
    request = request or image_input()
    assert request.artifact_manifest is not None
    return _exact(
        ImageScoringResult,
        "result_sha256",
        {
            "schema_version": "region-talk-image-scoring-result.v1",
            "input_fingerprint": request.input_fingerprint,
            "candidate_revision_fingerprint": REVISION,
            "media_manifest_sha256": request.artifact_manifest.manifest_sha256,
            "producer_exact_id": "offline-scorer@sha256:" + "5" * 64,
            "decision": "legacy_auto_accept",
            "reason_codes": ["legacy_anchor_passed"],
            "frames": [
                {
                    "media_id": "media-1",
                    "artifact_sha256": request.artifact_manifest.items[0].artifact_sha256,
                    "content_text_sha256": request.content.text_sha256,
                    "scorer_request_fingerprint": "f" * 64,
                    "cv_overall_media_score": 0.72,
                    "technical_quality_score": 0.74,
                    "clip_visual_fit_score": 0.71,
                    "laion_aesthetic_score": 0.64,
                    "nima_quality_score": 0.68,
                    "overall_media_score": 0.7,
                    "model_bundle_sha256": MODEL_BUNDLE,
                }
            ],
            "selected_media_ids": ["media-1"],
            "visual_adjudication": None,
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    )


def _fact_pack() -> FactPack:
    excerpt = "Организаторы подтвердили дату открытия площадки."
    return _exact(
        FactPack,
        "fact_pack_sha256",
        {
            "schema_version": "region-talk-fact-pack.v1",
            "candidate_revision_fingerprint": REVISION,
            "facts": [
                {
                    "fact_id": "fact-1",
                    "claim": "Площадка откроется в заявленную дату.",
                    "support_excerpt": excerpt,
                    "source_url": "https://publisher.example/articles/current",
                    "support_sha256": sha256_text(excerpt),
                }
            ],
        },
    )


def _source() -> SourceEvidence:
    return _exact(
        SourceEvidence,
        "source_fingerprint",
        {
            "candidate_revision_fingerprint": REVISION,
            "canonical_source_key": "publisher:example",
            "externality_status": "verified",
            "source_scope": "external",
        },
    )


def final_input() -> FinalVerifierInput:
    image = image_result()
    facts = _fact_pack()
    source = _source()
    return _heavy_exact(
        FinalVerifierInput,
        {
            "schema_version": "region-talk-final-verifier-input.v1",
            "candidate_revision_fingerprint": REVISION,
            "content": _content(),
            "fact_pack": facts.model_dump(mode="json"),
            "source": source.model_dump(mode="json"),
            "vector_result_sha256": "6" * 64,
            "image_result_sha256": image.result_sha256,
            "image_result": image.model_dump(mode="json"),
            "upstream_results": [
                {
                    "stage": "vector_fusion",
                    "input_fingerprint": "7" * 64,
                    "result_sha256": "6" * 64,
                    "result_metadata_sha256": "8" * 64,
                },
                {
                    "stage": "image_scoring",
                    "input_fingerprint": image.input_fingerprint,
                    "result_sha256": image.result_sha256,
                    "result_metadata_sha256": "9" * 64,
                },
            ],
            "policy": {
                "eligibility_gate_version": "region_talk_publication_eligibility_v5",
                "prompt_version": "region_talk_final_verifier_v7_grounded_draft",
                "model_id": "reviewed-verifier@2026-08",
            },
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    )


def final_result(request: FinalVerifierInput | None = None) -> FinalVerifierResult:
    request = request or final_input()
    return _exact(
        FinalVerifierResult,
        "result_sha256",
        {
            "schema_version": "region-talk-final-verifier-result.v1",
            "input_fingerprint": request.input_fingerprint,
            "candidate_revision_fingerprint": REVISION,
            "fact_pack_sha256": request.fact_pack.fact_pack_sha256,
            "source_fingerprint": request.source.source_fingerprint,
            "image_result_sha256": request.image_result_sha256,
            "producer_exact_id": "reviewed-verifier@sha256:" + "a" * 64,
            "decision": "accept",
            "reason_codes": ["grounding_current"],
            "grounding": [{"claim": "Площадка откроется.", "fact_ids": ["fact-1"]}],
            "request_fingerprint": "b" * 64,
            "model_id": request.policy.model_id,
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    )


def _source_profile() -> SourceProfile:
    return _exact(
        SourceProfile,
        "profile_fingerprint",
        {
            "candidate_revision_fingerprint": REVISION,
            "canonical_source_key": "publisher:example",
            "source_fingerprint": _source().source_fingerprint,
            "entity_type": "media_brand",
            "externality_status": "verified",
            "dimensions": {
                "publisher_identity": "Региональное независимое медиа",
                "intended_audience": "Жители региона",
                "distinctive_value": "Подтвержденные локальные новости",
            },
        },
    )


def writer_input() -> WriterInput:
    final_request = final_input()
    image = final_request.image_result
    verified = final_result(final_request)
    facts = final_request.fact_pack
    return _heavy_exact(
        WriterInput,
        {
            "schema_version": "region-talk-writer-input.v1",
            "candidate_revision_fingerprint": REVISION,
            "content": _content(),
            "fact_pack": facts.model_dump(mode="json"),
            "source_profile": _source_profile().model_dump(mode="json"),
            "image_result_sha256": image.result_sha256,
            "final_result_sha256": verified.result_sha256,
            "image_result": image.model_dump(mode="json"),
            "final_result": verified.model_dump(mode="json"),
            "upstream_results": [
                {
                    "stage": "image_scoring",
                    "input_fingerprint": image.input_fingerprint,
                    "result_sha256": image.result_sha256,
                    "result_metadata_sha256": "c" * 64,
                },
                {
                    "stage": "final_verifier",
                    "input_fingerprint": verified.input_fingerprint,
                    "result_sha256": verified.result_sha256,
                    "result_metadata_sha256": "d" * 64,
                },
            ],
            "history": [],
            "policy": {
                "writer_version": "region_talk_editorial_writer_v12_publisher_reader_brief",
                "output_contract": "region_talk_editorial_output_v6_publisher_reader_brief",
                "input_contract": "region_talk_editorial_input_v3_source_profile",
                "stage_execution_version": "region_talk_writer_v12_publisher_reader_brief_v2",
                "media_materialization_contract": "region_talk_media_materialization_v1",
                "model_id": "reviewed-writer@2026-08",
            },
            "publication_dispatch": False,
            "notification_dispatch": False,
        },
    )


@pytest.mark.parametrize(
    ("url", "object_ref"),
    [
        ("http://media.example/x.jpg", "task/x.jpg"),
        ("https://127.0.0.1/x.jpg", "task/x.jpg"),
        ("https://user:pass@media.example/x.jpg", "task/x.jpg"),
        ("https://media.example/x.jpg", "../x.jpg"),
    ],
)
def test_image_input_rejects_ssrf_or_unsafe_object_ref(url: str, object_ref: str) -> None:
    manifest = _manifest().model_dump(mode="json")
    manifest["items"][0]["normalized_source_url"] = url
    manifest["items"][0]["source_url_sha256"] = sha256_text(url)
    manifest["items"][0]["object_ref"] = object_ref
    manifest["manifest_sha256"] = canonical_sha256(
        {key: value for key, value in manifest.items() if key != "manifest_sha256"}
    )
    with pytest.raises(ValidationError):
        MediaArtifactManifest.model_validate(manifest)


def test_input_contracts_reject_stale_evidence_and_dispatch() -> None:
    value = final_input().model_dump(mode="json")
    value["fact_pack"]["candidate_revision_fingerprint"] = "e" * 64
    value["fact_pack"]["fact_pack_sha256"] = canonical_sha256(
        {key: item for key, item in value["fact_pack"].items() if key != "fact_pack_sha256"}
    )
    value["input_fingerprint"] = canonical_sha256(
        {key: item for key, item in value.items() if key != "input_fingerprint"}
    )
    with pytest.raises(ValidationError, match="fact pack is stale"):
        FinalVerifierInput.model_validate(value)

    source_value = final_input().model_dump(mode="json")
    source_value["source"]["candidate_revision_fingerprint"] = "e" * 64
    source_value["source"]["source_fingerprint"] = canonical_sha256(
        {key: item for key, item in source_value["source"].items() if key != "source_fingerprint"}
    )
    source_value["input_fingerprint"] = canonical_sha256(
        {key: item for key, item in source_value.items() if key != "input_fingerprint"}
    )
    with pytest.raises(ValidationError, match="source evidence is stale"):
        FinalVerifierInput.model_validate(source_value)

    media_value = image_input().model_dump(mode="json")
    media_value["artifact_manifest"]["candidate_revision_fingerprint"] = "e" * 64
    media_value["artifact_manifest"]["manifest_sha256"] = canonical_sha256(
        {
            key: item
            for key, item in media_value["artifact_manifest"].items()
            if key != "manifest_sha256"
        }
    )
    media_value["input_fingerprint"] = canonical_sha256(
        {key: item for key, item in media_value.items() if key != "input_fingerprint"}
    )
    with pytest.raises(ValidationError, match="immutable acquisition receipt"):
        ImageScoringInput.model_validate(media_value)

    writer = writer_input().model_dump(mode="json")
    writer["publication_dispatch"] = True
    writer["input_fingerprint"] = canonical_sha256(
        {key: item for key, item in writer.items() if key != "input_fingerprint"}
    )
    with pytest.raises(ValidationError):
        WriterInput.model_validate(writer)


def test_result_hash_and_stage_are_not_trusted() -> None:
    result = image_result()
    value = result.model_dump(mode="json")
    value["frames"][0]["overall_media_score"] = 1.0
    with pytest.raises(ValidationError, match="result_sha256 differs"):
        ImageScoringResult.model_validate(value)
    with pytest.raises(HeavyContractError, match="schema differs"):
        validate_heavy_stage_result(
            "writer",
            result.model_dump(mode="json"),
            expected_input_fingerprint=result.input_fingerprint,
        )
    with pytest.raises(HeavyContractError, match="work input fingerprint"):
        validate_heavy_stage_input(
            "image_scoring",
            image_input().model_dump(mode="json"),
            expected_work_input_fingerprint=ZERO,
        )

    rebound = result.model_dump(mode="json")
    rebound["frames"][0]["artifact_sha256"] = "e" * 64
    rebound["result_sha256"] = canonical_sha256(
        {key: item for key, item in rebound.items() if key != "result_sha256"}
    )
    with pytest.raises(HeavyContractError, match="score provenance"):
        validate_heavy_result_against_input(image_input(), ImageScoringResult.model_validate(rebound))


def test_offline_asset_manifest_is_honestly_not_production_ready() -> None:
    root = Path(__file__).parents[2]
    path = root / "src/my_data_hub/workloads/region_talk/assets/heavy-runtime-assets.v1.json"
    manifest = load_asset_manifest(path)
    assert manifest.production_ready is False
    assert any(item.status == "unresolved" for item in manifest.models)
    with pytest.raises(HeavyRuntimeUnavailable, match="not production-ready"):
        manifest.require_production_ready()


def test_shadow_fixture_is_canonical_and_bound_to_manifest() -> None:
    fixture = Path(__file__).with_name("fixtures") / "heavy_image_donor_shadow.v1.json"
    raw = fixture.read_bytes()
    value = json.loads(raw)
    assert json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode() == raw
    root = Path(__file__).parents[2]
    manifest_path = root / "src/my_data_hub/workloads/region_talk/assets/heavy-runtime-assets.v1.json"
    manifest = load_asset_manifest(manifest_path)
    assert hashlib.sha256(raw).hexdigest() == manifest.shadow_fixture_sha256


class _Reader:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.authorized_receipt_sha256s = frozenset({_manifest(body).acquisition_receipts[0].receipt_sha256})

    def read(self, object_ref: str, *, maximum_bytes: int) -> bytes:
        assert object_ref == "task-1/media-1.jpg"
        assert maximum_bytes == len(self.body)
        return self.body


class _Scorer:
    producer_exact_id = "offline-scorer@sha256:" + "5" * 64
    model_bundle_sha256 = MODEL_BUNDLE

    def __init__(self, scores: dict[str, float]) -> None:
        self.scores = scores

    def score(
        self,
        artifact: Any,
        body: bytes,
        *,
        content_text: str,
        request_fingerprint: str,
    ) -> ImageFrameScores:
        return ImageFrameScores(
            media_id=artifact.source_media_id,
            artifact_sha256=hashlib.sha256(body).hexdigest(),
            content_text_sha256=sha256_text(content_text),
            scorer_request_fingerprint=request_fingerprint,
            model_bundle_sha256=self.model_bundle_sha256,
            **self.scores,
        )


def test_donor_shadow_low_scores_abstain_instead_of_terminal_reject() -> None:
    fixture = Path(__file__).with_name("fixtures") / "heavy_image_donor_shadow.v1.json"
    cases = json.loads(fixture.read_bytes())["cases"]
    body = b"reviewed-image"
    for case in cases:
        scores = {key: value for key, value in case.items() if key != "case_id"}
        result = DeterministicImageRuntime(
            reader=_Reader(body),
            scorer=_Scorer(scores),
        ).execute(image_input(body=body))
        assert result.decision == "needs_visual_review", case["case_id"]
        assert result.selected_media_ids == ()
        assert result.publication_dispatch is False
        assert result.notification_dispatch is False


def test_image_runtime_is_retryable_without_exact_private_capabilities() -> None:
    with pytest.raises(HeavyRuntimeUnavailable) as caught:
        DeterministicImageRuntime(reader=None, scorer=None).execute(image_input())
    assert caught.value.retryable is True

    reader = _Reader(b"reviewed-image")
    reader.authorized_receipt_sha256s = frozenset()
    with pytest.raises(HeavyRuntimeUnavailable, match="immutable acquisition receipt"):
        DeterministicImageRuntime(reader=reader, scorer=_Scorer({
            "cv_overall_media_score": 0.7,
            "technical_quality_score": 0.7,
            "clip_visual_fit_score": 0.7,
            "laion_aesthetic_score": 0.7,
            "nima_quality_score": 0.7,
            "overall_media_score": 0.7,
        })).execute(image_input())


class _Verifier:
    producer_exact_id = "reviewed-verifier@sha256:" + "a" * 64

    def verify(self, request: FinalVerifierInput, *, request_fingerprint: str) -> Any:
        from my_data_hub.workloads.region_talk.heavy_contracts import FinalVerifierProviderResponse

        return FinalVerifierProviderResponse(
            decision="accept",
            reason_codes=("grounding_current",),
            grounding=({"claim": "Площадка откроется.", "fact_ids": ("fact-1",)},),
            request_fingerprint=request_fingerprint,
            model_id=request.policy.model_id,
        )


def test_final_verifier_requires_provider_and_returns_current_grounding() -> None:
    request = final_input()
    with pytest.raises(HeavyRuntimeUnavailable):
        DeterministicFinalVerifierRuntime(None).execute(request)
    result = DeterministicFinalVerifierRuntime(_Verifier()).execute(request)
    assert result.decision == "accept"
    assert result.grounding[0].fact_ids == ("fact-1",)
    assert result.publication_dispatch is False


class _Writer:
    producer_exact_id = "reviewed-writer@sha256:" + "e" * 64

    def __init__(self) -> None:
        self.writes = 0

    @staticmethod
    def _envelope(value: Any, request: WriterInput, request_fingerprint: str, wrapper: type[Any]) -> Any:
        field = "strategy" if wrapper is EditorialStrategyResponse else "draft"
        return wrapper.model_validate(
            {
                field: value.model_dump(mode="json"),
                "request_fingerprint": request_fingerprint,
                "model_id": request.policy.model_id,
            }
        )

    def strategy(self, request: WriterInput, *, request_fingerprint: str) -> EditorialStrategyResponse:
        strategy = EditorialStrategy(
            angle="Практическая польза новости для жителей",
            current_hook_fact_ids=("fact-1",),
            source_value_fact_ids=("fact-1",),
            visual_hook_media_ids=("media-1",),
        )
        return self._envelope(strategy, request, request_fingerprint, EditorialStrategyResponse)

    def write(
        self,
        request: WriterInput,
        strategy: EditorialStrategy,
        *,
        request_fingerprint: str,
        defects: tuple[str, ...] = (),
    ) -> WriterDraftResponse:
        self.writes += 1
        if self.writes == 1:
            paragraph_one = "Слишком коротко."
            paragraph_two = "Тоже коротко."
        else:
            paragraph_one = (
                "Региональное медиа подтвердило дату открытия новой площадки и сверило сведения с официальным "
                "сообщением организаторов. Для жителей это означает понятный ориентир при планировании визита и "
                "возможность заранее проверить программу события."
            )
            paragraph_two = (
                "Материал издателя объясняет локальный контекст, перечисляет подтвержденные детали и помогает "
                "читателю отделить актуальные сведения от предварительных анонсов без рекламных обещаний."
            )
        draft = WriterDraft(
            title="Что изменит открытие новой региональной площадки",
            paragraph_one=paragraph_one,
            paragraph_two=paragraph_two,
            grounding=({"claim": "Площадка откроется.", "fact_ids": ("fact-1",)},),
        )
        return self._envelope(draft, request, request_fingerprint, WriterDraftResponse)

    def critic(
        self,
        request: WriterInput,
        strategy: EditorialStrategy,
        draft: WriterDraft,
        *,
        request_fingerprint: str,
    ) -> CriticProviderResponse:
        critic = CriticResponse(
            decision="rewrite" if self.writes == 1 else "pass",
            defects=("copy_too_short",) if self.writes == 1 else (),
        )
        return CriticProviderResponse(
            critic=critic,
            request_fingerprint=request_fingerprint,
            model_id=request.policy.model_id,
        )


def test_writer_allows_one_rewrite_and_never_dispatches() -> None:
    provider = _Writer()
    result = DeterministicWriterRuntime(provider).execute(writer_input())
    assert provider.writes == 2
    assert result.status == "ready_for_operator_review"
    assert result.rewrite_count == 1
    assert result.publication_dispatch is False
    assert result.notification_dispatch is False


def test_writer_without_provider_is_retryable() -> None:
    with pytest.raises(HeavyRuntimeUnavailable):
        DeterministicWriterRuntime(None).execute(writer_input())


def test_0030_image_guard_metrics_are_derived_not_provider_supplied() -> None:
    request = image_input()
    result = image_result(request)
    assert request.artifact_manifest is not None
    pin = {
        "schema_version": "region-talk-stage-runtime-pin-receipt.v1",
        "registered": True,
        "stage": "image_scoring",
        "contract_version": "region-talk.image-diagnostic.v1",
        "effective_canonical_revision": 1,
        "pin_generation": 1,
        "master_instance_id": "11111111-1111-4111-8111-111111111111",
        "epoch": 1,
        "model_id": "reviewed-image-model",
        "model_revision": "1" * 40,
        "encoder_contract": "region-talk-image-encoder.v1",
        "semantic_bank_version": None,
        "semantic_bank_sha256": None,
        "runtime_source_sha256": "2" * 64,
        "asset_manifest_sha256": MODEL_BUNDLE,
        "provider_image_identity": "kaggle-image@sha256:" + "3" * 64,
        "provider_image_source_commit": "4" * 40,
        "producer_exact_id": result.producer_exact_id,
        "prior_pin_receipt_sha256": None,
        "pin_sha256": "5" * 64,
        "publication_dispatch": False,
        "notification_dispatch": False,
        "receipt_sha256": "6" * 64,
    }
    artifact = request.artifact_manifest.items[0]
    legacy_receipt = request.artifact_manifest.acquisition_receipts[0].model_dump(mode="json")
    legacy_receipt["schema_version"] = "region-talk-media-artifact-acquisition-receipt.v1"
    legacy_receipt.pop("legacy_receipt_sha256")
    # Migration 0031 bound this value to the raw source URL.  The sparse bridge
    # treats it as opaque legacy authority; 0032 supplies normalized v2 authority.
    legacy_receipt["source_url_sha256"] = "e" * 64
    legacy_receipt["receipt_sha256"] = canonical_sha256(
        {key: value for key, value in legacy_receipt.items() if key != "receipt_sha256"}
    )
    work = DagImageWorkInput.model_validate(
        {
            "schema_version": "region-talk-image-input.v1",
            "availability": "AVAILABLE",
            "asset_id": "22222222-2222-4222-8222-222222222222",
            "source_media_id": artifact.source_media_id,
            "normalized_source_url": artifact.normalized_source_url,
            "object_ref": artifact.object_ref,
            "artifact_sha256": artifact.artifact_sha256,
            "byte_size": artifact.byte_size,
            "content_type": artifact.content_type,
            "acquisition_receipt": legacy_receipt,
            "acquisition_receipt_sha256": legacy_receipt["receipt_sha256"],
            "candidate_revision": 1,
            "runtime_pin": pin,
        }
    )
    metrics = image_guard_metrics(result, work)
    assert metrics["decision"] == "accept"
    assert metrics["postcard_score"] == 0.7
    assert set(metrics) == {
        "schema_version",
        "decision",
        "actual_image",
        "postcard_score",
        "input_artifact_sha256",
        "model_id",
        "model_revision",
        "encoder_contract",
        "asset_manifest_sha256",
        "runtime_source_sha256",
        "provider_image_identity",
        "provider_image_source_commit",
        "pin_sha256",
    }

    envelope_base = {
        "schema_version": "region-talk-heavy-stage-input-receipt.v1",
        "status": "READY",
        "stage": "image_scoring",
        "work_input_fingerprint": request.work_input_fingerprint,
        "enrichment_sha256": request.enrichment_sha256,
        "dag_input": work.model_dump(mode="json"),
        "heavy_input": request.model_dump(mode="json"),
        "reason_code": "",
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    envelope = HeavyStageInputReceipt.model_validate(
        {**envelope_base, "receipt_sha256": canonical_sha256(envelope_base)}
    )
    execution = HeavyAttachedStageRuntime(
        stage="image_scoring",
        runtime=DeterministicImageRuntime(
            reader=_Reader(b"reviewed-image"),
            scorer=_Scorer(
                {
                    "cv_overall_media_score": 0.72,
                    "technical_quality_score": 0.74,
                    "clip_visual_fit_score": 0.71,
                    "laion_aesthetic_score": 0.64,
                    "nima_quality_score": 0.68,
                    "overall_media_score": 0.7,
                }
            ),
        ),
    ).execute(
        stage="image_scoring",
        input_fingerprint=request.work_input_fingerprint,
        payload=type("Payload", (), {"input_data": envelope.model_dump(mode="json")})(),
    )
    assert execution.metrics == metrics
    assert execution.artifact_sha256 == execution.private_result.result_sha256
    assert execution.private_result.enrichment_sha256 != execution.private_result.work_input_fingerprint

    metadata = {
        "input_fingerprint": request.work_input_fingerprint,
        "producer_exact_id": execution.private_result.result_data["producer_exact_id"],
        "artifact_sha256": execution.private_result.result_sha256,
        "metrics": metrics,
    }
    private = execution.private_result.model_dump(mode="json")
    assert validate_heavy_private_result_contract(
        stage="image_scoring",
        value=private,
        direct_result_sha256=execution.private_result.result_sha256,
        result_metadata=metadata,
    ).result_sha256 == execution.private_result.result_sha256

    minimal_base = {"input_fingerprint": result.input_fingerprint}
    minimal = {**minimal_base, "result_sha256": canonical_sha256(minimal_base)}
    fabricated = {
        **private,
        "result_sha256": minimal["result_sha256"],
        "result_data": minimal,
    }
    with pytest.raises(ValidationError):
        validate_heavy_private_result_contract(
            stage="image_scoring",
            value=fabricated,
            direct_result_sha256=minimal["result_sha256"],
            result_metadata={**metadata, "artifact_sha256": minimal["result_sha256"]},
        )

    with pytest.raises(ValueError, match="guard metric differs"):
        validate_heavy_private_result_contract(
            stage="image_scoring",
            value=private,
            direct_result_sha256=execution.private_result.result_sha256,
            result_metadata={**metadata, "metrics": {**metrics, "actual_image": False}},
        )
