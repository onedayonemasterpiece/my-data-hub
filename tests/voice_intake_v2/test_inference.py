from __future__ import annotations

import base64
import hashlib
import json
from typing import Any

import pytest
from pydantic import ValidationError, create_model

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight, ModelLimit
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.http import BoundedHTTPError, BoundedHTTPResponse
from my_data_hub.voice_intake_v2.contracts import StatusResponse
from my_data_hub.voice_intake_v2.inference import (
    SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS,
    AggregateGeminiInference,
)
from my_data_hub.voice_intake_v2.worker import StageFailure

from .conftest import summary_value


class Limiter:
    def __init__(self, *, deny: bool = False) -> None:
        self.deny = deny
        self.reserves: list[dict[str, Any]] = []
        self.finalized: list[tuple[LimiterLease, dict[str, Any]]] = []
        self.sent: list[str] = []
        self.provider_429: list[tuple[str, int | None]] = []

    async def preflight(self, model: str) -> LimiterPreflight:
        return LimiterPreflight(
            limit=ModelLimit(model=model, rpm=10, tpm=1_000_000, rpd=100, tpm_reserve_extra=1000),
            candidate_key_ids=("id",),
            candidate_env_names=frozenset({"KEY"}),
            contract="google_ai_project_model_atomic_v1",
            bucket_strategy="rolling_60s_pacific_day_v2",
        )

    async def reserve_generate_content(self, **kwargs: Any) -> LimiterLease:
        if self.deny:
            raise GoogleAIError(
                GoogleAIErrorCode.QUOTA_EXHAUSTED_RPD,
                retryable=True,
                retry_after_ms=1000,
            )
        self.reserves.append(kwargs)
        return LimiterLease(
            request_uid=kwargs["request_uid"],
            attempt_no=1,
            api_key_id="id",
            env_var_name="KEY",
            key_alias="alias",
            quota_scope="scope",
            reserved_tpm=kwargs["reserved_tpm"],
            contract="contract",
            bucket_strategy="bucket",
        )

    def secret_for(self, _lease: LimiterLease) -> str:
        return "secret"

    async def mark_sent(self, lease: LimiterLease) -> None:
        self.sent.append(lease.request_uid)

    async def release_unsent(self, _lease: LimiterLease, *, reason: str) -> None:
        return None

    async def report_provider_429(self, lease: LimiterLease, *, retry_after_ms: int | None) -> None:
        self.provider_429.append((lease.request_uid, retry_after_ms))

    async def finalize_generate_content(self, lease: LimiterLease, **kwargs: Any) -> None:
        self.finalized.append((lease, kwargs))

    @staticmethod
    def public_lease(lease: LimiterLease, *, actual_tpm: int | None) -> dict[str, int | None]:
        return {"reserved_tpm": lease.reserved_tpm, "actual_tpm": actual_tpm}


class Requester:
    def __init__(self, values: list[dict[str, Any]] | None = None) -> None:
        self.calls: list[tuple[str, str, dict[str, Any]]] = []
        self.values = list(values or [])

    async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
        self.calls.append((method, url, kwargs))
        if self.values:
            body = self.values.pop(0)
        elif len(self.calls) == 1:
            body = _segment_value(0, 20_000)
        else:
            body = summary_value()
        return _response(body)


def _segment_value(start_ms: int, end_ms: int, *, words: int = 20) -> dict[str, Any]:
    return {
        "transcript": " ".join(["тестовое"] * words),
        "language": "ru-RU",
        "uncertain_fragments": [],
        "coverage_start_ms": start_ms,
        "coverage_end_ms": end_ms,
    }


def _response(
    value: dict[str, Any] | str,
    *,
    finish_reason: str | None = "STOP",
    candidates: int = 1,
) -> BoundedHTTPResponse:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    generated_candidates: list[dict[str, Any]] = []
    for _ in range(candidates):
        candidate: dict[str, Any] = {"content": {"parts": [{"text": text}]}}
        if finish_reason is not None:
            candidate["finishReason"] = finish_reason
        generated_candidates.append(candidate)
    return BoundedHTTPResponse(
        status=200,
        json_body={
            "candidates": generated_candidates,
            "usageMetadata": {
                "promptTokenCount": 10,
                "candidatesTokenCount": 20,
                "thoughtsTokenCount": 0,
                "totalTokenCount": 30,
            },
        },
        retry_after=None,
        content_type="application/json",
    )


def _source(tmp_path: Any, content: bytes = b"synthetic-m4a-segment") -> tuple[Any, Any, str]:
    source = tmp_path / "chunk-00000.m4a"
    source.write_bytes(content)
    normalized = tmp_path / "chunk-00000.mp3"
    normalized.write_bytes(b"synthetic-normalized-mp3")
    return normalized, source, hashlib.sha256(content).hexdigest()


async def _transcribe(
    service: AggregateGeminiInference,
    audio: Any,
    source: Any,
    source_sha256: str,
    terminology: dict[str, Any],
    *,
    start_ms: int = 0,
    end_ms: int = 20_000,
    expected_speech_ms: int | None = None,
):
    return await service.transcribe_segment(
        audio_path=audio,
        source_path=source,
        chunk_index=0,
        source_sha256=source_sha256,
        source_audio_start_ms=start_ms,
        source_audio_end_ms=end_ms,
        expected_speech_ms=expected_speech_ms or end_ms - start_ms,
        terminology=terminology,
    )


@pytest.mark.asyncio
async def test_segment_then_summary_make_one_physical_post_each(tmp_path, auth_settings, terminology):
    audio, source, source_sha256 = _source(tmp_path)
    limiter, requester = Limiter(), Requester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    transcript = await _transcribe(service, audio, source, source_sha256, terminology)
    summary = await service.summarize(transcript=transcript.value, terminology=terminology)

    assert len(requester.calls) == 2
    assert all(call[0] == "POST" and call[1].endswith(":generateContent") for call in requester.calls)
    assert all("countTokens" not in call[1] for call in requester.calls)
    first_parts = requester.calls[0][2]["json_body"]["contents"][0]["parts"]
    assert first_parts[1]["inlineData"]["mimeType"] == "audio/mpeg"
    assert base64.b64decode(first_parts[1]["inlineData"]["data"]) == b"synthetic-normalized-mp3"
    assert transcript.source_sha256 == source_sha256
    assert transcript.input_audio_sha256 == hashlib.sha256(b"synthetic-normalized-mp3").hexdigest()
    assert transcript.input_audio_mime_type == "audio/mpeg"
    assert transcript.coverage_start_ms == 0 and transcript.coverage_end_ms == 20_000
    assert transcript.coverage_ms == 20_000 and transcript.coverage_ratio == 1.0
    assert transcript.finish_reason == "STOP"
    assert transcript.usage.total_tokens == 30
    assert transcript.request_uid != summary.request_uid
    assert len(limiter.finalized) == 2


@pytest.mark.asyncio
async def test_finalizer_failure_preserves_observed_provider_finish_reason(
    tmp_path, auth_settings, terminology
):
    class FinalizerFails(Limiter):
        async def finalize_generate_content(self, lease: LimiterLease, **kwargs: Any) -> None:
            raise RuntimeError("synthetic finalizer failure")

    audio, source, source_sha256 = _source(tmp_path)
    service = AggregateGeminiInference(
        auth_settings, limiter=FinalizerFails(), requester=Requester()
    )
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert raised.value.code == "limiter_finalization_failed"
    assert raised.value.sent and raised.value.ambiguous
    assert raised.value.finish_reason == "STOP"
    assert raised.value.provider_request_uid


@pytest.mark.asyncio
async def test_quota_denial_before_send_makes_zero_physical_posts(tmp_path, auth_settings, terminology):
    audio, source, source_sha256 = _source(tmp_path)
    requester = Requester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(deny=True), requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert not raised.value.sent and raised.value.retryable
    assert requester.calls == []


@pytest.mark.asyncio
async def test_provider_429_makes_one_physical_post_and_no_hidden_retry(tmp_path, auth_settings, terminology):
    class TooManyRequests(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            return BoundedHTTPResponse(429, {}, "1", "application/json")

    audio, source, source_sha256 = _source(tmp_path)
    limiter, requester = Limiter(), TooManyRequests()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert raised.value.code == "provider_429"
    assert raised.value.sent and raised.value.retryable and not raised.value.ambiguous
    assert len(requester.calls) == 1
    assert len(limiter.sent) == 1 and len(limiter.provider_429) == 1
    assert len(limiter.finalized) == 1


@pytest.mark.asyncio
async def test_provider_timeout_makes_one_physical_post_and_fences_ambiguity(tmp_path, auth_settings, terminology):
    class TimeoutRequester(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            raise BoundedHTTPError("total_timeout")

    audio, source, source_sha256 = _source(tmp_path)
    limiter, requester = Limiter(), TimeoutRequester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert raised.value.code == "provider_timeout"
    assert raised.value.sent and raised.value.ambiguous and not raised.value.retryable
    assert len(requester.calls) == 1
    assert len(limiter.sent) == 1 and len(limiter.finalized) == 1


@pytest.mark.asyncio
async def test_source_hash_is_verified_before_provider_send(tmp_path, auth_settings, terminology):
    audio, source, _ = _source(tmp_path)
    requester = Requester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(), requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, "f" * 64, terminology)
    assert raised.value.code == "segment_source_hash_mismatch"
    assert not raised.value.sent
    assert requester.calls == []


@pytest.mark.asyncio
async def test_bounded_segment_reserves_audio_output_and_limiter_headroom(tmp_path, auth_settings, terminology):
    audio, source, source_sha256 = _source(tmp_path)
    limiter, requester = Limiter(), Requester([_segment_value(60_000, 240_000, words=80)])
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    receipt = await _transcribe(
        service,
        audio,
        source,
        source_sha256,
        terminology,
        start_ms=60_000,
        end_ms=240_000,
        expected_speech_ms=180_000,
    )

    assert len(requester.calls) == 1
    generation = requester.calls[0][2]["json_body"]["generationConfig"]
    assert generation["maxOutputTokens"] == SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS
    assert limiter.reserves[0]["reserved_tpm"] == 180 * 32 + 16_384 + 1000
    assert receipt.plausibility.expected_speech_ms == 180_000
    assert (
        receipt.transcript_receipt_sha256
        == hashlib.sha256(
            json.dumps(
                {
                    "schema_version": "2.0.0",
                    "chunk_index": 0,
                    "source_sha256": source_sha256,
                    "input_audio_sha256": hashlib.sha256(b"synthetic-normalized-mp3").hexdigest(),
                    "input_audio_mime_type": "audio/mpeg",
                    "source_audio_start_ms": 60_000,
                    "source_audio_end_ms": 240_000,
                    "value": receipt.value,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "raw_response",
    [
        json.dumps(_segment_value(0, 240_000, words=100), ensure_ascii=False),
        '{"transcript":"synthetic truncated',
    ],
    ids=["parseable", "malformed"],
)
async def test_max_tokens_parseable_or_malformed_fails_closed_without_parsing_or_retry(
    tmp_path, auth_settings, terminology, raw_response
):
    class MaxTokensRequester(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            return _response(raw_response, finish_reason="MAX_TOKENS")

    audio, source, source_sha256 = _source(tmp_path)
    limiter, requester = Limiter(), MaxTokensRequester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology, end_ms=240_000)

    failure = raised.value
    assert failure.code == "response_schema_invalid"
    assert failure.sent and failure.retryable and not failure.ambiguous
    assert failure.diagnostics["finish_reason"] == "MAX_TOKENS"
    assert failure.diagnostics["truncated"] is True
    assert failure.diagnostics["actual"] == {
        "type": "string",
        "shape": {"characters": len(raw_response)},
    }
    assert raw_response not in json.dumps(failure.diagnostics, sort_keys=True)
    assert raw_response not in str(failure)
    assert len(requester.calls) == 1
    assert len(limiter.finalized) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("finish_reason", "ambiguous"),
    [
        (None, True),
        ("FUTURE_REASON", True),
        ("stop", True),
        ("OTHER", True),
        ("SAFETY", False),
        ("RECITATION", False),
    ],
    ids=["missing", "unknown", "non-exact-stop", "provider-other", "safety", "recitation"],
)
async def test_only_exact_stop_is_accepted(tmp_path, auth_settings, terminology, finish_reason, ambiguous):
    class FinishRequester(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            return _response(_segment_value(0, 20_000), finish_reason=finish_reason)

    audio, source, source_sha256 = _source(tmp_path)
    requester = FinishRequester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(), requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert raised.value.code == "response_schema_invalid"
    assert raised.value.sent and not raised.value.retryable
    assert raised.value.ambiguous is ambiguous
    expected_reason = finish_reason if finish_reason in {"OTHER", "SAFETY", "RECITATION"} else None
    assert raised.value.diagnostics["finish_reason"] == (
        expected_reason or ("MISSING" if finish_reason is None else "UNKNOWN")
    )
    assert len(requester.calls) == 1


@pytest.mark.asyncio
async def test_multiple_candidates_are_ambiguous_and_never_selected(tmp_path, auth_settings, terminology):
    class MultipleRequester(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            return _response(_segment_value(0, 20_000), candidates=2)

    audio, source, source_sha256 = _source(tmp_path)
    requester = MultipleRequester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(), requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert raised.value.code == "provider_response_ambiguous"
    assert raised.value.sent and raised.value.ambiguous and not raised.value.retryable
    assert len(requester.calls) == 1


@pytest.mark.asyncio
async def test_multiple_text_parts_are_ambiguous_and_never_concatenated(tmp_path, auth_settings, terminology):
    class MultiplePartsRequester(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            response = _response(_segment_value(0, 20_000))
            response.json_body["candidates"][0]["content"]["parts"].append(
                {"text": json.dumps(_segment_value(0, 20_000))}
            )
            return response

    audio, source, source_sha256 = _source(tmp_path)
    requester = MultiplePartsRequester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(), requester=requester)
    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)
    assert raised.value.code == "provider_response_ambiguous"
    assert raised.value.sent and raised.value.ambiguous and not raised.value.retryable
    assert len(requester.calls) == 1


@pytest.mark.asyncio
async def test_long_vad_segment_with_short_schema_valid_text_fails_content_verification(
    tmp_path, auth_settings, terminology
):
    private_like_text = "короткий синтетический ответ"
    value = _segment_value(0, 240_000)
    value["transcript"] = private_like_text
    limiter, requester = Limiter(), Requester([value])
    audio, source, source_sha256 = _source(tmp_path)
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    with pytest.raises(StageFailure) as raised:
        await _transcribe(
            service,
            audio,
            source,
            source_sha256,
            terminology,
            end_ms=240_000,
            expected_speech_ms=240_000,
        )

    failure = raised.value
    assert failure.code == "segment_content_incomplete"
    assert failure.sent and not failure.retryable and not failure.ambiguous
    assert failure.diagnostics["expected"]["constraint"] == ("duration_normalized_plausibility_failed")
    assert failure.diagnostics["plausibility"]["expected_speech_ms"] == 240_000
    assert private_like_text not in json.dumps(failure.diagnostics, ensure_ascii=False)
    assert limiter.finalized[0][1]["error_code"] == "segment_content_incomplete"


@pytest.mark.asyncio
async def test_coverage_echo_must_match_complete_source_range(tmp_path, auth_settings, terminology):
    limiter, requester = Limiter(), Requester([_segment_value(1, 19_000, words=20)])
    audio, source, source_sha256 = _source(tmp_path)
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)

    assert raised.value.code == "segment_coverage_invalid"
    assert raised.value.sent and not raised.value.retryable
    assert raised.value.diagnostics["coverage"] == {
        "expected_start_ms": 0,
        "expected_end_ms": 20_000,
        "actual_start_ms": 1,
        "actual_end_ms": 19_000,
    }


@pytest.mark.asyncio
async def test_malformed_stop_response_fails_closed_with_sanitized_shape(tmp_path, auth_settings, terminology):
    extra_values = {f"{index:02d}_" + "x" * 150: "synthetic-content-not-for-diagnostics" for index in range(40)}
    raw_response = json.dumps(
        {
            "transcript": 17,
            "language": "ru-RU",
            "uncertain_fragments": [],
            "coverage_start_ms": 0,
            "coverage_end_ms": 20_000,
            **extra_values,
        }
    )

    class MalformedRequester(Requester):
        async def request_json(self, method: str, url: str, **kwargs: Any) -> BoundedHTTPResponse:
            self.calls.append((method, url, kwargs))
            return _response(raw_response)

    audio, source, source_sha256 = _source(tmp_path)
    limiter, requester = Limiter(), MalformedRequester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    with pytest.raises(StageFailure) as raised:
        await _transcribe(service, audio, source, source_sha256, terminology)

    failure = raised.value
    assert failure.code == "response_schema_invalid"
    assert failure.sent and not failure.retryable and not failure.ambiguous
    assert failure.diagnostics["schema"] == "voice_intake_transcript"
    assert failure.diagnostics["json_path"] == "$.transcript"
    assert failure.diagnostics["expected"]["constraint"] == "string_type"
    assert failure.diagnostics["actual"] == {"type": "integer", "shape": {}}
    assert failure.diagnostics["extra_fields"] == sorted(field[:128] for field in extra_values)[:32]
    assert "synthetic-content-not-for-diagnostics" not in json.dumps(failure.diagnostics, sort_keys=True)
    assert len(requester.calls) == 1
    assert len(limiter.finalized) == 1


def test_missing_field_diagnostics_are_bounded_to_thirty_two():
    required_model = create_model(
        "RequiredDiagnosticModel",
        **{f"required_{index:02d}": (str, ...) for index in range(40)},
    )
    properties = {f"required_{index:02d}": {"type": "string"} for index in range(40)}
    with pytest.raises(ValidationError) as raised:
        required_model.model_validate({})

    diagnostics = AggregateGeminiInference._validation_diagnostics(
        schema={"type": "object", "properties": properties},
        schema_name="bounded_test_schema",
        error=raised.value,
        parsed_value={},
        response_body=None,
        finish_reason="STOP",
        usage=None,
        max_output_tokens=SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS,
    )

    assert diagnostics["missing_fields"] == [f"required_{index:02d}" for index in range(32)]
    assert len(diagnostics["missing_fields"]) == 32
    assert diagnostics["extra_fields"] == []


def test_status_contract_supports_dynamic_segment_plus_summary_counts():
    status = StatusResponse(
        session_id="voice-20260828-123456-abcdef12",
        state="summarizing",
        inference_batches_total=8,
        inference_batches_completed=7,
        gemini_requests_total=8,
        gemini_requests_completed=7,
        transcription_segments_total=7,
        transcription_segments_completed=7,
        transcription_coverage_complete=True,
        content_verification_status="passed",
        content_verified=True,
    )

    assert status.gemini_requests_total == 8
    assert status.transcription_segments_total == 7
    assert not status.client_audio_purge_allowed


def test_status_contract_rejects_completed_counts_above_dynamic_totals():
    with pytest.raises(ValidationError, match="completed Gemini request count"):
        StatusResponse(
            session_id="voice-20260828-123456-abcdef12",
            state="transcribing",
            inference_batches_total=2,
            inference_batches_completed=2,
            gemini_requests_total=2,
            gemini_requests_completed=3,
        )


def test_status_contract_rejects_client_purge_before_every_durable_gate():
    with pytest.raises(ValidationError, match="client purge permission"):
        StatusResponse(
            session_id="voice-20260828-123456-abcdef12",
            state="published_verified",
            transcription_segments_total=1,
            transcription_segments_completed=1,
            transcription_coverage_complete=True,
            content_verification_status="passed",
            content_verified=True,
            publication_verified=True,
            purge_authorized=True,
            audio_purged=False,
            server_audio_purged=False,
            client_audio_purge_allowed=True,
        )


def test_status_contract_requires_all_new_purge_gates_and_matching_physical_flags():
    status = StatusResponse(
        session_id="voice-20260828-123456-abcdef12",
        state="published_verified",
        transcription_segments_total=1,
        transcription_segments_completed=1,
        transcription_coverage_complete=True,
        content_verification_status="passed",
        content_verified=True,
        publication_verified=True,
        purge_authorized=True,
        audio_purged=True,
        server_audio_purged=True,
        client_audio_purge_allowed=True,
    )
    assert status.audio_purged and status.client_audio_purge_allowed
    assert not status.legacy_unverified_purge

    with pytest.raises(ValidationError, match="physical audio purge flags must agree"):
        StatusResponse(
            session_id="voice-20260828-123456-abcdef12",
            state="published_verified",
            audio_purged=False,
            server_audio_purged=True,
        )


def test_status_contract_preserves_truthful_legacy_unverified_purge_without_permission():
    status = StatusResponse(
        session_id="voice-20260828-123456-abcdef12",
        state="published_verified",
        github_verified=True,
        publication_verified=True,
        audio_purged=True,
        server_audio_purged=True,
        legacy_unverified_purge=True,
    )
    assert status.legacy_unverified_purge
    assert not status.content_verified
    assert not status.purge_authorized
    assert not status.client_audio_purge_allowed


@pytest.mark.parametrize(
    "unsafe_override",
    [
        {"audio_purged": False, "server_audio_purged": False},
        {"content_verification_status": "passed", "content_verified": True},
        {"purge_authorized": True},
        {"client_audio_purge_allowed": True},
    ],
    ids=["not-physically-purged", "content-verified", "purge-authorized", "client-enabled"],
)
def test_status_contract_rejects_legacy_marker_as_new_purge_bypass(unsafe_override):
    values = {
        "session_id": "voice-20260828-123456-abcdef12",
        "state": "published_verified",
        "publication_verified": True,
        "audio_purged": True,
        "server_audio_purged": True,
        "legacy_unverified_purge": True,
        **unsafe_override,
    }
    with pytest.raises(ValidationError):
        StatusResponse(**values)
