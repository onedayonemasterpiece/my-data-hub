from __future__ import annotations

import json

import pytest
from pydantic import ValidationError, create_model

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight, ModelLimit
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.http import BoundedHTTPError, BoundedHTTPResponse
from my_data_hub.voice_intake_v2.inference import AggregateGeminiInference
from my_data_hub.voice_intake_v2.worker import StageFailure

from .conftest import summary_value


class Limiter:
    def __init__(self, *, deny=False):
        self.deny = deny
        self.reserves = []
        self.finalized = []
        self.sent = []
        self.provider_429 = []

    async def preflight(self, model):
        return LimiterPreflight(
            limit=ModelLimit(model=model, rpm=10, tpm=1_000_000, rpd=100, tpm_reserve_extra=1000),
            candidate_key_ids=("id",), candidate_env_names=frozenset({"KEY"}),
            contract="google_ai_project_model_atomic_v1", bucket_strategy="rolling_60s_pacific_day_v2",
        )

    async def reserve_generate_content(self, **kwargs):
        if self.deny:
            raise GoogleAIError(GoogleAIErrorCode.QUOTA_EXHAUSTED_RPD, retryable=True, retry_after_ms=1000)
        self.reserves.append(kwargs)
        return LimiterLease(
            request_uid=kwargs["request_uid"], attempt_no=1, api_key_id="id", env_var_name="KEY",
            key_alias="alias", quota_scope="scope", reserved_tpm=kwargs["reserved_tpm"],
            contract="contract", bucket_strategy="bucket",
        )

    def secret_for(self, _lease):
        return "secret"

    async def mark_sent(self, _lease):
        self.sent.append(_lease.request_uid)
        return None

    async def release_unsent(self, _lease, *, reason):
        return None

    async def report_provider_429(self, _lease, *, retry_after_ms):
        self.provider_429.append((_lease.request_uid, retry_after_ms))
        return None

    async def finalize_generate_content(self, lease, **kwargs):
        self.finalized.append((lease, kwargs))

    @staticmethod
    def public_lease(lease, *, actual_tpm):
        return {"reserved_tpm": lease.reserved_tpm, "actual_tpm": actual_tpm}


class Requester:
    def __init__(self):
        self.calls = []

    async def request_json(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        value = (
            {"transcript": "full session", "language": "ru-RU", "uncertain_fragments": []}
            if len(self.calls) == 1 else summary_value()
        )
        return BoundedHTTPResponse(
            status=200,
            json_body={
                "candidates": [{"content": {"parts": [{"text": json.dumps(value)}]}}],
                "usageMetadata": {"promptTokenCount": 10, "candidatesTokenCount": 20, "totalTokenCount": 30},
            },
            retry_after=None, content_type="application/json",
        )


@pytest.mark.asyncio
async def test_two_stages_make_exactly_two_physical_posts_with_mp3_and_recorded_duration_reserve(
    tmp_path, auth_settings, terminology
):
    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"mp3")
    limiter, requester = Limiter(), Requester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    transcript = await service.transcribe(
        audio_path=audio, recorded_audio_ms=20_000, terminology=terminology
    )
    summary = await service.summarize(transcript=transcript.value, terminology=terminology)
    assert len(requester.calls) == 2
    assert all(call[0] == "POST" and call[1].endswith(":generateContent") for call in requester.calls)
    assert all("countTokens" not in call[1] for call in requester.calls)
    first_parts = requester.calls[0][2]["json_body"]["contents"][0]["parts"]
    assert first_parts[1]["inlineData"]["mimeType"] == "audio/mpeg"
    assert "один чанк" not in first_parts[0]["text"]
    assert limiter.reserves[0]["reserved_tpm"] == 20 * 32
    assert transcript.request_uid != summary.request_uid
    assert len(limiter.finalized) == 2


@pytest.mark.asyncio
async def test_quota_denial_before_send_makes_zero_physical_posts(tmp_path, auth_settings, terminology):
    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"mp3")
    requester = Requester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(deny=True), requester=requester)
    with pytest.raises(StageFailure) as raised:
        await service.transcribe(audio_path=audio, recorded_audio_ms=20_000, terminology=terminology)
    assert not raised.value.sent and raised.value.retryable
    assert requester.calls == []


@pytest.mark.asyncio
async def test_provider_429_makes_one_physical_post_and_no_hidden_retry(
    tmp_path, auth_settings, terminology
):
    class TooManyRequests(Requester):
        async def request_json(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))
            return BoundedHTTPResponse(429, {}, "1", "application/json")

    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"mp3")
    limiter, requester = Limiter(), TooManyRequests()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    with pytest.raises(StageFailure) as raised:
        await service.transcribe(audio_path=audio, recorded_audio_ms=20_000, terminology=terminology)
    assert raised.value.code == "provider_429"
    assert raised.value.sent and raised.value.retryable and not raised.value.ambiguous
    assert len(requester.calls) == 1
    assert len(limiter.sent) == 1 and len(limiter.provider_429) == 1
    assert len(limiter.finalized) == 1
    assert limiter.finalized[0][1]["error_code"] == "provider_failure"


@pytest.mark.asyncio
async def test_provider_timeout_makes_one_physical_post_and_fences_ambiguity(
    tmp_path, auth_settings, terminology
):
    class TimeoutRequester(Requester):
        async def request_json(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))
            raise BoundedHTTPError("total_timeout")

    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"mp3")
    limiter, requester = Limiter(), TimeoutRequester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    with pytest.raises(StageFailure) as raised:
        await service.transcribe(audio_path=audio, recorded_audio_ms=20_000, terminology=terminology)
    assert raised.value.code == "provider_timeout"
    assert raised.value.sent and raised.value.ambiguous and not raised.value.retryable
    assert len(requester.calls) == 1
    assert len(limiter.sent) == 1 and len(limiter.finalized) == 1


@pytest.mark.asyncio
async def test_twenty_minute_aggregate_transcription_has_bounded_headroom_and_one_post(
    tmp_path, auth_settings, terminology
):
    audio = tmp_path / "seven-normalized-chunks.mp3"
    audio.write_bytes(b"aggregate-mp3")
    limiter, requester = Limiter(), Requester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    await service.transcribe(
        audio_path=audio,
        recorded_audio_ms=1_207_620,
        terminology=terminology,
    )

    assert len(requester.calls) == 1
    generation = requester.calls[0][2]["json_body"]["generationConfig"]
    assert generation["maxOutputTokens"] == 65_536
    assert limiter.reserves[0]["reserved_tpm"] == 38_644


@pytest.mark.asyncio
async def test_max_tokens_truncation_is_retryable_and_diagnostics_never_contain_response(
    tmp_path, auth_settings, terminology
):
    raw_response = '{"transcript":"PRIVATE_RESPONSE_MUST_NOT_BE_LOGGED'

    class TruncatedRequester(Requester):
        async def request_json(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))
            return BoundedHTTPResponse(
                status=200,
                json_body={
                    "candidates": [{
                        "finishReason": "MAX_TOKENS",
                        "content": {"parts": [{"text": raw_response}]},
                    }],
                    "usageMetadata": {
                        "promptTokenCount": 38_644,
                        "candidatesTokenCount": 65_520,
                        "thoughtsTokenCount": 7,
                        "totalTokenCount": 104_171,
                    },
                },
                retry_after=None,
                content_type="application/json",
            )

    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"mp3")
    limiter, requester = Limiter(), TruncatedRequester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    with pytest.raises(StageFailure) as raised:
        await service.transcribe(
            audio_path=audio,
            recorded_audio_ms=1_207_620,
            terminology=terminology,
        )

    failure = raised.value
    assert failure.code == "response_schema_invalid"
    assert failure.sent and failure.retryable and not failure.ambiguous
    assert len(requester.calls) == 1
    assert len(limiter.finalized) == 1
    assert limiter.finalized[0][1]["error_code"] == "response_schema_invalid"
    assert failure.diagnostics == {
        "schema": "voice_intake_transcript",
        "schema_version": "1.0.0",
        "json_path": "$",
        "expected": {"type": "object", "constraint": "complete_valid_json_matching_schema"},
        "actual": {"type": "string", "shape": {"characters": len(raw_response)}},
        "missing_fields": [],
        "extra_fields": [],
        "finish_reason": "MAX_TOKENS",
        "token_counts": {"input": 38_644, "output": 65_520, "thought": 7, "total": 104_171},
        "configured_max_output_tokens": 65_536,
        "truncated": True,
    }
    assert raw_response not in json.dumps(failure.diagnostics, sort_keys=True)
    assert raw_response not in str(failure)


@pytest.mark.asyncio
async def test_malformed_stop_response_fails_closed_with_sanitized_shape(
    tmp_path, auth_settings, terminology
):
    extra_values = {
        f"{index:02d}_" + "x" * 150: "PRIVATE_CONTENT_MUST_NOT_BE_LOGGED"
        for index in range(40)
    }
    raw_response = json.dumps({
        "transcript": 17,
        "uncertain_fragments": [],
        **extra_values,
    })

    class MalformedRequester(Requester):
        async def request_json(self, method, url, **kwargs):
            self.calls.append((method, url, kwargs))
            return BoundedHTTPResponse(
                status=200,
                json_body={
                    "candidates": [{
                        "finishReason": "STOP",
                        "content": {"parts": [{"text": raw_response}]},
                    }],
                    "usageMetadata": {
                        "promptTokenCount": 10,
                        "candidatesTokenCount": 12,
                        "totalTokenCount": 22,
                    },
                },
                retry_after=None,
                content_type="application/json",
            )

    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"mp3")
    limiter, requester = Limiter(), MalformedRequester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)

    with pytest.raises(StageFailure) as raised:
        await service.transcribe(
            audio_path=audio,
            recorded_audio_ms=20_000,
            terminology=terminology,
        )

    failure = raised.value
    assert failure.code == "response_schema_invalid"
    assert failure.sent and not failure.retryable and not failure.ambiguous
    assert failure.diagnostics["schema"] == "voice_intake_transcript"
    assert failure.diagnostics["schema_version"] == "1.0.0"
    assert failure.diagnostics["json_path"] == "$.transcript"
    assert failure.diagnostics["expected"]["constraint"] == "string_type"
    assert failure.diagnostics["actual"] == {"type": "integer", "shape": {}}
    assert failure.diagnostics["missing_fields"] == []
    assert failure.diagnostics["extra_fields"] == sorted(
        field[:128] for field in extra_values
    )[:32]
    assert len(failure.diagnostics["extra_fields"]) == 32
    assert all(len(field) <= 128 for field in failure.diagnostics["extra_fields"])
    assert failure.diagnostics["finish_reason"] == "STOP"
    assert failure.diagnostics["truncated"] is False
    assert "PRIVATE_CONTENT_MUST_NOT_BE_LOGGED" not in json.dumps(
        failure.diagnostics, sort_keys=True
    )
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
        max_output_tokens=65_536,
    )

    assert diagnostics["missing_fields"] == [f"required_{index:02d}" for index in range(32)]
    assert len(diagnostics["missing_fields"]) == 32
    assert diagnostics["extra_fields"] == []
