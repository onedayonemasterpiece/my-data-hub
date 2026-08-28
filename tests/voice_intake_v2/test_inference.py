from __future__ import annotations

import json

import pytest

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight, ModelLimit
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.http import BoundedHTTPResponse
from my_data_hub.voice_intake_v2.inference import AggregateGeminiInference
from my_data_hub.voice_intake_v2.worker import StageFailure

from .conftest import summary_value


class Limiter:
    def __init__(self, *, deny=False):
        self.deny = deny
        self.reserves = []
        self.finalized = []

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
        return None

    async def release_unsent(self, _lease, *, reason):
        return None

    async def report_provider_429(self, _lease, *, retry_after_ms):
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
