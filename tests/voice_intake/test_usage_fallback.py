from __future__ import annotations

import asyncio
import json
from typing import Any

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight, ModelLimit
from my_data_hub.google_ai.http import BoundedHTTPResponse
from my_data_hub.voice_intake.contracts import ModelUsage
from my_data_hub.voice_intake.gemini import GeminiVoiceService
from my_data_hub.voice_intake.settings import VoiceIntakeSettings


def settings() -> VoiceIntakeSettings:
    return VoiceIntakeSettings(
        enabled=True,
        device_token="x" * 40,
        model="gemini-3.1-flash-lite",
        allowed_models=("gemini-3.1-flash-lite",),
        max_audio_bytes=8 * 1024 * 1024,
        max_json_bytes=2 * 1024 * 1024,
        provider_timeout_seconds=180,
        github_token="github-token",
        github_repository="onedayonemasterpiece/idea-hub",
        github_branch="main",
        limiter_supabase_url="https://example.supabase.co",
        limiter_supabase_service_key="service-key",
        normal_key_envs=("GOOGLE_API_KEY",),
    )


def wav_bytes() -> bytes:
    data = b"\x00\x00" * 1600
    fmt = (
        b"fmt "
        + (16).to_bytes(4, "little")
        + (1).to_bytes(2, "little")
        + (1).to_bytes(2, "little")
        + (16000).to_bytes(4, "little")
        + (32000).to_bytes(4, "little")
        + (2).to_bytes(2, "little")
        + (16).to_bytes(2, "little")
    )
    body = fmt + b"data" + len(data).to_bytes(4, "little") + data
    return b"RIFF" + (len(body) + 4).to_bytes(4, "little") + b"WAVE" + body


class Limiter:
    def __init__(self) -> None:
        self.reserved_tpm = 0
        self.finalized_usage: ModelUsage | None = None

    async def preflight(self, model: str) -> LimiterPreflight:
        return LimiterPreflight(
            limit=ModelLimit(model=model, rpm=13, tpm=240000, rpd=450, tpm_reserve_extra=1000),
            candidate_key_ids=("key-id",),
            candidate_env_names=frozenset({"GOOGLE_API_KEY"}),
            contract="google_ai_project_model_atomic_v1",
            bucket_strategy="rolling_60s_pacific_day_v2",
        )

    async def reserve_generate_content(self, **kwargs: Any) -> LimiterLease:
        self.reserved_tpm = int(kwargs["reserved_tpm"])
        return LimiterLease(
            request_uid=str(kwargs["request_uid"]),
            attempt_no=1,
            api_key_id="key-id",
            env_var_name="GOOGLE_API_KEY",
            key_alias="primary",
            quota_scope="google:project",
            reserved_tpm=self.reserved_tpm,
            contract="google_ai_project_model_atomic_v1",
            bucket_strategy="rolling_60s_pacific_day_v2",
        )

    def secret_for(self, _lease: LimiterLease) -> str:
        return "secret"

    async def mark_sent(self, _lease: LimiterLease) -> None:
        return None

    async def release_unsent(self, _lease: LimiterLease, reason: str) -> None:
        raise AssertionError(reason)

    async def report_provider_429(self, _lease: LimiterLease, retry_after_ms: int | None) -> None:
        raise AssertionError(retry_after_ms)

    async def finalize_generate_content(self, _lease: LimiterLease, **kwargs: Any) -> None:
        self.finalized_usage = kwargs["usage"]

    @staticmethod
    def public_lease(_lease: LimiterLease, *, actual_tpm: int | None) -> dict[str, Any]:
        return {"actual_tpm": actual_tpm}


class Requester:
    async def request_json(self, *_args: Any, **_kwargs: Any) -> BoundedHTTPResponse:
        return BoundedHTTPResponse(
            status=200,
            json_body={
                "candidates": [
                    {
                        "finishReason": "STOP",
                        "content": {
                            "parts": [
                                {
                                    "text": json.dumps(
                                        {
                                            "transcript": "Привет",
                                            "language": "ru-RU",
                                            "uncertain_fragments": [],
                                        },
                                        ensure_ascii=False,
                                    )
                                }
                            ]
                        },
                    }
                ]
            },
            retry_after=None,
            content_type="application/json",
        )


def test_success_without_usage_retains_conservative_reservation() -> None:
    limiter = Limiter()
    service = GeminiVoiceService(
        settings(),
        limiter=limiter,  # type: ignore[arg-type]
        requester=Requester(),  # type: ignore[arg-type]
    )
    result = asyncio.run(
        service.transcribe(
            session_id="voice-20260828-123456-abcdef12",
            chunk_index=0,
            duration_ms=1000,
            audio=wav_bytes(),
        )
    )
    assert limiter.reserved_tpm > 8_192
    assert result.usage.total_tokens == limiter.reserved_tpm
    assert limiter.finalized_usage is not None
    assert limiter.finalized_usage.total_tokens == limiter.reserved_tpm
    assert result.limiter == {
        "actual_tpm": None,
        "usage_accounting": "provider_usage_missing_reserved_tpm_retained",
        "audio_duration_ms": 1000,
        "prompt_version": "voice-transcribe-v1",
    }
