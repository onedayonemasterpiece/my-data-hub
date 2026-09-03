from __future__ import annotations

from typing import Any, Never

import pytest

from my_data_hub.google_ai.analyzer import GeminiYouTubeAnalyzer, YouTubeAnalyzerConfig
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode


class UnexpectedLimiter:
    def __init__(self) -> None:
        self.preflight_calls = 0

    async def preflight(self, _model: str) -> Never:
        self.preflight_calls += 1
        raise AssertionError("limiter preflight must not run for an invalid model/level pair")


class UnexpectedInteractions:
    def __init__(self) -> None:
        self.calls = 0

    async def create(self, **_kwargs: Any) -> Never:
        self.calls += 1
        raise AssertionError("provider transport must not run for an invalid model/level pair")


@pytest.mark.asyncio
async def test_gemini_37_minimal_is_typed_and_blocked_before_quota_admission() -> None:
    limiter = UnexpectedLimiter()
    interactions = UnexpectedInteractions()
    analyzer = GeminiYouTubeAnalyzer(
        config=YouTubeAnalyzerConfig(
            enabled=True,
            default_model="gemini-3.6-flash",
            allowed_models=frozenset({"gemini-3.6-flash", "gemini-3.7-flash"}),
            max_output_tokens=8192,
        ),
        limiter=limiter,  # type: ignore[arg-type]
        interactions=interactions,  # type: ignore[arg-type]
    )

    with pytest.raises(GoogleAIError) as caught:
        await analyzer.analyze(
            {
                "youtube_url": "https://youtu.be/6V2stDksGI8",
                "mode": "summary",
                "model": "gemini-3.7-flash",
                "thinking_level": "minimal",
                "max_output_tokens": 4096,
                "idempotency_key": "thinking-level-0001",
            }
        )

    assert caught.value.code is GoogleAIErrorCode.UNSUPPORTED_THINKING_LEVEL
    assert limiter.preflight_calls == 0
    assert interactions.calls == 0
