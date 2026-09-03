from __future__ import annotations

import pytest
from pydantic import ValidationError

from my_data_hub.google_ai.contracts import YouTubeAnalyzeRequest, YouTubeMode
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.youtube import (
    mode_prompt,
    normalize_youtube_url,
    provider_response_schema,
    response_schema,
)


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://youtu.be/6V2stDksGI8?si=tracking", "https://www.youtube.com/watch?v=6V2stDksGI8"),
        ("https://www.youtube.com/watch?v=6V2stDksGI8&t=10", "https://www.youtube.com/watch?v=6V2stDksGI8"),
        ("https://m.youtube.com/shorts/6V2stDksGI8", "https://www.youtube.com/watch?v=6V2stDksGI8"),
        ("https://youtube.com/embed/6V2stDksGI8", "https://www.youtube.com/watch?v=6V2stDksGI8"),
    ],
)
def test_normalizes_supported_youtube_urls(url: str, expected: str) -> None:
    normalized = normalize_youtube_url(url)
    assert normalized.video_id == "6V2stDksGI8"
    assert normalized.canonical_url == expected


@pytest.mark.parametrize(
    "url",
    [
        "http://youtu.be/6V2stDksGI8",
        "https://user:pass@youtu.be/6V2stDksGI8",
        "https://youtu.be:444/6V2stDksGI8",
        "https://127.0.0.1/6V2stDksGI8",
        "https://localhost/6V2stDksGI8",
        "https://example.com/watch?v=6V2stDksGI8",
        "https://youtu.be/6V2stDksGI8#fragment",
        "https://youtu.be/6V2stDksGI8?redirect=https://example.com",
        "https://www.youtube.com/watch?v=6V2stDksGI8&list=PL123",
    ],
)
def test_rejects_noncanonical_or_unsafe_urls(url: str) -> None:
    with pytest.raises(GoogleAIError) as caught:
        normalize_youtube_url(url)
    assert caught.value.code in {
        GoogleAIErrorCode.INVALID_YOUTUBE_URL,
        GoogleAIErrorCode.UNSUPPORTED_YOUTUBE_HOST,
        GoogleAIErrorCode.INVALID_VIDEO_ID,
    }


def request(**changes: object) -> YouTubeAnalyzeRequest:
    values: dict[str, object] = {
        "youtube_url": "https://youtu.be/6V2stDksGI8",
        "mode": "summary",
        "idempotency_key": "acceptance-0001",
    }
    values.update(changes)
    return YouTubeAnalyzeRequest.model_validate(values)


def test_mode_specific_fields_are_closed() -> None:
    assert request(mode="question", question="What is shown?").mode is YouTubeMode.QUESTION
    assert request(mode="custom", prompt="Extract engineering claims.").mode is YouTubeMode.CUSTOM
    with pytest.raises(ValidationError):
        request(mode="question")
    with pytest.raises(ValidationError):
        request(mode="summary", prompt="not allowed")
    with pytest.raises(ValidationError):
        YouTubeAnalyzeRequest.model_validate(
            {
                "youtube_url": "https://youtu.be/6V2stDksGI8",
                "idempotency_key": "acceptance-0001",
                "unexpected": True,
            }
        )


def test_transcript_schema_has_explicit_model_source_and_no_nullable_union() -> None:
    schema = response_schema(YouTubeMode.TRANSCRIPT)
    assert schema["additionalProperties"] is False
    assert schema["properties"]["transcript_source"] == {
        "type": "string",
        "const": "gemini_media_transcription",
    }
    speaker = schema["properties"]["segments"]["items"]["properties"]["speaker"]
    assert speaker["type"] == "string"
    assert "youtube_captions" not in repr(schema)


def test_provider_schema_preserves_shape_without_server_only_complexity_bounds() -> None:
    strict = response_schema(YouTubeMode.SUMMARY)
    provider = provider_response_schema(YouTubeMode.SUMMARY)

    assert provider["required"] == strict["required"]
    assert provider["properties"].keys() == strict["properties"].keys()
    rendered = repr(provider)
    for keyword in ("additionalProperties", "maxItems", "maxLength", "pattern", "const"):
        assert keyword not in rendered


@pytest.mark.parametrize("mode", list(YouTubeMode))
def test_all_mode_prompts_forbid_external_substitution(mode: YouTubeMode) -> None:
    kwargs: dict[str, object] = {"mode": mode.value}
    if mode is YouTubeMode.QUESTION:
        kwargs["question"] = "What claim is made?"
    elif mode is YouTubeMode.CUSTOM:
        kwargs["prompt"] = "List all numbers."
    prompt = mode_prompt(request(**kwargs))
    assert "Never use external facts as a substitute" in prompt
    if mode is YouTubeMode.TRANSCRIPT:
        assert "gemini_media_transcription" in prompt
        assert "[неразборчиво]" in prompt
