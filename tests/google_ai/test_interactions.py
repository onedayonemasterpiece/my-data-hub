from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any

import pytest

from my_data_hub.google_ai.contracts import YouTubeAnalyzeRequest
from my_data_hub.google_ai.http import BoundedHTTPError, BoundedHTTPResponse
from my_data_hub.google_ai.interactions import (
    API_REVISION,
    INTERACTIONS_ENDPOINT,
    GeminiInteractionsClient,
    ProviderTransportFailure,
)


class Requester:
    def __init__(self, responses: list[BoundedHTTPResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any] | None,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> BoundedHTTPResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "json": dict(json_body or {}),
                "timeout": timeout_seconds,
                "max_bytes": max_response_bytes,
            }
        )
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def request(**changes: object) -> YouTubeAnalyzeRequest:
    values: dict[str, object] = {
        "youtube_url": "https://youtu.be/6V2stDksGI8",
        "idempotency_key": "transport-0001",
        "media_resolution": "medium",
        "max_output_tokens": 4096,
        "thinking_level": "low",
    }
    values.update(changes)
    return YouTubeAnalyzeRequest.model_validate(values)


def response(payload: object, status: int = 200, retry_after: str | None = None) -> BoundedHTTPResponse:
    return BoundedHTTPResponse(status, payload, retry_after, "application/json")


@pytest.mark.asyncio
async def test_payload_uses_video_before_text_store_false_and_exact_revision() -> None:
    body = {
        "id": "interaction-1",
        "model": "gemini-3.6-flash",
        "status": "completed",
        "steps": [
            {
                "type": "model_output",
                "content": [
                    {
                        "type": "text",
                        "text": json.dumps(
                            {
                                "summary": "ok",
                                "timeline": [],
                                "key_points": [],
                                "claims_to_verify": [],
                                "visual_observations": [],
                                "warnings": [],
                                "incomplete": False,
                                "truncated": False,
                            }
                        ),
                    }
                ],
            }
        ],
        "usage": {
            "total_input_tokens": 1000,
            "total_output_tokens": 100,
            "total_thought_tokens": 25,
            "total_tokens": 1125,
            "input_tokens_by_modality": [
                {"modality": "video", "tokens": 900},
                {"modality": "text", "tokens": 100},
            ],
        },
    }
    requester = Requester([response(body)])
    client = GeminiInteractionsClient(requester=requester, timeout_seconds=120, max_response_bytes=524288)

    result = await client.create(
        api_key="secret-never-returned",
        canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
        request=request(),
        model="gemini-3.6-flash",
    )

    assert len(requester.calls) == 1
    call = requester.calls[0]
    assert call["method"] == "POST"
    assert call["url"] == INTERACTIONS_ENDPOINT
    assert call["headers"]["Api-Revision"] == API_REVISION
    assert call["headers"]["x-goog-api-key"] == "secret-never-returned"
    payload = call["json"]
    assert payload["input"][0] == {
        "type": "video",
        "uri": "https://www.youtube.com/watch?v=6V2stDksGI8",
        "resolution": "medium",
    }
    assert payload["input"][1]["type"] == "text"
    assert payload["store"] is False
    assert "previous_interaction_id" not in payload
    assert payload["response_format"]["mime_type"] == "application/json"
    assert result.interaction_id == "interaction-1"
    assert result.usage is not None
    assert result.usage.total_thought_tokens == 25
    assert result.usage.total_tokens == 1125
    assert result.usage.input_tokens_by_modality[0] == {"modality": "video", "tokens": 900}
    assert "secret-never-returned" not in repr(result)


@pytest.mark.asyncio
async def test_provider_error_is_returned_without_hidden_retry_or_secret_echo() -> None:
    requester = Requester(
        [
            response(
                {"error": {"status": "RESOURCE_EXHAUSTED", "message": "quota for secret-key"}},
                status=429,
                retry_after="2",
            )
        ]
    )
    client = GeminiInteractionsClient(requester=requester)
    result = await client.create(
        api_key="secret-key",
        canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
        request=request(),
        model="gemini-3.6-flash",
    )
    assert len(requester.calls) == 1
    assert result.provider_error_category == "provider_429"
    assert result.retry_after_ms == 2000
    assert "secret-key" not in repr(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["timeout", "network", "response_too_large", "malformed_json"])
async def test_transport_failure_performs_no_automatic_retry(kind: str) -> None:
    requester = Requester([BoundedHTTPError(kind)])
    client = GeminiInteractionsClient(requester=requester)
    with pytest.raises(ProviderTransportFailure) as caught:
        await client.create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.6-flash",
        )
    assert caught.value.kind == kind
    assert len(requester.calls) == 1
