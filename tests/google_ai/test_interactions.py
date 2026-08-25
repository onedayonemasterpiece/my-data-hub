from __future__ import annotations

import json
from collections.abc import Awaitable, Callable, Mapping
from typing import Any

import pytest

from my_data_hub.google_ai.contracts import YouTubeAnalyzeRequest
from my_data_hub.google_ai.http import BoundedHTTPError, BoundedSSEResponse, SSEEvent, StreamTimeouts
from my_data_hub.google_ai.interactions import (
    API_REVISION,
    INTERACTIONS_ENDPOINT,
    GeminiInteractionsClient,
    ProviderTransportFailure,
)


class Requester:
    def __init__(
        self,
        events: list[tuple[str, object]] | None = None,
        *,
        response: BoundedSSEResponse | None = None,
        error: Exception | None = None,
    ) -> None:
        self.events = events or []
        self.response = response or BoundedSSEResponse(200, None, None, "text/event-stream", len(self.events), True)
        self.error = error
        self.calls: list[dict[str, Any]] = []

    async def request_sse(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
        timeouts: StreamTimeouts,
        max_raw_bytes: int,
        on_event: Callable[[SSEEvent], Awaitable[None]],
    ) -> BoundedSSEResponse:
        self.calls.append(
            {
                "method": method,
                "url": url,
                "headers": dict(headers),
                "json": dict(json_body),
                "timeouts": timeouts,
                "max_raw_bytes": max_raw_bytes,
            }
        )
        if self.error:
            raise self.error
        for name, payload in self.events:
            data = payload if isinstance(payload, str) else json.dumps(payload)
            await on_event(SSEEvent(name, data))
        return self.response


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


def completed_events(output: str) -> list[tuple[str, object]]:
    return [
        (
            "interaction.created",
            {"type": "interaction.created", "interaction": {"id": "interaction-1", "status": "created"}},
        ),
        (
            "interaction.in_progress",
            {"type": "interaction.in_progress", "interaction": {"id": "interaction-1", "status": "in_progress"}},
        ),
        ("step.start", {"type": "step.start", "index": 0, "step": {"type": "thought"}}),
        ("step.delta", {"type": "step.delta", "index": 0, "delta": {"type": "thought", "text": "secret thought"}}),
        ("step.stop", {"type": "step.stop", "index": 0, "status": "done"}),
        ("step.start", {"type": "step.start", "index": 1, "step": {"type": "model_output"}}),
        ("step.delta", {"type": "step.delta", "index": 1, "delta": {"type": "text", "text": output[:8]}}),
        ("step.delta", {"type": "step.delta", "index": 1, "delta": {"type": "text", "text": output[8:]}}),
        ("step.stop", {"type": "step.stop", "index": 1, "status": "done"}),
        (
            "interaction.completed",
            {
                "type": "interaction.completed",
                "interaction": {
                    "id": "interaction-1",
                    "model": "gemini-3.7-flash",
                    "status": "completed",
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
                },
            },
        ),
    ]


async def _record(target: list[tuple[str, str]], interaction_id: str, status: str) -> None:
    target.append((interaction_id, status))


@pytest.mark.asyncio
async def test_stream_payload_endpoint_order_created_callback_output_and_usage() -> None:
    output = json.dumps(
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
    )
    requester = Requester(completed_events(output))
    client = GeminiInteractionsClient(requester=requester)
    started: list[tuple[str, str]] = []
    result = await client.create(
        api_key="secret-never-returned",
        canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
        request=request(),
        model="gemini-3.7-flash",
        on_interaction_started=lambda interaction_id, status: _record(started, interaction_id, status),
    )
    assert len(requester.calls) == 1
    call = requester.calls[0]
    assert call["url"] == INTERACTIONS_ENDPOINT
    assert call["headers"]["Api-Revision"] == API_REVISION
    assert call["headers"]["Accept"] == "text/event-stream"
    assert call["json"]["stream"] is True
    assert call["json"]["background"] is False
    assert call["json"]["store"] is False
    assert call["json"]["input"][0]["type"] == "video"
    assert call["json"]["input"][1]["type"] == "text"
    assert call["timeouts"] == StreamTimeouts(30, 120, 300, 1800)
    assert started == [("interaction-1", "created")]
    assert result.output_text == output
    assert "secret thought" not in repr(result)
    assert result.usage is not None
    assert result.usage.total_thought_tokens == 25
    assert result.usage.input_tokens_by_modality[0] == {"modality": "video", "tokens": 900}
    assert "secret-never-returned" not in repr(result)


@pytest.mark.asyncio
async def test_disconnect_preserves_started_id_and_never_retries() -> None:
    class Disconnecting(Requester):
        async def request_sse(self, *args: Any, **kwargs: Any) -> BoundedSSEResponse:
            self.calls.append({"physical_post": True})
            callback = kwargs["on_event"]
            await callback(
                SSEEvent(
                    "interaction.created",
                    json.dumps(
                        {"type": "interaction.created", "interaction": {"id": "interaction-1", "status": "created"}}
                    ),
                )
            )
            raise BoundedHTTPError("idle_timeout")

    requester = Disconnecting()
    client = GeminiInteractionsClient(requester=requester)
    with pytest.raises(ProviderTransportFailure) as caught:
        await client.create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
        )
    assert caught.value.kind == "idle_timeout"
    assert caught.value.interaction_id == "interaction-1"
    assert len(requester.calls) == 1


@pytest.mark.asyncio
async def test_started_callback_failure_preserves_id_and_stops_stream() -> None:
    async def fail_started(_interaction_id: str, _status: str) -> None:
        raise RuntimeError("ledger conflict")

    requester = Requester(completed_events("{}"))
    with pytest.raises(ProviderTransportFailure) as caught:
        await GeminiInteractionsClient(requester=requester).create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=fail_started,
        )
    assert caught.value.kind == "interaction_started_accounting_failed"
    assert caught.value.interaction_id == "interaction-1"
    assert len(requester.calls) == 1


@pytest.mark.asyncio
async def test_conflicting_interaction_id_fails_closed() -> None:
    events = completed_events("{}")
    events.insert(
        1,
        (
            "interaction.in_progress",
            {"type": "interaction.in_progress", "interaction": {"id": "interaction-2", "status": "in_progress"}},
        ),
    )
    client = GeminiInteractionsClient(requester=Requester(events))
    with pytest.raises(ProviderTransportFailure) as caught:
        await client.create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
        )
    assert caught.value.kind == "interaction_id_conflict"
    assert caught.value.interaction_id == "interaction-1"


@pytest.mark.asyncio
async def test_top_level_status_update_interaction_id_must_match_created_id() -> None:
    events = completed_events("{}")
    events[1] = (
        "interaction.status_update",
        {
            "type": "interaction.status_update",
            "interaction_id": "interaction-1",
            "status": "in_progress",
        },
    )
    result = await GeminiInteractionsClient(requester=Requester(events)).create(
        api_key="secret-key",
        canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
        request=request(),
        model="gemini-3.7-flash",
        on_interaction_started=lambda interaction_id, status: _record(
            [], interaction_id, status
        ),
    )
    assert result.interaction_id == "interaction-1"

    events[1][1]["interaction_id"] = "interaction-conflict"  # type: ignore[index]
    with pytest.raises(ProviderTransportFailure) as caught:
        await GeminiInteractionsClient(requester=Requester(events)).create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record(
                [], interaction_id, status
            ),
        )
    assert caught.value.kind == "interaction_id_conflict"
    assert caught.value.interaction_id == "interaction-1"


@pytest.mark.asyncio
async def test_output_before_interaction_created_fails_closed() -> None:
    requester = Requester(
        [
            (
                "step.start",
                {"type": "step.start", "index": 0, "step": {"type": "model_output"}},
            )
        ]
    )
    with pytest.raises(ProviderTransportFailure) as caught:
        await GeminiInteractionsClient(requester=requester).create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
        )
    assert caught.value.kind == "interaction_created_missing"


@pytest.mark.asyncio
async def test_model_output_limit_is_typed() -> None:
    client = GeminiInteractionsClient(requester=Requester(completed_events("not-json")), max_output_bytes=4)
    with pytest.raises(ProviderTransportFailure) as caught:
        await client.create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
        )
    assert caught.value.kind == "output_too_large"


@pytest.mark.asyncio
async def test_terminal_model_output_must_be_complete_json() -> None:
    client = GeminiInteractionsClient(requester=Requester(completed_events("not-json")))
    with pytest.raises(ProviderTransportFailure) as caught:
        await client.create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
        )
    assert caught.value.kind == "output_not_json"
    assert caught.value.interaction_id == "interaction-1"


@pytest.mark.asyncio
async def test_terminal_error_event_without_type_is_explicit_provider_failure() -> None:
    requester = Requester([("error", {"error": {"status": "INVALID_ARGUMENT", "message": "bad request"}})])
    result = await GeminiInteractionsClient(requester=requester).create(
        api_key="secret-key",
        canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
        request=request(),
        model="gemini-3.7-flash",
        on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
    )
    assert result.status == "failed"
    assert result.interaction_id is None
    assert result.provider_error_code == "INVALID_ARGUMENT"
    assert result.provider_error_category == "provider_rejected_video"
    assert "bad request" not in repr(result)
    assert "secret-key" not in repr(result)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind",
    [
        "connect_timeout",
        "first_event_timeout",
        "idle_timeout",
        "total_timeout",
        "network",
        "response_too_large",
        "malformed_sse",
    ],
)
async def test_transport_failure_performs_no_automatic_retry(kind: str) -> None:
    requester = Requester(error=BoundedHTTPError(kind))
    client = GeminiInteractionsClient(requester=requester)
    with pytest.raises(ProviderTransportFailure) as caught:
        await client.create(
            api_key="secret-key",
            canonical_youtube_url="https://www.youtube.com/watch?v=6V2stDksGI8",
            request=request(),
            model="gemini-3.7-flash",
            on_interaction_started=lambda interaction_id, status: _record([], interaction_id, status),
        )
    assert caught.value.kind == kind
    assert len(requester.calls) == 1
