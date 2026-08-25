from __future__ import annotations

import asyncio
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from my_data_hub.google_ai.contracts import ProviderInteraction, ProviderUsage, YouTubeAnalyzeRequest
from my_data_hub.google_ai.http import (
    AiohttpBoundedSSERequester,
    BoundedHTTPError,
    BoundedSSERequester,
    SSEEvent,
    StreamTimeouts,
)
from my_data_hub.google_ai.youtube import mode_prompt, provider_response_schema, system_instruction

INTERACTIONS_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/interactions?alt=sse"
API_REVISION = "2026-05-20"
_SAFE_PROVIDER_CODE = re.compile(r"^[A-Z0-9_.-]{1,80}$")
_SAFE_INTERACTION_ID = re.compile(r"^[^\s\x00-\x1f\x7f]{1,1024}$")
_TERMINAL_EVENTS = {
    "interaction.completed": "completed",
    "interaction.failed": "failed",
    "interaction.incomplete": "incomplete",
    "interaction.cancelled": "cancelled",
    "interaction.error": "failed",
    "error": "failed",
}


@dataclass(frozen=True, slots=True)
class ProviderTransportFailure(RuntimeError):
    kind: str
    interaction_id: str | None = None
    provider_status: str = "incomplete"

    def __str__(self) -> str:
        return self.kind


def _nonnegative_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _retry_after_ms(value: str | None) -> int | None:
    if not value:
        return None
    stripped = value.strip()
    try:
        seconds = float(stripped)
    except ValueError:
        try:
            target = parsedate_to_datetime(stripped)
            if target.tzinfo is None:
                target = target.replace(tzinfo=UTC)
            seconds = max(0.0, (target - datetime.now(UTC)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None
    return min(93_600_000, max(0, int(seconds * 1000)))


def _provider_error(body: Any, http_status: int) -> tuple[str | None, str, str | None]:
    code: str | None = None
    message = ""
    if isinstance(body, Mapping):
        error = body.get("error")
        envelope = error if isinstance(error, Mapping) else body
        raw_code = envelope.get("status") or envelope.get("code")
        if isinstance(raw_code, str) and _SAFE_PROVIDER_CODE.fullmatch(raw_code):
            code = raw_code
        raw_message = envelope.get("message")
        if isinstance(raw_message, str):
            message = raw_message.casefold()[:2000]
    if http_status == 429:
        return code, "provider_429", "provider_quota_rejected"
    public_markers = (
        "not public",
        "private video",
        "video is private",
        "unlisted",
        "video unavailable",
        "not found",
        "youtube video",
    )
    if http_status in {400, 403, 404} and any(marker in message for marker in public_markers):
        return code, "youtube_video_not_public", "video_not_public"
    diagnostic: str | None = None
    if http_status == 400:
        if any(
            marker in message
            for marker in (
                "response_format",
                "json payload",
                "json schema",
                "unknown name",
                "structured output",
            )
        ):
            diagnostic = "provider_response_schema_invalid"
        elif "model" in message and any(marker in message for marker in ("not found", "not supported", "unavailable")):
            diagnostic = "provider_model_unavailable"
        elif "resolution" in message:
            diagnostic = "provider_media_resolution_invalid"
        elif any(marker in message for marker in ("url", "uri", "video")):
            diagnostic = "provider_video_reference_invalid"
        else:
            diagnostic = "provider_request_invalid"
    return code, "provider_rejected_video", diagnostic


def _extract_output_text(body: Mapping[str, Any]) -> str | None:
    steps = body.get("steps")
    if not isinstance(steps, list):
        return None
    chunks: list[str] = []
    for step in steps:
        if not isinstance(step, Mapping) or step.get("type") != "model_output":
            continue
        content = step.get("content")
        if not isinstance(content, list):
            continue
        for block in content:
            if isinstance(block, Mapping) and block.get("type") == "text":
                text = block.get("text")
                if isinstance(text, str):
                    chunks.append(text)
    output = "".join(chunks).strip()
    return output or None


def _parse_usage(body: Mapping[str, Any]) -> ProviderUsage | None:
    raw = body.get("usage")
    if not isinstance(raw, Mapping):
        return None
    input_tokens = _nonnegative_int(raw.get("total_input_tokens"))
    output_tokens = _nonnegative_int(raw.get("total_output_tokens"))
    total_tokens = _nonnegative_int(raw.get("total_tokens"))
    thought_tokens = _nonnegative_int(raw.get("total_thought_tokens"))
    if input_tokens is None or output_tokens is None or total_tokens is None:
        return None
    modalities: list[Mapping[str, Any]] = []
    raw_modalities = raw.get("input_tokens_by_modality")
    if isinstance(raw_modalities, list):
        for item in raw_modalities:
            if not isinstance(item, Mapping):
                continue
            modality = item.get("modality")
            tokens = _nonnegative_int(item.get("tokens"))
            if (
                isinstance(modality, str)
                and modality
                in {
                    "text",
                    "image",
                    "audio",
                    "video",
                    "document",
                }
                and tokens is not None
            ):
                modalities.append({"modality": modality, "tokens": tokens})
    return ProviderUsage(
        total_input_tokens=input_tokens,
        total_output_tokens=output_tokens,
        total_thought_tokens=thought_tokens or 0,
        total_tokens=total_tokens,
        input_tokens_by_modality=tuple(modalities),
    )


def _interaction(value: Mapping[str, Any]) -> Mapping[str, Any]:
    nested = value.get("interaction")
    return nested if isinstance(nested, Mapping) else value


def _interaction_id(value: Mapping[str, Any]) -> str | None:
    nested = value.get("interaction")
    candidates = []
    if isinstance(nested, Mapping):
        candidates.append(nested.get("id"))
    else:
        candidates.append(value.get("id"))
    candidates.append(value.get("interaction_id"))
    present = [candidate for candidate in candidates if candidate is not None]
    if not present:
        return None
    if any(
        not isinstance(candidate, str) or not _SAFE_INTERACTION_ID.fullmatch(candidate)
        for candidate in present
    ):
        raise ProviderTransportFailure("interaction_id_invalid")
    resolved = str(present[0])
    if any(candidate != resolved for candidate in present[1:]):
        raise ProviderTransportFailure("interaction_id_conflict")
    return resolved


class _InteractionStreamAccumulator:
    def __init__(
        self,
        *,
        model: str,
        max_output_bytes: int,
        on_interaction_started: Callable[[str, str], Awaitable[None]],
    ) -> None:
        self.model = model
        self.max_output_bytes = max_output_bytes
        self.on_interaction_started = on_interaction_started
        self.interaction_id: str | None = None
        self.status = "incomplete"
        self.step_types: dict[int, str] = {}
        self.output: list[str] = []
        self.output_bytes = 0
        self.terminal: Mapping[str, Any] | None = None
        self.terminal_event: str | None = None

    async def accept(self, event: SSEEvent) -> None:
        try:
            payload = json.loads(event.data)
        except json.JSONDecodeError as exc:
            raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status) from exc
        if not isinstance(payload, Mapping):
            raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
        raw_type = payload.get("type", payload.get("event_type"))
        if raw_type is None and event.event != "message":
            raw_type = event.event
        if not isinstance(raw_type, str):
            raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
        event_type = raw_type
        if event.event != "message" and event.event != event_type:
            raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
        if self.terminal is not None:
            raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)

        try:
            candidate_id = _interaction_id(payload)
        except ProviderTransportFailure as exc:
            raise ProviderTransportFailure(
                exc.kind, self.interaction_id, self.status
            ) from exc
        if candidate_id is not None and self.interaction_id not in {None, candidate_id}:
            raise ProviderTransportFailure("interaction_id_conflict", self.interaction_id, self.status)

        if event_type == "interaction.created":
            if candidate_id is None:
                raise ProviderTransportFailure("interaction_id_invalid", None, "created")
            first = self.interaction_id is None
            self.interaction_id = candidate_id
            self.status = "created"
            if first:
                try:
                    await self.on_interaction_started(candidate_id, "created")
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    raise ProviderTransportFailure(
                        "interaction_started_accounting_failed", candidate_id, "created"
                    ) from exc
            return

        if candidate_id is not None:
            if self.interaction_id is None:
                raise ProviderTransportFailure("interaction_created_missing", None, self.status)
            self.interaction_id = candidate_id

        if event_type in {"interaction.in_progress", "interaction.status_update"}:
            if self.interaction_id is None:
                raise ProviderTransportFailure("interaction_created_missing", None, self.status)
            status = _interaction(payload).get("status")
            if isinstance(status, str) and 1 <= len(status) <= 80:
                self.status = status
            return
        if event_type == "step.start":
            if self.interaction_id is None:
                raise ProviderTransportFailure("interaction_created_missing", None, self.status)
            index = _nonnegative_int(payload.get("index"))
            step = payload.get("step")
            step_type = step.get("type") if isinstance(step, Mapping) else None
            if index is None or not isinstance(step_type, str):
                raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
            self.step_types[index] = step_type
            return
        if event_type == "step.delta":
            if self.interaction_id is None:
                raise ProviderTransportFailure("interaction_created_missing", None, self.status)
            index = _nonnegative_int(payload.get("index"))
            delta = payload.get("delta")
            if index is None or not isinstance(delta, Mapping):
                raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
            if self.step_types.get(index) == "model_output" and delta.get("type") == "text":
                text = delta.get("text")
                if not isinstance(text, str):
                    raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
                self.output_bytes += len(text.encode("utf-8"))
                if self.output_bytes > self.max_output_bytes:
                    raise ProviderTransportFailure("output_too_large", self.interaction_id, self.status)
                self.output.append(text)
            return
        if event_type == "step.stop":
            if self.interaction_id is None:
                raise ProviderTransportFailure("interaction_created_missing", None, self.status)
            index = _nonnegative_int(payload.get("index"))
            if index is None or index not in self.step_types:
                raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)
            return
        if event_type in _TERMINAL_EVENTS:
            if self.interaction_id is None and event_type != "error":
                raise ProviderTransportFailure("interaction_created_missing", None, self.status)
            self.terminal = payload
            self.terminal_event = event_type
            self.status = _TERMINAL_EVENTS[event_type]
            actual_status = _interaction(payload).get("status")
            if isinstance(actual_status, str) and 1 <= len(actual_status) <= 80:
                self.status = actual_status
            return
        raise ProviderTransportFailure("malformed_sse", self.interaction_id, self.status)

    def result(self, *, http_status: int, retry_after: str | None) -> ProviderInteraction:
        if self.terminal is None or self.terminal_event is None:
            raise ProviderTransportFailure("stream_disconnected", self.interaction_id, self.status)
        interaction = _interaction(self.terminal)
        resolved_model = interaction.get("model")
        if not isinstance(resolved_model, str):
            resolved_model = self.model
        output_text = "".join(self.output).strip() or _extract_output_text(interaction)
        if output_text is not None and len(output_text.encode("utf-8")) > self.max_output_bytes:
            raise ProviderTransportFailure("output_too_large", self.interaction_id, self.status)
        structured: Mapping[str, Any] | None = None
        if self.status == "completed":
            if output_text is None:
                raise ProviderTransportFailure("output_not_json", self.interaction_id, self.status)
            try:
                candidate = json.loads(output_text)
            except json.JSONDecodeError as exc:
                raise ProviderTransportFailure("output_not_json", self.interaction_id, self.status) from exc
            if not isinstance(candidate, Mapping):
                raise ProviderTransportFailure("output_not_json", self.interaction_id, self.status)
            structured = candidate
        code: str | None = None
        category: str | None = None
        diagnostic: str | None = None
        if self.status in {"failed", "error"}:
            code, category, diagnostic = _provider_error(interaction, http_status)
        return ProviderInteraction(
            interaction_id=self.interaction_id,
            model=resolved_model,
            status=self.status,
            structured_output=structured,
            output_text=output_text,
            usage=_parse_usage(interaction),
            http_status=http_status,
            retry_after_ms=_retry_after_ms(retry_after),
            provider_error_code=code,
            provider_error_category=category,
            provider_error_diagnostic=diagnostic,
        )


class GeminiInteractionsClient:
    def __init__(
        self,
        *,
        requester: BoundedSSERequester | None = None,
        timeouts: StreamTimeouts | None = None,
        max_raw_sse_bytes: int = 2_097_152,
        max_output_bytes: int = 1_048_576,
    ) -> None:
        self._requester = requester or AiohttpBoundedSSERequester()
        self._timeouts = timeouts or StreamTimeouts()
        self._max_raw_sse_bytes = max_raw_sse_bytes
        self._max_output_bytes = max_output_bytes

    @staticmethod
    def build_payload(
        *,
        canonical_youtube_url: str,
        request: YouTubeAnalyzeRequest,
        model: str,
    ) -> dict[str, Any]:
        video: dict[str, Any] = {"type": "video", "uri": canonical_youtube_url}
        if request.media_resolution is not None:
            video["resolution"] = request.media_resolution.value
        return {
            "model": model,
            "input": [video, {"type": "text", "text": mode_prompt(request)}],
            "system_instruction": system_instruction(request),
            "generation_config": {
                "max_output_tokens": request.max_output_tokens,
                "thinking_level": request.thinking_level.value,
            },
            "response_format": {
                "type": "text",
                "mime_type": "application/json",
                "schema": provider_response_schema(request.mode),
            },
            "stream": True,
            "background": False,
            "store": False,
        }

    async def create(
        self,
        *,
        api_key: str,
        canonical_youtube_url: str,
        request: YouTubeAnalyzeRequest,
        model: str,
        on_interaction_started: Callable[[str, str], Awaitable[None]],
    ) -> ProviderInteraction:
        payload = self.build_payload(canonical_youtube_url=canonical_youtube_url, request=request, model=model)
        accumulator = _InteractionStreamAccumulator(
            model=model,
            max_output_bytes=self._max_output_bytes,
            on_interaction_started=on_interaction_started,
        )
        try:
            response = await self._requester.request_sse(
                "POST",
                INTERACTIONS_ENDPOINT,
                headers={
                    "x-goog-api-key": api_key,
                    "Content-Type": "application/json",
                    "Accept": "text/event-stream",
                    "Api-Revision": API_REVISION,
                },
                json_body=payload,
                timeouts=self._timeouts,
                max_raw_bytes=self._max_raw_sse_bytes,
                on_event=accumulator.accept,
            )
        except asyncio.CancelledError:
            raise
        except BoundedHTTPError as exc:
            raise ProviderTransportFailure(exc.kind, accumulator.interaction_id, accumulator.status) from exc
        if not 200 <= response.status < 300:
            code, category, diagnostic = _provider_error(response.json_body, response.status)
            return ProviderInteraction(
                interaction_id=None,
                model=model,
                status="failed",
                structured_output=None,
                output_text=None,
                usage=None,
                http_status=response.status,
                retry_after_ms=_retry_after_ms(response.retry_after),
                provider_error_code=code,
                provider_error_category=category,
                provider_error_diagnostic=diagnostic,
            )
        return accumulator.result(http_status=response.status, retry_after=response.retry_after)
