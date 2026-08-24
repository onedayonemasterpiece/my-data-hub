from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from typing import Any

from my_data_hub.google_ai.contracts import (
    ProviderInteraction,
    ProviderUsage,
    YouTubeAnalyzeRequest,
)
from my_data_hub.google_ai.http import (
    AiohttpBoundedJSONRequester,
    BoundedHTTPError,
    BoundedJSONRequester,
)
from my_data_hub.google_ai.youtube import (
    mode_prompt,
    provider_response_schema,
    system_instruction,
)

INTERACTIONS_ENDPOINT = "https://generativelanguage.googleapis.com/v1beta/interactions"
API_REVISION = "2026-05-20"
_SAFE_PROVIDER_CODE = re.compile(r"^[A-Z0-9_.-]{1,80}$")


@dataclass(frozen=True, slots=True)
class ProviderTransportFailure(RuntimeError):
    kind: str

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
        if isinstance(envelope, Mapping):
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
        elif "model" in message and any(
            marker in message for marker in ("not found", "not supported", "unavailable")
        ):
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
            if isinstance(modality, str) and modality in {
                "text",
                "image",
                "audio",
                "video",
                "document",
            } and tokens is not None:
                modalities.append({"modality": modality, "tokens": tokens})
    return ProviderUsage(
        total_input_tokens=input_tokens,
        total_output_tokens=output_tokens,
        total_thought_tokens=thought_tokens or 0,
        total_tokens=total_tokens,
        input_tokens_by_modality=tuple(modalities),
    )


class GeminiInteractionsClient:
    def __init__(
        self,
        *,
        requester: BoundedJSONRequester | None = None,
        timeout_seconds: int = 120,
        max_response_bytes: int = 524_288,
    ) -> None:
        self._requester = requester or AiohttpBoundedJSONRequester()
        self._timeout_seconds = timeout_seconds
        self._max_response_bytes = max_response_bytes

    @staticmethod
    def build_payload(
        *,
        canonical_youtube_url: str,
        request: YouTubeAnalyzeRequest,
        model: str,
    ) -> dict[str, Any]:
        video: dict[str, Any] = {
            "type": "video",
            "uri": canonical_youtube_url,
        }
        if request.media_resolution is not None:
            video["resolution"] = request.media_resolution.value
        return {
            "model": model,
            "input": [
                video,
                {"type": "text", "text": mode_prompt(request)},
            ],
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
            "store": False,
        }

    async def create(
        self,
        *,
        api_key: str,
        canonical_youtube_url: str,
        request: YouTubeAnalyzeRequest,
        model: str,
    ) -> ProviderInteraction:
        payload = self.build_payload(
            canonical_youtube_url=canonical_youtube_url,
            request=request,
            model=model,
        )
        try:
            response = await self._requester.request_json(
                "POST",
                INTERACTIONS_ENDPOINT,
                headers={
                    "x-goog-api-key": api_key,
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Api-Revision": API_REVISION,
                },
                json_body=payload,
                timeout_seconds=self._timeout_seconds,
                max_response_bytes=self._max_response_bytes,
            )
        except BoundedHTTPError as exc:
            raise ProviderTransportFailure(exc.kind) from exc

        body = response.json_body
        if not 200 <= response.status < 300:
            code, category, diagnostic = _provider_error(body, response.status)
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
        if not isinstance(body, Mapping):
            raise ProviderTransportFailure("malformed_json")
        interaction_id = body.get("id") if isinstance(body.get("id"), str) else None
        resolved_model = body.get("model") if isinstance(body.get("model"), str) else model
        status = body.get("status") if isinstance(body.get("status"), str) else "unknown"
        output_text = _extract_output_text(body)
        structured: Mapping[str, Any] | None = None
        if output_text is not None:
            try:
                candidate = json.loads(output_text)
            except json.JSONDecodeError as exc:
                raise ProviderTransportFailure("output_not_json") from exc
            if isinstance(candidate, Mapping):
                structured = candidate
        return ProviderInteraction(
            interaction_id=interaction_id,
            model=resolved_model,
            status=status,
            structured_output=structured,
            output_text=output_text,
            usage=_parse_usage(body),
            http_status=response.status,
        )
