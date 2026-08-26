from __future__ import annotations

import re
from collections.abc import Mapping
from typing import Any
from urllib.parse import parse_qs, unquote, urlsplit

from my_data_hub.google_ai.contracts import NormalizedYouTubeURL, YouTubeAnalyzeRequest, YouTubeMode
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode

_ALLOWED_HOSTS = frozenset({"youtube.com", "www.youtube.com", "m.youtube.com", "youtu.be"})
_IGNORED_QUERY_KEYS = frozenset({"si", "t", "start", "feature", "app"})
_VIDEO_ID = re.compile(r"^[A-Za-z0-9_-]{11}$")


def normalize_youtube_url(raw: str) -> NormalizedYouTubeURL:
    value = raw.strip()
    if not value or len(value) > 2048:
        raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
    try:
        parsed = urlsplit(value)
        port = parsed.port
    except ValueError as exc:
        raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL) from exc
    if parsed.scheme.lower() != "https" or not parsed.hostname:
        raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
    if parsed.username is not None or parsed.password is not None or port not in {None, 443}:
        raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
    if parsed.fragment:
        raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
    host = parsed.hostname.casefold().rstrip(".")
    if host not in _ALLOWED_HOSTS:
        raise GoogleAIError(GoogleAIErrorCode.UNSUPPORTED_YOUTUBE_HOST)

    query = parse_qs(parsed.query, keep_blank_values=True, strict_parsing=False)
    path = unquote(parsed.path)
    video_id: str | None = None

    if host == "youtu.be":
        if set(query) - _IGNORED_QUERY_KEYS:
            raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
        parts = [part for part in path.split("/") if part]
        if len(parts) == 1:
            video_id = parts[0]
    else:
        if path in {"/watch", "/watch/"}:
            if set(query) - ({"v"} | _IGNORED_QUERY_KEYS):
                raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
            values = query.get("v", [])
            if len(values) == 1:
                video_id = values[0]
        else:
            if set(query) - _IGNORED_QUERY_KEYS:
                raise GoogleAIError(GoogleAIErrorCode.INVALID_YOUTUBE_URL)
            parts = [part for part in path.split("/") if part]
            if len(parts) == 2 and parts[0] in {"shorts", "embed"}:
                video_id = parts[1]

    if video_id is None or not _VIDEO_ID.fullmatch(video_id):
        raise GoogleAIError(GoogleAIErrorCode.INVALID_VIDEO_ID)
    return NormalizedYouTubeURL(
        video_id=video_id,
        canonical_url=f"https://www.youtube.com/watch?v={video_id}",
    )


def system_instruction(request: YouTubeAnalyzeRequest) -> str:
    return (
        "Analyze only the actual audio and visual stream supplied as the video input. "
        "Do not replace missing evidence with outside knowledge. Mark uncertain speech as "
        "[неразборчиво] and keep doubtful claims explicitly uncertain. Return only JSON that "
        f"matches the supplied schema. Write all natural-language fields in {request.language}. "
        "Timestamps must refer to the supplied video. Do not call the result YouTube captions."
    )


def mode_prompt(request: YouTubeAnalyzeRequest) -> str:
    timestamp_rule = (
        "Include grounded timestamps for material sections and evidence."
        if request.include_timestamps
        else "Do not invent timestamps; include them only when required by the response schema."
    )
    visual_rule = (
        "Inspect and report important visual evidence separately from spoken claims."
        if request.include_visual_observations
        else "Do not add a visual-observations section beyond schema-required empty arrays."
    )
    if request.mode is YouTubeMode.SUMMARY:
        task = (
            "Return a concise summary, a detailed timeline, key theses, factual claims/numbers/statements "
            "that require verification, important visual observations, and warnings about inaudible or "
            "uncertain fragments."
        )
    elif request.mode is YouTubeMode.TRANSCRIPT:
        task = (
            "Produce a model-generated media transcription with start/end timestamps, speaker labels only "
            "when distinguishable, [неразборчиво] for unclear speech, and a separate list of text read from "
            "the screen. Set transcript_source exactly to gemini_media_transcription."
        )
    elif request.mode is YouTubeMode.QUESTION:
        task = (
            f"Answer this question only from the video: {request.question}\nReturn supporting timestamps, "
            "an explanation, confidence, and whether the answer is actually supported by the video."
        )
    else:
        task = (
            "Perform the following bounded user task after applying all server instructions above:\n"
            f"{request.prompt}"
        )
    return f"{task}\n{timestamp_rule}\n{visual_rule}\nNever use external facts as a substitute for video evidence."


def _string(max_length: int = 12000) -> dict[str, Any]:
    return {"type": "string", "maxLength": max_length}


def _warnings_schema() -> dict[str, Any]:
    return {"type": "array", "maxItems": 50, "items": _string(2000)}


def response_schema(mode: YouTubeMode) -> Mapping[str, Any]:
    time_string = {"type": "string", "pattern": r"^(?:\d{1,3}:)?\d{1,2}:\d{2}(?:\.\d{1,3})?$"}
    base_tail: dict[str, Any] = {
        "warnings": _warnings_schema(),
        "incomplete": {"type": "boolean"},
        "truncated": {"type": "boolean"},
    }
    if mode is YouTubeMode.SUMMARY:
        properties = {
            "summary": _string(20000),
            "timeline": {
                "type": "array",
                "maxItems": 120,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["start", "end", "topic", "summary"],
                    "properties": {
                        "start": time_string,
                        "end": time_string,
                        "topic": _string(1000),
                        "summary": _string(5000),
                    },
                },
            },
            "key_points": {"type": "array", "maxItems": 100, "items": _string(5000)},
            "claims_to_verify": {"type": "array", "maxItems": 100, "items": _string(5000)},
            "visual_observations": {"type": "array", "maxItems": 100, "items": _string(5000)},
            **base_tail,
        }
    elif mode is YouTubeMode.TRANSCRIPT:
        properties = {
            "transcript_source": {"type": "string", "const": "gemini_media_transcription"},
            "segments": {
                "type": "array",
                "maxItems": 800,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["start", "end", "speaker", "text"],
                    "properties": {
                        "start": time_string,
                        "end": time_string,
                        "speaker": {"type": "string", "maxLength": 200},
                        "text": _string(8000),
                    },
                },
            },
            "on_screen_text": {
                "type": "array",
                "maxItems": 300,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["start", "end", "text"],
                    "properties": {"start": time_string, "end": time_string, "text": _string(5000)},
                },
            },
            **base_tail,
        }
    elif mode is YouTubeMode.QUESTION:
        properties = {
            "answer": _string(20000),
            "supporting_timestamps": {
                "type": "array",
                "maxItems": 50,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["start", "end", "evidence"],
                    "properties": {"start": time_string, "end": time_string, "evidence": _string(5000)},
                },
            },
            "explanation": _string(20000),
            "confidence": {"type": "string", "enum": ["low", "medium", "high"]},
            "supported_by_video": {"type": "boolean"},
            **base_tail,
        }
    else:
        properties = {
            "result": _string(30000),
            "evidence": {
                "type": "array",
                "maxItems": 100,
                "items": {
                    "type": "object",
                    "additionalProperties": False,
                    "required": ["start", "end", "evidence"],
                    "properties": {"start": time_string, "end": time_string, "evidence": _string(5000)},
                },
            },
            "visual_observations": {"type": "array", "maxItems": 100, "items": _string(5000)},
            **base_tail,
        }
    return {
        "type": "object",
        "additionalProperties": False,
        "required": list(properties),
        "properties": properties,
    }


def provider_response_schema(mode: YouTubeMode) -> Mapping[str, Any]:
    """Return the bounded schema using only Gemini-supported JSON Schema keywords.

    ``response_schema`` remains the stricter server-side validator.  Gemini's
    structured-output contract supports ``enum`` but not ``const``, ``pattern``
    or ``maxLength``.  Its backend also rejects this full schema as too complex
    when every server-side array/object bound is repeated in the provider
    schema.  Keep the provider shape and required fields, while enforcing
    ``maxItems`` and closed objects with the stricter server-side validator.
    """

    def normalize(value: Any) -> Any:
        if isinstance(value, Mapping):
            normalized = {
                key: normalize(child)
                for key, child in value.items()
                if key not in {"const", "pattern", "maxLength", "maxItems", "additionalProperties"}
            }
            constant = value.get("const")
            if "const" in value:
                normalized["enum"] = [normalize(constant)]
            return normalized
        if isinstance(value, list):
            return [normalize(child) for child in value]
        return value

    return normalize(response_schema(mode))
