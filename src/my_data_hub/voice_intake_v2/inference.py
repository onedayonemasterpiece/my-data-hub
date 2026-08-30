# ruff: noqa: RUF001
from __future__ import annotations

import asyncio
import base64
import json
import math
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight
from my_data_hub.google_ai.errors import GoogleAIError
from my_data_hub.google_ai.http import (
    AiohttpBoundedJSONRequester,
    BoundedHTTPError,
    BoundedJSONRequester,
)
from my_data_hub.voice_intake.contracts import (
    SUMMARY_JSON_SCHEMA,
    TRANSCRIPT_JSON_SCHEMA,
    ModelUsage,
    SummaryPayload,
    TranscriptPayload,
)
from my_data_hub.voice_intake.gemini import (
    SUMMARY_PROMPT,
    VoiceLimiter,
    _with_terminology,
)
from my_data_hub.voice_intake.settings import VoiceIntakeSettings

from .contracts import InferenceReceipt
from .worker import StageFailure

AGGREGATE_TRANSCRIBE_PROMPT = """Ты выполняешь максимально точную расшифровку полной упорядоченной
голосовой сессии владельца IdeaHub.
Верни только JSON по заданной схеме. Сохрани все содержательные слова, перечисления и самокоррекции, не превращай
расшифровку в пересказ, не добавляй фактов. Сверяй собственные имена с авторитетной карточкой терминологии,
но нормализуй только при акустической и контекстной совместимости. Сомнительные места отмечай как [неразборчиво]
и перечисляй в uncertain_fragments. Язык transcript — русский, language — ru-RU.
"""

STRUCTURED_RESPONSE_SCHEMA_VERSION = "1.0.0"
TRANSCRIPT_SCHEMA_NAME = "voice_intake_transcript"
SUMMARY_SCHEMA_NAME = "voice_intake_summary"
TRANSCRIPTION_MAX_OUTPUT_TOKENS = 32_768
_MISSING = object()


class AggregateGeminiInference:
    """Two-stage Gemini adapter with exactly one generateContent POST per stage."""

    def __init__(
        self,
        settings: VoiceIntakeSettings,
        *,
        limiter: VoiceLimiter | None = None,
        requester: BoundedJSONRequester | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.settings = settings
        self.limiter = limiter or VoiceLimiter(
            supabase_url=settings.limiter_supabase_url,
            service_key=settings.limiter_supabase_service_key,
            candidate_env_names=settings.normal_key_envs,
        )
        self.requester = requester or AiohttpBoundedJSONRequester()
        self.clock = clock

    async def transcribe(
        self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any]
    ) -> InferenceReceipt:
        audio = audio_path.read_bytes()
        # Documented Gemini audio admission is 32 tokens/second. This uses
        # recorded audio, never wall elapsed time or the entire model TPM.
        reserve = max(1, math.ceil(recorded_audio_ms / 1000 * 32))
        return await self._generate(
            prompt=_with_terminology(AGGREGATE_TRANSCRIBE_PROMPT, str(terminology.get("prompt", ""))),
            schema=TRANSCRIPT_JSON_SCHEMA, output_type=TranscriptPayload,
            audio=audio, audio_mime_type="audio/mpeg", reserved_tpm=reserve,
            max_output_tokens=TRANSCRIPTION_MAX_OUTPUT_TOKENS,
            consumer="my-data-hub.voice-intake.transcribe.v2",
            schema_name=TRANSCRIPT_SCHEMA_NAME,
        )

    async def summarize(
        self, *, transcript: dict[str, Any], terminology: dict[str, Any]
    ) -> InferenceReceipt:
        rendered = json.dumps(transcript, ensure_ascii=False, sort_keys=True)
        prompt = (
            _with_terminology(SUMMARY_PROMPT, str(terminology.get("prompt", "")))
            + "\nПОЛНАЯ РАСШИФРОВКА:\n" + rendered
        )
        preflight = await self._preflight()
        reserve = max(1, math.ceil(len(prompt) / 2) + 16_384 + preflight.limit.tpm_reserve_extra)
        if reserve > preflight.limit.tpm:
            raise StageFailure("voice_request_exceeds_model_tpm", sent=False, retryable=False)
        return await self._generate(
            prompt=prompt, schema=SUMMARY_JSON_SCHEMA, output_type=SummaryPayload,
            audio=None, audio_mime_type=None, reserved_tpm=reserve,
            max_output_tokens=16_384, consumer="my-data-hub.voice-intake.summarize.v2",
            preflight=preflight, schema_name=SUMMARY_SCHEMA_NAME,
        )

    async def _preflight(self) -> LimiterPreflight:
        try:
            return await self.limiter.preflight(self.settings.model)
        except GoogleAIError as exc:
            raise StageFailure(
                exc.code.value, sent=False, retryable=exc.retryable,
                retry_after_seconds=math.ceil((exc.retry_after_ms or 0) / 1000) or None,
            ) from exc

    async def _generate(
        self,
        *,
        prompt: str,
        schema: dict[str, Any],
        output_type: type[BaseModel],
        audio: bytes | None,
        audio_mime_type: str | None,
        reserved_tpm: int,
        max_output_tokens: int,
        consumer: str,
        schema_name: str,
        preflight: LimiterPreflight | None = None,
    ) -> InferenceReceipt:
        model = self.settings.model
        if model not in self.settings.allowed_models or "flash-lite" not in model.lower():
            raise StageFailure("unsupported_voice_model", sent=False)
        request_uid = str(uuid4())
        preflight = preflight or await self._preflight()
        lease: LimiterLease | None = None
        marked_sent = False
        try:
            lease = await self.limiter.reserve_generate_content(
                request_uid=request_uid, attempt_no=1, model=model, preflight=preflight,
                consumer=consumer, account_name="record-idea-hub", reserved_tpm=reserved_tpm,
            )
            api_key = self.limiter.secret_for(lease)
            await self.limiter.mark_sent(lease)
            marked_sent = True
        except GoogleAIError as exc:
            if lease is not None and not marked_sent:
                try:
                    await self.limiter.release_unsent(lease, reason="voice_v2_pre_send_failure")
                except Exception as release_exc:
                    raise StageFailure(
                        "limiter_reconciliation_required", sent=False, ambiguous=True
                    ) from release_exc
            raise StageFailure(
                exc.code.value, sent=False, retryable=exc.retryable,
                retry_after_seconds=math.ceil((exc.retry_after_ms or 0) / 1000) or None,
            ) from exc
        parts: list[dict[str, Any]] = [{"text": prompt}]
        if audio is not None:
            parts.append({"inlineData": {
                "mimeType": audio_mime_type, "data": base64.b64encode(audio).decode("ascii"),
            }})
        started = self.clock()
        usage: ModelUsage | None = None
        response_body: dict[str, Any] | None = None
        finish_reason = "UNSPECIFIED"
        parsed_value: Any = _MISSING
        try:
            response = await self.requester.request_json(
                "POST",
                "https://generativelanguage.googleapis.com/v1beta/models/"
                f"{quote(model, safe='-._~')}:generateContent",
                headers={
                    "Content-Type": "application/json", "Accept": "application/json",
                    "x-goog-api-key": api_key, "X-Goog-Request-Params": f"model=models/{model}",
                },
                json_body={
                    "contents": [{"role": "user", "parts": parts}],
                    "generationConfig": {
                        "maxOutputTokens": max_output_tokens,
                        "responseMimeType": "application/json", "responseJsonSchema": schema,
                    },
                },
                timeout_seconds=float(self.settings.provider_timeout_seconds),
                max_response_bytes=self.settings.max_json_bytes,
            )
            response_body = response.json_body
            usage = self._usage(response_body)
            if not 200 <= response.status < 300:
                if response.status == 429:
                    await self.limiter.report_provider_429(lease, retry_after_ms=None)
                raise StageFailure(
                    "provider_429" if response.status == 429 else "provider_rejected_request",
                    sent=True, retryable=response.status == 429 or response.status >= 500,
                )
            finish_reason = self._finish_reason(response_body)
            if finish_reason not in {"STOP", "UNSPECIFIED"}:
                raise StageFailure(
                    "response_schema_invalid",
                    sent=True,
                    retryable=finish_reason == "MAX_TOKENS",
                    diagnostics=self._validation_diagnostics(
                        schema=schema,
                        schema_name=schema_name,
                        error=None,
                        parsed_value=_MISSING,
                        response_body=response_body,
                        finish_reason=finish_reason,
                        usage=usage,
                        max_output_tokens=max_output_tokens,
                    ),
                )
            parsed_value = self._json_value(response_body)
            value = output_type.model_validate(parsed_value)
        except StageFailure:
            await self._finalize(lease, started, usage, "failed", "provider_failure")
            raise
        except (TimeoutError, BoundedHTTPError) as exc:
            await self._finalize(lease, started, usage, "failed", "provider_outcome_ambiguous")
            raise StageFailure(
                "provider_timeout" if "timeout" in str(exc).lower() else "provider_network_error",
                sent=True, retryable=False, ambiguous=True,
            ) from exc
        except (KeyError, TypeError, ValueError, ValidationError) as exc:
            await self._finalize(lease, started, usage, "failed", "response_schema_invalid")
            raise StageFailure(
                "response_schema_invalid",
                sent=True,
                retryable=False,
                diagnostics=self._validation_diagnostics(
                    schema=schema,
                    schema_name=schema_name,
                    error=exc,
                    parsed_value=parsed_value,
                    response_body=response_body,
                    finish_reason=finish_reason,
                    usage=usage,
                    max_output_tokens=max_output_tokens,
                ),
            ) from exc
        await self._finalize(lease, started, usage, "succeeded", None)
        public = self.limiter.public_lease(lease, actual_tpm=usage.total_tokens if usage else None)
        return InferenceReceipt(value=value.model_dump(mode="json"), request_uid=request_uid, limiter=public)

    async def _finalize(
        self, lease: LimiterLease, started: float, usage: ModelUsage | None, status: str, error: str | None
    ) -> None:
        try:
            await asyncio.shield(self.limiter.finalize_generate_content(
                lease, usage=usage, duration_ms=int((self.clock() - started) * 1000),
                provider_status=status, error_type="provider" if error else None,
                error_code=error, error_message=error,
            ))
        except Exception as exc:
            raise StageFailure("limiter_finalization_failed", sent=True, ambiguous=True) from exc

    @staticmethod
    def _usage(value: dict[str, Any]) -> ModelUsage | None:
        raw = value.get("usageMetadata")
        if not isinstance(raw, dict):
            return None
        return ModelUsage(
            input_tokens=max(0, int(raw.get("promptTokenCount", 0))),
            output_tokens=max(0, int(raw.get("candidatesTokenCount", 0))),
            thought_tokens=max(0, int(raw.get("thoughtsTokenCount", 0))),
            total_tokens=max(0, int(raw.get("totalTokenCount", 0))),
        )

    @staticmethod
    def _json_value(value: dict[str, Any]) -> Any:
        text = value["candidates"][0]["content"]["parts"][0]["text"]
        if not isinstance(text, str) or len(text) > 2_000_000:
            raise ValueError("provider response text invalid")
        return json.loads(text)

    @staticmethod
    def _finish_reason(value: Mapping[str, Any]) -> str:
        candidates = value.get("candidates")
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            return "UNSPECIFIED"
        if not candidates or not isinstance(candidates[0], Mapping):
            return "UNSPECIFIED"
        candidate = candidates[0]
        reason = candidate.get("finishReason") or candidate.get("finish_reason")
        return str(reason).strip().upper() if reason else "UNSPECIFIED"

    @classmethod
    def _validation_diagnostics(
        cls,
        *,
        schema: Mapping[str, Any],
        schema_name: str,
        error: Exception | None,
        parsed_value: Any,
        response_body: Mapping[str, Any] | None,
        finish_reason: str,
        usage: ModelUsage | None,
        max_output_tokens: int,
    ) -> dict[str, Any]:
        path: tuple[str | int, ...] = ()
        constraint = "complete_valid_json_matching_schema"
        expected_type: Any = schema.get("type", "schema")
        missing_fields: list[str] = []
        extra_fields: list[str] = []
        actual = cls._response_text_shape(response_body)

        if isinstance(error, ValidationError):
            errors = error.errors(include_input=False, include_url=False)
            if errors:
                first = errors[0]
                path = tuple(value for value in first.get("loc", ()) if isinstance(value, (str, int)))
                constraint = str(first.get("type") or "schema_constraint")
                expected_type = cls._schema_type_at_path(schema, path)
                actual = cls._shape(cls._value_at_path(parsed_value, path))
            missing_fields = sorted({
                cls._field_name(item.get("loc", ()))
                for item in errors
                if item.get("type") == "missing" and cls._field_name(item.get("loc", ()))
            })
            extra_fields = sorted({
                cls._field_name(item.get("loc", ()))
                for item in errors
                if item.get("type") == "extra_forbidden" and cls._field_name(item.get("loc", ()))
            })
        elif isinstance(error, json.JSONDecodeError):
            constraint = "valid_json_object"
        elif error is not None:
            constraint = "provider_response_structure"

        token_counts = {
            "input": usage.input_tokens if usage else None,
            "output": usage.output_tokens if usage else None,
            "thought": usage.thought_tokens if usage else None,
            "total": usage.total_tokens if usage else None,
        }
        return {
            "schema": schema_name,
            "schema_version": STRUCTURED_RESPONSE_SCHEMA_VERSION,
            "json_path": cls._json_path(path),
            "expected": {"type": expected_type, "constraint": constraint},
            "actual": actual,
            "missing_fields": missing_fields,
            "extra_fields": extra_fields,
            "finish_reason": finish_reason,
            "token_counts": token_counts,
            "configured_max_output_tokens": max_output_tokens,
            "truncated": finish_reason == "MAX_TOKENS",
        }

    @staticmethod
    def _response_text_shape(value: Mapping[str, Any] | None) -> dict[str, Any]:
        try:
            assert value is not None
            text = value["candidates"][0]["content"]["parts"][0]["text"]
        except (AssertionError, KeyError, IndexError, TypeError):
            return AggregateGeminiInference._shape(value)
        return AggregateGeminiInference._shape(text)

    @staticmethod
    def _shape(value: Any) -> dict[str, Any]:
        if value is _MISSING:
            return {"type": "missing", "shape": {}}
        if value is None:
            return {"type": "null", "shape": {}}
        if isinstance(value, bool):
            return {"type": "boolean", "shape": {}}
        if isinstance(value, int):
            return {"type": "integer", "shape": {}}
        if isinstance(value, float):
            return {"type": "number", "shape": {}}
        if isinstance(value, str):
            return {"type": "string", "shape": {"characters": len(value)}}
        if isinstance(value, Mapping):
            return {"type": "object", "shape": {"fields": len(value)}}
        if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
            return {"type": "array", "shape": {"items": len(value)}}
        return {"type": type(value).__name__, "shape": {}}

    @staticmethod
    def _value_at_path(value: Any, path: tuple[str | int, ...]) -> Any:
        current = value
        for part in path:
            if isinstance(part, str):
                if not isinstance(current, Mapping) or part not in current:
                    return _MISSING
            elif isinstance(part, int):
                if (
                    not isinstance(current, Sequence)
                    or isinstance(current, (str, bytes))
                    or not 0 <= part < len(current)
                ):
                    return _MISSING
            else:
                return _MISSING
            current = current[part]
        return current

    @staticmethod
    def _schema_type_at_path(schema: Mapping[str, Any], path: tuple[str | int, ...]) -> Any:
        current: Any = schema
        for part in path:
            if isinstance(part, str) and isinstance(current, Mapping):
                properties = current.get("properties")
                if not isinstance(properties, Mapping) or part not in properties:
                    return "forbidden"
                current = properties[part]
            elif isinstance(part, int) and isinstance(current, Mapping):
                current = current.get("items", {})
            else:
                return "schema"
        return current.get("type", "schema") if isinstance(current, Mapping) else "schema"

    @staticmethod
    def _field_name(path: Any) -> str:
        if not isinstance(path, (tuple, list)) or not path:
            return ""
        value = path[-1]
        return str(value)[:128] if isinstance(value, (str, int)) else ""

    @staticmethod
    def _json_path(path: tuple[str | int, ...]) -> str:
        rendered = "$"
        for part in path:
            if isinstance(part, int):
                rendered += f"[{part}]"
            elif part.isidentifier():
                rendered += f".{part}"
            else:
                rendered += f"[{json.dumps(part[:128])}]"
        return rendered
