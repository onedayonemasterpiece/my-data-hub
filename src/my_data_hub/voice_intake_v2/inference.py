# ruff: noqa: RUF001
from __future__ import annotations

import asyncio
import base64
import json
import math
import time
from pathlib import Path
from typing import Any
from urllib.parse import quote
from uuid import uuid4

from pydantic import BaseModel, ValidationError

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


class AggregateGeminiInference:
    """Two-stage Gemini adapter with exactly one generateContent POST per stage."""

    def __init__(
        self,
        settings: VoiceIntakeSettings,
        *,
        limiter: VoiceLimiter | None = None,
        requester: BoundedJSONRequester | None = None,
        clock=time.monotonic,
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
            max_output_tokens=8192, consumer="my-data-hub.voice-intake.transcribe.v2",
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
            preflight=preflight,
        )

    async def _preflight(self):
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
        preflight=None,
    ) -> InferenceReceipt:
        model = self.settings.model
        if model not in self.settings.allowed_models or "flash-lite" not in model.lower():
            raise StageFailure("unsupported_voice_model", sent=False)
        request_uid = str(uuid4())
        preflight = preflight or await self._preflight()
        lease = None
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
            usage = self._usage(response.json_body)
            if not 200 <= response.status < 300:
                if response.status == 429:
                    await self.limiter.report_provider_429(lease, retry_after_ms=None)
                raise StageFailure(
                    "provider_429" if response.status == 429 else "provider_rejected_request",
                    sent=True, retryable=response.status == 429 or response.status >= 500,
                )
            value = output_type.model_validate(self._json_value(response.json_body))
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
            raise StageFailure("response_schema_invalid", sent=True, retryable=False) from exc
        await self._finalize(lease, started, usage, "succeeded", None)
        public = self.limiter.public_lease(lease, actual_tpm=usage.total_tokens if usage else None)
        return InferenceReceipt(value=value.model_dump(mode="json"), request_uid=request_uid, limiter=public)

    async def _finalize(
        self, lease, started: float, usage: ModelUsage | None, status: str, error: str | None
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
