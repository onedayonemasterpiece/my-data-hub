from __future__ import annotations

import asyncio
import base64
import json
import math
import re
import time
from collections.abc import Mapping
from typing import Any, TypeVar
from urllib.parse import quote
from uuid import uuid4

from pydantic import BaseModel, ValidationError

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.http import (
    AiohttpBoundedJSONRequester,
    BoundedHTTPError,
    BoundedHTTPResponse,
    BoundedJSONRequester,
)
from my_data_hub.google_ai.limiter import (
    BUCKET_STRATEGY,
    LIMITER_CONTRACT,
    SupabaseGoogleAILimiter,
)

from .contracts import (
    ModelUsage,
    SUMMARY_JSON_SCHEMA,
    TRANSCRIPT_JSON_SCHEMA,
    ChunkTranscriptResponse,
    SessionSummaryResponse,
    SummaryPayload,
    TranscriptChunk,
    TranscriptPayload,
)
from .errors import VoiceIntakeError
from .settings import VoiceIntakeSettings

T = TypeVar("T", bound=BaseModel)

# Gemini documents audio admission at roughly 32 tokens/second. Keep a small
# safety margin so duration rounding or prompt overhead cannot under-reserve.
AUDIO_TOKENS_PER_SECOND = 35
# Russian text commonly tokenizes more densely than English. Two Unicode
# characters per token is intentionally conservative without adding a second
# provider countTokens request to every recording operation.
TEXT_CHARACTERS_PER_TOKEN = 2

TRANSCRIBE_PROMPT = """Ты выполняешь максимально точную расшифровку русского голосового фрагмента владельца IdeaHub.
Верни только JSON по заданной схеме.

Правила:
- передай все содержательные слова, перечисления и самокоррекции;
- не превращай расшифровку в пересказ;
- сохраняй названия продуктов, репозиториев, организаций, фамилии, даты и числа;
- не добавляй фактов, которых нет в аудио;
- сомнительное место оставь как [неразборчиво] и перечисли в uncertain_fragments;
- это один чанк более длинной сессии: не придумывай вступление или завершение;
- язык transcript — русский, language — ru-RU.
"""

SUMMARY_PROMPT = """Ниже находится полная упорядоченная расшифровка одной рабочей голосовой сессии владельца IdeaHub.
Верни только JSON по заданной схеме и подготовь подробную доказательную выжимку.

Правила:
- не теряй уникальные идеи, даже упомянутые вскользь;
- не выдавай гипотезу или размышление за принятое решение;
- не придумывай сроки, ответственных, факты или связи;
- отделяй задачи от идей, решения от предложений, факты от интерпретаций;
- owner и deadline в tasks оставляй null, если они не были явно названы;
- explicitly_stated=true только для прямо сформулированной задачи;
- сохраняй противоречия и неуверенность;
- title должен быть конкретным и пригодным как заголовок Markdown;
- detailed_summary должен позволять следующему агенту понять ход мысли без аудио;
- язык ответа — русский.

ПОЛНАЯ РАСШИФРОВКА:
"""


class VoiceLimiter(SupabaseGoogleAILimiter):
    """GenerateContent accounting on the canonical shared Google AI ledger."""

    async def reserve_generate_content(
        self,
        *,
        request_uid: str,
        attempt_no: int,
        model: str,
        preflight: LimiterPreflight,
        consumer: str,
        account_name: str,
        reserved_tpm: int,
    ) -> LimiterLease:
        if not 1 <= reserved_tpm <= preflight.limit.tpm:
            raise GoogleAIError(GoogleAIErrorCode.QUOTA_EXHAUSTED_TPM)
        result = await self._rpc(
            "google_ai_reserve",
            {
                "p_request_uid": request_uid,
                "p_attempt_no": attempt_no,
                "p_consumer": consumer,
                "p_account_name": account_name,
                "p_model": model,
                "p_reserved_tpm": reserved_tpm,
                "p_candidate_key_ids": list(preflight.candidate_key_ids),
            },
            attempts=2,
        )
        if not isinstance(result, Mapping):
            raise GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE, retryable=True)
        self._validate_reserve_markers(result)
        if result.get("ok") is not True:
            reason = str(result.get("blocked_reason") or "")
            retry_value = result.get("retry_after_ms")
            retry_after = (
                retry_value
                if isinstance(retry_value, int) and not isinstance(retry_value, bool) and retry_value >= 0
                else None
            )
            mapping = {
                "rpm": GoogleAIErrorCode.QUOTA_EXHAUSTED_RPM,
                "tpm": GoogleAIErrorCode.QUOTA_EXHAUSTED_TPM,
                "rpd": GoogleAIErrorCode.QUOTA_EXHAUSTED_RPD,
                "provider_429": GoogleAIErrorCode.PROVIDER_429,
                "model_not_found": GoogleAIErrorCode.MODEL_LIMIT_NOT_FOUND,
            }
            code = mapping.get(reason, GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE)
            raise GoogleAIError(
                code,
                retryable=code is not GoogleAIErrorCode.MODEL_LIMIT_NOT_FOUND,
                retry_after_ms=retry_after,
            )
        env_name = result.get("env_var_name")
        fields = (
            result.get("api_key_id"),
            env_name,
            result.get("key_alias"),
            result.get("quota_scope"),
        )
        if not all(isinstance(value, str) and value.strip() for value in fields):
            raise GoogleAIError(GoogleAIErrorCode.KEY_METADATA_MISSING)
        if env_name not in preflight.candidate_env_names:
            raise GoogleAIError(GoogleAIErrorCode.KEY_METADATA_MISSING)
        return LimiterLease(
            request_uid=request_uid,
            attempt_no=attempt_no,
            api_key_id=str(result["api_key_id"]),
            env_var_name=str(env_name),
            key_alias=str(result["key_alias"]),
            quota_scope=str(result["quota_scope"]),
            reserved_tpm=reserved_tpm,
            contract=LIMITER_CONTRACT,
            bucket_strategy=BUCKET_STRATEGY,
        )

    async def finalize_generate_content(
        self,
        lease: LimiterLease,
        *,
        usage: ModelUsage | None,
        duration_ms: int,
        provider_status: str,
        error_type: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        await self._rpc(
            "google_ai_finalize",
            {
                "p_request_uid": lease.request_uid,
                "p_attempt_no": lease.attempt_no,
                "p_usage_input_tokens": usage.input_tokens if usage else None,
                "p_usage_output_tokens": usage.output_tokens if usage else None,
                "p_usage_total_tokens": usage.total_tokens if usage else None,
                "p_duration_ms": max(0, int(duration_ms)),
                "p_provider_status": provider_status,
                "p_error_type": error_type,
                "p_error_code": error_code,
                "p_error_message": error_message[:500] if error_message else None,
            },
            attempts=3,
            allow_empty=True,
        )


class GeminiVoiceService:
    def __init__(
        self,
        settings: VoiceIntakeSettings,
        *,
        limiter: VoiceLimiter | None = None,
        requester: BoundedJSONRequester | None = None,
        clock=time.monotonic,
    ) -> None:
        self._settings = settings
        self._limiter = limiter or VoiceLimiter(
            supabase_url=settings.limiter_supabase_url,
            service_key=settings.limiter_supabase_service_key,
            candidate_env_names=settings.normal_key_envs,
        )
        self._requester = requester or AiohttpBoundedJSONRequester()
        self._clock = clock

    async def transcribe(
        self,
        *,
        session_id: str,
        chunk_index: int,
        duration_ms: int,
        audio: bytes,
    ) -> ChunkTranscriptResponse:
        value, usage, request_uid, public_limiter = await self._generate(
            prompt=TRANSCRIBE_PROMPT,
            response_schema=TRANSCRIPT_JSON_SCHEMA,
            output_type=TranscriptPayload,
            audio=audio,
            max_output_tokens=8_192,
            prompt_version="voice-transcribe-v1",
            duration_ms=duration_ms,
            consumer="my-data-hub.voice-intake.transcribe.v1",
        )
        return ChunkTranscriptResponse(
            session_id=session_id,
            chunk_index=chunk_index,
            model=self._settings.model,
            prompt_version="voice-transcribe-v1",
            transcript=value,
            usage=usage,
            request_uid=request_uid,
            limiter=public_limiter,
        )

    async def summarize(self, chunks: list[TranscriptChunk]) -> SessionSummaryResponse:
        transcript = self._render_transcript(chunks)
        value, usage, request_uid, public_limiter = await self._generate(
            prompt=SUMMARY_PROMPT + transcript,
            response_schema=SUMMARY_JSON_SCHEMA,
            output_type=SummaryPayload,
            audio=None,
            max_output_tokens=16_384,
            prompt_version="voice-summary-v1",
            duration_ms=None,
            consumer="my-data-hub.voice-intake.summarize.v1",
        )
        return SessionSummaryResponse(
            model=self._settings.model,
            prompt_version="voice-summary-v1",
            summary=value,
            usage=usage,
            request_uid=request_uid,
            limiter=public_limiter,
        )

    @staticmethod
    def _render_transcript(chunks: list[TranscriptChunk]) -> str:
        def stamp(value: int) -> str:
            total = value // 1000
            hours, remainder = divmod(total, 3600)
            minutes, seconds = divmod(remainder, 60)
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

        return "\n\n".join(
            f"[{stamp(chunk.start_ms)}–{stamp(chunk.end_ms)}]\n{chunk.transcript.transcript.strip()}"
            for chunk in chunks
        )

    @staticmethod
    def _reservation_tpm(
        *,
        preflight: LimiterPreflight,
        prompt: str,
        duration_ms: int | None,
        max_output_tokens: int,
    ) -> int:
        text_tokens = max(1, math.ceil(len(prompt) / TEXT_CHARACTERS_PER_TOKEN))
        audio_tokens = (
            math.ceil(max(0, duration_ms) / 1000 * AUDIO_TOKENS_PER_SECOND)
            if duration_ms is not None
            else 0
        )
        completion_margin = max(
            preflight.limit.tpm_reserve_extra,
            math.ceil(max_output_tokens * 0.25),
        )
        requested = text_tokens + audio_tokens + max_output_tokens + completion_margin
        if requested > preflight.limit.tpm:
            raise VoiceIntakeError(
                "voice_request_exceeds_model_tpm",
                retryable=False,
                status_code=413,
            )
        return max(1, requested)

    async def _generate(
        self,
        *,
        prompt: str,
        response_schema: Mapping[str, Any],
        output_type: type[T],
        audio: bytes | None,
        max_output_tokens: int,
        prompt_version: str,
        duration_ms: int | None,
        consumer: str,
    ) -> tuple[T, ModelUsage, str, dict[str, Any]]:
        model = self._settings.model
        if model not in self._settings.allowed_models or "flash-lite" not in model.lower():
            raise VoiceIntakeError("unsupported_voice_model", status_code=503)
        request_uid = str(uuid4())
        attempt_no = 1
        try:
            preflight = await self._limiter.preflight(model)
            reserved_tpm = self._reservation_tpm(
                preflight=preflight,
                prompt=prompt,
                duration_ms=duration_ms,
                max_output_tokens=max_output_tokens,
            )
            lease = await self._limiter.reserve_generate_content(
                request_uid=request_uid,
                attempt_no=attempt_no,
                model=model,
                preflight=preflight,
                consumer=consumer,
                account_name="record-idea-hub",
                reserved_tpm=reserved_tpm,
            )
        except GoogleAIError as exc:
            raise self._limiter_error(exc) from exc

        try:
            api_key = self._limiter.secret_for(lease)
        except GoogleAIError as exc:
            await self._release_or_reconcile(lease, "voice_key_secret_missing")
            raise self._limiter_error(exc) from exc
        try:
            await self._limiter.mark_sent(lease)
        except GoogleAIError as exc:
            await self._release_or_reconcile(lease, "voice_mark_sent_failed")
            raise self._limiter_error(exc) from exc

        started = self._clock()
        response: BoundedHTTPResponse | None = None
        try:
            parts: list[dict[str, Any]] = [{"text": prompt}]
            if audio is not None:
                parts.append(
                    {
                        "inlineData": {
                            "mimeType": "audio/wav",
                            "data": base64.b64encode(audio).decode("ascii"),
                        }
                    }
                )
            body = {
                "contents": [{"role": "user", "parts": parts}],
                "generationConfig": {
                    "maxOutputTokens": max_output_tokens,
                    "responseMimeType": "application/json",
                    "responseJsonSchema": dict(response_schema),
                },
            }
            endpoint = (
                "https://generativelanguage.googleapis.com/v1beta/models/"
                f"{quote(model, safe='-._~')}:generateContent"
            )
            response = await self._requester.request_json(
                "POST",
                endpoint,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "x-goog-api-key": api_key,
                    "X-Goog-Request-Params": f"model=models/{model}",
                },
                json_body=body,
                timeout_seconds=float(self._settings.provider_timeout_seconds),
                max_response_bytes=self._settings.max_json_bytes,
            )
        except asyncio.CancelledError:
            await self._finalize_or_reconcile(
                lease,
                usage=None,
                started=started,
                provider_status="failed",
                error_type="cancelled",
                error_code="provider_cancelled",
            )
            raise
        except BoundedHTTPError as exc:
            await self._finalize_or_reconcile(
                lease,
                usage=None,
                started=started,
                provider_status="failed",
                error_type="transport",
                error_code=exc.kind,
            )
            code = "provider_timeout" if "timeout" in exc.kind else "provider_network_error"
            raise VoiceIntakeError(
                code,
                retryable=False,
                status_code=504 if code == "provider_timeout" else 502,
                reconciliation_required=True,
            ) from exc

        assert response is not None
        usage = self._usage(response.json_body)
        if response.status == 429:
            retry_seconds = self._retry_after_seconds(response)
            try:
                await self._limiter.report_provider_429(
                    lease, retry_after_ms=retry_seconds * 1000 if retry_seconds else None
                )
            finally:
                await self._finalize_or_reconcile(
                    lease,
                    usage=usage,
                    started=started,
                    provider_status="failed",
                    error_type="provider",
                    error_code="provider_429",
                )
            raise VoiceIntakeError(
                "provider_429",
                retryable=True,
                retry_after_seconds=retry_seconds or 60,
                status_code=429,
            )
        if not 200 <= response.status < 300:
            await self._finalize_or_reconcile(
                lease,
                usage=usage,
                started=started,
                provider_status="failed",
                error_type="provider",
                error_code=f"http_{response.status}",
            )
            raise VoiceIntakeError(
                "provider_rejected_request" if response.status < 500 else "provider_unavailable",
                retryable=response.status >= 500,
                status_code=502,
            )

        try:
            text = self._response_text(response.json_body)
            value = output_type.model_validate(self._parse_json(text))
        except (ValueError, TypeError, ValidationError) as exc:
            await self._finalize_or_reconcile(
                lease,
                usage=usage,
                started=started,
                provider_status="failed",
                error_type="schema",
                error_code="response_schema_invalid",
            )
            raise VoiceIntakeError("response_schema_invalid", retryable=False, status_code=502) from exc

        await self._finalize_or_reconcile(
            lease,
            usage=usage,
            started=started,
            provider_status="succeeded",
        )
        public_limiter = self._limiter.public_lease(lease, actual_tpm=usage.total_tokens)
        if duration_ms is not None:
            public_limiter["audio_duration_ms"] = max(0, duration_ms)
        public_limiter["prompt_version"] = prompt_version
        return value, usage, request_uid, public_limiter

    async def _release_or_reconcile(self, lease: LimiterLease, reason: str) -> None:
        try:
            await asyncio.shield(self._limiter.release_unsent(lease, reason=reason))
        except Exception as exc:
            raise VoiceIntakeError(
                "limiter_reconciliation_required",
                retryable=False,
                status_code=503,
                reconciliation_required=True,
            ) from exc

    async def _finalize_or_reconcile(
        self,
        lease: LimiterLease,
        *,
        usage: ModelUsage | None,
        started: float,
        provider_status: str,
        error_type: str | None = None,
        error_code: str | None = None,
    ) -> None:
        try:
            await asyncio.shield(
                self._limiter.finalize_generate_content(
                    lease,
                    usage=usage,
                    duration_ms=int((self._clock() - started) * 1000),
                    provider_status=provider_status,
                    error_type=error_type,
                    error_code=error_code,
                    error_message=error_code,
                )
            )
        except Exception as exc:
            raise VoiceIntakeError(
                "limiter_finalization_failed",
                retryable=False,
                status_code=503,
                reconciliation_required=True,
            ) from exc

    @staticmethod
    def _limiter_error(exc: GoogleAIError) -> VoiceIntakeError:
        code = exc.code.value
        retry_after = math.ceil((exc.retry_after_ms or 0) / 1000) or None
        is_quota = code in {
            "quota_exhausted_rpm",
            "quota_exhausted_tpm",
            "quota_exhausted_rpd",
            "provider_429",
        }
        return VoiceIntakeError(
            code,
            retryable=bool(exc.retryable or is_quota),
            retry_after_seconds=retry_after,
            status_code=429 if is_quota else 503,
            reconciliation_required=exc.reconciliation_required,
        )

    @staticmethod
    def _parse_json(text: str) -> dict[str, Any]:
        cleaned = text.strip()
        fenced = re.fullmatch(r"```(?:json)?\s*(.*?)\s*```", cleaned, flags=re.I | re.S)
        if fenced:
            cleaned = fenced.group(1)
        value = json.loads(cleaned)
        if not isinstance(value, dict):
            raise ValueError("provider output is not an object")
        return value

    @staticmethod
    def _response_text(body: Any) -> str:
        if not isinstance(body, Mapping):
            raise ValueError("provider response is not an object")
        candidates = body.get("candidates")
        if not isinstance(candidates, list) or not candidates or not isinstance(candidates[0], Mapping):
            raise ValueError("provider candidates are missing")
        candidate = candidates[0]
        finish_reason = str(candidate.get("finishReason") or candidate.get("finish_reason") or "")
        if finish_reason and finish_reason.upper() != "STOP":
            raise ValueError(f"provider finish reason is {finish_reason}")
        content = candidate.get("content")
        if not isinstance(content, Mapping):
            raise ValueError("provider content is missing")
        parts = content.get("parts")
        if not isinstance(parts, list):
            raise ValueError("provider parts are missing")
        texts = [
            str(part["text"])
            for part in parts
            if isinstance(part, Mapping) and not part.get("thought") and isinstance(part.get("text"), str)
        ]
        text = "\n".join(value.strip() for value in texts if value.strip()).strip()
        if not text:
            raise ValueError("provider returned empty output")
        return text

    @staticmethod
    def _usage(body: Any) -> ModelUsage:
        metadata = body.get("usageMetadata", {}) if isinstance(body, Mapping) else {}
        if not isinstance(metadata, Mapping):
            metadata = {}

        def integer(*names: str) -> int:
            for name in names:
                value = metadata.get(name)
                if isinstance(value, int) and not isinstance(value, bool) and value >= 0:
                    return value
            return 0

        input_tokens = integer("promptTokenCount", "prompt_token_count")
        output_tokens = integer("candidatesTokenCount", "candidates_token_count")
        thought_tokens = integer("thoughtsTokenCount", "thoughts_token_count")
        total_tokens = integer("totalTokenCount", "total_token_count")
        if total_tokens == 0:
            total_tokens = input_tokens + output_tokens + thought_tokens
        return ModelUsage(
            input_tokens=input_tokens,
            output_tokens=output_tokens,
            thought_tokens=thought_tokens,
            total_tokens=total_tokens,
        )

    @staticmethod
    def _retry_after_seconds(response: BoundedHTTPResponse) -> int | None:
        if response.retry_after:
            try:
                return max(1, min(86_400, int(float(response.retry_after))))
            except ValueError:
                pass
        body = response.json_body
        if isinstance(body, Mapping):
            error = body.get("error")
            if isinstance(error, Mapping):
                details = error.get("details")
                if isinstance(details, list):
                    for detail in details:
                        if not isinstance(detail, Mapping):
                            continue
                        raw = detail.get("retryDelay") or detail.get("retry_delay")
                        if isinstance(raw, str) and raw.endswith("s"):
                            try:
                                return max(1, min(86_400, math.ceil(float(raw[:-1]))))
                            except ValueError:
                                continue
        return None
