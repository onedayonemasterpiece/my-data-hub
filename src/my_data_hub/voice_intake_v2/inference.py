# ruff: noqa: RUF001
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import re
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

from .contracts import (
    InferenceReceipt,
    SegmentInferenceReceipt,
    SegmentPlausibilityEvidence,
    SegmentTranscriptPayload,
)
from .worker import StageFailure

SEGMENT_TRANSCRIBE_PROMPT = """Ты выполняешь максимально точную расшифровку одного ограниченного
исходного сегмента голосовой сессии владельца IdeaHub.
Верни только JSON по заданной схеме. Сохрани все содержательные слова, перечисления и самокоррекции, не превращай
расшифровку в пересказ, не добавляй фактов. Сверяй собственные имена с авторитетной карточкой терминологии,
но нормализуй только при акустической и контекстной совместимости. Сомнительные места отмечай как [неразборчиво]
и перечисляй в uncertain_fragments. Прослушай сегмент полностью от начала до конца. Поля coverage_start_ms и
coverage_end_ms должны точно повторять указанный диапазон только после обработки всего сегмента. Язык transcript —
русский, language — ru-RU.
"""

STRUCTURED_RESPONSE_SCHEMA_VERSION = "1.0.0"
TRANSCRIPT_SCHEMA_NAME = "voice_intake_transcript"
SUMMARY_SCHEMA_NAME = "voice_intake_summary"
SEGMENT_RECEIPT_SCHEMA_VERSION = "2.0.0"
SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS = 16_384
MAX_SOURCE_SEGMENT_MS = 15 * 60 * 1000
MIN_ALPHANUMERIC_PER_SPEECH_SECOND = 0.75
MIN_WORDS_PER_SPEECH_MINUTE = 10.0
MAX_TOKEN_COUNT = 10_000_000
MAX_DIAGNOSTIC_FIELDS = 32
KNOWN_PROVIDER_FINISH_REASONS = frozenset(
    {
        "STOP",
        "MAX_TOKENS",
        "SAFETY",
        "RECITATION",
        "LANGUAGE",
        "OTHER",
        "BLOCKLIST",
        "PROHIBITED_CONTENT",
        "SPII",
        "MALFORMED_FUNCTION_CALL",
        "IMAGE_SAFETY",
        "IMAGE_PROHIBITED_CONTENT",
        "NO_IMAGE",
        "IMAGE_OTHER",
    }
)
_MISSING = object()

SEGMENT_TRANSCRIPT_JSON_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        **TRANSCRIPT_JSON_SCHEMA["properties"],
        "coverage_start_ms": {"type": "integer"},
        "coverage_end_ms": {"type": "integer"},
    },
    "required": [*TRANSCRIPT_JSON_SCHEMA["required"], "coverage_start_ms", "coverage_end_ms"],
    "additionalProperties": False,
}


class AggregateGeminiInference:
    """Compatibility-named adapter for bounded segments plus one summary request."""

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

    async def transcribe_segment(
        self,
        *,
        audio_path: Path,
        source_path: Path,
        chunk_index: int,
        source_sha256: str,
        source_audio_start_ms: int,
        source_audio_end_ms: int,
        expected_speech_ms: int,
        terminology: dict[str, Any],
    ) -> SegmentInferenceReceipt:
        source_ms = source_audio_end_ms - source_audio_start_ms
        if (
            not 0 <= chunk_index <= 10_000
            or source_audio_start_ms < 0
            or not 0 < source_ms <= MAX_SOURCE_SEGMENT_MS
            or expected_speech_ms != source_ms
            or not re.fullmatch(r"[0-9a-f]{64}", source_sha256)
        ):
            raise StageFailure("segment_source_metadata_invalid", sent=False, retryable=False)
        self._verify_source_audio(source_path, source_sha256)
        audio, input_audio_sha256 = self._read_transcription_audio(audio_path)
        prompt = _with_terminology(
            SEGMENT_TRANSCRIBE_PROMPT + f"\nОЖИДАЕМЫЙ ДИАПАЗОН: [{source_audio_start_ms}, {source_audio_end_ms}) ms.\n",
            str(terminology.get("prompt", "")),
        )
        # Admission includes bounded source audio, bounded output, and the shared
        # limiter's own extra reservation. No countTokens request or hidden retry.
        preflight = await self._preflight()
        reserve = max(1, math.ceil(source_ms / 1000 * 32))
        reserve += SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS + preflight.limit.tpm_reserve_extra
        if reserve > preflight.limit.tpm:
            raise StageFailure("voice_request_exceeds_model_tpm", sent=False, retryable=False)

        evidence: SegmentPlausibilityEvidence | None = None

        def validate_segment(value: BaseModel) -> None:
            nonlocal evidence
            assert isinstance(value, SegmentTranscriptPayload)
            if (
                value.coverage_start_ms != source_audio_start_ms
                or value.coverage_end_ms != source_audio_end_ms
                or value.language != "ru-RU"
            ):
                raise StageFailure(
                    "segment_coverage_invalid",
                    sent=True,
                    retryable=False,
                    diagnostics=self._segment_diagnostics(
                        value=value,
                        source_audio_start_ms=source_audio_start_ms,
                        source_audio_end_ms=source_audio_end_ms,
                        expected_speech_ms=expected_speech_ms,
                        reason="coverage_or_language_mismatch",
                    ),
                )
            evidence = self._plausibility(value.transcript, expected_speech_ms)
            if (
                evidence.alphanumeric_per_speech_second < MIN_ALPHANUMERIC_PER_SPEECH_SECOND
                or evidence.words_per_speech_minute < MIN_WORDS_PER_SPEECH_MINUTE
            ):
                raise StageFailure(
                    "segment_content_incomplete",
                    sent=True,
                    retryable=False,
                    diagnostics=self._segment_diagnostics(
                        value=value,
                        source_audio_start_ms=source_audio_start_ms,
                        source_audio_end_ms=source_audio_end_ms,
                        expected_speech_ms=expected_speech_ms,
                        reason="duration_normalized_plausibility_failed",
                        evidence=evidence,
                    ),
                )

        generated = await self._generate(
            prompt=prompt,
            schema=SEGMENT_TRANSCRIPT_JSON_SCHEMA,
            output_type=SegmentTranscriptPayload,
            audio=audio,
            audio_mime_type="audio/mpeg",
            reserved_tpm=reserve,
            max_output_tokens=SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS,
            consumer="my-data-hub.voice-intake.transcribe.v2",
            schema_name=TRANSCRIPT_SCHEMA_NAME,
            preflight=preflight,
            value_validator=validate_segment,
        )
        assert evidence is not None and generated.usage is not None
        validated = SegmentTranscriptPayload.model_validate(generated.value)
        transcript_value = TranscriptPayload(
            transcript=validated.transcript,
            language=validated.language,
            uncertain_fragments=validated.uncertain_fragments,
        ).model_dump(mode="json")
        transcript_receipt_sha256 = hashlib.sha256(
            json.dumps(
                {
                    "schema_version": SEGMENT_RECEIPT_SCHEMA_VERSION,
                    "chunk_index": chunk_index,
                    "source_sha256": source_sha256,
                    "input_audio_sha256": input_audio_sha256,
                    "input_audio_mime_type": "audio/mpeg",
                    "source_audio_start_ms": source_audio_start_ms,
                    "source_audio_end_ms": source_audio_end_ms,
                    "value": transcript_value,
                },
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
        ).hexdigest()
        return SegmentInferenceReceipt(
            chunk_index=chunk_index,
            source_sha256=source_sha256,
            input_audio_sha256=input_audio_sha256,
            input_audio_mime_type="audio/mpeg",
            source_audio_start_ms=source_audio_start_ms,
            source_audio_end_ms=source_audio_end_ms,
            coverage_start_ms=source_audio_start_ms,
            coverage_end_ms=source_audio_end_ms,
            coverage_ms=source_ms,
            coverage_ratio=1.0,
            finish_reason="STOP",
            transcript_receipt_sha256=transcript_receipt_sha256,
            value=transcript_value,
            request_uid=generated.request_uid,
            limiter=generated.limiter,
            usage=generated.usage,
            plausibility=evidence,
        )

    def _verify_source_audio(self, source_path: Path, expected_sha256: str) -> None:
        digest = hashlib.sha256()
        observed_size = 0
        try:
            with source_path.open("rb") as handle:
                while block := handle.read(1024 * 1024):
                    observed_size += len(block)
                    if observed_size > self.settings.max_audio_bytes:
                        raise StageFailure("segment_source_too_large", sent=False, retryable=False)
                    digest.update(block)
        except StageFailure:
            raise
        except OSError as exc:
            raise StageFailure("segment_source_unavailable", sent=False, retryable=False) from exc
        if observed_size == 0 or digest.hexdigest() != expected_sha256:
            raise StageFailure("segment_source_hash_mismatch", sent=False, retryable=False)

    def _read_transcription_audio(self, audio_path: Path) -> tuple[bytes, str]:
        digest = hashlib.sha256()
        observed_size = 0
        blocks: list[bytes] = []
        try:
            with audio_path.open("rb") as handle:
                while block := handle.read(1024 * 1024):
                    observed_size += len(block)
                    if observed_size > self.settings.max_audio_bytes:
                        raise StageFailure("segment_input_too_large", sent=False, retryable=False)
                    digest.update(block)
                    blocks.append(block)
        except StageFailure:
            raise
        except OSError as exc:
            raise StageFailure("segment_input_unavailable", sent=False, retryable=False) from exc
        if observed_size == 0:
            raise StageFailure("segment_input_empty", sent=False, retryable=False)
        return b"".join(blocks), digest.hexdigest()

    @staticmethod
    def _plausibility(transcript: str, expected_speech_ms: int) -> SegmentPlausibilityEvidence:
        transcript_characters = len(transcript)
        alphanumeric_characters = sum(character.isalnum() for character in transcript)
        transcript_words = len(re.findall(r"[^\W_]+", transcript, flags=re.UNICODE))
        speech_seconds = expected_speech_ms / 1000
        return SegmentPlausibilityEvidence(
            expected_speech_ms=expected_speech_ms,
            transcript_characters=transcript_characters,
            transcript_words=transcript_words,
            alphanumeric_characters=alphanumeric_characters,
            alphanumeric_per_speech_second=round(alphanumeric_characters / speech_seconds, 6),
            words_per_speech_minute=round(transcript_words / speech_seconds * 60, 6),
        )

    @staticmethod
    def _segment_diagnostics(
        *,
        value: SegmentTranscriptPayload,
        source_audio_start_ms: int,
        source_audio_end_ms: int,
        expected_speech_ms: int,
        reason: str,
        evidence: SegmentPlausibilityEvidence | None = None,
    ) -> dict[str, Any]:
        return {
            "schema": TRANSCRIPT_SCHEMA_NAME,
            "schema_version": SEGMENT_RECEIPT_SCHEMA_VERSION,
            "json_path": "$",
            "expected": {"type": "object", "constraint": reason},
            "actual": {"type": "object", "shape": {"fields": 5}},
            "missing_fields": [],
            "extra_fields": [],
            "finish_reason": "STOP",
            "token_counts": {
                "input": None,
                "output": None,
                "thought": None,
                "total": None,
            },
            "configured_max_output_tokens": SEGMENT_TRANSCRIPTION_MAX_OUTPUT_TOKENS,
            "truncated": False,
            "coverage": {
                "expected_start_ms": source_audio_start_ms,
                "expected_end_ms": source_audio_end_ms,
                "actual_start_ms": value.coverage_start_ms,
                "actual_end_ms": value.coverage_end_ms,
            },
            "plausibility": (
                evidence.model_dump(mode="json") if evidence is not None else {"expected_speech_ms": expected_speech_ms}
            ),
        }

    async def summarize(self, *, transcript: dict[str, Any], terminology: dict[str, Any]) -> InferenceReceipt:
        rendered = json.dumps(transcript, ensure_ascii=False, sort_keys=True)
        prompt = (
            _with_terminology(SUMMARY_PROMPT, str(terminology.get("prompt", ""))) + "\nПОЛНАЯ РАСШИФРОВКА:\n" + rendered
        )
        preflight = await self._preflight()
        reserve = max(1, math.ceil(len(prompt) / 2) + 16_384 + preflight.limit.tpm_reserve_extra)
        if reserve > preflight.limit.tpm:
            raise StageFailure("voice_request_exceeds_model_tpm", sent=False, retryable=False)
        return await self._generate(
            prompt=prompt,
            schema=SUMMARY_JSON_SCHEMA,
            output_type=SummaryPayload,
            audio=None,
            audio_mime_type=None,
            reserved_tpm=reserve,
            max_output_tokens=16_384,
            consumer="my-data-hub.voice-intake.summarize.v2",
            preflight=preflight,
            schema_name=SUMMARY_SCHEMA_NAME,
        )

    async def _preflight(self) -> LimiterPreflight:
        try:
            return await self.limiter.preflight(self.settings.model)
        except GoogleAIError as exc:
            raise StageFailure(
                exc.code.value,
                sent=False,
                retryable=exc.retryable,
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
        value_validator: Callable[[BaseModel], None] | None = None,
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
                request_uid=request_uid,
                attempt_no=1,
                model=model,
                preflight=preflight,
                consumer=consumer,
                account_name="record-idea-hub",
                reserved_tpm=reserved_tpm,
            )
            api_key = self.limiter.secret_for(lease)
            await self.limiter.mark_sent(lease)
            marked_sent = True
        except GoogleAIError as exc:
            if lease is not None and not marked_sent:
                try:
                    await self.limiter.release_unsent(lease, reason="voice_v2_pre_send_failure")
                except Exception as release_exc:
                    raise StageFailure("limiter_reconciliation_required", sent=False, ambiguous=True) from release_exc
            raise StageFailure(
                exc.code.value,
                sent=False,
                retryable=exc.retryable,
                retry_after_seconds=math.ceil((exc.retry_after_ms or 0) / 1000) or None,
            ) from exc
        parts: list[dict[str, Any]] = [{"text": prompt}]
        if audio is not None:
            parts.append(
                {
                    "inlineData": {
                        "mimeType": audio_mime_type,
                        "data": base64.b64encode(audio).decode("ascii"),
                    }
                }
            )
        started = self.clock()
        usage: ModelUsage | None = None
        response_body: dict[str, Any] | None = None
        finish_reason = "MISSING"
        parsed_value: Any = _MISSING
        try:
            response = await self.requester.request_json(
                "POST",
                f"https://generativelanguage.googleapis.com/v1beta/models/{quote(model, safe='-._~')}:generateContent",
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "x-goog-api-key": api_key,
                    "X-Goog-Request-Params": f"model=models/{model}",
                },
                json_body={
                    "contents": [{"role": "user", "parts": parts}],
                    "generationConfig": {
                        "maxOutputTokens": max_output_tokens,
                        "responseMimeType": "application/json",
                        "responseJsonSchema": schema,
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
                    sent=True,
                    retryable=response.status == 429 or response.status >= 500,
                )
            candidate_count = self._candidate_count(response_body)
            if candidate_count != 1:
                raise StageFailure(
                    "provider_response_ambiguous",
                    sent=True,
                    retryable=False,
                    ambiguous=True,
                    diagnostics=self._validation_diagnostics(
                        schema=schema,
                        schema_name=schema_name,
                        error=None,
                        parsed_value=_MISSING,
                        response_body=None,
                        finish_reason="MISSING",
                        usage=usage,
                        max_output_tokens=max_output_tokens,
                    ),
                )
            if not self._has_single_text_part(response_body):
                raise StageFailure(
                    "provider_response_ambiguous",
                    sent=True,
                    retryable=False,
                    ambiguous=True,
                    diagnostics=self._validation_diagnostics(
                        schema=schema,
                        schema_name=schema_name,
                        error=None,
                        parsed_value=_MISSING,
                        response_body=None,
                        finish_reason="MISSING",
                        usage=usage,
                        max_output_tokens=max_output_tokens,
                    ),
                )
            finish_reason = self._finish_reason(response_body)
            if finish_reason != "STOP":
                known_non_success = KNOWN_PROVIDER_FINISH_REASONS - {"STOP", "OTHER"}
                raise StageFailure(
                    "response_schema_invalid",
                    sent=True,
                    retryable=finish_reason == "MAX_TOKENS",
                    ambiguous=finish_reason == "MISSING" or finish_reason not in known_non_success,
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
            if usage is None:
                raise StageFailure(
                    "provider_response_ambiguous",
                    sent=True,
                    retryable=False,
                    ambiguous=True,
                    diagnostics=self._validation_diagnostics(
                        schema=schema,
                        schema_name=schema_name,
                        error=None,
                        parsed_value=parsed_value,
                        response_body=None,
                        finish_reason=finish_reason,
                        usage=None,
                        max_output_tokens=max_output_tokens,
                    ),
                )
            if value_validator is not None:
                value_validator(value)
        except StageFailure as exc:
            if exc.diagnostics:
                exc.diagnostics["finish_reason"] = finish_reason
                exc.diagnostics["token_counts"] = {
                    "input": usage.input_tokens if usage else None,
                    "output": usage.output_tokens if usage else None,
                    "thought": usage.thought_tokens if usage else None,
                    "total": usage.total_tokens if usage else None,
                }
                exc.diagnostics["configured_max_output_tokens"] = max_output_tokens
                exc.diagnostics["truncated"] = finish_reason == "MAX_TOKENS"
            error = (
                exc.code
                if exc.code
                in {
                    "response_schema_invalid",
                    "provider_response_ambiguous",
                    "segment_coverage_invalid",
                    "segment_content_incomplete",
                }
                else "provider_failure"
            )
            await self._finalize(lease, started, usage, "failed", error)
            raise
        except (TimeoutError, BoundedHTTPError) as exc:
            await self._finalize(lease, started, usage, "failed", "provider_outcome_ambiguous")
            raise StageFailure(
                "provider_timeout" if "timeout" in str(exc).lower() else "provider_network_error",
                sent=True,
                retryable=False,
                ambiguous=True,
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
        return InferenceReceipt(
            value=value.model_dump(mode="json"),
            request_uid=request_uid,
            limiter=public,
            finish_reason="STOP",
            usage=usage,
        )

    async def _finalize(
        self, lease: LimiterLease, started: float, usage: ModelUsage | None, status: str, error: str | None
    ) -> None:
        try:
            await asyncio.shield(
                self.limiter.finalize_generate_content(
                    lease,
                    usage=usage,
                    duration_ms=int((self.clock() - started) * 1000),
                    provider_status=status,
                    error_type="provider" if error else None,
                    error_code=error,
                    error_message=error,
                )
            )
        except Exception as exc:
            raise StageFailure("limiter_finalization_failed", sent=True, ambiguous=True) from exc

    @staticmethod
    def _usage(value: dict[str, Any]) -> ModelUsage | None:
        raw = value.get("usageMetadata")
        if not isinstance(raw, dict):
            return None
        usage = ModelUsage(
            input_tokens=max(0, int(raw.get("promptTokenCount", 0))),
            output_tokens=max(0, int(raw.get("candidatesTokenCount", 0))),
            thought_tokens=max(0, int(raw.get("thoughtsTokenCount", 0))),
            total_tokens=max(0, int(raw.get("totalTokenCount", 0))),
        )
        if any(count > MAX_TOKEN_COUNT for count in usage.model_dump().values()):
            raise ValueError("provider usage is outside the bounded range")
        return usage

    @staticmethod
    def _json_value(value: dict[str, Any]) -> Any:
        candidates = value["candidates"]
        if not isinstance(candidates, list) or len(candidates) != 1 or not isinstance(candidates[0], dict):
            raise ValueError("provider candidate structure invalid")
        parts = candidates[0]["content"]["parts"]
        if not isinstance(parts, list) or len(parts) != 1 or not isinstance(parts[0], dict):
            raise ValueError("provider response parts invalid")
        text = parts[0]["text"]
        if not isinstance(text, str) or len(text) > 2_000_000:
            raise ValueError("provider response text invalid")
        return json.loads(text)

    @staticmethod
    def _candidate_count(value: Mapping[str, Any]) -> int | None:
        candidates = value.get("candidates")
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            return None
        return len(candidates)

    @staticmethod
    def _has_single_text_part(value: Mapping[str, Any]) -> bool:
        try:
            candidates = value["candidates"]
            candidate = candidates[0]
            parts = candidate["content"]["parts"]
            return (
                isinstance(parts, list)
                and len(parts) == 1
                and isinstance(parts[0], Mapping)
                and isinstance(parts[0].get("text"), str)
            )
        except (KeyError, IndexError, TypeError):
            return False

    @staticmethod
    def _finish_reason(value: Mapping[str, Any]) -> str:
        candidates = value.get("candidates")
        if not isinstance(candidates, Sequence) or isinstance(candidates, (str, bytes)):
            return "MISSING"
        if not candidates or not isinstance(candidates[0], Mapping):
            return "MISSING"
        candidate = candidates[0]
        reason = candidate.get("finishReason") or candidate.get("finish_reason")
        if reason is None:
            return "MISSING"
        if not isinstance(reason, str) or reason not in KNOWN_PROVIDER_FINISH_REASONS:
            return "UNKNOWN"
        return reason

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
            missing_fields = sorted(
                {
                    cls._field_name(item.get("loc", ()))
                    for item in errors
                    if item.get("type") == "missing" and cls._field_name(item.get("loc", ()))
                }
            )[:MAX_DIAGNOSTIC_FIELDS]
            extra_fields = sorted(
                {
                    cls._field_name(item.get("loc", ()))
                    for item in errors
                    if item.get("type") == "extra_forbidden" and cls._field_name(item.get("loc", ()))
                }
            )[:MAX_DIAGNOSTIC_FIELDS]
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
