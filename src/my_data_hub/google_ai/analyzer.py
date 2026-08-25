from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from jsonschema import ValidationError, validate
from pydantic import ValidationError as PydanticValidationError

from my_data_hub.google_ai.contracts import (
    LimiterLease,
    ProviderInteraction,
    ProviderUsage,
    ThinkingLevel,
    YouTubeAnalyzeRequest,
    YouTubeMode,
)
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.interactions import GeminiInteractionsClient, ProviderTransportFailure
from my_data_hub.google_ai.limiter import SupabaseGoogleAILimiter
from my_data_hub.google_ai.youtube import normalize_youtube_url, response_schema


@dataclass(frozen=True, slots=True)
class YouTubeAnalyzerConfig:
    enabled: bool
    default_model: str
    allowed_models: frozenset[str]
    max_output_tokens: int
    max_result_bytes: int = 1_048_576
    consumer: str = "my-data-hub-mcp-youtube"
    account_name: str = "my-data-hub"


class GeminiYouTubeAnalyzer:
    def __init__(
        self,
        *,
        config: YouTubeAnalyzerConfig,
        limiter: SupabaseGoogleAILimiter,
        interactions: GeminiInteractionsClient,
        clock=time.monotonic,
    ) -> None:
        self._config = config
        self._limiter = limiter
        self._interactions = interactions
        self._clock = clock

    async def analyze(
        self,
        arguments: Mapping[str, Any] | YouTubeAnalyzeRequest,
    ) -> Mapping[str, Any]:
        if not self._config.enabled:
            raise GoogleAIError(GoogleAIErrorCode.FEATURE_DISABLED)
        try:
            request = (
                arguments
                if isinstance(arguments, YouTubeAnalyzeRequest)
                else YouTubeAnalyzeRequest.model_validate(arguments)
            )
        except PydanticValidationError as exc:
            raise GoogleAIError(GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID) from exc
        normalized = normalize_youtube_url(request.youtube_url)
        model = request.model or self._config.default_model
        if model not in self._config.allowed_models:
            raise GoogleAIError(GoogleAIErrorCode.UNSUPPORTED_MODEL)
        if request.max_output_tokens > self._config.max_output_tokens:
            raise GoogleAIError(GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID)
        if model == "gemini-3.7-flash" and request.thinking_level is ThinkingLevel.MINIMAL:
            raise GoogleAIError(GoogleAIErrorCode.UNSUPPORTED_THINKING_LEVEL)

        request_uid = str(uuid4())
        attempt_no = 1
        preflight = await self._limiter.preflight(model)
        lease = await self._limiter.reserve(
            request_uid=request_uid,
            attempt_no=attempt_no,
            model=model,
            preflight=preflight,
            consumer=self._config.consumer,
            account_name=self._config.account_name,
        )
        try:
            api_key = self._limiter.secret_for(lease)
        except GoogleAIError:
            await self._release_or_reconcile(lease, reason="key_secret_missing")
            raise
        try:
            await self._limiter.mark_sent(lease)
        except GoogleAIError:
            await self._release_or_reconcile(lease, reason="mark_sent_failed")
            raise

        started = self._clock()
        started_interaction_id: str | None = None
        interaction_started_confirmed = False

        async def interaction_started(interaction_id: str, provider_status: str) -> None:
            nonlocal interaction_started_confirmed, started_interaction_id
            started_interaction_id = interaction_id
            await self._limiter.mark_interaction_started(
                lease,
                interaction_id=interaction_id,
                provider_status=provider_status,
            )
            interaction_started_confirmed = True

        try:
            interaction = await self._interactions.create(
                api_key=api_key,
                canonical_youtube_url=normalized.canonical_url,
                request=request,
                model=model,
                on_interaction_started=interaction_started,
            )
        except asyncio.CancelledError as exc:
            if started_interaction_id is not None and not interaction_started_confirmed:
                raise GoogleAIError(
                    GoogleAIErrorCode.RECONCILIATION_REQUIRED,
                    retryable=False,
                    request_uid=request_uid,
                    interaction_id=started_interaction_id,
                    provider_status="created",
                    reconciliation_required=True,
                    warnings=("interaction_started_accounting_requires_reconciliation",),
                ) from exc
            duration_ms = int((self._clock() - started) * 1000)
            await self._finalize_or_raise(
                lease,
                interaction_id=started_interaction_id,
                provider_terminal_status="cancelled",
                semantic_status="not_evaluated",
                usage=None,
                duration_ms=duration_ms,
                error_type="cancelled",
                error_code="provider_cancelled",
                error_message="provider request cancelled after send",
            )
            raise
        except ProviderTransportFailure as exc:
            duration_ms = int((self._clock() - started) * 1000)
            code, retryable = self._transport_error(exc.kind)
            interaction_id = exc.interaction_id or started_interaction_id
            if exc.kind == "interaction_started_accounting_failed":
                raise GoogleAIError(
                    GoogleAIErrorCode.RECONCILIATION_REQUIRED,
                    retryable=False,
                    request_uid=request_uid,
                    interaction_id=interaction_id,
                    provider_status=exc.provider_status,
                    reconciliation_required=True,
                    warnings=("interaction_started_accounting_requires_reconciliation",),
                ) from exc
            await self._finalize_or_raise(
                lease,
                interaction_id=interaction_id,
                provider_terminal_status="incomplete",
                semantic_status="not_evaluated",
                usage=None,
                duration_ms=duration_ms,
                error_type="transport",
                error_code=code.value,
                error_message=code.value,
            )
            raise GoogleAIError(
                code,
                retryable=retryable,
                request_uid=request_uid,
                interaction_id=interaction_id,
                provider_status="incomplete",
                reconciliation_required=True,
                warnings=("provider_outcome_ambiguous_no_retry",),
            ) from exc

        duration_ms = int((self._clock() - started) * 1000)
        await self._handle_provider_failure(lease, interaction, duration_ms=duration_ms)
        if interaction.status != "completed":
            await self._finalize_or_raise(
                lease,
                interaction_id=interaction.interaction_id,
                provider_terminal_status=(
                    interaction.status
                    if interaction.status in {"failed", "cancelled", "incomplete", "budget_exceeded"}
                    else "incomplete"
                ),
                semantic_status="not_evaluated",
                usage=interaction.usage,
                duration_ms=duration_ms,
                error_type="provider",
                error_code="interaction_incomplete",
                error_message="interaction did not complete synchronously",
            )
            raise GoogleAIError(
                GoogleAIErrorCode.INTERACTION_INCOMPLETE,
                retryable=False,
                request_uid=request_uid,
                interaction_id=interaction.interaction_id,
                provider_status=interaction.status,
            )
        if interaction.usage is None:
            await self._finalize_or_raise(
                lease,
                interaction_id=interaction.interaction_id,
                provider_terminal_status="completed",
                semantic_status="failed",
                usage=None,
                duration_ms=duration_ms,
                error_type="schema",
                error_code="usage_missing",
                error_message="provider usage missing",
            )
            raise GoogleAIError(
                GoogleAIErrorCode.USAGE_MISSING,
                request_uid=request_uid,
                interaction_id=interaction.interaction_id,
                provider_status=interaction.status,
            )
        if interaction.structured_output is None:
            await self._semantic_failure(
                lease,
                interaction,
                duration_ms=duration_ms,
                code=GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID,
            )
        try:
            validate(instance=interaction.structured_output, schema=response_schema(request.mode))
        except ValidationError as exc:
            await self._semantic_failure(
                lease,
                interaction,
                duration_ms=duration_ms,
                code=GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID,
            )
            raise AssertionError("unreachable") from exc

        result = self._success_result(
            request_uid=request_uid,
            request=request,
            normalized_url=normalized.canonical_url,
            video_id=normalized.video_id,
            requested_model=request.model,
            interaction=interaction,
            lease=lease,
        )
        encoded = json.dumps(result, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        if len(encoded) > self._config.max_result_bytes:
            await self._finalize_or_raise(
                lease,
                interaction_id=interaction.interaction_id,
                provider_terminal_status="completed",
                semantic_status="failed",
                usage=interaction.usage,
                duration_ms=duration_ms,
                error_type="schema",
                error_code=GoogleAIErrorCode.RESPONSE_TOO_LARGE.value,
                error_message=GoogleAIErrorCode.RESPONSE_TOO_LARGE.value,
            )
            raise GoogleAIError(
                GoogleAIErrorCode.RESPONSE_TOO_LARGE,
                request_uid=request_uid,
                interaction_id=interaction.interaction_id,
                provider_status=interaction.status,
                reconciliation_required=False,
            )
        await self._finalize_or_raise(
            lease,
            interaction_id=interaction.interaction_id,
            provider_terminal_status="completed",
            semantic_status="passed",
            usage=interaction.usage,
            duration_ms=duration_ms,
        )
        return result

    async def _handle_provider_failure(
        self,
        lease: LimiterLease,
        interaction: ProviderInteraction,
        *,
        duration_ms: int,
    ) -> None:
        category = interaction.provider_error_category
        if category is None:
            return
        cooldown_error: Exception | None = None
        if category == "provider_429":
            try:
                await asyncio.shield(
                    self._limiter.report_provider_429(
                        lease,
                        retry_after_ms=interaction.retry_after_ms,
                    )
                )
            except Exception as exc:
                cooldown_error = exc
        await self._finalize_or_raise(
            lease,
            interaction_id=interaction.interaction_id,
            provider_terminal_status="failed",
            semantic_status="not_evaluated",
            usage=interaction.usage,
            duration_ms=duration_ms,
            error_type="provider",
            error_code=interaction.provider_error_code or category,
            error_message=interaction.provider_error_diagnostic or category,
        )
        if cooldown_error is not None:
            raise GoogleAIError(
                GoogleAIErrorCode.RECONCILIATION_REQUIRED,
                request_uid=lease.request_uid,
                interaction_id=interaction.interaction_id,
                provider_status="failed",
                reconciliation_required=True,
                warnings=("provider_429_cooldown_requires_reconciliation",),
            ) from cooldown_error
        code = {
            "provider_429": GoogleAIErrorCode.PROVIDER_429,
            "youtube_video_not_public": GoogleAIErrorCode.YOUTUBE_VIDEO_NOT_PUBLIC,
            "provider_rejected_video": GoogleAIErrorCode.PROVIDER_REJECTED_VIDEO,
        }[category]
        raise GoogleAIError(
            code,
            retryable=code is GoogleAIErrorCode.PROVIDER_429,
            retry_after_ms=interaction.retry_after_ms,
            request_uid=lease.request_uid,
            interaction_id=interaction.interaction_id,
            provider_status="failed",
            warnings=(
                (f"provider_diagnostic:{interaction.provider_error_diagnostic}",)
                if interaction.provider_error_diagnostic
                else ()
            ),
        )

    async def _semantic_failure(
        self,
        lease: LimiterLease,
        interaction: ProviderInteraction,
        *,
        duration_ms: int,
        code: GoogleAIErrorCode,
    ) -> None:
        await self._finalize_or_raise(
            lease,
            interaction_id=interaction.interaction_id,
            provider_terminal_status="completed",
            semantic_status="failed",
            usage=interaction.usage,
            duration_ms=duration_ms,
            error_type="schema",
            error_code=code.value,
            error_message=code.value,
        )
        raise GoogleAIError(
            code,
            request_uid=lease.request_uid,
            interaction_id=interaction.interaction_id,
            provider_status=interaction.status,
        )

    async def _release_or_reconcile(self, lease: LimiterLease, *, reason: str) -> None:
        try:
            await asyncio.shield(self._limiter.release_unsent(lease, reason=reason))
        except Exception as exc:
            raise GoogleAIError(
                GoogleAIErrorCode.RECONCILIATION_REQUIRED,
                request_uid=lease.request_uid,
                reconciliation_required=True,
            ) from exc

    async def _finalize_or_raise(
        self,
        lease: LimiterLease,
        *,
        interaction_id: str | None,
        provider_terminal_status: str,
        semantic_status: str,
        usage: ProviderUsage | None,
        duration_ms: int,
        error_type: str | None = None,
        error_code: str | None = None,
        error_message: str | None = None,
    ) -> None:
        try:
            await asyncio.shield(
                self._limiter.finalize_interaction(
                    lease,
                    interaction_id=interaction_id,
                    provider_terminal_status=provider_terminal_status,
                    semantic_status=semantic_status,
                    usage=usage,
                    duration_ms=duration_ms,
                    error_type=error_type,
                    error_code=error_code,
                    error_message=error_message,
                )
            )
        except Exception as exc:
            raise GoogleAIError(
                GoogleAIErrorCode.FINALIZATION_FAILED,
                request_uid=lease.request_uid,
                interaction_id=interaction_id,
                provider_status=provider_terminal_status,
                reconciliation_required=True,
                warnings=("provider_send_accounting_requires_reconciliation",),
            ) from exc

    @staticmethod
    def _transport_error(kind: str) -> tuple[GoogleAIErrorCode, bool]:
        mapping = {
            "timeout": (GoogleAIErrorCode.PROVIDER_TIMEOUT, False),
            "connect_timeout": (GoogleAIErrorCode.PROVIDER_TIMEOUT, False),
            "first_event_timeout": (GoogleAIErrorCode.PROVIDER_TIMEOUT, False),
            "idle_timeout": (GoogleAIErrorCode.PROVIDER_TIMEOUT, False),
            "total_timeout": (GoogleAIErrorCode.PROVIDER_TIMEOUT, False),
            "stream_disconnected": (GoogleAIErrorCode.PROVIDER_NETWORK_ERROR, False),
            "network": (GoogleAIErrorCode.PROVIDER_NETWORK_ERROR, False),
            "response_too_large": (GoogleAIErrorCode.RESPONSE_TOO_LARGE, False),
            "output_too_large": (GoogleAIErrorCode.RESPONSE_TOO_LARGE, False),
            "malformed_sse": (GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID, False),
            "malformed_json": (GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID, False),
            "output_not_json": (GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID, False),
        }
        return mapping.get(kind, (GoogleAIErrorCode.PROVIDER_NETWORK_ERROR, False))

    def _success_result(
        self,
        *,
        request_uid: str,
        request: YouTubeAnalyzeRequest,
        normalized_url: str,
        video_id: str,
        requested_model: str | None,
        interaction: ProviderInteraction,
        lease: LimiterLease,
    ) -> dict[str, Any]:
        assert interaction.usage is not None
        assert interaction.structured_output is not None
        structured = dict(interaction.structured_output)
        incomplete = bool(structured.get("incomplete"))
        truncated = bool(structured.get("truncated"))
        warnings = [
            "model_generated_video_analysis_not_youtube_captions",
            "youtube_preview_video_hours_quota_not_preflighted",
            "stateless_interaction_store_false",
            "idempotency_key_is_correlation_only_tool_is_non_idempotent",
        ]
        raw_warnings = structured.get("warnings")
        if isinstance(raw_warnings, list):
            warnings.extend(str(item)[:2000] for item in raw_warnings if isinstance(item, str))
        return {
            "status": "completed",
            "request_uid": request_uid,
            "interaction_id": interaction.interaction_id,
            "provider": "google_gemini_interactions",
            "source_type": "public_youtube_url",
            "canonical_youtube_url": normalized_url,
            "youtube_video_id": video_id,
            "mode": request.mode.value,
            "model_requested": requested_model,
            "model_resolved": interaction.model,
            "structured_output": structured,
            "transcript_source": (
                "gemini_media_transcription" if request.mode is YouTubeMode.TRANSCRIPT else None
            ),
            "provider_status": interaction.status,
            "incomplete": incomplete,
            "truncated": truncated,
            "usage": {
                "total_input_tokens": interaction.usage.total_input_tokens,
                "total_output_tokens": interaction.usage.total_output_tokens,
                "total_thought_tokens": interaction.usage.total_thought_tokens,
                "total_tokens": interaction.usage.total_tokens,
                "input_tokens_by_modality": list(interaction.usage.input_tokens_by_modality),
            },
            "limiter": self._limiter.public_lease(
                lease,
                actual_tpm=interaction.usage.total_tokens,
            ),
            "retryable": False,
            "retry_after_ms": None,
            "reconciliation_required": False,
            "warnings": warnings[:100],
        }
