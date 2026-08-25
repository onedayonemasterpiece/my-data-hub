from __future__ import annotations

import asyncio
from collections.abc import Mapping
from typing import Any

import pytest

from my_data_hub.google_ai.analyzer import GeminiYouTubeAnalyzer, YouTubeAnalyzerConfig
from my_data_hub.google_ai.contracts import (
    LimiterLease,
    LimiterPreflight,
    ModelLimit,
    ProviderInteraction,
    ProviderUsage,
)
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.interactions import ProviderTransportFailure

LEASE = LimiterLease(
    request_uid="00000000-0000-4000-8000-000000000001",
    attempt_no=1,
    api_key_id="key-a-id",
    env_var_name="GOOGLE_KEY_A",
    key_alias="key-a",
    quota_scope="google:project-shared",
    reserved_tpm=250000,
    contract="google_ai_project_model_atomic_v1",
    bucket_strategy="rolling_60s_pacific_day_v2",
)
PREFLIGHT = LimiterPreflight(
    limit=ModelLimit("gemini-3.6-flash", 5, 250000, 20, 1000),
    candidate_key_ids=("key-a-id",),
    candidate_env_names=frozenset({"GOOGLE_KEY_A"}),
    contract=LEASE.contract,
    bucket_strategy=LEASE.bucket_strategy,
)


def summary_output() -> dict[str, Any]:
    return {
        "summary": "A video-derived summary.",
        "timeline": [],
        "key_points": ["point"],
        "claims_to_verify": ["claim"],
        "visual_observations": ["visual"],
        "warnings": [],
        "incomplete": False,
        "truncated": False,
    }


def transcript_output() -> dict[str, Any]:
    return {
        "transcript_source": "gemini_media_transcription",
        "segments": [{"start": "00:00", "end": "00:03", "speaker": "Speaker 1", "text": "hello"}],
        "on_screen_text": [],
        "warnings": [],
        "incomplete": False,
        "truncated": False,
    }


class Limiter:
    def __init__(self, *, preflight_error: GoogleAIError | None = None, secret: str = "provider-secret") -> None:
        self.preflight_error = preflight_error
        self.secret = secret
        self.events: list[tuple[str, Any]] = []
        self.finalized: list[dict[str, Any]] = []

    async def preflight(self, model: str) -> LimiterPreflight:
        self.events.append(("preflight", model))
        if self.preflight_error:
            raise self.preflight_error
        return PREFLIGHT

    async def reserve(self, **kwargs: Any) -> LimiterLease:
        self.events.append(("reserve", kwargs))
        return LimiterLease(
            request_uid=kwargs["request_uid"],
            attempt_no=1,
            api_key_id=LEASE.api_key_id,
            env_var_name=LEASE.env_var_name,
            key_alias=LEASE.key_alias,
            quota_scope=LEASE.quota_scope,
            reserved_tpm=LEASE.reserved_tpm,
            contract=LEASE.contract,
            bucket_strategy=LEASE.bucket_strategy,
        )

    def secret_for(self, lease: LimiterLease) -> str:
        self.events.append(("secret", lease.request_uid))
        if not self.secret:
            raise GoogleAIError(GoogleAIErrorCode.KEY_SECRET_MISSING)
        return self.secret

    async def mark_sent(self, lease: LimiterLease) -> None:
        self.events.append(("sent", lease.request_uid))

    async def mark_interaction_started(
        self,
        lease: LimiterLease,
        *,
        interaction_id: str,
        provider_status: str,
    ) -> None:
        self.events.append(("interaction_started", (interaction_id, provider_status)))

    async def release_unsent(self, lease: LimiterLease, *, reason: str) -> None:
        self.events.append(("release", reason))

    async def report_provider_429(self, lease: LimiterLease, *, retry_after_ms: int | None) -> None:
        self.events.append(("provider_429", retry_after_ms))

    async def finalize_interaction(self, lease: LimiterLease, **kwargs: Any) -> None:
        self.events.append(("finalize", kwargs.get("provider_terminal_status")))
        self.finalized.append({"lease": lease, **kwargs})

    @staticmethod
    def public_lease(lease: LimiterLease, *, actual_tpm: int | None) -> dict[str, Any]:
        return {
            "reserved_tpm": lease.reserved_tpm,
            "actual_tpm": actual_tpm,
            "key_alias": "key:…safe",
            "quota_scope_alias": "scope:…safe",
            "contract": lease.contract,
            "bucket_strategy": lease.bucket_strategy,
        }


class Interactions:
    def __init__(self, result: ProviderInteraction | BaseException) -> None:
        self.result = result
        self.calls: list[dict[str, Any]] = []

    async def create(self, **kwargs: Any) -> ProviderInteraction:
        self.calls.append(kwargs)
        if isinstance(self.result, BaseException):
            raise self.result
        if self.result.interaction_id is not None:
            await kwargs["on_interaction_started"](self.result.interaction_id, "created")
        return self.result


def completed(structured: Mapping[str, Any]) -> ProviderInteraction:
    return ProviderInteraction(
        interaction_id="interaction-1",
        model="gemini-3.6-flash",
        status="completed",
        structured_output=structured,
        output_text="{}",
        usage=ProviderUsage(
            total_input_tokens=1000,
            total_output_tokens=100,
            total_thought_tokens=25,
            total_tokens=1125,
            input_tokens_by_modality=({"modality": "video", "tokens": 900},),
        ),
        http_status=200,
    )


def analyzer(
    limiter: Limiter,
    interactions: Interactions,
    *,
    max_result_bytes: int = 1_048_576,
) -> GeminiYouTubeAnalyzer:
    return GeminiYouTubeAnalyzer(
        config=YouTubeAnalyzerConfig(
            enabled=True,
            default_model="gemini-3.6-flash",
            allowed_models=frozenset({"gemini-3.6-flash", "gemini-3.7-flash"}),
            max_output_tokens=8192,
            max_result_bytes=max_result_bytes,
        ),
        limiter=limiter,  # type: ignore[arg-type]
        interactions=interactions,  # type: ignore[arg-type]
    )


def arguments(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "youtube_url": "https://youtu.be/6V2stDksGI8?si=tracking",
        "mode": "summary",
        "idempotency_key": "analyzer-0001",
        "max_output_tokens": 4096,
    }
    value.update(changes)
    return value


@pytest.mark.asyncio
async def test_success_orders_reserve_secret_sent_one_post_finalize_and_returns_actual_usage() -> None:
    shared = Limiter()
    provider = Interactions(completed(summary_output()))
    result = await analyzer(shared, provider).analyze(arguments())

    assert [event[0] for event in shared.events] == [
        "preflight",
        "reserve",
        "secret",
        "sent",
        "interaction_started",
        "finalize",
    ]
    assert len(provider.calls) == 1
    assert provider.calls[0]["api_key"] == "provider-secret"
    assert result["canonical_youtube_url"] == "https://www.youtube.com/watch?v=6V2stDksGI8"
    assert result["provider"] == "google_gemini_interactions"
    assert result["limiter"]["reserved_tpm"] == 250000
    assert result["limiter"]["actual_tpm"] == 1125
    assert result["usage"]["total_thought_tokens"] == 25
    assert "provider-secret" not in repr(result)
    assert result["transcript_source"] is None
    assert shared.finalized[0]["semantic_status"] == "passed"


@pytest.mark.asyncio
async def test_limiter_failure_blocks_provider_send() -> None:
    shared = Limiter(preflight_error=GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE))
    provider = Interactions(completed(summary_output()))
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, provider).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE
    assert provider.calls == []
    assert [event[0] for event in shared.events] == ["preflight"]


@pytest.mark.asyncio
async def test_missing_selected_secret_releases_unsent_and_never_calls_provider() -> None:
    shared = Limiter(secret="")
    provider = Interactions(completed(summary_output()))
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, provider).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.KEY_SECRET_MISSING
    assert provider.calls == []
    assert shared.events[-1] == ("release", "key_secret_missing")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("kind", "expected"),
    [
        ("timeout", GoogleAIErrorCode.PROVIDER_TIMEOUT),
        ("network", GoogleAIErrorCode.PROVIDER_NETWORK_ERROR),
        ("response_too_large", GoogleAIErrorCode.RESPONSE_TOO_LARGE),
        ("malformed_json", GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID),
    ],
)
async def test_transport_failure_after_send_is_finalized_ambiguous_without_retry(
    kind: str, expected: GoogleAIErrorCode
) -> None:
    shared = Limiter()
    provider = Interactions(ProviderTransportFailure(kind))
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, provider).analyze(arguments())
    assert caught.value.code is expected
    assert caught.value.reconciliation_required is True
    assert caught.value.warnings == ("provider_outcome_ambiguous_no_retry",)
    assert len(provider.calls) == 1
    assert shared.finalized[0]["provider_terminal_status"] == "incomplete"


@pytest.mark.asyncio
async def test_provider_429_closes_scope_model_and_finalizes_attempt() -> None:
    shared = Limiter()
    provider = Interactions(
        ProviderInteraction(
            interaction_id=None,
            model="gemini-3.6-flash",
            status="failed",
            structured_output=None,
            output_text=None,
            usage=None,
            http_status=429,
            retry_after_ms=5000,
            provider_error_code="RESOURCE_EXHAUSTED",
            provider_error_category="provider_429",
        )
    )
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, provider).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.PROVIDER_429
    assert caught.value.retry_after_ms == 5000
    assert ("provider_429", 5000) in shared.events
    assert shared.finalized[0]["provider_terminal_status"] == "failed"


@pytest.mark.asyncio
async def test_transcript_is_explicitly_model_generated_media_transcription() -> None:
    result = await analyzer(Limiter(), Interactions(completed(transcript_output()))).analyze(
        arguments(mode="transcript")
    )
    assert result["transcript_source"] == "gemini_media_transcription"
    assert result["structured_output"]["transcript_source"] == "gemini_media_transcription"


@pytest.mark.asyncio
async def test_cancel_after_send_finalizes_attempt() -> None:
    class CancelledInteractions:
        calls = 0

        async def create(self, **_kwargs: Any) -> ProviderInteraction:
            self.calls += 1
            raise asyncio.CancelledError

    shared = Limiter()
    provider = CancelledInteractions()
    with pytest.raises(asyncio.CancelledError):
        await analyzer(shared, provider).analyze(arguments())  # type: ignore[arg-type]
    assert provider.calls == 1
    assert shared.finalized[0]["provider_terminal_status"] == "cancelled"


@pytest.mark.asyncio
async def test_cancel_after_created_preserves_started_id_in_finalize() -> None:
    class CancelledAfterCreated:
        async def create(self, **kwargs: Any) -> ProviderInteraction:
            await kwargs["on_interaction_started"]("interaction-started", "created")
            raise asyncio.CancelledError

    shared = Limiter()
    with pytest.raises(asyncio.CancelledError):
        await analyzer(shared, CancelledAfterCreated()).analyze(arguments())  # type: ignore[arg-type]
    assert shared.finalized[0]["interaction_id"] == "interaction-started"
    assert [event[0] for event in shared.events][-2:] == ["interaction_started", "finalize"]


@pytest.mark.asyncio
async def test_disconnect_after_created_preserves_id_and_requires_reconciliation() -> None:
    shared = Limiter()
    failure = ProviderTransportFailure(
        "idle_timeout", interaction_id="interaction-started", provider_status="in_progress"
    )
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, Interactions(failure)).analyze(arguments())
    assert caught.value.interaction_id == "interaction-started"
    assert caught.value.reconciliation_required is True
    assert shared.finalized[0]["interaction_id"] == "interaction-started"


@pytest.mark.asyncio
async def test_started_marker_failure_requires_reconciliation_without_unsafe_finalize() -> None:
    shared = Limiter()
    failure = ProviderTransportFailure(
        "interaction_started_accounting_failed",
        interaction_id="provider-interaction",
        provider_status="created",
    )
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, Interactions(failure)).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.RECONCILIATION_REQUIRED
    assert caught.value.interaction_id == "provider-interaction"
    assert caught.value.reconciliation_required is True
    assert shared.finalized == []


@pytest.mark.asyncio
async def test_cancelled_started_marker_is_ambiguous_and_never_finalized() -> None:
    class CancelledMarkerLimiter(Limiter):
        async def mark_interaction_started(
            self,
            lease: LimiterLease,
            *,
            interaction_id: str,
            provider_status: str,
        ) -> None:
            self.events.append(("interaction_started", (interaction_id, provider_status)))
            raise asyncio.CancelledError

    shared = CancelledMarkerLimiter()
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, Interactions(completed(summary_output()))).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.RECONCILIATION_REQUIRED
    assert caught.value.interaction_id == "interaction-1"
    assert caught.value.reconciliation_required is True
    assert shared.finalized == []


@pytest.mark.asyncio
async def test_result_bound_is_accounted_as_semantic_failure_before_return() -> None:
    shared = Limiter()
    provider = Interactions(completed(summary_output()))
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, provider, max_result_bytes=128).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.RESPONSE_TOO_LARGE
    assert len(provider.calls) == 1
    assert shared.finalized[0]["provider_terminal_status"] == "completed"
    assert shared.finalized[0]["semantic_status"] == "failed"
    assert shared.finalized[0]["error_code"] == GoogleAIErrorCode.RESPONSE_TOO_LARGE.value


@pytest.mark.asyncio
async def test_provider_429_finalize_still_runs_when_cooldown_reporting_fails() -> None:
    class CooldownFailingLimiter(Limiter):
        async def report_provider_429(
            self,
            lease: LimiterLease,
            *,
            retry_after_ms: int | None,
        ) -> None:
            self.events.append(("provider_429", retry_after_ms))
            raise RuntimeError("ledger cooldown write failed")

    shared = CooldownFailingLimiter()
    provider = Interactions(
        ProviderInteraction(
            interaction_id=None,
            model="gemini-3.6-flash",
            status="failed",
            structured_output=None,
            output_text=None,
            usage=None,
            http_status=429,
            retry_after_ms=5000,
            provider_error_code="RESOURCE_EXHAUSTED",
            provider_error_category="provider_429",
        )
    )
    with pytest.raises(GoogleAIError) as caught:
        await analyzer(shared, provider).analyze(arguments())
    assert caught.value.code is GoogleAIErrorCode.RECONCILIATION_REQUIRED
    assert caught.value.reconciliation_required is True
    assert shared.finalized[0]["provider_terminal_status"] == "failed"
