from __future__ import annotations

from collections.abc import Mapping
from typing import Any

import pytest

from my_data_hub.google_ai.contracts import ProviderUsage
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.http import BoundedHTTPError, BoundedHTTPResponse
from my_data_hub.google_ai.limiter import (
    BUCKET_STRATEGY,
    INTERACTION_ACCOUNTING,
    LIMITER_CONTRACT,
    QUOTA_DIMENSION,
    SupabaseGoogleAILimiter,
)


class Requester:
    def __init__(self, responses: list[BoundedHTTPResponse | Exception]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    async def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any] | None,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> BoundedHTTPResponse:
        self.calls.append({"method": method, "url": url, "headers": dict(headers), "json": json_body})
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def ok(payload: object = None) -> BoundedHTTPResponse:
    return BoundedHTTPResponse(200, payload, None, "application/json")


def capabilities(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "limiter_contract": LIMITER_CONTRACT,
        "bucket_strategy": BUCKET_STRATEGY,
        "quota_dimension": QUOTA_DIMENSION,
        "lock_dimension": QUOTA_DIMENSION,
        "quota_scope_enforced": True,
        "interaction_accounting": INTERACTION_ACCOUNTING,
        "unsent_release_supported": True,
    }
    value.update(changes)
    return value


def preflight_responses() -> list[BoundedHTTPResponse]:
    return [
        ok(capabilities()),
        ok([{"model": "gemini-3.6-flash", "rpm": 5, "tpm": 250000, "rpd": 20, "tpm_reserve_extra": 1000}]),
        ok(
            [
                {
                    "id": "key-a-id",
                    "env_var_name": "GOOGLE_KEY_A",
                    "key_alias": "key-a",
                    "quota_scope": "google:project-shared",
                    "is_active": True,
                    "priority": 1,
                },
                {
                    "id": "key-b-id",
                    "env_var_name": "GOOGLE_KEY_B",
                    "key_alias": "key-b",
                    "quota_scope": "google:project-shared",
                    "is_active": True,
                    "priority": 2,
                },
            ]
        ),
    ]


def limiter(requester: Requester, *, environment: Mapping[str, str] | None = None) -> SupabaseGoogleAILimiter:
    return SupabaseGoogleAILimiter(
        supabase_url="https://quota.example.supabase.co",
        service_key="service-secret",
        candidate_env_names=("GOOGLE_KEY_A", "GOOGLE_KEY_B"),
        requester=requester,
        environment=environment or {"GOOGLE_KEY_A": "api-secret-a", "GOOGLE_KEY_B": "api-secret-b"},
    )


@pytest.mark.asyncio
async def test_preflight_and_reserve_use_full_model_tpm_and_same_scope_keys_do_not_double_quota() -> None:
    responses = preflight_responses()
    responses.append(
        ok(
            {
                "ok": True,
                "api_key_id": "key-b-id",
                "env_var_name": "GOOGLE_KEY_B",
                "key_alias": "key-b",
                "quota_scope": "google:project-shared",
                "limiter_contract": LIMITER_CONTRACT,
                "bucket_strategy": BUCKET_STRATEGY,
            }
        )
    )
    requester = Requester(responses)
    adapter = limiter(requester)
    preflight = await adapter.preflight("gemini-3.6-flash")
    lease = await adapter.reserve(
        request_uid="00000000-0000-4000-8000-000000000001",
        attempt_no=1,
        model="gemini-3.6-flash",
        preflight=preflight,
        consumer="test",
        account_name="test",
    )
    reserve = requester.calls[3]["json"]
    assert reserve["p_reserved_tpm"] == 250000
    assert reserve["p_candidate_key_ids"] == ["key-a-id", "key-b-id"]
    assert lease.quota_scope == "google:project-shared"
    assert adapter.secret_for(lease) == "api-secret-b"
    assert "GOOGLE_KEY_B" not in repr(adapter.public_lease(lease, actual_tpm=123))
    assert "api-secret" not in repr(requester.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("marker", "value", "code"),
    [
        ("limiter_contract", "old", GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH),
        ("bucket_strategy", "fixed_minute", GoogleAIErrorCode.LIMITER_BUCKET_STRATEGY_MISMATCH),
        ("interaction_accounting", "v1", GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH),
    ],
)
async def test_capability_mismatch_blocks_before_reserve(marker: str, value: object, code: GoogleAIErrorCode) -> None:
    requester = Requester([ok(capabilities(**{marker: value}))])
    adapter = limiter(requester)
    with pytest.raises(GoogleAIError) as caught:
        await adapter.preflight("gemini-3.6-flash")
    assert caught.value.code is code
    assert len(requester.calls) == 1
    assert not any("google_ai_reserve" in call["url"] for call in requester.calls)


@pytest.mark.asyncio
async def test_unavailable_limiter_is_retryable_and_never_returns_service_key() -> None:
    requester = Requester([BoundedHTTPError("network"), BoundedHTTPError("network")])
    adapter = limiter(requester)
    with pytest.raises(GoogleAIError) as caught:
        await adapter.preflight("gemini-3.6-flash")
    assert caught.value.code is GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE
    assert caught.value.retryable is True
    assert "service-secret" not in str(caught.value)


@pytest.mark.asyncio
async def test_selected_secret_must_exist_and_is_read_only_after_reserve() -> None:
    requester = Requester(
        [
            *preflight_responses(),
            ok(
                {
                    "ok": True,
                    "api_key_id": "key-b-id",
                    "env_var_name": "GOOGLE_KEY_B",
                    "key_alias": "key-b",
                    "quota_scope": "google:project-shared",
                    "limiter_contract": LIMITER_CONTRACT,
                    "bucket_strategy": BUCKET_STRATEGY,
                }
            ),
        ]
    )
    adapter = limiter(requester, environment={"GOOGLE_KEY_A": "api-secret-a"})
    preflight = await adapter.preflight("gemini-3.6-flash")
    lease = await adapter.reserve(
        request_uid="00000000-0000-4000-8000-000000000002",
        attempt_no=1,
        model="gemini-3.6-flash",
        preflight=preflight,
        consumer="test",
        account_name="test",
    )
    with pytest.raises(GoogleAIError) as caught:
        adapter.secret_for(lease)
    assert caught.value.code is GoogleAIErrorCode.KEY_SECRET_MISSING


@pytest.mark.asyncio
async def test_mark_finalize_release_and_provider_429_use_exact_attempt() -> None:
    requester = Requester(
        [
            *preflight_responses(),
            ok(
                {
                    "ok": True,
                    "api_key_id": "key-a-id",
                    "env_var_name": "GOOGLE_KEY_A",
                    "key_alias": "key-a",
                    "quota_scope": "google:project-shared",
                    "limiter_contract": LIMITER_CONTRACT,
                    "bucket_strategy": BUCKET_STRATEGY,
                }
            ),
            ok(None),
            ok(None),
            ok(None),
        ]
    )
    adapter = limiter(requester)
    preflight = await adapter.preflight("gemini-3.6-flash")
    lease = await adapter.reserve(
        request_uid="00000000-0000-4000-8000-000000000003",
        attempt_no=1,
        model="gemini-3.6-flash",
        preflight=preflight,
        consumer="test",
        account_name="test",
    )
    await adapter.mark_sent(lease)
    await adapter.report_provider_429(lease, retry_after_ms=3000)
    await adapter.finalize_interaction(
        lease,
        interaction_id="interaction-3",
        provider_terminal_status="failed",
        semantic_status="not_evaluated",
        usage=ProviderUsage(100, 20, 5, 125),
        duration_ms=10,
        error_type="provider",
        error_code="RESOURCE_EXHAUSTED",
    )
    paths = [call["url"].rsplit("/", 1)[-1] for call in requester.calls[-3:]]
    assert paths == ["google_ai_mark_sent", "google_ai_report_provider_429", "google_ai_finalize_interaction_v2"]
    finalize = requester.calls[-1]["json"]
    assert finalize["p_usage_thought_tokens"] == 5
    assert finalize["p_usage_total_tokens"] == 125
