from __future__ import annotations

import asyncio
import os
from collections.abc import Mapping
from typing import Any
from urllib.parse import quote, urlsplit, urlunsplit

from my_data_hub.google_ai.contracts import (
    LimiterLease,
    LimiterPreflight,
    ModelLimit,
    ProviderUsage,
)
from my_data_hub.google_ai.errors import GoogleAIError, GoogleAIErrorCode
from my_data_hub.google_ai.http import (
    AiohttpBoundedJSONRequester,
    BoundedHTTPError,
    BoundedHTTPResponse,
    BoundedJSONRequester,
)

LIMITER_CONTRACT = "google_ai_project_model_atomic_v1"
BUCKET_STRATEGY = "rolling_60s_pacific_day_v2"
QUOTA_DIMENSION = "quota_scope/model"
INTERACTION_ACCOUNTING = "google_ai_interaction_usage_v2"


def _positive_int(value: Any) -> int | None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        return None
    return value


def _redact(label: str, value: str) -> str:
    clean = value.strip()
    tail = clean[-6:] if clean else "unknown"
    return f"{label}:…{tail}"


class SupabaseGoogleAILimiter:
    """Narrow adapter to the canonical events-bot Google AI quota ledger."""

    def __init__(
        self,
        *,
        supabase_url: str,
        service_key: str,
        candidate_env_names: tuple[str, ...],
        requester: BoundedJSONRequester | None = None,
        environment: Mapping[str, str] | None = None,
        timeout_seconds: float = 10.0,
    ) -> None:
        parsed = urlsplit(supabase_url.rstrip("/"))
        self._origin = urlunsplit((parsed.scheme, parsed.netloc, parsed.path.rstrip("/"), "", ""))
        self._service_key = service_key
        self._candidate_env_names = tuple(candidate_env_names)
        self._requester = requester or AiohttpBoundedJSONRequester()
        self._environment = os.environ if environment is None else environment
        self._timeout_seconds = timeout_seconds

    @property
    def candidate_env_names(self) -> tuple[str, ...]:
        return self._candidate_env_names

    async def preflight(self, model: str) -> LimiterPreflight:
        capabilities = await self._rpc("google_ai_limiter_capabilities", {}, attempts=2)
        if not isinstance(capabilities, Mapping):
            raise GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE, retryable=True)
        self._validate_capabilities(capabilities)

        encoded_model = quote(model, safe="-._~")
        limits = await self._rest_get(
            "google_ai_model_limits",
            f"select=model,rpm,tpm,rpd,tpm_reserve_extra&model=eq.{encoded_model}&limit=2",
            attempts=2,
        )
        if not isinstance(limits, list) or not limits:
            raise GoogleAIError(GoogleAIErrorCode.MODEL_LIMIT_NOT_FOUND)
        if len(limits) != 1 or not isinstance(limits[0], Mapping):
            raise GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE, retryable=True)
        row = limits[0]
        rpm = _positive_int(row.get("rpm"))
        tpm = _positive_int(row.get("tpm"))
        rpd = _positive_int(row.get("rpd"))
        reserve_extra = _positive_int(row.get("tpm_reserve_extra"))
        if None in {rpm, tpm, rpd, reserve_extra} or min(rpm or 0, tpm or 0, rpd or 0) < 1:
            raise GoogleAIError(GoogleAIErrorCode.MODEL_LIMIT_NOT_FOUND)

        keys = await self._rest_get(
            "google_ai_api_keys",
            "select=id,env_var_name,key_alias,quota_scope,is_active,priority"
            "&provider=eq.google&is_active=eq.true&order=priority.asc,id.asc",
            attempts=2,
        )
        if not isinstance(keys, list):
            raise GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE, retryable=True)
        by_env: dict[str, Mapping[str, Any]] = {}
        for item in keys:
            if not isinstance(item, Mapping):
                continue
            env_name = item.get("env_var_name")
            if isinstance(env_name, str) and env_name in self._candidate_env_names:
                by_env[env_name] = item
        if set(by_env) != set(self._candidate_env_names):
            raise GoogleAIError(GoogleAIErrorCode.KEY_METADATA_MISSING)
        candidate_ids: list[str] = []
        for env_name in self._candidate_env_names:
            item = by_env[env_name]
            fields = (item.get("id"), item.get("key_alias"), item.get("quota_scope"))
            if not all(isinstance(value, str) and value.strip() for value in fields):
                raise GoogleAIError(GoogleAIErrorCode.KEY_METADATA_MISSING)
            candidate_ids.append(str(item["id"]))
        return LimiterPreflight(
            limit=ModelLimit(
                model=model,
                rpm=int(rpm),
                tpm=int(tpm),
                rpd=int(rpd),
                tpm_reserve_extra=int(reserve_extra),
            ),
            candidate_key_ids=tuple(candidate_ids),
            candidate_env_names=frozenset(self._candidate_env_names),
            contract=LIMITER_CONTRACT,
            bucket_strategy=BUCKET_STRATEGY,
        )

    async def reserve(
        self,
        *,
        request_uid: str,
        attempt_no: int,
        model: str,
        preflight: LimiterPreflight,
        consumer: str,
        account_name: str,
    ) -> LimiterLease:
        reserved_tpm = preflight.limit.tpm
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
            retry_after = _positive_int(result.get("retry_after_ms"))
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
                retryable=code not in {GoogleAIErrorCode.MODEL_LIMIT_NOT_FOUND},
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

    def secret_for(self, lease: LimiterLease) -> str:
        if lease.env_var_name not in self._candidate_env_names:
            raise GoogleAIError(GoogleAIErrorCode.KEY_METADATA_MISSING)
        secret = self._environment.get(lease.env_var_name, "").strip()
        if not secret:
            raise GoogleAIError(GoogleAIErrorCode.KEY_SECRET_MISSING)
        return secret

    async def mark_sent(self, lease: LimiterLease) -> None:
        await self._rpc(
            "google_ai_mark_sent",
            {"p_request_uid": lease.request_uid, "p_attempt_no": lease.attempt_no},
            attempts=2,
            allow_empty=True,
        )

    async def release_unsent(self, lease: LimiterLease, *, reason: str) -> None:
        await self._rpc(
            "google_ai_release_unsent_v2",
            {
                "p_request_uid": lease.request_uid,
                "p_attempt_no": lease.attempt_no,
                "p_reason": reason[:120],
            },
            attempts=3,
            allow_empty=True,
        )

    async def report_provider_429(
        self,
        lease: LimiterLease,
        *,
        retry_after_ms: int | None,
    ) -> None:
        await self._rpc(
            "google_ai_report_provider_429",
            {
                "p_request_uid": lease.request_uid,
                "p_attempt_no": lease.attempt_no,
                "p_retry_after_ms": retry_after_ms,
            },
            attempts=3,
            allow_empty=True,
        )

    async def finalize_interaction(
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
        await self._rpc(
            "google_ai_finalize_interaction_v2",
            {
                "p_request_uid": lease.request_uid,
                "p_attempt_no": lease.attempt_no,
                "p_provider_interaction_id": interaction_id,
                "p_provider_terminal_status": provider_terminal_status,
                "p_semantic_status": semantic_status,
                "p_usage_input_tokens": usage.total_input_tokens if usage else None,
                "p_usage_output_tokens": usage.total_output_tokens if usage else None,
                "p_usage_thought_tokens": usage.total_thought_tokens if usage else None,
                "p_usage_total_tokens": usage.total_tokens if usage else None,
                "p_duration_ms": max(0, duration_ms),
                "p_error_type": error_type,
                "p_error_code": error_code,
                "p_error_message": error_message[:500] if error_message else None,
            },
            attempts=3,
            allow_empty=True,
        )

    @staticmethod
    def public_lease(lease: LimiterLease, *, actual_tpm: int | None) -> dict[str, Any]:
        return {
            "reserved_tpm": lease.reserved_tpm,
            "actual_tpm": actual_tpm,
            "key_alias": _redact("key", lease.key_alias),
            "quota_scope_alias": _redact("scope", lease.quota_scope),
            "contract": lease.contract,
            "bucket_strategy": lease.bucket_strategy,
        }

    def _validate_capabilities(self, value: Mapping[str, Any]) -> None:
        if value.get("limiter_contract") != LIMITER_CONTRACT:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH)
        if value.get("bucket_strategy") != BUCKET_STRATEGY:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_BUCKET_STRATEGY_MISMATCH)
        if value.get("quota_dimension") != QUOTA_DIMENSION or value.get("lock_dimension") != QUOTA_DIMENSION:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH)
        if value.get("quota_scope_enforced") is not True:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH)
        if value.get("interaction_accounting") != INTERACTION_ACCOUNTING:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH)
        if value.get("unsent_release_supported") is not True:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH)

    @staticmethod
    def _validate_reserve_markers(value: Mapping[str, Any]) -> None:
        if value.get("limiter_contract") != LIMITER_CONTRACT:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH)
        if value.get("bucket_strategy") != BUCKET_STRATEGY:
            raise GoogleAIError(GoogleAIErrorCode.LIMITER_BUCKET_STRATEGY_MISMATCH)

    async def _rest_get(self, table: str, query: str, *, attempts: int) -> Any:
        response = await self._request(
            "GET",
            f"{self._origin}/rest/v1/{table}?{query}",
            body=None,
            attempts=attempts,
        )
        return response.json_body

    async def _rpc(
        self,
        name: str,
        body: Mapping[str, Any],
        *,
        attempts: int,
        allow_empty: bool = False,
    ) -> Any:
        response = await self._request(
            "POST",
            f"{self._origin}/rest/v1/rpc/{name}",
            body=body,
            attempts=attempts,
        )
        if response.json_body is None and not allow_empty:
            raise GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE, retryable=True)
        return response.json_body

    async def _request(
        self,
        method: str,
        url: str,
        *,
        body: Mapping[str, Any] | None,
        attempts: int,
    ) -> BoundedHTTPResponse:
        headers = {
            "apikey": self._service_key,
            "Authorization": f"Bearer {self._service_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        for attempt in range(1, attempts + 1):
            try:
                response = await self._requester.request_json(
                    method,
                    url,
                    headers=headers,
                    json_body=body,
                    timeout_seconds=self._timeout_seconds,
                    max_response_bytes=131_072,
                )
            except asyncio.CancelledError:
                raise
            except BoundedHTTPError as exc:
                if attempt < attempts and exc.kind in {"timeout", "network"}:
                    continue
                raise GoogleAIError(
                    GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE,
                    retryable=True,
                ) from exc
            if 200 <= response.status < 300:
                return response
            if attempt < attempts and response.status >= 500:
                continue
            raise GoogleAIError(
                GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE,
                retryable=response.status >= 500,
            )
        raise GoogleAIError(GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE, retryable=True)
