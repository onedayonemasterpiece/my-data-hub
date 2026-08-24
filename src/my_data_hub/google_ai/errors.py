from __future__ import annotations

from enum import StrEnum
from typing import Any


class GoogleAIErrorCode(StrEnum):
    FEATURE_DISABLED = "feature_disabled"
    INVALID_YOUTUBE_URL = "invalid_youtube_url"
    UNSUPPORTED_YOUTUBE_HOST = "unsupported_youtube_host"
    INVALID_VIDEO_ID = "invalid_video_id"
    UNSUPPORTED_MODEL = "unsupported_model"
    UNSUPPORTED_THINKING_LEVEL = "unsupported_thinking_level"
    SHARED_LIMITER_UNAVAILABLE = "shared_limiter_unavailable"
    LIMITER_CONTRACT_MISMATCH = "limiter_contract_mismatch"
    LIMITER_BUCKET_STRATEGY_MISMATCH = "limiter_bucket_strategy_mismatch"
    MODEL_LIMIT_NOT_FOUND = "model_limit_not_found"
    KEY_METADATA_MISSING = "key_metadata_missing"
    KEY_SECRET_MISSING = "key_secret_missing"
    QUOTA_EXHAUSTED_RPM = "quota_exhausted_rpm"
    QUOTA_EXHAUSTED_TPM = "quota_exhausted_tpm"
    QUOTA_EXHAUSTED_RPD = "quota_exhausted_rpd"
    PROVIDER_429 = "provider_429"
    PROVIDER_TIMEOUT = "provider_timeout"
    PROVIDER_NETWORK_ERROR = "provider_network_error"
    PROVIDER_REJECTED_VIDEO = "provider_rejected_video"
    YOUTUBE_VIDEO_NOT_PUBLIC = "youtube_video_not_public"
    INTERACTION_INCOMPLETE = "interaction_incomplete"
    RESPONSE_TOO_LARGE = "response_too_large"
    RESPONSE_SCHEMA_INVALID = "response_schema_invalid"
    USAGE_MISSING = "usage_missing"
    FINALIZATION_FAILED = "finalization_failed"
    RECONCILIATION_REQUIRED = "reconciliation_required"


_DEFAULT_MESSAGES: dict[GoogleAIErrorCode, str] = {
    GoogleAIErrorCode.FEATURE_DISABLED: "YouTube analysis is disabled.",
    GoogleAIErrorCode.INVALID_YOUTUBE_URL: "The YouTube URL is invalid.",
    GoogleAIErrorCode.UNSUPPORTED_YOUTUBE_HOST: "The YouTube host is not supported.",
    GoogleAIErrorCode.INVALID_VIDEO_ID: "The YouTube video ID is invalid.",
    GoogleAIErrorCode.UNSUPPORTED_MODEL: "The requested Gemini model is not allowed.",
    GoogleAIErrorCode.UNSUPPORTED_THINKING_LEVEL: (
        "The selected thinking level is not supported by the requested model."
    ),
    GoogleAIErrorCode.SHARED_LIMITER_UNAVAILABLE: "The shared Google AI limiter is unavailable.",
    GoogleAIErrorCode.LIMITER_CONTRACT_MISMATCH: "The shared limiter contract is incompatible.",
    GoogleAIErrorCode.LIMITER_BUCKET_STRATEGY_MISMATCH: "The shared limiter bucket strategy is incompatible.",
    GoogleAIErrorCode.MODEL_LIMIT_NOT_FOUND: "No shared limiter row exists for the requested model.",
    GoogleAIErrorCode.KEY_METADATA_MISSING: "Shared limiter key metadata is incomplete.",
    GoogleAIErrorCode.KEY_SECRET_MISSING: "The limiter-selected key secret is unavailable.",
    GoogleAIErrorCode.QUOTA_EXHAUSTED_RPM: "The shared RPM quota is exhausted.",
    GoogleAIErrorCode.QUOTA_EXHAUSTED_TPM: "The shared TPM quota is exhausted.",
    GoogleAIErrorCode.QUOTA_EXHAUSTED_RPD: "The shared RPD quota is exhausted.",
    GoogleAIErrorCode.PROVIDER_429: "Google rejected the request because provider quota is exhausted.",
    GoogleAIErrorCode.PROVIDER_TIMEOUT: "The Gemini request timed out after it was sent.",
    GoogleAIErrorCode.PROVIDER_NETWORK_ERROR: "The Gemini request failed at the network boundary.",
    GoogleAIErrorCode.PROVIDER_REJECTED_VIDEO: "Gemini rejected the supplied video reference.",
    GoogleAIErrorCode.YOUTUBE_VIDEO_NOT_PUBLIC: "The YouTube video is unavailable to public URL analysis.",
    GoogleAIErrorCode.INTERACTION_INCOMPLETE: "Gemini did not return a terminal completed interaction.",
    GoogleAIErrorCode.RESPONSE_TOO_LARGE: "The provider response exceeded the configured byte limit.",
    GoogleAIErrorCode.RESPONSE_SCHEMA_INVALID: "The provider response did not satisfy the bounded result schema.",
    GoogleAIErrorCode.USAGE_MISSING: "The provider response omitted required token usage.",
    GoogleAIErrorCode.FINALIZATION_FAILED: "Provider usage accounting could not be finalized.",
    GoogleAIErrorCode.RECONCILIATION_REQUIRED: "The sent provider attempt requires accounting reconciliation.",
}


class GoogleAIError(RuntimeError):
    """Typed operator-safe failure; details must never contain credentials."""

    def __init__(
        self,
        code: GoogleAIErrorCode,
        *,
        message: str | None = None,
        retryable: bool = False,
        retry_after_ms: int | None = None,
        provider_status: str | None = None,
        request_uid: str | None = None,
        interaction_id: str | None = None,
        reconciliation_required: bool = False,
        warnings: tuple[str, ...] = (),
    ) -> None:
        self.code = code
        self.safe_message = message or _DEFAULT_MESSAGES[code]
        self.retryable = bool(retryable)
        self.retry_after_ms = retry_after_ms
        self.provider_status = provider_status
        self.request_uid = request_uid
        self.interaction_id = interaction_id
        self.reconciliation_required = bool(reconciliation_required)
        self.warnings = warnings
        super().__init__(self.safe_message)

    def public(self) -> dict[str, Any]:
        return {
            "status": "error",
            "error": {
                "code": self.code.value,
                "message": self.safe_message,
            },
            "request_uid": self.request_uid,
            "interaction_id": self.interaction_id,
            "provider_status": self.provider_status,
            "retryable": self.retryable,
            "retry_after_ms": self.retry_after_ms,
            "reconciliation_required": self.reconciliation_required,
            "warnings": list(self.warnings),
        }
