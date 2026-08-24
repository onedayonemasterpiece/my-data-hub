from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field, model_validator


class YouTubeMode(StrEnum):
    SUMMARY = "summary"
    TRANSCRIPT = "transcript"
    QUESTION = "question"
    CUSTOM = "custom"


class MediaResolution(StrEnum):
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class ThinkingLevel(StrEnum):
    MINIMAL = "minimal"
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"


class YouTubeAnalyzeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    youtube_url: str = Field(min_length=1, max_length=2048)
    mode: YouTubeMode = YouTubeMode.SUMMARY
    question: str | None = Field(default=None, min_length=1, max_length=4000)
    prompt: str | None = Field(default=None, min_length=1, max_length=8000)
    language: str = Field(default="ru", pattern=r"^[A-Za-z]{2,8}(?:-[A-Za-z0-9]{1,8})?$")
    include_timestamps: bool = True
    include_visual_observations: bool = True
    model: str | None = Field(default=None, min_length=1, max_length=128)
    media_resolution: MediaResolution | None = None
    max_output_tokens: int = Field(default=4096, ge=256, le=65536)
    thinking_level: ThinkingLevel = ThinkingLevel.LOW
    idempotency_key: str = Field(min_length=8, max_length=200, pattern=r"^[A-Za-z0-9._:-]+$")

    @model_validator(mode="after")
    def validate_mode_fields(self) -> YouTubeAnalyzeRequest:
        if self.mode is YouTubeMode.QUESTION:
            if not self.question:
                raise ValueError("question is required for mode=question")
            if self.prompt is not None:
                raise ValueError("prompt is allowed only for mode=custom")
        elif self.mode is YouTubeMode.CUSTOM:
            if not self.prompt:
                raise ValueError("prompt is required for mode=custom")
            if self.question is not None:
                raise ValueError("question is allowed only for mode=question")
        elif self.question is not None or self.prompt is not None:
            raise ValueError("question and prompt are mode-specific")
        return self


@dataclass(frozen=True, slots=True)
class NormalizedYouTubeURL:
    video_id: str
    canonical_url: str


@dataclass(frozen=True, slots=True)
class ModelLimit:
    model: str
    rpm: int
    tpm: int
    rpd: int
    tpm_reserve_extra: int


@dataclass(frozen=True, slots=True)
class LimiterPreflight:
    limit: ModelLimit
    candidate_key_ids: tuple[str, ...]
    candidate_env_names: frozenset[str]
    contract: str
    bucket_strategy: str


@dataclass(frozen=True, slots=True)
class LimiterLease:
    request_uid: str
    attempt_no: int
    api_key_id: str
    env_var_name: str
    key_alias: str
    quota_scope: str
    reserved_tpm: int
    contract: str
    bucket_strategy: str


@dataclass(frozen=True, slots=True)
class ProviderUsage:
    total_input_tokens: int
    total_output_tokens: int
    total_thought_tokens: int
    total_tokens: int
    input_tokens_by_modality: tuple[Mapping[str, Any], ...] = ()


@dataclass(frozen=True, slots=True)
class ProviderInteraction:
    interaction_id: str | None
    model: str
    status: str
    structured_output: Mapping[str, Any] | None
    output_text: str | None
    usage: ProviderUsage | None
    http_status: int
    retry_after_ms: int | None = None
    provider_error_code: str | None = None
    provider_error_category: str | None = None


@runtime_checkable
class YouTubeVideoAnalyzer(Protocol):
    async def analyze(
        self,
        arguments: Mapping[str, Any] | YouTubeAnalyzeRequest,
    ) -> Mapping[str, Any]: ...
