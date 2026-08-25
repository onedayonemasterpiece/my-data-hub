from __future__ import annotations

from dataclasses import dataclass

from my_data_hub.config import Settings


@dataclass(frozen=True, slots=True)
class GoogleYouTubeSettings:
    enabled: bool
    model: str
    allowed_models: tuple[str, ...]
    connect_timeout_seconds: int
    first_event_timeout_seconds: int
    idle_timeout_seconds: int
    total_timeout_seconds: int
    max_raw_sse_bytes: int
    max_model_output_bytes: int
    max_result_bytes: int
    max_output_tokens: int
    default_store: bool
    limiter_supabase_url: str
    limiter_supabase_service_key: str
    normal_key_envs: tuple[str, ...]

    @classmethod
    def from_settings(cls, settings: Settings) -> GoogleYouTubeSettings:
        return cls(
            enabled=settings.google_youtube_enabled,
            model=settings.google_youtube_model,
            allowed_models=settings.google_youtube_allowed_models,
            connect_timeout_seconds=settings.google_youtube_connect_timeout_seconds,
            first_event_timeout_seconds=settings.google_youtube_first_event_timeout_seconds,
            idle_timeout_seconds=settings.google_youtube_idle_timeout_seconds,
            total_timeout_seconds=settings.google_youtube_total_timeout_seconds,
            max_raw_sse_bytes=settings.google_youtube_max_raw_sse_bytes,
            max_model_output_bytes=settings.google_youtube_max_model_output_bytes,
            max_result_bytes=settings.google_youtube_max_result_bytes,
            max_output_tokens=settings.google_youtube_max_output_tokens,
            default_store=settings.google_youtube_default_store,
            limiter_supabase_url=settings.google_ai_limiter_supabase_url,
            limiter_supabase_service_key=settings.google_ai_limiter_supabase_service_key,
            normal_key_envs=settings.google_ai_normal_key_envs,
        )
