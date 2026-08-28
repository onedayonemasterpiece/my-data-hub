from __future__ import annotations

import os
from dataclasses import dataclass


class VoiceIntakeConfigurationError(RuntimeError):
    pass


def _bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise VoiceIntakeConfigurationError(f"{name} must be a boolean")


def _int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise VoiceIntakeConfigurationError(f"{name} must be an integer") from exc


def _csv(name: str) -> tuple[str, ...]:
    return tuple(part.strip() for part in os.getenv(name, "").split(",") if part.strip())


@dataclass(frozen=True, slots=True)
class VoiceIntakeSettings:
    enabled: bool
    device_token: str
    model: str
    allowed_models: tuple[str, ...]
    max_audio_bytes: int
    max_json_bytes: int
    provider_timeout_seconds: int
    github_token: str
    github_repository: str
    github_branch: str
    limiter_supabase_url: str
    limiter_supabase_service_key: str
    normal_key_envs: tuple[str, ...]

    @classmethod
    def from_env(cls) -> "VoiceIntakeSettings":
        settings = cls(
            enabled=_bool("MY_DATA_HUB_VOICE_INTAKE_ENABLED", False),
            device_token=os.getenv("MY_DATA_HUB_VOICE_DEVICE_TOKEN", "").strip(),
            model=os.getenv("MY_DATA_HUB_VOICE_MODEL", "gemini-3.1-flash-lite").strip(),
            allowed_models=_csv("MY_DATA_HUB_VOICE_ALLOWED_MODELS")
            or (os.getenv("MY_DATA_HUB_VOICE_MODEL", "gemini-3.1-flash-lite").strip(),),
            max_audio_bytes=_int("MY_DATA_HUB_VOICE_MAX_AUDIO_BYTES", 8 * 1024 * 1024),
            max_json_bytes=_int("MY_DATA_HUB_VOICE_MAX_JSON_BYTES", 2 * 1024 * 1024),
            provider_timeout_seconds=_int("MY_DATA_HUB_VOICE_PROVIDER_TIMEOUT_SECONDS", 180),
            github_token=(
                os.getenv("MY_DATA_HUB_VOICE_GITHUB_TOKEN")
                or os.getenv("GH_TOKEN")
                or os.getenv("GITHUB_TOKEN")
                or ""
            ).strip(),
            github_repository=os.getenv(
                "MY_DATA_HUB_VOICE_GITHUB_REPOSITORY", "onedayonemasterpiece/idea-hub"
            ).strip(),
            github_branch=os.getenv("MY_DATA_HUB_VOICE_GITHUB_BRANCH", "main").strip(),
            limiter_supabase_url=os.getenv("GOOGLE_AI_LIMITER_SUPABASE_URL", "").strip(),
            limiter_supabase_service_key=os.getenv(
                "GOOGLE_AI_LIMITER_SUPABASE_SERVICE_KEY", ""
            ).strip(),
            normal_key_envs=_csv("GOOGLE_AI_NORMAL_KEY_ENVS"),
        )
        settings.validate()
        return settings

    def validate(self) -> None:
        if not self.enabled:
            return
        if not 32 <= len(self.device_token) <= 256 or any(ord(char) < 33 for char in self.device_token):
            raise VoiceIntakeConfigurationError(
                "MY_DATA_HUB_VOICE_DEVICE_TOKEN must contain 32..256 visible characters"
            )
        if self.model not in self.allowed_models:
            raise VoiceIntakeConfigurationError("voice model must be listed in allowed models")
        if any("flash-lite" not in model.lower() for model in self.allowed_models):
            raise VoiceIntakeConfigurationError("only explicitly allowed Gemini Flash-Lite models are supported")
        if not 64 * 1024 <= self.max_audio_bytes <= 32 * 1024 * 1024:
            raise VoiceIntakeConfigurationError("voice audio limit must be between 64 KiB and 32 MiB")
        if not 64 * 1024 <= self.max_json_bytes <= 8 * 1024 * 1024:
            raise VoiceIntakeConfigurationError("voice JSON limit must be between 64 KiB and 8 MiB")
        if not 10 <= self.provider_timeout_seconds <= 600:
            raise VoiceIntakeConfigurationError("voice provider timeout must be between 10 and 600 seconds")
        if self.github_repository != "onedayonemasterpiece/idea-hub" or self.github_branch != "main":
            raise VoiceIntakeConfigurationError("voice publication is bounded to idea-hub/main")
        if not self.github_token:
            raise VoiceIntakeConfigurationError("voice GitHub token is required")
        if not self.limiter_supabase_url or not self.limiter_supabase_service_key:
            raise VoiceIntakeConfigurationError("dedicated shared-limiter credentials are required")
        if not self.normal_key_envs:
            raise VoiceIntakeConfigurationError("GOOGLE_AI_NORMAL_KEY_ENVS is required")
