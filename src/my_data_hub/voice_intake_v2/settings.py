from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


class VoiceIntakeV2ConfigurationError(RuntimeError):
    pass


def _integer(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise VoiceIntakeV2ConfigurationError(f"{name} must be an integer") from exc


@dataclass(frozen=True, slots=True)
class VoiceIntakeV2Settings:
    enabled: bool
    spool_root: Path
    max_chunk_bytes: int
    max_json_bytes: int
    max_session_seconds: int
    active_ttl_seconds: int
    lease_seconds: int
    worker_poll_seconds: float
    ffprobe_timeout_seconds: int
    ffmpeg_timeout_seconds: int
    duration_tolerance_ms: int
    max_session_bytes: int = 64 * 1024 * 1024

    @classmethod
    def from_env(cls) -> VoiceIntakeV2Settings:
        raw_enabled = os.getenv("MY_DATA_HUB_VOICE_INTAKE_V2_ENABLED", "false").strip().lower()
        if raw_enabled not in {"true", "false", "1", "0", "yes", "no", "on", "off"}:
            raise VoiceIntakeV2ConfigurationError("MY_DATA_HUB_VOICE_INTAKE_V2_ENABLED must be boolean")
        result = cls(
            enabled=raw_enabled in {"true", "1", "yes", "on"},
            spool_root=Path(os.getenv("MY_DATA_HUB_VOICE_INTAKE_V2_SPOOL", "/voice-intake-v2")),
            max_chunk_bytes=_integer("MY_DATA_HUB_VOICE_V2_MAX_CHUNK_BYTES", 32 * 1024 * 1024),
            max_json_bytes=_integer("MY_DATA_HUB_VOICE_V2_MAX_JSON_BYTES", 2 * 1024 * 1024),
            max_session_seconds=_integer("MY_DATA_HUB_VOICE_V2_MAX_SESSION_SECONDS", 3600),
            active_ttl_seconds=_integer("MY_DATA_HUB_VOICE_V2_ACTIVE_TTL_SECONDS", 7 * 24 * 3600),
            lease_seconds=_integer("MY_DATA_HUB_VOICE_V2_LEASE_SECONDS", 600),
            worker_poll_seconds=float(os.getenv("MY_DATA_HUB_VOICE_V2_WORKER_POLL_SECONDS", "2")),
            ffprobe_timeout_seconds=_integer("MY_DATA_HUB_VOICE_V2_FFPROBE_TIMEOUT_SECONDS", 15),
            ffmpeg_timeout_seconds=_integer("MY_DATA_HUB_VOICE_V2_FFMPEG_TIMEOUT_SECONDS", 600),
            duration_tolerance_ms=_integer("MY_DATA_HUB_VOICE_V2_DURATION_TOLERANCE_MS", 2000),
            max_session_bytes=_integer(
                "MY_DATA_HUB_VOICE_V2_MAX_SESSION_BYTES", 64 * 1024 * 1024
            ),
        )
        result.validate()
        return result

    def validate(self) -> None:
        if not self.spool_root.is_absolute() or self.spool_root == Path("/"):
            raise VoiceIntakeV2ConfigurationError("v2 spool must be a dedicated absolute path")
        if self.max_chunk_bytes < 64 * 1024 or self.max_chunk_bytes > 128 * 1024 * 1024:
            raise VoiceIntakeV2ConfigurationError("v2 chunk bound must be 64 KiB..128 MiB")
        if self.max_json_bytes < 64 * 1024 or self.max_json_bytes > 8 * 1024 * 1024:
            raise VoiceIntakeV2ConfigurationError("v2 JSON bound must be 64 KiB..8 MiB")
        if self.max_session_seconds < 3600:
            raise VoiceIntakeV2ConfigurationError("v2 safety limit must be at least 60 minutes")
        if self.max_session_bytes < 16 * 1024 * 1024 or self.max_session_bytes > 512 * 1024 * 1024:
            raise VoiceIntakeV2ConfigurationError("v2 session byte bound must be 16 MiB..512 MiB")
        if self.active_ttl_seconds < 7 * 24 * 3600:
            raise VoiceIntakeV2ConfigurationError("v2 active TTL must be at least seven days")
        if not 30 <= self.lease_seconds <= 3600:
            raise VoiceIntakeV2ConfigurationError("v2 worker lease must be 30..3600 seconds")
        if not 0.1 <= self.worker_poll_seconds <= 60:
            raise VoiceIntakeV2ConfigurationError("v2 worker poll must be 0.1..60 seconds")
        if not 1 <= self.ffprobe_timeout_seconds <= 60:
            raise VoiceIntakeV2ConfigurationError("ffprobe timeout must be 1..60 seconds")
        if not 10 <= self.ffmpeg_timeout_seconds <= 1800:
            raise VoiceIntakeV2ConfigurationError("ffmpeg timeout must be 10..1800 seconds")
