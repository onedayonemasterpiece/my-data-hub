from __future__ import annotations

from typing import Any, Protocol

from fastapi import FastAPI

from my_data_hub.voice_intake.settings import VoiceIntakeSettings

from .api import attach_voice_intake_v2_routes
from .inference import AggregateGeminiInference
from .media import BoundedMediaTools
from .settings import VoiceIntakeV2Settings
from .store import VoiceIntakeV2Store
from .worker import SessionPublisher, VoiceIntakeV2Worker


class RuntimePublisher(SessionPublisher, Protocol):
    async def resolve_terminology(self) -> dict[str, Any]: ...


def attach_configured_voice_intake_v2(
    app: FastAPI,
    *,
    publisher: RuntimePublisher,
    auth_settings: VoiceIntakeSettings | None = None,
    settings: VoiceIntakeV2Settings | None = None,
) -> FastAPI:
    """Compose the real single worker while preserving the app's lifespan."""
    auth = auth_settings or VoiceIntakeSettings.from_env()
    config = settings or VoiceIntakeV2Settings.from_env()
    store = VoiceIntakeV2Store(config.spool_root)
    media = BoundedMediaTools(
        ffprobe_timeout=config.ffprobe_timeout_seconds,
        ffmpeg_timeout=config.ffmpeg_timeout_seconds,
    )
    inference = AggregateGeminiInference(auth)
    worker = VoiceIntakeV2Worker(
        store, config, media=media, inference=inference, publisher=publisher,
    )
    return attach_voice_intake_v2_routes(
        app, auth_settings=auth, settings=config, store=store, media=media,
        terminology_resolver=publisher.resolve_terminology, worker=worker,
    )


__all__ = ["RuntimePublisher", "attach_configured_voice_intake_v2"]
