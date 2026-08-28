from __future__ import annotations

from typing import Any, Protocol

from fastapi import FastAPI

from my_data_hub.voice_intake.settings import VoiceIntakeSettings

from .api import attach_voice_intake_v2_routes
from .inference import AggregateGeminiInference
from .media import BoundedMediaTools
from .publisher import V2IdeaHubPublisher
from .settings import VoiceIntakeV2Settings
from .store import VoiceIntakeV2Store
from .worker import SessionPublisher, VoiceIntakeV2Worker


class RuntimePublisher(SessionPublisher, Protocol):
    async def resolve_terminology_snapshot(self) -> dict[str, Any]: ...


def attach_configured_voice_intake_v2(
    app: FastAPI,
    *,
    publisher: RuntimePublisher | None = None,
    auth_settings: VoiceIntakeSettings | None = None,
    settings: VoiceIntakeV2Settings | None = None,
) -> FastAPI:
    """Compose the real single worker while preserving the app's lifespan."""
    auth = auth_settings or VoiceIntakeSettings.from_env()
    config = settings or VoiceIntakeV2Settings.from_env()
    if not config.enabled:
        return attach_voice_intake_v2_routes(
            app,
            auth_settings=auth,
            settings=config,
        )
    runtime_publisher = publisher or V2IdeaHubPublisher(auth)
    try:
        store = VoiceIntakeV2Store(config.spool_root)
    except Exception:
        # V2 is additive. A broken/unavailable private spool must fail V2
        # closed without preventing the existing V1 application from serving.
        return attach_voice_intake_v2_routes(
            app, auth_settings=auth, settings=config, require_worker=False,
            availability_error="spool_unavailable",
        )
    media = BoundedMediaTools(
        ffprobe_timeout=config.ffprobe_timeout_seconds,
        ffmpeg_timeout=config.ffmpeg_timeout_seconds,
    )
    inference = AggregateGeminiInference(auth)
    worker = VoiceIntakeV2Worker(
        store, config, media=media, inference=inference, publisher=runtime_publisher,
    )
    return attach_voice_intake_v2_routes(
        app, auth_settings=auth, settings=config, store=store, media=media,
        terminology_resolver=runtime_publisher.resolve_terminology_snapshot, worker=worker,
    )


__all__ = ["RuntimePublisher", "attach_configured_voice_intake_v2"]
