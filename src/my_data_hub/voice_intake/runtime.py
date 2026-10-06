from __future__ import annotations

from copy import deepcopy

from fastapi import FastAPI

from my_data_hub.control_plane.app import ControlPlaneSettings, create_app
from my_data_hub.voice_intake_v2.runtime import attach_configured_voice_intake_v2

from .api import attach_voice_intake_routes


def create_voice_control_app(settings: ControlPlaneSettings | None = None) -> FastAPI:
    return attach_configured_voice_intake_v2(
        attach_voice_intake_routes(create_app(settings))
    )


def serve() -> None:
    import uvicorn
    from uvicorn.config import LOGGING_CONFIG

    settings = ControlPlaneSettings.from_env()
    log_config = deepcopy(LOGGING_CONFIG)
    log_config["loggers"]["my_data_hub.voice_intake_v2.worker"] = {
        "handlers": ["default"], "level": "INFO", "propagate": False,
    }
    uvicorn.run(create_voice_control_app(settings), host=settings.host, port=settings.port, log_config=log_config)
