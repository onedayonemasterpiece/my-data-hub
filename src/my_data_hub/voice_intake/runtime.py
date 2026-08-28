from __future__ import annotations

from fastapi import FastAPI

from my_data_hub.control_plane.app import ControlPlaneSettings, create_app

from .api import attach_voice_intake_routes


def create_voice_control_app(settings: ControlPlaneSettings | None = None) -> FastAPI:
    return attach_voice_intake_routes(create_app(settings))


def serve() -> None:
    import uvicorn

    settings = ControlPlaneSettings.from_env()
    uvicorn.run(create_voice_control_app(settings), host=settings.host, port=settings.port)
