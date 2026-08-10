from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from fastapi import FastAPI, HTTPException, status

DATABASE_ENVIRONMENT_NAMES = (
    "MY_DATA_HUB_DATABASE_URL",
    "MY_DATA_HUB_APPLICATION_DATABASE_URL",
    "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL",
    "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL",
    "MY_DATA_HUB_MCP_READER_DATABASE_URL",
    "MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL",
)


class ControlPlaneConfigurationError(RuntimeError):
    """Raised when the lightweight devstand profile would acquire data-plane state."""


def _disabled(name: str) -> bool:
    value = os.getenv(name, "false").strip().lower()
    if value not in {"0", "false", "no", "off"}:
        raise ControlPlaneConfigurationError(f"{name} must remain false in PR-A")
    return False


@dataclass(frozen=True, slots=True)
class ControlPlaneSettings:
    host: str = "127.0.0.1"
    port: int = 8080
    environment: str = "production"
    scheduler_enabled: bool = False
    production_publish_enabled: bool = False
    remote_mcp_writes_enabled: bool = False

    def __post_init__(self) -> None:
        if not self.host or not 1 <= self.port <= 65535:
            raise ControlPlaneConfigurationError("control-plane listener is invalid")
        if self.scheduler_enabled or self.production_publish_enabled or self.remote_mcp_writes_enabled:
            raise ControlPlaneConfigurationError("PR-A control-plane write and publication gates must remain false")

    @classmethod
    def from_env(cls) -> ControlPlaneSettings:
        leaked = [name for name in DATABASE_ENVIRONMENT_NAMES if os.getenv(name, "").strip()]
        if leaked:
            raise ControlPlaneConfigurationError(
                "lightweight control plane must not receive master database credentials: "
                + ", ".join(leaked)
            )
        try:
            port = int(os.getenv("MY_DATA_HUB_CONTROL_PORT", "8080"))
        except ValueError as exc:
            raise ControlPlaneConfigurationError("MY_DATA_HUB_CONTROL_PORT must be an integer") from exc
        if not 1 <= port <= 65535:
            raise ControlPlaneConfigurationError("MY_DATA_HUB_CONTROL_PORT must be a valid TCP port")
        return cls(
            host=os.getenv("MY_DATA_HUB_CONTROL_HOST", "127.0.0.1").strip(),
            port=port,
            environment=os.getenv("MY_DATA_HUB_ENVIRONMENT", "production").strip().lower(),
            scheduler_enabled=_disabled("MY_DATA_HUB_SCHEDULER_ENABLED"),
            production_publish_enabled=_disabled("MY_DATA_HUB_PRODUCTION_PUBLISH_ENABLED"),
            remote_mcp_writes_enabled=_disabled("MY_DATA_HUB_MCP_WRITE_ENABLED"),
        )


def create_app(settings: ControlPlaneSettings | None = None) -> FastAPI:
    runtime = settings or ControlPlaneSettings.from_env()
    app = FastAPI(title="my-data-hub lightweight control plane", docs_url=None, redoc_url=None)

    def snapshot() -> dict[str, Any]:
        return {
            "control_plane_ready": True,
            "data_plane_ready": False,
            "master_state": "ABSENT",
            "master_instance_id": None,
            "master_epoch": None,
            "canonical_database_runtime": "kaggle_notebook",
            "lifecycle_implementation": "deferred_to_fakekaggle_phase",
            "production_publication": runtime.production_publish_enabled,
            "remote_mcp_writes": runtime.remote_mcp_writes_enabled,
        }

    @app.get("/health/live")
    def live() -> dict[str, Any]:
        return {"ok": True, "component": "my-data-hub-control-plane"}

    @app.get("/health/ready")
    def ready() -> dict[str, Any]:
        return {"ok": True, **snapshot()}

    @app.get("/control/v1/master")
    def master() -> dict[str, Any]:
        return snapshot()

    @app.api_route(
        "/{data_path:path}",
        methods=["GET", "POST", "PUT", "PATCH", "DELETE"],
        include_in_schema=False,
    )
    def unavailable_data_plane(data_path: str) -> None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={
                "code": "master_absent",
                "path": f"/{data_path}",
                "message": "data-plane operation unavailable; master lifecycle is deferred after PR-A",
            },
        )

    return app


def serve() -> None:
    import uvicorn

    settings = ControlPlaneSettings.from_env()
    uvicorn.run(create_app(settings), host=settings.host, port=settings.port)
