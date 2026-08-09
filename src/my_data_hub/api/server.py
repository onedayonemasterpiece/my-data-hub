from __future__ import annotations

from my_data_hub.api.app import create_app
from my_data_hub.config import Settings


def serve() -> None:
    try:
        import uvicorn
    except ImportError as exc:  # pragma: no cover
        raise RuntimeError("uvicorn is required to run the API") from exc
    settings = Settings.from_env()
    uvicorn.run(
        create_app(settings),
        host=settings.api_host,
        port=settings.api_port,
        log_level=settings.log_level.lower(),
        proxy_headers=False,
        server_header=False,
    )
