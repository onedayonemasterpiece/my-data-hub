from __future__ import annotations

import hmac
import json
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from my_data_hub.api.intake import WorkerResultConflict, WorkerResultRepository
from my_data_hub.artifact_store import LocalArtifactStore
from my_data_hub.config import Settings
from my_data_hub.db.health import verify_database
from my_data_hub.notebooks.contracts import NotebookResult


def _worker_auth(settings: Settings):  # type: ignore[no-untyped-def]
    async def dependency(
        authorization: Annotated[str | None, Header()] = None,
    ) -> None:
        token = settings.worker_result_token
        if not token:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="worker-result intake is not configured",
            )
        expected = f"Bearer {token}"
        if authorization is None or not hmac.compare_digest(authorization, expected):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )

    return dependency


async def _bounded_json_body(request: Request, max_bytes: int) -> dict[str, Any]:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > max_bytes:
            raise HTTPException(status_code=413, detail="request too large")
    try:
        value = json.loads(body or b"{}")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="request body is not valid JSON") from exc
    if not isinstance(value, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    return value


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(
        title="my-data-hub control API",
        version="0.1.0",
        docs_url=None if settings.environment in {"prod", "production"} else "/docs",
        redoc_url=None,
    )
    repository = WorkerResultRepository(
        settings.database_url,
        LocalArtifactStore(settings.artifact_root),
    )
    authenticate_worker = _worker_auth(settings)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):  # type: ignore[no-untyped-def]
        content_length = request.headers.get("content-length")
        early_response: JSONResponse | None = None
        if content_length:
            try:
                declared_size = int(content_length)
                if declared_size < 0:
                    early_response = JSONResponse(
                        status_code=400, content={"detail": "invalid content-length"}
                    )
                elif declared_size > settings.worker_result_max_bytes:
                    early_response = JSONResponse(
                        status_code=413, content={"detail": "request too large"}
                    )
            except ValueError:
                early_response = JSONResponse(
                    status_code=400, content={"detail": "invalid content-length"}
                )
        response = early_response or await call_next(request)
        response.headers["Cache-Control"] = "no-store"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "no-referrer"
        return response

    @app.get("/health/live")
    def live() -> dict[str, Any]:
        return {"ok": True, "component": "my-data-hub-control-api"}

    @app.get("/health/ready")
    def ready() -> dict[str, Any]:
        health = verify_database(settings.database_url)
        if not health.ok:
            raise HTTPException(status_code=503, detail={"findings": health.findings})
        return {
            "ok": True,
            "postgres_version": health.postgres_version,
            "schema_revision": health.schema_revision,
            "canonical_revision": health.canonical_revision,
        }

    @app.post(
        "/v1/worker-results",
        status_code=status.HTTP_202_ACCEPTED,
        dependencies=[Depends(authenticate_worker)],
    )
    async def submit_worker_result(request: Request) -> dict[str, Any]:
        raw = await _bounded_json_body(request, settings.worker_result_max_bytes)
        try:
            envelope = NotebookResult.model_validate(raw)
            return repository.store(envelope)
        except ValidationError as exc:
            raise HTTPException(status_code=422, detail=exc.errors()) from exc
        except WorkerResultConflict as exc:
            raise HTTPException(status_code=409, detail=str(exc)) from exc

    return app
