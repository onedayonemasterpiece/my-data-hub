from __future__ import annotations

import hmac
import json
from typing import Annotated, Any
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from pydantic import ValidationError

from my_data_hub.api.intake import WorkerResultConflict, WorkerResultRepository
from my_data_hub.artifact_store import LocalArtifactStore
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.connectors.contracts import ConnectorContractError
from my_data_hub.connectors.postgres import PostgresConnectorAcceptanceRepository
from my_data_hub.connectors.repository import AcceptanceDisposition
from my_data_hub.connectors.service import (
    ConnectorAuthorizationError,
    ConnectorIntakeService,
)
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


def _connector_auth(settings: Settings):  # type: ignore[no-untyped-def]
    async def dependency(
        authorization: Annotated[str | None, Header()] = None,
    ) -> tuple[str, str]:
        credentials = settings.connector_credentials
        if not credentials:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="connector intake is not configured",
            )
        supplied = authorization[7:] if authorization and authorization.startswith("Bearer ") else ""
        matched: str | None = None
        for connector_id, secret in credentials:
            if hmac.compare_digest(supplied, secret):
                matched = connector_id
        if matched is None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="authentication required",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return matched, f"service:{matched}"

    return dependency


async def _bounded_body_bytes(request: Request, max_bytes: int) -> bytes:
    body = bytearray()
    async for chunk in request.stream():
        body.extend(chunk)
        if len(body) > max_bytes:
            raise HTTPException(status_code=413, detail="request too large")
    return bytes(body)


async def _bounded_json_body(request: Request, max_bytes: int) -> dict[str, Any]:
    body = await _bounded_body_bytes(request, max_bytes)
    try:
        value = json.loads(body or b"{}")
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=400, detail="request body is not valid JSON") from exc
    if not isinstance(value, dict):
        raise HTTPException(status_code=422, detail="request body must be a JSON object")
    return value


def create_app(settings: Settings) -> FastAPI:
    if settings.environment in {"prod", "production"}:
        missing = [
            name
            for name, value in (
                ("MY_DATA_HUB_APPLICATION_DATABASE_URL", settings.application_database_url),
                (
                    "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL",
                    settings.connector_intake_database_url,
                ),
            )
            if not value
        ]
        if missing:
            raise ConfigurationError(
                "production API restricted database identities are incomplete: "
                + ", ".join(missing)
            )
        if not settings.worker_result_token:
            raise ConfigurationError("worker result token is required by the production API")
    app = FastAPI(
        title="my-data-hub control API",
        version="0.1.0",
        docs_url=None if settings.environment in {"prod", "production"} else "/docs",
        redoc_url=None,
    )
    repository = WorkerResultRepository(
        settings.application_database_url or settings.database_url,
        LocalArtifactStore(settings.artifact_root),
    )
    authenticate_worker = _worker_auth(settings)
    connector_repository = PostgresConnectorAcceptanceRepository(
        settings.connector_intake_database_url or settings.database_url
    )
    connector_service = ConnectorIntakeService(
        connector_repository,
        max_envelope_bytes=settings.connector_intake_max_bytes,
    )
    authenticate_connector = _connector_auth(settings)

    @app.middleware("http")
    async def security_headers(request: Request, call_next):  # type: ignore[no-untyped-def]
        request.state.correlation_id = uuid4().hex
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
        response.headers["X-Correlation-ID"] = request.state.correlation_id
        return response

    @app.get("/health/live")
    def live() -> dict[str, Any]:
        return {"ok": True, "component": "my-data-hub-control-api"}

    @app.get("/health/ready")
    def ready() -> dict[str, Any]:
        health = verify_database(settings.application_database_url or settings.database_url)
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

    @app.post("/intake/v1/batches", status_code=status.HTTP_202_ACCEPTED)
    async def submit_connector_batch(
        request: Request,
        connector_auth: Annotated[tuple[str, str], Depends(authenticate_connector)],
    ) -> JSONResponse:
        media_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if media_type != "application/json":
            raise HTTPException(status_code=415, detail="content-type must be application/json")
        exact_bytes = await _bounded_body_bytes(request, settings.connector_intake_max_bytes)
        connector_id, principal = connector_auth
        try:
            decision = connector_service.submit(
                exact_bytes,
                authenticated_connector_id=connector_id,
                authenticated_principal=principal,
                correlation_id=request.state.correlation_id,
            )
        except ConnectorContractError as exc:
            raise HTTPException(status_code=422, detail=str(exc)) from exc
        except ConnectorAuthorizationError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        except PermissionError as exc:
            raise HTTPException(status_code=403, detail=str(exc)) from exc
        if decision.disposition is AcceptanceDisposition.QUARANTINED:
            assert decision.quarantine is not None
            return JSONResponse(
                status_code=409,
                content={
                    "status": "conflicting_replay",
                    "quarantine_id": str(decision.quarantine.quarantine_id),
                    "batch_id": str(decision.quarantine.incoming_batch_id),
                },
            )
        assert decision.receipt is not None
        code = 200 if decision.disposition is AcceptanceDisposition.REPLAYED else 202
        return JSONResponse(
            status_code=code,
            content=decision.receipt.model_dump(mode="json"),
        )

    @app.get("/intake/v1/batches/{batch_id}/receipt")
    def connector_batch_receipt(
        batch_id: UUID,
        connector_auth: Annotated[tuple[str, str], Depends(authenticate_connector)],
    ) -> dict[str, Any]:
        receipt = connector_repository.get_receipt(batch_id)
        if receipt is None:
            raise HTTPException(status_code=404, detail="receipt not found")
        if receipt.connector_id != connector_auth[0]:
            raise HTTPException(status_code=404, detail="receipt not found")
        return receipt.model_dump(mode="json")

    @app.get("/intake/v1/connectors/{connector_id}/health")
    def connector_health(
        connector_id: str,
        connector_auth: Annotated[tuple[str, str], Depends(authenticate_connector)],
    ) -> dict[str, Any]:
        if connector_id != connector_auth[0]:
            raise HTTPException(status_code=404, detail="connector not found")
        try:
            return connector_repository.health(connector_id)
        except LookupError as exc:
            raise HTTPException(status_code=404, detail="connector not found") from exc

    return app
