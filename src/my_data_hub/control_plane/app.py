from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from fastapi import FastAPI, Header, HTTPException, Request, status

from my_data_hub.control_plane.adapters import LedgerMasterResolver
from my_data_hub.control_plane.ledger import ControlLedger, EventRejected
from my_data_hub.control_plane.runtime import (
    ControlPlaneMasterRuntime,
    MasterRuntimeSettings,
    SessionCredential,
    SessionCredentialRegistrar,
    build_production_runtime,
)

DATABASE_ENVIRONMENT_NAMES = (
    "MY_DATA_HUB_DATABASE_URL",
    "MY_DATA_HUB_MIGRATOR_DATABASE_URL",
    "MY_DATA_HUB_APPLICATION_DATABASE_URL",
    "MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL",
    "MY_DATA_HUB_ORCHESTRATOR_DATABASE_URL",
    "MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL",
    "MY_DATA_HUB_MONITORING_DATABASE_URL",
    "MY_DATA_HUB_MIGRATION_OPERATOR_DATABASE_URL",
    "MY_DATA_HUB_MCP_READER_DATABASE_URL",
    "MY_DATA_HUB_MCP_REVOCATION_DATABASE_URL",
    "MY_DATA_HUB_BACKUP_DATABASE_URL",
    "MY_DATA_HUB_RESTORE_DATABASE_URL",
    "MY_DATA_HUB_RECOVERY_CONTROL_DATABASE_URL",
    "MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL",
    "DATABASE_URL",
    "PGHOST",
    "PGPORT",
    "PGDATABASE",
    "PGUSER",
    "PGPASSWORD",
    "PGPASSFILE",
    "PGSERVICE",
    "PGSERVICEFILE",
)


class ControlPlaneConfigurationError(RuntimeError):
    """Raised when the lightweight devstand profile would acquire data-plane state."""


def _session_credential(
    item: object,
    identity: dict[str, Any],
    *,
    now: datetime,
    latest_expiry: datetime,
) -> SessionCredential:
    if not isinstance(item, dict) or set(item) != {"role", "database_url", "expires_at"}:
        raise HTTPException(status_code=422, detail={"code": "invalid_credential"})
    if item.get("role") != "reader":
        raise HTTPException(status_code=422, detail={"code": "credential_role_not_allowed"})
    try:
        expires_at = datetime.fromisoformat(str(item["expires_at"]).replace("Z", "+00:00")).astimezone(UTC)
    except (TypeError, ValueError) as exc:
        raise HTTPException(status_code=422, detail={"code": "credential_expiry_invalid"}) from exc
    if expires_at <= now or expires_at > latest_expiry:
        raise HTTPException(status_code=422, detail={"code": "credential_expiry_out_of_bounds"})
    database_url = str(item.get("database_url", ""))
    parsed = urlparse(database_url)
    query = parse_qs(parsed.query)
    if (
        parsed.scheme not in {"postgresql", "postgres"}
        or parsed.hostname not in {"127.0.0.1", "localhost", "::1"}
        or not parsed.username
        or not parsed.password
        or query.get("sslmode", [""])[0] not in {"verify-ca", "verify-full"}
    ):
        raise HTTPException(status_code=422, detail={"code": "credential_database_url_invalid"})
    return SessionCredential(
        master_instance_id=str(identity["master_instance_id"]),
        epoch=int(identity["epoch"]),
        role="reader",
        database_url=database_url,
        expires_at=expires_at,
    )


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
    ledger_path: Path | None = None
    master_runtime: MasterRuntimeSettings | None = None
    session_credentials_path: Path | None = None

    def __post_init__(self) -> None:
        if not self.host or not 1 <= self.port <= 65535:
            raise ControlPlaneConfigurationError("control-plane listener is invalid")
        if self.scheduler_enabled or self.production_publish_enabled or self.remote_mcp_writes_enabled:
            raise ControlPlaneConfigurationError("PR-A control-plane write and publication gates must remain false")

    @classmethod
    def from_env(cls) -> ControlPlaneSettings:
        candidates = set(DATABASE_ENVIRONMENT_NAMES)
        candidates.update(name for name in os.environ if name.endswith("_DATABASE_URL"))
        candidates.update(name for name in os.environ if name.startswith("PG"))
        leaked = sorted(name for name in candidates if os.getenv(name, "").strip())
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
            ledger_path=Path(
                os.getenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", "/state/control.sqlite3")
            ).expanduser(),
            master_runtime=MasterRuntimeSettings.from_env(),
            session_credentials_path=Path(
                os.getenv("MY_DATA_HUB_MASTER_SESSION_DIR", "/state/master-sessions")
            ).expanduser(),
        )


def create_app(
    settings: ControlPlaneSettings | None = None,
    *,
    ledger: ControlLedger | None = None,
    master_runtime: ControlPlaneMasterRuntime | None = None,
    session_registrar: SessionCredentialRegistrar | None = None,
) -> FastAPI:
    runtime = settings or ControlPlaneSettings.from_env()
    ledger_path = runtime.ledger_path or Path(tempfile.mkdtemp(prefix="mdh-control-")) / "control.sqlite3"
    control_ledger = ledger or ControlLedger(ledger_path)
    if master_runtime is None:
        production = build_production_runtime(
            control_ledger,
            runtime.master_runtime,
            session_credentials_path=runtime.session_credentials_path,
        )
        master_runtime = production.master
        provider_status = production.provider_status
        session_registrar = session_registrar or production.session_registrar
    else:
        if master_runtime.ledger.path != control_ledger.path:
            raise ControlPlaneConfigurationError("master runtime and app must share one control ledger")
        provider_status = "available"
    resolver = LedgerMasterResolver(control_ledger)
    app = FastAPI(title="my-data-hub lightweight control plane", docs_url=None, redoc_url=None)
    app.state.control_ledger = control_ledger
    app.state.master_runtime = master_runtime
    app.state.master_coordinator = master_runtime.coordinator if master_runtime is not None else None
    app.state.master_provider_status = provider_status
    app.state.session_registrar = session_registrar

    def snapshot() -> dict[str, Any]:
        master = resolver.resolve_master(_system_identity())
        return {
            "control_plane_ready": True,
            "data_plane_ready": master.state.value == "ACTIVE",
            "master_state": master.state.value,
            "master_instance_id": master.instance_id,
            "master_epoch": master.epoch,
            "canonical_database_runtime": "kaggle_notebook",
            "lifecycle_implementation": "durable_control_ledger_v1",
            "production_publication": runtime.production_publish_enabled,
            "remote_mcp_writes": runtime.remote_mcp_writes_enabled,
        }

    def _system_identity():  # type: ignore[no-untyped-def]
        from my_data_hub.mcp.oauth import AccessIdentity

        return AccessIdentity(
            subject="control-health", client_id="control-health", scopes=frozenset(),
            audience="local-control", token_id="control-health", expires_at=2**63 - 1,
            issuer="local-control", issued_at=0, resource="local-control",
        )

    @app.get("/health/live")
    def live() -> dict[str, Any]:
        return {"ok": True, "component": "my-data-hub-control-plane"}

    @app.get("/health/ready")
    def ready() -> dict[str, Any]:
        return {"ok": True, **snapshot()}

    @app.get("/control/v1/master")
    def master() -> dict[str, Any]:
        return snapshot()

    @app.post("/control/v1/master/ensure")
    async def ensure_master(request: Request) -> dict[str, Any]:
        raw = await request.body()
        if len(raw) > 16 * 1024:
            raise HTTPException(status_code=413, detail={"code": "intent_too_large"})
        body = await request.json()
        if not isinstance(body, dict):
            raise HTTPException(status_code=400, detail={"code": "invalid_intent"})
        key = str(body.get("idempotency_key", "")).strip()
        if not 8 <= len(key) <= 300:
            raise HTTPException(status_code=422, detail={"code": "idempotency_key_required"})
        if master_runtime is None:
            raise HTTPException(status_code=503, detail={"code": "provider_unavailable"})
        try:
            handle, duplicate = master_runtime.ensure(key)
        except Exception as exc:
            # Provider details can contain credential paths/account data.  Keep
            # the durable operation resumable and return only a stable code.
            raise HTTPException(status_code=503, detail={"code": "provider_unavailable"}) from exc
        return {
            "operation_id": handle.operation_id,
            "master_state": handle.state.value,
            "duplicate": duplicate,
            "terminal": handle.state.value in {"ACTIVE", "STOPPED", "FAILED", "FENCED", "ORPHANED"},
        }

    @app.get("/control/v1/operations/{operation_id}")
    def operation(operation_id: str) -> dict[str, Any]:
        record = control_ledger.get_operation(operation_id)
        if record is None:
            raise HTTPException(status_code=404, detail={"code": "operation_not_found"})
        return {
            "operation_id": record.operation_id,
            "kind": record.operation_kind,
            "state": record.state,
            "updated_at": record.updated_at.isoformat(),
        }

    @app.post("/internal/runtime/events")
    async def runtime_event(
        request: Request, authorization: str | None = Header(default=None)
    ) -> dict[str, Any]:
        # Callback ingestion exists independently of the provider worker. The
        # coordinator validates exact run/attempt/epoch and the per-run token;
        # without an attached coordinator this endpoint stays fail closed.
        coordinator = getattr(app.state, "master_coordinator", None)
        if coordinator is None:
            raise HTTPException(status_code=503, detail={"code": "orchestrator_unavailable"})
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_required"})
        raw = await request.body()
        if len(raw) > 64 * 1024:
            raise HTTPException(status_code=413, detail={"code": "runtime_event_too_large"})
        try:
            receipt = coordinator.accept_runtime_event(
                raw, header_token=authorization.removeprefix("Bearer ").strip()
            )
        except EventRejected as exc:
            raise HTTPException(status_code=400, detail={"code": "runtime_event_rejected"}) from exc
        return {
            "event_id": receipt.event_id,
            "disposition": receipt.disposition.value,
            "body_sha256": receipt.body_sha256,
        }

    @app.get("/internal/runtime/activation/{run_id}/{attempt_id}")
    def runtime_activation(
        run_id: str,
        attempt_id: str,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        """Return only the exact epoch activation decision to its notebook.

        The database gate stays closed until the service.ready callback was
        accepted atomically into the ACTIVE registry projection.  The same
        per-run secret used for callback authentication authorizes this bounded
        poll; no database credential or endpoint is returned.
        """

        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_required"})
        token = authorization.removeprefix("Bearer ").strip()
        if not control_ledger.runtime_token_valid(run_id, attempt_id, token):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_invalid"})
        operation = control_ledger.operation_for_attempt(run_id, attempt_id)
        if operation is None:
            raise HTTPException(status_code=404, detail={"code": "runtime_attempt_not_found"})
        service = control_ledger.resolve_service("postgres-master")
        identity = operation.identity
        active = bool(
            operation.state == "ACTIVE"
            and service is not None
            and service.run_id == run_id
            and service.attempt_id == attempt_id
            and service.service_instance_id == identity.get("service_instance_id")
            and service.master_instance_id == identity.get("master_instance_id")
            and service.epoch == int(identity.get("epoch", 0))
        )
        return {
            "active": active,
            "state": operation.state,
            "master_instance_id": identity.get("master_instance_id"),
            "epoch": int(identity.get("epoch", 0)),
        }

    @app.post("/internal/runtime/session-credentials/{run_id}/{attempt_id}")
    async def runtime_session_credentials(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        registrar = app.state.session_registrar
        if registrar is None:
            raise HTTPException(status_code=503, detail={"code": "credential_registrar_unavailable"})
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_required"})
        token = authorization.removeprefix("Bearer ").strip()
        if not control_ledger.runtime_token_valid(run_id, attempt_id, token):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_invalid"})
        raw = await request.body()
        if len(raw) > 16 * 1024:
            raise HTTPException(status_code=413, detail={"code": "credential_envelope_too_large"})
        operation = control_ledger.operation_for_attempt(run_id, attempt_id)
        if operation is None or operation.state not in {"REGISTERING", "ACTIVE"}:
            raise HTTPException(status_code=409, detail={"code": "runtime_attempt_not_registering"})
        try:
            body = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail={"code": "invalid_credential_envelope"}) from exc
        identity = operation.identity
        if (
            not isinstance(body, dict)
            or set(body) != {"master_instance_id", "epoch", "credentials"}
            or str(body.get("master_instance_id")) != str(identity.get("master_instance_id"))
            or int(body.get("epoch", 0)) != int(identity.get("epoch", 0))
            or not isinstance(body.get("credentials"), list)
            or not 1 <= len(body["credentials"]) <= 4
        ):
            raise HTTPException(status_code=422, detail={"code": "credential_identity_mismatch"})
        now = datetime.now(UTC)
        latest_expiry = now + timedelta(minutes=5)
        if operation.state == "ACTIVE":
            service = control_ledger.resolve_service("postgres-master", now=now)
            if (
                service is None
                or service.run_id != run_id
                or service.attempt_id != attempt_id
                or service.master_instance_id != str(identity["master_instance_id"])
                or service.epoch != int(identity["epoch"])
            ):
                raise HTTPException(status_code=409, detail={"code": "active_epoch_mismatch"})
            latest_expiry = service.lease_until
        credentials = [
            _session_credential(item, identity, now=now, latest_expiry=latest_expiry)
            for item in body["credentials"]
        ]
        references: list[str] = []
        for credential in credentials:
            reference = registrar.store(credential)
            references.append(Path(reference).name)
        return {"registered": len(references), "credential_refs": references}

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
