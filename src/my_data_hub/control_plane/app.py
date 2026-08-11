from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Header, HTTPException, Request, status

from my_data_hub.control_plane.adapters import LedgerMasterResolver
from my_data_hub.control_plane.ledger import ControlLedger, EventRejected

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
        )


def create_app(
    settings: ControlPlaneSettings | None = None,
    *,
    ledger: ControlLedger | None = None,
) -> FastAPI:
    runtime = settings or ControlPlaneSettings.from_env()
    ledger_path = runtime.ledger_path or Path(tempfile.mkdtemp(prefix="mdh-control-")) / "control.sqlite3"
    control_ledger = ledger or ControlLedger(ledger_path)
    resolver = LedgerMasterResolver(control_ledger)
    app = FastAPI(title="my-data-hub lightweight control plane", docs_url=None, redoc_url=None)
    app.state.control_ledger = control_ledger

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
        operation_id = __import__("hashlib").sha256(f"ensure:{key}".encode()).hexdigest()[:32]
        record, created = control_ledger.ensure_operation(
            operation_id=operation_id,
            idempotency_key=key,
            operation_kind="ensure_master",
            intent={"intent": str(body.get("intent", "control-api"))[:300]},
            initial_state="REQUESTED",
            identity={"requested_via": "control-api"},
        )
        return {
            "operation_id": record.operation_id,
            "master_state": record.state,
            "duplicate": not created,
            "terminal": False,
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
