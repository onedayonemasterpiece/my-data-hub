from __future__ import annotations

import asyncio
import json
import os
import tempfile
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID

from fastapi import FastAPI, Header, HTTPException, Request, status

from my_data_hub.checkpoints import CheckpointManifest, ControlLedgerCheckpointRegistry
from my_data_hub.checkpoints.manifest import ManifestError
from my_data_hub.control_plane.adapters import LedgerMasterResolver
from my_data_hub.control_plane.ledger import ControlLedger, ControlLedgerError, EventRejected, StaleRuntimeEvent
from my_data_hub.control_plane.runtime import (
    ControlPlaneMasterRuntime,
    MasterRuntimeSettings,
    SessionCredential,
    SessionCredentialRegistrar,
    build_production_runtime,
)
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal
from my_data_hub.providers.kaggle.contracts import (
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderKind
from my_data_hub.workloads.bloggers.master_stage import (
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
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
        or query.get("sslrootcert", [""])[0] != "/state/master-tls/ca.pem"
        or query.get("connect_timeout", [""])[0] != "5"
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
                "lightweight control plane must not receive master database credentials: " + ", ".join(leaked)
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
            ledger_path=Path(os.getenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", "/state/control.sqlite3")).expanduser(),
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
    provider_journal = ControlLedgerKaggleJournal(control_ledger)

    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        stopped = asyncio.Event()
        task: asyncio.Task[None] | None = None

        async def reconcile_requests() -> None:
            while not stopped.is_set():
                if master_runtime is not None:
                    with suppress(Exception):
                        await asyncio.to_thread(master_runtime.reconcile_requested_once)
                        reconcile_acceptance = getattr(master_runtime, "reconcile_acceptance_once", None)
                        if reconcile_acceptance is not None:
                            await asyncio.to_thread(reconcile_acceptance)
                        # The durable request remains PENDING; provider details
                        # never enter logs/responses and bounded retry resumes.
                try:
                    await asyncio.wait_for(stopped.wait(), timeout=5.0)
                except TimeoutError:
                    continue

        if master_runtime is not None:
            task = asyncio.create_task(reconcile_requests())
        try:
            yield
        finally:
            stopped.set()
            if task is not None:
                await task

    app = FastAPI(
        title="my-data-hub lightweight control plane",
        docs_url=None,
        redoc_url=None,
        lifespan=lifespan,
    )
    app.state.control_ledger = control_ledger
    app.state.master_runtime = master_runtime
    app.state.master_coordinator = master_runtime.coordinator if master_runtime is not None else None
    app.state.master_provider_status = provider_status
    app.state.session_registrar = session_registrar
    app.state.reconcile_master_requests = (
        master_runtime.reconcile_requested_once if master_runtime is not None else None
    )

    def _configured_master_assets():  # type: ignore[no-untyped-def]
        configured = runtime.master_runtime or (master_runtime.settings if master_runtime is not None else None)
        return configured.assets if configured else None

    def _current_checkpoint_claim(provider_ref: str) -> TaskResourceClaim | None:
        payload = control_ledger.latest_provider_resource_claim(
            provider_ref=provider_ref,
            resource_kind=ProviderKind.DATASET.value,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED.value,
        )
        return TaskResourceClaim.model_validate(payload) if payload else None

    def _runtime_authority(
        *,
        authorization: str | None,
        run_id: str | None,
        attempt_id: str | None,
        master_instance_id: str | None,
        epoch: str | None,
        allowed_states: frozenset[str] = frozenset({"ACTIVE", "DRAINING", "CHECKPOINTING"}),
    ):  # type: ignore[no-untyped-def]
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_required"})
        token = authorization.removeprefix("Bearer ").strip()
        try:
            exact_run_id = str(UUID(str(run_id)))
            exact_attempt_id = str(UUID(str(attempt_id)))
            exact_master_id = str(UUID(str(master_instance_id)))
            exact_epoch = int(str(epoch))
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=401, detail={"code": "runtime_identity_invalid"}) from exc
        if exact_epoch < 1 or not control_ledger.runtime_token_valid(exact_run_id, exact_attempt_id, token):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_invalid"})
        operation = control_ledger.operation_for_attempt(exact_run_id, exact_attempt_id)
        if operation is None:
            raise HTTPException(status_code=401, detail={"code": "runtime_attempt_unknown"})
        identity = operation.identity
        if (
            str(identity.get("run_id")) != exact_run_id
            or str(identity.get("attempt_id")) != exact_attempt_id
            or str(identity.get("master_instance_id")) != exact_master_id
            or int(identity.get("epoch", 0)) != exact_epoch
            or control_ledger.current_epoch("postgres-master") != exact_epoch
            or operation.state not in allowed_states
        ):
            raise HTTPException(status_code=409, detail={"code": "runtime_epoch_fenced"})
        return operation

    async def _bounded_json(request: Request) -> dict[str, Any]:
        raw = await request.body()
        if len(raw) > 256 * 1024:
            raise HTTPException(status_code=413, detail={"code": "metadata_too_large"})
        try:
            value = await request.json()
        except Exception as exc:
            raise HTTPException(status_code=400, detail={"code": "invalid_json"}) from exc
        if not isinstance(value, dict):
            raise HTTPException(status_code=400, detail={"code": "invalid_json_object"})

        forbidden = {"authorization", "database_url", "dsn", "password", "private_key", "secret", "token"}

        def inspect(candidate: object) -> None:
            if isinstance(candidate, dict):
                for key, nested in candidate.items():
                    if str(key).lower() in forbidden:
                        raise HTTPException(status_code=422, detail={"code": "secret_or_bytes_forbidden"})
                    inspect(nested)
            elif isinstance(candidate, list):
                for nested in candidate:
                    inspect(nested)
            elif isinstance(candidate, str) and len(candidate) > 16 * 1024:
                raise HTTPException(status_code=422, detail={"code": "inline_bytes_forbidden"})

        inspect(value)
        return value

    def _checkpoint_record(checkpoint_id: str | None) -> dict[str, Any] | None:
        if checkpoint_id is None:
            return None
        record = control_ledger.checkpoint_candidate(checkpoint_id)
        if record is None or record["status"] != "VERIFIED" or not record["version_ref"]:
            raise HTTPException(status_code=409, detail={"code": "checkpoint_head_invalid"})
        return {
            "checkpoint_id": record["checkpoint_id"],
            "dataset_ref": record["dataset_ref"],
            "exact_version_ref": record["version_ref"],
            "manifest_sha256": record["manifest_sha256"],
        }

    def _checkpoint_head(service_kind: str) -> dict[str, Any]:
        if service_kind != "postgres-master":
            raise HTTPException(status_code=404, detail={"code": "checkpoint_service_unknown"})
        head = control_ledger.checkpoint_head(service_kind)
        return {
            "generation": head.generation if head else 0,
            "current": _checkpoint_record(head.current_checkpoint_id) if head else None,
            "previous": _checkpoint_record(head.previous_checkpoint_id) if head else None,
        }

    def _assert_checkpoint_owner(checkpoint_id: str, operation_id: str, service_kind: str) -> None:
        record = control_ledger.checkpoint_candidate(checkpoint_id)
        if record is None or record["operation_id"] != operation_id or record["service_kind"] != service_kind:
            raise HTTPException(status_code=403, detail={"code": "checkpoint_authority_mismatch"})

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
            subject="control-health",
            client_id="control-health",
            scopes=frozenset(),
            audience="local-control",
            token_id="control-health",
            expires_at=2**63 - 1,
            issuer="local-control",
            issued_at=0,
            resource="local-control",
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

    @app.post("/control/v1/blogger-closure/requests")
    async def request_blogger_closure(request: Request) -> dict[str, Any]:
        body = await _bounded_json(request)
        try:
            migration = BloggerMigrationRequest.model_validate(body)
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "blogger_request_invalid"}) from exc
        operation = control_ledger.get_operation(str(migration.operation_id))
        if operation is None or operation.operation_kind != "ensure_master":
            raise HTTPException(status_code=409, detail={"code": "master_operation_invalid"})
        identity = operation.identity
        service = control_ledger.resolve_service("postgres-master")
        if (
            operation.state != "ACTIVE"
            or service is None
            or control_ledger.current_epoch("postgres-master") != int(identity["epoch"])
            or service.epoch != int(identity["epoch"])
            or service.run_id != str(identity["run_id"])
            or service.attempt_id != str(identity["attempt_id"])
            or service.master_instance_id != str(identity["master_instance_id"])
        ):
            raise HTTPException(status_code=409, detail={"code": "master_not_active"})
        record, created = control_ledger.ensure_blogger_migration_request(
            request_id=str(migration.request_id),
            operation_id=str(migration.operation_id),
            request_sha256=migration.request_sha256,
            request=migration.model_dump(mode="json"),
        )
        return {
            "request_id": record["request_id"],
            "request_sha256": record["request_sha256"],
            "state": record["state"],
            "created": created,
        }

    @app.get("/control/v1/blogger-closure/requests/{request_id}")
    def blogger_closure_status(request_id: str) -> dict[str, Any]:
        try:
            exact_id = str(UUID(request_id))
        except ValueError as exc:
            raise HTTPException(status_code=404, detail={"code": "blogger_request_not_found"}) from exc
        record = control_ledger.blogger_migration_request(exact_id)
        if record is None:
            raise HTTPException(status_code=404, detail={"code": "blogger_request_not_found"})
        if record["state"] in {"CLAIMED", "IMPORT_COMMITTED"} and master_runtime is not None:
            source_operation = control_ledger.get_operation(record["operation_id"])
            if source_operation is not None:
                with suppress(Exception):
                    master_runtime.coordinator.reconcile_operation(
                        source_operation.operation_id,
                        master_runtime.intent(source_operation.idempotency_key),
                    )
            record = control_ledger.reconcile_abandoned_blogger_migration_request(exact_id)
            assert record is not None
        if record["state"] == "IMPORT_COMMITTED":
            recovered = control_ledger.verified_checkpoint_for_operation(record["operation_id"])
            if recovered is not None:
                record = control_ledger.record_blogger_checkpoint_receipt(
                    request_id=exact_id,
                    run_id=str(record["claimed_run_id"]),
                    attempt_id=str(record["claimed_attempt_id"]),
                    receipt={
                        "request_id": exact_id,
                        "checkpoint_id": recovered["checkpoint_id"],
                        "manifest_sha256": recovered["manifest_sha256"],
                        "current_checkpoint_id": recovered["checkpoint_id"],
                        "canonical_revision": record["import_receipt"]["canonical_revision"],
                    },
                )
                control_ledger.record_connector_coverage(
                    connector_kind="region-talk-ydb-bloggers-v1",
                    contract_version="my-data-hub-blogger-closure.v1",
                    state="COMPLETE",
                    observed_at=control_ledger.clock.now(),
                )
        return {key: value for key, value in record.items() if key not in {"failure_code"} or value is not None}

    @app.get("/internal/runtime/blogger-migration/{run_id}/{attempt_id}")
    def claim_blogger_migration(
        run_id: str,
        attempt_id: str,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        record = control_ledger.claim_blogger_migration_request(
            operation_id=operation.operation_id,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=str(operation.identity["master_instance_id"]),
            epoch=int(operation.identity["epoch"]),
        )
        return (
            {"available": False}
            if record is None or record["state"] != "CLAIMED"
            else {
                "available": True,
                "request": record["request"],
                "request_sha256": record["request_sha256"],
                "state": record["state"],
            }
        )

    @app.post("/internal/runtime/blogger-migration/{run_id}/{attempt_id}/import-receipt")
    async def blogger_import_receipt(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        body = await _bounded_json(request)
        try:
            receipt = BloggerImportStageReceipt.model_validate(body)
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "blogger_receipt_invalid"}) from exc
        if (str(receipt.run_id), receipt.epoch, str(receipt.master_instance_id)) != (
            run_id,
            int(epoch or 0),
            str(master_instance_id),
        ):
            raise HTTPException(status_code=409, detail={"code": "blogger_receipt_epoch_mismatch"})
        record = control_ledger.record_blogger_import_receipt(
            request_id=str(receipt.request_id),
            run_id=run_id,
            attempt_id=attempt_id,
            receipt=receipt.model_dump(mode="json"),
        )
        return {"accepted": True, "state": record["state"], "receipt_sha256": receipt.receipt_sha256}

    @app.post("/internal/runtime/blogger-migration/{run_id}/{attempt_id}/failed")
    async def blogger_migration_failed(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        body = await _bounded_json(request)
        if set(body) != {"request_id", "failure_code"}:
            raise HTTPException(status_code=422, detail={"code": "blogger_failure_invalid"})
        try:
            control_ledger.fail_blogger_migration_request(
                request_id=str(body["request_id"]),
                run_id=run_id,
                attempt_id=attempt_id,
                failure_code=str(body["failure_code"]),
            )
        except StaleRuntimeEvent as exc:
            raise HTTPException(status_code=409, detail={"code": "blogger_import_already_committed"}) from exc
        return {"accepted": True, "state": "FAILED"}

    @app.post("/internal/runtime/blogger-migration/{run_id}/{attempt_id}/checkpoint-receipt")
    async def blogger_checkpoint_receipt(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"DRAINING", "CHECKPOINTING", "STOPPED"}),
        )
        body = await _bounded_json(request)
        required = {"request_id", "checkpoint_id", "manifest_sha256", "current_checkpoint_id", "canonical_revision"}
        if set(body) != required:
            raise HTTPException(status_code=422, detail={"code": "blogger_checkpoint_receipt_invalid"})
        record = control_ledger.blogger_migration_request(str(body["request_id"]))
        if record is None or record["import_receipt"]["canonical_revision"] != body["canonical_revision"]:
            raise HTTPException(status_code=409, detail={"code": "blogger_checkpoint_revision_mismatch"})
        head = control_ledger.checkpoint_head("postgres-master")
        candidate = control_ledger.checkpoint_candidate(str(body["checkpoint_id"]))
        if (
            head is None
            or head.current_checkpoint_id != body["checkpoint_id"]
            or candidate is None
            or candidate["status"] != "VERIFIED"
            or candidate["manifest_sha256"] != body["manifest_sha256"]
            or body["current_checkpoint_id"] != body["checkpoint_id"]
        ):
            raise HTTPException(status_code=409, detail={"code": "blogger_checkpoint_not_verified"})
        stored = control_ledger.record_blogger_checkpoint_receipt(
            request_id=str(body["request_id"]), run_id=run_id, attempt_id=attempt_id, receipt=body
        )
        control_ledger.record_connector_coverage(
            connector_kind="region-talk-ydb-bloggers-v1",
            contract_version="my-data-hub-blogger-closure.v1",
            state="COMPLETE",
            observed_at=control_ledger.clock.now(),
        )
        return {"accepted": True, "state": stored["state"]}

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
    async def runtime_event(request: Request, authorization: str | None = Header(default=None)) -> dict[str, Any]:
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
            receipt = coordinator.accept_runtime_event(raw, header_token=authorization.removeprefix("Bearer ").strip())
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
            _session_credential(item, identity, now=now, latest_expiry=latest_expiry) for item in body["credentials"]
        ]
        references: list[str] = []
        for credential in credentials:
            reference = registrar.store(credential)
            references.append(Path(reference).name)
        return {"registered": len(references), "credential_refs": references}

    @app.post("/internal/runtime/connector-coverage/{run_id}/{attempt_id}")
    async def runtime_connector_coverage(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        if master_runtime is None:
            raise HTTPException(status_code=503, detail={"code": "runtime_unavailable"})
        raw = await request.body()
        if len(raw) > 8 * 1024:
            raise HTTPException(status_code=413, detail={"code": "connector_heartbeat_too_large"})
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=400, detail={"code": "connector_heartbeat_invalid"}) from exc
        if not isinstance(body, dict) or set(body) != {"connector_kind", "contract_version", "state", "observed_at"}:
            raise HTTPException(status_code=422, detail={"code": "connector_heartbeat_invalid"})
        _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        connector_kind = str(body["connector_kind"])
        contract_version = str(body["contract_version"])
        state = str(body["state"])
        try:
            observed_at = datetime.fromisoformat(str(body["observed_at"]).replace("Z", "+00:00")).astimezone(UTC)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail={"code": "connector_heartbeat_time_invalid"}) from exc
        now = datetime.now(UTC)
        if (
            not 3 <= len(connector_kind) <= 100
            or not 1 <= len(contract_version) <= 100
            or state not in {"PENDING", "COMPLETE", "FAILED"}
            or observed_at < now - timedelta(days=1)
            or observed_at > now + timedelta(minutes=1)
        ):
            raise HTTPException(status_code=422, detail={"code": "connector_heartbeat_invalid"})
        token = authorization.removeprefix("Bearer ").strip() if authorization else ""
        master_runtime.record_connector_heartbeat(
            run_id=run_id,
            attempt_id=attempt_id,
            runtime_token=token,
            connector_kind=connector_kind,
            contract_version=contract_version,
            state=state,
            observed_at=observed_at,
        )
        return {
            "accepted": True,
            "connector_kind": connector_kind,
            "state": state,
            "observed_at": observed_at.isoformat().replace("+00:00", "Z"),
        }

    @app.post("/internal/provider-journal/intents")
    async def provider_journal_intent(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _bounded_json(request)
        if set(body) != {"intent"}:
            raise HTTPException(status_code=422, detail={"code": "intent_envelope_invalid"})
        try:
            intent = ProviderEffectIntent.model_validate(body["intent"])
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "intent_invalid"}) from exc
        if str(intent.operation_id) != operation.operation_id:
            raise HTTPException(status_code=403, detail={"code": "provider_authority_mismatch"})
        assets = _configured_master_assets()
        current_run = str(operation.identity["run_id"])
        authorized = False
        if assets is not None and intent.action is MutationAction.CREATE_DATASET:
            authorized = intent.provider_ref == assets.checkpoint_ref and str(intent.task_id) == current_run
        elif assets is not None and intent.action is MutationAction.PUSH_NOTEBOOK:
            authorized = intent.provider_ref == assets.checkpoint_verifier_ref and str(intent.task_id) == current_run
        elif assets is not None and intent.action is MutationAction.VERSION_DATASET:
            prior = _current_checkpoint_claim(intent.provider_ref)
            authorized = bool(
                intent.provider_ref == assets.checkpoint_ref
                and prior is not None
                and intent.task_id == prior.task_id
                and intent.expected_fingerprint == prior.fingerprint
            )
        if not authorized:
            raise HTTPException(status_code=403, detail={"code": "provider_effect_not_allowed"})
        provider_journal.persist_intent(intent)
        return {"persisted": True}

    @app.post("/internal/provider-journal/receipts")
    async def provider_journal_receipt(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _bounded_json(request)
        if set(body) != {"receipt"}:
            raise HTTPException(status_code=422, detail={"code": "receipt_envelope_invalid"})
        try:
            receipt = ProviderEffectReceipt.model_validate(body["receipt"])
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "receipt_invalid"}) from exc
        authority = control_ledger.provider_effect_authority(str(receipt.effect_id))
        if authority is None or (
            authority["operation_id"] != operation.operation_id
            or str(receipt.operation_id) != operation.operation_id
            or authority["provider_ref"] != receipt.provider_ref
            or authority["action"] != receipt.action.value
        ):
            raise HTTPException(status_code=403, detail={"code": "provider_authority_mismatch"})
        provider_journal.persist_receipt(receipt)
        return {"persisted": True}

    @app.post("/internal/provider-journal/resource-claims")
    async def provider_journal_claim(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _bounded_json(request)
        if set(body) != {"claim"}:
            raise HTTPException(status_code=422, detail={"code": "claim_envelope_invalid"})
        try:
            claim = TaskResourceClaim.model_validate(body["claim"])
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "claim_invalid"}) from exc
        authority = control_ledger.provider_effect_authority(str(claim.effect_id))
        assets = _configured_master_assets()
        prior = _current_checkpoint_claim(claim.provider_ref)
        try:
            control_ledger.assert_provider_resource_claim(
                claim.claim_sha256,
                claim.model_dump(mode="json"),
            )
        except PermissionError:
            exact_replay = False
        else:
            exact_replay = True
        current_run = str(operation.identity["run_id"])
        task_authorized = str(claim.task_id) == current_run
        if authority and authority["action"] == MutationAction.VERSION_DATASET.value:
            task_authorized = bool(
                exact_replay
                or (
                    prior is not None
                    and claim.task_id == prior.task_id
                    and claim.provider_version == prior.provider_version + 1
                )
            )
        expected_kind = (
            ProviderKind.DATASET.value
            if authority
            and authority["action"]
            in {
                MutationAction.CREATE_DATASET.value,
                MutationAction.VERSION_DATASET.value,
            }
            else ProviderKind.NOTEBOOK.value
        )
        if (
            authority is None
            or authority["operation_id"] != operation.operation_id
            or authority["task_id"] != str(claim.task_id)
            or authority["provider_ref"] != claim.provider_ref
            or not task_authorized
            or claim.control_class != ControlClass.ORCHESTRATOR_PROTECTED
            or claim.disposable
            or claim.kind.value != expected_kind
            or assets is None
            or claim.provider_ref not in {assets.checkpoint_ref, assets.checkpoint_verifier_ref}
        ):
            raise HTTPException(status_code=403, detail={"code": "provider_authority_mismatch"})
        provider_journal.persist_resource_claim(claim)
        return {"persisted": True}

    @app.post("/internal/provider-journal/resource-claims/assert")
    async def provider_journal_assert_claim(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _bounded_json(request)
        try:
            claim = TaskResourceClaim.model_validate(body.get("claim"))
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "claim_invalid"}) from exc
        authority = control_ledger.provider_effect_authority(str(claim.effect_id))
        assets = _configured_master_assets()
        current_checkpoint_claim = _current_checkpoint_claim(claim.provider_ref)
        is_current_checkpoint_authority = bool(
            assets is not None and claim.provider_ref == assets.checkpoint_ref and current_checkpoint_claim == claim
        )
        is_current_operation_authority = bool(
            authority is not None
            and authority["operation_id"] == operation.operation_id
            and str(claim.task_id) == str(operation.identity["run_id"])
        )
        if not (is_current_checkpoint_authority or is_current_operation_authority):
            raise HTTPException(status_code=403, detail={"code": "provider_authority_mismatch"})
        try:
            provider_journal.assert_resource_claim(claim)
        except PermissionError:
            return {"authorized": False}
        return {"authorized": True}

    @app.post("/internal/provider-journal/resource-claims/current")
    async def provider_journal_current_claim(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"REGISTERING", "ACTIVE", "DRAINING", "CHECKPOINTING"}),
        )
        body = await _bounded_json(request)
        assets = _configured_master_assets()
        expected_ref = assets.checkpoint_ref if assets else None
        if body != {
            "provider_ref": expected_ref,
            "kind": ProviderKind.DATASET.value,
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
        }:
            raise HTTPException(status_code=403, detail={"code": "provider_claim_lookup_forbidden"})
        claim = control_ledger.latest_provider_resource_claim(
            provider_ref=str(expected_ref),
            resource_kind=ProviderKind.DATASET.value,
            control_class=ControlClass.ORCHESTRATOR_PROTECTED.value,
        )
        return {"claim": claim}

    @app.get("/internal/checkpoints/{service_kind}/head")
    def checkpoint_head(
        service_kind: str,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"REGISTERING", "ACTIVE", "DRAINING", "CHECKPOINTING"}),
        )
        return _checkpoint_head(service_kind)

    @app.post("/internal/checkpoints/candidates")
    async def checkpoint_candidate(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _bounded_json(request)
        if set(body) != {"operation_id", "dataset_ref", "service_kind", "manifest"}:
            raise HTTPException(status_code=422, detail={"code": "checkpoint_envelope_invalid"})
        try:
            manifest = CheckpointManifest.from_payload(body["manifest"])
        except (ManifestError, TypeError) as exc:
            raise HTTPException(status_code=422, detail={"code": "checkpoint_manifest_invalid"}) from exc
        assets = _configured_master_assets()
        expected_dataset = assets.checkpoint_ref if assets else None
        if (
            body["operation_id"] != operation.operation_id
            or body["service_kind"] != "postgres-master"
            or not expected_dataset
            or body["dataset_ref"] != expected_dataset
            or str(manifest.master_instance_id) != str(operation.identity["master_instance_id"])
            or manifest.epoch != int(operation.identity["epoch"])
            or manifest.source_run_id != str(operation.identity["run_id"])
        ):
            raise HTTPException(status_code=403, detail={"code": "checkpoint_authority_mismatch"})
        registry = ControlLedgerCheckpointRegistry(
            control_ledger,
            operation_id=operation.operation_id,
            dataset_ref=body["dataset_ref"],
            service_kind=body["service_kind"],
        )
        try:
            registry.add_candidate(manifest)
        except (ValueError, ControlLedgerError) as exc:
            raise HTTPException(status_code=409, detail={"code": "checkpoint_candidate_rejected"}) from exc
        return {"persisted": True}

    async def _checkpoint_transition_body(request: Request) -> dict[str, Any]:
        body = await _bounded_json(request)
        if body.get("service_kind") != "postgres-master":
            raise HTTPException(status_code=422, detail={"code": "checkpoint_service_invalid"})
        return body

    def _checkpoint_registry_for(operation_id: str, checkpoint_id: str) -> ControlLedgerCheckpointRegistry:
        record = control_ledger.checkpoint_candidate(checkpoint_id)
        if record is None:
            raise HTTPException(status_code=404, detail={"code": "checkpoint_not_found"})
        _assert_checkpoint_owner(checkpoint_id, operation_id, str(record["service_kind"]))
        return ControlLedgerCheckpointRegistry(
            control_ledger,
            operation_id=operation_id,
            dataset_ref=str(record["dataset_ref"]),
            service_kind=str(record["service_kind"]),
        )

    @app.post("/internal/checkpoints/{checkpoint_id}/uploaded")
    async def checkpoint_uploaded(
        checkpoint_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _checkpoint_transition_body(request)
        if set(body) != {"service_kind", "exact_version_ref"}:
            raise HTTPException(status_code=422, detail={"code": "checkpoint_transition_invalid"})
        _checkpoint_registry_for(operation.operation_id, checkpoint_id).uploaded(
            UUID(checkpoint_id), str(body["exact_version_ref"])
        )
        return {"persisted": True}

    @app.post("/internal/checkpoints/{checkpoint_id}/package-identity")
    async def checkpoint_package_identity(
        checkpoint_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, str]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _checkpoint_transition_body(request)
        package_sha256 = str(body.get("package_sha256", ""))
        if (
            set(body) != {"service_kind", "package_sha256"}
            or len(package_sha256) != 64
            or any(char not in "0123456789abcdef" for char in package_sha256)
        ):
            raise HTTPException(status_code=422, detail={"code": "checkpoint_package_identity_invalid"})
        _assert_checkpoint_owner(checkpoint_id, operation.operation_id, "postgres-master")
        control_ledger.record_checkpoint_package_sha256(checkpoint_id, package_sha256)
        return {"checkpoint_id": checkpoint_id, "package_sha256": package_sha256}

    async def _simple_checkpoint_transition(
        checkpoint_id: str, request: Request, operation_id: str, action: str
    ) -> dict[str, bool]:
        body = await _checkpoint_transition_body(request)
        registry = _checkpoint_registry_for(operation_id, checkpoint_id)
        try:
            if action == "readback":
                registry.readback_verified(UUID(checkpoint_id))
            elif action == "restore":
                registry.restore_verified(UUID(checkpoint_id))
            elif action == "reject":
                if set(body) != {"service_kind", "reason"}:
                    raise ValueError("rejection reason missing")
                registry.reject(UUID(checkpoint_id), str(body["reason"]))
            else:
                raise AssertionError(action)
        except (ValueError, ControlLedgerError) as exc:
            raise HTTPException(status_code=409, detail={"code": "checkpoint_transition_rejected"}) from exc
        return {"persisted": True}

    @app.post("/internal/checkpoints/{checkpoint_id}/readback-verified")
    async def checkpoint_readback_verified(
        checkpoint_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        return await _simple_checkpoint_transition(checkpoint_id, request, operation.operation_id, "readback")

    @app.post("/internal/checkpoints/{checkpoint_id}/restore-verified")
    async def checkpoint_restore_verified(
        checkpoint_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        return await _simple_checkpoint_transition(checkpoint_id, request, operation.operation_id, "restore")

    @app.post("/internal/checkpoints/{checkpoint_id}/reject")
    async def checkpoint_reject(
        checkpoint_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, bool]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        return await _simple_checkpoint_transition(checkpoint_id, request, operation.operation_id, "reject")

    @app.post("/internal/checkpoints/{checkpoint_id}/promote")
    async def checkpoint_promote(
        checkpoint_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
        )
        body = await _checkpoint_transition_body(request)
        if set(body) != {"service_kind", "expected_generation"}:
            raise HTTPException(status_code=422, detail={"code": "checkpoint_promotion_invalid"})
        registry = _checkpoint_registry_for(operation.operation_id, checkpoint_id)
        try:
            registry.promote(UUID(checkpoint_id), expected_generation=int(body["expected_generation"]))
        except (ValueError, ControlLedgerError) as exc:
            raise HTTPException(status_code=409, detail={"code": "checkpoint_promotion_rejected"}) from exc
        return _checkpoint_head(str(body["service_kind"]))

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
