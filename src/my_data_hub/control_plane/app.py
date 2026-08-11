from __future__ import annotations

import asyncio
import base64
import hashlib
import hmac
import json
import os
import tempfile
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
from uuid import UUID, uuid4

from fastapi import FastAPI, Header, HTTPException, Request, status

from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceReceipt
from my_data_hub.checkpoints import CheckpointManifest, ControlLedgerCheckpointRegistry
from my_data_hub.checkpoints.manifest import ManifestError
from my_data_hub.control_plane.adapters import (
    KaggleMCPProviderGateway,
    LedgerControlReader,
    LedgerMasterResolver,
)
from my_data_hub.control_plane.ledger import (
    ControlLedger,
    ControlLedgerError,
    EventRejected,
    IdempotencyConflict,
    MasterAdmissionRejected,
    StaleRuntimeEvent,
)
from my_data_hub.control_plane.runtime import (
    ControlPlaneMasterRuntime,
    MasterRuntimeSettings,
    SessionCredential,
    SessionCredentialRegistrar,
    TunnelCertificate,
    TunnelCertificateBroker,
    build_production_runtime,
)
from my_data_hub.embeddings.master_stage import execute_embedding_production_stage
from my_data_hub.embeddings.production import (
    WORKER_ASSETS,
    EmbeddingProductionAdmissionBinding,
    EmbeddingProductionCapabilities,
    EmbeddingProductionRequest,
    EmbeddingProductionStageReceipt,
    embedding_provider_authority,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.catalog import TOOL_CONTRACTS
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.providers.kaggle import (
    ControlLedgerKaggleJournal,
    KaggleMasterRuntimeProvider,
    KaggleProviderAdapter,
)
from my_data_hub.providers.kaggle.contracts import (
    MutationAction,
    ProviderEffectIntent,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.models import ControlClass, ProviderKind
from my_data_hub.runtime_sdk import RuntimeEvent, RuntimeEventType
from my_data_hub.tunnel_broker_ipc import TunnelBrokerClient
from my_data_hub.workloads.bloggers.importer import batch_identity
from my_data_hub.workloads.bloggers.master_stage import (
    BLOGGER_REPLAY_STAGE_SCHEMA,
    MAX_REQUEST_BYTES,
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
    BloggerQuarantineReceipt,
    resolution_matches_quarantine,
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


def _validate_blogger_replay_source(
    migration: BloggerMigrationRequest,
    source: dict[str, Any] | None,
    *,
    now: datetime,
) -> str | None:
    """Return a stable failure code, without ever inspecting source row bytes."""

    envelope = migration.duplicate_resolution
    if envelope is None or migration.replay_of_request_id is None:
        return "blogger_replay_source_invalid"
    if (
        source is None
        or source["state"] != "FAILED"
        or source["failure_code"] != "BloggerMigrationQuarantined"
        or source["operation_id"] == str(migration.operation_id)
        or source["operation_id"] != str(envelope.source_operation_id)
        or source["request_id"] != str(envelope.source_request_id)
        or source["request_sha256"] != envelope.source_request_sha256
    ):
        return "blogger_replay_source_invalid"
    try:
        source_request = BloggerMigrationRequest.model_validate(source["request"])
        receipt = BloggerQuarantineReceipt.model_validate(source["quarantine_receipt"])
        quarantined_at = datetime.fromisoformat(source["updated_at"].replace("Z", "+00:00")).astimezone(UTC)
    except Exception:
        return "blogger_replay_source_invalid"
    if (
        source_request.project_id != migration.project_id
        or source_request.snapshot_at.astimezone(UTC) != migration.snapshot_at.astimezone(UTC)
        or source_request.expected_rows != migration.expected_rows
        or source_request.source_revision != migration.source_revision
        or source_request.source_query_sha256 != migration.source_query_sha256
        or envelope.authorized_at.astimezone(UTC) < quarantined_at
        or envelope.authorized_at.astimezone(UTC) > now.astimezone(UTC) + timedelta(minutes=5)
        or source.get("quarantine_receipt_sha256") != receipt.receipt_sha256
        or not resolution_matches_quarantine(envelope, receipt)
    ):
        return "blogger_replay_binding_invalid"
    return None


def assert_no_database_environment() -> None:
    """Reject static/libpq credentials in lightweight control or MCP processes."""

    candidates = set(DATABASE_ENVIRONMENT_NAMES)
    candidates.update(name for name in os.environ if name.endswith("_DATABASE_URL"))
    candidates.update(name for name in os.environ if name.startswith("PG"))
    leaked = sorted(name for name in candidates if os.getenv(name, "").strip())
    if leaked:
        raise ControlPlaneConfigurationError(
            "lightweight control plane must not receive master database credentials: "
            + ", ".join(leaked)
        )


def _session_credential(
    item: object,
    identity: dict[str, Any],
    *,
    now: datetime,
    latest_expiry: datetime,
) -> SessionCredential:
    if not isinstance(item, dict) or set(item) != {"role", "database_url", "expires_at"}:
        raise HTTPException(status_code=422, detail={"code": "invalid_credential"})
    role = str(item.get("role", ""))
    if role not in {"reader", "operator"}:
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
        role=role,
        database_url=database_url,
        expires_at=expires_at,
    )


def _disabled(name: str) -> bool:
    value = os.getenv(name, "false").strip().lower()
    if value not in {"0", "false", "no", "off"}:
        raise ControlPlaneConfigurationError(f"{name} must remain false in PR-A")
    return False


def _boolean(name: str, default: bool = False) -> bool:
    value = os.getenv(name, "true" if default else "false").strip().lower()
    if value in {"1", "true", "yes", "on"}:
        return True
    if value in {"0", "false", "no", "off"}:
        return False
    raise ControlPlaneConfigurationError(f"{name} must be a boolean")


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
    operator_credentials_enabled: bool = False
    provider_gateway_enabled: bool = False
    acceptance_scenarios_enabled: bool = False

    def __post_init__(self) -> None:
        if not self.host or not 1 <= self.port <= 65535:
            raise ControlPlaneConfigurationError("control-plane listener is invalid")
        if self.scheduler_enabled or self.production_publish_enabled or self.remote_mcp_writes_enabled:
            raise ControlPlaneConfigurationError("PR-A control-plane write and publication gates must remain false")
        if self.provider_gateway_enabled and not self.operator_credentials_enabled:
            raise ControlPlaneConfigurationError("provider gateway requires the explicit operator credential gate")
        if self.acceptance_scenarios_enabled and not self.provider_gateway_enabled:
            raise ControlPlaneConfigurationError(
                "acceptance scenarios require the authenticated single control gateway"
            )

    @classmethod
    def from_env(cls) -> ControlPlaneSettings:
        assert_no_database_environment()
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
            operator_credentials_enabled=_boolean("MY_DATA_HUB_MCP_OPERATOR_CREDENTIALS_ENABLED"),
            provider_gateway_enabled=_boolean("MY_DATA_HUB_MCP_PROVIDER_GATEWAY_ENABLED"),
            acceptance_scenarios_enabled=_boolean(
                "MY_DATA_HUB_MCP_ACCEPTANCE_SCENARIOS_ENABLED"
            ),
        )


def create_app(
    settings: ControlPlaneSettings | None = None,
    *,
    ledger: ControlLedger | None = None,
    master_runtime: ControlPlaneMasterRuntime | None = None,
    session_registrar: SessionCredentialRegistrar | None = None,
    embedding_stage_runner: object | None = None,
    operator_credential_enabled: bool | None = None,
    tunnel_certificate_broker: TunnelCertificateBroker | None = None,
    provider_gateway: KaggleMCPProviderGateway | None = None,
    provider_gateway_token: bytes | None = None,
    acceptance_scenario_adapter: object | None = None,
    checkpoint_acceptance_launcher: object | None = None,
    checkpoint_acceptance_catalog: object | None = None,
    old_epoch_denials: object | None = None,
) -> FastAPI:
    runtime = settings or ControlPlaneSettings.from_env()
    if operator_credential_enabled is None:
        operator_credential_enabled = runtime.operator_credentials_enabled
    if tunnel_certificate_broker is None:
        socket_value = os.getenv("MY_DATA_HUB_TUNNEL_BROKER_SOCKET", "").strip()
        if socket_value:
            tunnel_certificate_broker = TunnelBrokerClient(Path(socket_value))
    try:
        tunnel_listen_port = int(os.getenv("MY_DATA_HUB_TUNNEL_LISTEN_PORT", "25432"))
    except ValueError as exc:
        raise ControlPlaneConfigurationError("tunnel listen port must be an integer") from exc
    if not 1024 <= tunnel_listen_port <= 65535:
        raise ControlPlaneConfigurationError("tunnel listen port is outside 1024..65535")
    ledger_path = runtime.ledger_path or Path(tempfile.mkdtemp(prefix="mdh-control-")) / "control.sqlite3"
    control_ledger = ledger or ControlLedger(ledger_path)
    provider_adapter: KaggleProviderAdapter | None = None
    if master_runtime is None:
        production = build_production_runtime(
            control_ledger,
            runtime.master_runtime,
            session_credentials_path=runtime.session_credentials_path,
            tunnel_authority=(
                tunnel_certificate_broker
                if isinstance(tunnel_certificate_broker, TunnelBrokerClient)
                else None
            ),
            tunnel_listen_port=tunnel_listen_port,
        )
        master_runtime = production.master
        provider_status = production.provider_status
        provider_adapter = production.provider_adapter
        session_registrar = session_registrar or production.session_registrar
    else:
        if master_runtime.ledger.path != control_ledger.path:
            raise ControlPlaneConfigurationError("master runtime and app must share one control ledger")
        provider_status = "available"
    if provider_gateway is None and provider_adapter is not None:
        provider_gateway = KaggleMCPProviderGateway(control_ledger, provider_adapter)
    if runtime.provider_gateway_enabled:
        if not operator_credential_enabled or provider_gateway is None:
            raise ControlPlaneConfigurationError(
                "provider gateway requires the single authenticated control adapter"
            )
        if provider_gateway_token is None:
            token_path = Path(
                os.getenv("MY_DATA_HUB_MCP_CONTROL_GATEWAY_TOKEN_FILE", "")
            ).expanduser()
            if not token_path.is_absolute() or token_path.is_symlink() or not token_path.is_file():
                raise ControlPlaneConfigurationError("provider gateway token must be an absolute regular file")
            if token_path.stat().st_mode & 0o077:
                raise ControlPlaneConfigurationError("provider gateway token file must be private")
            provider_gateway_token = token_path.read_bytes().strip()
        if not 32 <= len(provider_gateway_token) <= 256 or any(
            byte < 33 or byte > 126 for byte in provider_gateway_token
        ):
            raise ControlPlaneConfigurationError("provider gateway token violates the bounded contract")
    if runtime.acceptance_scenarios_enabled and acceptance_scenario_adapter is None:
        if checkpoint_acceptance_launcher is None or checkpoint_acceptance_catalog is None:
            from my_data_hub.acceptance.checkpoint_launcher import (
                ControlCheckpointAcceptanceLauncher,
                checkpoint_acceptance_deployment_from_environment,
            )

            deployment = checkpoint_acceptance_deployment_from_environment()
            if deployment is None or provider_adapter is None or master_runtime is None:
                raise ControlPlaneConfigurationError(
                    "acceptance scenario opt-in requires the exact checkpoint deployment "
                    "and single authenticated control adapter"
                )
            checkpoint_acceptance_launcher = ControlCheckpointAcceptanceLauncher(
                ledger=control_ledger,
                adapter=provider_adapter,
                deployment=deployment,
            )
            checkpoint_acceptance_catalog = deployment.catalog
        if (
            master_runtime is None
            or checkpoint_acceptance_launcher is None
            or checkpoint_acceptance_catalog is None
        ):
            raise ControlPlaneConfigurationError(
                "acceptance scenario opt-in requires concrete master and checkpoint executors"
            )
        from my_data_hub.acceptance.master_production import ProductionControlAcceptanceContext
        from my_data_hub.acceptance.scenario_operator import (
            AcceptanceScenarioOperatorAdapter,
            UnifiedAcceptanceScenarioExecutor,
        )
        from my_data_hub.control_plane.acceptance_supervisor import (
            callback_loss_supervisor_from_environment,
        )

        callback_supervisor = callback_loss_supervisor_from_environment(
            control_ledger, master_recovery=master_runtime
        )
        if old_epoch_denials is None:
            from my_data_hub.acceptance.old_epoch_production import (
                TaskBoundOldEpochDenialFactory,
            )
            from my_data_hub.mcp.postgres_broker import DirectoryEpochCredentialSource

            tunnel_authority = getattr(master_runtime.coordinator, "tunnel_authority", None)
            if tunnel_authority is None or not all(
                callable(getattr(tunnel_authority, name, None))
                for name in ("acceptance_identity_snapshot", "acceptance_retired_denial")
            ):
                raise ControlPlaneConfigurationError(
                    "acceptance scenario opt-in requires the structured FM11 tunnel authority"
                )
            old_epoch_denials = TaskBoundOldEpochDenialFactory(
                ledger=control_ledger,
                source=DirectoryEpochCredentialSource(runtime.session_credentials_path),
                tunnel=tunnel_authority,
            )
        acceptance_scenario_adapter = AcceptanceScenarioOperatorAdapter(
            UnifiedAcceptanceScenarioExecutor(
                master=ProductionControlAcceptanceContext(
                    callback_supervisor=callback_supervisor,
                    old_epoch_denials=old_epoch_denials,  # type: ignore[arg-type]
                    session_directory=runtime.session_credentials_path,
                ).build(master_runtime),
                checkpoint=checkpoint_acceptance_launcher,  # type: ignore[arg-type]
                checkpoint_catalog=checkpoint_acceptance_catalog,  # type: ignore[arg-type]
            )
        )
    resolver = LedgerMasterResolver(control_ledger)
    provider_journal = ControlLedgerKaggleJournal(control_ledger)
    provider_control = (
        LedgerControlReader(
            control_ledger,
            provider_gateway=provider_gateway,
            acceptance_scenarios=acceptance_scenario_adapter,
        )
        if runtime.provider_gateway_enabled and provider_gateway is not None
        else None
    )

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
                        reconcile_status_cleanup = getattr(
                            master_runtime, "reconcile_status_cleanup_once", None
                        )
                        if reconcile_status_cleanup is not None:
                            await asyncio.to_thread(reconcile_status_cleanup)
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
    app.state.tunnel_certificate_broker = tunnel_certificate_broker
    app.state.embedding_stage_runner = embedding_stage_runner or execute_embedding_production_stage
    app.state.provider_gateway = provider_gateway if runtime.provider_gateway_enabled else None
    app.state.acceptance_scenario_adapter = acceptance_scenario_adapter
    # Process invocation identity, not the host/kernel boot ID. A real control
    # restart necessarily constructs a new application and therefore a new UUID.
    app.state.control_boot_id = uuid4()
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

    def _embedding_provider_authority(operation_id: str) -> dict[str, tuple[str, UUID]]:
        record = control_ledger.embedding_production_request_for_operation(operation_id)
        assets = _configured_master_assets()
        if record is None or record["state"] != "CLAIMED" or assets is None:
            return {}
        owner = assets.checkpoint_ref.split("/", 1)[0]
        return embedding_provider_authority(owner, UUID(record["request_id"]))

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

    @dataclass(frozen=True, slots=True)
    class _ProviderOperationAuthority:
        operation_id: str
        identity: dict[str, Any]
        acceptance: dict[str, Any] | None = None

    def _provider_authority(
        *,
        request: Request,
        authorization: str | None,
        run_id: str | None,
        attempt_id: str | None,
        master_instance_id: str | None,
        epoch: str | None,
        allowed_states: frozenset[str] = frozenset({"ACTIVE", "DRAINING", "CHECKPOINTING"}),
        require_acceptance_source_attestation: bool = True,
    ) -> _ProviderOperationAuthority:
        acceptance_request = request.headers.get("X-MDH-Acceptance-Request-ID")
        acceptance_task = request.headers.get("X-MDH-Acceptance-Task-Run-ID")
        acceptance_attempt = request.headers.get("X-MDH-Acceptance-Attempt-ID")
        acceptance_headers = (acceptance_request, acceptance_task, acceptance_attempt)
        if any(value is not None for value in acceptance_headers):
            if not all(value is not None for value in acceptance_headers):
                raise HTTPException(status_code=401, detail={"code": "acceptance_identity_incomplete"})
            if not authorization or not authorization.startswith("Bearer "):
                raise HTTPException(status_code=401, detail={"code": "acceptance_token_required"})
            try:
                exact_request = str(UUID(str(acceptance_request)))
                exact_task = str(UUID(str(acceptance_task)))
                exact_attempt = str(UUID(str(acceptance_attempt)))
            except ValueError as exc:
                raise HTTPException(status_code=401, detail={"code": "acceptance_identity_invalid"}) from exc
            if exact_request != exact_task:
                raise HTTPException(status_code=401, detail={"code": "acceptance_identity_invalid"})
            launch = control_ledger.authenticate_checkpoint_acceptance(
                request_id=exact_request,
                attempt_id=exact_attempt,
                token=authorization.removeprefix("Bearer ").strip(),
            )
            if launch is None or launch["task_run_id"] != exact_task:
                raise HTTPException(status_code=401, detail={"code": "acceptance_token_invalid"})
            if (
                require_acceptance_source_attestation
                and launch["source_attestation_state"] != "MATCHED"
            ):
                raise HTTPException(
                    status_code=409, detail={"code": "acceptance_source_attestation_required"}
                )
            return _ProviderOperationAuthority(
                operation_id=str(launch["operation_id"]),
                identity={
                    "run_id": exact_task,
                    "attempt_id": exact_attempt,
                    "acceptance_request_id": exact_request,
                },
                acceptance=launch,
            )
        operation = _runtime_authority(
            authorization=authorization, run_id=run_id, attempt_id=attempt_id,
            master_instance_id=master_instance_id, epoch=epoch, allowed_states=allowed_states,
        )
        return _ProviderOperationAuthority(
            operation_id=operation.operation_id, identity=dict(operation.identity), acceptance=None
        )

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

    @app.post("/internal/acceptance/events")
    async def checkpoint_acceptance_event(
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        authority = _provider_authority(
            request=request,
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            require_acceptance_source_attestation=False,
        )
        if authority.acceptance is None:
            raise HTTPException(status_code=403, detail={"code": "acceptance_identity_required"})
        body = await _bounded_json(request)
        try:
            event = RuntimeEvent.model_validate(body)
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "acceptance_event_invalid"}) from exc
        launch = authority.acceptance
        exact_request = str(launch["request_id"])
        exact_attempt = str(launch["attempt_id"])
        if (
            event.run_id != exact_request
            or event.attempt_id != exact_attempt
            or event.service_instance_id != exact_request
            or event.source_identity != str(launch["request"]["evidence_notebook_ref"])
            or event.source_version != str(launch["request"]["source_revision"])
            or event.epoch != 1
        ):
            raise HTTPException(status_code=403, detail={"code": "acceptance_event_binding_mismatch"})
        if event.event_type is RuntimeEventType.RUNTIME_STARTED:
            observed_source = event.data.get("progress", {}).get("runtime_source_sha256")
            if not isinstance(observed_source, str):
                raise HTTPException(
                    status_code=422, detail={"code": "acceptance_source_attestation_missing"}
                )
            try:
                launch = control_ledger.attest_checkpoint_acceptance_source(
                    request_id=exact_request,
                    attempt_id=exact_attempt,
                    observed_source_sha256=observed_source,
                )
            except (IdempotencyConflict, StaleRuntimeEvent, ValueError) as exc:
                raise HTTPException(
                    status_code=409, detail={"code": "acceptance_source_attestation_rejected"}
                ) from exc
        try:
            receipt = control_ledger.record_checkpoint_acceptance_event(
                request_id=exact_request,
                attempt_id=exact_attempt,
                event=event.model_dump(mode="json", exclude_none=True),
            )
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail={"code": "acceptance_event_uid_conflict"}) from exc
        except (StaleRuntimeEvent, ValueError) as exc:
            raise HTTPException(status_code=409, detail={"code": "acceptance_event_rejected"}) from exc
        return {"accepted": True, **receipt}

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
        return {
            "ok": True,
            "component": "my-data-hub-control-plane",
            "control_boot_id": str(app.state.control_boot_id),
        }

    @app.get("/health/ready")
    def ready() -> dict[str, Any]:
        return {"ok": True, "control_boot_id": str(app.state.control_boot_id), **snapshot()}

    if runtime.provider_gateway_enabled:
        provider_tools = frozenset(
            {
                "provider.resources.create",
                "provider.resources.version",
                "provider.resources.run",
                "provider.resources.read",
                "provider.resources.delete",
                "provider.acceptance.dataset.lifecycle",
                "provider.acceptance.notebook.lifecycle",
                "provider.acceptance.claim.get",
                "provider.acceptance.claim.cleanup",
                *(
                    {"acceptance.scenario.request", "acceptance.scenario.status"}
                    if runtime.acceptance_scenarios_enabled
                    else set()
                ),
            }
        )

        @app.post("/internal/mcp-provider/invoke")
        async def invoke_mcp_provider(
            request: Request,
            authorization: str | None = Header(default=None),
        ) -> dict[str, Any]:
            supplied = (
                authorization.removeprefix("Bearer ").strip()
                if authorization and authorization.startswith("Bearer ")
                else ""
            )
            expected = provider_gateway_token.decode("ascii") if provider_gateway_token else ""
            if not supplied or not hmac.compare_digest(supplied, expected):
                raise HTTPException(status_code=401, detail={"code": "provider_gateway_token_invalid"})
            raw = await request.body()
            if len(raw) > 512 * 1024:
                raise HTTPException(status_code=413, detail={"code": "provider_gateway_request_too_large"})
            try:
                body = json.loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise HTTPException(status_code=400, detail={"code": "provider_gateway_json_invalid"}) from exc
            if not isinstance(body, dict) or set(body) != {"tool", "arguments", "principal"}:
                raise HTTPException(status_code=422, detail={"code": "provider_gateway_envelope_invalid"})
            tool = body["tool"]
            arguments = body["arguments"]
            principal = body["principal"]
            principal_keys = {
                "subject",
                "client_id",
                "scopes",
                "audience",
                "expires_at",
                "issuer",
                "issued_at",
                "resource",
            }
            principal_string_keys = {
                "subject", "client_id", "audience", "issuer", "resource"
            }
            if (
                not isinstance(tool, str)
                or tool not in provider_tools
                or not isinstance(arguments, dict)
                or not isinstance(principal, dict)
                or set(principal) != principal_keys
                or not isinstance(principal["scopes"], list)
                or len(principal["scopes"]) > 32
                or not all(isinstance(value, str) and 1 <= len(value) <= 300 for value in principal["scopes"])
                or not all(
                    isinstance(principal[key], str) and 1 <= len(principal[key]) <= 1000
                    for key in principal_string_keys
                )
                or not isinstance(principal["expires_at"], int)
                or isinstance(principal["expires_at"], bool)
                or not isinstance(principal["issued_at"], int)
                or isinstance(principal["issued_at"], bool)
            ):
                raise HTTPException(status_code=422, detail={"code": "provider_gateway_contract_invalid"})
            now = int(datetime.now(UTC).timestamp())
            try:
                identity = AccessIdentity(
                    subject=str(principal["subject"]),
                    client_id=str(principal["client_id"]),
                    scopes=frozenset(principal["scopes"]),
                    audience=str(principal["audience"]),
                    token_id="internal-provider-gateway",
                    expires_at=int(principal["expires_at"]),
                    issuer=str(principal["issuer"]),
                    issued_at=int(principal["issued_at"]),
                    resource=str(principal["resource"]),
                )
            except (TypeError, ValueError) as exc:
                raise HTTPException(status_code=422, detail={"code": "provider_gateway_principal_invalid"}) from exc
            contract = TOOL_CONTRACTS[tool]
            if (
                not identity.subject
                or not identity.client_id
                or identity.issued_at > now
                or identity.expires_at <= now
                or contract.scope not in identity.scopes
            ):
                raise HTTPException(status_code=403, detail={"code": "provider_gateway_principal_denied"})

            forbidden = {"authorization", "database_url", "dsn", "password", "private_key", "secret", "token"}

            def reject_secrets(value: object) -> None:
                if isinstance(value, dict):
                    for key, nested in value.items():
                        if str(key).casefold() in forbidden:
                            raise HTTPException(
                                status_code=422, detail={"code": "provider_gateway_secret_forbidden"}
                            )
                        reject_secrets(nested)
                elif isinstance(value, list):
                    for nested in value:
                        reject_secrets(nested)

            reject_secrets(arguments)
            assert provider_control is not None
            try:
                result = await asyncio.to_thread(
                    provider_control.invoke_control, tool, arguments, identity
                )
            except PermissionError as exc:
                raise HTTPException(status_code=403, detail={"code": "provider_gateway_policy_denied"}) from exc
            except ValueError as exc:
                raise HTTPException(status_code=422, detail={"code": "provider_gateway_request_invalid"}) from exc
            except Exception as exc:
                raise HTTPException(status_code=502, detail={"code": "provider_gateway_effect_failed"}) from exc
            encoded = canonical_json_bytes(result)
            if len(encoded) > 2 * 1024 * 1024:
                raise HTTPException(status_code=502, detail={"code": "provider_gateway_response_too_large"})
            return result

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
        if len(canonical_json_bytes(migration.metadata_payload)) > MAX_REQUEST_BYTES:
            raise HTTPException(status_code=413, detail={"code": "blogger_request_too_large"})
        replay_source_id: str | None = None
        replay_receipt_sha256: str | None = None
        if migration.schema_version == BLOGGER_REPLAY_STAGE_SCHEMA:
            source = control_ledger.blogger_migration_request(str(migration.replay_of_request_id))
            replay_error = _validate_blogger_replay_source(
                migration, source, now=control_ledger.clock.now()
            )
            if replay_error is not None:
                raise HTTPException(status_code=409, detail={"code": replay_error})
            assert source is not None
            replay_source_id = source["request_id"]
            replay_receipt_sha256 = source["quarantine_receipt_sha256"]
        try:
            record, created = control_ledger.admit_blogger_migration_request(
                request_id=str(migration.request_id),
                operation_id=str(migration.operation_id),
                request_sha256=migration.request_sha256,
                request=migration.metadata_payload,
                replay_source_request_id=replay_source_id,
                replay_source_receipt_sha256=replay_receipt_sha256,
            )
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail={"code": "blogger_request_conflict"}) from exc
        except MasterAdmissionRejected as exc:
            raise HTTPException(status_code=409, detail={"code": "master_not_active"}) from exc
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
        if record["state"] in {"REQUESTED", "CLAIMED", "IMPORT_COMMITTED"} and master_runtime is not None:
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
        public = {
            key: value
            for key, value in record.items()
            if key not in {"quarantine_receipt", "quarantine_receipt_sha256"}
            and (key != "failure_code" or value is not None)
        }
        if record.get("quarantine_receipt") is not None:
            receipt = BloggerQuarantineReceipt.model_validate(record["quarantine_receipt"])
            public.update(
                {
                    "quarantine_receipt_sha256": receipt.receipt_sha256,
                    "quarantine_evidence": receipt.quarantine_evidence,
                    "duplicate_review": receipt.duplicate_review,
                    "duplicate_review_inputs": receipt.duplicate_review_inputs.model_dump(mode="json"),
                }
            )
        return public

    @app.get("/control/v1/embedding-production/capabilities")
    def embedding_production_capabilities() -> dict[str, Any]:
        binding = _embedding_admission_binding()
        if binding is None:
            raise HTTPException(status_code=503, detail={"code": "embedding_production_unavailable"})
        return EmbeddingProductionCapabilities(
            interface="control_executor",
            admission_ready=True,
            binding=binding,
            execution_location="active_kaggle_master",
            request_acceptance="durable_idempotent_ledger.v1",
            stage_contract="transactional_import_then_checkpoint.v1",
            completion_evidence="terminal_request_status_and_closure_receipt_only",
            runner_implementation="my_data_hub.embeddings.master_stage.execute_embedding_production_stage",
            provider_adapter_package="kaggle",
            provider_adapter_version="2.2.4",
            provider_adapter_implementation="my_data_hub.providers.kaggle.KaggleProviderAdapter",
            single_provider_adapter=True,
            worker_assets=WORKER_ASSETS,
        ).model_dump(mode="json", exclude_none=True)

    def _embedding_admission_binding() -> EmbeddingProductionAdmissionBinding | None:
        service = control_ledger.resolve_service("postgres-master")
        operation = (
            control_ledger.operation_for_attempt(service.run_id, service.attempt_id)
            if service is not None
            else None
        )
        head = control_ledger.checkpoint_head("postgres-master")
        provider = master_runtime.coordinator.provider if master_runtime is not None else None
        adapter = provider.adapter if isinstance(provider, KaggleMasterRuntimeProvider) else None
        if (
            app.state.embedding_stage_runner is not execute_embedding_production_stage
            or app.state.master_provider_status != "available"
            or not isinstance(adapter, KaggleProviderAdapter)
            or service is None
            or service.state != "ACTIVE"
            or service.master_instance_id is None
            or service.canonical_revision is None
            or operation is None
            or operation.state != "ACTIVE"
            or head is None
            or head.current_checkpoint_id is None
        ):
            return None
        try:
            return EmbeddingProductionAdmissionBinding(
                master_instance_id=service.master_instance_id,
                run_id=service.run_id,
                attempt_id=service.attempt_id,
                epoch=service.epoch,
                canonical_revision=service.canonical_revision,
                blogger_checkpoint_id=head.current_checkpoint_id,
            )
        except ValueError:
            return None

    @app.post("/control/v1/embedding-production/requests")
    async def request_embedding_production(request: Request) -> dict[str, Any]:
        body = await _bounded_json(request)
        try:
            embedding = EmbeddingProductionRequest.model_validate(body)
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "embedding_request_invalid"}) from exc
        existing = control_ledger.embedding_production_request(str(embedding.request_id))
        if existing is not None:
            if (
                existing["request_sha256"] != embedding.request_sha256
                or existing["idempotency_key_sha256"] != embedding.idempotency_key_sha256
            ):
                raise HTTPException(status_code=409, detail={"code": "embedding_request_conflict"})
            return {
                "request_id": existing["request_id"],
                "request_sha256": existing["request_sha256"],
                "state": existing["state"],
                "created": False,
            }
        provider = master_runtime.coordinator.provider if master_runtime is not None else None
        if (
            app.state.embedding_stage_runner is not execute_embedding_production_stage
            or app.state.master_provider_status != "available"
            or not isinstance(provider, KaggleMasterRuntimeProvider)
            or not isinstance(provider.adapter, KaggleProviderAdapter)
        ):
            raise HTTPException(status_code=409, detail={"code": "embedding_prerequisite_not_active"})
        try:
            record, created = control_ledger.admit_embedding_production_request(
                request_id=str(embedding.request_id),
                idempotency_key_sha256=embedding.idempotency_key_sha256,
                request_sha256=embedding.request_sha256,
                # Worker model contracts contain the harmless field ``max_tokens``;
                # the metadata ledger deliberately rejects any token-shaped key.
                request=embedding.model_dump(mode="json", exclude={"worker_assets"}),
                canonical_revision=embedding.blogger_canonical_revision,
                checkpoint_id=str(embedding.blogger_checkpoint_id),
            )
        except IdempotencyConflict as exc:
            raise HTTPException(status_code=409, detail={"code": "embedding_request_conflict"}) from exc
        except MasterAdmissionRejected as exc:
            raise HTTPException(
                status_code=409, detail={"code": "embedding_prerequisite_not_active"}
            ) from exc
        return {
            "request_id": record["request_id"], "request_sha256": record["request_sha256"],
            "state": record["state"], "created": created,
        }

    @app.get("/control/v1/embedding-production/requests/{request_id}")
    def embedding_production_status(request_id: str) -> dict[str, Any]:
        try:
            exact_id = str(UUID(request_id))
        except ValueError as exc:
            raise HTTPException(status_code=404, detail={"code": "embedding_request_not_found"}) from exc
        record = control_ledger.embedding_production_request(exact_id)
        if record is None:
            raise HTTPException(status_code=404, detail={"code": "embedding_request_not_found"})
        if record["state"] in {"REQUESTED", "CLAIMED"} and master_runtime is not None:
            source_operation = control_ledger.get_operation(record["operation_id"])
            if source_operation is not None:
                with suppress(Exception):
                    master_runtime.coordinator.reconcile_operation(
                        source_operation.operation_id,
                        master_runtime.intent(source_operation.idempotency_key),
                    )
            record = control_ledger.reconcile_abandoned_embedding_production_request(exact_id)
            assert record is not None
        if record["state"] == "STAGE_COMMITTED":
            recovered = control_ledger.verified_checkpoint_for_operation(record["operation_id"])
            if recovered is not None:
                receipt = record["stage_receipt"]
                record = control_ledger.record_embedding_checkpoint_receipt(
                    request_id=exact_id,
                    run_id=str(record["claimed_run_id"]),
                    attempt_id=str(record["claimed_attempt_id"]),
                    receipt={
                        "request_id": exact_id,
                        "checkpoint_id": recovered["checkpoint_id"],
                        "manifest_sha256": recovered["manifest_sha256"],
                        "exact_version_ref": recovered["version_ref"],
                        "canonical_revision": receipt["canonical_revision"],
                    },
                )
        stage = record["stage_receipt"] or {}
        return {
            "request_id": record["request_id"], "request_sha256": record["request_sha256"],
            "state": record["state"], "claimed_run_id": record["claimed_run_id"],
            "claimed_attempt_id": record["claimed_attempt_id"], "claimed_epoch": record["claimed_epoch"],
            "workers": stage.get("workers"), "imports": stage.get("imports"),
            "coverage": stage.get("coverage"),
            "query_vector_receipts": stage.get("query_vector_receipts"),
            "canonical_revision": stage.get("canonical_revision"),
            "checkpoint_receipt": record["checkpoint_receipt"],
            **({"failure_code": record["failure_code"]} if record["failure_code"] else {}),
        }

    @app.get("/internal/runtime/embedding-production/{run_id}/{attempt_id}")
    def claim_embedding_production(
        run_id: str, attempt_id: str, authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        operation = _runtime_authority(
            authorization=authorization, run_id=run_id, attempt_id=attempt_id,
            master_instance_id=master_instance_id, epoch=epoch, allowed_states=frozenset({"ACTIVE"}),
        )
        record = control_ledger.claim_embedding_production_request(
            operation_id=operation.operation_id, run_id=run_id, attempt_id=attempt_id,
            master_instance_id=str(operation.identity["master_instance_id"]), epoch=int(operation.identity["epoch"]),
        )
        return {"available": False} if record is None or record["state"] != "CLAIMED" else {
            "available": True, "request": record["request"], "request_sha256": record["request_sha256"],
        }

    @app.post("/internal/runtime/embedding-production/{run_id}/{attempt_id}/stage-receipt")
    async def embedding_stage_receipt(
        run_id: str, attempt_id: str, request: Request,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _runtime_authority(
            authorization=authorization, run_id=run_id, attempt_id=attempt_id,
            master_instance_id=master_instance_id, epoch=epoch, allowed_states=frozenset({"ACTIVE"}),
        )
        try:
            receipt = EmbeddingProductionStageReceipt.model_validate(await _bounded_json(request))
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "embedding_receipt_invalid"}) from exc
        if (str(receipt.run_id), str(receipt.master_instance_id), receipt.epoch) != (
            run_id, str(master_instance_id), int(epoch or 0)
        ):
            raise HTTPException(status_code=409, detail={"code": "embedding_receipt_epoch_mismatch"})
        pending = control_ledger.embedding_production_request(str(receipt.request_id))
        if (
            pending is None
            or pending["request_sha256"] != receipt.request_sha256
            or any(
                item.get("query_sha256") != pending["request"]["probe_query_sha256"]
                for item in receipt.query_vector_receipts.values()
            )
        ):
            raise HTTPException(status_code=409, detail={"code": "embedding_receipt_request_mismatch"})
        stored = control_ledger.record_embedding_stage_receipt(
            request_id=str(receipt.request_id), run_id=run_id, attempt_id=attempt_id,
            receipt=receipt.model_dump(mode="json"),
        )
        return {"accepted": True, "state": stored["state"], "receipt_sha256": receipt.receipt_sha256}

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
        pending = control_ledger.blogger_migration_request(str(receipt.request_id))
        if pending is None:
            raise HTTPException(status_code=409, detail={"code": "blogger_receipt_request_mismatch"})
        exact_request = BloggerMigrationRequest.model_validate(pending["request"])
        if (
            receipt.operation_id != exact_request.operation_id
            or receipt.request_sha256 != pending["request_sha256"]
            or receipt.export_batch_id
            != batch_identity(exact_request.snapshot_at, exact_request.expected_rows)
            or pending["claimed_run_id"] != run_id
            or pending["claimed_attempt_id"] != attempt_id
            or pending["claimed_master_instance_id"] != str(master_instance_id)
            or pending["claimed_epoch"] != int(epoch or 0)
        ):
            raise HTTPException(status_code=409, detail={"code": "blogger_receipt_request_mismatch"})
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
        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        body = await _bounded_json(request)
        if set(body) == {"request_id", "failure_code", "quarantine_receipt", "receipt_sha256"}:
            try:
                receipt = BloggerQuarantineReceipt.model_validate(body["quarantine_receipt"])
            except Exception as exc:
                raise HTTPException(
                    status_code=422, detail={"code": "blogger_quarantine_receipt_invalid"}
                ) from exc
            pending = control_ledger.blogger_migration_request(str(receipt.request_id))
            if pending is None:
                raise HTTPException(
                    status_code=409, detail={"code": "blogger_quarantine_receipt_mismatch"}
                )
            exact_request = BloggerMigrationRequest.model_validate(pending["request"])
            if (
                body["request_id"] != str(receipt.request_id)
                or body["failure_code"] != receipt.failure_code
                or body["receipt_sha256"] != receipt.receipt_sha256
                or receipt.operation_id != exact_request.operation_id
                or str(receipt.operation_id) != operation.operation_id
                or receipt.request_sha256 != pending["request_sha256"]
                or receipt.export_batch_id
                != batch_identity(exact_request.snapshot_at, exact_request.expected_rows)
                or receipt.run_id != run_id
                or receipt.attempt_id != attempt_id
                or str(receipt.master_instance_id) != str(master_instance_id)
                or receipt.epoch != int(epoch or 0)
                or pending["claimed_run_id"] != run_id
                or pending["claimed_attempt_id"] != attempt_id
                or pending["claimed_master_instance_id"] != str(master_instance_id)
                or pending["claimed_epoch"] != int(epoch or 0)
            ):
                raise HTTPException(
                    status_code=409, detail={"code": "blogger_quarantine_receipt_mismatch"}
                )
            try:
                stored = control_ledger.record_blogger_quarantine_receipt(
                    request_id=str(receipt.request_id),
                    run_id=run_id,
                    attempt_id=attempt_id,
                    receipt=receipt.model_dump(mode="json"),
                    receipt_sha256=receipt.receipt_sha256,
                )
            except StaleRuntimeEvent as exc:
                raise HTTPException(
                    status_code=409, detail={"code": "blogger_quarantine_receipt_conflict"}
                ) from exc
            return {
                "accepted": True,
                "state": stored["state"],
                "receipt_sha256": receipt.receipt_sha256,
            }
        if set(body) != {"request_id", "failure_code"} or body.get(
            "failure_code"
        ) == "BloggerMigrationQuarantined":
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
        token = authorization.removeprefix("Bearer ").strip()
        try:
            event = RuntimeEvent.model_validate_json(raw)
            armed = control_ledger.armed_master_acceptance_callback_loss(
                run_id=str(event.run_id), attempt_id=str(event.attempt_id), epoch=event.epoch
            )
            if armed is not None and event.event_type is RuntimeEventType.RUNTIME_HEARTBEAT:
                observed_hash = hashlib.sha256(raw).hexdigest()
                if armed["callback_state"] == "CAPTURED":
                    if (
                        armed["callback_event_id"] == str(event.event_id)
                        and hmac.compare_digest(str(armed["callback_body_sha256"]), observed_hash)
                    ):
                        raise HTTPException(
                            status_code=503,
                            detail={"code": "acceptance_callback_ack_suppressed"},
                        )
                    armed = None
            if armed is not None and event.event_type is RuntimeEventType.RUNTIME_HEARTBEAT:
                # Persist/authenticate/deduplicate the exact body, but
                # deliberately withhold its lifecycle projection and HTTP ACK.
                # A restart-safe owner supervisor later replays this stored ID.
                captured = control_ledger.ingest_runtime_event(raw, header_token=token)
                control_ledger.capture_master_acceptance_callback(
                    task_id=str(armed["task_id"]),
                    event_id=str(captured.event_id),
                    body_sha256=captured.body_sha256,
                )
                raise HTTPException(status_code=503, detail={"code": "acceptance_callback_ack_suppressed"})
            receipt = coordinator.accept_runtime_event(raw, header_token=token)
            if event.event_type in {RuntimeEventType.RUNTIME_TERMINAL, RuntimeEventType.RUNTIME_FAILED}:
                reconcile_status_cleanup = getattr(
                    master_runtime, "reconcile_status_cleanup_once", None
                )
                if reconcile_status_cleanup is not None:
                    # Cleanup failure never changes the already-durable runtime
                    # ACK. The periodic bounded reconciler resumes the exact
                    # delete intent after its cleanup claim expires.
                    with suppress(Exception):
                        await asyncio.to_thread(reconcile_status_cleanup)
            if event.event_type is RuntimeEventType.RUNTIME_HEARTBEAT:
                observed_hash = hashlib.sha256(raw).hexdigest()
                restarted = control_ledger.restarted_master_acceptance_callback(
                    run_id=str(event.run_id), attempt_id=str(event.attempt_id), epoch=event.epoch,
                    event_id=str(event.event_id), body_sha256=observed_hash,
                )
                if restarted is not None:
                    control_ledger.mark_master_acceptance_callback_replayed(
                        task_id=str(restarted["task_id"]), event_id=str(event.event_id),
                        body_sha256=observed_hash,
                    )
        except HTTPException:
            raise
        except (EventRejected, StaleRuntimeEvent, ValueError) as exc:
            raise HTTPException(status_code=400, detail={"code": "runtime_event_rejected"}) from exc
        return {
            "event_id": receipt.event_id,
            "disposition": receipt.disposition.value,
            "body_sha256": receipt.body_sha256,
        }

    @app.get("/internal/runtime/master-acceptance/{run_id}/{attempt_id}")
    def runtime_master_acceptance_command(
        run_id: str,
        attempt_id: str,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        """Claim one task-owned command; arbitrary fault payloads are impossible."""

        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        command = control_ledger.claim_master_acceptance_command(
            run_id=run_id,
            attempt_id=attempt_id,
            epoch=int(operation.identity["epoch"]),
        )
        return {"available": command is not None, "command": command}

    @app.get("/internal/runtime/master-acceptance/{run_id}/{attempt_id}/drain-directive")
    def runtime_master_acceptance_drain_directive(
        run_id: str,
        attempt_id: str,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        """Expose only an owner-host-claimed FM11/FM12 drain bit."""

        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        directive = control_ledger.master_acceptance_drain_directive(
            run_id=run_id,
            attempt_id=attempt_id,
            epoch=int(operation.identity["epoch"]),
        )
        return {"drain": directive is not None, "directive": directive}

    @app.get("/internal/runtime/master-acceptance/{run_id}/{attempt_id}/control-directive")
    def runtime_master_acceptance_control_directive(
        run_id: str,
        attempt_id: str,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        """Fixed FM10/FM24 booleans/counters; never a generic action body."""

        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        exact_epoch = int(str(epoch))
        identity = operation.identity
        if (
            identity.get("run_id") != run_id
            or identity.get("attempt_id") != attempt_id
            or identity.get("master_instance_id") != master_instance_id
            or int(identity.get("epoch", 0)) != exact_epoch
        ):
            raise HTTPException(status_code=409, detail={"code": "master_acceptance_runtime_binding_stale"})
        directive = control_ledger.master_acceptance_runtime_directive(
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=str(master_instance_id),
            epoch=exact_epoch,
        )
        return {
            "available": bool(directive["available"]),
            "renewal_suspended": bool(directive["renewal_suspended"]),
            "soak_requested_step": int(directive["soak_requested_step"]),
            "soak_completed_step": int(directive["soak_completed_step"]),
        }

    @app.post("/internal/runtime/master-acceptance/{run_id}/{attempt_id}/renewal-suspended")
    def runtime_master_acceptance_renewal_suspended(
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
        exact_epoch = int(str(epoch))
        if (
            operation.identity.get("master_instance_id") != master_instance_id
            or int(operation.identity.get("epoch", 0)) != exact_epoch
        ):
            raise HTTPException(status_code=409, detail={"code": "master_acceptance_runtime_binding_stale"})
        try:
            control_ledger.acknowledge_master_acceptance_renewal_suspension(
                run_id=run_id,
                attempt_id=attempt_id,
                master_instance_id=str(master_instance_id),
                epoch=exact_epoch,
            )
        except StaleRuntimeEvent as exc:
            raise HTTPException(status_code=409, detail={"code": "renewal_suspension_not_armed"}) from exc
        return {"accepted": True, "renewal_suspended": True}

    @app.post("/internal/runtime/master-acceptance/{run_id}/{attempt_id}/receipt")
    async def runtime_master_acceptance_receipt(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        """Accept only a validated live receipt from its exact ACTIVE runtime."""

        operation = _runtime_authority(
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"ACTIVE"}),
        )
        body = await _bounded_json(request)
        try:
            receipt = MasterAcceptanceReceipt.model_validate(body)
        except Exception as exc:
            raise HTTPException(status_code=422, detail={"code": "master_acceptance_receipt_invalid"}) from exc
        identity = operation.identity
        expected = (
            run_id,
            attempt_id,
            str(identity["service_instance_id"]),
            str(identity["master_instance_id"]),
            int(identity["epoch"]),
        )
        observed = (
            str(receipt.binding.run_id),
            str(receipt.binding.attempt_id),
            receipt.binding.service_instance_id,
            str(receipt.binding.master_instance_id),
            receipt.binding.epoch,
        )
        if observed != expected or str(receipt.binding.operation_id) != operation.operation_id:
            raise HTTPException(status_code=409, detail={"code": "master_acceptance_runtime_binding_stale"})
        task = control_ledger.complete_master_acceptance_command(
            command_id=str(receipt.command_id),
            command_sha256=receipt.command_sha256,
            run_id=run_id,
            attempt_id=attempt_id,
            epoch=receipt.binding.epoch,
            state="SUCCEEDED",
            receipt=receipt.model_dump(mode="json"),
        )
        return {
            "accepted": True,
            "task_id": task["task_id"],
            "state": task["state"],
            "receipt_sha256": receipt.receipt_sha256,
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
            "credential_roles": ["reader", "operator"] if active and operator_credential_enabled else ["reader"],
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
        expected_roles = (
            ["reader", "operator"]
            if operation.state == "ACTIVE" and operator_credential_enabled
            else ["reader"]
        )
        if [credential.role for credential in credentials] != expected_roles:
            raise HTTPException(status_code=422, detail={"code": "credential_roles_not_authorized"})
        references: list[str] = []
        for credential in credentials:
            reference = registrar.store(credential)
            references.append(Path(reference).name)
        return {"registered": len(references), "credential_refs": references}

    @app.post("/internal/runtime/tunnel-certificates/{run_id}/{attempt_id}")
    async def runtime_tunnel_certificate(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        broker = app.state.tunnel_certificate_broker
        if broker is None:
            raise HTTPException(status_code=503, detail={"code": "tunnel_certificate_broker_unavailable"})
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_required"})
        token = authorization.removeprefix("Bearer ").strip()
        if not control_ledger.runtime_token_valid(run_id, attempt_id, token):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_invalid"})
        raw = await request.body()
        if len(raw) > 16 * 1024:
            raise HTTPException(status_code=413, detail={"code": "tunnel_certificate_request_too_large"})
        try:
            body = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=400, detail={"code": "invalid_json"}) from exc
        if not isinstance(body, dict) or set(body) != {
            "master_instance_id", "epoch", "public_key", "valid_before"
        }:
            raise HTTPException(status_code=422, detail={"code": "tunnel_certificate_contract_invalid"})
        operation = control_ledger.operation_for_attempt(run_id, attempt_id)
        if operation is None or operation.state not in {"REGISTERING", "ACTIVE"}:
            raise HTTPException(status_code=409, detail={"code": "runtime_not_certificate_eligible"})
        identity = operation.identity
        try:
            exact_epoch = int(body["epoch"])
            valid_before = datetime.fromisoformat(str(body["valid_before"]).replace("Z", "+00:00")).astimezone(UTC)
        except (TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail={"code": "tunnel_certificate_contract_invalid"}) from exc
        if (
            str(identity.get("run_id")) != run_id
            or str(identity.get("attempt_id")) != attempt_id
            or str(identity.get("master_instance_id")) != str(body["master_instance_id"])
            or int(identity.get("epoch", 0)) != exact_epoch
            or exact_epoch < 1
        ):
            raise HTTPException(status_code=409, detail={"code": "runtime_epoch_fenced"})
        public_key = str(body["public_key"])
        parts = public_key.split(" ")
        try:
            decoded_key = base64.b64decode(parts[1], validate=True) if len(parts) == 2 else b""
        except ValueError as exc:
            raise HTTPException(status_code=422, detail={"code": "tunnel_public_key_invalid"}) from exc
        if (
            len(public_key) > 1_000
            or "\n" in public_key
            or parts[0] != "ssh-ed25519"
            or len(decoded_key) != 51
            or not decoded_key.startswith(b"\x00\x00\x00\x0bssh-ed25519\x00\x00\x00\x20")
        ):
            raise HTTPException(status_code=422, detail={"code": "tunnel_public_key_invalid"})
        now = control_ledger.clock.now().astimezone(UTC)
        latest_expiry = now + timedelta(minutes=5)
        if operation.state == "ACTIVE":
            service = control_ledger.resolve_service("postgres-master", now=now)
            if (
                service is None
                or service.run_id != run_id
                or service.attempt_id != attempt_id
                or service.master_instance_id != str(body["master_instance_id"])
                or service.epoch != exact_epoch
            ):
                raise HTTPException(status_code=409, detail={"code": "runtime_epoch_fenced"})
            latest_expiry = min(latest_expiry, service.lease_until.astimezone(UTC))
        lease_value = identity.get("lease_until")
        if operation.state == "REGISTERING" and lease_value:
            try:
                latest_expiry = min(
                    latest_expiry,
                    datetime.fromisoformat(str(lease_value).replace("Z", "+00:00")).astimezone(UTC),
                )
            except ValueError as exc:
                raise HTTPException(status_code=409, detail={"code": "runtime_lease_invalid"}) from exc
        if valid_before <= now or valid_before > latest_expiry:
            raise HTTPException(status_code=422, detail={"code": "tunnel_certificate_expiry_invalid"})
        try:
            issued = broker.issue_public_key(
                master_instance_id=str(body["master_instance_id"]),
                run_id=run_id,
                attempt_id=attempt_id,
                epoch=exact_epoch,
                public_key=public_key,
                valid_before=valid_before,
                now=now,
            )
        except Exception as exc:
            raise HTTPException(status_code=503, detail={"code": "tunnel_certificate_issue_failed"}) from exc
        try:
            certificate = TunnelCertificate(
                certificate=str(issued.certificate),
                serial=int(issued.serial),
                principal=str(issued.principal),
                valid_before=issued.valid_before,
                listen_host=str(issued.listen_host),
                listen_port=int(issued.listen_port),
            )
        except (AttributeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=503, detail={"code": "tunnel_certificate_result_invalid"}) from exc
        if certificate.valid_before != valid_before:
            raise HTTPException(status_code=503, detail={"code": "tunnel_certificate_result_invalid"})
        return {
            "certificate": certificate.certificate,
            "serial": certificate.serial,
            "principal": certificate.principal,
            "valid_before": certificate.valid_before.astimezone(UTC).isoformat().replace("+00:00", "Z"),
            "listen_host": certificate.listen_host,
            "listen_port": certificate.listen_port,
        }

    @app.post("/internal/runtime/tunnel-leases/{run_id}/{attempt_id}")
    async def runtime_tunnel_lease(
        run_id: str,
        attempt_id: str,
        request: Request,
        authorization: str | None = Header(default=None),
    ) -> dict[str, Any]:
        broker = app.state.tunnel_certificate_broker
        if broker is None:
            raise HTTPException(status_code=503, detail={"code": "tunnel_certificate_broker_unavailable"})
        if not authorization or not authorization.startswith("Bearer "):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_required"})
        token = authorization.removeprefix("Bearer ").strip()
        if not control_ledger.runtime_token_valid(run_id, attempt_id, token):
            raise HTTPException(status_code=401, detail={"code": "runtime_token_invalid"})
        raw = await request.body()
        if len(raw) > 4 * 1024:
            raise HTTPException(status_code=413, detail={"code": "tunnel_lease_request_too_large"})
        try:
            body = json.loads(raw)
            if not isinstance(body, dict) or set(body) != {"master_instance_id", "epoch", "lease_until"}:
                raise ValueError("fields")
            epoch_value = int(body["epoch"])
            lease_until = datetime.fromisoformat(str(body["lease_until"]).replace("Z", "+00:00")).astimezone(UTC)
        except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError) as exc:
            raise HTTPException(status_code=422, detail={"code": "tunnel_lease_contract_invalid"}) from exc
        operation = control_ledger.operation_for_attempt(run_id, attempt_id)
        if operation is None or operation.state not in {"REGISTERING", "ACTIVE"}:
            raise HTTPException(status_code=409, detail={"code": "runtime_not_tunnel_eligible"})
        identity = operation.identity
        if (
            str(identity.get("master_instance_id")) != str(body["master_instance_id"])
            or int(identity.get("epoch", 0)) != epoch_value
        ):
            raise HTTPException(status_code=409, detail={"code": "runtime_epoch_fenced"})
        now = control_ledger.clock.now().astimezone(UTC)
        if lease_until <= now + timedelta(seconds=15) or lease_until > now + timedelta(minutes=5):
            raise HTTPException(status_code=422, detail={"code": "tunnel_lease_expiry_invalid"})
        if operation.state == "ACTIVE":
            service = control_ledger.resolve_service("postgres-master", now=now)
            if (
                service is None
                or service.run_id != run_id
                or service.attempt_id != attempt_id
                or service.master_instance_id != str(body["master_instance_id"])
                or service.epoch != epoch_value
                or lease_until > service.lease_until.astimezone(UTC)
            ):
                raise HTTPException(status_code=409, detail={"code": "runtime_epoch_fenced"})
        try:
            renewed = broker.renew(
                master_instance_id=str(body["master_instance_id"]),
                run_id=run_id,
                attempt_id=attempt_id,
                epoch=epoch_value,
                lease_until=lease_until,
                now=now,
            )
        except Exception as exc:
            raise HTTPException(status_code=503, detail={"code": "tunnel_lease_renewal_failed"}) from exc
        observed = getattr(renewed, "lease_until", None)
        if not isinstance(observed, datetime) or observed < lease_until or observed > now + timedelta(minutes=10):
            raise HTTPException(status_code=503, detail={"code": "tunnel_lease_result_invalid"})
        return {"renewed": True, "lease_until": observed.isoformat().replace("+00:00", "Z")}

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
        operation = _provider_authority(
            request=request,
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
        embedding_authority = _embedding_provider_authority(operation.operation_id)
        embedding_datasets = {value for key, value in embedding_authority.items() if key.endswith("_input")}
        embedding_workers = {value for key, value in embedding_authority.items() if key.endswith("_worker")}
        authorized = False
        acceptance_request = operation.acceptance["request"] if operation.acceptance else None
        if acceptance_request is not None:
            allowed_dataset = str(acceptance_request["candidate_dataset_ref"])
            allowed_notebook = acceptance_request.get("verifier_notebook_ref")
            authorized = bool(
                str(intent.task_id) == current_run
                and (
                    (intent.action in {MutationAction.CREATE_DATASET, MutationAction.VERSION_DATASET}
                    and intent.provider_ref == allowed_dataset)
                    or (intent.action is MutationAction.PUSH_NOTEBOOK
                    and allowed_notebook is not None
                    and intent.provider_ref == allowed_notebook)
                )
            )
        elif assets is not None and intent.action is MutationAction.CREATE_DATASET:
            authorized = (
                (intent.provider_ref == assets.checkpoint_ref and str(intent.task_id) == current_run)
                or (intent.provider_ref, intent.task_id) in embedding_datasets
            )
        elif assets is not None and intent.action is MutationAction.PUSH_NOTEBOOK:
            authorized = (
                (intent.provider_ref == assets.checkpoint_verifier_ref and str(intent.task_id) == current_run)
                or (intent.provider_ref, intent.task_id) in embedding_workers
            )
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
        embedding_authority = _embedding_provider_authority(operation.operation_id)
        embedding_pairs = set(embedding_authority.values())
        task_authorized = str(claim.task_id) == current_run or (claim.provider_ref, claim.task_id) in embedding_pairs
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
        acceptance_request = operation.acceptance["request"] if operation.acceptance else None
        acceptance_claim = bool(
            acceptance_request is not None
            and str(claim.task_id) == current_run
            and claim.disposable
            and (
                (
                    claim.provider_ref == str(acceptance_request["candidate_dataset_ref"])
                    and claim.kind is ProviderKind.DATASET
                    and claim.control_class is ControlClass.MCP_EXCHANGE
                )
                or (
                    claim.provider_ref == str(acceptance_request.get("verifier_notebook_ref") or "")
                    and claim.kind is ProviderKind.NOTEBOOK
                    and claim.control_class is ControlClass.MCP_MANAGED
                )
            )
        )
        if (
            authority is None
            or authority["operation_id"] != operation.operation_id
            or authority["task_id"] != str(claim.task_id)
            or authority["provider_ref"] != claim.provider_ref
            or claim.kind.value != expected_kind
            or not (
                acceptance_claim
                or (
                    task_authorized
                    and claim.control_class is ControlClass.ORCHESTRATOR_PROTECTED
                    and not claim.disposable
                    and assets is not None
                    and claim.provider_ref in {
                        assets.checkpoint_ref, assets.checkpoint_verifier_ref,
                        *(provider_ref for provider_ref, _task_id in embedding_pairs),
                    }
                )
            )
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
            authorization=authorization,
            run_id=run_id,
            attempt_id=attempt_id,
            master_instance_id=master_instance_id,
            epoch=epoch,
            allowed_states=frozenset({"REGISTERING", "ACTIVE", "DRAINING", "CHECKPOINTING"}),
        )
        body = await _bounded_json(request)
        assets = _configured_master_assets()
        acceptance_request = operation.acceptance["request"] if operation.acceptance else None
        expected_ref = (
            str(acceptance_request["candidate_dataset_ref"])
            if acceptance_request is not None
            else assets.checkpoint_ref if assets else None
        )
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
        request: Request,
        authorization: str | None = Header(default=None),
        run_id: str | None = Header(default=None, alias="X-MDH-Run-ID"),
        attempt_id: str | None = Header(default=None, alias="X-MDH-Attempt-ID"),
        master_instance_id: str | None = Header(default=None, alias="X-MDH-Master-Instance-ID"),
        epoch: str | None = Header(default=None, alias="X-MDH-Epoch"),
    ) -> dict[str, Any]:
        _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
        acceptance_request = operation.acceptance["request"] if operation.acceptance else None
        expected_dataset = (
            str(acceptance_request["candidate_dataset_ref"])
            if acceptance_request is not None
            else assets.checkpoint_ref if assets else None
        )
        acceptance_manifest = bool(
            acceptance_request is not None
            and manifest.source_run_id == str(operation.identity["run_id"])
            and manifest.source_identity
            == f"checkpoint-acceptance:{acceptance_request['source_revision']}"
        )
        runtime_manifest = bool(
            acceptance_request is None
            and str(manifest.master_instance_id) == str(operation.identity["master_instance_id"])
            and manifest.epoch == int(operation.identity["epoch"])
            and manifest.source_run_id == str(operation.identity["run_id"])
        )
        if (
            body["operation_id"] != operation.operation_id
            or body["service_kind"] != "postgres-master"
            or not expected_dataset
            or body["dataset_ref"] != expected_dataset
            or not (acceptance_manifest or runtime_manifest)
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
        operation = _provider_authority(
            request=request,
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
