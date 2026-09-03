"""Production construction for the lightweight remote MCP resource server.

This process owns no canonical state and receives no PostgreSQL credentials. It
shares only the bounded control ledger with the control API and the dedicated
Google AI quota ledger. The Google ledger contains quota metadata, never API-key
secrets or business data.
"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from my_data_hub.auth.control import ControlLedgerRevocationStore
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.control_plane.adapters import (
    ControlLedgerOAuthAuthority,
    LedgerControlReader,
    LedgerMasterResolver,
    LedgerWriteGate,
)
from my_data_hub.control_plane.app import assert_no_database_environment
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.google_ai.analyzer import GeminiYouTubeAnalyzer, YouTubeAnalyzerConfig
from my_data_hub.google_ai.config import GoogleYouTubeSettings
from my_data_hub.google_ai.http import StreamTimeouts
from my_data_hub.google_ai.interactions import GeminiInteractionsClient
from my_data_hub.google_ai.limiter import SupabaseGoogleAILimiter
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.contracts import MasterSnapshot, MasterState, WriteGate, WritePermit
from my_data_hub.mcp.control_gateway import (
    AuthenticatedProviderControlClient,
    SplitControlPlaneReader,
)
from my_data_hub.mcp.oauth import (
    AccessIdentity,
    OAuthBearerValidator,
    OAuthValidationPolicy,
    VerifiedTokenDecoder,
)
from my_data_hub.mcp.oauth_jwt import JwksJwtDecoder
from my_data_hub.mcp.postgres_broker import (
    DirectoryEpochCredentialSource,
    PostgresMasterSessionBroker,
)
from my_data_hub.mcp.server import MCPDependencies
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy
from my_data_hub.mcp.youtube_server import (
    YouTubeMCPDependencies,
    create_streamable_http_app,
)


@dataclass(frozen=True, slots=True)
class RemoteMCPRuntime:
    settings: Settings
    ledger: ControlLedger
    validator: OAuthBearerValidator
    app: object


_PROVIDER_ONLY_MUTATIONS = frozenset(
    {
        "provider.resources.create",
        "provider.resources.version",
        "provider.resources.run",
        "provider.resources.delete",
        "provider.upload.start",
        "provider.upload.put_chunk",
        "provider.upload.finalize",
        "provider.upload.abort",
        "provider.acceptance.claim.cleanup",
    }
)
_OAUTH_PROTOCOL_SCOPES = frozenset({"openid", "offline_access"})
_GUARDED_WRITE_SCOPES = frozenset(
    {
        "master:ensure",
        "master:rotate",
        "recovery:request",
        "acceptance:probe",
        "acceptance:operate",
        "data:write",
        "migration:operate",
        "bloggers:write",
        "region-talk:operate",
        "provider:write",
    }
)


class ProviderOnlyWriteGate:
    """Authorize private provider effects without inventing canonical DB durability."""

    def __init__(self, signing_secret: bytes, *, clock=time.time) -> None:  # type: ignore[no-untyped-def]
        if len(signing_secret) < 32:
            raise ValueError("provider-only write-gate secret must contain at least 32 bytes")
        self._secret = signing_secret
        self._clock = clock

    def authorize_write(
        self,
        *,
        principal: AccessIdentity,
        tool: str,
        arguments: Mapping[str, object],
        master: MasterSnapshot,
    ) -> WritePermit:
        if tool not in _PROVIDER_ONLY_MUTATIONS or "provider:write" not in principal.scopes:
            raise PermissionError("provider-only write gate rejects this tool or scope")
        if master.state is not MasterState.ABSENT:
            raise PermissionError("provider-only write gate requires canonical master ABSENT")
        resource_class = (
            "mcp_managed"
            if tool == "provider.acceptance.claim.cleanup"
            else str(arguments.get("control_class", ""))
        )
        if resource_class not in {"mcp_managed", "mcp_exchange"}:
            raise PermissionError("provider-only write gate accepts only MCP-controlled resources")
        if tool != "provider.acceptance.claim.cleanup" and arguments.get("private") is not True:
            raise PermissionError("provider-only write gate accepts only private resources")
        expires_at = int(self._clock()) + 120
        payload = {
            "tool": tool,
            "principal": principal.subject,
            "client_id": principal.client_id,
            "arguments_sha256": hashlib.sha256(
                canonical_json_bytes(dict(arguments))
            ).hexdigest(),
            "expires_at": expires_at,
        }
        permit_id = hmac.new(
            self._secret,
            canonical_json_bytes(payload),
            hashlib.sha256,
        ).hexdigest()
        return WritePermit(
            permit_id=permit_id,
            tool=tool,
            principal=principal.subject,
            client_id=principal.client_id,
            master_epoch=0,
            canonical_revision=0,
            expires_at=expires_at,
            preview_bound=True,
            checkpoint_lifecycle_bound=False,
            pre_change_checkpoint_verified=False,
            allowed_resource_class=resource_class,
            private_resource_only=True,
            canonical_data_independent=True,
        )

    def record_write_result(
        self, *, permit: WritePermit, result: Mapping[str, object]
    ) -> Mapping[str, object]:
        del permit, result
        raise PermissionError("provider-only gate has no canonical write result path")

    def reconciliation_request(
        self,
        *,
        principal: AccessIdentity,
        master: MasterSnapshot,
        operation_id: str | None = None,
        arguments: Mapping[str, object] | None = None,
    ) -> None:
        del principal, master, operation_id, arguments
        return None

    def record_reconciled_write(
        self, *, operation_id: str, receipt: Mapping[str, object]
    ) -> Mapping[str, object]:
        del operation_id, receipt
        raise PermissionError("provider-only gate has no canonical reconciliation path")


class UnifiedBootstrapWriteGate(ProviderOnlyWriteGate):
    """Permit only provider effects while canonical master lifecycle stays independent."""

    def authorize_write(
        self,
        *,
        principal: AccessIdentity,
        tool: str,
        arguments: Mapping[str, object],
        master: MasterSnapshot,
    ) -> WritePermit:
        if tool not in _PROVIDER_ONLY_MUTATIONS or "provider:write" not in principal.scopes:
            raise PermissionError("unified bootstrap write gate rejects this tool or scope")
        resource_class = (
            "mcp_managed"
            if tool == "provider.acceptance.claim.cleanup"
            else str(arguments.get("control_class", ""))
        )
        if resource_class not in {"mcp_managed", "mcp_exchange"}:
            raise PermissionError("unified bootstrap accepts only MCP-controlled resources")
        if tool != "provider.acceptance.claim.cleanup" and arguments.get("private") is not True:
            raise PermissionError("unified bootstrap accepts only private resources")
        return super().authorize_write(
            principal=principal,
            tool=tool,
            arguments=arguments,
            master=MasterSnapshot(MasterState.ABSENT),
        )


def _build_youtube_analyzer(settings: Settings):  # type: ignore[no-untyped-def]
    feature = GoogleYouTubeSettings.from_settings(settings)
    if not feature.enabled:
        return None
    limiter = SupabaseGoogleAILimiter(
        supabase_url=feature.limiter_supabase_url,
        service_key=feature.limiter_supabase_service_key,
        candidate_env_names=feature.normal_key_envs,
    )
    interactions = GeminiInteractionsClient(
        timeouts=StreamTimeouts(
            connect_seconds=feature.connect_timeout_seconds,
            first_event_seconds=feature.first_event_timeout_seconds,
            idle_seconds=feature.idle_timeout_seconds,
            total_seconds=feature.total_timeout_seconds,
        ),
        max_raw_sse_bytes=feature.max_raw_sse_bytes,
        max_output_bytes=feature.max_model_output_bytes,
    )
    return GeminiYouTubeAnalyzer(
        config=YouTubeAnalyzerConfig(
            enabled=True,
            default_model=feature.model,
            allowed_models=frozenset(feature.allowed_models),
            max_output_tokens=feature.max_output_tokens,
            max_result_bytes=feature.max_result_bytes,
        ),
        limiter=limiter,
        interactions=interactions,
    )


def build_remote_runtime(
    *,
    settings: Settings | None = None,
    ledger: ControlLedger | None = None,
    decoder: VerifiedTokenDecoder | None = None,
    write_gate: WriteGate | None = None,
    provider_control: object | None = None,
    sql_policy: BoundedSQLPolicy | None = None,
) -> RemoteMCPRuntime:
    """Build the remote profile with explicit, fail-closed dependencies."""

    assert_no_database_environment()
    if any(name.startswith("KAGGLE_") for name in os.environ):
        raise ConfigurationError("remote MCP must not receive Kaggle provider credentials or configuration")
    runtime_settings = settings or Settings.from_env(require_database=False)
    if not runtime_settings.mcp_remote_enabled or runtime_settings.mcp_auth_mode != "oauth":
        raise ConfigurationError("remote MCP runtime requires the production OAuth profile")
    control_ledger = ledger or ControlLedger(
        Path(os.getenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", "/state/control.sqlite3")).expanduser()
    )
    if runtime_settings.mcp_write_enabled and not (
        runtime_settings.mcp_operator_profile_enabled
        or runtime_settings.mcp_provider_profile_enabled
        or runtime_settings.mcp_unified_bootstrap_profile_enabled
    ):
        raise ConfigurationError(
            "remote MCP writes require an explicit owner/operator, provider-only, or unified bootstrap profile"
        )
    guarded_write_enabled = bool(
        runtime_settings.mcp_write_enabled
        and runtime_settings.mcp_scopes.intersection(_GUARDED_WRITE_SCOPES)
    )
    if guarded_write_enabled:
        if write_gate is None:
            secret_path = Path(os.getenv("MY_DATA_HUB_MCP_WRITE_GATE_SECRET_FILE", "")).expanduser()
            if not secret_path.is_absolute() or secret_path.is_symlink() or not secret_path.is_file():
                raise ConfigurationError(
                    "remote MCP owner/operator writes require injected gate secret file"
                )
            mode = secret_path.stat().st_mode & 0o777
            secret = secret_path.read_bytes().strip()
            if mode & 0o077 or not 32 <= len(secret) <= 256:
                raise ConfigurationError("operator write-gate secret violates the bounded private-file contract")
            if runtime_settings.mcp_provider_profile_enabled:
                write_gate = ProviderOnlyWriteGate(secret)
            elif runtime_settings.mcp_unified_bootstrap_profile_enabled:
                write_gate = UnifiedBootstrapWriteGate(secret)
            else:
                write_gate = LedgerWriteGate(control_ledger, signing_secret=secret)
        if provider_control is None:
            try:
                provider_control = AuthenticatedProviderControlClient.from_token_file(
                    runtime_settings.mcp_control_gateway_url,
                    runtime_settings.mcp_control_gateway_token_file or Path(""),
                )
            except Exception as exc:
                raise ConfigurationError("authenticated provider control gateway is unavailable") from exc
        sql_policy = sql_policy or BoundedSQLPolicy()
    authority = ControlLedgerOAuthAuthority(control_ledger)
    token_decoder = decoder or JwksJwtDecoder(
        jwks_url=runtime_settings.mcp_oauth_jwks_url,
        issuer=runtime_settings.mcp_oauth_issuer,
        audience=runtime_settings.mcp_oauth_audience,
        algorithms=runtime_settings.mcp_oauth_algorithms,
    )
    validator = OAuthBearerValidator(
        decoder=token_decoder,
        policy=OAuthValidationPolicy(
            issuer=runtime_settings.mcp_oauth_issuer,
            audience=runtime_settings.mcp_oauth_audience,
            resource=runtime_settings.mcp_oauth_resource,
            allowed_scopes=runtime_settings.mcp_scopes | _OAUTH_PROTOCOL_SCOPES,
            max_token_lifetime_seconds=runtime_settings.mcp_token_max_lifetime_seconds,
        ),
        revocations=ControlLedgerRevocationStore(authority),
        control_ledger=authority,
    )
    exact_sql_policy = sql_policy or BoundedSQLPolicy(change_targets=frozenset())
    local_control = LedgerControlReader(
        control_ledger,
        deployed_commit=os.getenv("MY_DATA_HUB_DEPLOY_COMMIT") or None,
        write_gate=write_gate,
    )
    control = (
        SplitControlPlaneReader(local_control, provider_control)  # type: ignore[arg-type]
        if guarded_write_enabled and provider_control is not None
        else local_control
    )
    base_dependencies = MCPDependencies(
        resolver=LedgerMasterResolver(control_ledger),
        broker=PostgresMasterSessionBroker(
            DirectoryEpochCredentialSource(
                Path(os.getenv("MY_DATA_HUB_MASTER_SESSION_DIRECTORY", "/state/master-sessions"))
            ),
            sql_policy=exact_sql_policy,
        ),
        control=control,
        write_gate=write_gate,
        audit=authority,
        sql_policy=exact_sql_policy,
        acceptance_scenarios_enabled=runtime_settings.mcp_acceptance_scenarios_enabled,
        provider_only_profile_enabled=runtime_settings.mcp_provider_profile_enabled,
        unified_bootstrap_profile_enabled=runtime_settings.mcp_unified_bootstrap_profile_enabled,
        reader_profile_enabled=not (
            runtime_settings.mcp_operator_profile_enabled
            or runtime_settings.mcp_provider_profile_enabled
            or runtime_settings.mcp_unified_bootstrap_profile_enabled
        ),
    )
    dependencies = YouTubeMCPDependencies(
        base=base_dependencies,
        analyzer=_build_youtube_analyzer(runtime_settings),
        feature_enabled=runtime_settings.google_youtube_enabled,
    )
    app = create_streamable_http_app(
        runtime_settings,
        dependencies=dependencies,
        validator=validator,
    )
    return RemoteMCPRuntime(runtime_settings, control_ledger, validator, app)


def serve() -> None:
    import uvicorn

    runtime = build_remote_runtime()
    uvicorn.run(runtime.app, host=runtime.settings.mcp_host, port=runtime.settings.mcp_port)


def main() -> None:
    if len(os.sys.argv) != 1:
        raise SystemExit("remote MCP runtime accepts no command-line configuration")
    serve()


if __name__ == "__main__":
    main()
