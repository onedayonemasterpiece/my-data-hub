"""Production construction for the lightweight remote MCP resource server.

This process owns no canonical state and receives no PostgreSQL credentials.  It
shares only the bounded control ledger with the control API.  The data-plane
broker is deliberately absent until an epoch-bound tunnel broker is injected;
status and cold-start requests remain useful while ACTIVE data reads fail closed.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from my_data_hub.auth.control import ControlLedgerRevocationStore
from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.control_plane.adapters import (
    ControlLedgerOAuthAuthority,
    KaggleMCPProviderGateway,
    LedgerControlReader,
    LedgerMasterResolver,
    LedgerWriteGate,
)
from my_data_hub.control_plane.app import assert_no_database_environment
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.oauth import OAuthBearerValidator, OAuthValidationPolicy, VerifiedTokenDecoder
from my_data_hub.mcp.oauth_jwt import JwksJwtDecoder
from my_data_hub.mcp.postgres_broker import (
    DirectoryEpochCredentialSource,
    PostgresMasterSessionBroker,
)
from my_data_hub.mcp.server import MCPDependencies, create_streamable_http_app
from my_data_hub.mcp.sql_policy import BoundedSQLPolicy
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal, KaggleProviderAdapter


@dataclass(frozen=True, slots=True)
class RemoteMCPRuntime:
    settings: Settings
    ledger: ControlLedger
    validator: OAuthBearerValidator
    app: object


def build_remote_runtime(
    *,
    settings: Settings | None = None,
    ledger: ControlLedger | None = None,
    decoder: VerifiedTokenDecoder | None = None,
    write_gate: LedgerWriteGate | None = None,
    provider_adapter: KaggleProviderAdapter | None = None,
    sql_policy: BoundedSQLPolicy | None = None,
) -> RemoteMCPRuntime:
    """Build the remote reader profile from explicit, fail-closed dependencies."""

    # Reuse the control-plane environment guard so libpq variables cannot leak
    # into this long-running resource-server process.
    assert_no_database_environment()
    runtime_settings = settings or Settings.from_env(require_database=False)
    if not runtime_settings.mcp_remote_enabled or runtime_settings.mcp_auth_mode != "oauth":
        raise ConfigurationError("remote MCP runtime requires the production OAuth profile")
    control_ledger = ledger or ControlLedger(
        Path(os.getenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", "/state/control.sqlite3")).expanduser()
    )
    if runtime_settings.mcp_write_enabled:
        if not runtime_settings.mcp_operator_profile_enabled:
            raise ConfigurationError("remote MCP writes require the explicit owner/operator profile")
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
            write_gate = LedgerWriteGate(control_ledger, signing_secret=secret)
        if provider_adapter is None:
            try:
                provider_adapter = KaggleProviderAdapter.from_environment(
                    journal=ControlLedgerKaggleJournal(control_ledger)
                )
            except Exception as exc:
                raise ConfigurationError("operator Kaggle adapter is unavailable") from exc
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
            allowed_scopes=runtime_settings.mcp_scopes,
            max_token_lifetime_seconds=runtime_settings.mcp_token_max_lifetime_seconds,
        ),
        revocations=ControlLedgerRevocationStore(authority),
        control_ledger=authority,
    )
    gateway = (
        KaggleMCPProviderGateway(control_ledger, provider_adapter)
        if runtime_settings.mcp_write_enabled and provider_adapter is not None
        else None
    )
    exact_sql_policy = sql_policy or BoundedSQLPolicy(change_targets=frozenset())
    dependencies = MCPDependencies(
        resolver=LedgerMasterResolver(control_ledger),
        broker=PostgresMasterSessionBroker(
            DirectoryEpochCredentialSource(
                Path(os.getenv("MY_DATA_HUB_MASTER_SESSION_DIRECTORY", "/state/master-sessions"))
            ),
            sql_policy=exact_sql_policy,
        ),
        control=LedgerControlReader(
            control_ledger,
            deployed_commit=os.getenv("MY_DATA_HUB_DEPLOY_COMMIT") or None,
            write_gate=write_gate,
            provider_gateway=gateway,
        ),
        write_gate=write_gate,
        audit=authority,
        sql_policy=exact_sql_policy,
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
    # Refuse accidental CLI arguments such as a development transport flag.
    if len(os.sys.argv) != 1:
        raise SystemExit("remote MCP runtime accepts no command-line configuration")
    serve()


if __name__ == "__main__":
    main()
