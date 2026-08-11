"""Fail-closed production assembly for the owner OAuth authorization server."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path

from my_data_hub.control_plane.adapters import ControlLedgerOAuthAuthority
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.mcp.catalog import ALL_SCOPES, READER_PROFILE_SCOPES

from .app import create_authorization_app
from .control_store import ControlLedgerOAuthGrantStore
from .models import AuthorizationServerSettings, StaticClient
from .owner_oidc import OIDCSessionOwnerAuthenticator
from .service import AuthorizationService


@dataclass(frozen=True, slots=True)
class AuthorizationRuntime:
    app: object
    host: str
    port: int


def _required(name: str) -> str:
    value = os.getenv(name, "").strip()
    if not value:
        raise RuntimeError(f"required OAuth authorization setting is absent: {name}")
    return value


def _clients(raw: str) -> tuple[StaticClient, ...]:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("OAuth clients JSON is invalid") from exc
    if not isinstance(payload, list) or not 1 <= len(payload) <= 4:
        raise ValueError("OAuth clients JSON must contain one to four static clients")
    clients: list[StaticClient] = []
    for item in payload:
        if not isinstance(item, dict) or set(item) != {"client_id", "redirect_uris", "allowed_scopes"}:
            raise ValueError("OAuth client fields differ from the static contract")
        scopes = frozenset(str(value) for value in item["allowed_scopes"])
        if not scopes.issubset(ALL_SCOPES | {"openid", "offline_access"}):
            raise ValueError("OAuth client requests a scope outside the MCP catalog")
        clients.append(
            StaticClient(
                client_id=str(item["client_id"]),
                redirect_uris=tuple(str(value) for value in item["redirect_uris"]),
                allowed_scopes=scopes,
            )
        )
    return tuple(clients)


def build_authorization_runtime() -> AuthorizationRuntime:
    ledger = ControlLedger(
        Path(os.getenv("MY_DATA_HUB_CONTROL_LEDGER_PATH", "/state/control.sqlite3"))
    )
    issuer = _required("MY_DATA_HUB_OAUTH_ISSUER")
    owner_subject = _required("MY_DATA_HUB_OAUTH_OWNER_SUBJECT")
    key_path = Path(_required("MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE"))
    if key_path.is_symlink() or not key_path.is_file() or key_path.stat().st_mode & 0o077:
        raise RuntimeError("OAuth signing key must be a private regular file")
    settings = AuthorizationServerSettings(
        issuer=issuer,
        resource=_required("MY_DATA_HUB_MCP_OAUTH_RESOURCE"),
        audience=_required("MY_DATA_HUB_MCP_OAUTH_AUDIENCE"),
        owner_subject=owner_subject,
        clients=_clients(_required("MY_DATA_HUB_OAUTH_CLIENTS_JSON")),
        signing_key_pem=key_path.read_bytes(),
        signing_key_id=_required("MY_DATA_HUB_OAUTH_SIGNING_KEY_ID"),
    )
    authority = ControlLedgerOAuthAuthority(ledger)
    for client in settings.clients:
        ledger.register_oauth_client(
            issuer=issuer,
            client_id=client.client_id,
            principal_id=owner_subject,
            allowed_scopes=client.allowed_scopes,
            profile_kind=(
                "reader"
                if client.allowed_scopes - {"openid", "offline_access"} <= READER_PROFILE_SCOPES
                else "owner_operator"
            ),
        )
    service = AuthorizationService(
        settings=settings,
        control_ledger=authority,
        grant_store=ControlLedgerOAuthGrantStore(ledger),
    )
    owner = OIDCSessionOwnerAuthenticator(
        issuer=_required("MY_DATA_HUB_OWNER_OIDC_ISSUER"),
        audience=_required("MY_DATA_HUB_OWNER_OIDC_AUDIENCE"),
        jwks_url=_required("MY_DATA_HUB_OWNER_OIDC_JWKS_URL"),
        login_url=_required("MY_DATA_HUB_OWNER_LOGIN_URL"),
        owner_subject=owner_subject,
        cookie_name=os.getenv("MY_DATA_HUB_OWNER_SESSION_COOKIE", "mdh_owner_session"),
    )
    try:
        port = int(os.getenv("MY_DATA_HUB_OAUTH_PORT", "8780"))
    except ValueError as exc:
        raise RuntimeError("MY_DATA_HUB_OAUTH_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise RuntimeError("MY_DATA_HUB_OAUTH_PORT is invalid")
    return AuthorizationRuntime(
        app=create_authorization_app(service=service, owner_authenticator=owner),
        host=os.getenv("MY_DATA_HUB_OAUTH_HOST", "127.0.0.1"),
        port=port,
    )


def main() -> None:
    import uvicorn

    runtime = build_authorization_runtime()
    uvicorn.run(runtime.app, host=runtime.host, port=runtime.port)


if __name__ == "__main__":
    main()
