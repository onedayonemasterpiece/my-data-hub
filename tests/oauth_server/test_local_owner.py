from __future__ import annotations

import base64
import hashlib
import json
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from my_data_hub.auth.control import OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.oauth_server.app import create_authorization_app
from my_data_hub.oauth_server.client_metadata import (
    ChatGPTClientMetadataResolver,
    ClientMetadataResponse,
)
from my_data_hub.oauth_server.local_owner import (
    LocalOwnerTokenAuthenticator,
    LocalOwnerTokenPortal,
)
from my_data_hub.oauth_server.models import AuthorizationServerSettings, StaticClient
from my_data_hub.oauth_server.service import AuthorizationService
from my_data_hub.oauth_server.stores import MemoryOAuthGrantStore

ISSUER = "https://identity.example.test"
RESOURCE = "https://mcp.example.test/mcp"
TOKEN = "owner-bootstrap-" + "x" * 48
VERIFIER = "A" * 43
CHALLENGE = base64.urlsafe_b64encode(hashlib.sha256(VERIFIER.encode()).digest()).rstrip(b"=").decode()
NOW = 2_000_000_000


@dataclass
class Ledger:
    clients: dict[str, frozenset[str]]

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        scopes = self.clients.get(client_id)
        return None if scopes is None else OAuthClientRecord(issuer, client_id, True, scopes)

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return False

    def record_oauth_audit(self, event: object) -> None:
        return None


def _client() -> TestClient:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    clients = (
        StaticClient(
            client_id="opencode-my-data-hub",
            redirect_uris=("http://127.0.0.1:19876/mcp/oauth/callback",),
            allowed_scopes=frozenset({"openid", "offline_access", "provider:read"}),
        ),
        StaticClient(
            client_id="chatgpt-public",
            redirect_uris=("https://chatgpt.com/connector/oauth/callback-1",),
            allowed_scopes=frozenset({"openid", "offline_access", "provider:read"}),
        ),
    )
    settings = AuthorizationServerSettings(
        issuer=ISSUER,
        resource=RESOURCE,
        audience=RESOURCE,
        owner_subject="datahub-owner",
        clients=clients,
        signing_key_pem=pem,
        signing_key_id="key-1",
    )
    ledger = Ledger({client.client_id: client.allowed_scopes for client in clients})
    service = AuthorizationService(
        settings=settings,
        control_ledger=ledger,  # type: ignore[arg-type]
        grant_store=MemoryOAuthGrantStore(),
        clock=lambda: NOW,
    )
    authenticator = LocalOwnerTokenAuthenticator(
        issuer=ISSUER,
        authorization_url=f"{ISSUER}/authorize",
        login_url=f"{ISSUER}/owner/login",
        owner_subject="datahub-owner",
        operator_token=TOKEN,
        state_key=b"s" * 32,
        clock=lambda: NOW,
    )
    portal = LocalOwnerTokenPortal(authenticator=authenticator)
    return TestClient(
        create_authorization_app(
            service=service,
            owner_authenticator=authenticator,
            owner_login_portal=portal,
        ),
        base_url=ISSUER,
    )


def _params(client_id: str, redirect_uri: str) -> dict[str, str]:
    return {
        "response_type": "code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "resource": RESOURCE,
        "scope": "openid offline_access provider:read",
        "state": f"state-{client_id}",
        "nonce": f"nonce-{client_id}",
        "code_challenge": CHALLENGE,
        "code_challenge_method": "S256",
    }


def _login(client: TestClient, params: dict[str, str]) -> str:
    login = client.get("/authorize", params=params, follow_redirects=False)
    assert login.status_code == 200
    assert "location" not in login.headers
    assert login.headers["referrer-policy"] == "origin"
    assert (
        login.headers["content-security-policy"]
        == "default-src 'none'; style-src 'unsafe-inline'; "
        "form-action https://identity.example.test; base-uri 'none'; "
        "frame-ancestors 'none'"
    )
    assert '<form method="post" action="https://identity.example.test/owner/login"' in login.text
    assert "Операторский токен" in login.text
    assert TOKEN not in login.text
    import re

    sealed = re.search(r'name="owner_request" value="([^"]+)"', login.text)
    assert sealed is not None
    granted = client.post(
        "/owner/login",
        data={"owner_request": sealed.group(1), "operator_token": TOKEN},
        headers={"Origin": ISSUER},
        follow_redirects=False,
    )
    assert granted.status_code == 303
    assert granted.headers["location"].startswith(f"{ISSUER}/authorize?")
    assert "mdh_owner_session=" in granted.headers["set-cookie"]
    authorized = client.get(granted.headers["location"], follow_redirects=False)
    assert authorized.status_code == 303
    return authorized.headers["location"]


def test_local_owner_form_completes_opencode_loopback_pkce_authorization() -> None:
    client = _client()
    callback = _login(
        client,
        _params("opencode-my-data-hub", "http://127.0.0.1:19876/mcp/oauth/callback"),
    )
    assert callback.startswith("http://127.0.0.1:19876/mcp/oauth/callback?")
    assert parse_qs(urlsplit(callback).query)["state"] == ["state-opencode-my-data-hub"]


def test_same_local_owner_form_completes_chatgpt_public_callback() -> None:
    client = _client()
    callback = _login(
        client,
        _params("chatgpt-public", "https://chatgpt.com/connector/oauth/callback-1"),
    )
    assert callback.startswith("https://chatgpt.com/connector/oauth/callback-1?")
    assert parse_qs(urlsplit(callback).query)["state"] == ["state-chatgpt-public"]


def test_local_owner_form_completes_chatgpt_cimd_public_client() -> None:
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = private_key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    client_id = "https://chatgpt.com/connectors/my-data-hub/client.json"
    redirect_uri = "https://chatgpt.com/connector/oauth/my-data-hub-1"
    scopes = frozenset({"openid", "offline_access", "provider:read"})
    metadata = json.dumps(
        {
            "client_id": client_id,
            "redirect_uris": [redirect_uri],
            "response_types": ["code"],
            "grant_types": ["authorization_code", "refresh_token"],
            "token_endpoint_auth_method": "none",
        }
    ).encode()
    resolver = ChatGPTClientMetadataResolver(
        allowed_scopes=scopes,
        fetcher=lambda requested: ClientMetadataResponse(
            status=200,
            content_type="application/json",
            body=metadata,
            final_url=requested,
        ),
    )
    service = AuthorizationService(
        settings=AuthorizationServerSettings(
            issuer=ISSUER,
            resource=RESOURCE,
            audience=RESOURCE,
            owner_subject="datahub-owner",
            clients=(
                StaticClient(
                    client_id="opencode-my-data-hub",
                    redirect_uris=("http://127.0.0.1:19876/mcp/oauth/callback",),
                    allowed_scopes=scopes,
                ),
            ),
            signing_key_pem=pem,
            signing_key_id="key-1",
        ),
        control_ledger=Ledger({"opencode-my-data-hub": scopes}),  # type: ignore[arg-type]
        grant_store=MemoryOAuthGrantStore(),
        client_metadata_resolver=resolver,
        clock=lambda: NOW,
    )
    authenticator = LocalOwnerTokenAuthenticator(
        issuer=ISSUER,
        authorization_url=f"{ISSUER}/authorize",
        login_url=f"{ISSUER}/owner/login",
        owner_subject="datahub-owner",
        operator_token=TOKEN,
        state_key=b"c" * 32,
        clock=lambda: NOW,
    )
    client = TestClient(
        create_authorization_app(
            service=service,
            owner_authenticator=authenticator,
            owner_login_portal=LocalOwnerTokenPortal(authenticator),
        ),
        base_url=ISSUER,
    )
    callback = _login(client, _params(client_id, redirect_uri))
    assert callback.startswith(f"{redirect_uri}?")


def test_local_owner_rejects_wrong_token_and_tampered_request_without_cookie() -> None:
    client = _client()
    params = _params("opencode-my-data-hub", "http://127.0.0.1:19876/mcp/oauth/callback")
    login = client.get("/authorize", params=params, follow_redirects=False)
    assert login.status_code == 200
    import re

    sealed = re.search(r'name="owner_request" value="([^"]+)"', login.text)
    assert sealed is not None
    wrong = client.post(
        "/owner/login",
        data={"owner_request": sealed.group(1), "operator_token": "wrong"},
        follow_redirects=False,
    )
    assert wrong.status_code == 403
    assert TOKEN not in wrong.text
    tampered = client.post(
        "/owner/login",
        data={"owner_request": sealed.group(1) + "x", "operator_token": TOKEN},
        follow_redirects=False,
    )
    assert tampered.status_code == 403
    assert "mdh_owner_session=" not in tampered.headers.get("set-cookie", "")

    foreign_origin = client.post(
        "/owner/login",
        data={"owner_request": sealed.group(1), "operator_token": TOKEN},
        headers={"Origin": "https://mcp.example.test"},
        follow_redirects=False,
    )
    assert foreign_origin.status_code == 403
    assert foreign_origin.json() == {"error": "origin_not_allowed"}
