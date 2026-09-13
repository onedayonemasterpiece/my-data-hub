from __future__ import annotations

import base64
import hashlib
import json
import time
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import jwt
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient

from my_data_hub.auth.control import OAuthAuditEvent, OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.oauth_server import (
    AuthorizationServerSettings,
    AuthorizationService,
    MemoryOAuthGrantStore,
    OwnerIdentity,
    StaticClient,
    create_authorization_app,
)
from my_data_hub.oauth_server.runtime import build_authorization_runtime

ISSUER = "https://identity.example.test"
PRIMARY_RESOURCE = "https://mcp-datahub.example.test/mcp"
SECOND_RESOURCE = "https://mcp-dataset-loop.kenigevents.ru/mcp"
THIRD_RESOURCE = "https://unconfigured.example.test/mcp"
CLIENT_ID = "chatgpt-owner"
REDIRECT_URI = "https://chatgpt.example.test/oauth/callback"
VERIFIER = "A" * 43
CHALLENGE = base64.urlsafe_b64encode(
    hashlib.sha256(VERIFIER.encode("ascii")).digest()
).rstrip(b"=").decode("ascii")
NOW = int(time.time())


class ControlLedger:
    def __init__(self) -> None:
        self.allowed_scopes = frozenset({"openid", "data:read", "data:write"})

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return False

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        if issuer != ISSUER or client_id != CLIENT_ID:
            return None
        return OAuthClientRecord(issuer, client_id, True, self.allowed_scopes)

    def record_oauth_audit(self, event: OAuthAuditEvent) -> None:
        return None


class Owner:
    def authenticate_owner(self, request: object, *, return_to: str) -> OwnerIdentity:
        return OwnerIdentity("owner-1", NOW - 30)


def _settings() -> tuple[AuthorizationServerSettings, rsa.RSAPrivateKey]:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    return (
        AuthorizationServerSettings(
            issuer=ISSUER,
            resource=PRIMARY_RESOURCE,
            audience=PRIMARY_RESOURCE,
            owner_subject="owner-1",
            clients=(
                StaticClient(
                    client_id=CLIENT_ID,
                    redirect_uris=(REDIRECT_URI,),
                    allowed_scopes=frozenset({"openid", "data:read", "data:write"}),
                ),
            ),
            signing_key_pem=pem,
            signing_key_id="key-1",
            additional_resources=frozenset({SECOND_RESOURCE}),
            access_token_ttl_seconds=120,
        ),
        key,
    )


def _client() -> tuple[TestClient, rsa.RSAPrivateKey]:
    settings, key = _settings()
    service = AuthorizationService(
        settings=settings,
        control_ledger=ControlLedger(),
        grant_store=MemoryOAuthGrantStore(),
        clock=lambda: NOW,
    )
    app = create_authorization_app(service=service, owner_authenticator=Owner())
    return TestClient(app, base_url=ISSUER), key


def _authorize_and_exchange(client: TestClient, resource: str) -> dict[str, object]:
    response = client.get(
        "/authorize",
        params={
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "resource": resource,
            "scope": "openid data:read",
            "state": "state-1",
            "nonce": "nonce-1",
            "code_challenge": CHALLENGE,
            "code_challenge_method": "S256",
        },
        follow_redirects=False,
    )
    assert response.status_code == 303
    code = parse_qs(urlsplit(response.headers["location"]).query)["code"][0]
    token_response = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": VERIFIER,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "resource": resource,
        },
    )
    assert token_response.status_code == 200
    return token_response.json()


def _claims(token: str, key: rsa.RSAPrivateKey) -> dict[str, object]:
    return jwt.decode(
        token,
        key.public_key(),
        algorithms=["RS256"],
        options={"verify_aud": False, "verify_exp": False},
    )


def test_primary_and_dataset_loop_resources_are_bound_to_expected_claims() -> None:
    primary_client, primary_key = _client()
    primary = _authorize_and_exchange(primary_client, PRIMARY_RESOURCE)
    primary_claims = _claims(str(primary["access_token"]), primary_key)
    assert primary_claims["aud"] == PRIMARY_RESOURCE
    assert primary_claims["resource"] == PRIMARY_RESOURCE

    dataset_client, dataset_key = _client()
    dataset = _authorize_and_exchange(dataset_client, SECOND_RESOURCE)
    dataset_claims = _claims(str(dataset["access_token"]), dataset_key)
    assert dataset_claims["aud"] == SECOND_RESOURCE
    assert dataset_claims["resource"] == SECOND_RESOURCE


def test_unconfigured_resource_is_rejected() -> None:
    client, _key = _client()
    response = client.get(
        "/authorize",
        params={
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "resource": THIRD_RESOURCE,
            "scope": "openid data:read",
            "code_challenge": CHALLENGE,
            "code_challenge_method": "S256",
        },
        follow_redirects=False,
    )
    assert response.status_code == 400
    assert response.json() == {"error": "invalid_target"}


def test_refresh_token_remains_bound_to_dataset_loop_resource() -> None:
    wrong_client, _wrong_key = _client()
    wrong_initial = _authorize_and_exchange(wrong_client, SECOND_RESOURCE)
    wrong = wrong_client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": wrong_initial["refresh_token"],
            "client_id": CLIENT_ID,
            "resource": PRIMARY_RESOURCE,
        },
    )
    assert wrong.status_code == 400

    client, key = _client()
    initial = _authorize_and_exchange(client, SECOND_RESOURCE)
    refreshed = client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": initial["refresh_token"],
            "client_id": CLIENT_ID,
            "resource": SECOND_RESOURCE,
        },
    )
    assert refreshed.status_code == 200
    claims = _claims(str(refreshed.json()["access_token"]), key)
    assert claims["aud"] == SECOND_RESOURCE
    assert claims["resource"] == SECOND_RESOURCE


def test_runtime_metadata_exposes_only_explicit_dataset_loop_scopes(
    monkeypatch, tmp_path: Path
) -> None:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    key_path = tmp_path / "signing.pem"
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_path.chmod(0o600)

    values = {
        "MY_DATA_HUB_CONTROL_LEDGER_PATH": str(tmp_path / "control.sqlite3"),
        "MY_DATA_HUB_OAUTH_ISSUER": ISSUER,
        "MY_DATA_HUB_OAUTH_OWNER_SUBJECT": "owner-1",
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE": str(key_path),
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_ID": "key-1",
        "MY_DATA_HUB_MCP_OAUTH_RESOURCE": PRIMARY_RESOURCE,
        "MY_DATA_HUB_MCP_OAUTH_AUDIENCE": PRIMARY_RESOURCE,
        "MY_DATA_HUB_OAUTH_ADDITIONAL_RESOURCES": SECOND_RESOURCE,
        "MY_DATA_HUB_OAUTH_CLIENTS_JSON": json.dumps(
            [
                {
                    "client_id": "static-reader",
                    "redirect_uris": [REDIRECT_URI],
                    "allowed_scopes": ["platform:read"],
                }
            ]
        ),
        "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_ENABLED": "true",
        "MY_DATA_HUB_OAUTH_CHATGPT_CIMD_SCOPES": (
            "openid,offline_access,platform:read"
        ),
        "MY_DATA_HUB_OAUTH_CHATGPT_EXTRA_SCOPES": (
            "runs:read,runs:write,artifacts:read"
        ),
        "MY_DATA_HUB_OWNER_OIDC_ISSUER": "https://login.example.test",
        "MY_DATA_HUB_OWNER_OIDC_AUDIENCE": "owner-audience",
        "MY_DATA_HUB_OWNER_OIDC_JWKS_URL": "https://login.example.test/jwks",
        "MY_DATA_HUB_OWNER_LOGIN_URL": "https://login.example.test/start",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    runtime = build_authorization_runtime()
    metadata = TestClient(runtime.app, base_url=ISSUER).get(
        "/.well-known/oauth-authorization-server"
    )
    assert metadata.status_code == 200
    scopes = set(metadata.json()["scopes_supported"])
    assert {"runs:read", "runs:write", "artifacts:read"}.issubset(scopes)
    assert "admin:secret" not in scopes
