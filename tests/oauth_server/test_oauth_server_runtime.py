from __future__ import annotations

import json
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.oauth_server import runtime as runtime_module
from my_data_hub.oauth_server.owner_oidc import OIDCSessionOwnerAuthenticator
from my_data_hub.oauth_server.runtime import build_authorization_runtime


def test_production_oauth_runtime_uses_durable_ledger_and_external_owner_login(
    monkeypatch, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
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
    retired_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    retired_jwk = RSAAlgorithm.to_jwk(retired_key.public_key(), as_dict=True)
    retired_jwk.update({"kid": "retired-key", "use": "sig", "alg": "RS256"})
    overlap_path = tmp_path / "overlap-jwks.json"
    overlap_path.write_text(json.dumps({"keys": [retired_jwk]}), encoding="utf-8")
    ledger_path = tmp_path / "ledger" / "control.sqlite3"
    values = {
        "MY_DATA_HUB_CONTROL_LEDGER_PATH": str(ledger_path),
        "MY_DATA_HUB_OAUTH_ISSUER": "https://auth.example.test",
        "MY_DATA_HUB_OAUTH_OWNER_SUBJECT": "owner-1",
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE": str(key_path),
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_ID": "key-1",
        "MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE": str(overlap_path),
        "MY_DATA_HUB_MCP_OAUTH_RESOURCE": "https://mcp.example.test/mcp",
        "MY_DATA_HUB_MCP_OAUTH_AUDIENCE": "https://mcp.example.test/mcp",
        "MY_DATA_HUB_OAUTH_CLIENTS_JSON": json.dumps(
            [
                {
                    "client_id": "chatgpt-reader",
                    "redirect_uris": ["https://chatgpt.example.test/oauth/callback"],
                    "allowed_scopes": ["bloggers:read", "data:read"],
                }
            ]
        ),
        "MY_DATA_HUB_OWNER_OIDC_ISSUER": "https://login.example.test",
        "MY_DATA_HUB_OWNER_OIDC_AUDIENCE": "my-data-hub-owner",
        "MY_DATA_HUB_OWNER_OIDC_JWKS_URL": "https://login.example.test/.well-known/jwks.json",
        "MY_DATA_HUB_OWNER_LOGIN_URL": "https://login.example.test/start",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    runtime = build_authorization_runtime()
    ledger = ControlLedger(ledger_path)
    client_record = ledger.oauth_client(
        "https://auth.example.test", "chatgpt-reader"
    )
    assert client_record is not None and client_record["profile_kind"] == "reader"
    client = TestClient(runtime.app, base_url="https://auth.example.test")
    metadata = client.get("/.well-known/oauth-authorization-server")
    assert metadata.status_code == 200
    loopback_health = TestClient(runtime.app, base_url="http://127.0.0.1:8780").get(
        "/.well-known/oauth-authorization-server"
    )
    assert loopback_health.status_code == 200
    published_keys = client.get("/.well-known/jwks.json").json()["keys"]
    assert [key["kid"] for key in published_keys] == ["key-1", "retired-key"]
    authorization_parameters = {
        "response_type": "code",
        "client_id": "chatgpt-reader",
        "redirect_uri": "https://chatgpt.example.test/oauth/callback",
        "resource": "https://mcp.example.test/mcp",
        "scope": "bloggers:read data:read",
        "code_challenge": "A" * 43,
        "code_challenge_method": "S256",
        "state": "opaque-state",
    }
    response = client.get(
        "/authorize", params=authorization_parameters, follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith(
        "https://login.example.test/start?return_to="
    )
    ledger.register_oauth_client(
        issuer="https://auth.example.test",
        client_id="chatgpt-reader",
        principal_id="owner-1",
        allowed_scopes=frozenset({"bloggers:read", "data:read"}),
        profile_kind="reader",
        enabled=False,
    )
    restarted_runtime = build_authorization_runtime()
    restarted_client = ControlLedger(ledger_path).oauth_client(
        "https://auth.example.test", "chatgpt-reader"
    )
    assert restarted_client is not None and not restarted_client["enabled"]
    denied = TestClient(restarted_runtime.app, base_url="https://auth.example.test").get(
        "/authorize", params=authorization_parameters,
        follow_redirects=False,
    )
    assert denied.status_code == 401
    assert denied.json() == {"error": "invalid_client"}


def test_runtime_accepts_empty_overlap_jwks_for_initial_deployment(
    tmp_path: Path, monkeypatch
) -> None:  # type: ignore[no-untyped-def]
    overlap = tmp_path / "overlap.json"
    overlap.write_text('{"keys":[]}', encoding="utf-8")
    monkeypatch.setenv("MY_DATA_HUB_OAUTH_OVERLAP_JWKS_FILE", str(overlap))
    assert runtime_module._overlap_public_jwks() == ()


def test_external_provider_subject_maps_only_to_fixed_datahub_owner(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    authenticator = OIDCSessionOwnerAuthenticator(
        issuer="https://login.example.test",
        audience="owner-app",
        jwks_url="https://login.example.test/jwks",
        login_url="https://login.example.test/start",
        authorization_url="https://identity.example.test/authorize",
        owner_subject="datahub-owner",
        provider_subject="opaque-provider-subject",
    )

    class Keys:
        @staticmethod
        def get_signing_key_from_jwt(_token: str) -> object:
            return type("SigningKey", (), {"key": object()})()

    authenticator._client = Keys()
    monkeypatch.setattr(
        "jwt.decode",
        lambda *_args, **_kwargs: {
            "iss": "https://login.example.test",
            "sub": "opaque-provider-subject",
            "aud": "owner-app",
            "exp": 2_000_000_000,
            "iat": 1_900_000_000,
            "nbf": 1_900_000_000,
            "auth_time": 1_900_000_000,
        },
    )
    assert authenticator._verified_identity("provider-jwt") == ("datahub-owner", 1_900_000_000)


def test_runtime_registration_does_not_read_then_reenable_client(
    monkeypatch, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
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
    ledger_path = tmp_path / "ledger" / "control.sqlite3"
    for name, value in {
        "MY_DATA_HUB_CONTROL_LEDGER_PATH": str(ledger_path),
        "MY_DATA_HUB_OAUTH_ISSUER": "https://auth.example.test",
        "MY_DATA_HUB_OAUTH_OWNER_SUBJECT": "owner-1",
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_FILE": str(key_path),
        "MY_DATA_HUB_OAUTH_SIGNING_KEY_ID": "key-1",
        "MY_DATA_HUB_MCP_OAUTH_RESOURCE": "https://mcp.example.test/mcp",
        "MY_DATA_HUB_MCP_OAUTH_AUDIENCE": "https://mcp.example.test/mcp",
        "MY_DATA_HUB_OAUTH_CLIENTS_JSON": json.dumps(
            [{
                "client_id": "chatgpt-reader",
                "redirect_uris": ["https://chatgpt.example.test/oauth/callback"],
                "allowed_scopes": ["bloggers:read", "data:read"],
            }]
        ),
        "MY_DATA_HUB_OWNER_OIDC_ISSUER": "https://login.example.test",
        "MY_DATA_HUB_OWNER_OIDC_AUDIENCE": "my-data-hub-owner",
        "MY_DATA_HUB_OWNER_OIDC_JWKS_URL": "https://login.example.test/.well-known/jwks.json",
        "MY_DATA_HUB_OWNER_LOGIN_URL": "https://login.example.test/start",
    }.items():
        monkeypatch.setenv(name, value)

    ledger = ControlLedger(ledger_path)
    ledger.register_oauth_client(
        issuer="https://auth.example.test",
        client_id="chatgpt-reader",
        principal_id="owner-1",
        allowed_scopes=frozenset({"bloggers:read", "data:read"}),
        profile_kind="reader",
        enabled=False,
    )
    oauth_client = ControlLedger.oauth_client
    monkeypatch.setattr(
        ControlLedger,
        "oauth_client",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("startup read race")),
    )
    build_authorization_runtime()

    configured = oauth_client(
        ControlLedger(ledger_path),
        "https://auth.example.test", "chatgpt-reader"
    )
    assert configured is not None and configured["enabled"] is False
