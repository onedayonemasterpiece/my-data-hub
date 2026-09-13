from __future__ import annotations

import base64
import hashlib
import time
from urllib.parse import parse_qs, urlsplit

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from my_data_hub.oauth_server.app import create_authorization_app
from my_data_hub.oauth_server.client_metadata import ChatGPTClientMetadataResolver
from my_data_hub.oauth_server.models import AuthorizationServerSettings, StaticClient, valid_scope
from my_data_hub.oauth_server.service import AuthorizationService
from my_data_hub.oauth_server.stores import MemoryOAuthGrantStore

ISSUER = "https://identity.example"
PRIMARY_RESOURCE = "https://mcp.example/mcp"
PRIMARY_AUDIENCE = "https://mcp.example/mcp"
SECOND_RESOURCE = "https://mcp-dataset-loop.kenigevents.ru/mcp"
CLIENT_ID = "chatgpt-owner"
REDIRECT_URI = "https://chatgpt.example/oauth/callback"
VERIFIER = "A" * 43
CHALLENGE = base64.urlsafe_b64encode(hashlib.sha256(VERIFIER.encode()).digest()).rstrip(b"=").decode()
NOW = int(time.time())


class ControlLedger:
    def __init__(self) -> None:
        self.enabled = True
        self.client_id = CLIENT_ID
        self.allowed_scopes = frozenset({"openid", "data:read", "data:write"})

    def get_client(self, issuer: str, client_id: str):
        from my_data_hub.auth.control import OAuthClientRecord
        if issuer != ISSUER or client_id != self.client_id:
            return None
        return OAuthClientRecord(issuer, client_id, self.enabled, self.allowed_scopes)

    def record_oauth_audit(self, event):
        pass


class Owner:
    def authenticate_owner(self, request, *, return_to: str):
        from my_data_hub.oauth_server.models import OwnerIdentity
        return OwnerIdentity("owner-1", NOW - 30)


def make_settings(additional_resources=None):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    add = frozenset(additional_resources or ())
    return AuthorizationServerSettings(
        issuer=ISSUER,
        resource=PRIMARY_RESOURCE,
        audience=PRIMARY_AUDIENCE,
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
        additional_resources=add,
        access_token_ttl_seconds=120,
    ), key


def make_service(settings):
    ledger = ControlLedger()
    store = MemoryOAuthGrantStore()
    return AuthorizationService(settings=settings, control_ledger=ledger, grant_store=store, clock=lambda: NOW), ledger, store


def authorize_flow(service, client, resource):
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "resource": resource,
        "scope": "openid data:read",
        "state": "state-1",
        "nonce": "nonce-1",
        "code_challenge": CHALLENGE,
        "code_challenge_method": "S256",
    }
    resp = client.get("/authorize", params=params, follow_redirects=False)
    assert resp.status_code == 303
    code = parse_qs(urlsplit(resp.headers["location"]).query)["code"][0]
    token_resp = client.post("/token", data={
        "grant_type": "authorization_code",
        "code": code,
        "code_verifier": VERIFIER,
        "redirect_uri": REDIRECT_URI,
        "client_id": CLIENT_ID,
        "resource": resource,
    })
    return token_resp


def test_primary_resource_unchanged():
    settings, key = make_settings()
    service, _, _ = make_service(settings)
    app = create_authorization_app(service=service, owner_authenticator=Owner())
    client = TestClient(app, base_url=ISSUER)
    token_resp = authorize_flow(service, client, PRIMARY_RESOURCE)
    assert token_resp.status_code == 200
    payload = token_resp.json()
    claims = jwt.decode(payload["access_token"], RSAAlgorithm.from_jwk(RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)), algorithms=["RS256"], options={"verify_exp": False})
    assert claims["aud"] == PRIMARY_AUDIENCE
    assert claims["resource"] == PRIMARY_RESOURCE
    assert settings.audience_for_resource(PRIMARY_RESOURCE) == PRIMARY_AUDIENCE


def test_second_resource_accepted_and_token_claims():
    settings, key = make_settings(additional_resources=[SECOND_RESOURCE])
    service, _, _ = make_service(settings)
    app = create_authorization_app(service=service, owner_authenticator=Owner())
    client = TestClient(app, base_url=ISSUER)
    token_resp = authorize_flow(service, client, SECOND_RESOURCE)
    assert token_resp.status_code == 200
    payload = token_resp.json()
    claims = jwt.decode(payload["access_token"], RSAAlgorithm.from_jwk(RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)), algorithms=["RS256"], options={"verify_exp": False})
    assert claims["aud"] == SECOND_RESOURCE
    assert claims["resource"] == SECOND_RESOURCE
    assert settings.audience_for_resource(SECOND_RESOURCE) == SECOND_RESOURCE


def test_unconfigured_resource_rejected():
    settings, _ = make_settings(additional_resources=[SECOND_RESOURCE])
    service, _, _ = make_service(settings)
    app = create_authorization_app(service=service, owner_authenticator=Owner())
    client = TestClient(app, base_url=ISSUER)
    params = {
        "response_type": "code",
        "client_id": CLIENT_ID,
        "redirect_uri": REDIRECT_URI,
        "resource": "https://unconfigured.example/mcp",
        "scope": "openid data:read",
        "code_challenge": CHALLENGE,
        "code_challenge_method": "S256",
    }
    resp = client.get("/authorize", params=params, follow_redirects=False)
    assert resp.status_code == 400
    assert resp.json() == {"error": "invalid_target"}


def test_refresh_bound_to_second_resource():
    settings, key = make_settings(additional_resources=[SECOND_RESOURCE])
    service, _, _ = make_service(settings)
    app = create_authorization_app(service=service, owner_authenticator=Owner())
    client = TestClient(app, base_url=ISSUER)
    token_resp = authorize_flow(service, client, SECOND_RESOURCE)
    assert token_resp.status_code == 200
    refresh = token_resp.json()["refresh_token"]
    # refresh with correct resource
    refresh_resp = client.post("/token", data={
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": CLIENT_ID,
        "resource": SECOND_RESOURCE,
    })
    assert refresh_resp.status_code == 200
    claims = jwt.decode(refresh_resp.json()["access_token"], RSAAlgorithm.from_jwk(RSAAlgorithm.to_jwk(key.public_key(), as_dict=True)), algorithms=["RS256"], options={"verify_exp": False})
    assert claims["aud"] == SECOND_RESOURCE
    assert claims["resource"] == SECOND_RESOURCE
    # refresh with wrong resource must fail
    wrong_resp = client.post("/token", data={
        "grant_type": "refresh_token",
        "refresh_token": refresh,
        "client_id": CLIENT_ID,
        "resource": PRIMARY_RESOURCE,
    })
    assert wrong_resp.status_code == 400


def test_chatgpt_cimd_extra_scopes_allowed():
    base_scopes = frozenset({"openid", "data:read"})
    extra = frozenset({"runs:read", "runs:write", "artifacts:read"})
    for s in extra:
        assert valid_scope(s)
    settings = AuthorizationServerSettings(
        issuer=ISSUER,
        resource=PRIMARY_RESOURCE,
        audience=PRIMARY_AUDIENCE,
        owner_subject="owner-1",
        clients=(StaticClient(
            client_id=CLIENT_ID,
            redirect_uris=(REDIRECT_URI,),
            allowed_scopes=frozenset({"openid", "data:read"}),
        ),),
        signing_key_pem=rsa.generate_private_key(public_exponent=65537, key_size=2048).private_bytes(
            serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
        ),
        signing_key_id="key-1",
        additional_resources=frozenset(),
        access_token_ttl_seconds=120,
        chatgpt_extra_scopes=extra,
    )
    resolver = ChatGPTClientMetadataResolver.from_settings(settings)
    assert "runs:read" in resolver.allowed_scopes
    assert "runs:write" in resolver.allowed_scopes
    assert "artifacts:read" in resolver.allowed_scopes
    assert "admin:secret" not in resolver.allowed_scopes
    # valid_scope accepts it syntactically but not allowed
    assert valid_scope("admin:secret")
