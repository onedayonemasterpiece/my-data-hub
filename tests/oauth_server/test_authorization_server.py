from __future__ import annotations

import base64
import hashlib
import time
from dataclasses import dataclass
from urllib.parse import parse_qs, urlsplit

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from my_data_hub.auth.control import OAuthAuditEvent, OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.oauth_server import (
    AuthorizationServerSettings,
    AuthorizationService,
    MemoryOAuthGrantStore,
    OwnerAuthenticationChallenge,
    OwnerIdentity,
    StaticClient,
    create_authorization_app,
)

ISSUER = "https://identity.example"
RESOURCE = "https://mcp.example/mcp"
AUDIENCE = "https://mcp.example/mcp"
CLIENT_ID = "chatgpt-owner"
REDIRECT_URI = "https://chatgpt.example/oauth/callback"
VERIFIER = "A" * 43
CHALLENGE = base64.urlsafe_b64encode(hashlib.sha256(VERIFIER.encode()).digest()).rstrip(b"=").decode()
NOW = int(time.time())


class ControlLedger:
    def __init__(self) -> None:
        self.enabled = True
        self.allowed_scopes = frozenset({"openid", "data:read", "data:write"})
        self.events: list[OAuthAuditEvent] = []

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return False

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        if issuer != ISSUER or client_id != CLIENT_ID:
            return None
        return OAuthClientRecord(issuer, client_id, self.enabled, self.allowed_scopes)

    def record_oauth_audit(self, event: OAuthAuditEvent) -> None:
        self.events.append(event)


class Owner:
    def authenticate_owner(self, request: object, *, return_to: str) -> OwnerIdentity:
        assert return_to.startswith(f"{ISSUER}/authorize?")
        return OwnerIdentity("owner-1", NOW - 30)


@dataclass
class Harness:
    client: TestClient
    service: AuthorizationService
    ledger: ControlLedger
    store: MemoryOAuthGrantStore

    def authorization_parameters(self, **changes: str | None) -> dict[str, str]:
        result = {
            "response_type": "code",
            "client_id": CLIENT_ID,
            "redirect_uri": REDIRECT_URI,
            "resource": RESOURCE,
            "scope": "openid data:read",
            "state": "state-1",
            "nonce": "nonce-1",
            "code_challenge": CHALLENGE,
            "code_challenge_method": "S256",
        }
        for name, value in changes.items():
            if value is None:
                result.pop(name, None)
            else:
                result[name] = value
        return result

    def authorize(self, **changes: str | None) -> tuple[str, object]:
        response = self.client.get(
            "/authorize", params=self.authorization_parameters(**changes), follow_redirects=False
        )
        if response.status_code != 303:
            return "", response
        query = parse_qs(urlsplit(response.headers["location"]).query)
        return query["code"][0], response

    def exchange(self, code: str, **changes: str | None) -> object:
        data = {
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": VERIFIER,
            "redirect_uri": REDIRECT_URI,
            "client_id": CLIENT_ID,
            "resource": RESOURCE,
        }
        for name, value in changes.items():
            if value is None:
                data.pop(name, None)
            else:
                data[name] = value
        return self.client.post("/token", data=data)

    def initial_tokens(self) -> dict[str, object]:
        code, response = self.authorize()
        assert response.status_code == 303
        token_response = self.exchange(code)
        assert token_response.status_code == 200
        return token_response.json()


@pytest.fixture
def harness() -> Harness:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    pem = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    ledger = ControlLedger()
    store = MemoryOAuthGrantStore()
    settings = AuthorizationServerSettings(
        issuer=ISSUER,
        resource=RESOURCE,
        audience=AUDIENCE,
        clients=(
            StaticClient(
                client_id=CLIENT_ID,
                redirect_uris=(REDIRECT_URI,),
                allowed_scopes=frozenset({"openid", "data:read", "data:write"}),
            ),
        ),
        signing_key_pem=pem,
        signing_key_id="key-1",
        access_token_ttl_seconds=120,
    )
    service = AuthorizationService(settings=settings, control_ledger=ledger, grant_store=store, clock=lambda: NOW)
    app = create_authorization_app(service=service, owner_authenticator=Owner())
    return Harness(TestClient(app, base_url=ISSUER), service, ledger, store)


def test_rfc8414_oidc_discovery_and_public_jwks(harness: Harness) -> None:
    oauth = harness.client.get("/.well-known/oauth-authorization-server").json()
    oidc = harness.client.get("/.well-known/openid-configuration").json()
    jwks = harness.client.get("/.well-known/jwks.json").json()

    assert oauth["issuer"] == ISSUER
    assert oauth["grant_types_supported"] == ["authorization_code", "refresh_token"]
    assert oauth["code_challenge_methods_supported"] == ["S256"]
    assert oauth["token_endpoint_auth_methods_supported"] == ["none"]
    assert "registration_endpoint" not in oauth
    assert oidc["id_token_signing_alg_values_supported"] == ["RS256"]
    assert jwks["keys"][0]["kid"] == "key-1"
    assert jwks["keys"][0]["alg"] == "RS256"
    assert "d" not in jwks["keys"][0]


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"code_challenge": None}, "invalid_request"),
        ({"code_challenge": "A" * 42}, "invalid_request"),
        ({"code_challenge_method": None}, "invalid_request"),
        ({"code_challenge_method": "plain"}, "invalid_request"),
        ({"redirect_uri": None}, "invalid_request"),
        ({"redirect_uri": f"{REDIRECT_URI}/"}, "invalid_request"),
        ({"resource": None}, "invalid_request"),
        ({"resource": f"{RESOURCE}/"}, "invalid_target"),
        ({"audience": f"{AUDIENCE}/wrong"}, "invalid_target"),
        ({"scope": None}, "invalid_request"),
        ({"scope": "openid unknown"}, "invalid_scope"),
        ({"scope": "openid openid"}, "invalid_scope"),
        ({"scope": "openid", "nonce": None}, "invalid_request"),
    ],
)
def test_authorization_request_rejects_missing_or_inexact_security_values(
    harness: Harness, changes: dict[str, str | None], error: str
) -> None:
    code, response = harness.authorize(**changes)
    assert not code
    assert response.status_code == 400
    assert response.json() == {"error": error}
    assert response.headers["cache-control"] == "no-store"


def test_authorization_code_issues_exact_short_lived_access_and_id_tokens(harness: Harness) -> None:
    code, authorization = harness.authorize()
    assert authorization.status_code == 303
    assert parse_qs(urlsplit(authorization.headers["location"]).query)["state"] == ["state-1"]
    response = harness.exchange(code)
    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"
    payload = response.json()
    assert payload["token_type"] == "Bearer"
    assert payload["expires_in"] == 120
    assert payload["scope"] == "openid data:read"
    assert payload["refresh_token"]

    jwk = harness.service.jwt.jwks()["keys"][0]
    public_key = RSAAlgorithm.from_jwk(jwk)
    claims = jwt.decode(
        payload["access_token"],
        public_key,
        algorithms=["RS256"],
        audience=AUDIENCE,
        issuer=ISSUER,
    )
    assert claims["aud"] == AUDIENCE
    assert claims["resource"] == RESOURCE
    assert claims["scope"] == "openid data:read"
    assert claims["client_id"] == CLIENT_ID
    assert claims["exp"] - claims["iat"] == 120
    id_claims = jwt.decode(
        payload["id_token"],
        public_key,
        algorithms=["RS256"],
        audience=CLIENT_ID,
        issuer=ISSUER,
    )
    assert id_claims["sub"] == "owner-1"
    assert id_claims["nonce"] == "nonce-1"
    with pytest.raises(jwt.InvalidAudienceError):
        jwt.decode(
            payload["access_token"],
            public_key,
            algorithms=["RS256"],
            audience=f"{AUDIENCE}/wrong",
            issuer=ISSUER,
        )


def test_wrong_pkce_consumes_code_and_code_replay_is_denied(harness: Harness) -> None:
    code, _ = harness.authorize()
    wrong = harness.exchange(code, code_verifier="B" * 43)
    assert wrong.status_code == 400
    assert wrong.json() == {"error": "invalid_grant"}
    assert code not in wrong.text
    replay = harness.exchange(code)
    assert replay.status_code == 400
    assert replay.json() == {"error": "invalid_grant"}

    second_code, _ = harness.authorize()
    assert harness.exchange(second_code).status_code == 200
    assert harness.exchange(second_code).json() == {"error": "invalid_grant"}


@pytest.mark.parametrize(
    ("change", "value", "error"),
    [
        ("redirect_uri", f"{REDIRECT_URI}/", "invalid_grant"),
        ("resource", f"{RESOURCE}/", "invalid_target"),
        ("audience", f"{AUDIENCE}/wrong", "invalid_target"),
        ("client_id", f"{CLIENT_ID}-other", "invalid_client"),
    ],
)
def test_code_exchange_rejects_wrong_binding(
    harness: Harness, change: str, value: str, error: str
) -> None:
    code, _ = harness.authorize()
    response = harness.exchange(code, **{change: value})
    assert response.status_code in {400, 401}
    assert response.json() == {"error": error}


@pytest.mark.parametrize("missing", ["code_verifier", "redirect_uri", "client_id", "resource"])
def test_code_exchange_requires_every_security_binding(harness: Harness, missing: str) -> None:
    code, _ = harness.authorize()
    response = harness.exchange(code, **{missing: None})
    assert response.status_code == 400
    assert response.json() == {"error": "invalid_request"}


def test_missing_token_fields_duplicate_fields_and_secret_auth_fail_closed(harness: Harness) -> None:
    code, _ = harness.authorize()
    missing = harness.client.post(
        "/token",
        data={"grant_type": "authorization_code", "code": code, "client_id": CLIENT_ID},
    )
    assert missing.json() == {"error": "invalid_request"}
    duplicate = harness.client.post(
        "/token",
        content=f"grant_type=authorization_code&code={code}&code=duplicate",
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    assert duplicate.json() == {"error": "invalid_request"}
    basic = harness.client.post(
        "/token",
        data={"grant_type": "authorization_code"},
        headers={"authorization": "Basic dXNlcjpzZWNyZXQ="},
    )
    assert basic.status_code == 401
    assert basic.json() == {"error": "invalid_client"}
    assert "secret" not in basic.text


def test_refresh_rotates_once_and_replay_revokes_the_family(harness: Harness) -> None:
    initial = harness.initial_tokens()
    first = initial["refresh_token"]
    rotated = harness.client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": first,
            "client_id": CLIENT_ID,
            "resource": RESOURCE,
            "scope": "data:read",
        },
    )
    assert rotated.status_code == 200
    assert rotated.json()["scope"] == "data:read"
    second = rotated.json()["refresh_token"]
    assert second != first

    replay = harness.client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": first,
            "client_id": CLIENT_ID,
            "resource": RESOURCE,
        },
    )
    assert replay.json() == {"error": "invalid_grant"}
    family_member = harness.client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": second,
            "client_id": CLIENT_ID,
            "resource": RESOURCE,
        },
    )
    assert family_member.json() == {"error": "invalid_grant"}


@pytest.mark.parametrize(
    ("changes", "error"),
    [
        ({"resource": f"{RESOURCE}/"}, "invalid_target"),
        ({"audience": f"{AUDIENCE}/wrong"}, "invalid_target"),
        ({"scope": "admin"}, "invalid_scope"),
        ({"scope": "openid data:read data:write admin"}, "invalid_scope"),
        ({"scope": "data:write"}, "invalid_grant"),
        ({"client_id": f"{CLIENT_ID}-other"}, "invalid_client"),
    ],
)
def test_refresh_rejects_wrong_resource_or_scope(
    harness: Harness, changes: dict[str, str], error: str
) -> None:
    refresh_token = harness.initial_tokens()["refresh_token"]
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "resource": RESOURCE,
    }
    data.update(changes)
    response = harness.client.post("/token", data=data)
    assert response.json() == {"error": error}


@pytest.mark.parametrize("missing", ["refresh_token", "client_id", "resource"])
def test_refresh_requires_every_security_binding(harness: Harness, missing: str) -> None:
    refresh_token = harness.initial_tokens()["refresh_token"]
    data = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": CLIENT_ID,
        "resource": RESOURCE,
    }
    data.pop(missing)
    response = harness.client.post("/token", data=data)
    assert response.json() == {"error": "invalid_request"}


def test_refresh_revocation_makes_credential_unusable_without_disclosure(harness: Harness) -> None:
    refresh_token = harness.initial_tokens()["refresh_token"]
    revoked = harness.client.post(
        "/revoke", data={"token": refresh_token, "token_type_hint": "refresh_token", "client_id": CLIENT_ID}
    )
    assert revoked.status_code == 200
    response = harness.client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
            "client_id": CLIENT_ID,
            "resource": RESOURCE,
        },
    )
    assert response.json() == {"error": "invalid_grant"}
    unknown = harness.client.post("/revoke", data={"token": "x" * 43, "client_id": CLIENT_ID})
    assert unknown.status_code == 200
    assert unknown.text == ""


def test_control_ledger_client_enablement_is_required_at_every_gate(harness: Harness) -> None:
    harness.ledger.enabled = False
    _, authorization = harness.authorize()
    assert authorization.status_code == 401
    assert authorization.json() == {"error": "invalid_client"}


def test_owner_authentication_is_an_external_challenge_seam(harness: Harness) -> None:
    class Challenge:
        def authenticate_owner(
            self, request: object, *, return_to: str
        ) -> OwnerAuthenticationChallenge:
            return OwnerAuthenticationChallenge("https://login.example/passkey?flow=bootstrap")

    app = create_authorization_app(service=harness.service, owner_authenticator=Challenge())
    client = TestClient(app, base_url=ISSUER)
    response = client.get(
        "/authorize", params=harness.authorization_parameters(), follow_redirects=False
    )
    assert response.status_code == 303
    assert response.headers["location"].startswith("https://login.example/passkey")
    assert response.headers["cache-control"] == "no-store"
