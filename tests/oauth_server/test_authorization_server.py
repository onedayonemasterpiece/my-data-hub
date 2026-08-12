from __future__ import annotations

import asyncio
import base64
import hashlib
import time
from dataclasses import dataclass, replace
from urllib.parse import parse_qs, urlsplit

import httpx
import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi.testclient import TestClient
from jwt.algorithms import RSAAlgorithm

from my_data_hub.auth.control import OAuthAuditEvent, OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.mcp.admission import AdmissionLimits
from my_data_hub.oauth_server import (
    AuthorizationServerSettings,
    AuthorizationService,
    MemoryOAuthGrantStore,
    OAuthHTTPPolicy,
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
        self.client_id = CLIENT_ID
        self.allowed_scopes = frozenset({"openid", "data:read", "data:write"})
        self.events: list[OAuthAuditEvent] = []

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return False

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        if issuer != ISSUER or client_id != self.client_id:
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
    "redirect_uri",
    [
        "http://localhost:19876/mcp/oauth/callback",
        "http://0.0.0.0:19876/mcp/oauth/callback",
        "http://127.0.0.2:19876/mcp/oauth/callback",
        "http://127.0.0.1/mcp/oauth/callback",
        "http://127.0.0.1:019876/mcp/oauth/callback",
        "http://user@127.0.0.1:19876/mcp/oauth/callback",
        "http://127.0.0.1:19876/mcp/oauth/callback#fragment",
    ],
)
def test_static_native_redirect_rejects_every_nonexact_loopback(redirect_uri: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        StaticClient(
            client_id="opencode-my-data-hub",
            redirect_uris=(redirect_uri,),
            allowed_scopes=frozenset({"openid"}),
        )


def test_opencode_loopback_public_client_uses_pkce_code_and_rotating_refresh(
    harness: Harness,
) -> None:
    client_id = "opencode-my-data-hub"
    redirect_uri = "http://127.0.0.1:19876/mcp/oauth/callback"
    requested_scopes = (
        "openid offline_access platform:read provider:read provider:write"
    )
    allowed_scopes = frozenset(requested_scopes.split())
    ledger = ControlLedger()
    ledger.client_id = client_id
    ledger.allowed_scopes = allowed_scopes
    service = AuthorizationService(
        settings=replace(
            harness.service.settings,
            clients=(
                StaticClient(
                    client_id=client_id,
                    redirect_uris=(redirect_uri,),
                    allowed_scopes=allowed_scopes,
                ),
            ),
        ),
        control_ledger=ledger,
        grant_store=MemoryOAuthGrantStore(),
        clock=lambda: NOW,
    )
    client = TestClient(
        create_authorization_app(service=service, owner_authenticator=Owner()),
        base_url=ISSUER,
    )
    authorization = client.get(
        "/authorize",
        params={
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "resource": RESOURCE,
            "scope": requested_scopes,
            "state": "opencode-state",
            "nonce": "opencode-nonce",
            "code_challenge": CHALLENGE,
            "code_challenge_method": "S256",
        },
        follow_redirects=False,
    )
    assert authorization.status_code == 303
    assert authorization.headers["location"].startswith(f"{redirect_uri}?")
    code = parse_qs(urlsplit(authorization.headers["location"]).query)["code"][0]
    token_response = client.post(
        "/token",
        data={
            "grant_type": "authorization_code",
            "code": code,
            "code_verifier": VERIFIER,
            "redirect_uri": redirect_uri,
            "client_id": client_id,
            "resource": RESOURCE,
        },
    )
    assert token_response.status_code == 200
    first_refresh = token_response.json()["refresh_token"]
    assert token_response.json()["scope"] == requested_scopes

    refreshed = client.post(
        "/token",
        data={
            "grant_type": "refresh_token",
            "refresh_token": first_refresh,
            "client_id": client_id,
            "resource": RESOURCE,
        },
    )
    assert refreshed.status_code == 200
    assert refreshed.json()["refresh_token"] != first_refresh


def test_jwks_rotation_publishes_bounded_overlap_but_signs_only_with_active_key(
    harness: Harness,
) -> None:
    retired_private = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    retired_public = RSAAlgorithm.to_jwk(retired_private.public_key(), as_dict=True)
    retired_public.update({"kid": "retired-key", "use": "sig", "alg": "RS256"})
    settings = replace(
        harness.service.settings,
        overlap_public_jwks=(retired_public,),
    )
    service = AuthorizationService(
        settings=settings,
        control_ledger=harness.ledger,
        grant_store=harness.store,
        clock=lambda: NOW,
    )
    jwks = service.jwt.jwks()["keys"]
    assert [key["kid"] for key in jwks] == ["key-1", "retired-key"]
    token, _ = service.jwt.issue_access_token(
        subject="owner-1",
        client_id=CLIENT_ID,
        scopes=("data:read",),
        token_id="token-rotation-test",
        now=NOW,
    )
    assert jwt.get_unverified_header(token)["kid"] == "key-1"
    assert all("d" not in key for key in jwks)

    with pytest.raises(ValueError, match="overlap"):
        AuthorizationService(
            settings=replace(settings, overlap_public_jwks=({**retired_public, "kid": "key-1"},)),
            control_ledger=harness.ledger,
            grant_store=harness.store,
        )
    private_jwk = RSAAlgorithm.to_jwk(retired_private, as_dict=True)
    private_jwk.update({"kid": "private-key", "use": "sig", "alg": "RS256"})
    with pytest.raises(ValueError, match="public"):
        AuthorizationService(
            settings=replace(settings, overlap_public_jwks=(private_jwk,)),
            control_ledger=harness.ledger,
            grant_store=harness.store,
        )


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
        options={"verify_exp": False},
    )
    assert claims["iat"] == NOW
    assert claims["exp"] == NOW + 120
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
        options={"verify_exp": False},
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
            options={"verify_exp": False},
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


def test_authenticated_non_owner_cannot_receive_a_code(harness: Harness) -> None:
    class NonOwner:
        def authenticate_owner(self, request: object, *, return_to: str) -> OwnerIdentity:
            return OwnerIdentity("not-the-configured-owner", NOW)

    app = create_authorization_app(service=harness.service, owner_authenticator=NonOwner())
    response = TestClient(app, base_url=ISSUER).get(
        "/authorize", params=harness.authorization_parameters(), follow_redirects=False
    )
    assert response.status_code == 401
    assert response.json() == {"error": "access_denied"}


def test_authorization_server_rejects_untrusted_host_and_origin(harness: Harness) -> None:
    wrong_host = harness.client.get(
        "/.well-known/oauth-authorization-server", headers={"host": "attacker.example"}
    )
    wrong_origin = harness.client.get(
        "/.well-known/oauth-authorization-server", headers={"origin": "https://attacker.example"}
    )
    assert wrong_host.status_code == 403
    assert wrong_host.json() == {"error": "host_not_allowed"}
    assert wrong_origin.status_code == 403
    assert wrong_origin.json() == {"error": "origin_not_allowed"}


def test_owner_login_return_to_is_rebuilt_from_validated_public_contract(harness: Harness) -> None:
    class Capture:
        return_to = ""

        def authenticate_owner(
            self, request: object, *, return_to: str
        ) -> OwnerAuthenticationChallenge:
            self.return_to = return_to
            return OwnerAuthenticationChallenge("https://login.example/passkey")

    owner = Capture()
    app = create_authorization_app(service=harness.service, owner_authenticator=owner)
    response = TestClient(app, base_url="http://identity.example").get(
        "/authorize",
        params={**harness.authorization_parameters(), "ignored_attacker_value": "https://attacker.example"},
        follow_redirects=False,
    )
    assert response.status_code == 303
    parsed = urlsplit(owner.return_to)
    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == f"{ISSUER}/authorize"
    assert "ignored_attacker_value" not in parse_qs(parsed.query)
    assert parse_qs(parsed.query)["redirect_uri"] == [REDIRECT_URI]


def test_request_body_and_authorization_query_are_bounded(harness: Harness) -> None:
    oversized_body = harness.client.post(
        "/token",
        content=b"x" * 16_385,
        headers={"content-type": "application/x-www-form-urlencoded"},
    )
    oversized_query = harness.client.get(
        "/authorize",
        params={**harness.authorization_parameters(), "padding": "x" * 8192},
        follow_redirects=False,
    )
    assert oversized_body.status_code == 413
    assert oversized_body.json() == {"error": "request_too_large"}
    assert oversized_query.status_code == 414
    assert oversized_query.json() == {"error": "invalid_request"}


def test_oauth_peer_rate_and_request_time_are_bounded(harness: Harness) -> None:
    rate_policy = OAuthHTTPPolicy(
        allowed_hosts=("identity.example",),
        allowed_origins=(ISSUER,),
        limits=AdmissionLimits(requests_per_window=1),
    )
    rate_app = create_authorization_app(
        service=harness.service,
        owner_authenticator=Owner(),
        http_policy=rate_policy,
    )
    rate_client = TestClient(rate_app, base_url=ISSUER)
    assert rate_client.get("/.well-known/oauth-authorization-server").status_code == 200
    limited = rate_client.get("/.well-known/oauth-authorization-server")
    assert limited.status_code == 429
    assert limited.json() == {"error": "rate_limited"}

    class SlowOwner:
        async def authenticate_owner(self, request: object, *, return_to: str) -> OwnerIdentity:
            await asyncio.sleep(0.2)
            return OwnerIdentity("owner-1", NOW)

    timeout_policy = OAuthHTTPPolicy(
        allowed_hosts=("identity.example",),
        allowed_origins=(ISSUER,),
        limits=AdmissionLimits(request_timeout_seconds=0.1),
    )
    timeout_app = create_authorization_app(
        service=harness.service,
        owner_authenticator=SlowOwner(),
        http_policy=timeout_policy,
    )
    timed_out = TestClient(timeout_app, base_url=ISSUER).get(
        "/authorize", params=harness.authorization_parameters(), follow_redirects=False
    )
    assert timed_out.status_code == 504
    assert timed_out.json() == {"error": "request_timeout"}

    class SlowLedger(ControlLedger):
        def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
            time.sleep(0.2)
            return super().get_client(issuer, client_id)

    ledger_timeout_service = AuthorizationService(
        settings=harness.service.settings,
        control_ledger=SlowLedger(),
        grant_store=MemoryOAuthGrantStore(),
        clock=lambda: NOW,
    )
    ledger_timeout_app = create_authorization_app(
        service=ledger_timeout_service,
        owner_authenticator=Owner(),
        http_policy=timeout_policy,
    )
    ledger_timed_out = TestClient(ledger_timeout_app, base_url=ISSUER).get(
        "/authorize", params=harness.authorization_parameters(), follow_redirects=False
    )
    assert ledger_timed_out.status_code == 504
    assert ledger_timed_out.json() == {"error": "request_timeout"}


def test_oauth_concurrency_queue_is_bounded(harness: Harness) -> None:
    async def scenario() -> tuple[httpx.Response, httpx.Response]:
        started = asyncio.Event()
        release = asyncio.Event()

        class BlockingOwner:
            async def authenticate_owner(
                self, request: object, *, return_to: str
            ) -> OwnerIdentity:
                started.set()
                await release.wait()
                return OwnerIdentity("owner-1", NOW)

        policy = OAuthHTTPPolicy(
            allowed_hosts=("identity.example",),
            allowed_origins=(ISSUER,),
            limits=AdmissionLimits(
                max_concurrency=1,
                queue_timeout_seconds=0.01,
                request_timeout_seconds=1,
            ),
        )
        app = create_authorization_app(
            service=harness.service,
            owner_authenticator=BlockingOwner(),
            http_policy=policy,
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url=ISSUER
        ) as client:
            first_task = asyncio.create_task(
                client.get("/authorize", params=harness.authorization_parameters())
            )
            await asyncio.wait_for(started.wait(), timeout=1)
            queued = await client.get("/authorize", params=harness.authorization_parameters())
            release.set()
            first = await first_task
            return first, queued

    first, queued = asyncio.run(scenario())
    assert first.status_code == 303
    assert queued.status_code == 503
    assert queued.json() == {"error": "server_busy"}
