from __future__ import annotations

import asyncio
import base64
import hashlib
import json
from dataclasses import dataclass

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa

from my_data_hub.auth.control import OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.mcp.oauth import OAuthBearerValidator, OAuthValidationPolicy
from my_data_hub.oauth_server import (
    AuthorizationServerSettings,
    AuthorizationService,
    ChatGPTClientMetadataResolver,
    ClientMetadataError,
    ClientMetadataResponse,
    MemoryOAuthGrantStore,
    OwnerIdentity,
    StaticClient,
)
from my_data_hub.oauth_server import client_metadata as cimd_module

ISSUER = "https://identity.example"
RESOURCE = "https://mcp.example/mcp"
CLIENT_ID = "https://chatgpt.com/oauth/my-data-hub/client.json"
REDIRECT_URI = "https://chatgpt.com/connector/oauth/callback-123"
SCOPES = frozenset(
    {"openid", "offline_access", "platform:read", "provider:read", "provider:write"}
)


def metadata(**changes: object) -> bytes:
    payload: dict[str, object] = {
        "client_id": CLIENT_ID,
        "redirect_uris": [REDIRECT_URI],
        "response_types": ["code"],
        "grant_types": ["authorization_code", "refresh_token"],
        # ChatGPT currently publishes private_key_jwt as its preferred method
        # while also declaring that it can operate as a public ``none`` client.
        # Our AS advertises only ``none`` and must select that common method.
        "token_endpoint_auth_method": "private_key_jwt",
        "token_endpoint_auth_methods_supported": ["none", "private_key_jwt"],
        "token_endpoint_auth_signing_alg": "RS256",
        "jwks_uri": "https://chatgpt.com/oauth/jwks.json",
    }
    payload.update(changes)
    return json.dumps(payload).encode()


def response(body: bytes | None = None, **changes: object) -> ClientMetadataResponse:
    values: dict[str, object] = {
        "status": 200,
        "content_type": "application/json",
        "body": metadata() if body is None else body,
        "final_url": CLIENT_ID,
    }
    values.update(changes)
    return ClientMetadataResponse(**values)  # type: ignore[arg-type]


def test_resolver_accepts_exact_chatgpt_public_client_and_caches_nonsecret_metadata() -> None:
    calls: list[str] = []
    resolver = ChatGPTClientMetadataResolver(
        allowed_scopes=SCOPES,
        fetcher=lambda client_id: calls.append(client_id) or response(),
        clock=lambda: 100.0,
    )

    first = resolver.resolve(CLIENT_ID)
    second = resolver.resolve(CLIENT_ID)

    assert first is second
    assert first.client_id == CLIENT_ID
    assert first.redirect_uris == (REDIRECT_URI,)
    assert first.allowed_scopes == SCOPES
    assert calls == [CLIENT_ID]
    assert not hasattr(first, "client_secret")


def test_resolver_accepts_current_live_chatgpt_cimd_method_negotiation() -> None:
    resolver = ChatGPTClientMetadataResolver(
        allowed_scopes=SCOPES,
        fetcher=lambda _client_id: response(),
    )

    client = resolver.resolve(CLIENT_ID)

    assert client.client_id == CLIENT_ID
    assert client.redirect_uris == (REDIRECT_URI,)


def test_resolver_never_caches_invalid_or_error_metadata() -> None:
    calls = 0

    def failing(_client_id: str) -> ClientMetadataResponse:
        nonlocal calls
        calls += 1
        return response(status=503)

    resolver = ChatGPTClientMetadataResolver(allowed_scopes=SCOPES, fetcher=failing)
    for _ in range(2):
        with pytest.raises(ClientMetadataError):
            resolver.resolve(CLIENT_ID)
    assert calls == 2


def test_default_fetch_is_time_and_size_bounded_and_does_not_follow_redirects(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    class Headers:
        @staticmethod
        def get(name: str) -> str | None:
            return None if name == "Content-Length" else "application/json"

        @staticmethod
        def get_content_type() -> str:
            return "application/json"

    class FetchResponse:
        status = 200
        headers = Headers()

        def __enter__(self):  # type: ignore[no-untyped-def]
            return self

        def __exit__(self, *_args: object) -> None:
            return None

        @staticmethod
        def geturl() -> str:
            return CLIENT_ID

        @staticmethod
        def read(limit: int) -> bytes:
            observed["read_limit"] = limit
            return metadata()

    class Opener:
        @staticmethod
        def open(request, *, timeout: float):  # type: ignore[no-untyped-def]
            observed["url"] = request.full_url
            observed["timeout"] = timeout
            return FetchResponse()

    monkeypatch.setattr(cimd_module, "build_opener", lambda handler: Opener())
    fetched = cimd_module._fetch(CLIENT_ID)
    assert fetched.status == 200
    assert observed == {
        "url": CLIENT_ID,
        "timeout": 3.0,
        "read_limit": 32 * 1024 + 1,
    }
    assert cimd_module._NoRedirects().redirect_request(None, None, 302, "", {}, "") is None


@pytest.mark.parametrize(
    "client_id",
    [
        "http://chatgpt.com/oauth/client.json",
        "https://evil.example/oauth/client.json",
        "https://chatgpt.com.evil.example/oauth/client.json",
        "https://user@chatgpt.com/oauth/client.json",
        "https://chatgpt.com:443/oauth/client.json",
        "https://chatgpt.com/oauth/client.json?tenant=x",
        "https://chatgpt.com/oauth/client.json#fragment",
        "https://chatgpt.com/oauth/../client.json",
        "https://chatgpt.com/",
    ],
)
def test_resolver_rejects_every_nonexact_client_identifier_before_fetch(client_id: str) -> None:
    resolver = ChatGPTClientMetadataResolver(
        allowed_scopes=SCOPES,
        fetcher=lambda _client_id: (_ for _ in ()).throw(AssertionError("must not fetch")),
    )
    with pytest.raises(ClientMetadataError):
        resolver.resolve(client_id)


@pytest.mark.parametrize(
    "fetched",
    [
        response(status=302),
        response(final_url="https://chatgpt.com/oauth/redirected/client.json"),
        response(content_type="text/html"),
        response(body=b""),
        response(body=b"{"),
        response(body=b'{"client_id": NaN}'),
        response(
            body=(
                b'{"client_id":"' + CLIENT_ID.encode() + b'","client_id":"duplicate"}'
            )
        ),
        response(body=b"x" * (32 * 1024 + 1)),
        response(body=metadata(client_id="https://chatgpt.com/oauth/other/client.json")),
        response(body=metadata(client_secret="forbidden")),
        response(body=metadata(token_endpoint_auth_method="client_secret_post")),
        response(body=metadata(token_endpoint_auth_methods_supported=["client_secret_post"])),
        response(body=metadata(token_endpoint_auth_methods_supported=[{}])),
        response(body=metadata(redirect_uris=["https://evil.example/callback"])),
        response(body=metadata(redirect_uris=["https://chatgpt.com/connector_platform_oauth_redirect"])),
        response(body=metadata(redirect_uris=[REDIRECT_URI, REDIRECT_URI + "-2"])),
        response(body=metadata(response_types=["token"])),
        response(body=metadata(grant_types=["implicit"])),
        response(body=metadata(grant_types=[{}])),
    ],
)
def test_resolver_rejects_redirects_secrets_oversize_and_malformed_metadata(
    fetched: ClientMetadataResponse,
) -> None:
    resolver = ChatGPTClientMetadataResolver(
        allowed_scopes=SCOPES,
        fetcher=lambda _client_id: fetched,
    )
    with pytest.raises(ClientMetadataError):
        resolver.resolve(CLIENT_ID)


class Ledger:
    def __init__(self) -> None:
        self.clients = {
            "static-client": OAuthClientRecord(
                ISSUER,
                "static-client",
                True,
                frozenset({"platform:read"}),
            )
        }

    def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        return False

    def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        return self.clients.get(client_id)

    def register_resolved_client(
        self,
        record: OAuthClientRecord,
        *,
        principal_id: str,
    ) -> OAuthClientRecord:
        assert principal_id == "owner"
        current = self.clients.get(record.client_id)
        persisted = OAuthClientRecord(
            record.issuer,
            record.client_id,
            True if current is None else current.enabled,
            record.allowed_scopes,
        )
        self.clients[record.client_id] = persisted
        return persisted

    def record_oauth_audit(self, event: object) -> None:
        return None


@dataclass
class ServiceHarness:
    service: AuthorizationService
    resolver_calls: list[str]


@pytest.fixture
def service_harness() -> ServiceHarness:
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    signing_key = key.private_bytes(
        serialization.Encoding.PEM,
        serialization.PrivateFormat.PKCS8,
        serialization.NoEncryption(),
    )
    calls: list[str] = []
    resolver = ChatGPTClientMetadataResolver(
        allowed_scopes=SCOPES,
        fetcher=lambda client_id: calls.append(client_id) or response(),
    )
    service = AuthorizationService(
        settings=AuthorizationServerSettings(
            issuer=ISSUER,
            resource=RESOURCE,
            audience=RESOURCE,
            owner_subject="owner",
            clients=(
                StaticClient(
                    client_id="static-client",
                    redirect_uris=("https://static.example/callback",),
                    allowed_scopes=frozenset({"platform:read"}),
                ),
            ),
            signing_key_pem=signing_key,
            signing_key_id="active",
        ),
        control_ledger=Ledger(),
        grant_store=MemoryOAuthGrantStore(),
        client_metadata_resolver=resolver,
        clock=lambda: 1_900_000_000,
    )
    return ServiceHarness(service, calls)


def test_discovery_advertises_cimd_none_pkce_without_dcr(service_harness: ServiceHarness) -> None:
    document = service_harness.service.authorization_server_metadata()
    assert document["client_id_metadata_document_supported"] is True
    assert document["token_endpoint_auth_methods_supported"] == ["none"]
    assert document["code_challenge_methods_supported"] == ["S256"]
    assert "registration_endpoint" not in document
    assert set(document["scopes_supported"]) >= SCOPES


def test_cimd_authorization_uses_exact_resource_redirect_scope_and_pkce(
    service_harness: ServiceHarness,
) -> None:
    verifier = "A" * 43
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    request = asyncio.run(
        service_harness.service.validate_authorization_request(
            {
                "response_type": "code",
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "resource": RESOURCE,
                "scope": "openid offline_access platform:read provider:read provider:write",
                "nonce": "nonce",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
    )
    assert request.client.client_id == CLIENT_ID
    assert request.redirect_uri == REDIRECT_URI
    assert request.resource == RESOURCE
    assert request.code_challenge == challenge
    assert service_harness.resolver_calls == [CLIENT_ID]


def test_current_chatgpt_code_flow_is_valid_without_nonce(
    service_harness: ServiceHarness,
) -> None:
    verifier = "A" * 43
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()

    request = asyncio.run(
        service_harness.service.validate_authorization_request(
            {
                "response_type": "code",
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "resource": RESOURCE,
                "scope": "openid offline_access platform:read provider:read provider:write",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
                "state": "oauth_s_exact",
                "ui_locales": "ru-RU",
            }
        )
    )

    assert request.nonce is None


def test_cimd_public_client_exchanges_pkce_code_and_rotates_refresh_token(
    service_harness: ServiceHarness,
) -> None:
    verifier = "A" * 43
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    request = asyncio.run(
        service_harness.service.validate_authorization_request(
            {
                "response_type": "code",
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "resource": RESOURCE,
                "scope": "openid offline_access provider:write",
                "nonce": "nonce",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
    )
    code = asyncio.run(
        service_harness.service.complete_authorization(
            request, OwnerIdentity("owner", 1_899_999_900)
        )
    )
    tokens = asyncio.run(
        service_harness.service.exchange_authorization_code(
            {
                "code": code,
                "code_verifier": verifier,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "resource": RESOURCE,
            }
        )
    )
    assert tokens["expires_in"] == 300
    assert tokens["refresh_token"]
    rotated = asyncio.run(
        service_harness.service.rotate_refresh_token(
            {
                "refresh_token": str(tokens["refresh_token"]),
                "client_id": CLIENT_ID,
                "resource": RESOURCE,
            }
        )
    )
    assert rotated["access_token"]
    assert rotated["refresh_token"] != tokens["refresh_token"]
    assert service_harness.resolver_calls == [CLIENT_ID]


def test_cimd_resolution_registers_client_for_resource_server_admission(
    service_harness: ServiceHarness,
) -> None:
    configured, record = asyncio.run(service_harness.service._enabled_client(CLIENT_ID))

    assert configured.client_id == CLIENT_ID
    assert record == OAuthClientRecord(ISSUER, CLIENT_ID, True, SCOPES)
    assert service_harness.service.control_ledger.get_client(ISSUER, CLIENT_ID) == record


def test_cimd_access_token_is_accepted_by_shared_resource_server_ledger(
    service_harness: ServiceHarness,
) -> None:
    verifier = "A" * 43
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest()).rstrip(b"=").decode()
    request = asyncio.run(
        service_harness.service.validate_authorization_request(
            {
                "response_type": "code",
                "client_id": CLIENT_ID,
                "redirect_uri": REDIRECT_URI,
                "resource": RESOURCE,
                "scope": "openid offline_access provider:read provider:write",
                "code_challenge": challenge,
                "code_challenge_method": "S256",
            }
        )
    )
    code = asyncio.run(
        service_harness.service.complete_authorization(
            request,
            OwnerIdentity("owner", 1_899_999_900),
        )
    )
    tokens = asyncio.run(
        service_harness.service.exchange_authorization_code(
            {
                "code": code,
                "code_verifier": verifier,
                "redirect_uri": REDIRECT_URI,
                "client_id": CLIENT_ID,
                "resource": RESOURCE,
            }
        )
    )
    validator = OAuthBearerValidator(
        decoder=lambda token: jwt.decode(
            token,
            options={"verify_signature": False, "verify_aud": False},
        ),
        policy=OAuthValidationPolicy(
            issuer=ISSUER,
            audience=RESOURCE,
            resource=RESOURCE,
            allowed_scopes=SCOPES,
            max_token_lifetime_seconds=300,
        ),
        control_ledger=service_harness.service.control_ledger,
        clock=lambda: 1_900_000_000,
    )

    identity = asyncio.run(validator.validate_token(str(tokens["access_token"])))

    assert identity.client_id == CLIENT_ID
    assert identity.scopes == frozenset(
        {"openid", "offline_access", "provider:read", "provider:write"}
    )


def test_static_clients_remain_ledger_gated_and_do_not_fetch_cimd(
    service_harness: ServiceHarness,
) -> None:
    configured, record = asyncio.run(service_harness.service._enabled_client("static-client"))
    assert configured.client_id == "static-client"
    assert record.enabled is True
    assert service_harness.resolver_calls == []
