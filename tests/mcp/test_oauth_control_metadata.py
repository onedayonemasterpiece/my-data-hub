from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from my_data_hub.auth.control import OAuthAuditEvent, OAuthClientRecord, OAuthRevocationQuery
from my_data_hub.auth.metadata import OAuthProviderMetadata, ProtectedResourceMetadata
from my_data_hub.mcp.oauth import OAuthBearerValidator, OAuthValidationPolicy, TokenValidationError

NOW = 2_000_000_000
RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"


def claims(**changes: Any) -> dict[str, Any]:
    result = {
        "iss": "https://identity.example",
        "aud": RESOURCE,
        "resource": RESOURCE,
        "sub": "datahub-owner",
        "client_id": "chatgpt-owner-operator",
        "jti": "token-1",
        "iat": NOW - 10,
        "nbf": NOW - 10,
        "exp": NOW + 100,
        "scope": "data:read",
    }
    result.update(changes)
    return result


class Ledger:
    def __init__(self) -> None:
        self.revoked = False
        self.enabled = True
        self.events: list[OAuthAuditEvent] = []

    async def is_revoked(self, query: OAuthRevocationQuery) -> bool:
        assert query.token_id == "token-1"
        return self.revoked

    async def get_client(self, issuer: str, client_id: str) -> OAuthClientRecord | None:
        if not self.enabled:
            return None
        return OAuthClientRecord(issuer, client_id, True, frozenset({"data:read"}))

    async def record_oauth_audit(self, event: OAuthAuditEvent) -> None:
        self.events.append(event)


def validator(ledger: Ledger) -> OAuthBearerValidator:
    return OAuthBearerValidator(
        decoder=lambda _token: claims(),
        policy=OAuthValidationPolicy(
            issuer="https://identity.example",
            audience=RESOURCE,
            resource=RESOURCE,
            allowed_scopes=frozenset({"data:read", "data:write"}),
            clock_skew_seconds=0,
            max_token_lifetime_seconds=300,
        ),
        control_ledger=ledger,
        clock=lambda: NOW,
    )


def test_control_ledger_client_and_audit_are_part_of_authentication() -> None:
    ledger = Ledger()
    principal = asyncio.run(validator(ledger).validate_token("signed"))
    assert principal.client_id == "chatgpt-owner-operator"
    assert ledger.events == [
        OAuthAuditEvent(
            event="oauth_token",
            outcome="accepted",
            issuer="https://identity.example",
            client_id="chatgpt-owner-operator",
            subject="datahub-owner",
            token_id="token-1",
        )
    ]
    ledger.enabled = False
    with pytest.raises(TokenValidationError):
        asyncio.run(validator(ledger).validate_token("signed"))


def test_provider_metadata_requires_https_pkce_s256_and_pinned_jwks() -> None:
    metadata = OAuthProviderMetadata(
        issuer="https://identity.example",
        authorization_endpoint="https://identity.example/authorize",
        token_endpoint="https://identity.example/token",
        jwks_uri="https://identity.example/.well-known/jwks.json",
        scopes_supported=frozenset({"data:read"}),
    )
    assert metadata.document()["code_challenge_methods_supported"] == ["S256"]
    assert metadata.document()["jwks_uri"].startswith(metadata.issuer)
    with pytest.raises(ValueError, match="S256"):
        replace(metadata, code_challenge_methods_supported=("plain",))
    with pytest.raises(ValueError, match="HTTPS"):
        replace(metadata, jwks_uri="http://identity.example/jwks")


def test_protected_resource_metadata_is_exact_and_header_bearer_only() -> None:
    metadata = ProtectedResourceMetadata(
        resource=RESOURCE,
        authorization_servers=("https://identity.example",),
        scopes_supported=frozenset({"data:read"}),
    ).document()
    assert metadata["resource"] == RESOURCE
    assert metadata["bearer_methods_supported"] == ["header"]
