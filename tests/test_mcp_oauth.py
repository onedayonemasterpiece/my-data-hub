from __future__ import annotations

import asyncio
from dataclasses import replace
from typing import Any

import pytest

from my_data_hub.mcp.oauth import (
    OAuthBearerValidator,
    OAuthValidationPolicy,
    RevocationKey,
    TokenValidationError,
    validate_verified_claims,
)

NOW = 2_000_000_000
RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"


def policy() -> OAuthValidationPolicy:
    return OAuthValidationPolicy(
        issuer="https://identity.example",
        audience=RESOURCE,
        resource=RESOURCE,
        allowed_scopes=frozenset({"hub:read", "hub:write"}),
        clock_skew_seconds=0,
        max_token_lifetime_seconds=600,
    )


def claims(**changes: Any) -> dict[str, Any]:
    result: dict[str, Any] = {
        "iss": "https://identity.example",
        "aud": RESOURCE,
        "resource": RESOURCE,
        "sub": "principal-1",
        "client_id": "client-1",
        "jti": "token-1",
        "iat": NOW - 10,
        "nbf": NOW - 10,
        "exp": NOW + 100,
        "scope": "hub:read",
    }
    result.update(changes)
    return result


@pytest.mark.parametrize(
    ("change", "value"),
    [
        ("iss", "https://identity.example/near-match"),
        ("aud", "https://mcp-datahub.kenigevents.ru"),
        ("aud", [RESOURCE]),
        ("resource", f"{RESOURCE}/"),
        ("sub", ""),
        ("client_id", None),
        ("jti", ""),
    ],
)
def test_claim_identity_and_resource_values_are_exact(change: str, value: Any) -> None:
    with pytest.raises(TokenValidationError):
        validate_verified_claims(claims(**{change: value}), policy=policy(), now=NOW)


@pytest.mark.parametrize(
    "change",
    [
        {"exp": NOW},
        {"nbf": NOW + 1},
        {"iat": NOW + 1},
        {"iat": NOW - 700},
        {"exp": "2000000100"},
        {"nbf": False},
    ],
)
def test_numeric_dates_fail_closed(change: dict[str, Any]) -> None:
    with pytest.raises(TokenValidationError):
        validate_verified_claims(claims(**change), policy=policy(), now=NOW)


def test_scope_policy_rejects_unknown_missing_and_duplicate_scopes() -> None:
    with pytest.raises(TokenValidationError, match="invalid_scope"):
        validate_verified_claims(claims(scope="admin"), policy=policy(), now=NOW)
    with pytest.raises(TokenValidationError, match="insufficient_scope"):
        validate_verified_claims(
            claims(),
            policy=policy(),
            required_scopes=frozenset({"hub:write"}),
            now=NOW,
        )
    with pytest.raises(TokenValidationError):
        validate_verified_claims(claims(scope="hub:read hub:read"), policy=policy(), now=NOW)


def test_requested_resource_is_exact_even_when_token_audience_is_valid() -> None:
    with pytest.raises(TokenValidationError, match="invalid_target"):
        validate_verified_claims(
            claims(),
            policy=policy(),
            requested_resource=f"{RESOURCE}/",
            now=NOW,
        )


class RecordingRevocations:
    def __init__(self, result: bool = False, error: Exception | None = None) -> None:
        self.result = result
        self.error = error
        self.keys: list[RevocationKey] = []

    async def is_revoked(self, key: RevocationKey) -> bool:
        self.keys.append(key)
        if self.error:
            raise self.error
        return self.result


def test_bearer_validator_checks_revocation_after_verified_decode() -> None:
    store = RecordingRevocations()
    validator = OAuthBearerValidator(
        decoder=lambda token: claims(jti=f"jti:{token}"),
        policy=policy(),
        revocations=store,
        clock=lambda: NOW,
    )
    identity = asyncio.run(
        validator.validate_authorization_header(
            "Bearer signed-token",
            required_scopes=frozenset({"hub:read"}),
            requested_resource=RESOURCE,
        )
    )
    assert identity.subject == "principal-1"
    assert store.keys == [
        RevocationKey(
            issuer="https://identity.example",
            token_id="jti:signed-token",
            client_id="client-1",
            subject="principal-1",
            issued_at=NOW - 10,
        )
    ]


@pytest.mark.parametrize("store", [RecordingRevocations(True), RecordingRevocations(error=OSError("db down"))])
def test_revoked_or_unavailable_revocation_authority_denies_access(store: RecordingRevocations) -> None:
    validator = OAuthBearerValidator(
        decoder=lambda _token: claims(), policy=policy(), revocations=store, clock=lambda: NOW
    )
    with pytest.raises(TokenValidationError, match="invalid_token"):
        asyncio.run(validator.validate_authorization_header("Bearer signed-token"))


@pytest.mark.parametrize(
    "header",
    ["", "Basic signed-token", "Bearer", "Bearer  signed-token", "Bearer token extra", "Bearer bad token"],
)
def test_authorization_header_has_one_bearer_credential(header: str) -> None:
    validator = OAuthBearerValidator(
        decoder=lambda _token: claims(), policy=policy(), revocations=RecordingRevocations(), clock=lambda: NOW
    )
    with pytest.raises(TokenValidationError):
        asyncio.run(validator.validate_authorization_header(header))


def test_policy_configuration_is_bounded() -> None:
    with pytest.raises(ValueError):
        replace(policy(), allowed_scopes=frozenset())
    with pytest.raises(ValueError):
        replace(policy(), clock_skew_seconds=301)
