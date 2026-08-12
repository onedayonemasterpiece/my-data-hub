from __future__ import annotations

import time
from dataclasses import dataclass

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric import rsa

from my_data_hub.auth.control import ControlLedgerRevocationStore, OAuthRevocationQuery
from my_data_hub.mcp.oauth import RevocationKey, TokenValidationError
from my_data_hub.mcp.oauth_jwt import JwksJwtDecoder

RESOURCE = "https://mcp-datahub.kenigevents.ru/mcp"


@dataclass
class _SigningKey:
    key: object


class _StaticJwksClient:
    def __init__(self, key: object) -> None:
        self.key = key

    def get_signing_key_from_jwt(self, _token: str) -> _SigningKey:
        return _SigningKey(self.key)


def _claims(now: int) -> dict[str, object]:
    return {
        "iss": "https://identity.example",
        "aud": RESOURCE,
        "resource": RESOURCE,
        "sub": "principal-1",
        "client_id": "client-1",
        "jti": "token-1",
        "iat": now - 10,
        "nbf": now - 10,
        "exp": now + 60,
        "scope": "hub:read",
    }


def test_jwks_decoder_verifies_signature_issuer_and_audience() -> None:
    now = int(time.time())
    private_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    decoder = JwksJwtDecoder(
        jwks_url="https://identity.example/.well-known/jwks.json",
        issuer="https://identity.example",
        audience=RESOURCE,
    )
    decoder._client = _StaticJwksClient(private_key.public_key())
    token = jwt.encode(_claims(now), private_key, algorithm="RS256")

    assert decoder(token)["jti"] == "token-1"

    wrong_audience = jwt.encode(
        {**_claims(now), "aud": "https://identity.example/wrong"},
        private_key,
        algorithm="RS256",
    )
    with pytest.raises(TokenValidationError):
        decoder(wrong_audience)

    other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    wrong_signature = jwt.encode(_claims(now), other_key, algorithm="RS256")
    with pytest.raises(TokenValidationError):
        decoder(wrong_signature)


def test_jwks_decoder_rejects_insecure_authority_and_symmetric_algorithm() -> None:
    with pytest.raises(ValueError, match="HTTPS"):
        JwksJwtDecoder(
            jwks_url="http://identity.example/jwks.json",
            issuer="https://identity.example",
            audience=RESOURCE,
        )
    with pytest.raises(ValueError, match="asymmetric"):
        JwksJwtDecoder(
            jwks_url="https://identity.example/jwks.json",
            issuer="https://identity.example",
            audience=RESOURCE,
            algorithms=("HS256",),
        )


def test_control_ledger_revocation_adapter_preserves_exact_identity() -> None:
    class Ledger:
        def is_revoked(self, query: OAuthRevocationQuery) -> bool:
            assert query == OAuthRevocationQuery(
                issuer="https://identity.example",
                token_id="token-1",
                client_id="client-1",
                subject="principal-1",
                issued_at=1,
            )
            return True

    store = ControlLedgerRevocationStore(Ledger())  # type: ignore[arg-type]
    assert store.is_revoked(
        RevocationKey(
            issuer="https://identity.example",
            token_id="token-1",
            client_id="client-1",
            subject="principal-1",
            issued_at=1,
        )
    ) is True
