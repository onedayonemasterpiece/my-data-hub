from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from jwt.algorithms import RSAAlgorithm


@dataclass(slots=True)
class JwtIssuer:
    """RS256 JWT adapter implemented with PyJWT and cryptography."""

    issuer: str
    audience: str
    resource: str
    private_key_pem: bytes = field(repr=False)
    key_id: str
    access_token_ttl_seconds: int
    _private_key: Any = field(init=False, repr=False)
    _public_jwk: dict[str, Any] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        try:
            private_key = load_pem_private_key(self.private_key_pem, password=None)
            public_jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
        except Exception as exc:
            raise ValueError("signing_key_pem must contain an unencrypted RSA private key") from exc
        if public_jwk.get("kty") != "RSA" or getattr(private_key, "key_size", 0) < 2048:
            raise ValueError("an RSA signing key of at least 2048 bits is required")
        public_jwk.update({"kid": self.key_id, "use": "sig", "alg": "RS256"})
        self._private_key = private_key
        self._public_jwk = public_jwk

    def jwks(self) -> dict[str, object]:
        return {"keys": [dict(self._public_jwk)]}

    def issue_access_token(
        self,
        *,
        subject: str,
        client_id: str,
        scopes: tuple[str, ...],
        token_id: str,
        now: int,
    ) -> tuple[str, int]:
        expires_at = now + self.access_token_ttl_seconds
        token = jwt.encode(
            {
                "iss": self.issuer,
                "sub": subject,
                "aud": self.audience,
                "resource": self.resource,
                "client_id": client_id,
                "scope": " ".join(scopes),
                "jti": token_id,
                "iat": now,
                "nbf": now,
                "exp": expires_at,
            },
            self._private_key,
            algorithm="RS256",
            headers={"kid": self.key_id, "typ": "at+jwt"},
        )
        return token, expires_at

    def issue_id_token(
        self,
        *,
        subject: str,
        client_id: str,
        nonce: str | None,
        authenticated_at: int,
        now: int,
    ) -> str:
        claims: dict[str, object] = {
            "iss": self.issuer,
            "sub": subject,
            "aud": client_id,
            "iat": now,
            "exp": now + self.access_token_ttl_seconds,
            "auth_time": authenticated_at,
        }
        if nonce is not None:
            claims["nonce"] = nonce
        return jwt.encode(
            claims,
            self._private_key,
            algorithm="RS256",
            headers={"kid": self.key_id, "typ": "JWT"},
        )
