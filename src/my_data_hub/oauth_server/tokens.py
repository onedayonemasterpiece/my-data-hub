from __future__ import annotations

from collections.abc import Mapping
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
    overlap_public_jwks: tuple[Mapping[str, object], ...] = ()
    _private_key: Any = field(init=False, repr=False)
    _public_jwk: dict[str, Any] = field(init=False, repr=False)
    _overlap_jwks: tuple[dict[str, Any], ...] = field(init=False, repr=False)

    def __post_init__(self) -> None:
        if (
            not self.key_id
            or len(self.key_id) > 128
            or not self.key_id.isascii()
            or any(char.isspace() or ord(char) < 0x21 for char in self.key_id)
        ):
            raise ValueError("active signing key id is invalid")
        try:
            private_key = load_pem_private_key(self.private_key_pem, password=None)
            public_jwk = RSAAlgorithm.to_jwk(private_key.public_key(), as_dict=True)
        except Exception as exc:
            raise ValueError("signing_key_pem must contain an unencrypted RSA private key") from exc
        if public_jwk.get("kty") != "RSA" or getattr(private_key, "key_size", 0) < 2048:
            raise ValueError("an RSA signing key of at least 2048 bits is required")
        public_jwk.update({"kid": self.key_id, "use": "sig", "alg": "RS256"})
        overlap: list[dict[str, Any]] = []
        seen_key_ids = {self.key_id}
        for raw in self.overlap_public_jwks:
            normalized = self._validated_overlap_jwk(raw)
            kid = normalized["kid"]
            if kid in seen_key_ids:
                raise ValueError("overlap public JWK key ids must be unique and differ from the active key")
            seen_key_ids.add(kid)
            overlap.append(normalized)
        self._private_key = private_key
        self._public_jwk = public_jwk
        self._overlap_jwks = tuple(overlap)

    @staticmethod
    def _validated_overlap_jwk(raw: Mapping[str, object]) -> dict[str, Any]:
        allowed = {"kty", "kid", "use", "alg", "n", "e", "key_ops"}
        required = {"kty", "kid", "use", "alg", "n", "e"}
        if not isinstance(raw, Mapping) or not required.issubset(raw) or set(raw) - allowed:
            raise ValueError("overlap keys must be bounded public RSA JWKs")
        if raw.get("kty") != "RSA" or raw.get("use") != "sig" or raw.get("alg") != "RS256":
            raise ValueError("overlap keys must be RS256 public signing JWKs")
        kid = raw.get("kid")
        if (
            not isinstance(kid, str)
            or not kid
            or len(kid) > 128
            or not kid.isascii()
            or any(char.isspace() or ord(char) < 0x21 for char in kid)
        ):
            raise ValueError("overlap public JWK key id is invalid")
        key_ops = raw.get("key_ops")
        if key_ops is not None and key_ops != ["verify"]:
            raise ValueError("overlap public JWK key_ops must contain only verify")
        try:
            public_key = RSAAlgorithm.from_jwk(dict(raw))
        except Exception as exc:
            raise ValueError("overlap public JWK is invalid") from exc
        if getattr(public_key, "key_size", 0) < 2048:
            raise ValueError("overlap public RSA JWK must be at least 2048 bits")
        normalized = RSAAlgorithm.to_jwk(public_key, as_dict=True)
        normalized.update({"kid": kid, "use": "sig", "alg": "RS256"})
        if key_ops is not None:
            normalized["key_ops"] = ["verify"]
        return normalized

    def jwks(self) -> dict[str, object]:
        return {
            "keys": [
                dict(self._public_jwk),
                *(dict(key) for key in self._overlap_jwks),
            ]
        }

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
