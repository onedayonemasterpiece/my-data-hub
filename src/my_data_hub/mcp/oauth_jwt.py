from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

from my_data_hub.mcp.oauth import TokenValidationError


@dataclass(slots=True)
class JwksJwtDecoder:
    """Cryptographically verify asymmetric JWTs against a bounded JWKS authority."""

    jwks_url: str
    issuer: str
    audience: str
    algorithms: Sequence[str] = ("RS256",)
    cache_lifespan_seconds: int = 300
    network_timeout_seconds: float = 5.0
    _client: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for name, value in (("issuer", self.issuer), ("JWKS", self.jwks_url)):
            parsed = urlsplit(value)
            if (
                parsed.scheme != "https"
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.query
                or parsed.fragment
            ):
                raise ValueError(f"OAuth {name} URL must use exact HTTPS")
        audience = urlsplit(self.audience)
        if audience.scheme != "https" or not audience.netloc or audience.query or audience.fragment:
            raise ValueError("OAuth audience must be an exact HTTPS resource URL")
        allowed = tuple(self.algorithms)
        if not allowed or any(value not in {"RS256", "RS384", "RS512", "ES256", "ES384"} for value in allowed):
            raise ValueError("OAuth JWT algorithms must be an explicit asymmetric allowlist")
        self.algorithms = allowed
        try:
            from jwt import PyJWKClient
        except ImportError as exc:  # pragma: no cover - packaging guard
            raise RuntimeError("PyJWT[crypto] is required for OAuth JWT validation") from exc
        self._client = PyJWKClient(
            self.jwks_url,
            cache_keys=True,
            lifespan=self.cache_lifespan_seconds,
            timeout=self.network_timeout_seconds,
        )

    def __call__(self, token: str) -> Mapping[str, Any]:
        try:
            import jwt

            signing_key = self._client.get_signing_key_from_jwt(token)
            claims = jwt.decode(
                token,
                signing_key.key,
                algorithms=list(self.algorithms),
                audience=self.audience,
                issuer=self.issuer,
                options={"require": ["iss", "sub", "aud", "exp", "iat", "nbf", "jti"]},
            )
        except Exception as exc:
            raise TokenValidationError("invalid_token") from exc
        if not isinstance(claims, dict):
            raise TokenValidationError("invalid_token")
        return claims
