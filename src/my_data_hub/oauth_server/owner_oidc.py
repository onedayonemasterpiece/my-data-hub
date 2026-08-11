"""External OIDC-session owner authentication for the authorization UI."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode, urlsplit, urlunsplit

from fastapi import Request

from .models import OwnerAuthenticationChallenge, OwnerIdentity


@dataclass(slots=True)
class OIDCSessionOwnerAuthenticator:
    """Verify a JWT session cookie issued by a separate owner login portal.

    This service never accepts or stores an owner password. The login portal is
    responsible for its own passkey/upstream-OIDC ceremony and for setting the
    named Secure/HttpOnly/SameSite cookie on the authorization-server origin.
    """

    issuer: str
    audience: str
    jwks_url: str
    login_url: str
    authorization_url: str
    owner_subject: str
    cookie_name: str = "mdh_owner_session"
    algorithms: tuple[str, ...] = ("RS256",)
    _client: Any = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for value in (self.issuer, self.jwks_url, self.login_url, self.authorization_url):
            parsed = urlsplit(value)
            if (
                parsed.scheme != "https"
                or not parsed.netloc
                or parsed.username is not None
                or parsed.password is not None
                or parsed.fragment
            ):
                raise ValueError("owner OIDC URLs must use exact HTTPS")
        authorization = urlsplit(self.authorization_url)
        if authorization.query or not authorization.path.endswith("/authorize"):
            raise ValueError("owner authorization return URL must be the exact public authorize endpoint")
        if not self.audience or not self.owner_subject:
            raise ValueError("owner OIDC audience and exact subject are required")
        if not self.cookie_name.replace("_", "").isalnum():
            raise ValueError("owner session cookie name is invalid")
        if not self.algorithms or any(
            value not in {"RS256", "RS384", "RS512", "ES256", "ES384"}
            for value in self.algorithms
        ):
            raise ValueError("owner OIDC algorithms must be an asymmetric allowlist")
        from jwt import PyJWKClient

        self._client = PyJWKClient(self.jwks_url, cache_keys=True, lifespan=300, timeout=5)

    async def authenticate_owner(
        self, request: Request, *, return_to: str
    ) -> OwnerIdentity | OwnerAuthenticationChallenge:
        token = request.cookies.get(self.cookie_name, "")
        if not token:
            return OwnerAuthenticationChallenge(self._challenge(return_to))
        try:
            subject, authenticated_at = await asyncio.to_thread(self._verified_identity, token)
        except Exception:
            # Invalid cookies never reach OAuth grant issuance. The external
            # login portal can clear/replace them during a new ceremony.
            return OwnerAuthenticationChallenge(self._challenge(return_to))
        return OwnerIdentity(subject=subject, authenticated_at=authenticated_at)

    def _verified_identity(self, token: str) -> tuple[str, int]:
        import jwt

        signing_key = self._client.get_signing_key_from_jwt(token)
        claims = jwt.decode(
            token,
            signing_key.key,
            algorithms=list(self.algorithms),
            audience=self.audience,
            issuer=self.issuer,
            options={"require": ["iss", "sub", "aud", "exp", "iat", "nbf", "auth_time"]},
        )
        subject = claims.get("sub")
        authenticated_at = claims.get("auth_time")
        if (
            subject != self.owner_subject
            or isinstance(authenticated_at, bool)
            or not isinstance(authenticated_at, int)
            or authenticated_at < 0
        ):
            raise ValueError("owner session identity differs from policy")
        return subject, authenticated_at

    def _challenge(self, return_to: str) -> str:
        target = urlsplit(return_to)
        expected = urlsplit(self.authorization_url)
        if (
            target.scheme != expected.scheme
            or target.netloc != expected.netloc
            or target.path != expected.path
            or not target.query
            or target.fragment
            or len(return_to) > 16_384
        ):
            raise ValueError("owner login return URL differs from the configured authorize endpoint")
        parsed = urlsplit(self.login_url)
        query = parsed.query
        encoded = urlencode({"return_to": return_to})
        return urlunsplit(
            (parsed.scheme, parsed.netloc, parsed.path, f"{query}&{encoded}" if query else encoded, "")
        )
