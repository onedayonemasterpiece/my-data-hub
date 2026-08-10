from __future__ import annotations

import asyncio
import inspect
import math
import time
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol, runtime_checkable


class TokenValidationError(PermissionError):
    """A deliberately non-secret OAuth bearer rejection."""

    def __init__(self, code: str = "invalid_token") -> None:
        super().__init__(code)
        self.code = code


class RevocationCheckError(RuntimeError):
    """The durable revocation authority could not produce an answer."""


@dataclass(frozen=True, slots=True)
class AccessIdentity:
    """Principal derived from a cryptographically verified access token."""

    subject: str
    client_id: str
    scopes: frozenset[str]
    audience: str
    token_id: str
    expires_at: int
    issuer: str
    issued_at: int
    resource: str


OAuthPrincipal = AccessIdentity


@dataclass(frozen=True, slots=True)
class RevocationKey:
    """Values a durable store needs to revoke a token, client, or principal."""

    issuer: str
    token_id: str
    client_id: str
    subject: str
    issued_at: int


@runtime_checkable
class RevocationStore(Protocol):
    """PostgreSQL-friendly revocation boundary.

    A production implementation should perform one bounded PostgreSQL lookup for
    token, client and principal revocations and return ``True`` if any applies.
    Store errors must raise rather than returning ``False``.  The interface is
    intentionally storage-neutral and contains no SQLite fallback.
    """

    def is_revoked(self, key: RevocationKey) -> bool | Awaitable[bool]: ...


@runtime_checkable
class VerifiedTokenDecoder(Protocol):
    """Cryptographic JWT/introspection adapter.

    Implementations must verify signature/algorithm/key use before returning
    claims.  This module then applies resource-server claim and revocation policy.
    """

    def __call__(self, token: str) -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]: ...


@dataclass(frozen=True, slots=True)
class OAuthValidationPolicy:
    issuer: str
    audience: str
    resource: str
    allowed_scopes: frozenset[str]
    clock_skew_seconds: int = 30
    max_token_lifetime_seconds: int = 3600

    def __post_init__(self) -> None:
        if not self.issuer or not self.audience or not self.resource:
            raise ValueError("OAuth issuer, audience and resource must be exact non-empty values")
        if not self.allowed_scopes:
            raise ValueError("OAuth allowed scopes must not be empty")
        if any(not _valid_scope(scope) for scope in self.allowed_scopes):
            raise ValueError("OAuth allowed scopes contain an invalid value")
        if not 0 <= self.clock_skew_seconds <= 300:
            raise ValueError("OAuth clock skew must be between 0 and 300 seconds")
        if not 1 <= self.max_token_lifetime_seconds <= 86_400:
            raise ValueError("OAuth maximum token lifetime must be between 1 second and 1 day")


def _valid_scope(value: str) -> bool:
    # RFC 6749 scope-token excludes controls, space, quote and backslash.
    return bool(value) and all(
        ord(char) == 0x21 or 0x23 <= ord(char) <= 0x5B or 0x5D <= ord(char) <= 0x7E
        for char in value
    )


def _required_string(claims: Mapping[str, Any], name: str) -> str:
    value = claims.get(name)
    if not isinstance(value, str) or not value or len(value) > 2048:
        raise TokenValidationError("invalid_token")
    return value


def _numeric_date(claims: Mapping[str, Any], name: str) -> int:
    value = claims.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise TokenValidationError("invalid_token")
    if not math.isfinite(value) or int(value) != value:
        raise TokenValidationError("invalid_token")
    return int(value)


def _scopes(claims: Mapping[str, Any]) -> frozenset[str]:
    raw = claims.get("scope")
    if isinstance(raw, str):
        values = raw.split()
    elif isinstance(raw, (list, tuple)) and all(isinstance(value, str) for value in raw):
        values = list(raw)
    else:
        raise TokenValidationError("invalid_token")
    if not values or len(values) != len(set(values)) or any(not _valid_scope(value) for value in values):
        raise TokenValidationError("invalid_token")
    return frozenset(values)


def validate_verified_claims(
    claims: Mapping[str, Any],
    *,
    policy: OAuthValidationPolicy,
    required_scopes: frozenset[str] = frozenset(),
    requested_resource: str | None = None,
    now: int | None = None,
) -> AccessIdentity:
    """Validate already signature-verified access-token claims, fail closed."""

    current = int(time.time()) if now is None else int(now)
    if requested_resource is not None and requested_resource != policy.resource:
        raise TokenValidationError("invalid_target")
    if claims.get("iss") != policy.issuer:
        raise TokenValidationError("invalid_token")
    # Multiple audiences make the token broader than this exact resource policy.
    if claims.get("aud") != policy.audience:
        raise TokenValidationError("invalid_token")
    token_resource = claims.get("resource")
    if token_resource is not None and token_resource != policy.resource:
        raise TokenValidationError("invalid_token")

    subject = _required_string(claims, "sub")
    client_id = _required_string(claims, "client_id")
    token_id = _required_string(claims, "jti")
    issued_at = _numeric_date(claims, "iat")
    not_before = _numeric_date(claims, "nbf")
    expires_at = _numeric_date(claims, "exp")
    skew = policy.clock_skew_seconds
    if issued_at > current + skew or not_before > current + skew or expires_at <= current - skew:
        raise TokenValidationError("invalid_token")
    if not_before > expires_at or issued_at > expires_at:
        raise TokenValidationError("invalid_token")
    if expires_at - issued_at > policy.max_token_lifetime_seconds:
        raise TokenValidationError("invalid_token")

    granted = _scopes(claims)
    if not granted.issubset(policy.allowed_scopes):
        raise TokenValidationError("invalid_scope")
    if not required_scopes.issubset(policy.allowed_scopes):
        raise TokenValidationError("invalid_scope")
    if not required_scopes.issubset(granted):
        raise TokenValidationError("insufficient_scope")
    return AccessIdentity(
        subject=subject,
        client_id=client_id,
        scopes=granted,
        audience=policy.audience,
        token_id=token_id,
        expires_at=expires_at,
        issuer=policy.issuer,
        issued_at=issued_at,
        resource=policy.resource,
    )


class OAuthBearerValidator:
    """Signature adapter + strict claims + durable revocation validation."""

    def __init__(
        self,
        *,
        decoder: VerifiedTokenDecoder,
        policy: OAuthValidationPolicy,
        revocations: RevocationStore,
        clock: Callable[[], float] = time.time,
    ) -> None:
        if decoder is None or revocations is None:
            raise ValueError("OAuth decoder and durable revocation store are required")
        self.decoder = decoder
        self.policy = policy
        self.revocations = revocations
        self.clock = clock

    async def validate_token(
        self,
        token: str,
        *,
        required_scopes: frozenset[str] = frozenset(),
        requested_resource: str | None = None,
    ) -> AccessIdentity:
        if not token or len(token) > 16_384 or any(char.isspace() or ord(char) < 0x21 for char in token):
            raise TokenValidationError("invalid_token")
        try:
            decoder_is_async = inspect.iscoroutinefunction(
                self.decoder
            ) or inspect.iscoroutinefunction(type(self.decoder).__call__)
            if decoder_is_async:
                claims = await self.decoder(token)
            else:
                # JWKS retrieval and signature verification are synchronous; keep
                # them outside the ASGI event loop so request_timeout can bound them.
                claims = await asyncio.to_thread(self.decoder, token)
        except TokenValidationError:
            raise
        except Exception as exc:
            raise TokenValidationError("invalid_token") from exc
        if not isinstance(claims, Mapping):
            raise TokenValidationError("invalid_token")
        identity = validate_verified_claims(
            claims,
            policy=self.policy,
            required_scopes=required_scopes,
            requested_resource=requested_resource or self.policy.resource,
            now=int(self.clock()),
        )
        key = RevocationKey(
            issuer=identity.issuer,
            token_id=identity.token_id,
            client_id=identity.client_id,
            subject=identity.subject,
            issued_at=identity.issued_at,
        )
        try:
            revocation_check = self.revocations.is_revoked
            if inspect.iscoroutinefunction(revocation_check):
                revoked = await revocation_check(key)
            else:
                # psycopg's synchronous connection/query must not stall admission.
                revoked = await asyncio.to_thread(revocation_check, key)
        except Exception as exc:
            # Availability of the revocation authority is part of authentication.
            raise TokenValidationError("invalid_token") from exc
        if not isinstance(revoked, bool) or revoked:
            raise TokenValidationError("invalid_token")
        return identity

    async def validate_authorization_header(
        self,
        header: str,
        *,
        required_scopes: frozenset[str] = frozenset(),
        requested_resource: str | None = None,
    ) -> AccessIdentity:
        if not isinstance(header, str):
            raise TokenValidationError("invalid_token")
        parts = header.split(" ")
        if len(parts) != 2 or parts[0].casefold() != "bearer" or not parts[1]:
            raise TokenValidationError("invalid_token")
        return await self.validate_token(
            parts[1],
            required_scopes=required_scopes,
            requested_resource=requested_resource,
        )

    def challenge(self, *, insufficient_scope: bool = False) -> str:
        error = "insufficient_scope" if insufficient_scope else "invalid_token"
        return f'Bearer realm="my-data-hub", error="{error}", resource="{self.policy.resource}"'
