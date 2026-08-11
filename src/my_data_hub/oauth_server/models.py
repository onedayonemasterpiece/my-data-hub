from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from urllib.parse import parse_qsl, urlsplit

_PKCE_VALUE = re.compile(r"^[A-Za-z0-9._~-]{43,128}$")
_S256_CHALLENGE = re.compile(r"^[A-Za-z0-9_-]{43}$")


def valid_scope(value: str) -> bool:
    """Return whether *value* is one RFC 6749 scope-token."""

    return bool(value) and all(
        ord(char) == 0x21 or 0x23 <= ord(char) <= 0x5B or 0x5D <= ord(char) <= 0x7E
        for char in value
    )


def parse_scope(value: str) -> tuple[str, ...]:
    values = tuple(value.split(" "))
    if not value or any(not item or not valid_scope(item) for item in values):
        raise ValueError("invalid scope")
    if len(values) != len(set(values)):
        raise ValueError("duplicate scope")
    return values


def validate_pkce_value(value: str) -> bool:
    return bool(_PKCE_VALUE.fullmatch(value))


def validate_s256_challenge(value: str) -> bool:
    return bool(_S256_CHALLENGE.fullmatch(value))


def _exact_https_url(value: str, *, allow_query: bool = False, root_only: bool = False) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.fragment
        or (parsed.query and not allow_query)
        or (root_only and parsed.path not in {"", "/"})
    ):
        raise ValueError("OAuth URLs must be exact HTTPS URLs")
    return value.rstrip("/") if root_only else value


@dataclass(frozen=True, slots=True)
class StaticClient:
    client_id: str
    redirect_uris: tuple[str, ...]
    allowed_scopes: frozenset[str]

    def __post_init__(self) -> None:
        if not self.client_id or len(self.client_id) > 255:
            raise ValueError("client_id must be non-empty and bounded")
        if not self.redirect_uris or len(set(self.redirect_uris)) != len(self.redirect_uris):
            raise ValueError("redirect URI allowlist must be non-empty and unique")
        for redirect_uri in self.redirect_uris:
            _exact_https_url(redirect_uri, allow_query=True)
            reserved = {name for name, _ in parse_qsl(urlsplit(redirect_uri).query, keep_blank_values=True)}
            if reserved.intersection({"code", "state", "error", "error_description"}):
                raise ValueError("redirect URI query must not contain OAuth response parameters")
        if not self.allowed_scopes or any(not valid_scope(scope) for scope in self.allowed_scopes):
            raise ValueError("allowed scopes must contain valid scope-tokens")


@dataclass(frozen=True, slots=True)
class AuthorizationServerSettings:
    issuer: str
    resource: str
    audience: str
    owner_subject: str
    clients: tuple[StaticClient, ...]
    signing_key_pem: bytes
    signing_key_id: str
    overlap_public_jwks: tuple[Mapping[str, object], ...] = ()
    access_token_ttl_seconds: int = 300
    authorization_code_ttl_seconds: int = 180
    refresh_token_ttl_seconds: int = 2_592_000

    def __post_init__(self) -> None:
        object.__setattr__(self, "issuer", _exact_https_url(self.issuer, root_only=True))
        _exact_https_url(self.resource)
        _exact_https_url(self.audience)
        if not self.owner_subject or len(self.owner_subject) > 255:
            raise ValueError("one bounded owner subject is required")
        if not self.clients or len({client.client_id for client in self.clients}) != len(self.clients):
            raise ValueError("static clients must be non-empty and unique")
        if not self.signing_key_pem or not self.signing_key_id or len(self.signing_key_id) > 128:
            raise ValueError("an RSA signing key and bounded key id are required")
        if len(self.overlap_public_jwks) > 4:
            raise ValueError("at most four overlap public JWKs are supported")
        if not 30 <= self.access_token_ttl_seconds <= 600:
            raise ValueError("access tokens must live between 30 and 600 seconds")
        if not 30 <= self.authorization_code_ttl_seconds <= 300:
            raise ValueError("authorization codes must live between 30 and 300 seconds")
        if not 60 <= self.refresh_token_ttl_seconds <= 31_536_000:
            raise ValueError("refresh tokens must live between 1 minute and 1 year")

    @property
    def scopes_supported(self) -> frozenset[str]:
        return frozenset(scope for client in self.clients for scope in client.allowed_scopes)

    def client(self, client_id: str) -> StaticClient | None:
        return next((client for client in self.clients if client.client_id == client_id), None)


@dataclass(frozen=True, slots=True)
class OwnerIdentity:
    subject: str
    authenticated_at: int

    def __post_init__(self) -> None:
        if not self.subject or len(self.subject) > 255 or self.authenticated_at < 0:
            raise ValueError("invalid authenticated owner identity")


@dataclass(frozen=True, slots=True)
class OwnerAuthenticationChallenge:
    """A bootstrap authenticator-controlled redirect to an external login ceremony."""

    location: str

    def __post_init__(self) -> None:
        _exact_https_url(self.location, allow_query=True)
