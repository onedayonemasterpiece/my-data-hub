from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlsplit, urlunsplit


def _exact_https_url(value: str, label: str) -> str:
    parsed = urlsplit(value)
    if (
        parsed.scheme != "https"
        or not parsed.netloc
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(f"{label} must be an exact HTTPS URL without credentials, query or fragment")
    return value.rstrip("/") if parsed.path in {"", "/"} else value


def protected_resource_metadata_url(resource: str) -> str:
    parsed = urlsplit(_exact_https_url(resource, "OAuth resource"))
    path = parsed.path.rstrip("/")
    return urlunsplit(
        (parsed.scheme, parsed.netloc, f"/.well-known/oauth-protected-resource{path}", "", "")
    )


@dataclass(frozen=True, slots=True)
class OAuthProviderMetadata:
    """Validated OAuth 2.1 discovery surface consumed by MCP clients."""

    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str
    scopes_supported: frozenset[str]
    token_endpoint_auth_methods_supported: tuple[str, ...] = ("none",)
    code_challenge_methods_supported: tuple[str, ...] = ("S256",)
    client_id_metadata_document_supported: bool = True
    registration_endpoint: str | None = None

    def __post_init__(self) -> None:
        for name in ("issuer", "authorization_endpoint", "token_endpoint", "jwks_uri"):
            _exact_https_url(getattr(self, name), name)
        if self.registration_endpoint is not None:
            _exact_https_url(self.registration_endpoint, "registration_endpoint")
        if self.code_challenge_methods_supported != ("S256",):
            raise ValueError("OAuth 2.1 authorization-code clients must use PKCE S256 only")
        allowed_auth = {"none", "private_key_jwt", "client_secret_basic", "client_secret_post"}
        if not self.token_endpoint_auth_methods_supported or not set(
            self.token_endpoint_auth_methods_supported
        ).issubset(allowed_auth):
            raise ValueError("unsupported token endpoint authentication method")
        if not self.scopes_supported or any(not value or " " in value for value in self.scopes_supported):
            raise ValueError("OAuth scopes must be non-empty scope tokens")

    def document(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "issuer": self.issuer,
            "authorization_endpoint": self.authorization_endpoint,
            "token_endpoint": self.token_endpoint,
            "jwks_uri": self.jwks_uri,
            "response_types_supported": ["code"],
            "grant_types_supported": ["authorization_code", "refresh_token"],
            "code_challenge_methods_supported": ["S256"],
            "token_endpoint_auth_methods_supported": list(
                self.token_endpoint_auth_methods_supported
            ),
            "scopes_supported": sorted(self.scopes_supported),
            "client_id_metadata_document_supported": self.client_id_metadata_document_supported,
        }
        if self.registration_endpoint is not None:
            result["registration_endpoint"] = self.registration_endpoint
        return result


@dataclass(frozen=True, slots=True)
class ProtectedResourceMetadata:
    resource: str
    authorization_servers: tuple[str, ...]
    scopes_supported: frozenset[str]
    resource_name: str = "my-data-hub MCP"

    def __post_init__(self) -> None:
        _exact_https_url(self.resource, "OAuth resource")
        if not self.authorization_servers:
            raise ValueError("at least one authorization server is required")
        for issuer in self.authorization_servers:
            _exact_https_url(issuer, "authorization server")
        if not self.scopes_supported:
            raise ValueError("protected resource scopes must not be empty")

    def document(self) -> dict[str, Any]:
        return {
            "resource": self.resource,
            "authorization_servers": list(self.authorization_servers),
            "bearer_methods_supported": ["header"],
            "scopes_supported": sorted(self.scopes_supported),
            "resource_name": self.resource_name,
        }
