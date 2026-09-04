from __future__ import annotations

import os
import stat
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import httpx

from my_data_hub.auth.context import current_identity
from my_data_hub.mcp.oauth import AccessIdentity

SHOWCASE_GATEWAY_PATH = "/internal/mcp-showcase/invoke"
SHOWCASE_TOOLS = frozenset(
    {
        "showcase.list",
        "showcase.get_link",
        "showcase.get_source",
        "showcase.apply",
        "showcase.rebuild",
        "showcase.rotate_link",
        "showcase.create_view",
        "showcase.revoke_link",
    }
)
SHOWCASE_WRITE_TOOLS = frozenset(
    {
        "showcase.rebuild",
        "showcase.rotate_link",
        "showcase.create_view",
        "showcase.revoke_link",
    }
)


class ShowcaseGatewayError(RuntimeError):
    """Raised when the private showcase runtime cannot safely satisfy a call."""


@dataclass(frozen=True, slots=True)
class ShowcaseGatewaySettings:
    url: str
    token_file: Path
    timeout_seconds: float = 45.0

    @classmethod
    def from_env(cls) -> ShowcaseGatewaySettings:
        raw_url = os.getenv("MY_DATA_HUB_SHOWCASE_GATEWAY_URL", "").strip()
        raw_token_file = os.getenv("MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE", "").strip()
        if not raw_url:
            raise ShowcaseGatewayError("MY_DATA_HUB_SHOWCASE_GATEWAY_URL is required")
        if not raw_token_file:
            raise ShowcaseGatewayError("MY_DATA_HUB_SHOWCASE_GATEWAY_TOKEN_FILE is required")
        timeout_raw = os.getenv("MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS", "45")
        try:
            timeout = float(timeout_raw)
        except ValueError as exc:
            raise ShowcaseGatewayError("MY_DATA_HUB_SHOWCASE_GATEWAY_TIMEOUT_SECONDS must be numeric") from exc
        if not 1 <= timeout <= 180:
            raise ShowcaseGatewayError("showcase gateway timeout must be 1..180 seconds")
        settings = cls(
            url=_validate_gateway_url(raw_url),
            token_file=Path(raw_token_file).expanduser().resolve(),
            timeout_seconds=timeout,
        )
        _read_service_token(settings.token_file)
        return settings


def _validate_gateway_url(raw_url: str) -> str:
    parts = urlsplit(raw_url)
    if parts.scheme not in {"http", "https"} or not parts.hostname:
        raise ShowcaseGatewayError("showcase gateway URL must be an absolute HTTP URL")
    if parts.username or parts.password or parts.query or parts.fragment:
        raise ShowcaseGatewayError("showcase gateway URL must not contain credentials or query data")
    if parts.path.rstrip("/") != SHOWCASE_GATEWAY_PATH:
        raise ShowcaseGatewayError(f"showcase gateway URL path must be exactly {SHOWCASE_GATEWAY_PATH}")
    if parts.scheme == "http" and parts.hostname not in {
        "127.0.0.1",
        "localhost",
        "::1",
    }:
        raise ShowcaseGatewayError("plain HTTP showcase gateway is allowed only on loopback")
    return raw_url.rstrip("/")


def _read_service_token(path: Path) -> str:
    try:
        file_stat = path.stat()
    except OSError as exc:
        raise ShowcaseGatewayError("showcase gateway token file is unavailable") from exc
    if not stat.S_ISREG(file_stat.st_mode):
        raise ShowcaseGatewayError("showcase gateway token path must be a regular file")
    if stat.S_IMODE(file_stat.st_mode) & 0o077:
        raise ShowcaseGatewayError("showcase gateway token file must not be readable by group or others")
    try:
        token = path.read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise ShowcaseGatewayError("showcase gateway token cannot be read") from exc
    if not 32 <= len(token) <= 512 or any(ord(char) < 33 for char in token):
        raise ShowcaseGatewayError("showcase gateway token is malformed")
    return token


def _identity_document(identity: AccessIdentity) -> dict[str, Any]:
    return {
        "subject": identity.subject,
        "client_id": identity.client_id,
        "scopes": sorted(identity.scopes),
        "audience": identity.audience,
        "token_id": identity.token_id,
        "expires_at": identity.expires_at,
        "issuer": identity.issuer,
        "issued_at": identity.issued_at,
        "resource": identity.resource,
    }


class ShowcaseGatewayClient:
    """Synchronous exact-tool client used by the lightweight OAuth MCP edge.

    The client deliberately carries only a loopback service credential. GitHub,
    renderer and publisher credentials remain inside the dedicated showcase
    runtime process.
    """

    def __init__(
        self,
        settings: ShowcaseGatewaySettings,
        *,
        default_identity: AccessIdentity | None = None,
        identity_provider: Callable[[], AccessIdentity | None] = current_identity,
        transport: httpx.BaseTransport | None = None,
    ) -> None:
        self.settings = settings
        self.default_identity = default_identity
        self.identity_provider = identity_provider
        self._token = _read_service_token(settings.token_file)
        self._transport = transport

    @classmethod
    def from_env(
        cls,
        *,
        default_identity: AccessIdentity | None = None,
        identity_provider: Callable[[], AccessIdentity | None] = current_identity,
    ) -> ShowcaseGatewayClient:
        return cls(
            ShowcaseGatewaySettings.from_env(),
            default_identity=default_identity,
            identity_provider=identity_provider,
        )

    def _identity(self) -> AccessIdentity:
        identity = self.identity_provider() or self.default_identity
        if identity is None:
            raise ShowcaseGatewayError("showcase gateway requires an authenticated identity")
        return identity

    def _invoke(self, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any] | list[Any]:
        if tool not in SHOWCASE_TOOLS:
            raise ShowcaseGatewayError("unsupported showcase gateway tool")
        identity = self._identity()
        payload = {
            "tool": tool,
            "arguments": dict(arguments),
            "principal": _identity_document(identity),
        }
        headers = {
            "Authorization": f"Bearer {self._token}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        try:
            with httpx.Client(
                timeout=self.settings.timeout_seconds,
                transport=self._transport,
            ) as client:
                response = client.post(self.settings.url, headers=headers, json=payload)
        except httpx.HTTPError as exc:
            raise ShowcaseGatewayError("showcase runtime is unavailable") from exc
        try:
            document = response.json()
        except ValueError as exc:
            raise ShowcaseGatewayError("showcase runtime returned invalid JSON") from exc
        if response.status_code >= 400 or not isinstance(document, dict):
            code = document.get("code") if isinstance(document, dict) else None
            suffix = f" ({code})" if isinstance(code, str) and code else ""
            raise ShowcaseGatewayError(f"showcase runtime rejected the request{suffix}")
        result = document.get("result")
        if not isinstance(result, (dict, list)):
            raise ShowcaseGatewayError("showcase runtime returned an invalid result")
        return result

    def list_surfaces(self) -> dict[str, Any] | list[Any]:
        return self._invoke("showcase.list", {})

    def get_link(self, view_id: str) -> dict[str, Any] | list[Any]:
        return self._invoke("showcase.get_link", {"view_id": view_id})


    def get_source(self, view_id: str) -> dict[str, Any] | list[Any]:
        return self._invoke("showcase.get_source", {"view_id": view_id})

    def apply(self, view_id: str, *, expected_source_revision: str, view: dict[str, Any] | None, items: list[dict[str, Any]], dry_run: bool = True, publish: bool = False, idempotency_key: str) -> dict[str, Any] | list[Any]:
        return self._invoke("showcase.apply", {"view_id": view_id, "expected_source_revision": expected_source_revision, "view": view, "items": items, "dry_run": dry_run, "publish": publish, "idempotency_key": idempotency_key})

    def rebuild(
        self,
        view_id: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any] | list[Any]:
        return self._invoke(
            "showcase.rebuild",
            {"view_id": view_id, "idempotency_key": idempotency_key},
        )

    def rotate_link(
        self,
        view_id: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any] | list[Any]:
        return self._invoke(
            "showcase.rotate_link",
            {"view_id": view_id, "idempotency_key": idempotency_key},
        )

    def create_view(
        self,
        view_id: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any] | list[Any]:
        return self._invoke(
            "showcase.create_view",
            {"view_id": view_id, "idempotency_key": idempotency_key},
        )

    def revoke_link(
        self,
        view_id: str,
        *,
        idempotency_key: str,
    ) -> dict[str, Any] | list[Any]:
        return self._invoke(
            "showcase.revoke_link",
            {"view_id": view_id, "idempotency_key": idempotency_key},
        )
