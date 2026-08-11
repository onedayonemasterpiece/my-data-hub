from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Mapping
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from my_data_hub.mcp.contracts import ControlPlaneReader
from my_data_hub.mcp.oauth import AccessIdentity


class ProviderControlGatewayError(RuntimeError):
    """The authenticated control authority did not return bounded metadata."""


_REMOTE_PROVIDER_TOOLS = frozenset(
    {
        "provider.resources.create",
        "provider.resources.version",
        "provider.resources.run",
        "provider.resources.read",
        "provider.resources.delete",
        "provider.acceptance.dataset.lifecycle",
        "provider.acceptance.notebook.lifecycle",
        "provider.acceptance.claim.get",
        "provider.acceptance.claim.cleanup",
    }
)


class AuthenticatedProviderControlClient(ControlPlaneReader):
    """Metadata response gateway; it owns no Kaggle adapter or credential."""

    def __init__(self, endpoint: str, service_token: bytes) -> None:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
            or parsed.path != "/internal/mcp-provider/invoke"
        ):
            raise ValueError("provider control gateway endpoint is not exact")
        if not 32 <= len(service_token) <= 256 or any(byte < 33 or byte > 126 for byte in service_token):
            raise ValueError("provider control gateway token violates the bounded contract")
        self.endpoint = endpoint
        self._service_token = service_token.decode("ascii")

    @classmethod
    def from_token_file(cls, endpoint: str, path: Path) -> AuthenticatedProviderControlClient:
        if not path.is_absolute() or path.is_symlink() or not path.is_file():
            raise ValueError("provider control gateway token must be an absolute regular file")
        if path.stat().st_mode & 0o077:
            raise ValueError("provider control gateway token file must be private")
        return cls(endpoint, path.read_bytes().strip())

    async def invoke_control(
        self, tool: str, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]:
        if tool not in _REMOTE_PROVIDER_TOOLS:
            raise PermissionError("control gateway accepts only exact provider operations")
        return await asyncio.to_thread(self._invoke_sync, tool, dict(arguments), principal)

    def _invoke_sync(
        self, tool: str, arguments: dict[str, Any], principal: AccessIdentity
    ) -> dict[str, Any]:
        body = json.dumps(
            {
                "tool": tool,
                "arguments": arguments,
                "principal": {
                    "subject": principal.subject,
                    "client_id": principal.client_id,
                    "scopes": sorted(principal.scopes),
                    "audience": principal.audience,
                    "expires_at": principal.expires_at,
                    "issuer": principal.issuer,
                    "issued_at": principal.issued_at,
                    "resource": principal.resource,
                },
            },
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode()
        if len(body) > 512 * 1024:
            raise ProviderControlGatewayError("provider control request exceeds the bound")
        request = Request(
            self.endpoint,
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._service_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
        )
        try:
            with urlopen(request, timeout=3660) as response:
                encoded = response.read(2 * 1024 * 1024 + 1)
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            raise ProviderControlGatewayError("provider control gateway request failed") from exc
        if len(encoded) > 2 * 1024 * 1024:
            raise ProviderControlGatewayError("provider control response exceeds the bound")
        try:
            result = json.loads(encoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ProviderControlGatewayError("provider control response is invalid") from exc
        if not isinstance(result, dict):
            raise ProviderControlGatewayError("provider control response is not an object")
        return result


class SplitControlPlaneReader(ControlPlaneReader):
    """Keep ledger reads local and send provider effects to the sole authority."""

    def __init__(self, local: ControlPlaneReader, provider: ControlPlaneReader) -> None:
        self.local = local
        self.provider = provider

    def invoke_control(
        self, tool: str, arguments: Mapping[str, Any], principal: AccessIdentity
    ) -> Mapping[str, Any] | Awaitable[Mapping[str, Any]]:
        target = self.provider if tool in _REMOTE_PROVIDER_TOOLS else self.local
        return target.invoke_control(tool, arguments, principal)
