from __future__ import annotations

import json
from collections.abc import Awaitable, Callable
from typing import Any

from my_data_hub.mcp.catalog import DEFAULT_SECURITY_SCHEMES, TOOL_CONTRACTS

ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]


class ToolSecurityMetadataMiddleware:
    """Mirror OpenAI securitySchemes into the SDK's tools/list JSON.

    MCP Python SDK 2.0 preserves extension metadata under ``_meta`` but its
    generated Tool model does not yet serialize the OpenAI top-level mirror.
    This bounded JSON-only adapter adds both the list-level default and each
    tool's top-level field after the SDK has validated the core result.
    """

    def __init__(
        self,
        app: Any,
        *,
        security_schemes: list[dict[str, Any]] | None = None,
        max_response_bytes: int = 2_097_152,
    ) -> None:
        self.app = app
        self.security_schemes = security_schemes or DEFAULT_SECURITY_SCHEMES
        self.max_response_bytes = max_response_bytes

    async def __call__(self, scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        messages: list[dict[str, Any]] = []

        async def capture(message: dict[str, Any]) -> None:
            messages.append(dict(message))

        await self.app(scope, receive, capture)
        starts = [item for item in messages if item.get("type") == "http.response.start"]
        bodies = [item for item in messages if item.get("type") == "http.response.body"]
        if len(starts) != 1 or not bodies or any(item.get("more_body", False) for item in bodies):
            for message in messages:
                await send(message)
            return
        raw = b"".join(item.get("body", b"") for item in bodies)
        if len(raw) > self.max_response_bytes:
            raise RuntimeError("MCP response exceeds the security metadata bound")
        headers = starts[0].get("headers", [])
        content_type = next(
            (
                value.decode("latin-1").casefold()
                for key, value in headers
                if key.decode("latin-1").casefold() == "content-type"
            ),
            "",
        )
        if "application/json" not in content_type:
            for message in messages:
                await send(message)
            return
        try:
            payload = json.loads(raw)
            result = payload.get("result") if isinstance(payload, dict) else None
            tools = result.get("tools") if isinstance(result, dict) else None
            if not isinstance(tools, list):
                raise ValueError
            result["securitySchemes"] = self.security_schemes
            for tool in tools:
                if not isinstance(tool, dict) or tool.get("name") not in TOOL_CONTRACTS:
                    raise ValueError
                schemes = TOOL_CONTRACTS[tool["name"]].security_schemes()
                tool["securitySchemes"] = schemes
                meta = tool.setdefault("_meta", {})
                if not isinstance(meta, dict):
                    raise ValueError
                meta["securitySchemes"] = schemes
            encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode()
        except (TypeError, ValueError, json.JSONDecodeError):
            for message in messages:
                await send(message)
            return
        starts[0]["headers"] = [
            (key, value)
            for key, value in headers
            if key.decode("latin-1").casefold() != "content-length"
        ] + [(b"content-length", str(len(encoded)).encode("ascii"))]
        await send(starts[0])
        await send({"type": "http.response.body", "body": encoded})
