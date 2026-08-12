from __future__ import annotations

import json
from typing import Any

import pytest

from my_data_hub.mcp.transport import ToolSecurityMetadataMiddleware


@pytest.mark.asyncio
async def test_tools_list_has_top_level_and_per_tool_security_schemes() -> None:
    original = {
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "tools": [
                {
                    "name": "bloggers.search",
                    "inputSchema": {"type": "object"},
                    "_meta": {},
                }
            ]
        },
    }

    async def app(_scope, _receive, send):  # type: ignore[no-untyped-def]
        body = json.dumps(original).encode()
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [
                    (b"content-type", b"application/json"),
                    (b"content-length", str(len(body)).encode()),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})

    response: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        response.append(message)

    await ToolSecurityMetadataMiddleware(app)(
        {"type": "http", "method": "POST", "path": "/mcp"}, receive, send
    )
    payload = json.loads(response[1]["body"])
    assert payload["result"]["securitySchemes"][0]["type"] == "oauth2"
    tool = payload["result"]["tools"][0]
    assert tool["securitySchemes"] == [{"type": "oauth2", "scopes": ["bloggers:read"]}]
    assert tool["_meta"]["securitySchemes"] == tool["securitySchemes"]
