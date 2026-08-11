#!/usr/bin/env python3
"""Connect through the public Streamable HTTP endpoint and prove read-only MCP."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from urllib.parse import urlsplit

READ_ONLY_TOOLS = {
    "platform.status",
    "master.status",
    "operation.get",
    "checkpoint.status",
    "embedding.coverage",
    "provider.resources.status",
    "bloggers.list",
    "bloggers.get",
    "bloggers.search",
    "bloggers.provenance",
    "bloggers.statistics",
    "data.query",
    "data.change.status",
}
FORBIDDEN_FRAGMENTS = ("write", "enqueue", "submit", "delete", "create", "update", "operator")


def _structured_status(result: object) -> dict[str, object]:
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    for block in getattr(result, "content", ()):
        text = getattr(block, "text", None)
        if not isinstance(text, str):
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError("remote platform.status has no structured JSON result")


async def verify(endpoint: str, token: str, expected_commit: str) -> dict[str, object]:
    import httpx2
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    timeout = httpx2.Timeout(15.0, connect=5.0)
    async with httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
        timeout=timeout,
    ) as client, streamable_http_client(endpoint, http_client=client) as streams:
        read_stream, write_stream = streams
        async with ClientSession(
            read_stream, write_stream, read_timeout_seconds=15
        ) as session:
            await session.initialize()
            catalog = await session.list_tools()
            names = {tool.name for tool in catalog.tools}
            forbidden = sorted(
                name
                for name in names
                if name not in READ_ONLY_TOOLS
                or any(fragment in name.lower() for fragment in FORBIDDEN_FRAGMENTS)
            )
            if forbidden or names != READ_ONLY_TOOLS:
                raise RuntimeError(
                    "remote MCP catalog is not the exact R1 read-only catalog: "
                    f"missing={sorted(READ_ONLY_TOOLS - names)}, forbidden={forbidden}"
                )
            result = await session.call_tool("platform.status", {})
            if result.isError:
                raise RuntimeError("remote platform.status returned an MCP error")
            status = _structured_status(result)
            observed_commit = status.get("deployed_commit")
            if observed_commit != expected_commit:
                raise RuntimeError("remote platform.status commit differs from the requested deployment")
            return {
                "ok": True,
                "endpoint": endpoint,
                "tools": sorted(names),
                "health_content_blocks": len(result.content),
                "deployed_commit": observed_commit,
                "writes_discoverable": False,
            }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--endpoint",
        default=os.getenv(
            "MY_DATA_HUB_MCP_CANARY_ENDPOINT",
            "https://mcp-datahub.kenigevents.ru/mcp",
        ),
    )
    parser.add_argument("--token", default=os.getenv("MY_DATA_HUB_MCP_CANARY_TOKEN", ""))
    parser.add_argument(
        "--expected-commit",
        default=os.getenv("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT", ""),
    )
    args = parser.parse_args()
    parsed = urlsplit(args.endpoint)
    if parsed.scheme != "https" or parsed.path != "/mcp" or parsed.query or parsed.fragment:
        parser.error("endpoint must be an exact HTTPS /mcp resource without query or fragment")
    if not args.token:
        parser.error("MY_DATA_HUB_MCP_CANARY_TOKEN or --token is required")
    if len(args.expected_commit) != 40 or any(
        character not in "0123456789abcdef" for character in args.expected_commit
    ):
        parser.error("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT must be an exact lowercase Git SHA")
    print(
        json.dumps(
            asyncio.run(verify(args.endpoint, args.token, args.expected_commit)),
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
