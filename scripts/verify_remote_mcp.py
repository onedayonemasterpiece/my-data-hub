#!/usr/bin/env python3
"""Connect through the public Streamable HTTP endpoint and prove read-only MCP."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import time
from collections.abc import Awaitable, Callable
from urllib.parse import urlsplit

READ_ONLY_TOOLS = {
    "platform.status",
    "master.status",
    "operation.get",
    "checkpoint.status",
    "embedding.coverage",
    "embedding.production.capabilities",
    "provider.resources.status",
    "datasets.search",
    "datasets.inspect",
    "datasets.file.read",
    "research.list",
    "research.get",
    "notebooks.find",
    "notebooks.get",
    "runs.get",
    "runs.logs",
    "artifacts.list",
    "artifacts.read",
    "bloggers.list",
    "bloggers.get",
    "bloggers.search",
    "bloggers.provenance",
    "bloggers.statistics",
    "bloggers.migration.accounting",
    "region_talk.inventory",
    "region_talk.articles.list",
    "region_talk.articles.get",
    "region_talk.articles.search",
    "region_talk.posts.list",
    "region_talk.posts.get",
    "region_talk.posts.search",
    "region_talk.queue.list",
    "region_talk.queue.summary",
    "region_talk.pipeline.status",
    "data.query",
    "data.change.status",
}
FORBIDDEN_FRAGMENTS = ("write", "enqueue", "submit", "delete", "create", "update", "operator")
CANONICAL_MCP_HOST = "mcp-datahub.kenigevents.ru"


def _validate_endpoint(endpoint: str) -> str:
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "https"
        or parsed.hostname != CANONICAL_MCP_HOST
        or parsed.port not in (None, 443)
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path != "/mcp"
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "endpoint must be the canonical HTTPS mcp-datahub.kenigevents.ru/mcp resource"
        )
    return f"https://{CANONICAL_MCP_HOST}/mcp"


def _structured_status(result: object) -> dict[str, object]:
    structured = getattr(result, "structuredContent", None)
    if structured is None:
        structured = getattr(result, "structured_content", None)
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


def _result_is_error(result: object) -> bool:
    value = getattr(result, "isError", getattr(result, "is_error", False))
    return value is True


async def _call(session: object, tool: str, arguments: dict[str, object]) -> dict[str, object]:
    result = await session.call_tool(tool, arguments)  # type: ignore[attr-defined]
    if _result_is_error(result):
        raise RuntimeError(f"remote {tool} returned an MCP error")
    return _structured_status(result)


async def verify_acceptance_session(
    session: object,
    *,
    expected_commit: str,
    cold_start_timeout_seconds: float,
    poll_interval_seconds: float,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
) -> dict[str, object]:
    """Prove the read-only catalog, ABSENT health, cold ensure and ACTIVE read."""

    catalog = await session.list_tools()  # type: ignore[attr-defined]
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

    platform = await _call(session, "platform.status", {})
    if platform.get("deployed_commit") != expected_commit:
        raise RuntimeError("remote platform.status commit differs from the requested deployment")
    if (
        platform.get("control_plane_ready") is not True
        or platform.get("master_state") != "ABSENT"
        or platform.get("canonical_database_location") != "kaggle-master-only"
    ):
        raise RuntimeError("remote control status is not healthy with master=ABSENT")
    master = await _call(session, "master.status", {})
    if master.get("master_state") != "ABSENT" or master.get("instance_id") is not None:
        raise RuntimeError("remote master.status is not the exact ABSENT state")

    cold = await _call(
        session,
        "data.query",
        {
            "sql": "SELECT count(*) AS row_count FROM region_talk.bloggers_ru_v1",
            "parameters": [],
            "max_rows": 1,
            "max_bytes": 16384,
            "timeout_ms": 5000,
        },
    )
    operation_id = cold.get("operation_id")
    if (
        not isinstance(operation_id, str)
        or not operation_id
        or len(operation_id) > 255
        or cold.get("master_state") not in {"REQUESTED", "STARTING", "RESTORING", "REGISTERING"}
        or cold.get("terminal") is not False
    ):
        raise RuntimeError("cold data read did not return a durable non-terminal ensure receipt")

    deadline = clock() + cold_start_timeout_seconds
    terminal = {"FAILED", "FENCED", "CHECKPOINT_FAILED", "ORPHANED", "STOPPED"}
    operation_seen = False
    while True:
        operation = await _call(session, "operation.get", {"operation_id": operation_id})
        if operation.get("found") is True:
            operation_seen = True
        master = await _call(session, "master.status", {})
        state = master.get("master_state")
        if state == "ACTIVE":
            break
        if state in terminal or operation.get("state") in terminal:
            raise RuntimeError("cold ensure reached a terminal failure state")
        if clock() >= deadline:
            raise RuntimeError("cold ensure did not reach ACTIVE within the bounded wait")
        await sleep(poll_interval_seconds)
    if not operation_seen:
        raise RuntimeError("cold ensure operation was never visible in control status")
    if (
        not isinstance(master.get("master_epoch"), int)
        or int(master["master_epoch"]) < 1
        or not isinstance(master.get("instance_id"), str)
        or not master["instance_id"]
    ):
        raise RuntimeError("ACTIVE master status omitted its fenced identity")

    read = await _call(
        session,
        "data.query",
        {
            "sql": "SELECT count(*) AS row_count FROM region_talk.bloggers_ru_v1",
            "parameters": [],
            "max_rows": 1,
            "max_bytes": 16384,
            "timeout_ms": 5000,
        },
    )
    if (
        read.get("columns") != ["row_count"]
        or not isinstance(read.get("rows"), list)
        or len(read["rows"]) != 1
        or read.get("truncated") is not False
        or read.get("master_epoch") != master.get("master_epoch")
        or not isinstance(read.get("canonical_revision"), int)
        or int(read["canonical_revision"]) < 0
    ):
        raise RuntimeError("ACTIVE data read did not return one bounded fenced result")
    return {
        "tools": sorted(names),
        "deployed_commit": expected_commit,
        "initial_master_state": "ABSENT",
        "cold_operation_id": operation_id,
        "active_master_epoch": master["master_epoch"],
        "canonical_revision": read["canonical_revision"],
        "data_read_rows": 1,
        "writes_discoverable": False,
    }


async def verify_acceptance(
    endpoint: str,
    token: str,
    expected_commit: str,
    *,
    cold_start_timeout_seconds: float = 900,
    poll_interval_seconds: float = 5,
) -> dict[str, object]:
    import httpx2
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    endpoint = _validate_endpoint(endpoint)
    timeout = httpx2.Timeout(30.0, connect=5.0)
    async with httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
        timeout=timeout,
    ) as client, streamable_http_client(endpoint, http_client=client) as streams:
        read_stream, write_stream = streams
        async with ClientSession(
            read_stream, write_stream, read_timeout_seconds=30
        ) as session:
            await session.initialize()
            return await verify_acceptance_session(
                session,
                expected_commit=expected_commit,
                cold_start_timeout_seconds=cold_start_timeout_seconds,
                poll_interval_seconds=poll_interval_seconds,
            )


async def verify(endpoint: str, token: str, expected_commit: str) -> dict[str, object]:
    import httpx2
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    endpoint = _validate_endpoint(endpoint)
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
            if _result_is_error(result):
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
    try:
        args.endpoint = _validate_endpoint(args.endpoint)
    except ValueError as exc:
        parser.error(str(exc))
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
