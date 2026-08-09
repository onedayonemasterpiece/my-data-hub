from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from my_data_hub.mcp.http_security import DevelopmentBearerSecurity


async def run_asgi(
    app, *, headers: list[tuple[bytes, bytes]], chunks: list[bytes] | None = None
):  # type: ignore[no-untyped-def]
    chunks = chunks or [b""]
    messages = [
        {"type": "http.request", "body": chunk, "more_body": index < len(chunks) - 1}
        for index, chunk in enumerate(chunks)
    ]
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return messages.pop(0) if messages else {"type": "http.disconnect"}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "method": "POST",
        "path": "/mcp",
        "headers": headers,
    }
    await app(scope, receive, send)
    return sent


async def echo_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
    body = bytearray()
    while True:
        message = await receive()
        if message.get("type") != "http.request":
            break
        body.extend(message.get("body", b""))
        if not message.get("more_body"):
            break
    await send({"type": "http.response.start", "status": 200, "headers": []})
    await send({"type": "http.response.body", "body": bytes(body)})


def guard(max_request_bytes: int = 32) -> DevelopmentBearerSecurity:
    return DevelopmentBearerSecurity(
        echo_app,
        token="secret",
        allowed_origins=("http://localhost",),
        allowed_hosts=("localhost",),
        max_request_bytes=max_request_bytes,
    )


def status_of(messages: list[dict[str, Any]]) -> int:
    return next(message["status"] for message in messages if message["type"] == "http.response.start")


def test_http_guard_requires_bearer_token() -> None:
    messages = asyncio.run(run_asgi(guard(), headers=[(b"host", b"localhost")]))
    assert status_of(messages) == 401


def test_http_guard_validates_host_and_origin() -> None:
    headers = [
        (b"host", b"localhost"),
        (b"origin", b"https://evil.test"),
        (b"authorization", b"Bearer secret"),
    ]
    assert status_of(asyncio.run(run_asgi(guard(), headers=headers))) == 403


def test_http_guard_limits_chunked_body_and_adds_headers() -> None:
    headers = [(b"host", b"localhost"), (b"authorization", b"Bearer secret")]
    accepted = asyncio.run(run_asgi(guard(), headers=headers, chunks=[b"abc", b"def"]))
    assert status_of(accepted) == 200
    start = next(message for message in accepted if message["type"] == "http.response.start")
    names = {key for key, _ in start["headers"]}
    assert b"cache-control" in names
    assert b"x-content-type-options" in names

    rejected = asyncio.run(run_asgi(guard(5), headers=headers, chunks=[b"abc", b"def"]))
    assert status_of(rejected) == 413
