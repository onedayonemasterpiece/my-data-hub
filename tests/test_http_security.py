from __future__ import annotations

import asyncio
from typing import Any

from my_data_hub.mcp.admission import (
    AdmissionLimits,
    HTTPAdmissionSecurity,
    normalize_origin,
    parse_host_header,
)
from my_data_hub.mcp.http_security import DevelopmentBearerSecurity


async def run_asgi(
    app,
    *,
    headers: list[tuple[bytes, bytes]],
    chunks: list[bytes] | None = None,
    method: str = "POST",
    path: str = "/mcp",
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
        "method": method,
        "path": path,
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


def test_oauth_metadata_path_is_public_and_challenge_advertises_it() -> None:
    metadata_path = "/.well-known/oauth-protected-resource/mcp"
    metadata_url = f"https://mcp.example{metadata_path}"

    def reject_auth(_header: str) -> None:
        raise AssertionError("public metadata must not invoke bearer authentication")

    app = HTTPAdmissionSecurity(
        echo_app,
        allowed_origins=(),
        allowed_hosts=("mcp.example",),
        authenticator=reject_auth,
        unauthenticated_paths=(metadata_path,),
        resource_metadata_url=metadata_url,
    )
    public = asyncio.run(
        run_asgi(
            app,
            headers=[(b"host", b"mcp.example")],
            method="GET",
            path=metadata_path,
        )
    )
    assert status_of(public) == 200

    protected = asyncio.run(
        run_asgi(app, headers=[(b"host", b"mcp.example")])
    )
    assert status_of(protected) == 401
    assert (
        f'resource_metadata="{metadata_url}"'.encode()
        in header_of(protected, b"www-authenticate")
    )


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


def header_of(messages: list[dict[str, Any]], name: bytes) -> bytes:
    start = next(message for message in messages if message["type"] == "http.response.start")
    return next(value for key, value in start["headers"] if key.lower() == name.lower())


def test_host_parser_handles_bracketed_ipv6_and_rejects_ambiguous_ipv6() -> None:
    assert parse_host_header("[2001:0db8::1]:8443") == ("2001:db8::1", 8443)
    assert normalize_origin("https://[2001:db8::1]:443") == "https://[2001:db8::1]"
    try:
        parse_host_header("2001:db8::1")
    except ValueError:
        pass
    else:  # pragma: no cover - explicit assertion without pytest dependency
        raise AssertionError("unbracketed IPv6 Host must be rejected")


def test_http_guard_accepts_allowed_ipv6_host() -> None:
    app = DevelopmentBearerSecurity(
        echo_app,
        token="secret",
        allowed_origins=(),
        allowed_hosts=("[::1]",),
    )
    messages = asyncio.run(
        run_asgi(app, headers=[(b"host", b"[::1]:8765"), (b"authorization", b"Bearer secret")])
    )
    assert status_of(messages) == 200


def test_duplicate_and_untrusted_forwarding_headers_are_rejected() -> None:
    duplicate = asyncio.run(
        run_asgi(
            guard(),
            headers=[
                (b"host", b"localhost"),
                (b"host", b"evil.test"),
                (b"authorization", b"Bearer secret"),
            ],
        )
    )
    assert status_of(duplicate) == 400
    forwarded = asyncio.run(
        run_asgi(
            guard(),
            headers=[
                (b"host", b"localhost"),
                (b"x-forwarded-host", b"localhost"),
                (b"authorization", b"Bearer secret"),
            ],
        )
    )
    assert status_of(forwarded) == 400


def test_security_headers_are_overwritten_and_correlation_is_server_generated() -> None:
    async def insecure_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        await receive()
        assert scope["state"]["correlation_id"] != "attacker"
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"cache-control", b"public"), (b"x-correlation-id", b"attacker")],
            }
        )
        await send({"type": "http.response.body", "body": b"ok"})

    app = DevelopmentBearerSecurity(
        insecure_app,
        token="secret",
        allowed_origins=(),
        allowed_hosts=("localhost",),
    )
    messages = asyncio.run(
        run_asgi(
            app,
            headers=[
                (b"host", b"localhost"),
                (b"authorization", b"Bearer secret"),
                (b"x-correlation-id", b"attacker"),
            ],
        )
    )
    assert header_of(messages, b"cache-control") == b"no-store"
    assert header_of(messages, b"x-correlation-id") != b"attacker"


def test_response_size_and_total_timeout_are_bounded() -> None:
    async def large_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"too-large"})

    large = DevelopmentBearerSecurity(
        large_app,
        token="secret",
        allowed_origins=(),
        allowed_hosts=("localhost",),
        max_response_bytes=3,
    )
    headers = [(b"host", b"localhost"), (b"authorization", b"Bearer secret")]
    assert status_of(asyncio.run(run_asgi(large, headers=headers))) == 502

    async def slow_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        await receive()
        await asyncio.sleep(0.2)
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"late"})

    slow = DevelopmentBearerSecurity(
        slow_app,
        token="secret",
        allowed_origins=(),
        allowed_hosts=("localhost",),
        request_timeout_seconds=0.1,
    )
    assert status_of(asyncio.run(run_asgi(slow, headers=headers))) == 504


def test_rate_limit_and_query_string_bearer_fail_closed() -> None:
    app = DevelopmentBearerSecurity(
        echo_app,
        token="secret",
        allowed_origins=(),
        allowed_hosts=("localhost",),
        requests_per_minute=1,
    )
    headers = [(b"host", b"localhost"), (b"authorization", b"Bearer secret")]

    async def exercise() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        return await run_asgi(app, headers=headers), await run_asgi(app, headers=headers)

    first, second = asyncio.run(exercise())
    assert status_of(first) == 200
    assert status_of(second) == 429

    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "path": "/mcp",
        "query_string": b"%61ccess_token=secret",
        "headers": headers,
    }
    fresh = DevelopmentBearerSecurity(
        echo_app, token="secret", allowed_origins=(), allowed_hosts=("localhost",)
    )
    asyncio.run(fresh(scope, receive, send))
    assert status_of(sent) == 400


def test_trusted_proxy_headers_are_validated_then_removed() -> None:
    async def inspect_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        names = {name for name, _ in scope["headers"]}
        assert b"x-forwarded-for" not in names
        assert scope["state"]["client_ip"] == "2001:db8::2"
        await receive()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    app = HTTPAdmissionSecurity(
        inspect_app,
        allowed_hosts=("mcp.example",),
        allowed_origins=(),
        trusted_proxy_ips=("127.0.0.1",),
        limits=AdmissionLimits(requests_per_window=10),
    )
    sent: list[dict[str, Any]] = []

    async def receive() -> dict[str, Any]:
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message: dict[str, Any]) -> None:
        sent.append(message)

    scope = {
        "type": "http",
        "client": ("127.0.0.1", 1234),
        "headers": [
            (b"host", b"private-upstream"),
            (b"x-forwarded-host", b"mcp.example"),
            (b"x-forwarded-proto", b"https"),
            (b"x-forwarded-for", b"2001:db8::2"),
        ],
    }
    asyncio.run(app(scope, receive, send))
    assert status_of(sent) == 200


def test_concurrency_queue_is_bounded() -> None:
    entered = asyncio.Event()
    release = asyncio.Event()

    async def blocking_app(scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        await receive()
        entered.set()
        await release.wait()
        await send({"type": "http.response.start", "status": 200, "headers": []})
        await send({"type": "http.response.body", "body": b"ok"})

    app = HTTPAdmissionSecurity(
        blocking_app,
        allowed_hosts=("localhost",),
        allowed_origins=(),
        limits=AdmissionLimits(max_concurrency=1, queue_timeout_seconds=0.01),
    )

    async def invoke() -> list[dict[str, Any]]:
        sent: list[dict[str, Any]] = []
        delivered = False

        async def receive() -> dict[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.disconnect"}

        async def send(message: dict[str, Any]) -> None:
            sent.append(message)

        await app({"type": "http", "headers": [(b"host", b"localhost")]}, receive, send)
        return sent

    async def exercise() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        first_task = asyncio.create_task(invoke())
        await entered.wait()
        second = await invoke()
        release.set()
        return await first_task, second

    first, second = asyncio.run(exercise())
    assert status_of(first) == 200
    assert status_of(second) == 503
