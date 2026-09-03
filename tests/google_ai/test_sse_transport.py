from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from my_data_hub.google_ai.http import (
    AiohttpBoundedSSERequester,
    BoundedHTTPError,
    IncrementalSSEParser,
    SSEEvent,
    StreamTimeouts,
)


def test_parser_handles_arbitrary_chunks_crlf_comments_and_multiline_data() -> None:
    parser = IncrementalSSEParser()
    chunks = [
        b": keepalive\r",
        b'\nevent: interaction.created\r\ndata: {"type":\r\n',
        b'data: "interaction.created"}\r\n\r\n',
        b"data: [DO",
        b"NE]\n\n",
    ]
    events = [event for chunk in chunks for event in parser.feed(chunk)]
    parser.finish()

    assert [(event.event, event.data) for event in events] == [
        ("interaction.created", '{"type":\n"interaction.created"}'),
    ]
    assert parser.done is True


def test_parser_rejects_malformed_unterminated_event() -> None:
    parser = IncrementalSSEParser()
    parser.feed(b"event: interaction.created\ndata: {}")
    with pytest.raises(BoundedHTTPError, match="malformed_sse"):
        parser.finish()


def test_done_rejects_trailing_data_in_same_or_later_chunk() -> None:
    parser = IncrementalSSEParser()
    with pytest.raises(BoundedHTTPError, match="malformed_sse"):
        parser.feed(b"data: [DONE]\n\ndata: trailing\n\n")

    parser = IncrementalSSEParser()
    assert parser.feed(b"data: [DONE]\n\n") == []
    with pytest.raises(BoundedHTTPError, match="malformed_sse"):
        parser.feed(b"data: trailing\n\n")


@asynccontextmanager
async def _server(handler: Callable[[web.Request], Awaitable[web.StreamResponse]]) -> AsyncIterator[str]:
    app = web.Application()
    app.router.add_post("/stream", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    assert site._server is not None
    port = site._server.sockets[0].getsockname()[1]
    try:
        yield f"http://127.0.0.1:{port}/stream"
    finally:
        await runner.cleanup()


async def _ignore(_event: SSEEvent) -> None:
    return None


async def _call(url: str, timeouts: StreamTimeouts, *, max_raw_bytes: int = 1024) -> None:
    await AiohttpBoundedSSERequester().request_sse(
        "POST",
        url,
        headers={"Accept": "text/event-stream"},
        json_body={"stream": True},
        timeouts=timeouts,
        max_raw_bytes=max_raw_bytes,
        on_event=_ignore,
    )


@pytest.mark.asyncio
async def test_connect_first_event_idle_and_total_deadlines_are_distinct() -> None:
    entered = asyncio.Event()
    release_headers = asyncio.Event()

    async def before_headers(_request: web.Request) -> web.StreamResponse:
        entered.set()
        await release_headers.wait()
        return web.Response(
            body=b"event: ping\ndata: {}\n\ndata: [DONE]\n\n",
            headers={"Content-Type": "text/event-stream"},
        )

    async with _server(before_headers) as url:
        request = asyncio.create_task(
            _call(url, StreamTimeouts(0.01, 0.2, 0.2, 0.5))
        )
        await asyncio.wait_for(entered.wait(), timeout=0.2)
        await asyncio.sleep(0.03)
        release_headers.set()
        await request

    async def headers_after_first_budget(_request: web.Request) -> web.StreamResponse:
        await asyncio.sleep(0.05)
        return web.Response(
            body=b"event: ping\ndata: {}\n\n",
            headers={"Content-Type": "text/event-stream"},
        )

    async with _server(headers_after_first_budget) as url:
        with pytest.raises(BoundedHTTPError, match="first_event_timeout"):
            await _call(url, StreamTimeouts(0.2, 0.01, 0.2, 0.5))

    async def before_first_event(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await asyncio.sleep(0.05)
        return response

    async with _server(before_first_event) as url:
        with pytest.raises(BoundedHTTPError, match="first_event_timeout"):
            await _call(url, StreamTimeouts(0.2, 0.01, 0.2, 0.5))

    async def after_first_event(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"event: ping\ndata: {}\n\n")
        await asyncio.sleep(0.05)
        return response

    async with _server(after_first_event) as url:
        with pytest.raises(BoundedHTTPError, match="idle_timeout"):
            await _call(url, StreamTimeouts(0.2, 0.2, 0.01, 0.5))

    async def past_total(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        try:
            for _ in range(20):
                await response.write(b"event: ping\ndata: {}\n\n")
                await asyncio.sleep(0.005)
        except ConnectionResetError:
            pass
        return response

    async with _server(past_total) as url:
        with pytest.raises(BoundedHTTPError, match="total_timeout"):
            await _call(url, StreamTimeouts(0.2, 0.2, 0.2, 0.02))


@pytest.mark.asyncio
async def test_raw_sse_limit_is_enforced_incrementally() -> None:
    async def oversized(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await response.write(b"data: " + (b"x" * 64) + b"\n\n")
        return response

    async with _server(oversized) as url:
        with pytest.raises(BoundedHTTPError, match="response_too_large"):
            await _call(url, StreamTimeouts(0.2, 0.2, 0.2, 0.5), max_raw_bytes=16)


@pytest.mark.asyncio
async def test_non_success_streaming_error_is_delivered_as_sse_event() -> None:
    async def rejected(_request: web.Request) -> web.StreamResponse:
        return web.Response(
            status=400,
            body=(
                b"event: error\n"
                b'data: {"event_type":"error","error":{"code":"invalid_argument",'
                b'"message":"invalid request"}}\n\n'
            ),
            headers={"Content-Type": "text/event-stream"},
        )

    events: list[SSEEvent] = []

    async def record(event: SSEEvent) -> None:
        events.append(event)

    async with _server(rejected) as url:
        response = await AiohttpBoundedSSERequester().request_sse(
            "POST",
            url,
            headers={"Accept": "text/event-stream"},
            json_body={"stream": True},
            timeouts=StreamTimeouts(0.2, 0.2, 0.2, 0.5),
            max_raw_bytes=1024,
            on_event=record,
        )

    assert response.status == 400
    assert response.json_body is None
    assert response.event_count == 1
    assert [(event.event, event.data) for event in events] == [
        (
            "error",
            '{"event_type":"error","error":{"code":"invalid_argument","message":"invalid request"}}',
        )
    ]
