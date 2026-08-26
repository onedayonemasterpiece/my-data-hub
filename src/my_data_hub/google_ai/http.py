from __future__ import annotations

import asyncio
import codecs
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

import aiohttp


class BoundedHTTPError(RuntimeError):
    def __init__(self, kind: str) -> None:
        self.kind = kind
        super().__init__(kind)


@dataclass(frozen=True, slots=True)
class BoundedHTTPResponse:
    status: int
    json_body: Any
    retry_after: str | None
    content_type: str | None


@dataclass(frozen=True, slots=True)
class SSEEvent:
    event: str
    data: str


@dataclass(frozen=True, slots=True)
class StreamTimeouts:
    connect_seconds: float = 30.0
    first_event_seconds: float = 120.0
    idle_seconds: float = 300.0
    total_seconds: float = 1800.0


@dataclass(frozen=True, slots=True)
class BoundedSSEResponse:
    status: int
    json_body: Any
    retry_after: str | None
    content_type: str | None
    event_count: int
    done: bool


class IncrementalSSEParser:
    """Incremental UTF-8 SSE parser; network chunks never imply line boundaries."""

    def __init__(self) -> None:
        self._decoder = codecs.getincrementaldecoder("utf-8")(errors="strict")
        self._buffer = ""
        self._event = "message"
        self._data: list[str] = []
        self.done = False

    def feed(self, chunk: bytes) -> list[SSEEvent]:
        if self.done:
            if chunk.strip(b"\r\n"):
                raise BoundedHTTPError("malformed_sse")
            return []
        try:
            self._buffer += self._decoder.decode(chunk, final=False)
        except UnicodeDecodeError as exc:
            raise BoundedHTTPError("malformed_sse") from exc
        events: list[SSEEvent] = []
        while "\n" in self._buffer:
            line, self._buffer = self._buffer.split("\n", 1)
            if line.endswith("\r"):
                line = line[:-1]
            event = self._line(line)
            if event is not None:
                events.append(event)
            if self.done:
                if self._buffer.strip("\r\n"):
                    raise BoundedHTTPError("malformed_sse")
                self._buffer = ""
                break
        return events

    def finish(self) -> None:
        try:
            self._buffer += self._decoder.decode(b"", final=True)
        except UnicodeDecodeError as exc:
            raise BoundedHTTPError("malformed_sse") from exc
        if self._buffer or self._data or self._event != "message":
            raise BoundedHTTPError("malformed_sse")

    def _line(self, line: str) -> SSEEvent | None:
        if line == "":
            if not self._data:
                self._event = "message"
                return None
            data = "\n".join(self._data)
            event = self._event
            self._event = "message"
            self._data = []
            if data == "[DONE]":
                self.done = True
                return None
            return SSEEvent(event=event, data=data)
        if line.startswith(":"):
            return None
        field, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field == "event":
            if "\x00" in value:
                raise BoundedHTTPError("malformed_sse")
            self._event = value or "message"
        elif field == "data":
            self._data.append(value)
        return None


class BoundedJSONRequester(Protocol):
    async def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any] | None,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> BoundedHTTPResponse: ...


class BoundedSSERequester(Protocol):
    async def request_sse(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
        timeouts: StreamTimeouts,
        max_raw_bytes: int,
        on_event: Callable[[SSEEvent], Awaitable[None]],
    ) -> BoundedSSEResponse: ...


class AiohttpBoundedJSONRequester:
    """One call performs exactly one physical HTTP request and never retries."""

    async def request_json(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any] | None,
        timeout_seconds: float,
        max_response_bytes: int,
    ) -> BoundedHTTPResponse:
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        try:
            async with (
                aiohttp.ClientSession(timeout=timeout) as session,
                session.request(
                    method,
                    url,
                    headers=dict(headers),
                    json=dict(json_body) if json_body is not None else None,
                    allow_redirects=False,
                ) as response,
            ):
                body = bytearray()
                async for chunk in response.content.iter_chunked(16 * 1024):
                    body.extend(chunk)
                    if len(body) > max_response_bytes:
                        raise BoundedHTTPError("response_too_large")
                parsed: Any = None
                if body:
                    try:
                        parsed = json.loads(body.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        raise BoundedHTTPError("malformed_json") from exc
                return BoundedHTTPResponse(
                    status=response.status,
                    json_body=parsed,
                    retry_after=response.headers.get("Retry-After"),
                    content_type=response.headers.get("Content-Type"),
                )
        except TimeoutError as exc:
            raise BoundedHTTPError("timeout") from exc
        except asyncio.CancelledError:
            raise
        except BoundedHTTPError:
            raise
        except aiohttp.ClientError as exc:
            raise BoundedHTTPError("network") from exc


class AiohttpBoundedSSERequester:
    """Exactly one non-redirecting request with independent stream deadlines."""

    async def request_sse(
        self,
        method: str,
        url: str,
        *,
        headers: Mapping[str, str],
        json_body: Mapping[str, Any],
        timeouts: StreamTimeouts,
        max_raw_bytes: int,
        on_event: Callable[[SSEEvent], Awaitable[None]],
    ) -> BoundedSSEResponse:
        loop = asyncio.get_running_loop()
        started = loop.time()
        first_event_deadline = started + timeouts.first_event_seconds
        total_deadline = started + timeouts.total_seconds
        timeout = aiohttp.ClientTimeout(
            total=None,
            connect=timeouts.connect_seconds,
            sock_connect=timeouts.connect_seconds,
            sock_read=None,
        )
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                header_deadline = min(first_event_deadline, total_deadline)
                try:
                    async with asyncio.timeout(self._remaining(header_deadline, loop.time())):
                        response = await session.request(
                            method,
                            url,
                            headers=dict(headers),
                            json=dict(json_body),
                            allow_redirects=False,
                        )
                except aiohttp.ConnectionTimeoutError as exc:
                    raise BoundedHTTPError("connect_timeout") from exc
                except TimeoutError as exc:
                    kind = "total_timeout" if loop.time() >= total_deadline else "first_event_timeout"
                    raise BoundedHTTPError(kind) from exc
                try:
                    return await self._consume(
                        response,
                        timeouts=timeouts,
                        max_raw_bytes=max_raw_bytes,
                        first_event_deadline=first_event_deadline,
                        total_deadline=total_deadline,
                        on_event=on_event,
                    )
                finally:
                    response.release()
        except asyncio.CancelledError:
            raise
        except BoundedHTTPError:
            raise
        except aiohttp.ClientError as exc:
            raise BoundedHTTPError("network") from exc

    async def _consume(
        self,
        response: aiohttp.ClientResponse,
        *,
        timeouts: StreamTimeouts,
        max_raw_bytes: int,
        first_event_deadline: float,
        total_deadline: float,
        on_event: Callable[[SSEEvent], Awaitable[None]],
    ) -> BoundedSSEResponse:
        loop = asyncio.get_running_loop()
        parser = IncrementalSSEParser()
        raw = bytearray()
        event_count = 0
        event_deadline = first_event_deadline
        content_type = response.headers.get("Content-Type")
        is_sse = (content_type or "").lower().startswith("text/event-stream")
        while not response.content.at_eof() and not parser.done:
            now = loop.time()
            deadline = min(total_deadline, event_deadline)
            remaining = deadline - now
            if remaining <= 0:
                if now >= total_deadline:
                    kind = "total_timeout"
                elif event_count == 0:
                    kind = "first_event_timeout"
                else:
                    kind = "idle_timeout"
                raise BoundedHTTPError(kind)
            try:
                async with asyncio.timeout(remaining):
                    chunk = await response.content.readany()
            except TimeoutError as exc:
                if loop.time() >= total_deadline:
                    kind = "total_timeout"
                elif event_count == 0:
                    kind = "first_event_timeout"
                else:
                    kind = "idle_timeout"
                raise BoundedHTTPError(kind) from exc
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > max_raw_bytes:
                raise BoundedHTTPError("response_too_large")
            if not is_sse:
                continue
            for event in parser.feed(chunk):
                await on_event(event)
                event_count += 1
                event_deadline = loop.time() + timeouts.idle_seconds
        if loop.time() >= total_deadline:
            raise BoundedHTTPError("total_timeout")
        if is_sse:
            parser.finish()
            body: Any = None
        elif 200 <= response.status < 300:
            raise BoundedHTTPError("malformed_sse")
        else:
            try:
                body = json.loads(raw.decode("utf-8")) if raw else None
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                raise BoundedHTTPError("malformed_json") from exc
        return BoundedSSEResponse(
            status=response.status,
            json_body=body,
            retry_after=response.headers.get("Retry-After"),
            content_type=content_type,
            event_count=event_count,
            done=parser.done,
        )

    @staticmethod
    def _remaining(deadline: float, now: float) -> float:
        remaining = deadline - now
        if remaining <= 0:
            raise BoundedHTTPError("total_timeout")
        return remaining
