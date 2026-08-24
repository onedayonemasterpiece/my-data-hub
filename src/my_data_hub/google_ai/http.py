from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
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
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.request(
                    method,
                    url,
                    headers=dict(headers),
                    json=dict(json_body) if json_body is not None else None,
                    allow_redirects=False,
                ) as response:
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
