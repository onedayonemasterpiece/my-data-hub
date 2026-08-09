from __future__ import annotations

import hmac
from collections.abc import Awaitable, Callable
from typing import Any

ASGIApp = Callable[[dict[str, Any], Callable[..., Awaitable[Any]], Callable[..., Awaitable[Any]]], Awaitable[Any]]


class DevelopmentBearerSecurity:
    """Fail-closed local/private HTTP guard; production must use the OAuth adapter."""

    def __init__(
        self,
        app: ASGIApp,
        *,
        token: str,
        allowed_origins: tuple[str, ...],
        allowed_hosts: tuple[str, ...],
        max_request_bytes: int = 1_048_576,
    ) -> None:
        if not token:
            raise ValueError("development bearer token must not be empty")
        self.app = app
        self.token = token
        self.allowed_origins = set(allowed_origins)
        self.allowed_hosts = set(allowed_hosts)
        self.max_request_bytes = max_request_bytes

    async def __call__(self, scope, receive, send) -> None:  # type: ignore[no-untyped-def]
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        headers = {
            key.decode("latin-1").lower(): value.decode("latin-1")
            for key, value in scope.get("headers", [])
        }
        origin = headers.get("origin")
        host = headers.get("host", "").split(":", 1)[0]
        authorization = headers.get("authorization", "")
        content_length = headers.get("content-length")
        if origin and origin not in self.allowed_origins:
            await self._reject(send, 403, b"origin not allowed")
            return
        if self.allowed_hosts and host not in self.allowed_hosts:
            await self._reject(send, 403, b"host not allowed")
            return
        expected = f"Bearer {self.token}"
        if not hmac.compare_digest(authorization, expected):
            await self._reject(send, 401, b"authentication required", authenticate=True)
            return
        if content_length:
            try:
                if int(content_length) > self.max_request_bytes:
                    await self._reject(send, 413, b"request too large")
                    return
            except ValueError:
                await self._reject(send, 400, b"invalid content-length")
                return

        consumed = 0

        async def limited_receive():  # type: ignore[no-untyped-def]
            nonlocal consumed
            message = await receive()
            if message.get("type") == "http.request":
                consumed += len(message.get("body", b""))
                if consumed > self.max_request_bytes:
                    raise RuntimeError("request body exceeded configured limit")
            return message

        async def secure_send(message):  # type: ignore[no-untyped-def]
            if message.get("type") == "http.response.start":
                extra = [
                    (b"cache-control", b"no-store"),
                    (b"x-content-type-options", b"nosniff"),
                    (b"referrer-policy", b"no-referrer"),
                ]
                message["headers"] = list(message.get("headers", [])) + extra
            await send(message)

        try:
            await self.app(scope, limited_receive, secure_send)
        except RuntimeError as exc:
            if "configured limit" in str(exc):
                await self._reject(send, 413, b"request too large")
                return
            raise

    @staticmethod
    async def _reject(
        send, status: int, body: bytes, *, authenticate: bool = False
    ) -> None:  # type: ignore[no-untyped-def]
        headers = [(b"content-type", b"text/plain; charset=utf-8"), (b"cache-control", b"no-store")]
        if authenticate:
            headers.append((b"www-authenticate", b"Bearer"))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})
