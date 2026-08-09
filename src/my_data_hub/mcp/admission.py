from __future__ import annotations

import asyncio
import inspect
import ipaddress
import json
import threading
import time
from collections import defaultdict, deque
from collections.abc import Awaitable, Callable, Iterable
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qsl, urlsplit
from uuid import uuid4

from my_data_hub.mcp.oauth import AccessIdentity, OAuthBearerValidator, TokenValidationError

ASGIReceive = Callable[[], Awaitable[dict[str, Any]]]
ASGISend = Callable[[dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[dict[str, Any], ASGIReceive, ASGISend], Awaitable[None]]
Authenticator = Callable[[str], AccessIdentity | Awaitable[AccessIdentity | None] | None]

_SINGLETON_HEADERS = frozenset(
    {
        "authorization",
        "content-length",
        "host",
        "origin",
        "forwarded",
        "x-forwarded-for",
        "x-forwarded-host",
        "x-forwarded-proto",
    }
)
_FORWARDED_HEADERS = frozenset(
    {"forwarded", "x-forwarded-for", "x-forwarded-host", "x-forwarded-proto"}
)
_SECURITY_RESPONSE_HEADERS = (
    (b"cache-control", b"no-store"),
    (b"pragma", b"no-cache"),
    (b"x-content-type-options", b"nosniff"),
    (b"referrer-policy", b"no-referrer"),
    (b"x-frame-options", b"DENY"),
)


class AdmissionError(PermissionError):
    def __init__(self, status: int, code: str, *, authenticate: bool = False) -> None:
        super().__init__(code)
        self.status = status
        self.code = code
        self.authenticate = authenticate


class RequestTooLarge(AdmissionError):
    def __init__(self) -> None:
        super().__init__(413, "request_too_large")


class ResponseTooLarge(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class AdmissionLimits:
    max_header_bytes: int = 32_768
    max_request_bytes: int = 1_048_576
    max_response_bytes: int = 1_048_576
    max_concurrency: int = 16
    requests_per_window: int = 120
    rate_window_seconds: float = 60.0
    queue_timeout_seconds: float = 0.25
    request_timeout_seconds: float = 30.0
    max_rate_keys: int = 4096

    def __post_init__(self) -> None:
        if not 1024 <= self.max_header_bytes <= 1024 * 1024:
            raise ValueError("maximum header size must be between 1 KiB and 1 MiB")
        if not 1 <= self.max_request_bytes <= 64 * 1024 * 1024:
            raise ValueError("maximum request size must be between 1 byte and 64 MiB")
        if not 1 <= self.max_response_bytes <= 64 * 1024 * 1024:
            raise ValueError("maximum response size must be between 1 byte and 64 MiB")
        if not 1 <= self.max_concurrency <= 1024:
            raise ValueError("maximum concurrency must be between 1 and 1024")
        if not 1 <= self.requests_per_window <= 1_000_000:
            raise ValueError("request rate must be positive and bounded")
        if not 0.1 <= self.rate_window_seconds <= 86_400:
            raise ValueError("rate window must be between 0.1 seconds and one day")
        if not 0.01 <= self.queue_timeout_seconds <= 60:
            raise ValueError("queue timeout must be between 0.01 and 60 seconds")
        if not 0.1 <= self.request_timeout_seconds <= 3600:
            raise ValueError("request timeout must be between 0.1 seconds and one hour")
        if not 16 <= self.max_rate_keys <= 1_000_000:
            raise ValueError("rate-key capacity must be between 16 and 1,000,000")


class SlidingWindowRateLimiter:
    """Small process-local admission limiter; an edge limiter remains additive."""

    def __init__(self, *, max_keys: int = 4096, clock: Callable[[], float] = time.monotonic) -> None:
        self.max_keys = max_keys
        self.clock = clock
        self._events: dict[str, deque[float]] = defaultdict(deque)
        self._lock = threading.Lock()

    def allow(self, key: str, *, limit: int, window_seconds: float) -> bool:
        now = self.clock()
        cutoff = now - window_seconds
        with self._lock:
            queue = self._events[key]
            while queue and queue[0] <= cutoff:
                queue.popleft()
            if len(queue) >= limit:
                return False
            queue.append(now)
            if len(self._events) > self.max_keys:
                stale = sorted(
                    (last[-1] if last else float("-inf"), name)
                    for name, last in self._events.items()
                    if name != key
                )
                for _, name in stale[: len(self._events) - self.max_keys]:
                    self._events.pop(name, None)
            return True


def parse_host_header(value: str) -> tuple[str, int | None]:
    """Parse an RFC Host authority without breaking bracketed IPv6."""

    value = value.strip()
    if not value or any(char.isspace() or ord(char) < 0x21 for char in value):
        raise ValueError("invalid Host header")
    port: int | None = None
    if value.startswith("["):
        end = value.find("]")
        if end <= 1:
            raise ValueError("invalid IPv6 Host header")
        address = value[1:end]
        if "%" in address:
            raise ValueError("IPv6 zone identifiers are not accepted in Host")
        try:
            host = ipaddress.IPv6Address(address).compressed
        except ValueError as exc:
            raise ValueError("invalid IPv6 Host header") from exc
        remainder = value[end + 1 :]
        if remainder:
            if not remainder.startswith(":") or not remainder[1:].isdigit():
                raise ValueError("invalid IPv6 Host port")
            port = int(remainder[1:])
    else:
        if value.count(":") > 1:
            raise ValueError("IPv6 Host addresses must be bracketed")
        raw_host, separator, raw_port = value.rpartition(":")
        if separator:
            if not raw_host or not raw_port.isdigit():
                raise ValueError("invalid Host port")
            host, port = raw_host, int(raw_port)
        else:
            host = value
        if host.endswith("."):
            host = host[:-1]
        try:
            parsed_ip = ipaddress.ip_address(host)
        except ValueError:
            if not host or len(host) > 253:
                raise ValueError("invalid Host name") from None
            try:
                host = host.encode("idna").decode("ascii").casefold()
            except UnicodeError as exc:
                raise ValueError("invalid Host name") from exc
            labels = host.split(".")
            if any(
                not label
                or len(label) > 63
                or label.startswith("-")
                or label.endswith("-")
                or not all(char.isalnum() or char == "-" for char in label)
                for label in labels
            ):
                raise ValueError("invalid Host name") from None
        else:
            host = parsed_ip.compressed
    if port is not None and not 1 <= port <= 65535:
        raise ValueError("invalid Host port")
    return host.casefold(), port


def _format_authority(host: str, port: int | None) -> str:
    formatted = f"[{host}]" if ":" in host else host
    return f"{formatted}:{port}" if port is not None else formatted


def normalize_origin(value: str) -> str:
    if value == "null":
        raise ValueError("opaque origins are not accepted")
    parsed = urlsplit(value)
    if parsed.scheme.casefold() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("invalid Origin")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("invalid Origin")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ValueError("invalid Origin")
    try:
        host = parsed.hostname
        port = parsed.port
    except ValueError as exc:
        raise ValueError("invalid Origin") from exc
    if host is None:
        raise ValueError("invalid Origin")
    normalized_host, _ = parse_host_header(f"[{host}]" if ":" in host else host)
    default_port = 80 if parsed.scheme.casefold() == "http" else 443
    return f"{parsed.scheme.casefold()}://{_format_authority(normalized_host, None if port == default_port else port)}"


def _header_multimap(raw: Iterable[tuple[bytes, bytes]]) -> dict[str, list[str]]:
    result: dict[str, list[str]] = defaultdict(list)
    for raw_name, raw_value in raw:
        try:
            name = raw_name.decode("ascii").casefold()
            value = raw_value.decode("latin-1")
        except UnicodeDecodeError as exc:
            raise AdmissionError(400, "invalid_headers") from exc
        if not name or any(char not in "!#$%&'*+-.^_`|~0123456789abcdefghijklmnopqrstuvwxyz" for char in name):
            raise AdmissionError(400, "invalid_headers")
        if "\r" in value or "\n" in value or "\x00" in value:
            raise AdmissionError(400, "invalid_headers")
        result[name].append(value)
    for name in _SINGLETON_HEADERS:
        if len(result.get(name, ())) > 1:
            raise AdmissionError(400, "duplicate_security_header")
    return result


class HTTPAdmissionSecurity:
    """ASGI fail-closed transport admission boundary.

    Bodies and responses are buffered within explicit bounds so a late chunk
    cannot evade limits after response headers have already been emitted.
    """

    def __init__(
        self,
        app: ASGIApp,
        *,
        allowed_hosts: tuple[str, ...],
        allowed_origins: tuple[str, ...],
        limits: AdmissionLimits | None = None,
        authenticator: Authenticator | None = None,
        trusted_proxy_ips: tuple[str, ...] = (),
        unauthenticated_paths: tuple[str, ...] = (),
        resource_metadata_url: str | None = None,
    ) -> None:
        if not allowed_hosts:
            raise ValueError("at least one exact allowed Host is required")
        self.app = app
        self.limits = limits or AdmissionLimits()
        self.authenticator = authenticator
        self.allowed_hosts = frozenset(parse_host_header(value) for value in allowed_hosts)
        self.allowed_origins = frozenset(normalize_origin(value) for value in allowed_origins)
        self.trusted_proxy_ips = frozenset(ipaddress.ip_address(value).compressed for value in trusted_proxy_ips)
        if any(not path.startswith("/") for path in unauthenticated_paths):
            raise ValueError("unauthenticated paths must be exact absolute paths")
        if resource_metadata_url is not None and not resource_metadata_url.startswith("https://"):
            raise ValueError("OAuth resource metadata URL must use HTTPS")
        self.unauthenticated_paths = frozenset(unauthenticated_paths)
        self.resource_metadata_url = resource_metadata_url
        self._semaphore = asyncio.Semaphore(self.limits.max_concurrency)
        self._rate = SlidingWindowRateLimiter(max_keys=self.limits.max_rate_keys)

    def _peer_ip(self, scope: dict[str, Any]) -> str:
        client = scope.get("client")
        raw = client[0] if isinstance(client, (tuple, list)) and client else "unknown"
        try:
            return ipaddress.ip_address(str(raw)).compressed
        except ValueError:
            return "unknown"

    def _transport(self, scope: dict[str, Any]) -> tuple[dict[str, list[str]], str, str]:
        raw_headers = tuple(scope.get("headers", ()))
        if sum(len(name) + len(value) for name, value in raw_headers) > self.limits.max_header_bytes:
            raise AdmissionError(431, "request_headers_too_large")
        headers = _header_multimap(raw_headers)
        forwarded_present = _FORWARDED_HEADERS.intersection(headers)
        peer = self._peer_ip(scope)
        if "forwarded" in headers or any(
            name.startswith("x-forwarded-") and name not in _FORWARDED_HEADERS for name in headers
        ):
            raise AdmissionError(400, "forwarded_header_rejected")
        if forwarded_present and peer not in self.trusted_proxy_ips:
            raise AdmissionError(400, "forwarded_header_rejected")

        effective_host = headers.get("host", [""])[0]
        client_ip = peer
        if forwarded_present:
            if "x-forwarded-host" in headers:
                effective_host = headers["x-forwarded-host"][0].strip()
            if "x-forwarded-proto" in headers:
                proto = headers["x-forwarded-proto"][0].strip().casefold()
                if proto != "https":
                    raise AdmissionError(400, "forwarded_header_rejected")
            if "x-forwarded-for" in headers:
                forwarded_for = headers["x-forwarded-for"][0].strip()
                if "," in forwarded_for:
                    raise AdmissionError(400, "forwarded_header_rejected")
                try:
                    client_ip = ipaddress.ip_address(forwarded_for).compressed
                except ValueError as exc:
                    raise AdmissionError(400, "forwarded_header_rejected") from exc

        try:
            parsed_host = parse_host_header(effective_host)
        except ValueError as exc:
            raise AdmissionError(400, "invalid_host") from exc
        if not any(
            parsed_host[0] == allowed[0]
            and (allowed[1] is None or parsed_host[1] == allowed[1])
            for allowed in self.allowed_hosts
        ):
            raise AdmissionError(403, "host_not_allowed")
        origin = headers.get("origin", [None])[0]
        if origin is not None:
            try:
                normalized = normalize_origin(origin.strip())
            except ValueError as exc:
                raise AdmissionError(403, "origin_not_allowed") from exc
            if normalized not in self.allowed_origins:
                raise AdmissionError(403, "origin_not_allowed")
        return headers, client_ip, _format_authority(*parsed_host)

    async def _read_body(self, receive: ASGIReceive, content_length: str | None) -> bytes:
        if content_length is not None:
            try:
                declared = int(content_length)
            except ValueError as exc:
                raise AdmissionError(400, "invalid_content_length") from exc
            if declared < 0:
                raise AdmissionError(400, "invalid_content_length")
            if declared > self.limits.max_request_bytes:
                raise RequestTooLarge
        body = bytearray()
        while True:
            message = await receive()
            message_type = message.get("type")
            if message_type == "http.disconnect":
                raise AdmissionError(400, "client_disconnected")
            if message_type != "http.request":
                raise AdmissionError(400, "invalid_request_stream")
            chunk = message.get("body", b"")
            if not isinstance(chunk, bytes):
                raise AdmissionError(400, "invalid_request_stream")
            body.extend(chunk)
            if len(body) > self.limits.max_request_bytes:
                raise RequestTooLarge
            if not message.get("more_body", False):
                break
        if content_length is not None and len(body) != int(content_length):
            raise AdmissionError(400, "content_length_mismatch")
        return bytes(body)

    @staticmethod
    def _downstream_scope(
        scope: dict[str, Any], headers: dict[str, list[str]], effective_host: str, state: dict[str, Any]
    ) -> dict[str, Any]:
        cleaned: list[tuple[bytes, bytes]] = []
        for name, values in headers.items():
            if name in _FORWARDED_HEADERS or name == "x-correlation-id" or name == "host":
                continue
            cleaned.extend((name.encode("ascii"), value.encode("latin-1")) for value in values)
        cleaned.append((b"host", effective_host.encode("ascii")))
        result = dict(scope)
        result["headers"] = cleaned
        result["state"] = state
        return result

    async def _run_app(
        self,
        scope: dict[str, Any],
        body: bytes,
    ) -> list[dict[str, Any]]:
        delivered = False
        response: list[dict[str, Any]] = []
        response_bytes = 0
        started = False
        complete = False

        async def replay_receive() -> dict[str, Any]:
            nonlocal delivered
            if not delivered:
                delivered = True
                return {"type": "http.request", "body": body, "more_body": False}
            return {"type": "http.disconnect"}

        async def bounded_send(message: dict[str, Any]) -> None:
            nonlocal response_bytes, started, complete
            message_type = message.get("type")
            if message_type == "http.response.start":
                if started or complete:
                    raise RuntimeError("invalid ASGI response sequence")
                raw_headers = message.get("headers", ())
                if sum(len(name) + len(value) for name, value in raw_headers) > self.limits.max_header_bytes:
                    raise ResponseTooLarge
                started = True
            elif message_type == "http.response.body":
                if not started or complete:
                    raise RuntimeError("invalid ASGI response sequence")
                chunk = message.get("body", b"")
                if not isinstance(chunk, bytes):
                    raise RuntimeError("invalid ASGI response body")
                response_bytes += len(chunk)
                if response_bytes > self.limits.max_response_bytes:
                    raise ResponseTooLarge
                complete = not message.get("more_body", False)
            else:
                raise RuntimeError("unsupported ASGI response message")
            response.append(dict(message))

        await self.app(scope, replay_receive, bounded_send)
        if not started or not complete:
            raise RuntimeError("incomplete ASGI response")
        return response

    async def __call__(self, scope: dict[str, Any], receive: ASGIReceive, send: ASGISend) -> None:
        if scope.get("type") != "http":
            await self.app(scope, receive, send)
            return
        correlation_id = uuid4().hex
        try:
            headers, client_ip, effective_host = self._transport(scope)
            if scope.get("query_string"):
                query = scope["query_string"].decode("latin-1")
                if any(name.casefold() == "access_token" for name, _ in parse_qsl(query, keep_blank_values=True)):
                    raise AdmissionError(400, "query_bearer_forbidden")
            authorization = headers.get("authorization", [""])[0]
            identity: AccessIdentity | None = None
            public_metadata = (
                scope.get("method") == "GET"
                and scope.get("path") in self.unauthenticated_paths
            )
            if self.authenticator is not None and not public_metadata:
                if not authorization:
                    raise AdmissionError(401, "authentication_required", authenticate=True)
                try:
                    authenticated = self.authenticator(authorization)
                    identity = (
                        await asyncio.wait_for(authenticated, self.limits.request_timeout_seconds)
                        if inspect.isawaitable(authenticated)
                        else authenticated
                    )
                except TokenValidationError as exc:
                    raise AdmissionError(
                        403 if exc.code == "insufficient_scope" else 401,
                        exc.code,
                        authenticate=True,
                    ) from exc
                except Exception as exc:
                    raise AdmissionError(401, "invalid_token", authenticate=True) from exc
                if identity is None:
                    raise AdmissionError(401, "invalid_token", authenticate=True)
            rate_key = (
                f"principal:{identity.issuer}:{identity.client_id}:{identity.subject}"
                if identity is not None
                else f"peer:{client_ip}"
            )
            if not self._rate.allow(
                rate_key,
                limit=self.limits.requests_per_window,
                window_seconds=self.limits.rate_window_seconds,
            ):
                raise AdmissionError(429, "rate_limited")
            try:
                await asyncio.wait_for(self._semaphore.acquire(), self.limits.queue_timeout_seconds)
            except TimeoutError as exc:
                raise AdmissionError(503, "server_busy") from exc
            try:
                async with asyncio.timeout(self.limits.request_timeout_seconds):
                    body = await self._read_body(receive, headers.get("content-length", [None])[0])
                    state = dict(scope.get("state") or {})
                    state.update(
                        {
                            "correlation_id": correlation_id,
                            "client_ip": client_ip,
                            "oauth_principal": identity,
                        }
                    )
                    downstream = self._downstream_scope(scope, headers, effective_host, state)
                    response = await self._run_app(downstream, body)
            finally:
                self._semaphore.release()
            await self._send_buffered(send, response, correlation_id)
        except TimeoutError:
            await self._reject(send, 504, "request_timeout", correlation_id)
        except ResponseTooLarge:
            await self._reject(send, 502, "response_too_large", correlation_id)
        except AdmissionError as exc:
            await self._reject(
                send,
                exc.status,
                exc.code,
                correlation_id,
                authenticate=exc.authenticate,
                retry_after=exc.status in {429, 503},
                resource_metadata_url=(
                    self.resource_metadata_url if exc.authenticate else None
                ),
            )
        except Exception:
            # Never expose application, adapter, or ASGI protocol exception text.
            await self._reject(send, 500, "internal_error", correlation_id)

    @staticmethod
    def _secure_headers(existing: Iterable[tuple[bytes, bytes]], correlation_id: str) -> list[tuple[bytes, bytes]]:
        replaced = {
            "cache-control",
            "pragma",
            "x-content-type-options",
            "referrer-policy",
            "x-frame-options",
            "x-correlation-id",
        }
        headers = [(key, value) for key, value in existing if key.decode("latin-1").casefold() not in replaced]
        headers.extend(_SECURITY_RESPONSE_HEADERS)
        headers.append((b"x-correlation-id", correlation_id.encode("ascii")))
        return headers

    async def _send_buffered(
        self, send: ASGISend, messages: list[dict[str, Any]], correlation_id: str
    ) -> None:
        for message in messages:
            if message["type"] == "http.response.start":
                message["headers"] = self._secure_headers(message.get("headers", ()), correlation_id)
            await send(message)

    @classmethod
    async def _reject(
        cls,
        send: ASGISend,
        status: int,
        code: str,
        correlation_id: str,
        *,
        authenticate: bool = False,
        retry_after: bool = False,
        resource_metadata_url: str | None = None,
    ) -> None:
        body = json.dumps({"error": code}, separators=(",", ":")).encode("utf-8")
        headers = cls._secure_headers(((b"content-type", b"application/json"),), correlation_id)
        if authenticate:
            challenge = 'Bearer realm="my-data-hub"'
            if resource_metadata_url:
                challenge += f', resource_metadata="{resource_metadata_url}"'
            if code in {"invalid_token", "insufficient_scope"}:
                challenge += f', error="{code}"'
            headers.append((b"www-authenticate", challenge.encode("ascii")))
        if retry_after:
            headers.append((b"retry-after", b"1"))
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})


class OAuthAdmissionSecurity(HTTPAdmissionSecurity):
    def __init__(
        self,
        app: ASGIApp,
        *,
        validator: OAuthBearerValidator,
        required_scopes: frozenset[str],
        allowed_hosts: tuple[str, ...],
        allowed_origins: tuple[str, ...],
        limits: AdmissionLimits | None = None,
        trusted_proxy_ips: tuple[str, ...] = (),
        metadata_path: str | None = None,
        resource_metadata_url: str | None = None,
    ) -> None:
        async def authenticate(header: str) -> AccessIdentity:
            return await validator.validate_authorization_header(
                header,
                required_scopes=required_scopes,
                requested_resource=validator.policy.resource,
            )

        super().__init__(
            app,
            allowed_hosts=allowed_hosts,
            allowed_origins=allowed_origins,
            limits=limits,
            authenticator=authenticate,
            trusted_proxy_ips=trusted_proxy_ips,
            unauthenticated_paths=(metadata_path,) if metadata_path else (),
            resource_metadata_url=resource_metadata_url,
        )
