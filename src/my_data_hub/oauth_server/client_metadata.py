"""Bounded ChatGPT Client ID Metadata Document (CIMD) discovery."""

from __future__ import annotations

import json
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from threading import BoundedSemaphore, Lock
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .models import StaticClient

MAX_CLIENT_ID_BYTES = 255
MAX_METADATA_BYTES = 32 * 1_024
FETCH_TIMEOUT_SECONDS = 3.0
CACHE_TTL_SECONDS = 300


class ClientMetadataError(ValueError):
    """The public client metadata is unavailable or violates the bounded policy."""


@dataclass(frozen=True, slots=True)
class ClientMetadataResponse:
    status: int
    content_type: str
    body: bytes
    final_url: str


class _NoRedirects(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def _fetch(client_id: str) -> ClientMetadataResponse:
    opener = build_opener(_NoRedirects())
    request = Request(
        client_id,
        headers={
            "Accept": "application/json, application/*+json",
            "User-Agent": "my-data-hub-oauth-cimd/1",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=FETCH_TIMEOUT_SECONDS) as response:
            length = response.headers.get("Content-Length")
            if length is not None and (not length.isdigit() or int(length) > MAX_METADATA_BYTES):
                raise ClientMetadataError("client metadata exceeds the response bound")
            body = response.read(MAX_METADATA_BYTES + 1)
            return ClientMetadataResponse(
                status=int(response.status),
                content_type=response.headers.get_content_type(),
                body=body,
                final_url=response.geturl(),
            )
    except HTTPError as exc:
        raise ClientMetadataError("client metadata did not return HTTP 200") from exc
    except (OSError, TimeoutError, URLError) as exc:
        raise ClientMetadataError("client metadata fetch failed") from exc


def _exact_chatgpt_url(value: str, *, client_identifier: bool) -> None:
    if not value or len(value.encode("utf-8")) > MAX_CLIENT_ID_BYTES:
        raise ClientMetadataError("ChatGPT client URL is empty or oversized")
    parsed = urlsplit(value)
    try:
        port = parsed.port
    except ValueError as exc:
        raise ClientMetadataError("ChatGPT client URL has an invalid port") from exc
    path_parts = parsed.path.split("/")
    if (
        parsed.scheme != "https"
        or parsed.hostname != "chatgpt.com"
        or port is not None
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
        or parsed.path == "/"
        or any(part in {".", ".."} for part in path_parts)
    ):
        raise ClientMetadataError("client metadata URL must use the exact https://chatgpt.com origin")
    if client_identifier and not parsed.path.endswith("/client.json"):
        raise ClientMetadataError("ChatGPT client identifier must name client.json")


def _client(payload: object, *, client_id: str, allowed_scopes: frozenset[str]) -> StaticClient:
    if not isinstance(payload, Mapping):
        raise ClientMetadataError("client metadata must be a JSON object")
    if payload.get("client_id") != client_id:
        raise ClientMetadataError("client metadata client_id does not exactly match the fetched URL")
    if any(key in payload for key in ("client_secret", "client_secret_expires_at", "jwks")):
        raise ClientMetadataError("client metadata must not contain secret or inline key material")

    methods = payload.get("token_endpoint_auth_methods_supported")
    singular_method = payload.get("token_endpoint_auth_method")
    if singular_method is not None and singular_method != "none":
        raise ClientMetadataError("ChatGPT CIMD token method must be none")
    if methods is not None and (
        not isinstance(methods, list)
        or not methods
        or not all(isinstance(value, str) for value in methods)
        or len(methods) != len(set(methods))
        or "none" not in methods
        or any(value not in {"none", "private_key_jwt"} for value in methods)
    ):
        raise ClientMetadataError("ChatGPT CIMD token methods are not bounded public methods")
    if singular_method is None and methods is None:
        raise ClientMetadataError("ChatGPT CIMD must support the none public-client method")

    response_types = payload.get("response_types")
    if response_types is not None and response_types != ["code"]:
        raise ClientMetadataError("ChatGPT CIMD response types are not exact authorization code")
    grant_types = payload.get("grant_types")
    if grant_types is not None and (
        not isinstance(grant_types, list)
        or not all(isinstance(value, str) for value in grant_types)
        or "authorization_code" not in grant_types
        or any(value not in {"authorization_code", "refresh_token"} for value in grant_types)
    ):
        raise ClientMetadataError("ChatGPT CIMD grant types are not bounded")

    redirects = payload.get("redirect_uris")
    if not isinstance(redirects, list) or len(redirects) != 1 or not isinstance(redirects[0], str):
        raise ClientMetadataError("ChatGPT CIMD must publish one exact MCP redirect URI")
    redirect_uri = redirects[0]
    _exact_chatgpt_url(redirect_uri, client_identifier=False)
    redirect_path = urlsplit(redirect_uri).path
    prefix = "/connector/oauth/"
    callback_id = redirect_path.removeprefix(prefix)
    if (
        not redirect_path.startswith(prefix)
        or not re.fullmatch(r"[A-Za-z0-9._~-]{1,255}", callback_id)
    ):
        raise ClientMetadataError("ChatGPT CIMD redirect is not the exact MCP callback form")

    return StaticClient(
        client_id=client_id,
        redirect_uris=(redirect_uri,),
        allowed_scopes=allowed_scopes,
    )


def _unique_object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ClientMetadataError("client metadata contains a duplicate JSON key")
        result[key] = value
    return result


class ChatGPTClientMetadataResolver:
    """Resolve only nonsecret ChatGPT public-client metadata with bounded caching."""

    def __init__(
        self,
        *,
        allowed_scopes: frozenset[str],
        fetcher: Callable[[str], ClientMetadataResponse] = _fetch,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not allowed_scopes:
            raise ValueError("CIMD allowed scopes must be explicit and non-empty")
        self.allowed_scopes = allowed_scopes
        self.fetcher = fetcher
        self.clock = clock
        self._cache: dict[str, tuple[float, StaticClient]] = {}
        self._lock = Lock()
        self._fetch_slots = BoundedSemaphore(4)

    def resolve(self, client_id: str) -> StaticClient:
        _exact_chatgpt_url(client_id, client_identifier=True)
        now = self.clock()
        with self._lock:
            cached = self._cache.get(client_id)
            if cached is not None and cached[0] > now:
                return cached[1]
        if not self._fetch_slots.acquire(timeout=FETCH_TIMEOUT_SECONDS):
            raise ClientMetadataError("client metadata fetch concurrency is saturated")
        try:
            response = self.fetcher(client_id)
        finally:
            self._fetch_slots.release()
        content_type = response.content_type.partition(";")[0].strip().lower()
        if response.status != 200 or response.final_url != client_id:
            raise ClientMetadataError("client metadata redirect or non-200 response is forbidden")
        if content_type != "application/json" and not (
            content_type.startswith("application/") and content_type.endswith("+json")
        ):
            raise ClientMetadataError("client metadata content type is not JSON")
        if not response.body or len(response.body) > MAX_METADATA_BYTES:
            raise ClientMetadataError("client metadata body is empty or oversized")
        try:
            payload = json.loads(
                response.body.decode("utf-8"),
                object_pairs_hook=_unique_object,
                parse_constant=lambda _value: (_ for _ in ()).throw(
                    ClientMetadataError("client metadata contains a non-JSON number")
                ),
            )
        except (UnicodeError, json.JSONDecodeError, RecursionError) as exc:
            raise ClientMetadataError("client metadata body is not valid UTF-8 JSON") from exc
        client = _client(payload, client_id=client_id, allowed_scopes=self.allowed_scopes)
        with self._lock:
            self._cache[client_id] = (now + CACHE_TTL_SECONDS, client)
            while len(self._cache) > 16:
                oldest = min(self._cache, key=lambda key: self._cache[key][0])
                self._cache.pop(oldest, None)
        return client
