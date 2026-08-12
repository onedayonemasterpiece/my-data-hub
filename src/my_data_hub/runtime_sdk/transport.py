from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True, slots=True)
class TransportResponse:
    status: int
    body: bytes = b""


class CallbackTransport(Protocol):
    def post(self, url: str, body: bytes, headers: dict[str, str], timeout_seconds: float) -> TransportResponse: ...


class UrllibCallbackTransport:
    def post(self, url: str, body: bytes, headers: dict[str, str], timeout_seconds: float) -> TransportResponse:
        request = urllib.request.Request(url, data=body, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(request, timeout=timeout_seconds) as response:
                return TransportResponse(status=int(response.status), body=response.read(64 * 1024))
        except urllib.error.HTTPError as exc:
            return TransportResponse(status=int(exc.code), body=exc.read(64 * 1024))


def json_body(event: dict[str, object]) -> bytes:
    return json.dumps(event, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
