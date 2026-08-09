from __future__ import annotations

import json
from dataclasses import dataclass, replace
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from my_data_hub.connectors.contracts import ConnectorReceipt, ReceiptStatus
from my_data_hub.connectors.spool import DeliveryDisposition, DeliveryResult


def _retry_after(headers: Message) -> float | None:
    value = headers.get("Retry-After")
    if value is None:
        return None
    try:
        parsed = float(value)
    except ValueError:
        return None
    return max(0.0, parsed)


def _message(body: bytes) -> str:
    if not body:
        return ""
    try:
        value = json.loads(body)
        if isinstance(value, dict):
            return str(value.get("detail") or value.get("message") or value)[:2000]
    except (UnicodeDecodeError, json.JSONDecodeError):
        pass
    return body.decode("utf-8", errors="replace")[:2000]


@dataclass(slots=True)
class HttpConnectorTransport:
    """Small producer HTTP adapter; submission itself is the availability probe."""

    intake_url: str
    bearer_token: str
    timeout_seconds: float = 15.0
    allow_insecure_loopback: bool = False
    max_response_bytes: int = 256 * 1024

    def __post_init__(self) -> None:
        parsed = urlsplit(self.intake_url)
        loopback = parsed.hostname in {"127.0.0.1", "::1", "localhost"}
        if parsed.scheme != "https" and not (
            self.allow_insecure_loopback and parsed.scheme == "http" and loopback
        ):
            raise ValueError("connector intake URL must use HTTPS")
        if (
            not parsed.netloc
            or parsed.username is not None
            or parsed.password is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("connector intake URL must be an absolute URL without credentials/query/fragment")
        if not self.bearer_token:
            raise ValueError("connector bearer token must not be empty")
        if not 0.1 <= self.timeout_seconds <= 120:
            raise ValueError("connector timeout must be between 0.1 and 120 seconds")
        if not 1024 <= self.max_response_bytes <= 2 * 1024 * 1024:
            raise ValueError("connector response cap must be between 1 KiB and 2 MiB")

    def _read_bounded(self, response: object) -> bytes:
        body = response.read(self.max_response_bytes + 1)  # type: ignore[attr-defined]
        if len(body) > self.max_response_bytes:
            raise ValueError("connector response exceeded the byte cap")
        return body

    def _success(self, status_code: int, body: bytes) -> DeliveryResult:
        try:
            value = json.loads(body)
            if isinstance(value, dict) and isinstance(value.get("receipt"), dict):
                value = value["receipt"]
            if not isinstance(value, dict):
                raise ValueError("receipt body must be an object")
            status = ReceiptStatus.REPLAYED if status_code in {200, 201} else ReceiptStatus.ACCEPTED
            value = {**value, "status": status.value}
            receipt = ConnectorReceipt.model_validate(value)
        except (json.JSONDecodeError, UnicodeDecodeError, ValueError) as exc:
            return DeliveryResult(
                DeliveryDisposition.RETRY,
                message=f"successful HTTP response had an invalid receipt: {exc}",
            )
        disposition = (
            DeliveryDisposition.REPLAYED
            if receipt.status is ReceiptStatus.REPLAYED
            else DeliveryDisposition.ACCEPTED
        )
        return DeliveryResult(disposition, receipt=receipt)

    def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult:
        request = Request(
            self.intake_url,
            data=exact_envelope_bytes,
            headers={
                "Accept": "application/json",
                "Authorization": f"Bearer {self.bearer_token}",
                "Content-Type": "application/json",
            },
            method="POST",
        )
        try:
            class _RejectRedirects(HTTPRedirectHandler):
                def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
                    return None

            # Redirects are deliberately rejected: replaying Authorization at a
            # different authority could disclose the connector credential.
            with build_opener(_RejectRedirects).open(
                request, timeout=self.timeout_seconds
            ) as response:
                body = self._read_bounded(response)
                if response.status in {200, 201, 202}:
                    return self._success(response.status, body)
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    message=f"unexpected HTTP status {response.status}",
                )
        except HTTPError as exc:
            try:
                body = self._read_bounded(exc)
            except ValueError as body_error:
                return DeliveryResult(DeliveryDisposition.REJECTED, message=str(body_error))
            if exc.code == 409:
                return DeliveryResult(DeliveryDisposition.CONFLICT, message=_message(body))
            if exc.code == 422:
                return DeliveryResult(DeliveryDisposition.REJECTED, message=_message(body))
            if exc.code in {401, 403}:
                return DeliveryResult(DeliveryDisposition.AUTH_FAILURE, message=_message(body))
            if exc.code == 429 or exc.code in {502, 503, 504}:
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    message=_message(body) or f"HTTP {exc.code}",
                    retry_after_seconds=_retry_after(exc.headers),
                )
            return DeliveryResult(
                DeliveryDisposition.REJECTED,
                message=_message(body) or f"HTTP {exc.code}",
            )
        except ValueError as exc:
            return DeliveryResult(DeliveryDisposition.REJECTED, message=str(exc))
        except (TimeoutError, URLError) as exc:
            return DeliveryResult(
                DeliveryDisposition.RETRY,
                message=f"intake unavailable: {exc}",
            )


def with_url(transport: HttpConnectorTransport, intake_url: str) -> HttpConnectorTransport:
    """Return a copied transport without exposing its bearer token in diagnostics."""
    return replace(transport, intake_url=intake_url)
