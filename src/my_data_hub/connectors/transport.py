from __future__ import annotations

import json
from dataclasses import dataclass, replace
from email.message import Message
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

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
            with urlopen(request, timeout=self.timeout_seconds) as response:
                body = response.read()
                if response.status in {200, 201, 202}:
                    return self._success(response.status, body)
                return DeliveryResult(
                    DeliveryDisposition.RETRY,
                    message=f"unexpected HTTP status {response.status}",
                )
        except HTTPError as exc:
            body = exc.read()
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
        except (TimeoutError, URLError) as exc:
            return DeliveryResult(
                DeliveryDisposition.RETRY,
                message=f"intake unavailable: {exc}",
            )


def with_url(transport: HttpConnectorTransport, intake_url: str) -> HttpConnectorTransport:
    """Return a copied transport without exposing its bearer token in diagnostics."""
    return replace(transport, intake_url=intake_url)
