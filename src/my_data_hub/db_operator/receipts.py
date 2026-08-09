"""HMAC-signed operator preview and commit receipts."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

from my_data_hub.hashing import canonical_json_bytes, sha256_value

from .errors import ReceiptError


def _fingerprint_value(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, str)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise ReceiptError("non-finite parameters are forbidden")
        return value
    if isinstance(value, bytes):
        return {"type": "bytes", "base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, (datetime,)):
        if value.tzinfo is None or value.utcoffset() is None:
            raise ReceiptError("datetime parameters must be timezone-aware")
        return {"type": "datetime", "value": value.astimezone(UTC).isoformat()}
    if isinstance(value, (Decimal, UUID)):
        return {"type": type(value).__name__, "value": str(value)}
    if isinstance(value, (list, tuple)):
        return [_fingerprint_value(item) for item in value]
    if isinstance(value, Mapping):
        if not all(isinstance(key, str) for key in value):
            raise ReceiptError("parameter mappings must use string keys")
        return {key: _fingerprint_value(value[key]) for key in sorted(value)}
    raise ReceiptError(f"unsupported parameter type: {type(value).__name__}")


def parameter_fingerprint(params: Sequence[object]) -> str:
    return sha256_value(_fingerprint_value(tuple(params)))


def _b64_encode(value: bytes) -> str:
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def _b64_decode(value: str) -> bytes:
    try:
        return base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
    except Exception as exc:
        raise ReceiptError("receipt has invalid base64") from exc


@dataclass(frozen=True, slots=True)
class ReceiptSigner:
    secret: bytes
    key_id: str = "operator-hmac-v1"
    preview_ttl: timedelta = timedelta(minutes=5)

    def __post_init__(self) -> None:
        if len(self.secret) < 32:
            raise ValueError("receipt signing secret must contain at least 32 bytes")
        if not self.key_id:
            raise ValueError("receipt key_id must not be empty")
        if self.preview_ttl <= timedelta(0) or self.preview_ttl > timedelta(minutes=15):
            raise ValueError("preview receipt TTL must be in (0, 15 minutes]")

    def sign(self, payload: Mapping[str, Any]) -> str:
        envelope = {"key_id": self.key_id, "payload": dict(payload)}
        raw = canonical_json_bytes(envelope)
        signature = hmac.new(self.secret, raw, hashlib.sha256).digest()
        return f"{_b64_encode(raw)}.{_b64_encode(signature)}"

    def verify(self, token: str) -> dict[str, Any]:
        try:
            encoded_payload, encoded_signature = token.split(".", 1)
        except ValueError as exc:
            raise ReceiptError("receipt must have payload and signature") from exc
        raw = _b64_decode(encoded_payload)
        signature = _b64_decode(encoded_signature)
        expected = hmac.new(self.secret, raw, hashlib.sha256).digest()
        if not hmac.compare_digest(signature, expected):
            raise ReceiptError("receipt signature is invalid")
        try:
            envelope = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ReceiptError("receipt payload is invalid JSON") from exc
        if not isinstance(envelope, dict) or envelope.get("key_id") != self.key_id:
            raise ReceiptError("receipt signing key does not match")
        payload = envelope.get("payload")
        if not isinstance(payload, dict):
            raise ReceiptError("receipt payload must be an object")
        return payload

    def issue_preview(
        self,
        *,
        now: datetime,
        principal: str,
        session_id: str,
        correlation_id: str,
        sql_fingerprint: str,
        params_fingerprint: str,
        target: str,
        expected_revision: int,
        expected_row_min: int,
        expected_row_max: int,
        preview_affected_rows: int,
        backup_evidence_revision: str,
        backup_fingerprint: str,
    ) -> str:
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        issued_at = now.astimezone(UTC)
        return self.sign(
            {
                "kind": "operator-preview-v1",
                "receipt_id": str(uuid4()),
                "issued_at": issued_at.isoformat(),
                "expires_at": (issued_at + self.preview_ttl).isoformat(),
                "principal": principal,
                "session_id": session_id,
                "correlation_id": correlation_id,
                "sql_fingerprint": sql_fingerprint,
                "params_fingerprint": params_fingerprint,
                "target": target,
                "expected_revision": expected_revision,
                "expected_row_min": expected_row_min,
                "expected_row_max": expected_row_max,
                "preview_affected_rows": preview_affected_rows,
                "backup_evidence_revision": backup_evidence_revision,
                "backup_fingerprint": backup_fingerprint,
            }
        )

    def verify_preview(self, token: str, *, now: datetime) -> dict[str, Any]:
        payload = self.verify(token)
        if payload.get("kind") != "operator-preview-v1":
            raise ReceiptError("receipt is not an operator preview")
        try:
            expires_at = datetime.fromisoformat(str(payload["expires_at"]))
        except (KeyError, ValueError) as exc:
            raise ReceiptError("preview expiry is missing or invalid") from exc
        if expires_at.tzinfo is None or expires_at.utcoffset() is None:
            raise ReceiptError("preview expiry must be timezone-aware")
        if now.tzinfo is None or now.utcoffset() is None:
            raise ValueError("now must be timezone-aware")
        if now.astimezone(UTC) >= expires_at.astimezone(UTC):
            raise ReceiptError("preview receipt has expired")
        return payload

    def issue_apply(self, payload: Mapping[str, Any]) -> str:
        return self.sign({"kind": "operator-apply-v1", **dict(payload)})
