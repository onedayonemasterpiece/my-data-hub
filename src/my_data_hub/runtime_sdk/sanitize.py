from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

_SENSITIVE_KEY_FRAGMENTS = (
    "authorization",
    "credential",
    "password",
    "private_key",
    "secret",
    "token",
)


def sanitize(value: Any, *, secrets: Sequence[str] = ()) -> Any:
    """Return a JSON-compatible copy with secret-bearing fields removed."""

    known = tuple(secret for secret in secrets if secret)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        redacted = 0
        for key, nested in value.items():
            if any(fragment in str(key).lower() for fragment in _SENSITIVE_KEY_FRAGMENTS):
                redacted += 1
                continue
            result[str(key)] = sanitize(nested, secrets=known)
        if redacted:
            result["redacted_fields"] = int(result.get("redacted_fields", 0)) + redacted
        return result
    if isinstance(value, (list, tuple)):
        return [sanitize(nested, secrets=known) for nested in value]
    if isinstance(value, str):
        sanitized = value
        for secret in known:
            sanitized = sanitized.replace(secret, "[REDACTED]")
        return sanitized
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return str(value)
