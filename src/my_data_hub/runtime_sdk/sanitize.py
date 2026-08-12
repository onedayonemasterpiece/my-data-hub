from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

_SENSITIVE_NORMALIZED_KEY_FRAGMENTS = (
    "accesstoken",
    "apikey",
    "authorization",
    "authtoken",
    "clientsecret",
    "cookie",
    "credential",
    "passwd",
    "password",
    "privatekey",
    "refreshtoken",
    "secret",
    "sessionid",
    "setcookie",
    "token",
)
_LABELLED_SECRET = re.compile(
    r"(?i)(\b(?:authorization|bearer|cookie|credential|password|passwd|secret|token|"
    r"api[_-]?key|client[_-]?secret)\b"
    r"\s*[:=]\s*)([^\s,;]+)"
)
_AUTH_SCHEME = re.compile(r"(?i)\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+")
_JWT = re.compile(r"(?<![A-Za-z0-9_-])[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]{8,}(?![A-Za-z0-9_-])")
_PEM_PRIVATE_KEY = re.compile(
    r"-----BEGIN [^-\r\n]*PRIVATE KEY-----.*?-----END [^-\r\n]*PRIVATE KEY-----",
    re.DOTALL,
)


def _normalized_key(value: object) -> str:
    return "".join(character for character in str(value).casefold() if character.isalnum())


def _sensitive_key(value: object) -> bool:
    normalized = _normalized_key(value)
    return any(fragment in normalized for fragment in _SENSITIVE_NORMALIZED_KEY_FRAGMENTS)


def _sanitize_locator(value: str) -> str:
    """Remove user-info and sensitive query values while retaining locator diagnostics."""

    try:
        parsed = urlsplit(value)
    except ValueError:
        return value
    if not parsed.scheme or not parsed.netloc:
        return value
    try:
        hostname = parsed.hostname
        port = parsed.port
    except ValueError:
        return value
    if hostname is None:
        return value
    netloc = hostname
    if ":" in hostname and not hostname.startswith("["):
        netloc = f"[{hostname}]"
    if port is not None:
        netloc = f"{netloc}:{port}"
    if parsed.username is not None or parsed.password is not None:
        netloc = f"[REDACTED]@{netloc}"
    query = []
    for key, nested in parse_qsl(parsed.query, keep_blank_values=True):
        query.append((key, "[REDACTED]" if _sensitive_key(key) or key.casefold() == "sig" else nested))
    fragment = parsed.fragment
    if "=" in fragment:
        fragment = urlencode(
            [
                (key, "[REDACTED]" if _sensitive_key(key) or key.casefold() == "sig" else nested)
                for key, nested in parse_qsl(fragment, keep_blank_values=True)
            ]
        )
    return urlunsplit((parsed.scheme, netloc, parsed.path, urlencode(query), fragment))


def sanitize_text(value: str, *, secrets: Sequence[str] = ()) -> str:
    """Redact known and recognizable credentials from any serialized string surface."""

    # Parse locators before inserting the bracketed redaction marker into
    # user-info; otherwise a standards-compliant URL parser can interpret the
    # marker as malformed IPv6 syntax and skip query redaction.
    sanitized = _sanitize_locator(value)
    for secret in sorted((item for item in secrets if item), key=len, reverse=True):
        sanitized = sanitized.replace(secret, "[REDACTED]")
    sanitized = _PEM_PRIVATE_KEY.sub("[REDACTED PRIVATE KEY]", sanitized)
    sanitized = _AUTH_SCHEME.sub(lambda match: f"{match.group(1)} [REDACTED]", sanitized)
    sanitized = _LABELLED_SECRET.sub(lambda match: f"{match.group(1)}[REDACTED]", sanitized)
    sanitized = _JWT.sub("[REDACTED JWT]", sanitized)
    return sanitized


def sanitize(value: Any, *, secrets: Sequence[str] = ()) -> Any:
    """Return a JSON-compatible copy with secret-bearing keys and values redacted."""

    known = tuple(secret for secret in secrets if secret)
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        redacted = 0
        for key, nested in value.items():
            if _sensitive_key(key):
                redacted += 1
                continue
            sanitized_key = sanitize_text(str(key), secrets=known)
            if sanitized_key in result:
                redacted += 1
                continue
            result[sanitized_key] = sanitize(nested, secrets=known)
        if redacted:
            existing = result.get("redacted_fields", 0)
            result["redacted_fields"] = (existing if isinstance(existing, int) else 0) + redacted
        return result
    if isinstance(value, (list, tuple)):
        return [sanitize(nested, secrets=known) for nested in value]
    if isinstance(value, str):
        return sanitize_text(value, secrets=known)
    if value is None or isinstance(value, (bool, int, float)):
        return value
    return sanitize_text(str(value), secrets=known)
