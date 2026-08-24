from __future__ import annotations

import hashlib
import ipaddress
import json
import re
import unicodedata
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

TRACKING_QUERY_KEYS = {
    "fbclid",
    "gclid",
    "yclid",
    "ysclid",
    "mc_cid",
    "mc_eid",
    "ref",
    "ref_src",
}


class TransformationContractError(ValueError):
    pass


def canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str
    ).encode("utf-8")


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value)).hexdigest()


def stable_id(prefix: str, *parts: Any, length: int = 24) -> str:
    payload = "\0".join(str(part or "").strip().lower() for part in parts)
    return prefix + hashlib.sha256(payload.encode("utf-8")).hexdigest()[:length]


def normalize_exact_text(value: Any) -> str:
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).casefold().split())


def _forbidden_host(host: str) -> bool:
    value = (host or "").strip(".[]").lower()
    if not value or value == "localhost" or value.endswith(
        (".localhost", ".local", ".internal")
    ):
        return True
    try:
        address = ipaddress.ip_address(value)
    except ValueError:
        return False
    return bool(
        address.is_private
        or address.is_loopback
        or address.is_link_local
        or address.is_multicast
        or address.is_reserved
        or address.is_unspecified
    )


def canonicalize_http_url(value: Any) -> str:
    raw = str(value or "").strip()
    if not raw:
        raise TransformationContractError("URL is required")
    try:
        parsed = urlsplit(raw)
    except ValueError as exc:
        raise TransformationContractError("invalid URL") from exc
    scheme = parsed.scheme.lower()
    if scheme not in {"http", "https"}:
        raise TransformationContractError("URL scheme must be http or https")
    host = (parsed.hostname or "").lower()
    if _forbidden_host(host):
        raise TransformationContractError("URL host is local, private, or reserved")
    try:
        port = parsed.port
    except ValueError as exc:
        raise TransformationContractError("invalid URL port") from exc
    if port not in {None, 80, 443}:
        raise TransformationContractError("URL uses a non-web port")
    netloc = host.encode("idna").decode("ascii")
    path = re.sub(r"/{2,}", "/", parsed.path or "/")
    if path != "/":
        path = path.rstrip("/")
    query = sorted(
        (key, item)
        for key, item in parse_qsl(parsed.query, keep_blank_values=True)
        if not key.lower().startswith("utm_")
        and key.lower() not in TRACKING_QUERY_KEYS
    )
    return urlunsplit((scheme, netloc, path, urlencode(query, doseq=True), ""))


def canonical_url_identity(value: Any) -> str:
    parsed = urlsplit(canonicalize_http_url(value))
    host = (parsed.hostname or "").lower()
    if host.startswith("www."):
        host = host[4:]
    return urlunsplit(("https", host, parsed.path, parsed.query, ""))


def normalize_doi(value: Any) -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"^(?:https?://(?:dx\.)?doi\.org/|doi:\s*)", "", raw)
    if not raw:
        return ""
    if not re.fullmatch(r"10\.\d{4,9}/\S+", raw):
        raise TransformationContractError("invalid DOI")
    return raw.rstrip(".,; ")
