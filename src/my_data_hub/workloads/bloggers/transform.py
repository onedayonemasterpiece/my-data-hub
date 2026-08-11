"""Deterministic, non-destructive transformation into shared blogger semantics."""

from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID, uuid5

from .schema import BloggerSourceRow

BLOGGER_NAMESPACE = UUID("51e81cf3-3a19-52a1-a898-e483293eb5c6")


class BloggerDisposition(StrEnum):
    NORMALIZED = "normalized"
    DEDUPLICATED = "deduplicated"
    INTENTIONALLY_EXCLUDED = "intentionally_excluded"
    RETAINED_RAW = "retained_raw"
    QUARANTINED = "quarantined"


@dataclass(frozen=True, slots=True)
class AccountProjection:
    platform: str
    url: str
    normalized_url: str
    handle: str | None


@dataclass(frozen=True, slots=True)
class BloggerProjection:
    record_id: str
    actor_id: UUID
    actor_kind: str
    display_name: str
    summary: str
    geography_signal: str
    geography_provenance: str
    source_updated_at: str
    accounts: tuple[AccountProjection, ...]
    disposition: BloggerDisposition
    reason_code: str
    requires_review: bool


def _normalized_url(value: str) -> str:
    parsed = urlsplit(value.strip())
    if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
        raise ValueError("account/evidence URL must be absolute HTTP(S)")
    host = parsed.hostname.lower() if parsed.hostname else ""
    port = parsed.port
    netloc = host if port is None else f"{host}:{port}"
    path = re.sub(r"/{2,}", "/", parsed.path).rstrip("/") or "/"
    return urlunsplit(("https", netloc, path, parsed.query, ""))


def _handle(url: str) -> str | None:
    path = urlsplit(url).path.strip("/")
    if not path:
        return None
    candidate = path.split("/")[-1].lstrip("@")
    return candidate[:256] or None


def _actor_kind(row: BloggerSourceRow) -> str:
    signal = f"{row.level} {row.segment} {row.social_links_type or ''}".casefold()
    if any(token in signal for token in ("организац", "сообществ", "издани", "agency", "community")):
        return "organisation"
    if any(token in signal for token in ("персона", "личный блог", "person", "individual")):
        return "person"
    # Never silently turn a community into a person merely because the table is
    # called blogger evidence. Unknown is a supported canonical actor kind.
    return "unknown"


def transform_row(row: BloggerSourceRow) -> BloggerProjection:
    sources = (
        ("telegram", row.telegram_url),
        ("vk", row.vk_public_url),
        ("vk_video", row.vk_video_url),
        ("rutube", row.rutube_url),
        ("other", row.other_primary_url),
    )
    accounts: list[AccountProjection] = []
    seen: set[tuple[str, str]] = set()
    for platform, raw_url in sources:
        if raw_url is None:
            continue
        try:
            normalized = _normalized_url(raw_url)
        except ValueError:
            return BloggerProjection(
                record_id=row.record_id,
                actor_id=uuid5(BLOGGER_NAMESPACE, f"ydb:{row.record_id}"),
                actor_kind="unknown",
                display_name=row.blogger_name,
                summary=row.segment,
                geography_signal=row.locations_text,
                geography_provenance=row.confirmation_basis,
                source_updated_at=row.updated_at.isoformat().replace("+00:00", "Z"),
                accounts=(),
                disposition=BloggerDisposition.RETAINED_RAW,
                reason_code="malformed_public_account_url",
                requires_review=True,
            )
        identity = (platform, normalized)
        if identity in seen:
            continue
        seen.add(identity)
        accounts.append(AccountProjection(platform, raw_url, normalized, _handle(normalized)))
    accounts.sort(key=lambda item: (item.platform, item.normalized_url))
    return BloggerProjection(
        record_id=row.record_id,
        actor_id=uuid5(BLOGGER_NAMESPACE, f"ydb:{row.record_id}"),
        actor_kind=_actor_kind(row),
        display_name=row.blogger_name,
        summary=row.segment,
        geography_signal=row.locations_text,
        geography_provenance=row.confirmation_basis,
        source_updated_at=row.updated_at.isoformat().replace("+00:00", "Z"),
        accounts=tuple(accounts),
        disposition=BloggerDisposition.NORMALIZED,
        reason_code="typed_blogger_evidence_v1",
        requires_review=row.confirmation_status != "confirmed_external" or not accounts,
    )
