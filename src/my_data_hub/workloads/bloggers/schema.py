"""Exact typed source contract for Region Talk's bounded blogger registry."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from datetime import UTC, datetime
from typing import Any, ClassVar

SOURCE_DATABASE_ID = "etnkibjidis0o6stn2cq"
SOURCE_DATABASE_PATH = "/ru-central1/b1ghfk15fpug7mn5439l/etnkibjidis0o6stn2cq"
SOURCE_TABLE = "region_talk_external_blogger_evidence"
SOURCE_SCHEMA_SHA256 = "a44855556115f90c0b6353cac0b8c5d1eaebf91485ca594c621cc73f91b2564f"
SOURCE_COLUMNS = (
    "record_id",
    "batch_id",
    "list_order",
    "level",
    "blogger_name",
    "segment",
    "region_relation_status",
    "visit_period_text",
    "locations_text",
    "confirmation_basis",
    "evidence_url",
    "telegram_url",
    "vk_public_url",
    "vk_video_url",
    "rutube_url",
    "source_kind",
    "confirmation_status",
    "pipeline_status",
    "source_file_sha256",
    "ingested_at",
    "updated_at",
    "external_region_basis",
    "external_region_evidence_url",
    "submission_batch_ids_json",
    "other_primary_url",
    "social_links_type",
    "evidence_type",
)
SOURCE_QUERY = (
    "SELECT "
    + ", ".join(f"`{name}`" for name in SOURCE_COLUMNS)
    + f" FROM `{SOURCE_TABLE}` ORDER BY `record_id`;"
)
# Owner-approved normalized query identity recovered from the exact donor schema.
SOURCE_QUERY_SHA256 = "25dc6aafe54c0b89097d0604455cbe5f240bc4ad5da0239afeda1db0867b3937"


class BloggerSourceError(ValueError):
    """A source row cannot be accepted under the exact bounded contract."""


def _text(value: object, name: str, *, required: bool, limit: int = 16_384) -> str | None:
    if value is None:
        if required:
            raise BloggerSourceError(f"{name} is required")
        return None
    if not isinstance(value, str):
        raise BloggerSourceError(f"{name} must be text")
    normalized = value.strip()
    if required and not normalized:
        raise BloggerSourceError(f"{name} is empty")
    if len(normalized.encode("utf-8")) > limit:
        raise BloggerSourceError(f"{name} exceeds {limit} UTF-8 bytes")
    return normalized or None


def _timestamp(value: object, name: str) -> datetime:
    if isinstance(value, str):
        try:
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError as exc:
            raise BloggerSourceError(f"{name} is not an ISO timestamp") from exc
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise BloggerSourceError(f"{name} must be timezone-aware")
    return value.astimezone(UTC)


@dataclass(frozen=True, slots=True)
class BloggerSourceRow:
    """All 27 source columns, with no silent unknown-field discard."""

    record_id: str
    batch_id: str
    list_order: int
    level: str
    blogger_name: str
    segment: str
    region_relation_status: str
    visit_period_text: str
    locations_text: str
    confirmation_basis: str
    evidence_url: str
    telegram_url: str | None
    vk_public_url: str | None
    vk_video_url: str | None
    rutube_url: str | None
    source_kind: str
    confirmation_status: str
    pipeline_status: str
    source_file_sha256: str
    ingested_at: datetime
    updated_at: datetime
    external_region_basis: str | None
    external_region_evidence_url: str | None
    submission_batch_ids_json: str | None
    other_primary_url: str | None
    social_links_type: str | None
    evidence_type: str | None

    MAX_SERIALIZED_BYTES: ClassVar[int] = 128 * 1024

    @classmethod
    def from_mapping(cls, value: dict[str, Any]) -> BloggerSourceRow:
        if set(value) != set(SOURCE_COLUMNS):
            raise BloggerSourceError(
                f"source columns differ: missing={sorted(set(SOURCE_COLUMNS)-set(value))}, "
                f"unknown={sorted(set(value)-set(SOURCE_COLUMNS))}"
            )
        required_text = {
            "record_id",
            "batch_id",
            "level",
            "blogger_name",
            "segment",
            "region_relation_status",
            "visit_period_text",
            "locations_text",
            "confirmation_basis",
            "evidence_url",
            "source_kind",
            "confirmation_status",
            "pipeline_status",
            "source_file_sha256",
        }
        kwargs: dict[str, Any] = {}
        for name in SOURCE_COLUMNS:
            raw = value[name]
            if name == "list_order":
                if isinstance(raw, bool) or not isinstance(raw, int) or raw < 0:
                    raise BloggerSourceError("list_order must be a non-negative integer")
                kwargs[name] = raw
            elif name in {"ingested_at", "updated_at"}:
                kwargs[name] = _timestamp(raw, name)
            else:
                kwargs[name] = _text(raw, name, required=name in required_text)
        if len(kwargs["source_file_sha256"]) != 64 or any(
            char not in "0123456789abcdef" for char in kwargs["source_file_sha256"]
        ):
            raise BloggerSourceError("source_file_sha256 must be lowercase SHA-256")
        row = cls(**kwargs)
        if len(row.canonical_bytes()) > cls.MAX_SERIALIZED_BYTES:
            raise BloggerSourceError("source row exceeds bounded serialized size")
        return row

    def payload(self) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for item in fields(self):
            value = getattr(self, item.name)
            result[item.name] = value.isoformat().replace("+00:00", "Z") if isinstance(value, datetime) else value
        return result

    def canonical_bytes(self) -> bytes:
        return json.dumps(
            self.payload(), ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.canonical_bytes()).hexdigest()


def assert_query_identity() -> None:
    """Protect the reviewed query identity from casual textual changes.

    The normalized owner receipt predates this module. The literal text is also
    hashed and exposed for receipts, but the authoritative normalized hash is the
    fixed constant above.
    """

    if tuple(field.name for field in fields(BloggerSourceRow)) != SOURCE_COLUMNS:
        raise AssertionError("dataclass and exact source column order diverged")
