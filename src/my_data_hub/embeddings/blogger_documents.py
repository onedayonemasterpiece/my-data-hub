"""Exact compact public blogger documents built only inside the ACTIVE master."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from uuid import UUID, uuid5

from my_data_hub.embeddings.documents import SearchDocument, normalize_compact_text
from my_data_hub.hashing import sha256_value

_DOCUMENT_NAMESPACE = UUID("47c1ae5a-9405-5c06-900c-b09af192900b")


class BloggerDocumentError(ValueError):
    pass


@dataclass(frozen=True, slots=True)
class CanonicalBloggerDocument:
    actor_id: UUID
    document: SearchDocument


def _public_accounts(value: object) -> tuple[str, ...]:
    if value is None:
        return ()
    if not isinstance(value, list):
        raise BloggerDocumentError("public_accounts must be an array")
    encoded: list[str] = []
    for item in value:
        if not isinstance(item, Mapping):
            raise BloggerDocumentError("public account must be an object")
        unknown = set(item) - {"platform", "handle", "url"}
        if unknown:
            raise BloggerDocumentError("public account contains an unapproved field")
        parts = [
            f"{key}={normalized}"
            for key in ("platform", "handle", "url")
            if (normalized := normalize_compact_text(str(item.get(key) or "")))
        ]
        if parts:
            encoded.append(";".join(parts))
    return tuple(sorted(set(encoded)))


def build_compact_blogger_documents(
    rows: Iterable[Mapping[str, object]], *, expected_count: int = 266
) -> tuple[CanonicalBloggerDocument, ...]:
    """Normalize the exact public projection without serializing it off master."""

    if expected_count < 1:
        raise BloggerDocumentError("expected blogger count must be positive")
    built: list[CanonicalBloggerDocument] = []
    seen: set[UUID] = set()
    for row in rows:
        if set(row) != {
            "blogger_id", "display_name", "actor_kind", "public_description",
            "geography_signal", "project_id", "public_accounts",
        }:
            raise BloggerDocumentError("blogger projection columns are not exact")
        actor_id = UUID(str(row["blogger_id"]))
        if actor_id in seen:
            raise BloggerDocumentError("blogger projection contains a duplicate actor")
        seen.add(actor_id)
        payload = {
            "actor_id": str(actor_id),
            "actor_kind": normalize_compact_text(str(row["actor_kind"] or "")),
            "display_name": normalize_compact_text(str(row["display_name"] or "")),
            "description": normalize_compact_text(str(row["public_description"] or "")),
            "accounts": _public_accounts(row["public_accounts"]),
            "geography": tuple(
                value for value in (normalize_compact_text(str(row["geography_signal"] or "")),) if value
            ),
            "projects": (str(UUID(str(row["project_id"]))),),
        }
        if not payload["actor_kind"] or not payload["display_name"]:
            raise BloggerDocumentError("blogger actor kind/name is empty")
        payload_hash = sha256_value(payload)
        document = SearchDocument(
            document_id=uuid5(_DOCUMENT_NAMESPACE, f"{actor_id}:blogger_compact_v1:{payload_hash}"),
            representation_kind="blogger_compact_v1",
            actor_kind=str(payload["actor_kind"]),
            display_name=str(payload["display_name"]),
            description=str(payload["description"]),
            accounts=payload["accounts"],
            geography_signals=payload["geography"],
            project_memberships=payload["projects"],
        )
        if len(document.compact_text().encode("utf-8")) > 32_768:
            raise BloggerDocumentError("compact blogger document exceeds PostgreSQL bound")
        built.append(CanonicalBloggerDocument(actor_id=actor_id, document=document))
    if len(built) != expected_count:
        raise BloggerDocumentError(f"blogger projection count {len(built)} != {expected_count}")
    return tuple(sorted(built, key=lambda item: str(item.actor_id)))
