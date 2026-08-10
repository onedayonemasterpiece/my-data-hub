from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from my_data_hub.hashing import sha256_value

_WHITESPACE = re.compile(r"\s+")


def normalize_compact_text(value: str) -> str:
    return _WHITESPACE.sub(" ", unicodedata.normalize("NFC", value)).strip()


def _normalized_values(values: Iterable[str]) -> tuple[str, ...]:
    return tuple(sorted({item for value in values if (item := normalize_compact_text(value))}))


class SearchDocument(BaseModel):
    """Canonical compact public search representation for one actor/blogger."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: str = Field(default="blogger-search-document.v1", frozen=True)
    document_id: UUID
    representation_kind: str = Field(default="blogger_public_profile", frozen=True)
    actor_kind: str = Field(min_length=1, max_length=100)
    display_name: str = Field(min_length=1, max_length=1000)
    description: str = Field(default="", max_length=12_000)
    accounts: tuple[str, ...] = Field(default=(), max_length=100)
    geography_signals: tuple[str, ...] = Field(default=(), max_length=100)
    project_memberships: tuple[str, ...] = Field(default=(), max_length=100)

    @field_validator("actor_kind", "display_name", "description", mode="before")
    @classmethod
    def normalize_scalar(cls, value: object) -> str:
        return normalize_compact_text(str(value or ""))

    @field_validator("accounts", "geography_signals", "project_memberships", mode="before")
    @classmethod
    def normalize_lists(cls, value: object) -> tuple[str, ...]:
        if value is None:
            return ()
        if isinstance(value, str):
            raise ValueError("document list fields must be arrays, not strings")
        return _normalized_values(str(item) for item in value)  # type: ignore[union-attr]

    def compact_text(self) -> str:
        sections = [
            f"name: {self.display_name}",
            f"actor_kind: {self.actor_kind}",
        ]
        if self.description:
            sections.append(f"description: {self.description}")
        if self.accounts:
            sections.append("accounts: " + " | ".join(self.accounts))
        if self.geography_signals:
            sections.append("geography: " + " | ".join(self.geography_signals))
        if self.project_memberships:
            sections.append("projects: " + " | ".join(self.project_memberships))
        return "\n".join(sections)

    @property
    def document_hash(self) -> str:
        return sha256_value(
            {
                "schema_version": self.schema_version,
                "document_id": str(self.document_id),
                "representation_kind": self.representation_kind,
                "text": self.compact_text(),
            }
        )
