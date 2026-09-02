from __future__ import annotations

from datetime import UTC, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$"
Tone = Literal["blue", "green", "orange", "purple", "red", "neutral"]
Visibility = Literal["public", "partner"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Label(StrictModel):
    id: str = Field(pattern=_ID_PATTERN)
    label: str = Field(min_length=1, max_length=80)


class Category(Label):
    icon: str = Field(min_length=1, max_length=8)


class StatusLabel(Label):
    tone: Tone


class Contact(StrictModel):
    title: str = Field(default="Как обсудить", min_length=1, max_length=80)
    label: str = Field(default="Telegram", min_length=1, max_length=40)
    value: str = Field(min_length=1, max_length=120)
    href: str = Field(pattern=r"^https://")


class ShowcaseItem(StrictModel):
    schema_version: Literal[1] = 1
    id: str = Field(pattern=_ID_PATTERN)
    title: str = Field(min_length=3, max_length=120)
    summary: str = Field(min_length=10, max_length=220)
    audience: Label
    category: Category
    maturity: StatusLabel
    effort: StatusLabel
    benefit: str = Field(min_length=10, max_length=500)
    for_whom: list[str] = Field(min_length=1, max_length=8)
    available: list[str] = Field(min_length=1, max_length=10)
    requirements: list[str] = Field(min_length=1, max_length=10)
    visibility: Visibility = "partner"
    publish_state: Literal["draft", "ready"] = "draft"
    featured: bool = False

    @field_validator("for_whom", "available", "requirements")
    @classmethod
    def validate_text_list(cls, values: list[str]) -> list[str]:
        cleaned = [value.strip() for value in values]
        if any(not value or len(value) > 160 for value in cleaned):
            raise ValueError("list entries must contain 1-160 characters")
        if len(cleaned) != len(set(cleaned)):
            raise ValueError("list entries must be unique")
        return cleaned

    def published(self, contact: Contact) -> dict[str, object]:
        search_parts = [
            self.title,
            self.summary,
            self.audience.label,
            self.category.label,
            self.maturity.label,
            self.effort.label,
            self.benefit,
            *self.for_whom,
            *self.available,
            *self.requirements,
        ]
        return {
            "id": self.id,
            "title": self.title,
            "summary": self.summary,
            "audience": self.audience.model_dump(),
            "category": self.category.model_dump(),
            "maturity": self.maturity.model_dump(),
            "effort": self.effort.model_dump(),
            "benefit": self.benefit,
            "for_whom": self.for_whom,
            "available": self.available,
            "requirements": self.requirements,
            "featured": self.featured,
            "contact": contact.model_dump(),
            "search_text": " ".join(search_parts).casefold(),
        }


class ShowcaseView(StrictModel):
    schema_version: Literal[1] = 1
    id: str = Field(pattern=_ID_PATTERN)
    title: str = Field(min_length=3, max_length=100)
    subtitle: str = Field(min_length=3, max_length=220)
    access_label: str = Field(default="Доступ по секретной ссылке", min_length=3, max_length=100)
    visibility_ceiling: Visibility = "partner"
    item_ids: list[str] = Field(min_length=1, max_length=100)
    contact: Contact

    @field_validator("item_ids")
    @classmethod
    def validate_item_ids(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("item_ids must be unique")
        for value in values:
            if not value or len(value) > 64:
                raise ValueError("invalid item id")
        return values


class ShowcaseBundle(StrictModel):
    schema_version: Literal[1] = 1
    source_revision: str = Field(min_length=8, max_length=128)
    view: ShowcaseView
    items: list[ShowcaseItem]

    @model_validator(mode="after")
    def enforce_publication_policy(self) -> "ShowcaseBundle":
        by_id = {item.id: item for item in self.items}
        if len(by_id) != len(self.items):
            raise ValueError("duplicate item ids")
        missing = [item_id for item_id in self.view.item_ids if item_id not in by_id]
        if missing:
            raise ValueError(f"view references missing items: {', '.join(missing)}")
        order = {"public": 0, "partner": 1}
        for item_id in self.view.item_ids:
            item = by_id[item_id]
            if item.publish_state != "ready":
                raise ValueError(f"item {item_id} is not ready for publication")
            if order[item.visibility] > order[self.view.visibility_ceiling]:
                raise ValueError(f"item {item_id} exceeds view visibility ceiling")
        return self

    def published(self) -> dict[str, object]:
        by_id = {item.id: item for item in self.items}
        ordered = [by_id[item_id] for item_id in self.view.item_ids]
        return {
            "schema_version": 1,
            "source_revision": self.source_revision,
            "view": {
                "id": self.view.id,
                "title": self.view.title,
                "subtitle": self.view.subtitle,
                "access_label": self.view.access_label,
                "contact": self.view.contact.model_dump(),
            },
            "items": [item.published(self.view.contact) for item in ordered],
        }


class BuildReceipt(StrictModel):
    schema_version: Literal[1] = 1
    view_id: str
    source_revision: str
    slug: str
    url: str
    tree_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    file_count: int = Field(ge=1)
    html_count: int = Field(ge=1)
    built_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class SurfaceState(StrictModel):
    view_id: str = Field(pattern=_ID_PATTERN)
    slug: str = Field(pattern=r"^[A-Za-z0-9_-]{20,80}$")
    active: bool = True
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_build: BuildReceipt | None = None


class RegistryState(StrictModel):
    schema_version: Literal[1] = 1
    surfaces: dict[str, SurfaceState] = Field(default_factory=dict)
