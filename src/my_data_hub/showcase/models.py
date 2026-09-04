from __future__ import annotations

import re
from datetime import UTC, datetime
from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

_ID_PATTERN = r"^[a-z0-9][a-z0-9-]{1,62}[a-z0-9]$"
Tone = Literal["blue", "green", "orange", "purple", "red", "neutral"]
Visibility = Literal["public", "partner"]
CapabilityType = Literal["technical", "product", "business"]
FilterName = Literal["audience", "category", "maturity", "capabilityType"]


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class Label(StrictModel):
    id: str = Field(pattern=_ID_PATTERN)
    label: str = Field(min_length=1, max_length=80)


class Category(Label):
    icon: str = Field(default="✦", min_length=1, max_length=8)


class StatusLabel(Label):
    tone: Tone = "neutral"


class Contact(StrictModel):
    title: str = Field(default="Обсудить задачу", min_length=1, max_length=80)
    label: str = Field(default="Telegram", min_length=1, max_length=40)
    value: str = Field(default="@confidentmax", min_length=1, max_length=120)
    href: str = Field(default="https://t.me/confidentmax", min_length=5, max_length=500)

    @field_validator("href")
    @classmethod
    def validate_href(cls, value: str) -> str:
        if any(ord(char) < 33 for char in value) or "\\" in value:
            raise ValueError("contact must be an absolute HTTPS URL or a tel number")
        if value.startswith("tel:"):
            if not re.fullmatch(r"tel:\+?[0-9()-]{5,32}", value):
                raise ValueError("invalid telephone number")
            return value
        parts = urlsplit(value)
        if parts.scheme != "https" or not parts.hostname or parts.username or parts.password:
            raise ValueError("contact must be an absolute HTTPS URL without credentials")
        return value


class ShowcaseItem(StrictModel):
    schema_version: Literal[1] = 1
    id: str = Field(pattern=_ID_PATTERN)
    title: str = Field(min_length=3, max_length=120, description="The reader's working task, not a technology name.")
    summary: str = Field(min_length=10, max_length=220, description="A short concrete deliverable.")
    audience: Label
    category: Category
    capability_type: CapabilityType | None = None
    maturity: StatusLabel
    effort: StatusLabel
    benefit: str = Field(min_length=10, max_length=500, description="What the partner receives; shown in the list.")
    for_whom: list[str] = Field(min_length=1, max_length=8)
    available: list[str] = Field(min_length=1, max_length=10, description="Only actually verified capabilities.")
    requirements: list[str] = Field(min_length=1, max_length=10, description="Inputs and essential limits.")
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
            self.title, self.summary, self.audience.label, self.category.label,
            self.capability_type or "", self.maturity.label, self.effort.label,
            self.benefit, *self.for_whom, *self.available, *self.requirements,
        ]
        return {
            "id": self.id, "title": self.title, "summary": self.summary,
            "audience": self.audience.model_dump(), "category": self.category.model_dump(),
            "capability_type": self.capability_type, "maturity": self.maturity.model_dump(),
            "effort": self.effort.model_dump(), "benefit": self.benefit,
            "for_whom": self.for_whom, "available": self.available, "requirements": self.requirements,
            "featured": self.featured, "contact": contact.model_dump(),
            "search_text": " ".join(search_parts).casefold(),
        }


class ShowcaseWriteItem(ShowcaseItem):
    """Input schema: legacy missing types are readable, never silently authored."""

    capability_type: CapabilityType = Field(description="Required for new or changed cards.")


class ShowcaseView(StrictModel):
    schema_version: Literal[1] = 1
    id: str = Field(pattern=_ID_PATTERN)
    title: str = Field(min_length=3, max_length=100)
    subtitle: str = Field(min_length=3, max_length=220)
    access_label: str = Field(default="Доступ по ссылке", min_length=3, max_length=100)
    visibility_ceiling: Visibility = "partner"
    item_ids: list[str] = Field(
        min_length=1, max_length=100,
        description="Canonical order. Existing card IDs are resolved by the server; do not resend them in items.",
    )
    contact: Contact = Field(default_factory=Contact)
    contacts: list[Contact] = Field(default_factory=list, max_length=6)
    filters: list[FilterName] = Field(default_factory=lambda: ["audience", "category", "maturity"], max_length=4)

    @field_validator("item_ids")
    @classmethod
    def validate_item_ids(cls, values: list[str]) -> list[str]:
        if len(values) != len(set(values)):
            raise ValueError("item_ids must be unique")
        if any(not re.fullmatch(_ID_PATTERN, value) for value in values):
            raise ValueError("invalid item id")
        return values

    @field_validator("filters")
    @classmethod
    def unique_filters(cls, values: list[FilterName]) -> list[FilterName]:
        if len(values) != len(set(values)):
            raise ValueError("filters must be unique")
        return values

    @field_validator("contacts")
    @classmethod
    def unique_contacts(cls, values: list[Contact]) -> list[Contact]:
        if len({value.href for value in values}) != len(values):
            raise ValueError("contacts must be unique")
        return values

    def effective_contacts(self) -> list[Contact]:
        return self.contacts or [self.contact]


class ShowcaseViewInput(ShowcaseView):
    """The outer view_id supplies identity; id is accepted for old clients only."""

    id: str | None = Field(default=None, pattern=_ID_PATTERN, description="Optional legacy field; prefer outer view_id.")


class ShowcaseBundle(StrictModel):
    schema_version: Literal[1] = 1
    source_revision: str = Field(min_length=8, max_length=128)
    view: ShowcaseView
    items: list[ShowcaseItem]

    @model_validator(mode="after")
    def enforce_publication_policy(self, info: ValidationInfo) -> ShowcaseBundle:
        by_id = {item.id: item for item in self.items}
        if len(by_id) != len(self.items):
            raise ValueError("duplicate item ids")
        missing = [item_id for item_id in self.view.item_ids if item_id not in by_id]
        if missing:
            raise ValueError(f"view references missing items: {', '.join(missing)}")
        # This context is private to source reading/saving, not an MCP input field.
        if info.context and info.context.get("allow_drafts"):
            return self
        order = {"public": 0, "partner": 1}
        for item_id in self.view.item_ids:
            item = by_id[item_id]
            if item.publish_state != "ready":
                raise ValueError(f"item {item_id} is not ready for publication")
            if order[item.visibility] > order[self.view.visibility_ceiling]:
                raise ValueError(f"item {item_id} exceeds view visibility ceiling")
        return self

    def published(self) -> dict[str, object]:
        # Revalidate even a draft-read/model_copy bundle at the publication boundary.
        ShowcaseBundle.model_validate(self.model_dump())
        by_id = {item.id: item for item in self.items}
        ordered = [by_id[item_id] for item_id in self.view.item_ids]
        contacts = self.view.effective_contacts()
        return {
            "schema_version": 1,
            "source_revision": self.source_revision,
            "view": {
                "id": self.view.id, "title": self.view.title, "subtitle": self.view.subtitle,
                "access_label": self.view.access_label, "contact": contacts[0].model_dump(),
                "contacts": [contact.model_dump() for contact in contacts], "filters": self.view.filters,
            },
            "items": [
                {**item.published(contacts[0]), "display_number": index + 1}
                for index, item in enumerate(ordered)
            ],
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
