"""Bounded read facade over the sanitized blogger projection.

The broker may expose this facade to the reader profile after shared MCP wiring.
It deliberately has no caller-supplied SQL, relation, column, ordering or role.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class BloggerSearchRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    project_slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,99}$")
    query: str | None = Field(default=None, min_length=1, max_length=300)
    limit: int = Field(default=25, ge=1, le=100)
    after_name: str | None = Field(default=None, min_length=1, max_length=1000)
    after_blogger_id: UUID | None = None


@dataclass(frozen=True, slots=True)
class BloggerSearchResult:
    blogger_id: UUID
    display_name: str
    actor_kind: str
    public_description: str | None
    public_accounts: list[dict[str, str | None]]
    requires_review: bool


class BloggerDiscoveryReader:
    """Run one fixed keyset-paginated reader query."""

    @staticmethod
    def search(cursor: Any, request: BloggerSearchRequest) -> tuple[BloggerSearchResult, ...]:
        if (request.after_name is None) != (request.after_blogger_id is None):
            raise ValueError("both reader cursor fields are required together")
        rows = cursor.execute(
            """
            SELECT b.blogger_id,b.display_name,b.actor_kind,b.public_description,
                   b.public_accounts,b.requires_review
            FROM hub.bloggers_v1 b
            JOIN hub.project p ON p.project_id=b.project_id
            WHERE p.slug=%s
              AND (%s::text IS NULL OR b.display_name ILIKE '%%' || %s || '%%'
                   OR b.public_accounts::text ILIKE '%%' || %s || '%%')
              AND (%s::text IS NULL OR (lower(b.display_name),b.blogger_id) >
                   (lower(%s),%s::uuid))
            ORDER BY lower(b.display_name),b.blogger_id
            LIMIT %s
            """,
            (
                request.project_slug,
                request.query,
                request.query,
                request.query,
                request.after_name,
                request.after_name,
                request.after_blogger_id,
                request.limit,
            ),
        ).fetchall()
        return tuple(
            BloggerSearchResult(
                blogger_id=UUID(str(row["blogger_id"])),
                display_name=str(row["display_name"]),
                actor_kind=str(row["actor_kind"]),
                public_description=(
                    str(row["public_description"])
                    if row["public_description"] is not None
                    else None
                ),
                public_accounts=list(row["public_accounts"]),
                requires_review=bool(row["requires_review"]),
            )
            for row in rows
        )
