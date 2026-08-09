from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from my_data_hub.hashing import sha256_value

SyncDecision = Literal["no_change", "import", "push", "conflict", "tombstone"]


@dataclass(frozen=True, slots=True)
class JoplinNoteSnapshot:
    note_id: str
    title: str
    body: str
    updated_time: int
    deleted_time: int = 0

    @property
    def content_hash(self) -> str:
        return sha256_value({"title": self.title, "body": self.body})


def decide_sync(
    snapshot: JoplinNoteSnapshot,
    *,
    last_joplin_hash: str | None,
    last_hub_revision: int | None,
    hub_changed_since_revision: bool,
) -> SyncDecision:
    if snapshot.deleted_time:
        return "tombstone"
    joplin_changed = last_joplin_hash is None or snapshot.content_hash != last_joplin_hash
    if joplin_changed and hub_changed_since_revision:
        return "conflict"
    if joplin_changed:
        return "import"
    if hub_changed_since_revision and last_hub_revision is not None:
        return "push"
    return "no_change"
