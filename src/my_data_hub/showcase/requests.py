"""Shared, secret-free request semantics for the MCP edge and private runtime."""

from __future__ import annotations

import json
import re
from typing import Any, Literal

ShowcaseMode = Literal["preview", "save", "publish"]
MAX_ARGUMENT_BYTES = 131_072
MAX_REQUEST_BYTES = 262_144

# Messages are constants: never interpolate input values, provider errors or credentials.
PROBLEMS = {
    "VIEW_REQUIRED": (
        "A complete view is required to create a showcase.",
        "Pass view with title, subtitle and item_ids.",
    ),
    "VIEW_EXISTS": ("The showcase view already exists.", "Read showcase.get_source and update with showcase.apply."),
    "VIEW_NOT_FOUND": ("The showcase view does not exist.", "Use showcase.create_view with a complete view."),
    "REVISION_REQUIRED": (
        "An expected source revision is required for an update.",
        "Copy source_revision from showcase.get_source.",
    ),
    "REVISION_CONFLICT": (
        "The source revision changed.",
        "Read showcase.get_source, review the changes and retry with its revision.",
    ),
    "VIEW_ID_MISMATCH": ("The view ID does not match view_id.", "Omit view.id or make it equal to view_id."),
    "ITEM_NOT_FOUND": (
        "The view references items not included in the source or proposed bundle.",
        "Correct the item ID or supply its complete definition in items.",
    ),
    "DUPLICATE_ITEM": ("Item definitions must have unique IDs.", "Supply each item once; item_ids defines the order."),
    "UNREFERENCED_ITEM": (
        "An item definition is not referenced by this view.",
        "Remove the definition or add its ID to view.item_ids.",
    ),
    "ITEM_ID_CONFLICT": (
        "This item ID already belongs to a different existing card.",
        "Give the adapted card a new ID; creating a view never overwrites existing cards.",
    ),
    "SHARED_ITEM": (
        "This card is used by another showcase.",
        "Create an adapted card with a new ID and replace the ID in this view.",
    ),
    "CAPABILITY_TYPE_REQUIRED": (
        "New or changed cards require capability_type.",
        "Choose technical, product or business; unchanged legacy cards can be reused.",
    ),
    "ITEM_NOT_READY": (
        "A referenced card is a draft.",
        "Use mode=save, or explicitly mark the reviewed card publish_state=ready.",
    ),
    "VISIBILITY_EXCEEDED": (
        "A card exceeds the view visibility ceiling.",
        "Keep the view partner-only or remove the restricted card.",
    ),
    "INVALID_MODE": (
        "Write mode is invalid or conflicts with legacy flags.",
        "Use only mode=preview, save or publish; do not mix mode with dry_run or publish.",
    ),
    "IDEMPOTENCY_REQUIRED": (
        "A write requires an idempotency key of 8-200 safe characters.",
        "Use one unique key per write; preview needs no key. Reuse it only for an identical retry.",
    ),
    "IDEMPOTENCY_CONFLICT": (
        "This key was already used with different arguments.",
        "Use a new key for a different write; retry an identical write with the original key.",
    ),
    "REQUEST_TOO_LARGE": (
        "Showcase arguments exceed the 128 KiB UTF-8 limit.",
        "Reuse existing cards by item_ids; save a smaller draft, then add cards in bounded updates.",
    ),
    "INVALID_FIELD": ("A field is missing or invalid.", "Correct the indicated field using the tool input schema."),
    "SOURCE_UNAVAILABLE": (
        "The source could not be read or written.",
        "Do not change the payload blindly; check source connectivity and credentials.",
    ),
    "WRITE_UNAVAILABLE": (
        "The source writer is not configured.",
        "Ask the operator to configure the bounded Showcase writer.",
    ),
    "PUBLICATION_FAILED": (
        "Source was saved but publication failed.",
        "Retry showcase.rebuild; do not create a second view.",
    ),
    "READBACK_FAILED": (
        "Source was committed but exact readback could not be verified.",
        "Keep new_source_revision; read showcase.get_source before retrying or rebuilding.",
    ),
}


class ShowcaseSourceError(RuntimeError):
    """Source failure; raw details are for local debugging, not public responses."""


class ShowcaseRequestError(ShowcaseSourceError):
    def __init__(self, code: str, field: str | None = None) -> None:
        if code not in PROBLEMS:
            raise ValueError("unknown Showcase problem code")
        self.code = code
        self.field = field
        super().__init__(json.dumps(self.payload(), ensure_ascii=False))

    def payload(self) -> dict[str, Any]:
        message, next_action = PROBLEMS[self.code]
        return {"code": self.code, "field": self.field, "message": message, "next_action": next_action}

    @property
    def http_status(self) -> int:
        if self.code in {"VIEW_EXISTS", "REVISION_CONFLICT", "IDEMPOTENCY_CONFLICT", "ITEM_ID_CONFLICT", "SHARED_ITEM"}:
            return 409
        if self.code in {"VIEW_NOT_FOUND", "ITEM_NOT_FOUND"}:
            return 404
        if self.code == "REQUEST_TOO_LARGE":
            return 413
        if self.code in {"SOURCE_UNAVAILABLE", "WRITE_UNAVAILABLE"}:
            return 503
        return 400


def safe_problem(value: Any) -> dict[str, Any] | None:
    """Reconstruct an error from an allowlist instead of forwarding arbitrary text."""
    if not isinstance(value, dict) or not isinstance(value.get("code"), str) or value["code"] not in PROBLEMS:
        return None
    field = value.get("field")
    if not isinstance(field, str) or not re.fullmatch(r"[A-Za-z0-9_.\[\]-]{1,160}", field):
        field = None
    return ShowcaseRequestError(value["code"], field).payload()


def resolve_mode(
    mode: ShowcaseMode | None,
    dry_run: bool | None = None,
    publish: bool | None = None,
    *,
    default: ShowcaseMode = "preview",
) -> ShowcaseMode:
    if mode is not None:
        if mode not in {"preview", "save", "publish"} or dry_run is not None or publish is not None:
            raise ShowcaseRequestError("INVALID_MODE", "mode")
        return mode
    if dry_run is not None and not isinstance(dry_run, bool):
        raise ShowcaseRequestError("INVALID_MODE", "dry_run")
    if publish is not None and not isinstance(publish, bool):
        raise ShowcaseRequestError("INVALID_MODE", "publish")
    # Preserve the old apply defaults: publish=True alone never bypasses dry-run.
    if dry_run is None and publish is None:
        return default
    if dry_run is not False:
        return "preview"
    return "publish" if publish else "save"
