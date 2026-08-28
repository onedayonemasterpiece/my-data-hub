from __future__ import annotations

from dataclasses import dataclass
from typing import Any

import yaml

from .errors import VoiceIntakeError

MAX_CARD_BYTES = 64 * 1024
MAX_PROMPT_CHARACTERS = 32_000
MAX_ENTRIES = 200


@dataclass(frozen=True, slots=True)
class TerminologyContext:
    schema_version: str
    source_path: str
    source_commit_sha: str
    prompt: str


def _bounded_string(value: Any, *, field: str, maximum: int = 500) -> str:
    if not isinstance(value, str) or not value.strip() or len(value) > maximum:
        raise VoiceIntakeError(
            f"idea_hub_terminology_{field}_invalid",
            retryable=False,
            status_code=503,
        )
    return value.strip()


def _bounded_strings(value: Any, *, field: str, maximum_items: int = 50) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, list) or len(value) > maximum_items:
        raise VoiceIntakeError(
            f"idea_hub_terminology_{field}_invalid",
            retryable=False,
            status_code=503,
        )
    return [_bounded_string(item, field=field) for item in value]


def parse_terminology_card(
    text: str,
    *,
    source_path: str,
    source_commit_sha: str,
) -> TerminologyContext:
    if len(text.encode("utf-8")) > MAX_CARD_BYTES:
        raise VoiceIntakeError(
            "idea_hub_terminology_too_large",
            retryable=False,
            status_code=503,
        )
    try:
        value = yaml.safe_load(text)
    except yaml.YAMLError as exc:
        raise VoiceIntakeError(
            "idea_hub_terminology_yaml_invalid",
            retryable=False,
            status_code=503,
        ) from exc
    if not isinstance(value, dict):
        raise VoiceIntakeError(
            "idea_hub_terminology_shape_invalid",
            retryable=False,
            status_code=503,
        )
    schema_version = _bounded_string(value.get("schema_version"), field="version", maximum=32)
    _bounded_string(value.get("card_id"), field="card_id", maximum=120)
    rules = _bounded_strings(value.get("rules"), field="rules", maximum_items=30)
    entries = value.get("entries")
    if not isinstance(entries, list) or not entries or len(entries) > MAX_ENTRIES:
        raise VoiceIntakeError(
            "idea_hub_terminology_entries_invalid",
            retryable=False,
            status_code=503,
        )

    rendered = [
        f"Карточка терминологии IdeaHub, версия {schema_version}.",
        "Правила применения:",
    ]
    rendered.extend(f"- {rule}" for rule in rules)
    rendered.append("Канонические термины:")
    seen: set[str] = set()
    for raw_entry in entries:
        if not isinstance(raw_entry, dict):
            raise VoiceIntakeError(
                "idea_hub_terminology_entry_invalid",
                retryable=False,
                status_code=503,
            )
        canonical = _bounded_string(raw_entry.get("canonical"), field="canonical")
        key = canonical.casefold()
        if key in seen:
            raise VoiceIntakeError(
                "idea_hub_terminology_duplicate_canonical",
                retryable=False,
                status_code=503,
            )
        seen.add(key)
        kind = _bounded_string(raw_entry.get("kind"), field="kind", maximum=80)
        aliases = _bounded_strings(raw_entry.get("aliases"), field="aliases")
        misrecognitions = _bounded_strings(
            raw_entry.get("common_misrecognitions"), field="misrecognitions"
        )
        notes_value = raw_entry.get("notes")
        notes = (
            _bounded_string(notes_value, field="notes", maximum=1_000)
            if notes_value is not None
            else ""
        )
        line = f"- {canonical} [{kind}]"
        if aliases:
            line += f"; допустимые формы: {', '.join(aliases)}"
        if misrecognitions:
            line += f"; частые ошибки распознавания: {', '.join(misrecognitions)}"
        if notes:
            line += f"; примечание: {notes}"
        rendered.append(line)

    prompt = "\n".join(rendered).strip()
    if len(prompt) > MAX_PROMPT_CHARACTERS:
        raise VoiceIntakeError(
            "idea_hub_terminology_prompt_too_large",
            retryable=False,
            status_code=503,
        )
    return TerminologyContext(
        schema_version=schema_version,
        source_path=source_path,
        source_commit_sha=source_commit_sha,
        prompt=prompt,
    )


__all__ = ["TerminologyContext", "parse_terminology_card"]
