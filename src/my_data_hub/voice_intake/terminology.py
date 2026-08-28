from __future__ import annotations

import asyncio
import re
from collections.abc import Awaitable, Callable
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
    source_blob_sha: str
    status: str
    prompt: str


class SessionTerminologySnapshots:
    """Pin one freshly resolved terminology snapshot to each active voice session."""

    def __init__(self, *, maximum_sessions: int = 512) -> None:
        if maximum_sessions < 1:
            raise ValueError("maximum_sessions must be positive")
        self._maximum_sessions = maximum_sessions
        self._snapshots: dict[str, TerminologyContext] = {}
        self._inflight: dict[str, asyncio.Future[TerminologyContext]] = {}
        self._guard = asyncio.Lock()

    async def begin(
        self,
        session_id: str,
        resolver: Callable[[], Awaitable[TerminologyContext]],
    ) -> TerminologyContext:
        async with self._guard:
            existing = self._snapshots.get(session_id)
            if existing is not None:
                return existing
            task = self._inflight.get(session_id)
            if task is None:
                if len(self._snapshots) + len(self._inflight) >= self._maximum_sessions:
                    raise VoiceIntakeError(
                        "voice_session_terminology_capacity_exceeded",
                        retryable=True,
                        status_code=503,
                    )
                task = asyncio.ensure_future(resolver())
                self._inflight[session_id] = task
        try:
            snapshot = await asyncio.shield(task)
        except BaseException:
            async with self._guard:
                if self._inflight.get(session_id) is task:
                    self._inflight.pop(session_id, None)
            raise
        if snapshot.status != "current":
            async with self._guard:
                if self._inflight.get(session_id) is task:
                    self._inflight.pop(session_id, None)
            raise VoiceIntakeError(
                "idea_hub_terminology_not_current",
                retryable=True,
                status_code=503,
            )
        async with self._guard:
            self._snapshots[session_id] = snapshot
            if self._inflight.get(session_id) is task:
                self._inflight.pop(session_id, None)
        return snapshot

    async def require(self, session_id: str) -> TerminologyContext:
        async with self._guard:
            snapshot = self._snapshots.get(session_id)
        if snapshot is None:
            raise VoiceIntakeError(
                "voice_session_terminology_not_initialized",
                retryable=True,
                status_code=409,
            )
        return snapshot

    async def discard(self, session_id: str) -> None:
        async with self._guard:
            self._snapshots.pop(session_id, None)
            self._inflight.pop(session_id, None)


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
    source_blob_sha: str,
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
    if not re.fullmatch(r"[0-9a-f]{40,64}", source_commit_sha):
        raise VoiceIntakeError(
            "idea_hub_terminology_commit_invalid",
            retryable=False,
            status_code=503,
        )
    if not re.fullmatch(r"[0-9a-f]{40,64}", source_blob_sha):
        raise VoiceIntakeError(
            "idea_hub_terminology_blob_invalid",
            retryable=False,
            status_code=503,
        )
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
        source_blob_sha=source_blob_sha,
        status="current",
        prompt=prompt,
    )


__all__ = [
    "SessionTerminologySnapshots",
    "TerminologyContext",
    "parse_terminology_card",
]
