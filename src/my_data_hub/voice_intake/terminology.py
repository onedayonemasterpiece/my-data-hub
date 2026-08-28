from __future__ import annotations

import asyncio
import json
import os
import re
import tempfile
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from .errors import VoiceIntakeError

MAX_CARD_BYTES = 64 * 1024
MAX_PROMPT_CHARACTERS = 32_000
MAX_ENTRIES = 200
MAX_STATE_BYTES = 32 * 1024 * 1024
SESSION_ID_RE = re.compile(r"^voice-[0-9]{8}-[0-9]{6}-[a-f0-9]{8}$")


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

    def __init__(
        self,
        *,
        maximum_sessions: int = 512,
        state_path: Path | None = None,
    ) -> None:
        if maximum_sessions < 1:
            raise ValueError("maximum_sessions must be positive")
        self._maximum_sessions = maximum_sessions
        self._snapshots: dict[str, TerminologyContext] = {}
        self._inflight: dict[str, asyncio.Future[TerminologyContext]] = {}
        self._guard = asyncio.Lock()
        self._state_path = state_path
        self._load_state()

    def _load_state(self) -> None:
        if self._state_path is None or not self._state_path.exists():
            return
        try:
            raw = self._state_path.read_bytes()
            if len(raw) > MAX_STATE_BYTES:
                raise ValueError("state file is too large")
            value = json.loads(raw)
            if not isinstance(value, dict) or value.get("schema_version") != "1.0.0":
                raise ValueError("state envelope is invalid")
            sessions = value.get("sessions")
            if not isinstance(sessions, dict) or len(sessions) > self._maximum_sessions:
                raise ValueError("state sessions are invalid")
            for session_id, item in sessions.items():
                if not isinstance(session_id, str) or not SESSION_ID_RE.fullmatch(session_id):
                    raise ValueError("state session id is invalid")
                if not isinstance(item, dict):
                    raise ValueError("state session is invalid")
                context = TerminologyContext(
                    schema_version=_bounded_string(
                        item.get("schema_version"), field="version", maximum=32
                    ),
                    source_path=_bounded_string(item.get("source_path"), field="path"),
                    source_commit_sha=_bounded_git_sha(
                        item.get("source_commit_sha"), field="commit"
                    ),
                    source_blob_sha=_bounded_git_sha(item.get("source_blob_sha"), field="blob"),
                    status=_bounded_string(item.get("status"), field="status", maximum=20),
                    prompt=_bounded_string(
                        item.get("prompt"), field="prompt", maximum=MAX_PROMPT_CHARACTERS
                    ),
                )
                if context.source_path != "config/voice-terminology.yaml":
                    raise ValueError("state terminology path is invalid")
                if context.status != "current":
                    raise ValueError("state terminology status is invalid")
                self._snapshots[session_id] = context
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError) as exc:
            raise VoiceIntakeError(
                "voice_session_terminology_state_invalid",
                retryable=False,
                status_code=503,
            ) from exc

    def _persist_state(self) -> None:
        if self._state_path is None:
            return
        envelope = {
            "schema_version": "1.0.0",
            "sessions": {
                session_id: {
                    "schema_version": context.schema_version,
                    "source_path": context.source_path,
                    "source_commit_sha": context.source_commit_sha,
                    "source_blob_sha": context.source_blob_sha,
                    "status": context.status,
                    "prompt": context.prompt,
                }
                for session_id, context in sorted(self._snapshots.items())
            },
        }
        encoded = json.dumps(
            envelope,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
        if len(encoded) > MAX_STATE_BYTES:
            raise VoiceIntakeError(
                "voice_session_terminology_state_too_large",
                retryable=True,
                status_code=503,
            )
        try:
            self._state_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=self._state_path.parent,
                prefix=f".{self._state_path.name}.",
                delete=False,
            ) as handle:
                temporary = Path(handle.name)
                os.chmod(temporary, 0o600)
                handle.write(encoded)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self._state_path)
            os.chmod(self._state_path, 0o600)
        except OSError as exc:
            raise VoiceIntakeError(
                "voice_session_terminology_state_write_failed",
                retryable=True,
                status_code=503,
            ) from exc

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
            snapshot = await task
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
            try:
                self._persist_state()
            except VoiceIntakeError:
                self._snapshots.pop(session_id, None)
                raise
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
            previous = self._snapshots.pop(session_id, None)
            self._inflight.pop(session_id, None)
            try:
                self._persist_state()
            except VoiceIntakeError:
                if previous is not None:
                    self._snapshots[session_id] = previous
                raise


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


def _bounded_git_sha(value: Any, *, field: str) -> str:
    sha = _bounded_string(value, field=field, maximum=64)
    if not re.fullmatch(r"[0-9a-f]{40,64}", sha):
        raise VoiceIntakeError(
            f"idea_hub_terminology_{field}_invalid",
            retryable=False,
            status_code=503,
        )
    return sha


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
    source_commit_sha = _bounded_git_sha(source_commit_sha, field="commit")
    source_blob_sha = _bounded_git_sha(source_blob_sha, field="blob")
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
