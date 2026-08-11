from __future__ import annotations

import json
import os
import threading
from collections.abc import Iterator, Mapping
from pathlib import Path
from typing import Any


class JsonlEventSpool:
    """Append-only, fsync-backed notebook-local delivery journal."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._lock = threading.Lock()
        self.path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
        os.chmod(self.path.parent, 0o700)
        if self.path.exists() and self.path.is_symlink():
            raise ValueError("runtime event spool may not be a symbolic link")
        self.path.touch(mode=0o600, exist_ok=True)
        os.chmod(self.path, 0o600)

    def append_event(self, event: Mapping[str, Any]) -> None:
        self._append({"record": "event", "event": dict(event)})

    def acknowledge(self, event_id: str, delivered_at: str) -> None:
        self._append({"record": "delivered", "event_id": event_id, "delivered_at": delivered_at})

    def _append(self, record: Mapping[str, Any]) -> None:
        encoded = json.dumps(record, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n"
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())

    def records(self) -> Iterator[dict[str, Any]]:
        with self._lock, self.path.open(encoding="utf-8") as handle:
            lines = tuple(handle)
        for line_number, line in enumerate(lines, 1):
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid runtime JSONL record at line {line_number}") from exc
            if not isinstance(record, dict):
                raise ValueError(f"runtime JSONL record at line {line_number} is not an object")
            yield record

    def pending(self) -> list[dict[str, Any]]:
        events: dict[str, dict[str, Any]] = {}
        delivered: set[str] = set()
        for record in self.records():
            if record.get("record") == "event":
                event = record.get("event")
                if not isinstance(event, dict) or not isinstance(event.get("event_id"), str):
                    raise ValueError("runtime JSONL event record is malformed")
                events.setdefault(event["event_id"], event)
            elif record.get("record") == "delivered" and isinstance(record.get("event_id"), str):
                delivered.add(record["event_id"])
        return [event for event_id, event in events.items() if event_id not in delivered]

    def highest_local_sequence(self) -> int:
        highest = 0
        for record in self.records():
            event = record.get("event")
            if record.get("record") == "event" and isinstance(event, dict):
                sequence = event.get("local_sequence")
                if isinstance(sequence, int):
                    highest = max(highest, sequence)
        return highest
