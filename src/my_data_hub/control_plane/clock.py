from __future__ import annotations

import asyncio
import threading
from datetime import UTC, datetime, timedelta
from typing import Protocol


class Clock(Protocol):
    """Injectable wall-clock boundary used by control-plane state machines."""

    def now(self) -> datetime: ...

    async def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(UTC)

    async def sleep(self, seconds: float) -> None:
        await asyncio.sleep(seconds)


class DeterministicClock:
    """Thread-safe clock whose sleeps advance virtual time without blocking."""

    def __init__(self, initial: datetime | None = None) -> None:
        initial = initial or datetime(2026, 1, 1, tzinfo=UTC)
        if initial.tzinfo is None:
            raise ValueError("deterministic clock requires a timezone-aware initial time")
        self._current = initial.astimezone(UTC)
        self._lock = threading.Lock()

    def now(self) -> datetime:
        with self._lock:
            return self._current

    def advance(self, seconds: float = 0, *, delta: timedelta | None = None) -> datetime:
        if seconds < 0 or (delta is not None and delta.total_seconds() < 0):
            raise ValueError("clock cannot move backwards")
        increment = delta if delta is not None else timedelta(seconds=seconds)
        with self._lock:
            self._current += increment
            return self._current

    async def sleep(self, seconds: float) -> None:
        self.advance(seconds)
        await asyncio.sleep(0)
