from __future__ import annotations

import email.utils
import random
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any, Protocol, TypeVar

from pydantic import BaseModel, ConfigDict, Field

from .contracts import KaggleRetryExhausted, RetryClass

T = TypeVar("T")


class RandomSource(Protocol):
    def uniform(self, a: float, b: float) -> float: ...


class RetryPolicy(BaseModel):
    """Bounded transport retry policy.

    ``Retry-After`` is honored only within ``max_retry_after_seconds`` and the
    total elapsed budget. Exponential delay receives symmetric deterministic-
    injectable jitter and is capped before sleeping.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_attempts: int = Field(default=4, ge=1, le=10)
    initial_delay_seconds: float = Field(default=0.5, ge=0, le=30)
    multiplier: float = Field(default=2.0, ge=1, le=4)
    max_delay_seconds: float = Field(default=30.0, ge=0, le=300)
    max_retry_after_seconds: float = Field(default=60.0, ge=0, le=600)
    max_elapsed_seconds: float = Field(default=120.0, gt=0, le=1800)
    jitter_ratio: float = Field(default=0.2, ge=0, le=1)


class ClassifiedFailure(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    retry_class: RetryClass
    retryable: bool
    http_status: int | None = Field(default=None, ge=100, le=599)
    retry_after_seconds: float | None = Field(default=None, ge=0)


def _response(exc: BaseException) -> Any:
    return getattr(exc, "response", None)


def _status_code(exc: BaseException) -> int | None:
    value = getattr(_response(exc), "status_code", None)
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _headers(exc: BaseException) -> Any:
    headers = getattr(_response(exc), "headers", None)
    return headers if hasattr(headers, "get") else {}


def parse_retry_after(value: object, *, now: datetime) -> float | None:
    if not isinstance(value, str) or not value.strip():
        return None
    raw = value.strip()
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        parsed = email.utils.parsedate_to_datetime(raw)
    except (TypeError, ValueError, OverflowError):
        return None
    if parsed is None:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    if now.tzinfo is None or now.utcoffset() is None:
        raise ValueError("retry clock must be timezone-aware")
    return max(0.0, (parsed.astimezone(UTC) - now.astimezone(UTC)).total_seconds())


def classify_failure(exc: BaseException, *, now: datetime) -> ClassifiedFailure:
    status = _status_code(exc)
    if status == 429:
        return ClassifiedFailure(
            retry_class=RetryClass.RATE_LIMIT,
            retryable=True,
            http_status=status,
            retry_after_seconds=parse_retry_after(_headers(exc).get("Retry-After"), now=now),
        )
    if status in {408, 500, 502, 503, 504}:
        return ClassifiedFailure(retry_class=RetryClass.SERVER, retryable=True, http_status=status)
    if status == 401:
        return ClassifiedFailure(retry_class=RetryClass.AUTHENTICATION, retryable=False, http_status=status)
    if status == 403:
        return ClassifiedFailure(retry_class=RetryClass.AUTHORIZATION, retryable=False, http_status=status)
    if status == 404:
        return ClassifiedFailure(retry_class=RetryClass.NOT_FOUND, retryable=False, http_status=status)
    if status == 409:
        return ClassifiedFailure(retry_class=RetryClass.CONFLICT, retryable=False, http_status=status)
    if status is not None and 400 <= status < 500:
        return ClassifiedFailure(retry_class=RetryClass.INVALID_REQUEST, retryable=False, http_status=status)

    names = {type(item).__name__.casefold() for item in _exception_chain(exc)}
    if any("timeout" in name for name in names):
        return ClassifiedFailure(retry_class=RetryClass.TIMEOUT, retryable=True)
    if any(marker in name for name in names for marker in ("connection", "protocolerror", "chunkedencoding")):
        return ClassifiedFailure(retry_class=RetryClass.CONNECTION, retryable=True)
    return ClassifiedFailure(retry_class=RetryClass.UNKNOWN, retryable=False, http_status=status)


def _exception_chain(exc: BaseException) -> tuple[BaseException, ...]:
    result: list[BaseException] = []
    current: BaseException | None = exc
    seen: set[int] = set()
    while current is not None and id(current) not in seen and len(result) < 8:
        seen.add(id(current))
        result.append(current)
        current = current.__cause__ or current.__context__
    return tuple(result)


class BoundedRetry:
    def __init__(
        self,
        policy: RetryPolicy | None = None,
        *,
        sleep: Callable[[float], None],
        monotonic: Callable[[], float],
        wall_clock: Callable[[], datetime],
        random_source: RandomSource | None = None,
    ) -> None:
        self.policy = policy or RetryPolicy()
        self.sleep = sleep
        self.monotonic = monotonic
        self.wall_clock = wall_clock
        self.random = random_source or random.Random()

    def call(self, operation: str, fn: Callable[[], T]) -> tuple[T, int]:
        started = self.monotonic()
        last: BaseException | None = None
        for attempt in range(1, self.policy.max_attempts + 1):
            try:
                return fn(), attempt
            except BaseException as exc:
                if isinstance(exc, (KeyboardInterrupt, SystemExit)):
                    raise
                last = exc
                failure = classify_failure(exc, now=self.wall_clock())
                if not failure.retryable or attempt >= self.policy.max_attempts:
                    raise
                elapsed = max(0.0, self.monotonic() - started)
                remaining = self.policy.max_elapsed_seconds - elapsed
                delay = self._delay(attempt, failure)
                if remaining <= 0 or delay > remaining:
                    break
                self.sleep(delay)
        assert last is not None
        raise KaggleRetryExhausted(
            f"{operation} exhausted bounded retry budget after {self.policy.max_attempts} attempts"
        ) from last

    def _delay(self, attempt: int, failure: ClassifiedFailure) -> float:
        if failure.retry_after_seconds is not None:
            base = min(failure.retry_after_seconds, self.policy.max_retry_after_seconds)
        else:
            base = min(
                self.policy.initial_delay_seconds * (self.policy.multiplier ** (attempt - 1)),
                self.policy.max_delay_seconds,
            )
        spread = base * self.policy.jitter_ratio
        jittered = base + self.random.uniform(-spread, spread)
        return max(0.0, min(jittered, self.policy.max_delay_seconds, self.policy.max_retry_after_seconds))

    def decorator(self, operation: str) -> Callable[[Callable[..., T]], Callable[..., T]]:
        """Adapter for KaggleApi's internal ``with_retry`` extension point."""

        def decorate(fn: Callable[..., T]) -> Callable[..., T]:
            def wrapped(*args: object, **kwargs: object) -> T:
                result, _ = self.call(operation, lambda: fn(*args, **kwargs))
                return result

            return wrapped

        return decorate
