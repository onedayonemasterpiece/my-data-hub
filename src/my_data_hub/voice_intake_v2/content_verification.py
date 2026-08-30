from __future__ import annotations

import re
from typing import Any

MIN_ALPHANUMERIC_PER_SPEECH_SECOND = 0.75
MIN_WORDS_PER_SPEECH_MINUTE = 10.0


def transcript_plausibility(transcript: str, expected_speech_ms: int) -> dict[str, Any]:
    """Return deterministic, content-free duration-normalized evidence."""
    if expected_speech_ms <= 0:
        raise ValueError("expected speech duration must be positive")
    transcript_characters = len(transcript)
    alphanumeric_characters = sum(character.isalnum() for character in transcript)
    transcript_words = len(re.findall(r"[^\W_]+", transcript, flags=re.UNICODE))
    speech_seconds = expected_speech_ms / 1000
    return {
        "expected_speech_ms": expected_speech_ms,
        "transcript_characters": transcript_characters,
        "transcript_words": transcript_words,
        "alphanumeric_characters": alphanumeric_characters,
        "alphanumeric_per_speech_second": round(alphanumeric_characters / speech_seconds, 6),
        "words_per_speech_minute": round(transcript_words / speech_seconds * 60, 6),
    }


def transcript_plausibility_passed(evidence: dict[str, Any]) -> bool:
    return bool(
        evidence.get("alphanumeric_per_speech_second", 0)
        >= MIN_ALPHANUMERIC_PER_SPEECH_SECOND
        and evidence.get("words_per_speech_minute", 0) >= MIN_WORDS_PER_SPEECH_MINUTE
    )


__all__ = [
    "MIN_ALPHANUMERIC_PER_SPEECH_SECOND",
    "MIN_WORDS_PER_SPEECH_MINUTE",
    "transcript_plausibility",
    "transcript_plausibility_passed",
]
