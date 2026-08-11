from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class PlatformStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETE = "complete"
    ERROR = "error"
    UNKNOWN = "unknown"


class TerminalDecision(StrEnum):
    NONTERMINAL = "nonterminal"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class ExactOutput:
    run_id: str
    attempt_id: str
    source_identity: str
    source_version: str
    epoch: int
    status: str


def decide_terminal(
    *,
    platform_status: PlatformStatus,
    output: ExactOutput | None,
    run_id: str,
    attempt_id: str,
    source_identity: str,
    source_version: str,
    epoch: int,
) -> TerminalDecision:
    """Combine three-source evidence without allowing stale output to complete an attempt."""

    exact_output = output is not None and (
        output.run_id,
        output.attempt_id,
        output.source_identity,
        output.source_version,
        output.epoch,
    ) == (run_id, attempt_id, source_identity, source_version, epoch)
    if output is not None and not exact_output:
        return (
            TerminalDecision.AMBIGUOUS if platform_status == PlatformStatus.COMPLETE else TerminalDecision.NONTERMINAL
        )
    if exact_output:
        if output.status == "succeeded":
            return TerminalDecision.SUCCEEDED
        if output.status == "failed":
            return TerminalDecision.FAILED
        return TerminalDecision.AMBIGUOUS
    if platform_status == PlatformStatus.ERROR:
        return TerminalDecision.FAILED
    if platform_status == PlatformStatus.COMPLETE:
        return TerminalDecision.AMBIGUOUS
    return TerminalDecision.NONTERMINAL
