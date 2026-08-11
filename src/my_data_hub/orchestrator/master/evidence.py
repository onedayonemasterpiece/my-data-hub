from __future__ import annotations

import re
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID


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


@dataclass(frozen=True, slots=True)
class MasterTerminalOutput:
    """Secret-free terminal evidence recovered from one exact provider run."""

    run_id: str
    attempt_id: str
    service_instance_id: str
    master_instance_id: str
    source_identity: str
    source_version: str
    epoch: int
    status: str
    checkpoint_id: str
    manifest_sha256: str
    current_checkpoint_id: str
    recovered_events: tuple[bytes, ...]
    output_tree_sha256: str
    output_receipt_sha256: str

    def __post_init__(self) -> None:
        identities = (
            self.run_id,
            self.attempt_id,
            self.service_instance_id,
            self.master_instance_id,
            self.source_identity,
            self.source_version,
            self.checkpoint_id,
            self.current_checkpoint_id,
        )
        if any(not value or len(value) > 500 for value in identities):
            raise ValueError("master terminal output identity is invalid")
        try:
            UUID(self.master_instance_id)
            UUID(self.checkpoint_id)
            UUID(self.current_checkpoint_id)
        except ValueError as exc:
            raise ValueError("master terminal UUID identity is invalid") from exc
        if self.epoch < 1 or self.status != "succeeded":
            raise ValueError("master terminal output state is invalid")
        if self.checkpoint_id != self.current_checkpoint_id:
            raise ValueError("master terminal output checkpoint is not current")
        if not re.fullmatch(r"[a-f0-9]{64}", self.manifest_sha256):
            raise ValueError("master terminal manifest hash is invalid")
        if not re.fullmatch(r"[a-f0-9]{64}", self.output_tree_sha256):
            raise ValueError("master terminal output-tree hash is invalid")
        if not re.fullmatch(r"[a-f0-9]{64}", self.output_receipt_sha256):
            raise ValueError("master terminal output receipt hash is invalid")
        if not 1 <= len(self.recovered_events) <= 8:
            raise ValueError("master terminal recovered event count is invalid")
        if any(not 2 <= len(event) <= 64 * 1024 for event in self.recovered_events):
            raise ValueError("master terminal recovered event size is invalid")

    def exact_output(self) -> ExactOutput:
        return ExactOutput(
            run_id=self.run_id,
            attempt_id=self.attempt_id,
            source_identity=self.source_identity,
            source_version=self.source_version,
            epoch=self.epoch,
            status=self.status,
        )


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
            return (
                TerminalDecision.SUCCEEDED if platform_status == PlatformStatus.COMPLETE else TerminalDecision.AMBIGUOUS
            )
        if output.status == "failed":
            return (
                TerminalDecision.FAILED
                if platform_status in {PlatformStatus.COMPLETE, PlatformStatus.ERROR}
                else TerminalDecision.AMBIGUOUS
            )
        return TerminalDecision.AMBIGUOUS
    if platform_status == PlatformStatus.ERROR:
        return TerminalDecision.FAILED
    if platform_status == PlatformStatus.COMPLETE:
        return TerminalDecision.AMBIGUOUS
    return TerminalDecision.NONTERMINAL
