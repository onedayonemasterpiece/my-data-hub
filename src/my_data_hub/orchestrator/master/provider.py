from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol

from .evidence import MasterTerminalOutput, PlatformStatus


@dataclass(frozen=True, slots=True)
class PlannedProviderEffect:
    effect_id: str
    idempotency_key: str
    effect_kind: str
    exact_identity: dict[str, Any]


@dataclass(frozen=True, slots=True)
class ProviderEffectReceipt:
    provider: str
    effect_kind: str
    exact_ref: str
    source_identity: str
    source_version: str
    exact_identity: dict[str, Any] | None = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "provider": self.provider,
            "effect_kind": self.effect_kind,
            "exact_ref": self.exact_ref,
            "source_identity": self.source_identity,
            "source_version": self.source_version,
        }
        if self.exact_identity is not None:
            payload["exact_identity"] = self.exact_identity
        return payload


class ReconciliationStatus(StrEnum):
    FOUND = "found"
    ABSENT = "absent"
    AMBIGUOUS = "ambiguous"


@dataclass(frozen=True, slots=True)
class EffectReconciliation:
    status: ReconciliationStatus
    receipt: ProviderEffectReceipt | None = None

    def __post_init__(self) -> None:
        if (self.status == ReconciliationStatus.FOUND) != (self.receipt is not None):
            raise ValueError("found reconciliation requires exactly one provider receipt")


@dataclass(frozen=True, slots=True)
class MasterTerminalQuery:
    operation_id: str
    run_id: str
    attempt_id: str
    service_instance_id: str
    master_instance_id: str
    source_identity: str
    source_version: str
    epoch: int
    checkpoint_ref: str
    provider_run_identity: dict[str, Any]


@dataclass(frozen=True, slots=True)
class MasterTerminalEvidence:
    platform_status: PlatformStatus
    output: MasterTerminalOutput | None = None


class MasterRuntimeProvider(Protocol):
    """FakeKaggle-facing port; the core never imports a Kaggle SDK."""

    def execute(self, effect: PlannedProviderEffect) -> ProviderEffectReceipt: ...

    def reconcile(self, effect: PlannedProviderEffect) -> EffectReconciliation: ...

    def observe_terminal(self, query: MasterTerminalQuery) -> MasterTerminalEvidence: ...
