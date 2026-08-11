from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol


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

    def as_dict(self) -> dict[str, str]:
        return {
            "provider": self.provider,
            "effect_kind": self.effect_kind,
            "exact_ref": self.exact_ref,
            "source_identity": self.source_identity,
            "source_version": self.source_version,
        }


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


class MasterRuntimeProvider(Protocol):
    """FakeKaggle-facing port; the core never imports a Kaggle SDK."""

    def execute(self, effect: PlannedProviderEffect) -> ProviderEffectReceipt: ...

    def reconcile(self, effect: PlannedProviderEffect) -> EffectReconciliation: ...
