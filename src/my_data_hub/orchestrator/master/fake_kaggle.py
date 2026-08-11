from __future__ import annotations

import threading
from collections import Counter, defaultdict, deque
from collections.abc import Iterable
from typing import Any

from .provider import (
    EffectReconciliation,
    MasterRuntimeProvider,
    PlannedProviderEffect,
    ProviderEffectReceipt,
    ReconciliationStatus,
)


class SimulatedProcessCrash(RuntimeError):
    """Fault injected after a durable provider effect but before its host receipt."""


class FakeKaggleRuntime(MasterRuntimeProvider):
    """Deterministic provider double with exact-identity idempotency and fault scripts."""

    def __init__(self, fault_scripts: dict[str, Iterable[Any]] | None = None) -> None:
        self._scripts: defaultdict[str, deque[Any]] = defaultdict(deque)
        for kind, script in (fault_scripts or {}).items():
            self._scripts[kind].extend(script)
        self._receipts: dict[str, ProviderEffectReceipt] = {}
        self._lock = threading.Lock()
        self.physical_effect_counts: Counter[str] = Counter()

    def execute(self, effect: PlannedProviderEffect) -> ProviderEffectReceipt:
        with self._lock:
            existing = self._receipts.get(effect.idempotency_key)
            if existing is not None:
                return existing
            scripted = self._scripts[effect.effect_kind].popleft() if self._scripts[effect.effect_kind] else None
            if isinstance(scripted, Exception) and not isinstance(scripted, SimulatedProcessCrash):
                raise scripted
            exact_ref = str(effect.exact_identity["exact_ref"])
            receipt = ProviderEffectReceipt(
                provider="fake-kaggle",
                effect_kind=effect.effect_kind,
                exact_ref=exact_ref,
                source_identity=str(effect.exact_identity["source_identity"]),
                source_version=str(effect.exact_identity["source_version"]),
            )
            self._receipts[effect.idempotency_key] = receipt
            self.physical_effect_counts[effect.effect_kind] += 1
            if isinstance(scripted, SimulatedProcessCrash):
                raise scripted
            return receipt

    def reconcile(self, effect: PlannedProviderEffect) -> EffectReconciliation:
        with self._lock:
            receipt = self._receipts.get(effect.idempotency_key)
            if receipt is None:
                return EffectReconciliation(ReconciliationStatus.ABSENT)
            return EffectReconciliation(ReconciliationStatus.FOUND, receipt)

    def snapshot(self) -> dict[str, dict[str, str]]:
        with self._lock:
            return {key: receipt.as_dict() for key, receipt in self._receipts.items()}
