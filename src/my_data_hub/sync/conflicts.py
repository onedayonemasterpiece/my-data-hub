from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from my_data_hub.domain.commands import Operation

ConflictDisposition = Literal["idempotent", "auto_merge", "conditional", "quarantine"]


@dataclass(frozen=True, slots=True)
class ConflictPolicy:
    disposition: ConflictDisposition
    reason: str


def classify_operation(operation: Operation) -> ConflictPolicy:
    if operation.kind == "event.append":
        return ConflictPolicy("idempotent", "event identity deduplicates replay")
    if operation.kind in {"set.add", "set.remove", "relation.add", "relation.remove"}:
        return ConflictPolicy("auto_merge", "set/relation operation has explicit identity")
    if operation.kind in {"analysis.record", "pipeline.enqueue"}:
        return ConflictPolicy(
            "idempotent",
            "domain identity and input fingerprint deduplicate replay",
        )
    if operation.kind == "object.create":
        return ConflictPolicy(
            "conditional",
            "create may deduplicate by external identity and produce an ID remap",
        )
    if operation.kind in {"field.set", "state.transition", "object.tombstone"}:
        if operation.expected_revision is None and not operation.preconditions:
            return ConflictPolicy(
                "quarantine",
                "scalar/state mutation requires expected revision or semantic precondition",
            )
        return ConflictPolicy("conditional", "validate expected revision and invariants")
    return ConflictPolicy("quarantine", "unknown operation class")
