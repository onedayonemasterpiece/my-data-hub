from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class StageDefinition:
    key: str
    version: str
    compute_lane: str
    priority: int
    max_attempts: int
    timeout_seconds: int
    contract: str
    enabled_by_default: bool = True


@dataclass(frozen=True, slots=True)
class PipelineDefinition:
    workload: str
    name: str
    version: str
    status: str
    planning_policy: str
    stages: tuple[StageDefinition, ...]


@dataclass(frozen=True, slots=True)
class RegionTalkBacklog:
    completed_worker_results: int = 0
    exact_url_pending: int = 0
    bge_missing_for_e5: int = 0
    fusion_ready: int = 0
    text_gate_ready: int = 0
    image_ready: int = 0
    source_profile_ready: int = 0
    final_verifier_ready: int = 0
    writer_ready: int = 0
    review_dispatch_ready: int = 0
    review_sync_pending: int = 0
    publication_plan_ready: int = 0
    publication_dispatch_ready: int = 0
    source_discovery_due: int = 0
    post_discovery_due: int = 0
    e5_due: int = 0
    actionable_backlog_growth_cycles: int = 0
    provider_blocked: frozenset[str] = field(default_factory=frozenset)


@dataclass(frozen=True, slots=True)
class PlannedAction:
    stage: str
    requested_items: int
    reason: str
    priority: int
    metadata: dict[str, Any] = field(default_factory=dict)
