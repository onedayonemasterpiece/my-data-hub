from __future__ import annotations

import json
from pathlib import Path

from my_data_hub.orchestrator.models import PipelineDefinition, StageDefinition


def load_pipeline_definition(path: Path) -> PipelineDefinition:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != "my-data-hub-pipeline.v1":
        raise ValueError("unsupported pipeline definition")
    stages = tuple(
        StageDefinition(
            key=item["key"],
            version=item["version"],
            compute_lane=item["compute_lane"],
            priority=int(item["priority"]),
            max_attempts=int(item["max_attempts"]),
            timeout_seconds=int(item["timeout_seconds"]),
            contract=item["contract"],
            enabled_by_default=bool(item.get("enabled_by_default", True)),
        )
        for item in raw["stages"]
    )
    keys = [stage.key for stage in stages]
    if len(keys) != len(set(keys)):
        raise ValueError("pipeline stage keys must be unique")
    return PipelineDefinition(
        workload=raw["workload"],
        name=raw["name"],
        version=raw["version"],
        status=raw.get("status", "paused"),
        planning_policy=raw["planning_policy"],
        stages=stages,
    )
