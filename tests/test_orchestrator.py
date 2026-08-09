from __future__ import annotations

from pathlib import Path

import pytest

from my_data_hub.orchestrator.models import RegionTalkBacklog
from my_data_hub.orchestrator.policy import plan_region_talk
from my_data_hub.orchestrator.registry import load_pipeline_definition

ROOT = Path(__file__).resolve().parents[1]


def test_region_talk_pipeline_is_paused_and_side_effect_stage_disabled() -> None:
    definition = load_pipeline_definition(ROOT / "config/pipelines/region-talk.v1.json")
    assert definition.workload == "region-talk"
    assert definition.status == "paused"
    stages = {stage.key: stage for stage in definition.stages}
    assert stages["publication_dispatch"].enabled_by_default is False
    assert stages["publication_dispatch"].compute_lane == "local-side-effect"
    assert "source_profile" in stages
    assert "writer" in stages


def test_planner_prioritises_reconciliation_and_review() -> None:
    backlog = RegionTalkBacklog(
        completed_worker_results=5,
        review_sync_pending=4,
        publication_plan_ready=3,
        exact_url_pending=20,
        source_discovery_due=100,
    )
    actions = plan_region_talk(backlog, max_actions=8)
    assert [action.stage for action in actions[:3]] == [
        "reconcile_worker_results",
        "review_sync",
        "publication_plan",
    ]
    assert all(action.stage != "publication_dispatch" for action in actions)


def test_planner_stops_discovery_when_backlog_grows() -> None:
    backlog = RegionTalkBacklog(
        actionable_backlog_growth_cycles=2,
        post_discovery_due=100,
        source_discovery_due=100,
        e5_due=3,
    )
    stages = {item.stage for item in plan_region_talk(backlog, max_actions=20)}
    assert "e5_embedding" in stages
    assert "post_discovery" not in stages
    assert "source_discovery" not in stages


def test_planner_respects_provider_block_and_bounds() -> None:
    backlog = RegionTalkBacklog(
        image_ready=1000,
        source_profile_ready=1000,
        bge_missing_for_e5=1000,
        provider_blocked=frozenset({"image", "source_profile", "bge_m3"}),
    )
    stages = {item.stage for item in plan_region_talk(backlog, max_actions=20)}
    assert "image_scoring" not in stages
    assert "source_profile" not in stages
    assert "bge_m3_embedding" not in stages


def test_pipeline_registry_rejects_duplicate_stage(tmp_path: Path) -> None:
    source = (ROOT / "config/pipelines/region-talk.v1.json").read_text(encoding="utf-8")
    import json

    raw = json.loads(source)
    raw["stages"].append(raw["stages"][0])
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="unique"):
        load_pipeline_definition(path)
