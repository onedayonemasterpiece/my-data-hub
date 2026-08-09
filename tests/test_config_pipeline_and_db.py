from __future__ import annotations

import json
from pathlib import Path

import pytest

from my_data_hub.config import ConfigurationError, Settings
from my_data_hub.db.migrations import MigrationError, discover_migrations, validate_history
from my_data_hub.orchestrator.models import RegionTalkBacklog
from my_data_hub.orchestrator.policy import plan_region_talk
from my_data_hub.orchestrator.registry import load_pipeline_definition


ROOT = Path(__file__).resolve().parents[1]


def _clear(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    for key in list(__import__("os").environ):
        if key.startswith("MY_DATA_HUB_"):
            monkeypatch.delenv(key, raising=False)


def test_settings_safe_defaults(monkeypatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    _clear(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_DATABASE_URL", "postgresql://local/test")
    monkeypatch.setenv("MY_DATA_HUB_ARTIFACT_ROOT", str(tmp_path))
    settings = Settings.from_env()
    assert settings.scheduler_enabled is False
    assert settings.production_publish_enabled is False
    assert settings.mcp_write_enabled is False
    assert settings.mcp_host == "127.0.0.1"
    assert settings.worker_result_max_bytes == 4 * 1024 * 1024


def test_remote_mcp_requires_non_stdio_auth(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    _clear(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_DATABASE_URL", "postgresql://local/test")
    monkeypatch.setenv("MY_DATA_HUB_MCP_REMOTE_ENABLED", "true")
    with pytest.raises(ConfigurationError, match="remote MCP cannot use"):
        Settings.from_env()


def test_production_requires_worker_token(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    _clear(monkeypatch)
    monkeypatch.setenv("MY_DATA_HUB_DATABASE_URL", "postgresql://local/test")
    monkeypatch.setenv("MY_DATA_HUB_ENVIRONMENT", "production")
    with pytest.raises(ConfigurationError, match="worker result token"):
        Settings.from_env()


def test_pipeline_registry_and_notebook_contracts_align() -> None:
    definition = load_pipeline_definition(ROOT / "config/pipelines/region-talk.v1.json")
    contracts = {stage.key: stage.contract for stage in definition.stages}
    assert definition.status == "paused"
    assert contracts["source_profile"] == "region-talk.source-profile.v1"
    assert contracts["writer"] == "region-talk.writer.v1"
    publication = next(stage for stage in definition.stages if stage.key == "publication_dispatch")
    assert publication.enabled_by_default is False


def test_pipeline_policy_drains_downstream_before_discovery() -> None:
    backlog = RegionTalkBacklog(
        completed_worker_results=5,
        writer_ready=4,
        source_discovery_due=100,
        post_discovery_due=100,
        actionable_backlog_growth_cycles=3,
    )
    actions = plan_region_talk(backlog, max_actions=10)
    stages = [item.stage for item in actions]
    assert stages[:2] == ["reconcile_worker_results", "writer"]
    assert "source_discovery" not in stages
    assert "post_discovery" not in stages
    assert "publication_dispatch" not in stages


def test_pipeline_policy_respects_provider_block() -> None:
    backlog = RegionTalkBacklog(
        bge_missing_for_e5=10,
        image_ready=10,
        provider_blocked=frozenset({"bge_m3", "image"}),
    )
    stages = {item.stage for item in plan_region_talk(backlog, max_actions=20)}
    assert "bge_m3_embedding" not in stages
    assert "image_scoring" not in stages


def test_discover_migrations_is_contiguous_and_history_is_immutable(tmp_path) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "0001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "0002_second.sql").write_text("SELECT 2;", encoding="utf-8")
    migrations = discover_migrations(tmp_path)
    assert [item.version for item in migrations] == [1, 2]
    with pytest.raises(MigrationError, match="modified"):
        validate_history(migrations, {1: ("0001_first.sql", "0" * 64)})


def test_discover_migrations_rejects_gap(tmp_path) -> None:  # type: ignore[no-untyped-def]
    (tmp_path / "0001_first.sql").write_text("SELECT 1;", encoding="utf-8")
    (tmp_path / "0003_third.sql").write_text("SELECT 3;", encoding="utf-8")
    with pytest.raises(MigrationError, match="contiguous"):
        discover_migrations(tmp_path)


def test_pipeline_definition_rejects_duplicate_stage(tmp_path) -> None:  # type: ignore[no-untyped-def]
    raw = json.loads((ROOT / "config/pipelines/region-talk.v1.json").read_text())
    raw["stages"].append(raw["stages"][0])
    path = tmp_path / "pipeline.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    with pytest.raises(ValueError, match="unique"):
        load_pipeline_definition(path)
