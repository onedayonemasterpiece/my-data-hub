from __future__ import annotations

from contextlib import nullcontext
from uuid import UUID, uuid4

import pytest

from my_data_hub.domain.commands import SemanticCommand
from my_data_hub.mcp.service import HubPermissionError, HubService, SemanticCommandError


class StageCursor:
    def __init__(self, row):
        self.row = row
        self.executed: list[tuple[str, object]] = []

    def execute(self, query: str, parameters=None) -> None:  # type: ignore[no-untyped-def]
        self.executed.append((query, parameters))

    def fetchone(self):  # type: ignore[no-untyped-def]
        return self.row


def command_for_stage(stage: str, *, dry_run: bool = True) -> SemanticCommand:
    subject_id = uuid4()
    return SemanticCommand.model_validate(
        {
            "schema_version": "my-data-hub-semantic-command.v1",
            "command_id": str(uuid4()),
            "client_id": "test",
            "actor_id": "test",
            "idempotency_key": f"test:{stage}",
            "command_type": "region_talk.work.enqueue",
            "base_revision": 3,
            "expected_revision": 3,
            "target": {"type": "content_url", "id": str(subject_id)},
            "input_fingerprint": "a" * 64,
            "payload": {
                "stage": stage,
                "subject_type": "content_url",
                "subject_id": str(subject_id),
                "priority": 100,
            },
            "dry_run": dry_run,
        }
    )


def test_mcp_scope_and_write_gate_are_both_required() -> None:
    service = HubService("postgresql://unused", scopes=frozenset(), write_enabled=False)
    with pytest.raises(HubPermissionError, match="scope"):
        service._require("hub:read")

    service = HubService(
        "postgresql://unused",
        scopes=frozenset({"region-talk:write"}),
        write_enabled=False,
    )
    with pytest.raises(HubPermissionError, match="disabled"):
        service._require_write("region-talk:write")


def test_url_normalisation_removes_fragment_and_default_port() -> None:
    assert HubService._normalize_url("HTTPS://Example.COM:443/path?q=1#fragment") == (
        "https://example.com/path?q=1"
    )
    assert HubService._normalize_url("http://example.com") == "http://example.com/"
    with pytest.raises(ValueError, match="HTTP"):
        HubService._normalize_url("file:///etc/passwd")


def test_stage_preview_reports_paused_pipeline() -> None:
    service = HubService("unused", scopes=frozenset({"region-talk:write"}), write_enabled=True)
    # pipeline_id, stage_id, project_id, pipeline status, enabled, compute lane
    cursor = StageCursor((uuid4(), uuid4(), uuid4(), "paused", True, "local"))
    result = service._apply_supported_command(
        cursor,
        command_for_stage("exact_url_intake"),
        3,
        preview_only=True,
    )
    assert result["would_enqueue"] is False
    assert result["pipeline_status"] == "paused"
    assert result["canonical_change"] is False


def test_stage_preview_blocks_external_side_effect_lane() -> None:
    service = HubService("unused", scopes=frozenset({"region-talk:write"}), write_enabled=True)
    cursor = StageCursor((uuid4(), uuid4(), uuid4(), "active", True, "local-side-effect"))
    result = service._apply_supported_command(
        cursor,
        command_for_stage("publication_dispatch"),
        3,
        preview_only=True,
    )
    assert result["would_enqueue"] is False
    assert result["side_effect_stage_blocked"] is True


def test_revision_guard_rejects_future_and_stale_exact_revision() -> None:
    future = command_for_stage("exact_url_intake")
    future = future.model_copy(update={"base_revision": 4, "expected_revision": 4})
    with pytest.raises(SemanticCommandError, match="ahead"):
        HubService._validate_revision(future, 3)
    stale = command_for_stage("exact_url_intake")
    stale = stale.model_copy(update={"expected_revision": 2})
    with pytest.raises(SemanticCommandError, match="does not match"):
        HubService._validate_revision(stale, 3)
