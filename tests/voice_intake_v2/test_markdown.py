from __future__ import annotations

from dataclasses import replace

import yaml

from my_data_hub.voice_intake_v2.markdown import render_publication
from my_data_hub.voice_intake_v2.store import PublicationProjection

from .conftest import SESSION_ID, SHA, summary_value


def projection(create_request, complete_request, terminology) -> PublicationProjection:
    return PublicationProjection(
        session_id=SESSION_ID,
        create=create_request.model_dump(mode="json"),
        complete=complete_request.model_dump(mode="json"),
        terminology=terminology,
        transport_chunks=({"chunk_index": 0, "sha256": SHA, "duration_ms": 240000},),
        transcript={
            "transcript": "IdeaHub Map и MCP-сервер.",
            "language": "ru-RU",
            "uncertain_fragments": [],
        },
        summary=summary_value(),
        transcription_request_uid="transcription-uid",
        summary_request_uid="summary-uid",
        transcription_limiter={"request_uid": "transcription-uid", "reserved_tpm": 7680},
        summary_limiter={"request_uid": "summary-uid", "reserved_tpm": 12345},
        model="gemini-3.1-flash-lite",
    )


def test_v2_markdown_contains_contract_metadata_and_text_but_no_audio(
    create_request, complete_request, terminology
) -> None:
    rendered = render_publication(projection(create_request, complete_request, terminology))
    frontmatter = yaml.safe_load(rendered.source.split("---", 2)[1])

    assert frontmatter["api_contract"] == "voice-intake-v2"
    assert frontmatter["client_version"] == "1.1.0"
    assert frontmatter["capture_policy"] == "continuous_v1"
    assert frontmatter["audio_format"] == create_request.audio_format.model_dump(mode="json")
    assert frontmatter["wall_elapsed_ms"] == 240000
    assert frontmatter["manual_pause_ms"] == 0
    assert frontmatter["recorded_audio_ms"] == 240000
    assert frontmatter["auto_silence_skipped_ms"] == 0
    assert frontmatter["terminology_card_commit"] == "b" * 40
    assert frontmatter["terminology_card_blob_sha"] == "c" * 40
    assert frontmatter["terminology_card_status"] == "current"
    assert frontmatter["transcription_request_uid"] == "transcription-uid"
    assert frontmatter["summary_request_uid"] == "summary-uid"
    assert "IdeaHub Map и MCP-сервер." in rendered.source
    assert "audio/mp4" in rendered.detail
    assert ".m4a" not in rendered.source
    assert "base64" not in rendered.source.lower()
    assert rendered.registry_entry["session_id"] == SESSION_ID
    assert "aggregate_transcription_single_request" in rendered.registry_entry["quality_flags"]


def test_vad_provenance_is_preserved(create_request, complete_request, terminology) -> None:
    value = create_request.model_dump(mode="json")
    value["capture_policy"] = "voice_activity_auto_pause_v1"
    value["vad"] = {
        "engine": "webrtc_vad",
        "engine_version": "client-pin",
        "mode": 1,
        "frame_ms": 30,
        "config_version": "vad-auto-pause-efficient-v1",
    }
    item = projection(create_request, complete_request, terminology)
    item = replace(item, create=value)
    rendered = render_publication(item)
    frontmatter = yaml.safe_load(rendered.source.split("---", 2)[1])
    assert frontmatter["vad"] == value["vad"]


def test_priority_twenty_minute_projection_renders_without_legacy_chunk_limit(
    create_request, complete_request, terminology
) -> None:
    item = projection(create_request, complete_request, terminology)
    complete = dict(item.complete)
    complete.update({"wall_elapsed_ms": 1_200_000, "recorded_audio_ms": 1_200_000})
    rendered = render_publication(replace(item, complete=complete))
    assert "recorded_audio_ms: 1200000" in rendered.source
