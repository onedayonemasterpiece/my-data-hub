from __future__ import annotations

from typing import Any

import pytest

from my_data_hub.voice_intake.contracts import SummaryPayload
from my_data_hub.voice_intake.settings import VoiceIntakeSettings
from my_data_hub.voice_intake_v2.contracts import AudioFormat, SessionCompleteRequest, SessionCreateRequest

SESSION_ID = "voice-20260828-123456-abcdef12"
SHA = "a" * 64


@pytest.fixture
def auth_settings() -> VoiceIntakeSettings:
    return VoiceIntakeSettings(
        enabled=True, device_token="x" * 32, model="gemini-3.1-flash-lite",
        allowed_models=("gemini-3.1-flash-lite",), max_audio_bytes=8 * 1024 * 1024,
        max_json_bytes=2 * 1024 * 1024, provider_timeout_seconds=180,
        github_token="not-used", github_repository="onedayonemasterpiece/idea-hub",
        github_branch="main", limiter_supabase_url="https://example.invalid",
        limiter_supabase_service_key="not-used", normal_key_envs=("KEY",),
    )


@pytest.fixture
def create_request() -> SessionCreateRequest:
    return SessionCreateRequest(
        session_id=SESSION_ID, started_at="2026-08-28T12:34:56+02:00",
        timezone="Europe/Kaliningrad", device_label="Samsung SM-G998B", client_version="1.1.0",
        capture_policy="continuous_v1", audio_format=AudioFormat(
            container="mp4", codec="aac_lc", mime_type="audio/mp4",
            sample_rate_hz=16000, channels=1, target_bitrate_bps=32000,
        ), vad=None,
    )


@pytest.fixture
def complete_payload() -> dict[str, Any]:
    return {
        "ended_at": "2026-08-28T12:38:56+02:00", "wall_elapsed_ms": 240000,
        "manual_pause_ms": 0, "recorded_audio_ms": 240000,
        "auto_silence_skipped_ms": 0, "chunk_count": 1,
        "chunks": [{
            "chunk_index": 0, "sha256": SHA, "duration_ms": 240000,
            "audio_start_ms": 0, "audio_end_ms": 240000,
            "wall_start_ms": 0, "wall_end_ms": 240000,
        }],
    }


@pytest.fixture
def complete_request(complete_payload: dict[str, Any]) -> SessionCompleteRequest:
    return SessionCompleteRequest.model_validate(complete_payload)


@pytest.fixture
def terminology() -> dict[str, Any]:
    return {
        "status": "current", "source_path": "config/voice-terminology.yaml",
        "schema_version": "1.0.0", "source_commit_sha": "b" * 40,
        "source_blob_sha": "c" * 40, "prompt": "canonical terms",
    }


def summary_value() -> dict[str, Any]:
    return SummaryPayload(
        title="Title", short_summary="Short", detailed_summary="Detailed",
    ).model_dump(mode="json")
