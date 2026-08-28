from __future__ import annotations

import pytest
from pydantic import ValidationError

from my_data_hub.voice_intake_v2.contracts import SessionCompleteRequest
from my_data_hub.voice_intake_v2.store import StoreError, VoiceIntakeV2Store

from .conftest import SESSION_ID


def test_create_requires_aware_timestamp_and_matching_iana_timezone(create_request) -> None:
    with pytest.raises(ValidationError):
        type(create_request).model_validate(
            create_request.model_dump() | {"started_at": "not-a-timestamp"}
        )
    with pytest.raises(ValidationError):
        type(create_request).model_validate(
            create_request.model_dump() | {"timezone": "Not/A_Real_Zone"}
        )
    with pytest.raises(ValidationError):
        type(create_request).model_validate(
            create_request.model_dump() | {"started_at": "2026-08-28T12:34:56+03:00"}
        )


def test_complete_requires_aware_timestamp(complete_payload) -> None:
    with pytest.raises(ValidationError):
        SessionCompleteRequest.model_validate(
            complete_payload | {"ended_at": "2026-08-28T12:38:56"}
        )


def test_complete_cannot_end_before_durable_session_start(
    tmp_path, create_request, terminology
) -> None:
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    with pytest.raises(StoreError, match="complete_time_invalid"):
        store.complete(
            SESSION_ID,
            SessionCompleteRequest.model_validate({
                "ended_at": "2026-08-28T12:30:56+02:00",
                "wall_elapsed_ms": 240000,
                "manual_pause_ms": 0,
                "recorded_audio_ms": 240000,
                "auto_silence_skipped_ms": 0,
                "chunk_count": 1,
                "chunks": [{
                    "chunk_index": 0,
                    "sha256": "a" * 64,
                    "duration_ms": 240000,
                    "audio_start_ms": 0,
                    "audio_end_ms": 240000,
                    "wall_start_ms": 0,
                    "wall_end_ms": 240000,
                }],
            }),
        )


def test_complete_offset_must_match_durable_session_timezone(
    tmp_path, create_request, terminology
) -> None:
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    with pytest.raises(StoreError, match="complete_time_invalid"):
        store.complete(
            SESSION_ID,
            SessionCompleteRequest.model_validate({
                "ended_at": "2026-08-28T13:38:56+03:00",
                "wall_elapsed_ms": 240000,
                "manual_pause_ms": 0,
                "recorded_audio_ms": 240000,
                "auto_silence_skipped_ms": 0,
                "chunk_count": 1,
                "chunks": [{
                    "chunk_index": 0,
                    "sha256": "a" * 64,
                    "duration_ms": 240000,
                    "audio_start_ms": 0,
                    "audio_end_ms": 240000,
                    "wall_start_ms": 0,
                    "wall_end_ms": 240000,
                }],
            }),
        )
