from __future__ import annotations

from pathlib import Path

import pytest

from my_data_hub.voice_intake.contracts import TranscriptPayload
from my_data_hub.voice_intake_v2.contracts import InferenceReceipt, PublicationReceipt
from my_data_hub.voice_intake_v2.media import MediaProbe
from my_data_hub.voice_intake_v2.settings import VoiceIntakeV2Settings
from my_data_hub.voice_intake_v2.store import ChunkReceipt, VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import VoiceIntakeV2Worker

from .conftest import SESSION_ID, SHA, summary_value


class Media:
    async def probe(self, _path):
        return MediaProbe(240000, "aac", "LC", 16000, 1)

    async def normalize(self, _chunks, output):
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"mp3")


class Inference:
    def __init__(self):
        self.calls = []

    async def transcribe(self, **kwargs):
        self.calls.append(("transcribe", kwargs["recorded_audio_ms"], kwargs["terminology"]))
        return InferenceReceipt(
            value=TranscriptPayload(transcript="full session").model_dump(mode="json"),
            request_uid="transcription-uid", limiter={"reserved_tpm": 7680},
        )

    async def summarize(self, **kwargs):
        self.calls.append(("summarize", kwargs["transcript"], kwargs["terminology"]))
        return InferenceReceipt(
            value=summary_value(), request_uid="summary-uid", limiter={"reserved_tpm": 12345},
        )


class Publisher:
    def __init__(self):
        self.projections = []

    async def publish_and_verify(self, projection):
        self.projections.append(projection)
        return PublicationReceipt(
            github_url="https://github.com/example", github_commit_sha="d" * 40, github_verified=True,
        )


def settings(root: Path) -> VoiceIntakeV2Settings:
    return VoiceIntakeV2Settings(
        enabled=True, spool_root=root, max_chunk_bytes=1024 * 1024, max_json_bytes=1024 * 1024,
        max_session_seconds=3600, active_ttl_seconds=7 * 24 * 3600, lease_seconds=60,
        worker_poll_seconds=.1, ffprobe_timeout_seconds=5, ffmpeg_timeout_seconds=30,
        duration_tolerance_ms=2000,
    )


def queued(tmp_path, create_request, complete_request, terminology):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"m4a")
    store.record_chunk(ChunkReceipt(
        session_id=SESSION_ID, chunk_index=0, sha256=SHA, duration_ms=240000,
        audio_start_ms=0, audio_end_ms=240000, wall_start_ms=0, wall_end_ms=240000,
        size_bytes=3, path=str(path),
    ))
    store.complete(SESSION_ID, complete_request)
    return store


@pytest.mark.asyncio
async def test_n_chunks_result_in_exactly_two_aggregate_calls_and_purge_after_readback(
    tmp_path, create_request, complete_request, terminology
):
    store = queued(tmp_path, create_request, complete_request, terminology)
    inference, publisher = Inference(), Publisher()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(), inference=inference, publisher=publisher,
        owner="worker",
    )
    assert await worker.process_once()
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]
    status = store.status(SESSION_ID)
    assert status.state == "published_verified" and status.server_audio_purged
    assert status.gemini_requests_completed == 2
    assert status.transcription_request_uid == "transcription-uid"
    assert status.summary_request_uid == "summary-uid"
    assert not (store.session_directory(SESSION_ID) / "chunks").exists()
    assert (store.session_directory(SESSION_ID) / "transcript.json").is_file()
    projection = publisher.projections[0]
    assert projection.create["client_version"] == "1.1.0"
    assert projection.transcription_request_uid != projection.summary_request_uid

