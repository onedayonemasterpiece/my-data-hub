from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

from my_data_hub.voice_intake.contracts import TranscriptPayload
from my_data_hub.voice_intake.errors import GitHubPublicationConflict, VoiceIntakeError
from my_data_hub.voice_intake_v2.contracts import (
    InferenceReceipt,
    PublicationReceipt,
    SessionCompleteRequest,
)
from my_data_hub.voice_intake_v2.media import BoundedMediaTools, MediaProbe
from my_data_hub.voice_intake_v2.settings import VoiceIntakeV2Settings
from my_data_hub.voice_intake_v2.store import ChunkReceipt, StoreError, VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import StageFailure, VoiceIntakeV2Worker

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


class TrackingStore(VoiceIntakeV2Store):
    def __init__(self, root: Path) -> None:
        self.events: list[str] = []
        super().__init__(root)

    def persist_github_verified(self, *args, **kwargs):
        self.events.append("receipt")
        return super().persist_github_verified(*args, **kwargs)

    def purge_audio(self, *args, **kwargs):
        self.events.append("purge")
        return super().purge_audio(*args, **kwargs)


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
    tmp_path, create_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    chunks = []
    for index, digest in enumerate((SHA, "d" * 64)):
        path = store.session_directory(SESSION_ID) / "chunks" / f"{index:05d}-{digest}.m4a"
        path.write_bytes(b"m4a")
        start = index * 240000
        store.record_chunk(ChunkReceipt(
            session_id=SESSION_ID, chunk_index=index, sha256=digest, duration_ms=240000,
            audio_start_ms=start, audio_end_ms=start + 240000,
            wall_start_ms=start, wall_end_ms=start + 240000,
            size_bytes=3, path=str(path),
        ))
        chunks.append({
            "chunk_index": index, "sha256": digest, "duration_ms": 240000,
            "audio_start_ms": start, "audio_end_ms": start + 240000,
            "wall_start_ms": start, "wall_end_ms": start + 240000,
        })
    store.complete(SESSION_ID, SessionCompleteRequest.model_validate({
        "ended_at": "2026-08-28T12:42:56+02:00", "wall_elapsed_ms": 480000,
        "manual_pause_ms": 0, "recorded_audio_ms": 480000,
        "auto_silence_skipped_ms": 0, "chunk_count": 2, "chunks": chunks,
    }))
    # Re-open the SQLite/spool as a restarted process before either inference
    # stage; the one pinned terminology snapshot must survive and reach both.
    store = VoiceIntakeV2Store(store.root)
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
    assert len(projection.transport_chunks) == 2
    assert projection.transcription_request_uid != projection.summary_request_uid
    assert all(
        call[-1]["source_commit_sha"] == terminology["source_commit_sha"]
        for call in inference.calls
    )


@pytest.mark.asyncio
async def test_real_two_m4a_worker_pipeline_normalizes_once_and_runs_two_stages(
    tmp_path, create_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    media = BoundedMediaTools(ffprobe_timeout=10, ffmpeg_timeout=30)
    chunks = []
    audio_cursor = 0
    for index, frequency in enumerate((440, 660)):
        path = store.session_directory(SESSION_ID) / "chunks" / f"{index:05d}-fixture.m4a"
        process = await asyncio.create_subprocess_exec(
            "ffmpeg", "-nostdin", "-v", "error", "-f", "lavfi", "-i",
            f"sine=frequency={frequency}:sample_rate=16000:duration=1", "-ac", "1",
            "-c:a", "aac", "-profile:a", "aac_low", "-b:a", "32k", "-y", str(path),
        )
        assert await process.wait() == 0
        probe = await media.probe(path)
        digest = f"{index + 1:064x}"
        start = audio_cursor
        audio_cursor += probe.duration_ms
        store.record_chunk(ChunkReceipt(
            session_id=SESSION_ID, chunk_index=index, sha256=digest,
            duration_ms=probe.duration_ms, audio_start_ms=start, audio_end_ms=audio_cursor,
            wall_start_ms=start, wall_end_ms=audio_cursor, size_bytes=path.stat().st_size,
            path=str(path),
        ))
        chunks.append({
            "chunk_index": index, "sha256": digest, "duration_ms": probe.duration_ms,
            "audio_start_ms": start, "audio_end_ms": audio_cursor,
            "wall_start_ms": start, "wall_end_ms": audio_cursor,
        })
    store.complete(SESSION_ID, SessionCompleteRequest.model_validate({
        "ended_at": "2026-08-28T12:34:58+02:00", "wall_elapsed_ms": audio_cursor,
        "manual_pause_ms": 0, "recorded_audio_ms": audio_cursor,
        "auto_silence_skipped_ms": 0, "chunk_count": 2, "chunks": chunks,
    }))
    inference = Inference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=media, inference=inference, publisher=Publisher(),
        owner="worker",
    )
    assert await worker.process_once()
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]
    assert store.status(SESSION_ID).state == "published_verified"


@pytest.mark.asyncio
async def test_verified_receipt_is_durable_before_audio_purge(
    tmp_path, create_request, complete_request, terminology
):
    store = TrackingStore(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"m4a")
    store.record_chunk(ChunkReceipt(
        session_id=SESSION_ID, chunk_index=0, sha256=SHA, duration_ms=240000,
        audio_start_ms=0, audio_end_ms=240000, wall_start_ms=0, wall_end_ms=240000,
        size_bytes=3, path=str(path),
    ))
    store.complete(SESSION_ID, complete_request)
    inference = Inference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(), inference=inference, publisher=Publisher(),
        owner="worker",
    )
    assert await worker.process_once()
    assert store.events == ["receipt", "purge"]


@pytest.mark.asyncio
async def test_github_retry_reuses_durable_inference_without_new_provider_calls(
    tmp_path, create_request, complete_request, terminology
):
    class RetryPublisher(Publisher):
        async def publish_and_verify(self, projection):
            self.projections.append(projection)
            if len(self.projections) == 1:
                raise GitHubPublicationConflict("idea_hub_main_moved_repeatedly")
            return PublicationReceipt(
                github_url="https://github.com/example",
                github_commit_sha="d" * 40,
                github_verified=True,
            )

    store = queued(tmp_path, create_request, complete_request, terminology)
    inference, publisher = Inference(), RetryPublisher()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(), inference=inference, publisher=publisher,
        owner="worker",
    )
    assert await worker.process_once()
    failed = store.status(SESSION_ID)
    assert failed.state == "retryable_error"
    assert failed.transcription_complete and failed.summary_complete
    assert (store.session_directory(SESSION_ID) / "chunks").is_dir()

    store.complete(SESSION_ID, complete_request)
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]
    assert len(publisher.projections) == 2


@pytest.mark.asyncio
async def test_summary_retry_reuses_durable_transcript_without_new_transcription(
    tmp_path, create_request, complete_request, terminology
):
    class RetrySummaryInference(Inference):
        async def summarize(self, **kwargs):
            self.calls.append(("summarize", kwargs["transcript"], kwargs["terminology"]))
            if len([call for call in self.calls if call[0] == "summarize"]) == 1:
                raise StageFailure("summary_pre_send_retry", sent=False, retryable=True)
            return InferenceReceipt(
                value=summary_value(), request_uid="summary-uid", limiter={"reserved_tpm": 12345},
            )

    store = queued(tmp_path, create_request, complete_request, terminology)
    inference = RetrySummaryInference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(), inference=inference, publisher=Publisher(),
        owner="worker",
    )
    assert await worker.process_once()
    failed = store.status(SESSION_ID)
    assert failed.state == "retryable_error"
    assert failed.transcription_complete and not failed.summary_complete
    assert (store.session_directory(SESSION_ID) / "transcript.json").is_file()
    assert (store.session_directory(SESSION_ID) / "chunks").is_dir()

    store.complete(SESSION_ID, complete_request)
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize", "summarize"]


@pytest.mark.asyncio
async def test_failed_audio_deletion_never_marks_server_audio_purged(
    tmp_path, create_request, complete_request, terminology, monkeypatch
):
    store = queued(tmp_path, create_request, complete_request, terminology)

    def fail_purge(_session_id):
        raise StoreError("server_audio_purge_failed", status_code=500)

    monkeypatch.setattr(store, "purge_audio", fail_purge)
    inference = Inference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(), inference=inference, publisher=Publisher(),
        owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.github_verified
    assert not status.server_audio_purged
    assert status.error_code == "server_audio_purge_failed"
    assert status.retryable
    assert (store.session_directory(SESSION_ID) / "chunks").is_dir()
    monkeypatch.undo()
    store.complete(SESSION_ID, complete_request)
    assert await worker.process_once()
    recovered = store.status(SESSION_ID)
    assert recovered.state == "published_verified" and recovered.server_audio_purged
    assert not recovered.retryable and recovered.error_code is None
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]


@pytest.mark.asyncio
async def test_ambiguous_github_outcome_is_fenced_without_purge_or_inference_replay(
    tmp_path, create_request, complete_request, terminology, caplog
):
    class AmbiguousPublisher(Publisher):
        async def publish_and_verify(self, projection):
            self.projections.append(projection)
            raise VoiceIntakeError(
                "github_outcome_ambiguous",
                status_code=503,
                reconciliation_required=True,
            )

    store = queued(tmp_path, create_request, complete_request, terminology)
    inference, publisher = Inference(), AmbiguousPublisher()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(), inference=inference, publisher=publisher,
        owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.state == "reconciliation_required"
    assert status.reconciliation_required
    assert not status.github_verified and not status.server_audio_purged
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]
    assert not await worker.process_once()
    rendered_logs = caplog.text
    for forbidden in ("full session", "canonical terms", "mp3", "Detailed"):
        assert forbidden not in rendered_logs
