from __future__ import annotations

import hashlib
from pathlib import Path

import pytest

from my_data_hub.voice_intake.contracts import ModelUsage, TranscriptPayload
from my_data_hub.voice_intake.errors import VoiceIntakeError
from my_data_hub.voice_intake_v2.content_verification import transcript_plausibility
from my_data_hub.voice_intake_v2.contracts import (
    InferenceReceipt,
    PublicationReceipt,
    SegmentInferenceReceipt,
    SegmentPlausibilityEvidence,
    SessionCompleteRequest,
)
from my_data_hub.voice_intake_v2.media import MediaProbe
from my_data_hub.voice_intake_v2.settings import VoiceIntakeV2Settings
from my_data_hub.voice_intake_v2.store import ChunkReceipt, VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import StageFailure, VoiceIntakeV2Worker

from .conftest import SESSION_ID, summary_value


class Media:
    def __init__(self, durations: list[int]) -> None:
        self.durations = durations
        self.normalized: list[tuple[Path, Path]] = []

    async def probe(self, path: Path) -> MediaProbe:
        index = int(path.name.split("-", 1)[0])
        return MediaProbe(self.durations[index], "aac", "LC", 16_000, 1)

    async def normalize(self, chunks: tuple[Path, ...], output: Path) -> None:
        assert len(chunks) == 1
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_bytes(b"private-mp3-" + chunks[0].read_bytes())
        self.normalized.append((chunks[0], output))


class Inference:
    def __init__(self) -> None:
        self.calls: list[tuple[str, int | None]] = []

    async def transcribe_segment(self, **kwargs) -> SegmentInferenceReceipt:
        index = kwargs["chunk_index"]
        start, end = kwargs["source_audio_start_ms"], kwargs["source_audio_end_ms"]
        self.calls.append(("segment", index))
        transcript_text = " ".join([f"segment{index}"] * 60)
        value = TranscriptPayload(transcript=transcript_text, language="ru-RU").model_dump(
            mode="json"
        )
        plausibility = transcript_plausibility(transcript_text, end - start)
        return SegmentInferenceReceipt(
            chunk_index=index,
            source_sha256=kwargs["source_sha256"],
            input_audio_sha256=hashlib.sha256(kwargs["audio_path"].read_bytes()).hexdigest(),
            input_audio_mime_type="audio/mpeg",
            source_audio_start_ms=start,
            source_audio_end_ms=end,
            coverage_start_ms=start,
            coverage_end_ms=end,
            coverage_ms=end - start,
            coverage_ratio=1.0,
            finish_reason="STOP",
            transcript_receipt_sha256=hashlib.sha256(f"receipt-{index}".encode()).hexdigest(),
            value=value,
            request_uid=f"segment-{index}-uid",
            limiter={"reserved_tpm": 1000 + index},
            usage=ModelUsage(
                input_tokens=100, output_tokens=50, thought_tokens=0, total_tokens=150
            ),
            plausibility=SegmentPlausibilityEvidence.model_validate(plausibility),
        )

    async def summarize(self, **_kwargs) -> InferenceReceipt:
        self.calls.append(("summary", None))
        return InferenceReceipt(
            value=summary_value(), request_uid="summary-uid", limiter={"reserved_tpm": 12345}
        )


class Publisher:
    def __init__(self, chunks: Path | None = None) -> None:
        self.calls = 0
        self.chunks = chunks
        self.files_at_readback = 0

    async def publish_and_verify(self, _projection) -> PublicationReceipt:
        self.calls += 1
        if self.chunks is not None:
            self.files_at_readback = len(list(self.chunks.glob("*.m4a")))
        return PublicationReceipt(
            github_url="https://github.com/example",
            github_commit_sha="d" * 40,
            github_verified=True,
        )


class CrashAfterStore(VoiceIntakeV2Store):
    def __init__(self, root: Path, crash_stage: str) -> None:
        self.crash_stage, self.did_crash = crash_stage, False
        super().__init__(root)

    def _crash(self, stage: str) -> None:
        if self.crash_stage == stage and not self.did_crash:
            self.did_crash = True
            raise StageFailure("injected_restart", sent=False, retryable=True)

    def persist_segment_receipt(self, *args, **kwargs):
        result = super().persist_segment_receipt(*args, **kwargs)
        self._crash("segment")
        return result

    def persist_content_verification(self, *args, **kwargs):
        result = super().persist_content_verification(*args, **kwargs)
        self._crash("content")
        return result

    def persist_summary(self, *args, **kwargs):
        result = super().persist_summary(*args, **kwargs)
        self._crash("summary")
        return result

    def persist_github_verified(self, *args, **kwargs):
        result = super().persist_github_verified(*args, **kwargs)
        self._crash("publication")
        return result

    def authorize_purge(self, *args, **kwargs):
        result = super().authorize_purge(*args, **kwargs)
        self._crash("authorization")
        return result

    def purge_audio(self, *args, **kwargs):
        result = super().purge_audio(*args, **kwargs)
        self._crash("purge")
        return result


def settings(root: Path) -> VoiceIntakeV2Settings:
    return VoiceIntakeV2Settings(
        enabled=True, spool_root=root, max_chunk_bytes=1024 * 1024,
        max_json_bytes=1024 * 1024, max_session_seconds=3600,
        active_ttl_seconds=7 * 24 * 3600, lease_seconds=60,
        worker_poll_seconds=.1, ffprobe_timeout_seconds=5,
        ffmpeg_timeout_seconds=30, duration_tolerance_ms=2000,
    )


def queued(
    root: Path, create_request, terminology, durations: list[int],
    *, crash_stage: str | None = None,
) -> tuple[VoiceIntakeV2Store, SessionCompleteRequest]:
    store = (
        CrashAfterStore(root, crash_stage) if crash_stage is not None
        else VoiceIntakeV2Store(root)
    )
    store.create_session(create_request, terminology=terminology)
    chunks, cursor = [], 0
    for index, duration in enumerate(durations):
        audio = f"synthetic-m4a-{index}".encode()
        digest = hashlib.sha256(audio).hexdigest()
        path = store.session_directory(SESSION_ID) / "chunks" / f"{index:05d}-{digest}.m4a"
        path.write_bytes(audio)
        start, cursor = cursor, cursor + duration
        store.record_chunk(ChunkReceipt(
            session_id=SESSION_ID, chunk_index=index, sha256=digest,
            duration_ms=duration, audio_start_ms=start, audio_end_ms=cursor,
            wall_start_ms=start, wall_end_ms=cursor, size_bytes=len(audio), path=str(path),
        ))
        chunks.append({
            "chunk_index": index, "sha256": digest, "duration_ms": duration,
            "audio_start_ms": start, "audio_end_ms": cursor,
            "wall_start_ms": start, "wall_end_ms": cursor,
        })
    complete = SessionCompleteRequest.model_validate({
        "ended_at": "2026-08-28T13:34:56+02:00", "wall_elapsed_ms": cursor,
        "manual_pause_ms": 0, "recorded_audio_ms": cursor,
        "auto_silence_skipped_ms": 0, "chunk_count": len(chunks), "chunks": chunks,
    })
    store.complete(SESSION_ID, complete)
    return store, complete


def files(store: VoiceIntakeV2Store) -> list[Path]:
    return sorted((store.session_directory(SESSION_ID) / "chunks").glob("*.m4a"))


@pytest.mark.asyncio
async def test_twenty_minutes_short_valid_transcript_fails_closed_with_real_files(
    tmp_path, create_request, terminology
):
    durations = [180_000] * 6 + [127_620]

    class Short(Inference):
        async def transcribe_segment(self, **kwargs):
            self.calls.append(("segment", kwargs["chunk_index"]))
            raise StageFailure("segment_content_incomplete", sent=True, retryable=False)

    store, _ = queued(tmp_path / "spool", create_request, terminology, durations)
    inference, publisher = Short(), Publisher()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(durations), inference=inference,
        publisher=publisher, owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.error_code == "segment_content_incomplete"
    assert not status.content_verified and not status.publication_verified
    assert not status.purge_authorized and not status.audio_purged
    assert len(files(store)) == 7 and publisher.calls == 0


@pytest.mark.parametrize("shape", ["parseable_object", "malformed_json"])
@pytest.mark.asyncio
async def test_max_tokens_parseable_or_malformed_never_purges(
    tmp_path, create_request, terminology, shape, caplog
):
    class MaxTokens(Inference):
        async def transcribe_segment(self, **kwargs):
            self.calls.append(("segment", kwargs["chunk_index"]))
            raise StageFailure(
                "response_schema_invalid", sent=True, retryable=False,
                diagnostics={"finish_reason": "MAX_TOKENS", "actual": {"type": shape}},
            )

    store, _ = queued(tmp_path / "spool", create_request, terminology, [240_000])
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media([240_000]), inference=MaxTokens(),
        publisher=Publisher(), owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.error_code == "response_schema_invalid"
    assert not status.content_verified and not status.purge_authorized and not status.audio_purged
    assert len(files(store)) == 1
    assert SESSION_ID not in caplog.text


@pytest.mark.asyncio
async def test_missing_segment_coverage_reuses_prior_receipt_and_retains_files(
    tmp_path, create_request, terminology
):
    class MissingThenSuccess(Inference):
        failed = False

        async def transcribe_segment(self, **kwargs):
            if kwargs["chunk_index"] == 1 and not self.failed:
                self.failed = True
                self.calls.append(("segment", 1))
                raise StageFailure("segment_coverage_invalid", sent=True, retryable=True)
            return await super().transcribe_segment(**kwargs)

    durations = [120_000, 120_000]
    store, complete = queued(tmp_path / "spool", create_request, terminology, durations)
    inference = MissingThenSuccess()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(durations), inference=inference,
        publisher=Publisher(), owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.transcription_segments_completed == 1 and not status.content_verified
    assert not status.publication_verified and len(files(store)) == 2
    store.complete(SESSION_ID, complete)
    assert await worker.process_once()
    assert inference.calls.count(("segment", 0)) == 1
    assert inference.calls.count(("segment", 1)) == 2
    assert store.status(SESSION_ID).state == "published_verified"


@pytest.mark.parametrize(
    "crash_stage", ["segment", "content", "summary", "publication", "authorization", "purge"]
)
@pytest.mark.asyncio
async def test_restart_after_durable_stage_does_not_repeat_successful_calls(
    tmp_path, create_request, terminology, crash_stage
):
    durations = [120_000, 120_000]
    store, complete = queued(
        tmp_path / "spool", create_request, terminology, durations, crash_stage=crash_stage
    )
    inference, publisher = Inference(), Publisher()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(durations), inference=inference,
        publisher=publisher, owner="worker",
    )
    assert await worker.process_once()
    failed = store.status(SESSION_ID)
    assert failed.retryable and failed.error_code == "injected_restart"
    before = list(inference.calls)
    if crash_stage != "purge":
        assert len(files(store)) == 2
    store = VoiceIntakeV2Store(store.root)
    store.complete(SESSION_ID, complete)
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media(durations), inference=inference,
        publisher=publisher, owner="restarted-worker",
    )
    assert await worker.process_once()
    terminal = store.status(SESSION_ID)
    assert terminal.state == "published_verified"
    assert terminal.content_verified and terminal.publication_verified
    assert terminal.purge_authorized and terminal.audio_purged and files(store) == []
    for call in before:
        assert inference.calls.count(call) == before.count(call)
    if crash_stage in {"publication", "authorization", "purge"}:
        assert publisher.calls == 1


@pytest.mark.asyncio
async def test_github_readback_alone_cannot_delete_physical_audio(
    tmp_path, create_request, terminology
):
    store, complete = queued(tmp_path / "spool", create_request, terminology, [240_000])
    assert store.claim("legacy", 60) is not None
    store.persist_github_verified(
        SESSION_ID, "legacy", url="https://github.com/example", commit_sha="d" * 40
    )
    store.mark_error(SESSION_ID, "legacy", code="restart", retryable=True)
    store.complete(SESSION_ID, complete)
    inference = Inference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media([240_000]), inference=inference,
        publisher=Publisher(), owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.publication_verified and not status.content_verified
    assert not status.purge_authorized and not status.audio_purged
    assert status.error_code == "purge_not_authorized" and len(files(store)) == 1
    assert inference.calls == []


@pytest.mark.asyncio
async def test_legacy_transcript_without_content_receipt_never_skips_segments(
    tmp_path, create_request, terminology
):
    store, complete = queued(tmp_path / "spool", create_request, terminology, [240_000])
    assert store.claim("legacy", 60) is not None
    store.persist_transcript(
        SESSION_ID, "legacy",
        TranscriptPayload(transcript="legacy short", language="ru-RU").model_dump(mode="json"),
        "legacy-uid", {"reserved_tpm": 1},
    )
    store.mark_error(SESSION_ID, "legacy", code="restart", retryable=True)
    store.complete(SESSION_ID, complete)
    inference = Inference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media([240_000]), inference=inference,
        publisher=Publisher(), owner="worker",
    )
    assert await worker.process_once()
    assert inference.calls == [("segment", 0), ("summary", None)]
    assert store.status(SESSION_ID).content_verified


@pytest.mark.asyncio
async def test_full_multichunk_flow_authorizes_then_physically_deletes(
    tmp_path, create_request, terminology
):
    durations = [180_000] * 6 + [127_620]
    store, _ = queued(tmp_path / "spool", create_request, terminology, durations)
    chunk_dir = store.session_directory(SESSION_ID) / "chunks"
    inference, publisher, media = Inference(), Publisher(chunk_dir), Media(durations)
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=media, inference=inference,
        publisher=publisher, owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert inference.calls == [*(('segment', i) for i in range(7)), ("summary", None)]
    assert len(media.normalized) == 7 and publisher.files_at_readback == 7
    assert status.transcription_segments_completed == 7
    assert status.content_verified and status.summary_complete and status.publication_verified
    assert status.purge_authorized and status.audio_purged and status.client_audio_purge_allowed
    assert not chunk_dir.exists()
    assert not (store.session_directory(SESSION_ID) / "normalized").exists()
    assert (store.session_directory(SESSION_ID) / "transcript.json").is_file()
    assert (store.session_directory(SESSION_ID) / "summary.json").is_file()


@pytest.mark.asyncio
async def test_ambiguous_publication_retains_audio_and_does_not_replay_inference(
    tmp_path, create_request, terminology
):
    class Ambiguous(Publisher):
        async def publish_and_verify(self, _projection):
            self.calls += 1
            raise VoiceIntakeError(
                "github_outcome_ambiguous", status_code=503, reconciliation_required=True
            )

    store, _ = queued(tmp_path / "spool", create_request, terminology, [240_000])
    inference = Inference()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media([240_000]), inference=inference,
        publisher=Ambiguous(), owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.reconciliation_required and status.content_verified
    assert not status.publication_verified and not status.purge_authorized and not status.audio_purged
    assert len(files(store)) == 1 and not await worker.process_once()
    assert inference.calls == [("segment", 0), ("summary", None)]


@pytest.mark.asyncio
async def test_source_bytes_revalidated_before_inference(
    tmp_path, create_request, terminology
):
    store, _ = queued(tmp_path / "spool", create_request, terminology, [240_000])
    files(store)[0].write_bytes(b"tampered")
    inference, publisher = Inference(), Publisher()
    worker = VoiceIntakeV2Worker(
        store, settings(store.root), media=Media([240_000]), inference=inference,
        publisher=publisher, owner="worker",
    )
    assert await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.error_code == "audio_receipt_mismatch"
    assert not status.content_verified and not status.publication_verified
    assert inference.calls == [] and publisher.calls == 0
