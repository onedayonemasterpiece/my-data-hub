from __future__ import annotations

import hashlib
import json
import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight, ModelLimit
from my_data_hub.google_ai.http import BoundedHTTPResponse
from my_data_hub.voice_intake_v2.contracts import PublicationReceipt, SessionCompleteRequest
from my_data_hub.voice_intake_v2.inference import AggregateGeminiInference
from my_data_hub.voice_intake_v2.markdown import render_publication
from my_data_hub.voice_intake_v2.media import BoundedMediaTools
from my_data_hub.voice_intake_v2.settings import VoiceIntakeV2Settings
from my_data_hub.voice_intake_v2.store import ChunkReceipt, StoreError, VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import StageFailure, VoiceIntakeV2Worker

from .conftest import SESSION_ID, summary_value

LONG_DURATIONS_MS = [180_000] * 6 + [127_620]
LONG_TOTAL_MS = 1_207_620


@dataclass(frozen=True)
class GeneratedAudio:
    long: Path
    tail: Path
    short: Path


@dataclass(frozen=True)
class ResponseSpec:
    value: dict[str, Any] | str
    finish_reason: str | None = "STOP"


@pytest.fixture(scope="session")
def generated_audio(tmp_path_factory: pytest.TempPathFactory) -> GeneratedAudio:
    if shutil.which("ffmpeg") is None or shutil.which("ffprobe") is None:
        pytest.skip("issue #31 real-media acceptance requires ffmpeg and ffprobe")
    root = tmp_path_factory.mktemp("issue31-real-aac")

    def generate(name: str, duration_ms: int) -> Path:
        output = root / name
        completed = subprocess.run(
            (
                "ffmpeg",
                "-nostdin",
                "-v",
                "error",
                "-f",
                "lavfi",
                "-i",
                "anullsrc=r=16000:cl=mono",
                "-t",
                f"{duration_ms / 1000:.3f}",
                "-c:a",
                "aac",
                "-profile:a",
                "aac_low",
                "-b:a",
                "32k",
                "-movflags",
                "+faststart",
                "-y",
                str(output),
            ),
            check=False,
            capture_output=True,
            timeout=60,
        )
        assert completed.returncode == 0
        assert output.is_file() and output.stat().st_size > 0
        duration = subprocess.run(
            (
                "ffprobe",
                "-v",
                "error",
                "-show_entries",
                "format=duration",
                "-of",
                "default=noprint_wrappers=1:nokey=1",
                str(output),
            ),
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert abs(round(float(duration.stdout.strip()) * 1000) - duration_ms) <= 1
        return output

    return GeneratedAudio(
        long=generate("source-180000.m4a", 180_000),
        tail=generate("source-127620.m4a", 127_620),
        short=generate("source-4000.m4a", 4_000),
    )


class RecordingLimiter:
    def __init__(self) -> None:
        self.sent: list[str] = []
        self.finalized: list[tuple[str, str]] = []

    async def preflight(self, model: str) -> LimiterPreflight:
        return LimiterPreflight(
            limit=ModelLimit(
                model=model, rpm=100, tpm=1_000_000, rpd=10_000,
                tpm_reserve_extra=1_000,
            ),
            candidate_key_ids=("synthetic-key-id",),
            candidate_env_names=frozenset({"SYNTHETIC_KEY"}),
            contract="synthetic_acceptance_v1",
            bucket_strategy="synthetic_acceptance_v1",
        )

    async def reserve_generate_content(self, **kwargs: Any) -> LimiterLease:
        return LimiterLease(
            request_uid=kwargs["request_uid"],
            attempt_no=1,
            api_key_id="synthetic-key-id",
            env_var_name="SYNTHETIC_KEY",
            key_alias="synthetic",
            quota_scope="synthetic",
            reserved_tpm=kwargs["reserved_tpm"],
            contract="synthetic_acceptance_v1",
            bucket_strategy="synthetic_acceptance_v1",
        )

    @staticmethod
    def secret_for(_lease: LimiterLease) -> str:
        return "synthetic-non-secret"

    async def mark_sent(self, lease: LimiterLease) -> None:
        self.sent.append(lease.request_uid)

    async def release_unsent(self, _lease: LimiterLease, *, reason: str) -> None:
        assert reason

    async def report_provider_429(
        self, _lease: LimiterLease, *, retry_after_ms: int | None
    ) -> None:
        assert retry_after_ms is None or retry_after_ms >= 0

    async def finalize_generate_content(self, lease: LimiterLease, **kwargs: Any) -> None:
        self.finalized.append((lease.request_uid, kwargs["provider_status"]))

    @staticmethod
    def public_lease(lease: LimiterLease, *, actual_tpm: int | None) -> dict[str, int | None]:
        return {"reserved_tpm": lease.reserved_tpm, "actual_tpm": actual_tpm}


class ScriptedRequester:
    """Provider double that retains call metadata, never audio/request bodies."""

    def __init__(self, responses: list[ResponseSpec]) -> None:
        self.responses = list(responses)
        self.call_count = 0

    async def request_json(self, _method: str, _url: str, **_kwargs: Any) -> BoundedHTTPResponse:
        self.call_count += 1
        if not self.responses:
            raise AssertionError("unexpected provider call")
        spec = self.responses.pop(0)
        text = (
            spec.value if isinstance(spec.value, str)
            else json.dumps(spec.value, ensure_ascii=False)
        )
        candidate: dict[str, Any] = {"content": {"parts": [{"text": text}]}}
        if spec.finish_reason is not None:
            candidate["finishReason"] = spec.finish_reason
        return BoundedHTTPResponse(
            status=200,
            json_body={
                "candidates": [candidate],
                "usageMetadata": {
                    "promptTokenCount": 100,
                    "candidatesTokenCount": 200,
                    "thoughtsTokenCount": 0,
                    "totalTokenCount": 300,
                },
            },
            retry_after=None,
            content_type="application/json",
        )


class VerifyingPublisher:
    def __init__(self, chunk_directory: Path | None = None) -> None:
        self.calls = 0
        self.files_at_readback = 0
        self.rendered_full_transcript = False
        self.chunk_directory = chunk_directory

    async def publish_and_verify(self, projection: Any) -> PublicationReceipt:
        self.calls += 1
        if self.chunk_directory is not None:
            self.files_at_readback = len(list(self.chunk_directory.glob("*.m4a")))
        rendered = render_publication(projection)
        self.rendered_full_transcript = "## Полная расшифровка" in rendered.source
        return PublicationReceipt(
            github_url="https://github.com/example/synthetic",
            github_commit_sha="d" * 40,
            github_verified=True,
        )


class CrashAfterDurableStageStore(VoiceIntakeV2Store):
    def __init__(self, root: Path, crash_stage: str) -> None:
        self.crash_stage = crash_stage
        self.did_crash = False
        super().__init__(root)

    def _crash(self, stage: str) -> None:
        if self.crash_stage == stage and not self.did_crash:
            self.did_crash = True
            raise StageFailure("injected_restart", sent=False, retryable=True)

    def persist_segment_receipt(self, *args: Any, **kwargs: Any) -> bool:
        result = super().persist_segment_receipt(*args, **kwargs)
        self._crash("segment")
        return result

    def persist_content_verification(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        result = super().persist_content_verification(*args, **kwargs)
        self._crash("content")
        return result

    def persist_summary(self, *args: Any, **kwargs: Any) -> None:
        super().persist_summary(*args, **kwargs)
        self._crash("summary")

    def persist_github_verified(self, *args: Any, **kwargs: Any) -> None:
        super().persist_github_verified(*args, **kwargs)
        self._crash("publication")

    def authorize_purge(self, *args: Any, **kwargs: Any) -> bool:
        result = super().authorize_purge(*args, **kwargs)
        self._crash("authorization")
        return result

    def purge_audio(self, *args: Any, **kwargs: Any) -> None:
        super().purge_audio(*args, **kwargs)
        self._crash("purge")


class FailingPurgeStore(VoiceIntakeV2Store):
    def purge_audio(self, session_id: str) -> None:
        assert all(path.is_file() for path in source_files(self))
        raise StoreError("server_audio_purge_failed", status_code=500)


class CrashAfterFailedReceiptStore(VoiceIntakeV2Store):
    """Simulate process loss after failed evidence, before retry policy is durable."""

    def __init__(self, root: Path, *, clock: Any) -> None:
        self.did_crash = False
        super().__init__(root, clock=clock)

    def persist_segment_receipt(self, *args: Any, **kwargs: Any) -> bool:
        result = super().persist_segment_receipt(*args, **kwargs)
        receipt = args[2]
        if not receipt.accepted and not self.did_crash:
            self.did_crash = True
            raise StageFailure("injected_process_loss", sent=False, retryable=False)
        return result


def worker_settings(root: Path) -> VoiceIntakeV2Settings:
    return VoiceIntakeV2Settings(
        enabled=True,
        spool_root=root,
        max_chunk_bytes=8 * 1024 * 1024,
        max_json_bytes=2 * 1024 * 1024,
        max_session_seconds=3_600,
        active_ttl_seconds=7 * 24 * 3_600,
        lease_seconds=60,
        worker_poll_seconds=0.01,
        ffprobe_timeout_seconds=15,
        ffmpeg_timeout_seconds=120,
        duration_tolerance_ms=2_000,
    )


def segment_value(start_ms: int, end_ms: int, *, short: bool = False) -> dict[str, Any]:
    transcript = "я" if short else " ".join(["синтетическое"] * 48)
    return {
        "transcript": transcript,
        "language": "ru-RU",
        "uncertain_fragments": [],
        "coverage_start_ms": start_ms,
        "coverage_end_ms": end_ms,
    }


def successful_responses(durations: list[int]) -> list[ResponseSpec]:
    cursor = 0
    responses: list[ResponseSpec] = []
    for duration in durations:
        responses.append(ResponseSpec(segment_value(cursor, cursor + duration)))
        cursor += duration
    responses.append(ResponseSpec(summary_value()))
    return responses


def make_inference(auth_settings: Any, responses: list[ResponseSpec]) -> tuple[
    AggregateGeminiInference, ScriptedRequester, RecordingLimiter
]:
    requester = ScriptedRequester(responses)
    limiter = RecordingLimiter()
    return (
        AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester),
        requester,
        limiter,
    )


def queue_session(
    root: Path,
    create_request: Any,
    terminology: dict[str, Any],
    sources: list[tuple[Path, int]],
    *,
    store_type: type[VoiceIntakeV2Store] = VoiceIntakeV2Store,
    crash_stage: str | None = None,
) -> tuple[VoiceIntakeV2Store, SessionCompleteRequest]:
    if crash_stage is None:
        store = store_type(root)
    else:
        assert store_type is VoiceIntakeV2Store
        store = CrashAfterDurableStageStore(root, crash_stage)
    store.create_session(create_request, terminology=terminology)
    manifest: list[dict[str, Any]] = []
    cursor = 0
    for index, (source, duration_ms) in enumerate(sources):
        content = source.read_bytes()
        digest = hashlib.sha256(content).hexdigest()
        destination = (
            store.session_directory(SESSION_ID) / "chunks" / f"{index:05d}-{digest}.m4a"
        )
        shutil.copyfile(source, destination)
        assert destination.is_file() and destination.stat().st_size == len(content)
        start, cursor = cursor, cursor + duration_ms
        receipt = ChunkReceipt(
            session_id=SESSION_ID,
            chunk_index=index,
            sha256=digest,
            duration_ms=duration_ms,
            audio_start_ms=start,
            audio_end_ms=cursor,
            wall_start_ms=start,
            wall_end_ms=cursor,
            size_bytes=len(content),
            path=str(destination),
        )
        store.record_chunk(receipt)
        manifest.append(
            {
                "chunk_index": index,
                "sha256": digest,
                "duration_ms": duration_ms,
                "audio_start_ms": start,
                "audio_end_ms": cursor,
                "wall_start_ms": start,
                "wall_end_ms": cursor,
            }
        )
    complete = SessionCompleteRequest.model_validate(
        {
            "ended_at": "2026-08-28T12:55:03.620+02:00",
            "wall_elapsed_ms": cursor,
            "manual_pause_ms": 0,
            "recorded_audio_ms": cursor,
            "auto_silence_skipped_ms": 0,
            "chunk_count": len(manifest),
            "chunks": manifest,
        }
    )
    store.complete(SESSION_ID, complete)
    return store, complete


def long_sources(generated: GeneratedAudio) -> list[tuple[Path, int]]:
    return [*((generated.long, duration) for duration in LONG_DURATIONS_MS[:-1]),
            (generated.tail, LONG_DURATIONS_MS[-1])]


def short_sources(generated: GeneratedAudio, count: int = 2) -> list[tuple[Path, int]]:
    return [(generated.short, 4_000)] * count


def source_files(store: VoiceIntakeV2Store) -> list[Path]:
    return sorted((store.session_directory(SESSION_ID) / "chunks").glob("*.m4a"))


def assert_all_source_files_present(store: VoiceIntakeV2Store, expected: int) -> None:
    paths = source_files(store)
    assert len(paths) == expected
    assert all(path.exists() and path.is_file() and path.stat().st_size > 0 for path in paths)


def old_client_allows_local_purge(status: Any) -> bool:
    return bool(
        status.state == "published_verified"
        and status.github_verified
        and status.server_audio_purged
    )


@pytest.mark.asyncio
async def test_20_minute_schema_valid_short_segment_fails_closed_with_real_aac(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
) -> None:
    store, _complete = queue_session(
        tmp_path / "spool", create_request, terminology, long_sources(generated_audio)
    )
    assert store.status(SESSION_ID).recorded_audio_ms == LONG_TOTAL_MS
    assert_all_source_files_present(store, 7)
    inference, requester, _limiter = make_inference(
        auth_settings,
        [ResponseSpec(segment_value(0, LONG_DURATIONS_MS[0], short=True))],
    )
    publisher = VerifyingPublisher()
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 1
    assert status.error_code == "segment_content_incomplete"
    assert status.content_verification_status == "failed"
    assert not status.transcription_complete and not status.summary_complete
    assert not status.publication_verified and not status.purge_authorized
    assert not status.audio_purged and not status.server_audio_purged
    assert not old_client_allows_local_purge(status)
    assert publisher.calls == 0
    assert_all_source_files_present(store, 7)
    attempts = store.segment_receipts(SESSION_ID)
    assert len(attempts) == 1 and not attempts[0].accepted
    assert attempts[0].source_sha256 == store.get_chunk(SESSION_ID, 0).sha256
    assert attempts[0].audio_start_ms == 0
    assert attempts[0].audio_end_ms == LONG_DURATIONS_MS[0]
    assert attempts[0].finish_reason == "STOP"


@pytest.mark.parametrize(
    ("body", "finish_reason"),
    [
        (segment_value(0, 4_000), "MAX_TOKENS"),
        ('{"transcript":', "MAX_TOKENS"),
        (segment_value(0, 4_000), "FUTURE_FINISH_REASON"),
    ],
    ids=("parseable-max-tokens", "malformed-max-tokens", "unknown-finish-reason"),
)
@pytest.mark.asyncio
async def test_non_stop_finish_reasons_retain_every_real_source_file(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
    body: dict[str, Any] | str,
    finish_reason: str,
) -> None:
    store, _complete = queue_session(
        tmp_path / "spool", create_request, terminology, short_sources(generated_audio, 2)
    )
    assert_all_source_files_present(store, 2)
    inference, requester, _limiter = make_inference(
        auth_settings, [ResponseSpec(body, finish_reason)]
    )
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=VerifyingPublisher(),
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 1
    assert status.error_code == "response_schema_invalid"
    assert not status.content_verified and not status.publication_verified
    assert not status.purge_authorized and not status.audio_purged
    assert not old_client_allows_local_purge(status)
    assert_all_source_files_present(store, 2)
    attempts = store.segment_receipts(SESSION_ID)
    assert len(attempts) == 1 and not attempts[0].accepted
    assert attempts[0].finish_reason in {"MAX_TOKENS", "UNKNOWN"}


@pytest.mark.parametrize(
    ("coverage_start_ms", "coverage_end_ms"),
    [(1, 4_000), (0, 4_001)],
    ids=("gap", "overlap"),
)
@pytest.mark.asyncio
async def test_gap_or_overlap_response_cannot_create_content_receipt(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
    coverage_start_ms: int,
    coverage_end_ms: int,
) -> None:
    store, _complete = queue_session(
        tmp_path / "spool", create_request, terminology, short_sources(generated_audio, 2)
    )
    assert_all_source_files_present(store, 2)
    inference, requester, _limiter = make_inference(
        auth_settings,
        [ResponseSpec(segment_value(coverage_start_ms, coverage_end_ms))],
    )
    publisher = VerifyingPublisher()
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 1
    assert status.error_code == "segment_coverage_invalid"
    assert status.transcription_segments_completed == 0
    assert not status.content_verified and not status.summary_complete
    assert not status.publication_verified and publisher.calls == 0
    assert not status.purge_authorized and not status.audio_purged
    assert_all_source_files_present(store, 2)


@pytest.mark.asyncio
async def test_incomplete_segment_set_keeps_all_real_files_and_never_summarizes(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
) -> None:
    sources = short_sources(generated_audio, 2)
    store, _complete = queue_session(tmp_path / "spool", create_request, terminology, sources)
    assert_all_source_files_present(store, 2)
    inference, requester, _limiter = make_inference(
        auth_settings,
        [
            ResponseSpec(segment_value(0, 4_000)),
            ResponseSpec(segment_value(4_000, 8_000, short=True)),
        ],
    )
    publisher = VerifyingPublisher()
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 2
    assert status.transcription_segments_completed == 1
    assert not status.transcription_coverage_complete and not status.content_verified
    assert not status.summary_complete and not status.publication_verified
    assert not status.purge_authorized and not status.audio_purged
    assert publisher.calls == 0
    assert_all_source_files_present(store, 2)


@pytest.mark.asyncio
async def test_crash_after_failed_attempt_receipt_fences_without_hidden_replay(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
) -> None:
    root = tmp_path / "spool"
    initial, _complete = queue_session(
        root, create_request, terminology, short_sources(generated_audio, 1)
    )
    assert_all_source_files_present(initial, 1)
    now = [1_000.0]
    store = CrashAfterFailedReceiptStore(root, clock=lambda: now[0])
    inference, requester, _limiter = make_inference(
        auth_settings, [ResponseSpec(segment_value(0, 4_000, short=True))]
    )
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=VerifyingPublisher(),
        owner="crashing-worker",
        clock=lambda: now[0],
    )

    with pytest.raises(StageFailure, match="injected_process_loss"):
        await worker.process_once()

    assert requester.call_count == 1
    interrupted = store.status(SESSION_ID)
    assert interrupted.state == "transcribing"
    attempts = store.segment_receipts(SESSION_ID)
    assert len(attempts) == 1 and not attempts[0].accepted
    assert_all_source_files_present(store, 1)

    now[0] += 61
    reopened = VoiceIntakeV2Store(root, clock=lambda: now[0])
    restarted = VoiceIntakeV2Worker(
        reopened,
        worker_settings(reopened.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=VerifyingPublisher(),
        owner="restarted-worker",
        clock=lambda: now[0],
    )
    assert not await restarted.process_once()
    fenced = reopened.status(SESSION_ID)
    assert requester.call_count == 1
    assert fenced.state == "reconciliation_required"
    assert fenced.reconciliation_required
    assert fenced.error_code == "provider_outcome_ambiguous"
    assert not fenced.content_verified and not fenced.purge_authorized
    assert not fenced.audio_purged and not old_client_allows_local_purge(fenced)
    assert_all_source_files_present(reopened, 1)


@pytest.mark.parametrize(
    "crash_stage",
    ["segment", "content", "summary", "publication", "authorization", "purge"],
)
@pytest.mark.asyncio
async def test_restart_after_each_durable_stage_reuses_receipts_and_publication(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
    crash_stage: str,
) -> None:
    durations = [4_000, 4_000]
    store, complete = queue_session(
        tmp_path / "spool",
        create_request,
        terminology,
        short_sources(generated_audio, 2),
        crash_stage=crash_stage,
    )
    assert_all_source_files_present(store, 2)
    inference, requester, limiter = make_inference(auth_settings, successful_responses(durations))
    chunk_directory = store.session_directory(SESSION_ID) / "chunks"
    publisher = VerifyingPublisher(chunk_directory)
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="first-worker",
    )

    assert await worker.process_once()
    failed = store.status(SESSION_ID)
    assert failed.retryable and failed.error_code == "injected_restart"
    calls_before_restart = requester.call_count
    sent_before_restart = tuple(limiter.sent)
    publication_calls_before_restart = publisher.calls
    if crash_stage != "purge":
        assert_all_source_files_present(store, 2)

    reopened = VoiceIntakeV2Store(store.root)
    reopened.complete(SESSION_ID, complete)
    restarted = VoiceIntakeV2Worker(
        reopened,
        worker_settings(reopened.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="restarted-worker",
    )
    assert await restarted.process_once()

    terminal = reopened.status(SESSION_ID)
    assert requester.call_count == 3
    assert len(set(limiter.sent)) == 3
    assert tuple(limiter.sent[: len(sent_before_restart)]) == sent_before_restart
    assert terminal.transcription_segments_completed == 2
    assert terminal.content_verified and terminal.summary_complete
    assert terminal.publication_verified and terminal.purge_authorized
    assert terminal.audio_purged and terminal.client_audio_purge_allowed
    assert terminal.state == "published_verified"
    assert not chunk_directory.exists()
    if crash_stage in {"publication", "authorization", "purge"}:
        assert publication_calls_before_restart == 1 and publisher.calls == 1
        assert calls_before_restart == 3
    else:
        assert publisher.calls == 1


@pytest.mark.asyncio
async def test_exact_github_readback_alone_keeps_real_audio_and_old_client_closed(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
) -> None:
    store, complete = queue_session(
        tmp_path / "spool", create_request, terminology, short_sources(generated_audio, 1)
    )
    assert_all_source_files_present(store, 1)
    assert store.claim("legacy-worker", 60) is not None
    store.persist_github_verified(
        SESSION_ID,
        "legacy-worker",
        url="https://github.com/example/synthetic",
        commit_sha="d" * 40,
    )
    store.mark_error(
        SESSION_ID, "legacy-worker", code="synthetic_restart", retryable=True
    )
    store.complete(SESSION_ID, complete)
    inference, requester, _limiter = make_inference(auth_settings, [])
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=VerifyingPublisher(),
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 0
    assert status.github_verified and status.publication_verified
    assert not status.content_verified and not status.transcription_complete
    assert not status.purge_authorized and not status.audio_purged
    assert not status.server_audio_purged and not old_client_allows_local_purge(status)
    assert status.error_code == "purge_not_authorized"
    assert_all_source_files_present(store, 1)


@pytest.mark.asyncio
async def test_complete_20_minute_flow_verifies_then_authorizes_then_physically_purges(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
) -> None:
    store, _complete = queue_session(
        tmp_path / "spool", create_request, terminology, long_sources(generated_audio)
    )
    assert_all_source_files_present(store, 7)
    inference, requester, limiter = make_inference(
        auth_settings, successful_responses(LONG_DURATIONS_MS)
    )
    chunk_directory = store.session_directory(SESSION_ID) / "chunks"
    publisher = VerifyingPublisher(chunk_directory)
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 8
    assert len(limiter.sent) == 8 and len(limiter.finalized) == 8
    assert status.gemini_requests_total == 8
    assert status.gemini_requests_completed == 8
    assert status.transcription_segments_completed == 7
    assert status.transcription_coverage_complete and status.content_verified
    assert status.transcription_complete and status.summary_complete
    assert publisher.calls == 1 and publisher.files_at_readback == 7
    assert publisher.rendered_full_transcript
    assert status.publication_verified and status.github_verified
    assert status.purge_authorized and status.audio_purged
    assert status.server_audio_purged and status.client_audio_purge_allowed
    assert old_client_allows_local_purge(status)
    assert not chunk_directory.exists()
    assert not (store.session_directory(SESSION_ID) / "normalized").exists()
    assert (store.session_directory(SESSION_ID) / "transcript.json").is_file()
    assert (store.session_directory(SESSION_ID) / "summary.json").is_file()


@pytest.mark.asyncio
async def test_physical_purge_failure_retains_real_files_and_never_opens_client_gate(
    tmp_path: Path,
    generated_audio: GeneratedAudio,
    create_request: Any,
    terminology: dict[str, Any],
    auth_settings: Any,
) -> None:
    store, _complete = queue_session(
        tmp_path / "spool",
        create_request,
        terminology,
        short_sources(generated_audio, 2),
        store_type=FailingPurgeStore,
    )
    assert_all_source_files_present(store, 2)
    inference, requester, _limiter = make_inference(
        auth_settings, successful_responses([4_000, 4_000])
    )
    publisher = VerifyingPublisher()
    worker = VoiceIntakeV2Worker(
        store,
        worker_settings(store.root),
        media=BoundedMediaTools(ffprobe_timeout=15, ffmpeg_timeout=120),
        inference=inference,
        publisher=publisher,
        owner="acceptance-worker",
    )

    assert await worker.process_once()

    status = store.status(SESSION_ID)
    assert requester.call_count == 3 and publisher.calls == 1
    assert status.content_verified and status.publication_verified
    assert status.purge_authorized and not status.audio_purged
    assert not status.server_audio_purged and not status.client_audio_purge_allowed
    assert not old_client_allows_local_purge(status)
    assert status.retryable and status.error_code == "server_audio_purge_failed"
    assert_all_source_files_present(store, 2)
