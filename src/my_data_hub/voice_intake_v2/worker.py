from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from my_data_hub.voice_intake.errors import VoiceIntakeError

from .contracts import InferenceReceipt, PublicationReceipt, SegmentInferenceReceipt
from .media import BoundedMediaTools, MediaError
from .settings import VoiceIntakeV2Settings
from .store import (
    ClaimedSession,
    PublicationProjection,
    StoredSegmentReceipt,
    StoreError,
    VoiceIntakeV2Store,
)

LOGGER = logging.getLogger(__name__)


class StageFailure(RuntimeError):
    """Typed stage failure; ``sent`` prevents an unsafe hidden provider retry."""

    def __init__(
        self, code: str, *, sent: bool, retryable: bool = False,
        retry_after_seconds: int | None = None, ambiguous: bool = False,
        diagnostics: dict[str, Any] | None = None,
        provider_request_uid: str | None = None,
        finish_reason: str | None = None,
        usage: dict[str, int] | None = None,
        segment_attempt: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.sent = sent
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.ambiguous = ambiguous
        self.diagnostics = dict(diagnostics or {})
        self.provider_request_uid = provider_request_uid
        self.finish_reason = finish_reason
        self.usage = dict(usage or {})
        self.segment_attempt = dict(segment_attempt or {})


class SegmentInference(Protocol):
    async def transcribe_segment(
        self,
        *,
        audio_path: Path,
        source_path: Path,
        chunk_index: int,
        source_sha256: str,
        source_audio_start_ms: int,
        source_audio_end_ms: int,
        expected_speech_ms: int,
        terminology: dict[str, Any],
    ) -> SegmentInferenceReceipt: ...

    async def summarize(
        self, *, transcript: dict[str, Any], terminology: dict[str, Any]
    ) -> InferenceReceipt: ...


class SessionPublisher(Protocol):
    async def publish_and_verify(self, projection: PublicationProjection) -> PublicationReceipt: ...


def _atomic_json(path: Path, value: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        with temporary.open("w", encoding="utf-8") as handle:
            json.dump(value, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.chmod(temporary, 0o600)
        os.replace(temporary, path)
        descriptor = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


class VoiceIntakeV2Worker:
    """Exactly one bounded spool worker; each claimed stage executes once."""

    def __init__(
        self,
        store: VoiceIntakeV2Store,
        settings: VoiceIntakeV2Settings,
        *,
        media: BoundedMediaTools,
        inference: SegmentInference,
        publisher: SessionPublisher,
        owner: str | None = None,
        clock: Callable[[], float] = time.time,
    ) -> None:
        self.store = store
        self.settings = settings
        self.media = media
        self.inference = inference
        self.publisher = publisher
        self.owner = owner or f"voice-v2-{uuid4()}"
        self._clock = clock
        self._task: asyncio.Task[None] | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is None or self._task.done():
            self._stop.clear()
            self._task = asyncio.create_task(self.run(), name="voice-intake-v2-worker")

    async def stop(self) -> None:
        self._stop.set()
        if self._task is not None:
            await self._task
            self._task = None

    async def run(self) -> None:
        while not self._stop.is_set():
            with suppress(Exception):
                await self.process_once()
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.worker_poll_seconds)

    async def process_once(self) -> bool:
        self.store.fence_ambiguous_inference()
        self.store.reap_expired(self.settings.active_ttl_seconds)
        session = self.store.claim(self.owner, self.settings.lease_seconds)
        if session is None:
            return False
        try:
            await self._process(session)
        except StageFailure as exc:
            if exc.segment_attempt and exc.provider_request_uid:
                attempt = exc.segment_attempt
                self.store.persist_segment_receipt(
                    session.session_id,
                    self.owner,
                    StoredSegmentReceipt(
                        session_id=session.session_id,
                        chunk_index=int(attempt["chunk_index"]),
                        source_sha256=str(attempt["source_sha256"]),
                        audio_start_ms=int(attempt["audio_start_ms"]),
                        audio_end_ms=int(attempt["audio_end_ms"]),
                        coverage_start_ms=int(attempt["coverage_start_ms"]),
                        coverage_end_ms=int(attempt["coverage_end_ms"]),
                        provider_request_uid=exc.provider_request_uid,
                        finish_reason=exc.finish_reason or "MISSING",
                        schema_version=str(attempt["schema_version"]),
                        accepted=False,
                        transcript=None,
                        coverage={
                            "verdict": "failed",
                            "error_code": exc.code,
                            "usage": exc.usage,
                            "diagnostics": exc.diagnostics,
                        },
                        limiter={},
                    ),
                )
            if exc.diagnostics:
                diagnostic = exc.diagnostics
                LOGGER.warning(
                    "voice_v2_stage_failure session_id=%s code=%s schema=%s "
                    "schema_version=%s json_path=%s expected=%s actual=%s "
                    "missing_fields=%s extra_fields=%s finish_reason=%s token_counts=%s "
                    "configured_max_output_tokens=%s truncated=%s",
                    session.session_id,
                    exc.code,
                    diagnostic.get("schema"),
                    diagnostic.get("schema_version"),
                    diagnostic.get("json_path"),
                    json.dumps(diagnostic.get("expected"), sort_keys=True),
                    json.dumps(diagnostic.get("actual"), sort_keys=True),
                    json.dumps(diagnostic.get("missing_fields"), sort_keys=True),
                    json.dumps(diagnostic.get("extra_fields"), sort_keys=True),
                    diagnostic.get("finish_reason"),
                    json.dumps(diagnostic.get("token_counts"), sort_keys=True),
                    diagnostic.get("configured_max_output_tokens"),
                    diagnostic.get("truncated"),
                )
            ambiguous = exc.ambiguous
            retry_at = (
                self._clock() + exc.retry_after_seconds
                if not exc.sent and exc.retryable and exc.retry_after_seconds else None
            )
            self.store.mark_error(
                session.session_id, self.owner, code=exc.code,
                retryable=exc.retryable and not ambiguous, retry_at=retry_at,
                reconciliation_required=ambiguous,
            )
        except VoiceIntakeError as exc:
            retry_at = (
                self._clock() + exc.retry_after_seconds
                if exc.retryable and exc.retry_after_seconds
                else None
            )
            self.store.mark_error(
                session.session_id,
                self.owner,
                code=exc.code,
                retryable=exc.retryable and not exc.reconciliation_required,
                retry_at=retry_at,
                reconciliation_required=exc.reconciliation_required,
            )
        except MediaError as exc:
            self.store.mark_error(
                session.session_id, self.owner, code=str(exc), retryable=False,
                reconciliation_required=False,
            )
        except StoreError as exc:
            # The verified GitHub receipt is already durable when an ordinary
            # filesystem deletion fails. Expose an explicit retry path without
            # repeating either inference stage or claiming that audio vanished.
            retryable = exc.code == "server_audio_purge_failed"
            self.store.mark_error(
                session.session_id, self.owner, code=exc.code, retryable=retryable,
                reconciliation_required=False,
            )
        return True

    async def _validated_source_paths(
        self, session: ClaimedSession, directory: Path
    ) -> dict[int, Path]:
        """Reconcile every source receipt with a physical file before content work."""
        paths: dict[int, Path] = {}
        for chunk in session.chunks:
            path = Path(chunk.path)
            if path.resolve().parent != (directory / "chunks").resolve():
                raise MediaError("audio_path_invalid")
            digest = hashlib.sha256()
            observed_size = 0
            try:
                with path.open("rb") as handle:
                    while block := handle.read(1024 * 1024):
                        observed_size += len(block)
                        if observed_size > self.settings.max_chunk_bytes:
                            raise MediaError("audio_receipt_mismatch")
                        digest.update(block)
            except OSError as exc:
                raise MediaError("audio_receipt_mismatch") from exc
            if observed_size != chunk.size_bytes or digest.hexdigest() != chunk.sha256:
                raise MediaError("audio_receipt_mismatch")
            probe = await self.media.probe(path)
            if abs(probe.duration_ms - chunk.duration_ms) > self.settings.duration_tolerance_ms:
                raise MediaError("audio_duration_mismatch")
            paths[chunk.chunk_index] = path
        if set(paths) != set(range(len(session.chunks))):
            raise MediaError("chunks_missing")
        return paths

    @staticmethod
    def _stored_segment(
        session_id: str, receipt: SegmentInferenceReceipt
    ) -> StoredSegmentReceipt:
        # Keep the provider/source receipt hash distinct from the canonical
        # transcript JSON hash derived by the store.
        coverage = {
            "input_audio_sha256": receipt.input_audio_sha256,
            "input_audio_mime_type": receipt.input_audio_mime_type,
            "coverage_ms": receipt.coverage_ms,
            "coverage_ratio": receipt.coverage_ratio,
            "usage": receipt.usage.model_dump(mode="json"),
            "plausibility": receipt.plausibility.model_dump(mode="json"),
            "inference_receipt_sha256": receipt.transcript_receipt_sha256,
        }
        return StoredSegmentReceipt(
            session_id=session_id,
            chunk_index=receipt.chunk_index,
            source_sha256=receipt.source_sha256,
            audio_start_ms=receipt.source_audio_start_ms,
            audio_end_ms=receipt.source_audio_end_ms,
            coverage_start_ms=receipt.coverage_start_ms,
            coverage_end_ms=receipt.coverage_end_ms,
            provider_request_uid=receipt.request_uid,
            finish_reason=receipt.finish_reason,
            schema_version=receipt.schema_version,
            accepted=True,
            transcript=receipt.value,
            coverage=coverage,
            limiter=receipt.limiter,
            transcript_receipt_sha256=None,
            inference_receipt_sha256=receipt.transcript_receipt_sha256,
        )

    def _finish_verified_publication(self, session_id: str) -> None:
        state = self.store.verification_state(session_id)
        if state.audio_purged:
            self.store.finish_purge(session_id, self.owner)
            return
        if not state.purge_authorized:
            self.store.authorize_purge(
                session_id, self.owner, policy_version="voice-v2-content-publication-v1"
            )
        self.store.purge_audio(session_id)
        self.store.finish_purge(session_id, self.owner)

    async def _process(self, session: ClaimedSession) -> None:
        directory = self.store.session_directory(session.session_id)
        verification = self.store.verification_state(session.session_id)

        # A crash after durable purge authorization must only finish deletion;
        # source files may already be partially absent at this point.
        if verification.purge_authorized or verification.audio_purged:
            self._finish_verified_publication(session.session_id)
            return

        paths = await self._validated_source_paths(session, directory)
        if verification.publication_verified:
            # Exact GitHub readback is intentionally insufficient on its own:
            # authorize_purge also requires the independent content receipt.
            self._finish_verified_publication(session.session_id)
            return

        transcript = session.transcript if verification.content_verified else None
        summary = session.summary
        if not verification.content_verified:
            accepted = {
                receipt.chunk_index: receipt
                for receipt in self.store.segment_receipts(
                    session.session_id, accepted_only=True
                )
            }
            for chunk in session.chunks:
                durable = accepted.get(chunk.chunk_index)
                if durable is not None:
                    if (
                        durable.source_sha256 != chunk.sha256
                        or durable.audio_start_ms != chunk.audio_start_ms
                        or durable.audio_end_ms != chunk.audio_end_ms
                        or durable.coverage_start_ms != chunk.audio_start_ms
                        or durable.coverage_end_ms != chunk.audio_end_ms
                        or durable.finish_reason != "STOP"
                    ):
                        raise StoreError("segment_receipt_source_mismatch")
                    continue
                normalized = (
                    directory / "normalized"
                    / f"{chunk.chunk_index:05d}-{chunk.sha256}.mp3"
                )
                await self.media.normalize((paths[chunk.chunk_index],), normalized)
                self.store.set_state(session.session_id, self.owner, "transcribing")
                receipt = await self.inference.transcribe_segment(
                    audio_path=normalized,
                    source_path=paths[chunk.chunk_index],
                    chunk_index=chunk.chunk_index,
                    source_sha256=chunk.sha256,
                    source_audio_start_ms=chunk.audio_start_ms,
                    source_audio_end_ms=chunk.audio_end_ms,
                    expected_speech_ms=chunk.audio_end_ms - chunk.audio_start_ms,
                    terminology=session.terminology,
                )
                self.store.persist_segment_receipt(
                    session.session_id,
                    self.owner,
                    self._stored_segment(session.session_id, receipt),
                )
            transcript = self.store.persist_content_verification(
                session.session_id,
                self.owner,
                schema_version="2.0.0",
                verifier_version="bounded-per-chunk-v1",
                verification={
                    "mode": "ordered_exact_source_segments",
                    "segment_count": len(session.chunks),
                    "coverage": "contiguous_no_gap_no_overlap",
                },
            )
            _atomic_json(directory / "transcript.json", transcript)
            # A summary stored before the independent content receipt may have
            # been derived from an incomplete legacy aggregate; never reuse it.
            summary = None
        elif transcript is not None:
            _atomic_json(directory / "transcript.json", transcript)
        if transcript is None:
            raise StoreError("content_transcript_missing")
        if summary is None:
            self.store.set_state(session.session_id, self.owner, "summarizing")
            receipt = await self.inference.summarize(transcript=transcript, terminology=session.terminology)
            _atomic_json(directory / "summary.json", receipt.value)
            self.store.persist_summary(
                session.session_id, self.owner, receipt.value, receipt.request_uid, receipt.limiter
            )
            summary = receipt.value
        projection = self.store.publication_projection(session.session_id, self.owner)
        self.store.set_state(session.session_id, self.owner, "publishing")
        published = await self.publisher.publish_and_verify(projection)
        if not published.github_verified:
            raise StageFailure("github_readback_failed", sent=False, retryable=False)
        # Receipt durability precedes audio deletion. A restart can therefore
        # safely finish purge without repeating either Gemini stage.
        self.store.persist_github_verified(
            session.session_id, self.owner,
            url=published.github_url, commit_sha=published.github_commit_sha,
        )
        self._finish_verified_publication(session.session_id)
