from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from .contracts import InferenceReceipt, PublicationReceipt
from .media import BoundedMediaTools, MediaError
from .settings import VoiceIntakeV2Settings
from .store import ClaimedSession, PublicationProjection, StoreError, VoiceIntakeV2Store


class StageFailure(RuntimeError):
    """Typed stage failure; ``sent`` prevents an unsafe hidden provider retry."""

    def __init__(
        self, code: str, *, sent: bool, retryable: bool = False,
        retry_after_seconds: int | None = None, ambiguous: bool = False,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.sent = sent
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.ambiguous = ambiguous


class AggregateInference(Protocol):
    async def transcribe(
        self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any]
    ) -> InferenceReceipt: ...

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
        inference: AggregateInference,
        publisher: SessionPublisher,
        owner: str | None = None,
        clock=time.time,
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
        except (MediaError, StoreError) as exc:
            self.store.mark_error(
                session.session_id, self.owner, code=str(exc), retryable=False,
                reconciliation_required=False,
            )
        return True

    async def _process(self, session: ClaimedSession) -> None:
        directory = self.store.session_directory(session.session_id)
        normalized = directory / "normalized" / "session.mp3"
        transcript = session.transcript
        summary = session.summary
        if transcript is None:
            paths: list[Path] = []
            for chunk in session.chunks:
                path = Path(chunk.path)
                if path.resolve().parent != (directory / "chunks").resolve():
                    raise MediaError("audio_path_invalid")
                probe = await self.media.probe(path)
                if abs(probe.duration_ms - chunk.duration_ms) > self.settings.duration_tolerance_ms:
                    raise MediaError("audio_duration_mismatch")
                paths.append(path)
            await self.media.normalize(tuple(paths), normalized)
            self.store.set_state(session.session_id, self.owner, "transcribing")
            receipt = await self.inference.transcribe(
                audio_path=normalized,
                recorded_audio_ms=session.complete["recorded_audio_ms"],
                terminology=session.terminology,
            )
            _atomic_json(directory / "transcript.json", receipt.value)
            self.store.persist_transcript(
                session.session_id, self.owner, receipt.value, receipt.request_uid, receipt.limiter
            )
            transcript = receipt.value
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
        self.store.purge_audio(session.session_id)
        self.store.finish_purge(session.session_id, self.owner)
