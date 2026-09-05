from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import shutil
import time
from collections.abc import Callable
from contextlib import suppress
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from my_data_hub.voice_intake.errors import VoiceIntakeError

from .checkpoint import (
    AccountingPending,
    CheckpointError,
    StageCheckpoint,
    atomic_json,
    fingerprint,
)
from .contracts import InferenceReceipt, PublicationReceipt
from .media import BoundedMediaTools, MediaError
from .settings import VoiceIntakeV2Settings
from .store import ClaimedSession, PublicationProjection, StoreError, VoiceIntakeV2Store

LOGGER = logging.getLogger(__name__)


class StageFailure(RuntimeError):
    """Typed stage failure; ``sent`` prevents an unsafe hidden provider retry."""

    def __init__(
        self, code: str, *, sent: bool, retryable: bool = False,
        retry_after_seconds: int | None = None, ambiguous: bool = False,
        diagnostics: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(code)
        self.code = code
        self.sent = sent
        self.retryable = retryable
        self.retry_after_seconds = retry_after_seconds
        self.ambiguous = ambiguous
        self.diagnostics = dict(diagnostics or {})


class AggregateInference(Protocol):
    async def transcribe(
        self, *, audio_path: Path, recorded_audio_ms: int, terminology: dict[str, Any],
        checkpoint: StageCheckpoint | None = None,
    ) -> InferenceReceipt: ...

    async def summarize(
        self, *, transcript: dict[str, Any], terminology: dict[str, Any],
        checkpoint: StageCheckpoint | None = None,
    ) -> InferenceReceipt: ...


class SessionPublisher(Protocol):
    async def publish_and_verify(self, projection: PublicationProjection) -> PublicationReceipt: ...


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
            try:
                await self.process_once()
            except Exception as exc:
                # Never log exception messages or tracebacks containing dictated text.
                LOGGER.error("voice_v2_worker_failure type=%s", type(exc).__name__)
            with suppress(TimeoutError):
                await asyncio.wait_for(self._stop.wait(), timeout=self.settings.worker_poll_seconds)

    async def process_once(self) -> bool:
        self.store.fence_ambiguous_inference()
        self.store.reap_expired(self.settings.active_ttl_seconds)
        session = self.store.claim(self.owner, self.settings.lease_seconds)
        if session is None:
            return False
        heartbeat = asyncio.create_task(self._heartbeat(session.session_id))
        try:
            await self._process(session)
        except StageFailure as exc:
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
                self._clock() + (exc.retry_after_seconds or 30)
                if not exc.sent and exc.retryable and not exc.ambiguous else None
            )
            self.store.mark_error(
                session.session_id, self.owner, code=exc.code,
                retryable=exc.retryable and not ambiguous and not exc.sent, retry_at=retry_at,
                reconciliation_required=ambiguous,
            )
        except VoiceIntakeError as exc:
            retry_at = (
                self._clock() + (exc.retry_after_seconds or 30)
                if exc.retryable and not exc.reconciliation_required else None
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
        except AccountingPending:
            self.store.mark_error(
                session.session_id, self.owner, code="receipt_accounting_pending",
                retryable=True, retry_at=self._clock() + 30,
            )
        except CheckpointError:
            self.store.mark_error(
                session.session_id, self.owner, code="checkpoint_invalid", retryable=False,
                reconciliation_required=True,
            )
        except StoreError as exc:
            # The verified GitHub receipt is already durable when an ordinary
            # filesystem deletion fails. Expose an explicit retry path without
            # repeating either inference stage or claiming that audio vanished.
            if exc.code == "worker_lease_lost":
                return True
            retryable = exc.code == "server_audio_purge_failed"
            self.store.mark_error(
                session.session_id, self.owner, code=exc.code, retryable=retryable,
                retry_at=self._clock() + 30 if retryable else None,
                reconciliation_required=False,
            )
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat
        return True

    async def _heartbeat(self, session_id: str) -> None:
        while True:
            await asyncio.sleep(self.settings.lease_seconds / 3)
            try:
                self.store.renew_lease(session_id, self.owner, self.settings.lease_seconds)
            except Exception as exc:
                LOGGER.error("voice_v2_lease_renewal_failure session_id=%s type=%s",
                             session_id, type(exc).__name__)
                return

    def _checkpoint(self, session: ClaimedSession, stage: str) -> StageCheckpoint:
        return StageCheckpoint(
            self.store.session_directory(session.session_id), session.session_id,
            stage, fingerprint(session.complete),
            lambda: self.store.set_state(session.session_id, self.owner,
                                        "transcribing" if stage == "transcript" else "summarizing"),
        )

    async def _restore(self, checkpoint: StageCheckpoint) -> InferenceReceipt | None:
        saved = checkpoint.load()
        if saved is None:
            return None
        receipt, accounting = saved
        if accounting is not None:
            resume = getattr(self.inference, "resume_receipt", None)
            if resume is None:
                raise CheckpointError("accounting_recovery_unavailable")
            await resume(checkpoint)
        return receipt

    def _require_capacity(self) -> None:
        if shutil.disk_usage(self.store.root).free < 64 * 1024 * 1024 + 4 * self.settings.max_json_bytes:
            raise StageFailure("spool_capacity_low", sent=False, retryable=True, retry_after_seconds=60)

    async def _process(self, session: ClaimedSession) -> None:
        directory = self.store.session_directory(session.session_id)
        if session.github_verified:
            self.store.renew_lease(session.session_id, self.owner, self.settings.lease_seconds)
            self.store.purge_audio(session.session_id)
            self.store.finish_purge(session.session_id, self.owner)
            return
        normalized = directory / "normalized" / "session.mp3"
        transcript = session.transcript
        summary = session.summary
        if transcript is None:
            checkpoint = self._checkpoint(session, "transcript")
            receipt = await self._restore(checkpoint)
            if receipt is None:
                self._require_capacity()
                paths: list[Path] = []
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
                    paths.append(path)
                await self.media.normalize(tuple(paths), normalized)
                receipt = await self.inference.transcribe(
                    audio_path=normalized, recorded_audio_ms=session.complete["recorded_audio_ms"],
                    terminology=session.terminology, checkpoint=checkpoint,
                )
                checkpoint.save(receipt)
            atomic_json(directory / "transcript.json", receipt.value)
            self.store.persist_transcript(
                session.session_id, self.owner, receipt.value, receipt.request_uid, receipt.limiter
            )
            transcript = receipt.value
        if summary is None:
            checkpoint = self._checkpoint(session, "summary")
            receipt = await self._restore(checkpoint)
            if receipt is None:
                self._require_capacity()
                receipt = await self.inference.summarize(
                    transcript=transcript, terminology=session.terminology, checkpoint=checkpoint,
                )
                checkpoint.save(receipt)
            atomic_json(directory / "summary.json", receipt.value)
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
