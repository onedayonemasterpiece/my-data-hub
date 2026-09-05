from __future__ import annotations

import sqlite3

import pytest

from my_data_hub.voice_intake_v2.store import StoreError, VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import StageFailure, VoiceIntakeV2Worker

from .conftest import SESSION_ID
from .test_worker import Inference, Media, Publisher, matching_complete, queued, settings


def test_apk_upgrade_only_changes_telemetry(create_request, terminology, tmp_path):
    store = VoiceIntakeV2Store(tmp_path)
    store.create_session(create_request, terminology=terminology)
    upgraded = create_request.model_copy(update={"client_version": "1.1.0-rc4"})
    assert store.existing_session(upgraded).session_id == SESSION_ID
    assert store.create_session(upgraded, terminology=terminology)[1]
    with store._connect() as connection:
        assert '"client_version":"1.1.0"' in connection.execute(
            "SELECT create_json FROM sessions"
        ).fetchone()[0]
    changed = upgraded.model_copy(update={"device_label": "another device"})
    with pytest.raises(StoreError, match="session_metadata_conflict"):
        store.existing_session(changed)


@pytest.mark.asyncio
async def test_server_retries_purge_without_phone_or_republication(
    tmp_path, create_request, complete_request, terminology, monkeypatch
):
    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    inference, publisher = Inference(), Publisher()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
        inference=inference, publisher=publisher, clock=lambda: now[0])
    purge = store.purge_audio
    attempts = []

    def flaky_purge(session_id):
        attempts.append(session_id)
        if len(attempts) == 1:
            raise StoreError("server_audio_purge_failed")
        return purge(session_id)

    monkeypatch.setattr(store, "purge_audio", flaky_purge)
    assert await worker.process_once()
    assert store.status(SESSION_ID).retry_at is not None
    assert not await worker.process_once()
    now[0] += 300
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert len(inference.calls) == 2
    assert len(publisher.projections) == 1


@pytest.mark.asyncio
async def test_duplicate_complete_is_not_paid_retry_consent(
    tmp_path, create_request, complete_request, terminology
):
    class Truncated(Inference):
        async def transcribe(self, **kwargs):
            self.calls.append("sent")
            raise StageFailure("response_schema_invalid", sent=True, retryable=True)

    store = queued(tmp_path, create_request, complete_request, terminology)
    inference = Truncated()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
        inference=inference, publisher=Publisher())
    await worker.process_once()
    store.complete(SESSION_ID, matching_complete(store, complete_request))
    assert not await worker.process_once()
    assert len(inference.calls) == 1


def test_persisted_transcript_does_not_claim_summary_was_sent(
    tmp_path, create_request, complete_request, terminology
):
    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    store.claim("old", 60)
    store.persist_transcript(SESSION_ID, "old", {"transcript": "retained"}, "uid", {})
    now[0] += 61
    assert store.fence_ambiguous_inference() == 0
    recovered = store.claim("new", 60)
    assert recovered is not None and recovered.transcript == {"transcript": "retained"}


@pytest.mark.asyncio
async def test_successful_receipt_survives_sqlite_write_failure(
    tmp_path, create_request, complete_request, terminology, monkeypatch
):
    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    inference = Inference()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
        inference=inference, publisher=Publisher(), clock=lambda: now[0])
    persist = store.persist_transcript
    monkeypatch.setattr(store, "persist_transcript", lambda *a, **kw: (_ for _ in ()).throw(
        sqlite3.OperationalError("synthetic disk failure")))
    with pytest.raises(sqlite3.OperationalError):
        await worker.process_once()
    monkeypatch.setattr(store, "persist_transcript", persist)
    now[0] += 61
    await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]


@pytest.mark.asyncio
async def test_response_is_checkpointed_before_limiter_finalization(
    tmp_path, auth_settings, terminology
):
    from my_data_hub.voice_intake_v2.checkpoint import AccountingPending, StageCheckpoint
    from my_data_hub.voice_intake_v2.inference import AggregateGeminiInference

    from .test_inference import Limiter, Requester

    class OfflineAccounting(Limiter):
        async def finalize_generate_content(self, lease, **kwargs):
            self.finalized.append((lease, kwargs))
            if len(self.finalized) == 1:
                raise RuntimeError("accounting offline")

    audio = tmp_path / "session.mp3"
    audio.write_bytes(b"synthetic")
    limiter, requester = OfflineAccounting(), Requester()
    service = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    sends = []
    checkpoint = StageCheckpoint(tmp_path, SESSION_ID, "transcript", "a" * 64,
                                 lambda: sends.append("dispatch"))
    with pytest.raises(AccountingPending):
        await service.transcribe(audio_path=audio, recorded_audio_ms=1000,
                                 terminology=terminology, checkpoint=checkpoint)
    assert len(requester.calls) == 1 and sends == ["dispatch"]
    assert checkpoint.load()[1] is not None
    assert '"secret"' not in checkpoint.path.read_text()
    restarted = AggregateGeminiInference(auth_settings, limiter=limiter, requester=requester)
    restored = await restarted.resume_receipt(checkpoint)
    assert restored.value["transcript"] == "full session"
    assert checkpoint.load()[1] is None
    assert len(requester.calls) == 1
    assert limiter.finalized[0][0].request_uid == limiter.finalized[1][0].request_uid


def test_corrupt_or_foreign_receipt_cannot_unfence_inference(
    tmp_path, create_request, complete_request, terminology
):
    from my_data_hub.voice_intake_v2.checkpoint import StageCheckpoint
    from my_data_hub.voice_intake_v2.contracts import InferenceReceipt
    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    store.claim("old", 60)
    store.set_state(SESSION_ID, "old", "transcribing")
    checkpoint = StageCheckpoint(store.session_directory(SESSION_ID), SESSION_ID,
                                 "transcript", "wrong-manifest")
    checkpoint.save(InferenceReceipt(value={"transcript": "foreign"}, request_uid="other", limiter={}))
    now[0] += 61
    assert store.fence_ambiguous_inference() == 1
    assert store.claim("new", 60) is None
    assert store.status(SESSION_ID).reconciliation_required


def test_expired_owner_cannot_persist_or_extend_lease(
    tmp_path, create_request, complete_request, terminology
):
    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    store.claim("old", 60)
    now[0] += 61
    with pytest.raises(StoreError, match="worker_lease_lost"):
        store.persist_transcript(SESSION_ID, "old", {"transcript": "stale"}, "uid", {})
    with pytest.raises(StoreError, match="worker_lease_lost"):
        store.renew_lease(SESSION_ID, "old", 60)


@pytest.mark.asyncio
async def test_low_disk_prevents_provider_calls_and_keeps_audio(
    tmp_path, create_request, complete_request, terminology, monkeypatch
):
    from types import SimpleNamespace
    store = queued(tmp_path, create_request, complete_request, terminology)
    inference = Inference()
    monkeypatch.setattr("my_data_hub.voice_intake_v2.worker.shutil.disk_usage",
                        lambda _: SimpleNamespace(free=1024))
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
                                 inference=inference, publisher=Publisher())
    await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.retryable and status.retry_at and status.error_code == "spool_capacity_low"
    assert inference.calls == []
    assert (store.session_directory(SESSION_ID) / "chunks").is_dir()


@pytest.mark.asyncio
async def test_quota_preflight_does_not_claim_provider_was_called(
    tmp_path, auth_settings, terminology
):
    from my_data_hub.voice_intake_v2.checkpoint import StageCheckpoint
    from my_data_hub.voice_intake_v2.inference import AggregateGeminiInference

    from .test_inference import Limiter, Requester
    path = tmp_path / "session.mp3"
    path.write_bytes(b"synthetic")
    sent = []
    requester = Requester()
    service = AggregateGeminiInference(auth_settings, limiter=Limiter(deny=True), requester=requester)
    checkpoint = StageCheckpoint(tmp_path, SESSION_ID, "transcript", "a" * 64,
                                 lambda: sent.append(True))
    with pytest.raises(StageFailure):
        await service.transcribe(audio_path=path, recorded_audio_ms=1000,
                                 terminology=terminology, checkpoint=checkpoint)
    assert sent == [] and requester.calls == [] and checkpoint.load() is None
