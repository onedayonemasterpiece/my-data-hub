from __future__ import annotations

import pytest

from my_data_hub.voice_intake_v2.worker import StageFailure, VoiceIntakeV2Worker

from .conftest import SESSION_ID
from .test_worker import Inference, Media, Publisher, queued, settings


@pytest.mark.asyncio
async def test_explicit_quota_rejection_recovers_without_phone_and_no_hot_loop(
    tmp_path, create_request, complete_request, terminology
):
    class QuotaOnce(Inference):
        rejected = False

        async def transcribe(self, **kwargs):
            if not self.rejected:
                self.rejected = True
                raise StageFailure("provider_429", sent=True, retryable=True)
            return await super().transcribe(**kwargs)

    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    inference = QuotaOnce()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
        inference=inference, publisher=Publisher(), clock=lambda: now[0])
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "waiting_quota"
    assert store.status(SESSION_ID).retry_at is not None
    assert not await worker.process_once()
    now[0] += 61
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert [call[0] for call in inference.calls] == ["transcribe", "summarize"]


@pytest.mark.asyncio
async def test_ambiguous_quota_error_stays_fenced(
    tmp_path, create_request, complete_request, terminology
):
    class Ambiguous(Inference):
        async def transcribe(self, **kwargs):
            raise StageFailure("provider_429", sent=True, retryable=True, ambiguous=True)

    store = queued(tmp_path, create_request, complete_request, terminology)
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
        inference=Ambiguous(), publisher=Publisher())
    await worker.process_once()
    status = store.status(SESSION_ID)
    assert status.reconciliation_required and not status.retryable
    assert status.retry_at is None
