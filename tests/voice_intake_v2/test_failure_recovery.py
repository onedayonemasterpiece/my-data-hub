"""Real adapter + durable worker regressions for multi-day stranded recordings."""
from __future__ import annotations

import asyncio
import json
import sqlite3

import pytest

from my_data_hub.google_ai.http import BoundedHTTPError, BoundedHTTPResponse
from my_data_hub.voice_intake_v2.inference import AggregateGeminiInference
from my_data_hub.voice_intake_v2.store import VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import VoiceIntakeV2Worker

from .conftest import SESSION_ID, summary_value
from .test_inference import Limiter
from .test_worker import Media, Publisher, queued, settings


class Responses:
    def __init__(self, statuses):
        self.statuses = list(statuses)
        self.calls = []

    async def request_json(self, method, url, **kwargs):
        self.calls.append(kwargs)
        status = self.statuses.pop(0)
        if isinstance(status, Exception):
            raise status
        if status != 200:
            return BoundedHTTPResponse(status, {"error": {"message": "PRIVATE"}}, "75", "application/json")
        audio = len(kwargs["json_body"]["contents"][0]["parts"]) == 2
        value = (
            {"transcript": "synthetic voice", "language": "ru", "uncertain_fragments": []}
            if audio else summary_value()
        )
        return BoundedHTTPResponse(200, {
            "candidates": [{"finishReason": "STOP", "content": {"parts": [{"text": json.dumps(value)}]}}],
        }, None, "application/json")


def harness(tmp_path, create_request, complete_request, terminology, auth_settings, responses, limiter=None):
    store = queued(tmp_path, create_request, complete_request, terminology)
    now = [store._clock()]
    store._clock = lambda: now[0]
    requester = Responses(responses)
    limiter = limiter or Limiter()
    inference = AggregateGeminiInference(auth_settings, requester=requester, limiter=limiter)
    publisher = Publisher()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(),
        inference=inference, publisher=publisher, clock=lambda: now[0])
    return store, now, requester, limiter, worker, publisher


@pytest.mark.asyncio
@pytest.mark.parametrize("stage", ["transcript", "summary"])
async def test_long_provider_outage_recovers_after_restart_without_phone(
    tmp_path, create_request, complete_request, terminology, auth_settings, stage, caplog
):
    responses = ([200] if stage == "summary" else []) + [503] * 6 + [200] * (2 if stage == "transcript" else 1)
    store, now, requester, limiter, worker, publisher = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings, responses)
    for index in range(6):
        assert await worker.process_once()
        status = store.status(SESSION_ID)
        assert status.retryable and not status.reconciliation_required
        assert not await worker.process_once()
        # Restart the SQLite store/worker; durable counters and retry_at survive.
        worker.store = VoiceIntakeV2Store(store.root, clock=lambda: now[0])
        now[0] += max(75, min(900, 60 * 2 ** index)) + 1
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
    assert len(requester.calls) == 8
    assert len(publisher.projections) == 1
    assert len({r["request_uid"] for r in limiter.reserves}) == 8
    assert "PRIVATE" not in caplog.text
    with store._connect() as connection:
        failures = connection.execute("SELECT code,http_status FROM inference_failures").fetchall()
        assert [tuple(r) for r in failures] == [("provider_rejected_request", 503)] * 6
    if stage == "summary":
        assert sum(len(c["json_body"]["contents"][0]["parts"]) == 2 for c in requester.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [400, 401, 403, 404])
async def test_permanent_http_failure_is_not_retried(
    tmp_path, create_request, complete_request, terminology, auth_settings, status
):
    store, now, requester, _, worker, _ = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings, [status])
    assert await worker.process_once()
    now[0] += 86400
    assert not await worker.process_once()
    assert not store.status(SESSION_ID).retryable
    assert len(requester.calls) == 1
    assert (store.session_directory(SESSION_ID) / "chunks").is_dir()


@pytest.mark.asyncio
async def test_html_503_is_definite_but_timeout_still_fences(
    tmp_path, create_request, complete_request, terminology, auth_settings
):
    store, now, requester, _, worker, _ = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings,
        [BoundedHTTPError("malformed_json", status=503), BoundedHTTPError("timeout")])
    assert await worker.process_once()
    assert store.status(SESSION_ID).retryable
    now[0] += 61
    assert await worker.process_once()
    assert store.status(SESSION_ID).reconciliation_required
    now[0] += 86400
    assert not await worker.process_once()
    assert len(requester.calls) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [429, 503])
async def test_failure_receipt_survives_accounting_outage(
    tmp_path, create_request, complete_request, terminology, auth_settings, status
):
    class OfflineOnce(Limiter):
        async def finalize_generate_content(self, lease, **kwargs):
            await super().finalize_generate_content(lease, **kwargs)
            if len(self.finalized) == 1:
                raise RuntimeError("accounting unavailable")

    store, now, requester, limiter, worker, _ = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings, [status, 200, 200], OfflineOnce())
    assert await worker.process_once()
    assert store.status(SESSION_ID).error_code == "receipt_accounting_pending"
    receipt = store.session_directory(SESSION_ID) / "transcript.failure.json"
    assert receipt.stat().st_mode & 0o777 == 0o600
    assert "PRIVATE" not in receipt.read_text() and '"secret"' not in receipt.read_text()
    now[0] += 31
    assert await worker.process_once()
    assert len(requester.calls) == 1  # Only accounting is replayed.
    assert limiter.finalized[0][0].request_uid == limiter.finalized[1][0].request_uid
    now[0] += 76
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"


@pytest.mark.asyncio
async def test_old_failure_receipt_cannot_unfence_crash_during_new_send(
    tmp_path, create_request, complete_request, terminology, auth_settings
):
    store, now, requester, _, worker, _ = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings, [503])
    assert await worker.process_once()
    now[0] += 76
    async def crash(*args, **kwargs):
        raise asyncio.CancelledError()
    requester.request_json = crash
    with pytest.raises(asyncio.CancelledError):
        await worker.process_once()
    now[0] += 61
    assert not await worker.process_once()
    assert store.status(SESSION_ID).reconciliation_required


@pytest.mark.asyncio
async def test_http_rejections_do_not_spend_schema_budget(
    tmp_path, create_request, complete_request, terminology, auth_settings
):
    store, now, _, _, worker, _ = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings,
        [503] * 4 + [BoundedHTTPError("malformed_json", status=200), 200, 200])
    for _ in range(5):
        assert await worker.process_once()
        assert store.status(SESSION_ID).retryable
        now[0] += 901
    # The installed Android app hard-codes response_schema_invalid as manual
    # reconciliation, even when retryable=true. Preserve automatic polling.
    assert store.status(SESSION_ID).error_code == "provider_output_retry_pending"
    with store._connect() as connection:
        assert connection.execute("SELECT error_code FROM sessions").fetchone()[0] == "response_schema_invalid"
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"


@pytest.mark.asyncio
async def test_crash_after_definite_response_restores_failure_without_paid_replay(
    tmp_path, create_request, complete_request, terminology, auth_settings, monkeypatch
):
    store, now, requester, _, worker, _ = harness(
        tmp_path, create_request, complete_request, terminology, auth_settings, [503, 200, 200])
    mark = store.mark_error
    def crash(*args, **kwargs):
        raise sqlite3.OperationalError("synthetic write failure")
    monkeypatch.setattr(store, "mark_error", crash)
    with pytest.raises(sqlite3.OperationalError):
        await worker.process_once()
    monkeypatch.setattr(store, "mark_error", mark)
    now[0] += 61
    assert await worker.process_once()
    assert len(requester.calls) == 1
    assert not store.status(SESSION_ID).reconciliation_required
    with store._connect() as connection:
        assert connection.execute("SELECT COUNT(*) FROM inference_failures").fetchone()[0] == 1
    now[0] += 76
    assert await worker.process_once()
    assert store.status(SESSION_ID).state == "published_verified"
