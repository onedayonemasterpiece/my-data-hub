# ruff: noqa: RUF001
import hashlib
import json
from dataclasses import replace

import httpx
import pytest

from my_data_hub.voice_intake.errors import GitHubPublicationConflict
from my_data_hub.voice_intake_v2.analysis import AnalysisDelivery, AnalysisSettings, build_intent
from my_data_hub.voice_intake_v2.contracts import InferenceReceipt
from my_data_hub.voice_intake_v2.store import VoiceIntakeV2Store
from my_data_hub.voice_intake_v2.worker import VoiceIntakeV2Worker

from .test_publisher import MemoryPublisher, projection
from .test_worker import Inference, Media, Publisher, queued, settings


def config():
    return AnalysisSettings(repository="onedayonemasterpiece/idea-hub", owner_id="trusted-owner",
                            enabled=True, endpoint="https://hub.invalid/api/integrations/voice-analysis/intents",
                            token="test")


def test_trigger_final_transcript_only_and_bound_identity(create_request, complete_request, terminology):
    p = projection(create_request, complete_request, terminology)
    p = replace(p, transcript={**p.transcript, "transcript": "  А теперь ЗАДАЙ  мне вопросы.  "})
    intent = build_intent(p, "a" * 40, config())
    assert intent["owner_id"] == "trusted-owner"
    assert intent["trigger"] == {"phrase": "ЗАДАЙ  мне вопросы", "start": 9, "end": 27}
    assert intent == build_intent(p, "a" * 40, config())
    assert intent["intent_id"] != build_intent(p, "b" * 40, config())["intent_id"]
    assert build_intent(p, "a" * 40, replace(config(), enabled=False)) is None
    assert build_intent(p, "a" * 40, replace(config(), owner_id="")) is None
    p = replace(p, transcript={**p.transcript, "transcript": "обычная заметка"},
                summary={**p.summary, "short_summary": "задай мне вопросы"})
    assert build_intent(p, "a" * 40, config()) is None


class TriggerInference(Inference):
    async def transcribe(self, **kwargs):
        result = await super().transcribe(**kwargs)
        return InferenceReceipt(value={**result.value, "transcript": "задай мне вопросы"},
                                request_uid=result.request_uid, limiter=result.limiter)


@pytest.mark.asyncio
async def test_publication_purge_independent_and_durable_retry(tmp_path, create_request, complete_request, terminology):
    store = queued(tmp_path, create_request, complete_request, terminology)
    inference = TriggerInference()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(), inference=inference,
                                publisher=Publisher(), analysis_settings=config())
    await worker.process_once()
    status = store.status(create_request.session_id)
    assert status.github_verified and status.server_audio_purged
    # Durable intent survives a new store instance after audio purge.
    store = VoiceIntakeV2Store(store.root)
    delivery = AnalysisDelivery(store, config())
    seen = []
    def lost(request):
        seen.append(json.loads(request.content))
        raise httpx.ReadTimeout("lost after server accepted")
    async with httpx.AsyncClient(transport=httpx.MockTransport(lost)) as client:
        assert await delivery.process_once(client)
    with store._connect() as db:
        row = db.execute("SELECT * FROM analysis_outbox").fetchone()
        assert row["state"] == "pending"
        db.execute("UPDATE analysis_outbox SET retry_at=0")
    def accepted(request):
        body = json.loads(request.content)
        assert body == seen[0]
        return httpx.Response(200, json={"intent_id": body["intent_id"], "source": body["source"],
                                         "state": "accepted", "request_id": "r1", "event_id": "e1"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
        assert await delivery.process_once(client)
        assert not await delivery.process_once(client)
    assert len(inference.calls) == 2
    assert not await worker.process_once()


@pytest.mark.asyncio
async def test_source_projection_preserves_original_and_reconciles(auth_settings, create_request,
                                                                  complete_request, terminology):
    publisher = MemoryPublisher(auth_settings)
    p = projection(create_request, complete_request, terminology)
    p = replace(p, transcript={**p.transcript, "transcript": "задай мне вопросы"})
    publication = await publisher.publish_and_verify(p)
    intent = build_intent(p, publication.github_commit_sha, config())
    source = intent["source"]
    original = publisher.commits[publication.github_commit_sha][source["path"]]
    markdown = "## Вопросы от аналитика\n\n1. Какой результат нужен?"
    revision = hashlib.sha256(markdown.encode()).hexdigest()
    sha = await publisher.project_analysis(intent_id=intent["intent_id"], source=source,
                                           revision=revision, markdown=markdown)
    assert publisher.commits[publication.github_commit_sha][source["path"]] == original
    assert markdown in publisher.commits[sha][source["path"]]
    assert await publisher.project_analysis(intent_id=intent["intent_id"], source=source,
                                           revision=revision, markdown=markdown) == sha
    assert (await publisher.publish_and_verify(p)).github_commit_sha == publication.github_commit_sha
    publisher.commits[sha][source["path"]] = publisher.commits[sha][source["path"]].replace(
        "Какой результат", "Правка владельца")
    new = "## Анализ\n\nРезультат"
    with pytest.raises(GitHubPublicationConflict, match="manually_changed"):
        await publisher.project_analysis(intent_id=intent["intent_id"], source=source,
                                         revision=hashlib.sha256(new.encode()).hexdigest(), markdown=new,
                                         expected_previous_revision=revision)


@pytest.mark.asyncio
@pytest.mark.parametrize("status,expected", [(202, "pending"), (503, "pending"), (409, "blocked"), (403, "blocked")])
async def test_delivery_failure_classification(
    tmp_path, create_request, complete_request, terminology, status, expected
):
    store = queued(tmp_path, create_request, complete_request, terminology)
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(), inference=TriggerInference(),
                                publisher=Publisher(), analysis_settings=config())
    await worker.process_once()
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(status))) as client:
        await AnalysisDelivery(store, config()).process_once(client)
    with store._connect() as db:
        assert db.execute("SELECT state FROM analysis_outbox").fetchone()[0] == expected
    assert store.status(create_request.session_id).server_audio_purged


@pytest.mark.asyncio
async def test_restart_after_atomic_receipt_before_purge(tmp_path, create_request, complete_request, terminology):
    store = queued(tmp_path, create_request, complete_request, terminology)
    inference = TriggerInference()
    publisher = Publisher()
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(), inference=inference,
                                publisher=publisher, analysis_settings=config())
    purge = store.purge_audio
    def crash(_session_id):
        raise KeyboardInterrupt("simulated process death")
    store.purge_audio = crash
    with pytest.raises(KeyboardInterrupt):
        await worker.process_once()
    store.purge_audio = purge
    with store._connect() as db:
        assert db.execute("SELECT count(*) FROM analysis_outbox").fetchone()[0] == 1
        db.execute("UPDATE sessions SET lease_until=0")
    restarted = VoiceIntakeV2Worker(store, settings(store.root), media=Media(), inference=inference,
                                   publisher=publisher, analysis_settings=config())
    await restarted.process_once()
    assert store.status(create_request.session_id).server_audio_purged
    assert len(publisher.projections) == 1
    assert len(inference.calls) == 2


@pytest.mark.asyncio
async def test_projection_rejects_changed_transcript(auth_settings, create_request, complete_request, terminology):
    publisher = MemoryPublisher(auth_settings)
    p = projection(create_request, complete_request, terminology)
    p = replace(p, transcript={**p.transcript, "transcript": "задай мне вопросы"})
    published = await publisher.publish_and_verify(p)
    intent = build_intent(p, published.github_commit_sha, config())
    source = intent["source"]
    new_sha = "e" * 40
    publisher.commits[new_sha] = dict(publisher.commits[published.github_commit_sha])
    publisher.commits[new_sha][source["path"]] = publisher.commits[new_sha][source["path"]].replace(
        "задай мне вопросы", "новая версия текста")
    publisher.head_sha = new_sha
    with pytest.raises(GitHubPublicationConflict, match="analysis_source_changed"):
        await publisher.project_analysis(intent_id=intent["intent_id"], source=source,
                                         revision=hashlib.sha256(b"result").hexdigest(), markdown="result")


@pytest.mark.asyncio
async def test_explicit_transport_redrive_preserves_intent(tmp_path, create_request, complete_request, terminology):
    store = queued(tmp_path, create_request, complete_request, terminology)
    worker = VoiceIntakeV2Worker(store, settings(store.root), media=Media(), inference=TriggerInference(),
                                publisher=Publisher(), analysis_settings=config())
    await worker.process_once()
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(401))) as client:
        await AnalysisDelivery(store, config()).process_once(client)
    with store._connect() as db:
        row = db.execute("SELECT * FROM analysis_outbox").fetchone()
        original = row["payload_json"]
    assert store.redrive_analysis_intent(row["intent_id"], owner_id=config().owner_id, repository=config().repository)
    def accepted(request):
        body = json.loads(request.content)
        assert body == json.loads(original)
        return httpx.Response(200, json={"intent_id": body["intent_id"], "source": body["source"],
                                         "state": "accepted", "request_id": "r1", "event_id": "e1"})
    async with httpx.AsyncClient(transport=httpx.MockTransport(accepted)) as client:
        assert await AnalysisDelivery(store, config()).process_once(client)
    with store._connect() as db:
        assert db.execute("SELECT state FROM analysis_outbox").fetchone()[0] == "delivered"


@pytest.mark.asyncio
async def test_stale_projection_cannot_overwrite_newer_valid_block(auth_settings, create_request,
                                                                complete_request, terminology):
    publisher = MemoryPublisher(auth_settings)
    p = projection(create_request, complete_request, terminology)
    p = replace(p, transcript={**p.transcript, "transcript": "задай мне вопросы"})
    published = await publisher.publish_and_verify(p)
    intent = build_intent(p, published.github_commit_sha, config())
    async def write(text, previous=None):
        return await publisher.project_analysis(
            intent_id=intent["intent_id"], source=intent["source"], markdown=text,
            revision=hashlib.sha256(text.encode()).hexdigest(), expected_previous_revision=previous)
    await write("first")
    first = hashlib.sha256(b"first").hexdigest()
    # A fetched an intermediate projection and paused. B advanced the note.
    newer = await write("newer from B", first)
    with pytest.raises(GitHubPublicationConflict, match="revision_conflict"):
        await write("stale from A", first)
    assert publisher.head_sha == newer
    assert "newer from B" in publisher.commits[newer][intent["source"]["path"]]


def test_operator_status_works_while_delivery_disabled(tmp_path, monkeypatch, capsys):
    from my_data_hub.voice_intake_v2.analysis import main
    store = VoiceIntakeV2Store(tmp_path / "spool")
    monkeypatch.setenv("MY_DATA_HUB_VOICE_ANALYSIS_ENABLED", "false")
    monkeypatch.delenv("MY_DATA_HUB_VOICE_ANALYSIS_TOKEN", raising=False)
    monkeypatch.setattr("sys.argv", ["analysis", "--spool", str(store.root), "--intent-id", "missing", "--status"])
    main()
    assert capsys.readouterr().out.strip() == "null"


@pytest.mark.asyncio
async def test_removed_heading_enters_reconciliation(auth_settings, tmp_path, create_request,
                                                     complete_request, terminology):
    publisher = MemoryPublisher(auth_settings)
    p = projection(create_request, complete_request, terminology)
    p = replace(p, transcript={**p.transcript, "transcript": "задай мне вопросы"})
    published = await publisher.publish_and_verify(p)
    intent = build_intent(p, published.github_commit_sha, config())
    new_sha = "e" * 40
    publisher.commits[new_sha] = dict(publisher.commits[published.github_commit_sha])
    source_path = intent["source"]["path"]
    publisher.commits[new_sha][source_path] = publisher.commits[new_sha][source_path].replace(
        "## Полная расшифровка", "## Удалённый заголовок")
    publisher.head_sha = new_sha
    store = VoiceIntakeV2Store(tmp_path / "spool")
    with store._connect() as db:
        db.execute(
            "INSERT INTO analysis_outbox(intent_id,payload_json,state,receipt_json,created_at) VALUES(?,?,?,?,0)",
                   (intent["intent_id"], json.dumps(intent), "delivered", "{}"))
    response = {"intent_id": intent["intent_id"], "source": intent["source"],
                "projection": {"markdown": "result", "revision": hashlib.sha256(b"result").hexdigest()}}
    delivery = AnalysisDelivery(store, config(), publisher)
    async with httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json=response))) as client:
        assert await delivery.project_once(client)
        assert not await delivery.project_once(client)
    status = store.analysis_status(intent["intent_id"])
    assert status["state"] == "projection_reconciliation_required"
    assert status["last_error"] == "analysis_source_structure_changed"
    assert publisher.head_sha == new_sha


def test_private_config_file_fail_closed_without_secret_logging(tmp_path, monkeypatch, caplog):
    path = tmp_path / "analysis.json"
    payload = {"enabled": True, "owner_id": "owner", "endpoint": config().endpoint, "token": "secret-not-logged"}
    path.write_text(json.dumps(payload))
    path.chmod(0o600)
    monkeypatch.setenv("MY_DATA_HUB_VOICE_ANALYSIS_CONFIG_FILE", str(path))
    loaded = AnalysisSettings.from_env(config().repository)
    assert loaded.delivery_ready
    assert loaded.owner_id == "owner"
    assert payload["token"] not in repr(loaded)
    path.chmod(0o644)
    assert not AnalysisSettings.from_env(config().repository).enabled
    path.chmod(0o600)
    path.write_text('{"token": "secret-not-logged"}')
    assert not AnalysisSettings.from_env(config().repository).enabled
    assert payload["token"] not in caplog.text


@pytest.mark.asyncio
async def test_terminal_projection_stops_polling(tmp_path):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    intent = {"intent_id": "voice:old", "owner_id": config().owner_id,
              "source": {"repository": config().repository}}
    with store._connect() as db:
        db.execute("INSERT INTO analysis_outbox(intent_id,payload_json,state,receipt_json,created_at) "
                   "VALUES(?,?,'delivered','{}',0)", (intent["intent_id"], json.dumps(intent)))
    class Sink:
        async def project_analysis(self, **kwargs):
            return "verified-commit"
    calls = []
    def get(request):
        calls.append(request)
        return httpx.Response(200, json={**intent, "projection_terminal": True, "case_id": "voice:old",
            "input_revision": 1, "member_intent_ids": ["voice:old"],
            "projection": {"markdown": "final", "revision": hashlib.sha256(b"final").hexdigest()}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(get)) as client:
        delivery = AnalysisDelivery(store, config(), Sink())
        assert await delivery.project_once(client)
        store._clock = lambda: 99999999999
        assert not await delivery.project_once(client)
    assert len(calls) == 1
    assert store.analysis_status("voice:old")["state"] == "completed"


@pytest.mark.asyncio
async def test_new_linked_input_refreshes_completed_note_once(tmp_path):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    revision = hashlib.sha256(b"previous").hexdigest()
    def payload(name, owner=None):
        return {"intent_id": name, "owner_id": owner or config().owner_id,
                "source": {"repository": config().repository}}
    with store._connect() as db:
        for name, owner in [("old", None), ("foreign", "another-owner"), ("new", None)]:
            receipt = {"projection_revision": revision, "projection_commit": "old-commit",
                       "case_id": "old", "input_revision": 1}
            db.execute("INSERT INTO analysis_outbox(intent_id,payload_json,state,receipt_json,created_at) "
                       "VALUES(?,?,?,?,0)", (name, json.dumps(payload(name, owner)),
                         "delivered" if name == "new" else "completed", json.dumps(receipt)))
    writes = []
    class Sink:
        async def project_analysis(self, **kwargs):
            writes.append(kwargs["intent_id"])
            return "new-commit"
    def get(request):
        name = request.url.path.rsplit("/", 1)[-1]
        return httpx.Response(200, json={**payload(name), "projection_terminal": True, "case_id": "new",
            "input_revision": 2, "member_intent_ids": ["old", "new", "foreign", "never-enqueued"],
            "projection": {"markdown": "updated", "revision": hashlib.sha256(b"updated").hexdigest()}})
    async with httpx.AsyncClient(transport=httpx.MockTransport(get)) as client:
        delivery = AnalysisDelivery(store, config(), Sink())
        assert await delivery.project_once(client)  # New input reawakens old, not foreign or unknown.
        assert store.analysis_status("old")["state"] == "delivered"
        assert await delivery.project_once(client)
        store._clock = lambda: 99999999999
        assert not await delivery.project_once(client)
    assert writes == ["new", "old"]
    assert store.analysis_status("old")["state"] == "completed"
    assert store.analysis_status("foreign")["state"] == "completed"
    assert store.analysis_status("never-enqueued") is None


def test_new_case_observed_during_old_projection_prevents_stale_completion(tmp_path):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    with store._connect() as db:
        for name in ["old", "new"]:
            payload = {"intent_id": name, "owner_id": config().owner_id,
                       "source": {"repository": config().repository}}
            receipt = {"case_id": "old", "input_revision": 1}
            db.execute("INSERT INTO analysis_outbox(intent_id,payload_json,state,receipt_json,created_at) "
                       "VALUES(?,?,'delivered',?,0)", (name, json.dumps(payload), json.dumps(receipt)))
    old = store.claim_analysis_projection()
    new = store.claim_analysis_projection()
    assert old["intent_id"] == "old" and new["intent_id"] == "new"
    store.finish_analysis_projection("new", attempt=new["attempts"], completed=True,
        receipt={"case_id": "new", "input_revision": 2}, linked_members=["old", "new"],
        owner_id=config().owner_id, repository=config().repository)
    store.finish_analysis_projection("old", attempt=old["attempts"], completed=True,
        receipt={"case_id": "old", "input_revision": 1}, linked_members=["old"])
    assert store.analysis_status("old")["state"] == "delivered"
    store._clock = lambda: 99999999999
    retry = store.claim_analysis_projection()
    assert retry["intent_id"] == "old"
    store.finish_analysis_projection("old", attempt=retry["attempts"], completed=True,
        receipt={"case_id": "new", "input_revision": 2})
    assert store.analysis_status("old")["state"] == "completed"
