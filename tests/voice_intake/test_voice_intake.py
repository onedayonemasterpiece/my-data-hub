from __future__ import annotations

import asyncio
import hashlib
import json
import stat
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from my_data_hub.google_ai.contracts import LimiterLease, LimiterPreflight, ModelLimit
from my_data_hub.google_ai.http import BoundedHTTPResponse
from my_data_hub.voice_intake.api import attach_voice_intake_routes
from my_data_hub.voice_intake.contracts import (
    ChunkTranscriptResponse,
    ModelUsage,
    RemoteProgress,
    SessionCompleteRequest,
    SessionSummaryResponse,
    SummaryPayload,
    TranscriptPayload,
)
from my_data_hub.voice_intake.errors import VoiceIntakeError
from my_data_hub.voice_intake.gemini import GeminiVoiceService
from my_data_hub.voice_intake.github import IdeaHubPublisher, PublicationReceipt
from my_data_hub.voice_intake.markdown import (
    build_registry_entry,
    insert_registry_entry,
    paths_for,
    render_session_detail,
    render_source_packet,
    render_voice_index,
)
from my_data_hub.voice_intake.settings import VoiceIntakeSettings
from my_data_hub.voice_intake.terminology import (
    SessionTerminologySnapshots,
    parse_terminology_card,
)

TOKEN = "x" * 40
SESSION_ID = "voice-20260828-123456-abcdef12"


def settings(*, terminology_state_path: str = "") -> VoiceIntakeSettings:
    return VoiceIntakeSettings(
        enabled=True,
        device_token=TOKEN,
        model="gemini-3.1-flash-lite",
        allowed_models=("gemini-3.1-flash-lite",),
        max_audio_bytes=8 * 1024 * 1024,
        max_json_bytes=2 * 1024 * 1024,
        provider_timeout_seconds=180,
        github_token="github-token",
        github_repository="onedayonemasterpiece/idea-hub",
        github_branch="main",
        limiter_supabase_url="https://example.supabase.co",
        limiter_supabase_service_key="service-key",
        normal_key_envs=("GOOGLE_API_KEY",),
        terminology_state_path=terminology_state_path,
    )


def wav_bytes() -> bytes:
    data = b"\x00\x00" * 1600
    fmt = (
        b"fmt "
        + (16).to_bytes(4, "little")
        + (1).to_bytes(2, "little")
        + (1).to_bytes(2, "little")
        + (16000).to_bytes(4, "little")
        + (32000).to_bytes(4, "little")
        + (2).to_bytes(2, "little")
        + (16).to_bytes(2, "little")
    )
    body = fmt + b"data" + len(data).to_bytes(4, "little") + data
    return b"RIFF" + (len(body) + 4).to_bytes(4, "little") + b"WAVE" + body


class FakeService:
    def __init__(self) -> None:
        self.transcription_terminology: list[str] = []
        self.summary_terminology: list[str] = []

    async def transcribe(self, **kwargs: Any) -> ChunkTranscriptResponse:
        self.transcription_terminology.append(str(kwargs.get("terminology") or ""))
        return ChunkTranscriptResponse(
            session_id=kwargs["session_id"],
            chunk_index=kwargs["chunk_index"],
            model="gemini-3.1-flash-lite",
            prompt_version="voice-transcribe-v1",
            transcript=TranscriptPayload(
                transcript="Тестовая расшифровка",
                language="ru-RU",
                uncertain_fragments=[],
            ),
            usage=ModelUsage(
                input_tokens=10,
                output_tokens=2,
                thought_tokens=0,
                total_tokens=12,
            ),
            request_uid="request-1",
            limiter={},
        )

    async def summarize(self, _chunks: object, **_kwargs: Any) -> SessionSummaryResponse:
        self.summary_terminology.append(str(_kwargs.get("terminology") or ""))
        return SessionSummaryResponse(
            model="gemini-3.1-flash-lite",
            prompt_version="voice-summary-v1",
            summary=SummaryPayload(
                title="Тестовая идея",
                short_summary="Кратко",
                detailed_summary="Подробно",
            ),
            usage=ModelUsage(
                input_tokens=10,
                output_tokens=2,
                thought_tokens=0,
                total_tokens=12,
            ),
            request_uid="summary-1",
            limiter={},
        )


class FakePublisher:
    def __init__(self) -> None:
        self.terminology_calls = 0
        self.published_terminology: list[object] = []

    async def resolve_terminology(self) -> object:
        self.terminology_calls += 1
        return parse_terminology_card(
            """
schema_version: 1.0.0
card_id: idea-hub-voice-terminology
rules:
  - Use canonical project names only when acoustically compatible.
entries:
  - canonical: KenigEvents
    kind: product
    common_misrecognitions: [tionicavents]
""",
            source_path="config/voice-terminology.yaml",
            source_commit_sha="b" * 40,
            source_blob_sha="d" * 40,
        )

    async def status(self, _session_id: str) -> RemoteProgress:
        return RemoteProgress(state="processing", recording_finished=True)

    async def publish(self, **_kwargs: Any) -> PublicationReceipt:
        self.published_terminology.append(_kwargs["terminology"])
        return PublicationReceipt(
            source_path="inbox/voice/2026/08/test.md",
            detail_path="registry/sessions/2026/08/test.md",
            commit_sha="a" * 40,
            github_url="https://github.example/voice.md",
        )


def build_app() -> FastAPI:
    app = FastAPI()

    @app.api_route("/{data_path:path}", methods=["GET", "POST", "PUT"])
    def catch_all(data_path: str) -> None:
        raise HTTPException(status_code=503, detail=data_path)

    return attach_voice_intake_routes(
        app,
        settings=settings(),
        service=FakeService(),  # type: ignore[arg-type]
        publisher=FakePublisher(),  # type: ignore[arg-type]
    )


def create_payload(session_id: str = SESSION_ID) -> dict[str, Any]:
    return {
        "session_id": session_id,
        "started_at": "2026-08-28T12:34:56+02:00",
        "timezone": "Europe/Kaliningrad",
        "device_label": "Samsung S21 Ultra",
    }


def complete_payload() -> dict[str, Any]:
    return {
        "started_at": "2026-08-28T12:34:56+02:00",
        "ended_at": "2026-08-28T12:35:56+02:00",
        "timezone": "Europe/Kaliningrad",
        "device_label": "Samsung S21 Ultra",
        "duration_ms": 60000,
        "chunk_count": 1,
        "chunks": [
            {
                "chunk_index": 0,
                "start_ms": 0,
                "end_ms": 60000,
                "sha256": "a" * 64,
                "transcript": {
                    "transcript": "Тестовая расшифровка",
                    "language": "ru-RU",
                    "uncertain_fragments": [],
                },
            }
        ],
    }


def test_voice_routes_precede_control_plane_catch_all() -> None:
    client = TestClient(build_app())
    created = client.post(
        "/voice-intake/v1/sessions",
        json=create_payload(),
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert created.status_code == 200
    audio = wav_bytes()
    response = client.put(
        f"/voice-intake/v1/sessions/{SESSION_ID}/chunks/0",
        content=audio,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "audio/wav",
            "X-Chunk-SHA256": hashlib.sha256(audio).hexdigest(),
            "X-Chunk-Duration-Ms": "1000",
        },
    )
    assert response.status_code == 200
    assert response.json()["transcript"]["transcript"] == "Тестовая расшифровка"


def test_voice_api_requires_device_token() -> None:
    client = TestClient(build_app())
    payload = create_payload()
    assert client.post("/voice-intake/v1/sessions", json=payload).status_code == 401
    accepted = client.post(
        "/voice-intake/v1/sessions",
        json=payload,
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert accepted.status_code == 200
    assert accepted.json()["state"] == "receiving"
    assert accepted.json()["terminology_card_status"] == "current"
    assert accepted.json()["terminology_card_commit"] == "b" * 40
    assert accepted.json()["terminology_card_blob_sha"] == "d" * 40


def test_complete_returns_only_after_github_readback() -> None:
    client = TestClient(build_app())
    created = client.post(
        "/voice-intake/v1/sessions",
        json=create_payload(),
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert created.status_code == 200
    response = client.post(
        f"/voice-intake/v1/sessions/{SESSION_ID}/complete",
        json=complete_payload(),
        headers={"Authorization": f"Bearer {TOKEN}"},
    )
    assert response.status_code == 200
    assert response.json()["github_verified"] is True
    assert response.json()["github_commit_sha"] == "a" * 40


def test_one_fresh_terminology_snapshot_is_pinned_for_the_entire_session() -> None:
    service = FakeService()
    publisher = FakePublisher()
    app = attach_voice_intake_routes(
        FastAPI(),
        settings=settings(),
        service=service,  # type: ignore[arg-type]
        publisher=publisher,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    auth = {"Authorization": f"Bearer {TOKEN}"}
    first = client.post("/voice-intake/v1/sessions", json=create_payload(), headers=auth)
    second = client.post("/voice-intake/v1/sessions", json=create_payload(), headers=auth)
    assert first.status_code == second.status_code == 200
    assert publisher.terminology_calls == 1

    audio = wav_bytes()
    upload_headers = {
        **auth,
        "Content-Type": "audio/wav",
        "X-Chunk-SHA256": hashlib.sha256(audio).hexdigest(),
        "X-Chunk-Duration-Ms": "1000",
    }
    for chunk_index in (0, 1):
        response = client.put(
            f"/voice-intake/v1/sessions/{SESSION_ID}/chunks/{chunk_index}",
            content=audio,
            headers=upload_headers,
        )
        assert response.status_code == 200
    completed = client.post(
        f"/voice-intake/v1/sessions/{SESSION_ID}/complete",
        json=complete_payload(),
        headers=auth,
    )
    assert completed.status_code == 200
    assert publisher.terminology_calls == 1
    assert len(set(service.transcription_terminology + service.summary_terminology)) == 1
    assert publisher.published_terminology[0].source_commit_sha == "b" * 40


def test_first_chunk_lazily_initializes_session_snapshot_for_legacy_client() -> None:
    publisher = FakePublisher()
    app = attach_voice_intake_routes(
        FastAPI(),
        settings=settings(),
        service=FakeService(),  # type: ignore[arg-type]
        publisher=publisher,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    audio = wav_bytes()
    response = client.put(
        f"/voice-intake/v1/sessions/{SESSION_ID}/chunks/0",
        content=audio,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "audio/wav",
            "X-Chunk-SHA256": hashlib.sha256(audio).hexdigest(),
            "X-Chunk-Duration-Ms": "1000",
        },
    )
    assert response.status_code == 200
    assert response.json()["transcript"]["transcript"] == "Тестовая расшифровка"
    assert publisher.terminology_calls == 1


def test_first_chunk_fails_closed_when_current_terminology_cannot_be_loaded() -> None:
    class FailingPublisher(FakePublisher):
        async def resolve_terminology(self) -> object:
            self.terminology_calls += 1
            raise VoiceIntakeError(
                "github_network_error", retryable=True, status_code=503
            )

    publisher = FailingPublisher()
    app = attach_voice_intake_routes(
        FastAPI(),
        settings=settings(),
        service=FakeService(),  # type: ignore[arg-type]
        publisher=publisher,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    audio = wav_bytes()
    response = client.put(
        f"/voice-intake/v1/sessions/{SESSION_ID}/chunks/0",
        content=audio,
        headers={
            "Authorization": f"Bearer {TOKEN}",
            "Content-Type": "audio/wav",
            "X-Chunk-SHA256": hashlib.sha256(audio).hexdigest(),
            "X-Chunk-Duration-Ms": "1000",
        },
    )
    assert response.status_code == 503
    assert response.json()["detail"]["code"] == "github_network_error"
    assert publisher.terminology_calls == 1


def test_new_session_never_falls_back_to_previous_snapshot_on_github_error() -> None:
    class FailingSecondPublisher(FakePublisher):
        async def resolve_terminology(self) -> object:
            if self.terminology_calls:
                self.terminology_calls += 1
                raise VoiceIntakeError(
                    "github_network_error", retryable=True, status_code=503
                )
            return await super().resolve_terminology()

    publisher = FailingSecondPublisher()
    app = attach_voice_intake_routes(
        FastAPI(),
        settings=settings(),
        service=FakeService(),  # type: ignore[arg-type]
        publisher=publisher,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    auth = {"Authorization": f"Bearer {TOKEN}"}
    assert client.post(
        "/voice-intake/v1/sessions", json=create_payload(), headers=auth
    ).status_code == 200
    another = client.post(
        "/voice-intake/v1/sessions",
        json=create_payload("voice-20260828-123457-abcdef13"),
        headers=auth,
    )
    assert another.status_code == 503
    assert another.json()["detail"]["code"] == "github_network_error"


def test_each_new_session_resolves_fresh_commit_not_schema_version() -> None:
    class ChangingPublisher(FakePublisher):
        async def resolve_terminology(self) -> object:
            self.terminology_calls += 1
            marker = "a" if self.terminology_calls == 1 else "b"
            return parse_terminology_card(
                f"""
schema_version: 1.0.0
card_id: idea-hub-voice-terminology
rules: [Use the current snapshot.]
entries:
  - canonical: card-{marker}
    kind: test
""",
                source_path="config/voice-terminology.yaml",
                source_commit_sha=marker * 40,
                source_blob_sha=marker * 40,
            )

    publisher = ChangingPublisher()
    app = attach_voice_intake_routes(
        FastAPI(),
        settings=settings(),
        service=FakeService(),  # type: ignore[arg-type]
        publisher=publisher,  # type: ignore[arg-type]
    )
    client = TestClient(app)
    auth = {"Authorization": f"Bearer {TOKEN}"}
    first = client.post("/voice-intake/v1/sessions", json=create_payload(), headers=auth)
    second = client.post(
        "/voice-intake/v1/sessions",
        json=create_payload("voice-20260828-123457-abcdef13"),
        headers=auth,
    )
    assert first.status_code == second.status_code == 200
    assert first.json()["terminology_card_version"] == "1.0.0"
    assert second.json()["terminology_card_version"] == "1.0.0"
    assert first.json()["terminology_card_commit"] == "a" * 40
    assert second.json()["terminology_card_commit"] == "b" * 40
    assert publisher.terminology_calls == 2


def test_github_current_resolver_has_no_cache_or_stale_fallback() -> None:
    card = """
schema_version: 1.0.0
card_id: idea-hub-voice-terminology
rules: [Use canonical terms only when supported.]
entries:
  - canonical: IdeaHub
    kind: product
"""

    class DirectPublisher(IdeaHubPublisher):
        def __init__(self) -> None:
            self.calls = 0
            self.fail = False

        async def _head(self) -> tuple[str, str]:
            self.calls += 1
            if self.fail:
                raise VoiceIntakeError("github_network_error", retryable=True, status_code=503)
            marker = "a" if self.calls == 1 else "b"
            return marker * 40, "f" * 40

        async def _content(self, _path: str, ref: str) -> tuple[str, str]:
            return card, ref

    publisher = DirectPublisher()
    first = asyncio.run(publisher.resolve_terminology())
    second = asyncio.run(publisher.resolve_terminology())
    assert first.source_commit_sha == "a" * 40
    assert second.source_commit_sha == "b" * 40
    publisher.fail = True
    with pytest.raises(VoiceIntakeError) as caught:
        asyncio.run(publisher.resolve_terminology())
    assert caught.value.code == "github_network_error"


def test_session_snapshot_survives_restart_and_same_id_cannot_repin(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "voice-terminology-snapshots.json"
    runtime = settings(terminology_state_path=str(state_path))
    first_publisher = FakePublisher()
    first_service = FakeService()
    first_app = attach_voice_intake_routes(
        FastAPI(),
        settings=runtime,
        service=first_service,  # type: ignore[arg-type]
        publisher=first_publisher,  # type: ignore[arg-type]
    )
    auth = {"Authorization": f"Bearer {TOKEN}"}
    first_client = TestClient(first_app)
    created = first_client.post(
        "/voice-intake/v1/sessions", json=create_payload(), headers=auth
    )
    assert created.status_code == 200
    assert created.json()["terminology_card_commit"] == "b" * 40
    assert state_path.exists()
    assert stat.S_IMODE(state_path.stat().st_mode) == 0o600

    class NewMainPublisher(FakePublisher):
        async def resolve_terminology(self) -> object:
            self.terminology_calls += 1
            return parse_terminology_card(
                """
schema_version: 1.0.0
card_id: idea-hub-voice-terminology
rules: [Use the new main card.]
entries:
  - canonical: new-main
    kind: test
""",
                source_path="config/voice-terminology.yaml",
                source_commit_sha="e" * 40,
                source_blob_sha="f" * 40,
            )

    second_publisher = NewMainPublisher()
    second_service = FakeService()
    restarted_app = attach_voice_intake_routes(
        FastAPI(),
        settings=runtime,
        service=second_service,  # type: ignore[arg-type]
        publisher=second_publisher,  # type: ignore[arg-type]
    )
    restarted_client = TestClient(restarted_app)
    reopened = restarted_client.post(
        "/voice-intake/v1/sessions", json=create_payload(), headers=auth
    )
    assert reopened.status_code == 200
    assert reopened.json()["terminology_card_commit"] == "b" * 40
    assert second_publisher.terminology_calls == 0

    completed = restarted_client.post(
        f"/voice-intake/v1/sessions/{SESSION_ID}/complete",
        json=complete_payload(),
        headers=auth,
    )
    assert completed.status_code == 200
    assert second_publisher.published_terminology[0].source_commit_sha == "b" * 40
    assert "KenigEvents" in second_service.summary_terminology[0]
    persisted = json.loads(state_path.read_text(encoding="utf-8"))
    assert persisted["sessions"] == {}


def test_concurrent_duplicate_session_start_resolves_once() -> None:
    async def scenario() -> tuple[int, object, object]:
        store = SessionTerminologySnapshots()
        calls = 0

        async def resolver() -> object:
            nonlocal calls
            calls += 1
            await asyncio.sleep(0.01)
            return parse_terminology_card(
                """
schema_version: 1.0.0
card_id: idea-hub-voice-terminology
rules: [Use the current snapshot.]
entries:
  - canonical: IdeaHub
    kind: product
""",
                source_path="config/voice-terminology.yaml",
                source_commit_sha="a" * 40,
                source_blob_sha="b" * 40,
            )

        first, second = await asyncio.gather(
            store.begin(SESSION_ID, resolver),  # type: ignore[arg-type]
            store.begin(SESSION_ID, resolver),  # type: ignore[arg-type]
        )
        return calls, first, second

    calls, first, second = asyncio.run(scenario())
    assert calls == 1
    assert first is second


def test_markdown_and_registry_are_idempotent() -> None:
    request = SessionCompleteRequest.model_validate(complete_payload())
    summary = SummaryPayload(
        title="Тестовая идея",
        short_summary="Кратко",
        detailed_summary="Подробно",
    )
    source_path, detail_path = paths_for(SESSION_ID)
    source = render_source_packet(
        session_id=SESSION_ID,
        request=request,
        summary=summary,
        model="gemini-3.1-flash-lite",
        registered_at="2026-08-28T10:00:00Z",
        terminology_version="1.0.0",
        terminology_path="config/voice-terminology.yaml",
        terminology_commit_sha="c" * 40,
        terminology_blob_sha="d" * 40,
        terminology_status="current",
    )
    detail = render_session_detail(
        session_id=SESSION_ID,
        request=request,
        summary=summary,
        source_path=source_path,
        registered_at="2026-08-28T10:00:00Z",
    )
    assert f"packet_id: {SESSION_ID}" in source
    assert "transcription_prompt_version: voice-transcribe-v2" in source
    assert "terminology_card_version: 1.0.0" in source
    assert "terminology_card_commit: " + "c" * 40 in source
    assert "terminology_card_blob_sha: " + "d" * 40 in source
    assert "terminology_card_status: current" in source
    assert f"session_id: {SESSION_ID}" in detail
    entry = build_registry_entry(
        session_id=SESSION_ID,
        request=request,
        summary=summary,
        source_path=source_path,
        detail_path=detail_path,
        registered_at="2026-08-28T10:00:00Z",
    )
    registry = (
        "schema_version: 1.0.0\n"
        "registry_id: test\n"
        "updated_at: '2026-01-01T00:00:00Z'\n"
        "sessions: []\n"
    )
    updated = insert_registry_entry(
        registry,
        entry=entry,
        updated_at="2026-08-28T10:00:00Z",
    )
    assert updated.count(f"session_id: {SESSION_ID}") == 1
    assert (
        insert_registry_entry(
            updated,
            entry=entry,
            updated_at="2026-08-28T10:00:00Z",
        )
        == updated
    )


def test_terminology_card_is_bounded_and_rendered_for_prompts() -> None:
    context = parse_terminology_card(
        """
schema_version: 1.0.0
card_id: idea-hub-voice-terminology
rules:
  - Correct only when audio and context support the canonical term.
entries:
  - canonical: KenigEvents / «Полюбить Калининград Анонсы»
    kind: product
    aliases: [KenigEvents]
    common_misrecognitions: [tionicavents.ru]
    notes: Canonical domain is kenigevents.ru.
  - canonical: Penpot
    kind: design_tool
    common_misrecognitions: [Pinpod, Пинпод]
""",
        source_path="config/voice-terminology.yaml",
        source_commit_sha="c" * 40,
        source_blob_sha="d" * 40,
    )
    assert context.schema_version == "1.0.0"
    assert "KenigEvents / «Полюбить Калининград Анонсы»" in context.prompt
    assert "tionicavents.ru" in context.prompt
    assert "Penpot" in context.prompt
    assert context.source_commit_sha == "c" * 40
    assert context.source_blob_sha == "d" * 40
    assert context.status == "current"


def test_voice_index_lists_all_main_records_newest_first() -> None:
    registry = """
schema_version: 1.0.0
registry_id: idea-hub-intake-sessions
updated_at: '2026-08-28T10:00:00Z'
sessions:
- session_id: voice-20260828-100000-aaaaaaaa
  title: Первая запись
  registered_at: '2026-08-28T08:00:00Z'
  source: {platform: record-idea-hub-android}
  routes:
    destinations:
    - role: source_packet
      path: inbox/voice/2026/08/voice-20260828-100000-aaaaaaaa.md
  status: {overall: open}
- session_id: voice-20260828-110000-bbbbbbbb
  title: Новая запись
  registered_at: '2026-08-28T09:00:00Z'
  source: {platform: record-idea-hub-android}
  routes:
    destinations:
    - role: source_packet
      path: inbox/voice/2026/08/voice-20260828-110000-bbbbbbbb.md
  status: {overall: open}
"""
    index = render_voice_index(registry)
    assert index.index("Новая запись") < index.index("Первая запись")
    assert "2026/08/voice-20260828-110000-bbbbbbbb.md" in index
    assert "ветки `main`" in index


def test_github_navigation_url_is_stable_main_not_commit_snapshot() -> None:
    publisher = IdeaHubPublisher(
        settings(),
        requester=FakeRequester(
            BoundedHTTPResponse(
                status=200,
                json_body={},
                retry_after=None,
                content_type="application/json",
            )
        ),
    )
    url = publisher._branch_url("inbox/voice/2026/08/example.md")
    assert url.endswith("/blob/main/inbox/voice/2026/08/example.md")
    assert "/blob/" + "a" * 40 not in url


class FakeLimiter:
    def __init__(self) -> None:
        self.events: list[tuple[str, object]] = []

    async def preflight(self, model: str) -> LimiterPreflight:
        self.events.append(("preflight", model))
        return LimiterPreflight(
            limit=ModelLimit(
                model=model,
                rpm=13,
                tpm=240000,
                rpd=450,
                tpm_reserve_extra=1000,
            ),
            candidate_key_ids=("key-id",),
            candidate_env_names=frozenset({"GOOGLE_API_KEY"}),
            contract="google_ai_project_model_atomic_v1",
            bucket_strategy="rolling_60s_pacific_day_v2",
        )

    async def reserve_generate_content(self, **kwargs: Any) -> LimiterLease:
        reserved_tpm = int(kwargs["reserved_tpm"])
        self.events.append(("reserve", reserved_tpm))
        return LimiterLease(
            request_uid=kwargs["request_uid"],
            attempt_no=1,
            api_key_id="key-id",
            env_var_name="GOOGLE_API_KEY",
            key_alias="primary",
            quota_scope="google:project",
            reserved_tpm=reserved_tpm,
            contract="google_ai_project_model_atomic_v1",
            bucket_strategy="rolling_60s_pacific_day_v2",
        )

    def secret_for(self, _lease: LimiterLease) -> str:
        return "secret"

    async def mark_sent(self, lease: LimiterLease) -> None:
        self.events.append(("sent", lease.request_uid))

    async def release_unsent(self, _lease: LimiterLease, reason: str) -> None:
        self.events.append(("release", reason))

    async def report_provider_429(
        self, _lease: LimiterLease, retry_after_ms: int | None
    ) -> None:
        self.events.append(("429", retry_after_ms))

    async def finalize_generate_content(self, _lease: LimiterLease, **kwargs: Any) -> None:
        self.events.append(("finalize", kwargs["provider_status"]))

    @staticmethod
    def public_lease(_lease: LimiterLease, *, actual_tpm: int) -> dict[str, Any]:
        return {"actual_tpm": actual_tpm}


class FakeRequester:
    def __init__(self, response: BoundedHTTPResponse) -> None:
        self.response = response
        self.calls = 0
        self.last_json_body: dict[str, Any] | None = None

    async def request_json(self, *_args: Any, **kwargs: Any) -> BoundedHTTPResponse:
        self.calls += 1
        body = kwargs.get("json_body")
        self.last_json_body = dict(body) if isinstance(body, dict) else None
        return self.response


def test_gemini_success_has_one_accounted_provider_send() -> None:
    limiter = FakeLimiter()
    response = BoundedHTTPResponse(
        status=200,
        json_body={
            "candidates": [
                {
                    "finishReason": "STOP",
                    "content": {
                        "parts": [
                            {
                                "text": json.dumps(
                                    {
                                        "transcript": "Привет",
                                        "language": "ru-RU",
                                        "uncertain_fragments": [],
                                    },
                                    ensure_ascii=False,
                                )
                            }
                        ]
                    },
                }
            ],
            "usageMetadata": {
                "promptTokenCount": 20,
                "candidatesTokenCount": 4,
                "totalTokenCount": 24,
            },
        },
        retry_after=None,
        content_type="application/json",
    )
    requester = FakeRequester(response)
    service = GeminiVoiceService(
        settings(),
        limiter=limiter,  # type: ignore[arg-type]
        requester=requester,  # type: ignore[arg-type]
    )
    result = asyncio.run(
        service.transcribe(
            session_id=SESSION_ID,
            chunk_index=0,
            duration_ms=1000,
            audio=wav_bytes(),
            terminology="- KenigEvents [product]; частая ошибка: tionicavents",
        )
    )
    assert result.transcript.transcript == "Привет"
    assert requester.calls == 1
    assert [event[0] for event in limiter.events] == [
        "preflight",
        "reserve",
        "sent",
        "finalize",
    ]
    reserved_tpm = int(limiter.events[1][1])
    assert 8_192 < reserved_tpm < 20_000
    assert reserved_tpm < 240_000
    assert requester.last_json_body is not None
    generation = requester.last_json_body["generationConfig"]
    assert "temperature" not in generation
    assert generation["responseMimeType"] == "application/json"
    prompt = requester.last_json_body["contents"][0]["parts"][0]["text"]
    assert "АВТОРИТЕТНАЯ КАРТОЧКА ТЕРМИНОЛОГИИ" in prompt
    assert "KenigEvents" in prompt
    assert result.prompt_version == "voice-transcribe-v2"
    assert limiter.events[-1] == ("finalize", "succeeded")


def test_oversized_summary_is_rejected_before_reservation() -> None:
    preflight = LimiterPreflight(
        limit=ModelLimit(
            model="gemini-3.1-flash-lite",
            rpm=13,
            tpm=240000,
            rpd=450,
            tpm_reserve_extra=1000,
        ),
        candidate_key_ids=("key-id",),
        candidate_env_names=frozenset({"GOOGLE_API_KEY"}),
        contract="google_ai_project_model_atomic_v1",
        bucket_strategy="rolling_60s_pacific_day_v2",
    )
    with pytest.raises(VoiceIntakeError) as caught:
        GeminiVoiceService._reservation_tpm(
            preflight=preflight,
            prompt="я" * 500_000,
            duration_ms=None,
            max_output_tokens=16_384,
        )
    assert caught.value.code == "voice_request_exceeds_model_tpm"


def test_provider_429_is_reported_and_finalized() -> None:
    limiter = FakeLimiter()
    response = BoundedHTTPResponse(
        status=429,
        json_body={"error": {"details": [{"retryDelay": "12s"}]}},
        retry_after="12",
        content_type="application/json",
    )
    service = GeminiVoiceService(
        settings(),
        limiter=limiter,  # type: ignore[arg-type]
        requester=FakeRequester(response),  # type: ignore[arg-type]
    )
    with pytest.raises(VoiceIntakeError) as caught:
        asyncio.run(
            service.transcribe(
                session_id=SESSION_ID,
                chunk_index=0,
                duration_ms=1000,
                audio=wav_bytes(),
            )
        )
    assert caught.value.code == "provider_429"
    assert caught.value.retryable is True
    assert caught.value.retry_after_seconds == 12
    assert ("429", 12000) in limiter.events
    assert ("finalize", "failed") in limiter.events
