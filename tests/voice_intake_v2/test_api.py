from __future__ import annotations

import hashlib
from contextlib import asynccontextmanager
from dataclasses import replace
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from my_data_hub.voice_intake_v2.api import attach_voice_intake_v2_routes
from my_data_hub.voice_intake_v2.media import MediaProbe
from my_data_hub.voice_intake_v2.settings import VoiceIntakeV2Settings
from my_data_hub.voice_intake_v2.store import VoiceIntakeV2Store

from .conftest import SESSION_ID


class FakeMedia:
    def __init__(self) -> None:
        self.probes = 0

    async def probe(self, _path: Path) -> MediaProbe:
        self.probes += 1
        return MediaProbe(240000, "aac", "LC", 16000, 1)


def config(root: Path) -> VoiceIntakeV2Settings:
    return VoiceIntakeV2Settings(
        enabled=True, spool_root=root, max_chunk_bytes=1024 * 1024, max_json_bytes=1024 * 1024,
        max_session_seconds=3600, active_ttl_seconds=7 * 24 * 3600, lease_seconds=60,
        worker_poll_seconds=1, ffprobe_timeout_seconds=5, ffmpeg_timeout_seconds=30,
        duration_tolerance_ms=2000,
    )


def build(tmp_path, auth_settings, terminology, *, settings_override=None):
    media = FakeMedia()

    async def resolve():
        return terminology

    app = attach_voice_intake_v2_routes(
        FastAPI(), auth_settings=auth_settings,
        settings=settings_override or config(tmp_path / "spool"),
        store=VoiceIntakeV2Store(tmp_path / "spool"), media=media,
        terminology_resolver=resolve, require_worker=False,
    )
    return TestClient(app), media, app


def headers(sha: str) -> dict[str, str]:
    return {
        "Authorization": f"Bearer {'x' * 32}", "Content-Type": "audio/mp4",
        "X-Chunk-SHA256": sha, "X-Chunk-Duration-Ms": "240000",
        "X-Audio-Start-Ms": "0", "X-Audio-End-Ms": "240000",
        "X-Wall-Start-Ms": "0", "X-Wall-End-Ms": "240000",
    }


def test_auth_capabilities_and_upload_before_create_zero_provider(
    tmp_path, auth_settings, terminology
):
    client, media, _app = build(tmp_path, auth_settings, terminology)
    assert client.get("/voice-intake/v2/capabilities").status_code == 401
    response = client.get(
        "/voice-intake/v2/capabilities", headers={"Authorization": f"Bearer {'x' * 32}"}
    )
    assert response.status_code == 200
    assert response.json()["typical_gemini_requests"] == 2
    audio = b"independent-m4a"
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0",
        content=audio, headers=headers(hashlib.sha256(audio).hexdigest()),
    )
    assert response.status_code == 409
    assert response.json() == {
        "api_version": "2.0", "detail": {
            "code": "session_not_created", "retryable": False,
            "retry_after_seconds": None, "reconciliation_required": False,
        },
    }
    assert media.probes == 0


def test_create_upload_duplicate_complete_and_restart_status(
    tmp_path, auth_settings, terminology, create_request, complete_payload
):
    client, media, app = build(tmp_path, auth_settings, terminology)
    auth = {"Authorization": f"Bearer {'x' * 32}"}
    response = client.post("/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth)
    assert response.status_code == 200 and not response.json()["duplicate"]
    assert client.post(
        "/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth
    ).json()["duplicate"]
    audio = b"independent-m4a"
    sha = hashlib.sha256(audio).hexdigest()
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=audio, headers=headers(sha)
    )
    assert response.status_code == 200 and response.json()["accepted"]
    assert response.json()["chunks_received"] == 1
    assert client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=audio, headers=headers(sha)
    ).json()["duplicate"]
    complete_payload["chunks"][0]["sha256"] = sha
    response = client.post(
        f"/voice-intake/v2/sessions/{SESSION_ID}/complete", json=complete_payload, headers=auth
    )
    assert response.status_code == 202 and response.json()["state"] == "queued"
    assert media.probes == 2  # Every receipt validates actual bytes; still zero Gemini calls.
    restarted = VoiceIntakeV2Store(app.state.voice_intake_v2_store.root)
    assert restarted.status(SESSION_ID).state == "queued"


def test_changed_chunk_conflict_removes_unreferenced_final(
    tmp_path, auth_settings, terminology, create_request
):
    client, _media, app = build(tmp_path, auth_settings, terminology)
    auth = {"Authorization": f"Bearer {'x' * 32}"}
    client.post("/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth)
    first = b"first"
    first_sha = hashlib.sha256(first).hexdigest()
    client.put(f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=first, headers=headers(first_sha))
    changed = b"changed"
    changed_sha = hashlib.sha256(changed).hexdigest()
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=changed, headers=headers(changed_sha)
    )
    assert response.status_code == 409 and response.json()["detail"]["code"] == "chunk_conflict"
    chunk_dir = app.state.voice_intake_v2_store.session_directory(SESSION_ID) / "chunks"
    assert sorted(path.name for path in chunk_dir.iterdir()) == [f"00000-{first_sha}.m4a"]


def test_exact_retry_after_audio_purge_does_not_resurrect_server_audio(
    tmp_path, auth_settings, terminology, create_request, complete_payload
):
    client, _media, app = build(tmp_path, auth_settings, terminology)
    auth = {"Authorization": f"Bearer {'x' * 32}"}
    client.post("/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth)
    audio = b"independent-m4a"
    sha = hashlib.sha256(audio).hexdigest()
    assert client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=audio, headers=headers(sha)
    ).status_code == 200
    complete_payload["chunks"][0]["sha256"] = sha
    assert client.post(
        f"/voice-intake/v2/sessions/{SESSION_ID}/complete", json=complete_payload, headers=auth
    ).status_code == 202
    store = app.state.voice_intake_v2_store
    store.purge_audio(SESSION_ID)
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=audio, headers=headers(sha)
    )
    assert response.status_code == 200 and response.json()["duplicate"] is True
    chunks = store.session_directory(SESSION_ID) / "chunks"
    assert not tuple(chunks.glob("*.m4a"))


def test_non_integer_chunk_index_uses_v2_error_envelope(
    tmp_path, auth_settings, terminology
):
    client, _media, _app = build(tmp_path, auth_settings, terminology)
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/not-an-integer",
        content=b"x",
        headers=headers(hashlib.sha256(b"x").hexdigest()),
    )
    assert response.status_code == 422
    assert response.json()["api_version"] == "2.0"
    assert response.json()["detail"]["code"] == "chunk_metadata_invalid"


def test_cumulative_session_bytes_are_bounded_atomically(
    tmp_path, auth_settings, terminology, create_request
):
    bounded = replace(config(tmp_path / "spool"), max_session_bytes=20)
    client, _media, app = build(
        tmp_path, auth_settings, terminology, settings_override=bounded
    )
    auth = {"Authorization": f"Bearer {'x' * 32}"}
    client.post("/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth)
    first = b"a" * 15
    second = b"b" * 15
    assert client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0",
        content=first,
        headers=headers(hashlib.sha256(first).hexdigest()),
    ).status_code == 200
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/1",
        content=second,
        headers=headers(hashlib.sha256(second).hexdigest()),
    )
    assert response.status_code == 413
    assert response.json()["detail"]["code"] == "session_size_limit_exceeded"
    status = app.state.voice_intake_v2_store.status(SESSION_ID)
    assert status.chunks_received == 1 and status.bytes_received == len(first)


def test_complete_rejects_unaccounted_wall_elapsed(
    tmp_path, auth_settings, terminology, create_request, complete_payload
):
    client, _media, _app = build(tmp_path, auth_settings, terminology)
    auth = {"Authorization": f"Bearer {'x' * 32}"}
    client.post("/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth)
    audio = b"independent-m4a"
    sha = hashlib.sha256(audio).hexdigest()
    client.put(f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=audio, headers=headers(sha))
    complete_payload["chunks"][0]["sha256"] = sha
    complete_payload["wall_elapsed_ms"] += 10_000
    response = client.post(
        f"/voice-intake/v2/sessions/{SESSION_ID}/complete", json=complete_payload, headers=auth
    )
    assert response.status_code == 422
    assert response.json()["detail"]["code"] == "complete_manifest_invalid"


def test_upload_rejects_traversal_and_cleans_oversized_stream_temporary_file(
    tmp_path, auth_settings, terminology, create_request
):
    client, _media, app = build(tmp_path, auth_settings, terminology)
    auth = {"Authorization": f"Bearer {'x' * 32}"}
    response = client.put(
        "/voice-intake/v2/sessions/voice-..%2Fescape/chunks/0",
        content=b"audio",
        headers=headers(hashlib.sha256(b"audio").hexdigest()),
    )
    assert response.status_code in {404, 422}

    client.post("/voice-intake/v2/sessions", json=create_request.model_dump(mode="json"), headers=auth)
    audio = b"x" * (1024 * 1024 + 1)
    upload_headers = headers(hashlib.sha256(audio).hexdigest())
    upload_headers["Content-Length"] = "1"
    response = client.put(
        f"/voice-intake/v2/sessions/{SESSION_ID}/chunks/0", content=audio, headers=upload_headers
    )
    assert response.status_code == 413
    directory = app.state.voice_intake_v2_store.session_directory(SESSION_ID) / "chunks"
    assert list(directory.iterdir()) == []


def test_router_lifespan_composes_with_existing_application_lifespan(
    tmp_path, auth_settings, terminology
):
    events = []

    @asynccontextmanager
    async def base_lifespan(_app):
        events.append("base-start")
        yield
        events.append("base-stop")

    class Worker:
        async def start(self):
            events.append("worker-start")

        async def stop(self):
            events.append("worker-stop")

    async def resolve():
        return terminology

    app = attach_voice_intake_v2_routes(
        FastAPI(lifespan=base_lifespan), auth_settings=auth_settings,
        settings=config(tmp_path / "spool"), store=VoiceIntakeV2Store(tmp_path / "spool"),
        media=FakeMedia(), terminology_resolver=resolve, worker=Worker(),  # type: ignore[arg-type]
    )
    with TestClient(app):
        assert events == ["base-start", "worker-start"]
    assert events == ["base-start", "worker-start", "worker-stop", "base-stop"]


def test_v2_routes_are_inserted_before_control_plane_catch_all(
    tmp_path, auth_settings, terminology
):
    app = FastAPI()

    @app.get("/{data_path:path}")
    async def catch_all(_request: Request, data_path: str):
        return {"caught": data_path}

    async def resolve():
        return terminology

    attach_voice_intake_v2_routes(
        app, auth_settings=auth_settings, settings=config(tmp_path / "spool"),
        store=VoiceIntakeV2Store(tmp_path / "spool"), media=FakeMedia(),
        terminology_resolver=resolve, require_worker=False,
    )
    response = TestClient(app).get(
        "/voice-intake/v2/capabilities", headers={"Authorization": f"Bearer {'x' * 32}"}
    )
    assert response.status_code == 200 and response.json()["api_version"] == "2.0"
