from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

from my_data_hub.voice_intake_v2 import runtime
from my_data_hub.voice_intake_v2.runtime import attach_configured_voice_intake_v2
from my_data_hub.voice_intake_v2.settings import VoiceIntakeV2Settings


def test_disabled_runtime_does_not_create_spool(tmp_path, auth_settings) -> None:
    spool = tmp_path / "must-not-exist"
    settings = VoiceIntakeV2Settings(
        enabled=False,
        spool_root=spool,
        max_chunk_bytes=1024 * 1024,
        max_json_bytes=1024 * 1024,
        max_session_seconds=3600,
        active_ttl_seconds=7 * 24 * 3600,
        lease_seconds=60,
        worker_poll_seconds=0.1,
        ffprobe_timeout_seconds=5,
        ffmpeg_timeout_seconds=30,
        duration_tolerance_ms=2000,
    )
    app = attach_configured_voice_intake_v2(
        FastAPI(),
        auth_settings=auth_settings,
        settings=settings,
    )
    assert app.state.voice_intake_v2_store is None
    assert app.state.voice_intake_v2_worker is None
    assert not spool.exists()


def test_spool_initialization_failure_keeps_existing_v1_application_available(
    tmp_path, auth_settings, monkeypatch
) -> None:
    settings = VoiceIntakeV2Settings(
        enabled=True,
        spool_root=tmp_path / "unavailable",
        max_chunk_bytes=1024 * 1024,
        max_json_bytes=1024 * 1024,
        max_session_seconds=3600,
        active_ttl_seconds=7 * 24 * 3600,
        lease_seconds=60,
        worker_poll_seconds=0.1,
        ffprobe_timeout_seconds=5,
        ffmpeg_timeout_seconds=30,
        duration_tolerance_ms=2000,
    )

    def fail_store(_root):
        raise OSError("private spool unavailable")

    monkeypatch.setattr(runtime, "VoiceIntakeV2Store", fail_store)
    base = FastAPI()

    @base.get("/v1-proof")
    async def v1_proof():
        return {"status": "ready"}

    app = attach_configured_voice_intake_v2(
        base, auth_settings=auth_settings, settings=settings
    )
    client = TestClient(app)
    assert client.get("/v1-proof").status_code == 200
    response = client.get(
        "/voice-intake/v2/capabilities",
        headers={"Authorization": f"Bearer {'x' * 32}"},
    )
    assert response.status_code == 503
    assert response.json()["api_version"] == "2.0"
    assert response.json()["detail"]["code"] == "spool_unavailable"
