from __future__ import annotations

import os
import shutil
from pathlib import Path

import pytest

from my_data_hub.voice_intake_v2.store import ChunkReceipt, StoreError, VoiceIntakeV2Store

from .conftest import SESSION_ID, SHA


def receipt(path: Path) -> ChunkReceipt:
    return ChunkReceipt(
        session_id=SESSION_ID, chunk_index=0, sha256=SHA, duration_ms=240000,
        audio_start_ms=0, audio_end_ms=240000, wall_start_ms=0, wall_end_ms=240000,
        size_bytes=100, path=str(path),
    )


def test_create_repeat_restart_and_conflict(tmp_path, create_request, terminology):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    status, duplicate = store.create_session(create_request, terminology=terminology)
    assert status.state == "receiving" and not duplicate
    assert os.stat(store.root).st_mode & 0o777 == 0o700
    assert os.stat(store.db_path).st_mode & 0o777 == 0o600

    restarted = VoiceIntakeV2Store(tmp_path / "spool")
    status, duplicate = restarted.create_session(create_request, terminology={"different": True})
    assert status.state == "receiving" and duplicate
    changed = create_request.model_copy(update={"device_label": "changed"})
    with pytest.raises(StoreError, match="session_metadata_conflict"):
        restarted.create_session(changed, terminology=terminology)


def test_chunk_and_complete_are_durable_idempotent_and_conflict(
    tmp_path, create_request, complete_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"audio")
    os.chmod(path, 0o600)
    first, status = store.record_chunk(receipt(path))
    assert not first.duplicate and status.chunks_received == 1 and status.bytes_received == 100
    repeated, _ = store.record_chunk(receipt(path))
    assert repeated.duplicate
    with pytest.raises(StoreError, match="chunk_conflict"):
        store.record_chunk(ChunkReceipt(**{
            "session_id": SESSION_ID, "chunk_index": 0, "sha256": "d" * 64,
            "duration_ms": 240000, "audio_start_ms": 0, "audio_end_ms": 240000,
            "wall_start_ms": 0, "wall_end_ms": 240000, "size_bytes": 100, "path": str(path),
        }))
    status, duplicate = store.complete(SESSION_ID, complete_request)
    assert status.state == "queued" and status.recording_finished and not duplicate
    status, duplicate = VoiceIntakeV2Store(store.root).complete(SESSION_ID, complete_request)
    assert duplicate and status.state == "queued"
    repeated, repeated_status = VoiceIntakeV2Store(store.root).record_chunk(receipt(path))
    assert repeated.duplicate and repeated_status.state == "queued"
    changed = complete_request.model_copy(update={"wall_elapsed_ms": 240001})
    with pytest.raises(StoreError, match="complete_manifest_conflict"):
        store.complete(SESSION_ID, changed)


def test_upload_before_create_and_missing_manifest(tmp_path, create_request, complete_request, terminology):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    with pytest.raises(StoreError, match="session_not_created"):
        store.record_chunk(receipt(tmp_path / "missing.m4a"))
    store.create_session(create_request, terminology=terminology)
    with pytest.raises(StoreError, match="chunks_missing"):
        store.complete(SESSION_ID, complete_request)


def test_single_lease_restart_recovery_and_ttl_preserves_unverified_audio(
    tmp_path, create_request, complete_request, terminology
):
    now = [1_000_000.0]
    store = VoiceIntakeV2Store(tmp_path / "spool", clock=lambda: now[0])
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"audio")
    store.record_chunk(receipt(path))
    store.complete(SESSION_ID, complete_request)
    assert store.claim("worker-1", 30) is not None
    assert store.claim("worker-2", 30) is None
    store.set_state(SESSION_ID, "worker-1", "transcribing")
    now[0] += 31
    assert store.fence_ambiguous_inference() == 1
    assert store.status(SESSION_ID).state == "reconciliation_required"
    now[0] += 7 * 24 * 3600 + 1
    assert store.reap_expired(7 * 24 * 3600) == 0
    assert store.session_directory(SESSION_ID).is_dir()
    assert path.is_file()
    assert store.status(SESSION_ID).state == "reconciliation_required"


def test_ttl_preserves_old_receiving_session_and_audio(tmp_path, create_request, terminology):
    now = [1_000_000.0]
    store = VoiceIntakeV2Store(tmp_path / "spool", clock=lambda: now[0])
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / "pending.m4a"
    path.write_bytes(b"recoverable")
    now[0] += 30 * 24 * 3600
    assert store.reap_expired(7 * 24 * 3600) == 0
    assert store.status(SESSION_ID).state == "receiving"
    assert path.read_bytes() == b"recoverable"


def test_purge_audio_fails_closed_when_recursive_delete_does_not_remove_directory(
    tmp_path, create_request, terminology, monkeypatch
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    chunks = store.session_directory(SESSION_ID) / "chunks"
    (chunks / "audio.m4a").write_bytes(b"audio")
    monkeypatch.setattr(shutil, "rmtree", lambda _path: None)
    with pytest.raises(StoreError, match="server_audio_purge_failed"):
        store.purge_audio(SESSION_ID)
    assert chunks.is_dir()
    assert not store.status(SESSION_ID).server_audio_purged


def test_claim_after_waiting_quota_clears_stale_error_fields(
    tmp_path, create_request, complete_request, terminology
):
    now = [1_000_000.0]
    store = VoiceIntakeV2Store(tmp_path / "spool", clock=lambda: now[0])
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"audio")
    store.record_chunk(receipt(path))
    store.complete(SESSION_ID, complete_request)
    assert store.claim("worker-1", 30) is not None
    store.mark_error(
        SESSION_ID, "worker-1", code="quota_wait", retryable=True, retry_at=now[0] + 10
    )
    now[0] += 11
    assert store.claim("worker-2", 30) is not None
    status = store.status(SESSION_ID)
    assert not status.retryable
    assert status.retry_at is None
    assert status.error_code is None
