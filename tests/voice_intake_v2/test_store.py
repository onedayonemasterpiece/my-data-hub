from __future__ import annotations

import os
import shutil
import sqlite3
from dataclasses import asdict
from pathlib import Path

import pytest

from my_data_hub.voice_intake_v2.store import (
    ChunkReceipt,
    StoredSegmentReceipt,
    StoreError,
    VoiceIntakeV2Store,
)

from .conftest import SESSION_ID, SHA


def receipt(path: Path) -> ChunkReceipt:
    return ChunkReceipt(
        session_id=SESSION_ID, chunk_index=0, sha256=SHA, duration_ms=240000,
        audio_start_ms=0, audio_end_ms=240000, wall_start_ms=0, wall_end_ms=240000,
        size_bytes=100, path=str(path),
    )


def segment_receipt(*, accepted: bool = True, finish_reason: str = "STOP") -> StoredSegmentReceipt:
    return StoredSegmentReceipt(
        session_id=SESSION_ID, chunk_index=0, source_sha256=SHA,
        audio_start_ms=0, audio_end_ms=240000, coverage_start_ms=0,
        coverage_end_ms=240000, provider_request_uid="synthetic-request-1",
        finish_reason=finish_reason, schema_version="segment-transcript/1.0",
        accepted=accepted,
        transcript=(
            {"transcript": "synthetic fixture words", "language": "ru", "uncertain_fragments": []}
            if accepted else None
        ),
        coverage={"covered_ms": 240000, "ratio_ppm": 1_000_000},
        limiter={"input_tokens": 10, "output_tokens": 5, "total_tokens": 15},
    )


def prepare_purge_authorization(
    store: VoiceIntakeV2Store, create_request, complete_request, terminology
) -> Path:
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"synthetic-audio")
    store.record_chunk(receipt(path))
    store.complete(SESSION_ID, complete_request)
    assert store.claim("worker-1", 60) is not None
    assert not store.persist_segment_receipt(SESSION_ID, "worker-1", segment_receipt())
    aggregate = store.persist_content_verification(
        SESSION_ID, "worker-1", schema_version="content/1.0",
        verifier_version="coverage/1.0", verification={"coverage_ppm": 1_000_000},
    )
    store.persist_summary(
        SESSION_ID, "worker-1", {"title": "Fixture", "short_summary": "S", "detailed_summary": "D"},
        "synthetic-summary-request", {"input_tokens": 5, "output_tokens": 5, "total_tokens": 10},
    )
    assert aggregate["transcript"] == "synthetic fixture words"
    store.persist_github_verified(
        SESSION_ID, "worker-1", url="https://example.invalid/synthetic",
        commit_sha="b" * 40,
    )
    assert not store.authorize_purge(
        SESSION_ID, "worker-1", policy_version="verified-content-publication/1.0"
    )
    return path


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


def test_durable_segment_receipt_atomically_clears_ambiguous_inflight_state(
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
    store.set_state(SESSION_ID, "worker-1", "transcribing")
    store.persist_segment_receipt(SESSION_ID, "worker-1", segment_receipt())
    assert store.status(SESSION_ID).state == "normalizing"

    now[0] += 31
    assert store.fence_ambiguous_inference() == 0
    resumed = store.claim("worker-2", 30)
    assert resumed is not None
    assert len(store.segment_receipts(SESSION_ID, accepted_only=True)) == 1
    assert path.is_file()


def test_failed_segment_receipt_remains_fenced_until_retry_policy_is_durable(
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
    store.set_state(SESSION_ID, "worker-1", "transcribing")
    failed = segment_receipt(accepted=False, finish_reason="UNKNOWN")
    store.persist_segment_receipt(SESSION_ID, "worker-1", failed)
    assert store.status(SESSION_ID).state == "transcribing"

    now[0] += 31
    assert store.fence_ambiguous_inference() == 1
    assert store.status(SESSION_ID).state == "reconciliation_required"
    assert store.claim("worker-2", 30) is None
    assert path.is_file()


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


def test_github_readback_without_content_receipt_cannot_purge_real_chunk(
    tmp_path, create_request, complete_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"synthetic-audio")
    store.record_chunk(receipt(path))
    store.complete(SESSION_ID, complete_request)
    assert store.claim("worker-1", 60) is not None
    store.persist_github_verified(
        SESSION_ID, "worker-1", url="https://example.invalid/synthetic", commit_sha="b" * 40
    )

    with pytest.raises(StoreError, match="purge_not_authorized"):
        store.purge_audio(SESSION_ID)

    state = store.verification_state(SESSION_ID)
    assert state.publication_verified
    assert not state.content_verified and not state.purge_authorized and not state.audio_purged
    assert path.is_file()
    assert not store.status(SESSION_ID).server_audio_purged


def test_direct_boolean_forgery_cannot_bypass_durable_purge_receipts(
    tmp_path, create_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / "synthetic.m4a"
    path.write_bytes(b"synthetic-audio")
    with (
        pytest.raises(sqlite3.IntegrityError, match="_receipt_required"),
        store._transaction() as connection,  # intentional corruption simulation
    ):
        connection.execute(
            """UPDATE sessions SET content_verified=1,publication_verified=1,
               purge_authorized=1,github_verified=1,github_commit_sha=? WHERE session_id=?""",
            ("b" * 40, SESSION_ID),
        )
    with pytest.raises(StoreError, match="purge_not_authorized"):
        store.purge_audio(SESSION_ID)
    assert path.is_file()
    assert not store.status(SESSION_ID).server_audio_purged


def test_purge_audio_fails_closed_when_recursive_delete_does_not_remove_directory(
    tmp_path, create_request, complete_request, terminology, monkeypatch
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    path = prepare_purge_authorization(store, create_request, complete_request, terminology)
    chunks = path.parent
    monkeypatch.setattr(shutil, "rmtree", lambda _path: None)
    with pytest.raises(StoreError, match="server_audio_purge_failed"):
        store.purge_audio(SESSION_ID)
    assert chunks.is_dir()
    assert path.is_file()
    state = store.verification_state(SESSION_ID)
    assert state.content_verified and state.publication_verified and state.purge_authorized
    assert not state.audio_purged and not store.status(SESSION_ID).server_audio_purged


def test_purge_retry_finishes_after_crash_between_physical_delete_and_receipt(
    tmp_path, create_request, complete_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    path = prepare_purge_authorization(store, create_request, complete_request, terminology)
    shutil.rmtree(path.parent)
    shutil.rmtree(store.session_directory(SESSION_ID) / "normalized")

    store.purge_audio(SESSION_ID)
    store.finish_purge(SESSION_ID, "worker-1")

    state = store.verification_state(SESSION_ID)
    assert state.audio_purged and store.status(SESSION_ID).server_audio_purged
    assert store.status(SESSION_ID).state == "published_verified"
    assert not path.exists()


def test_segment_receipts_are_idempotent_immutable_and_failed_attempt_has_no_content(
    tmp_path, create_request, complete_request, terminology
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"synthetic-audio")
    store.record_chunk(receipt(path))
    store.complete(SESSION_ID, complete_request)
    store.claim("worker-1", 60)
    failed = segment_receipt(accepted=False, finish_reason="MAX_TOKENS")
    assert not store.persist_segment_receipt(SESSION_ID, "worker-1", failed)
    assert store.persist_segment_receipt(SESSION_ID, "worker-1", failed)
    assert store.segment_receipts(SESSION_ID)[0].transcript is None
    with pytest.raises(StoreError, match="segment_receipt_conflict"):
        store.persist_segment_receipt(
            SESSION_ID, "worker-1",
            StoredSegmentReceipt(**{**asdict(failed), "finish_reason": "UNKNOWN"}),
        )
    accepted = StoredSegmentReceipt(
        **{**asdict(segment_receipt()), "provider_request_uid": "synthetic-request-2"}
    )
    assert not store.persist_segment_receipt(SESSION_ID, "worker-1", accepted)
    assert [item.accepted for item in store.segment_receipts(SESSION_ID)] == [False, True]
    with (
        pytest.raises(sqlite3.IntegrityError, match="immutable_receipt"),
        store._transaction() as connection,
    ):
        connection.execute(
            """UPDATE segment_inference_receipts SET finish_reason='UNKNOWN'
               WHERE session_id=?""",
            (SESSION_ID,),
        )


@pytest.mark.parametrize(
    ("accepted", "finish_reason", "coverage_end_ms"),
    [(False, "MAX_TOKENS", 240000), (True, "STOP", 239999)],
)
def test_failed_or_partial_segment_cannot_verify_content_or_delete_audio(
    tmp_path, create_request, complete_request, terminology,
    accepted, finish_reason, coverage_end_ms,
):
    store = VoiceIntakeV2Store(tmp_path / "spool")
    store.create_session(create_request, terminology=terminology)
    path = store.session_directory(SESSION_ID) / "chunks" / f"00000-{SHA}.m4a"
    path.write_bytes(b"synthetic-audio")
    store.record_chunk(receipt(path))
    store.complete(SESSION_ID, complete_request)
    store.claim("worker-1", 60)
    candidate = StoredSegmentReceipt(
        **{
            **asdict(segment_receipt(accepted=accepted, finish_reason=finish_reason)),
            "coverage_end_ms": coverage_end_ms,
        }
    )
    store.persist_segment_receipt(SESSION_ID, "worker-1", candidate)
    with pytest.raises(StoreError, match="content_coverage_incomplete"):
        store.persist_content_verification(
            SESSION_ID, "worker-1", schema_version="content/1.0",
            verifier_version="coverage/1.0", verification={"coverage_ppm": 999_999},
        )
    with pytest.raises(StoreError, match="content_not_verified"):
        store.persist_summary(
            SESSION_ID, "worker-1", {"title": "Fixture"}, "summary-request", {},
        )
    with pytest.raises(StoreError, match="purge_not_authorized"):
        store.purge_audio(SESSION_ID)
    assert path.is_file()
    assert not store.verification_state(SESSION_ID).content_verified


def test_legacy_migration_twice_is_idempotent_truthful_and_bounded(tmp_path):
    root = tmp_path / "legacy"
    root.mkdir()
    db_path = root / "voice-intake-v2.sqlite3"
    with sqlite3.connect(db_path) as connection:
        connection.executescript(
            """
            CREATE TABLE sessions (
                session_id TEXT PRIMARY KEY, create_json TEXT NOT NULL, create_sha256 TEXT NOT NULL,
                terminology_json TEXT NOT NULL, model TEXT NOT NULL, state TEXT NOT NULL,
                complete_json TEXT, complete_sha256 TEXT, transcript_json TEXT,
                transcript_request_uid TEXT, transcript_limiter_json TEXT, summary_json TEXT,
                summary_request_uid TEXT, summary_limiter_json TEXT, github_url TEXT,
                github_commit_sha TEXT, github_verified INTEGER NOT NULL DEFAULT 0,
                server_audio_purged INTEGER NOT NULL DEFAULT 0, retryable INTEGER NOT NULL DEFAULT 0,
                retry_at REAL, error_code TEXT, reconciliation_required INTEGER NOT NULL DEFAULT 0,
                lease_owner TEXT, lease_until REAL, created_at REAL NOT NULL, updated_at REAL NOT NULL
            );
            CREATE TABLE chunks (
                session_id TEXT NOT NULL REFERENCES sessions(session_id) ON DELETE CASCADE,
                chunk_index INTEGER NOT NULL, sha256 TEXT NOT NULL, duration_ms INTEGER NOT NULL,
                audio_start_ms INTEGER NOT NULL, audio_end_ms INTEGER NOT NULL,
                wall_start_ms INTEGER NOT NULL, wall_end_ms INTEGER NOT NULL,
                size_bytes INTEGER NOT NULL, path TEXT NOT NULL, created_at REAL NOT NULL,
                PRIMARY KEY(session_id, chunk_index)
            );
            """
        )
        rows = []
        for index in range(1_005):
            complete_json = None
            transcript_json = None
            if index == 7:
                complete_json = '{"recorded_audio_ms":1200000}'
                transcript_json = '{"transcript":"short fixture"}'
            elif index == 8:
                complete_json = '{"recorded_audio_ms":1200000}'
                transcript_json = '{"transcript":"' + ("fixture " * 300) + '"}'
            rows.append((
                f"voice-20260828-{index:06d}-{index:08x}", "{}", "a" * 64, "{}", "model",
                "published_verified",
                complete_json, transcript_json, int(index % 2 == 0), int(index % 3 == 0),
                1.0, 1.0,
            ))
        connection.executemany(
            """INSERT INTO sessions(
               session_id,create_json,create_sha256,terminology_json,model,state,
               complete_json,transcript_json,github_verified,server_audio_purged,
               created_at,updated_at
            ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows,
        )

    first = VoiceIntakeV2Store(root)
    second = VoiceIntakeV2Store(root)
    audit = second.legacy_migration_audit()
    assert audit is not None
    assert audit.rows_examined == 1_000 and audit.rows_truncated
    assert set(asdict(audit)) == {
        "migration_version", "rows_examined", "rows_truncated",
        "publication_verified_observed", "audio_purged_observed",
        "legacy_unverified_purge_observed",
        "long_transcript_rows_observed", "suspicious_long_transcript_rows_observed",
        "finish_coverage_evidence_rows_observed",
        "transcript_without_finish_coverage_evidence_rows_observed",
    }
    assert audit.long_transcript_rows_observed == 2
    assert audit.suspicious_long_transcript_rows_observed == 1
    assert audit.finish_coverage_evidence_rows_observed == 0
    assert audit.transcript_without_finish_coverage_evidence_rows_observed == 2
    assert not any("session_id" in key or "content" in key for key in asdict(audit))
    github_only = first.verification_state("voice-20260828-000002-00000002")
    assert github_only.publication_verified and not github_only.content_verified
    assert not github_only.purge_authorized
    legacy_session_id = "voice-20260828-000000-00000000"
    legacy_purged = first.verification_state(legacy_session_id)
    assert legacy_purged.audio_purged and legacy_purged.legacy_unverified_purge
    assert not legacy_purged.content_verified and not legacy_purged.purge_authorized
    legacy_status = first.status(legacy_session_id)
    assert legacy_status.server_audio_purged and legacy_status.audio_purged
    assert legacy_status.legacy_unverified_purge
    assert not legacy_status.client_audio_purge_allowed
    with sqlite3.connect(db_path) as connection:
        assert connection.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert connection.execute(
            "SELECT COUNT(*) FROM voice_v2_schema_migrations WHERE version=?",
            (VoiceIntakeV2Store._CONTENT_MIGRATION,),
        ).fetchone()[0] == 1


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
