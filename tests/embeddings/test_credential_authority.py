from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from my_data_hub.embeddings.credential_authority import (
    DirectoryEmbeddingCredentialAuthority,
    EmbeddingCredentialRegistration,
)
from my_data_hub.embeddings.direct_access_factory import EmbeddingDirectAccessUnavailable
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata

NOW = datetime(2026, 8, 12, tzinfo=UTC)
TASK = UUID("22222222-2222-4222-8222-222222222222")
CREDENTIAL = UUID("33333333-3333-4333-8333-333333333333")
TOKEN = "one-time-task-token-with-enough-entropy"


def metadata() -> EmbeddingLaunchMetadata:
    return EmbeddingLaunchMetadata(
        schema_version="embedding-central-launch-metadata.v1",
        request_id=UUID("11111111-1111-4111-8111-111111111111"), request_sha256="a" * 64,
        task_run_id=TASK, model_exact_id="model@exact", input_jobs_sha256="b" * 64,
        job_count=1, worker_source_sha256="c" * 64,
        worker_primary_source_sha256="c" * 64, epoch=7,
    )


def registration(*, credential_id: UUID = CREDENTIAL) -> EmbeddingCredentialRegistration:
    return EmbeddingCredentialRegistration(
        master_instance_id=UUID("44444444-4444-4444-8444-444444444444"),
        epoch=7, task_run_id=TASK, credential_id=credential_id, role="embedding_worker",
        database_url=("postgresql://worker:opaque-secret@127.0.0.1:25432/postgres"
                      "?sslmode=verify-ca&sslrootcert=%2Fstate%2Fmaster-tls%2Fca.pem&connect_timeout=5"),
        expires_at=NOW + timedelta(minutes=4),
        task_token_sha256=hashlib.sha256(TOKEN.encode()).hexdigest(),
    )


def test_store_issue_refresh_and_revoke_are_task_bound(tmp_path) -> None:  # type: ignore[no-untyped-def]
    authority = DirectoryEmbeddingCredentialAuthority(tmp_path / "credentials", clock=lambda: NOW)
    first = authority.store(registration())
    assert first.stat().st_mode & 0o777 == 0o600
    assert TOKEN not in first.read_text()
    issued = authority.issue(metadata(), TOKEN)
    assert issued.task_run_id == TASK and issued.credential_id == CREDENTIAL

    refreshed_id = UUID("55555555-5555-4555-8555-555555555555")
    authority.store(registration(credential_id=refreshed_id))
    assert authority.issue(metadata(), TOKEN).credential_id == refreshed_id

    authority.revoke(CREDENTIAL, task_run_id=TASK)
    pending = authority.pending_revocations()
    assert [(task, credential) for task, credential, _ in pending] == [(TASK, CREDENTIAL)]
    authority.acknowledge_revocation(pending[0][2])
    assert authority.pending_revocations() == ()


def test_wrong_task_token_never_returns_secret(tmp_path) -> None:  # type: ignore[no-untyped-def]
    authority = DirectoryEmbeddingCredentialAuthority(tmp_path / "credentials", clock=lambda: NOW)
    authority.store(registration())
    with pytest.raises(EmbeddingDirectAccessUnavailable) as error:
        authority.issue(metadata(), "wrong-token-with-enough-characters-000")
    assert error.value.code == "EMBEDDING_TASK_TOKEN_INVALID"


def test_missing_credential_creates_hash_only_master_request(tmp_path) -> None:  # type: ignore[no-untyped-def]
    authority = DirectoryEmbeddingCredentialAuthority(
        tmp_path / "credentials", clock=lambda: NOW, credential_wait_seconds=0
    )
    authority.root.mkdir(mode=0o700)
    with pytest.raises(EmbeddingDirectAccessUnavailable) as error:
        authority.issue(metadata(), TOKEN)
    assert error.value.code == "EMBEDDING_JIT_CREDENTIAL_PENDING"
    pending = authority.pending_requests()
    assert len(pending) == 1 and pending[0]["task_run_id"] == str(TASK)
    assert pending[0]["task_token_sha256"] == hashlib.sha256(TOKEN.encode()).hexdigest()
    assert TOKEN not in str(pending)


def test_expired_registration_is_rejected_before_storage(tmp_path) -> None:  # type: ignore[no-untyped-def]
    authority = DirectoryEmbeddingCredentialAuthority(tmp_path / "credentials", clock=lambda: NOW)
    value = registration()
    expired = EmbeddingCredentialRegistration(
        value.master_instance_id, value.epoch, value.task_run_id, value.credential_id,
        value.role, value.database_url, NOW - timedelta(seconds=1), value.task_token_sha256,
    )
    with pytest.raises(EmbeddingDirectAccessUnavailable) as error:
        authority.store(expired)
    assert error.value.code == "EMBEDDING_CREDENTIAL_EXPIRED"
