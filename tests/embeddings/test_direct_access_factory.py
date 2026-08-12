from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from my_data_hub.embeddings.direct_access_factory import (
    EmbeddingDirectAccessUnavailable,
    ExistingEpochEmbeddingAccessFactory,
    TaskBoundEmbeddingCredential,
    WorkerReachableTunnel,
)
from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata

NOW = datetime(2026, 8, 12, tzinfo=UTC)
TASK = UUID("22222222-2222-4222-8222-222222222222")
CREDENTIAL = UUID("33333333-3333-4333-8333-333333333333")


def metadata() -> EmbeddingLaunchMetadata:
    return EmbeddingLaunchMetadata(
        schema_version="embedding-central-launch-metadata.v1",
        request_id=UUID("11111111-1111-4111-8111-111111111111"), request_sha256="a" * 64,
        task_run_id=TASK, model_exact_id="model@exact", input_jobs_sha256="b" * 64,
        job_count=1, worker_source_sha256="c" * 64,
        worker_primary_source_sha256="c" * 64, epoch=7,
    )


class Authority:
    def __init__(self, *, epoch: int = 7) -> None:
        self.epoch = epoch
        self.revoked: list[tuple[UUID, UUID]] = []

    def issue(self, launch, task_token):  # type: ignore[no-untyped-def]
        assert launch.task_run_id == TASK and len(task_token) >= 32
        return TaskBoundEmbeddingCredential(
            master_instance_id=UUID("44444444-4444-4444-8444-444444444444"),
            epoch=self.epoch, task_run_id=TASK, credential_id=CREDENTIAL,
            role="embedding_worker",
            database_url=("postgresql://worker:opaque-secret@127.0.0.1:25432/postgres"
                          "?sslmode=verify-ca&sslrootcert=%2Fstate%2Fmaster-tls%2Fca.pem&connect_timeout=5"),
            expires_at=NOW + timedelta(minutes=4),
        )

    def revoke(self, credential_id, *, task_run_id):  # type: ignore[no-untyped-def]
        self.revoked.append((credential_id, task_run_id))


def tunnel(tmp_path) -> WorkerReachableTunnel:  # type: ignore[no-untyped-def]
    ca = tmp_path / "ca.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----\nopaque\n-----END CERTIFICATE-----\n")
    ca.chmod(0o644)
    return WorkerReachableTunnel("worker-db.example.org", 443, ca)


def test_factory_retargets_existing_jit_secret_to_worker_tls_forward(tmp_path) -> None:  # type: ignore[no-untyped-def]
    authority = Authority()
    factory = ExistingEpochEmbeddingAccessFactory(authority, tunnel(tmp_path), clock=lambda: NOW)
    access = factory(metadata(), "t" * 43)
    url = access.database_url.get_secret_value()
    assert "worker-db.example.org:443" in url and "127.0.0.1" not in url
    assert "sslrootcert=%2Fkaggle%2Fworking%2Fmdh-worker-ca.pem" in url
    assert access.credential_id == CREDENTIAL and access.epoch == 7
    assert authority.revoked == []
    factory.revoke(CREDENTIAL, task_run_id=TASK)
    assert authority.revoked == [(CREDENTIAL, TASK)]


@pytest.mark.parametrize(
    ("authority", "endpoint", "code"),
    [
        (None, None, "EMBEDDING_JIT_CREDENTIAL_AUTHORITY_UNAVAILABLE"),
        (Authority(), None, "EMBEDDING_WORKER_TLS_FORWARD_UNAVAILABLE"),
    ],
)
def test_factory_reports_exact_missing_primitive(authority, endpoint, code) -> None:  # type: ignore[no-untyped-def]
    factory = ExistingEpochEmbeddingAccessFactory(authority, endpoint, clock=lambda: NOW)
    assert not factory.ready and factory.missing_component() == code


def test_wrong_epoch_is_revoked_before_delivery(tmp_path) -> None:  # type: ignore[no-untyped-def]
    authority = Authority(epoch=6)
    factory = ExistingEpochEmbeddingAccessFactory(authority, tunnel(tmp_path), clock=lambda: NOW)
    with pytest.raises(EmbeddingDirectAccessUnavailable) as error:
        factory(metadata(), "t" * 43)
    assert error.value.code == "EMBEDDING_CREDENTIAL_BINDING_INVALID"
    assert authority.revoked == [(CREDENTIAL, TASK)]


def test_loopback_forward_is_not_worker_reachable(tmp_path) -> None:  # type: ignore[no-untyped-def]
    ca = tmp_path / "ca.pem"
    ca.write_text("-----BEGIN CERTIFICATE-----\nx\n-----END CERTIFICATE-----")
    factory = ExistingEpochEmbeddingAccessFactory(
        Authority(), WorkerReachableTunnel("127.0.0.1", 25432, ca), clock=lambda: NOW
    )
    assert factory.missing_component() == "EMBEDDING_TUNNEL_NOT_WORKER_REACHABLE"
