from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import pytest

from my_data_hub.mcp.contracts import ExecutionLimits, SessionRequest
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.postgres_broker import (
    DirectoryEpochCredentialSource,
    EpochDatabaseCredential,
    PostgresMasterSession,
    PostgresMasterSessionBroker,
    SessionBrokerError,
)


def identity() -> AccessIdentity:
    return AccessIdentity(
        subject="reader",
        client_id="chatgpt-reader",
        scopes=frozenset({"bloggers:read"}),
        audience="mcp",
        token_id="token",
        expires_at=2**31,
        issuer="https://issuer.example",
        issued_at=1,
        resource="https://mcp.example/mcp",
    )


def request(*, role: str = "reader", epoch: int = 7) -> SessionRequest:
    return SessionRequest(
        principal=identity(),
        master_instance_id="11111111-1111-4111-8111-111111111111",
        epoch=epoch,
        role=role,
        tool="bloggers.list",
        limits=ExecutionLimits(),
    )


def credential(*, expires_at: datetime | None = None) -> EpochDatabaseCredential:
    return EpochDatabaseCredential(
        master_instance_id=request().master_instance_id,
        epoch=7,
        role="reader",
        database_url=(
            "postgresql://reader:opaque-password@postgres-master.internal:15432/postgres"
            "?hostaddr=127.0.0.1&sslmode=verify-full&sslrootcert=/run/secrets/master-ca.pem"
        ),
        expires_at=expires_at or datetime.now(UTC) + timedelta(minutes=3),
    )


def test_private_epoch_credential_round_trip_and_exact_binding(tmp_path) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path / "sessions"
    source = DirectoryEpochCredentialSource(root)
    stored = source.store(credential())
    assert stored.stat().st_mode & 0o077 == 0
    assert source.load(request()).database_url.startswith("postgresql://reader:")
    with pytest.raises(SessionBrokerError, match="absent"):
        source.load(request(epoch=8))
    with pytest.raises(SessionBrokerError, match="absent"):
        PostgresMasterSessionBroker(source).issue_session(request(role="operator"))


def test_epoch_credential_rejects_expiry_non_tls_and_non_loopback(tmp_path) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path / "sessions"
    source = DirectoryEpochCredentialSource(root)
    with pytest.raises(SessionBrokerError, match="expired"):
        source.store(credential(expires_at=datetime.now(UTC) - timedelta(seconds=1)))

    source.store(credential())
    path = next(root.glob("*.json"))
    payload = json.loads(path.read_text())
    payload["expires_at"] = (datetime.now(UTC) + timedelta(minutes=2)).isoformat()
    payload["database_url"] = "postgresql://reader:password@public.example/postgres?sslmode=disable"
    path.write_text(json.dumps(payload))
    path.chmod(0o600)
    with pytest.raises(SessionBrokerError, match="loopback"):
        source.load(request())


@pytest.mark.asyncio
async def test_broker_issues_one_exact_epoch_session(monkeypatch: pytest.MonkeyPatch, tmp_path) -> None:  # type: ignore[no-untyped-def]
    source = DirectoryEpochCredentialSource(tmp_path / "sessions")
    source.store(credential())
    session = PostgresMasterSessionBroker(source).issue_session(request())
    assert isinstance(session, PostgresMasterSession)
    monkeypatch.setattr(
        PostgresMasterSession,
        "_execute_sync",
        lambda self, arguments: {
            "items": [],
            "master_epoch": self.request.epoch,
            "canonical_revision": 0,
        },
    )
    result = await session.execute({"limit": 10})
    assert result["master_epoch"] == 7
    await session.close()
    with pytest.raises(SessionBrokerError, match="closed"):
        await session.execute({"limit": 10})
