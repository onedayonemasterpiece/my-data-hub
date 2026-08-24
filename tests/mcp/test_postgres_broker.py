from __future__ import annotations

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from uuid import UUID

import pytest

from my_data_hub.control_plane.runtime import SessionCredential
from my_data_hub.mcp.contracts import ExecutionLimits, SessionRequest
from my_data_hub.mcp.oauth import AccessIdentity
from my_data_hub.mcp.postgres_broker import (
    DirectoryEpochCredentialSource,
    EpochDatabaseCredential,
    PostgresMasterSession,
    PostgresMasterSessionBroker,
    SessionBrokerError,
)
from my_data_hub.workloads.bloggers.discovery_postgres import BloggerImportApplyReceipt


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
            "?hostaddr=127.0.0.1&sslmode=verify-full&sslrootcert=/state/master-tls/ca.pem"
            "&connect_timeout=5"
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


def test_production_registrar_accepts_control_plane_session_credential(tmp_path) -> None:
    """The runtime endpoint and the on-disk registrar share one validation contract."""

    root = tmp_path / "sessions"
    source = DirectoryEpochCredentialSource(root)
    value = credential()
    stored = source.store(
        SessionCredential(
            master_instance_id=value.master_instance_id,
            epoch=value.epoch,
            role=value.role,
            database_url=value.database_url,
            expires_at=value.expires_at,
        )
    )

    assert stored.stat().st_mode & 0o077 == 0
    assert source.load(request()).database_url == value.database_url


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


def test_credential_store_prunes_expired_and_superseded_envelopes(tmp_path) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path / "sessions"
    source = DirectoryEpochCredentialSource(root)
    old_instance = replace(
        credential(),
        master_instance_id="22222222-2222-4222-8222-222222222222",
        epoch=6,
    )
    source.store(old_instance)
    expired = root / "expired.json"
    expired.write_text(json.dumps({"expires_at": "2020-01-01T00:00:00Z"}))
    expired.chmod(0o600)

    current = credential()
    current_path = source.store(current)

    assert sorted(path.name for path in root.iterdir()) == [current_path.name]
    assert source.load(request()).epoch == 7


def test_credential_cleanup_is_bounded(tmp_path) -> None:  # type: ignore[no-untyped-def]
    root = tmp_path / "sessions"
    source = DirectoryEpochCredentialSource(root)
    source.store(credential())
    extra = root / "malformed.json"
    extra.write_text("not-json")
    extra.chmod(0o600)

    with pytest.raises(SessionBrokerError, match="exceeds the cleanup bound"):
        source.prune(now=datetime.now(UTC), max_files=1)


def test_restricted_login_rejects_superuser_or_wrong_group() -> None:
    operator_request = replace(request(), role="operator", tool="data.change.preview")
    session = PostgresMasterSession(
        operator_request,
        replace(credential(), role="operator"),
    )

    class Cursor:
        def __init__(self, row):  # type: ignore[no-untyped-def]
            self.row = row

        def execute(self, statement, parameters=()):  # type: ignore[no-untyped-def]
            assert parameters == ("mdh_mcp_editor",)
            assert "mdh_role_admin" not in statement
            return self

        def fetchone(self):  # type: ignore[no-untyped-def]
            return self.row

    safe = {
        "rolsuper": False,
        "rolcreatedb": False,
        "rolcreaterole": False,
        "rolreplication": False,
        "rolbypassrls": False,
        "requested_member": True,
        "owner_member": False,
    }
    session._assert_restricted_login(Cursor(safe))
    with pytest.raises(SessionBrokerError, match="restricted role login"):
        session._assert_restricted_login(Cursor({**safe, "rolsuper": True}))
    with pytest.raises(SessionBrokerError, match="restricted role login"):
        session._assert_restricted_login(Cursor({**safe, "requested_member": False}))


def test_session_timeouts_use_set_config_with_text_values() -> None:
    session = PostgresMasterSession(request(), credential())

    class Cursor:
        def __init__(self) -> None:
            self.queries: list[tuple[str, tuple[str, ...]]] = []

        def execute(self, statement, parameters=()):  # type: ignore[no-untyped-def]
            self.queries.append((statement, parameters))
            return self

    cursor = Cursor()
    session._set_local_timeouts(cursor)

    assert cursor.queries == [
        (
            "SELECT pg_catalog.set_config('statement_timeout', %s, true)",
            (f"{session.request.limits.timeout_ms}ms",),
        ),
        (
            "SELECT pg_catalog.set_config('lock_timeout', %s, true)",
            (f"{min(2_000, session.request.limits.timeout_ms)}ms",),
        ),
        (
            "SELECT pg_catalog.set_config('idle_in_transaction_session_timeout', %s, true)",
            (f"{session.request.limits.timeout_ms}ms",),
        ),
    ]


def test_session_revision_does_not_require_master_control_access() -> None:
    session = PostgresMasterSession(request(), credential())

    class Cursor:
        statement = ""

        def execute(self, statement, parameters=()):  # type: ignore[no-untyped-def]
            assert not parameters
            self.statement = statement
            return self

        def fetchone(self):  # type: ignore[no-untyped-def]
            return {"canonical_revision": 13}

    cursor = Cursor()
    assert session._canonical_revision(cursor) == 13
    assert "hub.canonical_state" in cursor.statement
    assert "master_control" not in cursor.statement


def test_connector_session_uses_resolved_revision_without_canonical_table_access() -> None:
    connector_request = replace(
        request(), role="connector", tool="submit_discovery_batch", canonical_revision=17
    )
    session = PostgresMasterSession(
        connector_request,
        replace(credential(), role="connector"),
    )

    class Cursor:
        def execute(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            raise AssertionError("connector must not query canonical or epoch-control tables")

    assert session._canonical_revision(Cursor()) == 17


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


def test_blogger_migration_accounting_is_typed_bounded_and_never_returns_rows() -> None:
    exact = "11111111-1111-4111-8111-111111111111"
    migration_request = SessionRequest(
        principal=identity(),
        master_instance_id=request().master_instance_id,
        epoch=7,
        role="reader",
        tool="bloggers.migration.accounting",
        limits=ExecutionLimits(),
    )
    session = PostgresMasterSession(migration_request, credential())

    class Cursor:
        statement = ""
        parameters = ()

        def execute(self, statement, parameters=()):  # type: ignore[no-untyped-def]
            self.statement = statement
            self.parameters = parameters
            return self

        def fetchone(self):  # type: ignore[no-untyped-def]
            return {"export_batch_id": exact, "raw_count": 266, "undispositioned_count": 0}

    cursor = Cursor()
    result = session._dispatch(cursor, {"export_batch_id": exact})
    assert result == {
        "found": True,
        "accounting": {"export_batch_id": exact, "raw_count": 266, "undispositioned_count": 0},
    }
    assert cursor.parameters == (exact,)
    assert "migration.raw_record" not in cursor.statement
    assert "payload" not in cursor.statement
    assert "verified_checkpoint_required" in cursor.statement


def test_blogger_migration_accounting_rejects_non_uuid_without_query() -> None:
    migration_request = SessionRequest(
        principal=identity(),
        master_instance_id=request().master_instance_id,
        epoch=7,
        role="reader",
        tool="bloggers.migration.accounting",
        limits=ExecutionLimits(),
    )
    session = PostgresMasterSession(migration_request, credential())

    class Cursor:
        def execute(self, *args, **kwargs):
            raise AssertionError("query must not execute")

    with pytest.raises(SessionBrokerError, match="exact UUID"):
        session._dispatch(Cursor(), {"export_batch_id": "not-a-uuid"})


def test_current_epoch_committer_can_reconcile_exact_old_epoch_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    current_master = "22222222-2222-4222-8222-222222222222"
    old_master = "11111111-1111-4111-8111-111111111111"
    reconcile_request = replace(
        request(),
        master_instance_id=current_master,
        epoch=8,
        role="canonical_committer",
        tool="bloggers.import.reconcile",
    )
    session = PostgresMasterSession(
        reconcile_request,
        replace(
            credential(),
            master_instance_id=current_master,
            epoch=8,
            role="canonical_committer",
        ),
    )
    observed: dict[str, object] = {}
    committed_at = datetime(2026, 8, 16, 12, 34, 56, tzinfo=UTC)

    def reconcile(_cursor, _identity, *, plan_sha256, master_instance_id, master_epoch):  # type: ignore[no-untyped-def]
        observed.update(
            plan_sha256=plan_sha256,
            master_instance_id=str(master_instance_id),
            master_epoch=master_epoch,
        )
        return BloggerImportApplyReceipt(
            operation_id="a" * 64,
            batch_id=UUID("33333333-3333-4333-8333-333333333333"),
            plan_sha256="c" * 64,
            affected_rows=2,
            revision_after=13,
            duplicate=True,
            committed_at=committed_at,
        )

    monkeypatch.setattr(
        "my_data_hub.workloads.bloggers.discovery_postgres.BloggerDiscoveryPostgres.reconcile",
        reconcile,
    )
    result = session._dispatch_blogger_import(
        object(),
        {
            "operation_id": "a" * 64,
            "batch_id": "33333333-3333-4333-8333-333333333333",
            "request_sha256": "b" * 64,
            "plan_sha256": "c" * 64,
            "master_instance_id": old_master,
            "master_epoch": 7,
            "expected_revision": 12,
            "principal_id": identity().subject,
            "client_id": identity().client_id,
        },
    )
    assert observed == {
        "plan_sha256": "c" * 64,
        "master_instance_id": old_master,
        "master_epoch": 7,
    }
    assert result["receipt_master_instance_id"] == old_master
    assert result["receipt_master_epoch"] == 7
    assert result["committed_at"] == committed_at
