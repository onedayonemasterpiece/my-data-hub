from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from uuid import UUID

import pytest

from my_data_hub.connectors.contracts import ObservedPeriod, canonical_json_bytes, payload_sha256
from my_data_hub.connectors.postgres import PostgresConnectorAcceptanceRepository
from my_data_hub.connectors.service import ConnectorIntakeService
from my_data_hub.db.migrations import migrate
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner
from my_data_hub.master_runtime.database_gate import DatabaseGate
from my_data_hub.workloads.bloggers.discovery import (
    BloggerDiscoveryRow,
    ProviderArtifactClaim,
    SubmitDiscoveryBatch,
)
from my_data_hub.workloads.bloggers.discovery_postgres import (
    BloggerDiscoveryPostgres,
    BloggerImportIdentity,
)

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _role_url(port: int, principal: str, password: str, role: str) -> str:
    options = quote(f"-c role={role}")
    return (
        f"postgresql://{principal}:{password}@127.0.0.1:{port}/postgres"
        f"?options={options}"
    )


@pytest.mark.skipif(
    os.getenv("MDH_RUN_DISPOSABLE_POSTGRES") != "1" or shutil.which("docker") is None,
    reason="set MDH_RUN_DISPOSABLE_POSTGRES=1 for disposable tmpfs PostgreSQL proof",
)
def test_typed_blogger_preview_apply_replay_and_role_boundary() -> None:
    import psycopg
    from psycopg.rows import dict_row

    port = _free_port()
    container = f"mdh-blogger-discovery-{os.getpid()}"
    admin_password = "fixture-admin-password-not-a-secret"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            container,
            "--tmpfs",
            "/var/lib/postgresql:rw,nosuid,nodev,size=768m",
            "-e",
            f"POSTGRES_PASSWORD={admin_password}",
            "-p",
            f"127.0.0.1:{port}:5432",
            IMAGE,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    admin_url = f"postgresql://postgres:{admin_password}@127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(admin_url, connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise AssertionError("disposable PostgreSQL did not become ready") from None
                time.sleep(0.5)
        with psycopg.connect(admin_url) as admin:
            admin.execute((ROOT / "sql/admin/bootstrap_roles.sql").read_text())
            admin.commit()
        migrate(admin_url, ROOT / "sql/migrations")
        with psycopg.connect(admin_url) as admin:
            admin.execute((ROOT / "sql/admin/role_contract.sql").read_text())
            admin.commit()

        now = datetime.now(UTC)
        master = MasterIdentity(
            UUID("11111111-1111-4111-8111-111111111111"), "docker-blogger", 1
        )
        connector_password = "connector-password-long-enough"
        committer_password = "committer-password-long-enough"
        materializer_password = "materializer-password-long-enough"
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.acquire(master, now + timedelta(minutes=10))
            gate.activate(master)
            provisioner = CredentialProvisioner(admin, gate)
            provisioner.create(
                principal="mdh_e1_connector_deadbeef",
                password=connector_password,
                group="mdh_connector_intake",
                identity=master,
                credential_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
            provisioner.create(
                principal="mdh_e1_materializer_facefeed",
                password=materializer_password,
                group="mdh_blogger_materializer",
                identity=master,
                credential_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
            provisioner.create(
                principal="mdh_e1_committer_cafebabe",
                password=committer_password,
                group="mdh_canonical_committer",
                identity=master,
                credential_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                expires_at=now + timedelta(minutes=9),
                now=now,
            )

        request = SubmitDiscoveryBatch(
            batch_id=UUID("22222222-2222-4222-8222-222222222222"),
            idempotency_key="discovery-live-exact-replay",
            project_slug="region-talk",
            produced_at=now,
            observed_period=ObservedPeriod(
                start=now - timedelta(hours=1), end=now, timezone="UTC"
            ),
            rows=[
                BloggerDiscoveryRow(
                    source_record_id="search-result-1",
                    actor_kind="person",
                    display_name="Проверяемый автор",
                    accounts=[
                        {
                            "platform": "telegram",
                            "handle": "verified_author",
                            "url": "https://t.me/verified_author",
                        }
                    ],
                    source_uri="https://example.test/search/1",
                    observed_at=now,
                    evidence={"query": "калининградский блогер"},
                )
            ],
        )
        connector_url = _role_url(
            port,
            "mdh_e1_connector_deadbeef",
            connector_password,
            "mdh_connector_intake",
        )
        decision = ConnectorIntakeService(
            PostgresConnectorAcceptanceRepository(connector_url)
        ).submit(
            request.connector_envelope_bytes(),
            authenticated_connector_id="mcp-blogger-discovery-inline-v1",
            authenticated_principal="service:mcp-blogger-discovery-inline-v1",
        )
        assert decision.receipt is not None
        invalid_records = [
            {
                "source_record_id": "invalid-search-result",
                "actor_kind": "person",
                "display_name": "Неполная запись",
                "accounts": [{"platform": "telegram"}],
                "source_uri": "https://example.test/search/invalid",
                "observed_at": now.isoformat(),
                "evidence": {},
            }
        ]
        invalid_envelope = request.connector_envelope().model_copy(
            update={
                "batch_id": UUID("33333333-3333-4333-8333-333333333333"),
                "idempotency_key": "discovery-live-quarantine",
                "payload_sha256": payload_sha256(invalid_records),
                "inline_records": invalid_records,
            }
        )
        invalid_decision = ConnectorIntakeService(
            PostgresConnectorAcceptanceRepository(connector_url)
        ).submit(
            canonical_json_bytes(invalid_envelope.model_dump(mode="json", exclude_none=True)),
            authenticated_connector_id="mcp-blogger-discovery-inline-v1",
            authenticated_principal="service:mcp-blogger-discovery-inline-v1",
        )
        assert invalid_decision.receipt is not None
        artifact_request = SubmitDiscoveryBatch(
            batch_id=UUID("44444444-4444-4444-8444-444444444444"),
            idempotency_key="discovery-live-artifact",
            project_slug="region-talk",
            produced_at=now,
            observed_period=ObservedPeriod(
                start=now - timedelta(hours=1), end=now, timezone="UTC"
            ),
            artifact=ProviderArtifactClaim(
                resource_ref="owner/private-bloggers",
                control_class="mcp_exchange",
                provider_version=9,
                path="exports/bloggers.json",
                media_type="application/json",
                byte_size=1024,
                sha256="9" * 64,
                claim_sha256="8" * 64,
                record_count=1,
            ),
        )
        artifact_decision = ConnectorIntakeService(
            PostgresConnectorAcceptanceRepository(connector_url)
        ).submit(
            artifact_request.connector_envelope_bytes(),
            authenticated_connector_id="mcp-blogger-discovery-artifact-v1",
            authenticated_principal="service:mcp-blogger-discovery-artifact-v1",
            artifact_record_count=1,
        )
        assert artifact_decision.receipt is not None
        artifact_records = [request.rows[0].model_dump(mode="json")]  # type: ignore[index]
        with psycopg.connect(connector_url) as generic_connector:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                generic_connector.execute(
                    "SELECT integration.materialize_blogger_discovery_artifact(%s,%s,%s::jsonb,%s)",
                    (
                        artifact_request.batch_id,
                        "9" * 64,
                        json.dumps(artifact_records),
                        "mdh_e1_connector_deadbeef",
                    ),
                )
            generic_connector.rollback()
        materializer_url = _role_url(
            port,
            "mdh_e1_materializer_facefeed",
            materializer_password,
            "mdh_blogger_materializer",
        )
        with psycopg.connect(materializer_url) as materializer:
            invalid_artifact_records = [
                {**artifact_records[0], "accounts": [{"platform": "telegram"}]}
            ]
            with pytest.raises(psycopg.errors.InvalidParameterValue, match="artifact account"):
                materializer.execute(
                    "SELECT integration.materialize_blogger_discovery_artifact(%s,%s,%s::jsonb,%s)",
                    (
                        artifact_request.batch_id,
                        "9" * 64,
                        json.dumps(invalid_artifact_records),
                        "mdh_e1_materializer_facefeed",
                    ),
                )
            materializer.rollback()
            materialized_sha = materializer.execute(
                "SELECT integration.materialize_blogger_discovery_artifact(%s,%s,%s::jsonb,%s)",
                (
                    artifact_request.batch_id,
                    "9" * 64,
                    json.dumps(artifact_records),
                    "mdh_e1_materializer_facefeed",
                ),
            ).fetchone()[0]
            materializer.commit()
            assert len(materialized_sha) == 64

        operation_id = "d" * 64
        identity = BloggerImportIdentity(
            batch_id=request.batch_id,
            operation_id=operation_id,
            request_sha256="e" * 64,
            expected_revision=0,
            principal_id="owner:test",
            client_id="chatgpt:test",
        )
        committer_url = _role_url(
            port,
            "mdh_e1_committer_cafebabe",
            committer_password,
            "mdh_canonical_committer",
        )
        with psycopg.connect(committer_url, row_factory=dict_row) as connection:
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(
                    "INSERT INTO hub.actor(actor_type,display_name) VALUES ('person','forbidden')"
                )
            connection.rollback()
            quarantined_identity = BloggerImportIdentity(
                batch_id=invalid_envelope.batch_id,
                operation_id="f" * 64,
                request_sha256="a" * 64,
                expected_revision=0,
                principal_id="owner:test",
                client_id="chatgpt:test",
            )
            quarantined = BloggerDiscoveryPostgres.preview(connection, quarantined_identity)
            connection.commit()
            assert quarantined.create_actor_count == 0
            assert quarantined.quarantine_count == 1
            with pytest.raises(psycopg.errors.InvalidParameterValue, match="no committable"):
                BloggerDiscoveryPostgres.apply(
                    connection,
                    quarantined_identity,
                    plan_sha256=quarantined.plan_sha256,
                )
            connection.rollback()
            preview = BloggerDiscoveryPostgres.preview(connection, identity)
            assert preview.create_actor_count == 1
            assert preview.account_count == 1
            assert preview.quarantine_count == 0
            connection.commit()

            applied = BloggerDiscoveryPostgres.apply(
                connection, identity, plan_sha256=preview.plan_sha256
            )
            connection.commit()
            assert applied.duplicate is False
            assert applied.revision_after == 1
            assert applied.affected_rows >= 1

            reconciled = BloggerDiscoveryPostgres.reconcile(
                connection,
                identity,
                plan_sha256=preview.plan_sha256,
                master_instance_id=master.master_instance_id,
                master_epoch=master.epoch,
            )
            connection.commit()
            assert reconciled is not None
            assert reconciled.duplicate is True
            assert reconciled.revision_after == applied.revision_after

            replay = BloggerDiscoveryPostgres.apply(
                connection, identity, plan_sha256=preview.plan_sha256
            )
            connection.commit()
            assert replay.duplicate is True
            assert replay.revision_after == applied.revision_after
            conflicting_identity = BloggerImportIdentity(
                batch_id=identity.batch_id,
                operation_id=identity.operation_id,
                request_sha256="c" * 64,
                expected_revision=identity.expected_revision,
                principal_id=identity.principal_id,
                client_id=identity.client_id,
            )
            with pytest.raises(psycopg.errors.SerializationFailure, match="conflicts"):
                BloggerDiscoveryPostgres.apply(
                    connection,
                    conflicting_identity,
                    plan_sha256=preview.plan_sha256,
                )
            connection.rollback()

        # Reconciliation is an immutable receipt lookup, not an old-epoch
        # mutation.  A newly ACTIVE epoch credential must therefore be able to
        # read the exact epoch-1 receipt after epoch 1 is fenced.
        successor = MasterIdentity(
            UUID("55555555-5555-4555-8555-555555555555"), "docker-blogger-2", 2
        )
        successor_password = "successor-committer-password-long-enough"
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.fence(master, "disposable-successor")
            gate.acquire(successor, now + timedelta(minutes=10))
            gate.activate(successor)
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e2_committer_feedface",
                password=successor_password,
                group="mdh_canonical_committer",
                identity=successor,
                credential_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
        successor_url = _role_url(
            port,
            "mdh_e2_committer_feedface",
            successor_password,
            "mdh_canonical_committer",
        )
        with psycopg.connect(successor_url, row_factory=dict_row) as successor_connection:
            successor_reconciled = BloggerDiscoveryPostgres.reconcile(
                successor_connection,
                identity,
                plan_sha256=preview.plan_sha256,
                master_instance_id=master.master_instance_id,
                master_epoch=master.epoch,
            )
            successor_connection.commit()
            assert successor_reconciled is not None
            assert successor_reconciled.duplicate is True
            assert successor_reconciled.revision_after == applied.revision_after

        with psycopg.connect(admin_url) as admin:
            assert admin.execute(
                "SELECT display_name FROM hub.bloggers_v1 WHERE display_name=%s",
                ("Проверяемый автор",),
            ).fetchone()[0] == "Проверяемый автор"
            assert admin.execute(
                "SELECT count(*) FROM sync.external_outbox "
                "WHERE aggregate_type='blogger_discovery_batch'"
            ).fetchone()[0] == 1
            assert admin.execute(
                "SELECT canonical_revision FROM hub.canonical_state WHERE singleton"
            ).fetchone()[0] == 1
    finally:
        subprocess.run(
            ["docker", "rm", "--force", container],
            check=False,
            capture_output=True,
        )
