"""Disposable PostgreSQL proof for Region Talk snapshot integrity and apply semantics."""
from __future__ import annotations

import json
import os
import shutil
import socket
import subprocess
import time
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import pytest

from my_data_hub.db.migrations import migrate
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner
from my_data_hub.master_runtime.database_gate import DatabaseGate
from my_data_hub.orchestrator.registry import load_pipeline_definition
from my_data_hub.orchestrator.repository import register_pipeline
from my_data_hub.workloads.region_talk.constants import DIRECT_SOURCE_TABLES
from my_data_hub.workloads.region_talk.direct_snapshot import DirectSnapshotRunner

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"
IDENTITY = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "fixture-run", 1)
PRINCIPAL = "mdh_e1_regiong1_deadbeef"
PASSWORD = "region-talk-fixture-password-long-enough"


class MemoryReader:
    def __init__(self, rows: dict[str, list[dict[str, Any]]]) -> None:
        self.rows = rows

    def scan_page(
        self,
        source_table: str,
        *,
        primary_key: str,
        after_primary_key: str | None,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        return [
            row
            for row in self.rows[source_table]
            if after_primary_key is None or str(row[primary_key]) > after_primary_key
        ][:limit]


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


def _rows(now: datetime) -> dict[str, list[dict[str, Any]]]:
    return {
        "acq_discovery_opportunities": [
            {"dedupe_key": "opp-1", "platform": "web", "payload_json": '{"status":"new","canonical_url":"https://example.test/opp"}'}
        ],
        "acq_discovery_runs": [{"run_uid": "run-1", "stats_json": "{}"}],
        "acq_discovery_surfaces": [
            {"external_id": "surface-1", "platform": "telegram", "status": "active", "url": "https://t.me/source"}
        ],
        "region_talk_compact_state_kv": [
            {"pk": "article-1", "kind": "external_publication_intake_item", "payload_json": {"title": "Article", "canonical_url": "https://example.test/article", "publication_status": "accepted"}},
            {"pk": "candidate-1", "kind": "publication_candidate_item", "payload_json": {"title": "Candidate", "canonical_url": "https://example.test/candidate", "publication_status": "ready", "body": "Ready text"}},
            {"pk": "post-1", "kind": "processed_post_item", "payload_json": {"title": "Post", "canonical_url": "https://example.test/post", "platform": "telegram", "external_id": "42", "status": "evaluated"}},
            {"pk": "review-1", "kind": "publication_review_event_item", "payload_json": {"title": "Candidate", "canonical_url": "https://example.test/candidate", "decision": "approve", "actor_ref": "owner-review"}},
            {"pk": "schedule-1", "kind": "publication_schedule_item", "payload_json": {"title": "Candidate", "canonical_url": "https://example.test/candidate", "publication_status": "planned", "channel": "region-talk-new-channel"}},
            {"pk": "source-candidate-1", "kind": "source_candidate_item", "payload_json": {"candidate_url": "https://t.me/candidate", "platform": "telegram", "status": "pending"}},
            {"pk": "source-queue-1", "kind": "source_queue_item", "payload_json": {"source_ref": "https://t.me/candidate", "platform": "telegram", "source_queue_status": "pending", "priority": "10", "readiness_state": "scan_due"}},
            {"pk": "source-status-1", "kind": "source_status_item", "payload_json": {"source_ref": "https://t.me/candidate", "platform": "telegram", "status": "active", "reason": "imported"}},
        ],
        "region_talk_external_blogger_evidence": [
            {"record_id": "blogger-1", "blogger_name": "One", "updated_at": now}
        ],
    }


@pytest.mark.skipif(
    os.getenv("MDH_RUN_DISPOSABLE_POSTGRES") != "1" or shutil.which("docker") is None,
    reason="set MDH_RUN_DISPOSABLE_POSTGRES=1 for disposable tmpfs PostgreSQL proof",
)
def test_snapshot_integrity_replay_canonical_apply_and_latest_views() -> None:
    import psycopg

    port = _free_port()
    container = f"mdh-region-talk-integrity-{os.getpid()}"
    admin_password = "fixture-admin-password-not-a-secret"
    subprocess.run(
        [
            "docker", "run", "--detach", "--rm", "--name", container,
            "--tmpfs", "/var/lib/postgresql:rw,nosuid,nodev,size=768m",
            "-e", f"POSTGRES_PASSWORD={admin_password}",
            "-p", f"127.0.0.1:{port}:5432", IMAGE,
        ],
        check=True, capture_output=True, text=True,
    )
    admin_url = f"postgresql://postgres:{admin_password}@127.0.0.1:{port}/postgres"
    role_url = f"postgresql://{PRINCIPAL}:{PASSWORD}@127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(admin_url, connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise
                time.sleep(0.25)
        with psycopg.connect(admin_url) as connection:
            connection.execute((ROOT / "sql/admin/bootstrap_roles.sql").read_text())
            connection.commit()
        migrate(admin_url, ROOT / "sql/migrations")
        with psycopg.connect(admin_url) as connection:
            connection.execute((ROOT / "sql/admin/role_contract.sql").read_text())
            connection.commit()

        now = datetime.now(UTC)
        register_pipeline(
            admin_url,
            load_pipeline_definition(ROOT / "config/pipelines/region-talk.v1.json"),
            ROOT / "config/pipelines/region-talk.v1.json",
        )
        with psycopg.connect(admin_url) as connection:
            gate = DatabaseGate(connection)
            gate.acquire(IDENTITY, now + timedelta(minutes=10))
            gate.activate(IDENTITY)
            CredentialProvisioner(connection, gate).create(
                principal=PRINCIPAL,
                password=PASSWORD,
                group="mdh_region_talk_pipeline",
                identity=IDENTITY,
                credential_id=UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                expires_at=now + timedelta(minutes=9),
                now=now,
            )

        rows = _rows(now)
        with psycopg.connect(role_url) as connection:
            runner = DirectSnapshotRunner(MemoryReader(rows), connection, page_size=3)
            manifest = runner.inventory(
                export_batch_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                task_run_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="1" * 64,
                created_at=now,
            )
            first = runner.run(manifest)
            replay = runner.run(manifest)
            conflicting_tables = list(manifest.tables)
            conflicting_tables[0] = conflicting_tables[0].model_copy(
                update={"logical_sha256": "0" * 64}
            )
            with pytest.raises(
                psycopg.errors.SerializationFailure,
                match="replay conflicts with verified Pass B",
            ):
                runner._finalize(manifest, conflicting_tables)
        assert first.status == replay.status == "complete"

        with psycopg.connect(admin_url) as connection:
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM migration.region_talk_canonical_apply_receipt").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM hub.content_item").fetchone()[0] >= 4
            assert connection.execute("SELECT count(*) FROM region_talk.source").fetchone()[0] >= 3
            assert connection.execute("SELECT count(*) FROM orchestration.work_item").fetchone()[0] >= 1
            assert connection.execute("SELECT count(*) FROM region_talk.publication_candidate").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.candidate_revision").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.publication_plan").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.review_decision").fetchone() == (1,)
            assert connection.execute(
                "SELECT channel,plan_status FROM region_talk.publication_queue_v3"
            ).fetchone() == ("region-talk-new-channel", "planned")

        # A distinct accepted snapshot of the same source identities advances
        # one revision, but does not duplicate immutable candidate/review/status
        # semantics. The latest accepted snapshot becomes the only typed source.
        with psycopg.connect(role_url) as connection:
            runner = DirectSnapshotRunner(MemoryReader(rows), connection, page_size=3)
            second_manifest = runner.inventory(
                export_batch_id=UUID("12121212-1212-4212-8212-121212121212"),
                task_run_id=UUID("34343434-3434-4434-8434-343434343434"),
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="4" * 64,
                created_at=now,
            )
            assert runner.run(second_manifest).status == "complete"
        with psycopg.connect(admin_url) as connection:
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (2,)
            assert connection.execute("SELECT count(*) FROM migration.region_talk_canonical_apply_receipt").fetchone() == (2,)
            assert connection.execute("SELECT count(*) FROM region_talk.candidate_revision").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.source_status").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.review_decision").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.publication_plan").fetchone() == (1,)
            assert connection.execute(
                "SELECT export_batch_id FROM region_talk.accepted_snapshot_v2"
            ).fetchone() == (second_manifest.export_batch_id,)

        # Same row count but changed payload lands with valid new row/page hashes;
        # finalization still rejects it against Pass A and persisted evidence.
        changed = {spec.name: [] for spec in DIRECT_SOURCE_TABLES}
        changed["region_talk_compact_state_kv"] = [
            {"pk": "mutation-1", "kind": "external_publication_intake_item", "payload_json": {"canonical_url": "https://example.test/original"}}
        ]
        with psycopg.connect(role_url) as connection:
            runner = DirectSnapshotRunner(MemoryReader(changed), connection, page_size=3)
            manifest = runner.inventory(
                export_batch_id=UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
                task_run_id=UUID("99999999-9999-4999-8999-999999999999"),
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="3" * 64,
                created_at=now,
            )
            runner._begin(manifest)
            changed["region_talk_compact_state_kv"][0]["payload_json"]["canonical_url"] = "https://example.test/changed"
            observed = [
                runner._scan(spec, land_batch_id=manifest.export_batch_id, task_run_id=manifest.task_run_id).receipt()
                for spec in DIRECT_SOURCE_TABLES
            ]
            with pytest.raises(psycopg.errors.ObjectNotInPrerequisiteState, match="server-recomputed evidence"):
                runner._finalize(manifest, observed)

        # A newer LANDING/failed attempt never leaks into the accepted typed readers.
        with psycopg.connect(admin_url) as connection:
            assert connection.execute("SELECT count(*) FROM region_talk.accepted_snapshot_v2").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.articles_v2").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.posts_v2").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.publication_queue_v3").fetchone() == (1,)
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (2,)
    finally:
        subprocess.run(["docker", "rm", "--force", container], check=False, capture_output=True)
