"""Disposable PostgreSQL proof for Region Talk snapshot integrity and apply semantics."""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from collections.abc import Mapping, Sequence
from copy import deepcopy
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
            {
                "dedupe_key": "opp-1",
                "platform": "web",
                "payload_json": '{"status":"new","canonical_url":"https://example.test/opp"}',
            }
        ],
        "acq_discovery_runs": [{"run_uid": "run-1", "stats_json": "{}"}],
        "acq_discovery_surfaces": [
            {
                "external_id": "surface-1",
                "platform": "telegram",
                "status": "active",
                "url": "https://t.me/source",
            }
        ],
        "region_talk_compact_state_kv": [
            {
                "pk": "article-1",
                "kind": "external_publication_intake_item",
                "updated_at": now,
                "payload_json": {
                    "title": "Article",
                    "canonical_url": "https://example.test/article",
                    "publication_status": "accepted",
                },
            },
            {
                "pk": "candidate-1",
                "kind": "publication_candidate_item",
                "updated_at": now,
                "payload_json": {
                    "title": "Candidate",
                    "canonical_url": "https://example.test/candidate",
                    "publication_status": "ready",
                    "body": "Ready text",
                },
            },
            {
                "pk": "post-1",
                "kind": "processed_post_item",
                "updated_at": now,
                "payload_json": {
                    "title": "Post",
                    "canonical_url": "https://example.test/post",
                    "platform": "telegram",
                    "external_id": "42",
                    "status": "evaluated",
                },
            },
            {
                "pk": "review-1",
                "kind": "publication_review_event_item",
                "updated_at": now,
                "payload_json": {
                    "title": "Candidate",
                    "canonical_url": "https://example.test/candidate",
                    "decision": "approve",
                    "actor_ref": "owner-review",
                },
            },
            {
                "pk": "schedule-1",
                "kind": "publication_schedule_item",
                "updated_at": now,
                "payload_json": {
                    "title": "Candidate",
                    "canonical_url": "https://example.test/candidate",
                    "publication_status": "planned",
                    "channel": "region-talk-new-channel",
                },
            },
            {
                "pk": "source-candidate-1",
                "kind": "source_candidate_item",
                "updated_at": now,
                "payload_json": {
                    "candidate_url": "https://t.me/candidate",
                    "platform": "telegram",
                    "status": "pending",
                },
            },
            {
                "pk": "source-queue-1",
                "kind": "source_queue_item",
                "updated_at": now,
                "payload_json": {
                    "source_ref": "https://t.me/candidate",
                    "platform": "telegram",
                    "source_queue_status": "pending",
                    "priority": "10",
                    "readiness_state": "scan_due",
                },
            },
            {
                "pk": "source-status-1",
                "kind": "source_status_item",
                "updated_at": now,
                "payload_json": {
                    "source_ref": "https://t.me/candidate",
                    "platform": "telegram",
                    "status": "active",
                    "reason": "imported",
                },
            },
        ],
        "region_talk_external_blogger_evidence": [{"record_id": "blogger-1", "blogger_name": "One", "updated_at": now}],
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
            registration = connection.execute(
                "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                    PRINCIPAL,
                    "region_talk",
                    UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                    1,
                    IDENTITY.master_instance_id,
                    1,
                    "5" * 64,
                    "6" * 64,
                ),
            ).fetchone()[0]
            connection.commit()
            assert registration == {
                "registered": True,
                "credential_id": "dddddddd-dddd-4ddd-8ddd-dddddddddddd",
                "principal": PRINCIPAL,
                "worker_kind": "region_talk",
                "task_run_id": "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
                "generation": 1,
                "master_instance_id": str(IDENTITY.master_instance_id),
                "epoch": 1,
                "command_sha256": "5" * 64,
                "task_token_sha256": "6" * 64,
            }
            assert (
                connection.execute(
                    "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                        PRINCIPAL,
                        "region_talk",
                        UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                        1,
                        IDENTITY.master_instance_id,
                        1,
                        "5" * 64,
                        "6" * 64,
                    ),
                ).fetchone()[0]
                == registration
            )
            with pytest.raises(
                psycopg.errors.UniqueViolation,
                match="conflicts with immutable task binding",
            ):
                connection.execute(
                    "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd"),
                        PRINCIPAL,
                        "region_talk",
                        UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                        1,
                        IDENTITY.master_instance_id,
                        1,
                        "5" * 64,
                        "6" * 64,
                    ),
                )
            connection.rollback()

        rows = _rows(now)
        with psycopg.connect(role_url) as connection:
            runner = DirectSnapshotRunner(MemoryReader(rows), connection, page_size=3)
            invented = runner.inventory(
                export_batch_id=UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc"),
                task_run_id=UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="7" * 64,
                created_at=now,
            )
            with pytest.raises(psycopg.errors.InsufficientPrivilege, match="not registered for exact task"):
                runner._begin(invented)
            connection.rollback()
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
            conflicting_tables[0] = conflicting_tables[0].model_copy(update={"logical_sha256": "0" * 64})
            with pytest.raises(
                psycopg.errors.SerializationFailure,
                match="replay conflicts with verified Pass B",
            ):
                runner._finalize(manifest, conflicting_tables)
        assert first.status == replay.status == "complete"

        with psycopg.connect(admin_url) as connection:
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (1,)
            assert connection.execute(
                "SELECT count(*) FROM migration.region_talk_canonical_apply_receipt"
            ).fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM hub.content_item").fetchone()[0] >= 4
            assert connection.execute("SELECT count(*) FROM region_talk.source").fetchone()[0] >= 3
            assert connection.execute("SELECT count(*) FROM orchestration.work_item").fetchone()[0] >= 1
            assert connection.execute("SELECT count(*) FROM region_talk.publication_candidate").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.candidate_revision").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.publication_plan").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.review_decision").fetchone() == (1,)
            assert connection.execute(
                "SELECT channel,plan_status,review_decision FROM region_talk.publication_queue_v3"
            ).fetchone() == ("region-talk-new-channel", "planned", "approve")

        # A distinct accepted snapshot with changed current state advances
        # one revision, but does not duplicate immutable candidate/review/status
        # semantics. The latest accepted snapshot becomes the only typed source.
        second_rows = deepcopy(rows)
        changed_at = now + timedelta(minutes=1)
        compact = {row["pk"]: row for row in second_rows["region_talk_compact_state_kv"]}
        for key in ("source-candidate-1", "source-status-1", "source-queue-1", "schedule-1", "review-1"):
            compact[key]["updated_at"] = changed_at
        compact["source-candidate-1"]["payload_json"].update(
            {"candidate_url": "https://t.me/candidate-new", "status": "accepted"}
        )
        compact["source-status-1"]["payload_json"].update({"status": "paused", "reason": "operator_pause"})
        compact["source-queue-1"]["payload_json"].update(
            {"source_queue_status": "completed", "priority": "5", "readiness_state": "terminal"}
        )
        compact["schedule-1"]["payload_json"].update(
            {
                "publication_status": "queued",
                "channel": "region-talk-updated-channel",
                "scheduled_for": changed_at.isoformat(),
            }
        )
        compact["review-1"]["payload_json"].update({"decision": "reject", "reason": "changed review"})
        principal2 = "mdh_e1_regiong2_eeeeeeee"
        password2 = "region-talk-second-password-long-enough"
        credential2 = UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee")
        second_task = UUID("34343434-3434-4434-8434-343434343434")
        with psycopg.connect(admin_url) as connection:
            gate = DatabaseGate(connection)
            CredentialProvisioner(connection, gate).create(
                principal=principal2,
                password=password2,
                group="mdh_region_talk_pipeline",
                identity=IDENTITY,
                credential_id=credential2,
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
            connection.execute(
                "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    credential2,
                    principal2,
                    "region_talk",
                    second_task,
                    1,
                    IDENTITY.master_instance_id,
                    1,
                    "8" * 64,
                    "9" * 64,
                ),
            )
            connection.commit()
        role_url2 = f"postgresql://{principal2}:{password2}@127.0.0.1:{port}/postgres"
        with psycopg.connect(role_url2) as connection:
            runner = DirectSnapshotRunner(MemoryReader(second_rows), connection, page_size=3)
            second_manifest = runner.inventory(
                export_batch_id=UUID("12121212-1212-4212-8212-121212121212"),
                task_run_id=second_task,
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="4" * 64,
                created_at=now,
            )
            assert runner.run(second_manifest).status == "complete"
        with psycopg.connect(admin_url) as connection:
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (2,)
            assert connection.execute(
                "SELECT count(*) FROM migration.region_talk_canonical_apply_receipt"
            ).fetchone() == (2,)
            assert connection.execute("SELECT count(*) FROM region_talk.candidate_revision").fetchone() == (1,)
            assert connection.execute("SELECT candidate_url,status FROM region_talk.source_candidate").fetchone() == (
                "https://t.me/candidate-new",
                "accepted",
            )
            assert connection.execute(
                "SELECT status,reason FROM region_talk.source_status ORDER BY occurred_at DESC LIMIT 1"
            ).fetchone() == ("paused", "operator_pause")
            assert connection.execute("SELECT count(*) FROM region_talk.source_status").fetchone() == (2,)
            assert connection.execute("SELECT status,priority FROM orchestration.work_item").fetchone() == (
                "succeeded",
                5,
            )
            assert connection.execute("SELECT count(*) FROM orchestration.work_item_event").fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM region_talk.review_decision").fetchone() == (2,)
            assert connection.execute(
                "SELECT decision FROM region_talk.review_decision ORDER BY occurred_at DESC LIMIT 1"
            ).fetchone() == ("reject",)
            assert connection.execute("SELECT count(*) FROM region_talk.publication_plan").fetchone() == (1,)
            assert connection.execute("SELECT channel,status FROM region_talk.publication_plan").fetchone() == (
                "region-talk-updated-channel",
                "queued",
            )
            assert connection.execute("SELECT status FROM region_talk.publication_candidate").fetchone() == (
                "rejected",
            )
            assert connection.execute(
                "SELECT channel,plan_status,review_decision FROM region_talk.publication_queue_v3"
            ).fetchone() == ("region-talk-updated-channel", "queued", "reject")
            assert connection.execute(
                "SELECT count(*) FROM migration.region_talk_canonical_state_observation"
            ).fetchone() == (10,)
            assert connection.execute("SELECT export_batch_id FROM region_talk.accepted_snapshot_v2").fetchone() == (
                second_manifest.export_batch_id,
            )

        # Same row count but changed payload lands with valid new row/page hashes;
        # finalization still rejects it against Pass A and persisted evidence.
        changed = {spec.name: [] for spec in DIRECT_SOURCE_TABLES}
        changed["region_talk_compact_state_kv"] = [
            {
                "pk": "mutation-1",
                "kind": "external_publication_intake_item",
                "payload_json": {"canonical_url": "https://example.test/original"},
            }
        ]
        principal3 = "mdh_e1_regiong3_ffffffff"
        password3 = "region-talk-third-password-long-enough"
        credential3 = UUID("ffffffff-eeee-4eee-8eee-ffffffffffff")
        third_task = UUID("99999999-9999-4999-8999-999999999999")
        with psycopg.connect(admin_url) as connection:
            gate = DatabaseGate(connection)
            CredentialProvisioner(connection, gate).create(
                principal=principal3,
                password=password3,
                group="mdh_region_talk_pipeline",
                identity=IDENTITY,
                credential_id=credential3,
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
            connection.execute(
                "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    credential3,
                    principal3,
                    "region_talk",
                    third_task,
                    1,
                    IDENTITY.master_instance_id,
                    1,
                    "a" * 64,
                    "b" * 64,
                ),
            )
            connection.commit()
        role_url3 = f"postgresql://{principal3}:{password3}@127.0.0.1:{port}/postgres"
        with psycopg.connect(role_url3) as connection:
            runner = DirectSnapshotRunner(MemoryReader(changed), connection, page_size=3)
            manifest = runner.inventory(
                export_batch_id=UUID("ffffffff-ffff-4fff-8fff-ffffffffffff"),
                task_run_id=third_task,
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
