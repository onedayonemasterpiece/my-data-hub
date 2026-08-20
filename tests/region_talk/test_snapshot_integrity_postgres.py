"""Disposable PostgreSQL proof for Region Talk snapshot integrity and apply semantics."""

from __future__ import annotations

import json
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
from uuid import UUID, uuid5

import pytest

from my_data_hub.db.migrations import migrate
from my_data_hub.hashing import sha256_value
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner
from my_data_hub.master_runtime.database_gate import DatabaseGate
from my_data_hub.workloads.region_talk.constants import DIRECT_SOURCE_TABLES
from my_data_hub.workloads.region_talk.direct_snapshot import DirectSnapshotRunner
from my_data_hub.workloads.region_talk.stage_execution import StagePreparation, form_stage_commit

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"
IDENTITY = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "fixture-run", 1)
PRINCIPAL = "mdh_e1_regiong1_deadbeef"
PASSWORD = "region-talk-fixture-password-long-enough"
STAGE_NAMESPACE = UUID("54a0dba7-1e4b-4d56-a143-173304989e85")
POST_IMPORT_DAG = [
    {
        "stage": "canonical_import",
        "contract_version": "region-talk-direct-snapshot-receipt.v2",
        "dependencies": [],
        "max_attempts": 1,
        "timeout_seconds": 3600,
    },
    {
        "stage": "e5_embedding",
        "contract_version": "e5_semantic_bank_scores_v1",
        "dependencies": ["canonical_import"],
        "max_attempts": 3,
        "timeout_seconds": 900,
    },
    {
        "stage": "bge_m3_embedding",
        "contract_version": "bge_m3_flagembedding_dense_v1",
        "dependencies": ["canonical_import"],
        "max_attempts": 3,
        "timeout_seconds": 1200,
    },
    {
        "stage": "vector_fusion",
        "contract_version": "region-talk.vector-fusion.v1",
        "dependencies": ["e5_embedding", "bge_m3_embedding"],
        "max_attempts": 3,
        "timeout_seconds": 300,
    },
    {
        "stage": "image_scoring",
        "contract_version": "region-talk.image-diagnostic.v1",
        "dependencies": ["vector_fusion"],
        "max_attempts": 3,
        "timeout_seconds": 1200,
    },
    {
        "stage": "final_verifier",
        "contract_version": "region-talk.final-verifier.v1",
        "dependencies": ["image_scoring"],
        "max_attempts": 3,
        "timeout_seconds": 600,
    },
    {
        "stage": "writer",
        "contract_version": "region-talk.writer.v1",
        "dependencies": ["final_verifier"],
        "max_attempts": 3,
        "timeout_seconds": 900,
    },
    {
        "stage": "review_queue",
        "contract_version": "region-talk.review-queue.v1",
        "dependencies": ["writer"],
        "max_attempts": 3,
        "timeout_seconds": 300,
    },
]


class MemoryReader:
    def __init__(self, rows: dict[str, list[dict[str, Any]]]) -> None:
        self.rows = rows

    def run_snapshot_pass(self, _phase, callback):  # type: ignore[no-untyped-def]
        return callback()

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
        with psycopg.connect(admin_url) as connection:
            assert connection.execute(
                "SELECT status FROM orchestration.pipeline "
                "WHERE workload='region-talk' AND name='region-talk-main' AND version='1.0.0'"
            ).fetchone() == ("paused",)
            assert connection.execute(
                "SELECT enabled FROM orchestration.pipeline_stage stage "
                "JOIN orchestration.pipeline pipeline USING(pipeline_id) "
                "WHERE pipeline.workload='region-talk' AND pipeline.name='region-talk-main' "
                "AND stage.stage_key='source_discovery' AND stage.stage_version='v1'"
            ).fetchone() == (True,)
            assert connection.execute(
                "SELECT enabled FROM orchestration.pipeline_stage stage "
                "JOIN orchestration.pipeline pipeline USING(pipeline_id) "
                "WHERE pipeline.workload='region-talk' AND pipeline.name='region-talk-main' "
                "AND stage.stage_key='publication_dispatch' AND stage.stage_version='v1'"
            ).fetchone() == (False,)
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

        # The same fixed pipeline credential can form durable post-import work
        # without caller-selected SQL/tables or any publication side effect.
        stage_run_id = uuid5(
            STAGE_NAMESPACE,
            f"region-talk-stage-run:{manifest.task_run_id}:{manifest.export_batch_id}",
        )
        prepare_request = {
            "schema_version": "region-talk-post-import-stage-request.v1",
            "operation": "prepare",
            "stage_run_id": str(stage_run_id),
            "task_run_id": str(manifest.task_run_id),
            "export_batch_id": str(manifest.export_batch_id),
            "ordered_stages": POST_IMPORT_DAG,
            "requested_at": now.isoformat(),
            "publication_dispatch": False,
            "notification_dispatch": False,
        }
        with psycopg.connect(role_url) as connection:
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            preparation = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            assert preparation["status"] == "PREPARED"
            assert preparation["publication_dispatch"] is False
            assert preparation["notification_dispatch"] is False
            assert len(preparation["candidates"]) == 1
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            assert (
                connection.execute(
                    "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                    (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
                ).fetchone()[0]
                == preparation
            )
            connection.commit()
            commit_request = form_stage_commit(
                StagePreparation.model_validate(preparation), now=now
            ).model_dump(mode="json")
            for digest_field in ("input_sha256", "output_sha256", "receipt_sha256"):
                tampered = deepcopy(commit_request)
                tampered["stage_receipts"][0][digest_field] = "0" * 64
                connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                with pytest.raises(
                    psycopg.errors.InvalidParameterValue,
                    match="stage receipt hash verification failed",
                ):
                    connection.execute(
                        "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                        (manifest.task_run_id, manifest.export_batch_id, json.dumps(tampered)),
                    )
                connection.rollback()
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            stage_receipt = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(commit_request)),
            ).fetchone()[0]
            connection.commit()
            assert stage_receipt["status"] == "WAITING_WORK"
            assert stage_receipt["queue_count"] == 1
            assert stage_receipt["work_request_count"] == 2
            assert stage_receipt["publication_dispatch"] is False
            assert stage_receipt["notification_dispatch"] is False

            metadata_claim_request = {
                "schema_version": "region-talk-stage-work-metadata-claim.v2",
                "claim_request_id": "89898989-8989-4989-8989-898989898989",
                "lease_owner": "disposable-postgres-supervisor",
                "requested_at": now.isoformat(),
                "publication_dispatch": False,
                "notification_dispatch": False,
            }
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            claim = connection.execute(
                "SELECT migration.claim_region_talk_stage_work_metadata(%s,%s,%s::jsonb)",
                (
                    manifest.task_run_id,
                    manifest.export_batch_id,
                    json.dumps(metadata_claim_request),
                ),
            ).fetchone()[0]
            assert claim["status"] == "CLAIMED"
            assert claim["master_instance_id"] == str(IDENTITY.master_instance_id)
            assert claim["epoch"] == 1
            serialized_claim = json.dumps(claim)
            for forbidden in (
                '"payload"',
                '"lease_token"',
                "canonical_url",
                "canonical_source_key",
                "input_data",
                "upstream_results",
            ):
                assert forbidden not in serialized_claim
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            assert connection.execute(
                "SELECT migration.claim_region_talk_stage_work_metadata(%s,%s,%s::jsonb)",
                (
                    manifest.task_run_id,
                    manifest.export_batch_id,
                    json.dumps(metadata_claim_request),
                ),
            ).fetchone()[0] == claim
            connection.commit()
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(
                    "SELECT migration.claim_region_talk_stage_work(%s,%s,%s::jsonb)",
                    (manifest.task_run_id, manifest.export_batch_id, "{}"),
                )
            connection.rollback()

            worker_principal = "mdh_e1_regiong9_abababab"
            worker_password = "region-talk-worker-password-long-enough"
            worker_credential = UUID("abababab-abab-4bab-8bab-abababababab")
            worker_task = UUID(claim["worker_task_run_id"])
            with psycopg.connect(admin_url) as worker_admin:
                gate = DatabaseGate(worker_admin)
                CredentialProvisioner(worker_admin, gate).create(
                    principal=worker_principal,
                    password=worker_password,
                    group="mdh_region_talk_pipeline",
                    identity=IDENTITY,
                    credential_id=worker_credential,
                    expires_at=now + timedelta(minutes=9),
                    now=now,
                )
                worker_admin.execute(
                    "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                    (
                        worker_credential,
                        worker_principal,
                        "region_talk",
                        worker_task,
                        2,
                        IDENTITY.master_instance_id,
                        1,
                        "7" * 64,
                        "8" * 64,
                    ),
                )
                worker_admin.commit()
            bind_request = {
                "schema_version": "region-talk-stage-worker-bind.v1",
                "dispatch_id": claim["dispatch_id"],
                "effect_id": claim["effect_id"],
                "claim_receipt_sha256": claim["claim_receipt_sha256"],
                "worker_task_run_id": str(worker_task),
                "worker_credential_id": str(worker_credential),
                "worker_generation": 2,
                "worker_command_sha256": "7" * 64,
                "worker_task_token_sha256": "8" * 64,
                "requested_at": now.isoformat(),
                "publication_dispatch": False,
                "notification_dispatch": False,
            }
            wrong_generation = {**bind_request, "worker_generation": 3}
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            with pytest.raises(psycopg.errors.NoDataFound):
                connection.execute(
                    "SELECT migration.bind_region_talk_stage_worker(%s,%s,%s::jsonb)",
                    (
                        manifest.task_run_id,
                        manifest.export_batch_id,
                        json.dumps(wrong_generation),
                    ),
                )
            connection.rollback()
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            binding = connection.execute(
                "SELECT migration.bind_region_talk_stage_worker(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(bind_request)),
            ).fetchone()[0]
            connection.commit()

            fetch_request = {
                "schema_version": "region-talk-stage-work-payload-fetch.v1",
                "worker_task_run_id": str(worker_task),
                "dispatch_id": claim["dispatch_id"],
                "effect_id": claim["effect_id"],
                "worker_binding_sha256": binding["worker_binding_sha256"],
                "requested_at": now.isoformat(),
                "publication_dispatch": False,
                "notification_dispatch": False,
            }
            worker_url = (
                f"postgresql://{worker_principal}:{worker_password}@127.0.0.1:{port}/postgres"
            )
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            with pytest.raises(
                psycopg.errors.InsufficientPrivilege,
                match="not registered for exact task",
            ):
                connection.execute(
                    "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(fetch_request)),
                )
            connection.rollback()
            with psycopg.connect(worker_url) as worker_connection:
                wrong_binding = {**fetch_request, "worker_binding_sha256": "0" * 64}
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                with pytest.raises(psycopg.errors.NoDataFound):
                    worker_connection.execute(
                        "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                        (worker_task, UUID(claim["effect_id"]), json.dumps(wrong_binding)),
                    )
                worker_connection.rollback()
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                payload_receipt = worker_connection.execute(
                    "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(fetch_request)),
                ).fetchone()[0]
                payload = payload_receipt["payload"]
                assert payload["input_fingerprint"] == claim["input_fingerprint"]
                result_metadata = {
                    "schema_version": "region-talk-stage-result-metadata.v1",
                    "stage": claim["stage"],
                    "contract_version": claim["contract_version"],
                    "subject_type": claim["subject_type"],
                    "subject_id": claim["subject_id"],
                    "candidate_revision": payload["candidate_revision"],
                    "revision_fingerprint": payload["revision_fingerprint"],
                    "input_fingerprint": claim["input_fingerprint"],
                    "producer_exact_id": "disposable-proof.v2",
                    "metrics": {},
                    "artifact_sha256": None,
                }
                worker_result = {
                    "schema_version": "region-talk-stage-worker-direct-result.v1",
                    "worker_task_run_id": str(worker_task),
                    "dispatch_id": claim["dispatch_id"],
                    "effect_id": claim["effect_id"],
                    "worker_binding_sha256": binding["worker_binding_sha256"],
                    "work_item_id": claim["work_item_id"],
                    "attempt": claim["attempt"],
                    "result_status": "SUCCEEDED",
                    "result_metadata": result_metadata,
                    "metadata_sha256": sha256_value(result_metadata),
                    "result_sha256": "a" * 64,
                    "completed_at": now.isoformat(),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                result_receipt = worker_connection.execute(
                    "SELECT migration.submit_region_talk_stage_worker_result(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(worker_result)),
                ).fetchone()[0]
                assert result_receipt["accepted"] is True
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                assert worker_connection.execute(
                    "SELECT migration.submit_region_talk_stage_worker_result(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(worker_result)),
                ).fetchone()[0] == result_receipt
                worker_connection.commit()

            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            refreshed = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            current_candidate = refreshed["candidates"][0]
            assert current_candidate["evidence"][claim["stage"]]["status"] == "CURRENT"
            cycle_request = form_stage_commit(
                StagePreparation.model_validate(refreshed), now=now
            ).model_dump(mode="json")
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            cycle_receipt = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(cycle_request)),
            ).fetchone()[0]
            assert cycle_receipt["status"] == "WAITING_WORK"
            status_request = {
                "schema_version": "region-talk-stage-supervisor-status-request.v1",
                "requested_at": now.isoformat(),
            }
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            stage_status = connection.execute(
                "SELECT migration.region_talk_stage_supervisor_status(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(status_request)),
            ).fetchone()[0]
            assert stage_status["status"] == "WAITING_WORK"
            assert stage_status["items"][0]["dispatch_id"] == claim["dispatch_id"]
            assert "payload" not in json.dumps(stage_status)
            connection.commit()

        with psycopg.connect(admin_url) as connection:
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (1,)
            assert connection.execute(
                "SELECT count(*) FROM migration.region_talk_canonical_apply_receipt"
            ).fetchone() == (1,)
            assert connection.execute("SELECT count(*) FROM hub.content_item").fetchone()[0] >= 4
            assert connection.execute("SELECT count(*) FROM region_talk.source").fetchone()[0] >= 3
            assert connection.execute("SELECT count(*) FROM orchestration.work_item").fetchone()[0] >= 1
            assert connection.execute(
                "SELECT status FROM region_talk.source WHERE evidence ? 'current_status_raw_record_id'"
            ).fetchone() == ("active",)
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
            assert connection.execute(
                "SELECT status,priority FROM orchestration.work_item "
                "WHERE subject_type='region_talk.source'"
            ).fetchone() == ("succeeded", 5)
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

        # An exact-payload observation with an older source timestamp advances
        # snapshot evidence without moving the current-state clock backwards.
        replay_rows = deepcopy(second_rows)
        replay_at = now - timedelta(minutes=1)
        replay_compact = {row["pk"]: row for row in replay_rows["region_talk_compact_state_kv"]}
        replay_compact["source-status-1"]["updated_at"] = replay_at
        replay_principal = "mdh_e1_regiong4_aaaaaaaa"
        replay_password = "region-talk-replay-password-long-enough"
        replay_credential = UUID("44444444-4444-4444-8444-444444444444")
        replay_task = UUID("45454545-4545-4545-8545-454545454545")
        with psycopg.connect(admin_url) as connection:
            gate = DatabaseGate(connection)
            CredentialProvisioner(connection, gate).create(
                principal=replay_principal,
                password=replay_password,
                group="mdh_region_talk_pipeline",
                identity=IDENTITY,
                credential_id=replay_credential,
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
            connection.execute(
                "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    replay_credential,
                    replay_principal,
                    "region_talk",
                    replay_task,
                    1,
                    IDENTITY.master_instance_id,
                    1,
                    "c" * 64,
                    "d" * 64,
                ),
            )
            connection.commit()
        with psycopg.connect(
            f"postgresql://{replay_principal}:{replay_password}@127.0.0.1:{port}/postgres"
        ) as connection:
            runner = DirectSnapshotRunner(MemoryReader(replay_rows), connection, page_size=3)
            replay_manifest = runner.inventory(
                export_batch_id=UUID("45454545-4545-4545-8545-454545454546"),
                task_run_id=replay_task,
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="e" * 64,
                created_at=now,
            )
            assert runner.run(replay_manifest).status == "complete"
        with psycopg.connect(admin_url) as connection:
            replay_raw_id = connection.execute(
                "SELECT raw_record_id FROM migration.raw_record "
                "WHERE export_batch_id=%s AND row_kind='source_status_item'",
                (replay_manifest.export_batch_id,),
            ).fetchone()[0]
            head = connection.execute(
                "SELECT target_id,payload_sha256,source_updated_at,canonical_revision,"
                "export_batch_id,raw_record_id,updated_at "
                "FROM migration.region_talk_canonical_state_head "
                "WHERE identity_kind='source_status' "
                "AND identity_key='region_talk_compact_state_kv:source-status-1'"
            ).fetchone()
            # The YDB fixture includes updated_at in its raw JSON, so changing
            # that timestamp also changes the landed payload hash. Exercise the
            # exact-payload replay branch directly with the accepted raw identity.
            assert connection.execute(
                "SELECT migration.region_talk_claim_canonical_state("
                "'source_status','region_talk_compact_state_kv:source-status-1',"
                "'region_talk.source',%s,%s,%s,%s,%s,3)",
                (
                    head[0],
                    replay_raw_id,
                    replay_manifest.export_batch_id,
                    replay_at,
                    head[1],
                ),
            ).fetchone() == (False,)
            assert connection.execute(
                "SELECT target_id,payload_sha256,source_updated_at,canonical_revision,"
                "export_batch_id,raw_record_id,updated_at "
                "FROM migration.region_talk_canonical_state_head "
                "WHERE identity_kind='source_status' "
                "AND identity_key='region_talk_compact_state_kv:source-status-1'"
            ).fetchone() == head
            assert connection.execute(
                "SELECT disposition FROM migration.region_talk_canonical_state_observation "
                "WHERE export_batch_id=%s AND identity_kind='source_status'",
                (replay_manifest.export_batch_id,),
            ).fetchone() == ("stale",)
            connection.commit()

        # A later accepted snapshot with a changed but older status payload is
        # retained as stale evidence and cannot overwrite the paused head.
        stale_rows = deepcopy(second_rows)
        stale_compact = {row["pk"]: row for row in stale_rows["region_talk_compact_state_kv"]}
        stale_compact["source-status-1"]["updated_at"] = now
        stale_compact["source-status-1"]["payload_json"].update({"status": "active", "reason": "stale_changed_payload"})
        stale_principal = "mdh_e1_regiong5_bbbbbbbb"
        stale_password = "region-talk-stale-password-long-enough"
        stale_credential = UUID("55555555-5555-4555-8555-555555555555")
        stale_task = UUID("56565656-5656-4565-8565-565656565656")
        with psycopg.connect(admin_url) as connection:
            gate = DatabaseGate(connection)
            CredentialProvisioner(connection, gate).create(
                principal=stale_principal,
                password=stale_password,
                group="mdh_region_talk_pipeline",
                identity=IDENTITY,
                credential_id=stale_credential,
                expires_at=now + timedelta(minutes=9),
                now=now,
            )
            connection.execute(
                "SELECT master_control.register_task_credential_binding(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                (
                    stale_credential,
                    stale_principal,
                    "region_talk",
                    stale_task,
                    1,
                    IDENTITY.master_instance_id,
                    1,
                    "1" * 64,
                    "2" * 64,
                ),
            )
            connection.commit()
        with psycopg.connect(
            f"postgresql://{stale_principal}:{stale_password}@127.0.0.1:{port}/postgres"
        ) as connection:
            runner = DirectSnapshotRunner(MemoryReader(stale_rows), connection, page_size=3)
            stale_manifest = runner.inventory(
                export_batch_id=UUID("56565656-5656-4565-8565-565656565657"),
                task_run_id=stale_task,
                master_instance_id=IDENTITY.master_instance_id,
                master_epoch=1,
                source_database="fixture",
                request_sha256="3" * 64,
                created_at=now,
            )
            assert runner.run(stale_manifest).status == "complete"
        with psycopg.connect(admin_url) as connection:
            assert connection.execute(
                "SELECT status FROM region_talk.source WHERE evidence->>'current_status_export_batch_id'=%s",
                (str(second_manifest.export_batch_id),),
            ).fetchone() == ("paused",)
            assert connection.execute("SELECT count(*) FROM region_talk.source_status").fetchone() == (2,)
            assert connection.execute(
                "SELECT source_updated_at FROM migration.region_talk_canonical_state_head "
                "WHERE identity_kind='source_status' "
                "AND identity_key='region_talk_compact_state_kv:source-status-1'"
            ).fetchone() == (changed_at,)
            assert connection.execute(
                "SELECT disposition FROM migration.region_talk_canonical_state_observation "
                "WHERE export_batch_id=%s AND identity_kind='source_status'",
                (stale_manifest.export_batch_id,),
            ).fetchone() == ("stale",)

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
                runner._scan(
                    spec,
                    phase="pass_b",
                    land_batch_id=manifest.export_batch_id,
                    task_run_id=manifest.task_run_id,
                ).receipt()
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
            assert connection.execute("SELECT canonical_revision FROM hub.canonical_state").fetchone() == (4,)
    finally:
        subprocess.run(["docker", "rm", "--force", container], check=False, capture_output=True)
