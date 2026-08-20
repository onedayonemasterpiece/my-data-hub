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
from hashlib import sha256
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
from my_data_hub.workloads.region_talk.heavy_contracts import (
    FinalVerifierInput,
    FinalVerifierResult,
    ImageScoringInput,
    ImageScoringResult,
    WriterInput,
    WriterResult,
    canonical_sha256,
    sha256_text,
)
from my_data_hub.workloads.region_talk.heavy_dag_bridge import (
    DagFinalVerifierWorkInput,
    DagImageWorkInput,
    DagWriterWorkInput,
    final_guard_metrics,
    image_guard_metrics,
    writer_guard_metrics,
)
from my_data_hub.workloads.region_talk.heavy_wiring import (
    HeavyStageInputReceipt,
    HeavyStagePrivateResult,
)
from my_data_hub.workloads.region_talk.stage_execution import StagePreparation, form_stage_commit
from my_data_hub.workloads.region_talk.transforms.evidence import SEMANTIC_BANK_HASH

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"
IDENTITY = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "fixture-run", 1)
PRINCIPAL = "mdh_e1_regiong1_deadbeef"
PASSWORD = "region-talk-fixture-password-long-enough"
STAGE_NAMESPACE = UUID("54a0dba7-1e4b-4d56-a143-173304989e85")
RUNTIME_IMAGE = "region-talk-runtime@sha256:" + "8" * 64
RUNTIME_COMMIT = "7" * 40
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


def _runtime_pin_request(stage: Mapping[str, Any], now: datetime) -> dict[str, Any]:
    is_text = stage["stage"] in {"e5_embedding", "bge_m3_embedding"}
    return {
        "schema_version": "region-talk-stage-runtime-pin.v1",
        "stage": stage["stage"],
        "contract_version": stage["contract_version"],
        "model_id": "fixture/" + stage["stage"],
        "model_revision": "1" * 40,
        "encoder_contract": "fixture-exact.v1",
        "semantic_bank_version": "semantic_bank_v1" if is_text else None,
        "semantic_bank_sha256": SEMANTIC_BANK_HASH if is_text else None,
        "runtime_source_sha256": "2" * 64,
        "asset_manifest_sha256": "3" * 64,
        "provider_image_identity": RUNTIME_IMAGE,
        "provider_image_source_commit": RUNTIME_COMMIT,
        "effective_canonical_revision": 1,
        "master_instance_id": str(IDENTITY.master_instance_id),
        "epoch": 1,
        "prior_pin_receipt_sha256": None,
        "requested_at": now.isoformat(),
        "publication_dispatch": False,
        "notification_dispatch": False,
    }


def _pinned_metrics(
    stage: str,
    contract: str,
    pin: Mapping[str, Any],
    input_data: Mapping[str, Any],
) -> dict[str, Any]:
    common = {
        "model_id": pin["model_id"],
        "model_revision": pin["model_revision"],
        "encoder_contract": pin["encoder_contract"],
        "asset_manifest_sha256": pin["asset_manifest_sha256"],
        "runtime_source_sha256": pin["runtime_source_sha256"],
        "provider_image_identity": pin["provider_image_identity"],
        "provider_image_source_commit": pin["provider_image_source_commit"],
        "pin_sha256": pin["pin_sha256"],
    }
    if stage in {"e5_embedding", "bge_m3_embedding"}:
        scores = {"ko_visit_impression": 0.8, "news_report": 0.2}
        evidence = sha256_value(
            {
                "contract_version": contract,
                "model_id": pin["model_id"],
                "text_hash": input_data["text_sha256"],
                "semantic_bank_version": pin["semantic_bank_version"],
                "semantic_bank_hash": pin["semantic_bank_sha256"],
                "scores": scores,
            }
        )
        return {
            **common,
            "text_sha256": input_data["text_sha256"],
            "semantic_bank_version": pin["semantic_bank_version"],
            "semantic_bank_hash": pin["semantic_bank_sha256"],
            "evidence_fingerprint": evidence,
            "scores": scores,
        }
    raise AssertionError(stage)


def _later_stage_metrics(
    stage: str, pin: Mapping[str, Any] | None, input_data: Mapping[str, Any]
) -> tuple[str, dict[str, Any]]:
    if stage == "vector_fusion":
        return "my-data-hub:vector_fusion@region-talk.vector-fusion.v1", {
            "contract_version": "region-talk.vector-fusion.v1",
            "status": "fused_e5_bge_m3",
            "reasons": [],
            "evidence_fingerprint": "6" * 64,
            "scores_by_model": {
                "fixture/e5_embedding": {"ko_visit_impression": 0.8, "news_report": 0.2},
                "fixture/bge_m3_embedding": {"ko_visit_impression": 0.8, "news_report": 0.2},
            },
            "fused_scores": {"ko_visit_impression": 0.8, "news_report": 0.2},
            "positive_class": "ko_visit_impression",
            "positive_score": 0.8,
            "negative_class": "news_report",
            "negative_score": 0.2,
            "margin": 0.6,
        }
    assert pin is not None
    common = {
        "model_id": pin["model_id"],
        "model_revision": pin["model_revision"],
        "encoder_contract": pin["encoder_contract"],
        "asset_manifest_sha256": pin["asset_manifest_sha256"],
        "runtime_source_sha256": pin["runtime_source_sha256"],
        "provider_image_identity": pin["provider_image_identity"],
        "provider_image_source_commit": pin["provider_image_source_commit"],
        "pin_sha256": pin["pin_sha256"],
    }
    if stage == "image_scoring":
        return pin["producer_exact_id"], {
            **common,
            "schema_version": "region-talk.image-diagnostic-result.v1",
            "decision": "accept",
            "actual_image": True,
            "postcard_score": 0.9,
            "input_artifact_sha256": input_data["artifact_sha256"],
        }
    if stage == "final_verifier":
        return pin["producer_exact_id"], {
            **common,
            "schema_version": "region-talk.final-verifier-result.v1",
            "decision": "PASS",
            "reason_codes": [],
            "vector_result_sha256": input_data["vector_result_sha256"],
            "image_result_sha256": input_data["image_result_sha256"],
        }
    if stage == "writer":
        return pin["producer_exact_id"], {
            **common,
            "schema_version": "region-talk.writer-result.v1",
            "draft_sha256": "a" * 64,
            "title_sha256": "b" * 64,
            "body_sha256": "c" * 64,
            "character_count": 400,
            "final_result_sha256": input_data["final_result_sha256"],
        }
    raise AssertionError(stage)


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

            # Owner/master registration freezes exact runtimes at the accepted
            # canonical revision; workers cannot invent producer identities.
            pins: dict[str, dict[str, Any]] = {}
            with psycopg.connect(admin_url) as owner:
                _candidate_id, content_id, candidate_revision = owner.execute(
                    "SELECT candidate_id,content_id,current_revision "
                    "FROM region_talk.publication_candidate"
                ).fetchone()
                asset_id = owner.execute(
                    "INSERT INTO hub.content_asset(content_id,asset_type,source_url,normalized_url,"
                    "source_external_id,position,mime_type,byte_size,sha256,status,metadata) "
                        "VALUES(%s,'image','https://EXAMPLE.test/private-image.jpg?utm_source=mutable',"
                    "'https://example.test/private-image.jpg','media-1',0,'image/jpeg',1234,%s,"
                    "'available',%s::jsonb) RETURNING asset_id",
                    (
                        content_id,
                        "4" * 64,
                        json.dumps(
                            {
                                "artifact_manifest": {
                                    "schema_version": "region-talk-media-artifact-manifest.v1",
                                    "candidate_revision": candidate_revision,
                                    "normalized_source_url": (
                                        "https://example.test/private-image.jpg"
                                    ),
                                    "source_media_id": "media-1",
                                    "object_ref": "artifacts/media-1.jpg",
                                    "artifact_sha256": "4" * 64,
                                    "byte_size": 1234,
                                    "content_type": "image/jpeg",
                                    "acquisition_receipt_sha256": "5" * 64,
                                    "task_readable": True,
                                    "publication_dispatch": False,
                                    "notification_dispatch": False,
                                }
                            }
                        ),
                    ),
                ).fetchone()[0]
                for stage in POST_IMPORT_DAG:
                    if stage["stage"] not in {
                        "e5_embedding",
                        "bge_m3_embedding",
                        "image_scoring",
                        "final_verifier",
                        "writer",
                    }:
                        continue
                    request = _runtime_pin_request(stage, now)
                    receipt = owner.execute(
                        "SELECT migration.register_region_talk_stage_runtime_pin(%s::jsonb)",
                        (json.dumps(request),),
                    ).fetchone()[0]
                    assert owner.execute(
                        "SELECT migration.register_region_talk_stage_runtime_pin(%s::jsonb)",
                        (json.dumps({**request, "requested_at": (now + timedelta(seconds=1)).isoformat()}),),
                    ).fetchone()[0] == receipt
                    pins[stage["stage"]] = receipt
                owner.commit()

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
            connection.commit()
            with psycopg.connect(admin_url) as proof:
                assert proof.execute(
                    "SELECT array_agg(stage ORDER BY stage),count(*) "
                    "FROM migration.region_talk_stage_work_input_v9"
                ).fetchone() == (["bge_m3_embedding", "e5_embedding"], 2)
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
                worker_connection.commit()

            rotated_principal = "mdh_e1_regiong8_acacacac"
            rotated_password = "region-talk-rotated-password-long-enough"
            rotated_credential = UUID("acacacac-acac-4cac-8cac-acacacacacac")
            cross_principal = "mdh_e1_regiong7_adadadad"
            cross_password = "region-talk-cross-worker-password-long-enough"
            cross_credential = UUID("adadadad-adad-4dad-8dad-adadadadadad")
            cross_task = UUID("aeaeaeae-aeae-4eae-8eae-aeaeaeaeaeae")
            rotated_supervisor_principal = "mdh_e1_regiong6_afafafaf"
            rotated_supervisor_password = "region-talk-supervisor-rotation-password"
            rotated_supervisor_credential = UUID("afafafaf-afaf-4faf-8faf-afafafafafaf")
            with psycopg.connect(admin_url) as worker_admin:
                gate = DatabaseGate(worker_admin)
                for principal, password, credential, registered_task, generation in (
                    (rotated_principal, rotated_password, rotated_credential, worker_task, 3),
                    (cross_principal, cross_password, cross_credential, cross_task, 1),
                    (
                        rotated_supervisor_principal,
                        rotated_supervisor_password,
                        rotated_supervisor_credential,
                        manifest.task_run_id,
                        2,
                    ),
                ):
                    CredentialProvisioner(worker_admin, gate).create(
                        principal=principal,
                        password=password,
                        group="mdh_region_talk_pipeline",
                        identity=IDENTITY,
                        credential_id=credential,
                        expires_at=now + timedelta(minutes=9),
                        now=now,
                    )
                    worker_admin.execute(
                        "SELECT master_control.register_task_credential_binding("
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            credential,
                            principal,
                            "region_talk",
                            registered_task,
                            generation,
                            IDENTITY.master_instance_id,
                            1,
                            "9" * 64,
                            "a" * 64,
                        ),
                    )
                worker_admin.commit()
            rotation_request = {
                "schema_version": "region-talk-stage-worker-rotate.v1",
                "dispatch_id": claim["dispatch_id"],
                "effect_id": claim["effect_id"],
                "work_item_id": claim["work_item_id"],
                "worker_task_run_id": str(worker_task),
                "prior_worker_generation": 2,
                "prior_worker_binding_sha256": binding["worker_binding_sha256"],
                "new_worker_credential_id": str(rotated_credential),
                "new_worker_generation": 3,
                "new_worker_command_sha256": "9" * 64,
                "new_worker_task_token_sha256": "a" * 64,
                "requested_at": now.isoformat(),
                "publication_dispatch": False,
                "notification_dispatch": False,
            }
            rotated_supervisor_url = (
                f"postgresql://{rotated_supervisor_principal}:{rotated_supervisor_password}"
                f"@127.0.0.1:{port}/postgres"
            )
            with psycopg.connect(rotated_supervisor_url) as rotated_supervisor:
                rotated_supervisor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                rotated = rotated_supervisor.execute(
                    "SELECT migration.rotate_region_talk_stage_worker_credential(%s,%s,%s::jsonb)",
                    (
                        manifest.task_run_id,
                        manifest.export_batch_id,
                        json.dumps(rotation_request),
                    ),
                ).fetchone()[0]
                assert rotated["worker_generation"] == 3
                rotated_supervisor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                assert rotated_supervisor.execute(
                    "SELECT migration.rotate_region_talk_stage_worker_credential(%s,%s,%s::jsonb)",
                    (
                        manifest.task_run_id,
                        manifest.export_batch_id,
                        json.dumps(rotation_request),
                    ),
                ).fetchone()[0] == rotated
                rotated_supervisor.commit()

            with psycopg.connect(admin_url) as worker_admin:
                assert worker_admin.execute(
                    "SELECT worker_generation,binding_status "
                    "FROM migration.region_talk_stage_worker_generation_status_v1 "
                    "WHERE dispatch_id=%s ORDER BY worker_generation",
                    (UUID(claim["dispatch_id"]),),
                ).fetchall() == [(2, "FENCED"), (3, "ACTIVE")]
                assert worker_admin.execute(
                    "SELECT schema_revision FROM hub.canonical_state"
                    ).fetchone() == (33,)

            # Generation one was valid before rotation and is now fenced.
            with psycopg.connect(worker_url) as worker_connection:
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                with pytest.raises(psycopg.errors.NoDataFound):
                    worker_connection.execute(
                        "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                        (worker_task, UUID(claim["effect_id"]), json.dumps(fetch_request)),
                    )

            rotated_url = (
                f"postgresql://{rotated_principal}:{rotated_password}@127.0.0.1:{port}/postgres"
            )
            rotated_fetch = {
                **fetch_request,
                "worker_binding_sha256": rotated["worker_binding_sha256"],
            }
            cross_url = (
                f"postgresql://{cross_principal}:{cross_password}@127.0.0.1:{port}/postgres"
            )
            cross_fetch = {**rotated_fetch, "worker_task_run_id": str(cross_task)}
            with psycopg.connect(cross_url) as cross_connection:
                cross_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                with pytest.raises(psycopg.errors.NoDataFound):
                    cross_connection.execute(
                        "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                        (cross_task, UUID(claim["effect_id"]), json.dumps(cross_fetch)),
                    )

            with psycopg.connect(rotated_url) as worker_connection:
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                rotated_payload = worker_connection.execute(
                    "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(rotated_fetch)),
                ).fetchone()[0]
                assert rotated_payload["payload"] == payload
                result_metadata = {
                    "schema_version": "region-talk-stage-result-metadata.v1",
                    "stage": claim["stage"],
                    "contract_version": claim["contract_version"],
                    "subject_type": claim["subject_type"],
                    "subject_id": claim["subject_id"],
                    "candidate_revision": payload["candidate_revision"],
                    "revision_fingerprint": payload["revision_fingerprint"],
                    "input_fingerprint": claim["input_fingerprint"],
                    "producer_exact_id": pins[claim["stage"]]["producer_exact_id"],
                    "metrics": _pinned_metrics(
                        claim["stage"],
                        claim["contract_version"],
                        pins[claim["stage"]],
                        payload["input_data"],
                    ),
                    "artifact_sha256": None,
                }
                worker_result = {
                    "schema_version": "region-talk-stage-worker-direct-result.v1",
                    "worker_task_run_id": str(worker_task),
                    "dispatch_id": claim["dispatch_id"],
                    "effect_id": claim["effect_id"],
                    "worker_binding_sha256": rotated["worker_binding_sha256"],
                    "work_item_id": claim["work_item_id"],
                    "attempt": claim["attempt"],
                    "result_status": "SUCCEEDED",
                    "result_metadata": result_metadata,
                    "metadata_sha256": sha256_value(result_metadata),
                    "result_sha256": sha256_value(result_metadata),
                    "completed_at": now.isoformat(),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
                def combined_result(value: dict[str, Any]) -> dict[str, Any]:
                    return {
                        "schema_version": "region-talk-stage-worker-combined-result.v1",
                        "direct_result": value,
                        "private_result": None,
                        "publication_dispatch": False,
                        "notification_dispatch": False,
                    }
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                forged = deepcopy(worker_result)
                forged["result_metadata"]["producer_exact_id"] = "attacker@unregistered"
                forged["result_metadata"]["metrics"] = {}
                forged["metadata_sha256"] = sha256_value(forged["result_metadata"])
                forged["result_sha256"] = "a" * 64
                with pytest.raises(
                    psycopg.errors.InvalidParameterValue,
                    match="fails exact v9 stage validation",
                ):
                    worker_connection.execute(
                        "SELECT migration.submit_region_talk_heavy_stage_worker_result(%s,%s,%s::jsonb)",
                        (worker_task, UUID(claim["effect_id"]), json.dumps(combined_result(forged))),
                    )
                worker_connection.rollback()
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                result_receipt = worker_connection.execute(
                    "SELECT migration.submit_region_talk_heavy_stage_worker_result(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(combined_result(worker_result))),
                ).fetchone()[0]
                assert result_receipt["accepted"] is True
                worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                assert worker_connection.execute(
                    "SELECT migration.submit_region_talk_heavy_stage_worker_result(%s,%s,%s::jsonb)",
                    (worker_task, UUID(claim["effect_id"]), json.dumps(combined_result(worker_result))),
                ).fetchone()[0] == result_receipt
                worker_connection.commit()
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            refreshed = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            current_candidate = refreshed["candidates"][0]
            assert current_candidate["evidence"][claim["stage"]]["status"] == "CURRENT"
            connection.commit()

            # Superseding the active runtime pin changes the immutable stage
            # input/work identity.  The generation-one success immediately
            # becomes stale and cannot be replayed as a current success.
            superseded_stage = claim["stage"]
            prior_pin = pins[superseded_stage]
            stage_contract = next(
                stage for stage in POST_IMPORT_DAG if stage["stage"] == superseded_stage
            )
            replacement_request = {
                **_runtime_pin_request(stage_contract, now + timedelta(seconds=2)),
                "runtime_source_sha256": "9" * 64,
                "asset_manifest_sha256": "a" * 64,
                "prior_pin_receipt_sha256": prior_pin["receipt_sha256"],
            }
            with psycopg.connect(admin_url) as owner:
                replacement_pin = owner.execute(
                    "SELECT migration.register_region_talk_stage_runtime_pin(%s::jsonb)",
                    (json.dumps(replacement_request),),
                ).fetchone()[0]
                replay_request = {
                    **replacement_request,
                    "requested_at": (now + timedelta(seconds=3)).isoformat(),
                }
                assert owner.execute(
                    "SELECT migration.register_region_talk_stage_runtime_pin(%s::jsonb)",
                    (json.dumps(replay_request),),
                ).fetchone()[0] == replacement_pin
                owner.commit()
            assert replacement_pin["pin_generation"] == prior_pin["pin_generation"] + 1
            pins[superseded_stage] = replacement_pin

            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            superseded = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            assert superseded["candidates"][0]["evidence"][superseded_stage]["status"] == (
                "MISSING"
            )
            connection.commit()
            with psycopg.connect(admin_url) as proof:
                work_rows = proof.execute(
                    "SELECT work_item_id,input_fingerprint,input_data->'runtime_pin' "
                    "FROM migration.region_talk_stage_work_input_v9 WHERE stage=%s "
                    "ORDER BY created_at",
                    (superseded_stage,),
                ).fetchall()
                assert len(work_rows) == 2
                assert len({row[0] for row in work_rows}) == 2
                assert len({row[1] for row in work_rows}) == 2
                assert work_rows[-1][2] == replacement_pin
                assert proof.execute(
                    "SELECT migration.region_talk_stage_result_valid_v9("
                    "landed.stage,landed.contract_version,1,landed.master_instance_id,"
                    "landed.epoch,input.input_data,input.upstream_results,landed.result_status,"
                    "landed.result_metadata,landed.result_sha256) "
                    "FROM migration.region_talk_stage_worker_result landed "
                    "JOIN migration.region_talk_stage_work_input_v9 input USING(work_item_id) "
                    "WHERE landed.work_item_id=%s",
                    (UUID(claim["work_item_id"]),),
                ).fetchone() == (False,)
            with psycopg.connect(rotated_url) as stale_worker:
                stale_worker.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                with pytest.raises(
                    psycopg.errors.InvalidParameterValue,
                    match="fails exact v9 stage validation",
                ):
                    stale_worker.execute(
                        "SELECT migration.submit_region_talk_heavy_stage_worker_result(%s,%s,%s::jsonb)",
                        (worker_task, UUID(claim["effect_id"]), json.dumps(combined_result(worker_result))),
                    )
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            assert connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0] == superseded
            connection.commit()

            def land_exact_stage(stage: str) -> None:
                with psycopg.connect(admin_url) as admin:
                    row = admin.execute(
                        "SELECT input.work_item_id,input.contract_version,input.subject_id,"
                        "input.candidate_revision,input.revision_fingerprint,input.input_fingerprint,"
                        "input.input_data,input.upstream_results "
                        "FROM migration.region_talk_stage_work_input_v9 input "
                        "LEFT JOIN migration.region_talk_stage_worker_result landed "
                        "ON landed.work_item_id=input.work_item_id AND landed.result_status='SUCCEEDED' "
                        "WHERE input.stage=%s AND landed.work_item_id IS NULL "
                        "ORDER BY input.created_at DESC LIMIT 1",
                        (stage,),
                    ).fetchone()
                    assert row is not None, stage
                    (
                        work_id,
                        contract,
                        subject_id,
                        revision,
                        revision_fingerprint,
                        input_fingerprint,
                        input_data,
                        upstream_results,
                    ) = row
                    if stage in {"e5_embedding", "bge_m3_embedding"}:
                        producer = pins[stage]["producer_exact_id"]
                        metrics = _pinned_metrics(stage, contract, pins[stage], input_data)
                    else:
                        producer, metrics = _later_stage_metrics(stage, pins.get(stage), input_data)
                    metadata = {
                        "schema_version": "region-talk-stage-result-metadata.v1",
                        "stage": stage,
                        "contract_version": contract,
                        "subject_type": "region_talk.candidate",
                        "subject_id": str(subject_id),
                        "candidate_revision": revision,
                        "revision_fingerprint": revision_fingerprint,
                        "input_fingerprint": input_fingerprint,
                        "producer_exact_id": producer,
                        "metrics": metrics,
                        "artifact_sha256": None,
                    }
                    result_sha = sha256_value(metadata)
                    assert admin.execute(
                        "SELECT migration.region_talk_stage_result_valid_v9("
                        "%s,%s,1,%s,1,%s::jsonb,%s::jsonb,'SUCCEEDED',%s::jsonb,%s)",
                        (
                            stage,
                            contract,
                            IDENTITY.master_instance_id,
                            json.dumps(input_data),
                            json.dumps(upstream_results),
                            json.dumps(metadata),
                            result_sha,
                        ),
                    ).fetchone() == (True,)
                    effect_id = admin.execute(
                        "SELECT migration.region_talk_stage_uuid5(%s)",
                        (
                            f"region-talk-stage-effect:{work_id}:1:{input_fingerprint}",
                        ),
                    ).fetchone()[0]
                    admin.execute(
                        "INSERT INTO migration.region_talk_stage_worker_result("
                        "work_item_id,attempt,task_run_id,export_batch_id,stage_run_id,"
                        "master_instance_id,epoch,stage,contract_version,subject_type,subject_id,"
                        "candidate_revision,revision_fingerprint,input_fingerprint,effect_id,"
                        "result_status,result_metadata,metadata_sha256,result_sha256,completed_at) "
                        "VALUES(%s,1,%s,%s,%s,%s,1,%s,%s,'region_talk.candidate',%s,%s,%s,%s,%s,"
                        "'SUCCEEDED',%s::jsonb,%s,%s,%s)",
                        (
                            work_id,
                            manifest.task_run_id,
                            manifest.export_batch_id,
                            stage_run_id,
                            IDENTITY.master_instance_id,
                            stage,
                            contract,
                            subject_id,
                            revision,
                            revision_fingerprint,
                            input_fingerprint,
                            effect_id,
                            json.dumps(metadata),
                            result_sha,
                            result_sha,
                            now,
                        ),
                    )
                    admin.execute(
                        "UPDATE orchestration.work_item SET status='succeeded',attempt_count=1,"
                        "result_ref=jsonb_build_object('schema_version',"
                        "'region-talk-stage-result-ref.v1','attempt',1,'result_sha256',%s::text,"
                        "'metadata_sha256',%s::text) WHERE work_item_id=%s",
                        (result_sha, result_sha, work_id),
                    )
                    admin.commit()

            def land_heavy_stage_via_combined(stage: str) -> None:
                """Exercise the real child binding/fetch/private combined-submit path."""

                claim_request = {
                    "schema_version": "region-talk-stage-work-metadata-claim.v2",
                    "claim_request_id": str(
                        uuid5(STAGE_NAMESPACE, f"heavy-claim:{stage_run_id}:{stage}")
                    ),
                    "lease_owner": f"disposable-heavy-{stage}",
                    "requested_at": now.isoformat(),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
                connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                heavy_claim = connection.execute(
                    "SELECT migration.claim_region_talk_stage_work_metadata(%s,%s,%s::jsonb)",
                    (manifest.task_run_id, manifest.export_batch_id, json.dumps(claim_request)),
                ).fetchone()[0]
                assert heavy_claim["status"] == "CLAIMED"
                assert heavy_claim["stage"] == stage
                connection.commit()

                worker_task_id = UUID(heavy_claim["worker_task_run_id"])
                worker_credential_id = uuid5(STAGE_NAMESPACE, f"heavy-credential:{stage}")
                worker_principal = f"mdh_e1_regiong4_{sha256(stage.encode()).hexdigest()[:8]}"
                worker_password = f"region-talk-{stage}-worker-password"
                command_sha = sha256(f"command:{stage}".encode()).hexdigest()
                token_sha = sha256(f"token:{stage}".encode()).hexdigest()
                with psycopg.connect(admin_url) as worker_admin:
                    worker_gate = DatabaseGate(worker_admin)
                    CredentialProvisioner(worker_admin, worker_gate).create(
                        principal=worker_principal,
                        password=worker_password,
                        group="mdh_region_talk_pipeline",
                        identity=IDENTITY,
                        credential_id=worker_credential_id,
                        expires_at=now + timedelta(minutes=9),
                        now=now,
                    )
                    worker_admin.execute(
                        "SELECT master_control.register_task_credential_binding("
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                        (
                            worker_credential_id,
                            worker_principal,
                            "region_talk",
                            worker_task_id,
                            1,
                            IDENTITY.master_instance_id,
                            1,
                            command_sha,
                            token_sha,
                        ),
                    )
                    worker_admin.commit()
                bind_request = {
                    "schema_version": "region-talk-stage-worker-bind.v1",
                    "dispatch_id": heavy_claim["dispatch_id"],
                    "effect_id": heavy_claim["effect_id"],
                    "claim_receipt_sha256": heavy_claim["claim_receipt_sha256"],
                    "worker_task_run_id": str(worker_task_id),
                    "worker_credential_id": str(worker_credential_id),
                    "worker_generation": 1,
                    "worker_command_sha256": command_sha,
                    "worker_task_token_sha256": token_sha,
                    "requested_at": now.isoformat(),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
                connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                heavy_binding = connection.execute(
                    "SELECT migration.bind_region_talk_stage_worker(%s,%s,%s::jsonb)",
                    (manifest.task_run_id, manifest.export_batch_id, json.dumps(bind_request)),
                ).fetchone()[0]
                connection.commit()
                fetch_request = {
                    "schema_version": "region-talk-stage-work-payload-fetch.v1",
                    "worker_task_run_id": str(worker_task_id),
                    "dispatch_id": heavy_claim["dispatch_id"],
                    "effect_id": heavy_claim["effect_id"],
                    "worker_binding_sha256": heavy_binding["worker_binding_sha256"],
                    "requested_at": now.isoformat(),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
                worker_url = (
                    f"postgresql://{worker_principal}:{worker_password}@127.0.0.1:{port}/postgres"
                )
                with psycopg.connect(worker_url) as worker_connection:
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    sparse = worker_connection.execute(
                        "SELECT migration.fetch_region_talk_stage_work_payload(%s,%s,%s::jsonb)",
                        (worker_task_id, UUID(heavy_claim["effect_id"]), json.dumps(fetch_request)),
                    ).fetchone()[0]
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    rich = HeavyStageInputReceipt.model_validate(
                        worker_connection.execute(
                            "SELECT migration.fetch_region_talk_heavy_stage_input(%s,%s,%s::jsonb)",
                            (
                                worker_task_id,
                                UUID(heavy_claim["effect_id"]),
                                json.dumps(fetch_request),
                            ),
                        ).fetchone()[0]
                    )
                    assert rich.status == "READY"
                    assert rich.heavy_input is not None and rich.enrichment_sha256 is not None
                    if stage == "image_scoring":
                        typed_input = ImageScoringInput.model_validate(rich.heavy_input)
                        assert typed_input.artifact_manifest is not None
                        artifact = typed_input.artifact_manifest.items[0]
                        result_base = {
                            "schema_version": "region-talk-image-scoring-result.v1",
                            "input_fingerprint": typed_input.input_fingerprint,
                            "candidate_revision_fingerprint": typed_input.candidate_revision_fingerprint,
                            "media_manifest_sha256": typed_input.artifact_manifest.manifest_sha256,
                            "producer_exact_id": rich.dag_input["runtime_pin"]["producer_exact_id"],
                            "decision": "legacy_auto_accept",
                            "reason_codes": ["legacy_anchor_passed"],
                            "frames": [{
                                "media_id": artifact.source_media_id,
                                "artifact_sha256": artifact.artifact_sha256,
                                "content_text_sha256": typed_input.content.text_sha256,
                                "scorer_request_fingerprint": "a" * 64,
                                "cv_overall_media_score": 0.8,
                                "technical_quality_score": 0.8,
                                "clip_visual_fit_score": 0.8,
                                "laion_aesthetic_score": 0.8,
                                "nima_quality_score": 0.8,
                                "overall_media_score": 0.8,
                                "model_bundle_sha256": typed_input.policy.model_bundle_sha256,
                            }],
                            "selected_media_ids": [artifact.source_media_id],
                            "visual_adjudication": None,
                            "publication_dispatch": False,
                            "notification_dispatch": False,
                        }
                        typed_result = ImageScoringResult.model_validate({
                            **result_base, "result_sha256": canonical_sha256(result_base)
                        })
                        metrics = image_guard_metrics(
                            typed_result, DagImageWorkInput.model_validate(rich.dag_input)
                        )
                    elif stage == "final_verifier":
                        typed_input = FinalVerifierInput.model_validate(rich.heavy_input)
                        fact_id = typed_input.fact_pack.facts[0].fact_id
                        result_base = {
                            "schema_version": "region-talk-final-verifier-result.v1",
                            "input_fingerprint": typed_input.input_fingerprint,
                            "candidate_revision_fingerprint": typed_input.candidate_revision_fingerprint,
                            "fact_pack_sha256": typed_input.fact_pack.fact_pack_sha256,
                            "source_fingerprint": typed_input.source.source_fingerprint,
                            "image_result_sha256": typed_input.image_result_sha256,
                            "producer_exact_id": rich.dag_input["runtime_pin"]["producer_exact_id"],
                            "decision": "accept",
                            "reason_codes": ["grounding_current"],
                            "grounding": [{"claim": "Fixture claim", "fact_ids": [fact_id]}],
                            "request_fingerprint": "b" * 64,
                            "model_id": typed_input.policy.model_id,
                            "publication_dispatch": False,
                            "notification_dispatch": False,
                        }
                        typed_result = FinalVerifierResult.model_validate({
                            **result_base, "result_sha256": canonical_sha256(result_base)
                        })
                        metrics = final_guard_metrics(
                            typed_result, DagFinalVerifierWorkInput.model_validate(rich.dag_input)
                        )
                    else:
                        typed_input = WriterInput.model_validate(rich.heavy_input)
                        fact_id = typed_input.fact_pack.facts[0].fact_id
                        media_ids = list(typed_input.image_result.selected_media_ids)
                        result_base = {
                            "schema_version": "region-talk-writer-result.v1",
                            "input_fingerprint": typed_input.input_fingerprint,
                            "candidate_revision_fingerprint": typed_input.candidate_revision_fingerprint,
                            "fact_pack_sha256": typed_input.fact_pack.fact_pack_sha256,
                            "source_profile_fingerprint": typed_input.source_profile.profile_fingerprint,
                            "final_result_sha256": typed_input.final_result_sha256,
                            "producer_exact_id": rich.dag_input["runtime_pin"]["producer_exact_id"],
                            "status": "ready_for_operator_review",
                            "title": "Fixture title",
                            "paragraph_one": "Fixture grounded first paragraph.",
                            "paragraph_two": "Fixture grounded second paragraph.",
                            "grounding": [{"claim": "Fixture claim", "fact_ids": [fact_id]}],
                            "strategy": {
                                "angle": "Fixture angle",
                                "current_hook_fact_ids": [fact_id],
                                "source_value_fact_ids": [fact_id],
                                "visual_hook_media_ids": media_ids,
                            },
                            "critic": {"decision": "pass", "defects": []},
                            "rewrite_count": 0,
                            "request_fingerprint": "c" * 64,
                            "model_id": typed_input.policy.model_id,
                            "publication_dispatch": False,
                            "notification_dispatch": False,
                        }
                        typed_result = WriterResult.model_validate({
                            **result_base, "result_sha256": canonical_sha256(result_base)
                        })
                        metrics = writer_guard_metrics(
                            typed_result, DagWriterWorkInput.model_validate(rich.dag_input)
                        )
                    private = HeavyStagePrivateResult(
                        stage=stage,
                        work_input_fingerprint=rich.work_input_fingerprint,
                        enrichment_sha256=rich.enrichment_sha256,
                        input_fingerprint=typed_input.input_fingerprint,
                        result_sha256=typed_result.result_sha256,
                        result_data=typed_result.model_dump(mode="json"),
                    ).model_dump(mode="json")
                    metadata = {
                        "schema_version": "region-talk-stage-result-metadata.v1",
                        "stage": stage,
                        "contract_version": heavy_claim["contract_version"],
                        "subject_type": "region_talk.candidate",
                        "subject_id": heavy_claim["subject_id"],
                        "candidate_revision": sparse["payload"]["candidate_revision"],
                        "revision_fingerprint": sparse["payload"]["revision_fingerprint"],
                        "input_fingerprint": heavy_claim["input_fingerprint"],
                        "producer_exact_id": rich.dag_input["runtime_pin"]["producer_exact_id"],
                        "metrics": metrics,
                        "artifact_sha256": typed_result.result_sha256,
                    }
                    direct = {
                        "schema_version": "region-talk-stage-worker-direct-result.v1",
                        "worker_task_run_id": str(worker_task_id),
                        "dispatch_id": heavy_claim["dispatch_id"],
                        "effect_id": heavy_claim["effect_id"],
                        "worker_binding_sha256": heavy_binding["worker_binding_sha256"],
                        "work_item_id": heavy_claim["work_item_id"],
                        "attempt": heavy_claim["attempt"],
                        "result_status": "SUCCEEDED",
                        "result_metadata": metadata,
                        "metadata_sha256": sha256_value(metadata),
                        "result_sha256": typed_result.result_sha256,
                        "completed_at": now.isoformat(),
                        "publication_dispatch": False,
                        "notification_dispatch": False,
                    }
                    combined = {
                        "schema_version": "region-talk-stage-worker-combined-result.v1",
                        "direct_result": direct,
                        "private_result": private,
                        "publication_dispatch": False,
                        "notification_dispatch": False,
                    }
                    typed_forgery = deepcopy(combined)
                    typed_forgery["private_result"]["result_data"].pop("producer_exact_id")
                    typed_forgery_sha = canonical_sha256(
                        {
                            key: value
                            for key, value in typed_forgery["private_result"]["result_data"].items()
                            if key != "result_sha256"
                        }
                    )
                    typed_forgery["private_result"]["result_data"]["result_sha256"] = (
                        typed_forgery_sha
                    )
                    typed_forgery["private_result"]["result_sha256"] = typed_forgery_sha
                    typed_forgery["direct_result"]["result_sha256"] = typed_forgery_sha
                    typed_forgery["direct_result"]["result_metadata"][
                        "artifact_sha256"
                    ] = typed_forgery_sha
                    typed_forgery["direct_result"]["metadata_sha256"] = sha256_value(
                        typed_forgery["direct_result"]["result_metadata"]
                    )
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    with pytest.raises(
                        psycopg.errors.InvalidParameterValue,
                        match="violates exact stage contract or guard metrics",
                    ):
                        worker_connection.execute(
                            "SELECT migration.submit_region_talk_heavy_stage_worker_result("
                            "%s,%s,%s::jsonb)",
                            (
                                worker_task_id,
                                UUID(heavy_claim["effect_id"]),
                                json.dumps(typed_forgery),
                            ),
                        )
                    worker_connection.rollback()

                    metric_forgery = deepcopy(combined)
                    metric_forgery["direct_result"]["result_metadata"]["metrics"][
                        "pin_sha256"
                    ] = "f" * 64
                    metric_forgery["direct_result"]["metadata_sha256"] = sha256_value(
                        metric_forgery["direct_result"]["result_metadata"]
                    )
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    with pytest.raises(
                        psycopg.errors.InvalidParameterValue,
                        match="violates exact stage contract or guard metrics",
                    ):
                        worker_connection.execute(
                            "SELECT migration.submit_region_talk_heavy_stage_worker_result("
                            "%s,%s,%s::jsonb)",
                            (
                                worker_task_id,
                                UUID(heavy_claim["effect_id"]),
                                json.dumps(metric_forgery),
                            ),
                        )
                    worker_connection.rollback()

                    forged = deepcopy(combined)
                    forged["private_result"]["enrichment_sha256"] = "f" * 64
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    with pytest.raises(
                        psycopg.errors.InvalidParameterValue,
                        match="private heavy result differs from current enriched work",
                    ):
                        worker_connection.execute(
                            "SELECT migration.submit_region_talk_heavy_stage_worker_result("
                            "%s,%s,%s::jsonb)",
                            (
                                worker_task_id,
                                UUID(heavy_claim["effect_id"]),
                                json.dumps(forged),
                            ),
                        )
                    worker_connection.rollback()
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    receipt = worker_connection.execute(
                        "SELECT migration.submit_region_talk_heavy_stage_worker_result("
                        "%s,%s,%s::jsonb)",
                        (worker_task_id, UUID(heavy_claim["effect_id"]), json.dumps(combined)),
                    ).fetchone()[0]
                    assert receipt["accepted"] is True
                    assert receipt["result_sha256"] == typed_result.result_sha256
                    worker_connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
                    assert worker_connection.execute(
                        "SELECT migration.submit_region_talk_heavy_stage_worker_result("
                        "%s,%s,%s::jsonb)",
                        (worker_task_id, UUID(heavy_claim["effect_id"]), json.dumps(combined)),
                    ).fetchone()[0] == receipt
                    worker_connection.commit()

            other_embedding = (
                "e5_embedding" if claim["stage"] == "bge_m3_embedding" else "bge_m3_embedding"
            )
            land_exact_stage(superseded_stage)
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            replacement_current = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            assert replacement_current["candidates"][0]["evidence"][superseded_stage][
                "status"
            ] == "CURRENT"
            connection.commit()
            land_exact_stage(other_embedding)
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            vector_ready = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            connection.commit()
            with psycopg.connect(admin_url) as proof:
                vector_input = proof.execute(
                    "SELECT input_data FROM migration.region_talk_stage_work_input_v9 "
                    "WHERE stage='vector_fusion'"
                ).fetchone()[0]
                assert vector_input["schema_version"] == "region-talk-vector-fusion-input.v1"
                assert {row["stage"] for row in vector_input["scores"]} == {
                    "e5_embedding",
                    "bge_m3_embedding",
                }
                assert all(row["result_sha256"] for row in vector_input["scores"])
                assert proof.execute(
                    "SELECT count(*) FROM migration.region_talk_stage_work_input_v9 "
                    "WHERE stage='vector_fusion'"
                ).fetchone() == (1,)
            assert vector_ready["candidates"][0]["evidence"]["vector_fusion"]["status"] == (
                "MISSING"
            )
            land_exact_stage("vector_fusion")
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            image_blocked = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            connection.commit()
            with psycopg.connect(admin_url) as proof:
                # The mutable legacy metadata above is intentionally
                # insufficient: no authoritative acquisition means no image
                # execution row can be formed.
                assert proof.execute(
                    "SELECT count(*) FROM migration.region_talk_stage_work_input_v9 "
                    "WHERE stage='image_scoring'"
                ).fetchone() == (0,)
            assert image_blocked["candidates"][0]["evidence"]["image_scoring"]["status"] == (
                "MISSING"
            )

            acquisition_request = {
                "schema_version": "region-talk-media-artifact-acquisition.v1",
                "task_run_id": str(manifest.task_run_id),
                "export_batch_id": str(manifest.export_batch_id),
                "stage_run_id": str(stage_run_id),
                "canonical_revision": 1,
                "master_instance_id": str(IDENTITY.master_instance_id),
                "epoch": 1,
                "candidate_id": str(_candidate_id),
                "candidate_revision": candidate_revision,
                "candidate_revision_fingerprint": vector_ready["candidates"][0][
                    "revision_fingerprint"
                ],
                "content_id": str(content_id),
                "asset_id": str(asset_id),
                "source_media_id": "media-1",
                "normalized_source_url": "https://example.test/private-image.jpg",
                "source_url_sha256": sha256(
                    b"https://EXAMPLE.test/private-image.jpg?utm_source=mutable"
                ).hexdigest(),
                "object_ref": "artifacts/media-1.jpg",
                "artifact_sha256": "4" * 64,
                "byte_size": 1234,
                "content_type": "image/jpeg",
                "width": None,
                "height": None,
                "acquisition_evidence_sha256": "5" * 64,
                "requested_at": now.isoformat(),
                "publication_dispatch": False,
                "notification_dispatch": False,
            }
            with psycopg.connect(admin_url) as owner:
                acquisition = owner.execute(
                    "SELECT migration.register_region_talk_media_artifact_acquisition(%s::jsonb)",
                    (json.dumps(acquisition_request),),
                ).fetchone()[0]
                acquisition_replay = {
                    **acquisition_request,
                    "requested_at": (now + timedelta(seconds=1)).isoformat(),
                }
                assert owner.execute(
                    "SELECT migration.register_region_talk_media_artifact_acquisition(%s::jsonb)",
                    (json.dumps(acquisition_replay),),
                ).fetchone()[0] == acquisition
                owner.commit()
            assert acquisition["task_readable"] is True
            assert acquisition["publication_dispatch"] is False
            assert acquisition["notification_dispatch"] is False
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            with pytest.raises(psycopg.errors.InsufficientPrivilege):
                connection.execute(
                    "SELECT migration.register_region_talk_media_artifact_acquisition(%s::jsonb)",
                    (json.dumps(acquisition_request),),
                )
            connection.rollback()

            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            image_ready = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            connection.commit()
            with psycopg.connect(admin_url) as proof:
                image_input = proof.execute(
                    "SELECT input_data FROM migration.region_talk_stage_work_input_v9 "
                    "WHERE stage='image_scoring'"
                ).fetchone()[0]
                assert image_input["availability"] == "AVAILABLE"
                assert image_input["artifact_sha256"] == "4" * 64
                assert image_input["object_ref"] == "artifacts/media-1.jpg"
                assert image_input["acquisition_receipt"] == acquisition
                assert image_input["acquisition_receipt_sha256"] == acquisition["receipt_sha256"]
                normalized_receipt = proof.execute(
                    "SELECT migration.region_talk_media_artifact_acquisition_receipt_v2(%s)",
                    (UUID(acquisition["acquisition_id"]),),
                ).fetchone()[0]
                assert normalized_receipt["source_url_sha256"] == sha256_text(
                    "https://example.test/private-image.jpg"
                )
                assert normalized_receipt["source_url_sha256"] != acquisition["source_url_sha256"]
                assert normalized_receipt["legacy_receipt_sha256"] == acquisition["receipt_sha256"]

                proof.execute(
                    "UPDATE hub.content_item SET metadata=metadata||%s::jsonb WHERE content_id=%s",
                    (json.dumps({"canonical_source_key": "web:example.test"}), content_id),
                )
                content_row = proof.execute(
                    "SELECT title,summary,body_excerpt,canonical_url,normalized_url,content_type "
                    "FROM hub.content_item WHERE content_id=%s",
                    (content_id,),
                ).fetchone()
                body_text = content_row[2] or ""
                content_evidence = {
                    "title": content_row[0] or "",
                    "summary": content_row[1] or "",
                    "body_text": body_text,
                    "text_sha256": sha256_text(
                        body_text or "\n\n".join(filter(None, (content_row[0], content_row[1])))
                    ),
                    "canonical_url": content_row[3] or content_row[4],
                    "canonical_source_key": "web:example.test",
                    "content_type": content_row[5],
                }
                fact = {
                    "fact_id": "fact-1",
                    "claim": "Fixture claim",
                    "support_excerpt": "Fixture support",
                    "source_url": content_evidence["canonical_url"],
                    "support_sha256": sha256_text("Fixture support"),
                }
                fact_pack_base = {
                    "schema_version": "region-talk-fact-pack.v1",
                    "candidate_revision_fingerprint": acquisition["candidate_revision_fingerprint"],
                    "facts": [fact],
                }
                fact_pack = {**fact_pack_base, "fact_pack_sha256": sha256_value(fact_pack_base)}
                source_base = {
                    "candidate_revision_fingerprint": acquisition["candidate_revision_fingerprint"],
                    "canonical_source_key": "web:example.test",
                    "externality_status": "verified",
                    "source_scope": "external",
                }
                source = {**source_base, "source_fingerprint": sha256_value(source_base)}
                profile_base = {
                    "candidate_revision_fingerprint": acquisition["candidate_revision_fingerprint"],
                    "canonical_source_key": "web:example.test",
                    "source_fingerprint": source["source_fingerprint"],
                    "entity_type": "media_brand",
                    "externality_status": "verified",
                    "dimensions": {
                        "publisher_identity": "Fixture publisher",
                        "intended_audience": "Fixture audience",
                        "distinctive_value": "Fixture value",
                    },
                }
                source_profile = {
                    **profile_base,
                    "profile_fingerprint": sha256_value(profile_base),
                }
                evidence_request = {
                    "schema_version": "region-talk-heavy-evidence-pack.v1",
                    "task_run_id": str(manifest.task_run_id),
                    "export_batch_id": str(manifest.export_batch_id),
                    "stage_run_id": str(stage_run_id),
                    "canonical_revision": 1,
                    "master_instance_id": str(IDENTITY.master_instance_id),
                    "epoch": 1,
                    "candidate_id": str(_candidate_id),
                    "candidate_revision": candidate_revision,
                    "revision_fingerprint": acquisition["candidate_revision_fingerprint"],
                    "content": content_evidence,
                    "eligibility_fingerprint": "e" * 64,
                    "fact_pack": fact_pack,
                    "source": source,
                    "source_profile": source_profile,
                    "history": [],
                    "requested_at": now.isoformat(),
                    "publication_dispatch": False,
                    "notification_dispatch": False,
                }
                evidence_receipt = proof.execute(
                    "SELECT migration.register_region_talk_heavy_evidence_pack(%s::jsonb)",
                    (json.dumps(evidence_request),),
                ).fetchone()[0]
                assert evidence_receipt["registered"] is True
                work_id = proof.execute(
                    "SELECT work_item_id FROM migration.region_talk_stage_work_input_v9 "
                    "WHERE stage='image_scoring'"
                ).fetchone()[0]
                rich_receipt = HeavyStageInputReceipt.model_validate(
                    proof.execute(
                        "SELECT migration.region_talk_heavy_stage_input_v11(%s)", (work_id,)
                    ).fetchone()[0]
                )
                rich_image = ImageScoringInput.model_validate(rich_receipt.heavy_input)
                assert rich_image.work_input_fingerprint == rich_receipt.work_input_fingerprint
                assert rich_image.enrichment_sha256 == rich_receipt.enrichment_sha256
                assert rich_image.artifact_manifest is not None
                assert rich_image.artifact_manifest.acquisition_receipts[0].schema_version.endswith(".v2")
                proof.commit()
            assert image_ready["candidates"][0]["evidence"]["image_scoring"]["status"] == (
                "MISSING"
            )
            land_heavy_stage_via_combined("image_scoring")
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            )
            connection.commit()
            land_heavy_stage_via_combined("final_verifier")
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            )
            connection.commit()
            land_heavy_stage_via_combined("writer")
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            complete_preparation = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0]
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            assert connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(prepare_request)),
            ).fetchone()[0] == complete_preparation
            assert all(
                value["status"] == "CURRENT"
                for value in complete_preparation["candidates"][0]["evidence"].values()
            )
            with psycopg.connect(admin_url) as proof:
                assert proof.execute(
                    "SELECT count(*),count(DISTINCT work_item_id) "
                    "FROM migration.region_talk_stage_work_input_v9"
                ).fetchone() == (7, 7)
            connection.commit()

            refreshed = complete_preparation
            cycle_request = form_stage_commit(
                StagePreparation.model_validate(refreshed), now=now
            ).model_dump(mode="json")
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            cycle_receipt = connection.execute(
                "SELECT migration.execute_region_talk_post_import_stages(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(cycle_request)),
            ).fetchone()[0]
            assert cycle_receipt["status"] == "COMPLETE"
            status_request = {
                "schema_version": "region-talk-stage-supervisor-status-request.v1",
                "requested_at": now.isoformat(),
            }
            connection.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            stage_status = connection.execute(
                "SELECT migration.region_talk_stage_supervisor_status(%s,%s,%s::jsonb)",
                (manifest.task_run_id, manifest.export_batch_id, json.dumps(status_request)),
            ).fetchone()[0]
            assert stage_status["status"] == "COMPLETE"
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
