from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.importer import (
    DuplicateResolution,
    DuplicateResolutionConflict,
    _build_resolution_plan,
    _DuplicateClaim,
    _observe,
)
from my_data_hub.workloads.bloggers.master_stage import (
    BLOGGER_REPLAY_STAGE_SCHEMA,
    BloggerDuplicateResolutionEnvelope,
    BloggerImportStageReceipt,
    BloggerMigrationRequest,
)
from my_data_hub.workloads.bloggers.schema import SOURCE_QUERY_SHA256


def source_row(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "record_id": "record-001", "batch_id": "batch-001", "list_order": 1,
        "level": "regional", "blogger_name": "Тестовый автор", "segment": "культура",
        "region_relation_status": "external", "visit_period_text": "2025",
        "locations_text": "Россия", "confirmation_basis": "public profile",
        "evidence_url": "https://example.test/evidence/1",
        "telegram_url": "http://t.me/TestAuthor/", "vk_public_url": None,
        "vk_video_url": None, "rutube_url": "https://rutube.ru/channel/123/",
        "source_kind": "manual_external_confirmation", "confirmation_status": "confirmed_external",
        "pipeline_status": "stored_only", "source_file_sha256": "a" * 64,
        "ingested_at": "2026-08-03T13:30:00Z", "updated_at": "2026-08-03T13:31:00Z",
        "external_region_basis": None, "external_region_evidence_url": None,
        "submission_batch_ids_json": None, "other_primary_url": None,
        "social_links_type": None, "evidence_type": None,
    }
    value.update(changes)
    return value


def _claim() -> tuple[str, _DuplicateClaim]:
    first = _observe(source_row(record_id="blogger-001"), 0)
    second = _observe(
        source_row(
            record_id="blogger-002",
            blogger_name="Вторая строка того же автора",
            evidence_url="https://example.test/evidence/2",
        ),
        1,
    )
    identity = hashlib.sha256(b"telegram\0https://t.me/TestAuthor").hexdigest()
    return identity, _DuplicateClaim(identity, (first, second), None)


def test_resolution_requires_exact_members_and_explicit_canonical_target() -> None:
    identity, claim = _claim()
    assert claim.members[0].projection is not None
    resolution = DuplicateResolution(
        identity_sha256=identity,
        canonical_record_id="blogger-001",
        canonical_actor_id=claim.members[0].projection.actor_id,
        member_record_ids=("blogger-001", "blogger-002"),
        decided_by="owner-review:test",
        reason="Exact fixture evidence confirms one person.",
    )
    plan = _build_resolution_plan({identity: claim}, (resolution,))
    assert set(plan.targets) == {"blogger-001", "blogger-002"}
    assert set(plan.targets.values()) == {resolution.canonical_actor_id}
    assert plan.canonical_records == {resolution.canonical_actor_id: "blogger-001"}
    assert len(resolution.resolution_sha256) == 64

    with pytest.raises(DuplicateResolutionConflict, match="incomplete or stale"):
        _build_resolution_plan({identity: claim}, ())
    with pytest.raises(ValueError, match="sorted, unique"):
        DuplicateResolution(
            identity_sha256=identity,
            canonical_record_id="blogger-001",
            canonical_actor_id=resolution.canonical_actor_id,
            member_record_ids=("blogger-002", "blogger-001"),
            decided_by="owner-review:test",
            reason="Invalid ordering must fail closed.",
        )


def test_existing_account_owner_must_be_selected_without_implicit_merge() -> None:
    identity, claim = _claim()
    existing = UUID("99999999-9999-4999-8999-999999999999")
    existing_claim = _DuplicateClaim(identity, claim.members, existing)
    assert claim.members[0].projection is not None
    with pytest.raises(DuplicateResolutionConflict, match="existing account owner"):
        _build_resolution_plan(
            {identity: existing_claim},
            (
                DuplicateResolution(
                    identity_sha256=identity,
                    canonical_record_id="blogger-001",
                    canonical_actor_id=claim.members[0].projection.actor_id,
                    member_record_ids=("blogger-001", "blogger-002"),
                    decided_by="owner-review:test",
                    reason="A different target must not silently take the account.",
                ),
            ),
        )


def test_resolved_receipt_allows_fewer_actors_and_nonzero_durable_groups() -> None:
    receipt = BloggerImportStageReceipt(
        schema_version="region-talk-ydb-bloggers-import-receipt.v3",
        request_id=UUID("11111111-1111-4111-8111-111111111111"),
        operation_id=UUID("22222222-2222-4222-8222-222222222222"),
        master_instance_id=UUID("33333333-3333-4333-8333-333333333333"),
        run_id="fixture",
        epoch=7,
        request_sha256="a" * 64,
        export_batch_id=UUID("44444444-4444-4444-8444-444444444444"),
        source_query_sha256=SOURCE_QUERY_SHA256,
        row_count=266,
        distinct_record_ids=266,
        source_file_count=14,
        dispositions={"normalized": 265, "deduplicated": 1},
        record_id_set_sha256="b" * 64,
        logical_sha256="c" * 64,
        canonical_outcome_sha256="d" * 64,
        actor_count=265,
        account_count=410,
        duplicate_group_count=1,
        duplicate_groups_pending=0,
        undispositioned=0,
        quarantined=0,
        replayed_count=266,
        canonical_revision=9,
        transaction_committed=True,
        ydb_write_denial_verified=True,
    )
    assert receipt.actor_count == 265
    assert receipt.duplicate_group_count == 1


def test_duplicate_resolution_and_resolved_receipt_examples_validate() -> None:
    root = Path(__file__).resolve().parents[2]
    pairs = (
        (
            "schemas/region-talk-blogger-duplicate-resolution-envelope.v1.schema.json",
            "examples/bloggers/region-talk-blogger-duplicate-resolution-envelope.v1.example.json",
        ),
        (
            "schemas/blogger-migration-request.v2.schema.json",
            "examples/bloggers/blogger-migration-request.v2.example.json",
        ),
        (
            "schemas/region-talk-blogger-duplicate-resolution-set.v1.schema.json",
            "examples/bloggers/region-talk-blogger-duplicate-resolution-set.v1.example.json",
        ),
        (
            "schemas/region-talk-ydb-bloggers-import-receipt.v3.schema.json",
            "examples/bloggers/region-talk-ydb-bloggers-import-receipt.v3.resolved.example.json",
        ),
    )
    for schema_path, example_path in pairs:
        schema = json.loads((root / schema_path).read_text())
        example = json.loads((root / example_path).read_text())
        Draft202012Validator(schema).validate(example)


def test_replay_request_hash_binds_authorization_source_and_sorted_decisions() -> None:
    root = Path(__file__).resolve().parents[2]
    raw = json.loads(
        (root / "examples/bloggers/region-talk-blogger-duplicate-resolution-envelope.v1.example.json").read_text()
    )
    envelope = BloggerDuplicateResolutionEnvelope.model_validate(raw)
    source = BloggerMigrationRequest(
        request_id=envelope.source_request_id,
        operation_id=envelope.source_operation_id,
        project_id=envelope.project_id,
        snapshot_at=envelope.snapshot_at,
        expected_rows=266,
        source_revision=envelope.source_revision,
    )
    assert source.request_sha256 == envelope.source_request_sha256
    request = BloggerMigrationRequest(
        schema_version=BLOGGER_REPLAY_STAGE_SCHEMA,
        request_id=UUID("66666666-6666-4666-8666-666666666666"),
        operation_id=UUID("77777777-7777-4777-8777-777777777777"),
        project_id=envelope.project_id,
        snapshot_at=envelope.snapshot_at,
        expected_rows=266,
        source_revision=envelope.source_revision,
        replay_of_request_id=envelope.source_request_id,
        duplicate_resolution=envelope,
    )
    assert len(envelope.envelope_sha256) == 64
    assert len(request.request_sha256) == 64
    assert request.duplicate_resolutions[0].resolution_sha256 == envelope.decisions[0].decision_sha256

    with pytest.raises(ValueError, match="exact authorizer"):
        BloggerDuplicateResolutionEnvelope.model_validate(
            {**raw, "decisions": [{**raw["decisions"][0], "decided_by": "another-owner"}]}
        )
    with pytest.raises(ValueError, match="exact snapshot"):
        BloggerDuplicateResolutionEnvelope.model_validate(
            {**raw, "snapshot_at": datetime(2026, 8, 10, tzinfo=UTC).isoformat()}
        )
    with pytest.raises(ValueError, match="requires one exact replay envelope"):
        BloggerMigrationRequest(
            schema_version=BLOGGER_REPLAY_STAGE_SCHEMA,
            request_id=UUID("88888888-8888-4888-8888-888888888888"),
            operation_id=UUID("99999999-9999-4999-8999-999999999999"),
            project_id=envelope.project_id,
            snapshot_at=envelope.snapshot_at,
            expected_rows=266,
            source_revision=envelope.source_revision,
        )


def test_v1_request_payload_and_hash_remain_backward_compatible() -> None:
    request = BloggerMigrationRequest(
        request_id=UUID("11111111-1111-4111-8111-111111111111"),
        operation_id=UUID("22222222-2222-4222-8222-222222222222"),
        project_id=UUID("33333333-3333-4333-8333-333333333333"),
        snapshot_at=datetime(2026, 8, 9, tzinfo=UTC),
        expected_rows=266,
        source_revision="b" * 40,
    )
    assert "replay_of_request_id" not in request.metadata_payload
    assert "duplicate_resolution" not in request.metadata_payload
    assert request.request_sha256 == hashlib.sha256(
        canonical_json_bytes(request.metadata_payload)
    ).hexdigest()


def test_migration_keeps_original_evidence_immutable_and_exposes_only_bounded_accounting() -> None:
    root = Path(__file__).resolve().parents[2]
    sql = (root / "sql/migrations/0015_blogger_duplicate_resolution.sql").read_text()
    assert "CREATE TABLE migration.blogger_replay" in sql
    assert "CREATE TABLE migration.blogger_duplicate_resolution" in sql
    assert "CREATE TABLE migration.blogger_replay_disposition" in sql
    assert "CREATE VIEW migration.blogger_duplicate_accounting" in sql
    assert "coalesce(replay.disposition, disp.disposition)" in sql
    assert "UPDATE migration.raw_record" not in sql
    assert "UPDATE migration.row_disposition" not in sql
    assert "GRANT SELECT ON migration.blogger_duplicate_accounting" in sql
    assert "GRANT SELECT, INSERT ON migration.blogger_replay" in sql
    assert "REVOKE UPDATE, DELETE ON migration.blogger_replay" in sql
    assert "schema_revision = 15" in sql
