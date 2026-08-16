from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator
from pydantic import ValidationError

from my_data_hub.connectors.contracts import DeliveryMode, validate_envelope_bytes
from my_data_hub.workloads.bloggers.discovery import (
    ARTIFACT_CONNECTOR_ID,
    INLINE_CONNECTOR_ID,
    BloggerDiscoveryRow,
    ProviderArtifactClaim,
    SubmitDiscoveryBatch,
    blogger_import_plan_sha256,
    blogger_import_request_sha256,
)

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 8, 16, 12, tzinfo=UTC)


def row(**changes: object) -> dict[str, object]:
    value: dict[str, object] = {
        "source_record_id": "search-result-001",
        "actor_kind": "person",
        "display_name": "Тестовый блогер",
        "canonical_name": "тестовый блогер",
        "summary": "Публичное описание.",
        "accounts": [
            {
                "platform": "telegram",
                "handle": "TestBlogger",
                "url": "HTTPS://T.ME:443/TestBlogger/#fragment",
            }
        ],
        "source_uri": "https://search.example/results/1#fragment",
        "observed_at": NOW.isoformat(),
        "evidence": {"query": "калининград блогер", "rank": 1},
    }
    value.update(changes)
    return value


def inline_request(**changes: object) -> SubmitDiscoveryBatch:
    value: dict[str, object] = {
        "batch_id": "11111111-1111-4111-8111-111111111111",
        "idempotency_key": "owner-search-20260816-001",
        "project_slug": "region-talk",
        "produced_at": NOW.isoformat(),
        "observed_period": {
            "start": (NOW - timedelta(hours=1)).isoformat(),
            "end": NOW.isoformat(),
            "timezone": "UTC",
        },
        "rows": [row()],
    }
    value.update(changes)
    return SubmitDiscoveryBatch.model_validate(value)


def test_closed_row_normalizes_account_identity_and_has_stable_hash() -> None:
    observed = BloggerDiscoveryRow.model_validate(row())
    assert observed.accounts[0].platform == "telegram"
    assert observed.accounts[0].url == "https://t.me/TestBlogger"
    assert observed.accounts[0].normalized_url == "https://t.me/TestBlogger"
    assert observed.source_uri == "https://search.example/results/1"
    assert observed.row_sha256 == BloggerDiscoveryRow.model_validate(row()).row_sha256
    with pytest.raises(ValidationError, match="extra_forbidden"):
        BloggerDiscoveryRow.model_validate(row(secret="must-not-land"))


def test_inline_submission_builds_fixed_connector_envelope_and_exact_hashes() -> None:
    request = inline_request()
    envelope = request.connector_envelope()
    assert envelope.connector_id == INLINE_CONNECTOR_ID
    assert envelope.delivery_mode is DeliveryMode.PUSH
    assert envelope.record_count == 1
    assert envelope.trace["project_slug"] == "region-talk"
    exact = request.connector_envelope_bytes()
    validated = validate_envelope_bytes(exact)
    assert validated.envelope == envelope
    assert request.request_sha256 == inline_request().request_sha256


def test_artifact_requires_exact_private_numeric_claim_and_safe_path() -> None:
    artifact = ProviderArtifactClaim(
        resource_ref="owner/private-bloggers",
        control_class="mcp_exchange",
        provider_version=7,
        path="exports/bloggers.jsonl",
        media_type="application/jsonl",
        byte_size=1234,
        sha256="a" * 64,
        claim_sha256="b" * 64,
        record_count=12,
    )
    request = inline_request(rows=None, artifact=artifact)
    envelope = request.connector_envelope()
    assert envelope.connector_id == ARTIFACT_CONNECTOR_ID
    assert envelope.delivery_mode is DeliveryMode.ARTIFACT_HANDOFF
    assert "/versions/7/exports/bloggers.jsonl" in envelope.artifact.locator
    assert envelope.payload_sha256 == "a" * 64
    with pytest.raises(ValidationError, match="safe relative path"):
        ProviderArtifactClaim.model_validate({**artifact.model_dump(), "path": "../secret.jsonl"})
    with pytest.raises(ValidationError):
        ProviderArtifactClaim.model_validate({**artifact.model_dump(), "provider_version": "latest"})


def test_cross_row_account_collision_and_duplicate_source_are_rejected() -> None:
    second = row(source_record_id="search-result-002", display_name="Другой")
    with pytest.raises(ValidationError, match="account identity"):
        inline_request(rows=[row(), second])
    with pytest.raises(ValidationError, match="source_record_id"):
        inline_request(rows=[row(), row()])


def test_import_and_plan_hashes_are_order_stable_and_bind_revision() -> None:
    first = blogger_import_request_sha256(
        batch_id=UUID("11111111-1111-4111-8111-111111111111"),
        expected_revision=3,
        idempotency_key="owner-preview-001",
    )
    second = blogger_import_request_sha256(
        batch_id="11111111-1111-4111-8111-111111111111",
        expected_revision=3,
        idempotency_key="owner-preview-001",
    )
    assert first == second
    assert first != blogger_import_request_sha256(
        batch_id="11111111-1111-4111-8111-111111111111",
        expected_revision=4,
        idempotency_key="owner-preview-001",
    )
    rows = [
        {"row_ordinal": 1, "record_sha256": "b" * 64, "disposition": "create_actor"},
        {"row_ordinal": 0, "record_sha256": "a" * 64, "disposition": "link_existing"},
    ]
    assert blogger_import_plan_sha256(rows) == blogger_import_plan_sha256(list(reversed(rows)))


def test_repository_json_schema_is_closed_and_matches_examples() -> None:
    schema = json.loads((ROOT / "schemas/blogger-discovery-batch.v1.schema.json").read_text())
    Draft202012Validator.check_schema(schema)
    validator = Draft202012Validator(schema)
    payload = inline_request().model_dump(mode="json", exclude_none=False)
    assert list(validator.iter_errors(payload)) == []
    payload["unexpected"] = True
    assert any(error.validator == "additionalProperties" for error in validator.iter_errors(payload))
