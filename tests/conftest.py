from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import pytest


@pytest.fixture
def notebook_manifest_payload() -> dict[str, object]:
    return {
        "schema_version": "my-data-hub-notebook-input.v1",
        "run_id": "11111111-1111-4111-8111-111111111111",
        "workload": "region-talk",
        "stage": "candidate_report",
        "stage_contract_version": "region-talk.candidate-report.v1",
        "canonical_revision": 7,
        "work_items": [
            {
                "work_item_id": "22222222-2222-4222-8222-222222222222",
                "subject_type": "content_url",
                "subject_id": "33333333-3333-4333-8333-333333333333",
                "input_fingerprint": "a" * 64,
                "payload": {"url": "https://example.test/post/1"},
            },
            {
                "work_item_id": "44444444-4444-4444-8444-444444444444",
                "subject_type": "content_url",
                "subject_id": "55555555-5555-4555-8555-555555555555",
                "input_fingerprint": "b" * 64,
                "payload": {"url": "https://example.test/post/2"},
            },
        ],
        "artifacts": [],
        "model": {
            "provider": "fixture",
            "name": "fixture-model",
            "version": "1",
            "task": "fixture",
            "configuration": {},
        },
        "policy_versions": {"eligibility": "v1"},
        "limits": {
            "max_runtime_seconds": 300,
            "max_output_bytes": 1048576,
            "max_items": 3,
        },
        "created_at": datetime(2026, 8, 9, tzinfo=UTC).isoformat(),
    }


@pytest.fixture
def notebook_manifest_file(tmp_path: Path, notebook_manifest_payload: dict[str, object]) -> Path:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(notebook_manifest_payload), encoding="utf-8")
    return path


@pytest.fixture
def notebook_result_payload() -> dict[str, object]:
    return {
        "schema_version": "my-data-hub-notebook-result.v1",
        "result_id": "66666666-6666-4666-8666-666666666666",
        "run_id": "11111111-1111-4111-8111-111111111111",
        "workload": "region-talk",
        "stage": "candidate_report",
        "stage_contract_version": "region-talk.candidate-report.v1",
        "input_manifest_sha256": "c" * 64,
        "producer": {
            "code_revision": "fixture",
            "runtime": "pytest",
            "model": {"provider": "fixture", "name": "fixture-model", "version": "1"},
        },
        "status": "succeeded",
        "items": [],
        "failures": [],
        "metrics": {},
        "provider_usage": [],
        "artifacts": [],
        "started_at": datetime(2026, 8, 9, tzinfo=UTC).isoformat(),
        "completed_at": datetime(2026, 8, 9, 0, 1, tzinfo=UTC).isoformat(),
    }
