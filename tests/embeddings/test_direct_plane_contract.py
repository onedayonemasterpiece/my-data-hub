from __future__ import annotations

from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.embeddings.direct_plane import EmbeddingLaunchMetadata
from my_data_hub.master_runtime.credentials import ALLOWED_GROUPS

ROOT = Path(__file__).resolve().parents[2]


def _metadata(**changes: object) -> EmbeddingLaunchMetadata:
    values: dict[str, object] = {
        "schema_version": "embedding-central-launch-metadata.v1",
        "request_id": UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "request_sha256": "a" * 64,
        "task_run_id": UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
        "model_exact_id": "model@" + "c" * 40,
        "input_jobs_sha256": "b" * 64,
        "job_count": 267,
        "worker_source_sha256": "c" * 64,
        "worker_primary_source_sha256": "c" * 64,
        "epoch": 9,
    }
    values.update(changes)
    return EmbeddingLaunchMetadata.model_validate(values)


def test_launch_metadata_rejects_business_payload_and_credentials() -> None:
    for forbidden in ("jobs", "documents", "vectors", "database_url", "token"):
        with pytest.raises(ValidationError):
            _metadata(**{forbidden: "must-not-cross-control"})


def test_direct_plane_migration_is_epoch_guarded_and_role_bounded() -> None:
    sql = (ROOT / "sql/migrations/0019_embedding_worker_direct_plane.sql").read_text()
    assert "CREATE TABLE search.embedding_dispatch" in sql
    assert "CREATE TABLE search.embedding_result_landing" in sql
    assert sql.count("CREATE CONSTRAINT TRIGGER mdh_epoch_write_guard") == 2
    assert "mdh_embedding_worker" in ALLOWED_GROUPS
    assert "GRANT EXECUTE ON FUNCTION search.claim_embedding_dispatch" in sql
    assert "GRANT EXECUTE ON FUNCTION search.submit_embedding_result" in sql
    assert "GRANT SELECT ON search.embedding_result_landing TO mdh_embedding_worker" not in sql
    assert "schema_revision=19" in sql


def test_notebook_embedding_path_has_no_provider_client_or_kaggle_token() -> None:
    stage = (ROOT / "src/my_data_hub/embeddings/master_stage.py").read_text()
    entrypoint = (ROOT / "src/my_data_hub/master_runtime/notebook_entrypoint.py").read_text()
    assert "KaggleProviderAdapter" not in stage
    assert "KAGGLE_API_TOKEN" not in stage
    assert "adapter=" not in entrypoint[entrypoint.index("if embedding_request is not None:") :]
    assert "PostgresEmbeddingWorkerExchange" in entrypoint
