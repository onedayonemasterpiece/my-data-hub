"""Transactional PostgreSQL reconciliation for immutable embedding artifacts.

Workers deliberately have no database credentials.  The ACTIVE master passes the
exact jobs it dispatched together with the returned artifact to this importer;
all validation and canonical mutations then happen at one PostgreSQL boundary.
"""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from psycopg.types.json import Jsonb

from my_data_hub.embeddings.contracts import (
    EmbeddingArtifactManifest,
    EmbeddingFailure,
    EmbeddingJob,
    EmbeddingVectorResult,
)
from my_data_hub.embeddings.models import (
    BGE_M3,
    E5_MULTILINGUAL_BASE,
    EmbeddingModelContract,
    model_by_key,
)
from my_data_hub.hashing import sha256_value

# These are the append-only registry identities seeded by migration 0012.  The
# canonical committer intentionally has no generic model-registry mutation path.
_MODEL_IDS: dict[str, UUID] = {
    E5_MULTILINGUAL_BASE.exact_id: UUID("9c2c5d32-cdb7-5c3b-9d9f-50161df3e2b4"),
    BGE_M3.exact_id: UUID("cc441a1c-b88b-564a-bf5e-e80458247367"),
}
_TABLE_BY_DIMENSIONS = {768: "search.embedding_768", 1024: "search.embedding_1024"}


class EmbeddingImportConflict(ValueError):
    """The artifact cannot be reconciled with immutable canonical state."""


@dataclass(frozen=True, slots=True)
class EmbeddingImportReceipt:
    artifact_id: UUID
    outbox_id: UUID
    canonical_revision: int
    inserted_count: int
    stale_count: int
    failed_count: int
    replayed: bool
    durability_state: str = "COMMITTED_PENDING_CHECKPOINT"


@dataclass(frozen=True, slots=True)
class _JobState:
    job: EmbeddingJob
    embedding_job_id: UUID
    status: str
    result_sha256: str | None
    document_is_current: bool
    document_source_revision: int


@dataclass(frozen=True, slots=True)
class _ValidatedImport:
    manifest: EmbeddingArtifactManifest
    jobs: tuple[EmbeddingJob, ...]
    model: EmbeddingModelContract
    model_id: UUID
    table: str
    manifest_sha256: str


class PostgresEmbeddingImporter:
    """Validate and apply one worker artifact in exactly one DB transaction."""

    def import_manifest(
        self,
        connection: Any,
        *,
        manifest: EmbeddingArtifactManifest | Mapping[str, object],
        expected_run_id: UUID,
        jobs: Sequence[EmbeddingJob | Mapping[str, object]],
    ) -> EmbeddingImportReceipt:
        validated = _validate_artifact(manifest, expected_run_id=expected_run_id, jobs=jobs)
        idempotency_key = f"embedding-import-checkpoint:{validated.manifest.artifact_id}"
        try:
            with connection.transaction(), connection.cursor() as cursor:
                replay = cursor.execute(
                    """
                    SELECT outbox_id,required_revision,payload
                    FROM sync.external_outbox
                    WHERE idempotency_key=%s
                    """,
                    (idempotency_key,),
                ).fetchone()
                if replay is not None:
                    return _replay_receipt(validated, replay)

                states = tuple(
                    _load_and_validate_job(cursor, job=job, model_id=validated.model_id)
                    for job in validated.jobs
                )
                states_by_key = {item.job.job_key: item for item in states}
                successes = {
                    result.job_key: result for result in validated.manifest.successful_results
                }
                failures = {failure.job_key: failure for failure in validated.manifest.failures}

                accepted: list[tuple[_JobState, EmbeddingVectorResult]] = []
                stale: list[tuple[_JobState, EmbeddingVectorResult]] = []
                existing_exact = 0
                for job_key, result in successes.items():
                    state = states_by_key[job_key]
                    existing = cursor.execute(
                        f"""
                        SELECT result_sha256 FROM {validated.table}
                        WHERE search_document_id=%s AND model_id=%s AND input_hash=%s
                        """,
                        (result.document_id, validated.model_id, result.input_hash),
                    ).fetchone()
                    if existing is not None:
                        if str(existing[0]) != result.vector_sha256:
                            raise EmbeddingImportConflict(
                                f"immutable vector conflict for job {result.job_key}"
                            )
                        if state.status != "succeeded" or state.result_sha256 != result.vector_sha256:
                            raise EmbeddingImportConflict(
                                f"vector/job receipt mismatch for job {result.job_key}"
                            )
                        existing_exact += 1
                        continue
                    if state.status == "succeeded" or state.result_sha256 is not None:
                        raise EmbeddingImportConflict(
                            f"succeeded job lacks its immutable vector for {result.job_key}"
                        )
                    if state.document_source_revision < result.canonical_revision:
                        raise EmbeddingImportConflict(
                            f"result revision is ahead of canonical document for {result.job_key}"
                        )
                    if (
                        not state.document_is_current
                        or state.document_source_revision > result.canonical_revision
                    ):
                        stale.append((state, result))
                    else:
                        accepted.append((state, result))

                if existing_exact:
                    # A concurrent first importer may have committed while this
                    # transaction waited on a job row lock.  Under READ COMMITTED this
                    # second lookup observes its atomic outbox receipt and remains a
                    # read-only exact replay.
                    concurrent_replay = cursor.execute(
                        """
                        SELECT outbox_id,required_revision,payload
                        FROM sync.external_outbox
                        WHERE idempotency_key=%s
                        """,
                        (idempotency_key,),
                    ).fetchone()
                    if concurrent_replay is not None:
                        return _replay_receipt(validated, concurrent_replay)
                    # Mixing receipt-less old state with new rows would make replay
                    # and checkpoint accounting ambiguous, so fail the transaction.
                    raise EmbeddingImportConflict("existing vectors lack an exact artifact receipt")

                for job_key, failure in failures.items():
                    state = states_by_key[job_key]
                    if state.status == "succeeded" or state.result_sha256 is not None:
                        raise EmbeddingImportConflict(
                            f"worker failure conflicts with succeeded job {failure.job_key}"
                        )

                for state, result in accepted:
                    cursor.execute(
                        f"""
                        INSERT INTO {validated.table}(
                            search_document_id,model_id,input_hash,result_sha256,embedding
                        ) VALUES (%s,%s,%s,%s,%s::halfvec)
                        """,
                        (
                            result.document_id,
                            validated.model_id,
                            result.input_hash,
                            result.vector_sha256,
                            _halfvec_text(result.vector),
                        ),
                    )
                    # In migration 0012 the mutable "current" projection is the job
                    # status; immutable old vectors remain available for audit/replay.
                    cursor.execute(
                        """
                        UPDATE search.embedding_job AS previous
                        SET status='cancelled',lease_until=NULL,
                            failure_reason=%s
                        FROM search.document AS previous_document,
                             search.document AS current_document
                        WHERE current_document.search_document_id=%s
                          AND previous.search_document_id=previous_document.search_document_id
                          AND previous_document.actor_id=current_document.actor_id
                          AND previous.representation_kind=%s
                          AND previous.model_id=%s
                          AND previous.input_hash<>%s
                          AND previous.status='succeeded'
                        """,
                        (
                            f"superseded_by:{result.job_key}",
                            result.document_id,
                            result.representation_kind,
                            validated.model_id,
                            result.input_hash,
                        ),
                    )
                    updated = cursor.execute(
                        """
                        UPDATE search.embedding_job
                        SET status='succeeded',lease_until=NULL,result_sha256=%s,failure_reason=NULL
                        WHERE embedding_job_id=%s
                          AND status IN ('pending','leased','running','retryable_failed')
                          AND result_sha256 IS NULL
                        RETURNING embedding_job_id
                        """,
                        (result.vector_sha256, state.embedding_job_id),
                    ).fetchone()
                    if updated is None:
                        raise EmbeddingImportConflict(
                            f"embedding job state changed during import for {result.job_key}"
                        )

                for state, result in stale:
                    updated = cursor.execute(
                        """
                        UPDATE search.embedding_job
                        SET status='cancelled',lease_until=NULL,result_sha256=NULL,
                            failure_reason=%s
                        WHERE embedding_job_id=%s
                          AND status IN ('pending','leased','running','retryable_failed')
                          AND result_sha256 IS NULL
                        RETURNING embedding_job_id
                        """,
                        (f"stale_result:{result.job_key}", state.embedding_job_id),
                    ).fetchone()
                    if updated is None:
                        raise EmbeddingImportConflict(
                            f"stale embedding job state changed during import for {result.job_key}"
                        )

                for job_key, failure in failures.items():
                    _update_failure(cursor, states_by_key[job_key], failure)

                previous_revision = int(
                    cursor.execute(
                        "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
                    ).fetchone()[0]
                )
                canonical_revision = int(
                    cursor.execute(
                        "SELECT hub.advance_canonical_revision(%s)", (previous_revision,)
                    ).fetchone()[0]
                )
                payload = {
                    "artifact_id": str(validated.manifest.artifact_id),
                    "durability_state": "COMMITTED_PENDING_CHECKPOINT",
                    "failed_count": len(failures),
                    "input_jobs_sha256": validated.manifest.input_jobs_sha256,
                    "inserted_count": len(accepted),
                    "manifest_sha256": validated.manifest_sha256,
                    "model_id": validated.model.exact_id,
                    "payload_sha256": validated.manifest.payload_sha256,
                    "run_id": str(validated.manifest.run_id),
                    "stale_count": len(stale),
                }
                outbox_id = cursor.execute(
                    """
                    INSERT INTO sync.external_outbox(
                        aggregate_type,aggregate_id,effect_kind,idempotency_key,payload,required_revision
                    ) VALUES ('embedding_import',%s,'verified_checkpoint_required',%s,%s,%s)
                    RETURNING outbox_id
                    """,
                    (
                        validated.manifest.artifact_id,
                        idempotency_key,
                        Jsonb(payload),
                        canonical_revision,
                    ),
                ).fetchone()[0]
        except Exception:
            connection.rollback()
            raise

        return EmbeddingImportReceipt(
            artifact_id=validated.manifest.artifact_id,
            outbox_id=UUID(str(outbox_id)),
            canonical_revision=canonical_revision,
            inserted_count=len(accepted),
            stale_count=len(stale),
            failed_count=len(failures),
            replayed=False,
        )


def _validate_artifact(
    manifest: EmbeddingArtifactManifest | Mapping[str, object],
    *,
    expected_run_id: UUID,
    jobs: Sequence[EmbeddingJob | Mapping[str, object]],
) -> _ValidatedImport:
    # Rebuild even already-instantiated Pydantic values so model_construct() or a
    # mutated serialized boundary cannot bypass vector/hash/norm validation.
    manifest_value = (
        manifest.model_dump(mode="python")
        if isinstance(manifest, EmbeddingArtifactManifest)
        else dict(manifest)
    )
    artifact = EmbeddingArtifactManifest.model_validate(manifest_value)
    if artifact.run_id != expected_run_id:
        raise EmbeddingImportConflict(
            f"artifact run_id {artifact.run_id} does not match expected run_id {expected_run_id}"
        )

    exact_jobs = tuple(
        EmbeddingJob.model_validate(
            item.model_dump(mode="python") if isinstance(item, EmbeddingJob) else dict(item)
        )
        for item in jobs
    )
    if not exact_jobs:
        raise EmbeddingImportConflict("artifact import requires the exact dispatched jobs")
    job_keys = [item.job_key for item in exact_jobs]
    if len(job_keys) != len(set(job_keys)):
        raise EmbeddingImportConflict("dispatched embedding jobs contain duplicate job_key values")
    if artifact.total_jobs != len(exact_jobs):
        raise EmbeddingImportConflict("artifact total_jobs does not match dispatched jobs")
    if artifact.input_jobs_sha256 != sha256_value(sorted(job_keys)):
        raise EmbeddingImportConflict("artifact input_jobs_sha256 does not match dispatched jobs")

    try:
        approved_model = model_by_key(artifact.model.model_key)
    except ValueError as exc:
        raise EmbeddingImportConflict("artifact embedding model is not approved") from exc
    if artifact.model != approved_model:
        raise EmbeddingImportConflict("artifact model revision/dimension/encoder contract is not approved")
    if any(item.model != approved_model for item in exact_jobs):
        raise EmbeddingImportConflict("dispatched job model revision/dimension does not match artifact")

    results = {item.job_key: item for item in artifact.successful_results}
    failures = {item.job_key: item for item in artifact.failures}
    if set(job_keys) != set(results) | set(failures):
        raise EmbeddingImportConflict("artifact outcomes do not exactly cover dispatched jobs")
    for job in exact_jobs:
        result = results.get(job.job_key)
        if result is not None:
            expected_result = EmbeddingVectorResult.from_job(job, result.vector)
            if result != expected_result:
                raise EmbeddingImportConflict(
                    f"result fields or input_hash do not match dispatched job {job.job_key}"
                )

    try:
        table = _TABLE_BY_DIMENSIONS[approved_model.dimensions]
        model_id = _MODEL_IDS[approved_model.exact_id]
    except KeyError as exc:  # defensive: approved models and migrations must advance together
        raise EmbeddingImportConflict("approved model has no canonical PostgreSQL vector space") from exc
    return _ValidatedImport(
        manifest=artifact,
        jobs=exact_jobs,
        model=approved_model,
        model_id=model_id,
        table=table,
        manifest_sha256=sha256_value(artifact.model_dump(mode="json")),
    )


def _load_and_validate_job(cursor: Any, *, job: EmbeddingJob, model_id: UUID) -> _JobState:
    row = cursor.execute(
        """
        SELECT job.embedding_job_id,job.search_document_id,job.representation_kind,
               job.input_hash,job.status,job.result_sha256,
               document.is_current,document.source_revision,document.input_hash,
               document.document_text,document.representation_kind
        FROM search.embedding_job AS job
        JOIN search.document AS document
          ON document.search_document_id=job.search_document_id
        WHERE job.search_document_id=%s
          AND job.representation_kind=%s
          AND job.model_id=%s
          AND job.input_hash=%s
        FOR UPDATE OF job,document
        """,
        (job.document.document_id, job.document.representation_kind, model_id, job.input_hash),
    ).fetchone()
    if row is None:
        raise EmbeddingImportConflict(f"canonical embedding job is missing for {job.job_key}")
    (
        embedding_job_id,
        search_document_id,
        representation_kind,
        input_hash,
        status,
        result_sha256,
        is_current,
        source_revision,
        document_hash,
        document_text,
        document_representation,
    ) = row
    if UUID(str(search_document_id)) != job.document.document_id:
        raise EmbeddingImportConflict(f"database document identity mismatch for {job.job_key}")
    if representation_kind != job.document.representation_kind or document_representation != representation_kind:
        raise EmbeddingImportConflict(f"database representation mismatch for {job.job_key}")
    if input_hash != job.input_hash:
        raise EmbeddingImportConflict(f"database input_hash mismatch for {job.job_key}")
    if document_hash != job.document_hash or document_text != job.document.compact_text():
        raise EmbeddingImportConflict(f"database document hash/text mismatch for {job.job_key}")
    return _JobState(
        job=job,
        embedding_job_id=UUID(str(embedding_job_id)),
        status=str(status),
        result_sha256=str(result_sha256) if result_sha256 is not None else None,
        document_is_current=bool(is_current),
        document_source_revision=int(source_revision),
    )


def _update_failure(cursor: Any, state: _JobState, failure: EmbeddingFailure) -> None:
    status = "retryable_failed" if failure.retryable else "dead"
    reason = json.dumps(
        {"code": str(failure.code), "message": failure.message},
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    updated = cursor.execute(
        """
        UPDATE search.embedding_job
        SET status=%s,lease_until=NULL,result_sha256=NULL,failure_reason=%s
        WHERE embedding_job_id=%s
          AND status IN ('pending','leased','running','retryable_failed')
          AND result_sha256 IS NULL
        RETURNING embedding_job_id
        """,
        (status, reason, state.embedding_job_id),
    ).fetchone()
    if updated is None:
        raise EmbeddingImportConflict(
            f"failed embedding job state changed during import for {failure.job_key}"
        )


def _halfvec_text(vector: Sequence[float]) -> str:
    # Values passed this point were fully revalidated as finite and L2-normalized.
    return "[" + ",".join(format(value, ".17g") for value in vector) + "]"


def _replay_receipt(validated: _ValidatedImport, row: Sequence[object]) -> EmbeddingImportReceipt:
    outbox_id, revision, raw_payload = row
    payload = json.loads(raw_payload) if isinstance(raw_payload, str) else raw_payload
    if not isinstance(payload, Mapping):
        raise EmbeddingImportConflict("embedding replay receipt payload is invalid")
    expected = {
        "artifact_id": str(validated.manifest.artifact_id),
        "input_jobs_sha256": validated.manifest.input_jobs_sha256,
        "manifest_sha256": validated.manifest_sha256,
        "model_id": validated.model.exact_id,
        "payload_sha256": validated.manifest.payload_sha256,
        "run_id": str(validated.manifest.run_id),
    }
    if any(payload.get(key) != value for key, value in expected.items()):
        raise EmbeddingImportConflict("embedding artifact idempotency receipt conflict")
    return EmbeddingImportReceipt(
        artifact_id=validated.manifest.artifact_id,
        outbox_id=UUID(str(outbox_id)),
        canonical_revision=int(revision),
        inserted_count=int(payload.get("inserted_count", 0)),
        stale_count=int(payload.get("stale_count", 0)),
        failed_count=int(payload.get("failed_count", 0)),
        replayed=True,
    )
