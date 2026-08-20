"""Exact bridge from closed heavy results to migration 0030 guard metrics.

Migration 0030 deliberately materializes only a minimal work envelope.  The private
worker must enrich and validate that envelope before executing the heavy contracts;
this module does not fetch that evidence.  It only parses the frozen SQL shapes and
derives the equally frozen result-metadata metrics from already validated results.
"""

from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import Field, model_validator

from my_data_hub.hashing import sha256_value

from .heavy_contracts import (
    SHA256_PATTERN,
    FinalVerifierResult,
    ImageScoringResult,
    StrictModel,
    WriterResult,
    canonical_sha256,
    sha256_text,
)


class DagMediaAcquisitionReceiptV1(StrictModel):
    """Frozen 0031 receipt.

    Its ``source_url_sha256`` was defined over the raw source URL, not the
    normalized URL.  It is accepted only for parsing the immutable sparse DAG
    envelope.  Heavy execution requires the authoritative v2 projection.
    """

    schema_version: Literal["region-talk-media-artifact-acquisition-receipt.v1"]
    registered: Literal[True]
    acquisition_id: UUID
    task_run_id: UUID
    export_batch_id: UUID
    stage_run_id: UUID
    canonical_revision: int = Field(ge=1)
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    candidate_id: UUID
    candidate_revision: int = Field(ge=1)
    candidate_revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content_id: UUID
    asset_id: UUID
    source_media_id: str = Field(min_length=1, max_length=500)
    normalized_source_url: str = Field(min_length=1, max_length=4_000)
    source_url_sha256: str = Field(pattern=SHA256_PATTERN)
    object_ref: str = Field(min_length=1, max_length=2_000)
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    byte_size: int = Field(ge=1, le=1_073_741_824)
    content_type: str = Field(min_length=1, max_length=200)
    width: int | None = Field(default=None, ge=1, le=100_000)
    height: int | None = Field(default=None, ge=1, le=100_000)
    acquisition_evidence_sha256: str = Field(pattern=SHA256_PATTERN)
    task_readable: Literal[True]
    publication_dispatch: Literal[False]
    notification_dispatch: Literal[False]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_legacy_receipt(self) -> DagMediaAcquisitionReceiptV1:
        if self.acquisition_id.version != 5:
            raise ValueError("acquisition_id must be server-derived UUIDv5")
        if canonical_sha256(self.model_dump(mode="json", exclude={"receipt_sha256"})) != self.receipt_sha256:
            raise ValueError("legacy acquisition receipt_sha256 differs")
        return self


class StageRuntimePinReceipt(StrictModel):
    schema_version: Literal["region-talk-stage-runtime-pin-receipt.v1"]
    registered: Literal[True]
    stage: Literal["image_scoring", "final_verifier", "writer"]
    contract_version: Literal[
        "region-talk.image-diagnostic.v1",
        "region-talk.final-verifier.v1",
        "region-talk.writer.v1",
    ]
    effective_canonical_revision: int = Field(ge=1)
    pin_generation: int = Field(ge=1)
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    model_id: str = Field(min_length=1, max_length=300)
    model_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    encoder_contract: str = Field(min_length=1, max_length=300)
    semantic_bank_version: None
    semantic_bank_sha256: None
    runtime_source_sha256: str = Field(pattern=SHA256_PATTERN)
    asset_manifest_sha256: str = Field(pattern=SHA256_PATTERN)
    provider_image_identity: str = Field(pattern=r"^[^@\s]+@sha256:[a-f0-9]{64}$")
    provider_image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    producer_exact_id: str = Field(min_length=1, max_length=2_000)
    prior_pin_receipt_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    pin_sha256: str = Field(pattern=SHA256_PATTERN)
    publication_dispatch: Literal[False]
    notification_dispatch: Literal[False]
    receipt_sha256: str = Field(pattern=SHA256_PATTERN)

    @model_validator(mode="after")
    def exact_stage_contract(self) -> StageRuntimePinReceipt:
        contracts = {
            "image_scoring": "region-talk.image-diagnostic.v1",
            "final_verifier": "region-talk.final-verifier.v1",
            "writer": "region-talk.writer.v1",
        }
        if self.contract_version != contracts[self.stage]:
            raise ValueError("runtime pin stage and contract differ")
        return self


class DagImageWorkInput(StrictModel):
    schema_version: Literal["region-talk-image-input.v1"]
    availability: Literal["AVAILABLE"]
    asset_id: UUID
    source_media_id: str = Field(min_length=1, max_length=500)
    normalized_source_url: str = Field(min_length=1, max_length=4_000)
    object_ref: str = Field(min_length=1, max_length=2_000)
    artifact_sha256: str = Field(pattern=SHA256_PATTERN)
    byte_size: int = Field(ge=1)
    content_type: str = Field(min_length=1, max_length=200)
    acquisition_receipt: DagMediaAcquisitionReceiptV1
    acquisition_receipt_sha256: str = Field(pattern=SHA256_PATTERN)
    candidate_revision: int = Field(ge=1)
    runtime_pin: StageRuntimePinReceipt

    @model_validator(mode="after")
    def image_pin(self) -> DagImageWorkInput:
        if self.runtime_pin.stage != "image_scoring":
            raise ValueError("image work has the wrong runtime pin")
        receipt = self.acquisition_receipt
        if self.acquisition_receipt_sha256 != receipt.receipt_sha256:
            raise ValueError("image DAG acquisition receipt hash differs")
        comparisons = {
            "asset_id": (str(self.asset_id), str(receipt.asset_id)),
            "source_media_id": (self.source_media_id, receipt.source_media_id),
            "normalized_source_url": (self.normalized_source_url, receipt.normalized_source_url),
            "object_ref": (self.object_ref, receipt.object_ref),
            "artifact_sha256": (self.artifact_sha256, receipt.artifact_sha256),
            "byte_size": (self.byte_size, receipt.byte_size),
            "content_type": (self.content_type, receipt.content_type),
            "candidate_revision": (self.candidate_revision, receipt.candidate_revision),
        }
        mismatched = [
            key for key, (work_value, receipt_value) in comparisons.items() if work_value != receipt_value
        ]
        if mismatched:
            raise ValueError(f"image DAG work differs from immutable acquisition receipt: {mismatched[0]}")
        return self


class DagContentPack(StrictModel):
    content_id: UUID
    canonical_source_key: str = Field(min_length=1, max_length=1_000)
    title_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    summary_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    text_payload_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)
    ordered_media_sha256: str | None = Field(default=None, pattern=SHA256_PATTERN)


class DagFinalVerifierWorkInput(StrictModel):
    schema_version: Literal["region-talk-final-verifier-input.v1"]
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content_pack: DagContentPack
    vector_result_sha256: str = Field(pattern=SHA256_PATTERN)
    image_result_sha256: str = Field(pattern=SHA256_PATTERN)
    runtime_pin: StageRuntimePinReceipt

    @model_validator(mode="after")
    def verifier_pin(self) -> DagFinalVerifierWorkInput:
        if self.runtime_pin.stage != "final_verifier":
            raise ValueError("final verifier work has the wrong runtime pin")
        if self.content_pack.title_sha256 is None or self.content_pack.summary_sha256 is None:
            raise ValueError("final verifier work lacks title/summary hashes")
        if self.content_pack.text_payload_sha256 is not None or self.content_pack.ordered_media_sha256 is not None:
            raise ValueError("final verifier content pack contains writer-only hashes")
        return self


class DagWriterWorkInput(StrictModel):
    schema_version: Literal["region-talk-writer-input.v1"]
    candidate_revision: int = Field(ge=1)
    revision_fingerprint: str = Field(pattern=SHA256_PATTERN)
    content_pack: DagContentPack
    final_result_sha256: str = Field(pattern=SHA256_PATTERN)
    runtime_pin: StageRuntimePinReceipt

    @model_validator(mode="after")
    def writer_pin(self) -> DagWriterWorkInput:
        if self.runtime_pin.stage != "writer":
            raise ValueError("writer work has the wrong runtime pin")
        if self.content_pack.text_payload_sha256 is None or self.content_pack.ordered_media_sha256 is None:
            raise ValueError("writer work lacks exact text/media hashes")
        if self.content_pack.title_sha256 is not None or self.content_pack.summary_sha256 is not None:
            raise ValueError("writer content pack contains verifier-only hashes")
        return self


def _pin_metrics(pin: StageRuntimePinReceipt) -> dict[str, Any]:
    return {
        "model_id": pin.model_id,
        "model_revision": pin.model_revision,
        "encoder_contract": pin.encoder_contract,
        "asset_manifest_sha256": pin.asset_manifest_sha256,
        "runtime_source_sha256": pin.runtime_source_sha256,
        "provider_image_identity": pin.provider_image_identity,
        "provider_image_source_commit": pin.provider_image_source_commit,
        "pin_sha256": pin.pin_sha256,
    }


def image_guard_metrics(result: ImageScoringResult, work: DagImageWorkInput) -> dict[str, Any]:
    anchor = result.frames[0]
    if anchor.artifact_sha256 != work.artifact_sha256:
        raise ValueError("image result differs from the DAG artifact")
    if anchor.model_bundle_sha256 != work.runtime_pin.asset_manifest_sha256:
        raise ValueError("image result differs from the pinned model asset manifest")
    if result.producer_exact_id != work.runtime_pin.producer_exact_id:
        raise ValueError("image result producer differs from the runtime pin")
    decision = "accept" if result.decision in {"legacy_auto_accept", "vlm_visual_accept"} else "needs_review"
    return {
        "schema_version": "region-talk.image-diagnostic-result.v1",
        "decision": decision,
        "actual_image": True,
        "postcard_score": anchor.overall_media_score,
        "input_artifact_sha256": work.artifact_sha256,
        **_pin_metrics(work.runtime_pin),
    }


def final_guard_metrics(result: FinalVerifierResult, work: DagFinalVerifierWorkInput) -> dict[str, Any]:
    if result.image_result_sha256 != work.image_result_sha256:
        raise ValueError("final verifier result differs from DAG image evidence")
    if result.producer_exact_id != work.runtime_pin.producer_exact_id or result.model_id != work.runtime_pin.model_id:
        raise ValueError("final verifier producer differs from the runtime pin")
    return {
        "schema_version": "region-talk.final-verifier-result.v1",
        "decision": {"accept": "PASS", "needs_review": "REVIEW", "reject": "REJECT"}[result.decision],
        "reason_codes": list(result.reason_codes),
        "vector_result_sha256": work.vector_result_sha256,
        "image_result_sha256": work.image_result_sha256,
        **_pin_metrics(work.runtime_pin),
    }


def writer_guard_metrics(result: WriterResult, work: DagWriterWorkInput) -> dict[str, Any]:
    if result.status != "ready_for_operator_review":
        raise ValueError("non-ready writer result cannot be landed as SUCCEEDED")
    if result.final_result_sha256 != work.final_result_sha256:
        raise ValueError("writer result differs from DAG final verifier evidence")
    if result.producer_exact_id != work.runtime_pin.producer_exact_id or result.model_id != work.runtime_pin.model_id:
        raise ValueError("writer producer differs from the runtime pin")
    body = result.paragraph_one + "\n\n" + result.paragraph_two
    return {
        "schema_version": "region-talk.writer-result.v1",
        "draft_sha256": sha256_value({"title": result.title, "body": body}),
        "title_sha256": sha256_text(result.title),
        "body_sha256": sha256_text(body),
        "character_count": len(result.title) + len(body),
        "final_result_sha256": work.final_result_sha256,
        **_pin_metrics(work.runtime_pin),
    }


__all__ = [
    "DagFinalVerifierWorkInput",
    "DagImageWorkInput",
    "DagMediaAcquisitionReceiptV1",
    "DagWriterWorkInput",
    "StageRuntimePinReceipt",
    "final_guard_metrics",
    "image_guard_metrics",
    "writer_guard_metrics",
]
