from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

DONOR_REPOSITORY = "https://github.com/onedayonemasterpiece/events-bot-new.git"
DONOR_COMMIT = "416d17e689acf0a4f69f2b4d1db5dad5b46c4bca"


class DonorCompatibilityPin(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    donor_repository: Literal["https://github.com/onedayonemasterpiece/events-bot-new.git"]
    donor_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    source_path: str = Field(min_length=1, max_length=500)
    blob_sha: str = Field(pattern=r"^[a-f0-9]{40}$")
    reused_contract: str = Field(min_length=1, max_length=500)
    adaptation_reason: str = Field(min_length=1, max_length=1000)
    compatibility_test: str = Field(pattern=r"^tests/provider/test_[A-Za-z0-9_]+\.py::test_[A-Za-z0-9_]+$")


DONOR_COMPATIBILITY_INVENTORY: tuple[DonorCompatibilityPin, ...] = (
    DonorCompatibilityPin(
        donor_repository=DONOR_REPOSITORY,
        donor_commit=DONOR_COMMIT,
        source_path="kaggle/execute_region_talk_candidate_report.py",
        blob_sha="a046fd4ca3143f85c558e4a6a5caa62b410c3c6a",
        reused_contract="private dataset create, readiness/readback, kernel status/output and bounded polling",
        adaptation_reason=(
            "Retain the proven official KaggleApi calls, but remove the donor's DirectKaggleClient fallback, "
            "delete-and-recreate behavior and prefix cleanup; require persist-intent plus exact identities instead."
        ),
        compatibility_test="tests/provider/test_kaggle_adapter.py::test_official_224_calls_are_private_and_exact",
    ),
    DonorCompatibilityPin(
        donor_repository=DONOR_REPOSITORY,
        donor_commit=DONOR_COMMIT,
        source_path="video_announce/kaggle_client.py",
        blob_sha="9c552e12b001f7a1a3b213369a74da8ebd7a0b32",
        reused_contract="Kaggle save response normalization, status polling and output recovery",
        adaptation_reason=(
            "Normalize protobuf camelCase/snake_case fields as the donor does, while binding every output to an "
            "exact source version and task run receipt rather than accepting latest-by-slug output."
        ),
        compatibility_test="tests/provider/test_kaggle_adapter.py::test_output_rejects_stale_run_receipt",
    ),
    DonorCompatibilityPin(
        donor_repository=DONOR_REPOSITORY,
        donor_commit=DONOR_COMMIT,
        source_path="kaggle/kaggle_status_client.py",
        blob_sha="4f06b7c9fc35a1cc725df2bdea4815d999508acf",
        reused_contract="per-run callback identity, heartbeat and terminal runtime receipts",
        adaptation_reason=(
            "Provider output validation consumes the generic runtime receipt contract; event persistence and "
            "dedupe remain owned by the control/runtime lane."
        ),
        compatibility_test="tests/provider/test_kaggle_contracts.py::test_output_receipt_binds_exact_run_and_source",
    ),
    DonorCompatibilityPin(
        donor_repository=DONOR_REPOSITORY,
        donor_commit=DONOR_COMMIT,
        source_path="kaggle_status.py",
        blob_sha="00ecc610580cc691197cf72c323bb47d20eff373",
        reused_contract="callback-loss recovery, heartbeat coalescing and event UID ledger boundary",
        adaptation_reason=(
            "Only the provider-facing compatibility seam is reused here; SQLite/event-ledger ownership remains "
            "outside the adapter."
        ),
        compatibility_test="tests/provider/test_kaggle_contracts.py::test_persist_intent_interface_owns_no_ledger",
    ),
    DonorCompatibilityPin(
        donor_repository=DONOR_REPOSITORY,
        donor_commit=DONOR_COMMIT,
        source_path="kaggle/RegionTalkBgeM3Enrichment/region_talk_bge_m3_enrichment.py",
        blob_sha="0598777af0c91d9d716817666cfc45c81cda642c",
        reused_contract="BGE-M3 Kaggle model/runtime source and terminal artifact pattern",
        adaptation_reason=(
            "Pin the proven workload shape only; model execution is owned by L06 and all launches use this adapter."
        ),
        compatibility_test="tests/provider/test_kaggle_contracts.py::test_donor_inventory_is_complete_and_exact",
    ),
    DonorCompatibilityPin(
        donor_repository=DONOR_REPOSITORY,
        donor_commit=DONOR_COMMIT,
        source_path="kaggle/RegionTalkQwen3Embedding06BEnrichment/region_talk_qwen3_embedding_06b_enrichment.py",
        blob_sha="230c4f7ffebdd123239f29bc4df7050c12e57acb",
        reused_contract="E5-compatible embedding worker runtime and checkpointed artifact shape",
        adaptation_reason=(
            "Pin the donor runtime pattern without copying workload-specific transport or fallback logic."
        ),
        compatibility_test="tests/provider/test_kaggle_contracts.py::test_donor_inventory_is_complete_and_exact",
    ),
)


def compatibility_inventory() -> tuple[DonorCompatibilityPin, ...]:
    return DONOR_COMPATIBILITY_INVENTORY
