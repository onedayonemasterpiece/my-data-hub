"""Central verification contracts for no-secret Region Talk model-source smokes."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes, sha256_file

SHA256 = r"^[a-f0-9]{64}$"
SHA40 = r"^[a-f0-9]{40}$"
MODEL_SOURCE = r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$"
OFFICIAL_TREES_NAME = "text-model-official-trees.v1.json"
SEMANTIC_BANK_NAME = "semantic-bank.v1.json"
EXPECTED_MODEL_SOURCES = {
    "e5_embedding": "tanviranjumapurbo/multilingual-e5-base/Transformers/default/1",
    "bge_m3_embedding": "yethukmutt/bge-m3/Transformers/m3/1",
}
EXPECTED_DIMENSIONS = {"e5_embedding": 768, "bge_m3_embedding": 1024}


class TextAssetAcquisitionError(RuntimeError):
    """Fail-closed central acquisition verification error."""


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class OfficialModelFile(StrictModel):
    path: str = Field(min_length=1, max_length=500)
    byte_size: int = Field(gt=0, le=20 * 1024**3)
    git_oid: str = Field(pattern=SHA40)
    lfs_sha256: str | None = Field(default=None, pattern=SHA256)


class OfficialModelTree(StrictModel):
    stage: Literal["e5_embedding", "bge_m3_embedding"]
    model_id: str
    revision: str = Field(pattern=SHA40)
    official_api_url: str
    files: tuple[OfficialModelFile, ...] = Field(min_length=3, max_length=2_000)

    @model_validator(mode="after")
    def complete_sorted_tree(self) -> OfficialModelTree:
        paths = tuple(item.path for item in self.files)
        if paths != tuple(sorted(paths)) or len(set(paths)) != len(paths):
            raise ValueError("official model tree must be complete, unique, and sorted")
        expected_url = f"https://huggingface.co/api/models/{self.model_id}/tree/{self.revision}"
        if not self.official_api_url.startswith(expected_url):
            raise ValueError("official model tree URL differs from its exact identity")
        return self


class OfficialModelTrees(StrictModel):
    schema_version: Literal["region-talk-official-model-trees.v1"]
    source_contract: Literal["huggingface-api-tree-recursive-expand.v1"]
    entries: tuple[OfficialModelTree, ...] = Field(min_length=2, max_length=2)
    receipt_sha256: str = Field(pattern=SHA256)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt(cls, value: Any) -> Any:
        if isinstance(value, dict):
            unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
            if value.get("receipt_sha256") != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest():
                raise ValueError("official tree receipt differs")
        return value

    @model_validator(mode="after")
    def exact_stages(self) -> OfficialModelTrees:
        if tuple(item.stage for item in self.entries) != ("bge_m3_embedding", "e5_embedding"):
            raise ValueError("official model trees must contain both sorted text stages")
        return self


class ObservedModelFile(StrictModel):
    path: str = Field(min_length=1, max_length=500)
    byte_size: int = Field(gt=0, le=20 * 1024**3)
    sha256: str = Field(pattern=SHA256)
    git_blob_oid: str = Field(pattern=SHA40)


class FixedOutputObservation(StrictModel):
    tokenizer_output_sha256: str = Field(pattern=SHA256)
    dense_output_sha256: str = Field(pattern=SHA256)
    dimensions: int = Field(gt=0)
    norms: tuple[float, ...] = Field(min_length=2, max_length=2)


class CandidateSmokeObservation(StrictModel):
    stage: Literal["e5_embedding", "bge_m3_embedding"]
    model_source: str = Field(pattern=MODEL_SOURCE)
    model_root_name: str
    files: tuple[ObservedModelFile, ...] = Field(min_length=3, max_length=2_000)
    inventory_sha256: str = Field(pattern=SHA256)
    huggingface_provenance: dict[str, Any] | None
    fixed_output: FixedOutputObservation

    @model_validator(mode="after")
    def exact_observation(self) -> CandidateSmokeObservation:
        dumped = [item.model_dump(mode="json") for item in self.files]
        paths = tuple(item.path for item in self.files)
        if (
            self.model_source != EXPECTED_MODEL_SOURCES[self.stage]
            or paths != tuple(sorted(paths))
            or len(set(paths)) != len(paths)
            or self.inventory_sha256 != hashlib.sha256(canonical_json_bytes(dumped)).hexdigest()
            or self.fixed_output.dimensions != EXPECTED_DIMENSIONS[self.stage]
            or any(abs(norm - 1.0) > 1e-4 for norm in self.fixed_output.norms)
        ):
            raise ValueError("candidate smoke observation differs from exact contract")
        return self


class TextAssetSmokeObservation(StrictModel):
    schema_version: Literal["region-talk-text-asset-smoke-observation.v1"]
    task_run_id: str
    status: Literal["observed"]
    python_version: str
    internet_enabled: Literal[False]
    notebook_kaggle_credentials: Literal[False]
    distributions: dict[str, str] = Field(min_length=5, max_length=20)
    candidates: tuple[CandidateSmokeObservation, ...] = Field(min_length=2, max_length=2)

    @model_validator(mode="after")
    def exact_candidates(self) -> TextAssetSmokeObservation:
        if (
            tuple(item.stage for item in self.candidates) != ("e5_embedding", "bge_m3_embedding")
            or set(self.distributions)
            != {"FlagEmbedding", "safetensors", "tokenizers", "torch", "transformers"}
            or any(not value for value in self.distributions.values())
        ):
            raise ValueError("smoke candidates must contain E5 then BGE-M3 exactly")
        return self


class ModelTreeMismatch(StrictModel):
    path: str
    reason: Literal[
        "missing",
        "unexpected",
        "byte_size",
        "lfs_sha256",
        "git_blob_oid",
    ]
    expected: str | int | None
    observed: str | int | None


class ModelEquivalenceResult(StrictModel):
    stage: Literal["e5_embedding", "bge_m3_embedding"]
    model_source: str = Field(pattern=MODEL_SOURCE)
    official_model_id: str
    official_revision: str = Field(pattern=SHA40)
    status: Literal["byte_equivalent", "mismatch"]
    official_file_count: int = Field(ge=1)
    observed_file_count: int = Field(ge=1)
    observed_inventory_sha256: str = Field(pattern=SHA256)
    tokenizer_output_sha256: str = Field(pattern=SHA256)
    dense_output_sha256: str = Field(pattern=SHA256)
    mismatches: tuple[ModelTreeMismatch, ...] = Field(max_length=2_000)


class TextAssetAcquisitionReceipt(StrictModel):
    schema_version: Literal["region-talk-text-asset-acquisition-receipt.v1"] = (
        "region-talk-text-asset-acquisition-receipt.v1"
    )
    outcome: Literal["PASS", "PARTIAL", "MISMATCH"]
    observed_at: datetime
    provider_run_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    source_commit: str = Field(pattern=SHA40)
    source_sha256: str = Field(pattern=SHA256)
    image_identity: str = Field(pattern=r"^[^@\s]+@sha256:[a-f0-9]{64}$")
    image_source_commit: str = Field(pattern=SHA40)
    official_trees_sha256: str = Field(pattern=SHA256)
    semantic_bank_sha256: str = Field(pattern=SHA256)
    observation_sha256: str = Field(pattern=SHA256)
    results: tuple[ModelEquivalenceResult, ...] = Field(min_length=2, max_length=2)
    notebook_private: Literal[True] = True
    internet_enabled: Literal[False] = False
    notebook_kaggle_credentials: Literal[False] = False
    verified_by_central_adapter: Literal[True] = True
    publication_dispatch: Literal[False] = False
    notification_dispatch: Literal[False] = False

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


def _read_canonical(path: Path) -> tuple[dict[str, Any], bytes]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise TextAssetAcquisitionError("acquisition contract file is absent or unsafe")
    body = path.read_bytes()
    try:
        value = json.loads(body)
    except ValueError as exc:
        raise TextAssetAcquisitionError("acquisition contract JSON is invalid") from exc
    if body != canonical_json_bytes(value) + b"\n":
        raise TextAssetAcquisitionError("acquisition contract JSON is not canonical")
    return value, body


def read_official_model_trees(path: Path | None = None) -> OfficialModelTrees:
    path = path or Path(__file__).with_name("assets") / OFFICIAL_TREES_NAME
    value, _body = _read_canonical(path)
    try:
        return OfficialModelTrees.model_validate(value)
    except ValueError as exc:
        raise TextAssetAcquisitionError("official model tree contract is invalid") from exc


def compare_smoke_to_official(
    observation: TextAssetSmokeObservation,
    official: OfficialModelTrees,
) -> tuple[ModelEquivalenceResult, ...]:
    by_stage = {entry.stage: entry for entry in official.entries}
    results: list[ModelEquivalenceResult] = []
    for candidate in observation.candidates:
        expected_tree = by_stage[candidate.stage]
        expected = {item.path: item for item in expected_tree.files}
        observed = {item.path: item for item in candidate.files}
        mismatches: list[ModelTreeMismatch] = []
        for path in sorted(expected.keys() - observed.keys()):
            mismatches.append(ModelTreeMismatch(path=path, reason="missing", expected="present", observed=None))
        for path in sorted(observed.keys() - expected.keys()):
            mismatches.append(ModelTreeMismatch(path=path, reason="unexpected", expected=None, observed="present"))
        for path in sorted(expected.keys() & observed.keys()):
            wanted, found = expected[path], observed[path]
            if wanted.byte_size != found.byte_size:
                mismatches.append(
                    ModelTreeMismatch(
                        path=path,
                        reason="byte_size",
                        expected=wanted.byte_size,
                        observed=found.byte_size,
                    )
                )
            if wanted.lfs_sha256 is not None and wanted.lfs_sha256 != found.sha256:
                mismatches.append(
                    ModelTreeMismatch(
                        path=path,
                        reason="lfs_sha256",
                        expected=wanted.lfs_sha256,
                        observed=found.sha256,
                    )
                )
            if wanted.lfs_sha256 is None and wanted.git_oid != found.git_blob_oid:
                mismatches.append(
                    ModelTreeMismatch(
                        path=path,
                        reason="git_blob_oid",
                        expected=wanted.git_oid,
                        observed=found.git_blob_oid,
                    )
                )
        results.append(
            ModelEquivalenceResult(
                stage=candidate.stage,
                model_source=candidate.model_source,
                official_model_id=expected_tree.model_id,
                official_revision=expected_tree.revision,
                status="mismatch" if mismatches else "byte_equivalent",
                official_file_count=len(expected),
                observed_file_count=len(observed),
                observed_inventory_sha256=candidate.inventory_sha256,
                tokenizer_output_sha256=candidate.fixed_output.tokenizer_output_sha256,
                dense_output_sha256=candidate.fixed_output.dense_output_sha256,
                mismatches=tuple(mismatches),
            )
        )
    return tuple(results)


def official_trees_sha256(path: Path | None = None) -> str:
    path = path or Path(__file__).with_name("assets") / OFFICIAL_TREES_NAME
    return sha256_file(path)


__all__ = [
    "EXPECTED_MODEL_SOURCES",
    "CandidateSmokeObservation",
    "FixedOutputObservation",
    "ModelEquivalenceResult",
    "ModelTreeMismatch",
    "ObservedModelFile",
    "OfficialModelFile",
    "OfficialModelTree",
    "OfficialModelTrees",
    "TextAssetAcquisitionError",
    "TextAssetAcquisitionReceipt",
    "TextAssetSmokeObservation",
    "compare_smoke_to_official",
    "official_trees_sha256",
    "read_official_model_trees",
]
