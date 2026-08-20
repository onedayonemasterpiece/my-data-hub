"""Offline, exact-asset E5/BGE runtimes for private Region Talk workers.

The committed registry deliberately has no verified model bundle hashes yet.  A
runtime is discoverable only after an owner acquires each exact upstream
snapshot outside the worker, builds a complete manifest, reviews its hashes,
and updates the registry.  Kaggle execution itself never downloads a model.
"""

from __future__ import annotations

import hashlib
import importlib.metadata
import json
import math
import os
import re
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.models import (
    BGE_M3,
    E5_MULTILINGUAL_BASE,
    EmbeddingModelContract,
)
from my_data_hub.hashing import canonical_json_bytes, sha256_file

from .stage_dispatch import StageExecutionPayload
from .transforms.evidence import (
    ALL_LABELS,
    BGE_M3_CONTRACT,
    E5_CONTRACT,
    SEMANTIC_BANK_HASH,
    SEMANTIC_BANK_VERSION,
    vector_evidence_fingerprint,
)

SHA256_RE = r"^[a-f0-9]{64}$"
MODEL_SOURCE_RE = (
    r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/"
    r"[A-Za-z0-9_.-]+/[1-9][0-9]*$"
)
KERNEL_SOURCE_RE = r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$"
REGISTRY_NAME = "text-runtime-assets.v1.json"
E5_PRODUCER_AUTHORITY_NAME = "region-talk-e5-frozen-producer-authority.v1.json"
TEXT_STAGES = frozenset({"e5_embedding", "bge_m3_embedding"})
_MAX_MANIFEST_BYTES = 2 * 1024 * 1024
_MAX_SEMANTIC_BANK_BYTES = 1024 * 1024
_MAX_MODEL_FILES = 2_000
_MAX_MODEL_BYTES = 20 * 1024**3


class TextRuntimeAssetError(RuntimeError):
    """Fail-closed asset or runtime contract error."""


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TextRuntimeFile(_StrictModel):
    relative_path: str = Field(pattern=r"^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$", max_length=500)
    sha256: str = Field(pattern=SHA256_RE)
    byte_size: int = Field(gt=0, le=_MAX_MODEL_BYTES)


class SemanticBankEntry(_StrictModel):
    label: str = Field(pattern=r"^[a-z][a-z0-9_]{1,99}$")
    examples: tuple[str, ...] = Field(min_length=2, max_length=10)

    @model_validator(mode="after")
    def exact_examples(self) -> SemanticBankEntry:
        if self.label not in ALL_LABELS:
            raise ValueError("semantic-bank label is not in the fixed evidence contract")
        if len(set(self.examples)) != len(self.examples) or any(
            not text or len(text) > 4_000 for text in self.examples
        ):
            raise ValueError("semantic-bank examples must be bounded and unique")
        return self


class SemanticBankDocument(_StrictModel):
    schema_version: Literal["region-talk-semantic-bank.v1"]
    semantic_bank_version: Literal["semantic_bank_v1"]
    semantic_bank_sha256: str = Field(pattern=SHA256_RE)
    donor_repository: str
    donor_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    donor_path: str
    donor_blob_git_oid: str = Field(pattern=r"^[a-f0-9]{40}$")
    entries: tuple[SemanticBankEntry, ...] = Field(min_length=len(ALL_LABELS), max_length=len(ALL_LABELS))
    receipt_sha256: str = Field(pattern=SHA256_RE)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt(cls, value: Any) -> Any:
        if isinstance(value, dict):
            unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
            if value.get("receipt_sha256") != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest():
                raise ValueError("semantic-bank receipt differs")
        return value

    @model_validator(mode="after")
    def exact_labels(self) -> SemanticBankDocument:
        labels = tuple(entry.label for entry in self.entries)
        logical_bank = {entry.label: list(entry.examples) for entry in self.entries}
        if (
            labels != tuple(sorted(ALL_LABELS))
            or self.semantic_bank_sha256
            != hashlib.sha256(canonical_json_bytes(logical_bank)).hexdigest()
        ):
            raise ValueError("semantic-bank labels must be complete, unique, and sorted")
        return self


class TextRuntimeModelIdentity(_StrictModel):
    model_id: str
    revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    dimensions: int = Field(gt=0)
    max_tokens: int = Field(gt=0)
    pooling: str
    normalization: Literal["l2"]
    query_prefix: str
    document_prefix: str
    encoder_contract: str

    @classmethod
    def from_model(cls, model: EmbeddingModelContract) -> TextRuntimeModelIdentity:
        return cls(
            model_id=model.model_key,
            revision=model.revision,
            dimensions=model.dimensions,
            max_tokens=model.max_tokens,
            pooling=model.pooling,
            normalization=model.normalization,
            query_prefix=model.query_prefix,
            document_prefix=model.document_prefix,
            encoder_contract=model.encoder_contract_version,
        )


class TextRuntimeAssetManifest(_StrictModel):
    schema_version: Literal["region-talk-text-runtime-assets.v1"]
    stage: Literal["e5_embedding", "bge_m3_embedding"]
    stage_contract_version: Literal[
        "e5_semantic_bank_scores_v1", "bge_m3_flagembedding_dense_v1"
    ]
    model: TextRuntimeModelIdentity
    model_directory: str = Field(
        pattern=r"^[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*$", max_length=400
    )
    model_files: tuple[TextRuntimeFile, ...] = Field(min_length=3, max_length=_MAX_MODEL_FILES)
    semantic_bank_file: TextRuntimeFile
    semantic_bank_version: Literal["semantic_bank_v1"]
    semantic_bank_sha256: str = Field(pattern=SHA256_RE)
    runtime_source_sha256: str = Field(pattern=SHA256_RE)
    required_distributions: dict[str, str] = Field(min_length=2, max_length=20)
    provider_model_source: str | None = Field(default=None, pattern=MODEL_SOURCE_RE)
    provider_kernel_source: str | None = Field(default=None, pattern=KERNEL_SOURCE_RE)
    producer_authority_sha256: str | None = Field(default=None, pattern=SHA256_RE)
    official_tree_receipt_sha256: str | None = Field(default=None, pattern=SHA256_RE)
    excluded_nonruntime_paths: tuple[str, ...] = Field(default=(), max_length=20)
    receipt_sha256: str = Field(pattern=SHA256_RE)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt(cls, value: Any) -> Any:
        if isinstance(value, dict):
            unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
            if value.get("receipt_sha256") != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest():
                raise ValueError("text-runtime asset receipt differs")
        return value

    @model_validator(mode="after")
    def exact_contract(self) -> TextRuntimeAssetManifest:
        expected_model, expected_contract = _stage_contract(self.stage)
        if (
            self.stage_contract_version != expected_contract
            or self.model != TextRuntimeModelIdentity.from_model(expected_model)
            or self.semantic_bank_version != SEMANTIC_BANK_VERSION
            or len({item.relative_path for item in self.model_files}) != len(self.model_files)
            or any(
                not item.relative_path.startswith(self.model_directory + "/")
                for item in self.model_files
            )
            or tuple(item.relative_path for item in self.model_files)
            != tuple(sorted(item.relative_path for item in self.model_files))
            or self.semantic_bank_file.relative_path
            in {item.relative_path for item in self.model_files}
            or any(not re.fullmatch(r"[A-Za-z0-9_.+-]+", name) for name in self.required_distributions)
            or any(not value or any(char.isspace() for char in value) for value in self.required_distributions.values())
        ):
            raise ValueError("text-runtime asset manifest differs from the fixed stage contract")
        names = {Path(item.relative_path).name for item in self.model_files}
        if "config.json" not in names or not ({"tokenizer.json", "sentencepiece.bpe.model"} & names):
            raise ValueError("text-runtime snapshot lacks config/tokenizer inventory")
        expected_weight = "model.safetensors" if self.stage == "e5_embedding" else "pytorch_model.bin"
        if expected_weight not in names:
            raise ValueError("text-runtime snapshot lacks its exact model weights")
        if self.provider_model_source is not None and (
            self.stage != "bge_m3_embedding"
            or self.provider_model_source != "yethukmutt/bge-m3/Transformers/m3/1"
            or self.official_tree_receipt_sha256
            != "526c363c7abfa3c60eed26ab559885a29cb23384abd06f4ead6beef636d3c418"
            or self.excluded_nonruntime_paths != ("imgs/.DS_Store",)
        ):
            raise ValueError("text-runtime acquired-model contract differs")
        e5_acquisition = (self.provider_kernel_source, self.producer_authority_sha256)
        if any(e5_acquisition) and (
            self.stage != "e5_embedding"
            or self.provider_kernel_source != "zigomaro/mdh-region-talk-e5-assets-v1"
            or self.producer_authority_sha256 is None
            or self.official_tree_receipt_sha256
            != "a9bf9a773342bb1593801f34bdd8d230b44c4a934842deea0b444ad5371aae70"
            or self.excluded_nonruntime_paths
        ):
            raise ValueError("text-runtime frozen-producer contract differs")
        if self.provider_model_source is not None and self.provider_kernel_source is not None:
            raise ValueError("text runtime must use exactly one provider asset carrier")
        return self


class TextRuntimeRegistryEntry(_StrictModel):
    stage: Literal["e5_embedding", "bge_m3_embedding"]
    availability: Literal["external_assets_required", "verified"]
    manifest_filename: str = Field(pattern=r"^region-talk-(?:e5|bge-m3)-assets\.v1\.json$")
    manifest_sha256: str | None = Field(default=None, pattern=SHA256_RE)
    model_source: str | None = Field(default=None, pattern=MODEL_SOURCE_RE)
    kernel_source: str | None = Field(default=None, pattern=KERNEL_SOURCE_RE)
    producer_authority_sha256: str | None = Field(default=None, pattern=SHA256_RE)
    model: TextRuntimeModelIdentity

    @model_validator(mode="after")
    def consistent_availability(self) -> TextRuntimeRegistryEntry:
        carrier_count = int(self.model_source is not None) + int(self.kernel_source is not None)
        if (self.availability == "verified") != (
            self.manifest_sha256 is not None and carrier_count == 1
        ) or (
            self.availability == "external_assets_required"
            and (carrier_count or self.producer_authority_sha256 is not None)
        ) or ((self.kernel_source is not None) != (self.producer_authority_sha256 is not None)):
            raise ValueError("text-runtime registry availability/hash differ")
        expected, _contract = _stage_contract(self.stage)
        if self.model != TextRuntimeModelIdentity.from_model(expected):
            raise ValueError("text-runtime registry model differs")
        return self


class TextRuntimeRegistry(_StrictModel):
    schema_version: Literal["region-talk-text-runtime-registry.v1"]
    entries: tuple[TextRuntimeRegistryEntry, ...] = Field(min_length=2, max_length=2)
    receipt_sha256: str = Field(pattern=SHA256_RE)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt(cls, value: Any) -> Any:
        if isinstance(value, dict):
            unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
            if value.get("receipt_sha256") != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest():
                raise ValueError("text-runtime registry receipt differs")
        return value

    @model_validator(mode="after")
    def exact_entries(self) -> TextRuntimeRegistry:
        if tuple(item.stage for item in self.entries) != ("bge_m3_embedding", "e5_embedding"):
            raise ValueError("text-runtime registry entries must be complete and sorted")
        return self


class TextRuntimeRegistrationMetadata(_StrictModel):
    """Exact runtime-owned fields for the master-owned 0030 registration port.

    Runtime image identity/source commit intentionally remain caller-owned.
    """

    stage: Literal["e5_embedding", "bge_m3_embedding"]
    contract_version: Literal[
        "e5_semantic_bank_scores_v1", "bge_m3_flagembedding_dense_v1"
    ]
    model_id: str
    model_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    encoder_contract: str
    semantic_bank_version: Literal["semantic_bank_v1"]
    semantic_bank_sha256: str = Field(pattern=SHA256_RE)
    runtime_source_sha256: str = Field(pattern=SHA256_RE)
    asset_manifest_sha256: str = Field(pattern=SHA256_RE)


class TextRuntimePinReceipt(_StrictModel):
    """Master-owned 0030 identity carried only inside the private 0028 payload."""

    schema_version: Literal["region-talk-stage-runtime-pin-receipt.v1"]
    registered: Literal[True]
    stage: Literal["e5_embedding", "bge_m3_embedding"]
    contract_version: Literal[
        "e5_semantic_bank_scores_v1", "bge_m3_flagembedding_dense_v1"
    ]
    effective_canonical_revision: int = Field(ge=1)
    pin_generation: int = Field(ge=1)
    master_instance_id: UUID
    epoch: int = Field(ge=1)
    model_id: str = Field(min_length=1, max_length=300)
    model_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    encoder_contract: str = Field(min_length=1, max_length=300)
    semantic_bank_version: Literal["semantic_bank_v1"]
    semantic_bank_sha256: str = Field(pattern=SHA256_RE)
    runtime_source_sha256: str = Field(pattern=SHA256_RE)
    asset_manifest_sha256: str = Field(pattern=SHA256_RE)
    provider_image_identity: str = Field(pattern=r"^[^@\s]+@sha256:[a-f0-9]{64}$")
    provider_image_source_commit: str = Field(pattern=r"^[a-f0-9]{40}$")
    producer_exact_id: str = Field(min_length=1, max_length=2_000)
    prior_pin_receipt_sha256: str | None = Field(default=None, pattern=SHA256_RE)
    pin_sha256: str = Field(pattern=SHA256_RE)
    publication_dispatch: Literal[False]
    notification_dispatch: Literal[False]
    receipt_sha256: str = Field(pattern=SHA256_RE)

    @model_validator(mode="before")
    @classmethod
    def exact_receipt(cls, value: Any) -> Any:
        if isinstance(value, dict):
            unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
            if value.get("receipt_sha256") != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest():
                raise ValueError("runtime-pin receipt_sha256 differs")
        return value

    @model_validator(mode="after")
    def exact_pin(self) -> TextRuntimePinReceipt:
        expected_producer = (
            f"{self.model_id}@{self.model_revision}"
            f"+assets:{self.asset_manifest_sha256}"
            f"+source:{self.runtime_source_sha256}"
            f"+image:{self.provider_image_identity}"
            f"+commit:{self.provider_image_source_commit}"
        )
        pin_base = {
            "stage": self.stage,
            "contract_version": self.contract_version,
            "effective_canonical_revision": self.effective_canonical_revision,
            "pin_generation": self.pin_generation,
            "master_instance_id": str(self.master_instance_id),
            "epoch": self.epoch,
            "model_id": self.model_id,
            "model_revision": self.model_revision,
            "encoder_contract": self.encoder_contract,
            "semantic_bank_version": self.semantic_bank_version,
            "semantic_bank_sha256": self.semantic_bank_sha256,
            "runtime_source_sha256": self.runtime_source_sha256,
            "asset_manifest_sha256": self.asset_manifest_sha256,
            "provider_image_identity": self.provider_image_identity,
            "provider_image_source_commit": self.provider_image_source_commit,
            "producer_exact_id": self.producer_exact_id,
        }
        if (
            self.producer_exact_id != expected_producer
            or self.pin_sha256 != hashlib.sha256(canonical_json_bytes(pin_base)).hexdigest()
            or (self.pin_generation == 1) != (self.prior_pin_receipt_sha256 is None)
        ):
            raise ValueError("runtime-pin identity hash differs")
        return self


@dataclass(frozen=True, slots=True)
class VerifiedTextRuntimeAssets:
    manifest: TextRuntimeAssetManifest
    manifest_sha256: str
    bundle_root: Path
    model_root: Path
    semantic_bank: SemanticBankDocument


class TextVectorEncoder(Protocol):
    def encode(
        self,
        texts: Sequence[str],
        *,
        model: EmbeddingModelContract,
        max_tokens: int,
        pooling: str,
        normalize: bool,
        dense_only: bool,
    ) -> Sequence[Sequence[float]]: ...


def _stage_contract(stage: str) -> tuple[EmbeddingModelContract, str]:
    if stage == "e5_embedding":
        return E5_MULTILINGUAL_BASE, E5_CONTRACT
    if stage == "bge_m3_embedding":
        return BGE_M3, BGE_M3_CONTRACT
    raise ValueError("text runtime supports only fixed E5/BGE stages")


def runtime_source_sha256() -> str:
    return sha256_file(Path(__file__).resolve())


def _safe_file(root: Path, relative_path: str, *, maximum: int) -> Path:
    candidate = root.joinpath(*relative_path.split("/"))
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
    except (OSError, ValueError) as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_ASSET_PATH_UNSAFE") from exc
    if (
        candidate.is_symlink()
        or not candidate.is_file()
        or resolved != candidate.absolute()
        or candidate.stat().st_size <= 0
        or candidate.stat().st_size > maximum
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_ASSET_PATH_UNSAFE")
    return candidate


def read_text_runtime_registry(path: Path | None = None) -> TextRuntimeRegistry:
    path = path or Path(__file__).with_name("assets") / REGISTRY_NAME
    if path.is_symlink() or not path.is_file() or path.stat().st_size > _MAX_MANIFEST_BYTES:
        raise TextRuntimeAssetError("TEXT_RUNTIME_REGISTRY_UNAVAILABLE")
    body = path.read_bytes()
    try:
        value = json.loads(body)
    except ValueError as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_REGISTRY_INVALID") from exc
    if body != canonical_json_bytes(value) + b"\n":
        raise TextRuntimeAssetError("TEXT_RUNTIME_REGISTRY_NOT_CANONICAL")
    try:
        return TextRuntimeRegistry.model_validate(value)
    except ValueError as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_REGISTRY_INVALID") from exc


def read_e5_frozen_producer_authority(path: Path | None = None) -> dict[str, Any]:
    """Read the committed, metadata-only authority for the immutable producer."""

    path = path or Path(__file__).with_name("assets") / E5_PRODUCER_AUTHORITY_NAME
    value, body = _read_canonical_runtime_json(path)
    unsigned = {key: item for key, item in value.items() if key != "authority_sha256"}
    required = {
        "schema_version",
        "provider_ref",
        "provider_version",
        "provider_kernel_id",
        "provider_run_ref",
        "task_run_id",
        "source_commit",
        "source_sha256",
        "image_identity",
        "image_source_commit",
        "claim",
        "producer_receipt",
        "publication_dispatch",
        "notification_dispatch",
        "authority_sha256",
    }
    if (
        set(value) != required
        or value.get("schema_version") != "region-talk-e5-frozen-producer-authority.v1"
        or value.get("provider_ref") != "zigomaro/mdh-region-talk-e5-assets-v1"
        or value.get("provider_version") != 1
        or value.get("provider_run_ref") != f"{value.get('provider_ref')}/1"
        or value.get("publication_dispatch") is not False
        or value.get("notification_dispatch") is not False
        or value.get("authority_sha256")
        != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
        or body != canonical_json_bytes(value) + b"\n"
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID")
    try:
        from my_data_hub.providers.kaggle.contracts import TaskResourceClaim

        claim = TaskResourceClaim.model_validate(value["claim"])
    except Exception as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID") from exc
    receipt = value.get("producer_receipt")
    if not isinstance(receipt, dict):
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID")
    receipt_unsigned = {key: item for key, item in receipt.items() if key != "receipt_sha256"}
    official_path = Path(__file__).with_name("assets") / "text-model-official-trees.v1.json"
    official, official_body = _read_canonical_runtime_json(official_path)
    official_unsigned = {key: item for key, item in official.items() if key != "receipt_sha256"}
    entries = official.get("entries")
    e5_entries = (
        [item for item in entries if isinstance(item, dict) and item.get("stage") == "e5_embedding"]
        if isinstance(entries, list)
        else []
    )
    receipt_files = receipt.get("files")
    if len(e5_entries) != 1 or not isinstance(receipt_files, list):
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID")
    official_e5 = e5_entries[0]
    expected_files = official_e5.get("files")
    if not isinstance(expected_files, list) or len(expected_files) != 23 or len(receipt_files) != 23:
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID")
    expected_by_path = {item.get("path"): item for item in expected_files if isinstance(item, dict)}
    observed_by_path = {item.get("path"): item for item in receipt_files if isinstance(item, dict)}
    file_contract_valid = set(expected_by_path) == set(observed_by_path) and len(observed_by_path) == 23
    for relative, expected in expected_by_path.items():
        observed = observed_by_path.get(relative, {})
        if (
            set(observed) != {"path", "byte_size", "sha256", "git_blob_oid"}
            or observed.get("byte_size") != expected.get("byte_size")
            or (
                expected.get("lfs_sha256") is not None
                and observed.get("sha256") != expected.get("lfs_sha256")
            )
            or (
                expected.get("lfs_sha256") is None
                and observed.get("git_blob_oid") != expected.get("git_oid")
            )
        ):
            file_contract_valid = False
    if (
        not file_contract_valid
        or official_body != canonical_json_bytes(official) + b"\n"
        or official.get("receipt_sha256")
        != hashlib.sha256(canonical_json_bytes(official_unsigned)).hexdigest()
        or receipt.get("schema_version") != "region-talk-e5-frozen-producer-receipt.v1"
        or receipt.get("receipt_sha256")
        != hashlib.sha256(canonical_json_bytes(receipt_unsigned)).hexdigest()
        or receipt.get("inventory_sha256")
        != hashlib.sha256(canonical_json_bytes(receipt_files)).hexdigest()
        or receipt.get("model_id") != official_e5.get("model_id")
        or receipt.get("model_revision") != official_e5.get("revision")
        or receipt.get("official_tree_receipt_sha256") != official.get("receipt_sha256")
        or receipt.get("semantic_bank_sha256")
        != "4ec81e6ede79f3dae1bb366a06366e7197d960e1c04e124f77b3db12f2f1981f"
        or receipt.get("notebook_kaggle_credentials") is not False
        or receipt.get("publication_dispatch") is not False
        or receipt.get("notification_dispatch") is not False
        or receipt.get("task_run_id") != value.get("task_run_id")
        or receipt.get("source_commit") != value.get("source_commit")
        or str(claim.task_id) != value.get("task_run_id")
        or claim.provider_ref != value.get("provider_ref")
        or claim.provider_version != value.get("provider_version")
        or claim.kind.value != "notebook"
        or claim.control_class.value != "orchestrator_protected"
        or claim.disposable
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID")
    return value


def _read_canonical_runtime_json(path: Path) -> tuple[dict[str, Any], bytes]:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > _MAX_MANIFEST_BYTES:
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_UNAVAILABLE")
    body = path.read_bytes()
    try:
        value = json.loads(body)
    except ValueError as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID") from exc
    if not isinstance(value, dict):
        raise TextRuntimeAssetError("TEXT_RUNTIME_E5_PRODUCER_AUTHORITY_INVALID")
    return value, body


def build_text_runtime_asset_manifest(
    *,
    stage: str,
    bundle_root: Path,
    model_directory: str,
    semantic_bank_relative_path: str,
    required_distributions: Mapping[str, str],
    expected_semantic_bank_sha256: str = SEMANTIC_BANK_HASH,
) -> bytes:
    """Build a deterministic complete inventory from already-acquired offline assets."""

    model, contract = _stage_contract(stage)
    root = bundle_root.resolve(strict=True)
    model_root = root.joinpath(*model_directory.split("/"))
    try:
        if model_root.is_symlink() or not model_root.is_dir():
            raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE")
        model_root.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE") from exc
    files: list[TextRuntimeFile] = []
    total = 0
    for path in sorted(model_root.rglob("*")):
        if path.is_dir():
            if path.is_symlink():
                raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE")
            continue
        if path.is_symlink() or not path.is_file():
            raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE")
        relative = path.relative_to(root).as_posix()
        size = path.stat().st_size
        total += size
        files.append(TextRuntimeFile(relative_path=relative, sha256=sha256_file(path), byte_size=size))
    if not 3 <= len(files) <= _MAX_MODEL_FILES or total > _MAX_MODEL_BYTES:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_INVENTORY_INVALID")
    semantic = _safe_file(root, semantic_bank_relative_path, maximum=_MAX_SEMANTIC_BANK_BYTES)
    semantic_sha = sha256_file(semantic)
    semantic_body = semantic.read_bytes()
    semantic_value = json.loads(semantic_body)
    if semantic_body != canonical_json_bytes(semantic_value) + b"\n":
        raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_NOT_CANONICAL")
    semantic_bank = SemanticBankDocument.model_validate(semantic_value)
    if semantic_bank.semantic_bank_sha256 != expected_semantic_bank_sha256:
        raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_HASH_MISMATCH")
    unsigned = {
        "schema_version": "region-talk-text-runtime-assets.v1",
        "stage": stage,
        "stage_contract_version": contract,
        "model": TextRuntimeModelIdentity.from_model(model).model_dump(mode="json"),
        "model_directory": model_directory,
        "model_files": [item.model_dump(mode="json") for item in files],
        "semantic_bank_file": TextRuntimeFile(
            relative_path=semantic_bank_relative_path,
            sha256=semantic_sha,
            byte_size=semantic.stat().st_size,
        ).model_dump(mode="json"),
        "semantic_bank_version": SEMANTIC_BANK_VERSION,
        "semantic_bank_sha256": semantic_bank.semantic_bank_sha256,
        "runtime_source_sha256": runtime_source_sha256(),
        "required_distributions": dict(sorted(required_distributions.items())),
        "provider_model_source": None,
        "provider_kernel_source": None,
        "producer_authority_sha256": None,
        "official_tree_receipt_sha256": None,
        "excluded_nonruntime_paths": [],
    }
    value = {
        **unsigned,
        "receipt_sha256": hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest(),
    }
    TextRuntimeAssetManifest.model_validate(value)
    return canonical_json_bytes(value) + b"\n"


def verify_text_runtime_asset_bundle(
    *,
    bundle_root: Path,
    manifest_path: Path,
    expected_manifest_sha256: str,
    expected_stage: str,
    expected_semantic_bank_sha256: str = SEMANTIC_BANK_HASH,
    version_resolver: Callable[[str], str] = importlib.metadata.version,
    model_root_override: Path | None = None,
    semantic_bank_path_override: Path | None = None,
) -> VerifiedTextRuntimeAssets:
    root = bundle_root.resolve(strict=True)
    try:
        manifest_path.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_PATH_UNSAFE") from exc
    if (
        manifest_path.is_symlink()
        or not manifest_path.is_file()
        or manifest_path.stat().st_size > _MAX_MANIFEST_BYTES
        or sha256_file(manifest_path) != expected_manifest_sha256
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_HASH_MISMATCH")
    body = manifest_path.read_bytes()
    try:
        value = json.loads(body)
    except ValueError as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_INVALID") from exc
    if body != canonical_json_bytes(value) + b"\n":
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_NOT_CANONICAL")
    try:
        manifest = TextRuntimeAssetManifest.model_validate(value)
    except ValueError as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_INVALID") from exc
    if (
        manifest.stage != expected_stage
        or manifest.semantic_bank_sha256 != expected_semantic_bank_sha256
        or manifest.runtime_source_sha256 != runtime_source_sha256()
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_BINDING_MISMATCH")
    listed = {item.relative_path for item in manifest.model_files}
    model_root = (
        model_root_override.resolve(strict=True)
        if model_root_override is not None
        else root.joinpath(*manifest.model_directory.split("/"))
    )
    try:
        if model_root.is_symlink() or not model_root.is_dir():
            raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE")
        if model_root_override is None:
            model_root.resolve(strict=True).relative_to(root)
    except (OSError, ValueError) as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE") from exc
    observed: set[str] = set()
    for path in model_root.rglob("*"):
        if path.is_symlink() or (not path.is_file() and not path.is_dir()):
            raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_UNSAFE")
        if path.is_file():
            observed.add(
                f"{manifest.model_directory}/{path.relative_to(model_root).as_posix()}"
                if model_root_override is not None
                else path.relative_to(root).as_posix()
            )
    if observed != listed:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_INVENTORY_MISMATCH")
    total = 0
    for item in manifest.model_files:
        relative = item.relative_path
        file_root = root
        if model_root_override is not None:
            relative = relative.removeprefix(manifest.model_directory + "/")
            file_root = model_root
        path = _safe_file(file_root, relative, maximum=_MAX_MODEL_BYTES)
        total += path.stat().st_size
        if path.stat().st_size != item.byte_size or sha256_file(path) != item.sha256:
            raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_FILE_MISMATCH")
    if total > _MAX_MODEL_BYTES:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_INVENTORY_INVALID")
    semantic_path = (
        semantic_bank_path_override.resolve(strict=True)
        if semantic_bank_path_override is not None
        else _safe_file(
            root,
            manifest.semantic_bank_file.relative_path,
            maximum=_MAX_SEMANTIC_BANK_BYTES,
        )
    )
    if semantic_bank_path_override is not None and (
        semantic_path.is_symlink()
        or not semantic_path.is_file()
        or semantic_path.stat().st_size > _MAX_SEMANTIC_BANK_BYTES
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_HASH_MISMATCH")
    if (
        semantic_path.stat().st_size != manifest.semantic_bank_file.byte_size
        or sha256_file(semantic_path) != manifest.semantic_bank_file.sha256
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_HASH_MISMATCH")
    semantic_body = semantic_path.read_bytes()
    try:
        semantic_value = json.loads(semantic_body)
        if semantic_body != canonical_json_bytes(semantic_value) + b"\n":
            raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_NOT_CANONICAL")
        semantic_bank = SemanticBankDocument.model_validate(semantic_value)
    except ValueError as exc:
        raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_INVALID") from exc
    if semantic_bank.semantic_bank_sha256 != expected_semantic_bank_sha256:
        raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_HASH_MISMATCH")
    for distribution, expected in manifest.required_distributions.items():
        try:
            observed_version = version_resolver(distribution)
        except Exception as exc:
            raise TextRuntimeAssetError("TEXT_RUNTIME_DEPENDENCY_UNAVAILABLE") from exc
        if observed_version != expected:
            raise TextRuntimeAssetError("TEXT_RUNTIME_DEPENDENCY_VERSION_MISMATCH")
    return VerifiedTextRuntimeAssets(
        manifest=manifest,
        manifest_sha256=expected_manifest_sha256,
        bundle_root=root,
        model_root=model_root,
        semantic_bank=semantic_bank,
    )


@dataclass(slots=True)
class VerifiedTextStageRuntime:
    assets: VerifiedTextRuntimeAssets
    encoder: TextVectorEncoder
    _master_instance_id: UUID | None = field(default=None, init=False, repr=False)
    _epoch: int | None = field(default=None, init=False, repr=False)
    _runtime_pin: TextRuntimePinReceipt | None = field(default=None, init=False, repr=False)

    @property
    def producer_exact_id(self) -> str:
        if self._runtime_pin is None:
            raise TextRuntimeAssetError("TEXT_RUNTIME_PIN_NOT_VERIFIED")
        return self._runtime_pin.producer_exact_id

    def bind_worker_capability(self, *, master_instance_id: UUID, epoch: int) -> None:
        """Bind the private 0028 receipt identity before processing its payload."""

        if epoch < 1:
            raise ValueError("worker capability epoch is invalid")
        if self._master_instance_id not in {None, master_instance_id} or self._epoch not in {
            None,
            epoch,
        }:
            raise ValueError("worker capability changed during one runtime")
        self._master_instance_id = master_instance_id
        self._epoch = epoch

    @property
    def registration_metadata(self) -> TextRuntimeRegistrationMetadata:
        manifest = self.assets.manifest
        return TextRuntimeRegistrationMetadata(
            stage=manifest.stage,
            contract_version=manifest.stage_contract_version,
            model_id=manifest.model.model_id,
            model_revision=manifest.model.revision,
            encoder_contract=manifest.model.encoder_contract,
            semantic_bank_version=manifest.semantic_bank_version,
            semantic_bank_sha256=manifest.semantic_bank_sha256,
            runtime_source_sha256=manifest.runtime_source_sha256,
            asset_manifest_sha256=self.assets.manifest_sha256,
        )

    def execute(
        self,
        *,
        stage: str,
        contract_version: str,
        subject_id: UUID,
        input_fingerprint: str,
        payload: StageExecutionPayload,
    ) -> dict[str, Any]:
        del subject_id
        model, expected_contract = _stage_contract(stage)
        if (
            stage != self.assets.manifest.stage
            or contract_version != expected_contract
            or payload.input_fingerprint != input_fingerprint
            or payload.input_data.get("schema_version") != "region-talk-stage-text-input.v1"
        ):
            raise ValueError("text runtime input differs from its exact stage binding")
        if self._master_instance_id is None or self._epoch is None:
            raise ValueError("text runtime lacks its private worker capability binding")
        try:
            runtime_pin = TextRuntimePinReceipt.model_validate(payload.input_data.get("runtime_pin"))
        except ValueError as exc:
            raise ValueError("text runtime pin is invalid") from exc
        manifest = self.assets.manifest
        if (
            runtime_pin.stage != stage
            or runtime_pin.contract_version != contract_version
            or runtime_pin.master_instance_id != self._master_instance_id
            or runtime_pin.epoch != self._epoch
            or runtime_pin.model_id != manifest.model.model_id
            or runtime_pin.model_revision != manifest.model.revision
            or runtime_pin.encoder_contract != manifest.model.encoder_contract
            or runtime_pin.semantic_bank_version != manifest.semantic_bank_version
            or runtime_pin.semantic_bank_sha256 != manifest.semantic_bank_sha256
            or runtime_pin.runtime_source_sha256 != manifest.runtime_source_sha256
            or runtime_pin.asset_manifest_sha256 != self.assets.manifest_sha256
        ):
            raise ValueError("text runtime pin differs from verified runtime/capability")
        if self._runtime_pin is not None and self._runtime_pin != runtime_pin:
            raise ValueError("text runtime pin changed during one runtime")
        self._runtime_pin = runtime_pin
        text = payload.input_data.get("text")
        text_sha = payload.input_data.get("text_sha256")
        if (
            not isinstance(text, str)
            or not text
            or not isinstance(text_sha, str)
            or hashlib.sha256(text.encode("utf-8")).hexdigest() != text_sha
        ):
            raise ValueError("text runtime input hash differs")
        entries = self.assets.semantic_bank.entries
        labels_and_examples = tuple(
            (entry.label, example) for entry in entries for example in entry.examples
        )
        texts = [
            model.prepare_text(text, query=True),
            *(model.prepare_text(example, query=False) for _label, example in labels_and_examples),
        ]
        raw = self.encoder.encode(
            texts,
            model=model,
            max_tokens=model.max_tokens,
            pooling=model.pooling,
            normalize=True,
            dense_only=True,
        )
        vectors = tuple(tuple(float(value) for value in vector) for vector in raw)
        if len(vectors) != len(labels_and_examples) + 1:
            raise ValueError("text encoder returned the wrong vector count")
        for vector in vectors:
            if len(vector) != model.dimensions or any(not math.isfinite(value) for value in vector):
                raise ValueError("text encoder returned an invalid vector")
            norm = math.sqrt(sum(value * value for value in vector))
            if not math.isclose(norm, 1.0, rel_tol=1e-4, abs_tol=1e-4):
                raise ValueError("text encoder returned a non-normalized vector")
        candidate = vectors[0]
        raw_scores: dict[str, float] = {}
        for (label, _example), vector in zip(labels_and_examples, vectors[1:], strict=True):
            score = sum(left * right for left, right in zip(candidate, vector, strict=True))
            raw_scores[label] = max(raw_scores.get(label, -1.0), score)
        scores = {
            label: round(min(1.0, max(0.0, score)), 6)
            for label, score in sorted(raw_scores.items())
        }
        evidence_sha = vector_evidence_fingerprint(
            contract_version=contract_version,
            model_id=model.model_key,
            text_hash=text_sha,
            semantic_bank_version=SEMANTIC_BANK_VERSION,
            semantic_bank_hash=self.assets.manifest.semantic_bank_sha256,
            scores=scores,
        )
        return {
            "model_id": model.model_key,
            "model_revision": model.revision,
            "encoder_contract": model.encoder_contract_version,
            "text_sha256": text_sha,
            "semantic_bank_version": SEMANTIC_BANK_VERSION,
            "semantic_bank_hash": self.assets.manifest.semantic_bank_sha256,
            "evidence_fingerprint": evidence_sha,
            "scores": scores,
            "asset_manifest_sha256": self.assets.manifest_sha256,
            "runtime_source_sha256": self.assets.manifest.runtime_source_sha256,
            "provider_image_identity": runtime_pin.provider_image_identity,
            "provider_image_source_commit": runtime_pin.provider_image_source_commit,
            "pin_sha256": runtime_pin.pin_sha256,
        }


class _E5OfflineEncoder:
    def __init__(self, model_root: Path) -> None:
        os.environ["HF_HUB_OFFLINE"] = "1"
        os.environ["TRANSFORMERS_OFFLINE"] = "1"
        try:
            import torch
            from transformers import AutoModel, AutoTokenizer
        except ImportError as exc:
            raise TextRuntimeAssetError("TEXT_RUNTIME_DEPENDENCY_UNAVAILABLE") from exc
        self._torch = torch
        self._tokenizer = AutoTokenizer.from_pretrained(model_root, local_files_only=True)
        self._model = AutoModel.from_pretrained(model_root, local_files_only=True).eval()

    def encode(
        self,
        texts: Sequence[str],
        *,
        model: EmbeddingModelContract,
        max_tokens: int,
        pooling: str,
        normalize: bool,
        dense_only: bool,
    ) -> Sequence[Sequence[float]]:
        if model != E5_MULTILINGUAL_BASE or pooling != "attention_mask_mean" or not normalize or not dense_only:
            raise ValueError("E5 offline encoder contract mismatch")
        encoded = self._tokenizer(
            list(texts), max_length=max_tokens, padding=True, truncation=True, return_tensors="pt"
        )
        with self._torch.inference_mode():
            hidden = self._model(**encoded).last_hidden_state
        mask = encoded["attention_mask"].unsqueeze(-1).to(hidden.dtype)
        vectors = (hidden * mask).sum(1) / mask.sum(1).clamp(min=1)
        return self._torch.nn.functional.normalize(vectors, p=2, dim=1).cpu().tolist()


class _BgeM3OfflineEncoder:
    def __init__(self, model_root: Path) -> None:
        os.environ["HF_HUB_OFFLINE"] = "1"
        try:
            from FlagEmbedding import BGEM3FlagModel
        except ImportError as exc:
            raise TextRuntimeAssetError("TEXT_RUNTIME_DEPENDENCY_UNAVAILABLE") from exc
        self._model = BGEM3FlagModel(str(model_root), normalize_embeddings=True, use_fp16=False)

    def encode(
        self,
        texts: Sequence[str],
        *,
        model: EmbeddingModelContract,
        max_tokens: int,
        pooling: str,
        normalize: bool,
        dense_only: bool,
    ) -> Sequence[Sequence[float]]:
        if model != BGE_M3 or pooling != "model_native_dense" or not normalize or not dense_only:
            raise ValueError("BGE-M3 offline encoder contract mismatch")
        result = self._model.encode(
            list(texts),
            batch_size=4,
            max_length=max_tokens,
            return_dense=True,
            return_sparse=False,
            return_colbert_vecs=False,
        )
        return result["dense_vecs"].tolist()


def discover_attached_text_runtime(
    stage: str,
    *,
    input_root: Path = Path("/kaggle/input"),
    registry_path: Path | None = None,
    encoder_factory: Callable[[str, Path], TextVectorEncoder] | None = None,
    version_resolver: Callable[[str], str] = importlib.metadata.version,
) -> VerifiedTextStageRuntime | None:
    """Return a runtime only for a registry-pinned, exact offline bundle."""

    if stage not in TEXT_STAGES:
        return None
    registry = read_text_runtime_registry(registry_path)
    entry = next(item for item in registry.entries if item.stage == stage)
    if entry.availability == "external_assets_required":
        return None
    assert entry.manifest_sha256 is not None
    if input_root.is_symlink() or not input_root.is_dir():
        raise TextRuntimeAssetError("TEXT_RUNTIME_INPUT_ROOT_UNSAFE")
    manifest_base = registry_path.parent if registry_path is not None else Path(__file__).with_name("assets")
    packaged_manifest = manifest_base / entry.manifest_filename
    if (
        packaged_manifest.is_symlink()
        or not packaged_manifest.is_file()
        or sha256_file(packaged_manifest) != entry.manifest_sha256
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_ABSENT_OR_AMBIGUOUS")
    manifest = TextRuntimeAssetManifest.model_validate_json(packaged_manifest.read_bytes())
    if (
        manifest.provider_model_source != entry.model_source
        or manifest.provider_kernel_source != entry.kernel_source
        or manifest.producer_authority_sha256 != entry.producer_authority_sha256
    ):
        raise TextRuntimeAssetError("TEXT_RUNTIME_MANIFEST_BINDING_MISMATCH")
    weight_name = "model.safetensors" if stage == "e5_embedding" else "pytorch_model.bin"
    weight = next(item for item in manifest.model_files if Path(item.relative_path).name == weight_name)
    model_roots = {
        path.parent.resolve()
        for path in input_root.rglob(weight_name)
        if path.is_file() and not path.is_symlink() and path.stat().st_size == weight.byte_size
    }
    if len(model_roots) != 1:
        raise TextRuntimeAssetError("TEXT_RUNTIME_MODEL_DIRECTORY_ABSENT_OR_AMBIGUOUS")
    model_root = next(iter(model_roots))
    if entry.kernel_source is None:
        semantic_bank_path = manifest_base / manifest.semantic_bank_file.relative_path
    else:
        semantic_matches = [
            path
            for path in input_root.rglob(Path(manifest.semantic_bank_file.relative_path).name)
            if path.is_file()
            and not path.is_symlink()
            and path.stat().st_size == manifest.semantic_bank_file.byte_size
            and sha256_file(path) == manifest.semantic_bank_file.sha256
        ]
        if len(semantic_matches) != 1:
            raise TextRuntimeAssetError("TEXT_RUNTIME_SEMANTIC_BANK_ABSENT_OR_AMBIGUOUS")
        semantic_bank_path = semantic_matches[0]
    assets = verify_text_runtime_asset_bundle(
        bundle_root=manifest_base,
        manifest_path=packaged_manifest,
        expected_manifest_sha256=entry.manifest_sha256,
        expected_stage=stage,
        model_root_override=model_root,
        semantic_bank_path_override=semantic_bank_path,
        version_resolver=version_resolver,
    )
    if encoder_factory is None:
        encoder: TextVectorEncoder = (
            _E5OfflineEncoder(assets.model_root)
            if stage == "e5_embedding"
            else _BgeM3OfflineEncoder(assets.model_root)
        )
    else:
        encoder = encoder_factory(stage, assets.model_root)
    return VerifiedTextStageRuntime(
        assets=assets,
        encoder=encoder,
    )


def verified_text_runtime_model_source(stage: str) -> str | None:
    """Return the exact KPA model source only after registry verification."""

    if stage not in TEXT_STAGES:
        return None
    entry = next(item for item in read_text_runtime_registry().entries if item.stage == stage)
    return entry.model_source if entry.availability == "verified" else None


def verified_text_runtime_kernel_source(stage: str) -> str | None:
    """Return the frozen producer slug only after registry verification."""

    if stage not in TEXT_STAGES:
        return None
    entry = next(item for item in read_text_runtime_registry().entries if item.stage == stage)
    return entry.kernel_source if entry.availability == "verified" else None


__all__ = [
    "SemanticBankDocument",
    "SemanticBankEntry",
    "TextRuntimeAssetError",
    "TextRuntimeAssetManifest",
    "TextRuntimePinReceipt",
    "TextRuntimeRegistrationMetadata",
    "TextRuntimeRegistry",
    "TextRuntimeRegistryEntry",
    "TextVectorEncoder",
    "VerifiedTextRuntimeAssets",
    "VerifiedTextStageRuntime",
    "build_text_runtime_asset_manifest",
    "discover_attached_text_runtime",
    "read_e5_frozen_producer_authority",
    "read_text_runtime_registry",
    "runtime_source_sha256",
    "verified_text_runtime_kernel_source",
    "verified_text_runtime_model_source",
    "verify_text_runtime_asset_bundle",
]
