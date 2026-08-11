"""Fail-closed orchestration contract for the post-blogger embedding closure.

The external command moves metadata only.  The required live control interface
executes the stage inside the ACTIVE master, where compact canonical documents
can be dispatched through the repository's single Kaggle adapter and returned
artifacts can be imported transactionally without crossing the control plane.
"""

from __future__ import annotations

import hashlib
import json
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import UUID, uuid5

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.models import BGE_M3, E5_MULTILINGUAL_BASE, EmbeddingModelContract
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.contracts import KAGGLE_API_PACKAGE, KAGGLE_API_VERSION
from my_data_hub.workloads.bloggers.closure import LOCAL_CONTROL_URL
from my_data_hub.workloads.bloggers.master_stage import (
    EXPECTED_BLOGGER_ROWS,
    BloggerImportStageReceipt,
)

EXTERNAL_BLOCKED = 78
CAPABILITY_SCHEMA = "my-data-hub-embedding-production-capabilities.v1"
REQUEST_SCHEMA = "my-data-hub-embedding-production-request.v1"
RECEIPT_SCHEMA = "my-data-hub-embedding-production-closure.v1"
MAX_METADATA_BYTES = 256 * 1024
_NAMESPACE = UUID("537f0c8e-c46a-54dd-8f18-ff4275a069f0")
_PROVIDER_NAMESPACE = UUID("fa58e1cb-dcf5-5c35-acf2-7ba79ef2ffb5")


class EmbeddingProductionError(RuntimeError):
    pass


class EmbeddingInterfacesUnavailable(EmbeddingProductionError):
    """The required master/MCP implementation is absent; no mutation is allowed."""


class WorkerAsset(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    notebook_slug: str = Field(pattern=r"^[a-z0-9-]+$", max_length=100)
    notebook_path: str = Field(pattern=r"^notebooks/[a-z0-9-]+/worker\.ipynb$")
    primary_source_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    model: EmbeddingModelContract


WORKER_ASSETS: tuple[WorkerAsset, ...] = (
    WorkerAsset(
        notebook_slug="05-e5-blogger-embedding-worker",
        notebook_path="notebooks/05-e5-blogger-embedding-worker/worker.ipynb",
        primary_source_sha256="9cfe2210c0f6d689445aadf917f1638dcaa173178e36f48832e91bfdeeafebfd",
        model=E5_MULTILINGUAL_BASE,
    ),
    WorkerAsset(
        notebook_slug="06-bge-m3-blogger-embedding-worker",
        notebook_path="notebooks/06-bge-m3-blogger-embedding-worker/worker.ipynb",
        primary_source_sha256="10e26e7da6bad0e65d048debce89f25c2b8dea715e058b788e9fd5b79a654682",
        model=BGE_M3,
    ),
)


class EmbeddingProductionCapabilities(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-embedding-production-capabilities.v1"] = CAPABILITY_SCHEMA
    ready: Literal[True]
    execution_location: Literal["active_kaggle_master"]
    provider_adapter_package: Literal["kaggle"]
    provider_adapter_version: Literal["2.2.4"]
    provider_adapter_implementation: Literal["my_data_hub.providers.kaggle.KaggleProviderAdapter"]
    single_provider_adapter: Literal[True]
    transactional_import: Literal[True]
    verified_checkpoint_restore: Literal[True]
    mcp_hybrid_search: Literal[True]
    worker_assets: tuple[WorkerAsset, WorkerAsset]

    @model_validator(mode="after")
    def exact_production_boundary(self) -> EmbeddingProductionCapabilities:
        if self.worker_assets != WORKER_ASSETS:
            raise ValueError("live interface worker assets differ from the generated pinned contracts")
        if self.provider_adapter_package != KAGGLE_API_PACKAGE or self.provider_adapter_version != KAGGLE_API_VERSION:
            raise ValueError("live interface does not use the repository's pinned Kaggle adapter")
        return self


class EmbeddingProductionRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-embedding-production-request.v1"] = REQUEST_SCHEMA
    request_id: UUID
    idempotency_key_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    blogger_receipt_id: UUID
    blogger_receipt_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    blogger_canonical_revision: int = Field(ge=1)
    blogger_checkpoint_id: UUID
    source_revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    expected_documents_per_model: Literal[266] = EXPECTED_BLOGGER_ROWS
    worker_assets: tuple[WorkerAsset, WorkerAsset] = WORKER_ASSETS
    probe_query: str = Field(min_length=1, max_length=500)
    probe_query_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_workers(self) -> EmbeddingProductionRequest:
        if self.worker_assets != WORKER_ASSETS:
            raise ValueError("embedding request worker assets are not exact")
        if hashlib.sha256(self.probe_query.strip().encode()).hexdigest() != self.probe_query_sha256:
            raise ValueError("embedding probe query hash differs from its exact text")
        return self

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


def embedding_provider_authority(owner: str, request_id: UUID) -> dict[str, tuple[str, UUID]]:
    """Exact protected refs/tasks; never a prefix or caller-selected resource."""

    if not owner or "/" in owner:
        raise ValueError("Kaggle owner is invalid")
    short = request_id.hex[:12]
    result: dict[str, tuple[str, UUID]] = {}
    for alias, asset in zip(("e5", "bge"), WORKER_ASSETS, strict=True):
        task_id = uuid5(_PROVIDER_NAMESPACE, f"{request_id}:{asset.model.exact_id}")
        result[f"{alias}_input"] = (f"{owner}/mdh-embed-{short}-{alias}", task_id)
        result[f"{alias}_worker"] = (f"{owner}/{asset.notebook_slug}", task_id)
    return result


class EmbeddingProductionStageReceipt(BaseModel):
    """Bounded metadata proof returned by the ACTIVE master; never vectors/documents."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["my-data-hub-embedding-production-stage-receipt.v1"] = (
        "my-data-hub-embedding-production-stage-receipt.v1"
    )
    request_id: UUID
    request_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    master_instance_id: UUID
    run_id: UUID
    epoch: int = Field(ge=1)
    workers: tuple[dict[str, Any], dict[str, Any]]
    imports: tuple[dict[str, Any], dict[str, Any]]
    coverage: tuple[dict[str, Any], dict[str, Any]]
    query_vector_receipts: dict[str, dict[str, Any]]
    canonical_revision: int = Field(ge=1)

    @model_validator(mode="after")
    def metadata_only_complete(self) -> EmbeddingProductionStageReceipt:
        if len(canonical_json_bytes(self.model_dump(mode="json"))) > MAX_METADATA_BYTES:
            raise ValueError("embedding stage receipt exceeds metadata bound")
        if {row.get("model_exact_id") for row in self.workers} != {
            asset.model.exact_id for asset in WORKER_ASSETS
        }:
            raise ValueError("embedding worker receipt does not cover both exact models")
        if {row.get("model_exact_id") for row in self.imports} != {
            asset.model.exact_id for asset in WORKER_ASSETS
        }:
            raise ValueError("embedding import receipt does not cover both exact models")
        if any(row.get("expected_documents") != EXPECTED_BLOGGER_ROWS for row in self.coverage):
            raise ValueError("embedding coverage expected count differs from imported corpus")
        if any(row.get("completed_documents") != EXPECTED_BLOGGER_ROWS for row in self.coverage):
            raise ValueError("embedding stage is not at 100 percent coverage")
        expected_vectors = {
            E5_MULTILINGUAL_BASE.exact_id: E5_MULTILINGUAL_BASE.dimensions,
            BGE_M3.exact_id: BGE_M3.dimensions,
        }
        if set(self.query_vector_receipts) != set(expected_vectors):
            raise ValueError("embedding query vectors do not cover both exact spaces")
        for model_id, dimensions in expected_vectors.items():
            receipt = self.query_vector_receipts[model_id]
            if set(receipt) != {"query_sha256", "vector_sha256", "dimensions"}:
                raise ValueError("embedding query receipt fields differ")
            if (
                len(str(receipt["query_sha256"])) != 64
                or len(str(receipt["vector_sha256"])) != 64
                or any(char not in "0123456789abcdef" for char in str(receipt["query_sha256"]))
                or any(char not in "0123456789abcdef" for char in str(receipt["vector_sha256"]))
                or receipt["dimensions"] != dimensions
            ):
                raise ValueError("embedding query receipt identity differs")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class EmbeddingProductionControl(Protocol):
    def capabilities(self) -> dict[str, Any]: ...
    def create_request(self, request: EmbeddingProductionRequest) -> dict[str, Any]: ...
    def request_status(self, request_id: UUID) -> dict[str, Any]: ...


class EmbeddingProductionMcp(Protocol):
    def call(self, tool: str, arguments: dict[str, Any]) -> dict[str, Any]: ...


@dataclass(frozen=True, slots=True)
class EmbeddingProductionConfig:
    control_url: str
    idempotency_key: str
    source_revision: str
    probe_query: str
    timeout_seconds: int = 43_000
    poll_seconds: float = 10.0

    def __post_init__(self) -> None:
        parsed = urlsplit(self.control_url)
        if (
            self.control_url != LOCAL_CONTROL_URL
            or parsed.scheme != "http"
            or parsed.hostname != "127.0.0.1"
            or parsed.port != 8080
            or parsed.path not in {"", "/"}
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError("embedding control URL must be the exact loopback-only endpoint")
        if not 8 <= len(self.idempotency_key) <= 200:
            raise ValueError("embedding closure idempotency key is invalid")
        if len(self.source_revision) != 40 or any(c not in "0123456789abcdef" for c in self.source_revision):
            raise ValueError("embedding source revision must be an exact lowercase commit SHA")
        if not 1 <= len(self.probe_query.strip()) <= 500:
            raise ValueError("hybrid-search probe query is empty or oversized")
        if not 600 <= self.timeout_seconds <= 43_000:
            raise ValueError("embedding closure timeout must be 600..43000 seconds")
        if not 1 <= self.poll_seconds <= 60:
            raise ValueError("embedding closure polling interval is invalid")


class LocalEmbeddingProductionControl:
    """Metadata-only client. These endpoints are deliberately capability-gated."""

    def __init__(self, config: EmbeddingProductionConfig) -> None:
        self.config = config

    def _call(self, method: str, path: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
        encoded = canonical_json_bytes(payload) if payload is not None else None
        request = urllib.request.Request(
            self.config.control_url.rstrip("/") + path,
            data=encoded,
            headers={"Accept": "application/json", "Content-Type": "application/json"},
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                raw = response.read(MAX_METADATA_BYTES + 1)
        except (OSError, urllib.error.HTTPError) as exc:
            if path.endswith("/capabilities"):
                raise EmbeddingInterfacesUnavailable("embedding production control capability is unavailable") from exc
            raise EmbeddingProductionError("bounded embedding control request failed") from exc
        if len(raw) > MAX_METADATA_BYTES:
            raise EmbeddingProductionError("embedding control response exceeds 256 KiB")
        try:
            value = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise EmbeddingProductionError("embedding control response is not JSON") from exc
        if not isinstance(value, dict):
            raise EmbeddingProductionError("embedding control response is not an object")
        return value

    def capabilities(self) -> dict[str, Any]:
        return self._call("GET", "/control/v1/embedding-production/capabilities")

    def create_request(self, request: EmbeddingProductionRequest) -> dict[str, Any]:
        return self._call("POST", "/control/v1/embedding-production/requests", request.model_dump(mode="json"))

    def request_status(self, request_id: UUID) -> dict[str, Any]:
        return self._call("GET", f"/control/v1/embedding-production/requests/{request_id}")


def _wait(deadline: float, interval: float, function: Any, predicate: Any) -> dict[str, Any]:
    while time.monotonic() < deadline:
        value = function()
        if predicate(value):
            return value
        time.sleep(min(interval, max(0.0, deadline - time.monotonic())))
    raise EmbeddingProductionError("embedding closure exceeded its bounded deadline")


def _validated_blogger_prerequisite(value: dict[str, Any]) -> tuple[BloggerImportStageReceipt, str, UUID]:
    required_keys = {
        "schema_version",
        "receipt_id",
        "status",
        "started_at",
        "completed_at",
        "closure_idempotency_key_sha256",
        "request_id",
        "request_sha256",
        "ensure_operation_id",
        "rotation_operation_id",
        "import_runtime",
        "import_receipt",
        "import_receipt_sha256",
        "checkpoint",
        "cold_restore",
        "mcp_accounting",
        "mcp_statistics",
        "mcp_projection",
    }
    if (
        set(value) != required_keys
        or value.get("schema_version") != "my-data-hub-blogger-closure.v1"
        or value.get("status") != "DURABLE_COMPLETE"
    ):
        raise EmbeddingProductionError("verified FINAL-BLOGGER closure receipt is required")
    imported = BloggerImportStageReceipt.model_validate(value.get("import_receipt"))
    raw_import = canonical_json_bytes(imported.model_dump(mode="json"))
    if value.get("import_receipt_sha256") != hashlib.sha256(raw_import).hexdigest():
        raise EmbeddingProductionError("blogger import receipt hash is mismatched")
    checkpoint = value.get("checkpoint")
    cold_restore = value.get("cold_restore")
    import_runtime = value.get("import_runtime")
    accounting = value.get("mcp_accounting")
    statistics = value.get("mcp_statistics")
    projection = value.get("mcp_projection")
    if not all(
        isinstance(item, dict)
        for item in (checkpoint, cold_restore, import_runtime, accounting, statistics, projection)
    ):
        raise EmbeddingProductionError("blogger checkpoint/cold-restore evidence is absent")
    assert isinstance(checkpoint, dict) and isinstance(cold_restore, dict)
    assert isinstance(import_runtime, dict) and isinstance(accounting, dict)
    assert isinstance(statistics, dict) and isinstance(projection, dict)
    expected_accounting = {
        "export_batch_id": str(imported.export_batch_id),
        "expected_row_count": EXPECTED_BLOGGER_ROWS,
        "status": "accepted",
        "logical_sha256": imported.logical_sha256,
        "record_id_set_sha256": imported.record_id_set_sha256,
        "canonical_outcome_sha256": imported.canonical_outcome_sha256,
        "duplicate_groups_pending": 0,
        "imported_canonical_revision": imported.canonical_revision,
        "raw_count": EXPECTED_BLOGGER_ROWS,
        "dispositioned_count": EXPECTED_BLOGGER_ROWS,
        "undispositioned_count": 0,
        "quarantined_count": 0,
        "actor_count": EXPECTED_BLOGGER_ROWS,
        "account_count": imported.account_count,
        "checkpoint_required": True,
    }
    if (
        set(checkpoint) != {"generation", "checkpoint_id", "exact_version_ref", "manifest_sha256"}
        or set(cold_restore) != {"master_instance_id", "epoch", "canonical_revision"}
        or set(import_runtime) != {"run_id", "attempt_id", "master_instance_id", "epoch"}
        or int(checkpoint.get("generation") or 0) < 1
        or not _exact_version_ref(checkpoint.get("exact_version_ref"))
        or not _sha(checkpoint.get("manifest_sha256"))
        or cold_restore.get("canonical_revision") != imported.canonical_revision
        or int(cold_restore.get("epoch") or 0) <= imported.epoch
        or import_runtime.get("run_id") != imported.run_id
        or import_runtime.get("master_instance_id") != str(imported.master_instance_id)
        or import_runtime.get("epoch") != imported.epoch
        or value.get("request_id") != str(imported.request_id)
        or value.get("request_sha256") != imported.request_sha256
        or value.get("ensure_operation_id") != str(imported.operation_id)
        or not isinstance(value.get("rotation_operation_id"), str)
        or not value["rotation_operation_id"]
        or not _sha(value.get("closure_idempotency_key_sha256"))
        or any(accounting.get(key) != expected for key, expected in expected_accounting.items())
        or statistics.get("bloggers") != EXPECTED_BLOGGER_ROWS
        or imported.actor_count != EXPECTED_BLOGGER_ROWS
        or projection.get("listed_bloggers") != EXPECTED_BLOGGER_ROWS
        or projection.get("get_found") is not True
        or not isinstance(projection.get("provenance_events"), int)
        or int(projection.get("provenance_events") or 0) < 1
        or not isinstance(projection.get("search_matches"), int)
        or int(projection.get("search_matches") or 0) < 1
        or not isinstance(projection.get("completed_retrievers"), list)
        or not {"exact", "fts"}.issubset(set(projection.get("completed_retrievers", [])))
    ):
        raise EmbeddingProductionError("blogger prerequisite is not exact and cold-restored")
    try:
        UUID(str(value["receipt_id"]))
        UUID(str(import_runtime["run_id"]))
        UUID(str(import_runtime["attempt_id"]))
        UUID(str(cold_restore["master_instance_id"]))
        checkpoint_id = UUID(str(checkpoint["checkpoint_id"]))
    except ValueError as exc:
        raise EmbeddingProductionError("blogger prerequisite UUID identity is invalid") from exc
    try:
        started = datetime.fromisoformat(str(value["started_at"]).replace("Z", "+00:00"))
        completed = datetime.fromisoformat(str(value["completed_at"]).replace("Z", "+00:00"))
    except ValueError as exc:
        raise EmbeddingProductionError("blogger prerequisite timestamps are invalid") from exc
    if started.tzinfo is None or completed.tzinfo is None or completed < started:
        raise EmbeddingProductionError("blogger prerequisite timestamps are invalid")
    return imported, hashlib.sha256(canonical_json_bytes(value)).hexdigest(), checkpoint_id


def _preflight_interfaces(
    control: EmbeddingProductionControl, mcp: EmbeddingProductionMcp
) -> EmbeddingProductionCapabilities:
    try:
        control_capabilities = EmbeddingProductionCapabilities.model_validate(control.capabilities())
        mcp_capabilities = EmbeddingProductionCapabilities.model_validate(
            mcp.call("embedding.production.capabilities", {})
        )
    except EmbeddingInterfacesUnavailable:
        raise
    except Exception as exc:
        raise EmbeddingInterfacesUnavailable(
            "required embedding master/MCP live interfaces are unavailable or mismatched"
        ) from exc
    if control_capabilities != mcp_capabilities:
        raise EmbeddingInterfacesUnavailable("control and MCP embedding capabilities differ")
    return control_capabilities


def _validate_stage_status(
    value: dict[str, Any], *, request: EmbeddingProductionRequest
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any], int]:
    if value.get("state") != "CHECKPOINT_VERIFIED" or value.get("request_sha256") != request.request_sha256:
        raise EmbeddingProductionError("embedding master stage did not verify the exact request")
    if (
        not isinstance(value.get("claimed_epoch"), int)
        or int(value["claimed_epoch"]) < 1
        or not isinstance(value.get("claimed_run_id"), str)
    ):
        raise EmbeddingProductionError("embedding stage runtime identity is absent")
    UUID(str(value["claimed_run_id"]))
    workers = value.get("workers")
    imports = value.get("imports")
    coverage = value.get("coverage")
    checkpoint = value.get("checkpoint_receipt")
    if not all(isinstance(item, list) for item in (workers, imports, coverage)) or not isinstance(checkpoint, dict):
        raise EmbeddingProductionError("embedding stage evidence is incomplete")
    assert isinstance(workers, list) and isinstance(imports, list) and isinstance(coverage, list)
    expected_models = {asset.model.exact_id for asset in WORKER_ASSETS}
    worker_models = {str(item.get("model_exact_id")) for item in workers if isinstance(item, dict)}
    provider_runs = {str(item.get("provider_run_ref")) for item in workers if isinstance(item, dict)}
    task_runs = {str(item.get("task_run_id")) for item in workers if isinstance(item, dict)}
    if len(workers) != 2 or worker_models != expected_models or len(provider_runs) != 2 or len(task_runs) != 2:
        raise EmbeddingProductionError("embedding worker run identities are absent or reused")
    worker_keys = {
        "model_exact_id",
        "task_run_id",
        "provider_ref",
        "provider_run_ref",
        "provider_kernel_id",
        "source_version",
        "source_sha256",
        "primary_source_sha256",
        "provider_status",
        "privacy",
        "control_class",
        "output_tree_sha256",
        "artifact_sha256",
        "artifact_id",
        "artifact_run_id",
        "input_dataset",
    }
    for item in workers:
        if (
            not isinstance(item, dict)
            or set(item) != worker_keys
            or any(
                (
                    item.get("provider_status") != "complete",
                    item.get("privacy") != "private",
                    item.get("control_class") != "orchestrator_protected",
                    not isinstance(item.get("source_version"), int),
                    int(item.get("source_version") or 0) < 1,
                    not _sha(item.get("source_sha256")),
                    not _sha(item.get("output_tree_sha256")),
                    not _sha(item.get("artifact_sha256")),
                    not isinstance(item.get("input_dataset"), dict),
                )
            )
        ):
            raise EmbeddingProductionError("embedding worker lacks exact private terminal evidence")
        asset = next((row for row in WORKER_ASSETS if row.model.exact_id == item.get("model_exact_id")), None)
        provider_ref = str(item.get("provider_ref", ""))
        if (
            asset is None
            or provider_ref.split("/", 1)[-1] != asset.notebook_slug
            or item.get("primary_source_sha256") != asset.primary_source_sha256
        ):
            raise EmbeddingProductionError("embedding worker source differs from the pinned generated asset")
        input_dataset = item["input_dataset"]
        if (
            set(input_dataset) != {"provider_ref", "provider_version", "package_sha256", "jobs_sha256"}
            or not isinstance(input_dataset.get("provider_ref"), str)
            or not isinstance(input_dataset.get("provider_version"), int)
            or not _sha(input_dataset.get("package_sha256"))
            or not _sha(input_dataset.get("jobs_sha256"))
            or str(item.get("artifact_run_id")) != str(item.get("task_run_id"))
        ):
            raise EmbeddingProductionError("embedding worker input/artifact identity is mismatched")
        try:
            UUID(str(item.get("task_run_id")))
            UUID(str(item.get("artifact_run_id")))
        except ValueError as exc:
            raise EmbeddingProductionError("embedding worker run identity is not a UUID") from exc
        if (
            not isinstance(item.get("provider_ref"), str)
            or str(item["provider_ref"]).count("/") != 1
            or item.get("provider_run_ref") != f"{item['provider_ref']}/{item['source_version']}"
            or not isinstance(item.get("provider_kernel_id"), int)
            or int(item.get("provider_kernel_id") or 0) < 1
        ):
            raise EmbeddingProductionError("embedding provider run identity is not exact")
        UUID(str(item.get("artifact_id")))
    import_models = {str(item.get("model_exact_id")) for item in imports if isinstance(item, dict)}
    if len(imports) != 2 or import_models != expected_models:
        raise EmbeddingProductionError("embedding transactional import receipts are incomplete")
    import_keys = {
        "model_exact_id",
        "artifact_id",
        "run_id",
        "artifact_sha256",
        "outbox_id",
        "canonical_revision",
        "inserted_count",
        "stale_count",
        "failed_count",
        "replayed",
        "durability_state",
    }
    for item in imports:
        if (
            not isinstance(item, dict)
            or set(item) != import_keys
            or any(
                (
                    item.get("inserted_count") != EXPECTED_BLOGGER_ROWS,
                    item.get("stale_count") != 0,
                    item.get("failed_count") != 0,
                    item.get("durability_state") != "COMMITTED_PENDING_CHECKPOINT",
                    not isinstance(item.get("artifact_id"), str),
                    not isinstance(item.get("outbox_id"), str),
                    not isinstance(item.get("canonical_revision"), int),
                    not isinstance(item.get("replayed"), bool),
                )
            )
        ):
            raise EmbeddingProductionError("embedding artifact was not transactionally imported exactly")
        UUID(str(item["artifact_id"]))
        UUID(str(item["run_id"]))
        UUID(str(item["outbox_id"]))
        worker = next(row for row in workers if row["model_exact_id"] == item["model_exact_id"])
        if (
            item.get("artifact_id") != worker.get("artifact_id")
            or item.get("run_id") != worker.get("artifact_run_id")
            or item.get("artifact_sha256") != worker.get("artifact_sha256")
        ):
            raise EmbeddingProductionError("transactional import is not bound to the exact worker artifact")
    coverage_models = {str(item.get("model_exact_id")) for item in coverage if isinstance(item, dict)}
    if len(coverage) != 2 or coverage_models != expected_models:
        raise EmbeddingProductionError("embedding coverage does not contain both exact vector spaces")
    for item in coverage:
        if (
            not isinstance(item, dict)
            or set(item) != {"model_exact_id", "expected_documents", "completed_documents", "coverage"}
            or (
                item.get("expected_documents") != EXPECTED_BLOGGER_ROWS
                or item.get("completed_documents") != EXPECTED_BLOGGER_ROWS
                or item.get("coverage") != 1.0
            )
        ):
            raise EmbeddingProductionError("embedding coverage is not exactly 100 percent")
    canonical_revision = int(value.get("canonical_revision") or 0)
    if (
        set(checkpoint)
        != {
            "checkpoint_id",
            "status",
            "canonical_revision",
            "manifest_sha256",
            "exact_version_ref",
        }
        or canonical_revision < request.blogger_canonical_revision
        or checkpoint.get("status") != "VERIFIED"
        or checkpoint.get("canonical_revision") != canonical_revision
        or not _sha(checkpoint.get("manifest_sha256"))
        or not _exact_version_ref(checkpoint.get("exact_version_ref"))
    ):
        raise EmbeddingProductionError("embedding verified checkpoint binding is invalid")
    UUID(str(checkpoint.get("checkpoint_id")))
    return workers, imports, coverage, checkpoint, canonical_revision


def _sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(c in "0123456789abcdef" for c in value)


def _exact_version_ref(value: object) -> bool:
    if not isinstance(value, str):
        return False
    parts = value.split("/")
    return len(parts) == 3 and all(parts[:2]) and parts[2].isdigit() and int(parts[2]) >= 1


def _validate_mcp_coverage(value: dict[str, Any], expected_revision: int) -> list[dict[str, Any]]:
    rows = value.get("models")
    if value.get("canonical_revision") != expected_revision or not isinstance(rows, list):
        raise EmbeddingProductionError("cold-restored MCP embedding coverage is absent")
    if len(rows) != 2 or not all(isinstance(row, dict) for row in rows):
        raise EmbeddingProductionError("MCP embedding coverage rows are malformed")
    expected = {asset.model.exact_id for asset in WORKER_ASSETS}
    if {str(row.get("model_exact_id")) for row in rows if isinstance(row, dict)} != expected:
        raise EmbeddingProductionError("MCP coverage vector spaces are mismatched")
    if any(
        set(row) != {"model_exact_id", "expected_documents", "completed_documents", "coverage"}
        or row.get("expected_documents") != EXPECTED_BLOGGER_ROWS
        or row.get("completed_documents") != EXPECTED_BLOGGER_ROWS
        or row.get("coverage") != 1.0
        for row in rows
        if isinstance(row, dict)
    ):
        raise EmbeddingProductionError("MCP does not prove 100 percent embedding coverage")
    return rows


def run_embedding_production_closure(
    config: EmbeddingProductionConfig,
    *,
    blogger_receipt: dict[str, Any],
    control: EmbeddingProductionControl,
    mcp: EmbeddingProductionMcp,
    live_evidence: bool = False,
    now: Any = lambda: datetime.now(UTC),
) -> dict[str, Any]:
    """Execute only after both read-only live capability probes succeed."""

    if live_evidence and not isinstance(control, LocalEmbeddingProductionControl):
        raise ValueError("live evidence requires the production loopback control client")
    imported, blogger_sha, blogger_checkpoint_id = _validated_blogger_prerequisite(blogger_receipt)
    capabilities = _preflight_interfaces(control, mcp)  # no mutation before this line succeeds
    started_at = now()
    deadline = time.monotonic() + config.timeout_seconds
    request_id = uuid5(_NAMESPACE, config.idempotency_key)
    request = EmbeddingProductionRequest(
        request_id=request_id,
        idempotency_key_sha256=hashlib.sha256(config.idempotency_key.encode()).hexdigest(),
        blogger_receipt_id=UUID(str(blogger_receipt["receipt_id"])),
        blogger_receipt_sha256=blogger_sha,
        blogger_canonical_revision=imported.canonical_revision,
        blogger_checkpoint_id=blogger_checkpoint_id,
        source_revision=config.source_revision,
        probe_query_sha256=hashlib.sha256(config.probe_query.strip().encode()).hexdigest(),
        probe_query=config.probe_query.strip(),
    )
    created = control.create_request(request)
    if created.get("request_sha256") != request.request_sha256:
        raise EmbeddingProductionError("control plane stored a different embedding request")
    status = _wait(
        deadline,
        config.poll_seconds,
        lambda: control.request_status(request_id),
        lambda value: value.get("state") in {"CHECKPOINT_VERIFIED", "FAILED"},
    )
    workers, imports, stage_coverage, checkpoint, canonical_revision = _validate_stage_status(status, request=request)
    checkpoint_status = mcp.call("checkpoint.status", {})
    current = checkpoint_status.get("current")
    if (
        not isinstance(current, dict)
        or checkpoint_status.get("current_checkpoint_id") != checkpoint.get("checkpoint_id")
        or current.get("checkpoint_id") != checkpoint.get("checkpoint_id")
        or current.get("manifest_sha256") != checkpoint.get("manifest_sha256")
        or current.get("exact_version_ref") != checkpoint.get("exact_version_ref")
        or current.get("status") != "VERIFIED"
        or current.get("canonical_revision") != canonical_revision
    ):
        raise EmbeddingProductionError("MCP checkpoint HEAD differs from the embedding promotion")
    _wait(
        deadline,
        config.poll_seconds,
        lambda: mcp.call("master.status", {}),
        lambda value: value.get("master_state") in {"ABSENT", "STOPPED"},
    )
    rotation = mcp.call(
        "master.rotation.request",
        {
            "idempotency_key": f"{config.idempotency_key}:embedding-cold-restore",
            "checkpoint_id": checkpoint["checkpoint_id"],
            "exact_version_ref": checkpoint["exact_version_ref"],
            "expected_active_epoch": int(status["claimed_epoch"]),
            "expected_canonical_revision": canonical_revision,
            "timeout_seconds": min(1800, config.timeout_seconds),
        },
    )
    rotation_operation_id = str(rotation.get("operation_id", ""))
    if not rotation_operation_id:
        raise EmbeddingProductionError("embedding cold restore returned no operation identity")
    operation = _wait(
        deadline,
        config.poll_seconds,
        lambda: mcp.call("operation.get", {"operation_id": rotation_operation_id}),
        lambda value: value.get("state") in {"DURABLE_COMPLETE", "FAILED", "FENCED", "ORPHANED"},
    )
    if operation.get("state") != "DURABLE_COMPLETE":
        raise EmbeddingProductionError("embedding cold restore did not become durable")
    cold_master = _wait(
        deadline,
        config.poll_seconds,
        lambda: mcp.call("master.status", {}),
        lambda value: (
            value.get("master_state") == "ACTIVE" and int(value.get("master_epoch") or 0) > int(status["claimed_epoch"])
        ),
    )
    if cold_master.get("canonical_revision") != canonical_revision:
        raise EmbeddingProductionError("cold-restored master revision differs from embedding imports")
    if not isinstance(cold_master.get("master_epoch"), int) or not isinstance(cold_master.get("instance_id"), str):
        raise EmbeddingProductionError("cold-restored master identity is absent")
    UUID(str(cold_master["instance_id"]))
    mcp_coverage = _validate_mcp_coverage(mcp.call("embedding.coverage", {}), canonical_revision)
    query_receipts = status.get("query_vector_receipts")
    if not isinstance(query_receipts, dict):
        raise EmbeddingProductionError("exact query vector receipts are absent from the embedding stage")
    search = mcp.call("bloggers.search", {"query": config.probe_query.strip(), "limit": 20})
    retrievers = search.get("retrievers")
    if (
        search.get("canonical_revision") != canonical_revision
        or not isinstance(search.get("items"), list)
        or not search["items"]
        or search.get("complete") is not True
        or not isinstance(retrievers, dict)
        or set(retrievers.get("requested", [])) != {"exact", "fts", "e5", "bge_m3"}
        or set(retrievers.get("completed", [])) != {"exact", "fts", "e5", "bge_m3"}
        or retrievers.get("unavailable") not in ([], ())
    ):
        raise EmbeddingProductionError("MCP hybrid search did not complete every requested retriever")
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "receipt_id": str(uuid5(_NAMESPACE, f"receipt:{request_id}")),
        "status": "DURABLE_COMPLETE",
        "live_evidence": live_evidence,
        "started_at": started_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "completed_at": now().astimezone(UTC).isoformat().replace("+00:00", "Z"),
        "request_id": str(request_id),
        "request_sha256": request.request_sha256,
        "source_revision": config.source_revision,
        "blogger_prerequisite": {
            "receipt_id": str(request.blogger_receipt_id),
            "receipt_sha256": blogger_sha,
            "canonical_revision": imported.canonical_revision,
            "checkpoint_id": str(blogger_checkpoint_id),
        },
        "provider_adapter": {
            "package": capabilities.provider_adapter_package,
            "version": capabilities.provider_adapter_version,
            "implementation": capabilities.provider_adapter_implementation,
            "single_adapter": True,
            "execution_location": capabilities.execution_location,
        },
        "workers": workers,
        "imports": imports,
        "stage_coverage": stage_coverage,
        "checkpoint": checkpoint,
        "cold_restore": {
            "rotation_operation_id": rotation_operation_id,
            "master_instance_id": cold_master.get("instance_id"),
            "epoch": cold_master.get("master_epoch"),
            "canonical_revision": cold_master.get("canonical_revision"),
        },
        "mcp_coverage": mcp_coverage,
        "hybrid_search": {
            "query_sha256": request.probe_query_sha256,
            "result_count": len(search["items"]),
            "requested_retrievers": sorted(retrievers["requested"]),
            "completed_retrievers": sorted(retrievers["completed"]),
            "unavailable_retrievers": [],
            "complete": True,
            "canonical_revision": canonical_revision,
        },
        "blockers": [],
    }
    if len(canonical_json_bytes(receipt)) > MAX_METADATA_BYTES:
        raise EmbeddingProductionError("embedding closure receipt exceeds 256 KiB")
    return receipt
