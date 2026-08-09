from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

from my_data_hub.hashing import sha256_file, sha256_value
from my_data_hub.notebooks.contracts import NotebookInputManifest, NotebookResult


class NotebookContractError(RuntimeError):
    pass


@dataclass(slots=True)
class NotebookResultBuilder:
    manifest_path: Path
    code_revision: str
    runtime_name: str
    manifest: NotebookInputManifest = field(init=False)
    manifest_sha256: str = field(init=False)
    started_at: datetime = field(init=False)
    items: list[dict[str, Any]] = field(default_factory=list)
    failures: list[dict[str, Any]] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    artifacts: list[dict[str, Any]] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.started_at = datetime.now(UTC)
        self.manifest_sha256 = sha256_file(self.manifest_path)
        try:
            self.manifest = NotebookInputManifest.model_validate_json(
                self.manifest_path.read_text(encoding="utf-8")
            )
        except Exception as exc:
            raise NotebookContractError(f"invalid notebook input manifest: {exc}") from exc

    def add_success(
        self,
        *,
        work_item_id: UUID | str,
        input_fingerprint: str,
        result: dict[str, Any],
        evidence: dict[str, Any] | None = None,
    ) -> None:
        self.items.append(
            {
                "work_item_id": str(work_item_id),
                "input_fingerprint": input_fingerprint,
                "output_fingerprint": sha256_value(result),
                "status": "succeeded",
                "result": result,
                "evidence": evidence or {},
            }
        )

    def add_failure(
        self,
        *,
        code: str,
        message: str,
        retryable: bool,
        work_item_id: UUID | str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.failures.append(
            {
                "work_item_id": str(work_item_id) if work_item_id else None,
                "code": code,
                "message": message,
                "retryable": retryable,
                "details": details or {},
            }
        )

    def build(self, model: dict[str, Any]) -> dict[str, Any]:
        expected = {str(item.work_item_id) for item in self.manifest.work_items}
        success_ids = {item["work_item_id"] for item in self.items}
        failure_ids = {
            item["work_item_id"] for item in self.failures if item.get("work_item_id")
        }
        unknown = (success_ids | failure_ids) - expected
        if unknown:
            raise NotebookContractError(
                "result contains unknown work_item_id: " + ", ".join(sorted(unknown))
            )
        for missing in sorted(expected - success_ids - failure_ids):
            self.add_failure(
                work_item_id=missing,
                code="UNACCOUNTED_WORK_ITEM",
                message="worker returned no terminal result for this input",
                retryable=True,
            )
        if self.items and self.failures:
            status = "partial"
        elif self.failures:
            status = "failed"
        else:
            status = "succeeded"
        raw = {
            "schema_version": "my-data-hub-notebook-result.v1",
            "result_id": str(uuid4()),
            "run_id": str(self.manifest.run_id),
            "workload": self.manifest.workload,
            "stage": self.manifest.stage,
            "stage_contract_version": self.manifest.stage_contract_version,
            "input_manifest_sha256": self.manifest_sha256,
            "producer": {
                "code_revision": self.code_revision,
                "runtime": self.runtime_name,
                "model": model,
            },
            "status": status,
            "items": self.items,
            "failures": self.failures,
            "metrics": {
                **self.metrics,
                "input_items": len(expected),
                "successful_items": len(self.items),
                "failed_items": len(self.failures),
                "accounted_items": len(expected),
            },
            "provider_usage": [],
            "artifacts": self.artifacts,
            "started_at": self.started_at.isoformat(),
            "completed_at": datetime.now(UTC).isoformat(),
        }
        return NotebookResult.model_validate(raw).model_dump(mode="json")


def manifest_path_from_env() -> Path:
    raw = os.getenv("MY_DATA_HUB_NOTEBOOK_INPUT_MANIFEST", "").strip()
    if not raw:
        raise NotebookContractError("MY_DATA_HUB_NOTEBOOK_INPUT_MANIFEST is required")
    path = Path(raw)
    if not path.is_file():
        raise NotebookContractError(f"input manifest does not exist: {path}")
    return path
