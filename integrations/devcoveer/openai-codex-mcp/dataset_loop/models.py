from __future__ import annotations

from dataclasses import dataclass
from typing import Any

RUN_SCHEMA = "devcoveer-dataset-loop-run.v1"
MANIFEST_SCHEMA = "devcoveer-frozen-dataset-manifest.v1"


@dataclass(frozen=True)
class DatasetSelector:
    dataset_id: str | None = None
    title: str | None = None
    path: str | None = None


@dataclass(frozen=True)
class CatalogModel:
    selection: str
    provider: str
    active: bool
    free: bool
    zen: bool
    metadata: dict[str, Any]
