from __future__ import annotations

import os
import re
import time
import uuid
from collections.abc import Iterable
from pathlib import Path
from typing import Any

from .catalog import classify_catalog, has_real_tool_receipt, profile_models
from .datasets import DatasetResolver
from .models import RUN_SCHEMA, DatasetSelector
from .store import AtomicFileStore

_RUN = re.compile(r"run_[0-9a-f]{32}")
_BLOCKER = "fewer than two eligible free Zen models have real websearch+webfetch terminal receipts"


class DatasetLoopService:
    def __init__(self, root: Path, *, resolver: DatasetResolver) -> None:
        self.store = AtomicFileStore(root)
        self.resolver = resolver

    def _key(self, run_id: str) -> str:
        if _RUN.fullmatch(run_id) is None:
            raise KeyError(run_id)
        return f"run-{run_id}"

    def _artifacts(self, run_id: str) -> None:
        directory = self.store.root / "artifacts" / run_id
        directory.mkdir(parents=True, exist_ok=True, mode=0o700)
        os.chmod(directory, 0o700)
        initial = {
            "ledger.jsonl": "",
            "metrics.json": "{}\n",
            "scorecard.json": "{}\n",
            "schema-gaps.json": "[]\n",
            "audit.jsonl": "",
        }
        for name, content in initial.items():
            target = directory / name
            target.write_text(content)
            os.chmod(target, 0o600)

    def start(
        self,
        *,
        selectors: Iterable[DatasetSelector],
        catalog_items: Iterable[dict[str, Any]],
        probe_receipts: dict[str, Any],
        profile: str = "zen_nvidia_audit",
    ) -> dict[str, Any]:
        catalog = classify_catalog(catalog_items)
        profiles = profile_models(profile, catalog)
        manifest = self.resolver.resolve(selectors)
        receipts = {
            model.selection: {"valid": has_real_tool_receipt(probe_receipts.get(model.selection))}
            for model in profiles["zen"]
        }
        capable = [name for name, receipt in receipts.items() if receipt["valid"]]
        run_id = "run_" + uuid.uuid4().hex
        status = "ready" if len(set(capable)) >= 2 else "blocked"
        record = {
            "schema": RUN_SCHEMA,
            "run_id": run_id,
            "status": status,
            "profile": profile,
            "created_at_ms": int(time.time() * 1000),
            "frozen_manifest": manifest,
            "catalog": [
                {
                    "selection": model.selection,
                    "provider": model.provider,
                    "active": model.active,
                    "free": model.free,
                    "zen": model.zen,
                }
                for model in catalog
            ],
            "probe_receipts": receipts,
            "eligible_zen_models": [model.selection for model in profiles["zen"]],
            "nvidia_candidates": [model.selection for model in profiles["nvidia"]],
            "blocker": None if status == "ready" else _BLOCKER,
            "mutations": 0,
            "control_history": [{"action": "start", "status": status}],
        }
        self._artifacts(run_id)
        self.store.write(self._key(run_id), record)
        return record

    def inspect(self, run_id: str) -> dict[str, Any]:
        record = self.store.read(self._key(run_id))
        if record is None or record.get("schema") != RUN_SCHEMA:
            raise KeyError(run_id)
        return record

    def status(self, run_id: str) -> str:
        return str(self.inspect(run_id)["status"])

    def list(self) -> list[dict[str, Any]]:
        records = (self.inspect(key.removeprefix("run-")) for key in self.store.keys("run-run_"))
        return sorted(records, key=lambda item: item["created_at_ms"], reverse=True)

    def control(self, run_id: str, action: str) -> dict[str, Any]:
        record = self.inspect(run_id)
        allowed = {
            "pause": {"ready": "paused"},
            "resume": {"paused": "ready"},
            "stop": {"ready": "stopped", "paused": "stopped", "blocked": "stopped"},
        }
        idempotent = {"pause": "paused", "resume": "ready", "stop": "stopped"}
        if action in idempotent and record["status"] == idempotent[action]:
            return record
        if action not in allowed or record["status"] not in allowed[action]:
            raise ValueError(f"cannot {action} run in {record['status']}")
        record["status"] = allowed[action][record["status"]]
        record["control_history"].append({"action": action, "status": record["status"]})
        self.store.write(self._key(run_id), record)
        return record
