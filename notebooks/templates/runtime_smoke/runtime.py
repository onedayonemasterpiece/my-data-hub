"""Primary source for the private Kaggle platform/runtime smoke notebook."""

from __future__ import annotations

import os
from pathlib import Path

from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.runtime_sdk import RuntimeClient, RuntimeEventType


def _required(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise RuntimeError(f"required runtime value is absent: {name}")
    return value


def main() -> int:
    output = Path("/kaggle/working")
    client = RuntimeClient(
        callback_url=_required("MY_DATA_HUB_CALLBACK_URL"),
        run_secret=_required("MY_DATA_HUB_RUN_SECRET"),
        run_id=_required("MY_DATA_HUB_RUN_ID"),
        attempt_id=_required("MY_DATA_HUB_ATTEMPT_ID"),
        service_instance_id=_required("MY_DATA_HUB_SERVICE_INSTANCE_ID"),
        source_identity=_required("MY_DATA_HUB_SOURCE_IDENTITY"),
        source_version=_required("MY_DATA_HUB_SOURCE_VERSION"),
        epoch=int(_required("MY_DATA_HUB_EPOCH")),
        spool_path=output / "runtime-events.jsonl",
        heartbeat_interval_seconds=5.0,
    )
    client.replay_pending()
    client.emit(RuntimeEventType.RUNTIME_STARTED, phase="smoke", status="running")
    client.emit(RuntimeEventType.RUNTIME_HEARTBEAT, phase="smoke", status="healthy", data={"step": 1})
    receipt = {
        "schema_version": "my-data-hub-run-receipt.v1",
        "task_run_id": client.run_id,
        "provider_ref": _required("MY_DATA_HUB_SOURCE_IDENTITY"),
        "source_version": _required("MY_DATA_HUB_SOURCE_VERSION"),
        "source_sha256": _required("MY_DATA_HUB_SOURCE_SHA256"),
        "terminal_state": "complete",
        "output_sha256": sha256_value({"runtime_contract": "platform-runtime-smoke.v1"}),
    }
    (output / "my-data-hub-run-receipt.json").write_bytes(canonical_json_bytes(receipt))
    client.emit(RuntimeEventType.RUNTIME_TERMINAL, phase="smoke", status="complete", data={"ok": True})
    return 0
