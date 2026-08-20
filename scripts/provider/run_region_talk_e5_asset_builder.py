#!/usr/bin/env python3
"""Launch the one frozen protected E5 v1 producer through the central KPA."""

from __future__ import annotations

import argparse
import hashlib
import json
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal, KaggleProviderAdapter
from my_data_hub.providers.kaggle.adapter import executable_source_sha256
from my_data_hub.providers.kaggle.contracts import MutationAction, ProviderEffectIntent
from my_data_hub.providers.models import ControlClass

IMAGE = "gcr.io/kaggle-images/python@sha256:c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2"
IMAGE_COMMIT = "fc61d5cda7da39530055bae9bd0e92865f995cd9"
RECEIPT = "region-talk-e5-frozen-producer-receipt.v1.json"
FAILURE = "region-talk-e5-frozen-producer-failure.v1.json"


def _clean(root: Path) -> str:
    c = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    d = subprocess.check_output(["git", "status", "--porcelain=v1", "-z"], cwd=root)
    if len(c) != 40 or d:
        raise RuntimeError("frozen E5 builder requires clean exact source")
    return c


def _source(root: Path, task: UUID, commit: str) -> bytes:
    raw = (root / "scripts/provider/assets/region_talk_e5_asset_builder.py").read_text()
    trees = json.loads(
        (root / "src/my_data_hub/workloads/region_talk/assets/text-model-official-trees.v1.json").read_bytes()
    )
    e5 = next(x for x in trees["entries"] if x["stage"] == "e5_embedding")
    e5["official_tree_receipt_sha256"] = trees["receipt_sha256"]
    bank = json.loads((root / "src/my_data_hub/workloads/region_talk/assets/semantic-bank.v1.json").read_bytes())
    replacements = {
        "R21_E5_TASK_RUN_ID": repr(str(task)),
        "R21_E5_SOURCE_COMMIT": repr(commit),
        "R21_E5_OFFICIAL_TREE_JSON": repr(json.dumps(e5, sort_keys=True, separators=(",", ":"))),
        "R21_E5_SEMANTIC_BANK_JSON": repr(json.dumps(bank, ensure_ascii=False, sort_keys=True, separators=(",", ":"))),
    }
    for marker, value in replacements.items():
        if raw.count(marker) != 1:
            raise RuntimeError(f"builder marker differs: {marker}")
        raw = raw.replace(marker, value, 1)
    return raw.encode()


def run(args: argparse.Namespace) -> dict:
    root = Path(__file__).resolve().parents[2]
    commit = _clean(root)
    task = uuid5(NAMESPACE_URL, f"region-talk-e5-frozen-v1:{commit}")
    op = uuid5(NAMESPACE_URL, f"region-talk-e5-frozen-v1-op:{task}")
    source = _source(root, task, commit)
    sha = executable_source_sha256(source, kernel_type="script")
    ledger = ControlLedger(args.ledger.resolve())
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    owner = adapter.provider_identity().username
    ref = f"{owner}/mdh-region-talk-e5-assets-v1"
    intent = ProviderEffectIntent.create(
        operation_id=op,
        effect_id=uuid5(NAMESPACE_URL, f"region-talk-e5-frozen-v1-push:{task}"),
        idempotency_key=f"region-talk-e5-frozen-v1-push:{task}",
        task_id=task,
        action=MutationAction.PUSH_NOTEBOOK,
        provider_ref=ref,
        arguments={
            "task_run_id": str(task),
            "source_sha256": sha,
            "dataset_sources": (),
            "control_class": "orchestrator_protected",
            "disposable": False,
            "docker_image": IMAGE,
            "docker_image_pinning_type": "original",
        },
        requested_at=datetime.now(UTC),
    )
    launched = adapter.push_private_master_notebook_pending_attestation(
        intent=intent,
        task_run_id=task,
        source=source,
        title=ref.split("/", 1)[1],
        code_file="builder.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        enable_internet=True,
        timeout_seconds=3600,
        docker_image=IMAGE,
        docker_image_pinning_type="original",
    )
    deadline = time.monotonic() + 3600
    while True:
        status = adapter.read_run_status(launched.run)
        if status.state.value in {"complete", "failed"}:
            break
        if time.monotonic() >= deadline:
            raise TimeoutError("frozen E5 builder exceeded one hour")
        time.sleep(20)
    if status.state.value == "failed":
        with tempfile.TemporaryDirectory() as d:
            adapter.download_exact_failed_run_output_file(
                launched.run, destination=Path(d), file_name=FAILURE, max_bytes=64 * 1024
            )
            failure = json.loads((Path(d) / FAILURE).read_bytes())
        raise RuntimeError(f"E5 builder failed at {failure.get('stage')}: {failure.get('exception_type')}")
    with tempfile.TemporaryDirectory() as d:
        adapter.download_exact_run_output_file(
            launched.run, destination=Path(d), file_name=RECEIPT, max_bytes=256 * 1024
        )
        raw = (Path(d) / RECEIPT).read_bytes()
    value = json.loads(raw)
    if raw != canonical_json_bytes(value):
        raise RuntimeError("E5 builder receipt is not canonical")
    authority = {
        "schema_version": "region-talk-e5-frozen-producer-authority.v1",
        "provider_ref": ref,
        "provider_version": launched.run.source_version,
        "provider_kernel_id": launched.run.provider_kernel_id,
        "provider_run_ref": launched.run.provider_run_ref,
        "task_run_id": str(task),
        "source_commit": commit,
        "source_sha256": sha,
        "image_identity": IMAGE,
        "image_source_commit": IMAGE_COMMIT,
        "claim": launched.claim.model_dump(mode="json"),
        "producer_receipt": value,
        "publication_dispatch": False,
        "notification_dispatch": False,
    }
    authority["authority_sha256"] = hashlib.sha256(canonical_json_bytes(authority)).hexdigest()
    args.authority.parent.mkdir(parents=True, exist_ok=True)
    args.authority.write_bytes(canonical_json_bytes(authority) + b"\n")
    return authority


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--ledger", required=True, type=Path)
    p.add_argument("--authority", required=True, type=Path)
    a = p.parse_args()
    v = run(a)
    print(
        json.dumps(
            {
                "provider_run_ref": v["provider_run_ref"],
                "authority_sha256": v["authority_sha256"],
                "producer_receipt_sha256": v["producer_receipt"]["receipt_sha256"],
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
