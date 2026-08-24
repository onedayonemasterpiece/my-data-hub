#!/usr/bin/env python3
"""Run and exactly clean one no-secret consumer of the frozen E5 producer."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import tempfile
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal, KaggleProviderAdapter
from my_data_hub.providers.kaggle.adapter import executable_source_sha256
from my_data_hub.providers.kaggle.contracts import KernelState, MutationAction, ProviderEffectIntent
from my_data_hub.providers.models import ControlClass
from my_data_hub.workloads.region_talk.text_runtimes import (
    read_e5_frozen_producer_authority,
)

IMAGE = "gcr.io/kaggle-images/python@sha256:c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2"
OUTPUT = "region-talk-e5-frozen-consumer-smoke.v1.json"


def _clean(root: Path) -> str:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain=v1", "-z"], cwd=root)
    if len(commit) != 40 or dirty:
        raise RuntimeError("live E5 consumer smoke requires one clean exact source")
    return commit


def _source(root: Path, task: UUID, authority: dict[str, object]) -> bytes:
    raw = (root / "scripts/provider/assets/region_talk_e5_frozen_consumer_smoke.py").read_text()
    manifest = (
        root
        / "src/my_data_hub/workloads/region_talk/assets/region-talk-e5-assets.v1.json"
    ).read_text().strip()
    replacements = {
        "R21_E5_CONSUMER_TASK": repr(str(task)),
        "R21_E5_CONSUMER_MANIFEST": repr(manifest),
        "R21_E5_CONSUMER_AUTHORITY": repr(str(authority["authority_sha256"])),
    }
    for marker, replacement in replacements.items():
        quoted_marker = f'"{marker}"'
        if raw.count(quoted_marker) != 1:
            raise RuntimeError(f"E5 consumer marker differs: {marker}")
        raw = raw.replace(quoted_marker, replacement, 1)
    compile(raw, "region_talk_e5_frozen_consumer_smoke.py", "exec")
    return raw.encode()


def _write_private(path: Path, value: dict[str, object]) -> None:
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("consumer smoke receipt path must be absolute and non-symlink")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical_json_bytes(value) + b"\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        Path(temporary).unlink(missing_ok=True)


def run(args: argparse.Namespace) -> dict[str, object]:
    root = Path(__file__).resolve().parents[2]
    commit = _clean(root)
    authority = read_e5_frozen_producer_authority()
    ledger = ControlLedger(args.ledger.resolve())
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    recovered = adapter.reconcile_private_notebook_run(
        task_run_id=UUID(str(authority["task_run_id"])),
        provider_ref=str(authority["provider_ref"]),
        expected_source_sha256=str(authority["source_sha256"]),
    )
    if (
        recovered is None
        or recovered.source_version != authority["provider_version"]
        or recovered.provider_kernel_id != authority["provider_kernel_id"]
        or recovered.provider_run_ref != authority["provider_run_ref"]
        or adapter.read_run_status(recovered).state is not KernelState.COMPLETE
    ):
        raise RuntimeError("frozen E5 producer source/version fence failed")

    canary = args.canary_id or uuid4()
    task = uuid5(NAMESPACE_URL, f"region-talk-e5-consumer:{canary}:{commit}")
    operation = uuid5(NAMESPACE_URL, f"region-talk-e5-consumer-op:{task}")
    source = _source(root, task, authority)
    source_sha = executable_source_sha256(source, kernel_type="script")
    owner = adapter.provider_identity().username
    ref = f"{owner}/mdh-region-talk-e5-consumer-smoke"
    kernel_sources = (str(authority["provider_ref"]),)
    intent = ProviderEffectIntent.create(
        operation_id=operation,
        effect_id=uuid5(NAMESPACE_URL, f"region-talk-e5-consumer-push:{task}"),
        idempotency_key=f"region-talk-e5-consumer-push:{task}",
        task_id=task,
        action=MutationAction.PUSH_NOTEBOOK,
        provider_ref=ref,
        arguments={
            "task_run_id": str(task),
            "source_sha256": source_sha,
            "dataset_sources": (),
            "kernel_sources": kernel_sources,
            "control_class": "orchestrator_protected",
            "disposable": True,
            "docker_image": IMAGE,
            "docker_image_pinning_type": "original",
        },
        requested_at=datetime.now(UTC),
    )
    launched = adapter.push_private_dependency_smoke_notebook(
        intent=intent,
        task_run_id=task,
        source=source,
        title=ref.split("/", 1)[1],
        code_file="smoke.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=True,
        kernel_sources=kernel_sources,
        enable_internet=False,
        timeout_seconds=3600,
        docker_image=IMAGE,
        docker_image_pinning_type="original",
    )
    try:
        deadline = time.monotonic() + 3600
        while True:
            status = adapter.read_run_status(launched.run)
            if status.state in {KernelState.COMPLETE, KernelState.FAILED}:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("frozen E5 consumer smoke exceeded one hour")
            time.sleep(20)
        if status.state is not KernelState.COMPLETE:
            raise RuntimeError(f"frozen E5 consumer smoke failed: {status.provider_status}")
        with tempfile.TemporaryDirectory(prefix="mdh-e5-consumer-smoke-") as folder:
            destination = Path(folder)
            adapter.download_exact_run_output_file(
                launched.run, destination=destination, file_name=OUTPUT, max_bytes=256 * 1024
            )
            body = (destination / OUTPUT).read_bytes()
        value = json.loads(body)
        unsigned = {key: item for key, item in value.items() if key != "receipt_sha256"}
        if (
            body != canonical_json_bytes(value)
            or value.get("task_run_id") != str(task)
            or value.get("producer_authority_sha256") != authority["authority_sha256"]
            or value.get("receipt_sha256")
            != hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
            or value.get("notebook_credentials") is not False
            or value.get("publication_dispatch") is not False
            or value.get("notification_dispatch") is not False
        ):
            raise RuntimeError("frozen E5 consumer receipt differs")
        _write_private(args.receipt.resolve(), value)
        return value
    finally:
        claim = launched.claim
        delete_intent = ProviderEffectIntent.create(
            operation_id=operation,
            effect_id=uuid5(NAMESPACE_URL, f"region-talk-e5-consumer-delete:{task}"),
            idempotency_key=f"region-talk-e5-consumer-delete:{task}",
            task_id=task,
            action=MutationAction.DELETE_NOTEBOOK,
            provider_ref=claim.provider_ref,
            expected_fingerprint=claim.fingerprint,
            arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
            requested_at=datetime.now(UTC),
        )
        adapter.delete_task_created_resource(intent=delete_intent, claim=claim)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--canary-id", type=UUID)
    value = run(parser.parse_args())
    print(json.dumps({"receipt_sha256": value["receipt_sha256"]}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
