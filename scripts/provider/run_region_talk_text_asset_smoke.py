#!/usr/bin/env python3
"""Run one disposable central Kaggle smoke for exact Region Talk model sources."""

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
from my_data_hub.providers.kaggle.contracts import MutationAction, ProviderEffectIntent
from my_data_hub.providers.models import ControlClass
from my_data_hub.workloads.region_talk.text_asset_acquisition import (
    EXPECTED_MODEL_SOURCES,
    TextAssetAcquisitionReceipt,
    TextAssetSmokeObservation,
    compare_smoke_to_official,
    official_trees_sha256,
    read_official_model_trees,
)
from my_data_hub.workloads.region_talk.text_runtimes import SemanticBankDocument

IMAGE_IDENTITY = "gcr.io/kaggle-images/python@sha256:c1fa4de30bc268e601e6dcddb6ceb2519b9adde3527dbbfb05e6bdfbbbdcd1a2"
IMAGE_SOURCE_COMMIT = "fc61d5cda7da39530055bae9bd0e92865f995cd9"
OBSERVATION = "region-talk-text-asset-observation.json"
FAILURE = "region-talk-text-asset-failure.json"


def _clean_commit(root: Path) -> str:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain=v1", "-z"], cwd=root)
    if len(commit) != 40 or dirty:
        raise RuntimeError("live text-asset smoke requires one clean exact source commit")
    return commit


def _write_private(path: Path, body: bytes) -> None:
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("text-asset receipt path must be absolute and non-symlink")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    fd, raw = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(raw, path)
    finally:
        Path(raw).unlink(missing_ok=True)


def _source(root: Path, task_id: UUID) -> bytes:
    path = root / "scripts/provider/assets/region_talk_text_asset_smoke.py"
    raw = path.read_text()
    marker = "R21_TASK_RUN_ID_REPLACED_BY_CENTRAL_RUNNER"
    if raw.count(marker) != 2:
        raise ValueError("text-asset smoke task marker differs")
    # Replace only the assignment value; the fail-closed comparison retains its marker.
    return raw.replace(f'TASK_RUN_ID = "{marker}"', f'TASK_RUN_ID = "{task_id}"', 1).encode()


def _delete(adapter: KaggleProviderAdapter, launched: object, operation_id: UUID, task_id: UUID) -> None:
    claim = launched.claim  # type: ignore[attr-defined]
    intent = ProviderEffectIntent.create(
        operation_id=operation_id,
        effect_id=uuid5(NAMESPACE_URL, f"r21-text-asset-smoke-delete:{task_id}"),
        idempotency_key=f"r21-text-asset-smoke-delete:{task_id}",
        task_id=task_id,
        action=MutationAction.DELETE_NOTEBOOK,
        provider_ref=claim.provider_ref,
        expected_fingerprint=claim.fingerprint,
        arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
        requested_at=datetime.now(UTC),
    )
    adapter.delete_task_created_resource(intent=intent, claim=claim)


def run(args: argparse.Namespace) -> TextAssetAcquisitionReceipt:
    root = Path(__file__).resolve().parents[2]
    commit = _clean_commit(root)
    canary_id = args.canary_id or uuid4()
    task_id = uuid5(NAMESPACE_URL, f"r21-text-assets:{canary_id}:{commit}")
    operation_id = uuid5(NAMESPACE_URL, f"r21-text-assets-operation:{task_id}")
    source = _source(root, task_id)
    source_sha = executable_source_sha256(source, kernel_type="script")
    model_sources = tuple(EXPECTED_MODEL_SOURCES[stage] for stage in ("e5_embedding", "bge_m3_embedding"))
    ledger = ControlLedger(args.ledger.expanduser().resolve())
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    owner = adapter.provider_identity().username
    notebook_ref = f"{owner}/mdh-region-talk-text-asset-smoke"
    intent = ProviderEffectIntent.create(
        operation_id=operation_id,
        effect_id=uuid5(NAMESPACE_URL, f"r21-text-assets-push:{task_id}"),
        idempotency_key=f"r21-text-assets-push:{task_id}",
        task_id=task_id,
        action=MutationAction.PUSH_NOTEBOOK,
        provider_ref=notebook_ref,
        arguments={
            "task_run_id": str(task_id),
            "source_sha256": source_sha,
            "dataset_sources": (),
            "model_sources": model_sources,
            "control_class": "orchestrator_protected",
            "disposable": True,
            "docker_image": IMAGE_IDENTITY,
            "docker_image_pinning_type": "original",
        },
        requested_at=datetime.now(UTC),
    )
    launched = adapter.push_private_dependency_smoke_notebook(
        intent=intent,
        task_run_id=task_id,
        source=source,
        title=notebook_ref.split("/", 1)[1],
        code_file="smoke.py",
        kernel_type="script",
        language="python",
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=True,
        model_sources=model_sources,
        enable_internet=False,
        docker_image=IMAGE_IDENTITY,
        docker_image_pinning_type="original",
        timeout_seconds=2400,
    )
    deadline = time.monotonic() + 2400
    try:
        while True:
            status = adapter.read_run_status(launched.run)
            if str(status.state) in {"complete", "failed"}:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError("text-asset smoke exceeded 40 minutes")
            time.sleep(15)
        if str(status.state) == "failed":
            with tempfile.TemporaryDirectory(prefix="mdh-r21-text-asset-failure-") as folder:
                destination = Path(folder)
                adapter.download_exact_failed_run_output_file(
                    launched.run,
                    destination=destination,
                    file_name=FAILURE,
                    max_bytes=64 * 1024,
                )
                failure = json.loads((destination / FAILURE).read_bytes())
            raise RuntimeError(
                f"text-asset smoke failed at {failure.get('stage')}: {failure.get('exception_type')}"
            )
        with tempfile.TemporaryDirectory(prefix="mdh-r21-text-asset-") as folder:
            destination = Path(folder)
            adapter.download_exact_run_output_file(
                launched.run,
                destination=destination,
                file_name=OBSERVATION,
                max_bytes=256 * 1024,
            )
            raw = (destination / OBSERVATION).read_bytes()
        if raw != canonical_json_bytes(json.loads(raw)):
            raise ValueError("text-asset smoke output is not canonical JSON")
        observation = TextAssetSmokeObservation.model_validate_json(raw)
        if observation.task_run_id != str(task_id):
            raise ValueError("text-asset smoke output differs from exact task")
        if args.observation is not None:
            _write_private(args.observation.expanduser().resolve(), raw)
        official = read_official_model_trees()
        results = compare_smoke_to_official(observation, official)
        semantic_path = (
            root / "src/my_data_hub/workloads/region_talk/assets/semantic-bank.v1.json"
        )
        semantic = SemanticBankDocument.model_validate_json(semantic_path.read_bytes())
        equivalent = sum(result.status == "byte_equivalent" for result in results)
        outcome = "PASS" if equivalent == 2 else "PARTIAL" if equivalent == 1 else "MISMATCH"
        receipt = TextAssetAcquisitionReceipt(
            outcome=outcome,
            observed_at=datetime.now(UTC),
            provider_run_ref=launched.run.provider_run_ref,
            source_commit=commit,
            source_sha256=source_sha,
            image_identity=IMAGE_IDENTITY,
            image_source_commit=IMAGE_SOURCE_COMMIT,
            official_trees_sha256=official_trees_sha256(),
            semantic_bank_sha256=semantic.semantic_bank_sha256,
            observation_sha256=hashlib.sha256(raw).hexdigest(),
            results=results,
        )
        _write_private(args.receipt.expanduser().resolve(), canonical_json_bytes(receipt.model_dump(mode="json")))
        return receipt
    finally:
        _delete(adapter, launched, operation_id, task_id)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ledger", required=True, type=Path)
    parser.add_argument("--receipt", required=True, type=Path)
    parser.add_argument("--observation", type=Path)
    parser.add_argument("--canary-id", type=UUID)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    receipt = run(args)
    print(
        json.dumps(
            {
                "outcome": receipt.outcome,
                "provider_run_ref": receipt.provider_run_ref,
                "receipt_sha256": receipt.receipt_sha256,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
