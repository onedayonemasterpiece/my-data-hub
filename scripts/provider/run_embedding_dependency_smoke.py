#!/usr/bin/env python3
"""Run one disposable, centrally verified Gate K dependency smoke.

The sole credentialed process creates a task-owned private input Dataset,
launches the credential-free/offline smoke Notebook, verifies its exact output,
and removes both resources.  No Kaggle credential is projected into inputs or
source.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import time
from datetime import UTC, datetime
from pathlib import Path
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.embeddings.dependency_smoke import CentralDependencySmoke
from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.providers.kaggle import ControlLedgerKaggleJournal, KaggleProviderAdapter
from my_data_hub.providers.kaggle.contracts import MutationAction, ProviderEffectIntent, TaskResourceClaim
from my_data_hub.providers.models import ControlClass, ProviderKind

_SHA40 = re.compile(r"^[a-f0-9]{40}$")


def _clean_commit(root: Path) -> str:
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=root, text=True).strip()
    dirty = subprocess.check_output(["git", "status", "--porcelain=v1", "-z"], cwd=root)
    if not _SHA40.fullmatch(commit) or dirty:
        raise RuntimeError("live dependency smoke requires one clean exact source commit")
    return commit


def _atomic_json(path: Path, value: dict[str, object]) -> None:
    if not path.is_absolute() or path.is_symlink():
        raise ValueError("dependency smoke path must be absolute and non-symlink")
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    path.parent.chmod(0o700)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    temporary.write_bytes(canonical_json_bytes(value))
    temporary.chmod(0o600)
    os.replace(temporary, path)


def _asset_files(bundle: Path, expected_commit: str) -> tuple[dict[str, bytes], dict[str, object]]:
    manifest_path = bundle / "master-asset-bundle.json"
    manifest_body = manifest_path.read_bytes()
    manifest = json.loads(manifest_body)
    if (
        manifest_body != canonical_json_bytes(manifest)
        or manifest.get("source_commit") != expected_commit
        or manifest.get("worker_runtime", {}).get("docker_image_pinning_type") != "original"
    ):
        raise ValueError("master asset bundle is not canonical/current")
    dataset = bundle / "dataset"
    dependency_path = dataset / "embedding-worker-dependencies.json"
    dependency = json.loads(dependency_path.read_bytes())
    names = {
        "embedding-worker-dependencies.json",
        "embedding-dependency-smoke.py",
        *(f"embedding-worker-wheelhouse/{item['filename']}" for item in dependency["wheels"]),
        manifest["assets"]["wheel"]["path"].removeprefix("dataset/"),
    }
    files: dict[str, bytes] = {}
    for name in sorted(names):
        path = dataset / name
        if path.is_symlink() or not path.is_file() or path.stat().st_mode & 0o077:
            raise ValueError("dependency smoke asset is missing or unsafe")
        files[name] = path.read_bytes()
    expected = {
        f"embedding-worker-wheelhouse/{item['filename']}": item["sha256"]
        for item in dependency["wheels"]
    }
    for name, digest in expected.items():
        if hashlib.sha256(files[name]).hexdigest() != digest:
            raise ValueError("dependency smoke wheel differs from bundle manifest")
    return files, manifest


def _delete(adapter: KaggleProviderAdapter, claim: TaskResourceClaim, operation_id: UUID) -> None:
    intent = ProviderEffectIntent.create(
        operation_id=operation_id,
        effect_id=uuid5(NAMESPACE_URL, f"dependency-smoke-input-delete:{operation_id}"),
        idempotency_key=f"dependency-smoke-input-delete:{operation_id}",
        task_id=claim.task_id,
        action=MutationAction.DELETE_DATASET,
        provider_ref=claim.provider_ref,
        expected_fingerprint=claim.fingerprint,
        arguments={"claim_sha256": claim.claim_sha256, "provider_version": claim.provider_version},
        requested_at=datetime.now(UTC),
    )
    adapter.delete_task_created_resource(intent=intent, claim=claim)


def _inventory_absence(adapter: KaggleProviderAdapter, refs: set[str]) -> str:
    seen: list[dict[str, str]] = []
    for kind in (ProviderKind.DATASET, ProviderKind.NOTEBOOK):
        cursor: str | None = None
        cursors: set[str] = set()
        for _ in range(20):
            page = adapter.list_resources(kind=kind, cursor=cursor, limit=100)
            for item in page.resources:
                if item.provider_ref in refs:
                    raise RuntimeError("cleaned dependency smoke resource remains in provider inventory")
                seen.append({"kind": kind.value, "provider_ref": item.provider_ref})
            if page.next_cursor is None:
                break
            if page.next_cursor in cursors:
                raise RuntimeError("provider inventory cursor repeated")
            cursors.add(page.next_cursor)
            cursor = page.next_cursor
        else:
            raise RuntimeError("provider inventory exceeded its bounded page count")
    return sha256_value({"absent": sorted(refs), "inventory": seen})


def run(args: argparse.Namespace) -> dict[str, object]:
    root = Path(__file__).resolve().parents[2]
    commit = _clean_commit(root)
    bundle = args.bundle.expanduser().resolve()
    subprocess.run(
        [
            str(root / ".venv/bin/python"),
            str(root / "scripts/provider/verify_master_assets.py"),
            "--bundle",
            str(bundle),
            "--expected-commit",
            commit,
        ],
        check=True,
        stdout=subprocess.DEVNULL,
    )
    files, manifest = _asset_files(bundle, commit)
    ledger = ControlLedger(args.ledger.expanduser().resolve())
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    owner = adapter.provider_identity().username
    canary_id = args.canary_id or uuid4()
    operation_id = uuid5(NAMESPACE_URL, f"embedding-dependency-live:{canary_id}:{commit}")
    dataset_ref = f"{owner}/mdh-embed-deps-{canary_id.hex[:16]}"
    intent = ProviderEffectIntent.create(
        operation_id=operation_id,
        effect_id=uuid5(NAMESPACE_URL, f"dependency-smoke-input:{operation_id}"),
        idempotency_key=f"dependency-smoke-input:{operation_id}",
        task_id=canary_id,
        action=MutationAction.CREATE_DATASET,
        provider_ref=dataset_ref,
        arguments={
            "content_tree_sha256": __import__(
                "my_data_hub.providers.kaggle.adapter", fromlist=["mapping_sha256"]
            ).mapping_sha256(files),
            "control_class": ControlClass.ORCHESTRATOR_PROTECTED.value,
            "disposable": True,
        },
        requested_at=datetime.now(UTC),
    )
    created = adapter.create_private_dataset(
        intent=intent,
        files=files,
        title=dataset_ref.split("/", 1)[1],
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=True,
    )
    runtime = manifest["worker_runtime"]
    dependency = files["embedding-worker-dependencies.json"]
    project_name = next(name for name in files if name.endswith(".whl") and "/" not in name)
    smoke = CentralDependencySmoke(
        adapter=adapter,
        owner=owner,
        runtime_dataset_exact_ref=f"{dataset_ref}/{created.claim.provider_version}",
        image_identity=runtime["image_identity"],
        image_source_commit=runtime["source_commit"],
        project_wheel_sha256=hashlib.sha256(files[project_name]).hexdigest(),
        project_wheel_relative_path=project_name,
        dependency_manifest_sha256=hashlib.sha256(dependency).hexdigest(),
        state_path=args.state.expanduser().resolve(),
        receipt_path=args.smoke_receipt.expanduser().resolve(),
    )
    deadline = time.monotonic() + 1800
    receipt = None
    try:
        while receipt is None and time.monotonic() < deadline:
            receipt = smoke.run_once()
            if receipt is None:
                time.sleep(10)
        if receipt is None:
            raise TimeoutError("dependency smoke did not complete in 30 minutes")
    except Exception:
        _delete(adapter, created.claim, operation_id)
        raise
    _delete(adapter, created.claim, operation_id)
    notebook_ref = f"{owner}/mdh-embedding-dependency-smoke"
    inventory_sha = _inventory_absence(adapter, {dataset_ref, notebook_ref})
    result: dict[str, object] = {
        "schema_version": "my-data-hub-embedding-dependency-live-receipt.v1",
        "outcome": "PASS",
        "source_commit": commit,
        "canary_id": str(canary_id),
        "dataset_ref": dataset_ref,
        "dataset_version": created.claim.provider_version,
        "provider_run_ref": receipt.provider_run_ref,
        "smoke_receipt_sha256": receipt.receipt_sha256,
        "dependency_manifest_sha256": receipt.dependency_manifest_sha256,
        "project_wheel_sha256": receipt.project_wheel_sha256,
        "image_identity": receipt.image_identity,
        "image_source_commit": receipt.image_source_commit,
        "cleanup_refs": sorted((dataset_ref, notebook_ref)),
        "inventory_absent": True,
        "inventory_sha256": inventory_sha,
        "central_kaggle_adapter_instances": 1,
        "notebook_kaggle_credentials": False,
        "completed_at": datetime.now(UTC).isoformat(),
    }
    _atomic_json(args.receipt.expanduser().resolve(), result)
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--smoke-receipt", type=Path, required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--canary-id", type=UUID)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run(args)
    print(json.dumps({"outcome": result["outcome"], "receipt": str(args.receipt.expanduser().resolve())}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
