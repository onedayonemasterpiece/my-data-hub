#!/usr/bin/env python3
"""Run bounded real Kaggle gates through the repository's single adapter."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle import KaggleProviderAdapter
from my_data_hub.providers.kaggle.contracts import (
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
)
from my_data_hub.providers.kaggle.control_journal import ControlLedgerKaggleJournal
from my_data_hub.providers.models import ControlClass

EXTERNAL_BLOCKED = 78


def clean_repository_commit() -> str:
    root = Path(__file__).resolve().parents[2]
    commit = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    dirty = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
        raise RuntimeError("real provider receipt requires an exact Git commit")
    if dirty:
        raise RuntimeError("real provider mutation requires a clean repository worktree")
    return commit


class _AnonymousDatasetProbeError(RuntimeError):
    """Shape an anonymous HTTP denial for the adapter's retry classifier."""

    def __init__(self, status_code: int) -> None:
        super().__init__(f"anonymous Kaggle dataset request denied with HTTP {status_code}")
        self.response = SimpleNamespace(status_code=status_code, headers={})


class AnonymousDatasetProbe:
    """Prove that an exact authenticated dataset version is not public."""

    def read_dataset(self, provider_ref: str, version: int) -> object:
        owner, slug = provider_ref.split("/", 1)
        quoted = "/".join(urllib.parse.quote(value, safe="") for value in (owner, slug))
        url = (
            f"https://www.kaggle.com/api/v1/datasets/download/{quoted}"
            f"?datasetVersionNumber={version}"
        )
        request = urllib.request.Request(
            url,
            headers={"Accept": "application/zip", "User-Agent": "my-data-hub-private-proof/1"},
        )
        try:
            with urllib.request.urlopen(request, timeout=20) as response:
                # A successful response is deliberately not read or persisted.  The
                # adapter treats any success as a privacy failure.
                return {"status": response.status}
        except urllib.error.HTTPError as exc:
            raise _AnonymousDatasetProbeError(exc.code) from exc


def modern_token_configured() -> bool:
    configured = bool(os.environ.get("KAGGLE_API_TOKEN", "").strip())
    token_path = Path(os.environ.get("KAGGLE_CONFIG_DIR", "~/.kaggle")).expanduser() / "access_token"
    return configured or (token_path.is_file() and not token_path.is_symlink() and token_path.stat().st_size > 20)


def _effect(
    *,
    action: MutationAction,
    ref: str,
    task_id: UUID,
    arguments: dict[str, object],
    expected=None,  # type: ignore[no-untyped-def]
) -> ProviderEffectIntent:
    effect_id = uuid4()
    return ProviderEffectIntent.create(
        operation_id=uuid4(),
        effect_id=effect_id,
        idempotency_key=f"real-kaggle:{task_id}:{action.value}:{effect_id}",
        task_id=task_id,
        action=action,
        provider_ref=ref,
        arguments=arguments,
        expected_fingerprint=expected,
        requested_at=datetime.now(UTC),
    )


def _notebook_source(*, task_run_id: UUID, provider_ref: str) -> bytes:
    # The source hashes its exact staged bytes at runtime; no self-referential
    # digest is embedded.  Unique task slugs make the first exact source version 1.
    return f'''from __future__ import annotations
import hashlib, json
from pathlib import Path
TASK_RUN_ID = {str(task_run_id)!r}
PROVIDER_REF = {provider_ref!r}
SOURCE_VERSION = 1
source_sha256 = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
receipt = {{
    "schema_version": "my-data-hub-run-receipt.v1",
    "task_run_id": TASK_RUN_ID,
    "provider_ref": PROVIDER_REF,
    "source_version": SOURCE_VERSION,
    "source_sha256": source_sha256,
    "terminal_state": "complete",
}}
Path("/kaggle/working/my-data-hub-run-receipt.json").write_text(
    json.dumps(receipt, sort_keys=True, separators=(",", ":")), encoding="utf-8"
)
Path("/kaggle/working/smoke-output.json").write_text(
    json.dumps({{"ok": True, "task_run_id": TASK_RUN_ID}}, sort_keys=True), encoding="utf-8"
)
'''.encode()


def run_dataset_canary(*, ledger_path: Path, receipt_path: Path) -> int:
    commit_sha = clean_repository_commit()
    ledger = ControlLedger(ledger_path)
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    task_id = uuid4()
    run_id = uuid4()
    started_at = datetime.now(UTC)
    slug = f"mdh-private-canary-{str(run_id)[:8]}"
    ref = f"{adapter.provider_identity().username}/{slug}"
    content = canonical_json_bytes(
        {
            "schema_version": "my-data-hub-provider-canary.v1",
            "task_id": str(task_id),
            "run_id": str(run_id),
        }
    )
    content_tree_sha = hashlib.sha256(
        canonical_json_bytes(
            {
                "files": [
                    {
                        "path": "canary.json",
                        "sha256": hashlib.sha256(content).hexdigest(),
                        "byte_size": len(content),
                    }
                ]
            }
        )
    ).hexdigest()
    create = _effect(
        action=MutationAction.CREATE_DATASET,
        ref=ref,
        task_id=task_id,
        arguments={
            "content_tree_sha256": content_tree_sha,
            "control_class": ControlClass.MCP_MANAGED.value,
            "disposable": True,
        },
    )
    claim = None
    result = None
    proof = None
    cleanup = None
    try:
        result = adapter.create_private_dataset(
            intent=create,
            files={"canary.json": content},
            title=slug,
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
        )
        claim = result.claim
        proof = adapter.prove_private_dataset_access(
            provider_ref=ref,
            version=result.identity.version,
            unauthenticated_probe=AnonymousDatasetProbe(),
        )
    finally:
        if claim is not None:
            delete = _effect(
                action=MutationAction.DELETE_DATASET,
                ref=ref,
                task_id=task_id,
                arguments={
                    "claim_sha256": claim.claim_sha256,
                    "provider_version": claim.provider_version,
                },
                expected=claim.fingerprint,
            )
            cleanup = adapter.delete_task_created_resource(intent=delete, claim=claim)
    if result is None or proof is None or cleanup is None:
        raise RuntimeError("dataset canary has no exact create/privacy/cleanup receipt")
    receipt = {
        "schema_version": "my-data-hub-real-kaggle-canary.v2",
        "scenario": "private_dataset_create_exact_readback_unauthenticated_denial_delete",
        "task_id": str(task_id),
        "run_id": str(run_id),
        "commit_sha": commit_sha,
        "provider_ref": ref,
        "provider_version": result.identity.version,
        "package_sha256": result.identity.package_sha256,
        "claim_sha256": result.claim.claim_sha256,
        "privacy": result.identity.privacy,
        "unauthenticated_http_status": proof.unauthenticated_http_status,
        "denial_class": proof.denial_class,
        "cleanup": cleanup.detail_code,
        "cleanup_outcome": "complete",
        "counts": {"created": 1, "exact_readback": 1, "deleted": 1},
        "gate_results": [
            {"name": "private_create", "outcome": "PASS"},
            {"name": "exact_version_readback", "outcome": "PASS"},
            {"name": "unauthenticated_access_denied", "outcome": "PASS"},
            {"name": "claim_bound_cleanup", "outcome": "PASS"},
        ],
        "blockers": [],
        "started_at": started_at.isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
    }
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_json_bytes(receipt))
    return 0


def run_notebook_canary(*, ledger_path: Path, receipt_path: Path) -> int:
    if not modern_token_configured():
        receipt_path.parent.mkdir(parents=True, exist_ok=True)
        receipt_path.write_bytes(
            canonical_json_bytes(
                {
                    "schema_version": "my-data-hub-real-kaggle-blocker.v1",
                    "gate": "private_notebook_exact_read_run_output",
                    "outcome": "BLOCKED",
                    "blocker_code": "KAGGLE_MODERN_API_TOKEN_REQUIRED",
                    "credential_location": "KAGGLE_API_TOKEN or ~/.kaggle/access_token",
                    "proof_command": (
                        "kaggle auth login && python "
                        "scripts/provider/real_kaggle_matrix.py notebook-canary"
                    ),
                    "observed_at": datetime.now(UTC).isoformat(),
                }
            )
        )
        return EXTERNAL_BLOCKED
    ledger = ControlLedger(ledger_path)
    adapter = KaggleProviderAdapter.from_environment(journal=ControlLedgerKaggleJournal(ledger))
    task_id = uuid4()
    task_run_id = uuid4()
    slug = f"mdh-private-smoke-{str(task_run_id)[:8]}"
    ref = f"{adapter.provider_identity().username}/{slug}"
    source = _notebook_source(task_run_id=task_run_id, provider_ref=ref)
    canonical_source = source
    source_sha = hashlib.sha256(canonical_source).hexdigest()
    create_arguments = {
        "task_run_id": str(task_run_id),
        "source_sha256": source_sha,
        "dataset_sources": (),
        "control_class": ControlClass.MCP_MANAGED.value,
        "disposable": True,
    }
    create = _effect(
        action=MutationAction.PUSH_NOTEBOOK,
        ref=ref,
        task_id=task_id,
        arguments=create_arguments,
    )
    claim = None
    result = None
    cleanup = None
    try:
        result = adapter.push_private_notebook(
            intent=create,
            task_run_id=task_run_id,
            source=source,
            title=slug,
            code_file="run.py",
            kernel_type="script",
            language="python",
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
            enable_internet=False,
            timeout_seconds=900,
        )
        claim = result.claim
        terminal = adapter.poll_run(
            result.run,
            PollPolicy(interval_seconds=15, timeout_seconds=900, max_polls=60),
        )
        output = adapter.read_exact_run_output(result.run)
        receipt = {
            "schema_version": "my-data-hub-real-kaggle-run.v1",
            "scenario": "private_notebook_exact_source_run_output_delete",
            "task_id": str(task_id),
            "task_run_id": str(task_run_id),
            "provider_ref": ref,
            "provider_kernel_id": result.run.provider_kernel_id,
            "source_version": result.run.source_version,
            "source_sha256": result.run.source_sha256,
            "provider_run_ref": result.run.provider_run_ref,
            "terminal_state": terminal.state.value,
            "output_tree_sha256": output.output_tree_sha256,
            "output_receipt_sha256": output.receipt_sha256,
            "privacy": result.source.privacy,
            "control_class": result.claim.control_class.value,
            "completed_at": datetime.now(UTC).isoformat(),
        }
    finally:
        if claim is not None:
            delete_arguments = {
                "claim_sha256": claim.claim_sha256,
                "provider_version": claim.provider_version,
            }
            delete = _effect(
                action=MutationAction.DELETE_NOTEBOOK,
                ref=ref,
                task_id=task_id,
                arguments=delete_arguments,
                expected=claim.fingerprint,
            )
            cleanup = adapter.delete_task_created_resource(intent=delete, claim=claim)
    if result is None or cleanup is None:
        raise RuntimeError("notebook canary has no exact create/cleanup receipt")
    receipt["cleanup"] = cleanup.detail_code
    receipt_path.parent.mkdir(parents=True, exist_ok=True)
    receipt_path.write_bytes(canonical_json_bytes(receipt))
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("preflight", "dataset-canary", "notebook-canary"))
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("~/.local/state/my-data-hub/provider-effects.sqlite3").expanduser(),
    )
    parser.add_argument(
        "--receipt",
        type=Path,
        default=None,
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "preflight":
        payload = {
            "modern_token_configured": modern_token_configured(),
            "legacy_credentials_present": (Path("~/.kaggle/kaggle.json").expanduser().is_file()),
            "private_notebook_exact_read_ready": modern_token_configured(),
        }
        print(json.dumps(payload, sort_keys=True))
        return 0 if payload["private_notebook_exact_read_ready"] else EXTERNAL_BLOCKED
    if args.command == "dataset-canary":
        receipt = args.receipt or Path(
            ".codex/operational-mvp/evidence/real-kaggle/dataset-canary.json"
        )
        return run_dataset_canary(ledger_path=args.ledger, receipt_path=receipt)
    receipt = args.receipt or Path(
        ".codex/operational-mvp/evidence/real-kaggle/notebook-canary.json"
    )
    return run_notebook_canary(ledger_path=args.ledger, receipt_path=receipt)


if __name__ == "__main__":
    raise SystemExit(main())
