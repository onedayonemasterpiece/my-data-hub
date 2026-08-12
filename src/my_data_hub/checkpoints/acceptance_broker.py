"""Central-only FM15 verifier for credential-free checkpoint acceptance."""

from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path
from uuid import UUID, uuid5

from my_data_hub.acceptance.checkpoint_launcher import _kaggle_input_discovery_source
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.adapter import KaggleProviderAdapter, _canonical_notebook_source
from my_data_hub.providers.kaggle.contracts import (
    KaggleDatasetIdentity,
    KaggleTerminalFailure,
    MutationAction,
    PollPolicy,
    ProviderEffectIntent,
)
from my_data_hub.providers.models import ControlClass

from .brokered_upload import RuntimeUploadAuthority
from .manifest import CheckpointManifest

_EFFECT_NAMESPACE = UUID("c378394a-e1b6-57cb-8c74-50dfe94d6298")
_RECEIPT_NAME = "checkpoint-acceptance-fm15-failure.json"


class CentralBrokeredFM15Verifier:
    """Launch and verify the fixed failing Notebook through the central adapter."""

    def __init__(self, adapter: KaggleProviderAdapter, output_root: Path) -> None:
        self.adapter = adapter
        self.output_root = output_root

    def verify_forced_failure(
        self,
        *,
        exact_version_ref: str,
        dataset_identity: KaggleDatasetIdentity,
        manifest: CheckpointManifest,
        authority: RuntimeUploadAuthority,
    ) -> dict[str, object]:
        if (
            authority.acceptance_scenario != "FM15"
            or authority.verifier_dataset_version_ref is None
            or authority.verifier_notebook_ref is None
            or authority.source_revision is None
        ):
            raise ValueError("FM15 verifier authority is incomplete")
        run_id = uuid5(
            _EFFECT_NAMESPACE,
            f"fm15:{authority.operation_id}:{authority.run_id}:{manifest.checkpoint_id}",
        )
        verifier_ref = authority.verifier_dataset_version_ref
        verifier_parts = verifier_ref.split("/")
        if len(verifier_parts) != 3 or not verifier_parts[2].isdigit():
            raise ValueError("FM15 verifier Dataset ref is not exact numeric")
        verifier_identity = self.adapter.read_private_dataset(
            provider_ref="/".join(verifier_parts[:2]), version=int(verifier_parts[2])
        )
        if (
            verifier_identity.provider_ref != "/".join(verifier_parts[:2])
            or verifier_identity.version != int(verifier_parts[2])
            or verifier_identity.privacy != "private"
        ):
            raise RuntimeError("FM15 verifier Dataset identity differs")
        candidate_provider_ref = exact_version_ref.rsplit("/", 1)[0]
        verifier_provider_ref = verifier_ref.rsplit("/", 1)[0]
        lines = [
            "# fixed central FM15 verifier; no Kaggle credential is available to the producer",
            "import hashlib, json, os, pathlib, runpy",
            *_kaggle_input_discovery_source((exact_version_ref, verifier_ref)),
            (
                f"if _mdh_tree_sha256(dataset_entries[{candidate_provider_ref!r}]) != "
                f"{dataset_identity.package_sha256!r}: "
                "raise RuntimeError('FM15 candidate Dataset package identity differs')"
            ),
            (
                f"if _mdh_tree_sha256(dataset_entries[{verifier_provider_ref!r}]) != "
                f"{verifier_identity.package_sha256!r}: "
                "raise RuntimeError('FM15 verifier Dataset package identity differs')"
            ),
            f"verifier_entries = _mdh_content_entries({verifier_provider_ref!r})",
            (
                "if len(verifier_entries) != 1 or verifier_entries[0]['path'] != 'worker.py' "
                "or verifier_entries[0]['byte_size'] > 1048576: "
                "raise RuntimeError('FM15 verifier file set is unsafe')"
            ),
            f"verifier_worker = roots[{verifier_provider_ref!r}] / 'worker.py'",
            "os.environ.update({",
            f" 'MY_DATA_HUB_FM15_TASK_RUN_ID': {str(run_id)!r},",
            f" 'MY_DATA_HUB_FM15_CANDIDATE_ID': {str(manifest.checkpoint_id)!r},",
            f" 'MY_DATA_HUB_FM15_EXACT_DATASET': {exact_version_ref!r},",
            f" 'MY_DATA_HUB_FM15_SOURCE_REVISION': {authority.source_revision!r},",
            f" 'MY_DATA_HUB_FM15_MANIFEST_SHA256': {manifest.manifest_sha256!r},",
            f" 'MY_DATA_HUB_FM15_EXPECTED_CONTENT_SHA256': {dataset_identity.package_sha256!r},",
            f" 'MY_DATA_HUB_FM15_RECEIPT_NAME': {_RECEIPT_NAME!r},",
            "})",
            "runpy.run_path(str(verifier_worker), run_name='__main__')",
            "raise RuntimeError('MY_DATA_HUB_FIXED_FM15_RESTORE_FAILURE')",
        ]
        source = ("\n".join(lines) + "\n").encode()
        source_sha = hashlib.sha256(_canonical_notebook_source(source, kernel_type="script")).hexdigest()
        intent = ProviderEffectIntent.create(
            operation_id=UUID(authority.operation_id),
            effect_id=uuid5(_EFFECT_NAMESPACE, f"fm15-notebook:{authority.operation_id}"),
            idempotency_key=f"checkpoint-acceptance:FM15:notebook:{authority.operation_id}",
            task_id=UUID(authority.run_id),
            action=MutationAction.PUSH_NOTEBOOK,
            provider_ref=authority.verifier_notebook_ref,
            arguments={
                "task_run_id": str(run_id),
                "source_sha256": source_sha,
                "dataset_sources": (exact_version_ref, authority.verifier_dataset_version_ref),
                "control_class": ControlClass.MCP_MANAGED.value,
                "disposable": True,
            },
            requested_at=manifest.created_at,
        )
        launched = self.adapter.reconcile_private_notebook_mutation(
            intent=intent,
            task_run_id=run_id,
            expected_source_sha256=source_sha,
            dataset_sources=(exact_version_ref, authority.verifier_dataset_version_ref),
            control_class=ControlClass.MCP_MANAGED,
            disposable=True,
        )
        if launched is None:
            launched = self.adapter.push_private_notebook(
                intent=intent,
                task_run_id=run_id,
                source=source,
                title=authority.verifier_notebook_ref.split("/", 1)[1],
                code_file="worker.py",
                kernel_type="script",
                language="python",
                control_class=ControlClass.MCP_MANAGED,
                disposable=True,
                dataset_sources=(exact_version_ref, authority.verifier_dataset_version_ref),
                enable_internet=False,
                timeout_seconds=600,
            )
        try:
            self.adapter.poll_run(
                launched.run,
                PollPolicy(interval_seconds=10, timeout_seconds=600, max_polls=120),
            )
        except KaggleTerminalFailure:
            destination = self.output_root / f"fm15-{run_id}"
            shutil.rmtree(destination, ignore_errors=True)
            identity = self.adapter.download_exact_failed_run_output_file(
                launched.run,
                destination=destination,
                file_name=_RECEIPT_NAME,
                max_bytes=64 * 1024,
            )
        else:
            raise RuntimeError("FM15 fixed failing verifier unexpectedly completed")
        path = destination / _RECEIPT_NAME
        if not path.is_file() or path.is_symlink() or path.stat().st_size > 64 * 1024:
            raise RuntimeError("FM15 failed verifier receipt is unavailable")
        value = json.loads(path.read_bytes())
        expected = {
            "schema_version": "my-data-hub-checkpoint-acceptance-fm15-failure.v1",
            "task_run_id": str(run_id),
            "candidate_checkpoint_id": str(manifest.checkpoint_id),
            "exact_version_ref": exact_version_ref,
            "source_revision": authority.source_revision,
            "manifest_sha256": manifest.manifest_sha256,
            "expected_content_sha256": dataset_identity.package_sha256,
            "detail_code": "FORCED_DISPOSABLE_RESTORE_FAILURE",
            "failure_code": "MY_DATA_HUB_FIXED_FM15_RESTORE_FAILURE",
            "restore_ok": False,
        }
        if value != expected:
            raise RuntimeError("FM15 failed verifier receipt differs from its fixed identity")
        return {
            "expected_failure": True,
            "provider_run_ref": launched.run.provider_run_ref,
            "source_sha256": launched.run.source_sha256,
            "failure_receipt_sha256": hashlib.sha256(canonical_json_bytes(value)).hexdigest(),
            "failed_output_tree_sha256": identity.output_tree_sha256,
        }
