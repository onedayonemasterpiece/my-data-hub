from __future__ import annotations

import hashlib
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid5

from my_data_hub.checkpoints.acceptance_broker import CentralBrokeredFM15Verifier
from my_data_hub.checkpoints.brokered_upload import RuntimeUploadAuthority
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle.contracts import KaggleDatasetIdentity, KaggleTerminalFailure
from my_data_hub.providers.models import ProviderFingerprint

NOW = datetime(2026, 8, 12, 12, tzinfo=UTC)
EFFECT_NAMESPACE = UUID("c378394a-e1b6-57cb-8c74-50dfe94d6298")


class BrokerAdapter:
    def __init__(self, manifest, authority, candidate: KaggleDatasetIdentity) -> None:  # type: ignore[no-untyped-def]
        self.manifest = manifest
        self.authority = authority
        self.candidate = candidate
        self.source = b""
        self.dataset_sources: tuple[str, ...] = ()
        self.read_calls: list[tuple[str, int]] = []

    def read_private_dataset(self, *, provider_ref: str, version: int) -> KaggleDatasetIdentity:
        self.read_calls.append((provider_ref, version))
        return KaggleDatasetIdentity(
            provider_ref=provider_ref,
            version=version,
            privacy="private",
            package_sha256="e" * 64,
            fingerprint=ProviderFingerprint(value="f" * 64),
            observed_at=NOW,
        )

    def reconcile_private_notebook_mutation(self, **_kwargs):  # type: ignore[no-untyped-def]
        return None

    def push_private_notebook(self, *, source: bytes, dataset_sources, **_kwargs):  # type: ignore[no-untyped-def]
        self.source = source
        self.dataset_sources = tuple(dataset_sources)
        return SimpleNamespace(
            run=SimpleNamespace(
                provider_run_ref="owner/fm15-verifier/7",
                source_sha256=hashlib.sha256(source).hexdigest(),
            )
        )

    def poll_run(self, _run, _policy) -> None:  # type: ignore[no-untyped-def]
        raise KaggleTerminalFailure("expected fixed failure")

    def download_exact_failed_run_output_file(
        self, _run, *, destination: Path, file_name: str, max_bytes: int
    ):  # type: ignore[no-untyped-def]
        assert max_bytes == 64 * 1024
        destination.mkdir(parents=True)
        run_id = uuid5(
            EFFECT_NAMESPACE,
            f"fm15:{self.authority.operation_id}:{self.authority.run_id}:{self.manifest.checkpoint_id}",
        )
        value = {
            "schema_version": "my-data-hub-checkpoint-acceptance-fm15-failure.v1",
            "task_run_id": str(run_id),
            "candidate_checkpoint_id": str(self.manifest.checkpoint_id),
            "exact_version_ref": "owner/candidate/5",
            "source_revision": self.authority.source_revision,
            "manifest_sha256": self.manifest.manifest_sha256,
            "expected_content_sha256": self.candidate.package_sha256,
            "detail_code": "FORCED_DISPOSABLE_RESTORE_FAILURE",
            "failure_code": "MY_DATA_HUB_FIXED_FM15_RESTORE_FAILURE",
            "restore_ok": False,
        }
        (destination / file_name).write_bytes(canonical_json_bytes(value))
        return SimpleNamespace(output_tree_sha256="9" * 64)


def test_fm15_broker_intent_uses_claim_bound_discovery_not_a_slug_mount(tmp_path: Path) -> None:
    manifest = SimpleNamespace(
        checkpoint_id=UUID("33333333-3333-4333-8333-333333333333"),
        manifest_sha256="a" * 64,
        created_at=NOW,
    )
    authority = RuntimeUploadAuthority(
        operation_id="11111111-1111-4111-8111-111111111111",
        run_id="22222222-2222-4222-8222-222222222222",
        attempt_id="44444444-4444-4444-8444-444444444444",
        master_instance_id="55555555-5555-4555-8555-555555555555",
        service_instance_id="66666666-6666-4666-8666-666666666666",
        epoch=1,
        master_run_ref="owner/master/1",
        lease_until=NOW + timedelta(minutes=15),
        authority_kind="acceptance",
        acceptance_scenario="FM15",
        source_revision="b" * 40,
        verifier_dataset_version_ref="owner/reviewed-verifier/4",
        verifier_notebook_ref="owner/fm15-verifier",
    )
    candidate = KaggleDatasetIdentity(
        provider_ref="owner/candidate",
        version=5,
        privacy="private",
        package_sha256="c" * 64,
        fingerprint=ProviderFingerprint(value="d" * 64),
        observed_at=NOW,
    )
    adapter = BrokerAdapter(manifest, authority, candidate)

    receipt = CentralBrokeredFM15Verifier(adapter, tmp_path / "outputs").verify_forced_failure(  # type: ignore[arg-type]
        exact_version_ref="owner/candidate/5",
        dataset_identity=candidate,
        manifest=manifest,  # type: ignore[arg-type]
        authority=authority,
    )

    source = adapter.source.decode()
    assert adapter.read_calls == [("owner/reviewed-verifier", 4)]
    assert adapter.dataset_sources == ("owner/candidate/5", "owner/reviewed-verifier/4")
    assert "/kaggle/input/reviewed-verifier" not in source
    assert "_mdh_dataset_entries" in source
    assert "FM15 candidate Dataset package identity differs" in source
    assert "FM15 verifier Dataset package identity differs" in source
    assert receipt["expected_failure"] is True
    assert receipt["failed_output_tree_sha256"] == "9" * 64
