from __future__ import annotations

import ast
import hashlib
import json
import os
import re
import shutil
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from my_data_hub.checkpoints.kaggle_runtime import (
    CHECKPOINT_RESTORE_RECEIPT_NAME,
    CheckpointRetryableError,
    CheckpointRuntimeError,
    ExactCheckpointReference,
    KaggleCheckpointCoordinator,
    KaggleCheckpointDatasetProvider,
    KaggleCheckpointReadback,
    KaggleCheckpointRestoreVerifier,
    KaggleCheckpointVerifierAssets,
    RemoteCheckpointHeadSnapshot,
    RemoteControlCheckpointRegistry,
    RuntimeCheckpointCoordinator,
    _expected_migration_history_sha256,
    _reject_notebook_kaggle_credentials,
    _render_verifier_source,
)
from my_data_hub.checkpoints.manifest import RestoreProbe, build_manifest, load_and_verify, write_manifest
from my_data_hub.checkpoints.publisher import PublishReceipt
from my_data_hub.checkpoints.registry import CheckpointHead
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.providers.kaggle import (
    AuthenticatedControlPlaneClient,
    ControlPlaneRuntimeIdentity,
    DatasetMutationResult,
    EffectOutcome,
    KaggleAmbiguousMutation,
    KaggleDatasetIdentity,
    KaggleKernelRunIdentity,
    KaggleProviderAdapter,
    KaggleProviderIdentity,
    KernelState,
    MetadataHttpResponse,
    MutationAction,
    PollPolicy,
    ProviderEffectReceipt,
    TaskResourceClaim,
)
from my_data_hub.providers.kaggle.source_attestation import executable_source_sha256
from my_data_hub.providers.models import ControlClass, ProviderFingerprint, ProviderKind
from my_data_hub.runtime_sdk import CHECKPOINT_VERIFIER_TIMEOUT_SECONDS

NOW = datetime(2026, 8, 11, tzinfo=UTC)
RUN_ID = UUID("11111111-1111-4111-8111-111111111111")
ATTEMPT_ID = UUID("22222222-2222-4222-8222-222222222222")
MASTER_ID = UUID("33333333-3333-4333-8333-333333333333")
CHECKPOINT_ID = UUID("44444444-4444-4444-8444-444444444444")
RUNTIME_IMAGE = "gcr.io/kaggle-images/python@sha256:" + "a" * 64


def _verifier_notebook() -> bytes:
    return (
        Path(__file__).resolve().parents[2]
        / "notebooks/03-checkpoint-verifier-restore-smoke/worker.ipynb"
    ).read_bytes()


def _verifier_assets(**updates: object) -> KaggleCheckpointVerifierAssets:
    values = {
        "notebook_ref": "owner/checkpoint-verifier",
        "notebook_source": _verifier_notebook(),
        "runtime_dataset_exact_ref": "owner/master-assets/3",
        "runtime_image_identity": RUNTIME_IMAGE,
        "runtime_image_source_commit": "c" * 40,
        "runtime_python_series": "3.12",
        "wheel_relative_path": "my_data_hub.whl",
        "wheel_sha256": "d" * 64,
        "postgres_runtime_archive_relative_path": "postgresql-18-runtime.bundle",
        "postgres_runtime_archive_sha256": "e" * 64,
        "postgres_runtime_manifest_relative_path": "postgresql-18-runtime.json",
        "postgres_runtime_manifest_sha256": "f" * 64,
    }
    values.update(updates)
    return KaggleCheckpointVerifierAssets(**values)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    "name",
    ["KAGGLE_KEY", "KAGGLE_API_TOKEN", "KAGGLE_API_V1_TOKEN", "KAGGLE_ACCESS_TOKEN"],
)
def test_master_checkpoint_runtime_rejects_every_kaggle_lifecycle_credential(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, name: str
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv(name, "must-not-enter-master")
    with pytest.raises(CheckpointRuntimeError, match=rf"forbidden in the master Notebook: {name}"):
        _reject_notebook_kaggle_credentials()


def test_master_checkpoint_runtime_accepts_kaggle_platform_username_without_secret(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("KAGGLE_USERNAME", "platform-account-metadata")
    for name in ("KAGGLE_KEY", "KAGGLE_API_TOKEN", "KAGGLE_API_V1_TOKEN", "KAGGLE_ACCESS_TOKEN"):
        monkeypatch.delenv(name, raising=False)

    _reject_notebook_kaggle_credentials()


@pytest.mark.parametrize("relative", [".kaggle/kaggle.json", ".kaggle/access_token"])
def test_master_checkpoint_runtime_rejects_kaggle_credential_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, relative: str
) -> None:
    monkeypatch.setenv("HOME", str(tmp_path))
    for name in (
        "KAGGLE_USERNAME",
        "KAGGLE_KEY",
        "KAGGLE_API_TOKEN",
        "KAGGLE_API_V1_TOKEN",
        "KAGGLE_ACCESS_TOKEN",
    ):
        monkeypatch.delenv(name, raising=False)
    target = tmp_path / relative
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text("credential")
    with pytest.raises(CheckpointRuntimeError, match="credential files are forbidden"):
        _reject_notebook_kaggle_credentials()


@pytest.fixture
def kaggle_working(tmp_path: Path, monkeypatch) -> Path:  # type: ignore[no-untyped-def]
    from my_data_hub.checkpoints import kaggle_runtime

    root = tmp_path / f"pytest-checkpoint-{uuid4()}"
    root.mkdir()
    monkeypatch.setattr(kaggle_runtime, "_KAGGLE_WORKING_ROOT", tmp_path)
    yield root


class OneResponseTransport:
    def __init__(self, payload: dict[str, object]) -> None:
        self.payload = payload
        self.calls: list[dict[str, object]] = []

    def request(self, **kwargs: object) -> MetadataHttpResponse:
        self.calls.append(kwargs)
        return MetadataHttpResponse(200, json.dumps(self.payload).encode())


def _client(transport: object) -> AuthenticatedControlPlaneClient:
    return AuthenticatedControlPlaneClient(
        base_url="https://control.example.test",
        bearer_token="runtime-token-that-is-long-enough",
        runtime_identity=ControlPlaneRuntimeIdentity(RUN_ID, ATTEMPT_ID, MASTER_ID, 9),
        transport=transport,  # type: ignore[arg-type]
    )


def test_remote_head_resolves_exact_numeric_current_and_previous_for_boot() -> None:
    transport = OneResponseTransport(
        {
            "generation": 2,
            "current": {
                "checkpoint_id": str(CHECKPOINT_ID),
                "dataset_ref": "owner/private-checkpoints",
                "exact_version_ref": "owner/private-checkpoints/7",
                "manifest_sha256": "a" * 64,
            },
            "previous": {
                "checkpoint_id": "55555555-5555-4555-8555-555555555555",
                "dataset_ref": "owner/private-checkpoints",
                "exact_version_ref": "owner/private-checkpoints/6",
                "manifest_sha256": "b" * 64,
            },
        }
    )
    registry = RemoteControlCheckpointRegistry(
        _client(transport),
        operation_id="checkpoint-operation",
        dataset_ref="owner/private-checkpoints",
    )
    exact = registry.resolve_head()
    assert exact.generation == 2
    assert exact.current is not None and exact.current.exact_version_ref.endswith("/7")
    assert exact.previous is not None and exact.previous.exact_version_ref.endswith("/6")
    assert registry.head.current == CHECKPOINT_ID
    assert str(transport.calls[0]["url"]).endswith("/internal/checkpoints/postgres-master/head")


def test_remote_registry_persists_metadata_only_package_identity() -> None:
    transport = OneResponseTransport({"accepted": True})
    registry = RemoteControlCheckpointRegistry(
        _client(transport),
        operation_id="checkpoint-operation",
        dataset_ref="owner/private-checkpoints",
    )

    registry.package_uploaded(CHECKPOINT_ID, "c" * 64)

    call = transport.calls[0]
    assert str(call["url"]).endswith(f"/internal/checkpoints/{CHECKPOINT_ID}/package-identity")
    assert json.loads(call["body"]) == {
        "service_kind": "postgres-master",
        "package_sha256": "c" * 64,
    }
    assert call["headers"]["X-MDH-Epoch"] == "9"


def _manifest(
    package: Path,
    *,
    checkpoint_id: UUID = CHECKPOINT_ID,
    parent_checkpoint_id: UUID | None = None,
):  # type: ignore[no-untyped-def]
    for relative, content in {
        "physical/base.tar.gz": b"base",
        "physical/backup_manifest": b"native",
        "physical/pg_wal.tar.gz": b"wal",
        "logical/hub.dump": b"logical",
        "receipts/verification.json": b'{"ok":true}',
    }.items():
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = build_manifest(
        package_directory=package,
        checkpoint_id=checkpoint_id,
        master_instance_id=MASTER_ID,
        epoch=9,
        parent_checkpoint_id=parent_checkpoint_id,
        postgres_version="18.0",
        pgvector_version="0.8.1",
        schema_version=13,
        canonical_revision=4,
        source_run_id=str(RUN_ID),
        source_identity="owner/postgres-master/3",
        created_at=NOW,
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "logical/hub.dump": "logical",
            "physical/backup_manifest": "postgres_backup_manifest",
            "physical/base.tar.gz": "physical",
            "physical/pg_wal.tar.gz": "physical",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(13, 4, "c" * 64, {"hub.canonical_state": 1}),
    )
    write_manifest(package / "checkpoint-manifest.json", manifest)
    return manifest


class FakeVerifierAdapter:
    def __init__(self, manifest: object, dataset: KaggleDatasetIdentity) -> None:
        self.manifest = manifest
        self.dataset = dataset
        self.run: KaggleKernelRunIdentity | None = None
        self.dataset_sources: tuple[str, ...] = ()
        self.launched: object | None = None
        self.push_count = 0
        self.download_count = 0

    def reconcile_private_notebook_mutation(self, **kwargs: object) -> object | None:
        if self.launched is not None:
            assert kwargs["task_run_id"] == self.run.task_run_id  # type: ignore[union-attr]
        return self.launched

    def push_private_notebook_pending_runtime_attestation(self, **kwargs: object) -> object:
        self.push_count += 1
        run_id = kwargs["task_run_id"]
        assert isinstance(run_id, UUID)
        source = kwargs["source"]
        assert isinstance(source, bytes) and str(run_id).encode() in source
        document = json.loads(source)
        bootstrap = str(document["cells"][0]["source"])
        match = re.search(r"_mdh_pins_body = (b'.*')\n", bootstrap)
        assert match is not None
        self.execution_pins_sha256 = hashlib.sha256(ast.literal_eval(match.group(1))).hexdigest()
        assert kwargs["intent"].task_id == ATTEMPT_ID  # type: ignore[union-attr]
        self.dataset_sources = tuple(kwargs["dataset_sources"])  # type: ignore[arg-type]
        assert kwargs["docker_image"] == RUNTIME_IMAGE
        assert kwargs["docker_image_pinning_type"] == "original"
        self.run = KaggleKernelRunIdentity(
            task_run_id=run_id,
            provider_ref="owner/checkpoint-verifier",
            source_version=4,
            source_sha256=executable_source_sha256(source, kernel_type="notebook"),
            provider_kernel_id=77,
            provider_run_ref="owner/checkpoint-verifier/4",
            started_at=NOW,
        )
        self.launched = SimpleNamespace(run=self.run)
        return self.launched

    def read_attested_master_run_status(self, run: KaggleKernelRunIdentity) -> object:
        assert run == self.run
        return SimpleNamespace(state=KernelState.COMPLETE)

    def download_attested_master_output_file(
        self, run: KaggleKernelRunIdentity, *, destination: Path, **_: object
    ) -> object:
        assert run == self.run
        self.download_count += 1
        destination.mkdir()
        manifest = self.manifest
        receipt = {
            "schema_version": "my-data-hub-checkpoint-restore-smoke.v2",
            "task_run_id": str(run.task_run_id),
            "checkpoint_id": str(manifest.checkpoint_id),
            "manifest_sha256": manifest.manifest_sha256,
            "manifest_file_sha256": hashlib.sha256(
                canonical_json_bytes(manifest.payload()) + b"\n"
            ).hexdigest(),
            "dataset_ref": self.dataset.provider_ref,
            "dataset_version": self.dataset.version,
            "package_sha256": self.dataset.package_sha256,
            "restore_mode": "isolated_physical_restore",
            "execution_pins_sha256": self.execution_pins_sha256,
            "runtime_image_identity": RUNTIME_IMAGE,
            "runtime_image_source_commit": "c" * 40,
            "input_dataset_versions": [
                "owner/master-assets/3",
                f"{self.dataset.provider_ref}/{self.dataset.version}",
            ],
            "ok": True,
            "observed": {
                "schema_version": manifest.restore_probe.schema_version,
                "canonical_revision": manifest.restore_probe.canonical_revision,
                "logical_hash_sha256": manifest.restore_probe.logical_hash_sha256,
                "row_counts": manifest.restore_probe.row_counts,
                "postgres_version": "18.4",
                "extensions": {
                    "citext": "1.6",
                    "pg_trgm": "1.6",
                    "pgcrypto": "1.3",
                    "vector": manifest.pgvector_version,
                },
                "migration_boundary": {
                    "first_version": 1,
                    "last_version": manifest.restore_probe.schema_version,
                    "applied_count": manifest.restore_probe.schema_version,
                    "contiguous": True,
                    "history_sha256": _expected_migration_history_sha256(
                        manifest.restore_probe.schema_version
                    ),
                },
                "database_invariants": {
                    "canonical_state_singletons": 1,
                    "epoch_state_singletons": 1,
                    "unvalidated_constraints": 0,
                },
                "vector_query": {
                    "operator": "cosine_distance",
                    "dimensions": 3,
                    "distance": 0.0,
                },
                "bounded_read_smoke": {
                    "relation_count": len(manifest.restore_probe.row_counts),
                    "total_rows": sum(manifest.restore_probe.row_counts.values()),
                    "statement_timeout_ms": 30000,
                    "lock_timeout_ms": 3000,
                },
            },
        }
        body = canonical_json_bytes(receipt)
        (destination / CHECKPOINT_RESTORE_RECEIPT_NAME).write_bytes(body)
        return SimpleNamespace(
            output_tree_sha256=hashlib.sha256(
                canonical_json_bytes(
                    {
                        "files": [
                            {
                                "path": CHECKPOINT_RESTORE_RECEIPT_NAME,
                                "byte_size": len(body),
                                "sha256": hashlib.sha256(body).hexdigest(),
                            }
                        ]
                    }
                )
            ).hexdigest()
        )


class _VerifierJournal:
    def __init__(self) -> None:
        self.intents: list[object] = []
        self.receipts: list[object] = []
        self.claims: list[object] = []

    def persist_intent(self, value: object) -> None:
        self.intents.append(value)

    def persist_receipt(self, value: object) -> None:
        self.receipts.append(value)

    def persist_resource_claim(self, value: object) -> None:
        self.claims.append(value)

    def assert_resource_claim(self, value: object) -> None:
        assert value in self.claims


class _VerifierKaggleApi:
    def __init__(
        self,
        manifest: object,
        dataset: KaggleDatasetIdentity,
        *,
        evidence_fault: str | None = None,
    ) -> None:
        self.manifest = manifest
        self.dataset = dataset
        self.evidence_fault = evidence_fault
        self.metadata: dict[str, object] | None = None
        self.source: bytes | None = None
        self.get_kernel_calls = 0

    def kernels_push(self, folder: str, **_: object) -> object:
        root = Path(folder)
        self.metadata = json.loads((root / "kernel-metadata.json").read_bytes())
        self.source = (root / str(self.metadata["code_file"])).read_bytes()
        return SimpleNamespace(
            ref="owner/checkpoint-verifier",
            kernelId=77,
            versionNumber=4,
            error="",
        )

    def get_kernel_latest_response(self, _ref: str) -> object:
        self.get_kernel_calls += 1
        raise AssertionError("pending-attested full SaveKernel response must not call GetKernel")

    def kernels_status(self, kernel: str) -> object:
        assert kernel == "owner/checkpoint-verifier"
        return SimpleNamespace(status="complete", failure_message=None)

    def kernels_output(
        self,
        kernel: str,
        path: str,
        file_pattern: str | None = None,
        **_: object,
    ) -> object:
        assert kernel == "owner/checkpoint-verifier/4"
        assert file_pattern == r"^checkpoint\-restore\-receipt\.json$"
        assert self.metadata is not None and self.source is not None
        document = json.loads(self.source)
        bootstrap = str(document["cells"][0]["source"])
        match = re.search(r"_mdh_pins_body = (b'.*')\n", bootstrap)
        assert match is not None
        pins_sha = hashlib.sha256(ast.literal_eval(match.group(1))).hexdigest()
        manifest = self.manifest
        receipt: dict[str, object] = {
            "schema_version": "my-data-hub-checkpoint-restore-smoke.v2",
            "task_run_id": str(RUN_ID),
            "checkpoint_id": str(manifest.checkpoint_id),  # type: ignore[attr-defined]
            "manifest_sha256": manifest.manifest_sha256,  # type: ignore[attr-defined]
            "manifest_file_sha256": hashlib.sha256(
                canonical_json_bytes(manifest.payload()) + b"\n"  # type: ignore[attr-defined]
            ).hexdigest(),
            "dataset_ref": self.dataset.provider_ref,
            "dataset_version": self.dataset.version,
            "package_sha256": self.dataset.package_sha256,
            "restore_mode": "isolated_physical_restore",
            "execution_pins_sha256": pins_sha,
            "runtime_image_identity": RUNTIME_IMAGE,
            "runtime_image_source_commit": "c" * 40,
            "input_dataset_versions": ["owner/master-assets/3", "owner/private-checkpoints/7"],
            "ok": True,
            "observed": {
                "schema_version": manifest.restore_probe.schema_version,  # type: ignore[attr-defined]
                "canonical_revision": manifest.restore_probe.canonical_revision,  # type: ignore[attr-defined]
                "logical_hash_sha256": manifest.restore_probe.logical_hash_sha256,  # type: ignore[attr-defined]
                "row_counts": manifest.restore_probe.row_counts,  # type: ignore[attr-defined]
                "postgres_version": "18.4",
                "extensions": {
                    "citext": "1.6",
                    "pg_trgm": "1.6",
                    "pgcrypto": "1.3",
                    "vector": manifest.pgvector_version,  # type: ignore[attr-defined]
                },
                "migration_boundary": {
                    "first_version": 1,
                    "last_version": manifest.restore_probe.schema_version,  # type: ignore[attr-defined]
                    "applied_count": manifest.restore_probe.schema_version,  # type: ignore[attr-defined]
                    "contiguous": True,
                    "history_sha256": _expected_migration_history_sha256(
                        manifest.restore_probe.schema_version  # type: ignore[attr-defined]
                    ),
                },
                "database_invariants": {
                    "canonical_state_singletons": 1,
                    "epoch_state_singletons": 1,
                    "unvalidated_constraints": 0,
                },
                "vector_query": {"operator": "cosine_distance", "dimensions": 3, "distance": 0.0},
                "bounded_read_smoke": {
                    "relation_count": len(manifest.restore_probe.row_counts),  # type: ignore[attr-defined]
                    "total_rows": sum(manifest.restore_probe.row_counts.values()),  # type: ignore[attr-defined]
                    "statement_timeout_ms": 30000,
                    "lock_timeout_ms": 3000,
                },
            },
        }
        observed = receipt["observed"]
        assert isinstance(observed, dict)
        if self.evidence_fault == "missing":
            observed.pop("vector_query")
        elif self.evidence_fault == "wrong":
            observed["postgres_version"] = "17.9"
        destination = Path(path)
        (destination / CHECKPOINT_RESTORE_RECEIPT_NAME).write_bytes(canonical_json_bytes(receipt))
        return [], ""


@pytest.mark.parametrize("evidence_fault", [None, "missing", "wrong"])
def test_real_adapter_checkpoint_verifier_contract_rejects_missing_or_wrong_live_evidence(
    kaggle_working: Path, evidence_fault: str | None
) -> None:
    package = kaggle_working / "real-adapter-candidate"
    package.mkdir()
    manifest = _manifest(package)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    api = _VerifierKaggleApi(manifest, dataset, evidence_fault=evidence_fault)
    journal = _VerifierJournal()
    adapter = KaggleProviderAdapter(
        api,  # type: ignore[arg-type]
        identity=KaggleProviderIdentity(username="owner"),
        journal=journal,  # type: ignore[arg-type]
        sleep=lambda _: None,
        monotonic=lambda: 0.0,
        clock=lambda: NOW,
    )
    output_root = kaggle_working / f"real-adapter-output-{evidence_fault or 'valid'}"
    output_root.mkdir()
    verifier = KaggleCheckpointRestoreVerifier(
        adapter,
        _verifier_assets(),
        output_directory=output_root,
        operation_id=UUID("66666666-6666-4666-8666-666666666666"),
        authorization_task_id=ATTEMPT_ID,
        clock=lambda: NOW,
        run_id_factory=lambda: RUN_ID,
    )
    if evidence_fault is None:
        result = verifier.verify_restore(
            exact_version_ref="owner/private-checkpoints/7",
            dataset_identity=dataset,
            manifest=manifest,
        )
        assert result["provider_run_ref"] == "owner/checkpoint-verifier/4"
        assert api.metadata is not None
        assert api.metadata["dataset_sources"] == [
            "owner/master-assets/3",
            "owner/private-checkpoints/7",
        ]
        assert api.metadata["docker_image"] == RUNTIME_IMAGE
        assert api.metadata["docker_image_pinning_type"] == "original"
        assert api.metadata["enable_internet"] is False
        assert api.get_kernel_calls == 0
    else:
        with pytest.raises(CheckpointRuntimeError, match=r"restore receipt|live restore evidence"):
            verifier.verify_restore(
                exact_version_ref="owner/private-checkpoints/7",
                dataset_identity=dataset,
                manifest=manifest,
            )


def test_verifier_launch_binds_exact_dataset_version_and_typed_restore_receipt(
    kaggle_working: Path,
) -> None:
    package = kaggle_working / "candidate"
    package.mkdir()
    manifest = _manifest(package)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    adapter = FakeVerifierAdapter(manifest, dataset)
    output_root = kaggle_working / "verifier-output"
    output_root.mkdir()
    verifier = KaggleCheckpointRestoreVerifier(
        adapter,  # type: ignore[arg-type]
        _verifier_assets(),
        output_directory=output_root,
        operation_id=UUID("66666666-6666-4666-8666-666666666666"),
        authorization_task_id=ATTEMPT_ID,
        clock=lambda: NOW,
        run_id_factory=lambda: RUN_ID,
    )
    receipt = verifier.verify_restore(
        exact_version_ref="owner/private-checkpoints/7",
        dataset_identity=dataset,
        manifest=manifest,
    )
    assert adapter.dataset_sources == ("owner/master-assets/3", "owner/private-checkpoints/7")
    assert receipt["provider_run_ref"] == "owner/checkpoint-verifier/4"
    assert receipt["checkpoint_id"] == str(CHECKPOINT_ID)


@pytest.mark.parametrize("duplicate_runtime_file", [False, True])
def test_rendered_verifier_discovers_normalized_mounts_and_rejects_ambiguity(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    duplicate_runtime_file: bool,
) -> None:
    package = tmp_path / "package-source"
    package.mkdir()
    manifest = _manifest(package)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    input_root = tmp_path / "provider-normalized-input"
    runtime_root = input_root / "master-assets-v3-provider-normalized" / "nested"
    checkpoint_root = input_root / "private-checkpoints-version-7-renamed"
    runtime_root.mkdir(parents=True)
    checkpoint_root.mkdir(parents=True)
    wheel = b"wheel"
    archive = b"archive"
    runtime_manifest = b"runtime-manifest"
    (runtime_root / "project.whl").write_bytes(wheel)
    (runtime_root / "postgres.bundle").write_bytes(archive)
    (runtime_root / "postgres.json").write_bytes(runtime_manifest)
    (checkpoint_root / "checkpoint-manifest.json").write_bytes(
        canonical_json_bytes(manifest.payload())
    )
    if duplicate_runtime_file:
        duplicate = input_root / "unrelated-provider-mount"
        duplicate.mkdir()
        (duplicate / "project.whl").write_bytes(wheel)
    assets = _verifier_assets(
        wheel_relative_path="dist/project.whl",
        wheel_sha256=hashlib.sha256(wheel).hexdigest(),
        postgres_runtime_archive_relative_path="runtime/postgres.bundle",
        postgres_runtime_archive_sha256=hashlib.sha256(archive).hexdigest(),
        postgres_runtime_manifest_relative_path="runtime/postgres.json",
        postgres_runtime_manifest_sha256=hashlib.sha256(runtime_manifest).hexdigest(),
    )
    source, _pins_sha = _render_verifier_source(
        assets,
        run_id=RUN_ID,
        dataset_identity=dataset,
        manifest=manifest,
        execution=assets.execution_contract(),
    )
    bootstrap = str(json.loads(source)["cells"][0]["source"])
    working = tmp_path / "working"
    working.mkdir()
    image_commit = tmp_path / "git_commit"
    image_commit.write_text("c" * 40)
    bootstrap = bootstrap.replace("/kaggle/input", str(input_root)).replace(
        "/kaggle/working/checkpoint-verifier-execution-pins.json",
        str(working / "execution-pins.json"),
    ).replace("/etc/git_commit", str(image_commit))
    if duplicate_runtime_file:
        with pytest.raises(RuntimeError, match="absent or ambiguous"):
            exec(compile(bootstrap, "<verifier-bootstrap>", "exec"), {})
    else:
        monkeypatch.delenv("MY_DATA_HUB_CHECKPOINT_DIRECTORY", raising=False)
        exec(compile(bootstrap, "<verifier-bootstrap>", "exec"), {})
        assert Path(os.environ["MY_DATA_HUB_CHECKPOINT_DIRECTORY"]) == checkpoint_root
        assert Path(os.environ["MY_DATA_HUB_WHEEL_PATH"]) == runtime_root / "project.whl"
        assert json.loads(os.environ["MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON"]) == [
            "owner/master-assets/3",
            "owner/private-checkpoints/7",
        ]


def test_verifier_rejects_runtime_or_polling_beyond_checkpoint_attempt_allocation(
    kaggle_working: Path,
) -> None:
    notebook = json.dumps({"cells": [], "metadata": {}, "nbformat": 4, "nbformat_minor": 5}).encode()
    with pytest.raises(ValueError, match="timeout exceeds"):
        KaggleCheckpointVerifierAssets(
            notebook_ref="owner/checkpoint-verifier",
            notebook_source=notebook,
            timeout_seconds=CHECKPOINT_VERIFIER_TIMEOUT_SECONDS + 1,
        )

    output_root = kaggle_working / "bounded-verifier-output"
    output_root.mkdir()
    with pytest.raises(ValueError, match="polling exceeds"):
        KaggleCheckpointRestoreVerifier(
            object(),  # type: ignore[arg-type]
            KaggleCheckpointVerifierAssets(
                notebook_ref="owner/checkpoint-verifier",
                notebook_source=notebook,
            ),
            output_directory=output_root,
            operation_id=UUID("66666666-6666-4666-8666-666666666666"),
            authorization_task_id=ATTEMPT_ID,
            poll_policy=PollPolicy(timeout_seconds=CHECKPOINT_VERIFIER_TIMEOUT_SECONDS + 1),
        )


def test_verifier_retry_reconciles_deterministic_run_without_second_push(
    kaggle_working: Path,
) -> None:
    package = kaggle_working / "candidate"
    package.mkdir()
    manifest = _manifest(package)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    adapter = FakeVerifierAdapter(manifest, dataset)
    output_root = kaggle_working / "verifier-output"
    output_root.mkdir()
    verifier = KaggleCheckpointRestoreVerifier(
        adapter,  # type: ignore[arg-type]
        _verifier_assets(),
        output_directory=output_root,
        operation_id=UUID("66666666-6666-4666-8666-666666666666"),
        authorization_task_id=ATTEMPT_ID,
        clock=lambda: datetime(2026, 8, 11, 1, tzinfo=UTC),
    )

    first = verifier.verify_restore(
        exact_version_ref="owner/private-checkpoints/7",
        dataset_identity=dataset,
        manifest=manifest,
    )
    second = verifier.verify_restore(
        exact_version_ref="owner/private-checkpoints/7",
        dataset_identity=dataset,
        manifest=manifest,
    )

    assert first == second
    assert adapter.push_count == 1
    assert adapter.download_count == 1


def test_boot_readback_uses_resolved_numeric_head_and_rechecks_manifest(
    kaggle_working: Path,
) -> None:
    source = kaggle_working / "remote-dataset"
    source.mkdir()
    manifest = _manifest(source)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )

    class DownloadAdapter:
        seen: tuple[str, int] | None = None

        def download_private_dataset_exact(
            self, *, provider_ref: str, version: int, destination: Path
        ) -> KaggleDatasetIdentity:
            self.seen = (provider_ref, version)
            shutil.copytree(source, destination)
            return dataset

    adapter = DownloadAdapter()
    provider = KaggleCheckpointDatasetProvider(
        adapter,  # type: ignore[arg-type]
        dataset_ref="owner/private-checkpoints",
        operation_id=uuid4(),
        resource_task_id=uuid4(),
    )
    destination = kaggle_working / "boot-readback"
    readback = provider.exact_head_readback(
        ExactCheckpointReference(
            checkpoint_id=CHECKPOINT_ID,
            dataset_ref="owner/private-checkpoints",
            exact_version_ref="owner/private-checkpoints/7",
            manifest_sha256=manifest.manifest_sha256,
        ),
        destination,
    )
    assert adapter.seen == ("owner/private-checkpoints", 7)
    assert readback.package == destination


def test_checkpoint_provider_retry_reconciles_same_effect_without_duplicate_create(
    kaggle_working: Path,
) -> None:
    package = kaggle_working / "candidate"
    package.mkdir()
    manifest = _manifest(package)

    class AmbiguousCreateAdapter:
        def __init__(self) -> None:
            self.current_version: int | None = None
            self.create_calls = 0
            self.reconcile_calls = 0
            self.intents: list[object] = []

        def current_private_dataset_version(self, *, provider_ref: str) -> int | None:
            assert provider_ref == "owner/private-checkpoints"
            return self.current_version

        def create_private_dataset_from_directory(self, **kwargs: object) -> object:
            self.create_calls += 1
            self.current_version = 1
            self.intents.append(kwargs["intent"])
            raise RuntimeError("provider succeeded but journal response was lost")

        def reconcile_private_dataset_directory_mutation(self, **kwargs: object) -> DatasetMutationResult:
            self.reconcile_calls += 1
            intent = kwargs["intent"]
            self.intents.append(intent)
            if self.reconcile_calls == 1:
                raise RuntimeError("control plane is still unavailable")
            identity = KaggleDatasetIdentity(
                provider_ref="owner/private-checkpoints",
                version=1,
                privacy="private",
                package_sha256="f" * 64,
                fingerprint=ProviderFingerprint(value="a" * 64),
                observed_at=NOW,
            )
            receipt = ProviderEffectReceipt(
                operation_id=intent.operation_id,
                effect_id=intent.effect_id,
                action=MutationAction.CREATE_DATASET,
                provider_ref=identity.provider_ref,
                outcome=EffectOutcome.ALREADY_APPLIED,
                attempts=0,
                observed_fingerprint=identity.fingerprint,
                provider_version=1,
                observed_at=NOW,
                detail_code="reconciled",
            )
            claim = TaskResourceClaim.create(
                task_id=intent.task_id,
                effect_id=intent.effect_id,
                provider_ref=identity.provider_ref,
                kind=ProviderKind.DATASET,
                control_class=ControlClass.ORCHESTRATOR_PROTECTED,
                disposable=False,
                fingerprint=identity.fingerprint,
                provider_version=1,
                registered_at=NOW,
            )
            return DatasetMutationResult(identity=identity, claim=claim, effect=receipt)

    adapter = AmbiguousCreateAdapter()
    provider = KaggleCheckpointDatasetProvider(
        adapter,  # type: ignore[arg-type]
        dataset_ref="owner/private-checkpoints",
        operation_id=UUID("77777777-7777-4777-8777-777777777777"),
        resource_task_id=RUN_ID,
    )

    with pytest.raises(CheckpointRuntimeError, match="remains ambiguous"):
        provider.upload_candidate(package, manifest)
    exact_ref = provider.upload_candidate(package, manifest)

    assert exact_ref == "owner/private-checkpoints/1"
    assert adapter.create_calls == 1
    assert adapter.reconcile_calls == 2
    assert adapter.intents[0] == adapter.intents[1] == adapter.intents[2]


def test_checkpoint_provider_never_versions_from_stale_claim(kaggle_working: Path) -> None:
    package = kaggle_working / "candidate"
    package.mkdir()
    manifest = _manifest(package)
    previous_identity = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=1,
        privacy="private",
        package_sha256="e" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    stale_claim = TaskResourceClaim.create(
        task_id=RUN_ID,
        effect_id=UUID("88888888-8888-4888-8888-888888888888"),
        provider_ref=previous_identity.provider_ref,
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=previous_identity.fingerprint,
        provider_version=1,
        registered_at=NOW,
    )

    class AdvancedAdapter:
        version_calls = 0

        def current_private_dataset_version(self, *, provider_ref: str) -> int:
            return 2

        def read_private_dataset(self, *, provider_ref: str, version: int) -> KaggleDatasetIdentity:
            assert version == 1
            return previous_identity

        def reconcile_private_dataset_directory_mutation(self, **kwargs: object) -> object:
            raise KaggleAmbiguousMutation("current bytes belong to another candidate")

        def create_private_dataset_version_from_directory(self, **kwargs: object) -> object:
            self.version_calls += 1
            raise AssertionError("stale claim must never authorize a new version")

    adapter = AdvancedAdapter()
    provider = KaggleCheckpointDatasetProvider(
        adapter,  # type: ignore[arg-type]
        dataset_ref="owner/private-checkpoints",
        operation_id=UUID("77777777-7777-4777-8777-777777777777"),
        resource_task_id=RUN_ID,
        claim=stale_claim,
    )

    with pytest.raises(CheckpointRuntimeError, match="advanced beyond"):
        provider.upload_candidate(package, manifest)
    assert adapter.version_calls == 0


def test_checkpoint_runtime_rejects_broad_non_metadata_output(tmp_path: Path) -> None:
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=1,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )
    adapter = FakeVerifierAdapter(SimpleNamespace(), dataset)
    with pytest.raises(ValueError, match="metadata-only"):
        KaggleCheckpointRestoreVerifier(
            adapter,  # type: ignore[arg-type]
            KaggleCheckpointVerifierAssets(
                notebook_ref="owner/checkpoint-verifier",
                notebook_source=b"{}",
            ),
            output_directory=tmp_path,
            operation_id=uuid4(),
            authorization_task_id=ATTEMPT_ID,
            metadata_only_output=False,
        )


def test_runtime_composite_matches_master_create_and_publish_protocol(
    kaggle_working: Path,
) -> None:
    captured: dict[str, object] = {}

    class Cursor:
        query = ""

        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> Cursor:
            self.query = query
            return self

        def fetchone(self) -> tuple[str] | None:
            if self.query == "SHOW server_version":
                return ("18.1",)
            if "extversion" in self.query:
                return ("0.8.1",)
            if "pg_current_wal_lsn" in self.query:
                return ("0/16B6C50",)
            return None

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> Cursor:
            return Cursor()

    class Builder:
        def build(self, **kwargs: object):  # type: ignore[no-untyped-def]
            captured.update(kwargs)
            package = kwargs["package_directory"]
            assert isinstance(package, Path)
            package.mkdir(parents=True)
            manifest = _manifest(package)
            return package, package / "checkpoint-manifest.json", manifest

    expected = PublishReceipt(
        checkpoint_id=str(CHECKPOINT_ID),
        exact_version_ref="owner/private-checkpoints/7",
        manifest_sha256="a" * 64,
        current_checkpoint_id=str(CHECKPOINT_ID),
        previous_checkpoint_id=None,
        upload_seconds=1,
        readback_seconds=2,
        restore_seconds=3,
        package_bytes=4,
        restore_receipt={"ok": True},
    )

    class Publisher:
        registry = SimpleNamespace(head=CheckpointHead())
        provider = SimpleNamespace(claim=None, dataset_ref="owner/private-checkpoints")

        def publish(self, **kwargs: object) -> PublishReceipt:
            assert str(kwargs["package"]).startswith(str(kaggle_working))
            assert str(kwargs["readback_directory"]).startswith(str(kaggle_working))
            return expected

    coordinator = RuntimeCheckpointCoordinator(
        builder=Builder(),  # type: ignore[arg-type]
        coordinator=Publisher(),  # type: ignore[arg-type]
        probe_relations=("hub.canonical_state",),
        source_identity="owner/postgres-master/3",
        connect=lambda *_args, **_kwargs: Connection(),
        clock=lambda: NOW,
        checkpoint_id_factory=lambda: CHECKPOINT_ID,
    )
    receipt = coordinator.create_and_publish(
        database_url="postgresql:///postgres",
        package_directory=kaggle_working / "checkpoints",
        identity=SimpleNamespace(master_instance_id=MASTER_ID, run_id=str(RUN_ID), epoch=9),
    )
    assert receipt == expected
    build_identity = captured["identity"]
    assert build_identity.postgres_version == "18.1"  # type: ignore[union-attr]
    assert build_identity.pgvector_version == "0.8.1"  # type: ignore[union-attr]
    assert build_identity.checkpoint_lsn == "0/16B6C50"  # type: ignore[union-attr]


def test_runtime_retry_rebuilds_partial_package_before_provider_effect(
    kaggle_working: Path,
) -> None:
    class Cursor:
        query = ""

        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> Cursor:
            self.query = query
            return self

        def fetchone(self) -> tuple[str] | None:
            if self.query == "SHOW server_version":
                return ("18.1",)
            if "extversion" in self.query:
                return ("0.8.1",)
            if "pg_current_wal_lsn" in self.query:
                return ("0/16B6C50",)
            return None

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> Cursor:
            return Cursor()

    class Builder:
        def __init__(self) -> None:
            self.calls = 0
            self.packages: list[Path] = []

        def build(self, **kwargs: object):  # type: ignore[no-untyped-def]
            self.calls += 1
            package = kwargs["package_directory"]
            assert isinstance(package, Path)
            self.packages.append(package)
            package.mkdir(parents=True)
            if self.calls == 1:
                (package / "partial-archive.tar.gz").write_bytes(b"partial")
                raise RuntimeError("simulated archive creator crash")
            assert not (package / "partial-archive.tar.gz").exists()
            manifest = _manifest(package)
            return package, package / "checkpoint-manifest.json", manifest

    expected = PublishReceipt(
        checkpoint_id=str(CHECKPOINT_ID),
        exact_version_ref="owner/private-checkpoints/1",
        manifest_sha256="a" * 64,
        current_checkpoint_id=str(CHECKPOINT_ID),
        previous_checkpoint_id=None,
        upload_seconds=1,
        readback_seconds=2,
        restore_seconds=3,
        package_bytes=4,
        restore_receipt={"ok": True},
    )

    class Publisher:
        registry = SimpleNamespace(head=CheckpointHead())
        provider = SimpleNamespace(claim=None, dataset_ref="owner/private-checkpoints")
        calls = 0

        def publish(self, **kwargs: object) -> PublishReceipt:
            self.calls += 1
            package = kwargs["package"]
            assert isinstance(package, Path)
            assert (package.parent / f".provider-started-{CHECKPOINT_ID}").is_file()
            return expected

    builder = Builder()
    publisher = Publisher()
    coordinator = RuntimeCheckpointCoordinator(
        builder=builder,  # type: ignore[arg-type]
        coordinator=publisher,  # type: ignore[arg-type]
        probe_relations=("hub.canonical_state",),
        source_identity="owner/postgres-master/3",
        connect=lambda *_args, **_kwargs: Connection(),
        clock=lambda: NOW,
        checkpoint_id_factory=lambda: CHECKPOINT_ID,
    )
    call = {
        "database_url": "postgresql:///postgres",
        "package_directory": kaggle_working / "checkpoints",
        "identity": SimpleNamespace(master_instance_id=MASTER_ID, run_id=str(RUN_ID), epoch=9),
    }

    with pytest.raises(RuntimeError, match="archive creator crash"):
        coordinator.create_and_publish(**call)
    partial_package = kaggle_working / "checkpoints" / str(CHECKPOINT_ID)
    assert (partial_package / "partial-archive.tar.gz").is_file()
    assert not (partial_package.parent / f".provider-started-{CHECKPOINT_ID}").exists()

    receipt = coordinator.create_and_publish(**call)

    assert receipt == expected
    assert builder.calls == 2
    assert builder.packages[0] == builder.packages[1] == partial_package
    assert publisher.calls == 1


def test_runtime_retry_reuses_package_and_refreshes_exact_durable_claim(
    kaggle_working: Path,
) -> None:
    previous_checkpoint_id = UUID("55555555-5555-4555-8555-555555555555")
    stale_claim = TaskResourceClaim.create(
        task_id=UUID("99999999-9999-4999-8999-999999999999"),
        effect_id=uuid4(),
        provider_ref="owner/private-checkpoints",
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="a" * 64),
        provider_version=3,
        registered_at=NOW,
    )
    exact_claim = TaskResourceClaim.create(
        task_id=RUN_ID,
        effect_id=uuid4(),
        provider_ref="owner/private-checkpoints",
        kind=ProviderKind.DATASET,
        control_class=ControlClass.ORCHESTRATOR_PROTECTED,
        disposable=False,
        fingerprint=ProviderFingerprint(value="b" * 64),
        provider_version=4,
        registered_at=NOW,
    )

    class Cursor:
        query = ""

        def __enter__(self) -> Cursor:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def execute(self, query: str) -> Cursor:
            self.query = query
            return self

        def fetchone(self) -> tuple[str] | None:
            if self.query == "SHOW server_version":
                return ("18.1",)
            if "extversion" in self.query:
                return ("0.8.1",)
            if "pg_current_wal_lsn" in self.query:
                return ("0/16B6C50",)
            return None

    class Connection:
        def __enter__(self) -> Connection:
            return self

        def __exit__(self, *args: object) -> None:
            return None

        def cursor(self) -> Cursor:
            return Cursor()

    class ClaimSource:
        calls = 0

        def current_resource_claim(self, **kwargs: object) -> TaskResourceClaim:
            self.calls += 1
            assert kwargs["provider_ref"] == exact_claim.provider_ref
            return exact_claim

    class Builder:
        calls = 0

        def build(self, **kwargs: object):  # type: ignore[no-untyped-def]
            self.calls += 1
            package = kwargs["package_directory"]
            identity = kwargs["identity"]
            assert isinstance(package, Path)
            package.mkdir(parents=True)
            manifest = _manifest(
                package,
                checkpoint_id=identity.checkpoint_id,
                parent_checkpoint_id=identity.parent_checkpoint_id,
            )
            return package, package / "checkpoint-manifest.json", manifest

    expected = PublishReceipt(
        checkpoint_id=str(CHECKPOINT_ID),
        exact_version_ref="owner/private-checkpoints/5",
        manifest_sha256="a" * 64,
        current_checkpoint_id=str(CHECKPOINT_ID),
        previous_checkpoint_id=str(previous_checkpoint_id),
        upload_seconds=1,
        readback_seconds=2,
        restore_seconds=3,
        package_bytes=4,
        restore_receipt={"ok": True},
    )

    class Publisher:
        registry = SimpleNamespace(head=CheckpointHead(generation=4, current=previous_checkpoint_id, previous=None))
        provider = SimpleNamespace(
            claim=stale_claim,
            dataset_ref="owner/private-checkpoints",
            operation_id=UUID("77777777-7777-4777-8777-777777777777"),
            resource_task_id=stale_claim.task_id,
        )

        def __init__(self) -> None:
            self.calls = 0
            self.packages: list[Path] = []

        def publish(self, **kwargs: object) -> PublishReceipt:
            self.calls += 1
            self.packages.append(kwargs["package"])  # type: ignore[arg-type]
            assert self.provider.claim == exact_claim
            assert self.provider.resource_task_id == exact_claim.task_id
            if self.calls == 1:
                raise CheckpointRetryableError("lost transition response")
            return expected

    builder = Builder()
    publisher = Publisher()
    claim_source = ClaimSource()
    coordinator = RuntimeCheckpointCoordinator(
        builder=builder,  # type: ignore[arg-type]
        coordinator=publisher,  # type: ignore[arg-type]
        probe_relations=("hub.canonical_state",),
        source_identity="owner/postgres-master/3",
        claim_source=claim_source,
        connect=lambda *_args, **_kwargs: Connection(),
        clock=lambda: NOW,
        checkpoint_id_factory=lambda: CHECKPOINT_ID,
    )
    call = {
        "database_url": "postgresql:///postgres",
        "package_directory": kaggle_working / "checkpoints",
        "identity": SimpleNamespace(master_instance_id=MASTER_ID, run_id=str(RUN_ID), epoch=9),
    }

    with pytest.raises(CheckpointRetryableError):
        coordinator.create_and_publish(**call)
    receipt = coordinator.create_and_publish(**call)

    assert receipt == expected
    assert builder.calls == 1
    assert publisher.packages[0] == publisher.packages[1]
    assert claim_source.calls == 2


def test_checkpoint_coordinator_recovers_lost_promotion_response(
    kaggle_working: Path,
) -> None:
    package = kaggle_working / "candidate"
    package.mkdir()
    _manifest(package)
    dataset = KaggleDatasetIdentity(
        provider_ref="owner/private-checkpoints",
        version=7,
        privacy="private",
        package_sha256="f" * 64,
        fingerprint=ProviderFingerprint(value="a" * 64),
        observed_at=NOW,
    )

    class Registry:
        durable_head = CheckpointHead()
        rejected = False

        @property
        def head(self) -> CheckpointHead:
            return self.durable_head

        def add_candidate(self, _manifest: object) -> None:
            return None

        def uploaded(self, _checkpoint_id: UUID, _exact_ref: str) -> None:
            return None

        def readback_verified(self, _checkpoint_id: UUID) -> None:
            return None

        def restore_verified(self, _checkpoint_id: UUID) -> None:
            return None

        def promote(self, checkpoint_id: UUID, *, expected_generation: int) -> CheckpointHead:
            assert expected_generation == 0
            self.durable_head = CheckpointHead(generation=1, current=checkpoint_id, previous=None)
            raise RuntimeError("promotion committed but response was lost")

        def reject(self, _checkpoint_id: UUID, _reason: str) -> None:
            self.rejected = True

        def resolve_head(self) -> RemoteCheckpointHeadSnapshot:
            return RemoteCheckpointHeadSnapshot(
                generation=self.durable_head.generation,
                current=(
                    ExactCheckpointReference(
                        checkpoint_id=CHECKPOINT_ID,
                        dataset_ref="owner/private-checkpoints",
                        exact_version_ref="owner/private-checkpoints/7",
                        manifest_sha256=_manifest_sha,
                    )
                    if self.durable_head.current is not None
                    else None
                ),
                previous=None,
            )

    class Provider:
        dataset_ref = "owner/private-checkpoints"

        def upload_candidate(self, _package: Path, _manifest: object) -> str:
            return "owner/private-checkpoints/7"

        def exact_readback(self, _exact_ref: str, destination: Path) -> KaggleCheckpointReadback:
            shutil.copytree(package, destination)
            return KaggleCheckpointReadback(package=destination, identity=dataset)

    class Verifier:
        def verify_restore(self, **_kwargs: object) -> dict[str, object]:
            return {"ok": True}

    _manifest_sha = load_and_verify(
        package / "checkpoint-manifest.json",
        package,
    ).manifest_sha256
    registry = Registry()
    publisher = KaggleCheckpointCoordinator(
        registry=registry,  # type: ignore[arg-type]
        provider=Provider(),  # type: ignore[arg-type]
        restore_verifier=Verifier(),  # type: ignore[arg-type]
    )
    receipt = publisher.publish(
        package=package,
        manifest_path=package / "checkpoint-manifest.json",
        readback_directory=kaggle_working / "readback",
    )

    assert receipt.current_checkpoint_id == str(CHECKPOINT_ID)
    assert receipt.exact_version_ref == "owner/private-checkpoints/7"
    assert registry.rejected is False
    reconciled = publisher.reconcile_promoted(
        package=package,
        manifest_path=package / "checkpoint-manifest.json",
    )
    assert reconciled is not None
    assert reconciled.exact_version_ref == "owner/private-checkpoints/7"
    assert reconciled.restore_receipt["reconciled_from_durable_verified_head"] is True
