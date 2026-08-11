from __future__ import annotations

import json
import shutil
from datetime import UTC, datetime
from pathlib import Path
from uuid import UUID

import jsonschema
import pytest

from my_data_hub.checkpoints.manifest import (
    ManifestError,
    RestoreProbe,
    build_manifest,
    load_and_verify,
    write_manifest,
)
from my_data_hub.checkpoints.publisher import CheckpointPublisher, PublishError, assert_restore_equality
from my_data_hub.checkpoints.registry import CheckpointRegistry, CheckpointStatus

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 8, 10, tzinfo=UTC)
INSTANCE = UUID("22222222-2222-4222-8222-222222222222")


def _package(path: Path, checkpoint_id: UUID, parent: UUID | None = None) -> tuple[Path, Path]:
    package = path / str(checkpoint_id)
    for relative, content in {
        "physical/base.tar.gz": b"physical",
        "logical/hub.dump": b"logical",
        "receipts/verification.json": b'{"ok":true}',
        "receipts/restore-smoke.json": b'{"ok":true}',
    }.items():
        target = package / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
    manifest = build_manifest(
        package_directory=package,
        checkpoint_id=checkpoint_id,
        master_instance_id=INSTANCE,
        epoch=1,
        parent_checkpoint_id=parent,
        postgres_version="18.0",
        pgvector_version="0.8.1",
        schema_version=11,
        canonical_revision=7,
        source_run_id="run-exact-1",
        source_identity="private-dataset/version/1",
        created_at=NOW,
        checkpoint_lsn="0/16B6C50",
        file_kinds={
            "logical/hub.dump": "logical",
            "physical/base.tar.gz": "physical",
            "receipts/restore-smoke.json": "restore_smoke_receipt",
            "receipts/verification.json": "verification_receipt",
        },
        restore_probe=RestoreProbe(11, 7, "e" * 64, {"hub.canonical_state": 1}),
    )
    manifest_path = package / "checkpoint-manifest.json"
    write_manifest(manifest_path, manifest)
    return package, manifest_path


def test_manifest_schema_example_and_self_hash() -> None:
    schema = json.loads((ROOT / "schemas/checkpoint-manifest.v1.schema.json").read_text())
    example = json.loads((ROOT / "examples/contracts/checkpoint-manifest.v1.example.json").read_text())
    jsonschema.Draft202012Validator(schema, format_checker=jsonschema.FormatChecker()).validate(example)
    from my_data_hub.checkpoints.manifest import CheckpointManifest

    CheckpointManifest.from_payload(example)


def test_exact_package_verification_rejects_corruption_and_symlinks(tmp_path: Path) -> None:
    package, manifest_path = _package(tmp_path, UUID("11111111-1111-4111-8111-111111111111"))
    manifest = load_and_verify(manifest_path, package)
    assert manifest.canonical_revision == 7
    (package / "logical/hub.dump").write_bytes(b"corrupt")
    with pytest.raises(ManifestError, match="hash/size mismatch"):
        load_and_verify(manifest_path, package)

    (package / "logical/hub.dump").unlink()
    (package / "logical/hub.dump").symlink_to(package / "physical/base.tar.gz")
    with pytest.raises(ManifestError, match="absent or unsafe"):
        load_and_verify(manifest_path, package)


class _Provider:
    def __init__(self, remote: Path, *, corrupt: bool = False) -> None:
        self.remote = remote
        self.corrupt = corrupt

    def upload_candidate(self, package: Path, manifest: object) -> str:
        shutil.copytree(package, self.remote)
        return "private-dataset/exact-version-1"

    def exact_readback(self, exact_version_ref: str, destination: Path) -> Path:
        assert exact_version_ref == "private-dataset/exact-version-1"
        result = destination / "readback"
        shutil.copytree(self.remote, result)
        if self.corrupt:
            (result / "logical/hub.dump").write_bytes(b"provider-corruption")
        return result


class _Restore:
    def __init__(self, *, ok: bool = True) -> None:
        self.ok = ok

    def verify_restore(self, package: Path, manifest: object) -> dict[str, object]:
        return {"ok": self.ok, "verifier_run_id": "independent-run-1"}


def test_publish_promotes_current_then_previous_only_after_restore(tmp_path: Path) -> None:
    registry = CheckpointRegistry()
    first_id = UUID("11111111-1111-4111-8111-111111111111")
    first, first_manifest = _package(tmp_path / "first-local", first_id)
    publisher = CheckpointPublisher(
        registry=registry,
        provider=_Provider(tmp_path / "first-remote"),
        restore_verifier=_Restore(),
    )
    receipt = publisher.publish(
        package=first,
        manifest_path=first_manifest,
        readback_directory=tmp_path / "first-readback",
    )
    assert receipt.current_checkpoint_id == str(first_id)
    assert receipt.previous_checkpoint_id is None
    assert registry.record(first_id).status is CheckpointStatus.VERIFIED

    second_id = UUID("33333333-3333-4333-8333-333333333333")
    second, second_manifest = _package(tmp_path / "second-local", second_id, parent=first_id)
    publisher = CheckpointPublisher(
        registry=registry,
        provider=_Provider(tmp_path / "second-remote"),
        restore_verifier=_Restore(),
    )
    receipt = publisher.publish(
        package=second,
        manifest_path=second_manifest,
        readback_directory=tmp_path / "second-readback",
    )
    assert receipt.current_checkpoint_id == str(second_id)
    assert receipt.previous_checkpoint_id == str(first_id)


@pytest.mark.parametrize("failure", ["corrupt", "restore"])
def test_failed_candidate_never_advances_head(tmp_path: Path, failure: str) -> None:
    registry = CheckpointRegistry()
    checkpoint_id = UUID("11111111-1111-4111-8111-111111111111")
    package, manifest_path = _package(tmp_path / "local", checkpoint_id)
    publisher = CheckpointPublisher(
        registry=registry,
        provider=_Provider(tmp_path / "remote", corrupt=failure == "corrupt"),
        restore_verifier=_Restore(ok=failure != "restore"),
    )
    with pytest.raises(PublishError):
        publisher.publish(
            package=package,
            manifest_path=manifest_path,
            readback_directory=tmp_path / "readback",
        )
    assert registry.head.current is None and registry.head.previous is None
    assert registry.record(checkpoint_id).status is CheckpointStatus.REJECTED


def test_restore_equality_is_exact(tmp_path: Path) -> None:
    package, path = _package(tmp_path, UUID("11111111-1111-4111-8111-111111111111"))
    manifest = load_and_verify(path, package)
    observed = {
        "schema_version": 11,
        "canonical_revision": 7,
        "logical_hash_sha256": "e" * 64,
        "row_counts": {"hub.canonical_state": 1},
    }
    assert_restore_equality(manifest, observed)
    observed["row_counts"] = {"hub.canonical_state": 2}
    with pytest.raises(PublishError, match="row counts"):
        assert_restore_equality(manifest, observed)
