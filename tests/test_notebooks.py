from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.hashing import sha256_file
from my_data_hub.notebooks.contracts import NotebookInputManifest, NotebookResult
from my_data_hub.notebooks.runtime import NotebookContractError, NotebookResultBuilder
from scripts import create_notebooks

ROOT = Path(__file__).resolve().parents[1]


def test_manifest_model_accepts_fixture(notebook_manifest_payload: dict[str, object]) -> None:
    manifest = NotebookInputManifest.model_validate(notebook_manifest_payload)
    assert len(manifest.work_items) == 2
    assert manifest.limits.max_items == 3


def test_manifest_rejects_duplicate_work_items(notebook_manifest_payload: dict[str, object]) -> None:
    duplicate = dict(notebook_manifest_payload)
    duplicate["work_items"] = [
        notebook_manifest_payload["work_items"][0],  # type: ignore[index]
        notebook_manifest_payload["work_items"][0],  # type: ignore[index]
    ]
    with pytest.raises(ValidationError, match="unique"):
        NotebookInputManifest.model_validate(duplicate)


def test_result_builder_accounts_missing_items(notebook_manifest_file: Path) -> None:
    builder = NotebookResultBuilder(
        notebook_manifest_file,
        code_revision="deadbeef",
        runtime_name="pytest",
    )
    first = builder.manifest.work_items[0]
    builder.add_success(
        work_item_id=first.work_item_id,
        input_fingerprint=first.input_fingerprint,
        result={"accepted": True},
    )
    result = NotebookResult.model_validate(builder.build({"name": "fixture"}))
    assert result.status == "partial"
    assert len(result.items) == 1
    assert len(result.failures) == 1
    assert result.failures[0].code == "UNACCOUNTED_WORK_ITEM"
    assert result.input_manifest_sha256 == sha256_file(notebook_manifest_file)


def test_result_builder_rejects_unknown_work_item(notebook_manifest_file: Path) -> None:
    builder = NotebookResultBuilder(
        notebook_manifest_file,
        code_revision="deadbeef",
        runtime_name="pytest",
    )
    builder.add_success(
        work_item_id=UUID("99999999-9999-4999-8999-999999999999"),
        input_fingerprint="f" * 64,
        result={},
    )
    with pytest.raises(NotebookContractError, match="unknown work_item_id"):
        builder.build({})


def test_notebook_result_rejects_duplicate_accounting(notebook_manifest_file: Path) -> None:
    builder = NotebookResultBuilder(
        notebook_manifest_file,
        code_revision="deadbeef",
        runtime_name="pytest",
    )
    first = builder.manifest.work_items[0]
    builder.add_success(
        work_item_id=first.work_item_id,
        input_fingerprint=first.input_fingerprint,
        result={},
    )
    builder.add_failure(
        work_item_id=first.work_item_id,
        code="DUPLICATE",
        message="same item twice",
        retryable=False,
    )
    with pytest.raises(ValidationError, match="only once"):
        NotebookResult.model_validate(builder.build({}))


def test_generated_notebooks_have_no_drift() -> None:
    process = subprocess.run(
        [sys.executable, str(ROOT / "scripts/create_notebooks.py"), "--check"],
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    assert process.returncode == 0, process.stdout + process.stderr
    report = json.loads(process.stdout)
    assert report["drift"] == []


def test_embedding_runtime_assets_are_generator_owned_exact_notebook_copies() -> None:
    expected = create_notebooks.expected_files()
    pairs = (
        (
            ROOT / "notebooks/05-e5-blogger-embedding-worker/worker.ipynb",
            ROOT / "src/my_data_hub/embeddings/assets/e5-worker.json",
        ),
        (
            ROOT / "notebooks/06-bge-m3-blogger-embedding-worker/worker.ipynb",
            ROOT / "src/my_data_hub/embeddings/assets/bge-worker.json",
        ),
    )
    for notebook_path, asset_path in pairs:
        assert asset_path in expected
        assert expected[asset_path] == expected[notebook_path]
        assert asset_path.read_bytes() == notebook_path.read_bytes()


def test_operational_notebook_metadata_declares_complete_fail_closed_pin_contract() -> None:
    for spec in create_notebooks.OPERATIONAL_SPECS:
        notebook = create_notebooks.build_operational_notebook(spec)
        metadata = notebook.metadata["my_data_hub"]
        pin_contract = metadata["execution_pin_contract"]
        kernel = json.loads(create_notebooks.operational_kernel_metadata(spec))

        assert metadata["privacy"] == "private"
        assert metadata["activation_prerequisites_satisfied"] is False
        assert kernel["is_private"] is True
        assert kernel["my_data_hub"]["production_ready"] is False
        assert kernel["my_data_hub"]["activation_prerequisites_satisfied"] is False
        assert pin_contract == kernel["my_data_hub"]["execution_pin_contract"]
        assert pin_contract["schema"] == "my-data-hub-notebook-execution-pins/v1"
        assert pin_contract["notebook"] == spec.directory
        assert pin_contract["supported_python_series"] == "3.12"
        assert pin_contract["immutable_assets"] == [
            "my_data_hub_wheel_sha256",
            "primary_source_sha256",
        ]
        assert pin_contract["input_dataset_versions"] == "required-exact-numeric-private-refs-at-launch"
        assert pin_contract["kaggle_runtime_image_identity"] == "required-immutable-sha256-at-launch"
        assert pin_contract["output_contract"] == spec.runtime_contract
        assert pin_contract["cleanup_retention_policy"]["cleanup_receipt_required"] is True


def test_operational_notebook_fails_before_install_without_hashed_execution_pins(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    notebook = create_notebooks.build_operational_notebook(create_notebooks.OPERATIONAL_SPECS[0])
    source = notebook.cells[1].source
    for name in (
        "MY_DATA_HUB_EXECUTION_PINS_PATH",
        "MY_DATA_HUB_EXECUTION_PINS_SHA256",
        "MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY",
        "MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON",
        "MY_DATA_HUB_NOTEBOOK_IS_PRIVATE",
        "MY_DATA_HUB_WHEEL_PATH",
        "MY_DATA_HUB_WHEEL_SHA256",
    ):
        monkeypatch.delenv(name, raising=False)
    invoked = False

    def forbidden_install(*_args, **_kwargs):  # type: ignore[no-untyped-def]
        nonlocal invoked
        invoked = True
        raise AssertionError("install must not run before pin validation")

    monkeypatch.setattr(subprocess, "run", forbidden_install)
    with pytest.raises(RuntimeError, match="hashed execution pins"):
        exec(compile(source, "<generated-install-cell>", "exec"), {})
    assert not invoked


def test_operational_notebook_accepts_only_exact_runtime_bound_pinset(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    spec = create_notebooks.OPERATIONAL_SPECS[0]
    notebook = create_notebooks.build_operational_notebook(spec)
    metadata = notebook.metadata["my_data_hub"]
    wheel = tmp_path / "my_data_hub-0.1.0-py3-none-any.whl"
    wheel.write_bytes(b"immutable-test-wheel")
    wheel_sha = hashlib.sha256(wheel.read_bytes()).hexdigest()
    image_identity = "registry.example/kaggle-python@sha256:" + "a" * 64
    dataset_versions = ["owner/private-runtime-assets/17"]
    pins = {
        "schema": "my-data-hub-notebook-execution-pins/v1",
        "notebook": spec.directory,
        "python_version": platform.python_version(),
        "kaggle_runtime_image_identity": image_identity,
        "input_dataset_versions": dataset_versions,
        "immutable_asset_sha256s": {
            "my_data_hub_wheel_sha256": wheel_sha,
            "primary_source_sha256": metadata["primary_source_sha256"],
        },
        "output_contract": spec.runtime_contract,
        "model": None,
        "privacy": "private",
        "resource_class": "orchestrator_protected",
        "cleanup_retention_policy": create_notebooks.OPERATIONAL_CLEANUP_RETENTION_POLICY,
    }
    pin_path = tmp_path / "execution-pins.json"
    pin_path.write_text(json.dumps(pins, sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("MY_DATA_HUB_EXECUTION_PINS_PATH", str(pin_path))
    monkeypatch.setenv("MY_DATA_HUB_EXECUTION_PINS_SHA256", hashlib.sha256(pin_path.read_bytes()).hexdigest())
    monkeypatch.setenv("MY_DATA_HUB_KAGGLE_RUNTIME_IMAGE_IDENTITY", image_identity)
    monkeypatch.setenv("MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON", json.dumps(dataset_versions))
    monkeypatch.setenv("MY_DATA_HUB_NOTEBOOK_IS_PRIVATE", "true")
    monkeypatch.setenv("MY_DATA_HUB_WHEEL_PATH", str(wheel))
    monkeypatch.setenv("MY_DATA_HUB_WHEEL_SHA256", wheel_sha)
    calls: list[list[str]] = []

    def record_install(command, **kwargs):  # type: ignore[no-untyped-def]
        calls.append(command)
        assert kwargs["check"] is True

    monkeypatch.setattr(subprocess, "run", record_install)
    exec(compile(notebook.cells[1].source, "<generated-install-cell>", "exec"), {})
    assert calls and calls[0][-1] == str(wheel)

    pins["input_dataset_versions"] = ["owner/private-runtime-assets/latest"]
    pin_path.write_text(json.dumps(pins, sort_keys=True), encoding="utf-8")
    monkeypatch.setenv("MY_DATA_HUB_EXECUTION_PINS_SHA256", hashlib.sha256(pin_path.read_bytes()).hexdigest())
    monkeypatch.setenv("MY_DATA_HUB_INPUT_DATASET_VERSIONS_JSON", json.dumps(pins["input_dataset_versions"]))
    with pytest.raises(RuntimeError, match="exact numeric input Dataset versions"):
        exec(compile(notebook.cells[1].source, "<generated-install-cell>", "exec"), {})
