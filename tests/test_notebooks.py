from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest
from pydantic import ValidationError

from my_data_hub.hashing import sha256_file
from my_data_hub.notebooks.contracts import NotebookInputManifest, NotebookResult
from my_data_hub.notebooks.runtime import NotebookContractError, NotebookResultBuilder

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
