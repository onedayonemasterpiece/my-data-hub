from __future__ import annotations

import json
from copy import deepcopy
from uuid import uuid4

import pytest
from pydantic import ValidationError

from my_data_hub.notebooks.contracts import NotebookInputManifest, NotebookResult
from my_data_hub.notebooks.runtime import NotebookContractError, NotebookResultBuilder


def test_input_manifest_rejects_duplicate_items(notebook_manifest_payload) -> None:  # type: ignore[no-untyped-def]
    payload = deepcopy(notebook_manifest_payload)
    payload["work_items"].append(deepcopy(payload["work_items"][0]))
    with pytest.raises(ValidationError, match="work_item_id values must be unique"):
        NotebookInputManifest.model_validate(payload)


def test_result_rejects_duplicate_accounting(notebook_result_payload) -> None:  # type: ignore[no-untyped-def]
    payload = deepcopy(notebook_result_payload)
    work_item_id = str(uuid4())
    item = {
        "work_item_id": work_item_id,
        "input_fingerprint": "a" * 64,
        "output_fingerprint": "b" * 64,
        "status": "succeeded",
        "result": {},
        "evidence": {},
    }
    payload["status"] = "partial"
    payload["items"] = [item]
    payload["failures"] = [
        {
            "work_item_id": work_item_id,
            "code": "X",
            "message": "duplicate",
            "retryable": False,
            "details": {},
        }
    ]
    with pytest.raises(ValidationError, match="only once"):
        NotebookResult.model_validate(payload)


def test_builder_accounts_missing_items_as_failure(
    tmp_path, notebook_manifest_payload
) -> None:  # type: ignore[no-untyped-def]
    payload = deepcopy(notebook_manifest_payload)
    payload["work_items"] = [deepcopy(payload["work_items"][0])]
    payload["limits"]["max_items"] = 2
    second = deepcopy(payload["work_items"][0])
    second["work_item_id"] = str(uuid4())
    second["subject_id"] = str(uuid4())
    payload["work_items"].append(second)
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(payload), encoding="utf-8")
    builder = NotebookResultBuilder(path, "abc123", "pytest")
    first = builder.manifest.work_items[0]
    builder.add_success(
        work_item_id=first.work_item_id,
        input_fingerprint=first.input_fingerprint,
        result={"ok": True},
    )
    result = builder.build({"provider": "none", "name": "test", "version": "1", "task": "test"})
    assert result["status"] == "partial"
    assert result["metrics"] == {
        "input_items": 2,
        "successful_items": 1,
        "failed_items": 1,
        "accounted_items": 2,
    }
    assert result["failures"][0]["code"] == "UNACCOUNTED_WORK_ITEM"


def test_builder_rejects_unknown_work_item(tmp_path, notebook_manifest_payload) -> None:  # type: ignore[no-untyped-def]
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(notebook_manifest_payload), encoding="utf-8")
    builder = NotebookResultBuilder(path, "abc123", "pytest")
    builder.add_success(
        work_item_id=uuid4(),
        input_fingerprint="a" * 64,
        result={"ok": True},
    )
    with pytest.raises(NotebookContractError, match="unknown work_item_id"):
        builder.build({"provider": "none"})
