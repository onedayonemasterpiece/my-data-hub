from __future__ import annotations

import json
from pathlib import Path
from uuid import UUID

from scripts.provider.real_kaggle_matrix import EXTERNAL_BLOCKED, _notebook_source, run_notebook_canary


def test_real_notebook_source_emits_exact_dynamic_identity_receipt(tmp_path: Path) -> None:
    run_id = UUID("11111111-1111-4111-8111-111111111111")
    source = _notebook_source(task_run_id=run_id, provider_ref="owner/mdh-private-smoke-11111111")
    script = tmp_path / "run.py"
    script.write_bytes(source)
    assert str(run_id).encode() in source
    assert b"Path(__file__).read_bytes()" in source
    assert b"source_sha256" in source
    assert b"is_private" not in source


def test_real_notebook_canary_fails_before_mutation_without_modern_token(
    monkeypatch, tmp_path: Path
) -> None:  # type: ignore[no-untyped-def]
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path / "no-token"))
    receipt = tmp_path / "blocker.json"
    assert run_notebook_canary(ledger_path=tmp_path / "ledger.sqlite3", receipt_path=receipt) == EXTERNAL_BLOCKED
    payload = json.loads(receipt.read_text())
    assert payload["blocker_code"] == "KAGGLE_MODERN_API_TOKEN_REQUIRED"
    assert not (tmp_path / "ledger.sqlite3").exists()
