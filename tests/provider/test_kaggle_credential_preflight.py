from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.provider.kaggle_credential_preflight import (
    kaggle_credentials_configured,
    kaggle_exact_kernel_read_credentials_configured,
)


def test_preflight_accepts_legacy_file_used_by_official_sdk(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path))
    credential = tmp_path / "kaggle.json"
    credential.write_text(
        json.dumps({"username": "owner", "key": "k" * 32}),
        encoding="utf-8",
    )
    credential.chmod(0o600)

    assert kaggle_credentials_configured() is True
    assert kaggle_exact_kernel_read_credentials_configured() is False


def test_preflight_rejects_incomplete_or_unsafe_legacy_file(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.delenv("KAGGLE_USERNAME", raising=False)
    monkeypatch.delenv("KAGGLE_KEY", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path))
    credential = tmp_path / "kaggle.json"
    credential.write_text(json.dumps({"username": "owner"}), encoding="utf-8")
    credential.chmod(0o600)
    assert kaggle_credentials_configured() is False

    credential.write_text(
        json.dumps({"username": "owner", "key": "k" * 32}),
        encoding="utf-8",
    )
    credential.chmod(0o644)
    assert kaggle_credentials_configured() is False


def test_preflight_accepts_complete_legacy_environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path))
    monkeypatch.setenv("KAGGLE_USERNAME", "owner")
    monkeypatch.setenv("KAGGLE_KEY", "k" * 32)
    assert kaggle_credentials_configured() is True
    assert kaggle_exact_kernel_read_credentials_configured() is False
