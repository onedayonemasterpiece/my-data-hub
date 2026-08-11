from __future__ import annotations

import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pytest

from my_data_hub.providers.kaggle.retry import RetryClass, classify_failure
from scripts.provider.real_kaggle_matrix import (
    AnonymousDatasetProbe,
    _AnonymousDatasetProbeError,
    modern_token_configured,
)


class _Response:
    status = 200

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *args: object) -> None:
        return None


def test_modern_token_preflight_accepts_only_supported_nonempty_sources(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("KAGGLE_API_TOKEN", raising=False)
    monkeypatch.setenv("KAGGLE_CONFIG_DIR", str(tmp_path))
    assert modern_token_configured() is False

    token = tmp_path / "access_token"
    token.write_text("x" * 32, encoding="utf-8")
    assert modern_token_configured() is True

    token.unlink()
    target = tmp_path / "elsewhere"
    target.write_text("x" * 32, encoding="utf-8")
    token.symlink_to(target)
    assert modern_token_configured() is False

    monkeypatch.setenv("KAGGLE_API_TOKEN", "runtime-token-is-present")
    assert modern_token_configured() is True


def test_anonymous_probe_uses_exact_https_ref_and_version(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observed: dict[str, object] = {}

    def open_request(request: urllib.request.Request, *, timeout: int) -> _Response:
        observed["url"] = request.full_url
        observed["timeout"] = timeout
        return _Response()

    monkeypatch.setattr("urllib.request.urlopen", open_request)
    assert AnonymousDatasetProbe().read_dataset("owner/private-dataset", 7) == {"status": 200}
    assert observed == {
        "url": (
            "https://www.kaggle.com/api/v1/datasets/download/owner/private-dataset"
            "?datasetVersionNumber=7"
        ),
        "timeout": 20,
    }


def test_anonymous_probe_shapes_http_denial_for_adapter_classifier(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def deny(*_args: object, **_kwargs: object) -> object:
        raise urllib.error.HTTPError("https://www.kaggle.com", 403, "denied", {}, None)

    monkeypatch.setattr("urllib.request.urlopen", deny)
    with pytest.raises(_AnonymousDatasetProbeError) as captured:
        AnonymousDatasetProbe().read_dataset("owner/private-dataset", 1)
    failure = classify_failure(captured.value, now=datetime.now(UTC))
    assert failure.retry_class == RetryClass.AUTHORIZATION
    assert failure.http_status == 403
