"""Credential-presence checks aligned with the pinned official Kaggle SDK.

The SDK authenticates with an access token first and a legacy username/API-key
pair second.  This preflight only determines whether one of those supported
credential shapes is present; the single production adapter still performs
the actual authentication and every provider operation.
"""

from __future__ import annotations

import json
import os
import stat
from pathlib import Path
from typing import Any

_MIN_SECRET_LENGTH = 20
_MAX_CONFIG_BYTES = 64 * 1024


def _config_directory() -> Path:
    return Path(os.environ.get("KAGGLE_CONFIG_DIR", "~/.kaggle")).expanduser()


def _bounded_regular_private_file(path: Path) -> bool:
    try:
        metadata = path.lstat()
    except FileNotFoundError:
        return False
    return bool(
        stat.S_ISREG(metadata.st_mode)
        and not path.is_symlink()
        and metadata.st_uid == os.geteuid()
        and metadata.st_size <= _MAX_CONFIG_BYTES
        and metadata.st_size > _MIN_SECRET_LENGTH
        and metadata.st_mode & 0o077 == 0
    )


def _legacy_file_configured(path: Path) -> bool:
    if not _bounded_regular_private_file(path):
        return False
    try:
        payload: Any = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return False
    if not isinstance(payload, dict):
        return False
    username = payload.get("username")
    key = payload.get("key")
    return bool(
        isinstance(username, str)
        and username.strip()
        and isinstance(key, str)
        and len(key.strip()) > _MIN_SECRET_LENGTH
    )


def kaggle_credentials_configured() -> bool:
    """Return whether pinned ``kaggle==2.2.4`` has a supported credential.

    No secret is returned, logged, copied, or persisted.  Environment-backed
    legacy credentials are intentionally accepted because the official SDK
    supports them and the existing production events-bot uses that contract.
    """

    if os.environ.get("KAGGLE_API_TOKEN", "").strip():
        return True
    config_dir = _config_directory()
    if _bounded_regular_private_file(config_dir / "access_token"):
        return True
    username = os.environ.get("KAGGLE_USERNAME", "").strip()
    key = os.environ.get("KAGGLE_KEY", "").strip()
    if username and len(key) > _MIN_SECRET_LENGTH:
        return True
    return _legacy_file_configured(config_dir / "kaggle.json")

