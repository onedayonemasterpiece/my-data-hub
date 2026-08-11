"""Credential-presence checks aligned with the pinned official Kaggle SDK.

This module never returns, logs, copies, or persists credential values.  It is
shared by the production control runtime and offline preflights so the adapter
availability decision cannot drift from the supported SDK credential shapes.
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
    except (FileNotFoundError, OSError):
        return False
    return bool(
        stat.S_ISREG(metadata.st_mode)
        and not path.is_symlink()
        and metadata.st_uid == os.geteuid()
        and 0 < metadata.st_size <= _MAX_CONFIG_BYTES
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
    """Return whether the official SDK has one supported control credential.

    Access-token and legacy username/key authentication are both accepted.
    Credential material remains in the control process environment or its
    private SDK configuration directory; it is never projected into a launch.
    """

    if os.environ.get("KAGGLE_API_TOKEN", "").strip():
        return True
    config_dir = _config_directory()
    if _bounded_regular_private_file(config_dir / "access_token"):
        try:
            if (config_dir / "access_token").read_text(encoding="utf-8").strip():
                return True
        except (OSError, UnicodeDecodeError):
            pass
    username = os.environ.get("KAGGLE_USERNAME", "").strip()
    key = os.environ.get("KAGGLE_KEY", "").strip()
    if username and len(key) > _MIN_SECRET_LENGTH:
        return True
    return _legacy_file_configured(config_dir / "kaggle.json")


def kaggle_exact_kernel_read_credentials_configured() -> bool:
    """Return whether the private exact-kernel source API has proven auth.

    Legacy credentials are intentionally excluded from this narrower check.
    Master launches instead use push-time identity followed by authenticated
    runtime source attestation; general worker launches retain exact readback.
    """

    if os.environ.get("KAGGLE_API_TOKEN", "").strip():
        return True
    path = _config_directory() / "access_token"
    if not _bounded_regular_private_file(path):
        return False
    try:
        return bool(path.read_text(encoding="utf-8").strip())
    except (OSError, UnicodeDecodeError):
        return False
