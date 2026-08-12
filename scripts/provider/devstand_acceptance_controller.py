#!/usr/bin/env python3
"""Fail-closed bootstrap for the devstand-local operational acceptance controller."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from my_data_hub.auth.oauth_credentials import (
    OAuthCredentialError,
    validate_oauth_credential_file,
)

EXTERNAL_BLOCKED = 78
STATIC_MCP_BEARER_NAMES = (
    "MY_DATA_HUB_MCP_CANARY_TOKEN",
    "MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN",
    "MY_DATA_HUB_MCP_MIGRATION_OPERATOR_TOKEN",
    "MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN",
    "MY_DATA_HUB_DATA_MCP_READER_TOKEN",
    "MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN",
)


def _inside(path: Path, root: Path) -> bool:
    try:
        path.relative_to(root)
    except ValueError:
        return False
    return True


def preflight() -> None:
    if os.getenv("RUNNER_ENVIRONMENT", "").strip() != "self-hosted":
        raise OAuthCredentialError("DEVSTAND_SELF_HOSTED_RUNNER_REQUIRED")
    if any(os.getenv(name, "").strip() for name in STATIC_MCP_BEARER_NAMES):
        raise OAuthCredentialError("DEVSTAND_STATIC_MCP_BEARER_FORBIDDEN")
    raw_path = os.getenv("MY_DATA_HUB_MCP_OAUTH_CREDENTIAL_FILE", "").strip()
    path = Path(raw_path)
    if not raw_path or not path.is_absolute() or path != path.resolve(strict=False):
        raise OAuthCredentialError("DEVSTAND_OAUTH_CREDENTIAL_PATH_INVALID")
    for variable in ("GITHUB_WORKSPACE", "RUNNER_TEMP"):
        raw_root = os.getenv(variable, "").strip()
        if raw_root and _inside(path, Path(raw_root).resolve(strict=False)):
            raise OAuthCredentialError("DEVSTAND_OAUTH_CREDENTIAL_PATH_EPHEMERAL")
    validate_oauth_credential_file(
        path, required_profiles=frozenset({"reader", "operator", "provider"})
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("preflight",))
    parser.parse_args()
    try:
        preflight()
    except OAuthCredentialError as exc:
        print(str(exc), file=sys.stderr)
        return EXTERNAL_BLOCKED
    print("DEVSTAND_OAUTH_PREFLIGHT_OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
