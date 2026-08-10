#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import re
import subprocess
import tempfile
from pathlib import Path

from common import (
    RecoveryContractError,
    atomic_write_json,
    canonical_sha256,
    load_object,
    require_safe_label,
    require_safe_locator,
    sha256_file,
    utc_now,
    validate_manifest,
)


def _adapter(environment_name: str) -> Path:
    raw = os.environ.get(environment_name, "")
    candidate = Path(raw)
    if not raw or not candidate.is_absolute() or not candidate.is_file():
        raise RecoveryContractError(f"{environment_name} must name an absolute executable file")
    if not os.access(candidate, os.X_OK):
        raise RecoveryContractError(f"{environment_name} is not executable")
    return candidate


def _run_adapter(
    executable: Path,
    *,
    action: str,
    provider: str,
    locator: str,
    source: Path | None,
    destination: Path | None,
    timeout: int,
) -> None:
    environment = {
        name: os.environ[name]
        for name in ("PATH", "HOME", "LANG", "LC_ALL", "SSL_CERT_FILE", "SSL_CERT_DIR")
        if name in os.environ
    }
    raw_allowlist = os.environ.get("MY_DATA_HUB_OFFHOST_ADAPTER_ENV_ALLOWLIST", "")
    forbidden_fragments = (
        "DATABASE_URL",
        "AGE_IDENTITY",
        "WORKER_RESULT_TOKEN",
        "CONNECTOR_CREDENTIALS",
        "MCP_",
    )
    for name in (part.strip() for part in raw_allowlist.split(",") if part.strip()):
        if not re.fullmatch(r"[A-Z][A-Z0-9_]{0,127}", name):
            raise RecoveryContractError("off-host adapter environment allowlist is invalid")
        if any(fragment in name for fragment in forbidden_fragments):
            raise RecoveryContractError(f"secret {name} is forbidden in adapter environment")
        if name not in os.environ:
            raise RecoveryContractError(f"allowlisted adapter environment variable {name} is absent")
        environment[name] = os.environ[name]
    environment.update(
        {
            "MDH_RECOVERY_ACTION": action,
            "MDH_RECOVERY_PROVIDER": provider,
            "MDH_RECOVERY_OBJECT_LOCATOR": locator,
            "MDH_RECOVERY_SOURCE_PATH": str(source) if source else "",
            "MDH_RECOVERY_DESTINATION_PATH": str(destination) if destination else "",
        }
    )
    try:
        result = subprocess.run(
            [str(executable)],
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=environment,
            check=False,
            timeout=timeout,
        )
    except subprocess.TimeoutExpired as exc:
        raise RecoveryContractError(f"{action} adapter timed out") from exc
    if result.returncode != 0:
        raise RecoveryContractError(f"{action} adapter failed with exit code {result.returncode}")


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Upload encrypted bytes and verify exact bytes through provider readback"
    )
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--object-locator", required=True)
    parser.add_argument("--evidence", type=Path, required=True)
    args = parser.parse_args()

    if os.environ.get("MY_DATA_HUB_OFFHOST_UPLOAD_CONFIRM") != "UPLOAD_ENCRYPTED_BACKUP":
        parser.error("set MY_DATA_HUB_OFFHOST_UPLOAD_CONFIRM=UPLOAD_ENCRYPTED_BACKUP")
    if os.environ.get("MY_DATA_HUB_OFFHOST_PRIVATE_CONFIRM") != "PRIVATE_ENCRYPTED_STORAGE":
        parser.error("set MY_DATA_HUB_OFFHOST_PRIVATE_CONFIRM=PRIVATE_ENCRYPTED_STORAGE")
    if os.environ.get("MY_DATA_HUB_OFFHOST_IS_REMOTE") != "OFF_HOST_PRIVATE_STORAGE":
        parser.error("set MY_DATA_HUB_OFFHOST_IS_REMOTE=OFF_HOST_PRIVATE_STORAGE")

    provider = require_safe_label(args.provider, "provider")
    locator = require_safe_locator(args.object_locator)
    manifest = load_object(args.manifest)
    validate_manifest(manifest, args.artifact)
    upload = _adapter("MY_DATA_HUB_OFFHOST_UPLOAD_ADAPTER")
    readback = _adapter("MY_DATA_HUB_OFFHOST_READBACK_ADAPTER")
    try:
        timeout = int(os.environ.get("MY_DATA_HUB_OFFHOST_TIMEOUT_SECONDS", "900"))
    except ValueError as exc:
        raise RecoveryContractError("MY_DATA_HUB_OFFHOST_TIMEOUT_SECONDS must be an integer") from exc
    if timeout < 1 or timeout > 86400:
        raise RecoveryContractError("MY_DATA_HUB_OFFHOST_TIMEOUT_SECONDS must be between 1 and 86400")

    expected_sha = manifest["artifact"]["sha256"]
    expected_size = manifest["artifact"]["byte_size"]
    _run_adapter(
        upload,
        action="upload",
        provider=provider,
        locator=locator,
        source=args.artifact.resolve(),
        destination=None,
        timeout=timeout,
    )
    # Detect a local mutation between validation and upload completion.
    if sha256_file(args.artifact) != expected_sha or args.artifact.stat().st_size != expected_size:
        raise RecoveryContractError("local encrypted artifact changed during upload")

    with tempfile.TemporaryDirectory(prefix="my-data-hub-readback-") as directory:
        readback_path = Path(directory) / "provider-readback.age"
        _run_adapter(
            readback,
            action="readback",
            provider=provider,
            locator=locator,
            source=None,
            destination=readback_path,
            timeout=timeout,
        )
        if not readback_path.is_file() or readback_path.is_symlink():
            raise RecoveryContractError("readback adapter did not create a regular file")
        readback_sha = sha256_file(readback_path)
        readback_size = readback_path.stat().st_size
        if readback_sha != expected_sha or readback_size != expected_size:
            raise RecoveryContractError("off-host readback bytes do not exactly match the encrypted artifact")

    evidence = {
        "schema_version": "my-data-hub-offhost-readback.v1",
        "backup_id": manifest["backup_id"],
        "manifest_sha256": sha256_file(args.manifest),
        "provider": provider,
        "object_locator": locator,
        "uploaded_sha256": expected_sha,
        "uploaded_byte_size": expected_size,
        "readback_sha256": readback_sha,
        "readback_byte_size": readback_size,
        "exact_match": True,
        "verified_at": utc_now(),
    }
    evidence["evidence_sha256"] = canonical_sha256(evidence, omit="evidence_sha256")
    atomic_write_json(args.evidence, evidence)
    print(f"evidence={args.evidence}")
    print(f"readback_sha256={readback_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
