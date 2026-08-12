#!/usr/bin/env python3
"""Fail-closed offline verification of an exact checkpoint package."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from my_data_hub.checkpoints.manifest import load_and_verify


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--manifest", type=Path)
    args = parser.parse_args()
    manifest_path = args.manifest or args.package / "checkpoint-manifest.json"
    manifest = load_and_verify(manifest_path, args.package)
    print(
        json.dumps(
            {
                "ok": True,
                "checkpoint_id": str(manifest.checkpoint_id),
                "manifest_sha256": manifest.manifest_sha256,
                "file_count": len(manifest.files),
                "byte_size": sum(item.byte_size for item in manifest.files),
                "schema_version": manifest.schema_version,
                "canonical_revision": manifest.canonical_revision,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
