#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import load_object, validate_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify an encrypted backup against its manifest")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    args = parser.parse_args()
    manifest = load_object(args.manifest)
    validate_manifest(manifest, args.artifact)
    print(f"backup_id={manifest['backup_id']}")
    print(f"encrypted_sha256={manifest['artifact']['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
