#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from common import RecoveryContractError, load_object, validate_manifest
from write_receipt import validate_evidence


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify off-host evidence is bound to a backup")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--offhost-evidence", type=Path, required=True)
    args = parser.parse_args()
    if args.offhost_evidence.is_symlink() or not args.offhost_evidence.is_file():
        raise RecoveryContractError("off-host evidence must be a regular, non-symlink file")
    manifest = load_object(args.manifest)
    validate_manifest(manifest, args.artifact)
    evidence = load_object(args.offhost_evidence)
    validate_evidence(evidence, manifest, args.manifest)
    print(f"source_instance={manifest['source_instance']}")
    print(f"evidence_sha256={evidence['evidence_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
