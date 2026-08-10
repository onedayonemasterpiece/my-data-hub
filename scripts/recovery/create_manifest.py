#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
from pathlib import Path
from uuid import uuid4

from common import atomic_write_json, sha256_file, validate_manifest


def main() -> int:
    parser = argparse.ArgumentParser(description="Create a v2 encrypted PostgreSQL backup manifest")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--completed-at", required=True)
    parser.add_argument("--source-instance", required=True)
    parser.add_argument("--source-environment", required=True)
    parser.add_argument("--repository-commit", required=True)
    parser.add_argument("--pg-dump-version", required=True)
    parser.add_argument("--age-recipient", required=True)
    args = parser.parse_args()

    if args.artifact.is_symlink() or not args.artifact.is_file():
        parser.error("--artifact must be a regular, non-symlink file")
    artifact = args.artifact.resolve(strict=True)
    payload = {
        "schema_version": "my-data-hub-postgres-backup.v2",
        "backup_id": str(uuid4()),
        "started_at": args.started_at,
        "completed_at": args.completed_at,
        "source_instance": args.source_instance,
        "source_environment": args.source_environment,
        "repository_commit": args.repository_commit,
        "artifact": {
            "file_name": artifact.name,
            "sha256": sha256_file(artifact),
            "byte_size": artifact.stat().st_size,
            "format": "pg_dump-custom",
            "encryption": "age",
            "recipient_fingerprint_sha256": hashlib.sha256(
                args.age_recipient.encode("utf-8")
            ).hexdigest(),
        },
        "dump": {
            "pg_dump_version": args.pg_dump_version,
            "options": ["--format=custom", "--compress=9", "--no-owner", "--no-privileges"],
        },
    }
    validate_manifest(payload, artifact)
    atomic_write_json(args.output, payload)
    print(f"manifest={args.output}")
    print(f"encrypted_sha256={payload['artifact']['sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
