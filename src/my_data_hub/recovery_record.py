"""Append a verified recovery receipt to the canonical recovery evidence journal."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
from uuid import UUID

from my_data_hub.hashing import canonical_json_bytes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument(
        "--database-url", default=os.getenv("MY_DATA_HUB_RECOVERY_CONTROL_DATABASE_URL", "")
    )
    args = parser.parse_args()
    if not args.database_url:
        parser.error("recovery control database URL is required")
    receipt = json.loads(args.receipt.read_text(encoding="utf-8"))
    unsigned = {key: value for key, value in receipt.items() if key != "receipt_sha256"}
    expected = hashlib.sha256(canonical_json_bytes(unsigned)).hexdigest()
    if receipt.get("status") != "succeeded" or receipt.get("receipt_sha256") != expected:
        parser.error("recovery receipt is not a successful self-hashed receipt")
    restore = receipt["restore"]
    backup = receipt["backup"]
    off_host = receipt["off_host"]
    evidence_id = UUID(str(receipt["receipt_id"]))
    canonical_revision = int(restore["canonical_revision"])

    import psycopg

    with psycopg.connect(args.database_url, connect_timeout=3) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SET LOCAL statement_timeout = '3000ms'")
            cursor.execute(
                """
                INSERT INTO recovery.evidence (
                    evidence_id, run_id, commit_sha, evidence_type, status,
                    artifact_sha256, readback_sha256, encrypted, private_offhost,
                    readback_verified, restore_verified, schema_revision, manifest,
                    completed_at
                ) VALUES (%s, %s, %s, 'isolated_restore', 'passed', %s, %s,
                    true, true, true, true, %s, %s::jsonb, %s)
                ON CONFLICT (evidence_id) DO NOTHING
                """,
                (
                    evidence_id,
                    str(receipt["receipt_id"]),
                    str(backup["repository_commit"]),
                    str(backup["encrypted_artifact_sha256"]),
                    str(off_host["readback_sha256"]),
                    int(restore["schema_revision"]),
                    json.dumps(receipt, ensure_ascii=False, sort_keys=True),
                    str(restore["completed_at"]),
                ),
            )
            cursor.execute(
                """
                INSERT INTO sync.checkpoint (
                    canonical_revision, checkpoint_kind, locator, sha256,
                    manifest_sha256, postgres_major, extension_versions,
                    encrypted, verified_readback_at
                ) VALUES (%s, 'portable_logical', %s, %s, %s, %s, %s::jsonb,
                    true, %s)
                ON CONFLICT (canonical_revision) DO NOTHING
                """,
                (
                    canonical_revision,
                    str(off_host["object_locator"]),
                    str(backup["encrypted_artifact_sha256"]),
                    str(backup["manifest_sha256"]),
                    int(restore["postgres_major"]),
                    json.dumps(restore["extension_versions"], sort_keys=True),
                    str(off_host["verified_at"]),
                ),
            )
            cursor.execute(
                """
                SELECT checkpoint_id FROM sync.checkpoint
                WHERE canonical_revision = %s
                  AND checkpoint_kind = 'portable_logical'
                  AND locator = %s
                  AND sha256 = %s
                  AND manifest_sha256 = %s
                  AND postgres_major = %s
                  AND extension_versions = %s::jsonb
                  AND encrypted
                  AND verified_readback_at = %s
                """,
                (
                    canonical_revision,
                    str(off_host["object_locator"]),
                    str(backup["encrypted_artifact_sha256"]),
                    str(backup["manifest_sha256"]),
                    int(restore["postgres_major"]),
                    json.dumps(restore["extension_versions"], sort_keys=True),
                    str(off_host["verified_at"]),
                ),
            )
            checkpoint = cursor.fetchone()
            if checkpoint is None:
                raise RuntimeError(
                    "canonical revision is already bound to different checkpoint evidence"
                )
            checkpoint_id = checkpoint[0]
        connection.commit()
    print(
        json.dumps(
            {
                "ok": True,
                "evidence_id": str(evidence_id),
                "checkpoint_id": str(checkpoint_id),
                "canonical_revision": canonical_revision,
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
