#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
from uuid import uuid4

from common import (
    RecoveryContractError,
    atomic_write_json,
    canonical_sha256,
    load_object,
    require_safe_label,
    require_safe_locator,
    require_sha256,
    require_timestamp,
    sha256_file,
    utc_now,
    validate_manifest,
)


def validate_evidence(
    evidence: dict[str, object], manifest: dict[str, object], manifest_path: Path
) -> None:
    required = {
        "schema_version",
        "backup_id",
        "manifest_sha256",
        "provider",
        "object_locator",
        "uploaded_sha256",
        "uploaded_byte_size",
        "readback_sha256",
        "readback_byte_size",
        "exact_match",
        "verified_at",
        "evidence_sha256",
    }
    if set(evidence) != required:
        raise RecoveryContractError("off-host evidence fields do not match the v1 contract")
    if evidence["schema_version"] != "my-data-hub-offhost-readback.v1":
        raise RecoveryContractError("unsupported off-host evidence schema_version")
    if evidence["backup_id"] != manifest["backup_id"]:
        raise RecoveryContractError("off-host evidence backup_id does not match manifest")
    if evidence["manifest_sha256"] != sha256_file(manifest_path):
        raise RecoveryContractError("off-host evidence manifest SHA-256 does not match")
    require_safe_label(evidence["provider"], "provider")
    require_safe_locator(evidence["object_locator"])
    expected_sha = manifest["artifact"]["sha256"]  # type: ignore[index]
    expected_size = manifest["artifact"]["byte_size"]  # type: ignore[index]
    if evidence["uploaded_sha256"] != expected_sha or evidence["readback_sha256"] != expected_sha:
        raise RecoveryContractError("off-host evidence SHA-256 does not match the manifest")
    if evidence["uploaded_byte_size"] != expected_size or evidence["readback_byte_size"] != expected_size:
        raise RecoveryContractError("off-host evidence byte size does not match the manifest")
    if evidence["exact_match"] is not True:
        raise RecoveryContractError("off-host evidence is not an exact readback match")
    require_timestamp(evidence["verified_at"], "verified_at")
    actual_evidence_sha = canonical_sha256(evidence, omit="evidence_sha256")
    if require_sha256(evidence["evidence_sha256"], "evidence_sha256") != actual_evidence_sha:
        raise RecoveryContractError("off-host evidence self-hash mismatch")


def main() -> int:
    parser = argparse.ArgumentParser(description="Write a successful isolated recovery receipt")
    parser.add_argument("--artifact", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--offhost-evidence", type=Path, required=True)
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--started-at", required=True)
    parser.add_argument("--completed-at", required=True)
    parser.add_argument("--relations-before", type=int, required=True)
    parser.add_argument("--verification", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    manifest = load_object(args.manifest)
    validate_manifest(manifest, args.artifact)
    evidence = load_object(args.offhost_evidence)
    validate_evidence(evidence, manifest, args.manifest)
    require_safe_label(args.target_id, "target_id")
    require_safe_label(args.target_database, "target_database")
    require_timestamp(args.started_at, "restore.started_at")
    require_timestamp(args.completed_at, "restore.completed_at")
    if args.relations_before != 0:
        raise RecoveryContractError("recovery receipt requires a fresh target with zero relations")
    verification = load_object(args.verification)
    if verification.get("ok") is not True:
        raise RecoveryContractError("restored-state verification did not pass")
    verification_evidence = verification.get("evidence")
    if not isinstance(verification_evidence, dict):
        raise RecoveryContractError("restored-state verification lacks evidence")
    for integer_field in ("schema_revision", "canonical_revision", "postgres_major"):
        if not isinstance(verification_evidence.get(integer_field), int):
            raise RecoveryContractError(
                f"restored-state verification lacks integer {integer_field}"
            )
    extension_versions = verification_evidence.get("extension_versions")
    if not isinstance(extension_versions, dict) or not extension_versions or not all(
        isinstance(name, str) and isinstance(version, str)
        for name, version in extension_versions.items()
    ):
        raise RecoveryContractError(
            "restored-state verification lacks extension versions"
        )

    artifact = manifest["artifact"]
    receipt = {
        "schema_version": "my-data-hub-recovery-receipt.v1",
        "receipt_id": str(uuid4()),
        "backup_id": manifest["backup_id"],
        "status": "succeeded",
        "created_at": utc_now(),
        "backup": {
            "manifest_sha256": sha256_file(args.manifest),
            "encrypted_artifact_sha256": artifact["sha256"],
            "encrypted_byte_size": artifact["byte_size"],
            "encryption": artifact["encryption"],
            "source_instance": manifest["source_instance"],
            "source_environment": manifest["source_environment"],
            "repository_commit": manifest["repository_commit"],
        },
        "off_host": {
            "provider": evidence["provider"],
            "object_locator": evidence["object_locator"],
            "uploaded_sha256": evidence["uploaded_sha256"],
            "readback_sha256": evidence["readback_sha256"],
            "readback_byte_size": evidence["readback_byte_size"],
            "exact_match": True,
            "verified_at": evidence["verified_at"],
            "evidence_sha256": evidence["evidence_sha256"],
        },
        "restore": {
            "target_id": args.target_id,
            "target_database": args.target_database,
            "isolated": True,
            "freshness_check": "passed",
            "relations_before": args.relations_before,
            "started_at": args.started_at,
            "completed_at": args.completed_at,
            "pg_restore": "passed",
            "application_verify": "passed",
            "restored_state_verify": "passed",
            "restored_state_verify_sha256": sha256_file(args.verification),
            "schema_revision": verification_evidence["schema_revision"],
            "canonical_revision": verification_evidence["canonical_revision"],
            "postgres_major": verification_evidence["postgres_major"],
            "extension_versions": extension_versions,
            "automatic_promotion": False,
        },
    }
    receipt["receipt_sha256"] = canonical_sha256(receipt, omit="receipt_sha256")
    atomic_write_json(args.output, receipt)
    print(f"receipt={args.output}")
    print(f"receipt_sha256={receipt['receipt_sha256']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
