#!/usr/bin/env python3
"""Issue/verify the explicit, short-lived remote operator deployment gate."""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import sqlite3
import stat
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

CONTRACT = "my-data-hub-operator-security-gate.v1"
MAX_LIFETIME = timedelta(hours=24)
_SHA = re.compile(r"^[a-f0-9]{64}$")
_COMMIT = re.compile(r"^[a-f0-9]{40}$")
_FIELDS = {
    "contract",
    "release_commit",
    "outcome",
    "checkpoint_id",
    "checkpoint_revision",
    "role_verification_sha256",
    "security_test_receipt_sha256",
    "issued_at",
    "expires_at",
    "signature",
}


class OperatorGateError(ValueError):
    pass


def canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def _private_bytes(path: Path, label: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise OperatorGateError(f"{label} must be a regular non-symlink file")
    mode = stat.S_IMODE(path.stat().st_mode)
    if mode & 0o077:
        raise OperatorGateError(f"{label} must not be group/world accessible")
    value = path.read_bytes().strip()
    if not 32 <= len(value) <= 256:
        raise OperatorGateError(f"{label} length is outside 32..256 bytes")
    return value


def _timestamp(value: Any, label: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise OperatorGateError(f"{label} must be an ISO-8601 timestamp") from exc
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise OperatorGateError(f"{label} must include a timezone")
    return parsed.astimezone(UTC)


def _unsigned(payload: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in payload.items() if key != "signature"}


def _ledger_authority(path: Path, *, commit: str) -> dict[str, Any]:
    if path.is_symlink() or not path.is_file() or stat.S_IMODE(path.stat().st_mode) & 0o077:
        raise OperatorGateError("control ledger must be a private regular non-symlink file")
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        evidence = connection.execute(
            "SELECT * FROM master_security_evidence WHERE source_commit=? ORDER BY observed_at DESC LIMIT 1",
            (commit,),
        ).fetchone()
        head = connection.execute(
            "SELECT h.current_checkpoint_id,c.* FROM checkpoint_heads h "
            "JOIN checkpoint_candidates c ON c.checkpoint_id=h.current_checkpoint_id "
            "WHERE h.service_kind='postgres-master'",
        ).fetchone()
    except sqlite3.Error as exc:
        raise OperatorGateError("control ledger lacks master security/checkpoint authority") from exc
    finally:
        connection.close()
    if evidence is None or head is None or head["status"] != "VERIFIED" or not head["verified_at"]:
        raise OperatorGateError("operator gate requires current verified security and checkpoint evidence")
    try:
        manifest = json.loads(str(head["manifest_json"]))
        checkpoint_revision = int(manifest["canonical_revision"])
    except (TypeError, ValueError, KeyError, json.JSONDecodeError) as exc:
        raise OperatorGateError("current checkpoint manifest is invalid") from exc
    if (
        head["master_instance_id"] != evidence["master_instance_id"]
        or int(head["epoch"]) != int(evidence["epoch"])
        or checkpoint_revision < int(evidence["canonical_revision"])
    ):
        raise OperatorGateError("security evidence is not protected by the current verified checkpoint")
    return {
        "checkpoint_id": str(head["checkpoint_id"]),
        "checkpoint_revision": checkpoint_revision,
        "role_verification_sha256": str(evidence["role_verification_sha256"]),
        "security_test_receipt_sha256": str(evidence["security_test_receipt_sha256"]),
    }


def _validate_contract(payload: dict[str, Any], *, commit: str, now: datetime) -> None:
    if set(payload) != _FIELDS:
        raise OperatorGateError("operator gate fields differ from the exact contract")
    if payload["contract"] != CONTRACT or payload["outcome"] != "PASSED":
        raise OperatorGateError("operator security gates are not PASSED")
    if not _COMMIT.fullmatch(commit) or payload["release_commit"] != commit:
        raise OperatorGateError("operator gate does not bind the exact release commit")
    try:
        UUID(str(payload["checkpoint_id"]))
    except ValueError as exc:
        raise OperatorGateError("checkpoint_id must be an exact UUID") from exc
    revision = payload["checkpoint_revision"]
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise OperatorGateError("checkpoint_revision must be a non-negative integer")
    for key in ("role_verification_sha256", "security_test_receipt_sha256"):
        if not isinstance(payload[key], str) or not _SHA.fullmatch(payload[key]):
            raise OperatorGateError(f"{key} must be an exact SHA-256")
    issued = _timestamp(payload["issued_at"], "issued_at")
    expires = _timestamp(payload["expires_at"], "expires_at")
    if expires <= issued or expires - issued > MAX_LIFETIME:
        raise OperatorGateError("operator gate lifetime must be positive and no more than 24 hours")
    if issued > now + timedelta(minutes=5) or expires <= now:
        raise OperatorGateError("operator gate is not currently valid")


def verify(
    receipt: Path,
    signing_key: Path,
    *,
    commit: str,
    now: datetime | None = None,
    control_ledger: Path | None = None,
) -> dict[str, Any]:
    key = _private_bytes(signing_key, "operator write-gate signing key")
    if receipt.is_symlink() or not receipt.is_file() or receipt.stat().st_size > 16 * 1024:
        raise OperatorGateError("operator gate receipt must be a bounded regular non-symlink file")
    try:
        payload = json.loads(receipt.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise OperatorGateError("operator gate receipt is invalid JSON") from exc
    if not isinstance(payload, dict):
        raise OperatorGateError("operator gate receipt must be an object")
    _validate_contract(payload, commit=commit, now=(now or datetime.now(UTC)))
    supplied = str(payload["signature"])
    expected = hmac.new(key, canonical_json_bytes(_unsigned(payload)), hashlib.sha256).hexdigest()
    if not _SHA.fullmatch(supplied) or not hmac.compare_digest(supplied, expected):
        raise OperatorGateError("operator gate signature is invalid")
    if control_ledger is not None:
        authority = _ledger_authority(control_ledger, commit=commit)
        if any(payload[key] != authority[key] for key in authority):
            raise OperatorGateError("operator gate differs from current ledger security/checkpoint authority")
    return payload


def issue(args: argparse.Namespace) -> None:
    key = _private_bytes(args.signing_key_file, "operator write-gate signing key")
    now = datetime.now(UTC)
    expires = _timestamp(args.expires_at, "expires_at")
    payload: dict[str, Any] = {
        "contract": CONTRACT,
        "release_commit": args.commit,
        "outcome": "PASSED",
        "checkpoint_id": args.checkpoint_id,
        "checkpoint_revision": args.checkpoint_revision,
        "role_verification_sha256": args.role_verification_sha256,
        "security_test_receipt_sha256": args.security_test_receipt_sha256,
        "issued_at": now.isoformat().replace("+00:00", "Z"),
        "expires_at": expires.isoformat().replace("+00:00", "Z"),
    }
    payload["signature"] = hmac.new(
        key, canonical_json_bytes(payload), hashlib.sha256
    ).hexdigest()
    _validate_contract(payload, commit=args.commit, now=now)
    output: Path = args.output
    if output.is_symlink() or not output.parent.is_dir() or output.parent.is_symlink():
        raise OperatorGateError("operator gate output parent must be a real directory")
    temporary = output.with_name(f".{output.name}.tmp-{os.getpid()}")
    temporary.write_bytes(canonical_json_bytes(payload) + b"\n")
    temporary.chmod(0o600)
    temporary.replace(output)
    print(f"issued_operator_gate_commit={args.commit}")


def issue_from_ledger(args: argparse.Namespace) -> None:
    authority = _ledger_authority(args.control_ledger, commit=args.commit)
    for key, value in authority.items():
        setattr(args, key, value)
    issue(args)


def parser() -> argparse.ArgumentParser:
    root = argparse.ArgumentParser()
    commands = root.add_subparsers(dest="command", required=True)
    verify_parser = commands.add_parser("verify")
    verify_parser.add_argument("--commit", required=True)
    verify_parser.add_argument("--receipt", type=Path, required=True)
    verify_parser.add_argument("--signing-key-file", type=Path, required=True)
    verify_parser.add_argument("--control-ledger", type=Path)
    issue_parser = commands.add_parser("issue")
    issue_parser.add_argument("--commit", required=True)
    issue_parser.add_argument("--checkpoint-id", required=True)
    issue_parser.add_argument("--checkpoint-revision", type=int, required=True)
    issue_parser.add_argument("--role-verification-sha256", required=True)
    issue_parser.add_argument("--security-test-receipt-sha256", required=True)
    issue_parser.add_argument("--expires-at", required=True)
    issue_parser.add_argument("--signing-key-file", type=Path, required=True)
    issue_parser.add_argument("--output", type=Path, required=True)
    ledger_parser = commands.add_parser("issue-from-ledger")
    ledger_parser.add_argument("--commit", required=True)
    ledger_parser.add_argument("--control-ledger", type=Path, required=True)
    ledger_parser.add_argument("--expires-at", required=True)
    ledger_parser.add_argument("--signing-key-file", type=Path, required=True)
    ledger_parser.add_argument("--output", type=Path, required=True)
    return root


def main() -> None:
    args = parser().parse_args()
    try:
        if args.command == "verify":
            verify(
                args.receipt,
                args.signing_key_file,
                commit=args.commit,
                control_ledger=args.control_ledger,
            )
            print(f"verified_operator_gate_commit={args.commit}")
        elif args.command == "issue-from-ledger":
            issue_from_ledger(args)
        else:
            issue(args)
    except OperatorGateError as exc:
        raise SystemExit(f"operator gate rejected: {exc}") from exc


if __name__ == "__main__":
    main()
