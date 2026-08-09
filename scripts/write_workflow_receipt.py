#!/usr/bin/env python3
"""Write and validate a secret-free GitHub Actions evidence receipt."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
SCHEMA = ROOT / "schemas/workflow-receipt.v1.schema.json"


def _utc(value: str | None = None) -> str:
    if value:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return parsed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _version(command: list[str]) -> str:
    try:
        return subprocess.run(
            command,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        ).stdout.strip().splitlines()[0][:500]
    except Exception:
        return "unavailable"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--workflow", required=True)
    parser.add_argument("--environment", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--outcome", choices=("PASS", "FAIL", "BLOCKED"), required=True)
    parser.add_argument("--check", default="workflow execution")
    parser.add_argument("--expected", default="all required checks pass")
    parser.add_argument("--observed", required=True)
    parser.add_argument("--cleanup", choices=("not_required", "complete", "failed", "blocked"), default="not_required")
    parser.add_argument("--blocker", action="append", default=[])
    parser.add_argument("--resource", action="append", default=[])
    parser.add_argument("--hash-file", action="append", default=[])
    args = parser.parse_args()

    resources: dict[str, str] = {}
    for item in args.resource:
        key, separator, value = item.partition("=")
        if not separator or not key:
            raise SystemExit("--resource must use key=value")
        resources[key] = value

    hashes: dict[str, str] = {}
    for item in args.hash_file:
        key, separator, value = item.partition("=")
        path = Path(value)
        if not separator or not key or not path.is_file():
            raise SystemExit("--hash-file must use key=existing-path")
        hashes[key] = hashlib.sha256(path.read_bytes()).hexdigest()

    commit = os.getenv("GITHUB_SHA") or _version(["git", "rev-parse", "HEAD"])
    receipt = {
        "schema_version": "my-data-hub-workflow-receipt.v1",
        "workflow": args.workflow,
        "run_id": os.getenv("GITHUB_RUN_ID", "local"),
        "run_attempt": int(os.getenv("GITHUB_RUN_ATTEMPT", "1")),
        "trigger": os.getenv("GITHUB_EVENT_NAME", "local"),
        "actor": os.getenv("GITHUB_ACTOR", "local-operator"),
        "repository": os.getenv("GITHUB_REPOSITORY", "onedayonemasterpiece/my-data-hub"),
        "commit": commit,
        "environment": args.environment,
        "started_at": _utc(os.getenv("MY_DATA_HUB_WORKFLOW_STARTED_AT")),
        "finished_at": _utc(),
        "versions": {
            "python": platform.python_version(),
            "postgresql_client": _version(["psql", "--version"]),
            "docker": _version(["docker", "--version"]),
        },
        "resource_ids": resources,
        "hashes": hashes,
        "checks": [
            {
                "name": args.check,
                "expected": args.expected,
                "observed": args.observed,
                "outcome": args.outcome,
            }
        ],
        "cleanup": args.cleanup,
        "blockers": args.blocker,
    }
    schema = json.loads(SCHEMA.read_text(encoding="utf-8"))
    errors = sorted(
        Draft202012Validator(schema, format_checker=FormatChecker()).iter_errors(receipt),
        key=lambda error: list(error.path),
    )
    if errors:
        raise SystemExit("invalid workflow receipt: " + "; ".join(error.message for error in errors))
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(receipt, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
