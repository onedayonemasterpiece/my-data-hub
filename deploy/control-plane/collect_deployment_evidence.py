#!/usr/bin/env python3
"""Exercise and sign host-derived devstand deployment evidence.

The three commands are intentionally separate because a real host reboot must
occur between PREPARE_REBOOT and SIGN_DEPLOYMENT_EVIDENCE.  This program never
initiates a host reboot and never accepts claimed evidence fields as arguments.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

from my_data_hub.control_plane.deployment_evidence import (
    CollectorPaths,
    CommandRunner,
    DeploymentEvidenceError,
    collect_and_sign,
    exercise_process_kill,
    prepare_reboot,
)


def _paths(args: argparse.Namespace) -> CollectorPaths:
    runtime = Path(args.runtime_root).expanduser()
    release = Path(args.release_root).expanduser()
    state = Path(args.state_file).expanduser() if args.state_file else runtime / "deployment-evidence-state.v1.json"
    return CollectorPaths(
        source_root=Path(args.source_root).expanduser(),
        runtime_root=runtime,
        release_root=release,
        state_file=state,
    )


def main() -> int:
    home = Path.home()
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=("EXERCISE_PROCESS_KILL", "PREPARE_REBOOT", "SIGN_DEPLOYMENT_EVIDENCE"),
    )
    parser.add_argument("--source-root", default=str(Path.cwd()))
    parser.add_argument(
        "--runtime-root",
        default=os.getenv(
            "MY_DATA_HUB_CONTROL_RUNTIME_DIR", str(home / ".local/state/my-data-hub-control-plane")
        ),
    )
    parser.add_argument(
        "--release-root",
        default=os.getenv(
            "MY_DATA_HUB_CONTROL_RELEASE_ROOT", str(home / ".local/opt/my-data-hub-control-plane")
        ),
    )
    parser.add_argument("--state-file", default="")
    parser.add_argument("--target-service", default="remote-mcp")
    parser.add_argument("--timeout-seconds", type=float, default=120)
    parser.add_argument("--signing-key-file", default="")
    parser.add_argument("--key-id", default="")
    parser.add_argument("--output", default="")
    parser.add_argument("--ttl-seconds", type=int, default=3600)
    args = parser.parse_args()

    try:
        paths = _paths(args)
        runner = CommandRunner()
        if args.action == "EXERCISE_PROCESS_KILL":
            result = exercise_process_kill(
                paths,
                runner,
                target_service=args.target_service,
                timeout_seconds=args.timeout_seconds,
            )
            summary = {
                "ok": True,
                "action": args.action,
                "deployed_commit": result["deployed_commit"],
                "state_file": str(paths.state_file),
            }
        elif args.action == "PREPARE_REBOOT":
            result = prepare_reboot(paths, runner)
            summary = {
                "ok": True,
                "action": args.action,
                "deployed_commit": result["deployed_commit"],
                "state_file": str(paths.state_file),
                "next_action": "operator-authorized host reboot, then SIGN_DEPLOYMENT_EVIDENCE",
            }
        else:
            if not args.signing_key_file or not args.key_id or not args.output:
                raise DeploymentEvidenceError("signing requires the external key path, key id and output path")
            receipt = collect_and_sign(
                paths,
                runner,
                signing_key_file=Path(args.signing_key_file).expanduser(),
                key_id=args.key_id,
                output=Path(args.output).expanduser(),
                ttl_seconds=args.ttl_seconds,
            )
            summary = {
                "ok": True,
                "action": args.action,
                "source_identity": receipt["source_identity"],
                "deployed_commit": receipt["deployed_commit"],
                "receipt": str(Path(args.output).expanduser()),
            }
        print(json.dumps(summary, sort_keys=True))
        return 0
    except Exception as exc:
        # Host command output, environment values, private key material and
        # unsigned state are intentionally excluded from operator logs.
        print(f"deployment evidence collection failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
