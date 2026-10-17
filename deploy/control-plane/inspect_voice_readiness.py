#!/usr/bin/env python3
"""Read only safe activation metadata from the installed voice runtime."""

import argparse
import json
import re

from verify_voice_package import VerificationError, capture

SCRIPT = """
import json
import os
import signal

signal.alarm(15)
from my_data_hub.voice_intake_v2.analysis import AnalysisSettings
settings = AnalysisSettings.from_env(os.getenv("MY_DATA_HUB_VOICE_GITHUB_REPOSITORY", ""))
print(json.dumps({
    "enabled": settings.enabled,
    "delivery_ready": settings.delivery_ready,
    "owner_bound": bool(settings.owner_id),
    "source_repository_set": bool(settings.repository),
    "private_config_file_set": bool(os.getenv("MY_DATA_HUB_VOICE_ANALYSIS_CONFIG_FILE", "").strip()),
}))
"""


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", required=True)
    args = parser.parse_args()
    if not re.fullmatch(r"[0-9a-f]{64}", args.container):
        parser.error("full existing container ID required")
    try:
        result = capture(
            ["docker", "exec", args.container, "python", "-c", SCRIPT],
            limit=4096,
            timeout=20,
        )
        status = json.loads(result)
        keys = {"enabled", "delivery_ready", "owner_bound", "source_repository_set", "private_config_file_set"}
        if (
            not isinstance(status, dict)
            or set(status) != keys
            or any(not isinstance(value, bool) for value in status.values())
        ):
            raise ValueError("invalid_runtime_read")
        print(json.dumps({"container_id": args.container, **status}, sort_keys=True))
        return 0 if all(status.values()) else 1
    except (ValueError, VerificationError):
        print(json.dumps({"status": "voice_readiness_unavailable"}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
