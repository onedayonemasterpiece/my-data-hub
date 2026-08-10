#!/usr/bin/env python3
from __future__ import annotations

import argparse

from common import RecoveryContractError, require_safe_label


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate non-secret recovery target identifiers")
    parser.add_argument("--target-id", required=True)
    parser.add_argument("--target-database", required=True)
    parser.add_argument("--source-instance", required=True)
    args = parser.parse_args()
    target_id = require_safe_label(args.target_id, "target_id")
    require_safe_label(args.target_database, "target_database")
    source_instance = require_safe_label(args.source_instance, "source_instance")
    if target_id == source_instance:
        raise RecoveryContractError("target_id must differ from source_instance")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
