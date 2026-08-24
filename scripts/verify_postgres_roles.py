#!/usr/bin/env python3
"""Run positive and adversarial PostgreSQL ACL probes under each R1 remote role."""

from __future__ import annotations

import argparse
import json
import os

import psycopg

from my_data_hub.master_runtime.role_security_probe import run_role_security_probes


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", ""))
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL or --database-url is required")
    with psycopg.connect(args.database_url) as connection:
        result = run_role_security_probes(connection)
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0 if result["ok"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
