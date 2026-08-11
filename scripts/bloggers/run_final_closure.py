#!/usr/bin/env python3
"""Run the production FINAL-BLOGGER closure; no token means EX_CONFIG/78."""

from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path
from uuid import UUID

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.closure import (
    CANONICAL_MCP_URL,
    EXTERNAL_BLOCKED,
    LOCAL_CONTROL_URL,
    ClosureConfig,
    LocalClosureControl,
    StreamableHttpClosureMcp,
    modern_kaggle_token_configured,
    run_blogger_closure,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", choices=("run",))
    parser.add_argument("--control-url", default=os.getenv("MY_DATA_HUB_CONTROL_URL", LOCAL_CONTROL_URL))
    parser.add_argument("--mcp-url", default=os.getenv("MY_DATA_HUB_MCP_CANARY_ENDPOINT", CANONICAL_MCP_URL))
    parser.add_argument("--mcp-token", default=os.getenv("MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN", ""))
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--project-id", type=UUID, required=True)
    parser.add_argument("--snapshot-at", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=43_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    # This is deliberately the first environmental/provider decision.  In
    # particular, no control request, ledger file, or receipt is created first.
    if not modern_kaggle_token_configured():
        return EXTERNAL_BLOCKED
    config = ClosureConfig(
        control_url=args.control_url,
        idempotency_key=args.idempotency_key,
        project_id=args.project_id,
        snapshot_at=datetime.fromisoformat(args.snapshot_at.replace("Z", "+00:00")),
        source_revision=args.source_revision,
        timeout_seconds=args.timeout_seconds,
    )
    mcp = StreamableHttpClosureMcp(args.mcp_url, args.mcp_token)
    receipt = run_blogger_closure(config, control=LocalClosureControl(config), mcp=mcp)
    encoded = canonical_json_bytes(receipt)
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.receipt.with_suffix(args.receipt.suffix + ".tmp")
    temporary.write_bytes(encoded + b"\n")
    temporary.chmod(0o600)
    temporary.replace(args.receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
