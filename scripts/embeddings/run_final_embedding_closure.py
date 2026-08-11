#!/usr/bin/env python3
"""Run FINAL-EMBED; absent modern token/live interfaces exit EX_CONFIG/78."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from my_data_hub.embeddings.production import (
    EXTERNAL_BLOCKED,
    EmbeddingInterfacesUnavailable,
    EmbeddingProductionConfig,
    LocalEmbeddingProductionControl,
    run_embedding_production_closure,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.workloads.bloggers.closure import (
    CANONICAL_MCP_URL,
    LOCAL_CONTROL_URL,
    StreamableHttpClosureMcp,
    modern_kaggle_token_configured,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("run", choices=("run",))
    parser.add_argument("--control-url", default=os.getenv("MY_DATA_HUB_CONTROL_URL", LOCAL_CONTROL_URL))
    parser.add_argument("--mcp-url", default=os.getenv("MY_DATA_HUB_MCP_CANARY_ENDPOINT", CANONICAL_MCP_URL))
    parser.add_argument("--mcp-token", default=os.getenv("MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN", ""))
    parser.add_argument("--idempotency-key", required=True)
    parser.add_argument("--source-revision", required=True)
    parser.add_argument("--blogger-receipt", type=Path, required=True)
    parser.add_argument("--probe-query", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument("--timeout-seconds", type=int, default=43_000)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    # First decision: never touch prerequisite/control/MCP state without a modern token.
    if not modern_kaggle_token_configured():
        return EXTERNAL_BLOCKED
    if not args.blogger_receipt.is_file() or args.blogger_receipt.is_symlink():
        return EXTERNAL_BLOCKED
    try:
        blogger_receipt = json.loads(args.blogger_receipt.read_bytes())
        if not isinstance(blogger_receipt, dict):
            raise ValueError("blogger receipt must be an object")
        config = EmbeddingProductionConfig(
            control_url=args.control_url,
            idempotency_key=args.idempotency_key,
            source_revision=args.source_revision,
            probe_query=args.probe_query,
            timeout_seconds=args.timeout_seconds,
        )
        control = LocalEmbeddingProductionControl(config)
        try:
            mcp = StreamableHttpClosureMcp(args.mcp_url, args.mcp_token)
        except ValueError:
            return EXTERNAL_BLOCKED
        receipt = run_embedding_production_closure(
            config,
            blogger_receipt=blogger_receipt,
            control=control,
            mcp=mcp,
            live_evidence=True,
        )
    except EmbeddingInterfacesUnavailable:
        return EXTERNAL_BLOCKED
    encoded = canonical_json_bytes(receipt) + b"\n"
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    temporary = args.receipt.with_suffix(args.receipt.suffix + ".tmp")
    temporary.write_bytes(encoded)
    temporary.chmod(0o600)
    temporary.replace(args.receipt)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
