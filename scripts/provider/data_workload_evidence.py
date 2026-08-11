#!/usr/bin/env python3
"""Resume one exact H6 metadata workload until a terminal pause/result/deadline."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

from my_data_hub.acceptance.data_production import (
    AtomicJsonStateStore,
    ControlPlaneDataWorkloadGateway,
    ProductionCapabilityBlocker,
    ProductionDataWorkloadConfig,
    ProductionDataWorkloadReceipt,
    StreamableHttpMcpMetadataClient,
    UrllibControlMetadataClient,
    load_owner_authorization,
    run_production_data_workload,
)
from my_data_hub.acceptance.data_workloads import DataWorkloadPlan
from my_data_hub.hashing import canonical_json_bytes

MAX_INPUT_BYTES = 256 * 1024


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--plan", type=Path, required=True)
    parser.add_argument("--production-config", type=Path, required=True)
    parser.add_argument("--state", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--owner-envelope", type=Path)
    parser.add_argument("--control-token-env", default="MY_DATA_HUB_DATA_CONTROL_TOKEN")
    parser.add_argument("--reader-token-env", default="MY_DATA_HUB_DATA_MCP_READER_TOKEN")
    parser.add_argument("--operator-token-env", default="MY_DATA_HUB_DATA_MCP_OPERATOR_TOKEN")
    return parser


def _read_model(path: Path, model: Any) -> Any:
    if path.is_symlink() or not path.is_file():
        raise ValueError(f"{path.name} must be a regular non-symlink file")
    raw = path.read_bytes()
    if not raw or len(raw) > MAX_INPUT_BYTES:
        raise ValueError(f"{path.name} is empty or exceeds 256 KiB")
    return model.model_validate_json(raw)


def _token(name: str, *, optional: bool = False) -> str | None:
    value = os.environ.get(name, "")
    if not value and optional:
        return None
    if not 24 <= len(value) <= 4096 or any(char.isspace() for char in value):
        raise ValueError(f"required credential environment {name} is absent or invalid")
    return value


def _write_output(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(payload) + b"\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


async def _run(args: argparse.Namespace) -> int:
    plan = _read_model(args.plan, DataWorkloadPlan)
    config = _read_model(args.production_config, ProductionDataWorkloadConfig)
    store = AtomicJsonStateStore(args.state)
    envelope = None
    authorization = None
    if args.owner_envelope is not None:
        try:
            envelope, authorization = load_owner_authorization(args.owner_envelope)
        except ProductionCapabilityBlocker as blocker:
            state = store.load(plan)
            receipt = ProductionDataWorkloadReceipt(
                matrix_id=plan.matrix_id,
                outcome="BLOCKED",
                state_sha256=hashlib.sha256(canonical_json_bytes(state.model_dump(mode="json"))).hexdigest(),
                blocker_code=blocker.code,
            )
            _write_output(args.output, receipt.model_dump(mode="json", exclude_none=True))
            print(receipt.outcome)
            return 2
    control = UrllibControlMetadataClient(
        config.control_base_url,
        _token(args.control_token_env, optional=config.control_base_url.startswith("http://127.0.0.1:8080")),
    )
    mcp = StreamableHttpMcpMetadataClient(
        config.mcp_endpoint,
        {
            "reader": str(_token(args.reader_token_env)),
            "operator": str(_token(args.operator_token_env)),
        },
    )
    gateway = ControlPlaneDataWorkloadGateway(
        plan=plan,
        config=config,
        control=control,
        mcp=mcp,
        owner_envelope=envelope,
    )
    receipt = await run_production_data_workload(
        plan=plan,
        store=store,
        gateway=gateway,
        owner_authorization=authorization,
        timeout_seconds=config.timeout_seconds,
        poll_seconds=config.poll_seconds,
    )
    _write_output(args.output, receipt.model_dump(mode="json", exclude_none=True))
    print(receipt.outcome)
    return 0 if receipt.outcome in {"AWAITING_OWNER_AUTHORIZATION", "EVIDENCE_READY"} else 2


def main() -> int:
    args = _parser().parse_args()
    try:
        return asyncio.run(_run(args))
    except (ProductionCapabilityBlocker, ValueError, json.JSONDecodeError) as exc:
        code = exc.code if isinstance(exc, ProductionCapabilityBlocker) else "DATA_WORKLOAD_INPUT_INVALID"
        print(code, file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
