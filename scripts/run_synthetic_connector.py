#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from my_data_hub.connectors.spool import ConnectorDeliveryService, DurableConnectorSpool
from my_data_hub.connectors.synthetic import SyntheticConnectorProducer
from my_data_hub.connectors.transport import HttpConnectorTransport


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Enqueue and optionally deliver a deterministic synthetic connector batch."
    )
    parser.add_argument(
        "--reporting-date",
        type=date.fromisoformat,
        default=date.today() - timedelta(days=1),
    )
    parser.add_argument("--timezone", default="UTC")
    parser.add_argument("--sequence", type=int, default=1)
    parser.add_argument("--spool-dir", type=Path, required=True)
    parser.add_argument("--intake-url")
    parser.add_argument(
        "--token-env",
        default="MY_DATA_HUB_CONNECTOR_TOKEN",
        help="environment variable containing the bearer token",
    )
    parser.add_argument("--timeout-seconds", type=float, default=15.0)
    parser.add_argument(
        "--enqueue-only",
        action="store_true",
        help="persist exact bytes without attempting delivery (outage exercise)",
    )
    return parser


def main() -> int:
    args = _parser().parse_args()
    producer = SyntheticConnectorProducer()
    exact_bytes = producer.exact_bytes(
        args.reporting_date,
        timezone_name=args.timezone,
        sequence=args.sequence,
    )
    spool = DurableConnectorSpool(args.spool_dir)
    item = spool.enqueue(exact_bytes)
    output: dict[str, object] = {
        "batch_id": str(item.validated.envelope.batch_id),
        "envelope_sha256": item.validated.envelope_sha256,
        "idempotency_key": item.validated.envelope.idempotency_key,
        "spool_id": item.spool_id,
        "status": "spooled",
    }
    if not args.enqueue_only:
        if not args.intake_url:
            raise SystemExit("--intake-url is required unless --enqueue-only is set")
        token = os.environ.get(args.token_env)
        if not token:
            raise SystemExit(f"connector bearer token is missing from {args.token_env}")
        transport = HttpConnectorTransport(
            args.intake_url,
            token,
            timeout_seconds=args.timeout_seconds,
        )
        summary = ConnectorDeliveryService(spool, transport).deliver_ready(now=datetime.now(UTC))
        output["delivery"] = {
            "attempted": summary.attempted,
            "deferred": summary.deferred,
            "delivered": summary.delivered,
            "quarantined": summary.quarantined,
        }
        output["status"] = "delivered" if summary.delivered else "retained"
    print(json.dumps(output, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
