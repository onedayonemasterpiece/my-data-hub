#!/usr/bin/env python3
"""Prove the live PostgreSQL synthetic connector R1 flow and exact-once commit."""

from __future__ import annotations

import argparse
import json
import os
import tempfile
import time
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from my_data_hub.connectors.contracts import canonical_json_bytes, payload_sha256
from my_data_hub.connectors.postgres import (
    PostgresConnectorAcceptanceRepository,
    PostgresDailyStatisticsCommitter,
)
from my_data_hub.connectors.repository import AcceptanceDisposition
from my_data_hub.connectors.service import ConnectorIntakeService
from my_data_hub.connectors.spool import (
    ConnectorDeliveryService,
    DeliveryDisposition,
    DeliveryResult,
    DurableConnectorSpool,
)
from my_data_hub.connectors.synthetic import SyntheticConnectorProducer
from my_data_hub.mcp.service import HubService


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--intake-database-url",
        default=os.getenv("MY_DATA_HUB_CONNECTOR_INTAKE_DATABASE_URL", ""),
    )
    parser.add_argument(
        "--committer-database-url",
        default=os.getenv("MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL", ""),
    )
    parser.add_argument(
        "--mcp-reader-database-url",
        default=os.getenv("MY_DATA_HUB_MCP_READER_DATABASE_URL", ""),
    )
    parser.add_argument(
        "--verification-database-url",
        default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", ""),
    )
    parser.add_argument("--sequence", type=int, default=None)
    args = parser.parse_args()
    urls = {
        "intake": args.intake_database_url,
        "committer": args.committer_database_url,
        "MCP reader": args.mcp_reader_database_url,
        "verification": args.verification_database_url,
    }
    missing = [name for name, value in urls.items() if not value]
    if missing:
        raise SystemExit("missing dedicated database URL(s): " + ", ".join(missing))

    import psycopg

    producer = SyntheticConnectorProducer()
    # Millisecond epoch values remain unique enough for a serialized canary while
    # staying inside RFC 8785's interoperable IEEE-754 integer range.
    sequence = args.sequence if args.sequence is not None else time.time_ns() // 1_000_000
    # The daily projection has one initial row per logical date. Map the unique canary
    # sequence into a wide, bounded fixture-only date range so repeated post-deploy runs
    # never masquerade as corrections to an earlier canary.
    reporting_date = date(2000, 1, 1) + timedelta(days=sequence % 1_000_000)
    exact = producer.exact_bytes(reporting_date, sequence=sequence)
    repository = PostgresConnectorAcceptanceRepository(args.intake_database_url)
    intake = ConnectorIntakeService(repository)
    accepted = intake.submit(
        exact,
        authenticated_connector_id=producer.connector_id,
        authenticated_principal=f"service:{producer.connector_id}",
        correlation_id="r1-synthetic-accept",
    )
    replay = intake.submit(
        json.dumps(json.loads(exact), ensure_ascii=False, indent=2).encode(),
        authenticated_connector_id=producer.connector_id,
        authenticated_principal=f"service:{producer.connector_id}",
        correlation_id="r1-synthetic-replay",
    )
    changed = json.loads(exact)
    changed["inline_records"][0]["counts"]["accepted"] += 1
    changed["payload_sha256"] = payload_sha256(changed["inline_records"])
    conflict = intake.submit(
        canonical_json_bytes(changed),
        authenticated_connector_id=producer.connector_id,
        authenticated_principal=f"service:{producer.connector_id}",
        correlation_id="r1-synthetic-conflict",
    )
    if not (
        accepted.disposition is AcceptanceDisposition.ACCEPTED
        and replay.disposition is AcceptanceDisposition.REPLAYED
        and conflict.disposition is AcceptanceDisposition.QUARANTINED
        and accepted.receipt is not None
        and replay.receipt == accepted.receipt
        and conflict.quarantine is not None
    ):
        raise SystemExit("connector replay/conflict dispositions did not match the contract")

    committer = PostgresDailyStatisticsCommitter(args.committer_database_url)
    first_commit = committer.commit(accepted.receipt.batch_id)
    repeated_commit = committer.commit(accepted.receipt.batch_id)
    if first_commit.duplicate or not repeated_commit.duplicate or first_commit != repeated_commit.__class__(
        batch_id=repeated_commit.batch_id,
        canonical_revision=repeated_commit.canonical_revision,
        outbox_id=repeated_commit.outbox_id,
        duplicate=False,
    ):
        raise SystemExit("connector canonical commit was not exactly once")

    outage_exact = producer.exact_bytes(
        reporting_date + timedelta(days=1), sequence=sequence + 1
    )
    outage_at = datetime.now(UTC)

    class UnavailableTransport:
        def submit(self, _exact_envelope_bytes: bytes) -> DeliveryResult:
            raise TimeoutError("synthetic transport outage")

    class IntakeTransport:
        def submit(self, exact_envelope_bytes: bytes) -> DeliveryResult:
            result = intake.submit(
                exact_envelope_bytes,
                authenticated_connector_id=producer.connector_id,
                authenticated_principal=f"service:{producer.connector_id}",
                correlation_id="r1-synthetic-eventual-delivery",
            )
            if result.receipt is None:
                return DeliveryResult(DeliveryDisposition.CONFLICT, message="intake conflict")
            disposition = (
                DeliveryDisposition.ACCEPTED
                if result.disposition is AcceptanceDisposition.ACCEPTED
                else DeliveryDisposition.REPLAYED
            )
            return DeliveryResult(disposition, receipt=result.receipt)

    with tempfile.TemporaryDirectory(prefix="mdh-connector-spool-") as temp:
        spool_root = Path(temp) / "spool"
        first_spool = DurableConnectorSpool(spool_root)
        first_spool.enqueue(outage_exact, queued_at=outage_at)
        outage_summary = ConnectorDeliveryService(
            first_spool, UnavailableTransport()
        ).deliver_ready(now=outage_at)
        restarted_spool = DurableConnectorSpool(spool_root)
        recovery_summary = ConnectorDeliveryService(
            restarted_spool, IntakeTransport()
        ).deliver_ready(now=outage_at + timedelta(seconds=2))
        receipt_files = list(restarted_spool.receipts_dir.glob("*.json"))
        eventual_receipt = json.loads(receipt_files[0].read_bytes())
        eventual_commit = committer.commit(eventual_receipt["batch_id"])
        eventual_replay = committer.commit(eventual_receipt["batch_id"])
        spool_ok = (
            outage_summary.deferred == 1
            and recovery_summary.delivered == 1
            and not restarted_spool.pending(ready_at=outage_at + timedelta(seconds=2))
            and len(receipt_files) == 1
            and not eventual_commit.duplicate
            and eventual_replay.duplicate
        )

    mcp_status = HubService(
        args.mcp_reader_database_url,
        scopes=frozenset({"connector:read"}),
        write_enabled=False,
    ).connector_status()
    mcp_row = next(
        row for row in mcp_status if row["data_product"] == "synthetic.daily-statistics.v1"
    )

    with psycopg.connect(
        args.verification_database_url
    ) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                (SELECT count(*) FROM integration.batch WHERE batch_id = %s),
                (SELECT count(*) FROM integration.daily_statistic WHERE batch_id = %s),
                (SELECT count(*) FROM integration.quarantine WHERE quarantine_id = %s),
                (SELECT count(*) FROM sync.external_outbox WHERE idempotency_key = %s),
                (SELECT canonical_revision FROM hub.canonical_state WHERE singleton = true)
            """,
            (
                accepted.receipt.batch_id,
                accepted.receipt.batch_id,
                conflict.quarantine.quarantine_id,
                f"connector-commit:{accepted.receipt.batch_id}",
            ),
        )
        counts = tuple(int(value) for value in cursor.fetchone())
    ok = (
        counts[:4] == (1, 1, 1, 1)
        and counts[4] >= first_commit.canonical_revision
        and spool_ok
        and mcp_row["committed_batches"] >= 2
    )
    report = {
        "ok": ok,
        "batch_id": str(accepted.receipt.batch_id),
        "acceptance_receipt_id": str(accepted.receipt.receipt_id),
        "quarantine_id": str(conflict.quarantine.quarantine_id),
        "commit_receipt": {
            **asdict(first_commit),
            "batch_id": str(first_commit.batch_id),
            "outbox_id": str(first_commit.outbox_id),
        },
        "counts": {
            "batch": counts[0],
            "projection": counts[1],
            "quarantine": counts[2],
            "semantic_outbox": counts[3],
            "canonical_revision": counts[4],
        },
        "outage_restart": {
            "first_delivery_deferred": outage_summary.deferred,
            "eventual_delivery_count": recovery_summary.delivered,
            "durable_receipts": len(receipt_files),
            "eventual_batch_id": str(eventual_receipt["batch_id"]),
            "commit_replayed": eventual_replay.duplicate,
        },
        "mcp_read": mcp_row,
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
