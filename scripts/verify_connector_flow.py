#!/usr/bin/env python3
"""Prove the live PostgreSQL synthetic connector R1 flow and exact-once commit."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict
from datetime import date

from my_data_hub.connectors.contracts import canonical_json_bytes, payload_sha256
from my_data_hub.connectors.postgres import (
    PostgresConnectorAcceptanceRepository,
    PostgresDailyStatisticsCommitter,
)
from my_data_hub.connectors.repository import AcceptanceDisposition
from my_data_hub.connectors.service import ConnectorIntakeService
from my_data_hub.connectors.synthetic import SyntheticConnectorProducer


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--database-url", default=os.getenv("MY_DATA_HUB_CONNECTOR_DATABASE_URL", ""))
    args = parser.parse_args()
    if not args.database_url:
        raise SystemExit("MY_DATA_HUB_CONNECTOR_DATABASE_URL or --database-url is required")

    import psycopg

    producer = SyntheticConnectorProducer()
    exact = producer.exact_bytes(date(2026, 8, 9), sequence=987)
    repository = PostgresConnectorAcceptanceRepository(args.database_url)
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

    committer = PostgresDailyStatisticsCommitter(args.database_url)
    first_commit = committer.commit(accepted.receipt.batch_id)
    repeated_commit = committer.commit(accepted.receipt.batch_id)
    if first_commit.duplicate or not repeated_commit.duplicate or first_commit != repeated_commit.__class__(
        batch_id=repeated_commit.batch_id,
        canonical_revision=repeated_commit.canonical_revision,
        outbox_id=repeated_commit.outbox_id,
        duplicate=False,
    ):
        raise SystemExit("connector canonical commit was not exactly once")

    with psycopg.connect(args.database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                (SELECT count(*) FROM integration.batch WHERE batch_id = %s),
                (SELECT count(*) FROM integration.daily_statistic WHERE batch_id = %s),
                (SELECT count(*) FROM integration.quarantine WHERE connector_id = %s),
                (SELECT count(*) FROM sync.external_outbox WHERE idempotency_key = %s),
                (SELECT canonical_revision FROM hub.canonical_state WHERE singleton = true)
            """,
            (
                accepted.receipt.batch_id,
                accepted.receipt.batch_id,
                producer.connector_id,
                f"connector-commit:{accepted.receipt.batch_id}",
            ),
        )
        counts = tuple(int(value) for value in cursor.fetchone())
    ok = counts[:4] == (1, 1, 1, 1) and counts[4] == first_commit.canonical_revision
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
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
