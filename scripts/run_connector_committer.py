#!/usr/bin/env python3
"""Commit a bounded batch of accepted connector payloads under the sole committer role."""

from __future__ import annotations

import argparse
import json
import os
from dataclasses import asdict

from my_data_hub.connectors.postgres import PostgresDailyStatisticsCommitter


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database-url",
        default=os.getenv("MY_DATA_HUB_CANONICAL_COMMITTER_DATABASE_URL", ""),
    )
    parser.add_argument("--limit", type=int, default=25)
    args = parser.parse_args()
    if not args.database_url:
        parser.error("canonical committer database URL is required")
    if not 1 <= args.limit <= 100:
        parser.error("--limit must be between 1 and 100")

    import psycopg

    with psycopg.connect(
        args.database_url, connect_timeout=3
    ) as connection, connection.cursor() as cursor:
        cursor.execute("SET TRANSACTION READ ONLY")
        cursor.execute("SET LOCAL statement_timeout = '2000ms'")
        cursor.execute("SET LOCAL lock_timeout = '1000ms'")
        cursor.execute("SET LOCAL transaction_timeout = '5000ms'")
        cursor.execute(
            """
            SELECT batch_id
            FROM integration.batch
            WHERE status = 'accepted'
            ORDER BY accepted_at, batch_id
            LIMIT %s
            """,
            (args.limit,),
        )
        batch_ids = [row[0] for row in cursor.fetchall()]

    committer = PostgresDailyStatisticsCommitter(args.database_url)
    receipts = []
    quarantines = []
    transient_failures = []
    for batch_id in batch_ids:
        try:
            receipts.append(committer.commit(batch_id))
        except ValueError:
            quarantines.append(committer.quarantine_semantic_failure(batch_id))
        except Exception as exc:  # keep later batches progressing; fail the unit for retry
            transient_failures.append(
                {"batch_id": str(batch_id), "error_type": type(exc).__name__}
            )
    print(
        json.dumps(
            {
                "ok": not transient_failures,
                "selected": len(batch_ids),
                "committed": sum(not item.duplicate for item in receipts),
                "semantic_quarantined": len(quarantines),
                "receipts": [
                    {
                        **asdict(item),
                        "batch_id": str(item.batch_id),
                        "outbox_id": str(item.outbox_id),
                    }
                    for item in receipts
                ],
                "quarantines": [
                    {
                        **asdict(item),
                        "batch_id": str(item.batch_id),
                        "quarantine_id": str(item.quarantine_id),
                    }
                    for item in quarantines
                ],
                "transient_failures": transient_failures,
            },
            sort_keys=True,
        )
    )
    return 0 if not transient_failures else 2


if __name__ == "__main__":
    raise SystemExit(main())
