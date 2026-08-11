#!/usr/bin/env python3
"""Prove the live PostgreSQL synthetic connector R1 flow and exact-once commit."""

from __future__ import annotations

import argparse
import asyncio
import atexit
import json
import os
import tempfile
import time
from dataclasses import asdict
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.connectors.contracts import (
    ConnectorCheckpointRequest,
    ConnectorCheckpointState,
    ConnectorCheckpointStatusReceipt,
    ConnectorDurabilityState,
    ConnectorReceipt,
    canonical_json_bytes,
    payload_sha256,
)
from my_data_hub.connectors.durability import ConnectorDurabilityService
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
    DurabilityDeliveryResult,
    DurabilityDisposition,
    DurableConnectorSpool,
)
from my_data_hub.connectors.synthetic import SyntheticConnectorProducer


def _open_disposable_epoch(
    admin_database_url: str, *, connector_url: str, committer_url: str
) -> tuple[UUID, int]:
    """Bind the disposable CI LOGINs to one real leased write epoch."""

    import psycopg

    master_instance_id = uuid5(NAMESPACE_URL, "my-data-hub:disposable-connector-ci")
    now = datetime.now(UTC)
    lease_until = now + timedelta(minutes=10)
    credential_until = now + timedelta(minutes=5)
    principals = tuple(urlsplit(value).username for value in (connector_url, committer_url))
    if any(not principal for principal in principals) or len(set(principals)) != 2:
        raise ValueError("disposable connector epoch requires two distinct LOGIN principals")
    with psycopg.connect(admin_database_url) as connection, connection.cursor() as cursor:
        highest_epoch, current_epoch = cursor.execute(
            "SELECT highest_epoch,current_epoch FROM master_control.epoch_state WHERE singleton=true"
        ).fetchone()
        if current_epoch is not None:
            raise RuntimeError("disposable connector fixture found an already registered master epoch")
        epoch = int(highest_epoch) + 1
        cursor.execute(
            "SELECT master_control.begin_epoch(%s,%s,%s,%s)",
            (master_instance_id, "disposable-connector-ci", epoch, lease_until),
        )
        cursor.execute(
            "SELECT master_control.open_write_gate(%s,%s)",
            (master_instance_id, epoch),
        )
        for principal in principals:
            cursor.execute(
                "SELECT master_control.bind_epoch_credential(%s,%s,%s,%s,%s)",
                (
                    uuid5(NAMESPACE_URL, f"my-data-hub:disposable:{principal}:{epoch}"),
                    principal,
                    master_instance_id,
                    epoch,
                    credential_until,
                ),
            )
        connection.commit()
    return master_instance_id, epoch


def _close_disposable_epoch(admin_database_url: str, master_instance_id: UUID, epoch: int) -> None:
    import psycopg

    with psycopg.connect(admin_database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            "SELECT master_control.close_write_gate(%s,%s,'closed','disposable_ci_complete')",
            (master_instance_id, epoch),
        )
        connection.commit()


def _reader_connector_status(database_url: str) -> list[dict[str, object]]:
    """Read the bounded connector projection through the restricted reader LOGIN.

    Production MCP never receives a static PostgreSQL URL.  The disposable
    integration job still proves that the short-lived master reader identity can
    observe the exact allowlisted connector tables that a brokered MCP session
    will use.
    """

    import psycopg

    with psycopg.connect(database_url) as connection, connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT p.data_product,
                   count(b.batch_id) FILTER (WHERE b.status = 'accepted'),
                   count(b.batch_id) FILTER (WHERE b.status = 'canonical_committed'),
                   max(b.accepted_at),
                   max(b.committed_at)
            FROM integration.data_product p
            LEFT JOIN integration.batch b ON b.data_product = p.data_product
            GROUP BY p.data_product
            ORDER BY p.data_product
            """
        )
        rows = cursor.fetchall()
    return [
        {
            "data_product": str(row[0]),
            "accepted_uncommitted_batches": int(row[1]),
            "committed_batches": int(row[2]),
            "last_accepted_at": row[3].isoformat() if row[3] else None,
            "last_committed_at": row[4].isoformat() if row[4] else None,
        }
        for row in rows
    ]


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
        default=os.getenv("MY_DATA_HUB_MONITORING_DATABASE_URL", ""),
    )
    parser.add_argument(
        "--admin-database-url",
        default=os.getenv("MY_DATA_HUB_ROLE_ADMIN_DATABASE_URL", ""),
    )
    parser.add_argument("--bootstrap-disposable-epoch", action="store_true")
    parser.add_argument(
        "--bootstrap-disposable-checkpoint",
        action="store_true",
        help=(
            "complete durability with an explicit synthetic checkpoint fixture; "
            "CI-only and never live evidence"
        ),
    )
    parser.add_argument("--sequence", type=int, default=None)
    parser.add_argument("--durability-timeout-seconds", type=float, default=0.0)
    args = parser.parse_args()
    urls = {
        "intake": args.intake_database_url,
        "committer": args.committer_database_url,
        "MCP reader": args.mcp_reader_database_url,
        "monitoring verification": args.verification_database_url,
    }
    missing = [name for name, value in urls.items() if not value]
    if missing:
        raise SystemExit("missing dedicated database URL(s): " + ", ".join(missing))
    disposable_epoch: tuple[UUID, int] | None = None
    if args.bootstrap_disposable_epoch:
        if not args.admin_database_url:
            raise SystemExit("disposable epoch bootstrap requires the role-admin database URL")
        disposable_epoch = _open_disposable_epoch(
            args.admin_database_url,
            connector_url=args.intake_database_url,
            committer_url=args.committer_database_url,
        )
        atexit.register(_close_disposable_epoch, args.admin_database_url, *disposable_epoch)

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
    committer = PostgresDailyStatisticsCommitter(args.committer_database_url)

    poison = json.loads(
        producer.exact_bytes(reporting_date + timedelta(days=2), sequence=sequence + 2)
    )
    poison["inline_records"][0]["counts"]["accepted"] = -1
    poison["payload_sha256"] = payload_sha256(poison["inline_records"])
    poison_result = intake.submit(
        canonical_json_bytes(poison),
        authenticated_connector_id=producer.connector_id,
        authenticated_principal=f"service:{producer.connector_id}",
        correlation_id="r1-synthetic-semantic-poison",
    )
    if poison_result.receipt is None:
        raise SystemExit("semantic poison batch was not durably accepted for normalization")
    try:
        committer.commit(poison_result.receipt.batch_id)
    except ValueError:
        semantic_quarantine = committer.quarantine_semantic_failure(
            poison_result.receipt.batch_id
        )
        semantic_quarantine_replay = committer.quarantine_semantic_failure(
            poison_result.receipt.batch_id
        )
    else:
        raise SystemExit("semantically invalid product record unexpectedly committed")
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

    # A locked oldest row must fail within the bounded lock timeout rather than hang
    # the sole timer. Once the transient lock is gone, the same batch progresses.
    with psycopg.connect(
        args.committer_database_url, connect_timeout=3
    ) as lock_connection, lock_connection.cursor() as lock_cursor:
        lock_cursor.execute(
            "SELECT batch_id FROM integration.batch WHERE batch_id = %s FOR UPDATE",
            (accepted.receipt.batch_id,),
        )
        lock_timeout_sqlstate = None
        try:
            committer.commit(accepted.receipt.batch_id)
        except psycopg.errors.LockNotAvailable as exc:
            lock_timeout_sqlstate = exc.sqlstate
        else:
            raise SystemExit("canonical committer did not honor its bounded row-lock timeout")
        finally:
            lock_connection.rollback()
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

        def durability(self, _acceptance: ConnectorReceipt) -> DurabilityDeliveryResult:
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

        def durability(self, acceptance: ConnectorReceipt) -> DurabilityDeliveryResult:
            receipt = repository.get_durability_receipt(acceptance.batch_id)
            if receipt is None:
                return DurabilityDeliveryResult(
                    DurabilityDisposition.RETRY,
                    message="durability receipt is not visible",
                )
            disposition = (
                DurabilityDisposition.COMPLETE
                if receipt.state is ConnectorDurabilityState.DURABLE_COMPLETE
                else DurabilityDisposition.PENDING
            )
            return DurabilityDeliveryResult(disposition, receipt=receipt)

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
        accepted_files = list(restarted_spool.receipts_dir.glob("*.accepted.json"))
        eventual_acceptance = ConnectorReceipt.model_validate_json(accepted_files[0].read_bytes())
        eventual_commit = committer.commit(eventual_acceptance.batch_id)
        eventual_replay = committer.commit(eventual_acceptance.batch_id)
        if args.bootstrap_disposable_checkpoint:
            if not args.bootstrap_disposable_epoch:
                raise SystemExit(
                    "synthetic checkpoint bootstrap requires the disposable epoch fixture"
                )

            class DisposableCheckpointGateway:
                """Exact CI fixture; it is never constructed by the live verifier path."""

                def __init__(self) -> None:
                    self.request: ConnectorCheckpointRequest | None = None

                def request_checkpoint(
                    self, request: ConnectorCheckpointRequest
                ) -> ConnectorCheckpointStatusReceipt:
                    self.request = request
                    return ConnectorCheckpointStatusReceipt(
                        request_id=request.request_id,
                        operation_id=f"disposable-ci:{request.request_id}",
                        state=ConnectorCheckpointState.REQUESTED,
                        canonical_revision=request.canonical_revision,
                    )

                def checkpoint_status(
                    self, operation_id: str
                ) -> ConnectorCheckpointStatusReceipt:
                    request = self.request
                    if request is None or operation_id != f"disposable-ci:{request.request_id}":
                        raise RuntimeError("disposable checkpoint operation identity changed")
                    return ConnectorCheckpointStatusReceipt(
                        request_id=request.request_id,
                        operation_id=operation_id,
                        state=ConnectorCheckpointState.DURABLE_COMPLETE,
                        canonical_revision=request.canonical_revision,
                        checkpoint_id=f"disposable-ci:{request.canonical_revision}",
                        manifest_sha256=request.exact_sha256(),
                        verified_at=datetime.now(UTC),
                    )

            asyncio.run(
                ConnectorDurabilityService(
                    repository, DisposableCheckpointGateway()
                ).advance(eventual_acceptance.batch_id)
            )
        deadline = time.monotonic() + max(0.0, args.durability_timeout_seconds)
        durability_summary = ConnectorDeliveryService(
            restarted_spool, IntakeTransport()
        ).deliver_ready(now=outage_at + timedelta(seconds=4))
        while durability_summary.delivered != 1 and time.monotonic() < deadline:
            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))
            durability_summary = ConnectorDeliveryService(
                restarted_spool, IntakeTransport()
            ).deliver_ready(now=datetime.now(UTC) + timedelta(minutes=5))
        receipt_files = [
            path
            for path in restarted_spool.receipts_dir.glob("*.json")
            if not path.name.endswith(".accepted.json")
        ]
        eventual_durability = (
            json.loads(receipt_files[0].read_bytes()) if len(receipt_files) == 1 else None
        )
        spool_ok = (
            outage_summary.deferred == 1
            and recovery_summary.deferred == 1
            and durability_summary.delivered == 1
            and not restarted_spool.pending(ready_at=datetime.now(UTC) + timedelta(minutes=5))
            and len(receipt_files) == 1
            and eventual_durability is not None
            and eventual_durability["state"] == "DURABLE_COMPLETE"
            and not eventual_commit.duplicate
            and eventual_replay.duplicate
        )

    reader_status = _reader_connector_status(args.mcp_reader_database_url)
    reader_row = next(
        row for row in reader_status if row["data_product"] == "synthetic.daily-statistics.v1"
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
                (SELECT canonical_revision FROM hub.canonical_state WHERE singleton = true),
                (SELECT count(*) FROM integration.quarantine WHERE quarantine_id = %s)
            """,
            (
                accepted.receipt.batch_id,
                accepted.receipt.batch_id,
                conflict.quarantine.quarantine_id,
                f"connector-commit:{accepted.receipt.batch_id}",
                semantic_quarantine.quarantine_id,
            ),
        )
        counts = tuple(int(value) for value in cursor.fetchone())
    ok = (
        counts[:4] == (1, 1, 1, 1)
        and counts[4] >= first_commit.canonical_revision
        and counts[5] == 1
        and spool_ok
        and not semantic_quarantine.duplicate
        and semantic_quarantine_replay.duplicate
        and reader_row["committed_batches"] >= 2
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
            "semantic_quarantine": counts[5],
        },
        "outage_restart": {
            "checkpoint_evidence_class": (
                "synthetic_disposable_ci"
                if args.bootstrap_disposable_checkpoint
                else "live_external_gateway_required"
            ),
            "first_delivery_deferred": outage_summary.deferred,
            "acceptance_deferred": recovery_summary.deferred,
            "durable_complete_count": durability_summary.delivered,
            "durable_receipts": len(receipt_files),
            "eventual_batch_id": str(eventual_acceptance.batch_id),
            "durability_state": (
                eventual_durability["state"]
                if eventual_durability is not None
                else repository.get_durability_receipt(eventual_acceptance.batch_id).state.value
            ),
            "commit_replayed": eventual_replay.duplicate,
        },
        "restricted_master_reader": reader_row,
        "semantic_poison": {
            "batch_id": str(poison_result.receipt.batch_id),
            "quarantine_id": str(semantic_quarantine.quarantine_id),
            "terminal_replay": semantic_quarantine_replay.duplicate,
            "later_valid_batches_progressed": True,
        },
        "transient_lock": {
            "bounded_sqlstate": lock_timeout_sqlstate,
            "eventual_commit": True,
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))
    if disposable_epoch is not None:
        _close_disposable_epoch(args.admin_database_url, *disposable_epoch)
        atexit.unregister(_close_disposable_epoch)
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
