from __future__ import annotations

from datetime import UTC, date, datetime, time, timedelta
from typing import Any
from uuid import UUID, uuid5
from zoneinfo import ZoneInfo

from my_data_hub.connectors.contracts import ConnectorEnvelope, canonical_json_bytes, payload_sha256

SYNTHETIC_NAMESPACE = UUID("523462b4-d0f5-52f2-89b8-dbf0c45c2f93")


class SyntheticConnectorProducer:
    """Deterministic non-sensitive producer used for outage and replay proofs."""

    connector_id = "synthetic.daily-statistics"
    data_product = "synthetic.daily-statistics.v1"
    schema_version = "synthetic-daily-statistics.v1"

    def build(
        self,
        reporting_date: date,
        *,
        timezone_name: str = "UTC",
        sequence: int = 1,
        values: dict[str, int] | None = None,
    ) -> ConnectorEnvelope:
        if sequence < 0:
            raise ValueError("sequence must not be negative")
        timezone = ZoneInfo(timezone_name)
        period_start = datetime.combine(reporting_date, time.min, timezone)
        period_end = datetime.combine(reporting_date + timedelta(days=1), time.min, timezone)
        stable_identity = f"{reporting_date.isoformat()}:{timezone_name}:{sequence}:v1"
        batch_id = uuid5(SYNTHETIC_NAMESPACE, stable_identity)
        record: dict[str, Any] = {
            "counts": values or {"accepted": 3, "deferred": 1, "rejected": 0},
            "reporting_date": reporting_date.isoformat(),
            "sequence": sequence,
            "source_revision": "synthetic.v1",
            "timezone": timezone_name,
        }
        records = [record]
        return ConnectorEnvelope(
            contract_version="my-data-hub-data-connector.v1",
            connector_id=self.connector_id,
            data_product=self.data_product,
            batch_id=batch_id,
            idempotency_key=f"synthetic:{stable_identity}",
            schema_version=self.schema_version,
            produced_at=(period_end.astimezone(UTC) + timedelta(minutes=5)),
            observed_period={
                "start": period_start,
                "end": period_end,
                "timezone": timezone_name,
            },
            source_cursor={
                "partition": "daily",
                "watermark": reporting_date.isoformat(),
                "sequence": sequence,
            },
            delivery_mode="push",
            record_count=len(records),
            payload_sha256=payload_sha256(records),
            inline_records=records,
            trace={
                "producer": "my-data-hub.synthetic",
                "scenario": "connector-outage-replay",
            },
        )

    def exact_bytes(self, reporting_date: date, **kwargs: Any) -> bytes:
        envelope = self.build(reporting_date, **kwargs)
        return canonical_json_bytes(envelope.model_dump(mode="json", exclude_none=True))
