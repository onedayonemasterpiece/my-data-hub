"""Lossless two-pass Region Talk transfer from YDB to the ACTIVE master.

The runner never writes source rows to disk and never sends them through the
devstand control plane.  A source adapter scans the five allow-listed YDB
tables and bounded pages go straight to fixed PostgreSQL functions exposed to
the short-lived ``mdh_region_talk_pipeline`` role.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
from collections import Counter
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from decimal import Decimal
from typing import Any, Protocol
from uuid import NAMESPACE_URL, UUID, uuid5

from my_data_hub.hashing import canonical_json_bytes, sha256_value
from my_data_hub.workloads.region_talk.constants import (
    DIRECT_SOURCE_TABLES,
    DirectSourceTable,
)
from my_data_hub.workloads.region_talk.contracts import (
    DirectSnapshotManifest,
    DirectSnapshotPage,
    DirectSnapshotReceipt,
    DirectSnapshotRow,
    DirectSnapshotTableReceipt,
)


class DirectSnapshotError(RuntimeError):
    """The source changed, violated its contract, or the master rejected it."""


class DirectYdbReader(Protocol):
    """Read-only keyset pagination implemented by the Kaggle pipeline adapter."""

    def scan_page(
        self,
        source_table: str,
        *,
        primary_key: str,
        after_primary_key: str | None,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]:
        """Return at most ``limit`` rows strictly after the supplied key."""


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, bool)):
        return value
    if isinstance(value, float):
        if value != value or value in {float("inf"), float("-inf")}:
            raise DirectSnapshotError("non-finite source number is not valid JSON")
        return value
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        observed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
        return observed.astimezone(UTC).isoformat().replace("+00:00", "Z")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, bytes):
        return {"$binary_base64": base64.b64encode(value).decode("ascii")}
    if isinstance(value, Mapping):
        result = {str(key): _json_value(item) for key, item in value.items()}
        # Preserve the exact source string while making JSON columns available
        # to the fixed typed mapper.  An invalid JSON string remains raw and is
        # not promoted to a typed projection.
        for key, item in tuple(result.items()):
            if key.endswith("_json") and isinstance(item, str):
                with contextlib.suppress(TypeError, ValueError):
                    result[f"{key[:-5]}_decoded"] = json.loads(item)
        return result
    if isinstance(value, (list, tuple)):
        return [_json_value(item) for item in value]
    raise DirectSnapshotError(f"unsupported source value type: {type(value).__name__}")


def _source_timestamp(payload: Mapping[str, Any]) -> datetime | None:
    for key in ("updated_at", "ingested_at", "created_at", "imported_at", "generated_at"):
        value = payload.get(key)
        if isinstance(value, datetime):
            return (value if value.tzinfo is not None else value.replace(tzinfo=UTC)).astimezone(UTC)
        if isinstance(value, str) and value.strip():
            try:
                parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
            except ValueError:
                continue
            return (parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)).astimezone(UTC)
    return None


def _row_kind(spec: DirectSourceTable, payload: Mapping[str, Any]) -> str:
    if spec.fixed_kind is not None:
        return spec.fixed_kind
    value = payload.get("kind")
    if isinstance(value, str) and value.strip():
        return value.strip()
    # Missing compact kind is quarantined later.  It is never guessed from pk.
    return "malformed_compact_kind"


def _logical_component(value: str | None) -> bytes:
    """Encode one logical-hash field without separator ambiguity.

    PostgreSQL migration 0024 implements the same UTF-8 byte-length framing.
    Keeping this deliberately simpler than JSON avoids depending on PostgreSQL
    ``jsonb`` rendering rules while still binding every required source field.
    """

    if value is None:
        return b"-1:"
    encoded = value.encode("utf-8")
    return str(len(encoded)).encode("ascii") + b":" + encoded


def _logical_timestamp(value: datetime | None) -> str | None:
    if value is None:
        return None
    observed = value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    return observed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _row_logical_bytes(row: DirectSnapshotRow) -> bytes:
    return b"".join(
        _logical_component(value)
        for value in (
            row.source_table,
            row.source_pk,
            row.row_kind,
            _logical_timestamp(row.source_updated_at),
            row.payload_sha256,
        )
    )


def _row_logical_sha256(row: DirectSnapshotRow) -> str:
    return hashlib.sha256(_row_logical_bytes(row)).hexdigest()


def source_row(
    spec: DirectSourceTable,
    value: Mapping[str, Any],
    *,
    export_batch_id: UUID | None = None,
) -> DirectSnapshotRow:
    payload = _json_value(value)
    if not isinstance(payload, dict):  # pragma: no cover - Mapping above guarantees this
        raise DirectSnapshotError("source row must be an object")
    primary_key = payload.get(spec.primary_key)
    if not isinstance(primary_key, str) or not primary_key:
        raise DirectSnapshotError(f"{spec.name} row has no non-empty {spec.primary_key}")
    payload_json = canonical_json_bytes(payload).decode("utf-8")
    payload_sha256 = hashlib.sha256(payload_json.encode("utf-8")).hexdigest()
    row = DirectSnapshotRow(
        raw_record_id=uuid5(
            NAMESPACE_URL,
            f"my-data-hub:region-talk:v2:{export_batch_id or 'inventory'}:{spec.name}:{primary_key}",
        ),
        source_table=spec.name,
        source_pk=primary_key,
        row_kind=_row_kind(spec, payload),
        source_updated_at=_source_timestamp(value),
        payload_json=payload_json,
        payload_sha256=payload_sha256,
        logical_sha256="0" * 64,
    )
    return row.model_copy(update={"logical_sha256": _row_logical_sha256(row)})


@dataclass(slots=True)
class _Accumulator:
    source_table: str
    count: int = 0
    last_pk: str | None = None
    kinds: Counter[str] | None = None
    digest: Any = None

    def __post_init__(self) -> None:
        self.kinds = Counter()
        self.digest = hashlib.sha256()

    def add(self, row: DirectSnapshotRow) -> None:
        if row.source_table != self.source_table:
            raise DirectSnapshotError("source adapter returned a row for another table")
        if self.last_pk is not None and row.source_pk <= self.last_pk:
            raise DirectSnapshotError(
                f"{self.source_table} is not strictly ordered after {self.last_pk!r}"
            )
        self.last_pk = row.source_pk
        self.count += 1
        assert self.kinds is not None
        self.kinds[row.row_kind] += 1
        self.digest.update(row.logical_sha256.encode("ascii") + b"\n")

    def receipt(self) -> DirectSnapshotTableReceipt:
        return DirectSnapshotTableReceipt(
            source_table=self.source_table,
            row_count=self.count,
            logical_sha256=self.digest.hexdigest(),
        )


def _snapshot_logical_sha256(tables: Sequence[DirectSnapshotTableReceipt]) -> str:
    digest = hashlib.sha256()
    for item in tables:
        digest.update(_logical_component(item.source_table))
        digest.update(_logical_component(str(item.row_count)))
        digest.update(_logical_component(item.logical_sha256))
    return digest.hexdigest()


class DirectSnapshotRunner:
    """Execute pass A, direct bounded landing, and exact pass-B reconciliation."""

    def __init__(
        self,
        reader: DirectYdbReader,
        connection: Any,
        *,
        page_size: int = 250,
        transport_refresh: Callable[..., Any] | None = None,
    ) -> None:
        if not 1 <= page_size <= 500:
            raise ValueError("page_size must be between 1 and 500")
        self.reader = reader
        self.connection = connection
        self.page_size = page_size
        self.transport_refresh = transport_refresh

    def _refresh(
        self,
        *,
        phase: str,
        source_table: str = "",
        after_primary_key: str | None = None,
        page_number: int = 0,
    ) -> None:
        if self.transport_refresh is None:
            return
        replacement = self.transport_refresh(
            self.connection,
            phase=phase,
            source_table=source_table,
            after_primary_key=after_primary_key,
            page_number=page_number,
        )
        if replacement is None:
            raise DirectSnapshotError("transport refresh returned no database connection")
        self.connection = replacement

    def _scan(
        self,
        spec: DirectSourceTable,
        *,
        phase: str,
        land_batch_id: UUID | None = None,
        task_run_id: UUID | None = None,
    ) -> _Accumulator:
        accumulator = _Accumulator(spec.name)
        after: str | None = None
        page_number = 0
        while True:
            self._refresh(
                phase=phase,
                source_table=spec.name,
                after_primary_key=after,
                page_number=page_number + 1,
            )
            values = self.reader.scan_page(
                spec.name,
                primary_key=spec.primary_key,
                after_primary_key=after,
                limit=self.page_size,
            )
            if len(values) > self.page_size:
                raise DirectSnapshotError("source adapter exceeded the bounded page size")
            if not values:
                break
            rows = [
                source_row(spec, value, export_batch_id=land_batch_id)
                for value in values
            ]
            page_digest = hashlib.sha256()
            for row in rows:
                accumulator.add(row)
                page_digest.update(row.logical_sha256.encode("ascii") + b"\n")
            page_number += 1
            if land_batch_id is not None:
                if task_run_id is None:  # pragma: no cover - internal invariant
                    raise AssertionError("task_run_id is required while landing")
                page = DirectSnapshotPage(
                    schema_version="region-talk-direct-page.v2",
                    source_table=spec.name,
                    page_number=page_number,
                    first_source_pk=rows[0].source_pk,
                    last_source_pk=rows[-1].source_pk,
                    logical_sha256=page_digest.hexdigest(),
                    rows=rows,
                )
                self._refresh(
                    phase="land_page",
                    source_table=spec.name,
                    after_primary_key=after,
                    page_number=page_number,
                )
                self._land_page(land_batch_id, task_run_id, page)
            after = rows[-1].source_pk
        return accumulator

    def inventory(
        self,
        *,
        export_batch_id: UUID,
        task_run_id: UUID,
        master_instance_id: UUID,
        master_epoch: int,
        source_database: str,
        request_sha256: str,
        created_at: datetime,
    ) -> DirectSnapshotManifest:
        tables: list[DirectSnapshotTableReceipt] = []
        kinds: Counter[str] = Counter()
        for spec in DIRECT_SOURCE_TABLES:
            accumulator = self._scan(spec, phase="pass_a")
            tables.append(accumulator.receipt())
            assert accumulator.kinds is not None
            kinds.update(accumulator.kinds)
        base = {
            "schema_version": "region-talk-direct-snapshot.v2",
            "export_batch_id": str(export_batch_id),
            "task_run_id": str(task_run_id),
            "master_instance_id": str(master_instance_id),
            "master_epoch": master_epoch,
            "source_database": source_database,
            "request_sha256": request_sha256,
            "logical_sha256": _snapshot_logical_sha256(tables),
            "expected_row_count": sum(item.row_count for item in tables),
            "tables": [item.model_dump(mode="json") for item in tables],
            "row_kind_counts": dict(sorted(kinds.items())),
            "publication_effects_enabled": False,
            "created_at": created_at.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }
        return DirectSnapshotManifest.model_validate(
            {**base, "manifest_sha256": sha256_value(base)}
        )

    def run(self, manifest: DirectSnapshotManifest) -> DirectSnapshotReceipt:
        self._refresh(phase="begin")
        self._begin(manifest)
        try:
            pass_b: list[DirectSnapshotTableReceipt] = []
            for expected, spec in zip(manifest.tables, DIRECT_SOURCE_TABLES, strict=True):
                accumulator = self._scan(
                    spec,
                    phase="pass_b",
                    land_batch_id=manifest.export_batch_id,
                    task_run_id=manifest.task_run_id,
                )
                observed = accumulator.receipt()
                if observed != expected:
                    raise DirectSnapshotError(
                        f"source changed between passes for {spec.name}"
                    )
                pass_b.append(observed)
            if _snapshot_logical_sha256(pass_b) != manifest.logical_sha256:
                raise DirectSnapshotError("source logical hash changed between passes")
            self._refresh(phase="finalize")
            return self._finalize(manifest, pass_b)
        except Exception as exc:
            self.connection.rollback()
            self._fail(manifest, type(exc).__name__)
            raise

    def _begin(self, manifest: DirectSnapshotManifest) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                "SELECT migration.begin_region_talk_direct_snapshot(%s::jsonb)",
                (manifest.model_dump_json(),),
            ).fetchone()
        if row is None or str(row[0]) != str(manifest.export_batch_id):
            self.connection.rollback()
            raise DirectSnapshotError("master returned a different export batch")
        self.connection.commit()

    def _land_page(self, batch_id: UUID, task_run_id: UUID, page: DirectSnapshotPage) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                "SELECT migration.land_region_talk_direct_page(%s,%s,%s::jsonb)",
                (batch_id, task_run_id, page.model_dump_json()),
            ).fetchone()
        if row is None:
            self.connection.rollback()
            raise DirectSnapshotError("master did not acknowledge direct page")
        self.connection.commit()

    def _finalize(
        self,
        manifest: DirectSnapshotManifest,
        pass_b: Sequence[DirectSnapshotTableReceipt],
    ) -> DirectSnapshotReceipt:
        request = {
            "schema_version": "region-talk-direct-pass-b.v2",
            "logical_sha256": _snapshot_logical_sha256(pass_b),
            "tables": [item.model_dump(mode="json") for item in pass_b],
        }
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            row = cursor.execute(
                "SELECT migration.finalize_region_talk_direct_snapshot(%s,%s,%s::jsonb)",
                (manifest.export_batch_id, manifest.task_run_id, json.dumps(request)),
            ).fetchone()
        if row is None:
            self.connection.rollback()
            raise DirectSnapshotError("master did not return a direct snapshot receipt")
        self.connection.commit()
        value = row[0]
        if isinstance(value, str):
            return DirectSnapshotReceipt.model_validate_json(value)
        return DirectSnapshotReceipt.model_validate(value)

    def _fail(self, manifest: DirectSnapshotManifest, error_code: str) -> None:
        with self.connection.cursor() as cursor:
            cursor.execute("SET LOCAL ROLE mdh_region_talk_pipeline")
            cursor.execute(
                "SELECT migration.fail_region_talk_direct_snapshot(%s,%s,%s)",
                (manifest.export_batch_id, manifest.task_run_id, error_code[:128]),
            ).fetchone()
        self.connection.commit()
