"""One-transaction bounded import orchestration inside the ACTIVE master.

Source faults are data, not transaction failures.  Every observed item is first
classified under a bounded envelope.  A bad snapshot commits only immutable raw
and terminal disposition evidence; it never advances canonical state or emits a
checkpoint request.
"""

from __future__ import annotations

import hashlib
import json
import math
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid5

from psycopg.types.json import Jsonb

from .accounting import BloggerExportAccumulator, BloggerExportReceipt
from .postgres import PostgresBloggerWriter, WriteOutcome, canonical_outcome_hash
from .schema import (
    SOURCE_DATABASE_PATH,
    SOURCE_QUERY_SHA256,
    SOURCE_SCHEMA_SHA256,
    SOURCE_TABLE,
    BloggerSourceError,
    BloggerSourceRow,
)
from .transform import BloggerDisposition, BloggerProjection, transform_row

_BATCH_NAMESPACE = UUID("fa5115d2-39c3-5eab-b849-df13bf06cbb0")
_DUPLICATE_NAMESPACE = UUID("77c173c7-73b5-562d-af57-eb86ff06cc24")
_MAX_QUARANTINE_JSON_BYTES = 128 * 1024
_MAX_QUARANTINE_VALUE_BYTES = 4096
_MAX_CONTAINER_ITEMS = 128


@dataclass(frozen=True, slots=True)
class ImportReceipt:
    export: BloggerExportReceipt
    canonical_outcome_sha256: str
    actor_count: int
    account_count: int
    duplicate_group_count: int
    replayed_count: int
    canonical_revision: int
    durability_state: str = "COMMITTED_PENDING_CHECKPOINT"

    @property
    def accounting_complete(self) -> bool:
        return (
            self.durability_state == "COMMITTED_PENDING_CHECKPOINT"
            and self.export.complete
            and self.duplicate_group_count == 0
        )

    @property
    def durable_complete(self) -> bool:
        return self.durability_state == "DURABLE_COMPLETE"


@dataclass(frozen=True, slots=True)
class _Observation:
    ordinal: int
    source_pk: str
    logical_id: str
    payload: dict[str, Any]
    payload_bytes: bytes
    payload_sha256: str
    row: BloggerSourceRow | None
    projection: BloggerProjection | None
    reason_code: str | None


def batch_identity(snapshot_at: datetime, expected_count: int) -> UUID:
    if snapshot_at.tzinfo is None or expected_count < 0:
        raise ValueError("snapshot identity is invalid")
    return uuid5(
        _BATCH_NAMESPACE,
        f"{SOURCE_DATABASE_PATH}\0{SOURCE_TABLE}\0{SOURCE_QUERY_SHA256}\0"
        f"{snapshot_at.isoformat()}\0{expected_count}",
    )


def _manifest_hash(batch_id: UUID, snapshot_at: datetime, expected_count: int) -> str:
    value = {
        "batch_id": str(batch_id),
        "consistency": "QuerySnapshotReadOnly",
        "expected_count": expected_count,
        "query_sha256": SOURCE_QUERY_SHA256,
        "schema_sha256": SOURCE_SCHEMA_SHA256,
        "snapshot_at": snapshot_at.isoformat(),
        "sort_key": "record_id",
        "source_database": SOURCE_DATABASE_PATH,
        "source_table": SOURCE_TABLE,
    }
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def _bounded_text(value: str) -> str | dict[str, Any]:
    encoded = value.encode("utf-8", errors="replace")
    if len(encoded) <= _MAX_QUARANTINE_VALUE_BYTES:
        return value
    prefix = encoded[:_MAX_QUARANTINE_VALUE_BYTES].decode("utf-8", errors="ignore")
    return {
        "truncated": True,
        "utf8_bytes": len(encoded),
        "sha256": hashlib.sha256(encoded).hexdigest(),
        "prefix": prefix,
    }


def _bounded_value(value: Any, *, depth: int = 0) -> Any:
    if depth > 4:
        return {"truncated": True, "reason": "maximum_depth", "type": type(value).__name__}
    if value is None or isinstance(value, bool):
        return value
    if isinstance(value, int):
        encoded = str(value).encode()
        return value if len(encoded) <= 100 else {
            "type": "integer",
            "decimal_digits": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
        }
    if isinstance(value, float):
        return value if math.isfinite(value) else {"type": "float", "value": repr(value)}
    if isinstance(value, datetime):
        return value.astimezone(UTC).isoformat() if value.tzinfo else value.isoformat()
    if isinstance(value, str):
        return _bounded_text(value)
    if isinstance(value, bytes):
        return {
            "type": "bytes",
            "byte_length": len(value),
            "sha256": hashlib.sha256(value).hexdigest(),
            "prefix_hex": value[:256].hex(),
        }
    if isinstance(value, Mapping):
        items = sorted(value.items(), key=lambda item: str(item[0]))
        result: dict[str, Any] = {}
        for key, child in items[:_MAX_CONTAINER_ITEMS]:
            bounded_key = str(_bounded_text(str(key)))[:512]
            result[bounded_key] = _bounded_value(child, depth=depth + 1)
        if len(items) > _MAX_CONTAINER_ITEMS:
            result["__truncated_items__"] = len(items) - _MAX_CONTAINER_ITEMS
        return result
    if isinstance(value, (list, tuple)):
        result = [_bounded_value(child, depth=depth + 1) for child in value[:_MAX_CONTAINER_ITEMS]]
        if len(value) > _MAX_CONTAINER_ITEMS:
            result.append({"truncated_items": len(value) - _MAX_CONTAINER_ITEMS})
        return result
    return {"unsupported_type": type(value).__name__, "repr": _bounded_text(repr(value))}


def _bounded_payload(raw: Any, reason_code: str) -> tuple[dict[str, Any], bytes, str]:
    evidence = {
        "schema_version": "region-talk-blogger-quarantine-evidence.v1",
        "reason_code": reason_code,
        "observed_type": type(raw).__name__,
        "source": _bounded_value(raw),
    }
    encoded = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode()
    if len(encoded) > _MAX_QUARANTINE_JSON_BYTES:
        # The hash binds the first bounded representation while the stored JSON
        # stays safely below PostgreSQL/control limits.
        evidence = {
            "schema_version": "region-talk-blogger-quarantine-evidence.v1",
            "reason_code": reason_code,
            "observed_type": type(raw).__name__,
            "representation_sha256": hashlib.sha256(encoded).hexdigest(),
            "representation_bytes": len(encoded),
            "truncated": True,
        }
        encoded = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
    return evidence, encoded, hashlib.sha256(encoded).hexdigest()


def _reason_for_source_error(error: BloggerSourceError) -> str:
    message = str(error)
    if "unknown=" in message and "unknown=[]" not in message:
        return "unknown_source_value"
    if "missing=" in message or " is required" in message or " is empty" in message:
        return "missing_source_value"
    if "exceeds" in message or "bounded serialized size" in message:
        return "oversized_source_value"
    return "malformed_source_value"


def _observe(raw: Any, ordinal: int) -> _Observation:
    try:
        if not isinstance(raw, Mapping):
            raise BloggerSourceError("source row must be a mapping")
        row = BloggerSourceRow.from_mapping(dict(raw))
    except BloggerSourceError as error:
        reason = _reason_for_source_error(error)
        payload, encoded, digest = _bounded_payload(raw, reason)
        logical_id = f"invalid:{ordinal}:{digest}"
        return _Observation(ordinal, logical_id, logical_id, payload, encoded, digest, None, None, reason)
    payload = row.payload()
    encoded = row.canonical_bytes()
    return _Observation(
        ordinal,
        row.record_id,
        row.record_id,
        payload,
        encoded,
        row.payload_sha256,
        row,
        transform_row(row),
        None,
    )


def _blocked_hash(items: list[tuple[_Observation, BloggerDisposition, str]]) -> str:
    value = [
        {
            "ordinal": observation.ordinal,
            "record_id": observation.logical_id,
            "payload_sha256": observation.payload_sha256,
            "disposition": disposition.value,
        }
        for observation, disposition, _reason in items
    ]
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


class BloggerSnapshotImporter:
    """Import one bounded snapshot, durably preserving terminal fault evidence."""

    def __init__(self, writer: PostgresBloggerWriter | None = None) -> None:
        self.writer = writer or PostgresBloggerWriter()

    def import_rows(
        self,
        connection: Any,
        *,
        project_id: UUID,
        snapshot_at: datetime,
        expected_row_count: int,
        rows: Iterable[dict[str, object]],
        source_code_revision: str,
    ) -> ImportReceipt:
        batch_id = batch_identity(snapshot_at, expected_row_count)
        manifest_sha = _manifest_hash(batch_id, snapshot_at, expected_row_count)
        observations = [_observe(raw, ordinal) for ordinal, raw in enumerate(rows)]
        id_counts = Counter(item.logical_id for item in observations if item.row is not None)
        duplicate_input_ids = {key for key, count in id_counts.items() if count > 1}
        outcomes: list[WriteOutcome] = []
        terminal: list[tuple[_Observation, BloggerDisposition, str]] = []
        duplicate_groups: dict[str, list[_Observation]] = defaultdict(list)
        durability_state = "BLOCKED_QUARANTINE"
        try:
            with connection.transaction(), connection.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO migration.export_batch(
                        export_batch_id,source_system,source_database,source_tables,source_scope,
                        schema_version,source_revision,source_code_revision,consistency_mode,
                        watermark_start,watermark_end,expected_row_count,manifest_sha256,status,metadata
                    ) VALUES (%s,'ydb',%s,%s,'region-talk-bloggers-v1',%s,%s,%s,
                              'QuerySnapshotReadOnly',%s,%s,%s,%s,'landing',%s)
                    ON CONFLICT (export_batch_id) DO NOTHING
                    """,
                    (
                        batch_id,
                        SOURCE_DATABASE_PATH,
                        Jsonb([SOURCE_TABLE]),
                        SOURCE_SCHEMA_SHA256,
                        snapshot_at.isoformat(),
                        source_code_revision,
                        snapshot_at,
                        snapshot_at,
                        expected_row_count,
                        manifest_sha,
                        Jsonb({"query_sha256": SOURCE_QUERY_SHA256, "sort_key": "record_id"}),
                    ),
                )
                observed_batch = cursor.execute(
                    "SELECT manifest_sha256,expected_row_count,status FROM migration.export_batch "
                    "WHERE export_batch_id=%s",
                    (batch_id,),
                ).fetchone()
                if observed_batch is None or observed_batch[:2] != (manifest_sha, expected_row_count):
                    raise ValueError("export batch idempotency conflict")
                prior_status = observed_batch[2]
                cursor.execute(
                    """
                    INSERT INTO migration.export_batch_kind(export_batch_id,row_kind,expected_row_count)
                    VALUES (%s,'region_talk_external_blogger_evidence',%s)
                    ON CONFLICT DO NOTHING
                    """,
                    (batch_id, expected_row_count),
                )

                states: dict[int, tuple[UUID, str, BloggerDisposition | None] | None] = {}
                different_payload: set[int] = set()
                for observation in observations:
                    if observation.row is None:
                        continue
                    state = self.writer.raw_state(
                        cursor, export_batch_id=batch_id, source_pk=observation.source_pk
                    )
                    states[observation.ordinal] = state
                    if state is not None and state[1] != observation.payload_sha256:
                        different_payload.add(observation.ordinal)

                typed = [item for item in observations if item.row is not None]
                input_clean = (
                    len(observations) == expected_row_count
                    and len(typed) == expected_row_count
                    and not duplicate_input_ids
                    and not different_payload
                )
                all_new = input_clean and all(states.get(item.ordinal) is None for item in typed)
                all_replay = input_clean and all(states.get(item.ordinal) is not None for item in typed)

                if all_new:
                    account_claims: dict[tuple[str, str], list[_Observation]] = defaultdict(list)
                    for item in typed:
                        assert item.projection is not None
                        for account in item.projection.accounts:
                            account_claims[(account.platform, account.normalized_url)].append(item)
                    for identity, claimants in account_claims.items():
                        existing_account = cursor.execute(
                            "SELECT actor_id FROM hub.external_account WHERE platform=%s AND normalized_url=%s",
                            identity,
                        ).fetchone()
                        actor_ids = {item.projection.actor_id for item in claimants if item.projection}
                        if existing_account is not None:
                            actor_ids.add(existing_account[0])
                        if len(actor_ids) > 1:
                            identity_hash = hashlib.sha256(f"{identity[0]}\0{identity[1]}".encode()).hexdigest()
                            duplicate_groups[identity_hash].extend(claimants)
                    if duplicate_groups:
                        all_new = False

                can_commit = all_new or (all_replay and prior_status == "accepted")
                if can_commit:
                    for item in typed:
                        assert item.row is not None and item.projection is not None
                        outcome = self.writer.write_row(
                            cursor,
                            export_batch_id=batch_id,
                            project_id=project_id,
                            row=item.row,
                            projection=item.projection,
                        )
                        outcomes.append(outcome)
                        terminal.append((item, outcome.disposition, item.projection.reason_code))
                    replayed_count = sum(item.replayed for item in outcomes)
                    if replayed_count not in {0, expected_row_count}:
                        raise ValueError("batch is partially replayed; exact all-or-nothing import required")
                    if replayed_count == expected_row_count:
                        stored = cursor.execute(
                            "SELECT metadata->>'canonical_revision', metadata->>'canonical_outcome_sha256' "
                            "FROM migration.export_batch WHERE export_batch_id=%s",
                            (batch_id,),
                        ).fetchone()
                        if stored is None or None in stored:
                            raise ValueError("replayed batch lacks canonical revision receipt")
                        canonical_revision, canonical_hash = int(stored[0]), stored[1]
                    else:
                        canonical_hash = canonical_outcome_hash(outcomes)
                        previous_revision = cursor.execute(
                            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
                        ).fetchone()[0]
                        canonical_revision = cursor.execute(
                            "SELECT hub.advance_canonical_revision(%s)", (previous_revision,)
                        ).fetchone()[0]
                        cursor.execute(
                            """
                            INSERT INTO sync.external_outbox(
                                aggregate_type,aggregate_id,effect_kind,idempotency_key,payload,required_revision
                            ) VALUES ('blogger_import',%s,'verified_checkpoint_required',%s,%s,%s)
                            """,
                            (
                                batch_id,
                                f"blogger-import-checkpoint:{batch_id}:{canonical_revision}",
                                Jsonb(
                                    {
                                        "export_batch_id": str(batch_id),
                                        "durability_state": "COMMITTED_PENDING_CHECKPOINT",
                                    }
                                ),
                                canonical_revision,
                            ),
                        )
                        cursor.execute(
                            """
                            INSERT INTO sync.audit_event(
                                actor_id,client_id,action,outcome,subject_type,subject_id,details
                            ) VALUES ('migration-operator','region-talk-ydb-bloggers-v1',
                                      'blogger_import_commit','pending_checkpoint','export_batch',%s,%s)
                            """,
                            (
                                batch_id,
                                Jsonb(
                                    {
                                        "canonical_revision": canonical_revision,
                                        "row_count": expected_row_count,
                                    }
                                ),
                            ),
                        )
                    durability_state = "COMMITTED_PENDING_CHECKPOINT"
                else:
                    global_reason = "source_accounting_mismatch"
                    if duplicate_groups:
                        global_reason = "duplicate_account_quarantined"
                    elif different_payload:
                        global_reason = "same_source_key_different_payload"
                    elif prior_status == "rejected":
                        global_reason = "previously_quarantined_batch"
                    elif any(item.reason_code for item in observations):
                        global_reason = "malformed_source_snapshot"
                    elif all(states.get(item.ordinal) is not None for item in typed) and typed:
                        global_reason = "non_accepted_replay"
                    elif any(states.get(item.ordinal) is not None for item in typed):
                        global_reason = "partial_replay"

                    seen_in_call: Counter[str] = Counter()
                    for item in observations:
                        seen_in_call[item.logical_id] += 1
                        if item.row is None:
                            source_pk = item.source_pk
                            disposition = BloggerDisposition.QUARANTINED
                            reason = item.reason_code or "malformed_source_value"
                            target_refs: list[dict[str, str]] = []
                        elif item.logical_id in duplicate_input_ids:
                            occurrence = seen_in_call[item.logical_id]
                            state = states.get(item.ordinal)
                            if occurrence == 1 and state is not None and state[1] == item.payload_sha256:
                                if state[2] is None:
                                    raise ValueError("existing raw row lacks terminal disposition")
                                terminal.append(
                                    (item, state[2], "exact_replay_existing_disposition")
                                )
                                continue
                            if state is not None and state[1] != item.payload_sha256:
                                suffix = (
                                    f"#conflict:{item.payload_sha256}"
                                    if occurrence == 1
                                    else f"#conflict:{item.payload_sha256}:{item.ordinal}"
                                )
                                reason = "same_source_key_different_payload"
                                target_refs = [
                                    {"table": "migration.raw_record", "id": str(state[0])}
                                ]
                            else:
                                suffix = (
                                    ""
                                    if occurrence == 1
                                    else f"#duplicate:{item.ordinal}:{item.payload_sha256}"
                                )
                                reason = "duplicate_source_record_id"
                                target_refs = []
                            source_pk = f"{item.source_pk}{suffix}"
                            disposition = BloggerDisposition.QUARANTINED
                        elif item.ordinal in different_payload:
                            source_pk = f"{item.source_pk}#conflict:{item.payload_sha256}"
                            disposition = BloggerDisposition.QUARANTINED
                            reason = "same_source_key_different_payload"
                            original = states[item.ordinal]
                            target_refs = (
                                [{"table": "migration.raw_record", "id": str(original[0])}]
                                if original
                                else []
                            )
                        else:
                            state = states.get(item.ordinal)
                            if state is not None:
                                if state[2] is None:
                                    raise ValueError("existing raw row lacks terminal disposition")
                                disposition = state[2]
                                reason = "exact_replay_existing_disposition"
                                terminal.append((item, disposition, reason))
                                continue
                            source_pk = item.source_pk
                            disposition = BloggerDisposition.RETAINED_RAW
                            reason = global_reason
                            target_refs = []
                        self.writer.retain_observation(
                            cursor,
                            export_batch_id=batch_id,
                            source_pk=source_pk,
                            payload=item.payload,
                            payload_sha256=item.payload_sha256,
                            disposition=disposition,
                            reason_code=reason,
                            source_updated_at=item.row.updated_at if item.row else None,
                            target_refs=target_refs,
                        )
                        terminal.append((item, disposition, reason))

                    for identity_hash, members in duplicate_groups.items():
                        group_id = uuid5(_DUPLICATE_NAMESPACE, f"{batch_id}:{identity_hash}")
                        cursor.execute(
                            """
                            INSERT INTO migration.duplicate_group(
                                duplicate_group_id,export_batch_id,identity_kind,identity_hash,
                                decision_status,reason,decided_by,decided_at
                            ) VALUES (%s,%s,'account_url',%s,'quarantined',
                                      'conflicting account ownership requires explicit operator review',
                                      'region-talk-bloggers.v1',clock_timestamp())
                            ON CONFLICT (duplicate_group_id) DO NOTHING
                            """,
                            (group_id, batch_id, identity_hash),
                        )
                        for member in members:
                            state = self.writer.raw_state(
                                cursor, export_batch_id=batch_id, source_pk=member.source_pk
                            )
                            if state:
                                cursor.execute(
                                    """
                                    INSERT INTO migration.duplicate_group_member(
                                        duplicate_group_id,raw_record_id,actor_id,evidence
                                    ) VALUES (%s,%s,NULL,%s) ON CONFLICT DO NOTHING
                                    """,
                                    (group_id, state[0], Jsonb({"identity_sha256": identity_hash})),
                                )
                    canonical_revision = cursor.execute(
                        "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
                    ).fetchone()[0]
                    canonical_hash = _blocked_hash(terminal)
                    # A blocked receipt describes durable evidence, not delivery
                    # mechanics.  Keeping this stable makes an exact fault replay
                    # the same receipt/no-op.
                    replayed_count = 0

                accumulator = BloggerExportAccumulator(batch_id, snapshot_at)
                for item, disposition, _reason in terminal:
                    accumulator.add_evidence(
                        record_id=item.logical_id,
                        canonical_bytes=item.payload_bytes,
                        disposition=disposition,
                        source_file_sha256=item.row.source_file_sha256 if item.row else None,
                    )
                export = accumulator.finish(
                    expected_row_count=expected_row_count, allow_incomplete=not can_commit
                )
                accounting = cursor.execute(
                    """
                    SELECT raw_count,undispositioned_count,quarantined_count
                    FROM migration.batch_accounting WHERE export_batch_id=%s
                    """,
                    (batch_id,),
                ).fetchone()
                duplicate_count = cursor.execute(
                    "SELECT count(*) FROM migration.duplicate_group WHERE export_batch_id=%s",
                    (batch_id,),
                ).fetchone()[0]
                if can_commit and accounting != (expected_row_count, 0, 0):
                    raise ValueError(f"canonical accounting failed: {accounting!r}")
                if can_commit and duplicate_count:
                    raise ValueError("duplicate decision accounting is not empty")
                actor_count = cursor.execute(
                    "SELECT count(*) FROM region_talk.blogger_profile WHERE export_batch_id=%s",
                    (batch_id,),
                ).fetchone()[0]
                account_count = cursor.execute(
                    """
                    SELECT count(*) FROM hub.external_account account
                    JOIN region_talk.blogger_profile profile ON profile.actor_id=account.actor_id
                    WHERE profile.export_batch_id=%s
                    """,
                    (batch_id,),
                ).fetchone()[0]
                cursor.execute(
                    """
                    UPDATE migration.export_batch
                    SET logical_sha256=%s,status=%s,completed_at=clock_timestamp(),metadata=metadata || %s
                    WHERE export_batch_id=%s
                    """,
                    (
                        export.logical_sha256,
                        "accepted" if can_commit else "rejected",
                        Jsonb(
                            {
                                "record_id_set_sha256": export.record_id_set_sha256,
                                "canonical_outcome_sha256": canonical_hash,
                                "duplicate_groups": duplicate_count,
                                "canonical_revision": canonical_revision,
                                "observed_row_count": export.row_count,
                                "undispositioned": export.undispositioned,
                                "quarantined": export.dispositions.get("quarantined", 0),
                                "durability_state": durability_state,
                            }
                        ),
                        batch_id,
                    ),
                )
        except Exception:
            connection.rollback()
            raise
        return ImportReceipt(
            export=export,
            canonical_outcome_sha256=canonical_hash,
            actor_count=actor_count,
            account_count=account_count,
            duplicate_group_count=duplicate_count,
            replayed_count=replayed_count,
            canonical_revision=canonical_revision,
            durability_state=durability_state,
        )
