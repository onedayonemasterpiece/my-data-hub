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
_REPLAY_NAMESPACE = UUID("f99ec159-bc7c-5d13-90e5-e535a5c7bb37")
_RESOLUTION_NAMESPACE = UUID("68d8fbd7-a7ad-5acc-a44f-39fc17834a93")
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
    duplicate_groups_pending: int = 0
    durability_state: str = "COMMITTED_PENDING_CHECKPOINT"
    duplicate_review_groups: tuple[DuplicateReviewGroup, ...] = ()

    @property
    def accounting_complete(self) -> bool:
        return (
            self.durability_state == "COMMITTED_PENDING_CHECKPOINT"
            and self.export.complete
            and self.duplicate_groups_pending == 0
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


@dataclass(frozen=True, slots=True)
class DuplicateReviewMember:
    record_id: str
    projected_actor_id: UUID


@dataclass(frozen=True, slots=True)
class DuplicateReviewGroup:
    """Metadata-only facts for owner review; never an owner decision."""

    identity_sha256: str
    members: tuple[DuplicateReviewMember, ...]
    existing_actor_id: UUID | None

    @property
    def member_record_ids(self) -> tuple[str, ...]:
        return tuple(item.record_id for item in self.members)


@dataclass(frozen=True, slots=True)
class DuplicateResolution:
    """One explicit same-person decision supplied only inside the ACTIVE master."""

    identity_sha256: str
    canonical_record_id: str
    canonical_actor_id: UUID
    member_record_ids: tuple[str, ...]
    decided_by: str
    reason: str

    def __post_init__(self) -> None:
        if len(self.identity_sha256) != 64 or any(
            char not in "0123456789abcdef" for char in self.identity_sha256
        ):
            raise ValueError("duplicate identity_sha256 must be lowercase SHA-256")
        if not self.canonical_record_id or len(self.canonical_record_id.encode()) > 4096:
            raise ValueError("canonical_record_id is invalid")
        if (
            not self.member_record_ids
            or tuple(sorted(set(self.member_record_ids))) != self.member_record_ids
            or self.canonical_record_id not in self.member_record_ids
        ):
            raise ValueError("duplicate member_record_ids must be sorted, unique, and include canonical")
        if not self.decided_by.strip() or len(self.decided_by.encode()) > 512:
            raise ValueError("duplicate decided_by is invalid")
        if not self.reason.strip() or len(self.reason.encode()) > 4096:
            raise ValueError("duplicate reason is invalid")

    @property
    def member_record_id_set_sha256(self) -> str:
        return _string_set_hash(self.member_record_ids)

    @property
    def resolution_sha256(self) -> str:
        return hashlib.sha256(
            json.dumps(
                {
                    "canonical_actor_id": str(self.canonical_actor_id),
                    "canonical_record_id": self.canonical_record_id,
                    "decided_by": self.decided_by,
                    "identity_sha256": self.identity_sha256,
                    "member_record_ids": self.member_record_ids,
                    "reason": self.reason,
                    "schema_version": "region-talk-blogger-duplicate-resolution.v1",
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        ).hexdigest()


@dataclass(frozen=True, slots=True)
class _DuplicateClaim:
    identity_hash: str
    members: tuple[_Observation, ...]
    existing_actor_id: UUID | None


@dataclass(frozen=True, slots=True)
class _ResolutionPlan:
    targets: dict[str, UUID]
    canonical_records: dict[UUID, str]
    resolutions: tuple[DuplicateResolution, ...]


class DuplicateResolutionConflict(ValueError):
    """Resolution input is partial, stale, or would silently merge identities."""


def _string_set_hash(values: Iterable[str]) -> str:
    digest = hashlib.sha256()
    for value in sorted(set(values)):
        encoded = value.encode("utf-8")
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
    return digest.hexdigest()


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
        list_result = [_bounded_value(child, depth=depth + 1) for child in value[:_MAX_CONTAINER_ITEMS]]
        if len(value) > _MAX_CONTAINER_ITEMS:
            list_result.append({"truncated_items": len(value) - _MAX_CONTAINER_ITEMS})
        return list_result
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


def _resolution_set_hash(resolutions: Iterable[DuplicateResolution]) -> str:
    return _string_set_hash(item.resolution_sha256 for item in resolutions)


def _build_resolution_plan(
    claims: Mapping[str, _DuplicateClaim],
    resolutions: Iterable[DuplicateResolution],
) -> _ResolutionPlan:
    """Validate a complete decision set and return exact per-source targets.

    Every observed shared identity must have exactly one decision. Connected
    identity groups must select the same actor and canonical source row. This
    forbids a resolution set from implicitly splitting or merging a person.
    """

    supplied: dict[str, DuplicateResolution] = {}
    for resolution in resolutions:
        if resolution.identity_sha256 in supplied:
            raise DuplicateResolutionConflict("duplicate resolution identity")
        supplied[resolution.identity_sha256] = resolution
    if set(supplied) != set(claims):
        raise DuplicateResolutionConflict("duplicate resolution set is incomplete or stale")

    targets: dict[str, UUID] = {}
    canonical_records: dict[UUID, str] = {}
    by_record = {
        member.logical_id: member
        for claim in claims.values()
        for member in claim.members
    }
    for identity_hash in sorted(claims):
        claim = claims[identity_hash]
        resolution = supplied[identity_hash]
        member_ids = tuple(sorted({member.logical_id for member in claim.members}))
        if resolution.member_record_ids != member_ids:
            raise DuplicateResolutionConflict("duplicate resolution members differ from durable claim")
        canonical = by_record.get(resolution.canonical_record_id)
        if canonical is None or canonical.projection is None:
            raise DuplicateResolutionConflict("duplicate canonical record is absent")
        if claim.existing_actor_id is not None:
            if resolution.canonical_actor_id != claim.existing_actor_id:
                raise DuplicateResolutionConflict("existing account owner was not selected explicitly")
        elif resolution.canonical_actor_id != canonical.projection.actor_id:
            raise DuplicateResolutionConflict("new canonical actor does not match canonical source identity")
        prior_canonical = canonical_records.setdefault(
            resolution.canonical_actor_id, resolution.canonical_record_id
        )
        if prior_canonical != resolution.canonical_record_id:
            raise DuplicateResolutionConflict("connected duplicate groups select different canonical rows")
        for member_id in member_ids:
            prior_target = targets.setdefault(member_id, resolution.canonical_actor_id)
            if prior_target != resolution.canonical_actor_id:
                raise DuplicateResolutionConflict("connected duplicate groups select different actors")
    return _ResolutionPlan(targets, canonical_records, tuple(supplied[key] for key in sorted(supplied)))


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
        duplicate_resolutions: Iterable[DuplicateResolution] = (),
    ) -> ImportReceipt:
        batch_id = batch_identity(snapshot_at, expected_row_count)
        manifest_sha = _manifest_hash(batch_id, snapshot_at, expected_row_count)
        observations = [_observe(raw, ordinal) for ordinal, raw in enumerate(rows)]
        id_counts = Counter(item.logical_id for item in observations if item.row is not None)
        duplicate_input_ids = {key for key, count in id_counts.items() if count > 1}
        outcomes: list[WriteOutcome] = []
        terminal: list[tuple[_Observation, BloggerDisposition, str]] = []
        duplicate_groups: dict[str, _DuplicateClaim] = {}
        supplied_resolutions = tuple(duplicate_resolutions)
        resolution_plan: _ResolutionPlan | None = None
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

                if input_clean:
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
                            duplicate_groups[identity_hash] = _DuplicateClaim(
                                identity_hash=identity_hash,
                                members=tuple(
                                    sorted(
                                        {item.logical_id: item for item in claimants}.values(),
                                        key=lambda item: item.logical_id,
                                    )
                                ),
                                existing_actor_id=existing_account[0] if existing_account else None,
                            )
                if duplicate_groups and all_new:
                    all_new = False

                resolving_replay = bool(
                    duplicate_groups
                    and all_replay
                    and prior_status == "rejected"
                    and supplied_resolutions
                )
                if resolving_replay:
                    try:
                        resolution_plan = _build_resolution_plan(
                            duplicate_groups, supplied_resolutions
                        )
                        for claim in duplicate_groups.values():
                            if claim.existing_actor_id is not None and cursor.execute(
                                "SELECT 1 FROM region_talk.blogger_profile WHERE actor_id=%s",
                                (claim.existing_actor_id,),
                            ).fetchone() is None:
                                raise DuplicateResolutionConflict(
                                    "existing account owner is not a canonical blogger target"
                                )
                        by_id = {item.logical_id: item for item in typed}
                        for record_id, target_actor_id in resolution_plan.targets.items():
                            item = by_id[record_id]
                            assert item.projection is not None
                            for account in item.projection.accounts:
                                owner = cursor.execute(
                                    "SELECT actor_id FROM hub.external_account "
                                    "WHERE platform=%s AND normalized_url=%s",
                                    (account.platform, account.normalized_url),
                                ).fetchone()
                                if owner is not None and owner[0] != target_actor_id:
                                    raise DuplicateResolutionConflict(
                                        "resolved account owner changed after quarantine"
                                    )
                    except DuplicateResolutionConflict:
                        resolution_plan = None

                can_commit = (
                    all_new
                    or (all_replay and prior_status == "accepted")
                    or resolution_plan is not None
                )
                if can_commit:
                    if resolution_plan is not None:
                        by_id = {item.logical_id: item for item in typed}
                        target_by_record = {
                            item.logical_id: resolution_plan.targets.get(
                                item.logical_id, item.projection.actor_id  # type: ignore[union-attr]
                            )
                            for item in typed
                        }
                        canonical_by_target = dict(resolution_plan.canonical_records)
                        for item in typed:
                            assert item.projection is not None
                            canonical_by_target.setdefault(item.projection.actor_id, item.logical_id)
                        group_ids_by_record: dict[str, list[UUID]] = defaultdict(list)
                        for identity_hash, claim in duplicate_groups.items():
                            group_id = uuid5(_DUPLICATE_NAMESPACE, f"{batch_id}:{identity_hash}")
                            for member in claim.members:
                                group_ids_by_record[member.logical_id].append(group_id)

                        planned: list[WriteOutcome] = []
                        for item in typed:
                            assert item.projection is not None
                            target = target_by_record[item.logical_id]
                            canonical_record = canonical_by_target[target]
                            disposition = (
                                BloggerDisposition.DEDUPLICATED
                                if target != item.projection.actor_id
                                or canonical_record != item.logical_id
                                else item.projection.disposition
                            )
                            planned.append(
                                WriteOutcome(
                                    item.logical_id,
                                    target,
                                    disposition,
                                    True,
                                    tuple(sorted(group_ids_by_record[item.logical_id], key=str)),
                                )
                            )
                        canonical_hash = canonical_outcome_hash(planned)
                        target_ids = sorted({item.actor_id for item in planned}, key=str)
                        existing_accounts = {
                            (row[0], row[1], row[2])
                            for row in cursor.execute(
                                "SELECT actor_id,platform,normalized_url FROM hub.external_account "
                                "WHERE actor_id = ANY(%s)",
                                (target_ids,),
                            ).fetchall()
                        }
                        projected_accounts = {
                            (
                                target_by_record[item.logical_id],
                                account.platform,
                                account.normalized_url,
                            )
                            for item in typed
                            for account in item.projection.accounts  # type: ignore[union-attr]
                        }
                        account_count = len(existing_accounts | projected_accounts)
                        actor_count = len(target_ids)
                        previous_revision = cursor.execute(
                            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
                        ).fetchone()[0]
                        canonical_revision = cursor.execute(
                            "SELECT hub.advance_canonical_revision(%s)", (previous_revision,)
                        ).fetchone()[0]
                        replay_id = uuid5(_REPLAY_NAMESPACE, str(batch_id))
                        cursor.execute(
                            """
                            INSERT INTO migration.blogger_replay(
                                blogger_replay_id,export_batch_id,resolution_set_sha256,
                                canonical_outcome_sha256,canonical_revision,actor_count,
                                account_count,replayed_row_count
                            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                replay_id,
                                batch_id,
                                _resolution_set_hash(resolution_plan.resolutions),
                                canonical_hash,
                                canonical_revision,
                                actor_count,
                                account_count,
                                expected_row_count,
                            ),
                        )
                        existing_profiles = {
                            row[0]
                            for row in cursor.execute(
                                "SELECT actor_id FROM region_talk.blogger_profile "
                                "WHERE actor_id = ANY(%s)",
                                (target_ids,),
                            ).fetchall()
                        }
                        ordered = sorted(
                            typed,
                            key=lambda item: (
                                str(target_by_record[item.logical_id]),
                                item.logical_id != canonical_by_target[target_by_record[item.logical_id]],
                                item.logical_id,
                            ),
                        )
                        planned_by_record = {item.record_id: item for item in planned}
                        created_profiles: set[UUID] = set(existing_profiles)
                        for item in ordered:
                            assert item.row is not None and item.projection is not None
                            planned_outcome = planned_by_record[item.logical_id]
                            target = planned_outcome.actor_id
                            canonical_item = by_id[canonical_by_target[target]]
                            assert canonical_item.projection is not None
                            create_profile = (
                                target not in created_profiles
                                and item.logical_id == canonical_item.logical_id
                            )
                            outcome = self.writer.write_resolved_row(
                                cursor,
                                export_batch_id=batch_id,
                                project_id=project_id,
                                replay_id=replay_id,
                                row=item.row,
                                projection=item.projection,
                                canonical_actor_id=target,
                                canonical_projection=canonical_item.projection,
                                disposition=planned_outcome.disposition,
                                create_canonical_profile=create_profile,
                                duplicate_group_ids=planned_outcome.duplicate_group_ids,
                            )
                            if create_profile:
                                created_profiles.add(target)
                            outcomes.append(outcome)
                            terminal.append(
                                (item, outcome.disposition, "explicit_duplicate_resolution")
                            )
                        for resolution in resolution_plan.resolutions:
                            group_id = uuid5(
                                _DUPLICATE_NAMESPACE,
                                f"{batch_id}:{resolution.identity_sha256}",
                            )
                            cursor.execute(
                                """
                                INSERT INTO migration.blogger_duplicate_resolution(
                                    duplicate_resolution_id,blogger_replay_id,duplicate_group_id,
                                    canonical_source_pk,canonical_actor_id,
                                    member_record_id_set_sha256,resolution_sha256,
                                    decision_kind,reason,decided_by
                                ) VALUES (%s,%s,%s,%s,%s,%s,%s,'same_actor',%s,%s)
                                """,
                                (
                                    uuid5(_RESOLUTION_NAMESPACE, f"{batch_id}:{resolution.identity_sha256}"),
                                    replay_id,
                                    group_id,
                                    resolution.canonical_record_id,
                                    resolution.canonical_actor_id,
                                    resolution.member_record_id_set_sha256,
                                    resolution.resolution_sha256,
                                    resolution.reason,
                                    resolution.decided_by,
                                ),
                            )
                        replayed_count = expected_row_count
                    else:
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
                    if replayed_count == expected_row_count and resolution_plan is None:
                        stored = cursor.execute(
                            "SELECT metadata->>'canonical_revision', metadata->>'canonical_outcome_sha256', "
                            "metadata->>'actor_count', metadata->>'account_count' "
                            "FROM migration.export_batch WHERE export_batch_id=%s",
                            (batch_id,),
                        ).fetchone()
                        if stored is None or None in stored:
                            raise ValueError("replayed batch lacks canonical revision receipt")
                        canonical_revision, canonical_hash = int(stored[0]), stored[1]
                        actor_count, account_count = int(stored[2]), int(stored[3])
                    elif resolution_plan is None:
                        canonical_hash = canonical_outcome_hash(outcomes)
                        actor_count = len({item.actor_id for item in outcomes})
                        previous_revision = cursor.execute(
                            "SELECT canonical_revision FROM hub.canonical_state WHERE singleton=true"
                        ).fetchone()[0]
                        canonical_revision = cursor.execute(
                            "SELECT hub.advance_canonical_revision(%s)", (previous_revision,)
                        ).fetchone()[0]
                        target_ids = sorted({item.actor_id for item in outcomes}, key=str)
                        account_count = cursor.execute(
                            "SELECT count(*) FROM hub.external_account WHERE actor_id = ANY(%s)",
                            (target_ids,),
                        ).fetchone()[0]
                    if replayed_count == 0 or resolution_plan is not None:
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

                    duplicate_member_ids = {
                        member.logical_id
                        for claim in duplicate_groups.values()
                        for member in claim.members
                    }
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
                            disposition = (
                                BloggerDisposition.QUARANTINED
                                if item.logical_id in duplicate_member_ids
                                else BloggerDisposition.RETAINED_RAW
                            )
                            reason = (
                                "duplicate_account_requires_explicit_resolution"
                                if item.logical_id in duplicate_member_ids
                                else global_reason
                            )
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

                    for identity_hash, claim in duplicate_groups.items():
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
                        for member in claim.members:
                            state = self.writer.raw_state(
                                cursor, export_batch_id=batch_id, source_pk=member.source_pk
                            )
                            if state:
                                assert member.projection is not None
                                cursor.execute(
                                    """
                                    INSERT INTO migration.duplicate_group_member(
                                        duplicate_group_id,raw_record_id,actor_id,evidence
                                    ) VALUES (%s,%s,NULL,%s) ON CONFLICT DO NOTHING
                                    """,
                                    (
                                        group_id,
                                        state[0],
                                        Jsonb(
                                            {
                                                "identity_sha256": identity_hash,
                                                "projected_actor_id": str(member.projection.actor_id),
                                                "source_pk": member.source_pk,
                                                "existing_actor_id": (
                                                    str(claim.existing_actor_id)
                                                    if claim.existing_actor_id
                                                    else None
                                                ),
                                            }
                                        ),
                                    ),
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
                duplicate_groups_pending = cursor.execute(
                    "SELECT duplicate_groups_pending "
                    "FROM migration.blogger_duplicate_accounting WHERE export_batch_id=%s",
                    (batch_id,),
                ).fetchone()[0]
                if can_commit and accounting != (expected_row_count, 0, 0):
                    raise ValueError(f"canonical accounting failed: {accounting!r}")
                if can_commit and duplicate_groups_pending:
                    raise ValueError("duplicate decision accounting remains pending")
                if not can_commit:
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
                                "duplicate_groups_pending": duplicate_groups_pending,
                                "canonical_revision": canonical_revision,
                                "actor_count": actor_count,
                                "account_count": account_count,
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
        review_groups = (
            tuple(
                DuplicateReviewGroup(
                    identity_sha256=identity_hash,
                    members=tuple(
                        DuplicateReviewMember(
                            record_id=member.logical_id,
                            projected_actor_id=member.projection.actor_id,  # type: ignore[union-attr]
                        )
                        for member in sorted(claim.members, key=lambda item: item.logical_id)
                    ),
                    existing_actor_id=claim.existing_actor_id,
                )
                for identity_hash, claim in sorted(duplicate_groups.items())
            )
            if durability_state == "BLOCKED_QUARANTINE"
            else ()
        )
        return ImportReceipt(
            export=export,
            canonical_outcome_sha256=canonical_hash,
            actor_count=actor_count,
            account_count=account_count,
            duplicate_group_count=duplicate_count,
            duplicate_groups_pending=duplicate_groups_pending,
            replayed_count=replayed_count,
            canonical_revision=canonical_revision,
            durability_state=durability_state,
            duplicate_review_groups=review_groups,
        )
