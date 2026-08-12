"""Bounded FINAL-BLOGGER migration stage executed only inside the ACTIVE master.

The stage streams the exact YDB snapshot directly into the local PostgreSQL
primary.  Source rows never cross the control plane or touch a local artifact.
"""

from __future__ import annotations

import hashlib
import os
import secrets
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Annotated, Any, Literal
from urllib.parse import quote
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner, LoginPolicy

from .importer import (
    BloggerSnapshotImporter,
    DuplicateResolution,
    DuplicateReviewGroup,
    ImportReceipt,
    batch_identity,
)
from .schema import SOURCE_DATABASE_PATH, SOURCE_QUERY_SHA256
from .ydb_reader import (
    MAX_BLOGGER_SOURCE_ROWS,
    BloggerYdbScanEvidence,
    BloggerYdbSourceReadReceipt,
    YdbBloggerSnapshot,
    scan_ydb_rows,
)

# Compatibility for the separately owned embedding closure. Blogger ingestion
# never derives or validates source accounting from this historical baseline.
EXPECTED_BLOGGER_ROWS = 266
BLOGGER_STAGE_SCHEMA = "my-data-hub-blogger-migration-request.v1"
BLOGGER_REPLAY_STAGE_SCHEMA = "my-data-hub-blogger-migration-request.v2"
BLOGGER_RESOLUTION_ENVELOPE_SCHEMA = "region-talk-blogger-duplicate-resolution-envelope.v1"
BLOGGER_IMPORT_RECEIPT_SCHEMA = "region-talk-ydb-bloggers-import-receipt.v2"
BLOGGER_IMPORT_RECEIPT_SCHEMA_V3 = "region-talk-ydb-bloggers-import-receipt.v3"
MAX_REQUEST_BYTES = 256 * 1024
MAX_RECEIPT_BYTES = 64 * 1024


class BloggerDuplicateReviewMember(BaseModel):
    """One metadata-only member projection; source values never enter control."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    record_id: str = Field(min_length=1, max_length=4096)
    projected_actor_id: UUID


class BloggerDuplicateReviewGroup(BaseModel):
    """Deterministic facts needed by an owner to resolve one duplicate group."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    members: tuple[BloggerDuplicateReviewMember, ...] = Field(
        min_length=2, max_length=MAX_BLOGGER_SOURCE_ROWS
    )
    existing_actor_id: UUID | None = None

    @model_validator(mode="after")
    def exact_members(self) -> BloggerDuplicateReviewGroup:
        ids = tuple(member.record_id for member in self.members)
        if tuple(sorted(set(ids))) != ids:
            raise ValueError("duplicate review members must be sorted and unique")
        return self

    @property
    def member_record_id_set_sha256(self) -> str:
        ids = tuple(member.record_id for member in self.members)
        return hashlib.sha256(canonical_json_bytes(ids)).hexdigest()


class BloggerDuplicateReviewInputs(BaseModel):
    """Bounded, row-free owner review projection persisted after quarantine."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    groups: tuple[BloggerDuplicateReviewGroup, ...] = Field(min_length=1, max_length=MAX_BLOGGER_SOURCE_ROWS * 5)

    @model_validator(mode="after")
    def exact_groups(self) -> BloggerDuplicateReviewInputs:
        identities = tuple(group.identity_sha256 for group in self.groups)
        if tuple(sorted(set(identities))) != identities:
            raise ValueError("duplicate review groups must be sorted and unique")
        return self

    @property
    def identity_set_sha256(self) -> str:
        return hashlib.sha256(
            canonical_json_bytes(tuple(group.identity_sha256 for group in self.groups))
        ).hexdigest()

    @property
    def member_record_id_set_sha256(self) -> str:
        members = tuple(
            sorted(member.record_id for group in self.groups for member in group.members)
        )
        return hashlib.sha256(canonical_json_bytes(members)).hexdigest()

    @property
    def review_projection_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


class BloggerDuplicateDecision(BaseModel):
    """One bounded decision record; it contains no source payload fields."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    identity_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_record_id: str = Field(min_length=1, max_length=4096)
    canonical_actor_id: UUID
    member_record_ids: tuple[
        Annotated[str, Field(min_length=1, max_length=4096)], ...
    ] = Field(min_length=1, max_length=MAX_BLOGGER_SOURCE_ROWS)
    decided_by: str = Field(min_length=1, max_length=512)
    reason: str = Field(min_length=1, max_length=4096)

    @model_validator(mode="after")
    def exact_members(self) -> BloggerDuplicateDecision:
        if tuple(sorted(set(self.member_record_ids))) != self.member_record_ids:
            raise ValueError("duplicate member record ids must be sorted and unique")
        if self.canonical_record_id not in self.member_record_ids:
            raise ValueError("duplicate canonical record must be an explicit member")
        return self

    def to_importer_resolution(self) -> DuplicateResolution:
        return DuplicateResolution(
            identity_sha256=self.identity_sha256,
            canonical_record_id=self.canonical_record_id,
            canonical_actor_id=self.canonical_actor_id,
            member_record_ids=self.member_record_ids,
            decided_by=self.decided_by,
            reason=self.reason,
        )

    @property
    def decision_sha256(self) -> str:
        return self.to_importer_resolution().resolution_sha256


class BloggerDuplicateResolutionEnvelope(BaseModel):
    """Owner-authorized metadata binding decisions to one quarantined source request."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[
        "region-talk-blogger-duplicate-resolution-envelope.v1"
    ] = BLOGGER_RESOLUTION_ENVELOPE_SCHEMA
    authorization_id: UUID
    authorized_by: str = Field(min_length=1, max_length=512)
    authorized_at: datetime
    source_request_id: UUID
    source_operation_id: UUID
    source_request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    export_batch_id: UUID
    project_id: UUID
    snapshot_at: datetime
    expected_rows: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256
    decisions: tuple[BloggerDuplicateDecision, ...] = Field(min_length=1, max_length=MAX_BLOGGER_SOURCE_ROWS * 5)

    @model_validator(mode="after")
    def exact_binding(self) -> BloggerDuplicateResolutionEnvelope:
        if self.authorized_at.tzinfo is None or self.snapshot_at.tzinfo is None:
            raise ValueError("duplicate authorization and snapshot timestamps must be timezone-aware")
        if self.export_batch_id != batch_identity(self.snapshot_at, self.expected_rows):
            raise ValueError("duplicate envelope export batch does not match the exact snapshot")
        identities = tuple(item.identity_sha256 for item in self.decisions)
        if tuple(sorted(set(identities))) != identities:
            raise ValueError("duplicate decisions must be sorted by unique identity hash")
        if any(item.decided_by != self.authorized_by for item in self.decisions):
            raise ValueError("every duplicate decision must bind the exact authorizer")
        for item in self.decisions:
            item.to_importer_resolution()
        return self

    @property
    def envelope_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()

    @property
    def importer_resolutions(self) -> tuple[DuplicateResolution, ...]:
        return tuple(item.to_importer_resolution() for item in self.decisions)


class BloggerMigrationRequest(BaseModel):
    """Secret-free exact request stored in the metadata-only control ledger."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[
        "my-data-hub-blogger-migration-request.v1",
        "my-data-hub-blogger-migration-request.v2",
    ] = BLOGGER_STAGE_SCHEMA
    request_id: UUID
    operation_id: UUID
    project_id: UUID
    snapshot_at: datetime
    expected_rows: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    source_revision: str = Field(pattern=r"^[0-9a-f]{40}$")
    source_query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256
    source_read_receipt: BloggerYdbSourceReadReceipt | None = None
    replay_of_request_id: UUID | None = None
    duplicate_resolution: BloggerDuplicateResolutionEnvelope | None = None

    @model_validator(mode="after")
    def exact_snapshot(self) -> BloggerMigrationRequest:
        if self.snapshot_at.tzinfo is None:
            raise ValueError("blogger snapshot timestamp must be timezone-aware")
        source_receipt = self.source_read_receipt
        if source_receipt is not None and (
            source_receipt.snapshot_at.astimezone(UTC) != self.snapshot_at.astimezone(UTC)
            or source_receipt.row_count != self.expected_rows
            or source_receipt.source_revision != self.source_revision
            or source_receipt.query_sha256 != self.source_query_sha256
            or source_receipt.export_batch_id != batch_identity(self.snapshot_at, self.expected_rows)
        ):
            raise ValueError("detached YDB read receipt differs from the requested source snapshot")
        if self.schema_version == BLOGGER_STAGE_SCHEMA:
            if self.replay_of_request_id is not None or self.duplicate_resolution is not None:
                raise ValueError("v1 blogger request cannot carry duplicate decisions")
        else:
            envelope = self.duplicate_resolution
            if envelope is None or self.replay_of_request_id != envelope.source_request_id:
                raise ValueError("v2 blogger request requires one exact replay envelope")
            if (
                envelope.project_id != self.project_id
                or envelope.snapshot_at.astimezone(UTC) != self.snapshot_at.astimezone(UTC)
                or envelope.expected_rows != self.expected_rows
                or envelope.source_revision != self.source_revision
                or envelope.source_query_sha256 != self.source_query_sha256
            ):
                raise ValueError("duplicate replay envelope differs from the requested source snapshot")
        return self

    @property
    def request_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.metadata_payload)).hexdigest()

    @property
    def metadata_payload(self) -> dict[str, Any]:
        """Canonical control payload; v1 remains byte/hash compatible."""

        return self.model_dump(mode="json", exclude_none=True)

    @property
    def duplicate_resolutions(self) -> tuple[DuplicateResolution, ...]:
        return self.duplicate_resolution.importer_resolutions if self.duplicate_resolution else ()


class BloggerQuarantineReceipt(BaseModel):
    """Durable metadata proof that a rejected import is awaiting owner review."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["region-talk-ydb-bloggers-quarantine-receipt.v1"] = (
        "region-talk-ydb-bloggers-quarantine-receipt.v1"
    )
    request_id: UUID
    operation_id: UUID
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    master_instance_id: UUID
    run_id: str = Field(min_length=1, max_length=200)
    attempt_id: str = Field(min_length=1, max_length=200)
    epoch: int = Field(ge=1)
    export_batch_id: UUID
    failure_code: Literal["BloggerMigrationQuarantined"] = "BloggerMigrationQuarantined"
    row_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    raw_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    dispositioned_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    undispositioned_count: Literal[0]
    quarantined_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    logical_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    record_id_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_outcome_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    duplicate_group_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS * 5)
    duplicate_groups_pending: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS * 5)
    duplicate_review_inputs: BloggerDuplicateReviewInputs
    transaction_committed: Literal[True] = True
    durability_state: Literal["BLOCKED_QUARANTINE"] = "BLOCKED_QUARANTINE"

    @model_validator(mode="after")
    def exact_accounting(self) -> BloggerQuarantineReceipt:
        if self.duplicate_group_count != self.duplicate_groups_pending:
            raise ValueError("all quarantined duplicate groups must remain pending")
        if self.duplicate_group_count != len(self.duplicate_review_inputs.groups):
            raise ValueError("quarantine review group count differs from durable accounting")
        if (
            self.raw_count != self.row_count
            or self.dispositioned_count != self.row_count
            or self.undispositioned_count != 0
        ):
            raise ValueError("quarantine receipt does not account for the dynamic source snapshot")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()

    @property
    def quarantine_evidence(self) -> dict[str, Any]:
        return {
            "request_id": str(self.request_id),
            "request_sha256": self.request_sha256,
            "operation_id": str(self.operation_id),
            "export_batch_id": str(self.export_batch_id),
            "failure_code": self.failure_code,
            "row_count": self.row_count,
            "raw_count": self.raw_count,
            "dispositioned_count": self.dispositioned_count,
            "undispositioned_count": self.undispositioned_count,
            "quarantined_count": self.quarantined_count,
            "logical_sha256": self.logical_sha256,
            "record_id_set_sha256": self.record_id_set_sha256,
            "canonical_outcome_sha256": self.canonical_outcome_sha256,
            "duplicate_group_count": self.duplicate_group_count,
            "duplicate_groups_pending": self.duplicate_groups_pending,
        }

    @property
    def duplicate_review(self) -> dict[str, Any]:
        inputs = self.duplicate_review_inputs
        return {
            "export_batch_id": str(self.export_batch_id),
            "source_request_id": str(self.request_id),
            "source_operation_id": str(self.operation_id),
            "source_request_sha256": self.request_sha256,
            "duplicate_group_count": self.duplicate_group_count,
            "duplicate_groups_pending": self.duplicate_groups_pending,
            "identity_set_sha256": inputs.identity_set_sha256,
            "member_record_id_set_sha256": inputs.member_record_id_set_sha256,
            "review_projection_sha256": inputs.review_projection_sha256,
        }


class BloggerMigrationQuarantined(RuntimeError):
    """The exact batch is durable but explicit duplicate authorization is required."""

    def __init__(self, receipt: BloggerQuarantineReceipt) -> None:
        super().__init__(
            "blogger migration evidence was durably quarantined; "
            "canonical completion and checkpoint publication are blocked"
        )
        self.receipt = receipt


def resolution_matches_quarantine(
    envelope: BloggerDuplicateResolutionEnvelope,
    receipt: BloggerQuarantineReceipt,
) -> bool:
    """Verify owner decisions cover exactly the persisted review facts."""

    if (
        envelope.source_request_id != receipt.request_id
        or envelope.source_operation_id != receipt.operation_id
        or envelope.source_request_sha256 != receipt.request_sha256
        or envelope.export_batch_id != receipt.export_batch_id
    ):
        return False
    groups = {group.identity_sha256: group for group in receipt.duplicate_review_inputs.groups}
    if tuple(item.identity_sha256 for item in envelope.decisions) != tuple(sorted(groups)):
        return False
    for decision in envelope.decisions:
        group = groups[decision.identity_sha256]
        projected = {member.record_id: member.projected_actor_id for member in group.members}
        if decision.member_record_ids != tuple(projected):
            return False
        expected_actor = group.existing_actor_id or projected[decision.canonical_record_id]
        if decision.canonical_actor_id != expected_actor:
            return False
    return True


class BloggerImportStageReceipt(BaseModel):
    """Sanitized commit receipt; contains accounting and identities, never rows."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal[
        "region-talk-ydb-bloggers-import-receipt.v2",
        "region-talk-ydb-bloggers-import-receipt.v3",
    ] = BLOGGER_IMPORT_RECEIPT_SCHEMA
    request_id: UUID
    operation_id: UUID
    master_instance_id: UUID
    run_id: str = Field(min_length=1, max_length=200)
    epoch: int = Field(ge=1)
    request_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    export_batch_id: UUID
    source_query_sha256: Literal[SOURCE_QUERY_SHA256] = SOURCE_QUERY_SHA256
    row_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    distinct_record_ids: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    source_file_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    dispositions: dict[str, int]
    record_id_set_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    logical_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    canonical_outcome_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    actor_count: int = Field(ge=1, le=MAX_BLOGGER_SOURCE_ROWS)
    account_count: int = Field(ge=0, le=MAX_BLOGGER_SOURCE_ROWS * 8)
    duplicate_group_count: int = Field(default=0, ge=0, le=MAX_BLOGGER_SOURCE_ROWS * 5)
    duplicate_groups_pending: Literal[0] = 0
    undispositioned: Literal[0] = 0
    quarantined: Literal[0] = 0
    replayed_count: int = Field(ge=0, le=MAX_BLOGGER_SOURCE_ROWS)
    canonical_revision: int = Field(ge=1)
    transaction_committed: Literal[True] = True
    ydb_write_denial_verified: Literal[True] = True
    durability_state: Literal["COMMITTED_PENDING_CHECKPOINT"] = "COMMITTED_PENDING_CHECKPOINT"

    @model_validator(mode="after")
    def lossless_accounting(self) -> BloggerImportStageReceipt:
        if self.distinct_record_ids != self.row_count:
            raise ValueError("blogger source record identities are not exact")
        if sum(self.dispositions.values()) != self.row_count:
            raise ValueError("blogger dispositions do not account for the dynamic source snapshot")
        if any(not isinstance(key, str) or value < 0 for key, value in self.dispositions.items()):
            raise ValueError("blogger dispositions are invalid")
        if self.replayed_count not in {0, self.row_count}:
            raise ValueError("blogger import is a partial replay")
        deduplicated = self.dispositions.get("deduplicated", 0)
        if self.actor_count > self.row_count:
            raise ValueError("blogger actor accounting exceeds source rows")
        if self.duplicate_group_count == 0 and deduplicated:
            raise ValueError("deduplicated rows require durable duplicate groups")
        if self.schema_version == BLOGGER_IMPORT_RECEIPT_SCHEMA and (
            self.actor_count != self.row_count
            or self.duplicate_group_count != 0
            or deduplicated != 0
        ):
            raise ValueError("v2 blogger receipt cannot describe duplicate resolution")
        return self

    @property
    def receipt_sha256(self) -> str:
        return hashlib.sha256(canonical_json_bytes(self.model_dump(mode="json"))).hexdigest()


@dataclass(frozen=True, slots=True)
class BloggerStageContext:
    identity: MasterIdentity
    request: BloggerMigrationRequest
    local_database_url: str
    lease_until: datetime
    attempt_id: str


def _migration_url(base_url: str, principal: str, password: str) -> str:
    """Bind a short-lived LOGIN to the master's existing local socket URL."""

    if not base_url.startswith("postgresql://postgres@/"):
        raise ValueError("blogger stage requires the in-master local PostgreSQL socket")
    return base_url.replace(
        "postgresql://postgres@/",
        f"postgresql://{quote(principal, safe='')}:{quote(password, safe='')}@/",
        1,
    )


def _to_receipt(
    context: BloggerStageContext,
    imported: ImportReceipt,
) -> BloggerImportStageReceipt:
    export = imported.export
    quarantined = export.dispositions.get("quarantined", 0)
    return BloggerImportStageReceipt(
        schema_version=(
            BLOGGER_IMPORT_RECEIPT_SCHEMA_V3
            if imported.duplicate_group_count or imported.actor_count != imported.export.row_count
            else BLOGGER_IMPORT_RECEIPT_SCHEMA
        ),
        request_id=context.request.request_id,
        operation_id=context.request.operation_id,
        master_instance_id=context.identity.master_instance_id,
        run_id=context.identity.run_id,
        epoch=context.identity.epoch,
        request_sha256=context.request.request_sha256,
        export_batch_id=export.export_batch_id,
        row_count=export.row_count,
        distinct_record_ids=export.distinct_record_ids,
        source_file_count=export.source_file_count,
        dispositions=export.dispositions,
        record_id_set_sha256=export.record_id_set_sha256,
        logical_sha256=export.logical_sha256,
        canonical_outcome_sha256=imported.canonical_outcome_sha256,
        actor_count=imported.actor_count,
        account_count=imported.account_count,
        duplicate_group_count=imported.duplicate_group_count,
        duplicate_groups_pending=imported.duplicate_groups_pending,
        undispositioned=export.undispositioned,
        quarantined=quarantined,
        replayed_count=imported.replayed_count,
        canonical_revision=imported.canonical_revision,
        transaction_committed=True,
        ydb_write_denial_verified=True,
    )


def _review_group(group: DuplicateReviewGroup) -> BloggerDuplicateReviewGroup:
    return BloggerDuplicateReviewGroup(
        identity_sha256=group.identity_sha256,
        members=tuple(
            BloggerDuplicateReviewMember(
                record_id=member.record_id,
                projected_actor_id=member.projected_actor_id,
            )
            for member in group.members
        ),
        existing_actor_id=group.existing_actor_id,
    )


def _to_quarantine_receipt(
    context: BloggerStageContext,
    imported: ImportReceipt,
) -> BloggerQuarantineReceipt:
    export = imported.export
    inputs = BloggerDuplicateReviewInputs(
        groups=tuple(_review_group(group) for group in imported.duplicate_review_groups)
    )
    receipt = BloggerQuarantineReceipt(
        request_id=context.request.request_id,
        operation_id=context.request.operation_id,
        request_sha256=context.request.request_sha256,
        master_instance_id=context.identity.master_instance_id,
        run_id=context.identity.run_id,
        attempt_id=context.attempt_id,
        epoch=context.identity.epoch,
        export_batch_id=export.export_batch_id,
        row_count=export.row_count,
        raw_count=export.row_count,
        dispositioned_count=sum(export.dispositions.values()),
        undispositioned_count=export.undispositioned,
        quarantined_count=export.dispositions.get("quarantined", 0),
        logical_sha256=export.logical_sha256,
        record_id_set_sha256=export.record_id_set_sha256,
        canonical_outcome_sha256=imported.canonical_outcome_sha256,
        duplicate_group_count=imported.duplicate_group_count,
        duplicate_groups_pending=imported.duplicate_groups_pending,
        duplicate_review_inputs=inputs,
    )
    if len(canonical_json_bytes(receipt.model_dump(mode="json"))) > MAX_REQUEST_BYTES:
        raise RuntimeError("bounded blogger quarantine receipt exceeds the control payload limit")
    return receipt


def execute_blogger_migration_stage(
    context: BloggerStageContext,
    *,
    owner_connection: Any,
    driver: Any | None = None,
    importer: BloggerSnapshotImporter | None = None,
    duplicate_resolutions: tuple[DuplicateResolution, ...] | None = None,
    now: datetime | None = None,
) -> BloggerImportStageReceipt:
    """Execute one dynamically accounted source import under an epoch-bound login.

    The temporary LOGIN is created and consumed only within the master process.
    It is always dropped; no DSN is returned, persisted, logged, or handed to a
    second Kaggle notebook. The production path performs fresh read-only YDB
    scans and streams rows only inside this ACTIVE-master process. Source bytes
    never enter the control ledger, a detached receipt, or the devstand.
    """

    import psycopg

    request = context.request
    bound_resolutions = request.duplicate_resolutions
    if duplicate_resolutions is not None and duplicate_resolutions != bound_resolutions:
        raise ValueError("blogger duplicate decisions differ from the hashed request")
    observed = (now or datetime.now(UTC)).astimezone(UTC)
    expiry = min(observed + timedelta(minutes=5), context.lease_until.astimezone(UTC))
    if expiry <= observed + timedelta(seconds=270):
        raise RuntimeError("ACTIVE epoch lease is too short for the bounded blogger stage")
    credential_id = UUID(bytes=secrets.token_bytes(16), version=4)
    principal = f"mdh_e{context.identity.epoch}_migration_{credential_id.hex[:8]}"
    password = secrets.token_urlsafe(36)
    provisioner = CredentialProvisioner(owner_connection)
    provisioner.create(
        principal=principal,
        password=password,
        group="mdh_migration_operator",
        identity=context.identity,
        credential_id=credential_id,
        expires_at=expiry,
        now=observed,
        policy=LoginPolicy(
            statement_timeout_ms=180_000,
            lock_timeout_ms=5_000,
            idle_transaction_timeout_ms=30_000,
            connection_limit=1,
        ),
    )
    owns_driver = driver is None
    if driver is None:
        import ydb

        endpoint = os.environ.get("MY_DATA_HUB_YDB_ENDPOINT", "").strip()
        database = os.environ.get("MY_DATA_HUB_YDB_DATABASE", "").strip()
        if not endpoint or not database:
            provisioner.drop(principal)
            raise RuntimeError("bounded YDB endpoint/database configuration is absent")
        if database != SOURCE_DATABASE_PATH:
            provisioner.drop(principal)
            raise RuntimeError("ACTIVE-master YDB database differs from the pinned source")
        driver = ydb.Driver(
            endpoint=endpoint,
            database=database,
            credentials=ydb.credentials_from_env_variables(),
        )
        driver.wait(timeout=20, fail_fast=True)
    try:
        snapshot = YdbBloggerSnapshot(driver)
        snapshot.assert_write_denied()
        source_receipt = request.source_read_receipt
        if owns_driver and source_receipt is None:
            raise RuntimeError("production blogger import requires detached two-scan YDB evidence")
        first_scan: BloggerYdbScanEvidence | None = None
        if source_receipt is not None:
            reader_id = os.environ.get("MY_DATA_HUB_YDB_READER_SERVICE_ACCOUNT_ID", "").strip()
            if owns_driver and not reader_id:
                raise RuntimeError("ACTIVE-master YDB reader identity configuration is absent")
            if reader_id and reader_id != source_receipt.reader_service_account_id:
                raise RuntimeError("ACTIVE-master YDB reader differs from detached receipt")
            scan_started = datetime.now(UTC)
            with snapshot.iter_rows() as observed_rows:
                first_scan = scan_ydb_rows(observed_rows, started_at=scan_started)
            if first_scan.consistency_binding != source_receipt.first_scan.consistency_binding:
                raise RuntimeError("fresh ACTIVE-master YDB scan differs from detached receipt")
        with psycopg.connect(
            _migration_url(context.local_database_url, principal, password), connect_timeout=5
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SET ROLE mdh_migration_operator")
                cursor.execute("SET statement_timeout='180s'")
                cursor.execute("SET lock_timeout='5s'")
                cursor.execute("SET idle_in_transaction_session_timeout='30s'")
                cursor.execute("SET transaction_timeout='180s'")
            with snapshot.iter_rows() as rows:
                imported = (importer or BloggerSnapshotImporter()).import_rows(
                    connection,
                    project_id=request.project_id,
                    snapshot_at=request.snapshot_at.astimezone(UTC),
                    expected_row_count=request.expected_rows,
                    rows=rows,
                    source_code_revision=request.source_revision,
                    duplicate_resolutions=bound_resolutions,
                    expected_source_evidence=(
                        {
                            "row_count": first_scan.row_count,
                            "distinct_record_ids": first_scan.distinct_record_ids,
                            "record_id_set_sha256": first_scan.record_id_set_sha256,
                            "logical_sha256": first_scan.logical_sha256,
                            "source_file_count": first_scan.source_file_count,
                        }
                        if first_scan is not None
                        else None
                    ),
                )
            if not imported.accounting_complete:
                raise BloggerMigrationQuarantined(_to_quarantine_receipt(context, imported))
        return _to_receipt(context, imported)
    finally:
        if owns_driver:
            driver.stop(timeout=5)
        provisioner.drop(principal)
