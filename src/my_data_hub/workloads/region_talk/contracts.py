from __future__ import annotations

from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

_SHA256_RE = r"^[a-f0-9]{64}$"
_IDENTIFIER_RE = r"^[A-Za-z0-9_./:-]+$"


class YdbExportSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    system: Literal["ydb"] = "ydb"
    database: str = Field(min_length=1, max_length=500)
    tables: list[str] = Field(min_length=1, max_length=100)
    scope: str = Field(default="region-talk", min_length=1, max_length=200)
    source_revision: str | None = Field(default=None, max_length=500)
    source_code_revision: str | None = Field(default=None, max_length=200)

    @field_validator("tables")
    @classmethod
    def tables_must_be_unique_and_safe(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("source.tables must not contain duplicates")
        if any(not item or "`" in item or "\x00" in item for item in value):
            raise ValueError("source.tables contains an unsafe table path")
        return value


class ExportConsistency(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["consistent_snapshot", "bounded_scan", "final_delta"]
    ordering: list[str] = Field(min_length=1, max_length=10)
    watermark_start: datetime | None = None
    watermark_end: datetime | None = None

    @field_validator("ordering")
    @classmethod
    def ordering_must_be_unique(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("consistency.ordering must not contain duplicates")
        return value

    @model_validator(mode="after")
    def watermark_order(self) -> ExportConsistency:
        if (
            self.watermark_start is not None
            and self.watermark_end is not None
            and self.watermark_end < self.watermark_start
        ):
            raise ValueError("watermark_end precedes watermark_start")
        return self


class YdbExportRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["region-talk-ydb-export-row.v1"]
    export_batch_id: UUID
    source_table: str = Field(min_length=1, max_length=500)
    source_pk: str = Field(min_length=1, max_length=4000)
    row_kind: str = Field(pattern=_IDENTIFIER_RE)
    source_updated_at: datetime | None = None
    payload: Any
    payload_sha256: str = Field(pattern=_SHA256_RE)

    @model_validator(mode="after")
    def source_identity_matches_kind(self) -> YdbExportRow:
        if not self.row_kind.startswith("unknown_") and not self.source_pk.startswith(
            f"{self.row_kind}:"
        ):
            raise ValueError("source_pk prefix must match row_kind")
        return self


class ExportFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    path: str = Field(min_length=1, max_length=1000)
    source_table: str = Field(min_length=1, max_length=500)
    sha256: str = Field(pattern=_SHA256_RE)
    row_count: int = Field(ge=0)
    byte_size: int = Field(ge=0)

    @field_validator("path")
    @classmethod
    def relative_safe_path(cls, value: str) -> str:
        normalized = value.replace("\\", "/")
        if normalized.startswith("/") or ".." in normalized.split("/"):
            raise ValueError("export file path must be relative and may not traverse parents")
        if any(char in value for char in "*?[]"):
            raise ValueError("export file path must name one exact file, not a glob")
        return normalized


class YdbExportManifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["region-talk-ydb-export-manifest.v1"]
    export_batch_id: UUID
    source: YdbExportSource
    consistency: ExportConsistency
    expected_row_count: int = Field(ge=0)
    row_kind_counts: dict[str, int]
    files: list[ExportFile] = Field(min_length=1)
    logical_sha256: str = Field(pattern=_SHA256_RE)
    created_at: datetime
    metadata: dict[str, Any] = Field(default_factory=dict)

    @field_validator("row_kind_counts")
    @classmethod
    def non_negative_kind_counts(cls, value: dict[str, int]) -> dict[str, int]:
        if any(not key or count < 0 for key, count in value.items()):
            raise ValueError("row_kind_counts must use non-empty keys and non-negative counts")
        return value

    @model_validator(mode="after")
    def totals_and_sources_must_match(self) -> YdbExportManifest:
        if sum(self.row_kind_counts.values()) != self.expected_row_count:
            raise ValueError("sum(row_kind_counts) must equal expected_row_count")
        if sum(item.row_count for item in self.files) != self.expected_row_count:
            raise ValueError("sum(files.row_count) must equal expected_row_count")
        if len({item.path for item in self.files}) != len(self.files):
            raise ValueError("manifest file paths must be unique")
        file_tables = {item.source_table for item in self.files}
        source_tables = set(self.source.tables)
        if not file_tables.issubset(source_tables):
            raise ValueError("export files reference a table absent from source.tables")
        if file_tables != source_tables:
            raise ValueError("every source table must have at least one export file")
        if self.consistency.ordering != ["source_table", "source_pk"]:
            raise ValueError("v1 export ordering must be ['source_table', 'source_pk']")
        return self


MigrationBatchStatus = Literal[
    "created",
    "validating",
    "landing",
    "landed",
    "mapping",
    "reconciled",
    "accepted",
    "rejected",
]
MigrationBlockingReason = Literal[
    "raw_count_mismatch",
    "undispositioned_rows",
    "quarantined_rows",
]


class MigrationAccountingRow(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_kind: str = Field(pattern=_IDENTIFIER_RE)
    expected: int = Field(ge=0)
    raw: int = Field(ge=0)
    normalized: int = Field(ge=0)
    deduplicated: int = Field(ge=0)
    intentionally_excluded: int = Field(ge=0)
    retained_raw: int = Field(ge=0)
    quarantined: int = Field(ge=0)
    undispositioned: int = Field(ge=0)
    raw_matches_expected: bool
    fully_accounted: bool
    cutover_ready: bool

    @model_validator(mode="after")
    def arithmetic_is_self_consistent(self) -> MigrationAccountingRow:
        dispositioned = (
            self.normalized
            + self.deduplicated
            + self.intentionally_excluded
            + self.retained_raw
            + self.quarantined
        )
        if dispositioned + self.undispositioned != self.raw:
            raise ValueError("dispositions plus undispositioned must equal raw")
        expected_raw_match = self.raw == self.expected
        if self.raw_matches_expected != expected_raw_match:
            raise ValueError("raw_matches_expected contradicts expected/raw counts")
        expected_fully_accounted = expected_raw_match and self.undispositioned == 0
        if self.fully_accounted != expected_fully_accounted:
            raise ValueError("fully_accounted contradicts row accounting")
        expected_cutover_ready = expected_fully_accounted and self.quarantined == 0
        if self.cutover_ready != expected_cutover_ready:
            raise ValueError("cutover_ready contradicts quarantine/accounting state")
        return self


class MigrationBlockingFinding(BaseModel):
    model_config = ConfigDict(extra="forbid")

    row_kind: str = Field(pattern=_IDENTIFIER_RE)
    reasons: list[MigrationBlockingReason] = Field(min_length=1, max_length=3)
    accounting: MigrationAccountingRow

    @field_validator("reasons")
    @classmethod
    def reasons_are_unique(
        cls, value: list[MigrationBlockingReason]
    ) -> list[MigrationBlockingReason]:
        if len(value) != len(set(value)):
            raise ValueError("blocking reasons must be unique")
        return value

    @model_validator(mode="after")
    def finding_matches_accounting(self) -> MigrationBlockingFinding:
        if self.row_kind != self.accounting.row_kind:
            raise ValueError("finding row_kind must match accounting row_kind")
        expected: list[MigrationBlockingReason] = []
        if not self.accounting.raw_matches_expected:
            expected.append("raw_count_mismatch")
        if self.accounting.undispositioned > 0:
            expected.append("undispositioned_rows")
        if self.accounting.quarantined > 0:
            expected.append("quarantined_rows")
        if self.reasons != expected:
            raise ValueError("blocking reasons contradict accounting")
        return self


class MigrationReconciliationSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    database: str = Field(min_length=1, max_length=500)
    tables: list[str] = Field(min_length=1, max_length=100)

    @field_validator("tables")
    @classmethod
    def tables_are_unique_and_safe(cls, value: list[str]) -> list[str]:
        if len(value) != len(set(value)):
            raise ValueError("source tables must be unique")
        if any(not item or "`" in item or "\x00" in item for item in value):
            raise ValueError("source tables contain an unsafe path")
        return value


class MigrationReconciliationReport(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["migration-reconciliation-report.v1"]
    workload: Literal["region-talk"]
    export_batch_id: UUID
    source: MigrationReconciliationSource
    batch_status: MigrationBatchStatus
    expected_row_count: int = Field(ge=0)
    manifest_sha256: str = Field(pattern=_SHA256_RE)
    logical_sha256: str = Field(pattern=_SHA256_RE)
    completed_at: datetime | None
    accounting: list[MigrationAccountingRow]
    blocking_findings: list[MigrationBlockingFinding]
    passed: bool

    @model_validator(mode="after")
    def report_is_self_consistent(self) -> MigrationReconciliationReport:
        row_kinds = [row.row_kind for row in self.accounting]
        if len(row_kinds) != len(set(row_kinds)):
            raise ValueError("accounting row kinds must be unique")
        if sum(row.expected for row in self.accounting) != self.expected_row_count:
            raise ValueError("accounting expected counts must equal expected_row_count")
        expected_blocking = [row.row_kind for row in self.accounting if not row.cutover_ready]
        actual_blocking = [finding.row_kind for finding in self.blocking_findings]
        if actual_blocking != expected_blocking:
            raise ValueError("blocking findings must cover every non-cutover-ready row kind")
        if self.passed != (not self.blocking_findings):
            raise ValueError("passed contradicts blocking findings")
        return self
