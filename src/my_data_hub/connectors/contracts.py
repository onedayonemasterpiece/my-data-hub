from __future__ import annotations

import hashlib
import json
import math
from datetime import datetime
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

CONTRACT_VERSION = "my-data-hub-data-connector.v1"
DEFAULT_MAX_ENVELOPE_BYTES = 2 * 1024 * 1024
MAX_SAFE_JSON_INTEGER = (2**53) - 1
TraceValue = Annotated[str, Field(max_length=1000)]


class ConnectorContractError(ValueError):
    """The submitted bytes do not satisfy the versioned connector contract."""


def _reject_json_constant(value: str) -> None:
    raise ConnectorContractError(f"non-finite JSON number is not permitted: {value}")


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ConnectorContractError(f"duplicate JSON object key: {key}")
        result[key] = value
    return result


def _canonical_number(value: int | float) -> str:
    """Serialize the practical I-JSON number subset using ECMAScript thresholds.

    CPython and ECMAScript both use shortest-round-trip binary64 formatting.  Their
    exponent thresholds and cosmetic exponent formatting differ; normalizing those
    differences provides RFC 8785 output for finite binary64 values.  Integers are
    emitted exactly, which preserves JSON business counters without float coercion.
    """
    if isinstance(value, int):
        if not -MAX_SAFE_JSON_INTEGER <= value <= MAX_SAFE_JSON_INTEGER:
            raise ConnectorContractError(
                "integers outside the interoperable IEEE-754 range must be encoded as strings"
            )
        return str(value)
    if not math.isfinite(value):
        raise ConnectorContractError("RFC 8785 does not permit non-finite numbers")
    if value == 0:
        return "0"

    sign = "-" if value < 0 else ""
    absolute = abs(value)
    rendered = repr(absolute).lower()
    if "e" not in rendered:
        return sign + (rendered[:-2] if rendered.endswith(".0") else rendered)

    mantissa, exponent_text = rendered.split("e", 1)
    exponent = int(exponent_text)
    digits = mantissa.replace(".", "").rstrip("0")
    if 1e-6 <= absolute < 1e21:
        decimal_position = 1 + exponent
        if decimal_position <= 0:
            fixed = "0." + ("0" * -decimal_position) + digits
        elif decimal_position >= len(digits):
            fixed = digits + ("0" * (decimal_position - len(digits)))
        else:
            fixed = digits[:decimal_position] + "." + digits[decimal_position:]
        return sign + fixed

    scientific_mantissa = digits if len(digits) == 1 else digits[0] + "." + digits[1:]
    exponent_sign = "+" if exponent >= 0 else ""
    return f"{sign}{scientific_mantissa}e{exponent_sign}{exponent}"


def canonical_json_bytes(value: Any) -> bytes:
    """Return RFC 8785-style canonical UTF-8 JSON bytes.

    Object keys are sorted by UTF-16 code units as required by JCS.  Lone Unicode
    surrogates, unsupported values, duplicate keys (during parsing), and non-finite
    numbers are rejected rather than hashed ambiguously.
    """

    def encode(item: Any) -> str:
        if item is None:
            return "null"
        if item is True:
            return "true"
        if item is False:
            return "false"
        if isinstance(item, (int, float)):
            return _canonical_number(item)
        if isinstance(item, str):
            try:
                item.encode("utf-8")
            except UnicodeEncodeError as exc:
                raise ConnectorContractError("lone Unicode surrogates are not permitted") from exc
            return json.dumps(item, ensure_ascii=False, separators=(",", ":"))
        if isinstance(item, list):
            return "[" + ",".join(encode(element) for element in item) + "]"
        if isinstance(item, dict):
            if not all(isinstance(key, str) for key in item):
                raise ConnectorContractError("JSON object keys must be strings")
            keys = sorted(item, key=lambda key: key.encode("utf-16-be", errors="surrogatepass"))
            return "{" + ",".join(f"{encode(key)}:{encode(item[key])}" for key in keys) + "}"
        raise ConnectorContractError(f"unsupported JSON value type: {type(item).__name__}")

    return encode(value).encode("utf-8")


def sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def payload_sha256(records: list[dict[str, Any]]) -> str:
    return sha256_bytes(canonical_json_bytes(records))


class ObservedPeriod(BaseModel):
    model_config = ConfigDict(extra="forbid")

    start: datetime
    end: datetime
    timezone: str = Field(min_length=1, max_length=100)

    @model_validator(mode="after")
    def is_timezone_aware_and_ordered(self) -> ObservedPeriod:
        if self.start.tzinfo is None or self.end.tzinfo is None:
            raise ValueError("observed period timestamps must include a timezone offset")
        if self.end <= self.start:
            raise ValueError("observed period end must be after start")
        return self


class SourceCursor(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partition: str | None = Field(default=None, min_length=1, max_length=200)
    watermark: str | None = Field(default=None, min_length=1, max_length=1000)
    sequence: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def is_not_empty(self) -> SourceCursor:
        if self.partition is None and self.watermark is None and self.sequence is None:
            raise ValueError("source_cursor must have at least one property")
        return self


class ArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid")

    locator: str = Field(min_length=1, max_length=4000)
    media_type: str = Field(min_length=1, max_length=200)
    byte_size: int = Field(ge=1, le=10_737_418_240)
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


class Correction(BaseModel):
    model_config = ConfigDict(extra="forbid")

    supersedes_batch_id: UUID
    reason: str = Field(min_length=1, max_length=1000)


class ConnectorEnvelope(BaseModel):
    """Runtime representation of ``data-connector-envelope.v1``."""

    model_config = ConfigDict(extra="forbid")

    contract_version: Literal["my-data-hub-data-connector.v1"]
    connector_id: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{2,199}$")
    data_product: str = Field(pattern=r"^[a-z0-9][a-z0-9._-]{2,199}$")
    batch_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    schema_version: str = Field(min_length=1, max_length=200)
    produced_at: datetime
    observed_period: ObservedPeriod
    source_cursor: SourceCursor | None = None
    delivery_mode: Literal["push", "pull", "artifact_handoff", "trusted_database_landing"]
    record_count: int = Field(ge=0, le=10_000_000)
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    inline_records: list[dict[str, Any]] | None = Field(default=None, max_length=10_000)
    artifact: ArtifactRef | None = None
    correction: Correction | None = None
    trace: dict[str, TraceValue] = Field(default_factory=dict, max_length=20)

    @model_validator(mode="after")
    def payload_is_unambiguous_and_self_consistent(self) -> ConnectorEnvelope:
        if self.produced_at.tzinfo is None:
            raise ValueError("produced_at must include a timezone offset")
        if (self.inline_records is None) == (self.artifact is None):
            raise ValueError("exactly one of inline_records or artifact is required")
        if self.inline_records is not None:
            if self.record_count != len(self.inline_records):
                raise ValueError("record_count does not match inline_records length")
            actual_hash = payload_sha256(self.inline_records)
            if self.payload_sha256 != actual_hash:
                raise ValueError("payload_sha256 does not match canonical inline_records")
        elif self.artifact is not None and self.payload_sha256 != self.artifact.sha256:
            raise ValueError("payload_sha256 must equal artifact.sha256")
        if self.correction is not None and self.correction.supersedes_batch_id == self.batch_id:
            raise ValueError("a correction cannot supersede its own batch")
        return self


class ValidatedEnvelope(BaseModel):
    """Validated semantic envelope plus the exact evidence bytes received."""

    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)

    envelope: ConnectorEnvelope
    exact_bytes: bytes
    exact_bytes_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    envelope_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")


def validate_envelope_bytes(
    raw: bytes,
    *,
    max_envelope_bytes: int = DEFAULT_MAX_ENVELOPE_BYTES,
    artifact_record_count: int | None = None,
) -> ValidatedEnvelope:
    """Parse and validate exact UTF-8 connector bytes before repository acceptance."""
    if not raw:
        raise ConnectorContractError("connector envelope is empty")
    if len(raw) > max_envelope_bytes:
        raise ConnectorContractError(
            f"connector envelope exceeds {max_envelope_bytes} byte limit"
        )
    try:
        decoded = raw.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ConnectorContractError("connector envelope must be UTF-8 JSON") from exc
    try:
        value = json.loads(
            decoded,
            object_pairs_hook=_unique_object,
            parse_constant=_reject_json_constant,
        )
    except ConnectorContractError:
        raise
    except (json.JSONDecodeError, TypeError, ValueError) as exc:
        raise ConnectorContractError(f"invalid connector JSON: {exc}") from exc
    if not isinstance(value, dict):
        raise ConnectorContractError("connector envelope must be a JSON object")
    try:
        envelope = ConnectorEnvelope.model_validate(value)
    except ValidationError as exc:
        raise ConnectorContractError(f"connector envelope validation failed: {exc}") from exc
    if (
        envelope.artifact is not None
        and artifact_record_count is not None
        and (artifact_record_count < 0 or artifact_record_count != envelope.record_count)
    ):
        raise ConnectorContractError(
            "record_count does not match the validated artifact manifest"
        )
    canonical_envelope = canonical_json_bytes(envelope.model_dump(mode="json", exclude_none=True))
    return ValidatedEnvelope(
        envelope=envelope,
        exact_bytes=raw,
        exact_bytes_sha256=sha256_bytes(raw),
        envelope_sha256=sha256_bytes(canonical_envelope),
    )


class ReceiptStatus(StrEnum):
    ACCEPTED = "accepted"
    REPLAYED = "replayed"


class ConnectorReceipt(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    receipt_id: UUID
    status: ReceiptStatus
    connector_id: str
    batch_id: UUID
    idempotency_key: str
    payload_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    envelope_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    accepted_at: datetime

    @model_validator(mode="after")
    def accepted_time_is_aware(self) -> ConnectorReceipt:
        if self.accepted_at.tzinfo is None:
            raise ValueError("accepted_at must include a timezone offset")
        return self
