"""Closed semantic contract for owner-submitted blogger discovery batches.

The public caller supplies domain records or an immutable private Kaggle artifact
claim.  Connector identity, product identity and canonical UUIDs are deliberately
server-owned and therefore absent from the caller-controlled models.
"""

from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Annotated, Any, Literal
from urllib.parse import quote, urlsplit, urlunsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.connectors.contracts import (
    CONTRACT_VERSION,
    ConnectorEnvelope,
    DeliveryMode,
    ObservedPeriod,
    canonical_json_bytes,
    payload_sha256,
)

DISCOVERY_SCHEMA_VERSION = "blogger-discovery-batch.v1"
INLINE_CONNECTOR_ID = "mcp-blogger-discovery-inline-v1"
INLINE_DATA_PRODUCT = "mcp.bloggers.discovery.inline.v1"
ARTIFACT_CONNECTOR_ID = "mcp-blogger-discovery-artifact-v1"
ARTIFACT_DATA_PRODUCT = "mcp.bloggers.discovery.artifact.v1"
MAX_DISCOVERY_RECORDS = 500
MAX_EVIDENCE_PROPERTIES = 20
MAX_ARTIFACT_BYTES = 10_737_418_240

Sha256 = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


def _sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def normalize_http_url(value: str) -> str:
    """Return a stable absolute HTTP(S) identity without credentials/fragments."""

    raw = value.strip()
    parsed = urlsplit(raw)
    if (
        parsed.scheme.casefold() not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
    ):
        raise ValueError("account/source URLs must be absolute HTTP(S) URLs without credentials")
    scheme = parsed.scheme.casefold()
    host = parsed.hostname.casefold()
    try:
        port = parsed.port
    except ValueError as exc:
        raise ValueError("account/source URL port is invalid") from exc
    default_port = (scheme == "http" and port == 80) or (scheme == "https" and port == 443)
    netloc = host if port is None or default_port else f"{host}:{port}"
    path = parsed.path or "/"
    if path != "/":
        path = path.rstrip("/")
    return urlunsplit((scheme, netloc, path, parsed.query, ""))


class DiscoveryAccount(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    platform: str = Field(min_length=1, max_length=100, pattern=r"^[a-z0-9][a-z0-9_.-]*$")
    external_id: str | None = Field(default=None, min_length=1, max_length=500)
    handle: str | None = Field(default=None, min_length=1, max_length=500)
    url: str | None = Field(default=None, min_length=1, max_length=4000)
    normalized_url: str | None = Field(default=None, min_length=1, max_length=4000)

    @model_validator(mode="after")
    def normalize_and_require_identity(self) -> DiscoveryAccount:
        if self.external_id is None and self.handle is None and self.url is None:
            raise ValueError("account requires external_id, handle or url")
        supplied = normalize_http_url(self.normalized_url) if self.normalized_url else None
        derived = normalize_http_url(self.url) if self.url else None
        if supplied is not None and derived is not None and supplied != derived:
            raise ValueError("normalized_url differs from the normalized account url")
        object.__setattr__(self, "platform", self.platform.casefold())
        object.__setattr__(self, "url", derived)
        object.__setattr__(self, "normalized_url", supplied or derived)
        return self

    def identity(self) -> tuple[str, str, str]:
        if self.external_id is not None:
            return self.platform, "external_id", self.external_id
        if self.normalized_url is not None:
            return self.platform, "normalized_url", self.normalized_url
        assert self.handle is not None
        return self.platform, "handle", self.handle.casefold()


class BloggerDiscoveryRow(BaseModel):
    model_config = ConfigDict(
        extra="forbid", frozen=True, str_strip_whitespace=True, allow_inf_nan=False
    )

    source_record_id: str = Field(min_length=1, max_length=500)
    actor_kind: Literal["person", "organisation", "outlet", "collective", "unknown"]
    display_name: str = Field(min_length=1, max_length=1000)
    canonical_name: str | None = Field(default=None, min_length=1, max_length=1000)
    summary: str | None = Field(default=None, min_length=1, max_length=16_384)
    accounts: list[DiscoveryAccount] = Field(min_length=1, max_length=25)
    source_uri: str = Field(min_length=1, max_length=4000)
    observed_at: datetime
    evidence: dict[str, str | int | float | bool | None] = Field(
        default_factory=dict, max_length=MAX_EVIDENCE_PROPERTIES
    )

    @model_validator(mode="after")
    def validate_row(self) -> BloggerDiscoveryRow:
        if self.observed_at.tzinfo is None:
            raise ValueError("observed_at must include a timezone offset")
        object.__setattr__(self, "source_uri", normalize_http_url(self.source_uri))
        identities = [account.identity() for account in self.accounts]
        if len(set(identities)) != len(identities):
            raise ValueError("one discovery row contains a duplicate account identity")
        for key, value in self.evidence.items():
            if not key or len(key) > 100:
                raise ValueError("evidence keys must contain 1..100 characters")
            if isinstance(value, str) and len(value.encode("utf-8")) > 2000:
                raise ValueError("evidence text exceeds 2000 UTF-8 bytes")
        return self

    @property
    def row_sha256(self) -> str:
        return _sha256(self.model_dump(mode="json"))


class ProviderArtifactClaim(BaseModel):
    """Exact immutable private provider claim, never an arbitrary/latest URL."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    resource_ref: str = Field(pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$", max_length=400)
    control_class: Literal["mcp_exchange", "mcp_managed"]
    provider_version: int = Field(ge=1)
    path: str = Field(min_length=1, max_length=1000)
    media_type: Literal["application/json", "application/jsonl", "application/x-ndjson"]
    byte_size: int = Field(ge=1, le=MAX_ARTIFACT_BYTES)
    sha256: Sha256
    claim_sha256: Sha256
    record_count: int = Field(ge=1, le=MAX_DISCOVERY_RECORDS)

    @model_validator(mode="after")
    def safe_path(self) -> ProviderArtifactClaim:
        parts = self.path.replace("\\", "/").split("/")
        if self.path.startswith(("/", "\\")) or any(part in {"", ".", ".."} for part in parts):
            raise ValueError("artifact path must be a safe relative path")
        return self

    @property
    def locator(self) -> str:
        owner, slug = self.resource_ref.split("/", 1)
        return (
            f"kaggle-private://{owner}/{slug}/versions/{self.provider_version}/"
            f"{quote(self.path, safe='/._-')}?claim_sha256={self.claim_sha256}"
        )


class SubmitDiscoveryBatch(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    contract_version: Literal["submit-discovery-batch.v1"] = "submit-discovery-batch.v1"
    batch_id: UUID
    idempotency_key: str = Field(min_length=8, max_length=300)
    project_slug: str = Field(pattern=r"^[a-z0-9][a-z0-9-]{1,99}$")
    produced_at: datetime
    observed_period: ObservedPeriod
    rows: list[BloggerDiscoveryRow] | None = Field(default=None, min_length=1, max_length=MAX_DISCOVERY_RECORDS)
    artifact: ProviderArtifactClaim | None = None

    @model_validator(mode="after")
    def one_payload_and_no_duplicate_source_identity(self) -> SubmitDiscoveryBatch:
        if self.produced_at.tzinfo is None:
            raise ValueError("produced_at must include a timezone offset")
        if (self.rows is None) == (self.artifact is None):
            raise ValueError("exactly one of rows or artifact is required")
        if self.rows is not None:
            source_ids = [row.source_record_id for row in self.rows]
            if len(set(source_ids)) != len(source_ids):
                raise ValueError("source_record_id must be unique within a discovery batch")
            account_owners: dict[tuple[str, str, str], str] = {}
            for row in self.rows:
                for account in row.accounts:
                    identity = account.identity()
                    owner = account_owners.setdefault(identity, row.source_record_id)
                    if owner != row.source_record_id:
                        raise ValueError("one account identity cannot identify two submitted actors")
        return self

    @property
    def delivery_mode(self) -> DeliveryMode:
        return DeliveryMode.PUSH if self.rows is not None else DeliveryMode.ARTIFACT_HANDOFF

    @property
    def record_count(self) -> int:
        return len(self.rows) if self.rows is not None else self.artifact.record_count  # type: ignore[union-attr]

    @property
    def semantic_payload_sha256(self) -> str:
        if self.rows is not None:
            return payload_sha256([row.model_dump(mode="json") for row in self.rows])
        assert self.artifact is not None
        return self.artifact.sha256

    @property
    def request_sha256(self) -> str:
        return _sha256(self.model_dump(mode="json", exclude_none=True))

    def connector_envelope(self) -> ConnectorEnvelope:
        trace = {
            "semantic_contract": self.contract_version,
            "project_slug": self.project_slug,
            "submit_request_sha256": self.request_sha256,
        }
        if self.rows is not None:
            records = [row.model_dump(mode="json") for row in self.rows]
            return ConnectorEnvelope(
                contract_version=CONTRACT_VERSION,
                connector_id=INLINE_CONNECTOR_ID,
                data_product=INLINE_DATA_PRODUCT,
                batch_id=self.batch_id,
                idempotency_key=self.idempotency_key,
                schema_version=DISCOVERY_SCHEMA_VERSION,
                produced_at=self.produced_at,
                observed_period=self.observed_period,
                delivery_mode=DeliveryMode.PUSH,
                record_count=len(records),
                payload_sha256=payload_sha256(records),
                inline_records=records,
                trace=trace,
            )
        assert self.artifact is not None
        from my_data_hub.connectors.contracts import ArtifactRef

        return ConnectorEnvelope(
            contract_version=CONTRACT_VERSION,
            connector_id=ARTIFACT_CONNECTOR_ID,
            data_product=ARTIFACT_DATA_PRODUCT,
            batch_id=self.batch_id,
            idempotency_key=self.idempotency_key,
            schema_version=DISCOVERY_SCHEMA_VERSION,
            produced_at=self.produced_at,
            observed_period=self.observed_period,
            delivery_mode=DeliveryMode.ARTIFACT_HANDOFF,
            record_count=self.artifact.record_count,
            payload_sha256=self.artifact.sha256,
            artifact=ArtifactRef(
                locator=self.artifact.locator,
                media_type=self.artifact.media_type,
                byte_size=self.artifact.byte_size,
                sha256=self.artifact.sha256,
            ),
            trace=trace,
        )

    def connector_envelope_bytes(self) -> bytes:
        return canonical_json_bytes(
            self.connector_envelope().model_dump(mode="json", exclude_none=True)
        )


def blogger_import_request_sha256(
    *, batch_id: UUID | str, expected_revision: int, idempotency_key: str
) -> str:
    if expected_revision < 0 or not 8 <= len(idempotency_key) <= 300:
        raise ValueError("blogger import request identity is invalid")
    return _sha256(
        {
            "contract_version": "bloggers-import-request.v1",
            "batch_id": str(UUID(str(batch_id))),
            "expected_revision": expected_revision,
            "idempotency_key": idempotency_key,
        }
    )


def blogger_import_plan_sha256(rows: list[dict[str, Any]]) -> str:
    """Hash immutable normalized plan rows in caller-independent ordinal order."""

    ordered = sorted(rows, key=lambda row: int(row["row_ordinal"]))
    return _sha256(ordered)
