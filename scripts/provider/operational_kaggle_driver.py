#!/usr/bin/env python3
"""Trusted, fail-closed production driver for the operational Kaggle matrix.

This executable binds each FM01--FM24 scenario to the production MCP/control
surface that exists today.  It deliberately performs only non-mutating
capability/status probes until the repository exposes an exact evidence
Notebook submission contract capable of returning the matrix-planned task run
identity.  Every unresolved scenario therefore has a named internal API gap;
there is no generic fallback and no synthetic PASS.

The matrix runner invokes this file with ``--request`` and ``--result``.  A
future executor may return PASS only after it owns an exact Kaggle run locator;
the matrix runner will independently reconcile and download that run through
its one ``KaggleProviderAdapter`` before accepting the scenario.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import sys
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.hashing import canonical_json_bytes

if __package__:
    from scripts.provider.operational_kaggle_matrix import EXTERNAL_BLOCKED, SCENARIOS, modern_token_configured
else:  # Direct repository script execution places this directory on sys.path.
    from operational_kaggle_matrix import EXTERNAL_BLOCKED, SCENARIOS, modern_token_configured

FAIL = 1
MAX_REQUEST_BYTES = 256 * 1024
MAX_RESULT_BYTES = 256 * 1024
DEFAULT_ENDPOINT = "https://mcp-datahub.kenigevents.ru/mcp"


class DriverRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-driver-request.v1"]
    matrix_id: UUID
    commit_sha: str = Field(pattern=r"^[a-f0-9]{40}$")
    ordinal: int = Field(ge=1, le=24)
    requirement_id: str = Field(pattern=r"^FM(0[1-9]|1[0-9]|2[0-4])$")
    scenario: str = Field(pattern=r"^[a-z0-9-]+$", max_length=100)
    task_run_id: UUID
    required_assertions: tuple[str, ...] = Field(min_length=2)
    lifecycle_gates: tuple[str, ...]
    minimum_soak_seconds: int | None = Field(default=None, ge=3600, le=3600)
    maximum_soak_seconds: int | None = Field(default=None, ge=5400, le=5400)
    result_file: Literal["operational-result.json"]
    resume_only: bool

    @model_validator(mode="after")
    def exact_catalog_entry(self) -> DriverRequest:
        spec = SCENARIOS[self.ordinal - 1]
        if (
            self.requirement_id != spec.requirement_id
            or self.scenario != spec.name
            or self.required_assertions != spec.assertions
            or self.lifecycle_gates != spec.lifecycle_gates
        ):
            raise ValueError("driver request differs from the exact FM01-FM24 catalog")
        if ("soak" in self.lifecycle_gates) != bool(
            self.minimum_soak_seconds == 3600 and self.maximum_soak_seconds == 5400
        ):
            raise ValueError("driver request has inconsistent soak bounds")
        return self


class CapabilityCheck(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    name: str = Field(pattern=r"^[a-z0-9_.-]+$", max_length=120)
    outcome: Literal["PASS", "BLOCKED", "FAIL"]
    evidence_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    detail_code: str = Field(pattern=r"^[A-Z0-9_]+$", max_length=120)


class TrustedDriverResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    schema_version: Literal["my-data-hub-operational-kaggle-driver-result.v1"]
    outcome: Literal["PASS", "FAIL", "BLOCKED"]
    scenario: str
    task_run_id: UUID
    provider_ref: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")
    provider_run_ref: str | None = Field(default=None, pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[1-9][0-9]*$")
    provider_kernel_id: int | None = Field(default=None, ge=1)
    source_version: int | None = Field(default=None, ge=1)
    source_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")
    blocker_code: str | None = Field(default=None, pattern=r"^[A-Z0-9_]+$", max_length=120)
    integration_dependency: str | None = Field(default=None, min_length=1, max_length=500)
    mutations_started: int = Field(ge=0)
    capability_checks: tuple[CapabilityCheck, ...]
    observation_sha256: str | None = Field(default=None, pattern=r"^[a-f0-9]{64}$")

    @model_validator(mode="after")
    def exact_outcome_shape(self) -> TrustedDriverResult:
        locator = (
            self.provider_ref,
            self.provider_run_ref,
            self.provider_kernel_id,
            self.source_version,
            self.source_sha256,
        )
        if self.outcome == "PASS" and any(value is None for value in locator):
            raise ValueError("driver PASS lacks an exact provider run locator")
        if self.outcome == "BLOCKED" and not (self.blocker_code and self.integration_dependency):
            raise ValueError("driver BLOCKED lacks a named integration dependency")
        return self


class McpGateway(Protocol):
    async def catalog(self, profile: str) -> frozenset[str]: ...

    async def call(self, profile: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]: ...


def _structured_result(result: object) -> dict[str, Any]:
    structured = getattr(result, "structured_content", None)
    if isinstance(structured, Mapping):
        return dict(structured)
    for block in getattr(result, "content", ()):
        text = getattr(block, "text", None)
        if not isinstance(text, str) or len(text.encode()) > MAX_RESULT_BYTES:
            continue
        try:
            value = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict):
            return value
    raise RuntimeError("MCP tool returned no bounded structured object")


def _result_is_error(result: object) -> bool:
    return getattr(result, "isError", getattr(result, "is_error", False)) is True


class RemoteMcpGateway:
    """Bounded MCP client with profile-specific bearer credentials."""

    def __init__(self, endpoint: str, tokens: Mapping[str, str]) -> None:
        parsed = urlsplit(endpoint)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.path.rstrip("/") != "/mcp"
        ):
            raise ValueError("operational MCP endpoint must be a bounded HTTPS /mcp URL")
        self.endpoint = endpoint
        self.tokens = dict(tokens)

    def _token(self, profile: str) -> str:
        token = self.tokens.get(profile, "")
        if not token:
            raise MissingCredential(profile)
        return token

    async def _invoke(self, profile: str, tool: str | None, arguments: Mapping[str, Any]) -> object:
        import httpx2
        from mcp import ClientSession
        from mcp.client.streamable_http import streamable_http_client

        timeout = httpx2.Timeout(25.0, connect=5.0)
        async with (
            httpx2.AsyncClient(
                headers={"Authorization": f"Bearer {self._token(profile)}"},
                follow_redirects=False,
                timeout=timeout,
            ) as client,
            streamable_http_client(self.endpoint, http_client=client) as streams,
        ):
            read_stream, write_stream = streams
            async with ClientSession(read_stream, write_stream, read_timeout_seconds=25) as session:
                await session.initialize()
                if tool is None:
                    return await session.list_tools()
                return await session.call_tool(tool, dict(arguments))

    async def catalog(self, profile: str) -> frozenset[str]:
        result = await self._invoke(profile, None, {})
        return frozenset(str(tool.name) for tool in result.tools)  # type: ignore[attr-defined]

    async def call(self, profile: str, tool: str, arguments: Mapping[str, Any]) -> dict[str, Any]:
        result = await self._invoke(profile, tool, arguments)
        if _result_is_error(result):
            raise RuntimeError(f"MCP {tool} returned an error")
        return _structured_result(result)


class MissingCredential(RuntimeError):
    def __init__(self, profile: str) -> None:
        super().__init__(profile)
        self.profile = profile


@dataclass(frozen=True, slots=True)
class ExecutorSpec:
    requirement_id: str
    profiles: tuple[str, ...]
    tools: tuple[tuple[str, str], ...]
    probe: str
    gap_code: str
    gap_dependency: str


# Every scenario has an explicit production surface and a concrete remaining
# gap.  There is intentionally no catch-all executor.
EXECUTORS: tuple[ExecutorSpec, ...] = (
    ExecutorSpec(
        "FM01",
        ("provider",),
        (("provider", "provider.resources.status"),),
        "provider_status",
        "PROVIDER_DATASET_EXACT_PAYLOAD_CONTRACT_MISSING",
        "provider control gateway create/read/delete payload and exact evidence-run locator contract",
    ),
    ExecutorSpec(
        "FM02",
        ("provider",),
        (("provider", "provider.resources.status"),),
        "provider_status",
        "PROVIDER_NOTEBOOK_EXACT_PAYLOAD_CONTRACT_MISSING",
        "provider control gateway source/run/output/delete payload and exact evidence-run locator contract",
    ),
    ExecutorSpec(
        "FM03",
        ("reader",),
        (("reader", "master.status"),),
        "master_status",
        "RUNTIME_EVENT_HISTORY_TOOL_MISSING",
        "bounded callback/heartbeat/terminal event history tool keyed by provider run and epoch",
    ),
    ExecutorSpec(
        "FM04",
        ("reader", "operator"),
        (("reader", "master.status"), ("operator", "master.ensure")),
        "master_status",
        "EMPTY_MASTER_BOOTSTRAP_SELECTOR_MISSING",
        "master.ensure request contract selecting a verified empty checkpoint and returning bootstrap evidence",
    ),
    ExecutorSpec(
        "FM05",
        ("reader", "operator"),
        (("reader", "checkpoint.status"),),
        "checkpoint_status",
        "CHECKPOINT_CANDIDATE_PUBLISH_TOOL_MISSING",
        "operator checkpoint candidate publish/exact-readback/restore API",
    ),
    ExecutorSpec(
        "FM06",
        ("reader", "operator"),
        (("reader", "checkpoint.status"), ("operator", "checkpoint.restore.request"), ("operator", "operation.get")),
        "checkpoint_status",
        "RESTORE_EVIDENCE_RUN_LOCATOR_MISSING",
        "restore operation terminal result bound to the matrix task run and exact verifier Notebook locator",
    ),
    ExecutorSpec(
        "FM07",
        ("reader", "operator"),
        (("reader", "master.status"), ("operator", "master.ensure")),
        "master_status",
        "ENSURE_REQUEST_IDENTITY_AND_INVENTORY_PROOF_MISSING",
        "master.ensure idempotency/task identity arguments plus exact physical provider-run inventory proof",
    ),
    ExecutorSpec(
        "FM08",
        ("reader", "operator"),
        (("reader", "master.status"), ("operator", "operation.get")),
        "master_status",
        "CALLBACK_LOSS_AND_CONTROL_RESTART_FAULT_API_MISSING",
        "privileged callback suppression, abrupt termination, and in-flight control restart API",
    ),
    ExecutorSpec(
        "FM09",
        ("operator",),
        (("operator", "operation.get"),),
        "catalog_only",
        "CALLBACK_OUTPUT_REPLAY_FAULT_API_MISSING",
        "duplicate/stale callback and stale exact-output replay injection API",
    ),
    ExecutorSpec(
        "FM10",
        ("reader", "operator"),
        (("reader", "master.status"), ("operator", "runtime.stale_epoch.probe")),
        "master_status",
        "LEASE_EXPIRY_CLOCK_AND_WRITE_PROBE_MISSING",
        "lease-expiry clock control plus expired-session real write admission probe",
    ),
    ExecutorSpec(
        "FM11",
        ("reader", "operator"),
        (("reader", "master.status"), ("operator", "runtime.stale_epoch.probe")),
        "stale_epoch",
        "OLD_RUN_RENEW_REGISTER_PROBES_MISSING",
        "old provider run resume plus renew/register/write/tunnel denial probes",
    ),
    ExecutorSpec(
        "FM12",
        ("reader", "operator"),
        (("reader", "master.status"), ("reader", "checkpoint.status")),
        "master_checkpoint_status",
        "MASTER_CLEAN_DRAIN_TOOL_MISSING",
        "operator clean drain/checkpoint/stop request and terminal receipt tool",
    ),
    ExecutorSpec(
        "FM13",
        ("reader", "operator"),
        (
            ("reader", "master.status"),
            ("reader", "checkpoint.status"),
            ("operator", "master.rotation.request"),
            ("operator", "operation.get"),
        ),
        "master_checkpoint_status",
        "ROTATION_EVIDENCE_RUN_LOCATOR_MISSING",
        "rotation terminal result bound to old/new numeric Kaggle runs and matrix evidence run",
    ),
    ExecutorSpec(
        "FM14",
        ("reader", "operator"),
        (("reader", "checkpoint.status"),),
        "checkpoint_status",
        "CHECKPOINT_CORRUPTION_FAULT_API_MISSING",
        "candidate upload corruption/hash-mismatch injector with before/after HEAD identity",
    ),
    ExecutorSpec(
        "FM15",
        ("reader", "operator"),
        (("reader", "checkpoint.status"),),
        "checkpoint_status",
        "RESTORE_SMOKE_FAILURE_FAULT_API_MISSING",
        "restore-smoke forced-failure injector with before/after HEAD identity",
    ),
    ExecutorSpec(
        "FM16",
        ("reader", "migration"),
        (
            ("reader", "bloggers.migration.accounting"),
            ("migration", "bloggers.import.preview"),
            ("migration", "bloggers.import.apply"),
        ),
        "blogger_accounting",
        "YDB_FULL_EXPORT_BATCH_BINDING_MISSING",
        "trusted full YDB export batch locator and post-import checkpoint terminal operation binding",
    ),
    ExecutorSpec(
        "FM17",
        ("reader", "operator"),
        (
            ("reader", "master.status"),
            ("reader", "bloggers.statistics"),
            ("reader", "checkpoint.status"),
            ("operator", "checkpoint.restore.request"),
        ),
        "blogger_checkpoint_status",
        "BLOGGER_LOGICAL_HASH_READ_TOOL_MISSING",
        "bounded canonical blogger logical-hash tool before and after cold restore",
    ),
    ExecutorSpec(
        "FM18",
        ("reader", "operator"),
        (("reader", "embedding.coverage"),),
        "embedding_status",
        "E5_WORKER_SUBMISSION_TOOL_MISSING",
        "exact E5 corpus worker submission/import/checkpoint operation tool",
    ),
    ExecutorSpec(
        "FM19",
        ("reader", "operator"),
        (("reader", "embedding.coverage"),),
        "embedding_status",
        "BGE_M3_WORKER_SUBMISSION_TOOL_MISSING",
        "exact BGE-M3 corpus worker submission/import/checkpoint operation tool",
    ),
    ExecutorSpec(
        "FM20",
        ("reader",),
        (("reader", "master.status"), ("reader", "bloggers.search")),
        "master_status",
        "HOST_REBOOT_CONTROL_AND_BOOT_IDENTITY_API_MISSING",
        "trusted host reboot controller plus before/after boot identity exposed to the driver",
    ),
    ExecutorSpec(
        "FM21",
        ("reader", "operator"),
        (
            ("reader", "checkpoint.status"),
            ("operator", "data.change.preview"),
            ("operator", "data.change.apply"),
            ("reader", "data.change.status"),
        ),
        "checkpoint_status",
        "CONTROLLED_BUSINESS_ROW_FIXTURE_MISSING",
        "owner-approved disposable business-row SQL/parameters/revision fixture and cleanup contract",
    ),
    ExecutorSpec(
        "FM22",
        ("provider",),
        (
            ("provider", "provider.resources.create"),
            ("provider", "provider.resources.run"),
            ("provider", "provider.resources.read"),
            ("provider", "provider.resources.delete"),
        ),
        "catalog_only",
        "PROVIDER_MCP_EXACT_PAYLOAD_CONTRACT_MISSING",
        "versioned MCP-managed Dataset/Notebook payload, receipt, and claim cleanup contract",
    ),
    ExecutorSpec(
        "FM23",
        ("reader", "operator"),
        (("reader", "provider.resources.status"), ("operator", "provider.protected_resource.probe")),
        "protected_probe",
        "PROTECTED_PROBE_EVIDENCE_RUN_LOCATOR_MISSING",
        "probe result binding to a distinct matrix evidence Notebook exact run locator",
    ),
    ExecutorSpec(
        "FM24",
        ("reader", "operator"),
        (("reader", "master.status"), ("reader", "checkpoint.status"), ("operator", "master.rotation.request")),
        "master_checkpoint_status",
        "ACCELERATED_SOAK_SESSION_CONTROL_API_MISSING",
        "60-90 minute session rotation/heartbeat/read/checkpoint/recovery controller with exact event stream",
    ),
)

if len(EXECUTORS) != 24 or tuple(item.requirement_id for item in EXECUTORS) != tuple(
    f"FM{ordinal:02d}" for ordinal in range(1, 25)
):  # pragma: no cover
    raise RuntimeError("trusted driver requires one exact executor for every FM01-FM24 scenario")


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _tokens_from_environment() -> dict[str, str]:
    return {
        "reader": os.environ.get("MY_DATA_HUB_MCP_CANARY_TOKEN", "").strip(),
        "operator": os.environ.get("MY_DATA_HUB_MCP_ACCEPTANCE_OPERATOR_TOKEN", "").strip(),
        "migration": os.environ.get("MY_DATA_HUB_MCP_MIGRATION_OPERATOR_TOKEN", "").strip(),
        "provider": os.environ.get("MY_DATA_HUB_MCP_PROVIDER_OPERATOR_TOKEN", "").strip(),
    }


def _check(
    name: str, outcome: Literal["PASS", "BLOCKED", "FAIL"], detail: str, evidence: object | None = None
) -> CapabilityCheck:
    return CapabilityCheck(
        name=name,
        outcome=outcome,
        detail_code=detail,
        evidence_sha256=_sha(evidence) if evidence is not None else None,
    )


async def _safe_probe(gateway: McpGateway, spec: ExecutorSpec) -> tuple[list[CapabilityCheck], dict[str, Any]]:
    checks: list[CapabilityCheck] = []
    observations: dict[str, Any] = {}
    if spec.probe == "catalog_only":
        return checks, observations
    if spec.probe in {"master_status", "master_checkpoint_status", "blogger_checkpoint_status"}:
        observations["master"] = await gateway.call("reader", "master.status", {})
    if spec.probe in {"checkpoint_status", "master_checkpoint_status", "blogger_checkpoint_status"}:
        observations["checkpoint"] = await gateway.call("reader", "checkpoint.status", {})
    if spec.probe == "provider_status":
        observations["provider"] = await gateway.call(spec.profiles[0], "provider.resources.status", {"limit": 100})
    if spec.probe == "embedding_status":
        observations["embedding"] = await gateway.call("reader", "embedding.coverage", {})
    if spec.probe == "blogger_accounting":
        batch_id = os.environ.get("MY_DATA_HUB_OPERATIONAL_BLOGGER_BATCH_ID", "").strip()
        if not batch_id:
            checks.append(_check("blogger-batch", "BLOCKED", "BLOGGER_BATCH_ID_MISSING"))
        else:
            observations["blogger_accounting"] = await gateway.call(
                "reader", "bloggers.migration.accounting", {"export_batch_id": batch_id}
            )
    if spec.probe == "blogger_checkpoint_status":
        observations["blogger_statistics"] = await gateway.call("reader", "bloggers.statistics", {})
    if spec.probe == "stale_epoch":
        master = await gateway.call("reader", "master.status", {})
        observations["master"] = master
        epoch = master.get("master_epoch", master.get("epoch"))
        if isinstance(epoch, int) and not isinstance(epoch, bool) and epoch > 1:
            observations["stale_epoch"] = await gateway.call(
                "operator",
                "runtime.stale_epoch.probe",
                {"expected_active_epoch": epoch, "submitted_epoch": epoch - 1},
            )
        else:
            checks.append(_check("stale-epoch-precondition", "BLOCKED", "ACTIVE_EPOCH_GT_ONE_REQUIRED"))
    if spec.probe == "protected_probe":
        provider = await gateway.call("reader", "provider.resources.status", {"limit": 100})
        observations["provider"] = provider
        rows = provider.get("resources")
        resource_ref = (
            next(
                (
                    row.get("resource_ref")
                    for row in rows
                    if isinstance(rows, list)
                    and isinstance(row, Mapping)
                    and row.get("control_class") == "orchestrator_protected"
                    and isinstance(row.get("resource_ref"), str)
                ),
                None,
            )
            if isinstance(rows, list)
            else None
        )
        if resource_ref is None:
            checks.append(_check("protected-resource", "BLOCKED", "PROTECTED_RESOURCE_NOT_REGISTERED"))
        else:
            observations["protected_probe"] = await gateway.call(
                "operator", "provider.protected_resource.probe", {"resource_ref": resource_ref}
            )
    for name, value in observations.items():
        checks.append(_check(f"probe.{name}", "PASS", "SAFE_OBSERVATION_COMPLETE", value))
    return checks, observations


async def execute(request: DriverRequest, gateway: McpGateway) -> TrustedDriverResult:
    spec = EXECUTORS[request.ordinal - 1]
    checks: list[CapabilityCheck] = []
    if not modern_token_configured():
        return _blocked(
            request,
            checks=[_check("kaggle-token", "BLOCKED", "KAGGLE_MODERN_API_TOKEN_REQUIRED")],
            code="KAGGLE_MODERN_API_TOKEN_REQUIRED",
            dependency="KAGGLE_API_TOKEN or a regular non-symlinked access_token",
        )
    checks.append(_check("kaggle-token", "PASS", "KAGGLE_MODERN_API_TOKEN_PRESENT", {"present": True}))
    for profile in spec.profiles:
        try:
            catalog = await gateway.catalog(profile)
        except MissingCredential:
            return _blocked(
                request,
                checks=[*checks, _check(f"credential.{profile}", "BLOCKED", f"{profile.upper()}_MCP_TOKEN_MISSING")],
                code=f"{profile.upper()}_MCP_TOKEN_MISSING",
                dependency=f"configured {profile} MCP OAuth credential",
            )
        except Exception as exc:
            return _blocked(
                request,
                checks=[
                    *checks,
                    _check(f"credential.{profile}", "FAIL", "MCP_PROFILE_CATALOG_UNAVAILABLE"),
                ],
                code=f"{profile.upper()}_MCP_CATALOG_UNAVAILABLE",
                dependency=f"bounded {profile} MCP catalog observation ({type(exc).__name__})",
            )
        checks.append(_check(f"credential.{profile}", "PASS", "MCP_PROFILE_AUTHENTICATED", sorted(catalog)))
        required = {tool for tool_profile, tool in spec.tools if tool_profile == profile}
        missing = sorted(required - catalog)
        if missing:
            return _blocked(
                request,
                checks=[*checks, _check(f"catalog.{profile}", "BLOCKED", "REQUIRED_MCP_TOOL_MISSING", missing)],
                code=f"{request.requirement_id}_MCP_TOOLSET_INCOMPLETE",
                dependency=f"{profile} MCP catalog tools: {', '.join(missing)}",
            )
        checks.append(_check(f"catalog.{profile}", "PASS", "REQUIRED_MCP_TOOLS_PRESENT", sorted(required)))
    try:
        probe_checks, observations = await _safe_probe(gateway, spec)
    except Exception as exc:
        return _blocked(
            request,
            checks=[*checks, _check("safe-probe", "FAIL", "SAFE_PROBE_UNAVAILABLE")],
            code=f"{request.requirement_id}_SAFE_PROBE_UNAVAILABLE",
            dependency=f"bounded {spec.probe} production observation ({type(exc).__name__})",
        )
    checks.extend(probe_checks)
    return _blocked(
        request,
        checks=checks,
        code=spec.gap_code,
        dependency=spec.gap_dependency,
        observations=observations,
    )


def _blocked(
    request: DriverRequest,
    *,
    checks: list[CapabilityCheck],
    code: str,
    dependency: str,
    observations: Mapping[str, Any] | None = None,
) -> TrustedDriverResult:
    return TrustedDriverResult(
        schema_version="my-data-hub-operational-kaggle-driver-result.v1",
        outcome="BLOCKED",
        scenario=request.scenario,
        task_run_id=request.task_run_id,
        blocker_code=code,
        integration_dependency=dependency,
        mutations_started=0,
        capability_checks=tuple(checks),
        observation_sha256=_sha(dict(observations)) if observations else None,
    )


def _bounded_json(path: Path, *, maximum: int) -> bytes:
    if path.is_symlink() or not path.is_file() or not 1 <= path.stat().st_size <= maximum:
        raise ValueError(f"unsafe bounded JSON path: {path}")
    return path.read_bytes()


def _atomic_write(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(canonical_json_bytes(dict(payload)))
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--request", type=Path, required=True)
    parser.add_argument("--result", type=Path, required=True)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        request = DriverRequest.model_validate_json(_bounded_json(args.request, maximum=MAX_REQUEST_BYTES))
        gateway = RemoteMcpGateway(
            os.environ.get("MY_DATA_HUB_MCP_CANARY_ENDPOINT", DEFAULT_ENDPOINT),
            _tokens_from_environment(),
        )
        result = asyncio.run(execute(request, gateway))
        _atomic_write(args.result, result.model_dump(mode="json"))
    except Exception as exc:
        print(f"trusted operational driver failed closed: {type(exc).__name__}", file=sys.stderr)
        return FAIL
    return EXTERNAL_BLOCKED if result.outcome == "BLOCKED" else (FAIL if result.outcome == "FAIL" else 0)


if __name__ == "__main__":
    raise SystemExit(main())
