#!/usr/bin/env python3
"""Run bounded scheduled acceptance probes and emit a sanitized receipt.

The runner deliberately distinguishes a failed assertion from a missing safe
runtime interface.  Missing interfaces produce BLOCKED receipts and exit 78;
they are never converted into synthetic success evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import urllib.error
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from pathlib import Path
from urllib.parse import urlsplit

from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.mcp.catalog import TOOL_CONTRACTS
from my_data_hub.providers import BoundedInventory, ProviderKind, ProviderRegistry
from my_data_hub.providers.kaggle import KaggleProviderAdapter
from my_data_hub.providers.kaggle.control_journal import ControlLedgerKaggleJournal

PASS = 0
FAIL = 1
EXTERNAL_BLOCKED = 78
RECEIPT_SCHEMA = "my-data-hub-scheduled-acceptance.v1"
MAX_RECEIPT_BYTES = 512 * 1024
MAX_INVENTORY_RESOURCES = 1_000
READ_ONLY_TOOLS = frozenset(
    name for name, contract in TOOL_CONTRACTS.items() if contract.read_only and contract.role == "reader"
)
_FORBIDDEN_RECEIPT_KEYS = frozenset(
    {
        "token",
        "authorization",
        "password",
        "secret",
        "database_url",
        "rows",
        "items",
        "blogger",
        "bloggers",
    }
)


class Mode(StrEnum):
    NIGHTLY = "nightly"
    WEEKLY = "weekly"
    MANUAL = "manual"


class Outcome(StrEnum):
    PASS = "PASS"
    FAIL = "FAIL"
    BLOCKED = "BLOCKED"


def modern_token_configured() -> bool:
    configured = bool(os.environ.get("KAGGLE_API_TOKEN", "").strip())
    token_path = Path(os.environ.get("KAGGLE_CONFIG_DIR", "~/.kaggle")).expanduser() / "access_token"
    return configured or (
        token_path.is_file() and not token_path.is_symlink() and token_path.stat().st_size > 20
    )


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    category: str
    outcome: Outcome
    observed: Mapping[str, object] = field(default_factory=dict)
    blocker_code: str | None = None
    missing_interface: str | None = None

    def payload(self) -> dict[str, object]:
        value: dict[str, object] = {
            "name": self.name,
            "category": self.category,
            "outcome": self.outcome.value,
            "observed": dict(self.observed),
        }
        if self.blocker_code:
            value["blocker_code"] = self.blocker_code
        if self.missing_interface:
            value["missing_interface"] = self.missing_interface
        return value


@dataclass(slots=True)
class Observations:
    live_resources: list[dict[str, object]] | None = None
    registered_resources: list[dict[str, object]] | None = None
    platform_status: dict[str, object] | None = None
    master_status: dict[str, object] | None = None
    checkpoint_status: dict[str, object] | None = None
    embedding_status: dict[str, object] | None = None
    mcp_tools: set[str] | None = None
    unauthenticated_http_status: int | None = None
    invalid_token_http_status: int | None = None
    deployed_commit_matches: bool | None = None
    lifecycle_receipts: dict[str, dict[str, object]] = field(default_factory=dict)
    blockers: dict[str, tuple[str, str]] = field(default_factory=dict)


def _blocked(name: str, category: str, code: str, interface: str) -> Check:
    return Check(
        name=name,
        category=category,
        outcome=Outcome.BLOCKED,
        blocker_code=code,
        missing_interface=interface,
    )


def _component_blocker(observations: Observations, component: str, name: str, category: str) -> Check:
    code, interface = observations.blockers.get(
        component,
        (f"{component.upper()}_OBSERVATION_UNAVAILABLE", component),
    )
    return _blocked(name, category, code, interface)


def _parse_time(value: object) -> datetime | None:
    if not isinstance(value, str) or len(value) > 100:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _resource_checks(observations: Observations, *, now: datetime, freshness: timedelta) -> list[Check]:
    if observations.live_resources is None:
        return [
            _component_blocker(observations, "kaggle_inventory", "live_provider_inventory", "PROVIDER_KAGGLE"),
            _component_blocker(observations, "kaggle_inventory", "provider_public_scan", "PROVIDER_KAGGLE"),
            _component_blocker(observations, "kaggle_inventory", "provider_orphan_scan", "PROVIDER_KAGGLE"),
        ]
    live = observations.live_resources
    inventory = Check(
        "live_provider_inventory",
        "PROVIDER_KAGGLE",
        Outcome.PASS if len(live) <= MAX_INVENTORY_RESOURCES else Outcome.FAIL,
        {"bounded": len(live) <= MAX_INVENTORY_RESOURCES, "resource_count": len(live)},
    )
    if observations.registered_resources is None:
        public = _component_blocker(
            observations,
            "provider_registry",
            "provider_public_scan",
            "PROVIDER_KAGGLE",
        )
        orphan = _component_blocker(
            observations,
            "provider_registry",
            "provider_orphan_scan",
            "PROVIDER_KAGGLE",
        )
        freshness_check = _component_blocker(
            observations,
            "provider_registry",
            "provider_registry_freshness",
            "PROVIDER_KAGGLE",
        )
    else:
        registered = observations.registered_resources
        registered_refs = {
            str(item.get("resource_ref"))
            for item in registered
            if isinstance(item.get("resource_ref"), str)
        }
        live_by_ref = {
            str(item.get("provider_ref")): item
            for item in live
            if isinstance(item.get("provider_ref"), str)
        }
        public_count = sum(
            live_by_ref[ref].get("private") is not True
            for ref in registered_refs & live_by_ref.keys()
        )
        public = Check(
            "provider_public_scan",
            "PROVIDER_KAGGLE",
            Outcome.PASS if public_count == 0 else Outcome.FAIL,
            {
                "registered_public_or_unproven_count": public_count,
                "registered_count": len(registered_refs),
            },
        )
        missing_registered = registered_refs - live_by_ref.keys()
        external_read_only = live_by_ref.keys() - registered_refs
        orphan = Check(
            "provider_orphan_scan",
            "PROVIDER_KAGGLE",
            Outcome.PASS if not missing_registered else Outcome.FAIL,
            {
                "missing_registered_count": len(missing_registered),
                "external_read_only_count": len(external_read_only),
                "registered_count": len(registered_refs),
            },
        )
        stale_count = 0
        invalid_time_count = 0
        for item in registered:
            observed_at = _parse_time(item.get("observed_at"))
            if observed_at is None or observed_at > now:
                invalid_time_count += 1
            elif now - observed_at > freshness:
                stale_count += 1
        freshness_check = Check(
            "provider_registry_freshness",
            "PROVIDER_KAGGLE",
            Outcome.PASS if stale_count == 0 and invalid_time_count == 0 else Outcome.FAIL,
            {"stale_count": stale_count, "invalid_timestamp_count": invalid_time_count},
        )
    return [inventory, public, orphan, freshness_check]


def _master_checks(observations: Observations, *, now: datetime) -> list[Check]:
    status = observations.master_status
    if status is None:
        return [_component_blocker(observations, "mcp", "master_active_epoch", "DEPLOYMENT")]
    epoch = status.get("master_epoch", status.get("epoch"))
    lease = _parse_time(status.get("lease_expires_at"))
    active = status.get("state") == "ACTIVE" or status.get("master_state") == "ACTIVE"
    valid_epoch = isinstance(epoch, int) and not isinstance(epoch, bool) and epoch >= 1
    lease_current = lease is not None and lease > now
    return [
        Check(
            "master_active_epoch",
            "DEPLOYMENT",
            Outcome.PASS if active and valid_epoch and lease_current else Outcome.FAIL,
            {"active": active, "epoch_present": valid_epoch, "lease_current": lease_current},
        )
    ]


def _checkpoint_checks(
    observations: Observations,
    *,
    now: datetime,
    freshness: timedelta,
) -> list[Check]:
    status = observations.checkpoint_status
    if status is None:
        return [
            _component_blocker(observations, "mcp", "checkpoint_current_previous", "BACKUP_RECOVERY"),
            _component_blocker(observations, "mcp", "checkpoint_freshness", "BACKUP_RECOVERY"),
        ]
    current = bool(status.get("current_checkpoint_id"))
    previous = bool(status.get("previous_checkpoint_id"))
    exact_current = bool(status.get("current_exact_version_ref"))
    exact_previous = bool(status.get("previous_exact_version_ref"))
    if current and previous and not (exact_current and exact_previous):
        generations = _blocked(
            "checkpoint_current_previous",
            "BACKUP_RECOVERY",
            "CHECKPOINT_EXACT_REF_API_MISSING",
            "MCP checkpoint.status exact numeric current/previous version references",
        )
    else:
        generations = Check(
            "checkpoint_current_previous",
            "BACKUP_RECOVERY",
            Outcome.PASS if current and previous and exact_current and exact_previous else Outcome.FAIL,
            {
                "current_present": current,
                "previous_present": previous,
                "current_exact_version_present": exact_current,
                "previous_exact_version_present": exact_previous,
            },
        )
    verified_at = _parse_time(status.get("verified_at"))
    if verified_at is None:
        checkpoint_freshness = _blocked(
            "checkpoint_freshness",
            "BACKUP_RECOVERY",
            "CHECKPOINT_VERIFIED_AT_API_MISSING",
            "MCP checkpoint.status must expose verified_at for the exact current generation",
        )
    else:
        age = max(0, int((now - verified_at).total_seconds()))
        within_limit = verified_at <= now and now - verified_at <= freshness
        checkpoint_freshness = Check(
            "checkpoint_freshness",
            "BACKUP_RECOVERY",
            Outcome.PASS if within_limit else Outcome.FAIL,
            {"age_seconds": age, "within_limit": within_limit},
        )
    return [generations, checkpoint_freshness]


def _embedding_check(observations: Observations) -> Check:
    status = observations.embedding_status
    if status is None:
        return _component_blocker(observations, "mcp", "embedding_coverage", "CONNECTOR_DELIVERY")
    values: list[float] = []
    for model in ("e5", "bge_m3"):
        row = status.get(model)
        coverage = row.get("coverage") if isinstance(row, Mapping) else None
        if isinstance(coverage, (int, float)) and not isinstance(coverage, bool):
            values.append(float(coverage))
    complete = len(values) == 2 and all(value == 1.0 for value in values)
    return Check(
        "embedding_coverage",
        "CONNECTOR_DELIVERY",
        Outcome.PASS if complete else Outcome.FAIL,
        {"model_count": len(values), "all_complete": complete},
    )


def _mcp_checks(observations: Observations) -> list[Check]:
    if observations.mcp_tools is None:
        catalog = _component_blocker(observations, "mcp", "mcp_reader_catalog", "AUTHORIZATION")
    else:
        exact = observations.mcp_tools == READ_ONLY_TOOLS
        catalog = Check(
            "mcp_reader_catalog",
            "AUTHORIZATION",
            Outcome.PASS if exact else Outcome.FAIL,
            {
                "tool_count": len(observations.mcp_tools),
                "exact_read_only_catalog": exact,
                "write_tools_visible": bool(observations.mcp_tools - READ_ONLY_TOOLS),
            },
        )
    unauth = observations.unauthenticated_http_status
    invalid = observations.invalid_token_http_status
    return [
        catalog,
        (
            Check(
                "mcp_unauthenticated_denial",
                "AUTHORIZATION",
                Outcome.PASS if unauth == 401 else Outcome.FAIL,
                {"http_status": unauth or 0},
            )
            if unauth is not None
            else _component_blocker(observations, "mcp", "mcp_unauthenticated_denial", "AUTHORIZATION")
        ),
        (
            Check(
                "mcp_invalid_token_denial",
                "AUTHORIZATION",
                Outcome.PASS if invalid == 401 else Outcome.FAIL,
                {"http_status": invalid or 0},
            )
            if invalid is not None
            else _component_blocker(observations, "mcp", "mcp_invalid_token_denial", "AUTHORIZATION")
        ),
    ]


def _deployment_check(observations: Observations) -> Check:
    if observations.deployed_commit_matches is None:
        return _component_blocker(observations, "mcp", "deployed_commit", "DEPLOYMENT")
    return Check(
        "deployed_commit",
        "DEPLOYMENT",
        Outcome.PASS if observations.deployed_commit_matches else Outcome.FAIL,
        {"matches_expected": observations.deployed_commit_matches},
    )


def _lifecycle_check(observations: Observations, kind: str) -> Check:
    receipt = observations.lifecycle_receipts.get(kind)
    name = f"mcp_managed_{kind}_lifecycle_cleanup"
    if receipt is None:
        return _blocked(
            name,
            "PROVIDER_KAGGLE",
            f"MCP_MANAGED_{kind.upper()}_LIFECYCLE_EVIDENCE_MISSING",
            f"real_kaggle_matrix.py {kind}-canary receipt",
        )
    if kind == "dataset":
        gates = receipt.get("gate_results")
        gates_pass = isinstance(gates, list) and bool(gates) and all(
            isinstance(item, Mapping) and item.get("outcome") == "PASS" for item in gates
        )
        complete = (
            receipt.get("privacy") == "private"
            and receipt.get("cleanup_outcome") == "complete"
            and gates_pass
        )
    else:
        complete = (
            receipt.get("privacy") == "private"
            and receipt.get("terminal_state") == "complete"
            and isinstance(receipt.get("cleanup"), str)
            and bool(receipt.get("cleanup"))
        )
    return Check(
        name,
        "PROVIDER_KAGGLE",
        Outcome.PASS if complete else Outcome.FAIL,
        {"private": receipt.get("privacy") == "private", "cleanup_complete": complete},
    )


def evaluate(
    mode: Mode,
    observations: Observations,
    *,
    now: datetime,
    freshness: timedelta,
) -> list[Check]:
    checks = [
        _deployment_check(observations),
        *_resource_checks(observations, now=now, freshness=freshness),
        *_master_checks(observations, now=now),
        *_checkpoint_checks(observations, now=now, freshness=freshness),
        _embedding_check(observations),
        *_mcp_checks(observations),
        _blocked(
            "connector_coverage",
            "CONNECTOR_DELIVERY",
            "CONNECTOR_COVERAGE_API_MISSING",
            "bounded MCP connector.coverage status without business rows",
        ),
        _blocked(
            "bounded_cold_restore_request",
            "BACKUP_RECOVERY",
            "COLD_RESTORE_REQUEST_API_MISSING",
            "bounded control API for isolated current-checkpoint restore smoke",
        ),
        _blocked(
            "stale_epoch_rejection",
            "AUTHORIZATION",
            "STALE_EPOCH_PROBE_API_MISSING",
            "safe control API that submits a synthetic stale-epoch request",
        ),
    ]
    if mode in {Mode.WEEKLY, Mode.MANUAL}:
        checks.extend(
            [
                _blocked(
                    "forced_master_rotation",
                    "DEPLOYMENT",
                    "FORCED_ROTATION_API_MISSING",
                    "checkpoint-bound control API for forced master rotation",
                ),
                _blocked(
                    "previous_checkpoint_restore",
                    "BACKUP_RECOVERY",
                    "PREVIOUS_CHECKPOINT_RESTORE_API_MISSING",
                    "bounded control API for isolated previous-checkpoint restore",
                ),
                _blocked(
                    "protected_resource_mutation_denial",
                    "AUTHORIZATION",
                    "PROTECTED_RESOURCE_DENIAL_PROBE_API_MISSING",
                    "safe provider-control denial probe for an exact protected resource",
                ),
                _lifecycle_check(observations, "dataset"),
                _lifecycle_check(observations, "notebook"),
            ]
        )
    return checks


def _structured_result(result: object) -> dict[str, object]:
    structured = getattr(result, "structuredContent", None)
    if isinstance(structured, dict):
        return structured
    for block in getattr(result, "content", ()):
        text = getattr(block, "text", None)
        if not isinstance(text, str) or len(text) > 2_097_152:
            continue
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(parsed, dict):
            return parsed
    raise RuntimeError("MCP tool returned no bounded structured object")


async def _collect_mcp(endpoint: str, token: str) -> dict[str, object]:
    import httpx2
    from mcp import ClientSession
    from mcp.client.streamable_http import streamable_http_client

    timeout = httpx2.Timeout(20.0, connect=5.0)
    async with httpx2.AsyncClient(
        headers={"Authorization": f"Bearer {token}"},
        follow_redirects=False,
        timeout=timeout,
    ) as client, streamable_http_client(endpoint, http_client=client) as streams:
        read_stream, write_stream = streams
        async with ClientSession(read_stream, write_stream, read_timeout_seconds=20) as session:
            await session.initialize()
            catalog = await session.list_tools()
            values: dict[str, object] = {"tools": {tool.name for tool in catalog.tools}}
            for tool, key, arguments in (
                ("platform.status", "platform", {}),
                ("master.status", "master", {}),
                ("checkpoint.status", "checkpoint", {}),
                ("embedding.coverage", "embedding", {}),
                ("provider.resources.status", "provider", {"limit": 100}),
            ):
                result = await session.call_tool(tool, arguments)
                if result.isError:
                    raise RuntimeError(f"MCP {tool} returned an error")
                values[key] = _structured_result(result)
            return values


def _negative_http_status(endpoint: str, authorization: str | None) -> int:
    body = canonical_json_bytes(
        {
            "jsonrpc": "2.0",
            "id": "scheduled-auth-negative",
            "method": "initialize",
            "params": {
                "protocolVersion": "2026-07-28",
                "capabilities": {},
                "clientInfo": {"name": "my-data-hub-scheduled-acceptance", "version": "1"},
            },
        }
    )
    headers = {
        "Accept": "application/json, text/event-stream",
        "Content-Type": "application/json",
        "User-Agent": "my-data-hub-scheduled-acceptance/1",
    }
    if authorization:
        headers["Authorization"] = authorization
    request = urllib.request.Request(endpoint, data=body, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return int(response.status)
    except urllib.error.HTTPError as exc:
        return int(exc.code)


def _load_receipt(path: Path) -> dict[str, object]:
    if not path.is_file() or path.is_symlink() or path.stat().st_size > MAX_RECEIPT_BYTES:
        raise ValueError("lifecycle receipt is absent or unsafe")
    value = json.loads(path.read_bytes())
    if not isinstance(value, dict):
        raise ValueError("lifecycle receipt must be an object")
    return value


def collect_live(
    *,
    endpoint: str,
    token: str,
    expected_commit: str,
    ledger_path: Path,
    lifecycle_paths: Mapping[str, Path],
) -> Observations:
    observations = Observations()
    if not modern_token_configured():
        observations.blockers["kaggle_inventory"] = (
            "KAGGLE_MODERN_API_TOKEN_REQUIRED",
            "KAGGLE_API_TOKEN or ~/.kaggle/access_token",
        )
    else:
        try:
            ledger = ControlLedger(ledger_path)
            adapter = KaggleProviderAdapter.from_environment(
                journal=ControlLedgerKaggleJournal(ledger)
            )
            inventory = BoundedInventory(adapter, ProviderRegistry())
            resources = [
                *inventory.collect(ProviderKind.DATASET),
                *inventory.collect(ProviderKind.NOTEBOOK),
            ]
            observations.live_resources = [
                {
                    "provider_ref": item.provider_ref,
                    "kind": item.kind.value,
                    "private": item.private,
                    "state": item.state,
                }
                for item in resources
            ]
        except Exception as exc:
            observations.blockers["kaggle_inventory"] = (
                "KAGGLE_INVENTORY_UNAVAILABLE",
                f"single KaggleProviderAdapter inventory ({type(exc).__name__})",
            )
    parsed = urlsplit(endpoint)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.path != "/mcp"
        or parsed.query
        or parsed.fragment
        or not token
        or len(expected_commit) != 40
        or any(character not in "0123456789abcdef" for character in expected_commit)
    ):
        observations.blockers["mcp"] = (
            "MCP_SCHEDULED_CREDENTIAL_OR_ENDPOINT_MISSING",
            "exact HTTPS /mcp endpoint, reader token, and deployed commit SHA",
        )
    else:
        try:
            snapshot = asyncio.run(_collect_mcp(endpoint, token))
            observations.mcp_tools = set(snapshot["tools"])  # type: ignore[arg-type]
            observations.platform_status = dict(snapshot["platform"])  # type: ignore[arg-type]
            observations.master_status = dict(snapshot["master"])  # type: ignore[arg-type]
            observations.checkpoint_status = dict(snapshot["checkpoint"])  # type: ignore[arg-type]
            observations.embedding_status = dict(snapshot["embedding"])  # type: ignore[arg-type]
            provider = dict(snapshot["provider"])  # type: ignore[arg-type]
            rows = provider.get("resources")
            if not isinstance(rows, list) or len(rows) > 100:
                raise RuntimeError("provider status is not a bounded resource list")
            observations.registered_resources = [dict(item) for item in rows if isinstance(item, dict)]
            if len(rows) == 100:
                observations.registered_resources = None
                observations.blockers["provider_registry"] = (
                    "PROVIDER_REGISTRY_PAGINATION_API_MISSING",
                    "MCP provider.resources.status completeness cursor/has_more contract",
                )
            deployed = observations.platform_status.get("deployed_commit")
            observations.deployed_commit_matches = deployed == expected_commit
            observations.unauthenticated_http_status = _negative_http_status(endpoint, None)
            observations.invalid_token_http_status = _negative_http_status(
                endpoint,
                "Bearer intentionally-invalid-scheduled-acceptance-token",
            )
        except Exception as exc:
            observations.blockers["mcp"] = (
                "MCP_SCHEDULED_OBSERVATION_UNAVAILABLE",
                f"bounded MCP status/auth probes ({type(exc).__name__})",
            )
    for kind, path in lifecycle_paths.items():
        try:
            observations.lifecycle_receipts[kind] = _load_receipt(path)
        except (OSError, ValueError, json.JSONDecodeError):
            continue
    return observations


def _assert_sanitized(value: object, *, path: str = "receipt") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).casefold()
            if normalized in _FORBIDDEN_RECEIPT_KEYS or any(
                fragment in normalized for fragment in ("password", "secret", "authorization")
            ):
                raise ValueError(f"scheduled receipt contains forbidden key at {path}")
            _assert_sanitized(item, path=f"{path}.{key}")
    elif isinstance(value, (list, tuple)):
        for index, item in enumerate(value):
            _assert_sanitized(item, path=f"{path}[{index}]")
    elif isinstance(value, str):
        lowered = value.casefold()
        if "postgresql://" in lowered or "postgres://" in lowered or "bearer " in lowered:
            raise ValueError(f"scheduled receipt contains credential-shaped text at {path}")


def build_receipt(
    *,
    mode: Mode,
    checks: list[Check],
    commit_sha: str,
    started_at: datetime,
    completed_at: datetime,
    workflow_run_id: str,
) -> dict[str, object]:
    if any(check.outcome is Outcome.FAIL for check in checks):
        outcome = Outcome.FAIL
    elif any(check.outcome is Outcome.BLOCKED for check in checks):
        outcome = Outcome.BLOCKED
    else:
        outcome = Outcome.PASS
    blockers = [
        {
            "check": check.name,
            "code": check.blocker_code,
            "missing_interface": check.missing_interface,
        }
        for check in checks
        if check.outcome is Outcome.BLOCKED
    ]
    receipt: dict[str, object] = {
        "schema_version": RECEIPT_SCHEMA,
        "mode": mode.value,
        "outcome": outcome.value,
        "commit_sha": commit_sha,
        "workflow_run_id": workflow_run_id[:100],
        "started_at": started_at.isoformat(),
        "completed_at": completed_at.isoformat(),
        "checks": [check.payload() for check in checks],
        "blockers": blockers,
        "counts": {
            "pass": sum(check.outcome is Outcome.PASS for check in checks),
            "fail": sum(check.outcome is Outcome.FAIL for check in checks),
            "blocked": sum(check.outcome is Outcome.BLOCKED for check in checks),
        },
    }
    _assert_sanitized(receipt)
    if len(canonical_json_bytes(receipt)) > MAX_RECEIPT_BYTES:
        raise ValueError("scheduled acceptance receipt exceeds its byte cap")
    return receipt


def _exit_code(receipt: Mapping[str, object]) -> int:
    return {
        Outcome.PASS.value: PASS,
        Outcome.FAIL.value: FAIL,
        Outcome.BLOCKED.value: EXTERNAL_BLOCKED,
    }[str(receipt["outcome"])]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("command", choices=("run",))
    parser.add_argument("--mode", choices=tuple(Mode), default=Mode.NIGHTLY.value)
    parser.add_argument("--receipt", type=Path, required=True)
    parser.add_argument(
        "--ledger",
        type=Path,
        default=Path("artifacts/scheduled-provider-effects.sqlite3"),
    )
    parser.add_argument("--dataset-lifecycle-receipt", type=Path)
    parser.add_argument("--notebook-lifecycle-receipt", type=Path)
    parser.add_argument(
        "--freshness-seconds",
        type=int,
        default=int(os.getenv("MY_DATA_HUB_SCHEDULED_FRESHNESS_SECONDS", "93600")),
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mode = Mode(args.mode)
    if not 300 <= args.freshness_seconds <= 604_800:
        raise SystemExit("--freshness-seconds must be between 300 and 604800")
    started = datetime.now(UTC)
    endpoint = os.getenv("MY_DATA_HUB_MCP_CANARY_ENDPOINT", "").strip()
    token = os.getenv("MY_DATA_HUB_MCP_CANARY_TOKEN", "").strip()
    expected_commit = os.getenv("MY_DATA_HUB_EXPECTED_DEPLOY_COMMIT", "").strip()
    lifecycle_paths = {
        kind: path
        for kind, path in {
            "dataset": args.dataset_lifecycle_receipt,
            "notebook": args.notebook_lifecycle_receipt,
        }.items()
        if path is not None
    }
    observations = collect_live(
        endpoint=endpoint,
        token=token,
        expected_commit=expected_commit,
        ledger_path=args.ledger,
        lifecycle_paths=lifecycle_paths,
    )
    checks = evaluate(
        mode,
        observations,
        now=datetime.now(UTC),
        freshness=timedelta(seconds=args.freshness_seconds),
    )
    receipt = build_receipt(
        mode=mode,
        checks=checks,
        commit_sha=(
            expected_commit
            if len(expected_commit) == 40
            and all(character in "0123456789abcdef" for character in expected_commit)
            else "unknown"
        ),
        started_at=started,
        completed_at=datetime.now(UTC),
        workflow_run_id=os.getenv("GITHUB_RUN_ID", "local"),
    )
    if args.receipt.is_symlink():
        raise SystemExit("--receipt must not be a symbolic link")
    args.receipt.parent.mkdir(parents=True, exist_ok=True)
    args.receipt.write_bytes(canonical_json_bytes(receipt))
    print(json.dumps({"outcome": receipt["outcome"], "receipt": str(args.receipt)}, sort_keys=True))
    return _exit_code(receipt)


if __name__ == "__main__":
    raise SystemExit(main())
