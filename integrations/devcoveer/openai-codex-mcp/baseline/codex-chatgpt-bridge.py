#!/home/dev/.local/share/openai-codex-mcp/bridge-venv/bin/python
"""Dual-era MCP bridge from ChatGPT to the legacy Codex MCP server.

Wire protocol handling, including MCP 2026-07-28 server/discover and the
legacy initialize flow, is provided entirely by the official mcp==2.0.0 SDK.
"""

from __future__ import annotations

import hashlib
import asyncio
import difflib
import json
import logging
import os
import queue
import re
import secrets
import signal
import subprocess
import sys
import threading
import time
import unicodedata
from contextlib import suppress
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import anyio
import mcp_types as types
import yaml
from jsonschema import Draft202012Validator
from mcp import Client, StdioServerParameters
from mcp.client.stdio import stdio_client
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from yaml.constructor import ConstructorError
from yaml.resolver import BaseResolver
# importlib-based verification loads this file without automatically adding its
# directory to sys.path. Pin the sibling backend module to the trusted install dir.
_BRIDGE_MODULE_DIR = str(Path(__file__).resolve().parent)
if _BRIDGE_MODULE_DIR not in sys.path:
    sys.path.insert(0, _BRIDGE_MODULE_DIR)
from opencode_backend import (
    DEFAULT_OPENCODE_MODEL as BACKEND_DEFAULT_OPENCODE_MODEL,
    OpenCodeBackend,
    OpenCodeError,
    TASK_ID_RE,
    TaskRegistry,
)

BRIDGE_VERSION = "3.6.1"
LEGACY_WRAPPER = "/home/dev/.local/libexec/openai-codex-mcp/run-codex-mcp-legacy"
# Stable absolute entrypoint maintained alongside the VS Code extension.
# Do not pin the extension's versioned directory: VS Code removes it on update.
CODEX_BINARY = "/home/dev/.local/bin/codex"
OPENCODE_BINARY = "/home/dev/.opencode/bin/opencode"
MODEL_VERIFICATION_FILE = Path(
    "/home/dev/.local/share/openai-codex-mcp/model-verification.json"
)
DEFAULT_OPENCODE_MODEL = "opencode/nemotron-3-ultra-free"
COUNCIL_TEXT_LIMIT = 8_000
COUNCIL_LIST_LIMIT = 6
COUNCIL_ITEM_LIMIT = 600
COUNCIL_SESSION_TIMEOUT = 900.0
COUNCIL_TRANSIENT_RETRY_DELAYS = (5.0, 15.0, 30.0, 60.0)
PAID_COUNCIL_CONFIRMATION_TTL_SECONDS = 600
COUNCIL_PRESETS: dict[str, tuple[dict[str, str], ...]] = {
    "free": (
        {"provider": "opencode", "model": "opencode/nemotron-3-ultra-free"},
        {"provider": "opencode", "model": "opencode/nemotron-3.5-lightning-free"},
        {"provider": "opencode", "model": "opencode/mimo-v2.5-free"},
        {"provider": "opencode", "model": "opencode/muse-spark-1.2-contributor-free"},
        {"provider": "opencode", "model": "opencode/big-pickle"},
        {"provider": "opencode", "model": "opencode/ling-3.0-flash-fin-free"},
    ),
    # Extended spends at most one NVIDIA inference: the NVIDIA model receives
    # the completed free debate as a final expert review rather than joining
    # all three rounds.
    "extended": (
        {"provider": "opencode", "model": "opencode/nemotron-3-ultra-free"},
        {"provider": "opencode", "model": "opencode/nemotron-3.5-lightning-free"},
        {"provider": "opencode", "model": "opencode/mimo-v2.5-free"},
        {"provider": "opencode", "model": "opencode/muse-spark-1.2-contributor-free"},
        {"provider": "opencode", "model": "opencode/big-pickle"},
        {"provider": "opencode", "model": "opencode/ling-3.0-flash-fin-free"},
        {"provider": "nvidia", "model": "nvidia/moonshotai/kimi-k3"},
    ),
    # Pro uses three strong NVIDIA participants throughout the full debate.
    "pro": (
        {"provider": "opencode", "model": "opencode/nemotron-3-ultra-free"},
        {"provider": "nvidia", "model": "nvidia/moonshotai/kimi-k3"},
        {"provider": "nvidia", "model": "nvidia/nvidia/nemotron-3-super-120b-a12b"},
        {"provider": "nvidia", "model": "nvidia/nvidia/nemotron-3-ultra-550b-a55b"},
    ),
}
WRITE_PERMISSION_PROFILE = "devcoveer-write"
PROJECTS_ROOT = Path("/home/dev/projects").resolve(strict=True)
PROJECT_ALIASES_FILE = Path(
    "/home/dev/.config/openai-codex-mcp/project-aliases.yaml"
)
ACCESS_TO_SANDBOX = {"read": "read-only", "write": "workspace-write"}
REASONING_EFFORTS = ("low", "medium", "high", "xhigh", "max", "ultra")
THREAD_METADATA_DIR = Path(
    "/home/dev/.local/share/openai-codex-mcp/thread-metadata"
)
SENSITIVE_ENV = (
    "CONTROL_PLANE_API_KEY",
    "OPENAI_API_KEY",
    "CODEX_API_KEY",
    "OPENAI_ADMIN_KEY",
)
INHERITED_CODEX_CONTEXT_ENV = (
    # A user service can inherit these from the VS Code remote extension.
    # They must never relabel MCP threads as vscode or weaken the bridge's
    # explicitly verified sandbox policy.
    "CODEX_INTERNAL_ORIGINATOR_OVERRIDE",
    "CODEX_PERMISSION_PROFILE",
    "CODEX_SESSION_ID",
    "CODEX_THREAD_ID",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s codex-chatgpt-bridge %(levelname)s %(message)s",
    stream=sys.stderr,
)
log = logging.getLogger("codex-chatgpt-bridge")
TASK_REGISTRY = TaskRegistry()
OPENCODE_BACKEND = OpenCodeBackend()

GENERIC_OUTPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "additionalProperties": True,
}

FIND_PROJECTS_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {"query": {"type": "string", "minLength": 1, "maxLength": 128,
        "description": "Approximate project name, such as events, events bot, or data hub."}},
    "required": ["query"], "additionalProperties": False,
}
MODEL_INPUT_PROPERTY: dict[str, Any] = {
    "type": "string",
    "minLength": 1,
    "maxLength": 255,
    "pattern": r"^[A-Za-z0-9][A-Za-z0-9._:/-]*$",
    "description": (
        "Exact live model selection. Codex IDs look like gpt-5.6-terra; OpenCode "
        "selections include provider, for example opencode/nemotron-3-ultra-free or "
        "nvidia/nvidia/nemotron-3-super-120b-a12b."
    ),
}
PROVIDER_INPUT_PROPERTY: dict[str, Any] = {
    "type": "string", "minLength": 1, "maxLength": 64,
    "pattern": r"^[A-Za-z0-9][A-Za-z0-9._-]*$",
    "description": (
        "Optional exact provider. Omit for normal deterministic routing; use codex, "
        "opencode, nvidia, or another provider shown by list_models to disambiguate."
    ),
}
REASONING_EFFORT_INPUT_PROPERTY: dict[str, Any] = {
    "type": "string",
    "enum": list(REASONING_EFFORTS),
    "description": (
        "Exact Codex reasoning effort requested by the user. Omit it unless the user "
        "explicitly selects an effort. The bridge validates it against the live model catalog."
    ),
}
START_TASK_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "project": {"type": "string", "minLength": 1, "maxLength": 128,
            "description": "Approximate project name; no path is required."},
        "prompt": {"type": "string", "minLength": 1},
        "access": {"type": "string", "enum": ["read", "write"], "default": "read",
            "description": "Use read for analysis/status and write for implementation/fixes."},
        "model": MODEL_INPUT_PROPERTY,
        "provider": PROVIDER_INPUT_PROPERTY,
        "reasoning_effort": REASONING_EFFORT_INPUT_PROPERTY,
    },
    "required": ["project", "prompt"], "additionalProperties": False,
}
LIST_TASKS_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "project": {"type": "string", "minLength": 1, "maxLength": 128},
        "search": {"type": "string", "minLength": 1, "maxLength": 256},
        "limit": {"type": "integer", "minimum": 1, "maximum": 100, "default": 20},
    },
    "additionalProperties": False,
}
READ_TASK_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "task": {"type": "string", "minLength": 1,
            "description": "Thread ID, unique ID prefix, name fragment, or latest."},
        "project": {"type": "string", "minLength": 1, "maxLength": 128},
        "detail": {"type": "string", "enum": ["summary", "full"], "default": "summary",
            "description": "Use summary for status and final result; full only when complete history is explicitly needed."},
    },
    "required": ["task"], "additionalProperties": False,
}
CONTINUE_TASK_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "task": {"type": "string", "minLength": 1,
            "description": "Thread ID, unique ID prefix, name fragment, or latest."},
        "project": {"type": "string", "minLength": 1, "maxLength": 128},
        "prompt": {"type": "string", "minLength": 1,
            "description": (
                "Continuation instructions. For compatibility with conversations that cached "
                "an older tool schema, an exact first line [access:write] or [access:read] "
                "also selects the continuation access when the access field is omitted."
            )},
        "access": {"type": "string", "enum": ["read", "write"],
            "description": (
                "Omit to preserve the task's current access. Set write when a read-only "
                "task moves from diagnosis/planning to implementation/fixes; this upgrades "
                "the same task instead of requiring a competing task. Set read for an "
                "explicit downgrade."
            )},
        "model": MODEL_INPUT_PROPERTY,
        "provider": PROVIDER_INPUT_PROPERTY,
        "reasoning_effort": REASONING_EFFORT_INPUT_PROPERTY,
    },
    "required": ["task", "prompt"], "additionalProperties": False,
}
LIST_MODELS_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "provider": {
            **PROVIDER_INPUT_PROPERTY,
            "description": (
                "Optional provider filter. codex is the native backend; all other values "
                "are discovered from the authenticated localhost OpenCode service."
            ),
        },
        "verified_only": {
            "type": "boolean",
            "default": False,
            "description": (
                "Default false returns every model in the currently connected live catalog. "
                "Set true to restrict providers with a verification receipt to its tested or "
                "live-verified subset. Listing never performs inference."
            ),
        },
    },
    "additionalProperties": False,
}
CANCEL_TASK_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "task": {"type": "string", "minLength": 1,
            "description": "Exact task ID, unique ID prefix, name fragment, or latest."},
        "project": {"type": "string", "minLength": 1, "maxLength": 128},
    },
    "required": ["task"],
    "additionalProperties": False,
}
COUNCIL_RUN_INPUT_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "project": {"type": "string", "minLength": 1, "maxLength": 128},
        "prompt": {"type": "string", "minLength": 1, "maxLength": 50_000},
        "baseline_conclusion": {
            "type": "string", "minLength": 1, "maxLength": 50_000,
            "description": (
                "Optional conclusion ChatGPT formed before the council. When supplied, models "
                "stress-test it and the result document preserves it for an explicit before/after comparison."
            ),
        },
        "tier": {
            "type": "string", "enum": ["free", "extended", "pro"], "default": "free",
            "description": (
                "Council cost policy. free (default) uses only OpenCode Zen; extended uses "
                "the free debate plus one final NVIDIA review; pro uses the strongest "
                "configured NVIDIA participants throughout the debate. extended/pro never "
                "launch on the first call: the tool returns a one-time confirmation token "
                "that may be replayed only after the user explicitly confirms the displayed budget."
            ),
        },
        "paid_confirmation_token": {
            "type": "string", "pattern": r"^pcf_[0-9a-f]{32}$",
            "description": (
                "One-time token returned by a prior confirmation_required response for the exact "
                "same extended/pro request. Never obtain and replay it without a new explicit user confirmation."
            ),
        },
        "participants": {
            "type": "array", "minItems": 2, "maxItems": 8,
            "description": (
                "Optional exact override. Omit to use the tier preset. Overrides still obey "
                "the selected tier's NVIDIA budget and provider policy."
            ),
            "items": {
                "type": "object",
                "properties": {
                    "provider": PROVIDER_INPUT_PROPERTY,
                    "model": MODEL_INPUT_PROPERTY,
                },
                "required": ["provider", "model"], "additionalProperties": False,
            },
        },
        "rounds": {"type": "integer", "enum": [1, 2], "default": 2},
        "mode": {"type": "string", "enum": ["debate"], "default": "debate"},
    },
    "required": ["prompt"], "additionalProperties": False,
}

TOOLS = [
    types.Tool(name="list_models", title="List available models",
        description=("List provider-filtered models available to DevCoveer. Codex models come "
            "from the live native app-server catalog. OpenCode Zen models come from the live "
            "OpenCode catalog. verified_only=false (default) lists the current connected catalog; "
            "verified_only=true applies available verification receipts. This tool never performs "
            "inference or exposes credentials. NVIDIA entries are always marked limited/non-free, "
            "and the default free OpenCode model is explicitly identified."),
        input_schema=LIST_MODELS_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=True, destructive_hint=False,
            idempotent_hint=True, open_world_hint=True)),
    types.Tool(name="find_projects", title="Find projects",
        description=("Resolve a natural project hint against direct Git repositories under "
            "/home/dev/projects. Use this when a project name is unclear; ambiguity is "
            "reported instead of choosing a weak match."),
        input_schema=FIND_PROJECTS_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=True, destructive_hint=False,
            idempotent_hint=True, open_world_hint=False)),
    types.Tool(name="start_task", title="Start Codex task",
        description=("Start a named Codex task asynchronously in the uniquely resolved physical project. "
            "This returns threadId and turnId immediately while Codex continues in the background. Infer "
            "access=read for analysis, review, reading, planning, and status; infer "
            "access=write for implementation, fixes, creation, and updates. The user "
            "does not need to provide a path, sandbox, approval policy, or thread ID. "
            "When the user explicitly names a Codex model or reasoning level, pass the exact "
            "model and reasoning_effort fields; never treat those words as prompt-only hints. "
            "Use read_task later to check status and obtain the result."),
        input_schema=START_TASK_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=False, destructive_hint=False,
            idempotent_hint=False, open_world_hint=True)),
    types.Tool(name="list_tasks", title="List Codex tasks",
        description=("List native Codex task history, optionally filtered by approximate project "
            "and task-name text. Use returned task references for follow-up requests."),
        input_schema=LIST_TASKS_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=True, destructive_hint=False,
            idempotent_hint=True, open_world_hint=False)),
    types.Tool(name="read_task", title="Read Codex task",
        description=("Check a background task and read its saved native Codex result without "
            "resuming or changing it. The task may be latest, a full/partial ID, or a name "
            "fragment. Default summary returns runtime status, errors, and final agent result; "
            "use detail=full only when the complete transcript is explicitly requested."),
        input_schema=READ_TASK_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=True, destructive_hint=False,
            idempotent_hint=True, open_world_hint=False)),
    types.Tool(name="continue_task", title="Continue Codex task",
        description=("Continue one uniquely resolved native Codex task asynchronously using its "
            "saved project and access context. When intent changes from analysis/planning to "
            "implementation/fixes, pass access=write to upgrade this same task; do not create a "
            "second task merely to obtain write access. Omit access to preserve the current "
            "policy. For an active compatible turn this steers that same turn; for a terminal "
            "task it starts a new turn. Codex continues in the background. Do not ask the user "
            "for cwd, approval policy, "
            "or threadId. Preserve the task model/reasoning unless the user explicitly requests "
            "new model or reasoning_effort values; use read_task later for status and result."),
        input_schema=CONTINUE_TASK_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=False, destructive_hint=False,
            idempotent_hint=False, open_world_hint=True)),
    types.Tool(name="cancel_task", title="Cancel active Codex task",
        description=("Interrupt the active turn of one uniquely resolved Codex task without "
            "deleting its thread or history. After cancellation, use continue_task to resume "
            "the same task with corrected instructions. Calling this for a terminal task is "
            "an idempotent no-op."),
        input_schema=CANCEL_TASK_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=False, destructive_hint=True,
            idempotent_hint=True, open_world_hint=False)),
    types.Tool(name="council_run", title="Run multi-model council",
        description=("Start a bounded, read-only multi-model council. tier=free is the default "
            "and performs no NVIDIA inference. Never supplement a free council with start_task, "
            "Codex, NVIDIA, or any outside model unless the user separately asks for that. "
            "tier=extended runs the free debate plus one target NVIDIA expert review; tier=pro "
            "uses strong NVIDIA models throughout the full debate. Paid tiers always require a "
            "two-step confirmation: the first call performs no inference and returns a one-time "
            "token plus the budget; show that plan to the user and wait for explicit confirmation "
            "before replaying the exact request with paid_confirmation_token. Participants are selected by the tier preset "
            "unless an exact 2-8 model override is supplied; overrides cannot exceed the tier "
            "budget. Retryable provider queue/rate-limit failures are retried with bounded backoff; "
            "terminal errors such as HTTP 410 are not retried. Results are an attributed structured document designed for ChatGPT to "
            "compare with its prior conclusion, identify the strongest model contribution, "
            "and state what changed. There is no majority vote, hidden reasoning request, or "
            "silent fallback. Poll the returned taskId with read_task."),
        input_schema=COUNCIL_RUN_INPUT_SCHEMA, output_schema=GENERIC_OUTPUT_SCHEMA,
        annotations=types.ToolAnnotations(read_only_hint=False, destructive_hint=False,
            idempotent_hint=False, open_world_hint=True)),
]
VALIDATORS = {
    "list_models": Draft202012Validator(LIST_MODELS_INPUT_SCHEMA),
    "find_projects": Draft202012Validator(FIND_PROJECTS_INPUT_SCHEMA),
    "start_task": Draft202012Validator(START_TASK_INPUT_SCHEMA),
    "list_tasks": Draft202012Validator(LIST_TASKS_INPUT_SCHEMA),
    "read_task": Draft202012Validator(READ_TASK_INPUT_SCHEMA),
    "continue_task": Draft202012Validator(CONTINUE_TASK_INPUT_SCHEMA),
    "cancel_task": Draft202012Validator(CANCEL_TASK_INPUT_SCHEMA),
    "council_run": Draft202012Validator(COUNCIL_RUN_INPUT_SCHEMA),
}


def _invalid_params(field_name: str, message: str = "Invalid tool parameters") -> MCPError:
    return MCPError(code=types.INVALID_PARAMS, message=message, data={"field": field_name})


def _validate_schema(name: str, arguments: dict[str, Any]) -> None:
    errors = sorted(VALIDATORS[name].iter_errors(arguments), key=lambda e: list(e.absolute_path))
    if errors:
        path = list(errors[0].absolute_path)
        raise _invalid_params(str(path[0]) if path else "$input", errors[0].message)


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """Safe YAML loader that rejects duplicate keys instead of choosing one."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader, node: yaml.MappingNode, deep: bool = False
) -> dict[Any, Any]:
    mapping: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in mapping
        except TypeError as exc:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                "found an unhashable mapping key",
                key_node.start_mark,
            ) from exc
        if duplicate:
            raise ConstructorError(
                "while constructing a mapping",
                node.start_mark,
                f"found duplicate key {key!r}",
                key_node.start_mark,
            )
        mapping[key] = loader.construct_object(value_node, deep=deep)
    return mapping


_UniqueKeySafeLoader.add_constructor(
    BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


@dataclass(frozen=True, slots=True)
class ProjectEntry:
    canonical: str
    directory_name: str
    path: str
    aliases: tuple[str, ...] = ()


def _validate_project_hint(project: str) -> str:
    if not isinstance(project, str):
        raise _invalid_params("project", "project hint must be a string")
    value = project.strip()
    if not value or len(value) > 128:
        raise _invalid_params("project", "project hint must contain 1 to 128 characters")
    if ".." in value or "/" in value or "\\" in value:
        raise _invalid_params("project", "project hint must not contain '..' or slashes")
    if any(unicodedata.category(char).startswith("C") for char in value):
        raise _invalid_params("project", "project hint must not contain control characters")
    return value


def _validate_project_name(project: str) -> str:
    """Backward-compatible metadata validator; external callers use project hints."""
    return _validate_project_hint(project)


def _normal_tokens(value: str) -> tuple[str, ...]:
    normalized = unicodedata.normalize("NFKC", value).casefold()
    return tuple(re.findall(r"[\w]+", normalized, flags=re.UNICODE))


def _normal_name(value: str) -> str:
    return " ".join(_normal_tokens(value))


def _load_project_aliases() -> list[tuple[str, str]]:
    if not PROJECT_ALIASES_FILE.exists():
        return []
    if PROJECT_ALIASES_FILE.is_symlink() or not PROJECT_ALIASES_FILE.is_file():
        raise MCPError(code=types.INTERNAL_ERROR,
            message="project aliases configuration must be a regular file")
    try:
        document = yaml.load(PROJECT_ALIASES_FILE.read_text(encoding="utf-8"),
            Loader=_UniqueKeySafeLoader)
    except (OSError, UnicodeError, yaml.YAMLError) as exc:
        log.error("project aliases configuration is invalid (%s)", type(exc).__name__)
        raise MCPError(code=types.INTERNAL_ERROR,
            message="project aliases configuration is invalid") from None
    if not isinstance(document, dict) or set(document) != {"aliases"}:
        raise MCPError(code=types.INTERNAL_ERROR,
            message="project aliases configuration must contain only an aliases mapping")
    aliases = document["aliases"]
    if aliases is None:
        return []
    if not isinstance(aliases, dict):
        raise MCPError(code=types.INTERNAL_ERROR,
            message="project aliases configuration has a non-mapping aliases value")
    result: list[tuple[str, str]] = []
    seen: set[str] = set()
    for alias, target in aliases.items():
        if not isinstance(alias, str) or not isinstance(target, str):
            raise MCPError(code=types.INTERNAL_ERROR,
                message="every project alias and target must be a string")
        _validate_project_hint(alias)
        key = _normal_name(alias)
        if not key or key in seen:
            raise MCPError(code=types.INTERNAL_ERROR,
                message=f"duplicate normalized project alias: {alias!r}")
        seen.add(key)
        if not Path(target).is_absolute():
            raise MCPError(code=types.INTERNAL_ERROR,
                message=f"project alias {alias!r} must use an absolute target")
        result.append((alias, target))
    return result


def _validated_project_path(candidate: Path, project: str) -> str:
    if candidate.is_symlink():
        raise _invalid_params("project", f"project {project!r} must not be a symlink")
    try:
        resolved = candidate.resolve(strict=True)
    except (FileNotFoundError, OSError):
        raise _invalid_params("project", f"unknown project: {project}") from None
    if not resolved.is_dir() or resolved.parent != PROJECTS_ROOT:
        raise _invalid_params("project",
            "project must resolve to a direct child directory of /home/dev/projects")
    try:
        completed = subprocess.run(
            ["git", "-C", str(resolved), "rev-parse", "--show-toplevel", "--is-bare-repository"],
            check=False, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=10, env=_child_environment())
    except (OSError, subprocess.SubprocessError):
        raise _invalid_params("project", f"project {project!r} could not be validated as Git") from None
    lines = completed.stdout.splitlines()
    if completed.returncode != 0 or len(lines) < 2 or lines[-1].strip() != "false":
        raise _invalid_params("project", f"project {project!r} is not a Git working repository")
    try:
        git_root = Path(lines[0].strip()).resolve(strict=True)
    except (FileNotFoundError, OSError):
        raise _invalid_params("project", f"project {project!r} has an invalid Git root") from None
    if git_root != resolved:
        raise _invalid_params("project", f"project {project!r} is not an exact Git repository root")
    return str(resolved)


def _project_catalog() -> list[ProjectEntry]:
    raw: list[tuple[str, str]] = []
    try:
        entries = sorted(os.scandir(PROJECTS_ROOT), key=lambda item: item.name.casefold())
    except OSError as exc:
        raise MCPError(code=types.INTERNAL_ERROR, message="project catalog is unavailable") from exc
    for item in entries:
        if item.is_symlink() or not item.is_dir(follow_symlinks=False):
            continue
        try:
            path = _validated_project_path(Path(item.path), item.name)
        except MCPError:
            continue
        raw.append((item.name, path))
    by_path = {path: name for name, path in raw}
    aliases_by_path: dict[str, list[str]] = {path: [] for path in by_path}
    for alias, target in _load_project_aliases():
        try:
            resolved = _validated_project_path(Path(target), alias)
        except MCPError as exc:
            raise MCPError(code=types.INTERNAL_ERROR,
                message=f"project alias {alias!r} has an invalid target") from exc
        if resolved not in by_path:
            raise MCPError(code=types.INTERNAL_ERROR,
                message=f"project alias {alias!r} is outside the project catalog")
        aliases_by_path[resolved].append(alias)
    catalog: list[ProjectEntry] = []
    for path, directory_name in sorted(by_path.items(), key=lambda item: item[1].casefold()):
        aliases = tuple(aliases_by_path[path])
        canonical = aliases[0] if aliases else directory_name
        catalog.append(ProjectEntry(canonical, directory_name, path, aliases))
    return catalog


def _candidate(entry: ProjectEntry, score: float | None = None) -> dict[str, Any]:
    value: dict[str, Any] = {
        "project": entry.canonical,
        "directory": entry.directory_name,
        "cwd": entry.path,
    }
    if entry.aliases:
        value["aliases"] = list(entry.aliases)
    if score is not None:
        value["score"] = round(score, 3)
    return value


def _match_projects(raw_hint: str) -> tuple[str, list[ProjectEntry]]:
    hint = _validate_project_hint(raw_hint)
    catalog = _project_catalog()
    query = _normal_name(hint)
    query_tokens = set(_normal_tokens(hint))
    if not query:
        raise _invalid_params("project", "project hint must contain letters or numbers")

    alias_exact = [entry for entry in catalog if any(_normal_name(a) == query for a in entry.aliases)]
    if alias_exact:
        return "exact", alias_exact

    exact = [entry for entry in catalog if query in {
        _normal_name(entry.canonical), _normal_name(entry.directory_name)}]
    if exact:
        return "exact", exact

    token_matches: list[ProjectEntry] = []
    # One- and two-character prefixes are too weak to select a repository
    # silently. Exact configured aliases above remain authoritative.
    strong_tokens = bool(query_tokens) and all(len(token) >= 3 for token in query_tokens)
    if strong_tokens:
        for entry in catalog:
            labels = (entry.canonical, entry.directory_name, *entry.aliases)
            matched = False
            for label in labels:
                tokens = set(_normal_tokens(label))
                if query_tokens.issubset(tokens):
                    matched = True
                elif all(any(token.startswith(q) or q.startswith(token)
                        for token in tokens) for q in query_tokens):
                    matched = True
                if matched:
                    break
            if matched:
                token_matches.append(entry)
    if token_matches:
        return "token", token_matches

    scored: list[tuple[float, ProjectEntry]] = []
    for entry in catalog:
        labels = (entry.canonical, entry.directory_name, *entry.aliases)
        score = max(difflib.SequenceMatcher(None, query, _normal_name(label)).ratio()
                    for label in labels)
        if score >= 0.88:
            scored.append((score, entry))
    scored.sort(key=lambda pair: (-pair[0], pair[1].canonical.casefold()))
    if not scored:
        return "not_found", []
    if len(scored) == 1 or scored[0][0] - scored[1][0] >= 0.12:
        return "fuzzy", [scored[0][1]]
    return "fuzzy", [entry for _score, entry in scored[:5]]


def _resolve_project_entry(raw_hint: str) -> ProjectEntry:
    tier, matches = _match_projects(raw_hint)
    if len(matches) == 1:
        return matches[0]
    if not matches:
        raise _invalid_params("project", f"unknown project: {raw_hint}")
    names = ", ".join(entry.canonical for entry in matches[:5])
    raise _invalid_params("project",
        f"ambiguous project {raw_hint!r}; candidates: {names}")


def _resolve_project(raw_project: str) -> str:
    return _resolve_project_entry(raw_project).path


def _first_prompt(project: str, prompt: str, access: str) -> str:
    prefix = f"Source: ChatGPT external control\nProject: {project}\n\n"
    if access != "write":
        return prefix + prompt
    safeguards = (
        "Write-access safeguards:\n"
        "- Before changing anything, run and record `git status --short --branch`.\n"
        "- Preserve all existing user changes and never overwrite unexpected parallel changes.\n"
        "- Re-check relevant files immediately before editing.\n"
        "- Work directly in this repository; do not create a clone, checkout copy, "
        "worktree, or separate workspace.\n"
        "- Never read or expose .env files, credentials, tokens, or personal sessions.\n"
        "- Do not commit, push, or deploy unless the user explicitly requests it.\n\n"
    )
    return prefix + safeguards + prompt


def _child_environment() -> dict[str, str]:
    env = os.environ.copy()
    for key in list(env):
        if (
            key.endswith("_API_KEY")
            or key in SENSITIVE_ENV
            or key in INHERITED_CODEX_CONTEXT_ENV
        ):
            env.pop(key, None)
    env["HOME"] = "/home/dev"
    env["CODEX_HOME"] = "/home/dev/.codex"
    return env


def _thread_metadata_path(thread_id: str) -> Path:
    digest = hashlib.sha256(thread_id.encode("utf-8")).hexdigest()
    return THREAD_METADATA_DIR / f"{digest}.json"


def _save_thread_metadata(
    thread_id: str,
    *,
    project: str,
    access: str,
    cwd: str,
    sandbox: str,
    permissions: str | None = None,
    model: str | None = None,
    reasoning_effort: str | None,
) -> None:
    THREAD_METADATA_DIR.mkdir(parents=True, exist_ok=True, mode=0o700)
    os.chmod(THREAD_METADATA_DIR, 0o700)
    target = _thread_metadata_path(thread_id)
    temporary = target.with_name(f".{target.name}.{os.getpid()}.tmp")
    payload = {
        "threadId": thread_id,
        "project": project,
        "access": access,
        "cwd": cwd,
        "sandbox": sandbox,
        "permissions": permissions,
        "model": model,
        "reasoningEffort": reasoning_effort,
    }
    flags = os.O_WRONLY | os.O_CREAT | os.O_TRUNC
    descriptor = os.open(temporary, flags, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(payload, handle, ensure_ascii=False, separators=(",", ":"))
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, target)
        os.chmod(target, 0o600)
    finally:
        with suppress(FileNotFoundError):
            temporary.unlink()


def _load_thread_metadata(thread_id: str) -> dict[str, Any] | None:
    try:
        payload = json.loads(_thread_metadata_path(thread_id).read_text(encoding="utf-8"))
    except (FileNotFoundError, OSError, ValueError, TypeError):
        return None
    if not isinstance(payload, dict) or payload.get("threadId") != thread_id:
        return None
    cwd = payload.get("cwd")
    sandbox = payload.get("sandbox")
    permissions = payload.get("permissions")
    model = payload.get("model")
    reasoning_effort = payload.get("reasoningEffort")
    project = payload.get("project")
    access = payload.get("access")
    if not isinstance(cwd, str) or sandbox not in ("read-only", "workspace-write"):
        return None
    if permissions is not None and permissions != WRITE_PERMISSION_PROFILE:
        return None
    if model is not None and (
        not isinstance(model, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", model)
    ):
        return None
    if reasoning_effort is not None and reasoning_effort not in REASONING_EFFORTS:
        return None
    if project is None:
        project = Path(cwd).name
    if access is None:
        access = "read" if sandbox == "read-only" else "write"
    if not isinstance(project, str) or access not in ACCESS_TO_SANDBOX:
        return None
    if ACCESS_TO_SANDBOX[access] != sandbox:
        return None
    if permissions is not None and access != "write":
        return None
    try:
        _validate_project_name(project)
        payload["cwd"] = _validated_project_path(Path(cwd), project)
    except MCPError:
        return None
    payload["project"] = project
    payload["access"] = access
    return payload


def _continuation_metadata(
    metadata: dict[str, Any], requested_access: str | None
) -> dict[str, Any]:
    """Return the fixed policy for a continuation, optionally changing access in-place."""

    if requested_access is None:
        return dict(metadata)
    if requested_access not in ACCESS_TO_SANDBOX:
        raise ValueError(f"unsupported continuation access: {requested_access}")
    updated = dict(metadata)
    updated["access"] = requested_access
    updated["sandbox"] = ACCESS_TO_SANDBOX[requested_access]
    # The installed app-server does not yet expose the experimental
    # ``permissions`` lifecycle field. Select the trusted named profile via
    # the request's config layer instead; unlike legacy workspace-write, that
    # profile explicitly permits Git metadata while denying environment files.
    updated["permissions"] = (
        WRITE_PERMISSION_PROFILE if requested_access == "write" else None
    )
    return updated


def _continuation_access(arguments: dict[str, Any]) -> str | None:
    """Resolve access, including a strict prompt marker for cached MCP schemas.

    ChatGPT conversations can retain an older tool schema after the bridge is
    upgraded. Such clients cannot send the new ``access`` argument, but they
    can still transmit the prompt. Only an exact first-line marker is honored;
    ordinary natural-language requests never silently change sandbox access.
    """

    explicit = arguments.get("access")
    if explicit is not None:
        return explicit
    first_line = arguments["prompt"].splitlines()[0].strip().casefold()
    if first_line == "[access:write]":
        return "write"
    if first_line == "[access:read]":
        return "read"
    return None


def _session_not_found(result: types.CallToolResult) -> bool:
    if not result.is_error:
        return False
    for item in result.content:
        if isinstance(item, types.TextContent) and item.text.startswith(
            "Session not found for thread_id:"
        ):
            return True
    return False


async def _resume_persisted_thread(
    thread_id: str,
    prompt: str,
    metadata: dict[str, Any],
) -> types.CallToolResult:
    """Use Codex's official headless resume path when legacy MCP lost RAM state."""

    command = [
        CODEX_BINARY,
        "exec",
        "--json",
        "--color",
        "never",
        "-C",
        metadata["cwd"],
        "-s",
        metadata["sandbox"],
        "-c",
        'approval_policy="never"',
        "--skip-git-repo-check",
    ]
    if metadata.get("model"):
        command.extend(["-m", metadata["model"]])
    command.extend(["resume", thread_id, "-"])

    try:
        with anyio.fail_after(3600):
            completed = await anyio.run_process(
                command,
                input=prompt.encode("utf-8"),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                cwd=metadata["cwd"],
                env=_child_environment(),
            )
    except TimeoutError:
        return types.CallToolResult(
            content=[types.TextContent(type="text", text="Codex session resume timed out")],
            structured_content={"threadId": thread_id, "content": "Codex session resume timed out"},
            is_error=True,
        )
    except BaseException as exc:
        log.error("official Codex resume failed (%s)", type(exc).__name__)
        return types.CallToolResult(
            content=[types.TextContent(type="text", text="Codex session resume failed")],
            structured_content={"threadId": thread_id, "content": "Codex session resume failed"},
            is_error=True,
        )

    resumed_ids: list[str] = []
    agent_messages: list[str] = []
    for raw_line in completed.stdout.splitlines():
        try:
            event = json.loads(raw_line)
        except (ValueError, TypeError):
            continue
        if event.get("type") == "thread.started" and isinstance(event.get("thread_id"), str):
            resumed_ids.append(event["thread_id"])
        item = event.get("item")
        if (
            event.get("type") == "item.completed"
            and isinstance(item, dict)
            and item.get("type") == "agent_message"
            and isinstance(item.get("text"), str)
        ):
            agent_messages.append(item["text"])

    if completed.returncode != 0 or resumed_ids != [thread_id] or not agent_messages:
        message = "Codex session resume did not complete successfully"
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=message)],
            structured_content={"threadId": thread_id, "content": message},
            is_error=True,
        )

    content = agent_messages[-1]
    log.info("persisted Codex thread resumed through the official Codex CLI")
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=content)],
        structured_content={"threadId": thread_id, "content": content},
    )


@dataclass(slots=True)
class PendingCall:
    name: str
    arguments: dict[str, Any]
    done: anyio.Event = field(default_factory=anyio.Event)
    result: types.CallToolResult | None = None
    error: MCPError | None = None


class LegacyCodexBroker:
    """Single-owner broker for a persistent, legacy-only Codex MCP child."""

    def __init__(self, send_stream: anyio.abc.ObjectSendStream[PendingCall]) -> None:
        self._send_stream = send_stream
        self._client: Client | None = None

    async def call(self, name: str, arguments: dict[str, Any]) -> types.CallToolResult:
        request = PendingCall(name=name, arguments=arguments)
        try:
            await self._send_stream.send(request)
        except (anyio.BrokenResourceError, anyio.ClosedResourceError):
            raise MCPError(code=types.CONNECTION_CLOSED, message="Codex bridge is shutting down") from None
        await request.done.wait()
        if request.error is not None:
            raise MCPError.from_error_data(request.error.error)
        if request.result is None:
            raise MCPError(code=types.INTERNAL_ERROR, message="Codex bridge returned no result")
        return request.result

    async def _start_child(self) -> None:
        if self._client is not None:
            return
        params = StdioServerParameters(
            command=LEGACY_WRAPPER,
            args=[],
            env=_child_environment(),
            cwd="/home/dev",
        )
        client = Client(
            stdio_client(params),
            mode="legacy",
            cache=None,
            read_timeout_seconds=3600,
        )
        try:
            await client.__aenter__()
            listed = await client.list_tools(cache_mode="bypass")
            names = {tool.name for tool in listed.tools}
            required = {"codex", "codex-reply"}
            if not required.issubset(names):
                raise RuntimeError("legacy Codex server does not expose the required tools")
        except BaseException:
            # Client.__aenter__ owns and unwinds its partial ExitStack on failure.
            raise
        self._client = client
        log.info("legacy Codex child connected in MCP mode %s", client.protocol_version)

    async def _stop_child(self) -> None:
        client, self._client = self._client, None
        if client is None:
            return
        # The official stdio transport performs its own shielded, bounded shutdown.
        # Do not add another CancelScope here: AnyIO context stacks must unwind LIFO.
        with suppress(BaseException):
            await client.__aexit__(None, None, None)

    async def _restart_child(self) -> None:
        await self._stop_child()
        await self._start_child()
        log.info("legacy Codex child restarted")

    async def _preflight(self) -> None:
        await self._start_child()
        assert self._client is not None
        try:
            await self._client.list_tools(cache_mode="bypass")
        except MCPError as exc:
            if exc.code != types.CONNECTION_CLOSED:
                raise
            log.warning("legacy Codex child connection closed; restarting before tool call")
            await self._restart_child()
            assert self._client is not None
            await self._client.list_tools(cache_mode="bypass")

    async def _execute(self, request: PendingCall) -> None:
        try:
            await self._preflight()
            assert self._client is not None
            request.result = await self._client.call_tool(request.name, request.arguments)
        except MCPError as exc:
            if exc.code == types.CONNECTION_CLOSED:
                # Restore availability, but never replay a possibly state-changing call.
                log.warning("legacy Codex child exited during tool call; restarting without replay")
                with suppress(BaseException):
                    await self._restart_child()
            request.error = exc
        except BaseException as exc:
            log.error("legacy Codex broker failure (%s); restarting", type(exc).__name__)
            with suppress(BaseException):
                await self._restart_child()
            request.error = MCPError(code=types.INTERNAL_ERROR, message="Legacy Codex bridge failure")
        finally:
            request.done.set()

    async def _idle_healthcheck(self) -> None:
        try:
            await self._start_child()
            assert self._client is not None
            await self._client.send_ping()
        except MCPError as exc:
            if exc.code == types.CONNECTION_CLOSED:
                log.warning("legacy Codex child exited while idle; restarting")
                with suppress(BaseException):
                    await self._restart_child()
        except BaseException as exc:
            log.error("legacy Codex idle check failed (%s); restarting", type(exc).__name__)
            with suppress(BaseException):
                await self._restart_child()

    async def run(
        self,
        receive_stream: anyio.abc.ObjectReceiveStream[PendingCall],
        *,
        task_status: anyio.abc.TaskStatus[None] = anyio.TASK_STATUS_IGNORED,
    ) -> None:
        try:
            try:
                await self._start_child()
            except BaseException as exc:
                log.error("initial legacy Codex child start failed (%s); will retry", type(exc).__name__)
            task_status.started()
            async with receive_stream:
                while True:
                    request: PendingCall | None = None
                    with anyio.move_on_after(30) as timeout_scope:
                        try:
                            request = await receive_stream.receive()
                        except anyio.EndOfStream:
                            return
                    if timeout_scope.cancel_called:
                        await self._idle_healthcheck()
                    elif request is not None:
                        await self._execute(request)
        finally:
            await self._stop_child()


class _LifecycleGate:
    """Allow concurrent app-server reads while making restarts exclusive.

    Native app-server JSON-RPC is multiplexed by request id, so ordinary
    requests are safe to run concurrently.  A policy-change restart is the
    exception: it must not close stdio while another request is in flight.
    """

    def __init__(self) -> None:
        self._condition = threading.Condition()
        self._readers = 0
        self._writer = False
        self._writers_waiting = 0

    def enter_shared(self) -> None:
        with self._condition:
            while self._writer or self._writers_waiting:
                self._condition.wait()
            self._readers += 1

    def exit_shared(self) -> None:
        with self._condition:
            self._readers -= 1
            self._condition.notify_all()

    def enter_exclusive(self) -> None:
        with self._condition:
            self._writers_waiting += 1
            try:
                while self._writer or self._readers:
                    self._condition.wait()
                self._writer = True
            finally:
                self._writers_waiting -= 1

    def exit_exclusive(self) -> None:
        with self._condition:
            self._writer = False
            self._condition.notify_all()


class NativeAppServerError(RuntimeError):
    """Structured app-server error; transport failures remain distinct."""

    def __init__(self, message: str, *, code: Any = None, data: Any = None) -> None:
        super().__init__(message)
        self.code = code
        self.data = data


class NativeHistoryClient:
    """Persistent, fully drained client for Codex's native app-server API.

    Codex turns are intentionally asynchronous: ``turn/start`` returns while the
    app-server keeps working.  A dedicated reader thread therefore drains every
    notification and demultiplexes responses even when no MCP request is active.
    This prevents a long Codex turn from filling the stdout pipe or holding the
    Secure MCP Tunnel request open.
    """

    _RETRYABLE_METHODS = {"thread/list", "thread/read", "model/list"}

    def __init__(self) -> None:
        self._process: subprocess.Popen[str] | None = None
        self._reader: threading.Thread | None = None
        self._next_id = 1
        self._generation = 0
        self._start_lock = threading.Lock()
        self._process_lock = threading.RLock()
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._lifecycle_gate = _LifecycleGate()
        self._pending: dict[int, queue.Queue[dict[str, Any]]] = {}
        self._loaded_threads: set[str] = set()
        self._thread_status: dict[str, Any] = {}
        self._turn_status: dict[str, dict[str, Any]] = {}

    def _write_for_process(
        self, process: subprocess.Popen[str], payload: dict[str, Any]
    ) -> None:
        with self._write_lock:
            if self._process is not process or process.poll() is not None or process.stdin is None:
                raise ConnectionError("Codex app-server is not running")
            process.stdin.write(
                json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n"
            )
            process.stdin.flush()

    def _write(self, payload: dict[str, Any]) -> None:
        with self._process_lock:
            process = self._process
        if process is None:
            raise ConnectionError("Codex app-server is not running")
        self._write_for_process(process, payload)

    def _record_notification(self, message: dict[str, Any]) -> None:
        method = message.get("method")
        params = message.get("params")
        if not isinstance(method, str) or not isinstance(params, dict):
            return
        thread_id = params.get("threadId")
        if method == "thread/status/changed" and isinstance(thread_id, str):
            self._thread_status[thread_id] = params.get("status")
        elif method in {"turn/started", "turn/completed"} and isinstance(thread_id, str):
            turn = params.get("turn")
            if isinstance(turn, dict):
                self._turn_status[thread_id] = turn
                if method == "turn/completed":
                    log.info(
                        "native Codex turn completed thread=%s turn=%s status=%s",
                        thread_id,
                        turn.get("id"),
                        turn.get("status"),
                    )

    def _fail_pending(self, reason: str) -> None:
        with self._pending_lock:
            pending = list(self._pending.values())
        for response_queue in pending:
            with suppress(queue.Full):
                response_queue.put_nowait({"_bridge_error": reason})

    def _reader_loop(self, process: subprocess.Popen[str], generation: int) -> None:
        reason = "Codex app-server closed stdout"
        try:
            assert process.stdout is not None
            for line in process.stdout:
                try:
                    message = json.loads(line)
                except (ValueError, TypeError):
                    continue
                if not isinstance(message, dict):
                    continue
                incoming_id = message.get("id")
                if incoming_id is not None and not isinstance(message.get("method"), str):
                    with self._pending_lock:
                        response_queue = self._pending.get(incoming_id)
                    if response_queue is not None:
                        response_queue.put(message)
                    continue
                if incoming_id is not None and isinstance(message.get("method"), str):
                    # approval-policy=never and no dynamic tools mean requests from
                    # app-server are unexpected.  Reject instead of hanging it.
                    with suppress(BaseException):
                        self._write_for_process(process, {
                            "jsonrpc": "2.0",
                            "id": incoming_id,
                            "error": {
                                "code": -32601,
                                "message": "Method not supported by bridge client",
                            },
                        })
                    continue
                self._record_notification(message)
        except BaseException as exc:
            reason = f"Codex app-server reader failed: {type(exc).__name__}"
        finally:
            with self._process_lock:
                if self._process is process and self._generation == generation:
                    self._process = None
                    self._loaded_threads.clear()
            self._fail_pending(reason)
            if process.returncode not in (None, 0):
                log.warning("native Codex app-server exited unexpectedly rc=%s", process.returncode)

    def _stop_sync(self) -> None:
        with self._process_lock:
            process, self._process = self._process, None
            reader, self._reader = self._reader, None
            self._loaded_threads.clear()
        if process is None:
            return
        with suppress(Exception):
            if process.stdin:
                process.stdin.close()
        try:
            process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            with suppress(Exception):
                process.terminate()
            try:
                process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                with suppress(Exception):
                    process.kill()
                with suppress(Exception):
                    process.wait(timeout=3)
        if reader is not None and reader is not threading.current_thread():
            reader.join(timeout=3)

    def _exchange_existing(
        self, method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        with self._pending_lock:
            request_id = self._next_id
            self._next_id += 1
            response_queue: queue.Queue[dict[str, Any]] = queue.Queue(maxsize=1)
            self._pending[request_id] = response_queue
        try:
            self._write({
                "jsonrpc": "2.0",
                "id": request_id,
                "method": method,
                "params": params,
            })
            try:
                message = response_queue.get(timeout=timeout)
            except queue.Empty:
                raise TimeoutError(f"Codex app-server {method} request timed out") from None
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        if isinstance(message.get("_bridge_error"), str):
            raise ConnectionError(message["_bridge_error"])
        if isinstance(message.get("error"), dict):
            error = message["error"]
            raise NativeAppServerError(
                str(error.get("message", "Codex app-server error")),
                code=error.get("code"),
                data=error.get("data"),
            )
        result = message.get("result", {})
        if not isinstance(result, dict):
            raise RuntimeError("Codex app-server returned an invalid result")
        return result

    def _start_sync(self) -> None:
        with self._start_lock:
            with self._process_lock:
                if self._process is not None and self._process.poll() is None:
                    return
            self._stop_sync()
            with self._process_lock:
                process = subprocess.Popen(
                    [CODEX_BINARY, "app-server", "--stdio"],
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=None,
                    text=True,
                    bufsize=1,
                    cwd="/home/dev",
                    env=_child_environment(),
                )
                self._process = process
                self._generation += 1
                generation = self._generation
                reader = threading.Thread(
                    target=self._reader_loop,
                    args=(process, generation),
                    name="codex-app-server-reader",
                    daemon=True,
                )
                self._reader = reader
                reader.start()
            try:
                initialized = self._exchange_existing("initialize", {
                    "clientInfo": {
                        "name": "codex-devcoveer-mcp-bridge",
                        "version": BRIDGE_VERSION,
                    },
                    # Descendant-aware cancellation uses the documented
                    # experimental ancestorThreadId filter on thread/list.
                    "capabilities": {"experimentalApi": True},
                })
                if initialized.get("codexHome") != "/home/dev/.codex":
                    raise RuntimeError("Codex app-server used an unexpected CODEX_HOME")
            except BaseException:
                self._stop_sync()
                raise
            log.info("native Codex app-server connected with asynchronous turn draining")

    def _request_sync(
        self, method: str, params: dict[str, Any], timeout: float
    ) -> dict[str, Any]:
        self._lifecycle_gate.enter_shared()
        try:
            attempts = 2 if method in self._RETRYABLE_METHODS else 1
            last_error: BaseException | None = None
            for attempt in range(attempts):
                try:
                    self._start_sync()
                    result = self._exchange_existing(method, params, timeout)
                    thread = result.get("thread")
                    if method in {"thread/start", "thread/resume"} and isinstance(thread, dict):
                        thread_id = thread.get("id")
                        if isinstance(thread_id, str):
                            self._loaded_threads.add(thread_id)
                    return result
                except (BrokenPipeError, ConnectionError, OSError) as exc:
                    last_error = exc
                    self._stop_sync()
                    if attempt + 1 < attempts:
                        log.warning("native Codex app-server exited; restarting read request")
                        continue
                    break
            assert last_error is not None
            raise RuntimeError("native Codex app-server unavailable") from last_error
        finally:
            self._lifecycle_gate.exit_shared()

    async def request(
        self, method: str, params: dict[str, Any], timeout: float = 30.0
    ) -> dict[str, Any]:
        return await anyio.to_thread.run_sync(self._request_sync, method, params, timeout)

    async def ensure_thread_loaded(
        self, thread_id: str, metadata: dict[str, Any], *, force_resume: bool = False
    ) -> dict[str, Any] | None:
        with self._process_lock:
            process_alive = self._process is not None and self._process.poll() is None
            loaded = process_alive and thread_id in self._loaded_threads
        if loaded and not force_resume:
            return None
        return await self.request("thread/resume", self._resume_params(thread_id, metadata))

    @staticmethod
    def _resume_params(thread_id: str, metadata: dict[str, Any]) -> dict[str, Any]:
        params: dict[str, Any] = {
            "threadId": thread_id,
            "cwd": metadata["cwd"],
            "approvalPolicy": "never",
        }
        if metadata.get("permissions") is None:
            params["sandbox"] = metadata["sandbox"]
        if metadata.get("model"):
            params["model"] = metadata["model"]
        fixed_config: dict[str, Any] = {}
        if metadata.get("permissions") == WRITE_PERMISSION_PROFILE:
            fixed_config["default_permissions"] = WRITE_PERMISSION_PROFILE
        if metadata.get("reasoningEffort"):
            fixed_config["model_reasoning_effort"] = metadata["reasoningEffort"]
        if fixed_config:
            params["config"] = fixed_config
        return params

    def cached_status(self, thread_id: str) -> tuple[Any, dict[str, Any] | None]:
        return self._thread_status.get(thread_id), self._turn_status.get(thread_id)

    def _resume_for_policy_change_sync(
        self, thread_id: str, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        """Atomically restart and resume with authoritative profile config.

        This app-server build ignores resume config for an already loaded
        thread. Refuse to restart while any *other* turn is running, because
        stopping the shared process would interrupt unrelated work.  The
        exclusive lifecycle gate stays held through ``thread/resume`` so a
        concurrent read cannot relaunch/load the thread between those steps.
        """

        self._lifecycle_gate.enter_exclusive()
        try:
            with self._process_lock:
                running = [
                    other_id
                    for other_id, turn in self._turn_status.items()
                    if other_id != thread_id and turn.get("status") == "inProgress"
                ]
            if running:
                raise RuntimeError("another Codex turn is running")
            self._stop_sync()
            self._start_sync()
            result = self._exchange_existing(
                "thread/resume", self._resume_params(thread_id, metadata), 30.0
            )
            thread = result.get("thread")
            if isinstance(thread, dict):
                resumed_id = thread.get("id")
                if isinstance(resumed_id, str):
                    self._loaded_threads.add(resumed_id)
            return result
        finally:
            self._lifecycle_gate.exit_exclusive()

    async def resume_for_policy_change(
        self, thread_id: str, metadata: dict[str, Any]
    ) -> dict[str, Any]:
        return await anyio.to_thread.run_sync(
            self._resume_for_policy_change_sync, thread_id, metadata
        )

    async def close(self) -> None:
        await anyio.to_thread.run_sync(self._stop_sync)


def _load_model_verification() -> dict[str, Any]:
    if (
        not MODEL_VERIFICATION_FILE.is_file()
        or MODEL_VERIFICATION_FILE.is_symlink()
    ):
        raise MCPError(
            code=types.INTERNAL_ERROR,
            message="model verification state is unavailable",
        )
    try:
        if MODEL_VERIFICATION_FILE.stat().st_size > 65_536:
            raise ValueError("model verification state is too large")
        document = json.loads(MODEL_VERIFICATION_FILE.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, ValueError, TypeError):
        raise MCPError(
            code=types.INTERNAL_ERROR,
            message="model verification state is invalid",
        ) from None
    if (
        not isinstance(document, dict)
        or document.get("schema") != "devcoveer-model-verification.v1"
        or not isinstance(document.get("providers"), dict)
        or not isinstance(document.get("verifiedAt"), str)
        or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z",
            document["verifiedAt"],
        ) is None
        or document.get("defaultOpenCodeModel") != DEFAULT_OPENCODE_MODEL
    ):
        raise MCPError(
            code=types.INTERNAL_ERROR,
            message="model verification state is invalid",
        )
    providers = document["providers"]
    for provider_name, list_names, prefix in (
        ("opencode", ("inferenceVerified", "free"), "opencode/"),
        ("nvidia", ("inferenceVerified", "liveCatalogVerified"), "nvidia/"),
    ):
        state = providers.get(provider_name)
        if not isinstance(state, dict):
            raise MCPError(code=types.INTERNAL_ERROR,
                message="model verification state is invalid")
        for list_name in list_names:
            values = state.get(list_name)
            if (
                not isinstance(values, list)
                or len(values) > 128
                or any(
                    not isinstance(value, str)
                    or len(value) > 192
                    or not value.startswith(prefix)
                    or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", value) is None
                    for value in values
                )
            ):
                raise MCPError(code=types.INTERNAL_ERROR,
                    message="model verification state is invalid")
        excluded = state.get("excluded", {})
        if (
            not isinstance(excluded, dict)
            or len(excluded) > 128
            or any(
                not isinstance(model, str)
                or not model.startswith(prefix)
                or len(model) > 192
                or re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._/-]*", model) is None
                or not isinstance(reason, str)
                or not reason
                or len(reason) > 500
                for model, reason in excluded.items()
            )
        ):
            raise MCPError(code=types.INTERNAL_ERROR,
                message="model verification state is invalid")
    return document


def _known_model_exclusion(selection: str) -> str | None:
    try:
        document = _load_model_verification()
    except Exception:
        return None
    provider = selection.split("/", 1)[0]
    state = document.get("providers", {}).get(provider, {})
    excluded = state.get("excluded") if isinstance(state, dict) else None
    reason = excluded.get(selection) if isinstance(excluded, dict) else None
    return reason if isinstance(reason, str) else None


def _assert_model_not_excluded(selection: str) -> None:
    reason = _known_model_exclusion(selection)
    if reason is not None:
        raise OpenCodeError("model_unavailable", f"Model is excluded: {reason}", status=410)


def _opencode_discovery_environment() -> dict[str, str]:
    """Return a credential-free environment for catalog-only OpenCode calls."""
    env = os.environ.copy()
    for key in list(env):
        if key.endswith("_API_KEY") or key in SENSITIVE_ENV:
            env.pop(key, None)
    env["HOME"] = "/home/dev"
    return env


async def _opencode_catalog(provider: str) -> list[str]:
    if provider != "opencode":
        raise _invalid_params("provider", "only the OpenCode Zen catalog is queried live")
    try:
        with anyio.fail_after(30):
            completed = await anyio.run_process(
                [OPENCODE_BINARY, "models", provider, "--pure"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                check=False,
                cwd="/tmp",
                env=_opencode_discovery_environment(),
            )
    except TimeoutError:
        raise MCPError(code=types.INTERNAL_ERROR,
            message="OpenCode model discovery timed out") from None
    except (OSError, RuntimeError):
        raise MCPError(code=types.INTERNAL_ERROR,
            message="OpenCode model discovery is unavailable") from None
    if completed.returncode != 0:
        raise MCPError(code=types.INTERNAL_ERROR,
            message="OpenCode model discovery failed")
    if len(completed.stdout) > 262_144:
        raise MCPError(code=types.INTERNAL_ERROR,
            message="OpenCode model discovery returned too much data")
    models: list[str] = []
    for raw in completed.stdout.decode("utf-8", errors="replace").splitlines():
        model = raw.strip()
        if re.fullmatch(r"opencode/[A-Za-z0-9][A-Za-z0-9._/-]{0,190}", model):
            models.append(model)
    return sorted(set(models))


async def _codex_model_records(history: NativeHistoryClient) -> list[dict[str, Any]]:
    data: list[Any] = []
    cursor: str | None = None
    seen: set[str] = set()
    while True:
        result = await history.request("model/list", {
            "cursor": cursor,
            "limit": 100,
            "includeHidden": False,
        })
        page = result.get("data")
        if not isinstance(page, list):
            raise MCPError(code=types.INTERNAL_ERROR,
                message="Codex returned an invalid model catalog")
        data.extend(page)
        if len(data) > 500:
            raise MCPError(code=types.INTERNAL_ERROR,
                message="Codex returned an oversized model catalog")
        next_cursor = result.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
            break
        seen.add(next_cursor)
        cursor = next_cursor
    records: list[dict[str, Any]] = []
    for item in data:
        if not isinstance(item, dict):
            continue
        model_id = item.get("id") or item.get("model")
        if not isinstance(model_id, str):
            continue
        efforts = []
        for effort in item.get("supportedReasoningEfforts") or []:
            if isinstance(effort, dict) and isinstance(effort.get("reasoningEffort"), str):
                efforts.append(effort["reasoningEffort"])
        records.append({
            "id": model_id,
            "provider": "codex",
            "backend": "codex",
            "displayName": item.get("displayName"),
            "description": item.get("description"),
            "reasoningEfforts": efforts,
            "defaultReasoningEffort": item.get("defaultReasoningEffort"),
            "availability": "live_catalog",
            "taskRoutingReady": True,
        })
    return records


async def _model_discovery_payload(
    history: NativeHistoryClient, provider: str | None, verified_only: bool
) -> dict[str, Any]:
    models: list[dict[str, Any]] = []
    warnings: list[str] = []
    provider_status: dict[str, dict[str, Any]] = {}
    verification: dict[str, Any] | None = None
    try:
        verification = _load_model_verification()
    except Exception:
        warnings.append("Inference-verification receipt is unavailable; live catalogs remain visible.")

    if provider in (None, "codex"):
        try:
            codex_models = await _codex_model_records(history)
            models.extend(codex_models)
            provider_status["codex"] = {"status": "available", "count": len(codex_models)}
        except Exception:
            provider_status["codex"] = {"status": "degraded", "error": "catalog_unavailable"}
            warnings.append("The live Codex model catalog is unavailable.")

    if provider != "codex":
        try:
            catalog = await OPENCODE_BACKEND.catalog()
            connected = catalog["connected"]
            requested_providers = connected if provider is None else [provider]
            for provider_id in requested_providers:
                provider_models = [item for item in catalog["models"] if item["provider"] == provider_id]
                if not provider_models:
                    provider_status[provider_id] = {
                        "status": "unavailable", "connected": provider_id in connected, "count": 0,
                    }
                    continue
                receipt_state = (
                    verification.get("providers", {}).get(provider_id, {})
                    if verification is not None else {}
                )
                inferred = set(receipt_state.get("inferenceVerified") or [])
                live_snapshot = set(receipt_state.get("liveCatalogVerified") or [])
                accepted = inferred | live_snapshot
                added = 0
                for item in provider_models:
                    selection = item["selection"]
                    exclusion_reason = (
                        receipt_state.get("excluded", {}).get(selection)
                        if isinstance(receipt_state.get("excluded"), dict) else None
                    )
                    if isinstance(exclusion_reason, str):
                        warnings.append(f"Excluded unavailable model {selection}: {exclusion_reason}")
                        continue
                    verified = selection in inferred
                    snapshot = selection in live_snapshot
                    # For providers with an explicit verification receipt, the
                    # default view is its proven subset. Other connected providers
                    # remain discoverable as live connected catalog entries.
                    if verified_only and accepted and selection not in accepted:
                        continue
                    record = dict(item)
                    record.update({
                        "availability": (
                            "inference_verified" if verified else
                            "provider_live_verified" if snapshot else
                            "connected_catalog_active"
                        ),
                        "verifiedAt": verification.get("verifiedAt") if (verified or snapshot) and verification else None,
                        "default": selection == DEFAULT_OPENCODE_MODEL,
                        "taskRoutingReady": True,
                    })
                    models.append(record)
                    added += 1
                provider_status[provider_id] = {
                    "status": "available", "connected": True, "count": added,
                }
            if provider is not None and provider not in connected:
                provider_status[provider] = {"status": "unavailable", "connected": False, "count": 0}
        except Exception:
            target = provider or "opencode"
            provider_status[target] = {
                "status": "degraded", "connected": False, "error": "catalog_unavailable",
            }
            warnings.append("The localhost OpenCode model catalog is unavailable.")

    overall = "ok" if provider_status and all(
        item.get("status") == "available" for item in provider_status.values()
    ) else "degraded"
    return {
        "status": overall,
        "providerFilter": provider,
        "verifiedOnly": verified_only,
        "defaultOpenCodeModel": DEFAULT_OPENCODE_MODEL,
        "verifiedAt": verification.get("verifiedAt") if verification else None,
        "probesPerformed": False,
        "providers": provider_status,
        "count": len(models),
        "models": models,
        "warnings": warnings,
        "content": f"Found {len(models)} models. OpenCode default: {DEFAULT_OPENCODE_MODEL}.",
    }


async def _resolve_model_selection(
    history: NativeHistoryClient,
    requested_model: str | None,
    requested_effort: str | None,
) -> tuple[str | None, str | None]:
    """Validate an allowlisted selection against Codex's live model catalog."""
    if requested_model is None and requested_effort is None:
        return None, None
    models: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    while True:
        result = await history.request("model/list", {
            "cursor": cursor,
            "limit": 100,
            "includeHidden": False,
        })
        data = result.get("data")
        if not isinstance(data, list):
            raise MCPError(code=types.INTERNAL_ERROR,
                message="Codex returned an invalid model catalog")
        models.extend(item for item in data if isinstance(item, dict))
        next_cursor = result.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
            break
        seen.add(next_cursor)
        cursor = next_cursor

    selected: list[dict[str, Any]]
    if requested_model is None:
        selected = [item for item in models if item.get("isDefault") is True]
        if len(selected) != 1:
            raise MCPError(code=types.INTERNAL_ERROR,
                message="Codex model catalog has no unique default model")
    else:
        selected = [item for item in models
            if requested_model in {item.get("id"), item.get("model")}]
        if len(selected) != 1:
            raise _invalid_params("model", f"Codex model is unavailable: {requested_model}")
    item = selected[0]
    canonical_model = item.get("model")
    if not isinstance(canonical_model, str) or not canonical_model:
        raise MCPError(code=types.INTERNAL_ERROR,
            message="Codex model catalog returned an invalid model ID")
    if requested_effort is not None:
        options = item.get("supportedReasoningEfforts")
        supported = {
            option.get("reasoningEffort")
            for option in options if isinstance(option, dict)
        } if isinstance(options, list) else set()
        if requested_effort not in supported:
            available = ", ".join(sorted(value for value in supported if isinstance(value, str)))
            raise _invalid_params("reasoning_effort",
                f"Reasoning effort {requested_effort!r} is unavailable for {canonical_model}; "
                f"supported: {available or 'none'}")
    return canonical_model, requested_effort


def _tool_result(payload: dict[str, Any], *, error: bool = False) -> types.CallToolResult:
    text_payload = payload.get("content")
    if not isinstance(text_payload, str):
        text_payload = json.dumps(payload, ensure_ascii=False, indent=2)
    return types.CallToolResult(
        content=[types.TextContent(type="text", text=text_payload)],
        structured_content=payload,
        is_error=error,
    )


def _project_selection(raw_hint: str) -> tuple[ProjectEntry | None, dict[str, Any]]:
    tier, matches = _match_projects(raw_hint)
    candidates = [_candidate(entry) for entry in matches[:5]]
    if len(matches) == 1:
        entry = matches[0]
        return entry, {"status": "resolved", "matchType": tier,
            "project": entry.canonical, "cwd": entry.path, "candidates": candidates}
    if not matches:
        return None, {"status": "not_found", "matchType": tier,
            "message": f"No Git project matches {raw_hint!r}.", "candidates": []}
    return None, {"status": "ambiguous", "matchType": tier,
        "message": f"Project hint {raw_hint!r} is ambiguous; choose one candidate.",
        "candidates": candidates}


def _project_by_cwd(catalog: list[ProjectEntry]) -> dict[str, ProjectEntry]:
    return {entry.path: entry for entry in catalog}


def _runtime_status(thread: dict[str, Any]) -> Any:
    status = thread.get("status")
    if isinstance(status, dict) and status.get("type") not in (None, "notLoaded"):
        return status
    turns = thread.get("turns")
    if isinstance(turns, list) and turns:
        latest = turns[-1]
        if isinstance(latest, dict):
            turn_status = latest.get("status")
            if isinstance(turn_status, str):
                return {"type": turn_status, "turnId": latest.get("id")}
    return status if status is not None else {"type": "unknown"}


def _effective_sandbox_matches(
    result: dict[str, Any], *, cwd: str, sandbox: str,
    permissions: str | None = None,
) -> bool:
    if result.get("cwd") != cwd or result.get("approvalPolicy") != "never":
        return False
    policy = result.get("sandbox")
    if not isinstance(policy, dict):
        return False
    if permissions == WRITE_PERMISSION_PROFILE:
        active = result.get("activePermissionProfile")
        return (
            isinstance(active, dict)
            and active.get("id") == WRITE_PERMISSION_PROFILE
            and policy.get("type") == "workspaceWrite"
            and policy.get("networkAccess") is True
        )
    if sandbox == "read-only":
        return policy.get("type") == "readOnly" and policy.get("networkAccess") is False
    return (
        policy.get("type") == "workspaceWrite"
        and policy.get("networkAccess") is True
    )


def _effective_model_settings(
    result: dict[str, Any],
    *,
    requested_model: str | None,
    requested_effort: str | None,
) -> tuple[str, str | None] | None:
    model = result.get("model")
    effort = result.get("reasoningEffort")
    if (
        not isinstance(model, str)
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", model)
        or (effort is not None and effort not in REASONING_EFFORTS)
    ):
        return None
    if requested_model is not None and model != requested_model:
        return None
    if requested_effort is not None and effort != requested_effort:
        return None
    return model, effort


def _turn_sandbox_policy(sandbox: str, cwd: str | None = None) -> dict[str, Any]:
    if sandbox == "read-only":
        return {"type": "readOnly", "networkAccess": False}
    writable_roots = []
    if cwd is not None:
        writable_roots = [
            cwd,
            str(PROJECTS_ROOT),
            "/home/dev/.codex/worktrees",
        ]
    return {
        "type": "workspaceWrite",
        "writableRoots": writable_roots,
        "networkAccess": True,
        "excludeTmpdirEnvVar": False,
        "excludeSlashTmp": False,
    }


def _last_turn(thread: dict[str, Any]) -> dict[str, Any] | None:
    turns = thread.get("turns")
    if not isinstance(turns, list):
        return None
    for turn in reversed(turns):
        if isinstance(turn, dict):
            return turn
    return None


def _turn_summary(turn: dict[str, Any] | None) -> dict[str, Any] | None:
    if turn is None:
        return None
    final_response: str | None = None
    items = turn.get("items")
    if isinstance(items, list):
        for item in items:
            if (
                isinstance(item, dict)
                and item.get("type") == "agentMessage"
                and isinstance(item.get("text"), str)
            ):
                final_response = item["text"]
    summary: dict[str, Any] = {
        "turnId": turn.get("id"),
        "status": turn.get("status"),
        "startedAt": turn.get("startedAt"),
        "completedAt": turn.get("completedAt"),
        "durationMs": turn.get("durationMs"),
        "error": turn.get("error"),
    }
    if final_response is not None:
        summary["finalResponse"] = final_response
    return summary


def _status_content(summary: dict[str, Any] | None) -> str:
    if summary is None:
        return "Codex task exists but has no turn yet."
    status = summary.get("status")
    if status == "inProgress":
        return "Codex task is still running in the background. Check it again with read_task."
    if isinstance(summary.get("finalResponse"), str):
        return summary["finalResponse"]
    error = summary.get("error")
    if isinstance(error, dict) and isinstance(error.get("message"), str):
        return error["message"]
    return f"Codex task finished with status: {status}."


def _task_references(threads: list[dict[str, Any]]) -> dict[str, str]:
    ids = [str(thread.get("id", "")) for thread in threads]
    references: dict[str, str] = {}
    for thread_id in ids:
        length = 8
        while length < len(thread_id) and sum(other.startswith(thread_id[:length]) for other in ids) > 1:
            length += 1
        references[thread_id] = thread_id[:length]
    return references


def _task_record(thread: dict[str, Any], entry: ProjectEntry, reference: str) -> dict[str, Any]:
    thread_id = str(thread.get("id", ""))
    metadata = _load_thread_metadata(thread_id) or {}
    return {
        "taskReference": reference,
        "threadId": thread_id,
        "name": thread.get("name"),
        "canonicalProject": entry.canonical,
        "cwd": entry.path,
        "createdAt": thread.get("createdAt"),
        "updatedAt": thread.get("updatedAt"),
        "runtimeStatus": _runtime_status(thread),
        "model": metadata.get("model"),
        "reasoningEffort": metadata.get("reasoningEffort"),
        "preview": thread.get("preview", ""),
        "resumeCommand": f"codex resume {thread_id}",
    }


async def _native_threads(
    history: NativeHistoryClient,
    *,
    project: ProjectEntry | None = None,
    search: str | None = None,
    maximum: int = 500,
) -> list[dict[str, Any]]:
    catalog = _project_catalog()
    by_cwd = _project_by_cwd(catalog)
    collected: list[dict[str, Any]] = []
    cursor: str | None = None
    seen_cursors: set[str] = set()
    while len(collected) < maximum:
        params: dict[str, Any] = {
            "cursor": cursor,
            "limit": min(100, maximum - len(collected)),
            "sortKey": "updated_at",
            "sortDirection": "desc",
            # Current official app-server builds persist custom-client threads
            # with source=vscode (see openai/codex#16614).  Include that source,
            # but admit only threads carrying bridge-owned sandbox metadata.
            "sourceKinds": ["appServer", "vscode"],
            "archived": False,
            # Avoid an expensive rollout-directory scan/repair on every MCP
            # list. Bridge-created tasks are already present in the state DB.
            "useStateDbOnly": True,
        }
        if project is not None:
            params["cwd"] = project.path
        result = await history.request("thread/list", params)
        data = result.get("data", [])
        if not isinstance(data, list):
            raise MCPError(code=types.INTERNAL_ERROR, message="native Codex history returned invalid data")
        for thread in data:
            if not isinstance(thread, dict):
                continue
            thread_id = thread.get("id")
            if (
                thread.get("source") == "vscode"
                and (
                    not isinstance(thread_id, str)
                    or _load_thread_metadata(thread_id) is None
                )
            ):
                continue
            cwd = thread.get("cwd")
            if not isinstance(cwd, str) or cwd not in by_cwd:
                continue
            if project is not None and cwd != project.path:
                continue
            if search:
                needle = search.casefold()
                haystack = str(thread.get("name") or "").casefold()
                if needle not in haystack:
                    continue
            collected.append(thread)
            if len(collected) >= maximum:
                break
        next_cursor = result.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen_cursors:
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    return collected


async def _native_descendants(
    history: NativeHistoryClient, *, ancestor_thread_id: str, cwd: str,
    maximum: int = 200,
) -> list[dict[str, Any]]:
    """Return only descendants of one exact thread, bounded and cwd-scoped."""
    collected: list[dict[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    while len(collected) < maximum:
        result = await history.request("thread/list", {
            "ancestorThreadId": ancestor_thread_id,
            "cwd": cwd,
            "cursor": cursor,
            "limit": min(100, maximum - len(collected)),
            "sourceKinds": [
                "subAgent", "subAgentReview", "subAgentCompact",
                "subAgentThreadSpawn", "subAgentOther",
            ],
            "archived": False,
            "useStateDbOnly": True,
        })
        data = result.get("data")
        if not isinstance(data, list):
            raise MCPError(code=types.INTERNAL_ERROR,
                message="native Codex descendant history returned invalid data")
        for thread in data:
            if (
                isinstance(thread, dict)
                and isinstance(thread.get("id"), str)
                and thread.get("cwd") == cwd
            ):
                collected.append(thread)
                if len(collected) >= maximum:
                    break
        next_cursor = result.get("nextCursor")
        if not isinstance(next_cursor, str) or not next_cursor or next_cursor in seen:
            break
        seen.add(next_cursor)
        cursor = next_cursor
    return collected


async def _interrupt_thread_turn(
    history: NativeHistoryClient, *, thread_id: str, cwd: str,
) -> dict[str, Any]:
    """Interrupt once, then read back on an app/transport race; never replay blindly."""
    read_result = await history.request("thread/read", {
        "threadId": thread_id, "includeTurns": True,
    })
    thread = read_result.get("thread")
    if not isinstance(thread, dict) or thread.get("cwd") != cwd:
        return {"threadId": thread_id, "status": "invalid_thread"}
    turn = _last_turn(thread)
    if not isinstance(turn, dict) or turn.get("status") != "inProgress":
        return {"threadId": thread_id, "status": "not_running"}
    turn_id = turn.get("id")
    if not isinstance(turn_id, str) or not turn_id:
        return {"threadId": thread_id, "status": "invalid_turn"}
    try:
        await history.request("turn/interrupt", {
            "threadId": thread_id, "turnId": turn_id,
        })
    except BaseException:
        # The request may have raced with natural completion, or its response may
        # have been lost. Read state once; never replay a mutating RPC automatically.
        with suppress(BaseException):
            reread = await history.request("thread/read", {
                "threadId": thread_id, "includeTurns": True,
            })
            fresh = reread.get("thread")
            fresh_turn = _last_turn(fresh) if isinstance(fresh, dict) else None
            if (
                isinstance(fresh, dict)
                and fresh.get("cwd") == cwd
                and (
                    not isinstance(fresh_turn, dict)
                    or fresh_turn.get("status") != "inProgress"
                )
            ):
                return {
                    "threadId": thread_id, "turnId": turn_id,
                    "status": "terminal_after_race",
                }
        return {"threadId": thread_id, "turnId": turn_id, "status": "outcome_unknown"}
    return {"threadId": thread_id, "turnId": turn_id, "status": "cancel_requested"}


async def _global_task_references(history: NativeHistoryClient) -> dict[str, str]:
    """Return prefixes unique across every authorized native project task."""
    universe = await _native_threads(history, maximum=5000)
    return _task_references(universe)


def _ordered_threads(threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return sorted(threads, key=lambda item: (item.get("updatedAt") or 0, str(item.get("id", ""))), reverse=True)


def _task_candidates(threads: list[dict[str, Any]]) -> list[dict[str, Any]]:
    catalog = _project_catalog()
    by_cwd = _project_by_cwd(catalog)
    ordered = _ordered_threads(threads)
    refs = _task_references(ordered)
    return [_task_record(thread, by_cwd[str(thread["cwd"])], refs[str(thread["id"])])
        for thread in ordered[:5]]


async def _resolve_task(
    history: NativeHistoryClient, task: str, project: ProjectEntry | None
) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    query = task.strip()
    # A freshly started app-server thread may not be indexed by thread/list
    # until its rollout receives more events.  The bridge already has the exact
    # returned ID, so read it directly for fast, reliable status polling.
    if re.fullmatch(r"[0-9a-fA-F]{8}-[0-9a-fA-F-]{27}", query):
        try:
            direct = await history.request(
                "thread/read", {"threadId": query, "includeTurns": True}
            )
        except RuntimeError:
            direct = {}
        direct_thread = direct.get("thread")
        if isinstance(direct_thread, dict):
            cwd = direct_thread.get("cwd")
            authorized = _project_by_cwd(_project_catalog())
            if isinstance(cwd, str) and cwd in authorized:
                if project is None or cwd == project.path:
                    return direct_thread, None

    threads = _ordered_threads(await _native_threads(history, project=project, maximum=5000))
    if query.casefold() == "latest":
        if not threads:
            return None, {"status": "not_found", "message": "No matching Codex tasks found.", "candidates": []}
        return threads[0], None

    exact_id = [thread for thread in threads if thread.get("id") == query]
    if exact_id:
        return exact_id[0], None
    id_prefix = [thread for thread in threads
        if isinstance(thread.get("id"), str) and thread["id"].startswith(query)]
    if len(id_prefix) == 1:
        return id_prefix[0], None
    if len(id_prefix) > 1:
        return None, {"status": "ambiguous", "message": "Task ID prefix is ambiguous.",
            "candidates": _task_candidates(id_prefix)}

    folded = query.casefold()
    exact_name = [thread for thread in threads
        if isinstance(thread.get("name"), str) and thread["name"].casefold() == folded]
    if len(exact_name) == 1:
        return exact_name[0], None
    if len(exact_name) > 1:
        return None, {"status": "ambiguous", "message": "Task name is ambiguous.",
            "candidates": _task_candidates(exact_name)}
    partial = [thread for thread in threads
        if isinstance(thread.get("name"), str) and folded in thread["name"].casefold()]
    if len(partial) == 1:
        return partial[0], None
    if len(partial) > 1:
        return None, {"status": "ambiguous", "message": "Task name fragment is ambiguous.",
            "candidates": _task_candidates(partial)}
    return None, {"status": "not_found", "message": f"No Codex task matches {task!r}.",
        "candidates": []}


def _task_title(project: ProjectEntry, prompt: str) -> str:
    first = re.split(r"(?<=[.!?])\s+|[\r\n]+", prompt.strip(), maxsplit=1)[0]
    first = re.sub(r"\s+", " ", first).strip() or "Task"
    prefix = f"{project.canonical}: "
    return (prefix + first)[:60].rstrip()


def _inner_content(result: types.CallToolResult) -> str:
    if isinstance(result.structured_content, dict) and isinstance(result.structured_content.get("content"), str):
        return result.structured_content["content"]
    texts = [item.text for item in result.content if isinstance(item, types.TextContent)]
    return "\n".join(texts)


async def _set_thread_name(history: NativeHistoryClient, thread_id: str, name: str) -> None:
    for attempt in range(2):
        try:
            await history.request("thread/name/set", {"threadId": thread_id, "name": name})
            return
        except RuntimeError as exc:
            if attempt == 0 and "rollout" in str(exc).casefold():
                await anyio.sleep(0.5)
                continue
            raise


async def _routing_decision(
    history: NativeHistoryClient, *, project: ProjectEntry,
    provider: str | None, model: str | None, reasoning_effort: str | None,
) -> dict[str, Any]:
    """Resolve one backend exactly; never fall back after a backend failure."""
    if provider == "codex":
        selected_model, selected_effort = await _resolve_model_selection(
            history, model, reasoning_effort,
        )
        return {"backend": "codex", "provider": "codex", "model": selected_model,
                "reasoningEffort": selected_effort, "reason": "explicit_codex_provider"}
    if provider is not None:
        if reasoning_effort is not None:
            raise _invalid_params("reasoning_effort",
                "reasoning_effort is supported only by the Codex backend")
        selected = await OPENCODE_BACKEND.resolve_model(
            provider=provider, model=model, directory=project.path,
        )
        _assert_model_not_excluded(selected["selection"])
        return {"backend": "opencode", "provider": selected["provider"],
                "model": selected["nativeModelId"], "selection": selected["selection"],
                "reasoningEffort": None, "reason": "explicit_opencode_provider"}
    if model is None:
        selected_model, selected_effort = await _resolve_model_selection(
            history, None, reasoning_effort,
        )
        return {"backend": "codex", "provider": "codex", "model": selected_model,
                "reasoningEffort": selected_effort, "reason": "default_codex"}
    codex_models = await _codex_model_records(history)
    if any(model == item.get("id") for item in codex_models):
        selected_model, selected_effort = await _resolve_model_selection(
            history, model, reasoning_effort,
        )
        return {"backend": "codex", "provider": "codex", "model": selected_model,
                "reasoningEffort": selected_effort, "reason": "codex_model_match"}
    if reasoning_effort is not None:
        raise _invalid_params("reasoning_effort",
            "reasoning_effort is supported only by the Codex backend")
    selected = await OPENCODE_BACKEND.resolve_model(
        provider=None, model=model, directory=project.path,
    )
    _assert_model_not_excluded(selected["selection"])
    return {"backend": "opencode", "provider": selected["provider"],
            "model": selected["nativeModelId"], "selection": selected["selection"],
            "reasoningEffort": None, "reason": "opencode_model_match"}


def _registry_task(task_hint: str, project: ProjectEntry | None) -> dict[str, Any] | None:
    if TASK_ID_RE.fullmatch(task_hint):
        record = TASK_REGISTRY.get(task_hint)
        if record is not None and (project is None or record.get("cwd") == project.path):
            return record
        return None
    if task_hint.startswith("dvt_") and len(task_hint) >= 8:
        matches = [item for item in TASK_REGISTRY.list(
            project=project.canonical if project else None, limit=5000,
        ) if str(item.get("taskId", "")).startswith(task_hint)]
        if len(matches) == 1:
            return matches[0]
    return None


def _registry_public(record: dict[str, Any]) -> dict[str, Any]:
    return {key: record.get(key) for key in (
        "taskId", "backend", "project", "provider", "model", "selection", "access",
        "status", "createdAt", "updatedAt", "finishedAt", "errorCategory",
        "routingReason", "title",
    ) if record.get(key) is not None}


async def _read_opencode_task(record: dict[str, Any], *, detail: str = "summary") -> dict[str, Any]:
    session_id = record.get("backendSessionId")
    if not isinstance(session_id, str):
        return {"status": "failed", "task": _registry_public(record),
                "content": "OpenCode task has no valid session ID."}
    try:
        inspected = await OPENCODE_BACKEND.inspect(
            session_id=session_id, directory=str(record["cwd"]),
            include_messages=True,
        )
    except OpenCodeError as exc:
        TASK_REGISTRY.update(record["taskId"], status="failed", errorCategory=exc.category,
                             finishedAt=int(time.time() * 1000))
        return {"status": "failed", "task": _registry_public(TASK_REGISTRY.get(record["taskId"]) or record),
                "errorCategory": exc.category, "content": str(exc)}
    status = inspected["status"]
    inspected_error = inspected.get("error")
    if (
        status == "failed"
        and isinstance(inspected_error, dict)
        and inspected_error.get("name") == "MessageAbortedError"
    ):
        status = "interrupted"
    if status == "idle" and inspected.get("finalResponse") is None:
        if record.get("status") == "cancelling":
            status = "interrupted"
        elif int(time.time() * 1000) - int(record.get("createdAt") or 0) < 900_000:
            # prompt_async may briefly report idle before its worker registers
            # busy state. Preserve running during that bounded startup window.
            status = "running"
        else:
            status = "failed"
    changes: dict[str, Any] = {"status": status}
    if status in {"completed", "failed", "interrupted"}:
        changes["finishedAt"] = int(time.time() * 1000)
    if status == "failed":
        changes["errorCategory"] = "model_error"
    updated = TASK_REGISTRY.update(record["taskId"], **changes)
    payload = {
        "status": status,
        "task": _registry_public(updated),
        "latestTurn": {
            "messageId": inspected.get("messageId"),
            "status": status,
            "finalResponse": inspected.get("finalResponse"),
            "error": inspected.get("error"),
        },
        "content": (
            "OpenCode task is still running. Check it again with read_task."
            if status == "running" else
            inspected.get("finalResponse") or
            ("OpenCode session is idle with no assistant result." if status == "idle" else "OpenCode task failed.")
        ),
    }
    if detail == "full":
        payload["history"] = inspected.get("messages", [])
    return payload


_COUNCIL_BACKGROUND: dict[str, asyncio.Task[Any]] = {}
_PAID_COUNCIL_CONFIRMATIONS: dict[str, dict[str, Any]] = {}


def _paid_council_digest(arguments: dict[str, Any]) -> str:
    bounded = {key: value for key, value in arguments.items()
               if key != "paid_confirmation_token"}
    encoded = json.dumps(
        bounded, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _issue_paid_council_confirmation(arguments: dict[str, Any]) -> tuple[str, int]:
    now = int(time.time())
    for token, receipt in list(_PAID_COUNCIL_CONFIRMATIONS.items()):
        if int(receipt.get("expiresAt") or 0) <= now:
            _PAID_COUNCIL_CONFIRMATIONS.pop(token, None)
    token = "pcf_" + secrets.token_hex(16)
    expires_at = now + PAID_COUNCIL_CONFIRMATION_TTL_SECONDS
    _PAID_COUNCIL_CONFIRMATIONS[token] = {
        "digest": _paid_council_digest(arguments), "expiresAt": expires_at,
    }
    return token, expires_at


def _validate_paid_council_confirmation(
    token: str, arguments: dict[str, Any], *, consume: bool,
) -> bool:
    receipt = _PAID_COUNCIL_CONFIRMATIONS.get(token)
    valid = (
        isinstance(receipt, dict)
        and int(receipt.get("expiresAt") or 0) > int(time.time())
        and receipt.get("digest") == _paid_council_digest(arguments)
    )
    if consume or not valid:
        _PAID_COUNCIL_CONFIRMATIONS.pop(token, None)
    return valid


def _council_requests_for_tier(
    tier: str, requested: list[dict[str, Any]] | None,
) -> list[dict[str, str]]:
    source = requested if requested is not None else list(COUNCIL_PRESETS[tier])
    normalized = [
        {"provider": str(item["provider"]).lower(), "model": str(item["model"])}
        for item in source
    ]
    unsupported = sorted({item["provider"] for item in normalized} - {"opencode", "nvidia"})
    if unsupported:
        raise OpenCodeError(
            "council_policy",
            "Council tiers support only OpenCode Zen and NVIDIA providers; unsupported: "
            + ", ".join(unsupported),
        )
    nvidia_count = sum(item["provider"] == "nvidia" for item in normalized)
    if tier == "free" and nvidia_count:
        raise OpenCodeError(
            "council_budget",
            "tier=free prohibits NVIDIA inference; select tier=extended or tier=pro.",
        )
    if tier == "extended" and nvidia_count > 1:
        raise OpenCodeError(
            "council_budget",
            "tier=extended permits at most one NVIDIA participant and one NVIDIA inference.",
        )
    return normalized


def _council_participation_mode(tier: str, provider: str) -> str:
    if tier == "extended" and provider == "nvidia":
        return "final_review_only"
    return "full_debate"


def _council_usage_plan(participants: list[dict[str, Any]], rounds: int) -> dict[str, Any]:
    full_calls = 1 if rounds == 1 else 3
    by_provider: dict[str, int] = {}
    for item in participants:
        calls = 1 if item["participationMode"] == "final_review_only" else full_calls
        by_provider[item["provider"]] = by_provider.get(item["provider"], 0) + calls
    max_multiplier = 1 + len(COUNCIL_TRANSIENT_RETRY_DELAYS)
    maximum_by_provider = {
        provider: calls * max_multiplier for provider, calls in by_provider.items()
    }
    return {
        "freeCalls": by_provider.get("opencode", 0),
        "nvidiaCalls": by_provider.get("nvidia", 0),
        "totalCalls": sum(by_provider.values()),
        "byProvider": by_provider,
        "maxTransientAttemptsPerStage": max_multiplier,
        "maxFreeAttempts": maximum_by_provider.get("opencode", 0),
        "maxNvidiaAttempts": maximum_by_provider.get("nvidia", 0),
        "maxAttemptsByProvider": maximum_by_provider,
    }


def _bounded_string_list(parsed: dict[str, Any], key: str) -> list[str]:
    values = parsed.get(key)
    return [str(item)[:COUNCIL_ITEM_LIMIT] for item in values[:COUNCIL_LIST_LIMIT]] \
        if isinstance(values, list) else []


def _structured_council_revision(text_value: str | None) -> dict[str, Any]:
    fallback = {"revised_position": (text_value or "")[:COUNCIL_TEXT_LIMIT], "agreements": [],
                "disagreements": [], "novel_findings": [], "unresolved_questions": [],
                "key_contributions": []}
    if not text_value:
        return fallback
    match = re.search(r"\{.*\}", text_value, flags=re.DOTALL)
    if match is None:
        return fallback
    try:
        parsed = json.loads(match.group(0))
    except (ValueError, TypeError):
        return fallback
    if not isinstance(parsed, dict):
        return fallback
    result = {"revised_position": str(parsed.get("revised_position") or text_value)[:COUNCIL_TEXT_LIMIT]}
    for key in ("agreements", "disagreements", "novel_findings", "unresolved_questions",
                "key_contributions"):
        result[key] = _bounded_string_list(parsed, key)
    return result


def _structured_expert_review(text_value: str | None) -> dict[str, Any]:
    fallback = {
        "expert_assessment": (text_value or "")[:COUNCIL_TEXT_LIMIT], "strongest_contributions": [],
        "weakest_assumptions": [], "missing_points": [], "recommended_expansion": [],
        "unresolved_questions": [],
    }
    if not text_value:
        return fallback
    match = re.search(r"\{.*\}", text_value, flags=re.DOTALL)
    if match is None:
        return fallback
    try:
        parsed = json.loads(match.group(0))
    except (ValueError, TypeError):
        return fallback
    if not isinstance(parsed, dict):
        return fallback
    result = {"expert_assessment": str(parsed.get("expert_assessment") or text_value)[:COUNCIL_TEXT_LIMIT]}
    for key in ("weakest_assumptions", "missing_points", "recommended_expansion",
                "unresolved_questions"):
        result[key] = _bounded_string_list(parsed, key)
    strongest: list[dict[str, str]] = []
    raw_strongest = parsed.get("strongest_contributions")
    if isinstance(raw_strongest, list):
        for item in raw_strongest[:COUNCIL_LIST_LIMIT]:
            if isinstance(item, dict):
                strongest.append({
                    "model": str(item.get("model") or "")[:255],
                    "contribution": str(item.get("contribution") or "")[:COUNCIL_ITEM_LIMIT],
                    "why_strong": str(item.get("why_strong") or "")[:COUNCIL_ITEM_LIMIT],
                })
            else:
                strongest.append({"model": "", "contribution": str(item)[:COUNCIL_ITEM_LIMIT],
                                  "why_strong": ""})
    result["strongest_contributions"] = strongest
    return result


def _council_failure_detail(inspected: dict[str, Any]) -> dict[str, Any]:
    error = inspected.get("error") if isinstance(inspected.get("error"), dict) else {}
    data = error.get("data") if isinstance(error.get("data"), dict) else {}
    status_code = data.get("statusCode")
    if not isinstance(status_code, int):
        status_code = None
    explicit_retryable = data.get("isRetryable") is True
    retryable_statuses = {408, 425, 429, 500, 502, 503, 504}
    retryable = explicit_retryable or status_code in retryable_statuses
    if status_code == 410:
        category = "model_unavailable"
        retryable = False
    elif status_code in {425, 429, 503}:
        category = "provider_queue"
    elif retryable:
        category = "provider_transient"
    else:
        category = "model_error"
    return {
        "category": category,
        "statusCode": status_code,
        "retryable": retryable,
        "providerError": str(data.get("message") or error.get("name") or "model failed")[:1_000],
    }


async def _wait_council_session(
    session_id: str, cwd: str, timeout: float = COUNCIL_SESSION_TIMEOUT,
) -> dict[str, Any]:
    with anyio.fail_after(timeout):
        while True:
            inspected = await OPENCODE_BACKEND.inspect(
                session_id=session_id, directory=cwd, include_messages=True,
            )
            if inspected["status"] in {"completed", "failed"}:
                return inspected
            await anyio.sleep(2)


async def _wait_council_response_with_retries(
    item: dict[str, Any], *, cwd: str, retry_prompt: str,
) -> dict[str, Any]:
    try:
        inspected = await _wait_council_session(item["sessionId"], cwd)
    except TimeoutError:
        item["failureDetail"] = {
            "category": "timeout", "statusCode": None, "retryable": False,
            "providerError": (
                f"No terminal response within {int(COUNCIL_SESSION_TIMEOUT)} seconds; "
                "the session may still finish and remains inspectable."
            ),
        }
        raise
    for retry_index, delay in enumerate(COUNCIL_TRANSIENT_RETRY_DELAYS, start=1):
        if inspected.get("status") == "completed":
            return inspected
        detail = _council_failure_detail(inspected)
        detail["attempt"] = int(item.get("inferenceCallsUsed") or 0)
        item["failureDetail"] = detail
        if not detail["retryable"]:
            return inspected
        item["retryCount"] = retry_index
        await anyio.sleep(delay)
        try:
            await OPENCODE_BACKEND.prompt(
                session_id=item["sessionId"], directory=cwd, prompt=retry_prompt,
                provider=item["provider"], model_id=item["model"], access="read",
            )
            item["inferenceCallsUsed"] = int(item.get("inferenceCallsUsed") or 0) + 1
            inspected = await _wait_council_session(item["sessionId"], cwd)
        except OpenCodeError as exc:
            inspected = {"status": "failed", "error": {"name": "OpenCodeError", "data": {
                "message": str(exc), "statusCode": exc.status,
                "isRetryable": exc.status in {408, 425, 429, 500, 502, 503, 504},
            }}}
    if inspected.get("status") != "completed":
        detail = _council_failure_detail(inspected)
        detail["attempt"] = int(item.get("inferenceCallsUsed") or 0)
        detail["retryExhausted"] = detail["retryable"]
        item["failureDetail"] = detail
    return inspected


def _council_result_document(
    council: dict[str, Any], prompt: str, participants: list[dict[str, Any]],
) -> dict[str, Any]:
    full = [item for item in participants if item.get("participationMode") == "full_debate"]
    expert = [item for item in participants if item.get("participationMode") == "final_review_only"]
    actual_by_provider: dict[str, int] = {}
    for item in participants:
        calls = int(item.get("inferenceCallsUsed") or 0)
        actual_by_provider[item["provider"]] = actual_by_provider.get(item["provider"], 0) + calls
    planned = council.get("usagePlan") if isinstance(council.get("usagePlan"), dict) else {}
    actual = {
        "freeCalls": actual_by_provider.get("opencode", 0),
        "nvidiaCalls": actual_by_provider.get("nvidia", 0),
        "totalCalls": sum(actual_by_provider.values()),
        "byProvider": actual_by_provider,
    }
    model_outputs: dict[str, Any] = {}
    for item in participants:
        revision = item.get("revision") if isinstance(item.get("revision"), dict) else {}
        model_outputs[item["selection"]] = {
            "provider": item["provider"],
            "participationMode": item["participationMode"],
            "status": item.get("status"),
            "inferenceCallsUsed": int(item.get("inferenceCallsUsed") or 0),
            "initial_position": item.get("initial"),
            "cross_critique": item.get("critique"),
            "revised_position": revision.get("revised_position"),
            "key_contributions": revision.get("key_contributions", []),
            "expert_review": item.get("expertReview"),
            "failure": item.get("failure"),
            "failure_detail": item.get("failureDetail"),
            "retry_count": int(item.get("retryCount") or 0),
        }
    failures = {
        item["selection"]: item.get("failure") for item in participants
        if item.get("status") in {"failed", "skipped"}
    }
    any_completed = any(item.get("status") == "completed" for item in participants)
    return {
        "schema": "devcoveer-council-result.v2",
        "documentType": "attributed_multi_model_council",
        "councilLevel": council.get("tier", "free"),
        "mode": council.get("mode", "debate"),
        "question": prompt,
        "baseline_conclusion": council.get("baselineConclusion"),
        "executionStatus": (
            "failed" if not any_completed else "degraded" if failures else "complete"
        ),
        "usage": {
            "policy": council.get("costPolicy"),
            "planned": planned,
            "actual": actual,
            "nvidiaInferencePerformed": actual["nvidiaCalls"] > 0,
        },
        "participants": [
            {"provider": item["provider"], "model": item["selection"],
             "participationMode": item["participationMode"], "status": item.get("status"),
             "inferenceCallsUsed": int(item.get("inferenceCallsUsed") or 0)}
            for item in participants
        ],
        "model_outputs": model_outputs,
        # Stable compatibility fields for existing clients.
        "initial_positions": {item["selection"]: item.get("initial") for item in full
                              if item.get("initial") is not None},
        "cross_critiques": {item["selection"]: item.get("critique") for item in full
                            if item.get("critique") is not None},
        "revised_positions": {
            item["selection"]: item["revision"].get("revised_position") for item in full
            if isinstance(item.get("revision"), dict)
        },
        "agreements": {item["selection"]: item["revision"].get("agreements", []) for item in full
                       if isinstance(item.get("revision"), dict)},
        "disagreements": {item["selection"]: item["revision"].get("disagreements", []) for item in full
                          if isinstance(item.get("revision"), dict)},
        "novel_findings": {item["selection"]: item["revision"].get("novel_findings", []) for item in full
                           if isinstance(item.get("revision"), dict)},
        "unresolved_questions": {
            item["selection"]: item["revision"].get("unresolved_questions", []) for item in full
            if isinstance(item.get("revision"), dict)
        },
        "expert_reviews": {item["selection"]: item.get("expertReview") for item in expert
                           if item.get("expertReview") is not None},
        "failures": failures,
        "attribution": [
            {"provider": item["provider"], "model": item["selection"],
             "initialAvailable": item.get("initial") is not None,
             "critiqueAvailable": item.get("critique") is not None,
             "revisionAvailable": isinstance(item.get("revision"), dict),
             "expertReviewAvailable": isinstance(item.get("expertReview"), dict)}
            for item in participants
        ],
        "integration_guide": {
            "purpose": (
                "Compare this attributed council document with the conclusion formed before "
                "the council; do not present the council as an untraceable consensus."
            ),
            "requiredResponseSections": [
                "original_conclusion", "what_the_council_added",
                "strongest_model_contribution", "revised_conclusion",
                "remaining_uncertainty",
            ],
            "instructionsForChatGPT": [
                "State which parts of the pre-council conclusion remain unchanged.",
                "Identify additions, corrections, or alternatives using attributed model outputs.",
                "Name the model that supplied the strongest contribution and explain why.",
                "Produce a revised conclusion and preserve unresolved uncertainty.",
            ],
        },
    }


def _final_council_record(
    council: dict[str, Any], *, stage: str, participants: list[dict[str, Any]],
    result: dict[str, Any],
) -> dict[str, Any]:
    # The structured result is the canonical final document. Persist only compact
    # participant receipts beside it so the 2 MB registry limit is not consumed by
    # duplicating every position/critique/revision a second time.
    compact = {
        key: value for key, value in council.items()
        if key not in {"participants", "result", "baselineConclusion"}
    }
    compact["stage"] = stage
    compact["participants"] = [
        {"provider": item["provider"], "model": item["model"],
         "selection": item["selection"], "participationMode": item["participationMode"],
         "status": item.get("status"),
         "inferenceCallsUsed": int(item.get("inferenceCallsUsed") or 0),
         "failure": item.get("failure"), "failureDetail": item.get("failureDetail"),
         "retryCount": int(item.get("retryCount") or 0)}
        for item in participants
    ]
    compact["result"] = result
    return compact


async def _run_council_task(
    task_id: str, prompt: str, rounds: int, baseline_conclusion: str | None = None,
) -> None:
    record = TASK_REGISTRY.get(task_id)
    if record is None:
        return
    council = record.get("council") if isinstance(record.get("council"), dict) else {}
    participants = council.get("participants") if isinstance(council.get("participants"), list) else []
    full_participants = [
        item for item in participants if item.get("participationMode") == "full_debate"
    ]
    expert_participants = [
        item for item in participants if item.get("participationMode") == "final_review_only"
    ]
    cwd = str(record["cwd"])
    project = str(record["project"])
    baseline_context = (
        "\n\nBASELINE CONCLUSION FROM CHATGPT TO STRESS-TEST:\n" + baseline_conclusion
        if baseline_conclusion else ""
    )

    async def initial_one(item: dict[str, Any]) -> None:
        try:
            initial_prompt = (
                "MULTI-MODEL COUNCIL — INDEPENDENT ROUND. Do not seek agreement and do not "
                "assume other participants' views. Analyze the task independently. Identify "
                "facts, assumptions, alternatives, risks, and a concrete recommendation. "
                "Do not expose hidden chain-of-thought; provide only the normal answer.\n\n"
                + prompt + baseline_context
            )
            started = await OPENCODE_BACKEND.start(
                directory=cwd, project=project, prompt=initial_prompt,
                access="read", provider=item["provider"], model_id=item["model"],
                title=f"Council initial: {item['selection']}",
            )
            item["inferenceCallsUsed"] = int(item.get("inferenceCallsUsed") or 0) + 1
            item["sessionId"] = started["sessionId"]
            inspected = await _wait_council_response_with_retries(
                item, cwd=cwd, retry_prompt=initial_prompt,
            )
            if inspected["status"] != "completed":
                detail = item.get("failureDetail") or _council_failure_detail(inspected)
                raise OpenCodeError(detail["category"], detail["providerError"],
                                    status=detail.get("statusCode"))
            item["initial"] = str(inspected.get("finalResponse") or "")[:COUNCIL_TEXT_LIMIT]
            item["status"] = "initial_completed"
        except Exception as exc:
            item["status"] = "failed"
            item["failure"] = getattr(exc, "category", type(exc).__name__)

    try:
        TASK_REGISTRY.update(task_id, status="running", council={**council, "stage": "initial", "participants": participants})
        async with anyio.create_task_group() as tg:
            for participant in full_participants:
                tg.start_soon(initial_one, participant)

        successful = [item for item in full_participants if item.get("status") == "initial_completed"]
        if not successful:
            for item in expert_participants:
                item["status"] = "skipped"
                item["failure"] = "skipped_no_free_debate_results"
            result = _council_result_document(council, prompt, participants)
            TASK_REGISTRY.update(task_id, status="failed", finishedAt=int(time.time() * 1000),
                                 errorCategory="all_participants_failed",
                                 council=_final_council_record(
                                     council, stage="failed", participants=participants,
                                     result=result,
                                 ))
            return
        if rounds == 1:
            for item in successful:
                item["status"] = "completed"
        else:
            initial_context = "\n\n".join(
                f"PARTICIPANT {item['selection']} INITIAL POSITION:\n{str(item.get('initial') or '')[:30_000]}"
                for item in successful
            )[:100_000]

            async def critique_one(item: dict[str, Any]) -> None:
                try:
                    critique_prompt = (
                        "COUNCIL CROSS-REVIEW ROUND. Review every other position below. "
                        "Actively find factual errors, omissions, weak assumptions, alternative "
                        "solutions, metric/accuracy improvements, and genuinely new deductions. "
                        "Do not merely agree and do not expose hidden chain-of-thought.\n\n"
                        + initial_context
                    )
                    await OPENCODE_BACKEND.prompt(
                        session_id=item["sessionId"], directory=cwd,
                        prompt=critique_prompt,
                        provider=item["provider"], model_id=item["model"], access="read",
                    )
                    item["inferenceCallsUsed"] = int(item.get("inferenceCallsUsed") or 0) + 1
                    inspected = await _wait_council_response_with_retries(
                        item, cwd=cwd, retry_prompt=critique_prompt,
                    )
                    if inspected["status"] != "completed":
                        detail = item.get("failureDetail") or _council_failure_detail(inspected)
                        raise OpenCodeError(detail["category"], detail["providerError"],
                                            status=detail.get("statusCode"))
                    item["critique"] = str(inspected.get("finalResponse") or "")[:COUNCIL_TEXT_LIMIT]
                    item["status"] = "critique_completed"
                except Exception as exc:
                    item["status"] = "failed"
                    item["failure"] = getattr(exc, "category", type(exc).__name__)

            TASK_REGISTRY.update(task_id, council={**council, "stage": "critique", "participants": participants})
            async with anyio.create_task_group() as tg:
                for participant in successful: tg.start_soon(critique_one, participant)

            revision_candidates = [item for item in full_participants if item.get("status") == "critique_completed"]
            critique_context = "\n\n".join(
                f"PARTICIPANT {item['selection']} CRITIQUE:\n{str(item.get('critique') or '')[:30_000]}"
                for item in revision_candidates
            )[:100_000]

            async def revise_one(item: dict[str, Any]) -> None:
                try:
                    revision_prompt = (
                        "COUNCIL REVISION ROUND. Revise your own conclusion using the cross-"
                        "critiques. Preserve justified disagreements; do not optimize for consensus. "
                        "Return one JSON object with keys revised_position (string), agreements, "
                        "disagreements, novel_findings, unresolved_questions, key_contributions "
                        "(arrays of strings). No markdown fence and no hidden chain-of-thought.\n\n"
                        + critique_context
                    )
                    await OPENCODE_BACKEND.prompt(
                        session_id=item["sessionId"], directory=cwd,
                        prompt=revision_prompt,
                        provider=item["provider"], model_id=item["model"], access="read",
                    )
                    item["inferenceCallsUsed"] = int(item.get("inferenceCallsUsed") or 0) + 1
                    inspected = await _wait_council_response_with_retries(
                        item, cwd=cwd, retry_prompt=revision_prompt,
                    )
                    if inspected["status"] != "completed":
                        detail = item.get("failureDetail") or _council_failure_detail(inspected)
                        raise OpenCodeError(detail["category"], detail["providerError"],
                                            status=detail.get("statusCode"))
                    revision_raw = str(inspected.get("finalResponse") or "")
                    item["revision"] = _structured_council_revision(revision_raw)
                    item["status"] = "completed"
                except Exception as exc:
                    item["status"] = "failed"
                    item["failure"] = getattr(exc, "category", type(exc).__name__)

            TASK_REGISTRY.update(task_id, council={**council, "stage": "revision", "participants": participants})
            async with anyio.create_task_group() as tg:
                for participant in revision_candidates: tg.start_soon(revise_one, participant)

        debate_completed = [item for item in full_participants if item.get("status") == "completed"]
        if expert_participants and debate_completed:
            preview = _council_result_document(council, prompt, participants)
            expert_context = json.dumps({
                "question": prompt,
                "model_outputs": preview["model_outputs"],
                "novel_findings": preview["novel_findings"],
                "unresolved_questions": preview["unresolved_questions"],
            }, ensure_ascii=False)[:120_000]

            async def expert_one(item: dict[str, Any]) -> None:
                try:
                    expert_prompt = (
                        "MULTI-MODEL COUNCIL — FINAL EXPERT REVIEW. You are the limited-"
                        "budget expert reviewer. Inspect the attributed debate document below. "
                        "Do not invent consensus and do not expose hidden chain-of-thought. "
                        "Return one JSON object with expert_assessment (string), "
                        "strongest_contributions (array of objects with model, contribution, "
                        "why_strong), weakest_assumptions, missing_points, "
                        "recommended_expansion, unresolved_questions (arrays of strings). "
                        "No markdown fence.\n\n" + expert_context
                    )
                    started = await OPENCODE_BACKEND.start(
                        directory=cwd, project=project,
                        prompt=expert_prompt,
                        access="read", provider=item["provider"], model_id=item["model"],
                        title=f"Council expert review: {item['selection']}",
                    )
                    item["inferenceCallsUsed"] = int(item.get("inferenceCallsUsed") or 0) + 1
                    item["sessionId"] = started["sessionId"]
                    inspected = await _wait_council_response_with_retries(
                        item, cwd=cwd, retry_prompt=expert_prompt,
                    )
                    if inspected["status"] != "completed":
                        detail = item.get("failureDetail") or _council_failure_detail(inspected)
                        raise OpenCodeError(detail["category"], detail["providerError"],
                                            status=detail.get("statusCode"))
                    expert_review_raw = str(inspected.get("finalResponse") or "")
                    item["expertReview"] = _structured_expert_review(expert_review_raw)
                    item["status"] = "completed"
                except Exception as exc:
                    item["status"] = "failed"
                    item["failure"] = getattr(exc, "category", type(exc).__name__)

            TASK_REGISTRY.update(task_id, council={**council, "stage": "expert_review",
                                                   "participants": participants})
            async with anyio.create_task_group() as tg:
                for participant in expert_participants:
                    tg.start_soon(expert_one, participant)

        result = _council_result_document(council, prompt, participants)
        final_status = "completed" if debate_completed else "failed"
        TASK_REGISTRY.update(task_id, status=final_status, finishedAt=int(time.time() * 1000),
            errorCategory=None if debate_completed else "all_participants_failed",
            council=_final_council_record(
                council, stage="completed" if debate_completed else "failed",
                participants=participants, result=result,
            ))
    except BaseException as exc:
        if isinstance(exc, asyncio.CancelledError):
            return
        with suppress(BaseException):
            current = TASK_REGISTRY.get(task_id)
            if current is not None and current.get("status") != "interrupted":
                TASK_REGISTRY.update(task_id, status="failed", finishedAt=int(time.time() * 1000),
                                     errorCategory=getattr(exc, "category", type(exc).__name__))


def _launch_council(
    task_id: str, prompt: str, rounds: int, baseline_conclusion: str | None = None,
) -> None:
    task = asyncio.create_task(_run_council_task(
        task_id, prompt, rounds, baseline_conclusion,
    ),
                               name=f"devcoveer-council-{task_id}")
    _COUNCIL_BACKGROUND[task_id] = task
    task.add_done_callback(lambda _done: _COUNCIL_BACKGROUND.pop(task_id, None))


def _read_council_task(record: dict[str, Any]) -> dict[str, Any]:
    council = record.get("council") if isinstance(record.get("council"), dict) else {}
    result = council.get("result") if isinstance(council.get("result"), dict) else None
    return {
        "status": record.get("status"),
        "task": _registry_public(record),
        "stage": council.get("stage"),
        "councilLevel": council.get("tier"),
        "usagePlan": council.get("usagePlan"),
        "documentSchema": result.get("schema") if result is not None else None,
        "result": result,
        "content": (
            "Council is still running; poll the same taskId with read_task."
            if record.get("status") in {"starting", "running"} else
            "Council completed with structured participant results."
            if record.get("status") == "completed" else
            "Council failed; inspect per-participant failures."
        ),
    }


def build_server(history: NativeHistoryClient) -> Server[Any]:
    # Only task creation/continuation changes app-server lifecycle state.  Keep
    # those operations ordered, but never queue read_task/list_tasks behind
    # them: ChatGPT may poll several background tasks concurrently.  Native
    # JSON-RPC request/restart safety is handled by NativeHistoryClient.
    task_lifecycle_lock = anyio.Lock()

    async def list_tools(_ctx: Any, _params: types.PaginatedRequestParams | None) -> types.ListToolsResult:
        return types.ListToolsResult(tools=TOOLS)

    async def _call_tool_impl(_ctx: Any, params: types.CallToolRequestParams) -> types.CallToolResult:
        name = params.name
        if name not in VALIDATORS:
            raise MCPError(code=types.METHOD_NOT_FOUND, message=f"Unknown tool: {name}")
        arguments = params.arguments or {}
        _validate_schema(name, arguments)

        if name == "find_projects":
            _entry, payload = _project_selection(arguments["query"])
            return _tool_result(payload, error=payload["status"] != "resolved")

        if name == "list_models":
            return _tool_result(await _model_discovery_payload(
                history,
                arguments.get("provider"),
                arguments.get("verified_only", False),
            ))

        project: ProjectEntry | None = None
        if "project" in arguments:
            project, project_payload = _project_selection(arguments["project"])
            if project is None:
                return _tool_result(project_payload, error=True)

        if name == "council_run":
            council_project = project.canonical if project is not None else "__no_project__"
            council_cwd = project.path if project is not None else "/home/dev"
            tier = arguments.get("tier", "free")
            rounds = arguments.get("rounds", 2)
            try:
                requested_participants = _council_requests_for_tier(
                    tier, arguments.get("participants")
                )
            except OpenCodeError as exc:
                return _tool_result({"status": "failed", "errorCategory": exc.category,
                    "councilLevel": tier, "content": str(exc)}, error=True)
            requested_plan = [
                {"provider": item["provider"],
                 "participationMode": _council_participation_mode(tier, item["provider"])}
                for item in requested_participants
            ]
            usage_plan = _council_usage_plan(requested_plan, rounds)
            cost_policy = {
                "free": "free_only_no_nvidia",
                "extended": "nvidia_economical_one_review_with_bounded_transient_retries",
                "pro": "nvidia_full_debate_with_bounded_transient_retries",
            }[tier]
            paid_token = arguments.get("paid_confirmation_token")
            if tier == "free" and paid_token is not None:
                return _tool_result({"status": "failed", "errorCategory": "council_policy",
                    "content": "A paid confirmation token cannot be used with tier=free."}, error=True)
            if tier != "free":
                if not isinstance(paid_token, str) or not _validate_paid_council_confirmation(
                    paid_token, arguments, consume=False,
                ):
                    confirmation_token, expires_at = _issue_paid_council_confirmation(arguments)
                    return _tool_result({
                        "status": "confirmation_required", "councilLevel": tier,
                        "requiresExplicitUserConfirmation": True,
                        "paidConfirmationToken": confirmation_token,
                        "confirmationExpiresAt": expires_at * 1000,
                        "costPolicy": cost_policy, "usagePlan": usage_plan,
                        "participants": [
                            {"provider": item["provider"], "model": item["model"],
                             "participationMode": _council_participation_mode(
                                 tier, item["provider"])}
                            for item in requested_participants
                        ],
                        "content": (
                            "No council or NVIDIA inference was started. Show this exact model/"
                            "attempt budget to the user and wait for explicit confirmation. Only "
                            "then replay the identical request with paid_confirmation_token."
                        ),
                    })
            participants: list[dict[str, Any]] = []
            seen_selections: set[str] = set()
            for requested in requested_participants:
                try:
                    selected = await OPENCODE_BACKEND.resolve_model(
                        provider=requested["provider"], model=requested["model"],
                        directory=council_cwd,
                    )
                except OpenCodeError as exc:
                    return _tool_result({"status": "failed", "errorCategory": exc.category,
                        "participant": requested, "content": str(exc)}, error=True)
                try:
                    _assert_model_not_excluded(selected["selection"])
                except OpenCodeError as exc:
                    return _tool_result({"status": "failed", "errorCategory": exc.category,
                        "participant": requested, "content": str(exc)}, error=True)
                if selected.get("provider") != requested["provider"]:
                    return _tool_result({"status": "failed",
                        "errorCategory": "council_policy",
                        "participant": requested,
                        "content": "Resolved provider does not match the requested council provider."},
                        error=True)
                if selected.get("provider") == "opencode" and selected.get("free") is not True:
                    return _tool_result({"status": "failed",
                        "errorCategory": "council_budget",
                        "participant": requested,
                        "content": (
                            "The selected OpenCode Zen model is not currently marked free by "
                            "the live catalog, so the council will not launch it as a free participant."
                        )}, error=True)
                if selected["selection"] in seen_selections:
                    return _tool_result({"status": "failed",
                        "content": "Council participants must be distinct exact models."}, error=True)
                seen_selections.add(selected["selection"])
                participants.append({
                    "provider": selected["provider"], "model": selected["nativeModelId"],
                    "selection": selected["selection"], "status": "pending",
                    "participationMode": _council_participation_mode(
                        tier, selected["provider"]
                    ),
                    "inferenceCallsUsed": 0,
                })
            # Resolve-time provider verification prevents an alias/catalog mismatch from
            # escaping the advertised tier budget.
            resolved_nvidia = sum(item["provider"] == "nvidia" for item in participants)
            if tier == "free" and resolved_nvidia:
                return _tool_result({"status": "failed", "errorCategory": "council_budget",
                    "content": "Resolved tier=free council contains an NVIDIA participant."},
                    error=True)
            if tier == "extended" and resolved_nvidia > 1:
                return _tool_result({"status": "failed", "errorCategory": "council_budget",
                    "content": "Resolved tier=extended council exceeds one NVIDIA participant."},
                    error=True)
            usage_plan = _council_usage_plan(participants, rounds)
            if tier != "free" and not _validate_paid_council_confirmation(
                str(paid_token), arguments, consume=True,
            ):
                return _tool_result({"status": "failed", "errorCategory": "confirmation_expired",
                    "content": "The paid council confirmation expired before launch; no inference started."},
                    error=True)
            record = TASK_REGISTRY.create(
                backend="council", backendSessionId="pending",
                project=council_project, cwd=council_cwd, provider="opencode",
                model="multi-model", access="read", status="starting",
                routingReason=f"council_{tier}", title=f"Multi-model council ({tier})",
                council={"stage": "starting", "rounds": rounds, "tier": tier,
                         "costPolicy": cost_policy, "usagePlan": usage_plan,
                         "baselineConclusion": arguments.get("baseline_conclusion"),
                         "mode": arguments.get("mode", "debate"),
                         "participants": participants},
            )
            _launch_council(record["taskId"], arguments["prompt"], rounds,
                            arguments.get("baseline_conclusion"))
            return _tool_result({
                "status": "running", "taskId": record["taskId"],
                "taskReference": record["taskId"], "backend": "council",
                "councilLevel": tier, "costPolicy": cost_policy,
                "paidAuthorizationVerified": tier != "free",
                "usagePlan": usage_plan,
                "baselineConclusionIncluded": bool(arguments.get("baseline_conclusion")),
                "participants": [{"provider": x["provider"], "model": x["selection"],
                                  "participationMode": x["participationMode"]}
                                 for x in participants],
                "content": (
                    "Council started in the background. Poll this taskId with read_task. "
                    f"Target NVIDIA calls: {usage_plan['nvidiaCalls']}; transient-error "
                    f"attempt ceiling: {usage_plan['maxNvidiaAttempts']}."
                ),
            })

        if name == "start_task":
            assert project is not None
            access = arguments.get("access", "read")
            try:
                route = await _routing_decision(
                    history, project=project, provider=arguments.get("provider"),
                    model=arguments.get("model"),
                    reasoning_effort=arguments.get("reasoning_effort"),
                )
            except OpenCodeError as exc:
                return _tool_result({"status": "failed", "errorCategory": exc.category,
                    "content": str(exc)}, error=True)
            task_name = _task_title(project, arguments["prompt"])
            if route["backend"] == "opencode":
                if access == "write":
                    for existing in TASK_REGISTRY.list(project=project.canonical, limit=500):
                        if existing.get("access") == "write" and existing.get("status") == "running":
                            return _tool_result({
                                "status": "conflict", "blockingTaskId": existing.get("taskId"),
                                "content": "Another OpenCode writer is active for this project. "
                                           "Read or cancel it before starting a second writer.",
                            }, error=True)
                try:
                    started = await OPENCODE_BACKEND.start(
                        directory=project.path, project=project.canonical,
                        prompt=arguments["prompt"], access=access,
                        provider=route["provider"], model_id=route["model"], title=task_name,
                    )
                except OpenCodeError as exc:
                    return _tool_result({
                        "status": "failed", "backend": "opencode",
                        "provider": route["provider"], "model": route.get("selection"),
                        "errorCategory": exc.category, "content": str(exc),
                    }, error=True)
                task_record = TASK_REGISTRY.create(
                    backend="opencode", backendSessionId=started["sessionId"],
                    project=project.canonical, cwd=project.path,
                    provider=route["provider"], model=route["model"],
                    selection=route["selection"], access=access, status="running",
                    routingReason=route["reason"], title=task_name,
                )
                return _tool_result({
                    "status": "running", "taskId": task_record["taskId"],
                    "taskReference": task_record["taskId"], "backend": "opencode",
                    "routedTo": "opencode", "routingReason": route["reason"],
                    "provider": route["provider"], "model": route["selection"],
                    "canonicalProject": project.canonical, "cwd": project.path,
                    "access": access, "selectionVerified": True,
                    "content": "OpenCode task started in the background. Use read_task with "
                               "the returned taskId to obtain the result.",
                })
            sandbox = ACCESS_TO_SANDBOX[access]
            permissions = WRITE_PERMISSION_PROFILE if access == "write" else None
            selected_model = route["model"]
            selected_effort = route["reasoningEffort"]
            start_params: dict[str, Any] = {
                "cwd": project.path,
                "approvalPolicy": "never",
                "threadSource": "appServer",
            }
            if permissions is None:
                start_params["sandbox"] = sandbox
            if selected_model is not None:
                start_params["model"] = selected_model
            fixed_config: dict[str, Any] = {}
            if permissions == WRITE_PERMISSION_PROFILE:
                fixed_config["default_permissions"] = WRITE_PERMISSION_PROFILE
            if selected_effort is not None:
                fixed_config["model_reasoning_effort"] = selected_effort
            if fixed_config:
                start_params["config"] = fixed_config
            try:
                started = await history.request("thread/start", start_params)
            except BaseException as exc:
                log.error("native asynchronous thread start failed (%s)", type(exc).__name__)
                return _tool_result({
                    "status": "failed",
                    "project": project.canonical,
                    "content": "Codex app-server could not create the task.",
                }, error=True)
            native_thread = started.get("thread")
            thread_id = native_thread.get("id") if isinstance(native_thread, dict) else None
            if not isinstance(thread_id, str) or not thread_id:
                return _tool_result({"status": "failed", "content": "Codex returned no threadId."}, error=True)
            if not _effective_sandbox_matches(
                started, cwd=project.path, sandbox=sandbox, permissions=permissions
            ):
                log.error("Codex app-server returned an unsafe or unexpected effective policy")
                return _tool_result({
                    "status": "failed",
                    "threadId": thread_id,
                    "content": "Codex refused the required fixed sandbox policy; no turn was started.",
                    "resumeCommand": f"codex resume {thread_id}",
                }, error=True)
            effective_settings = _effective_model_settings(
                started,
                requested_model=selected_model,
                requested_effort=selected_effort,
            )
            if effective_settings is None:
                log.error("Codex app-server returned unexpected effective model settings")
                return _tool_result({
                    "status": "failed",
                    "threadId": thread_id,
                    "content": "Codex did not apply the requested model/reasoning settings; no turn was started.",
                    "resumeCommand": f"codex resume {thread_id}",
                }, error=True)
            effective_model, effective_effort = effective_settings
            _save_thread_metadata(thread_id, project=project.canonical, access=access,
                cwd=project.path, sandbox=sandbox, permissions=permissions,
                model=effective_model, reasoning_effort=effective_effort)
            naming_warning: str | None = None
            try:
                await _set_thread_name(history, thread_id, task_name)
            except BaseException as exc:
                log.error("native thread naming failed (%s)", type(exc).__name__)
                naming_warning = "The task started, but its native display name could not be saved."
            turn_params = {
                "threadId": thread_id,
                "input": [{
                    "type": "text",
                    "text": _first_prompt(project.canonical, arguments["prompt"], access),
                }],
                "cwd": project.path,
                "approvalPolicy": "never",
                "model": effective_model,
            }
            if permissions is None:
                turn_params["sandboxPolicy"] = _turn_sandbox_policy(sandbox, project.path)
            if effective_effort is not None:
                turn_params["effort"] = effective_effort
            try:
                turn_started = await history.request("turn/start", turn_params)
            except BaseException as exc:
                log.error("native asynchronous turn start failed (%s)", type(exc).__name__)
                return _tool_result({
                    "status": "failed",
                    "threadId": thread_id,
                    "name": task_name,
                    "canonicalProject": project.canonical,
                    "content": "The Codex thread was created, but its first turn did not start.",
                    "resumeCommand": f"codex resume {thread_id}",
                }, error=True)
            turn = turn_started.get("turn")
            turn_id = turn.get("id") if isinstance(turn, dict) else None
            if not isinstance(turn_id, str) or not turn_id:
                return _tool_result({
                    "status": "failed",
                    "threadId": thread_id,
                    "content": "Codex returned no turnId.",
                    "resumeCommand": f"codex resume {thread_id}",
                }, error=True)
            # UUIDv7 threads created in the same time window commonly share the
            # first eight characters.  Return an exact identifier so a later
            # read never falls back to a costly, potentially ambiguous scan.
            task_reference = thread_id
            task_record = TASK_REGISTRY.create(
                backend="codex", backendSessionId=thread_id,
                project=project.canonical, cwd=project.path, provider="codex",
                model=effective_model, access=access, status="running",
                routingReason=route["reason"], title=task_name,
            )
            payload = {"status": "running", "threadId": thread_id, "turnId": turn_id,
                "taskId": task_record["taskId"],
                "taskReference": task_reference, "name": task_name,
                "canonicalProject": project.canonical, "cwd": project.path,
                "access": access,
                "model": effective_model, "reasoningEffort": effective_effort,
                "backend": "codex", "routedTo": "codex", "routingReason": route["reason"],
                "selectionVerified": True,
                "content": "Codex task started in the background. Use read_task to check status and obtain the result.",
                "resumeCommand": f"codex resume {thread_id}"}
            if naming_warning is not None:
                payload["warning"] = naming_warning
            return _tool_result(payload)

        if name == "list_tasks":
            limit = arguments.get("limit", 20)
            maximum = limit if project is not None and not arguments.get("search") else max(100, limit * 5)
            threads = _ordered_threads(await _native_threads(history, project=project,
                search=arguments.get("search"), maximum=maximum))
            catalog = _project_catalog(); by_cwd = _project_by_cwd(catalog)
            refs = _task_references(threads)
            tasks = []
            for thread in threads:
                item = _task_record(thread, by_cwd[str(thread["cwd"])], refs[str(thread["id"])])
                registered = TASK_REGISTRY.find_by_backend_session("codex", str(thread["id"]))
                if registered is not None:
                    item["taskId"] = registered["taskId"]
                item["backend"] = "codex"
                tasks.append(item)
            registered_tasks = TASK_REGISTRY.list(
                project=project.canonical if project else None, limit=maximum,
            )
            needle = str(arguments.get("search") or "").casefold()
            for registered in registered_tasks:
                if registered.get("backend") == "codex":
                    continue
                if needle and needle not in str(registered.get("title") or "").casefold():
                    continue
                tasks.append(_registry_public(registered))
            tasks.sort(key=lambda item: int(item.get("updatedAt") or 0), reverse=True)
            tasks = tasks[:limit]
            return _tool_result({"status": "ok", "count": len(tasks), "tasks": tasks})

        registered = _registry_task(arguments["task"], project)
        if registered is not None and registered.get("backend") == "council":
            if name == "read_task":
                return _tool_result(_read_council_task(TASK_REGISTRY.get(registered["taskId"]) or registered))
            if name == "continue_task":
                return _tool_result({"status": "conflict", "taskId": registered["taskId"],
                    "content": "Council tasks are immutable; start a new council for another prompt."}, error=True)
            assert name == "cancel_task"
            if registered.get("status") not in {"starting", "running", "cancelling"}:
                return _tool_result({"status": "not_running", "taskId": registered["taskId"],
                    "content": "Council is already terminal; its structured result was preserved."})
            background = _COUNCIL_BACKGROUND.get(registered["taskId"])
            if background is not None:
                background.cancel()
            council = registered.get("council") if isinstance(registered.get("council"), dict) else {}
            failures: list[str] = []
            for participant in council.get("participants", []):
                session_id = participant.get("sessionId") if isinstance(participant, dict) else None
                if isinstance(session_id, str):
                    try:
                        await OPENCODE_BACKEND.cancel(session_id=session_id, directory=str(registered["cwd"]))
                    except OpenCodeError:
                        failures.append(str(participant.get("selection")))
            updated = TASK_REGISTRY.update(registered["taskId"], status="interrupted",
                finishedAt=int(time.time() * 1000), errorCategory=None,
                council={**council, "stage": "interrupted"})
            return _tool_result({"status": "interrupted", "taskId": updated["taskId"],
                "participantCancelFailures": failures,
                "content": "Council execution was cancelled; completed participant evidence was preserved."},
                error=bool(failures))

        if registered is not None and registered.get("backend") == "opencode":
            if name == "read_task":
                return _tool_result(await _read_opencode_task(
                    registered, detail=arguments.get("detail", "summary"),
                ))
            if name == "cancel_task":
                current = await _read_opencode_task(registered)
                if current.get("status") != "running":
                    return _tool_result({
                        "status": "not_running", "taskId": registered["taskId"],
                        "taskReference": registered["taskId"],
                        "content": "The OpenCode session has no active turn; its history was preserved.",
                    })
                try:
                    accepted = await OPENCODE_BACKEND.cancel(
                        session_id=str(registered["backendSessionId"]), directory=str(registered["cwd"]),
                    )
                except OpenCodeError as exc:
                    return _tool_result({"status": "outcome_unknown", "taskId": registered["taskId"],
                        "errorCategory": exc.category, "content": str(exc)}, error=True)
                TASK_REGISTRY.update(registered["taskId"], status="cancelling")
                return _tool_result({
                    "status": "cancel_requested" if accepted else "outcome_unknown",
                    "taskId": registered["taskId"], "taskReference": registered["taskId"],
                    "content": "OpenCode abort was requested; the same session and history were preserved.",
                }, error=not accepted)
            assert name == "continue_task"
            current = await _read_opencode_task(registered)
            if current.get("status") == "running":
                return _tool_result({
                    "status": "conflict", "taskId": registered["taskId"],
                    "content": "The OpenCode session is active. Cancel it or wait for terminal status "
                               "before continuing the same session.",
                }, error=True)
            requested_access = _continuation_access(arguments)
            if requested_access is not None and requested_access != registered.get("access"):
                return _tool_result({
                    "status": "conflict", "taskId": registered["taskId"],
                    "content": "OpenCode session access cannot be changed after creation; start a new "
                               "task for a different permission boundary.",
                }, error=True)
            if arguments.get("reasoning_effort") is not None:
                return _tool_result({"status": "failed", "taskId": registered["taskId"],
                    "content": "reasoning_effort is not supported by the OpenCode backend."}, error=True)
            provider_id = str(registered["provider"])
            model_id = str(registered["model"])
            selection = str(registered.get("selection") or f"{provider_id}/{model_id}")
            if arguments.get("provider") is not None or arguments.get("model") is not None:
                try:
                    selected = await OPENCODE_BACKEND.resolve_model(
                        provider=arguments.get("provider", provider_id),
                        model=arguments.get("model", selection), directory=str(registered["cwd"]),
                    )
                except OpenCodeError as exc:
                    return _tool_result({"status": "failed", "taskId": registered["taskId"],
                        "errorCategory": exc.category, "content": str(exc)}, error=True)
                provider_id = selected["provider"]
                model_id = selected["nativeModelId"]
                selection = selected["selection"]
            try:
                await OPENCODE_BACKEND.prompt(
                    session_id=str(registered["backendSessionId"]), directory=str(registered["cwd"]),
                    prompt=arguments["prompt"], provider=provider_id, model_id=model_id,
                    access=str(registered["access"]),
                )
            except OpenCodeError as exc:
                return _tool_result({"status": "failed", "taskId": registered["taskId"],
                    "errorCategory": exc.category, "content": str(exc)}, error=True)
            updated = TASK_REGISTRY.update(registered["taskId"], status="running",
                provider=provider_id, model=model_id, selection=selection,
                finishedAt=None, errorCategory=None)
            return _tool_result({
                "status": "running", "taskId": updated["taskId"],
                "taskReference": updated["taskId"], "backend": "opencode",
                "provider": provider_id, "model": selection,
                "action": "continued_same_session",
                "content": "The same OpenCode session was continued in the background.",
            })

        if registered is not None and registered.get("backend") == "codex":
            arguments = dict(arguments)
            arguments["task"] = str(registered["backendSessionId"])

        thread, task_error = await _resolve_task(history, arguments["task"], project)
        if thread is None:
            assert task_error is not None
            return _tool_result(task_error, error=True)
        thread_id = str(thread["id"])
        catalog = _project_catalog(); entry = _project_by_cwd(catalog)[str(thread["cwd"])]
        record = _task_record(thread, entry, thread_id)
        registered_codex = TASK_REGISTRY.find_by_backend_session("codex", thread_id)

        if name == "cancel_task":
            metadata = _load_thread_metadata(thread_id)
            if metadata is None or metadata.get("cwd") != entry.path:
                return _tool_result({"status": "failed", "threadId": thread_id,
                    "content": (
                        "This task has no trusted bridge sandbox metadata, so MCP will not "
                        "interrupt a VS Code or CLI-owned thread."
                    )}, error=True)
            root_result = await _interrupt_thread_turn(
                history, thread_id=thread_id, cwd=entry.path,
            )
            descendant_results: list[dict[str, Any]] = []
            descendant_error: str | None = None
            try:
                descendants = await _native_descendants(
                    history, ancestor_thread_id=thread_id, cwd=entry.path,
                )
                for descendant in descendants:
                    descendant_results.append(await _interrupt_thread_turn(
                        history, thread_id=str(descendant["id"]), cwd=entry.path,
                    ))
            except BaseException:
                descendant_error = "descendant_scan_failed"

            requested = [item for item in [root_result, *descendant_results]
                if item.get("status") in {"cancel_requested", "terminal_after_race"}]
            unknown = [item for item in [root_result, *descendant_results]
                if item.get("status") in {"outcome_unknown", "invalid_thread", "invalid_turn"}]
            if not requested and not unknown and descendant_error is None:
                return _tool_result({
                    "status": "not_running",
                    "threadId": thread_id,
                    "taskReference": thread_id,
                    "descendantsChecked": len(descendant_results),
                    "content": (
                        "The task and its known descendants have no active turns. Their thread "
                        "history was left intact; "
                        "use continue_task to add corrected instructions."
                    ),
                    "resumeCommand": f"codex resume {thread_id}",
                })
            partial = bool(unknown or descendant_error)
            if registered_codex is not None:
                TASK_REGISTRY.update(registered_codex["taskId"],
                    status="outcome_unknown" if partial else "cancelling")
            return _tool_result({
                "status": "outcome_unknown" if partial else "cancel_requested",
                "threadId": thread_id,
                "turnId": root_result.get("turnId"),
                "taskReference": thread_id,
                "root": root_result,
                "descendants": descendant_results,
                "descendantError": descendant_error,
                "content": (
                    "Cancellation was requested for the active task and its active sub-agent "
                    "descendants. The thread was preserved. Poll read_task until terminal, "
                    "then continue_task the same task with corrected instructions."
                    if not partial else
                    "Cancellation had a transport or descendant-cleanup race. No mutating RPC "
                    "was replayed blindly; inspect read_task before retrying."
                ),
                "resumeCommand": f"codex resume {thread_id}",
            }, error=partial)

        if name == "read_task":
            read_result = await history.request("thread/read",
                {"threadId": thread_id, "includeTurns": True})
            native_thread = read_result.get("thread")
            if not isinstance(native_thread, dict) or native_thread.get("cwd") != entry.path:
                return _tool_result({"status": "failed",
                    "content": "Native Codex history returned an invalid task."}, error=True)
            record = _task_record(native_thread, entry, record["taskReference"])
            summary = _turn_summary(_last_turn(native_thread))
            turn_status = summary.get("status") if isinstance(summary, dict) else None
            public_status = {
                "inProgress": "running",
                "completed": "completed",
                "failed": "failed",
                "interrupted": "interrupted",
            }.get(turn_status, "unknown")
            if registered_codex is not None:
                changes: dict[str, Any] = {"status": public_status}
                if public_status in {"completed", "failed", "interrupted"}:
                    changes["finishedAt"] = int(time.time() * 1000)
                registered_codex = TASK_REGISTRY.update(registered_codex["taskId"], **changes)
            payload = {
                # The read operation itself succeeded even when the Codex turn
                # failed; preserve the terminal state instead of misreporting it
                # as a bridge/tool-call failure.
                "status": public_status,
                "task": record,
                "latestTurn": summary,
                "content": _status_content(summary),
                "resumeCommand": f"codex resume {thread_id}",
            }
            if registered_codex is not None:
                payload["taskId"] = registered_codex["taskId"]
            if arguments.get("detail", "summary") == "full":
                payload["history"] = native_thread.get("turns", [])
            return _tool_result(payload)

        assert name == "continue_task"
        metadata = _load_thread_metadata(thread_id)
        if metadata is None or metadata.get("cwd") != entry.path:
            return _tool_result({
                "status": "failed",
                "threadId": thread_id,
                "content": (
                    "This task has no trusted bridge sandbox metadata. Start a new task "
                    "or continue it explicitly with the Codex CLI."
                ),
                "resumeCommand": f"codex resume {thread_id}",
            }, error=True)
        if arguments.get("provider") not in (None, "codex"):
            return _tool_result({
                "status": "conflict", "threadId": thread_id,
                "content": "A Codex task cannot change execution backend. Start a new "
                           "OpenCode task with the requested provider.",
            }, error=True)
        read_result = await history.request("thread/read",
            {"threadId": thread_id, "includeTurns": True})
        native_thread = read_result.get("thread")
        if not isinstance(native_thread, dict) or native_thread.get("cwd") != entry.path:
            return _tool_result({"status": "failed", "threadId": thread_id,
                "content": "Native Codex history returned an invalid task."}, error=True)
        active_turn = _last_turn(native_thread)
        if isinstance(active_turn, dict) and active_turn.get("status") == "inProgress":
            summary = _turn_summary(active_turn)
            requested_access = _continuation_access(arguments)
            explicit_model_settings = (
                arguments.get("model") is not None
                or arguments.get("reasoning_effort") is not None
            )
            selected_model = metadata.get("model")
            selected_effort = metadata.get("reasoningEffort")
            if explicit_model_settings:
                selected_model, selected_effort = await _resolve_model_selection(
                    history,
                    arguments.get("model", metadata.get("model")),
                    arguments.get("reasoning_effort", metadata.get("reasoningEffort")),
                )
            policy_change_requested = (
                requested_access is not None
                and requested_access != metadata.get("access")
            )
            model_change_requested = (
                arguments.get("model") is not None
                and selected_model != metadata.get("model")
            )
            effort_change_requested = (
                arguments.get("reasoning_effort") is not None
                and selected_effort != metadata.get("reasoningEffort")
            )
            if policy_change_requested or model_change_requested or effort_change_requested:
                return _tool_result({
                    "status": "conflict",
                    "threadId": thread_id,
                    "turnId": active_turn.get("id"),
                    "activeTurnStatus": "running",
                    "content": (
                        "The task is still running and an access/model/reasoning change cannot "
                        "be applied mid-turn. Use cancel_task, wait for a terminal status, then "
                        "continue_task the same thread with the requested settings."
                    ),
                    "latestTurn": summary,
                    "resumeCommand": f"codex resume {thread_id}",
                }, error=True)
            active_turn_id = active_turn.get("id")
            if not isinstance(active_turn_id, str) or not active_turn_id:
                return _tool_result({
                    "status": "failed", "threadId": thread_id,
                    "content": "The active Codex turn has no valid turnId.",
                }, error=True)
            try:
                steered = await history.request("turn/steer", {
                    "threadId": thread_id,
                    "expectedTurnId": active_turn_id,
                    "input": [{"type": "text", "text": arguments["prompt"]}],
                })
            except BaseException:
                race_status = "outcome_unknown"
                fresh_summary = summary
                with suppress(BaseException):
                    reread = await history.request("thread/read", {
                        "threadId": thread_id, "includeTurns": True,
                    })
                    fresh_thread = reread.get("thread")
                    fresh_turn = _last_turn(fresh_thread) if isinstance(fresh_thread, dict) else None
                    fresh_summary = _turn_summary(fresh_turn)
                    if not isinstance(fresh_turn, dict) or fresh_turn.get("status") != "inProgress":
                        race_status = "terminal_race"
                    elif fresh_turn.get("id") != active_turn_id:
                        race_status = "active_turn_changed"
                return _tool_result({
                    "status": "conflict",
                    "reason": race_status,
                    "threadId": thread_id,
                    "turnId": active_turn_id,
                    "latestTurn": fresh_summary,
                    "content": (
                        "The active turn changed or completed while applying the correction, "
                        "or the response outcome is unknown. The bridge did not replay the "
                        "mutation or create a duplicate turn. Read the task before retrying."
                    ),
                    "resumeCommand": f"codex resume {thread_id}",
                }, error=True)
            steered_turn_id = steered.get("turnId")
            if steered_turn_id != active_turn_id:
                return _tool_result({
                    "status": "failed", "threadId": thread_id,
                    "content": "Codex returned an invalid turnId after steering; no retry was made.",
                }, error=True)
            payload = {
                "status": "running",
                "threadId": thread_id,
                "turnId": steered_turn_id,
                "taskReference": thread_id,
                "action": "steered_active_turn",
                "content": (
                    "The correction was delivered to the active Codex turn. No duplicate task "
                    "or thread was created; use read_task to check the same task."
                ),
                "latestTurn": summary,
                "resumeCommand": f"codex resume {thread_id}",
            }
            if registered_codex is not None:
                payload["taskId"] = registered_codex["taskId"]
            return _tool_result(payload)
        selected_model, selected_effort = await _resolve_model_selection(
            history,
            arguments.get("model", metadata.get("model")),
            arguments.get("reasoning_effort", metadata.get("reasoningEffort")),
        )
        resume_metadata = _continuation_metadata(metadata, _continuation_access(arguments))
        resume_metadata["model"] = selected_model
        resume_metadata["reasoningEffort"] = selected_effort
        policy_changed = (
            metadata.get("access") != resume_metadata.get("access")
            or metadata.get("permissions") != resume_metadata.get("permissions")
        )
        try:
            if policy_changed:
                resumed = await history.resume_for_policy_change(
                    thread_id, resume_metadata
                )
            else:
                resumed = await history.ensure_thread_loaded(
                    thread_id,
                    resume_metadata,
                )
        except BaseException as exc:
            log.error("native asynchronous thread resume failed (%s)", type(exc).__name__)
            return _tool_result({"status": "failed", "threadId": thread_id,
                "content": "Codex app-server could not resume the task.",
                "resumeCommand": f"codex resume {thread_id}"}, error=True)
        if resumed is not None:
            if not _effective_sandbox_matches(
                resumed, cwd=entry.path, sandbox=resume_metadata["sandbox"],
                permissions=resume_metadata.get("permissions"),
            ):
                log.error("Codex app-server resumed a thread with an unexpected effective policy")
                return _tool_result({"status": "failed", "threadId": thread_id,
                    "content": "Codex refused the task's fixed sandbox policy; no turn was started.",
                    "resumeCommand": f"codex resume {thread_id}"}, error=True)
            effective_settings = _effective_model_settings(
                resumed,
                requested_model=selected_model,
                requested_effort=selected_effort,
            )
            if effective_settings is None:
                log.error("Codex app-server resumed a thread with unexpected model settings")
                return _tool_result({"status": "failed", "threadId": thread_id,
                    "content": "Codex did not apply the requested model/reasoning settings; no turn was started.",
                    "resumeCommand": f"codex resume {thread_id}"}, error=True)
            selected_model, selected_effort = effective_settings
        turn_params = {
            "threadId": thread_id,
            "input": [{"type": "text", "text": arguments["prompt"]}],
            "cwd": entry.path,
            "approvalPolicy": "never",
        }
        if resume_metadata.get("permissions") is None:
            turn_params["sandboxPolicy"] = _turn_sandbox_policy(
                resume_metadata["sandbox"], entry.path
            )
        if selected_model is not None:
            turn_params["model"] = selected_model
        if selected_effort is not None:
            turn_params["effort"] = selected_effort
        try:
            turn_started = await history.request("turn/start", turn_params)
        except BaseException as exc:
            log.error("native asynchronous continuation start failed (%s)", type(exc).__name__)
            return _tool_result({"status": "failed", "threadId": thread_id,
                "content": "Codex could not start the continuation turn.",
                "resumeCommand": f"codex resume {thread_id}"}, error=True)
        turn = turn_started.get("turn")
        turn_id = turn.get("id") if isinstance(turn, dict) else None
        if not isinstance(turn_id, str) or not turn_id:
            return _tool_result({"status": "failed", "threadId": thread_id,
                "content": "Codex returned no turnId for the continuation.",
                "resumeCommand": f"codex resume {thread_id}"}, error=True)
        _save_thread_metadata(
            thread_id,
            project=entry.canonical,
            access=resume_metadata["access"],
            cwd=entry.path,
            sandbox=resume_metadata["sandbox"],
            permissions=resume_metadata.get("permissions"),
            model=selected_model,
            reasoning_effort=selected_effort,
        )
        if registered_codex is not None:
            registered_codex = TASK_REGISTRY.update(
                registered_codex["taskId"], status="running",
                access=resume_metadata["access"], model=selected_model,
                finishedAt=None, errorCategory=None,
            )
        payload = {"status": "running", "threadId": thread_id,
            "turnId": turn_id,
            "taskReference": record["taskReference"], "name": record["name"],
            "canonicalProject": entry.canonical, "cwd": entry.path,
            "access": resume_metadata["access"],
            "model": selected_model, "reasoningEffort": selected_effort,
            "selectionVerified": True,
            "content": "Codex continuation started in the background. Use read_task to check status and obtain the result.",
            "resumeCommand": f"codex resume {thread_id}"}
        if registered_codex is not None:
            payload["taskId"] = registered_codex["taskId"]
        return _tool_result(payload)

    async def call_tool(_ctx: Any, params: types.CallToolRequestParams) -> types.CallToolResult:
        try:
            if params.name in {"start_task", "continue_task", "cancel_task"}:
                async with task_lifecycle_lock:
                    return await _call_tool_impl(_ctx, params)
            return await _call_tool_impl(_ctx, params)
        except MCPError:
            raise
        except Exception as exc:
            # Do not let AnyIO/MCP collapse the actionable boundary into a bare
            # ExceptionGroup.  The full traceback stays in the private journal;
            # the remote response contains no environment values or secrets.
            log.exception("tool %s failed (%s)", params.name, type(exc).__name__)
            return _tool_result({
                "status": "failed",
                "content": (
                    f"Codex DevCoveer internal error ({type(exc).__name__}). "
                    "The detailed traceback is available in the local service journal."
                ),
            }, error=True)

    return Server(
        "openai-codex-mcp-bridge", version=BRIDGE_VERSION,
        title="Codex DevCoveer",
        description="Asynchronous natural-project bridge to native Codex app-server tasks.",
        instructions=(
            "Infer a project hint from natural language and use start_task. Infer read for "
            "analysis/review/status and write for implementation/fixes. start_task and "
            "continue_task return immediately while Codex works asynchronously in its native "
            "app-server. If an existing read-only task moves to implementation/fixes, continue "
            "that same task with access=write rather than creating a competing task. Omit "
            "continue_task access only when the existing access must be preserved. "
            "Preserve the returned task reference and use read_task to check runtime "
            "status and obtain the final result; do not treat status=running as completion. "
            "If the user explicitly names Terra, Sol, or a reasoning level, map it to the exact "
            "model and reasoning_effort tool fields (for example gpt-5.6-terra + high); never "
            "leave an explicit model/effort request only inside prompt text. Preserve those "
            "settings on continuation unless the user explicitly overrides them. "
            "If a task is still running, continue_task steers compatible correction text into "
            "that active turn instead of creating a duplicate. When access/model/reasoning must "
            "change, use cancel_task, wait for terminal status, and continue the same task. "
            "Use list_models with a provider filter before selecting non-default models. "
            "For an unqualified request to run a council, call council_run with tier omitted "
            "or tier=free and do not add start_task, Codex, NVIDIA, or any outside reviewer. "
            "extended/pro are paid-policy tiers: their first call only returns a budget and "
            "one-time token. Show that plan and wait for a new explicit user confirmation; "
            "never replay the token in the same autonomous turn. "
            "A running task remains available after this ChatGPT response, so report that it was "
            "started and let the user request the result later rather than holding one tool call "
            "open. Use list_tasks and read_task for native history; never ask the user for cwd, "
            "sandbox, approval policy, or threadId. Resolve ambiguity rather than guessing."
        ),
        on_list_tools=list_tools, on_call_tool=call_tool,
    )


async def _watch_signals(cancel_scope: anyio.CancelScope) -> None:
    with anyio.open_signal_receiver(signal.SIGTERM, signal.SIGINT) as signals:
        async for received in signals:
            log.info("received signal %s; shutting down", signal.Signals(received).name)
            cancel_scope.cancel()
            return


async def _serve_and_cancel(
    server: Server[Any], read_stream: Any, write_stream: Any, cancel_scope: anyio.CancelScope
) -> None:
    try:
        await server.run(
            read_stream,
            write_stream,
            server.create_initialization_options(),
        )
    finally:
        cancel_scope.cancel()


async def main() -> None:
    history = NativeHistoryClient()
    server = build_server(history)
    try:
        async with stdio_server() as (read_stream, write_stream):
            async with anyio.create_task_group() as task_group:
                task_group.start_soon(
                    _serve_and_cancel,
                    server,
                    read_stream,
                    write_stream,
                    task_group.cancel_scope,
                )
                task_group.start_soon(_watch_signals, task_group.cancel_scope)
    finally:
        await history.close()


if __name__ == "__main__":
    try:
        anyio.run(main)
    except KeyboardInterrupt:
        pass
