#!/usr/bin/env python3
from __future__ import annotations

import asyncio
import base64
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Mapping

from scripts.provider.operational_kaggle_driver import (
    DEFAULT_ENDPOINT,
    RemoteMcpGateway,
    _tokens_from_environment,
    bearer_source_from_environment,
)

ROOT = Path(__file__).resolve().parents[2]
CONTROL = ROOT / "artifacts" / "tts-yazyki-rossii"
CONTROL.mkdir(parents=True, exist_ok=True)
NOTEBOOK_SOURCE_PATH = ROOT / "scratch" / "tts-yazyki-rossii" / "notebook.py"

RESOURCE_REF = "zigomaro/yazyki-rossii-qwen3-tts-0830-v2"
TASK_ID = "17039ad4-b5bf-4a50-8165-2978b4acc272"
EFFECT_ID = "033fc33d-77b6-4b20-89d3-7cd0a669f0e8"
TASK_RUN_ID = "b782bf2d-4465-460d-bf78-6834bbde7d54"
IDEMPOTENCY_KEY = "qwen3-tts-yazyki-rossii-20260830-b782bf2d"
CLAIM_FILE = CONTROL / "claim.json"
FINAL_READ_FILE = CONTROL / "final-read.json"
FINAL_LIST_FILE = CONTROL / "final-list.json"

EXPECTED_OUTPUTS = [
    {"path": "audio.mp3", "max_bytes": 8 * 1024 * 1024, "media_type": "audio/mpeg"},
    {"path": "status.json", "max_bytes": 1024 * 1024, "media_type": "application/json"},
    {"path": "run.log", "max_bytes": 16 * 1024 * 1024, "media_type": "text/plain"},
    {"path": "dependency-readback.json", "max_bytes": 4 * 1024 * 1024, "media_type": "application/json"},
    {"path": "source-reference.json", "max_bytes": 1024 * 1024, "media_type": "application/json"},
    {"path": "raw-extraction.txt", "max_bytes": 512 * 1024, "media_type": "text/plain"},
    {"path": "extraction-report.json", "max_bytes": 1024 * 1024, "media_type": "application/json"},
    {"path": "speech-text.txt", "max_bytes": 512 * 1024, "media_type": "text/plain"},
    {"path": "normalization-report.json", "max_bytes": 1024 * 1024, "media_type": "application/json"},
    {"path": "critical-token-ledger.json", "max_bytes": 1024 * 1024, "media_type": "application/json"},
    {"path": "voice-plan.json", "max_bytes": 1024 * 1024, "media_type": "application/json"},
    {"path": "chunk-map.json", "max_bytes": 2 * 1024 * 1024, "media_type": "application/json"},
    {"path": "segments-manifest.json", "max_bytes": 8 * 1024 * 1024, "media_type": "application/json"},
    {"path": "asr-readback.json", "max_bytes": 16 * 1024 * 1024, "media_type": "application/json"},
    {"path": "asr-transcript.txt", "max_bytes": 2 * 1024 * 1024, "media_type": "text/plain"},
    {"path": "media-smoke.json", "max_bytes": 2 * 1024 * 1024, "media_type": "application/json"},
    {"path": "artifact-manifest.json", "max_bytes": 4 * 1024 * 1024, "media_type": "application/json"},
]

def dump(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")

def print_compact(label: str, value: object) -> None:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if len(text) > 12000:
        text = text[:12000] + "...<truncated>"
    print(f"{label}: {text}", flush=True)

def find_claim_sha(value: object) -> str | None:
    if isinstance(value, Mapping):
        for key in ("claim_sha256", "claimSha256"):
            candidate = value.get(key)
            if isinstance(candidate, str) and len(candidate) == 64:
                return candidate
        for child in value.values():
            found = find_claim_sha(child)
            if found:
                return found
    elif isinstance(value, list):
        for child in value:
            found = find_claim_sha(child)
            if found:
                return found
    return None

def status_values(value: object) -> list[tuple[str, str]]:
    found: list[tuple[str, str]] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_l = str(key).lower()
            if key_l in {"state", "status", "terminal_state", "provider_status", "run_state"} and isinstance(child, str):
                found.append((key_l, child.lower()))
            found.extend(status_values(child))
    elif isinstance(value, list):
        for child in value:
            found.extend(status_values(child))
    return found

def terminal_class(value: object) -> str | None:
    statuses = status_values(value)
    failure = {"failed", "failure", "error", "cancelled", "canceled"}
    success = {"complete", "completed", "succeeded", "success"}
    if any(status in failure for _, status in statuses):
        return "failure"
    if any(status in success for _, status in statuses):
        return "success"
    return None

def extract_file_names(value: object) -> set[str]:
    names: set[str] = set()
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_l = str(key).lower()
            if key_l in {"path", "name", "file_name", "filename"} and isinstance(child, str):
                names.add(child)
            names.update(extract_file_names(child))
    elif isinstance(value, list):
        for child in value:
            names.update(extract_file_names(child))
    return names

def decode_download_response(value: object) -> bytes | None:
    if isinstance(value, Mapping):
        for key, child in value.items():
            key_l = str(key).lower()
            if isinstance(child, str) and (
                "base64" in key_l or key_l in {"content_b64", "data_b64", "chunk_b64"}
            ):
                try:
                    return base64.b64decode(child, validate=True)
                except Exception:
                    pass
        for child in value.values():
            decoded = decode_download_response(child)
            if decoded is not None:
                return decoded
    elif isinstance(value, list):
        for child in value:
            decoded = decode_download_response(child)
            if decoded is not None:
                return decoded
    return None

async def call_download(gateway: RemoteMcpGateway, claim_sha: str, path: str, max_bytes: int) -> dict[str, Any]:
    response = await gateway.call(
        "provider",
        "provider.resources.download",
        {
            "resource_ref": RESOURCE_REF,
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "claim_sha256": claim_sha,
                "path": path,
                "offset": 0,
                "max_bytes": max_bytes,
            },
        },
    )
    dump(CONTROL / f"download-{path.replace('/', '_')}-response.json", response)
    decoded = decode_download_response(response)
    if decoded is not None:
        (CONTROL / path.replace("/", "_")).write_bytes(decoded)
    return response

async def main() -> int:
    source = NOTEBOOK_SOURCE_PATH.read_text(encoding="utf-8")
    if TASK_RUN_ID not in source:
        raise RuntimeError("notebook source does not contain exact task_run_id")

    gateway = RemoteMcpGateway(
        os.environ.get("MY_DATA_HUB_MCP_CANARY_ENDPOINT", DEFAULT_ENDPOINT),
        bearer_source_from_environment(_tokens_from_environment()),
    )
    catalog = await gateway.catalog("provider")
    required = {
        "provider.resources.run",
        "provider.resources.read",
        "provider.resources.list",
        "provider.resources.download",
    }
    missing = sorted(required - catalog)
    print(f"provider catalog size={len(catalog)} required_missing={missing}", flush=True)
    if missing:
        raise RuntimeError(f"provider MCP toolset incomplete: {missing}")

    run_arguments = {
        "resource_ref": RESOURCE_REF,
        "control_class": "mcp_managed",
        "private": True,
        "payload": {
            "kind": "notebook",
            "task_id": TASK_ID,
            "effect_id": EFFECT_ID,
            "idempotency_key": IDEMPOTENCY_KEY,
            "task_run_id": TASK_RUN_ID,
            "title": RESOURCE_REF.split("/", 1)[1],
            "code_file": "main.py",
            "kernel_type": "script",
            "language": "python",
            "source_utf8": source,
            "dataset_inputs": [],
            "disposable": True,
            "enable_internet": True,
            "accelerator": "none",
            "expected_outputs": EXPECTED_OUTPUTS,
            "timeout_seconds": 43200,
        },
    }
    print("submitting exact idempotent Kaggle CPU notebook run", flush=True)
    claim = await gateway.call("provider", "provider.resources.run", run_arguments)
    dump(CLAIM_FILE, claim)
    print_compact("provider.resources.run", claim)
    claim_sha = find_claim_sha(claim)
    if claim_sha is None:
        raise RuntimeError("provider run response lacks claim_sha256")
    (CONTROL / "claim-sha256.txt").write_text(claim_sha + "\n", encoding="utf-8")

    read_arguments = {
        "resource_ref": RESOURCE_REF,
        "control_class": "mcp_managed",
        "private": True,
        "payload": {"kind": "notebook", "claim_sha256": claim_sha},
    }
    deadline = time.monotonic() + 5.5 * 3600
    poll = 0
    final_read: dict[str, Any] | None = None
    while time.monotonic() < deadline:
        poll += 1
        try:
            current = await gateway.call("provider", "provider.resources.read", read_arguments)
        except Exception as exc:
            print(f"read poll {poll} transient {type(exc).__name__}: {exc}", flush=True)
            await asyncio.sleep(60)
            continue
        dump(CONTROL / "latest-read.json", current)
        statuses = status_values(current)
        if poll == 1 or poll % 5 == 0:
            print(f"poll={poll} statuses={statuses[:20]}", flush=True)
        terminal = terminal_class(current)
        if terminal is not None:
            final_read = current
            print(f"terminal={terminal} poll={poll}", flush=True)
            break
        await asyncio.sleep(60)

    if final_read is None:
        raise RuntimeError("Kaggle notebook did not reach terminal state within 5.5 hours")
    dump(FINAL_READ_FILE, final_read)
    print_compact("final provider.resources.read", final_read)

    list_response = await gateway.call(
        "provider",
        "provider.resources.list",
        {
            "resource_ref": RESOURCE_REF,
            "control_class": "mcp_managed",
            "private": True,
            "payload": {
                "kind": "notebook",
                "claim_sha256": claim_sha,
                "cursor": 0,
                "limit": 100,
            },
        },
    )
    dump(FINAL_LIST_FILE, list_response)
    print_compact("provider.resources.list", list_response)
    names = extract_file_names(list_response)
    print(f"observed output names={sorted(names)}", flush=True)

    # Download bounded diagnostics whether the run succeeded or failed.
    for path, maximum in (
        ("status.json", 1024 * 1024),
        ("artifact-manifest.json", 4 * 1024 * 1024),
        ("media-smoke.json", 2 * 1024 * 1024),
        ("asr-readback.json", 16 * 1024 * 1024),
        ("run.log", 16 * 1024 * 1024),
    ):
        try:
            await call_download(gateway, claim_sha, path, maximum)
        except Exception as exc:
            print(f"download {path} unavailable: {type(exc).__name__}: {exc}", flush=True)

    if terminal_class(final_read) != "success":
        raise RuntimeError("Kaggle notebook reached terminal failure")
    if not any(name == "audio.mp3" or name.endswith("/audio.mp3") for name in names):
        raise RuntimeError("terminal-success run has no audio.mp3 in exact output listing")
    if not any(name == "artifact-manifest.json" or name.endswith("/artifact-manifest.json") for name in names):
        raise RuntimeError("terminal-success run has no artifact-manifest.json")
    print(
        "SUCCESS "
        "kaggle_output=https://www.kaggle.com/code/zigomaro/"
        "yazyki-rossii-qwen3-tts-0830-v2/output "
        f"claim_sha256={claim_sha}",
        flush=True,
    )
    return 0

if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
