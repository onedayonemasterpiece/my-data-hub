#!/usr/bin/env python3
"""Run one fixed FM05/FM14/FM15 acceptance operation inside Kaggle."""

from __future__ import annotations

import argparse
import json
import os
import re
import stat
import sys
from collections.abc import Callable, Sequence
from datetime import UTC, datetime
from pathlib import Path

from pydantic import ValidationError

from my_data_hub.checkpoints.acceptance import CheckpointAcceptanceReceipt
from my_data_hub.checkpoints.acceptance_runtime import (
    CheckpointAcceptanceEntrypointBlocker,
    CheckpointAcceptanceOperationalResult,
    CheckpointAcceptanceProductionConfig,
    CheckpointAcceptanceProviderLocator,
    CheckpointAcceptanceRuntimeAmbiguity,
    ProductionCheckpointAcceptanceRuntime,
    build_production_checkpoint_acceptance_runtime,
)
from my_data_hub.hashing import canonical_json_bytes

EX_CONFIG = 78
FAIL = 1
MAX_CONFIG_BYTES = 256 * 1024
MAX_RESULT_BYTES = 256 * 1024
RUNTIME_ROOT = Path("/kaggle/working")
CONFIG_FILE_NAME = "checkpoint-acceptance-config.json"
RESULT_FILE_NAME = "operational-result.json"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    return parser


def _runtime_path(path: Path, *, exact_name: str) -> Path:
    if not path.is_absolute() or path.name != exact_name or path.is_symlink():
        raise ValueError(f"{exact_name} must be an absolute non-symlink path with its fixed name")
    root = RUNTIME_ROOT.resolve()
    resolved = path.resolve()
    if path != resolved:
        raise ValueError(f"{exact_name} path must be normalized")
    if resolved == root or root not in resolved.parents:
        raise ValueError(f"{exact_name} must stay below /kaggle/working")
    current = path
    while current != RUNTIME_ROOT and current != current.parent:
        if current.exists() and current.is_symlink():
            raise ValueError(f"{exact_name} path contains a symbolic link")
        current = current.parent
    return path


def load_config(path: Path) -> CheckpointAcceptanceProductionConfig:
    path = _runtime_path(path, exact_name=CONFIG_FILE_NAME)
    flags = os.O_RDONLY | getattr(os, "O_CLOEXEC", 0) | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags)
    try:
        metadata = os.fstat(descriptor)
        if not stat.S_ISREG(metadata.st_mode) or stat.S_IMODE(metadata.st_mode) != 0o600:
            raise ValueError("checkpoint acceptance config must be a regular mode-0600 file")
        if not 1 <= metadata.st_size <= MAX_CONFIG_BYTES:
            raise ValueError("checkpoint acceptance config is empty or exceeds 256 KiB")
        with os.fdopen(descriptor, "rb", closefd=False) as handle:
            raw = handle.read(MAX_CONFIG_BYTES + 1)
        if not 1 <= len(raw) <= MAX_CONFIG_BYTES:
            raise ValueError("checkpoint acceptance config changed or exceeds 256 KiB")
    finally:
        os.close(descriptor)
    return CheckpointAcceptanceProductionConfig.model_validate_json(raw)


def _locator(
    config: CheckpointAcceptanceProductionConfig,
    receipt: CheckpointAcceptanceReceipt | None = None,
) -> CheckpointAcceptanceProviderLocator:
    refs: list[str] = []
    if receipt is not None:
        for stage in receipt.stages:
            if stage.exact_version_ref is not None and stage.exact_version_ref not in refs:
                refs.append(stage.exact_version_ref)
    return CheckpointAcceptanceProviderLocator(
        provider_owner=config.provider_owner,
        dataset_ref=config.dataset_ref,
        evidence_notebook_ref=config.evidence_notebook_ref,
        verifier_notebook_ref=config.verifier_notebook_ref,
        template_dataset_version_ref=config.template_dataset_version_ref,
        template_claim_sha256=config.template_claim_sha256,
        verifier_dataset_version_ref=(config.verifier.dataset_version_ref if config.verifier is not None else None),
        verifier_claim_sha256=(config.verifier.claim_sha256 if config.verifier is not None else None),
        exact_version_refs=tuple(refs),
    )


def _ready_result(
    config: CheckpointAcceptanceProductionConfig,
    receipt: CheckpointAcceptanceReceipt,
    *,
    completed_at: datetime,
) -> CheckpointAcceptanceOperationalResult:
    if (
        receipt.scenario != config.scenario
        or receipt.operation_id != config.operation_id
        or receipt.task_run_id != config.task_run_id
        or receipt.evidence_class != "live"
        or receipt.verdict != "LIVE_PASS"
    ):
        raise CheckpointAcceptanceRuntimeAmbiguity("terminal receipt differs from the exact live config")
    return CheckpointAcceptanceOperationalResult(
        scenario=config.scenario,
        outcome="LIVE_EVIDENCE_READY",
        live_evidence=True,
        operation_id=config.operation_id,
        task_run_id=config.task_run_id,
        source_revision=config.source_revision,
        config_sha256=config.config_sha256,
        mutations_started=max(1, len(receipt.stages)),
        locator=_locator(config, receipt),
        candidate_checkpoint_id=receipt.candidate_checkpoint_id,
        intent_sha256=receipt.intent_sha256,
        verdict="LIVE_PASS",
        evidence_class="live",
        initial_head=receipt.initial_head,
        final_head=receipt.final_head,
        head_unchanged=receipt.head_unchanged,
        stages=receipt.stages,
        receipt=receipt,
        receipt_sha256=receipt.receipt_sha256,
        completed_at=completed_at,
    )


def _blocked_result(
    config: CheckpointAcceptanceProductionConfig,
    code: str,
    *,
    completed_at: datetime,
) -> CheckpointAcceptanceOperationalResult:
    return CheckpointAcceptanceOperationalResult(
        scenario=config.scenario,
        outcome="BLOCKED",
        live_evidence=False,
        operation_id=config.operation_id,
        task_run_id=config.task_run_id,
        source_revision=config.source_revision,
        config_sha256=config.config_sha256,
        mutations_started=0,
        locator=_locator(config),
        blocker_code=code,
        completed_at=completed_at,
    )


def _failure_code(exc: BaseException) -> str:
    raw = re.sub(r"(?<!^)(?=[A-Z])", "_", type(exc).__name__).upper()
    code = re.sub(r"[^A-Z0-9_]", "_", raw).strip("_")[:120]
    return code if re.fullmatch(r"[A-Z][A-Z0-9_]{2,119}", code) else "CHECKPOINT_ACCEPTANCE_FAILED"


def _failed_result(
    config: CheckpointAcceptanceProductionConfig,
    exc: BaseException,
    *,
    completed_at: datetime,
) -> CheckpointAcceptanceOperationalResult:
    return CheckpointAcceptanceOperationalResult(
        scenario=config.scenario,
        outcome="FAIL",
        live_evidence=False,
        operation_id=config.operation_id,
        task_run_id=config.task_run_id,
        source_revision=config.source_revision,
        config_sha256=config.config_sha256,
        mutations_started=1,
        locator=_locator(config),
        failure_code=_failure_code(exc),
        completed_at=completed_at,
    )


def write_result(path: Path, result: CheckpointAcceptanceOperationalResult) -> None:
    path = _runtime_path(path, exact_name=RESULT_FILE_NAME)
    payload = canonical_json_bytes(result.model_dump(mode="json", exclude_none=True)) + b"\n"
    if len(payload) > MAX_RESULT_BYTES:
        raise ValueError("checkpoint acceptance operational result exceeds 256 KiB")
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
        os.chmod(path, 0o600)
    finally:
        if temporary.exists():
            temporary.unlink()


def execute(
    config: CheckpointAcceptanceProductionConfig,
    *,
    factory: Callable[[CheckpointAcceptanceProductionConfig], ProductionCheckpointAcceptanceRuntime] = (
        build_production_checkpoint_acceptance_runtime
    ),
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> tuple[int, CheckpointAcceptanceOperationalResult]:
    try:
        runtime = factory(config)
    except CheckpointAcceptanceEntrypointBlocker as exc:
        return EX_CONFIG, _blocked_result(config, exc.code, completed_at=clock())
    except Exception as exc:
        return FAIL, _failed_result(config, exc, completed_at=clock())
    try:
        receipt = runtime.run()
        return 0, _ready_result(config, receipt, completed_at=clock())
    except Exception as exc:
        return FAIL, _failed_result(config, exc, completed_at=clock())


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        output = _runtime_path(args.output, exact_name=RESULT_FILE_NAME)
        config = load_config(args.config)
    except (OSError, ValueError, ValidationError, json.JSONDecodeError) as exc:
        print(_failure_code(exc), file=sys.stderr)
        return EX_CONFIG
    exit_code, result = execute(config)
    try:
        write_result(output, result)
    except (OSError, ValueError, ValidationError, json.JSONDecodeError) as exc:
        print(_failure_code(exc), file=sys.stderr)
        return EX_CONFIG if exit_code == EX_CONFIG else FAIL
    print(result.outcome)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
