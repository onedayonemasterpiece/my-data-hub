#!/usr/bin/env python3
"""Fail closed when tracked content or reachable Git patches contain credentials."""

from __future__ import annotations

import re
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MAX_FILE_BYTES = 8 * 1024 * 1024
MAX_HISTORY_BYTES = 128 * 1024 * 1024

_STRONG_PATTERNS: tuple[tuple[str, re.Pattern[str]], ...] = (
    ("private-key", re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----")),
    ("github-token", re.compile(r"\b(?:github_pat_[A-Za-z0-9_]{20,}|gh[pousr]_[A-Za-z0-9]{20,})\b")),
    ("openai-token", re.compile(r"\bsk-(?:proj-)?[A-Za-z0-9_-]{24,}\b")),
    ("slack-token", re.compile(r"\bxox[baprs]-[A-Za-z0-9-]{20,}\b")),
    ("google-api-key", re.compile(r"\bAIza[0-9A-Za-z_-]{20,}\b")),
    ("aws-access-key", re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b")),
    ("stripe-live-key", re.compile(r"\b(?:sk|rk)_live_[0-9A-Za-z]{16,}\b")),
)
_ASSIGNMENT = re.compile(
    r"(?m)^[ \t]*(?:export[ \t]+)?"
    r"(?:KAGGLE_API_TOKEN|KAGGLE_KEY|[A-Z0-9_]*(?:PASSWORD|SECRET|ACCESS_TOKEN|REFRESH_TOKEN|PRIVATE_KEY))"
    r"[ \t]*[=:][ \t]*['\"]?([^\s'\"#]+)"
)
_SAFE_ASSIGNMENT_FRAGMENTS = (
    "${{",
    "${",
    "$",
    "example",
    "placeholder",
    "integration-only",
    "test-only",
    "change-me",
    "change_me",
    "not-configured",
    "<redacted>",
    "correct-horse-battery-staple",
    "must-not-appear",
    "runtime-secret-long-enough",
)

# A historical unit-test patch used this exact deliberately invalid three-line
# value to exercise TLS file plumbing. Keep the reachable-history scan strict
# for every real PEM body while allowing only that byte-for-byte sentinel.
_INVALID_TEST_PEM_SENTINEL = (
    "-----BEGIN " + "PRIVATE KEY-----\nTEST\n-----END PRIVATE " + "KEY-----"
)
_INVALID_TEST_PEM_SOURCE_SENTINEL = _INVALID_TEST_PEM_SENTINEL.replace("\n", "\\n")


def findings(text: str) -> list[str]:
    scanned = text.replace(_INVALID_TEST_PEM_SENTINEL, "test-only-invalid-pem-sentinel")
    scanned = scanned.replace(_INVALID_TEST_PEM_SOURCE_SENTINEL, "test-only-invalid-pem-sentinel")
    found = [name for name, pattern in _STRONG_PATTERNS if pattern.search(scanned)]
    for match in _ASSIGNMENT.finditer(scanned):
        value = match.group(1)
        lowered = value.casefold()
        if len(value) < 16 or any(fragment in lowered for fragment in _SAFE_ASSIGNMENT_FRAGMENTS):
            continue
        if len(set(value)) == 1:
            continue
        found.append("credential-assignment")
        break
    return sorted(set(found))


def _git(*arguments: str, max_bytes: int) -> bytes:
    result = subprocess.run(
        ["git", *arguments],
        cwd=ROOT,
        check=True,
        capture_output=True,
        timeout=120,
    )
    if len(result.stdout) > max_bytes:
        raise RuntimeError("secret-scan Git input exceeded its bounded size")
    return result.stdout


def scan_repository() -> list[str]:
    failures: list[str] = []
    tracked = _git("ls-files", "-z", max_bytes=16 * 1024 * 1024).split(b"\0")
    for raw_path in tracked:
        if not raw_path:
            continue
        relative = raw_path.decode("utf-8", errors="strict")
        path = ROOT / relative
        if not path.is_file() or path.is_symlink():
            failures.append(f"unsafe tracked path: {relative}")
            continue
        size = path.stat().st_size
        if size > MAX_FILE_BYTES:
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue
        for kind in findings(text):
            failures.append(f"tracked {kind}: {relative}")

    history = _git(
        "log",
        "--all",
        "--format=commit:%H",
        "-p",
        "--no-ext-diff",
        "--no-textconv",
        max_bytes=MAX_HISTORY_BYTES,
    ).decode("utf-8", errors="replace")
    for kind in findings(history):
        failures.append(f"reachable-history {kind}")
    return sorted(set(failures))


def main() -> int:
    failures = scan_repository()
    if failures:
        for failure in failures:
            print(f"ERROR: {failure}")
        return 1
    print("tracked-secret scan: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
