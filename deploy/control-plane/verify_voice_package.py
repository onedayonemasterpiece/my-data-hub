#!/usr/bin/env python3
"""Compare effective installed my_data_hub Python sources with one Git commit."""

import argparse
import hashlib
import io
import json
import os
import re
import selectors
import subprocess
import tarfile
import time
from pathlib import Path, PurePosixPath

CHILD_SCRIPT = r"""
import hashlib
import importlib.util
import json
import pathlib
import signal

signal.alarm(20)
spec = importlib.util.find_spec("my_data_hub")
locations = list(spec.submodule_search_locations or []) if spec else []
if len(locations) != 1:
    raise SystemExit(2)
root = pathlib.Path(locations[0]).resolve()
paths = sorted(root.rglob("*.py"))
if not paths or len(paths) > 4096:
    raise SystemExit(2)
files = {}
total = 0
for path in paths:
    if path.is_symlink() or not path.is_file():
        raise SystemExit(2)
    size = path.stat().st_size
    total += size
    if size > 2 * 1024 * 1024 or total > 16 * 1024 * 1024:
        raise SystemExit(2)
    files[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
print(json.dumps({"package_root": str(root), "files": files}, ensure_ascii=False))
"""


class VerificationError(Exception):
    pass


def capture(command, *, cwd=None, limit=1024 * 1024, timeout=30):
    """Bound stdout while reading, suppress stderr, and reap the subprocess."""
    output = bytearray()
    deadline = time.monotonic() + timeout
    try:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
    except OSError:
        raise VerificationError("command_unavailable") from None
    try:
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            while True:
                remaining = deadline - time.monotonic()
                if remaining <= 0 or not selector.select(remaining):
                    raise VerificationError("command_timeout")
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    break
                output.extend(chunk)
                if len(output) > limit:
                    raise VerificationError("command_output_limit")
        if process.wait(timeout=max(0.01, deadline - time.monotonic())):
            raise VerificationError(f"{command[0]}_exit_{process.returncode}")
        return bytes(output)
    except subprocess.TimeoutExpired:
        raise VerificationError("command_timeout") from None
    finally:
        if process.poll() is None:
            process.kill()
        process.wait()
        process.stdout.close()


def manifest_hash(files):
    canonical = json.dumps(files, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def compare(expected, actual):
    return {
        "missing": sorted(expected.keys() - actual.keys()),
        "extra": sorted(actual.keys() - expected.keys()),
        "changed": sorted(path for path in expected.keys() & actual.keys() if expected[path] != actual[path]),
    }


def source_manifest(archive):
    files = {}
    prefix = "src/my_data_hub/"
    with tarfile.open(fileobj=io.BytesIO(archive), mode="r:") as members:
        for member in members:
            if member.name.startswith(prefix) and member.name.endswith(".py"):
                if not member.isfile():
                    raise VerificationError("non_regular_python_source")
                with members.extractfile(member) as source:
                    files[member.name[len(prefix) :]] = hashlib.sha256(source.read()).hexdigest()
    if not files:
        raise VerificationError("empty_expected_package")
    return files


def verify(args):
    repo = capture(["git", "rev-parse", "--show-toplevel"], limit=8192).decode().strip()
    revision = (
        capture(
            ["git", "rev-parse", "--verify", "--end-of-options", args.expected_ref + "^{commit}"],
            cwd=repo,
            limit=256,
        )
        .decode()
        .strip()
    )
    if not re.fullmatch(r"(?:[0-9a-f]{40}|[0-9a-f]{64})", revision):
        raise VerificationError("invalid_git_revision")
    expected = source_manifest(
        capture(
            ["git", "archive", "--format=tar", revision, "--", "src/my_data_hub/*.py"],
            cwd=repo,
            limit=16 * 1024 * 1024,
        )
    )
    if args.image:
        identity = (
            capture(["docker", "image", "inspect", "--format", "{{.Id}}", args.image], limit=256).decode().strip()
        )
        if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity):
            raise VerificationError("invalid_image_id")
        target = {"image_id": identity}
        command = [
            "docker",
            "run",
            "--rm",
            "--network",
            "none",
            "--read-only",
            "--cap-drop",
            "ALL",
            "--pids-limit",
            "32",
            "--memory",
            "128m",
            "--entrypoint",
            "python",
            identity,
        ]
    else:
        if not re.fullmatch(r"[0-9a-f]{12,64}", args.container):
            raise VerificationError("invalid_container_id")
        target = {"container_id": args.container}
        command = ["docker", "exec", args.container, "python"]
    package = json.loads(capture([*command, "-c", CHILD_SCRIPT]))
    actual, root = package["files"], package["package_root"]
    if not isinstance(actual, dict) or not actual or not isinstance(root, str) or not Path(root).is_absolute():
        raise VerificationError("invalid_package_manifest")
    for path, digest in actual.items():
        if (
            not path.endswith(".py")
            or PurePosixPath(path).is_absolute()
            or ".." in PurePosixPath(path).parts
            or not isinstance(digest, str)
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
        ):
            raise VerificationError("invalid_package_manifest")
    mismatches = compare(expected, actual)
    return {
        **target,
        "expected_revision": revision,
        "package_root": root,
        "expected_manifest_sha256": manifest_hash(expected),
        "expected_file_count": len(expected),
        "installed_manifest_sha256": manifest_hash(actual),
        "installed_file_count": len(actual),
        "mismatches": mismatches,
        "status": "mismatch" if any(mismatches.values()) else "matched",
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    target = parser.add_mutually_exclusive_group(required=True)
    target.add_argument("--image", help="Locally available image tag or sha256 ID")
    target.add_argument("--container", help="Existing container ID (12-64 hexadecimal characters)")
    parser.add_argument("--expected-ref", required=True, help="Full Git commit SHA")
    args = parser.parse_args()
    try:
        if not re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", args.expected_ref):
            raise VerificationError("expected_ref_must_be_full_commit_sha")
        result = verify(args)
    except VerificationError as error:
        result = {"expected_revision": args.expected_ref, "status": "error:" + str(error)}
    except (ValueError, KeyError, TypeError, tarfile.TarError):
        result = {"expected_revision": args.expected_ref, "status": "error:invalid_output"}
    print(json.dumps(result, sort_keys=True, ensure_ascii=False))
    return 0 if result["status"] == "matched" else 1


if __name__ == "__main__":
    raise SystemExit(main())
