#!/usr/bin/env python3
"""Guarded control-plane-only image rollout. Default dry-run; never provisions credentials.

Preserves the current compose file/profile/env/project chain verbatim. Other services
are never passed to `up`; existing ExecStop is untouched. Use only after preparing
an exact-SHA image and obtaining deployment approval. No database/secret reads.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import shlex
import stat
import subprocess
import tempfile
import time
from pathlib import Path

SERVICE = "my-data-hub-control-plane.service"
IMAGE = re.compile(r"my-data-hub-control-plane:[0-9a-f]{40}")


class RolloutError(RuntimeError):
    pass


def systemd_env():
    # Same current-user bus selection as Projects Hub's deployment installer.
    # Direct Ops runners need not inherit an interactive login's bus variables.
    runtime = Path(f"/run/user/{os.getuid()}")
    bus = runtime / "bus"
    if not bus.exists():
        raise RolloutError("user-systemd bus is unavailable for current UID")
    env = os.environ.copy()
    env["HOME"] = str(Path.home())
    env["XDG_RUNTIME_DIR"] = str(runtime)
    env["DBUS_SESSION_BUS_ADDRESS"] = f"unix:path={bus}"
    return env


def command(argv):
    is_systemd = Path(argv[0]).name == "systemctl" and "--user" in argv
    env = systemd_env() if is_systemd else None
    result = subprocess.run(argv, text=True, capture_output=True, timeout=180, env=env)
    if result.returncode:
        # Never emit raw stderr: it may contain configuration or unexpected data.
        category = "command_error"
        if is_systemd:
            error = result.stderr.lower()
            if "failed to connect to bus" in error:
                category = "user_bus_connection_failed"
            elif "access denied" in error or "permission denied" in error:
                category = "permission_denied"
            elif "not found" in error or "could not be found" in error:
                category = "unit_or_executable_not_found"
        raise RolloutError(f"command failed: {Path(argv[0]).name}; exit={result.returncode}; category={category}")
    return result.stdout.strip()


def systemd_argv(value):
    match = re.fullmatch(r"\{ path=[^;]+ ; argv\[\]=(.*?) ; ignore_errors=.* \}", value.strip())
    if not match:
        raise RolloutError("single systemd command required")
    raw = match.group(1)
    if any(x in raw for x in ("\\", "\n", "\r", "$", "%", ";")):
        raise RolloutError("unsupported systemd escaping or expansion")
    return shlex.split(raw)


def compose_parts(argv):
    if len(argv) < 5 or argv[:2] != ["/usr/bin/docker", "compose"]:
        raise RolloutError("expected /usr/bin/docker compose")
    index = 2
    options = {"--env-file", "--profile", "--project-directory", "--project-name", "-p", "-f"}
    files = 0
    while index < len(argv) and argv[index] != "up":
        if argv[index] not in options or index + 1 >= len(argv):
            raise RolloutError("unsupported compose option")
        if argv[index] in ("--env-file", "--project-directory", "-f") and not argv[index + 1].startswith("/"):
            raise RolloutError("compose paths must be absolute")
        if argv[index] == "-f":
            files += 1
        index += 2
    if not files or index >= len(argv) or "control-plane" not in argv[index + 1 :]:
        raise RolloutError("explicit existing control-plane service required")
    if any(not re.fullmatch(r"[A-Za-z0-9_./:@=-]+", x) for x in argv):
        raise RolloutError("unsupported unit argument")
    return argv[:index], argv[index:]


def unit_line(argv):
    # compose_parts has excluded systemd expansion and whitespace syntax.
    return " ".join(argv)


def inspect_container(run, container):
    template = (
        '{"Id":{{json .Id}},"Image":{{json .Image}},'
        '"Service":{{json (index .Config.Labels "com.docker.compose.service")}},'
        '"Revision":{{json (index .Config.Labels "org.opencontainers.image.revision")}},'
        '"State":{"Running":{{json .State.Running}},"StartedAt":{{json .State.StartedAt}},'
        '"Health":{"Status":{{if .State.Health}}{{json .State.Health.Status}}{{else}}"missing"{{end}}}}}'
    )
    return json.loads(run(["/usr/bin/docker", "inspect", "--format", template, container]))


def snapshot(run, prefix):
    ids = run([*prefix, "ps", "--all", "-q"]).splitlines()
    result = {}
    for cid in ids:
        if not cid:
            continue
        value = inspect_container(run, cid)
        name = value.get("Service")
        if not name or name in result:
            raise RolloutError("ambiguous compose container inventory")
        result[name] = {
            "id": value["Id"],
            "image": value["Image"],
            "started": value["State"].get("StartedAt"),
            "running": value["State"].get("Running", False),
        }
    if not result.get("control-plane", {}).get("running"):
        raise RolloutError("existing control-plane must be running")
    return result


def atomic(path, text):
    if path.is_symlink() or path.parent.is_symlink():
        raise RolloutError("symlink output denied")
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix="." + path.name)
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def unit_paths(run, property_name):
    raw = run(["systemctl", "--user", "show", SERVICE, "--property=" + property_name, "--value"])
    if len(raw) > 16384:
        raise RolloutError("unit path metadata exceeds bound")
    paths = shlex.split(raw)
    if len(paths) > 64 or any(not re.fullmatch(r"/[A-Za-z0-9_./:@=-]+", path) for path in paths):
        raise RolloutError("unsupported unit path metadata")
    if property_name == "FragmentPath" and len(paths) != 1:
        raise RolloutError("one unit fragment path required")
    return paths


def inspect_current(run=command):
    """Only allowlisted unit argv and nonsecret Docker identity/status fields."""
    start = systemd_argv(run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"]))
    prefix, _ = compose_parts(start)
    reload_raw = run(["systemctl", "--user", "show", SERVICE, "--property=ExecReload", "--value"])
    reload_argv = systemd_argv(reload_raw) if reload_raw else None
    if reload_argv and compose_parts(reload_argv)[0] != prefix:
        raise RolloutError("ExecReload compose chain differs")
    return {
        "state": "inspection",
        "fragment_path": unit_paths(run, "FragmentPath")[0],
        "dropin_paths": unit_paths(run, "DropInPaths"),
        "exec_start": unit_line(start),
        "exec_reload": unit_line(reload_argv) if reload_argv else None,
        "service_state": run(["systemctl", "--user", "is-active", SERVICE]),
        "containers": snapshot(run, prefix),
    }


def rollout(
    *,
    expected_start=None,
    expected_start_sha256=None,
    expected_image,
    target_image,
    dropin,
    override,
    apply=False,
    run=command,
    timeout=120,
    voice_config_file=None,
):
    if not IMAGE.fullmatch(target_image) or not re.fullmatch(r"sha256:[a-f0-9]{64}", expected_image):
        raise RolloutError("exact old image ID and prepared SHA image tag required")
    start = systemd_argv(run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"]))
    if (expected_start is None) == (expected_start_sha256 is None):
        raise RolloutError("exactly one ExecStart guard required")
    if expected_start_sha256 is not None:
        if not re.fullmatch(r"[a-f0-9]{64}", expected_start_sha256):
            raise RolloutError("ExecStart SHA256 must be 64 lowercase hexadecimal characters")
        if hashlib.sha256(unit_line(start).encode("utf-8")).hexdigest() != expected_start_sha256:
            raise RolloutError("ExecStart changed")
    elif start != shlex.split(expected_start):
        raise RolloutError("ExecStart changed")
    prefix, suffix = compose_parts(start)
    reload_raw = run(["systemctl", "--user", "show", SERVICE, "--property=ExecReload", "--value"])
    reload_argv = systemd_argv(reload_raw) if reload_raw else None
    if reload_argv and compose_parts(reload_argv)[0] != prefix:
        raise RolloutError("ExecReload compose chain differs")
    if run(["systemctl", "--user", "is-active", SERVICE]) != "active":
        raise RolloutError("service not active")
    before = snapshot(run, prefix)
    if before["control-plane"]["image"] != expected_image:
        raise RolloutError("current image changed")
    target = json.loads(run(["/usr/bin/docker", "image", "inspect", "--format", "{{json .Id}}", target_image]))
    if not re.fullmatch(r"sha256:[a-f0-9]{64}", target):
        raise RolloutError("prepared image unavailable")
    if target == expected_image:
        raise RolloutError("target image already active")
    if not override.is_absolute() or not dropin.is_absolute():
        raise RolloutError("absolute output paths required")
    newprefix = [*prefix, "-f", str(override)]
    compose_parts(newprefix + suffix)
    service_config = {"image": target, "labels": {"org.opencontainers.image.revision": target_image.split(":")[1]}}
    if voice_config_file is not None:
        if not voice_config_file.is_absolute() or voice_config_file.is_symlink():
            raise RolloutError("approved config must be an absolute regular private file")
        metadata = voice_config_file.stat()
        if (
            not stat.S_ISREG(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o600
            or metadata.st_uid != os.getuid()
            or not 0 < metadata.st_size <= 32768
        ):
            raise RolloutError("approved config permissions/owner/size invalid")
        service_config["volumes"] = [str(voice_config_file) + ":/run/secrets/voice-analysis.json:ro"]
        service_config["environment"] = {"MY_DATA_HUB_VOICE_ANALYSIS_CONFIG_FILE": "/run/secrets/voice-analysis.json"}
    overlay = json.dumps({"services": {"control-plane": service_config}}, indent=2) + "\n"
    unit = "[Service]\nExecStart=\nExecStart=" + unit_line(newprefix + suffix) + "\n"
    if reload_argv:
        unit += "ExecReload=\nExecReload=" + unit_line(newprefix + compose_parts(reload_argv)[1]) + "\n"
    plan = {
        "state": "dry_run",
        "target_image": target_image,
        "expected_old_image": expected_image,
        "services_changed": ["control-plane"],
        "other_services": sorted(set(before) - {"control-plane"}),
        "override": str(override),
        "dropin": str(dropin),
        "command": [*newprefix, "up", "-d", "--no-deps", "control-plane"],
    }
    if not apply:
        return plan
    rollback_override = override.with_name(override.name + ".rollback.json")
    if override.exists() or rollback_override.exists():
        raise RolloutError("new immutable override path already exists")
    if dropin.is_symlink() or override.is_symlink():
        raise RolloutError("symlink output denied")
    old = dropin.read_text() if dropin.exists() else None
    if dropin.is_symlink() or override.is_symlink():
        raise RolloutError("symlink output denied")
    rollback_prefix = [*prefix, "-f", str(rollback_override)]
    compose_parts(rollback_prefix + suffix)
    rollback_unit = (old or "") + "\n[Service]\nExecStart=\nExecStart=" + unit_line(rollback_prefix + suffix) + "\n"
    if reload_argv:
        rollback_unit += "ExecReload=\nExecReload=" + unit_line(rollback_prefix + compose_parts(reload_argv)[1]) + "\n"
    # Pin both directions to captured immutable image IDs, never mutable tags.
    atomic(rollback_override, json.dumps({"services": {"control-plane": {"image": expected_image}}}, indent=2) + "\n")
    atomic(override, overlay)
    activated = False
    switched = False
    try:
        # Recheck source immediately before switching; no broad installer or restart.
        if systemd_argv(run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"])) != start:
            raise RolloutError("ExecStart changed during preparation")
        if snapshot(run, prefix) != before:
            raise RolloutError("container inventory changed during preparation")
        atomic(dropin, unit)
        activated = True
        run(["systemctl", "--user", "daemon-reload"])
        if (
            systemd_argv(run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"]))
            != newprefix + suffix
        ):
            raise RolloutError("new drop-in is ineffective: ExecStart differs before container switch")
        effective_reload = run(["systemctl", "--user", "show", SERVICE, "--property=ExecReload", "--value"])
        expected_reload = newprefix + compose_parts(reload_argv)[1] if reload_argv else None
        if (systemd_argv(effective_reload) if effective_reload else None) != expected_reload:
            raise RolloutError("new drop-in is ineffective: ExecReload differs before container switch")
        switched = True
        run([*newprefix, "up", "-d", "--no-deps", "control-plane"])
        deadline = time.monotonic() + timeout
        while True:
            cid = run([*newprefix, "ps", "-q", "control-plane"])
            current = inspect_container(run, cid)
            if (
                current["Image"] == target
                and current.get("Revision") == target_image.split(":")[1]
                and current["State"].get("Running")
                and current["State"].get("Health", {}).get("Status") == "healthy"
            ):
                break
            if time.monotonic() >= deadline:
                raise RolloutError("new control-plane health not confirmed")
            time.sleep(1)
        after = snapshot(run, newprefix)
        if {k: v for k, v in after.items() if k != "control-plane"} != {
            k: v for k, v in before.items() if k != "control-plane"
        }:
            raise RolloutError("other service changed; external concurrency or unexpected compose effect")
        if (
            systemd_argv(run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"]))
            != newprefix + suffix
        ):
            raise RolloutError("persistent startup does not match target chain")
        return {
            **plan,
            "state": "healthy",
            "image_id": target,
            "rollback_old_chain": [*rollback_prefix, "up", "-d", "--no-deps", "control-plane"],
        }
    except Exception as exc:
        if activated and not switched:
            # Only undo our own metadata change; no container recreation before switch.
            if old is None:
                dropin.unlink(missing_ok=True)
            else:
                atomic(dropin, old)
            run(["systemctl", "--user", "daemon-reload"])
            restored_start = systemd_argv(
                run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"])
            )
            restored_reload_raw = run(["systemctl", "--user", "show", SERVICE, "--property=ExecReload", "--value"])
            restored_reload = systemd_argv(restored_reload_raw) if restored_reload_raw else None
            if restored_start != start or restored_reload != reload_argv or snapshot(run, prefix) != before:
                raise RolloutError("pre-switch metadata restoration not confirmed; operator required") from None
            raise RolloutError(
                "ineffective drop-in rejected before container switch; prior metadata and containers unchanged"
            ) from exc
        if activated:
            # Retain all prior drop-in settings and original compose overlays, adding
            # only the captured old image pin for this and future starts.
            atomic(dropin, rollback_unit)
            run(["systemctl", "--user", "daemon-reload"])
            run([*rollback_prefix, "up", "-d", "--no-deps", "control-plane"])
            deadline = time.monotonic() + timeout
            while True:
                restored = snapshot(run, rollback_prefix)
                old_container = inspect_container(run, restored["control-plane"]["id"])
                if (
                    restored["control-plane"]["image"] == expected_image
                    and old_container["State"].get("Health", {}).get("Status") == "healthy"
                ):
                    break
                if time.monotonic() >= deadline:
                    raise RolloutError("rollback image/health not confirmed; operator required") from None
                time.sleep(1)
            if (
                systemd_argv(run(["systemctl", "--user", "show", SERVICE, "--property=ExecStart", "--value"]))
                != rollback_prefix + suffix
            ):
                raise RolloutError("rollback persistent startup not confirmed; operator required") from None
            if {k: v for k, v in restored.items() if k != "control-plane"} != {
                k: v for k, v in before.items() if k != "control-plane"
            }:
                raise RolloutError("rollback left another service changed; operator required") from None
        raise RolloutError(
            "rollout failed; prior startup and image restored" if activated else "preparation failed; service unchanged"
        ) from exc


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Read only current allowlisted unit argv and Docker identities; no configuration contents",
    )
    guard = parser.add_mutually_exclusive_group()
    guard.add_argument("--expected-exec-start")
    guard.add_argument(
        "--expected-exec-start-sha256",
        help="SHA256 of exact UTF-8 exec_start string returned by --inspect, without newline",
    )
    parser.add_argument("--expected-image-id")
    parser.add_argument("--target-image")
    parser.add_argument("--override", type=Path)
    parser.add_argument(
        "--dropin", type=Path, default=Path.home() / ".config/systemd/user" / f"{SERVICE}.d/z-voice-analysis-image.conf"
    )
    parser.add_argument(
        "--voice-config-file",
        type=Path,
        help="Existing separately approved 0600 file; only metadata inspected, mounted read-only",
    )
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    try:
        if args.inspect:
            if (
                args.apply
                or args.voice_config_file
                or any(
                    (
                        args.expected_exec_start,
                        args.expected_exec_start_sha256,
                        args.expected_image_id,
                        args.target_image,
                        args.override,
                    )
                )
            ):
                raise RolloutError("--inspect cannot be combined with deployment arguments")
            print(json.dumps(inspect_current(), sort_keys=True))
            return 0
        if not all(
            (
                args.expected_exec_start or args.expected_exec_start_sha256,
                args.expected_image_id,
                args.target_image,
                args.override,
            )
        ):
            raise RolloutError("deployment requires expected ExecStart, image ID, target image and override")
        if args.apply:
            lock = args.dropin.parent / "voice-image-rollout.lock"
            lock.parent.mkdir(parents=True, exist_ok=True)
            with lock.open("a") as f:
                fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
                value = rollout(
                    expected_start=args.expected_exec_start,
                    expected_start_sha256=args.expected_exec_start_sha256,
                    expected_image=args.expected_image_id,
                    target_image=args.target_image,
                    dropin=args.dropin,
                    override=args.override,
                    apply=True,
                    voice_config_file=args.voice_config_file,
                )
        else:
            value = rollout(
                expected_start=args.expected_exec_start,
                expected_start_sha256=args.expected_exec_start_sha256,
                expected_image=args.expected_image_id,
                target_image=args.target_image,
                dropin=args.dropin,
                override=args.override,
                voice_config_file=args.voice_config_file,
            )
        print(json.dumps(value, sort_keys=True))
        return 0
    except (RolloutError, OSError, ValueError, subprocess.TimeoutExpired) as exc:
        detail = str(exc) if isinstance(exc, RolloutError) else type(exc).__name__
        print(json.dumps({"state": "failed", "error": "VOICE_IMAGE_ROLLOUT_FAILED", "details": detail}))
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
