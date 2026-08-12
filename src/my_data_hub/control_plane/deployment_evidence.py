"""Host-derived, sanitized deployment evidence for the DB-free devstand.

This module deliberately separates observation from signing.  No caller can pass
claimed service, database, listener, process-recovery, or reboot results into a
receipt: those values come from the local checkout, immutable release, procfs,
systemd and Docker inspection performed here.
"""

from __future__ import annotations

import base64
import contextlib
import hashlib
import json
import os
import re
import signal
import stat
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path, PurePosixPath
from typing import Any
from urllib.parse import urlsplit

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from cryptography.hazmat.primitives.serialization import load_pem_private_key

SERVICES = ("control-plane", "oauth-server", "remote-mcp")
SERVICE_PORTS = {"control-plane": 8080, "oauth-server": 8780, "remote-mcp": 8765}
LOOPBACK_PORTS = [8080, 8765, 8780]
UNIT = "my-data-hub-control-plane.service"
PROJECT = "my-data-hub-control-plane"
STATE_SCHEMA_VERSION = "my-data-hub-deployment-evidence-state.v1"
RECEIPT_SCHEMA_VERSION = "my-data-hub-deployment-evidence.v2"
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_IMAGE_ID = re.compile(r"^sha256:[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"^[0-9a-f]{40}$")
_SOURCE_IDENTITY = re.compile(r"^[A-Za-z0-9_.-]{1,100}/[A-Za-z0-9_.-]{1,100}$")
_KEY_ID = re.compile(r"^[A-Za-z0-9._:-]{1,128}$")
_FORBIDDEN_ENV = re.compile(
    r"^(?:PG(?:HOST|PORT|DATABASE|USER|PASSWORD|PASSFILE|SERVICE|SERVICEFILE|DATA)|"
    r"DATABASE_URL|[A-Z0-9_]+_DATABASE_URL)$"
)
_POSTGRES_TOKEN = re.compile(
    r"(?:^|[:/_.-])(?:postgres(?:ql)?|postmaster|pg_ctl)(?:$|[:/_.-])",
    re.I,
)
_PROJECT_VOLUME = re.compile(
    r"(?=.*(?:my[-_]?data[-_]?hub|content[-_]?platform))(?=.*(?:postgres|pgdata))",
    re.I,
)


class DeploymentEvidenceError(RuntimeError):
    """A deliberately non-secret diagnostic for a failed evidence gate."""


class CommandRunner:
    """Bounded subprocess adapter that never includes captured output in errors."""

    def run(self, arguments: Sequence[str], *, timeout_seconds: float = 15) -> str:
        try:
            completed = subprocess.run(
                list(arguments),
                check=True,
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env={**os.environ, "LC_ALL": "C"},
            )
        except Exception as exc:
            raise DeploymentEvidenceError("a required bounded host command failed") from exc
        if len(completed.stdout.encode("utf-8")) > 2_097_152:
            raise DeploymentEvidenceError("a required host command returned oversized output")
        return completed.stdout.strip()


@dataclass(frozen=True, slots=True)
class CollectorPaths:
    source_root: Path
    runtime_root: Path
    release_root: Path
    state_file: Path


@dataclass(frozen=True, slots=True)
class DeploymentIdentity:
    source_identity: str
    deployed_commit: str
    source_tree_sha256: str
    installed_release_tree_sha256: str
    release: Path
    compose_environment: Path


@dataclass(frozen=True, slots=True)
class ContainerObservation:
    service: str
    container_id: str
    image_id: str
    pid: int
    started_at: str
    process_sha256: str


@dataclass(frozen=True, slots=True)
class HostObservation:
    identity: DeploymentIdentity
    host_id_sha256: str
    boot_id_sha256: str
    booted_at: datetime
    services: dict[str, str]
    service_image_ids: dict[str, str]
    containers: dict[str, ContainerObservation]
    public_listener_ports: list[int]
    loopback_listener_ports: list[int]
    unit_enabled: bool
    unit_active: bool
    linger_enabled: bool
    database_process_present: bool
    pgdata_present: bool
    database_environment_present: bool


def _utc_text(value: datetime) -> str:
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse_utc(value: object, name: str) -> datetime:
    if not isinstance(value, str) or len(value) > 64:
        raise DeploymentEvidenceError(f"stored {name} timestamp is invalid")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise DeploymentEvidenceError(f"stored {name} timestamp is invalid") from exc
    if parsed.tzinfo is None:
        raise DeploymentEvidenceError(f"stored {name} timestamp is invalid")
    return parsed.astimezone(UTC)


def _hash_reference(namespace: str, *values: object) -> str:
    encoded = json.dumps(
        [namespace, *values], ensure_ascii=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _regular_file(path: Path, *, private: bool = False, maximum_bytes: int = 2_097_152) -> None:
    try:
        info = path.lstat()
    except OSError as exc:
        raise DeploymentEvidenceError("a required evidence input file is absent") from exc
    if not stat.S_ISREG(info.st_mode) or path.is_symlink() or info.st_size > maximum_bytes:
        raise DeploymentEvidenceError("an evidence input is not a bounded regular file")
    if private and (info.st_mode & 0o077 or info.st_uid != os.geteuid()):
        raise DeploymentEvidenceError("a private evidence file has unsafe ownership or permissions")


def _source_identity(remote: str) -> str:
    value = remote.strip()
    if value.startswith("git@github.com:"):
        path = value.removeprefix("git@github.com:")
    else:
        parsed = urlsplit(value)
        if parsed.scheme != "https" or parsed.hostname != "github.com" or parsed.username:
            raise DeploymentEvidenceError("the deployment source remote is not an exact GitHub repository")
        path = parsed.path.lstrip("/")
    identity = path.removesuffix(".git").rstrip("/")
    if not _SOURCE_IDENTITY.fullmatch(identity):
        raise DeploymentEvidenceError("the deployment source identity is invalid")
    return identity


def _tracked_entries(raw: str) -> list[tuple[str, str]]:
    entries: list[tuple[str, str]] = []
    for record in raw.split("\0"):
        if not record:
            continue
        metadata, separator, path = record.partition("\t")
        fields = metadata.split()
        if not separator or len(fields) != 3 or fields[2] != "0":
            raise DeploymentEvidenceError("the source Git index inventory is invalid")
        mode = fields[0]
        relative = PurePosixPath(path)
        if mode not in {"100644", "100755", "120000"} or relative.is_absolute() or ".." in relative.parts:
            raise DeploymentEvidenceError("the source Git index contains an unsupported entry")
        entries.append((mode, path))
    if not entries or entries != sorted(entries, key=lambda item: item[1].encode("utf-8")):
        raise DeploymentEvidenceError("the source Git index inventory is empty or unordered")
    return entries


def _entry_bytes(root: Path, relative: str, mode: str) -> bytes:
    path = root.joinpath(*PurePosixPath(relative).parts)
    try:
        info = path.lstat()
        if mode == "120000":
            if not stat.S_ISLNK(info.st_mode):
                raise DeploymentEvidenceError("a tracked symbolic link changed type")
            return os.readlink(path).encode("utf-8")
        if not stat.S_ISREG(info.st_mode) or path.is_symlink():
            raise DeploymentEvidenceError("a tracked release entry changed type")
        executable = bool(info.st_mode & 0o111)
        if executable != (mode == "100755"):
            raise DeploymentEvidenceError("a tracked release entry changed executable mode")
        return path.read_bytes()
    except DeploymentEvidenceError:
        raise
    except OSError as exc:
        raise DeploymentEvidenceError("a tracked release entry cannot be read") from exc


def _tree_manifest_sha256(root: Path, entries: Sequence[tuple[str, str]]) -> str:
    digest = hashlib.sha256()
    for mode, relative in entries:
        content = _entry_bytes(root, relative, mode)
        digest.update(mode.encode("ascii") + b"\0")
        digest.update(relative.encode("utf-8") + b"\0")
        digest.update(hashlib.sha256(content).digest())
    return digest.hexdigest()


def _release_entries(root: Path) -> set[str]:
    result: set[str] = set()
    try:
        for directory, directories, files in os.walk(root, followlinks=False):
            base = Path(directory)
            for name in [*directories, *files]:
                path = base / name
                if path.is_symlink() or path.is_file():
                    result.add(path.relative_to(root).as_posix())
    except OSError as exc:
        raise DeploymentEvidenceError("the installed release tree cannot be inventoried") from exc
    return result


def _assert_immutable_release(root: Path) -> None:
    try:
        paths = [
            root,
            *(
                Path(directory) / name
                for directory, dirs, files in os.walk(root)
                for name in [*dirs, *files]
            ),
        ]
        if any(path.lstat().st_mode & 0o222 for path in paths if not path.is_symlink()):
            raise DeploymentEvidenceError("the installed release is writable")
    except DeploymentEvidenceError:
        raise
    except OSError as exc:
        raise DeploymentEvidenceError("the installed release permissions cannot be verified") from exc


def derive_deployment_identity(paths: CollectorPaths, runner: CommandRunner) -> DeploymentIdentity:
    source = paths.source_root.resolve(strict=True)
    commit = runner.run(["git", "-C", str(source), "rev-parse", "HEAD"])
    if not _GIT_SHA.fullmatch(commit):
        raise DeploymentEvidenceError("the source checkout commit is invalid")
    if runner.run(["git", "-C", str(source), "status", "--porcelain", "--untracked-files=no"]):
        raise DeploymentEvidenceError("the source checkout has tracked changes")
    identity = _source_identity(runner.run(["git", "-C", str(source), "remote", "get-url", "origin"]))

    current = paths.release_root / "current"
    if not current.is_symlink():
        raise DeploymentEvidenceError("the installed current release pointer is absent")
    release = current.resolve(strict=True)
    expected_release = (paths.release_root / "releases" / commit).resolve(strict=True)
    if release != expected_release or release.name != commit:
        raise DeploymentEvidenceError("the installed release does not match the source commit")
    _assert_immutable_release(release)

    entries = _tracked_entries(
        runner.run(["git", "-C", str(source), "ls-files", "--stage", "-z"])
    )
    tracked_paths = {path for _, path in entries}
    if _release_entries(release) != tracked_paths:
        raise DeploymentEvidenceError("the installed release file inventory differs from the source tree")
    source_hash = _tree_manifest_sha256(source, entries)
    release_hash = _tree_manifest_sha256(release, entries)
    if source_hash != release_hash:
        raise DeploymentEvidenceError("the installed release content differs from the source tree")

    compose_environment = paths.runtime_root / f"compose.{commit}.env"
    _regular_file(compose_environment, private=True, maximum_bytes=65_536)
    return DeploymentIdentity(
        source_identity=identity,
        deployed_commit=commit,
        source_tree_sha256=source_hash,
        installed_release_tree_sha256=release_hash,
        release=release,
        compose_environment=compose_environment,
    )


def _json_array(raw: str, name: str) -> list[dict[str, Any]]:
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DeploymentEvidenceError(f"Docker {name} inventory is invalid") from exc
    if not isinstance(parsed, list) or any(not isinstance(item, dict) for item in parsed):
        raise DeploymentEvidenceError(f"Docker {name} inventory is invalid")
    return parsed


def _container_ids(runner: CommandRunner, *, all_containers: bool = False) -> list[str]:
    arguments = ["docker", "ps"]
    if all_containers:
        arguments.append("--all")
    arguments.extend(["--quiet"])
    if not all_containers:
        arguments.extend(["--filter", f"label=com.docker.compose.project={PROJECT}"])
    ids = [line for line in runner.run(arguments).splitlines() if line]
    if any(not re.fullmatch(r"[0-9a-f]{12,64}", value) for value in ids):
        raise DeploymentEvidenceError("Docker returned an invalid container identity")
    return ids


def _inspect(runner: CommandRunner, ids: Sequence[str]) -> list[dict[str, Any]]:
    if not ids:
        return []
    return _json_array(runner.run(["docker", "inspect", *ids]), "container")


def _mapping(value: object, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise DeploymentEvidenceError(f"Docker {name} metadata is absent")
    return value


def _container_observations(
    identity: DeploymentIdentity,
    runner: CommandRunner,
    *,
    boot_id_sha256: str,
) -> tuple[dict[str, ContainerObservation], list[int], list[int]]:
    records = _inspect(runner, _container_ids(runner))
    observed: dict[str, ContainerObservation] = {}
    public_ports: list[int] = []
    loopback_ports: list[int] = []
    expected_tag = f"my-data-hub-control-plane:{identity.deployed_commit}"
    local_image = _json_array(runner.run(["docker", "image", "inspect", expected_tag]), "image")
    if len(local_image) != 1 or not _IMAGE_ID.fullmatch(str(local_image[0].get("Id", ""))):
        raise DeploymentEvidenceError("the exact deployment image tag is unavailable")
    expected_image_id = str(local_image[0]["Id"])

    for record in records:
        labels = _mapping(_mapping(record.get("Config"), "config").get("Labels"), "labels")
        service = labels.get("com.docker.compose.service")
        if service not in SERVICES or labels.get("com.docker.compose.project") != PROJECT or service in observed:
            raise DeploymentEvidenceError("the running control service inventory differs from policy")
        state = _mapping(record.get("State"), "state")
        health = _mapping(state.get("Health"), "health")
        image_id = str(record.get("Image", ""))
        container_id = str(record.get("Id", ""))
        pid = state.get("Pid")
        started_at = state.get("StartedAt")
        restart = _mapping(_mapping(record.get("HostConfig"), "host config").get("RestartPolicy"), "restart policy")
        if (
            state.get("Running") is not True
            or state.get("Status") != "running"
            or health.get("Status") != "healthy"
            or restart.get("Name") != "unless-stopped"
            or image_id != expected_image_id
            or not re.fullmatch(r"[0-9a-f]{64}", container_id)
            or not isinstance(pid, int)
            or pid < 2
            or not isinstance(started_at, str)
            or not started_at
            or _mapping(record.get("Config"), "config").get("Image") != expected_tag
        ):
            raise DeploymentEvidenceError("a control service is not healthy on the immutable image")
        ports = _mapping(_mapping(record.get("NetworkSettings"), "network settings").get("Ports"), "ports")
        expected_port = SERVICE_PORTS[str(service)]
        if set(ports) != {f"{expected_port}/tcp"}:
            raise DeploymentEvidenceError("a control service container port inventory differs from policy")
        bindings = ports[f"{expected_port}/tcp"]
        if not isinstance(bindings, list) or len(bindings) != 1 or not isinstance(bindings[0], Mapping):
            raise DeploymentEvidenceError("a control service host binding differs from policy")
        host_ip = bindings[0].get("HostIp")
        host_port = bindings[0].get("HostPort")
        if host_port != str(expected_port):
            raise DeploymentEvidenceError("a control service host port differs from policy")
        if host_ip in {"127.0.0.1", "::1"}:
            loopback_ports.append(expected_port)
        else:
            public_ports.append(expected_port)
        process_ref = _hash_reference(
            "my-data-hub-container-process.v1",
            boot_id_sha256,
            container_id,
            pid,
            started_at,
        )
        observed[str(service)] = ContainerObservation(
            service=str(service),
            container_id=container_id,
            image_id=image_id,
            pid=pid,
            started_at=started_at,
            process_sha256=process_ref,
        )
    if set(observed) != set(SERVICES):
        raise DeploymentEvidenceError("the exact three control services are not running")
    if public_ports or sorted(loopback_ports) != LOOPBACK_PORTS:
        raise DeploymentEvidenceError("the my-data-hub host listener inventory differs from policy")
    return observed, sorted(public_ports), sorted(loopback_ports)


def _environment_keys(path: Path) -> set[str]:
    _regular_file(path, private=True, maximum_bytes=262_144)
    keys: set[str] = set()
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#") or "=" not in stripped:
                continue
            key = stripped.partition("=")[0].removeprefix("export ").strip().upper()
            if key:
                keys.add(key)
    except (OSError, UnicodeError) as exc:
        raise DeploymentEvidenceError("a deployment environment inventory cannot be read") from exc
    return keys


def _database_environment_present(
    identity: DeploymentIdentity,
    paths: CollectorPaths,
    all_containers: Sequence[Mapping[str, Any]],
) -> bool:
    keys = {str(key).upper() for key in os.environ}
    for record in all_containers:
        config = _mapping(record.get("Config"), "config")
        environment = config.get("Env", [])
        if not isinstance(environment, list):
            return True
        for item in environment:
            if isinstance(item, str):
                keys.add(item.partition("=")[0].upper())
            else:
                return True
    keys.update(_environment_keys(identity.compose_environment))
    try:
        for path in paths.runtime_root.rglob("*.env"):
            if path.is_file():
                keys.update(_environment_keys(path))
    except OSError:
        return True
    return any(_FORBIDDEN_ENV.fullmatch(key) for key in keys)


def _database_process_present(proc_root: Path = Path("/proc")) -> bool:
    try:
        entries = [path for path in proc_root.iterdir() if path.name.isdigit()]
    except OSError:
        return True
    for entry in entries:
        try:
            comm = (entry / "comm").read_text(encoding="utf-8").strip()
            cmdline = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode("utf-8", "replace")
        except OSError:
            continue
        executable = cmdline.split(maxsplit=1)[0] if cmdline else comm
        if _POSTGRES_TOKEN.search(comm) or _POSTGRES_TOKEN.search(executable):
            return True
    return False


def _pgdata_present(paths: CollectorPaths, runner: CommandRunner) -> bool:
    for root in (paths.runtime_root, paths.release_root):
        try:
            if any(path.is_file() for path in root.rglob("PG_VERSION")):
                return True
        except OSError:
            return True
    volume_names = [line for line in runner.run(["docker", "volume", "ls", "--quiet"]).splitlines() if line]
    return any(_PROJECT_VOLUME.search(name) for name in volume_names)


def _docker_database_container_present(records: Sequence[Mapping[str, Any]]) -> bool:
    for record in records:
        config = _mapping(record.get("Config"), "config")
        tokens = [config.get("Image", ""), *(config.get("Entrypoint") or []), *(config.get("Cmd") or [])]
        if any(isinstance(token, str) and _POSTGRES_TOKEN.search(token) for token in tokens):
            return True
        mounts = record.get("Mounts", [])
        if not isinstance(mounts, list):
            return True
        for mount in mounts:
            if isinstance(mount, Mapping) and _POSTGRES_TOKEN.search(str(mount.get("Destination", ""))):
                return True
    return False


def _boot_facts(proc_root: Path = Path("/proc")) -> tuple[str, datetime]:
    try:
        boot_id = (proc_root / "sys/kernel/random/boot_id").read_text(encoding="ascii").strip()
        stat_text = (proc_root / "stat").read_text(encoding="ascii")
        boot_seconds = next(
            int(line.partition(" ")[2]) for line in stat_text.splitlines() if line.startswith("btime ")
        )
    except Exception as exc:
        raise DeploymentEvidenceError("kernel boot identity cannot be derived") from exc
    if not re.fullmatch(r"[0-9a-f-]{36}", boot_id) or boot_seconds < 1:
        raise DeploymentEvidenceError("kernel boot identity cannot be derived")
    return _hash_reference("my-data-hub-boot.v1", boot_id), datetime.fromtimestamp(boot_seconds, UTC)


def _host_id() -> str:
    path = Path("/etc/machine-id")
    _regular_file(path, maximum_bytes=4096)
    value = path.read_text(encoding="ascii").strip()
    if not re.fullmatch(r"[0-9a-f]{32}", value):
        raise DeploymentEvidenceError("host machine identity cannot be derived")
    return _hash_reference("my-data-hub-host.v1", value)


def observe_host(paths: CollectorPaths, runner: CommandRunner) -> HostObservation:
    identity = derive_deployment_identity(paths, runner)
    boot_id_sha256, booted_at = _boot_facts()
    containers, public_ports, loopback_ports = _container_observations(
        identity, runner, boot_id_sha256=boot_id_sha256
    )
    all_container_records = _inspect(runner, _container_ids(runner, all_containers=True))

    enabled = runner.run(["systemctl", "--user", "is-enabled", UNIT]) == "enabled"
    active = runner.run(["systemctl", "--user", "is-active", UNIT]) == "active"
    linger = runner.run(
        ["loginctl", "show-user", str(os.geteuid()), "--property=Linger", "--value"]
    ) == "yes"
    fragment = Path(
        runner.run(["systemctl", "--user", "show", UNIT, "--property=FragmentPath", "--value"])
    )
    _regular_file(fragment, private=True, maximum_bytes=65_536)
    unit_text = fragment.read_text(encoding="utf-8")
    if (
        str(identity.release) not in unit_text
        or str(identity.compose_environment) not in unit_text
        or not all(service in unit_text for service in SERVICES)
    ):
        raise DeploymentEvidenceError("the enabled unit is not bound to the installed release")

    database_process = _database_process_present() or _docker_database_container_present(all_container_records)
    pgdata = _pgdata_present(paths, runner)
    database_environment = _database_environment_present(identity, paths, all_container_records)
    return HostObservation(
        identity=identity,
        host_id_sha256=_host_id(),
        boot_id_sha256=boot_id_sha256,
        booted_at=booted_at,
        services={service: "running" for service in SERVICES},
        service_image_ids={service: containers[service].image_id for service in SERVICES},
        containers=containers,
        public_listener_ports=public_ports,
        loopback_listener_ports=loopback_ports,
        unit_enabled=enabled,
        unit_active=active,
        linger_enabled=linger,
        database_process_present=database_process,
        pgdata_present=pgdata,
        database_environment_present=database_environment,
    )


def _assert_healthy(observation: HostObservation) -> None:
    if (
        observation.services != {service: "running" for service in SERVICES}
        or observation.public_listener_ports
        or observation.loopback_listener_ports != LOOPBACK_PORTS
        or not observation.unit_enabled
        or not observation.unit_active
        or not observation.linger_enabled
        or observation.database_process_present
        or observation.pgdata_present
        or observation.database_environment_present
        or len(set(observation.service_image_ids.values())) != 1
        or any(not _IMAGE_ID.fullmatch(value) for value in observation.service_image_ids.values())
    ):
        raise DeploymentEvidenceError("the devstand does not satisfy the signed evidence boundary")


def _exact_state(value: object) -> dict[str, Any]:
    expected = {
        "schema_version",
        "source_identity",
        "deployed_commit",
        "source_tree_sha256",
        "installed_release_tree_sha256",
        "host_id_sha256",
        "pre_reboot_boot_id_sha256",
        "process_kill",
        "reboot_prepared_at",
    }
    if not isinstance(value, dict) or set(value) != expected:
        raise DeploymentEvidenceError("the staged deployment evidence fields differ from policy")
    if (
        value["schema_version"] != STATE_SCHEMA_VERSION
        or not _SOURCE_IDENTITY.fullmatch(str(value["source_identity"]))
        or not _GIT_SHA.fullmatch(str(value["deployed_commit"]))
        or any(
            not _SHA256.fullmatch(str(value[name]))
            for name in (
                "source_tree_sha256",
                "installed_release_tree_sha256",
                "host_id_sha256",
                "pre_reboot_boot_id_sha256",
            )
        )
        or value["source_tree_sha256"] != value["installed_release_tree_sha256"]
        or (value["reboot_prepared_at"] is not None
        and not isinstance(value["reboot_prepared_at"], str))
    ):
        raise DeploymentEvidenceError("the staged deployment evidence values differ from policy")
    process = value["process_kill"]
    process_keys = {
        "target_service",
        "killed_at",
        "recovered_at",
        "before_process_sha256",
        "after_process_sha256",
        "recovered",
    }
    if (
        not isinstance(process, dict)
        or set(process) != process_keys
        or process["target_service"] not in SERVICES
        or process["recovered"] is not True
        or not _SHA256.fullmatch(str(process["before_process_sha256"]))
        or not _SHA256.fullmatch(str(process["after_process_sha256"]))
        or process["before_process_sha256"] == process["after_process_sha256"]
    ):
        raise DeploymentEvidenceError("the staged process-kill evidence differs from policy")
    _parse_utc(process["killed_at"], "process killed_at")
    _parse_utc(process["recovered_at"], "process recovered_at")
    if value["reboot_prepared_at"] is not None:
        _parse_utc(value["reboot_prepared_at"], "reboot prepared_at")
    return value


def _read_state(path: Path) -> dict[str, Any]:
    _regular_file(path, private=True, maximum_bytes=32_768)
    try:
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DeploymentEvidenceError("the staged deployment evidence is invalid") from exc
    return _exact_state(parsed)


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    parent = path.parent
    if parent.is_symlink() or not parent.is_dir() or parent.stat().st_mode & 0o077:
        raise DeploymentEvidenceError("the evidence output directory is absent or not private")
    if path.exists() and (path.is_symlink() or not path.is_file()):
        raise DeploymentEvidenceError("the evidence output path is unsafe")
    temporary = parent / f".{path.name}.{os.getpid()}.tmp"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    try:
        descriptor = os.open(temporary, flags, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2, sort_keys=True)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    except Exception as exc:
        with contextlib.suppress(OSError):
            temporary.unlink(missing_ok=True)
        if isinstance(exc, DeploymentEvidenceError):
            raise
        raise DeploymentEvidenceError("the evidence output could not be written safely") from exc


def _same_deployment(state: Mapping[str, Any], observation: HostObservation) -> None:
    expected = {
        "source_identity": observation.identity.source_identity,
        "deployed_commit": observation.identity.deployed_commit,
        "source_tree_sha256": observation.identity.source_tree_sha256,
        "installed_release_tree_sha256": observation.identity.installed_release_tree_sha256,
        "host_id_sha256": observation.host_id_sha256,
    }
    if any(state.get(name) != value for name, value in expected.items()):
        raise DeploymentEvidenceError("the staged evidence belongs to a different host or deployment")


def exercise_process_kill(
    paths: CollectorPaths,
    runner: CommandRunner,
    *,
    target_service: str = "remote-mcp",
    timeout_seconds: float = 120,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    kill_process: Callable[[int, int], None] = os.kill,
    observe: Callable[[CollectorPaths, CommandRunner], HostObservation] = observe_host,
) -> dict[str, Any]:
    if target_service not in SERVICES or not 10 <= timeout_seconds <= 300:
        raise DeploymentEvidenceError("the process-kill exercise arguments differ from policy")
    before = observe(paths, runner)
    _assert_healthy(before)
    target = before.containers[target_service]
    # Re-observe immediately to close the PID-reuse window before SIGKILL.
    confirmed = observe(paths, runner)
    if confirmed.containers[target_service].process_sha256 != target.process_sha256:
        raise DeploymentEvidenceError("the target process changed before the controlled kill")
    killed_at = clock().astimezone(UTC)
    try:
        kill_process(target.pid, signal.SIGKILL)
    except Exception as exc:
        raise DeploymentEvidenceError("the controlled process kill could not be executed") from exc

    deadline = monotonic() + timeout_seconds
    recovered: HostObservation | None = None
    while monotonic() < deadline:
        sleep(1)
        try:
            candidate = observe(paths, runner)
            _assert_healthy(candidate)
        except DeploymentEvidenceError:
            continue
        after = candidate.containers[target_service]
        if after.process_sha256 != target.process_sha256:
            recovered = candidate
            break
    if recovered is None:
        raise DeploymentEvidenceError("the killed control process did not recover within the bound")
    recovered_at = clock().astimezone(UTC)
    state: dict[str, Any] = {
        "schema_version": STATE_SCHEMA_VERSION,
        "source_identity": recovered.identity.source_identity,
        "deployed_commit": recovered.identity.deployed_commit,
        "source_tree_sha256": recovered.identity.source_tree_sha256,
        "installed_release_tree_sha256": recovered.identity.installed_release_tree_sha256,
        "host_id_sha256": recovered.host_id_sha256,
        "pre_reboot_boot_id_sha256": recovered.boot_id_sha256,
        "process_kill": {
            "target_service": target_service,
            "killed_at": _utc_text(killed_at),
            "recovered_at": _utc_text(recovered_at),
            "before_process_sha256": target.process_sha256,
            "after_process_sha256": recovered.containers[target_service].process_sha256,
            "recovered": True,
        },
        "reboot_prepared_at": None,
    }
    _exact_state(state)
    _atomic_json(paths.state_file, state)
    return state


def prepare_reboot(
    paths: CollectorPaths,
    runner: CommandRunner,
    *,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    observe: Callable[[CollectorPaths, CommandRunner], HostObservation] = observe_host,
) -> dict[str, Any]:
    state = _read_state(paths.state_file)
    current = observe(paths, runner)
    _assert_healthy(current)
    _same_deployment(state, current)
    if state["pre_reboot_boot_id_sha256"] != current.boot_id_sha256:
        raise DeploymentEvidenceError("the host already rebooted before the reboot gate was prepared")
    target = str(state["process_kill"]["target_service"])
    if state["process_kill"]["after_process_sha256"] != current.containers[target].process_sha256:
        raise DeploymentEvidenceError("the recovered target process changed before reboot preparation")
    prepared = clock().astimezone(UTC)
    if prepared < _parse_utc(state["process_kill"]["recovered_at"], "process recovered_at"):
        raise DeploymentEvidenceError("the reboot preparation clock is out of order")
    state["reboot_prepared_at"] = _utc_text(prepared)
    _exact_state(state)
    _atomic_json(paths.state_file, state)
    return state


def _load_signing_key(path: Path) -> Ed25519PrivateKey:
    _regular_file(path, private=True, maximum_bytes=8192)
    try:
        key = load_pem_private_key(path.read_bytes(), password=None)
    except Exception as exc:
        raise DeploymentEvidenceError("the external deployment evidence signing key is invalid") from exc
    if not isinstance(key, Ed25519PrivateKey):
        raise DeploymentEvidenceError("the external deployment evidence signing key must be Ed25519")
    return key


def _canonical_unsigned(receipt: Mapping[str, Any]) -> bytes:
    unsigned = dict(receipt)
    unsigned.pop("signature", None)
    return json.dumps(unsigned, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode(
        "utf-8"
    )


def collect_and_sign(
    paths: CollectorPaths,
    runner: CommandRunner,
    *,
    signing_key_file: Path,
    key_id: str,
    output: Path,
    ttl_seconds: int = 3600,
    maximum_procedure_age_seconds: int = 86_400,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    observe: Callable[[CollectorPaths, CommandRunner], HostObservation] = observe_host,
) -> dict[str, Any]:
    if not _KEY_ID.fullmatch(key_id) or not 300 <= ttl_seconds <= 86_400:
        raise DeploymentEvidenceError("the signing policy arguments differ from policy")
    if not 300 <= maximum_procedure_age_seconds <= 604_800:
        raise DeploymentEvidenceError("the procedure age bound differs from policy")
    state = _read_state(paths.state_file)
    if state["reboot_prepared_at"] is None:
        raise DeploymentEvidenceError("the reboot gate was not prepared")
    current = observe(paths, runner)
    _assert_healthy(current)
    _same_deployment(state, current)
    if state["pre_reboot_boot_id_sha256"] == current.boot_id_sha256:
        raise DeploymentEvidenceError("a different host boot was not observed")

    now = clock().astimezone(UTC)
    killed_at = _parse_utc(state["process_kill"]["killed_at"], "process killed_at")
    recovered_at = _parse_utc(state["process_kill"]["recovered_at"], "process recovered_at")
    prepared_at = _parse_utc(state["reboot_prepared_at"], "reboot prepared_at")
    if (
        not killed_at <= recovered_at <= prepared_at <= current.booted_at <= now
        or now - killed_at > timedelta(seconds=maximum_procedure_age_seconds)
    ):
        raise DeploymentEvidenceError("the process-kill and reboot observations are stale or out of order")

    receipt: dict[str, Any] = {
        "schema_version": RECEIPT_SCHEMA_VERSION,
        "source_identity": current.identity.source_identity,
        "deployed_commit": current.identity.deployed_commit,
        "source_tree_sha256": current.identity.source_tree_sha256,
        "installed_release_tree_sha256": current.identity.installed_release_tree_sha256,
        "host_id_sha256": current.host_id_sha256,
        "issued_at": _utc_text(now),
        "expires_at": _utc_text(now + timedelta(seconds=ttl_seconds)),
        "checks": {
            "services": current.services,
            "service_image_ids": current.service_image_ids,
            "database_process_present": current.database_process_present,
            "pgdata_present": current.pgdata_present,
            "database_environment_present": current.database_environment_present,
            "my_data_hub_public_listener_ports": current.public_listener_ports,
            "my_data_hub_loopback_listener_ports": current.loopback_listener_ports,
            "process_kill": state["process_kill"],
            "reboot_autostart": {
                "rebooted_at": _utc_text(current.booted_at),
                "verified_at": _utc_text(now),
                "before_boot_id_sha256": state["pre_reboot_boot_id_sha256"],
                "after_boot_id_sha256": current.boot_id_sha256,
                "systemd_unit": UNIT,
                "unit_enabled": current.unit_enabled,
                "linger_enabled": current.linger_enabled,
                "autostart_services": list(SERVICES),
            },
        },
    }
    signature = _load_signing_key(signing_key_file).sign(_canonical_unsigned(receipt))
    receipt["signature"] = {
        "algorithm": "Ed25519",
        "key_id": key_id,
        "value": base64.urlsafe_b64encode(signature).rstrip(b"=").decode("ascii"),
    }
    _atomic_json(output, receipt)
    return receipt
