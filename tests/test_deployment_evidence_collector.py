from __future__ import annotations

import json
import signal
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from jsonschema import Draft202012Validator, FormatChecker

from my_data_hub.control_plane.deployment_evidence import (
    CollectorPaths,
    CommandRunner,
    ContainerObservation,
    DeploymentEvidenceError,
    DeploymentIdentity,
    HostObservation,
    _container_observations,
    _database_process_present,
    collect_and_sign,
    derive_deployment_identity,
    exercise_process_kill,
    prepare_reboot,
)
from scripts.verify_post_deploy import validate_deployment_evidence

COMMIT = "a" * 40
SOURCE = "onedayonemasterpiece/my-data-hub"
TREE = "b" * 64
IMAGE = "sha256:" + "c" * 64
BEFORE_BOOT = "d" * 64
AFTER_BOOT = "e" * 64
HOST = "f" * 64
NOW = datetime(2026, 8, 11, 4, 0, tzinfo=UTC)


class NoCommands(CommandRunner):
    def run(self, arguments: list[str], *, timeout_seconds: float = 15) -> str:
        raise AssertionError(arguments)


def _identity(tmp_path: Path) -> DeploymentIdentity:
    return DeploymentIdentity(
        source_identity=SOURCE,
        deployed_commit=COMMIT,
        source_tree_sha256=TREE,
        installed_release_tree_sha256=TREE,
        release=tmp_path / "release",
        compose_environment=tmp_path / "compose.env",
    )


def _observation(
    tmp_path: Path,
    *,
    boot: str = BEFORE_BOOT,
    booted_at: datetime = NOW - timedelta(hours=2),
    target_process: str = "1" * 64,
) -> HostObservation:
    containers = {
        service: ContainerObservation(
            service=service,
            container_id=str(index) * 64,
            image_id=IMAGE,
            pid=100 + index,
            started_at=f"2026-08-11T03:0{index}:00Z",
            process_sha256=target_process if service == "remote-mcp" else str(index + 3) * 64,
        )
        for index, service in enumerate(("control-plane", "oauth-server", "remote-mcp"), 1)
    }
    return HostObservation(
        identity=_identity(tmp_path),
        host_id_sha256=HOST,
        boot_id_sha256=boot,
        booted_at=booted_at,
        services={
            "control-plane": "running",
            "oauth-server": "running",
            "remote-mcp": "running",
        },
        service_image_ids={service: IMAGE for service in containers},
        containers=containers,
        public_listener_ports=[],
        loopback_listener_ports=[8080, 8765, 8780],
        unit_enabled=True,
        unit_active=True,
        linger_enabled=True,
        database_process_present=False,
        pgdata_present=False,
        database_environment_present=False,
    )


def _paths(tmp_path: Path) -> CollectorPaths:
    tmp_path.chmod(0o700)
    return CollectorPaths(
        source_root=tmp_path / "source",
        runtime_root=tmp_path,
        release_root=tmp_path / "opt",
        state_file=tmp_path / "deployment-evidence-state.v1.json",
    )


def test_process_kill_reboot_and_sign_are_observed_not_asserted(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    before = _observation(tmp_path)
    recovered = _observation(tmp_path, target_process="2" * 64)
    post_reboot = _observation(
        tmp_path,
        boot=AFTER_BOOT,
        booted_at=NOW + timedelta(minutes=3),
        target_process="3" * 64,
    )
    observations = iter((before, before, recovered))
    killed: list[tuple[int, int]] = []
    times = iter((NOW, NOW + timedelta(minutes=1)))
    monotonic = iter((0.0, 1.0))

    state = exercise_process_kill(
        paths,
        NoCommands(),
        observe=lambda *_: next(observations),
        kill_process=lambda pid, sig: killed.append((pid, sig)),
        clock=lambda: next(times),
        monotonic=lambda: next(monotonic),
        sleep=lambda _: None,
    )
    assert killed == [(before.containers["remote-mcp"].pid, signal.SIGKILL)]
    assert state["process_kill"]["before_process_sha256"] == "1" * 64
    assert state["process_kill"]["after_process_sha256"] == "2" * 64
    assert paths.state_file.stat().st_mode & 0o077 == 0

    prepared = prepare_reboot(
        paths,
        NoCommands(),
        observe=lambda *_: recovered,
        clock=lambda: NOW + timedelta(minutes=2),
    )
    assert prepared["reboot_prepared_at"] == "2026-08-11T04:02:00Z"

    private = Ed25519PrivateKey.generate()
    key_file = tmp_path / "external-evidence-key.pem"
    key_file.write_bytes(
        private.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    key_file.chmod(0o600)
    output = tmp_path / "deployment-evidence.v1.json"
    receipt = collect_and_sign(
        paths,
        NoCommands(),
        signing_key_file=key_file,
        key_id="devstand-evidence-2026-08",
        output=output,
        observe=lambda *_: post_reboot,
        clock=lambda: NOW + timedelta(minutes=4),
    )
    assert receipt["checks"]["service_image_ids"] == {
        "control-plane": IMAGE,
        "oauth-server": IMAGE,
        "remote-mcp": IMAGE,
    }
    assert receipt["source_tree_sha256"] == receipt["installed_release_tree_sha256"] == TREE
    public_pem = private.public_key().public_bytes(
        serialization.Encoding.PEM,
        serialization.PublicFormat.SubjectPublicKeyInfo,
    ).decode()
    verified = validate_deployment_evidence(
        output.read_text(encoding="utf-8"),
        public_pem,
        expected_commit=COMMIT,
        expected_source_identity=SOURCE,
        expected_key_id="devstand-evidence-2026-08",
        now=NOW + timedelta(minutes=4),
    )
    assert verified["service_image_ids"]["remote-mcp"] == IMAGE
    assert "private" not in output.read_text(encoding="utf-8").casefold()


def test_signing_fails_without_a_different_observed_boot(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    before = _observation(tmp_path)
    recovered = _observation(tmp_path, target_process="2" * 64)
    observations = iter((before, before, recovered))
    times = iter((NOW, NOW + timedelta(minutes=1)))
    monotonic = iter((0.0, 1.0))
    exercise_process_kill(
        paths,
        NoCommands(),
        observe=lambda *_: next(observations),
        kill_process=lambda *_: None,
        clock=lambda: next(times),
        monotonic=lambda: next(monotonic),
        sleep=lambda _: None,
    )
    prepare_reboot(
        paths,
        NoCommands(),
        observe=lambda *_: recovered,
        clock=lambda: NOW + timedelta(minutes=2),
    )
    key = tmp_path / "key.pem"
    key.write_text("not-read-because-boot-gate-fails", encoding="utf-8")
    key.chmod(0o600)
    with pytest.raises(DeploymentEvidenceError, match="different host boot"):
        collect_and_sign(
            paths,
            NoCommands(),
            signing_key_file=key,
            key_id="key-1",
            output=tmp_path / "receipt.json",
            observe=lambda *_: recovered,
            clock=lambda: NOW + timedelta(minutes=3),
        )


def test_unhealthy_or_mutable_image_observation_fails_closed(tmp_path: Path) -> None:
    paths = _paths(tmp_path)
    unhealthy = replace(_observation(tmp_path), database_environment_present=True)
    with pytest.raises(DeploymentEvidenceError, match="signed evidence boundary"):
        exercise_process_kill(paths, NoCommands(), observe=lambda *_: unhealthy)

    mismatched_images = replace(
        _observation(tmp_path),
        service_image_ids={
            "control-plane": IMAGE,
            "oauth-server": IMAGE,
            "remote-mcp": "sha256:" + "9" * 64,
        },
    )
    with pytest.raises(DeploymentEvidenceError, match="signed evidence boundary"):
        exercise_process_kill(paths, NoCommands(), observe=lambda *_: mismatched_images)


class GitRunner(CommandRunner):
    def __init__(self, source: Path, entries: str) -> None:
        self.source = source
        self.entries = entries

    def run(self, arguments: list[str], *, timeout_seconds: float = 15) -> str:
        suffix = arguments[3:]
        if suffix == ["rev-parse", "HEAD"]:
            return COMMIT
        if suffix == ["status", "--porcelain", "--untracked-files=no"]:
            return ""
        if suffix == ["remote", "get-url", "origin"]:
            return "https://github.com/onedayonemasterpiece/my-data-hub.git"
        if suffix == ["ls-files", "--stage", "-z"]:
            return self.entries
        raise AssertionError(arguments)


def test_installed_release_hash_is_derived_from_exact_clean_source(tmp_path: Path) -> None:
    source = tmp_path / "source"
    runtime = tmp_path / "runtime"
    release_root = tmp_path / "opt"
    release = release_root / "releases" / COMMIT
    for root in (source, runtime, release):
        root.mkdir(parents=True)
    (source / "a.txt").write_text("same content\n", encoding="utf-8")
    (release / "a.txt").write_text("same content\n", encoding="utf-8")
    entries = f"100644 {'1' * 40} 0\ta.txt\0"
    (release_root / "current").symlink_to(release)
    compose = runtime / f"compose.{COMMIT}.env"
    compose.write_text("MY_DATA_HUB_IMAGE_TAG=" + COMMIT + "\n", encoding="utf-8")
    compose.chmod(0o600)
    (release / "a.txt").chmod(0o444)
    release.chmod(0o555)
    paths = CollectorPaths(source, runtime, release_root, runtime / "state.json")

    identity = derive_deployment_identity(paths, GitRunner(source, entries))
    assert identity.source_tree_sha256 == identity.installed_release_tree_sha256
    assert len(identity.source_tree_sha256) == 64

    release.chmod(0o755)
    (release / "a.txt").chmod(0o644)
    (release / "a.txt").write_text("different\n", encoding="utf-8")
    with pytest.raises(DeploymentEvidenceError, match=r"writable|differs"):
        derive_deployment_identity(paths, GitRunner(source, entries))


class DockerRunner(CommandRunner):
    def __init__(self, *, mismatch: bool = False) -> None:
        self.mismatch = mismatch

    def run(self, arguments: list[str], *, timeout_seconds: float = 15) -> str:
        if arguments[:2] == ["docker", "ps"]:
            return "\n".join(str(index) * 64 for index in range(1, 4))
        if arguments[:2] == ["docker", "image"]:
            return json.dumps([{"Id": IMAGE}])
        if arguments[:2] == ["docker", "inspect"]:
            records: list[dict[str, Any]] = []
            for index, service in enumerate(("control-plane", "oauth-server", "remote-mcp"), 1):
                port = {"control-plane": 8080, "oauth-server": 8780, "remote-mcp": 8765}[service]
                image = "sha256:" + "9" * 64 if self.mismatch and service == "remote-mcp" else IMAGE
                records.append(
                    {
                        "Id": str(index) * 64,
                        "Image": image,
                        "Config": {
                            "Image": f"my-data-hub-control-plane:{COMMIT}",
                            "Labels": {
                                "com.docker.compose.project": "my-data-hub-control-plane",
                                "com.docker.compose.service": service,
                            },
                        },
                        "State": {
                            "Running": True,
                            "Status": "running",
                            "Pid": 100 + index,
                            "StartedAt": f"2026-08-11T03:0{index}:00Z",
                            "Health": {"Status": "healthy"},
                        },
                        "HostConfig": {"RestartPolicy": {"Name": "unless-stopped"}},
                        "NetworkSettings": {
                            "Ports": {
                                f"{port}/tcp": [{"HostIp": "127.0.0.1", "HostPort": str(port)}]
                            }
                        },
                    }
                )
            return json.dumps(records)
        raise AssertionError(arguments)


def test_docker_observation_binds_every_service_to_one_immutable_image(tmp_path: Path) -> None:
    containers, public, loopback = _container_observations(
        _identity(tmp_path), DockerRunner(), boot_id_sha256=BEFORE_BOOT
    )
    assert {item.image_id for item in containers.values()} == {IMAGE}
    assert public == []
    assert loopback == [8080, 8765, 8780]
    with pytest.raises(DeploymentEvidenceError, match="immutable image"):
        _container_observations(
            _identity(tmp_path), DockerRunner(mismatch=True), boot_id_sha256=BEFORE_BOOT
        )


def test_proc_scan_detects_postgres_without_exposing_command(tmp_path: Path) -> None:
    proc = tmp_path / "proc"
    process = proc / "123"
    process.mkdir(parents=True)
    (process / "comm").write_text("postgres\n", encoding="utf-8")
    (process / "cmdline").write_bytes(b"/usr/lib/postgresql/18/bin/postgres\0-D\0/private\0")
    assert _database_process_present(proc) is True


def test_committed_evidence_examples_validate_and_contain_no_key_material() -> None:
    checker = FormatChecker()
    for name in ("deployment-evidence.v1", "deployment-evidence-state.v1"):
        schema = json.loads(Path(f"schemas/{name}.schema.json").read_text(encoding="utf-8"))
        example_text = Path(f"examples/contracts/{name}.example.json").read_text(encoding="utf-8")
        example = json.loads(example_text)
        assert not list(Draft202012Validator(schema, format_checker=checker).iter_errors(example))
        assert "begin private key" not in example_text.casefold()
        assert "password" not in example_text.casefold()
