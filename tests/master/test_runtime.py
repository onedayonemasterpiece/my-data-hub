from __future__ import annotations

import subprocess
from collections.abc import Mapping, Sequence
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.master_runtime.bootstrap import BootstrapError, BootstrapRequest, MasterBootstrap
from my_data_hub.master_runtime.contracts import BootPhase, BootSource, MasterIdentity, MasterPaths
from my_data_hub.master_runtime.postgres import (
    KAGGLE_POSTGRES_GID,
    KAGGLE_POSTGRES_UID,
    PostgresBinaries,
    PostgresConfig,
    PostgresSupervisor,
    SubprocessRunner,
)
from my_data_hub.master_runtime.tunnel import ReverseTunnelSpec

NOW = datetime(2026, 8, 10, tzinfo=UTC)
IDENTITY = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "run-1", 1)


class _Runner:
    def __init__(self) -> None:
        self.calls: list[list[str]] = []

    def run(
        self,
        arguments: Sequence[str],
        *,
        timeout_seconds: int,
        environment: Mapping[str, str] | None = None,
    ) -> subprocess.CompletedProcess[str]:
        self.calls.append(list(arguments))
        return subprocess.CompletedProcess(list(arguments), 0, "", "")


def _tls(tmp_path: Path) -> tuple[Path, Path]:
    certificate = tmp_path / "server.crt"
    key = tmp_path / "server.key"
    certificate.write_text("certificate")
    key.write_text("key")
    key.chmod(0o600)
    return certificate, key


def test_postgres_config_is_tls_loopback_bounded_and_paths_stay_in_working(tmp_path: Path) -> None:
    certificate, key = _tls(tmp_path)
    paths = MasterPaths.under(tmp_path / "working")
    config = PostgresConfig(15432, certificate, key)
    rendered = config.render(paths)
    hba = config.render_hba()
    assert "listen_addresses = '127.0.0.1'" in rendered
    assert "ssl_min_protocol_version = 'TLSv1.3'" in rendered
    assert "log_parameter_max_length = 0" in rendered
    assert "local replication postgres trust" in hba
    assert hba.index("local replication postgres trust") < hba.index("local all postgres trust")
    assert "hostnossl all all 0.0.0.0/0 reject" in hba
    assert "hostssl all all 127.0.0.1/32 scram-sha-256" in hba
    escaped = MasterPaths(
        working=tmp_path / "working",
        pgdata=tmp_path / "outside",
        socket=paths.socket,
        logs=paths.logs,
        runtime_events=paths.runtime_events,
        checkpoints=paths.checkpoints,
    )
    with pytest.raises(ValueError, match="escapes"):
        escaped.validate()


def test_empty_bootstrap_commands_are_deterministic_and_no_shell(tmp_path: Path) -> None:
    certificate, key = _tls(tmp_path)
    binaries = PostgresBinaries(**{name: Path(f"/fixture/{name}") for name in PostgresBinaries.__annotations__})
    runner = _Runner()
    supervisor = PostgresSupervisor(
        paths=MasterPaths.under(tmp_path / "working"),
        binaries=binaries,
        config=PostgresConfig(15432, certificate, key),
        runner=runner,
    )
    supervisor.initialize_empty()
    supervisor.start()
    assert runner.calls[0][0] == "/fixture/initdb"
    assert "--auth-host=scram-sha-256" in runner.calls[0]
    assert runner.calls[1][0] == "/fixture/pg_ctl"
    assert runner.calls[2][0] == "/fixture/pg_isready"
    assert (tmp_path / "working/postgres/data/postgresql.auto.conf").is_file()


def test_root_kaggle_postgres_uses_setpriv_without_python_preexec(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    observed: dict[str, object] = {}

    def fake_run(arguments, **kwargs):  # type: ignore[no-untyped-def]
        observed["arguments"] = arguments
        observed["kwargs"] = kwargs
        return subprocess.CompletedProcess(arguments, 0, "", "")

    monkeypatch.setattr("my_data_hub.master_runtime.postgres.os.geteuid", lambda: 0)
    setpriv = tmp_path / "setpriv"
    setpriv.write_text("fixture", encoding="utf-8")
    setpriv.chmod(0o700)
    monkeypatch.setattr("my_data_hub.master_runtime.postgres.SETPRIV", setpriv)
    monkeypatch.setattr("my_data_hub.master_runtime.postgres.os.access", lambda *_args: True)
    monkeypatch.setattr("my_data_hub.master_runtime.postgres.subprocess.run", fake_run)
    SubprocessRunner().run(["/runtime/bin/initdb", "--version"], timeout_seconds=10)
    assert observed["arguments"] == [
        str(setpriv),
        f"--reuid={KAGGLE_POSTGRES_UID}",
        f"--regid={KAGGLE_POSTGRES_GID}",
        "--clear-groups",
        "--",
        "/runtime/bin/initdb",
        "--version",
    ]
    assert "preexec_fn" not in observed["kwargs"]  # type: ignore[operator]


def test_root_kaggle_runtime_transfers_only_postgres_paths(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    certificate, key = _tls(tmp_path)
    paths = MasterPaths.under(tmp_path / "working")
    binaries = PostgresBinaries(**{name: Path(f"/fixture/{name}") for name in PostgresBinaries.__annotations__})
    ownership: list[tuple[Path, int, int]] = []
    monkeypatch.setattr("my_data_hub.master_runtime.postgres.os.geteuid", lambda: 0)
    monkeypatch.setattr(
        "my_data_hub.master_runtime.postgres.os.chown",
        lambda path, uid, gid: ownership.append((Path(path), uid, gid)),
    )
    supervisor = PostgresSupervisor(
        paths=paths,
        binaries=binaries,
        config=PostgresConfig(15432, certificate, key),
        runner=_Runner(),
    )
    supervisor.prepare_directories()
    owned = {item[0] for item in ownership}
    assert {paths.pgdata, paths.socket, paths.logs, certificate, key} <= owned
    assert paths.runtime_events.parent not in owned
    assert all(uid == KAGGLE_POSTGRES_UID and gid == KAGGLE_POSTGRES_GID for _, uid, gid in ownership)


def test_reverse_tunnel_is_loopback_only_and_disables_shell_agent_and_unknown_hosts(tmp_path: Path) -> None:
    identity = tmp_path / "id_ed25519"
    certificate = tmp_path / "id_ed25519-cert.pub"
    known_hosts = tmp_path / "known_hosts"
    identity.write_text("private")
    identity.chmod(0o600)
    certificate.write_text("ssh-ed25519-cert-v01@openssh.com fixture")
    certificate.chmod(0o600)
    known_hosts.write_text("gateway fixture-key")
    spec = ReverseTunnelSpec(
        gateway_host="gateway.example.test",
        gateway_port=22,
        gateway_user="mdh-tunnel",
        remote_bind_host="127.0.0.1",
        remote_bind_port=25432,
        local_postgres_port=15432,
        identity_file=identity,
        certificate_file=certificate,
        known_hosts_file=known_hosts,
        expires_at=NOW + timedelta(minutes=5),
    )
    arguments = spec.arguments(now=NOW)
    joined = " ".join(arguments)
    assert "-N -T" in joined
    assert arguments[:4] == ["ssh", "-F", "/dev/null", "-N"]
    assert "ClearAllForwardings=yes" not in joined
    assert "StrictHostKeyChecking=yes" in joined
    assert "ForwardAgent=no" in joined
    assert f"CertificateFile={certificate}" in joined
    assert "127.0.0.1:25432:127.0.0.1:15432" in joined

    public = replace(spec, remote_bind_host="0.0.0.0")
    with pytest.raises(ValueError, match="loopback"):
        public.arguments(now=NOW)


class _Postgres:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def initialize_empty(self) -> None:
        self.events.append("empty")

    def start(self) -> None:
        self.events.append("postgres-start")

    def stop(self, *, immediate: bool = False) -> None:
        self.events.append(f"postgres-stop-{immediate}")


class _Gate:
    def __init__(self, events: list[str]) -> None:
        self.events = events

    def acquire(self, identity: MasterIdentity, lease_until: datetime) -> None:
        self.events.append("epoch-closed")

    def fence(self, identity: MasterIdentity, reason: str) -> None:
        self.events.append(f"fence-{reason}")


class _Tunnel:
    def __init__(self, events: list[str], fail: bool = False) -> None:
        self.events = events
        self.fail = fail

    def start(self, *, now: datetime) -> None:
        self.events.append("tunnel-start")
        if self.fail:
            raise RuntimeError("fixture failure")

    def stop(self) -> None:
        self.events.append("tunnel-stop")


def _bootstrap(events: list[str], *, fail_tunnel: bool = False) -> MasterBootstrap:
    return MasterBootstrap(
        postgres=_Postgres(events),
        gate=_Gate(events),
        tunnel=_Tunnel(events, fail=fail_tunnel),
        restore=lambda path: events.append(f"restore-{path.name}"),
        migrate=lambda: events.append("migrate"),
        reconcile_roles=lambda: events.append("roles"),
        verify_database=lambda: (11, 7, "a" * 64),
        announce_ready=lambda receipt: events.append(f"ready-{receipt.identity.epoch}"),
        endpoint=lambda: "tunnel://loopback/25432",
    )


def test_bootstrap_does_not_open_gate_before_external_activation() -> None:
    events: list[str] = []
    bootstrap = _bootstrap(events)
    receipt = bootstrap.run(
        BootstrapRequest(IDENTITY, BootSource.EMPTY_BASELINE, NOW + timedelta(minutes=1), NOW)
    )
    assert receipt.schema_version == 11
    assert events == ["empty", "postgres-start", "migrate", "roles", "epoch-closed", "tunnel-start", "ready-1"]
    assert bootstrap.phases[-1] is BootPhase.WAITING_FOR_ACTIVATION
    assert not any("open" in event for event in events)


def test_restore_bootstrap_and_failure_fence_are_deterministic(tmp_path: Path) -> None:
    events: list[str] = []
    bootstrap = _bootstrap(events, fail_tunnel=True)
    with pytest.raises(BootstrapError, match="starting_tunnel"):
        bootstrap.run(
            BootstrapRequest(
                IDENTITY,
                BootSource.VERIFIED_CHECKPOINT,
                NOW + timedelta(minutes=1),
                NOW,
                checkpoint_directory=tmp_path / "checkpoint",
            )
        )
    assert events == [
        "restore-checkpoint",
        "postgres-start",
        "migrate",
        "roles",
        "epoch-closed",
        "tunnel-start",
        "fence-bootstrap_failed",
        "postgres-stop-True",
    ]
