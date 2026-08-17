from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from my_data_hub.control_plane.clock import DeterministicClock
from my_data_hub.control_plane.ledger import ControlLedger
from my_data_hub.orchestrator.master import FakeKaggleRuntime, MasterCoordinator, MasterIntent, MasterState
from my_data_hub.tunnel_broker import (
    DEFAULT_ACCOUNT,
    TunnelBroker,
    TunnelBrokerError,
    render_sshd_config,
)
from my_data_hub.tunnel_broker_ipc import _dispatch

ROOT = Path(__file__).resolve().parents[2]
INSTALLER = ROOT / "deploy" / "control-plane" / "install_master_tunnel_broker.sh"
INSTANCE = "9b6f6627-373c-4760-9dd8-c9c58e50178c"
NOW = datetime(2026, 8, 11, 12, tzinfo=UTC)
RUN_ID = "run-7"
ATTEMPT_ID = "attempt-2"


def _key(path: Path, comment: str) -> Path:
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-C", comment, "-f", str(path)],
        check=True,
    )
    return Path(f"{path}.pub")


def _broker(tmp_path: Path) -> tuple[TunnelBroker, list[str], Path]:
    ca = tmp_path / "ca"
    _key(ca, "test tunnel CA")
    os.chmod(ca, 0o600)
    terminated: list[str] = []
    broker = TunnelBroker(
        tmp_path / "broker",
        ca_private_key=ca,
        session_terminator=terminated.append,
    )
    broker.initialize()
    return broker, terminated, ca


def test_authorized_principals_parent_is_traversable_without_exposing_private_state(
    tmp_path: Path,
) -> None:
    broker, _terminated, _ca = _broker(tmp_path)

    # OpenSSH reads AuthorizedPrincipalsFile after temporarily switching to
    # the target tunnel UID.  The root-owned parent must therefore be
    # searchable, while durable broker state and locks remain private.
    assert broker.root.stat().st_mode & 0o777 == 0o711
    assert broker.principals_path.stat().st_mode & 0o777 == 0o644
    assert broker.worker_principals_path.stat().st_mode & 0o777 == 0o644
    assert broker.state_path.stat().st_mode & 0o777 == 0o600
    assert broker.lock_path.stat().st_mode & 0o777 == 0o600


def test_sshd_match_is_certificate_only_remote_forward_to_one_loopback_port() -> None:
    config = render_sshd_config(
        account=DEFAULT_ACCOUNT,
        ca_public_key=Path("/etc/my-data-hub/tunnel-user-ca.pub"),
        principals_file=Path("/var/lib/my-data-hub/tunnel-broker/authorized_principals"),
        revoked_keys_file=Path("/var/lib/my-data-hub/tunnel-broker/revoked.krl"),
        listen_port=25432,
    )
    required = (
        "Match User mdh-master-tunnel",
        "AuthenticationMethods publickey",
        "AuthorizedKeysFile none",
        "TrustedUserCAKeys /etc/my-data-hub/tunnel-user-ca.pub",
        "AllowTcpForwarding remote",
        "PermitListen 127.0.0.1:25432",
        "PermitOpen none",
        "GatewayPorts no",
        "PermitTTY no",
        "AllowAgentForwarding no",
        "X11Forwarding no",
        "PermitTunnel no",
        "PermitUserRC no",
        "MaxSessions 0",
        "Match all",
    )
    assert all(item in config for item in required)
    assert "0.0.0.0" not in config
    assert "*:*" not in config
    assert "PermitListen any" not in config
    assert "AllowTcpForwarding yes" not in config
    assert "ForceCommand" not in config  # -N forwarding stays alive; MaxSessions denies shells.


def test_issue_is_epoch_and_lease_bound_and_returns_only_public_certificate(tmp_path: Path) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    active = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=7,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    client_private = tmp_path / "ephemeral"
    public_key = _key(client_private, "ephemeral master tunnel").read_text(encoding="ascii")
    certificate = broker.issue_public_key(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=7,
        public_key=public_key,
        valid_before=NOW + timedelta(minutes=4),
        now=NOW,
    )
    certificate_path = tmp_path / "ephemeral-cert.pub"
    certificate_path.write_text(certificate.certificate, encoding="ascii")
    inspected = subprocess.run(
        ["ssh-keygen", "-L", "-f", str(certificate_path)],
        check=True,
        capture_output=True,
        text=True,
    ).stdout

    assert certificate.serial == 1
    assert certificate.principal == active.principal
    assert certificate.listen_host == "127.0.0.1"
    assert certificate.listen_port == 25432
    assert "Serial: 1" in inspected
    assert active.principal in inspected
    assert "permit-port-forwarding" in inspected
    for forbidden_extension in ("permit-agent-forwarding", "permit-pty", "permit-user-rc", "permit-X11-forwarding"):
        assert forbidden_extension not in inspected
    state_text = broker.state_path.read_text(encoding="utf-8")
    assert public_key not in state_text
    assert certificate.certificate not in state_text
    assert client_private.read_text(encoding="ascii") not in state_text
    assert set(json.loads(state_text)) == {
        "schema_version",
        "highest_epoch",
        "next_serial",
        "active",
        "issued",
        "revoked_serials",
        "worker_issued",
    }
    assert terminated == [DEFAULT_ACCOUNT]


def test_deactivate_revokes_certificate_blanks_principal_and_terminates_account(tmp_path: Path) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=3,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    client_public = _key(tmp_path / "client", "one-run key")
    certificate_path = tmp_path / "client-cert.out"
    serial = broker.issue(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=3,
        public_key=client_public,
        certificate_output=certificate_path,
        valid_before=NOW + timedelta(minutes=4),
        now=NOW,
    )

    broker.deactivate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=3,
        reason="runtime_terminal",
    )

    assert serial == 1
    assert broker.principals_path.read_text(encoding="ascii") == ""
    query = subprocess.run(
        ["ssh-keygen", "-Q", "-f", str(broker.krl_path), str(certificate_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert query.returncode != 0
    assert terminated == [DEFAULT_ACCOUNT, DEFAULT_ACCOUNT]
    state = json.loads(broker.state_path.read_text(encoding="utf-8"))
    assert state["active"] is None
    assert state["revoked_serials"] == [1]
    with pytest.raises(TunnelBrokerError, match="fenced"):
        broker.issue_public_key(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=3,
            public_key=client_public.read_text(encoding="ascii"),
            valid_before=NOW + timedelta(minutes=4),
            now=NOW + timedelta(seconds=10),
        )


def test_deactivate_is_idempotent_before_tunnel_activation(tmp_path: Path) -> None:
    broker, terminated, _ca = _broker(tmp_path)

    for _ in range(2):
        broker.deactivate(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=1,
            reason="provider_terminal_failed",
        )

    state = json.loads(broker.state_path.read_text(encoding="utf-8"))
    assert state["active"] is None
    assert broker.principals_path.read_text(encoding="ascii") == ""
    assert terminated == [DEFAULT_ACCOUNT, DEFAULT_ACCOUNT]


def test_exact_serial_revocation_is_immediate_and_wrong_serial_is_only_denied(tmp_path: Path) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    active = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=4,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    client_public = _key(tmp_path / "client", "one-run key")
    certificate_path = tmp_path / "client-cert.out"
    broker.issue(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=4,
        public_key=client_public,
        certificate_output=certificate_path,
        valid_before=NOW + timedelta(minutes=4),
        now=NOW,
    )
    with pytest.raises(TunnelBrokerError, match="not bound"):
        broker.revoke(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=4,
            serial=99,
            reason="operator_rejected",
        )
    assert broker.principals_path.read_text(encoding="ascii") == f"{active.principal}\n"
    assert terminated == [DEFAULT_ACCOUNT]

    broker.revoke(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=4,
        serial=1,
        reason="operator_rejected",
    )
    query = subprocess.run(
        ["ssh-keygen", "-Q", "-f", str(broker.krl_path), str(certificate_path)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert query.returncode != 0
    assert "REVOKED" in f"{query.stdout}\n{query.stderr}"
    assert terminated == [DEFAULT_ACCOUNT, DEFAULT_ACCOUNT]


def test_expiry_reconcile_and_corrupt_authority_fail_closed(tmp_path: Path) -> None:
    broker, terminated, ca = _broker(tmp_path)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=9,
        lease_until=NOW + timedelta(minutes=1),
        listen_port=25432,
        now=NOW,
    )
    assert broker.reconcile(now=NOW + timedelta(seconds=59)) is True
    assert broker.reconcile(now=NOW + timedelta(minutes=1)) is False
    assert broker.principals_path.read_bytes() == b""

    broker.state_path.write_text("not-json", encoding="utf-8")
    os.chmod(broker.state_path, 0o600)
    with pytest.raises(TunnelBrokerError, match="unreadable"):
        broker.reconcile(now=NOW + timedelta(minutes=2))
    ca_query = subprocess.run(
        ["ssh-keygen", "-Q", "-f", str(broker.krl_path), str(Path(f"{ca}.pub"))],
        check=False,
        capture_output=True,
        text=True,
    )
    assert ca_query.returncode != 0
    assert "REVOKED" in f"{ca_query.stdout}\n{ca_query.stderr}"
    assert broker.principals_path.read_bytes() == b""
    assert terminated[-2:] == [DEFAULT_ACCOUNT, "mdh-embedding-worker"]


def test_stale_epoch_wrong_identity_and_overlong_certificate_are_denied(tmp_path: Path) -> None:
    broker, _terminated, _ca = _broker(tmp_path)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=2,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    public_key = _key(tmp_path / "client", "one-run key").read_text(encoding="ascii")
    active_principal = broker.principals_path.read_text(encoding="ascii")
    with pytest.raises(TunnelBrokerError, match="current unexpired epoch"):
        broker.issue_public_key(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id="wrong-attempt",
            epoch=2,
            public_key=public_key,
            valid_before=NOW + timedelta(minutes=2),
            now=NOW,
        )
    with pytest.raises(TunnelBrokerError, match="current unexpired epoch"):
        broker.issue_public_key(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=1,
            public_key=public_key,
            valid_before=NOW + timedelta(minutes=2),
            now=NOW,
        )
    assert broker.principals_path.read_text(encoding="ascii") == active_principal
    with pytest.raises(TunnelBrokerError, match="validity"):
        broker.issue_public_key(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=2,
            public_key=public_key,
            valid_before=NOW + timedelta(minutes=6),
            now=NOW,
        )
    replay = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=2,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    assert replay.principal == active_principal.strip()
    shorter_replay = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=2,
        lease_until=NOW + timedelta(minutes=4),
        listen_port=25432,
        now=NOW + timedelta(seconds=10),
    )
    assert shorter_replay.lease_until == NOW + timedelta(minutes=5)
    with pytest.raises(TunnelBrokerError, match="advance"):
        broker.activate(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id="different-attempt",
            epoch=2,
            lease_until=NOW + timedelta(minutes=4),
            listen_port=25432,
            now=NOW,
        )


def test_activation_response_loss_advances_same_identity_without_reviving_expired_epoch(tmp_path: Path) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    first = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=5,
        lease_until=NOW + timedelta(minutes=2),
        listen_port=25432,
        now=NOW,
    )
    advanced = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=5,
        lease_until=NOW + timedelta(minutes=3),
        listen_port=25432,
        now=NOW + timedelta(seconds=30),
    )
    assert first.lease_until == NOW + timedelta(minutes=2)
    assert advanced.lease_until == NOW + timedelta(minutes=3)
    assert advanced.principal == first.principal
    assert terminated == [DEFAULT_ACCOUNT]
    durable = json.loads(broker.state_path.read_text(encoding="utf-8"))
    assert durable["highest_epoch"] == 5
    assert durable["active"]["lease_until"] == "2026-08-11T12:03:00Z"
    delayed_replay = broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=5,
        lease_until=NOW + timedelta(seconds=10),
        listen_port=25432,
        now=NOW + timedelta(minutes=1),
    )
    assert delayed_replay.lease_until == NOW + timedelta(minutes=3)

    with pytest.raises(TunnelBrokerError, match="listener"):
        broker.activate(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=5,
            lease_until=NOW + timedelta(minutes=4),
            listen_port=25433,
            now=NOW + timedelta(minutes=1),
        )
    with pytest.raises(TunnelBrokerError, match="cannot be revived"):
        broker.activate(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=5,
            lease_until=NOW + timedelta(minutes=8),
            listen_port=25432,
            now=NOW + timedelta(minutes=3),
        )
    expired_state = json.loads(broker.state_path.read_text(encoding="utf-8"))
    assert expired_state["active"] is None
    assert expired_state["highest_epoch"] == 5
    assert broker.principals_path.read_bytes() == b""
    assert terminated == [DEFAULT_ACCOUNT, DEFAULT_ACCOUNT]
    with pytest.raises(TunnelBrokerError, match="advance"):
        broker.activate(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=5,
            lease_until=NOW + timedelta(minutes=9),
            listen_port=25432,
            now=NOW + timedelta(minutes=4),
        )


def test_coordinator_absent_reconciliation_reuses_epoch_and_monotonically_extends_broker_lease(
    tmp_path: Path,
) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    clock = DeterministicClock(NOW)
    ledger = ControlLedger(tmp_path / "control.sqlite3", clock=clock)
    provider = FakeKaggleRuntime({"trigger_run": [RuntimeError("lost-before-provider-effect")]})
    coordinator = MasterCoordinator(ledger, provider, tunnel_authority=broker)
    request = MasterIntent(
        idempotency_key="tunnel-activation-response-loss",
        source_identity="my-data-hub/postgres-master",
        source_version="git:0123456789abcdef",
        checkpoint_ref="EMPTY_BASELINE",
        dataset_ref="private/checkpoint-dataset",
        notebook_ref="private/postgres-master",
    )

    with pytest.raises(RuntimeError, match="lost-before-provider-effect"):
        coordinator.ensure_master(request, runtime_secret="correct-horse-battery-staple")
    first = json.loads(broker.state_path.read_text(encoding="utf-8"))["active"]
    assert first["lease_until"] == "2026-08-11T12:05:00Z"

    clock.advance(30)
    recovered = coordinator.ensure_master(request, runtime_secret="correct-horse-battery-staple")
    second = json.loads(broker.state_path.read_text(encoding="utf-8"))["active"]
    assert recovered.state is MasterState.REGISTERING
    assert recovered.epoch == 1
    assert second["master_instance_id"] == first["master_instance_id"]
    assert second["run_id"] == first["run_id"]
    assert second["attempt_id"] == first["attempt_id"]
    assert second["epoch"] == first["epoch"] == 1
    assert second["lease_until"] == "2026-08-11T12:05:30Z"
    assert provider.physical_effect_counts["trigger_run"] == 1
    assert terminated == [DEFAULT_ACCOUNT]


def test_root_installer_is_explicitly_gated_and_does_not_add_listener_or_vpn(tmp_path: Path) -> None:
    subprocess.run(["bash", "-n", str(INSTALLER)], check=True)
    rejected = subprocess.run(
        ["bash", str(INSTALLER), "WRONG_TOKEN"],
        check=False,
        capture_output=True,
        text=True,
    )
    assert rejected.returncode == 2
    source = INSTALLER.read_text(encoding="utf-8")
    for required in (
        "INSTALL_MY_DATA_HUB_MASTER_TUNNEL_BROKER",
        "useradd --system --user-group",
        "--shell /usr/sbin/nologin",
        "sshd -t",
        "my-data-hub-master-tunnel-reconcile.timer",
        "my-data-hub-master-tunnel-broker.service",
        "--allowed-uid",
        "control.sock",
        "OnUnitActiveSec=5s",
        "systemctl reload ssh.service",
        "RuntimeDirectory=$broker_runtime_directory",
        "RuntimeDirectoryMode=0750",
        'install -d -o root -g root -m 0711 "$state_root"',
        "systemctl is-active --quiet my-data-hub-master-tunnel-broker.service",
        "stat.S_ISSOCK",
    ):
        assert required in source
    assert "ExecStartPre=/usr/bin/install -d" not in source
    lowered = source.casefold()
    assert "listenaddress" not in lowered
    assert "vpn" not in lowered
    assert "postgresql://" not in lowered
    assert "pgdata" not in lowered
    assert "business" not in lowered


def test_local_ipc_dispatch_accepts_only_exact_epoch_metadata(tmp_path: Path) -> None:
    broker, _terminated, _ca = _broker(tmp_path)
    now = datetime.now(UTC)
    active = _dispatch(
        broker,
        {
            "action": "activate",
            "payload": {
                "master_instance_id": INSTANCE,
                "run_id": RUN_ID,
                "attempt_id": ATTEMPT_ID,
                "epoch": 9,
                "lease_until": (now + timedelta(minutes=5)).isoformat(),
                "listen_port": 25432,
            },
        },
    )
    assert active["epoch"] == 9
    with pytest.raises(TunnelBrokerError, match="fields"):
        _dispatch(
            broker,
            {
                "action": "deactivate",
                "payload": {
                    "master_instance_id": INSTANCE,
                    "run_id": RUN_ID,
                    "attempt_id": ATTEMPT_ID,
                    "epoch": 9,
                    "reason": "test",
                    "database_url": "forbidden",
                },
            },
        )


def test_fm11_ipc_snapshot_and_retired_denial_are_structured_and_task_bound(
    tmp_path: Path,
) -> None:
    broker, _terminated, _ca = _broker(tmp_path)
    now = datetime.now(UTC)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=1,
        lease_until=now + timedelta(minutes=5),
        listen_port=25432,
        now=now,
    )
    public_key = _key(tmp_path / "runtime", "fm11 old runtime").read_text(encoding="ascii")
    certificate = broker.issue_public_key(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=1,
        public_key=public_key,
        valid_before=now + timedelta(minutes=4),
        now=now,
    )
    snapshot = _dispatch(
        broker,
        {
            "action": "acceptance_snapshot",
            "payload": {
                "master_instance_id": INSTANCE,
                "run_id": RUN_ID,
                "attempt_id": ATTEMPT_ID,
                "epoch": 1,
            },
        },
    )
    assert snapshot["serial"] == certificate.serial
    replacement = "4a9e762d-1f71-4aec-af75-6ec36f384629"
    broker.activate(
        master_instance_id=replacement,
        run_id="run-8",
        attempt_id="attempt-3",
        epoch=2,
        lease_until=now + timedelta(minutes=5),
        listen_port=25432,
        now=now,
    )
    denial = _dispatch(
        broker,
        {
            "action": "acceptance_retired_denial",
            "payload": {
                "master_instance_id": INSTANCE,
                "run_id": RUN_ID,
                "attempt_id": ATTEMPT_ID,
                "epoch": 1,
                "certificate_serial": snapshot["serial"],
                "principal_sha256": snapshot["principal_sha256"],
                "public_key_sha256": snapshot["public_key_sha256"],
                "replacement_master_instance_id": replacement,
                "replacement_epoch": 2,
            },
        },
    )
    assert denial == {
        "lease_renewal_denied": True,
        "certificate_renewal_denied": True,
        "lease_denial_code": "MDH_RETIRED_TUNNEL_LEASE",
        "certificate_denial_code": "MDH_RETIRED_TUNNEL_CERTIFICATE",
        "certificate_serial": certificate.serial,
        "principal_sha256": snapshot["principal_sha256"],
    }


def test_worker_certificate_is_task_bound_local_forward_only_and_exactly_replayable(
    tmp_path: Path,
) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=8,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    task = "11111111-1111-4111-8111-111111111111"
    credential = "22222222-2222-4222-8222-222222222222"
    public_key = _key(tmp_path / "worker", "embedding worker").read_text(encoding="ascii")
    issued = broker.issue_worker_public_key(
        master_instance_id=INSTANCE,
        epoch=8,
        task_run_id=task,
        credential_id=credential,
        public_key=public_key,
        valid_before=NOW + timedelta(minutes=4),
        now=NOW,
    )
    replay = broker.issue_worker_public_key(
        master_instance_id=INSTANCE,
        epoch=8,
        task_run_id=task,
        credential_id=credential,
        public_key=public_key,
        valid_before=NOW + timedelta(minutes=4),
        now=NOW,
    )
    assert replay == issued
    assert issued.account == "mdh-embedding-worker"
    assert (issued.connect_host, issued.connect_port) == ("127.0.0.1", 25432)
    assert broker.worker_principals_path.read_text() == (
        f'restrict,port-forwarding,permitopen="127.0.0.1:25432" {issued.principal}\n'
    )
    state = json.loads(broker.state_path.read_text())
    assert state["worker_issued"][0]["task_run_id"] == task
    assert state["worker_issued"][0]["credential_id"] == credential
    assert public_key not in broker.state_path.read_text()
    with pytest.raises(TunnelBrokerError, match="exact replay"):
        broker.issue_worker_public_key(
            master_instance_id=INSTANCE,
            epoch=8,
            task_run_id=task,
            credential_id=credential,
            public_key=_key(tmp_path / "other", "other").read_text(),
            valid_before=NOW + timedelta(minutes=4),
            now=NOW,
        )
    assert terminated == [DEFAULT_ACCOUNT]


def test_worker_revoke_and_epoch_fence_remove_local_forward_authority(tmp_path: Path) -> None:
    broker, terminated, _ca = _broker(tmp_path)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=3,
        lease_until=NOW + timedelta(minutes=5),
        listen_port=25432,
        now=NOW,
    )
    task = "11111111-1111-4111-8111-111111111111"
    credential = "22222222-2222-4222-8222-222222222222"
    public_key = _key(tmp_path / "worker", "embedding worker").read_text()
    issued = broker.issue_worker_public_key(
        master_instance_id=INSTANCE,
        epoch=3,
        task_run_id=task,
        credential_id=credential,
        public_key=public_key,
        valid_before=NOW + timedelta(minutes=4),
        now=NOW,
    )
    with pytest.raises(TunnelBrokerError, match="not bound"):
        broker.revoke_worker_certificate(
            master_instance_id=INSTANCE,
            epoch=3,
            task_run_id=task,
            credential_id="33333333-3333-4333-8333-333333333333",
            serial=issued.serial,
            reason="task_terminal",
        )
    broker.revoke_worker_certificate(
        master_instance_id=INSTANCE,
        epoch=3,
        task_run_id=task,
        credential_id=credential,
        serial=issued.serial,
        reason="task_terminal",
    )
    assert broker.worker_principals_path.read_bytes() == b""
    assert json.loads(broker.state_path.read_text())["revoked_serials"] == [issued.serial]
    with pytest.raises(TunnelBrokerError, match="exact replay"):
        broker.issue_worker_public_key(
            master_instance_id=INSTANCE,
            epoch=3,
            task_run_id=task,
            credential_id=credential,
            public_key=public_key,
            valid_before=NOW + timedelta(minutes=4),
            now=NOW,
        )
    assert terminated[-1] == "mdh-embedding-worker"


def test_sshd_config_separates_master_remote_and_worker_local_forward_policy() -> None:
    config = render_sshd_config(
        account=DEFAULT_ACCOUNT,
        worker_account="mdh-embedding-worker",
        ca_public_key=Path("/etc/my-data-hub/tunnel-user-ca.pub"),
        principals_file=Path("/var/lib/my-data-hub/tunnel-broker/authorized_principals"),
        revoked_keys_file=Path("/var/lib/my-data-hub/tunnel-broker/revoked.krl"),
        listen_port=25432,
    )
    master, worker = config.split("Match User mdh-embedding-worker")
    assert "AllowTcpForwarding remote" in master and "PermitListen 127.0.0.1:25432" in master
    assert "PermitOpen none" in master
    assert "AllowTcpForwarding local" in worker and "PermitOpen 127.0.0.1:25432" in worker
    assert "PermitListen none" in worker and "0.0.0.0" not in config


def test_worker_ipc_dispatch_is_exact_public_metadata_only(tmp_path: Path) -> None:
    broker, _terminated, _ca = _broker(tmp_path)
    now = datetime.now(UTC)
    broker.activate(
        master_instance_id=INSTANCE,
        run_id=RUN_ID,
        attempt_id=ATTEMPT_ID,
        epoch=6,
        lease_until=now + timedelta(minutes=5),
        listen_port=25432,
        now=now,
    )
    public_key = _key(tmp_path / "ipc-worker", "ipc worker").read_text()
    payload = {
        "master_instance_id": INSTANCE,
        "epoch": 6,
        "task_run_id": "11111111-1111-4111-8111-111111111111",
        "credential_id": "22222222-2222-4222-8222-222222222222",
        "public_key": public_key,
        "valid_before": (now + timedelta(minutes=4)).isoformat(),
    }
    issued = _dispatch(broker, {"action": "issue_worker_public_key", "payload": payload})
    assert set(issued) == {
        "certificate",
        "serial",
        "principal",
        "valid_before",
        "task_run_id",
        "credential_id",
        "connect_host",
        "connect_port",
        "account",
    }
    assert "private" not in json.dumps(issued).casefold()
    replay = _dispatch(broker, {"action": "issue_worker_public_key", "payload": payload})
    assert replay == issued
    revoked = _dispatch(
        broker,
        {
            "action": "revoke_worker_certificate",
            "payload": {
                "master_instance_id": INSTANCE,
                "epoch": 6,
                "task_run_id": payload["task_run_id"],
                "credential_id": payload["credential_id"],
                "serial": issued["serial"],
                "reason": "task_terminal",
            },
        },
    )
    assert revoked == {"revoked": True}
    with pytest.raises(TunnelBrokerError, match="fields"):
        _dispatch(broker, {"action": "issue_worker_public_key", "payload": {**payload, "private_key": "forbidden"}})
