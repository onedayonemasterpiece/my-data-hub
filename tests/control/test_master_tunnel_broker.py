from __future__ import annotations

import json
import os
import subprocess
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from my_data_hub.tunnel_broker import (
    DEFAULT_ACCOUNT,
    TunnelBroker,
    TunnelBrokerError,
    render_sshd_config,
)

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
    assert terminated[-1] == DEFAULT_ACCOUNT


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
    with pytest.raises(TunnelBrokerError, match="advance"):
        broker.activate(
            master_instance_id=INSTANCE,
            run_id=RUN_ID,
            attempt_id=ATTEMPT_ID,
            epoch=2,
            lease_until=NOW + timedelta(minutes=5),
            listen_port=25432,
            now=NOW,
        )


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
        "OnUnitActiveSec=5s",
        "systemctl reload ssh.service",
    ):
        assert required in source
    lowered = source.casefold()
    assert "listenaddress" not in lowered
    assert "vpn" not in lowered
    assert "postgresql://" not in lowered
    assert "pgdata" not in lowered
    assert "business" not in lowered
