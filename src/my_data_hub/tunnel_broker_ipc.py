#!/usr/bin/env python3
"""Bounded Unix-socket bridge to the root-owned OpenSSH tunnel broker."""

from __future__ import annotations

import argparse
import json
import os
import socket
import struct
from contextlib import suppress
from datetime import UTC, datetime
from pathlib import Path
from typing import NoReturn, cast

try:  # Installed package / repository execution.
    from my_data_hub.tunnel_broker import (
        ActiveTunnelLease,
        TunnelBroker,
        TunnelBrokerError,
        TunnelCertificate,
        WorkerTunnelCertificate,
    )
except ModuleNotFoundError:  # Root installer copies both scripts beside each other.
    from tunnel_broker import (  # type: ignore[no-redef]
        ActiveTunnelLease,
        TunnelBroker,
        TunnelBrokerError,
        TunnelCertificate,
        WorkerTunnelCertificate,
    )

MAX_MESSAGE_BYTES = 32 * 1024


def _format(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise TunnelBrokerError("broker time must be timezone-aware")
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _parse(value: object) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except (TypeError, ValueError) as exc:
        raise TunnelBrokerError("broker time is invalid") from exc
    return parsed


class TunnelBrokerClient:
    """Metadata-only client used by the unprivileged control-plane container."""

    def __init__(self, socket_path: Path, *, timeout_seconds: float = 5.0) -> None:
        if not socket_path.is_absolute() or socket_path.parent.is_symlink():
            raise TunnelBrokerError("broker socket path must be absolute with a non-symlink parent")
        if not 0.1 <= timeout_seconds <= 10:
            raise TunnelBrokerError("broker timeout is outside 0.1..10 seconds")
        self.socket_path = socket_path
        self.timeout_seconds = timeout_seconds

    def _call(self, action: str, payload: dict[str, object]) -> dict[str, object]:
        encoded = (
            json.dumps({"action": action, "payload": payload}, sort_keys=True, separators=(",", ":")).encode("utf-8")
            + b"\n"
        )
        if len(encoded) > MAX_MESSAGE_BYTES:
            raise TunnelBrokerError("broker request is oversized")
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as stream:
            stream.settimeout(self.timeout_seconds)
            stream.connect(str(self.socket_path))
            stream.sendall(encoded)
            raw = bytearray()
            while not raw.endswith(b"\n"):
                chunk = stream.recv(min(4096, MAX_MESSAGE_BYTES + 1 - len(raw)))
                if not chunk:
                    break
                raw.extend(chunk)
                if len(raw) > MAX_MESSAGE_BYTES:
                    raise TunnelBrokerError("broker response is oversized")
        try:
            response = json.loads(bytes(raw))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TunnelBrokerError("broker response is malformed") from exc
        if (
            not isinstance(response, dict)
            or response.get("ok") is not True
            or not isinstance(response.get("result"), dict)
        ):
            raise TunnelBrokerError("broker denied the operation")
        return cast(dict[str, object], response["result"])

    @staticmethod
    def _identity(master_instance_id: str, run_id: str, attempt_id: str, epoch: int) -> dict[str, object]:
        return {
            "master_instance_id": master_instance_id,
            "run_id": run_id,
            "attempt_id": attempt_id,
            "epoch": epoch,
        }

    def activate(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        lease_until: datetime,
        listen_port: int,
        now: datetime,
    ) -> ActiveTunnelLease:
        _format(now)
        payload = self._identity(master_instance_id, run_id, attempt_id, epoch)
        payload.update({"lease_until": _format(lease_until), "listen_port": listen_port})
        return ActiveTunnelLease.from_json(self._call("activate", payload))

    def renew(
        self, *, master_instance_id: str, run_id: str, attempt_id: str, epoch: int, lease_until: datetime, now: datetime
    ) -> ActiveTunnelLease:
        _format(now)
        payload = self._identity(master_instance_id, run_id, attempt_id, epoch)
        payload["lease_until"] = _format(lease_until)
        return ActiveTunnelLease.from_json(self._call("renew", payload))

    def issue_public_key(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        public_key: str,
        valid_before: datetime,
        now: datetime,
    ) -> TunnelCertificate:
        _format(now)
        payload = self._identity(master_instance_id, run_id, attempt_id, epoch)
        payload.update({"public_key": public_key, "valid_before": _format(valid_before)})
        result = self._call("issue", payload)
        expected = {"certificate", "serial", "principal", "valid_before", "listen_host", "listen_port"}
        if set(result) != expected:
            raise TunnelBrokerError("broker certificate response differs from the contract")
        return TunnelCertificate(
            certificate=str(result["certificate"]),
            serial=int(cast(int, result["serial"])),
            principal=str(result["principal"]),
            valid_before=_parse(result["valid_before"]),
            listen_host=str(result["listen_host"]),
            listen_port=int(cast(int, result["listen_port"])),
        )

    def issue_worker_public_key(
        self,
        *,
        master_instance_id: str,
        epoch: int,
        task_run_id: str,
        credential_id: str,
        public_key: str,
        valid_before: datetime,
        now: datetime,
    ) -> WorkerTunnelCertificate:
        _format(now)
        result = self._call(
            "issue_worker",
            {
                "master_instance_id": master_instance_id,
                "epoch": epoch,
                "task_run_id": task_run_id,
                "credential_id": credential_id,
                "public_key": public_key,
                "valid_before": _format(valid_before),
            },
        )
        expected = {
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
        if set(result) != expected:
            raise TunnelBrokerError("worker certificate response differs from the contract")
        return WorkerTunnelCertificate(
            certificate=str(result["certificate"]),
            serial=int(cast(int, result["serial"])),
            principal=str(result["principal"]),
            valid_before=_parse(result["valid_before"]),
            task_run_id=str(result["task_run_id"]),
            credential_id=str(result["credential_id"]),
            connect_host=str(result["connect_host"]),
            connect_port=int(cast(int, result["connect_port"])),
            account=str(result["account"]),
        )

    def revoke_worker_certificate(
        self,
        *,
        master_instance_id: str,
        epoch: int,
        task_run_id: str,
        credential_id: str,
        serial: int,
        reason: str,
    ) -> None:
        result = self._call(
            "revoke_worker",
            {
                "master_instance_id": master_instance_id,
                "epoch": epoch,
                "task_run_id": task_run_id,
                "credential_id": credential_id,
                "serial": serial,
                "reason": reason,
            },
        )
        if result != {"revoked": True}:
            raise TunnelBrokerError("worker revocation response differs from the contract")

    def deactivate(self, *, master_instance_id: str, run_id: str, attempt_id: str, epoch: int, reason: str) -> None:
        payload = self._identity(master_instance_id, run_id, attempt_id, epoch)
        payload["reason"] = reason
        if self._call("deactivate", payload) != {"deactivated": True}:
            raise TunnelBrokerError("broker deactivation response differs from the contract")

    def acceptance_identity_snapshot(
        self, *, master_instance_id: str, run_id: str, attempt_id: str, epoch: int
    ) -> dict[str, object]:
        result = self._call(
            "acceptance_snapshot",
            self._identity(master_instance_id, run_id, attempt_id, epoch),
        )
        if set(result) != {"serial", "principal_sha256", "public_key_sha256"}:
            raise TunnelBrokerError("FM11 tunnel snapshot response differs from the contract")
        return result

    def acceptance_retired_denial(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        certificate_serial: int,
        principal_sha256: str,
        public_key_sha256: str,
        replacement_master_instance_id: str,
        replacement_epoch: int,
    ) -> dict[str, object]:
        payload = self._identity(master_instance_id, run_id, attempt_id, epoch)
        payload.update(
            {
                "certificate_serial": certificate_serial,
                "principal_sha256": principal_sha256,
                "public_key_sha256": public_key_sha256,
                "replacement_master_instance_id": replacement_master_instance_id,
                "replacement_epoch": replacement_epoch,
            }
        )
        result = self._call("acceptance_retired_denial", payload)
        expected = {
            "lease_renewal_denied",
            "certificate_renewal_denied",
            "lease_denial_code",
            "certificate_denial_code",
            "certificate_serial",
            "principal_sha256",
        }
        if set(result) != expected:
            raise TunnelBrokerError("FM11 retired tunnel response differs from the contract")
        return result


def _exact(value: object, fields: set[str]) -> dict[str, object]:
    if not isinstance(value, dict) or set(value) != fields:
        raise TunnelBrokerError("broker request fields differ from the contract")
    return cast(dict[str, object], value)


def _dispatch(broker: TunnelBroker, request: object) -> dict[str, object]:
    envelope = _exact(request, {"action", "payload"})
    action = envelope["action"]
    base = {"master_instance_id", "run_id", "attempt_id", "epoch"}
    if action == "activate":
        payload = _exact(envelope["payload"], base | {"lease_until", "listen_port"})
        return broker.activate(
            master_instance_id=str(payload["master_instance_id"]),
            run_id=str(payload["run_id"]),
            attempt_id=str(payload["attempt_id"]),
            epoch=int(cast(int, payload["epoch"])),
            lease_until=_parse(payload["lease_until"]),
            listen_port=int(cast(int, payload["listen_port"])),
            now=datetime.now(UTC),
        ).to_json()
    if action == "renew":
        payload = _exact(envelope["payload"], base | {"lease_until"})
        return broker.renew(
            master_instance_id=str(payload["master_instance_id"]),
            run_id=str(payload["run_id"]),
            attempt_id=str(payload["attempt_id"]),
            epoch=int(cast(int, payload["epoch"])),
            lease_until=_parse(payload["lease_until"]),
            now=datetime.now(UTC),
        ).to_json()
    if action == "issue":
        payload = _exact(envelope["payload"], base | {"public_key", "valid_before"})
        return broker.issue_public_key(
            master_instance_id=str(payload["master_instance_id"]),
            run_id=str(payload["run_id"]),
            attempt_id=str(payload["attempt_id"]),
            epoch=int(cast(int, payload["epoch"])),
            public_key=str(payload["public_key"]),
            valid_before=_parse(payload["valid_before"]),
            now=datetime.now(UTC),
        ).public_response()
    if action == "issue_worker":
        payload = _exact(
            envelope["payload"],
            {
                "master_instance_id",
                "epoch",
                "task_run_id",
                "credential_id",
                "public_key",
                "valid_before",
            },
        )
        return broker.issue_worker_public_key(
            master_instance_id=str(payload["master_instance_id"]),
            epoch=int(cast(int, payload["epoch"])),
            task_run_id=str(payload["task_run_id"]),
            credential_id=str(payload["credential_id"]),
            public_key=str(payload["public_key"]),
            valid_before=_parse(payload["valid_before"]),
            now=datetime.now(UTC),
        ).public_response()
    if action == "revoke_worker":
        payload = _exact(
            envelope["payload"],
            {
                "master_instance_id",
                "epoch",
                "task_run_id",
                "credential_id",
                "serial",
                "reason",
            },
        )
        broker.revoke_worker_certificate(
            master_instance_id=str(payload["master_instance_id"]),
            epoch=int(cast(int, payload["epoch"])),
            task_run_id=str(payload["task_run_id"]),
            credential_id=str(payload["credential_id"]),
            serial=int(cast(int, payload["serial"])),
            reason=str(payload["reason"]),
        )
        return {"revoked": True}
    if action == "deactivate":
        payload = _exact(envelope["payload"], base | {"reason"})
        broker.deactivate(
            master_instance_id=str(payload["master_instance_id"]),
            run_id=str(payload["run_id"]),
            attempt_id=str(payload["attempt_id"]),
            epoch=int(cast(int, payload["epoch"])),
            reason=str(payload["reason"]),
        )
        return {"deactivated": True}
    if action == "acceptance_snapshot":
        payload = _exact(envelope["payload"], base)
        return broker.acceptance_identity_snapshot(
            master_instance_id=str(payload["master_instance_id"]),
            run_id=str(payload["run_id"]),
            attempt_id=str(payload["attempt_id"]),
            epoch=int(cast(int, payload["epoch"])),
            now=datetime.now(UTC),
        )
    if action == "acceptance_retired_denial":
        payload = _exact(
            envelope["payload"],
            base
            | {
                "certificate_serial",
                "principal_sha256",
                "public_key_sha256",
                "replacement_master_instance_id",
                "replacement_epoch",
            },
        )
        return broker.acceptance_retired_denial(
            master_instance_id=str(payload["master_instance_id"]),
            run_id=str(payload["run_id"]),
            attempt_id=str(payload["attempt_id"]),
            epoch=int(cast(int, payload["epoch"])),
            certificate_serial=int(cast(int, payload["certificate_serial"])),
            principal_sha256=str(payload["principal_sha256"]),
            public_key_sha256=str(payload["public_key_sha256"]),
            replacement_master_instance_id=str(payload["replacement_master_instance_id"]),
            replacement_epoch=int(cast(int, payload["replacement_epoch"])),
            now=datetime.now(UTC),
        )
    raise TunnelBrokerError("broker action is not allowlisted")


def serve(broker: TunnelBroker, *, socket_path: Path, allowed_uid: int, socket_gid: int) -> NoReturn:
    if not socket_path.is_absolute() or socket_path.parent.is_symlink():
        raise TunnelBrokerError("broker socket path must be absolute with a non-symlink parent")
    socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o750)
    if socket_path.exists() or socket_path.is_symlink():
        raise TunnelBrokerError("refusing to replace an existing broker socket")
    server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        server.bind(str(socket_path))
        os.chown(socket_path, 0, socket_gid)
        os.chmod(socket_path, 0o660)
        server.listen(8)
        while True:
            connection, _ = server.accept()
            with connection:
                connection.settimeout(5)
                credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, struct.calcsize("3i"))
                _pid, peer_uid, _peer_gid = struct.unpack("3i", credentials)
                if peer_uid not in {0, allowed_uid}:
                    continue
                raw = bytearray()
                try:
                    while not raw.endswith(b"\n") and len(raw) <= MAX_MESSAGE_BYTES:
                        chunk = connection.recv(min(4096, MAX_MESSAGE_BYTES + 1 - len(raw)))
                        if not chunk:
                            break
                        raw.extend(chunk)
                    if not raw.endswith(b"\n") or len(raw) > MAX_MESSAGE_BYTES:
                        raise TunnelBrokerError("broker request is incomplete or oversized")
                    result = _dispatch(broker, json.loads(bytes(raw)))
                    response = {"ok": True, "result": result}
                except Exception:
                    response = {"ok": False, "result": {"code": "denied"}}
                with suppress(OSError):
                    connection.sendall(
                        json.dumps(response, sort_keys=True, separators=(",", ":")).encode("utf-8") + b"\n"
                    )
    finally:
        server.close()
        with suppress(FileNotFoundError):
            socket_path.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description="local metadata-only master tunnel broker IPC")
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--ca-private-key", required=True)
    parser.add_argument("--account", required=True)
    parser.add_argument("--worker-account", required=True)
    parser.add_argument("--socket", required=True)
    parser.add_argument("--allowed-uid", required=True, type=int)
    parser.add_argument("--socket-gid", required=True, type=int)
    args = parser.parse_args()
    if not 1 <= args.allowed_uid <= 2_147_483_647 or not 1 <= args.socket_gid <= 2_147_483_647:
        raise SystemExit("broker allowed UID/GID must be non-root")
    broker = TunnelBroker(
        Path(args.state_root),
        ca_private_key=Path(args.ca_private_key),
        account=args.account,
        worker_account=args.worker_account,
    )
    serve(
        broker,
        socket_path=Path(args.socket),
        allowed_uid=args.allowed_uid,
        socket_gid=args.socket_gid,
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
