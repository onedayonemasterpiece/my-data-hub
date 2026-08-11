#!/usr/bin/env python3
"""Fail-closed OpenSSH certificate broker for the ACTIVE master tunnel.

The broker stores only tunnel authorization metadata and public SSH material.  It
never accepts a PostgreSQL URL, database credential, payload, checkpoint, or
business row.  OpenSSH remains the data-plane listener and enforces the exact
loopback ``PermitListen`` destination rendered by :func:`render_sshd_config`.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import tempfile
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, NoReturn, cast
from uuid import UUID

SCHEMA_VERSION = "my-data-hub-master-tunnel-broker.v1"
DEFAULT_ACCOUNT = "mdh-master-tunnel"
DEFAULT_LISTEN_HOST = "127.0.0.1"
DEFAULT_LISTEN_PORT = 25432
MAX_LEASE = timedelta(minutes=10)
MIN_CERTIFICATE_LIFETIME = timedelta(seconds=15)
MAX_ISSUED_CERTIFICATES = 4096
MAX_BROKER_REQUEST_BYTES = 32 * 1024
_ACCOUNT = re.compile(r"^[a-z_][a-z0-9_-]{0,30}$")
_REASON = re.compile(r"^[a-z][a-z0-9_.-]{0,63}$")
_RUNTIME_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$")
_PUBLIC_KEY_TYPES = frozenset({"ssh-ed25519"})


class TunnelBrokerError(RuntimeError):
    """Tunnel authorization could not be changed safely."""


def _utc(value: datetime, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{label} must be timezone-aware")
    return value.astimezone(UTC)


def _parse_time(value: str, label: str) -> datetime:
    try:
        return _utc(datetime.fromisoformat(value.replace("Z", "+00:00")), label)
    except ValueError as exc:
        raise TunnelBrokerError(f"{label} is not an ISO-8601 UTC time") from exc


def _format_time(value: datetime) -> str:
    return _utc(value, "time").isoformat().replace("+00:00", "Z")


def _certificate_time(value: datetime) -> str:
    return _utc(value, "certificate time").strftime("%Y%m%d%H%M%SZ")


def _validate_identity(instance_id: str, epoch: int) -> tuple[str, int]:
    try:
        normalized = str(UUID(instance_id))
    except ValueError as exc:
        raise TunnelBrokerError("master_instance_id must be a UUID") from exc
    if isinstance(epoch, bool) or epoch < 1:
        raise TunnelBrokerError("epoch must be a positive integer")
    return normalized, epoch


def _validate_runtime_ids(run_id: str, attempt_id: str) -> tuple[str, str]:
    if not _RUNTIME_ID.fullmatch(run_id) or not _RUNTIME_ID.fullmatch(attempt_id):
        raise TunnelBrokerError("run_id and attempt_id must be bounded opaque identifiers")
    return run_id, attempt_id


def _runtime_digest(run_id: str, attempt_id: str) -> str:
    return hashlib.sha256(f"{run_id}\0{attempt_id}".encode()).hexdigest()[:12]


def _principal(instance_id: str, epoch: int, run_id: str, attempt_id: str) -> str:
    compact = UUID(instance_id).hex
    return f"mdh-master-e{epoch}-{compact}-{_runtime_digest(run_id, attempt_id)}"


def _regular_file(path: Path, label: str, *, private: bool = False) -> None:
    if path.is_symlink() or not path.is_file():
        raise TunnelBrokerError(f"{label} must be a regular non-symlink file")
    if private and path.stat().st_mode & 0o077:
        raise TunnelBrokerError(f"{label} must not be group/world accessible")


def _atomic_write(path: Path, content: bytes, mode: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    if path.is_symlink():
        raise TunnelBrokerError(f"refusing to replace symbolic link: {path}")
    descriptor, raw_tmp = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary = Path(raw_tmp)
    try:
        os.fchmod(descriptor, mode)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(content)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if temporary.exists():
            temporary.unlink()


@dataclass(frozen=True, slots=True)
class ActiveTunnelLease:
    master_instance_id: str
    run_id: str
    attempt_id: str
    epoch: int
    lease_until: datetime
    listen_host: str
    listen_port: int
    principal: str

    @classmethod
    def create(
        cls,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        lease_until: datetime,
        listen_host: str,
        listen_port: int,
    ) -> ActiveTunnelLease:
        instance, normalized_epoch = _validate_identity(master_instance_id, epoch)
        normalized_run, normalized_attempt = _validate_runtime_ids(run_id, attempt_id)
        if listen_host != DEFAULT_LISTEN_HOST:
            raise TunnelBrokerError("master tunnel listen host must be exact IPv4 loopback")
        if isinstance(listen_port, bool) or not 1024 <= listen_port <= 65535:
            raise TunnelBrokerError("master tunnel listen port must be within 1024..65535")
        return cls(
            master_instance_id=instance,
            run_id=normalized_run,
            attempt_id=normalized_attempt,
            epoch=normalized_epoch,
            lease_until=_utc(lease_until, "lease_until"),
            listen_host=listen_host,
            listen_port=listen_port,
            principal=_principal(instance, normalized_epoch, normalized_run, normalized_attempt),
        )

    @classmethod
    def from_json(cls, value: object) -> ActiveTunnelLease:
        required = {
            "master_instance_id",
            "run_id",
            "attempt_id",
            "epoch",
            "lease_until",
            "listen_host",
            "listen_port",
            "principal",
        }
        if not isinstance(value, dict) or set(value) != required:
            raise TunnelBrokerError("active tunnel lease fields differ from the contract")
        if (
            not isinstance(value["master_instance_id"], str)
            or not isinstance(value["run_id"], str)
            or not isinstance(value["attempt_id"], str)
            or not isinstance(value["epoch"], int)
            or isinstance(value["epoch"], bool)
            or not isinstance(value["listen_port"], int)
            or isinstance(value["listen_port"], bool)
            or not isinstance(value["lease_until"], str)
            or not isinstance(value["listen_host"], str)
            or not isinstance(value["principal"], str)
        ):
            raise TunnelBrokerError("active tunnel lease values are invalid")
        lease = cls.create(
            master_instance_id=value["master_instance_id"],
            run_id=value["run_id"],
            attempt_id=value["attempt_id"],
            epoch=value["epoch"],
            lease_until=_parse_time(value["lease_until"], "lease_until"),
            listen_host=value["listen_host"],
            listen_port=value["listen_port"],
        )
        if value["principal"] != lease.principal:
            raise TunnelBrokerError("active tunnel principal does not match its epoch identity")
        return lease

    def to_json(self) -> dict[str, object]:
        return {
            "master_instance_id": self.master_instance_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "epoch": self.epoch,
            "lease_until": _format_time(self.lease_until),
            "listen_host": self.listen_host,
            "listen_port": self.listen_port,
            "principal": self.principal,
        }


@dataclass(frozen=True, slots=True)
class TunnelCertificate:
    certificate: str
    serial: int
    principal: str
    valid_before: datetime
    listen_host: str
    listen_port: int

    def public_response(self) -> dict[str, object]:
        return {
            "certificate": self.certificate,
            "serial": self.serial,
            "principal": self.principal,
            "valid_before": _format_time(self.valid_before),
            "listen_host": self.listen_host,
            "listen_port": self.listen_port,
        }


@dataclass(slots=True)
class BrokerState:
    highest_epoch: int
    next_serial: int
    active: ActiveTunnelLease | None
    issued: list[dict[str, object]]
    revoked_serials: list[int]

    @classmethod
    def empty(cls) -> BrokerState:
        return cls(highest_epoch=0, next_serial=1, active=None, issued=[], revoked_serials=[])

    @classmethod
    def from_json(cls, value: object) -> BrokerState:
        required = {
            "schema_version",
            "highest_epoch",
            "next_serial",
            "active",
            "issued",
            "revoked_serials",
        }
        if not isinstance(value, dict) or set(value) != required or value.get("schema_version") != SCHEMA_VERSION:
            raise TunnelBrokerError("tunnel broker state fields differ from the contract")
        highest_epoch = value["highest_epoch"]
        next_serial = value["next_serial"]
        issued = value["issued"]
        revoked = value["revoked_serials"]
        if (
            not isinstance(highest_epoch, int)
            or isinstance(highest_epoch, bool)
            or highest_epoch < 0
            or not isinstance(next_serial, int)
            or isinstance(next_serial, bool)
            or not 1 <= next_serial < 2**63
            or not isinstance(issued, list)
            or len(issued) > MAX_ISSUED_CERTIFICATES
            or not isinstance(revoked, list)
            or any(not isinstance(item, int) or isinstance(item, bool) or item < 1 for item in revoked)
            or revoked != sorted(set(revoked))
        ):
            raise TunnelBrokerError("tunnel broker counters or revocations are invalid")
        parsed_issued: list[dict[str, object]] = []
        issued_required = {
            "serial",
            "master_instance_id",
            "run_id",
            "attempt_id",
            "epoch",
            "key_id",
            "principal",
            "valid_before",
            "public_key_sha256",
        }
        seen_serials: set[int] = set()
        for item in issued:
            if not isinstance(item, dict) or set(item) != issued_required:
                raise TunnelBrokerError("issued certificate metadata differs from the contract")
            serial = item["serial"]
            if (
                not isinstance(item["master_instance_id"], str)
                or not isinstance(item["run_id"], str)
                or not isinstance(item["attempt_id"], str)
                or not isinstance(item["epoch"], int)
                or isinstance(item["epoch"], bool)
                or not isinstance(serial, int)
                or isinstance(serial, bool)
                or serial < 1
                or serial in seen_serials
                or not isinstance(item["key_id"], str)
                or not isinstance(item["principal"], str)
                or not isinstance(item["valid_before"], str)
                or not isinstance(item["public_key_sha256"], str)
                or not re.fullmatch(r"[a-f0-9]{64}", str(item["public_key_sha256"]))
            ):
                raise TunnelBrokerError("issued certificate metadata is invalid")
            instance, epoch = _validate_identity(item["master_instance_id"], item["epoch"])
            run_id, attempt_id = _validate_runtime_ids(item["run_id"], item["attempt_id"])
            if (
                item["principal"] != _principal(instance, epoch, run_id, attempt_id)
                or item["key_id"]
                != f"mdh:{instance}:{epoch}:{_runtime_digest(run_id, attempt_id)}:{serial}"
            ):
                raise TunnelBrokerError("issued certificate identity is invalid")
            _parse_time(item["valid_before"], "valid_before")
            seen_serials.add(serial)
            parsed_issued.append(dict(item))
        # Revoked serials are intentionally also present in issued metadata.
        if seen_serials and max(seen_serials) >= next_serial:
            raise TunnelBrokerError("issued certificate serial exceeds the durable counter")
        if any(serial >= next_serial for serial in revoked):
            raise TunnelBrokerError("revocation serial exceeds the durable counter")
        active = ActiveTunnelLease.from_json(value["active"]) if value["active"] is not None else None
        if active is not None and (active.epoch != highest_epoch or active.epoch < 1):
            raise TunnelBrokerError("active tunnel epoch is not the highest durable epoch")
        return cls(highest_epoch, next_serial, active, parsed_issued, list(revoked))

    def to_json(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "highest_epoch": self.highest_epoch,
            "next_serial": self.next_serial,
            "active": self.active.to_json() if self.active else None,
            "issued": self.issued,
            "revoked_serials": self.revoked_serials,
        }


class TunnelBroker:
    """Serialize lifecycle changes and maintain OpenSSH fail-closed artifacts."""

    def __init__(
        self,
        root: Path,
        *,
        ca_private_key: Path,
        account: str = DEFAULT_ACCOUNT,
        command_runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
        session_terminator: Callable[[str], None] | None = None,
    ) -> None:
        if not root.is_absolute() or root.is_symlink():
            raise TunnelBrokerError("broker root must be an absolute non-symlink path")
        if not _ACCOUNT.fullmatch(account):
            raise TunnelBrokerError("tunnel account name is invalid")
        self.root = root
        self.ca_private_key = ca_private_key
        self.ca_public_key = Path(f"{ca_private_key}.pub")
        self.account = account
        self.command_runner = command_runner
        self.session_terminator = session_terminator or terminate_account_sessions
        self.state_path = root / "state.json"
        self.principals_path = root / "authorized_principals"
        self.krl_path = root / "revoked.krl"
        self.lock_path = root / "broker.lock"

    def _run(self, arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
        completed = self.command_runner(
            list(arguments), check=False, capture_output=True, text=True, stdin=subprocess.DEVNULL
        )
        if completed.returncode != 0:
            raise TunnelBrokerError(f"OpenSSH command failed with status {completed.returncode}")
        return completed

    def _lock(self) -> Any:
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.root.is_symlink() or not self.root.is_dir():
            raise TunnelBrokerError("broker root must remain a non-symlink directory")
        os.chmod(self.root, 0o700)
        stream = self.lock_path.open("a+b")
        os.chmod(self.lock_path, 0o600)
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX)
        return stream

    def _load(self) -> BrokerState:
        _regular_file(self.state_path, "broker state", private=True)
        if self.state_path.stat().st_size > 2 * 1024 * 1024:
            raise TunnelBrokerError("tunnel broker state is oversized")
        try:
            raw = json.loads(self.state_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise TunnelBrokerError("tunnel broker state is unreadable") from exc
        return BrokerState.from_json(raw)

    def _save(self, state: BrokerState) -> None:
        payload = json.dumps(state.to_json(), sort_keys=True, separators=(",", ":")).encode() + b"\n"
        _atomic_write(self.state_path, payload, 0o600)

    def _write_principal(self, active: ActiveTunnelLease | None) -> None:
        content = b"" if active is None else f"{active.principal}\n".encode()
        _atomic_write(self.principals_path, content, 0o644)

    def _write_krl(self, revoked_serials: Sequence[int], *, revoke_ca: bool = False) -> None:
        _regular_file(self.ca_public_key, "tunnel CA public key")
        with tempfile.TemporaryDirectory(prefix=".krl.", dir=self.root) as raw_tmp:
            temporary_root = Path(raw_tmp)
            output = temporary_root / "revoked.krl"
            if revoke_ca:
                self._run(["ssh-keygen", "-q", "-k", "-f", str(output), str(self.ca_public_key)])
            else:
                spec = temporary_root / "revocations"
                spec.write_text("".join(f"serial: {serial}\n" for serial in revoked_serials), encoding="ascii")
                self._run(
                    [
                        "ssh-keygen",
                        "-q",
                        "-k",
                        "-f",
                        str(output),
                        "-s",
                        str(self.ca_public_key),
                        str(spec),
                    ]
                )
            _atomic_write(self.krl_path, output.read_bytes(), 0o644)

    def _fail_closed(self, state: BrokerState | None = None) -> None:
        first_failure: Exception | None = None
        try:
            self._write_principal(None)
        except Exception as exc:  # continue to independent KRL/session denial
            first_failure = exc
        if state is not None:
            try:
                state.active = None
                state.revoked_serials = sorted(
                    {*state.revoked_serials, *(cast(int, item["serial"]) for item in state.issued)}
                )
                self._save(state)
            except Exception as exc:
                first_failure = first_failure or exc
        try:
            self._write_krl([], revoke_ca=True)
        except Exception as exc:
            first_failure = first_failure or exc
        try:
            self.session_terminator(self.account)
        except Exception as exc:
            first_failure = first_failure or exc
        if first_failure is not None:
            raise TunnelBrokerError("one or more fail-closed tunnel denial actions failed") from first_failure

    def _load_or_fail_closed(self) -> BrokerState:
        try:
            return self._load()
        except Exception:
            self._fail_closed()
            raise

    def initialize(self) -> None:
        _regular_file(self.ca_private_key, "tunnel CA private key", private=True)
        _regular_file(self.ca_public_key, "tunnel CA public key")
        with self._lock():
            if self.state_path.exists() or self.principals_path.exists() or self.krl_path.exists():
                raise TunnelBrokerError("refusing to replace existing tunnel broker state")
            state = BrokerState.empty()
            self._save(state)
            self._write_principal(None)
            self._write_krl([])

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
        observed = _utc(now, "now")
        active = ActiveTunnelLease.create(
            master_instance_id=master_instance_id,
            run_id=run_id,
            attempt_id=attempt_id,
            epoch=epoch,
            lease_until=lease_until,
            listen_host=DEFAULT_LISTEN_HOST,
            listen_port=listen_port,
        )
        if active.lease_until <= observed or active.lease_until - observed > MAX_LEASE:
            raise TunnelBrokerError("tunnel lease must be positive and no more than 10 minutes")
        with self._lock():
            state = self._load_or_fail_closed()
            if state.active == active:
                self._write_krl(state.revoked_serials)
                self._write_principal(active)
                return active
            if active.epoch <= state.highest_epoch:
                raise TunnelBrokerError("activation epoch must advance the durable high-water mark")
            try:
                self._write_principal(None)
                self.session_terminator(self.account)
                superseded = [
                    cast(int, item["serial"])
                    for item in state.issued
                    if cast(int, item["serial"]) not in state.revoked_serials
                ]
                state.revoked_serials = sorted({*state.revoked_serials, *superseded})
                self._write_krl(state.revoked_serials)
                state.highest_epoch = active.epoch
                state.active = active
                self._save(state)
                self._write_principal(active)
                return active
            except Exception:
                self._fail_closed(state)
                raise

    def renew(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        lease_until: datetime,
        now: datetime,
    ) -> ActiveTunnelLease:
        instance, epoch = _validate_identity(master_instance_id, epoch)
        run_id, attempt_id = _validate_runtime_ids(run_id, attempt_id)
        observed = _utc(now, "now")
        requested = _utc(lease_until, "lease_until")
        with self._lock():
            state = self._load_or_fail_closed()
            active = state.active
            if (
                active is None
                or active.master_instance_id != instance
                or active.run_id != run_id
                or active.attempt_id != attempt_id
                or active.epoch != epoch
                or active.lease_until <= observed
            ):
                raise TunnelBrokerError("only the current unexpired tunnel epoch may renew")
            if requested <= active.lease_until:
                self._write_krl(state.revoked_serials)
                self._write_principal(active)
                return active
            if requested - observed > MAX_LEASE:
                raise TunnelBrokerError("renewed lease must advance within the 10 minute bound")
            renewed = ActiveTunnelLease.create(
                master_instance_id=instance,
                run_id=run_id,
                attempt_id=attempt_id,
                epoch=epoch,
                lease_until=requested,
                listen_host=active.listen_host,
                listen_port=active.listen_port,
            )
            try:
                state.active = renewed
                self._save(state)
                self._write_principal(renewed)
                return renewed
            except Exception:
                self._fail_closed(state)
                raise

    def issue(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        public_key: Path,
        certificate_output: Path,
        valid_before: datetime,
        now: datetime,
    ) -> int:
        _regular_file(public_key, "ephemeral tunnel public key")
        if public_key.stat().st_size > 16 * 1024:
            raise TunnelBrokerError("ephemeral tunnel public key is oversized")
        if certificate_output.exists() or certificate_output.is_symlink():
            raise TunnelBrokerError("certificate output must not already exist")
        if not certificate_output.parent.is_dir() or certificate_output.parent.is_symlink():
            raise TunnelBrokerError("certificate output parent must be a non-symlink directory")
        try:
            key_text = public_key.read_text(encoding="ascii")
        except (OSError, UnicodeError) as exc:
            raise TunnelBrokerError("ephemeral tunnel public key is unreadable") from exc
        certificate = self.issue_public_key(
            master_instance_id=master_instance_id,
            run_id=run_id,
            attempt_id=attempt_id,
            epoch=epoch,
            public_key=key_text,
            valid_before=valid_before,
            now=now,
        )
        _atomic_write(certificate_output, certificate.certificate.encode("ascii"), 0o600)
        return certificate.serial

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
        """Sign one public key; suitable for the authenticated control route.

        ``public_key`` and the returned OpenSSH certificate are public material.
        A private key is neither accepted nor representable by this interface.
        """

        instance, epoch = _validate_identity(master_instance_id, epoch)
        run_id, attempt_id = _validate_runtime_ids(run_id, attempt_id)
        observed = _utc(now, "now")
        expiry = _utc(valid_before, "valid_before")
        _regular_file(self.ca_private_key, "tunnel CA private key", private=True)
        _regular_file(self.ca_public_key, "tunnel CA public key")
        if len(public_key.encode("utf-8")) > 16 * 1024:
            raise TunnelBrokerError("ephemeral tunnel public key is oversized")
        try:
            public_key.encode("ascii")
        except UnicodeEncodeError as exc:
            raise TunnelBrokerError("ephemeral tunnel public key must be ASCII") from exc
        key_text = public_key.strip()
        fields = key_text.split()
        if len(fields) < 2 or fields[0] not in _PUBLIC_KEY_TYPES or "\n" in key_text:
            raise TunnelBrokerError("only one Ed25519 public key may be certified")
        with self._lock():
            state = self._load_or_fail_closed()
            active = state.active
            if active is None and epoch <= state.highest_epoch:
                self._write_principal(None)
                self._write_krl(state.revoked_serials)
                self.session_terminator(self.account)
                return
            if (
                active is None
                or active.master_instance_id != instance
                or active.run_id != run_id
                or active.attempt_id != attempt_id
                or active.epoch != epoch
                or active.lease_until <= observed
            ):
                raise TunnelBrokerError("certificate request does not match the current unexpired epoch")
            if (
                expiry - observed < MIN_CERTIFICATE_LIFETIME
                or expiry > active.lease_until
                or expiry - observed > MAX_LEASE
            ):
                raise TunnelBrokerError("certificate validity must be 15 seconds..10 minutes within the lease")
            if len(state.issued) >= MAX_ISSUED_CERTIFICATES:
                raise TunnelBrokerError("tunnel certificate metadata limit reached")
            try:
                serial = state.next_serial
                state.next_serial += 1
                digest = hashlib.sha256(key_text.encode("ascii")).hexdigest()
                key_id = f"mdh:{instance}:{epoch}:{_runtime_digest(run_id, attempt_id)}:{serial}"
                state.issued.append(
                    {
                        "serial": serial,
                        "master_instance_id": instance,
                        "run_id": run_id,
                        "attempt_id": attempt_id,
                        "epoch": epoch,
                        "key_id": key_id,
                        "principal": active.principal,
                        "valid_before": _format_time(expiry),
                        "public_key_sha256": digest,
                    }
                )
                # Reserve and persist the serial before signing.  A failure may
                # consume a serial, but can never create an untracked certificate.
                self._save(state)
                with tempfile.TemporaryDirectory(prefix=".certificate.", dir=self.root) as raw_tmp:
                    temporary_root = Path(raw_tmp)
                    signing_key = temporary_root / "ephemeral.pub"
                    signing_key.write_text(key_text + "\n", encoding="ascii")
                    os.chmod(signing_key, 0o600)
                    self._run(["ssh-keygen", "-l", "-f", str(signing_key)])
                    self._run(
                        [
                            "ssh-keygen",
                            "-q",
                            "-s",
                            str(self.ca_private_key),
                            "-I",
                            key_id,
                            "-z",
                            str(serial),
                            "-n",
                            active.principal,
                            "-V",
                            f"{_certificate_time(observed - timedelta(seconds=5))}:{_certificate_time(expiry)}",
                            "-O",
                            "clear",
                            "-O",
                            "permit-port-forwarding",
                            str(signing_key),
                        ]
                    )
                    generated = temporary_root / "ephemeral-cert.pub"
                    _regular_file(generated, "issued tunnel certificate")
                    certificate = generated.read_text(encoding="ascii").strip()
                return TunnelCertificate(
                    certificate=certificate,
                    serial=serial,
                    principal=active.principal,
                    valid_before=expiry,
                    listen_host=active.listen_host,
                    listen_port=active.listen_port,
                )
            except Exception:
                self._fail_closed(state)
                raise

    def revoke(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        serial: int,
        reason: str,
    ) -> None:
        instance, epoch = _validate_identity(master_instance_id, epoch)
        run_id, attempt_id = _validate_runtime_ids(run_id, attempt_id)
        if isinstance(serial, bool) or serial < 1 or not _REASON.fullmatch(reason):
            raise TunnelBrokerError("revocation serial or reason is invalid")
        with self._lock():
            state = self._load_or_fail_closed()
            match = next(
                (
                    item
                    for item in state.issued
                    if item["serial"] == serial
                    and item["master_instance_id"] == instance
                    and item["run_id"] == run_id
                    and item["attempt_id"] == attempt_id
                    and item["epoch"] == epoch
                ),
                None,
            )
            if match is None:
                raise TunnelBrokerError("certificate serial is not bound to the requested epoch")
            try:
                state.revoked_serials = sorted({*state.revoked_serials, serial})
                self._write_krl(state.revoked_serials)
                self._save(state)
                self.session_terminator(self.account)
            except Exception:
                self._fail_closed(state)
                raise

    def deactivate(
        self,
        *,
        master_instance_id: str,
        run_id: str,
        attempt_id: str,
        epoch: int,
        reason: str,
    ) -> None:
        instance, epoch = _validate_identity(master_instance_id, epoch)
        run_id, attempt_id = _validate_runtime_ids(run_id, attempt_id)
        if not _REASON.fullmatch(reason):
            raise TunnelBrokerError("deactivation reason is invalid")
        with self._lock():
            state = self._load_or_fail_closed()
            active = state.active
            if (
                active is None
                or active.master_instance_id != instance
                or active.run_id != run_id
                or active.attempt_id != attempt_id
                or active.epoch != epoch
            ):
                raise TunnelBrokerError("deactivation does not match the current tunnel epoch")
            try:
                self._write_principal(None)
                state.revoked_serials = sorted(
                    {
                        *state.revoked_serials,
                        *(
                            cast(int, item["serial"])
                            for item in state.issued
                            if item["master_instance_id"] == instance
                            and item["run_id"] == run_id
                            and item["attempt_id"] == attempt_id
                            and item["epoch"] == epoch
                        ),
                    }
                )
                self._write_krl(state.revoked_serials)
                state.active = None
                self._save(state)
                self.session_terminator(self.account)
            except Exception:
                self._fail_closed(state)
                raise

    def reconcile(self, *, now: datetime) -> bool:
        observed = _utc(now, "now")
        with self._lock():
            state = self._load_or_fail_closed()
            try:
                active = state.active
                if active is None or active.lease_until <= observed:
                    self._write_principal(None)
                    if active is not None:
                        state.revoked_serials = sorted(
                            {
                                *state.revoked_serials,
                                *(
                                    cast(int, item["serial"])
                                    for item in state.issued
                                    if item["master_instance_id"] == active.master_instance_id
                                    and item["run_id"] == active.run_id
                                    and item["attempt_id"] == active.attempt_id
                                    and item["epoch"] == active.epoch
                                ),
                            }
                        )
                        state.active = None
                        self._save(state)
                    self._write_krl(state.revoked_serials)
                    self.session_terminator(self.account)
                    return False
                self._write_krl(state.revoked_serials)
                self._write_principal(active)
                return True
            except Exception:
                self._fail_closed(state)
                raise


def terminate_account_sessions(account: str) -> None:
    """Terminate only sshd children owned by the dedicated tunnel account."""

    completed = subprocess.run(
        ["pkill", "-TERM", "-u", account, "-x", "sshd"],
        check=False,
        capture_output=True,
        text=True,
        stdin=subprocess.DEVNULL,
    )
    if completed.returncode not in {0, 1}:
        raise TunnelBrokerError("could not terminate dedicated tunnel account sessions")


def render_sshd_config(
    *,
    account: str,
    ca_public_key: Path,
    principals_file: Path,
    revoked_keys_file: Path,
    listen_port: int,
) -> str:
    if not _ACCOUNT.fullmatch(account):
        raise TunnelBrokerError("tunnel account name is invalid")
    if isinstance(listen_port, bool) or not 1024 <= listen_port <= 65535:
        raise TunnelBrokerError("master tunnel listen port must be within 1024..65535")
    for path in (ca_public_key, principals_file, revoked_keys_file):
        if not path.is_absolute() or any(character.isspace() for character in str(path)):
            raise TunnelBrokerError("OpenSSH broker paths must be absolute and whitespace-free")
    return f"""# Managed by my-data-hub master tunnel broker. Do not broaden.
Match User {account}
    AuthenticationMethods publickey
    PubkeyAuthentication yes
    PasswordAuthentication no
    KbdInteractiveAuthentication no
    GSSAPIAuthentication no
    HostbasedAuthentication no
    AuthorizedKeysFile none
    TrustedUserCAKeys {ca_public_key}
    AuthorizedPrincipalsFile {principals_file}
    RevokedKeys {revoked_keys_file}
    AllowTcpForwarding remote
    PermitListen {DEFAULT_LISTEN_HOST}:{listen_port}
    PermitOpen none
    GatewayPorts no
    PermitTTY no
    X11Forwarding no
    AllowAgentForwarding no
    PermitTunnel no
    PermitUserRC no
    MaxSessions 0
Match all
"""


def _broker(arguments: argparse.Namespace) -> TunnelBroker:
    return TunnelBroker(
        Path(arguments.state_root),
        ca_private_key=Path(arguments.ca_private_key),
        account=arguments.account,
    )


def _now() -> datetime:
    return datetime.now(UTC)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="epoch-bound OpenSSH master tunnel broker")
    parser.add_argument("--state-root", required=True)
    parser.add_argument("--ca-private-key", required=True)
    parser.add_argument("--account", default=DEFAULT_ACCOUNT)
    subparsers = parser.add_subparsers(dest="action", required=True)
    subparsers.add_parser("initialize")

    def identity(command: argparse.ArgumentParser) -> None:
        command.add_argument("--master-instance-id", required=True)
        command.add_argument("--run-id", required=True)
        command.add_argument("--attempt-id", required=True)
        command.add_argument("--epoch", required=True, type=int)

    activate = subparsers.add_parser("activate")
    identity(activate)
    activate.add_argument("--lease-until", required=True)
    activate.add_argument("--listen-port", type=int, default=DEFAULT_LISTEN_PORT)
    renew = subparsers.add_parser("renew")
    identity(renew)
    renew.add_argument("--lease-until", required=True)
    issue = subparsers.add_parser("issue")
    identity(issue)
    issue.add_argument("--public-key", required=True)
    issue.add_argument("--certificate-output", required=True)
    issue.add_argument("--valid-before", required=True)
    revoke = subparsers.add_parser("revoke")
    identity(revoke)
    revoke.add_argument("--serial", required=True, type=int)
    revoke.add_argument("--reason", required=True)
    deactivate = subparsers.add_parser("deactivate")
    identity(deactivate)
    deactivate.add_argument("--reason", required=True)
    subparsers.add_parser("reconcile")
    render = subparsers.add_parser("render-sshd-config")
    render.add_argument("--output", required=True)
    render.add_argument("--listen-port", type=int, default=DEFAULT_LISTEN_PORT)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    arguments = _parser().parse_args(argv)
    broker = _broker(arguments)
    try:
        if arguments.action == "initialize":
            broker.initialize()
        elif arguments.action == "activate":
            broker.activate(
                master_instance_id=arguments.master_instance_id,
                run_id=arguments.run_id,
                attempt_id=arguments.attempt_id,
                epoch=arguments.epoch,
                lease_until=_parse_time(arguments.lease_until, "lease_until"),
                listen_port=arguments.listen_port,
                now=_now(),
            )
        elif arguments.action == "renew":
            broker.renew(
                master_instance_id=arguments.master_instance_id,
                run_id=arguments.run_id,
                attempt_id=arguments.attempt_id,
                epoch=arguments.epoch,
                lease_until=_parse_time(arguments.lease_until, "lease_until"),
                now=_now(),
            )
        elif arguments.action == "issue":
            serial = broker.issue(
                master_instance_id=arguments.master_instance_id,
                run_id=arguments.run_id,
                attempt_id=arguments.attempt_id,
                epoch=arguments.epoch,
                public_key=Path(arguments.public_key),
                certificate_output=Path(arguments.certificate_output),
                valid_before=_parse_time(arguments.valid_before, "valid_before"),
                now=_now(),
            )
            print(json.dumps({"issued": True, "serial": serial}, separators=(",", ":")))
        elif arguments.action == "revoke":
            broker.revoke(
                master_instance_id=arguments.master_instance_id,
                run_id=arguments.run_id,
                attempt_id=arguments.attempt_id,
                epoch=arguments.epoch,
                serial=arguments.serial,
                reason=arguments.reason,
            )
        elif arguments.action == "deactivate":
            broker.deactivate(
                master_instance_id=arguments.master_instance_id,
                run_id=arguments.run_id,
                attempt_id=arguments.attempt_id,
                epoch=arguments.epoch,
                reason=arguments.reason,
            )
        elif arguments.action == "reconcile":
            active = broker.reconcile(now=_now())
            print(json.dumps({"active": active}, separators=(",", ":")))
        elif arguments.action == "render-sshd-config":
            rendered = render_sshd_config(
                account=arguments.account,
                ca_public_key=broker.ca_public_key,
                principals_file=broker.principals_path,
                revoked_keys_file=broker.krl_path,
                listen_port=arguments.listen_port,
            )
            output = Path(arguments.output)
            if not output.is_absolute() or output.is_symlink():
                raise TunnelBrokerError("sshd config output must be an absolute non-symlink path")
            _atomic_write(output, rendered.encode(), 0o600)
        else:  # pragma: no cover - argparse enforces the closed action set
            raise AssertionError(arguments.action)
    except (OSError, UnicodeError, ValueError, TunnelBrokerError) as exc:
        _die(str(exc))
    return 0


def _die(message: str) -> NoReturn:
    raise SystemExit(f"master tunnel broker denied operation: {message}")


if __name__ == "__main__":
    raise SystemExit(main())
