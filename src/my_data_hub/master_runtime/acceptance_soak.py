"""Runtime-local production wiring for the fixed FM24 soak port."""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import psycopg

from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceBinding
from my_data_hub.acceptance.soak_session import (
    ActiveServiceReceipt,
    BoundedReadReceipt,
    CredentialExpiryReceipt,
    CredentialRotationReceipt,
    ProductionSoakSessionPort,
    SoakStateJournal,
    StaleReconnectReceipt,
)
from my_data_hub.hashing import canonical_json_bytes
from my_data_hub.master_runtime.credentials import CredentialProvisioner


def _sha(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def _credential_sha(principal: str) -> str:
    return hashlib.sha256(principal.encode()).hexdigest()


@dataclass(frozen=True, slots=True)
class _RegisteredCredential:
    principal: str
    database_url: str = field(repr=False)


@dataclass(slots=True)
class NotebookSoakCredentialAuthority:
    """Rotate and revoke only credentials created by this Notebook process."""

    provisioner: CredentialProvisioner
    local_postgres_port: int
    rotate: Callable[[datetime], tuple[str, ...]] | None = field(default=None, repr=False)
    _current: _RegisteredCredential | None = field(default=None, init=False, repr=False)
    _prior: _RegisteredCredential | None = field(default=None, init=False, repr=False)
    _rotations: dict[tuple[int, str], CredentialRotationReceipt] = field(
        default_factory=dict, init=False, repr=False
    )
    _expiries: dict[tuple[int, str], CredentialExpiryReceipt] = field(
        default_factory=dict, init=False, repr=False
    )

    def observe_registration(
        self, principals: tuple[str, ...], credentials: tuple[dict[str, str], ...]
    ) -> None:
        readers = [
            _RegisteredCredential(principal, self._local_url(item["database_url"]))
            for principal, item in zip(principals, credentials, strict=True)
            if item["role"] == "reader"
        ]
        if len(readers) != 1:
            raise RuntimeError("FM24 requires one exact reader credential")
        if self._current is not None and readers[0].principal != self._current.principal:
            self._prior = self._current
        self._current = readers[0]

    def ensure_rotation(
        self, binding: MasterAcceptanceBinding, *, step: int, intent_sha256: str,
        expires_at: datetime,
    ) -> CredentialRotationReceipt:
        del binding
        key = (step, intent_sha256)
        if key in self._rotations:
            return self._rotations[key]
        prior = self._current
        if prior is None or self.rotate is None:
            raise RuntimeError("FM24 initial reader/rotation callback is unavailable")
        principals = self.rotate(expires_at)
        current = self._current
        assert current is not None
        receipt = CredentialRotationReceipt(
            evidence_class="live",
            step=step,
            current_credential_sha256=_credential_sha(current.principal),
            prior_credential_sha256=_credential_sha(prior.principal),
            registration_receipt_sha256=_sha(
                {"step": step, "intent_sha256": intent_sha256, "principals": sorted(principals)}
            ),
            expires_at=expires_at,
        )
        self._rotations[key] = receipt
        return receipt

    def ensure_prior_expired(
        self, binding: MasterAcceptanceBinding, *, step: int, intent_sha256: str,
    ) -> CredentialExpiryReceipt:
        del binding
        key = (step, intent_sha256)
        if key in self._expiries:
            return self._expiries[key]
        prior = self._prior
        if prior is None:
            raise RuntimeError("FM24 prior reader credential is unavailable")
        self.provisioner.drop(prior.principal)
        receipt = CredentialExpiryReceipt(
            evidence_class="live",
            step=step,
            prior_credential_sha256=_credential_sha(prior.principal),
            expiry_receipt_sha256=_sha(
                {"step": step, "intent_sha256": intent_sha256, "principal": prior.principal}
            ),
            expired=True,
        )
        self._expiries[key] = receipt
        return receipt

    @property
    def current(self) -> _RegisteredCredential:
        if self._current is None:
            raise RuntimeError("FM24 current reader credential is unavailable")
        return self._current

    @property
    def prior(self) -> _RegisteredCredential:
        if self._prior is None:
            raise RuntimeError("FM24 prior reader credential is unavailable")
        return self._prior

    def prior_was_expired(self, step: int) -> bool:
        return any(key[0] == step for key in self._expiries)

    def _local_url(self, value: str) -> str:
        parsed = urlsplit(value)
        host = "127.0.0.1"
        userinfo = ""
        if parsed.username is not None:
            from urllib.parse import quote

            userinfo = quote(parsed.username, safe="")
            if parsed.password is not None:
                userinfo += ":" + quote(parsed.password, safe="")
            userinfo += "@"
        return urlunsplit(
            (
                parsed.scheme,
                f"{userinfo}{host}:{self.local_postgres_port}",
                parsed.path,
                "sslmode=require&connect_timeout=5",
                "",
            )
        )


@dataclass(frozen=True, slots=True)
class NotebookSoakReadProbe:
    credentials: NotebookSoakCredentialAuthority

    def bounded_read(
        self, binding: MasterAcceptanceBinding, *, step: int, intent_sha256: str,
    ) -> BoundedReadReceipt:
        with psycopg.connect(self.credentials.current.database_url, connect_timeout=5) as connection:
            row = connection.execute(
                "SELECT current_epoch FROM master_control.epoch_state WHERE singleton=true"
            ).fetchone()
            connection.rollback()
        if row is None or int(row[0]) != binding.epoch:
            raise RuntimeError("FM24 bounded read observed another epoch")
        return BoundedReadReceipt(
            evidence_class="live", step=step, query_contract="fm24_active_epoch_read.v1",
            observed_rows=1, active_epoch=binding.epoch,
            read_receipt_sha256=_sha({"step": step, "intent_sha256": intent_sha256, "epoch": binding.epoch}),
        )

    def stale_reconnect_denied(
        self, binding: MasterAcceptanceBinding, *, step: int, intent_sha256: str,
    ) -> StaleReconnectReceipt:
        del binding
        if not self.credentials.prior_was_expired(step):
            raise RuntimeError("FM24 prior broker binding was not explicitly expired")
        denied = False
        try:
            with psycopg.connect(self.credentials.prior.database_url, connect_timeout=5):
                pass
        except psycopg.Error:
            denied = True
        if not denied:
            raise RuntimeError("FM24 stale credential unexpectedly reconnected")
        return StaleReconnectReceipt(
            evidence_class="live", step=step, denied=True,
            denial_code="MDH_CREDENTIAL_EXPIRED_OR_REVOKED", broker_binding_verified=True,
            denial_receipt_sha256=_sha({"step": step, "intent_sha256": intent_sha256, "denied": True}),
        )

    def exact_service_active(self, binding: MasterAcceptanceBinding) -> ActiveServiceReceipt:
        with psycopg.connect(self.credentials.current.database_url, connect_timeout=5) as connection:
            row = connection.execute(
                "SELECT current_epoch,gate_state,lease_until>clock_timestamp() "
                "FROM master_control.epoch_state WHERE singleton=true"
            ).fetchone()
            connection.rollback()
        if row is None or int(row[0]) != binding.epoch or row[1] != "open" or row[2] is not True:
            raise RuntimeError("FM24 final service is not the exact ACTIVE epoch")
        return ActiveServiceReceipt(
            evidence_class="live", active=True, epoch=binding.epoch,
            service_receipt_sha256=_sha({"epoch": binding.epoch, "active": True}),
        )


@dataclass(frozen=True, slots=True)
class HttpTunnelLeaseAuthority:
    renew_exact: Callable[[datetime], None]

    def renew(self, *, master_instance_id: str, run_id: str, attempt_id: str, epoch: int,
              lease_until: datetime, now: datetime) -> Any:
        del now
        self.renew_exact(lease_until)
        return _TunnelLease(master_instance_id, run_id, attempt_id, epoch, lease_until)


@dataclass(frozen=True, slots=True)
class _TunnelLease:
    master_instance_id: str
    run_id: str
    attempt_id: str
    epoch: int
    lease_until: datetime

    def to_json(self) -> dict[str, object]:
        return {
            "master_instance_id": self.master_instance_id,
            "run_id": self.run_id,
            "attempt_id": self.attempt_id,
            "epoch": self.epoch,
            "lease_until": self.lease_until.astimezone(UTC).isoformat().replace("+00:00", "Z"),
        }


def build_notebook_soak_port(
    *, task_id: Any, binding: MasterAcceptanceBinding, journal_path: Path,
    runtime_client: Any, database_gate: Any, credential_authority: NotebookSoakCredentialAuthority,
    renew_tunnel: Callable[[datetime], None],
) -> ProductionSoakSessionPort:
    return ProductionSoakSessionPort(
        task_id=task_id,
        binding=binding,
        journal=SoakStateJournal(journal_path),
        runtime_client=runtime_client,
        database_gate=database_gate,
        tunnel_authority=HttpTunnelLeaseAuthority(renew_tunnel),  # type: ignore[arg-type]
        credential_registrar=credential_authority,
        read_probe=NotebookSoakReadProbe(credential_authority),
        evidence_class="live",
    )
