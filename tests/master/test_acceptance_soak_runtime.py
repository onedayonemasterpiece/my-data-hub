from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from my_data_hub.acceptance.master_lifecycle import MasterAcceptanceBinding
from my_data_hub.master_runtime.acceptance_soak import NotebookSoakCredentialAuthority


class Provisioner:
    def __init__(self) -> None:
        self.dropped: list[str] = []

    def drop(self, principal: str) -> None:
        self.dropped.append(principal)


def _credential(principal: str) -> tuple[tuple[str, ...], tuple[dict[str, str], ...]]:
    return (
        (principal,),
        ({
            "role": "reader",
            "database_url": (
                f"postgresql://{principal}:not-a-real-secret@127.0.0.1:25432/postgres?"
                "sslmode=verify-ca&sslrootcert=/state/master-tls/ca.pem"
            ),
            "expires_at": "2026-08-11T12:00:00Z",
        },),
    )


def test_notebook_soak_authority_rotates_and_revokes_exact_reader_once() -> None:
    provisioner = Provisioner()
    authority = NotebookSoakCredentialAuthority(provisioner=provisioner, local_postgres_port=5432)  # type: ignore[arg-type]
    initial_principals, initial_credentials = _credential("mdh_e3_reader_initial")
    authority.observe_registration(initial_principals, initial_credentials)

    def rotate(_expires_at):
        principals, credentials = _credential("mdh_e3_reader_rotated")
        authority.observe_registration(principals, credentials)
        return principals

    authority.rotate = rotate
    binding = MasterAcceptanceBinding(
        operation_id=uuid4(), run_id=uuid4(), attempt_id=uuid4(),
        service_instance_id=str(uuid4()), master_instance_id=uuid4(), epoch=3,
    )
    expires_at = datetime.now(UTC) + timedelta(minutes=4)
    rotation = authority.ensure_rotation(
        binding, step=1, intent_sha256="a" * 64, expires_at=expires_at
    )
    assert rotation.current_credential_sha256 != rotation.prior_credential_sha256
    assert "127.0.0.1:5432" in authority.current.database_url
    assert "sslmode=require" in authority.current.database_url
    expired = authority.ensure_prior_expired(binding, step=1, intent_sha256="b" * 64)
    replay = authority.ensure_prior_expired(binding, step=1, intent_sha256="b" * 64)
    assert expired == replay and expired.expired is True
    assert provisioner.dropped == ["mdh_e3_reader_initial"]
