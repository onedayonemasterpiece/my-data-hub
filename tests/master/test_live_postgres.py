from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from uuid import UUID

import pytest

from my_data_hub.db.migrations import migrate
from my_data_hub.master_runtime.contracts import MasterIdentity
from my_data_hub.master_runtime.credentials import CredentialProvisioner
from my_data_hub.master_runtime.database_gate import DatabaseGate

ROOT = Path(__file__).resolve().parents[2]
IMAGE = "pgvector/pgvector:0.8.6-pg18-bookworm"


def _free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return int(listener.getsockname()[1])


@pytest.mark.skipif(
    os.getenv("MDH_RUN_DISPOSABLE_POSTGRES") != "1" or shutil.which("docker") is None,
    reason="set MDH_RUN_DISPOSABLE_POSTGRES=1 for disposable tmpfs PostgreSQL proof",
)
def test_live_old_session_commit_is_rejected_after_fence_and_epoch_rotation() -> None:
    import psycopg

    port = _free_port()
    name = f"mdh-l03-{os.getpid()}"
    password = "fixture-admin-password-not-a-secret"
    subprocess.run(
        [
            "docker",
            "run",
            "--detach",
            "--rm",
            "--name",
            name,
            "--tmpfs",
            "/var/lib/postgresql:rw,nosuid,nodev,size=768m",
            "-e",
            f"POSTGRES_PASSWORD={password}",
            "-p",
            f"127.0.0.1:{port}:5432",
            IMAGE,
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    admin_url = f"postgresql://postgres:{password}@127.0.0.1:{port}/postgres"
    try:
        deadline = time.monotonic() + 60
        while True:
            try:
                with psycopg.connect(admin_url, connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    logs = subprocess.run(
                        ["docker", "logs", name], capture_output=True, text=True, check=False
                    ).stdout
                    raise AssertionError(f"PostgreSQL did not become ready: {logs[-2000:]}") from None
                time.sleep(0.5)

        with psycopg.connect(admin_url) as admin:
            admin.execute((ROOT / "sql/admin/bootstrap_roles.sql").read_text())
            admin.commit()
        migrate(admin_url, ROOT / "sql/migrations")
        with psycopg.connect(admin_url) as admin:
            admin.execute((ROOT / "sql/admin/role_contract.sql").read_text())
            admin.commit()

        now = datetime.now(UTC)
        a = MasterIdentity(UUID("11111111-1111-4111-8111-111111111111"), "docker-run-a", 1)
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.acquire(a, now + timedelta(minutes=5))
            gate.activate(a)
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e1_writer_deadbeef",
                password="writer-a-password-long-enough",
                group="mdh_application",
                identity=a,
                credential_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )

        a_url = f"postgresql://mdh_e1_writer_deadbeef:writer-a-password-long-enough@127.0.0.1:{port}/postgres"
        old = psycopg.connect(a_url)
        old.execute("SET ROLE mdh_application")
        old.execute(
            "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('a','a','before','ok')"
        )
        old.commit()

        # Begin a transaction while A is active, then fence it.  The deferred guard
        # re-evaluates at commit, proving already-open sessions cannot sneak a commit.
        old.execute(
            "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('a','a','after','blocked')"
        )
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.fence(a, "forced_rotation")
        with pytest.raises(psycopg.Error, match="epoch lease gate"):
            old.commit()
        old.rollback()

        b = MasterIdentity(UUID("22222222-2222-4222-8222-222222222222"), "docker-run-b", 2)
        with psycopg.connect(admin_url) as admin:
            gate = DatabaseGate(admin)
            gate.acquire(b, now + timedelta(minutes=6))
            gate.activate(b)
            CredentialProvisioner(admin, gate).create(
                principal="mdh_e2_writer_cafebabe",
                password="writer-b-password-long-enough",
                group="mdh_application",
                identity=b,
                credential_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
                expires_at=now + timedelta(minutes=4),
                now=now,
            )

        # A remains connected and knows B's public epoch, but session_user is
        # immutably bound to epoch 1 and therefore remains fenced.
        old.execute("SET ROLE mdh_application")
        old.execute(
            "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('a','a','stale','blocked')"
        )
        with pytest.raises(psycopg.Error, match="epoch lease gate"):
            old.commit()
        old.close()

        b_url = f"postgresql://mdh_e2_writer_cafebabe:writer-b-password-long-enough@127.0.0.1:{port}/postgres"
        with psycopg.connect(b_url) as current:
            current.execute("SET ROLE mdh_application")
            current.execute(
                "INSERT INTO sync.audit_event(actor_id,client_id,action,outcome) VALUES ('b','b','current','ok')"
            )
            current.commit()
        with psycopg.connect(admin_url) as admin:
            assert admin.execute("SELECT count(*) FROM sync.audit_event").fetchone()[0] == 2
            state = admin.execute(
                "SELECT highest_epoch,current_epoch,gate_state FROM master_control.epoch_state"
            ).fetchone()
            assert state == (2, 2, "open")
    finally:
        subprocess.run(["docker", "rm", "--force", name], check=False, capture_output=True)
