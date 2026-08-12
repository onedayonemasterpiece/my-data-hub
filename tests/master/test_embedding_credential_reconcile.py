from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from uuid import UUID

from my_data_hub.master_runtime.notebook_entrypoint import _reconcile_embedding_worker_credentials


class Response:
    def __init__(self, value): self.value = value  # type: ignore[no-untyped-def]
    def __enter__(self): return self
    def __exit__(self, *_args): return None  # type: ignore[no-untyped-def]
    def read(self, _limit): return json.dumps(self.value).encode()  # type: ignore[no-untyped-def]


def test_master_issues_exact_task_credential_and_replay_is_idempotent(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    task = UUID("22222222-2222-4222-8222-222222222222")
    command = {"task_run_id": str(task), "epoch": 7, "task_token_sha256": "a" * 64}
    responses = iter((Response({"commands": [command], "revocations": []}), Response({"registered": True})))
    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: next(responses))
    created = []
    dropped = []
    provisioner = SimpleNamespace(
        create=lambda **kwargs: created.append(kwargs), drop=lambda principal: dropped.append(principal)
    )
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner",
        lambda *_args: provisioner,
    )
    config = SimpleNamespace(
        master_instance_id=UUID("44444444-4444-4444-8444-444444444444"),
        run_id="run-1", attempt_id="attempt-1", epoch=7, tunnel_remote_port=25432,
    )
    issued = {}
    _reconcile_embedding_worker_credentials(
        connection=object(), gate=object(), config=config,
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        run_secret="secret", lease_until=datetime.now(UTC) + timedelta(minutes=5), issued=issued,
    )
    assert len(created) == 1 and created[0]["group"] == "mdh_embedding_worker"
    assert len(issued) == 1 and dropped == []


def test_master_revokes_exact_credential(monkeypatch) -> None:  # type: ignore[no-untyped-def]
    credential = UUID("33333333-3333-4333-8333-333333333333")
    task = UUID("22222222-2222-4222-8222-222222222222")
    monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: Response({
        "commands": [], "revocations": [{"task_run_id": str(task), "credential_id": str(credential)}]
    }))
    dropped = []
    monkeypatch.setattr(
        "my_data_hub.master_runtime.notebook_entrypoint.CredentialProvisioner",
        lambda *_args: SimpleNamespace(drop=lambda principal: dropped.append(principal)),
    )
    gate = SimpleNamespace(revoke_credential=lambda *_args: None)
    config = SimpleNamespace(run_id="run-1", attempt_id="attempt-1")
    issued = {credential: "mdh_e7_embed_deadbeef"}
    _reconcile_embedding_worker_credentials(
        connection=object(), gate=gate, config=config,
        callback_url="https://mcp-datahub.kenigevents.ru/internal/runtime/events",
        run_secret="secret", lease_until=datetime.now(UTC) + timedelta(minutes=5), issued=issued,
    )
    assert issued == {} and dropped == ["mdh_e7_embed_deadbeef"]
