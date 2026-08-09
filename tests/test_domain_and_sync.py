from __future__ import annotations

from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from my_data_hub.domain.commands import Changeset, Operation, SemanticCommand
from my_data_hub.sync.conflicts import classify_operation
from my_data_hub.sync.ordering import (
    DependencyCycleError,
    PendingCommand,
    deterministic_dependency_order,
)


def semantic_payload() -> dict[str, object]:
    return {
        "schema_version": "my-data-hub-semantic-command.v1",
        "command_id": str(uuid4()),
        "client_id": "test-client",
        "actor_id": "test-actor",
        "idempotency_key": "fixture-1",
        "command_type": "region_talk.work.enqueue",
        "base_revision": 1,
        "target": {"type": "content_url", "id": str(uuid4())},
        "input_fingerprint": "a" * 64,
        "payload": {"stage": "exact_url_intake"},
    }


def test_semantic_command_rejects_duplicate_dependencies() -> None:
    payload = semantic_payload()
    dependency = uuid4()
    payload["depends_on"] = [str(dependency), str(dependency)]
    with pytest.raises(ValidationError, match="unique"):
        SemanticCommand.model_validate(payload)


def test_changeset_rejects_duplicate_operation_ids() -> None:
    operation_id = uuid4()
    raw = {
        "schema_version": "my-data-hub-changeset.v1",
        "changeset_id": str(uuid4()),
        "session_id": str(uuid4()),
        "client_id": "client",
        "actor_id": "actor",
        "idempotency_key": "key",
        "base_revision": 0,
        "input_fingerprint": "b" * 64,
        "operations": [
            {
                "operation_id": str(operation_id),
                "kind": "event.append",
                "preconditions": {},
                "payload": {},
            },
            {
                "operation_id": str(operation_id),
                "kind": "event.append",
                "preconditions": {},
                "payload": {},
            },
        ],
    }
    with pytest.raises(ValidationError, match="unique"):
        Changeset.model_validate(raw)


def test_conflict_policy_requires_precondition_for_scalar_mutation() -> None:
    operation = Operation(
        operation_id=uuid4(),
        kind="field.set",
        preconditions={},
        payload={"field": "title", "value": "new"},
    )
    assert classify_operation(operation).disposition == "quarantine"
    guarded = operation.model_copy(update={"expected_revision": 2})
    assert classify_operation(guarded).disposition == "conditional"


def test_dependency_order_is_deterministic_and_respects_edges() -> None:
    session = UUID("11111111-1111-4111-8111-111111111111")
    first = PendingCommand(UUID("10000000-0000-4000-8000-000000000001"), session, 1)
    second = PendingCommand(
        UUID("10000000-0000-4000-8000-000000000002"),
        session,
        2,
        (first.command_id,),
    )
    third = PendingCommand(UUID("10000000-0000-4000-8000-000000000003"), session, 3)
    ordered = deterministic_dependency_order([second, third, first])
    assert [item.command_id for item in ordered] == [
        first.command_id,
        second.command_id,
        third.command_id,
    ]


def test_dependency_cycle_is_rejected() -> None:
    session = uuid4()
    first_id, second_id = uuid4(), uuid4()
    commands = [
        PendingCommand(first_id, session, 1, (second_id,)),
        PendingCommand(second_id, session, 2, (first_id,)),
    ]
    with pytest.raises(DependencyCycleError):
        deterministic_dependency_order(commands)
