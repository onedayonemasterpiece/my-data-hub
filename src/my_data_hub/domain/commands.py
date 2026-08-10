from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

OperationKind = Literal[
    "object.create",
    "field.set",
    "set.add",
    "set.remove",
    "relation.add",
    "relation.remove",
    "state.transition",
    "object.tombstone",
    "event.append",
    "analysis.record",
    "pipeline.enqueue",
]


class Target(BaseModel):
    model_config = ConfigDict(extra="forbid")
    type: str = Field(min_length=1, max_length=200, pattern=r"^[a-z][a-z0-9_.-]+$")
    id: UUID


class SemanticCommand(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["my-data-hub-semantic-command.v1"]
    command_id: UUID
    session_id: UUID | None = None
    client_id: str = Field(min_length=1, max_length=200)
    actor_id: str = Field(min_length=1, max_length=200)
    idempotency_key: str = Field(min_length=1, max_length=300)
    command_type: str = Field(min_length=3, max_length=200, pattern=r"^[a-z][a-z0-9_.-]+$")
    base_revision: int = Field(ge=0)
    expected_revision: int | None = Field(default=None, ge=0)
    target: Target | None = None
    depends_on: list[UUID] = Field(default_factory=list, max_length=1000)
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    payload: dict[str, Any]
    reason: str | None = Field(default=None, max_length=2000)
    dry_run: bool = False

    @model_validator(mode="after")
    def dependencies_are_unique(self) -> SemanticCommand:
        if len(self.depends_on) != len(set(self.depends_on)):
            raise ValueError("depends_on values must be unique")
        if self.command_id in self.depends_on:
            raise ValueError("command cannot depend on itself")
        return self


class Operation(BaseModel):
    model_config = ConfigDict(extra="forbid")
    operation_id: UUID
    kind: OperationKind
    target_type: str | None = Field(
        default=None, min_length=1, max_length=200, pattern=r"^[a-z][a-z0-9_.-]+$"
    )
    target_id: UUID | None = None
    expected_revision: int | None = Field(default=None, ge=0)
    preconditions: dict[str, Any] = Field(default_factory=dict)
    payload: dict[str, Any]


class Changeset(BaseModel):
    model_config = ConfigDict(extra="forbid")
    schema_version: Literal["my-data-hub-changeset.v1"]
    changeset_id: UUID
    session_id: UUID
    client_id: str = Field(min_length=1, max_length=200)
    actor_id: str = Field(min_length=1, max_length=200)
    idempotency_key: str = Field(min_length=1, max_length=300)
    base_revision: int = Field(ge=0)
    depends_on: list[UUID] = Field(default_factory=list, max_length=1000)
    input_fingerprint: str = Field(pattern=r"^[a-f0-9]{64}$")
    operations: list[Operation] = Field(min_length=1, max_length=100)
    reason: str | None = Field(default=None, max_length=2000)
    dry_run: bool = False

    @model_validator(mode="after")
    def identities_are_unique(self) -> Changeset:
        if len(self.depends_on) != len(set(self.depends_on)):
            raise ValueError("depends_on values must be unique")
        if self.changeset_id in self.depends_on:
            raise ValueError("changeset cannot depend on itself")
        operation_ids = [item.operation_id for item in self.operations]
        if len(operation_ids) != len(set(operation_ids)):
            raise ValueError("operation_id values must be unique")
        return self
