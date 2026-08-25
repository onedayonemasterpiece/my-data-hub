from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

DatasetRef = Annotated[str, Field(min_length=3, max_length=300, pattern=r"^[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+$")]
OpaqueCursor = Annotated[str, Field(min_length=1, max_length=2000)]
PublicId = UUID


class _Closed(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)


class ResearchSelector(_Closed):
    research_id: PublicId | None = None
    alias: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    dataset_ref: DatasetRef | None = None

    @model_validator(mode="after")
    def one_selector(self) -> ResearchSelector:
        if sum(value is not None for value in (self.research_id, self.alias, self.dataset_ref)) != 1:
            raise ValueError("exactly one research selector is required")
        return self


class OptionalResearchSelector(_Closed):
    research_id: PublicId | None = None
    alias: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    dataset_ref: DatasetRef | None = None

    @model_validator(mode="after")
    def at_most_one_selector(self) -> OptionalResearchSelector:
        if sum(value is not None for value in (self.research_id, self.alias, self.dataset_ref)) > 1:
            raise ValueError("at most one research selector is allowed")
        return self


class RevisionSelector(_Closed):
    revision_id: PublicId | None = None
    revision_no: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def at_most_one_revision(self) -> RevisionSelector:
        if self.revision_id is not None and self.revision_no is not None:
            raise ValueError("at most one revision selector is allowed")
        return self


class RuntimeOptions(_Closed):
    accelerator: Literal["none", "gpu"] = "none"
    enable_internet: Literal[False] = False
    timeout_seconds: int = Field(default=1800, ge=60, le=3600)


class DatasetSelection(_Closed):
    dataset_ref: DatasetRef
    provider_version: int | None = Field(default=None, ge=1)


class DatasetsSearchRequest(_Closed):
    query: str = Field(min_length=1, max_length=200)
    visibility: Literal["public", "owner_private", "all"] = "all"
    cursor: OpaqueCursor | None = None
    limit: int = Field(default=20, ge=1, le=50)


class DatasetsInspectRequest(_Closed):
    dataset_ref: DatasetRef
    provider_version: int | None = Field(default=None, ge=1)
    file_cursor: OpaqueCursor | None = None
    file_limit: int = Field(default=100, ge=1, le=200)


class DatasetsFileReadRequest(_Closed):
    dataset_ref: DatasetRef
    provider_version: int = Field(ge=1)
    path: str = Field(min_length=1, max_length=1000)
    offset: int = Field(default=0, ge=0)
    max_bytes: int = Field(default=131_072, ge=1, le=131_072)


class ResearchCreateRequest(_Closed):
    alias: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    title: str = Field(min_length=1, max_length=200)
    goal: str = Field(min_length=1, max_length=4000)
    dataset_ref: DatasetRef


class ResearchListRequest(_Closed):
    cursor: OpaqueCursor | None = None
    limit: int = Field(default=20, ge=1, le=50)


class ResearchGetRequest(ResearchSelector):
    revision_cursor: OpaqueCursor | None = None
    run_cursor: OpaqueCursor | None = None
    history_limit: int = Field(default=20, ge=1, le=50)


class NotebooksFindRequest(OptionalResearchSelector):
    query: str | None = Field(default=None, min_length=1, max_length=200)
    cursor: OpaqueCursor | None = None
    limit: int = Field(default=20, ge=1, le=50)


class NotebooksGetRequest(_Closed):
    research_id: PublicId | None = None
    alias: str | None = Field(default=None, min_length=1, max_length=64, pattern=r"^[a-z0-9][a-z0-9-]*$")
    dataset_ref: DatasetRef | None = None
    revision_id: PublicId | None = None
    revision_no: int | None = Field(default=None, ge=1)
    notebook_ref: DatasetRef | None = None
    source_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def one_revision(self) -> NotebooksGetRequest:
        research_count = sum(
            value is not None for value in (self.research_id, self.alias, self.dataset_ref)
        )
        revision_count = sum(value is not None for value in (self.revision_id, self.revision_no))
        provider_mode = self.notebook_ref is not None or self.source_version is not None
        if provider_mode:
            if self.notebook_ref is None or self.source_version is None or research_count or revision_count:
                raise ValueError("owner Notebook mode requires only notebook_ref and source_version")
        elif research_count != 1 or revision_count != 1:
            raise ValueError("stored revision mode requires one research and one revision selector")
        return self


class NotebooksSaveRequest(ResearchSelector):
    revision_id: PublicId | None = None
    revision_no: int | None = Field(default=None, ge=1)
    code_file: str = Field(default="research.py", min_length=1, max_length=300)
    kernel_type: Literal["script", "notebook"] = "script"
    source_utf8: str = Field(min_length=1, max_length=262_144)
    runtime: RuntimeOptions = Field(default_factory=RuntimeOptions)

    @model_validator(mode="after")
    def parent_selector(self) -> NotebooksSaveRequest:
        if self.revision_id is not None and self.revision_no is not None:
            raise ValueError("at most one parent revision selector is allowed")
        return self


class NotebooksInputsSetRequest(ResearchSelector):
    revision_id: PublicId | None = None
    revision_no: int | None = Field(default=None, ge=1)
    inputs: list[DatasetSelection] = Field(min_length=1, max_length=16)

    @model_validator(mode="after")
    def revision_selector(self) -> NotebooksInputsSetRequest:
        if self.revision_id is not None and self.revision_no is not None:
            raise ValueError("at most one draft revision selector is allowed")
        return self


class RunsStartRequest(ResearchSelector):
    revision_id: PublicId | None = None
    revision_no: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def revision_selector(self) -> RunsStartRequest:
        if self.revision_id is not None and self.revision_no is not None:
            raise ValueError("at most one run revision selector is allowed")
        return self


class RunsGetRequest(_Closed):
    run_id: PublicId


class RunsLogsRequest(_Closed):
    run_id: PublicId
    offset: int = Field(default=0, ge=0)
    max_bytes: int = Field(default=65_536, ge=1, le=131_072)


class RunsRetryRequest(_Closed):
    run_id: PublicId


class ArtifactsListRequest(_Closed):
    run_id: PublicId


class ArtifactsReadRequest(_Closed):
    artifact_id: PublicId
    offset: int = Field(default=0, ge=0)
    max_bytes: int = Field(default=131_072, ge=1, le=131_072)


KAGGLE_RESEARCH_REQUESTS: dict[str, type[_Closed]] = {
    "datasets.search": DatasetsSearchRequest,
    "datasets.inspect": DatasetsInspectRequest,
    "datasets.file.read": DatasetsFileReadRequest,
    "research.create": ResearchCreateRequest,
    "research.list": ResearchListRequest,
    "research.get": ResearchGetRequest,
    "notebooks.find": NotebooksFindRequest,
    "notebooks.get": NotebooksGetRequest,
    "notebooks.save": NotebooksSaveRequest,
    "notebooks.inputs.set": NotebooksInputsSetRequest,
    "runs.start": RunsStartRequest,
    "runs.get": RunsGetRequest,
    "runs.logs": RunsLogsRequest,
    "runs.retry": RunsRetryRequest,
    "artifacts.list": ArtifactsListRequest,
    "artifacts.read": ArtifactsReadRequest,
}


def validate_kaggle_research_arguments(tool: str, arguments: Any) -> dict[str, Any]:
    model = KAGGLE_RESEARCH_REQUESTS.get(tool)
    if model is None:
        raise ValueError("unknown Kaggle research tool")
    value = model.model_validate(arguments)
    return value.model_dump(mode="json", exclude_none=True)
