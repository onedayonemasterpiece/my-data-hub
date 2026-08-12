from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field, model_validator

from my_data_hub.embeddings.models import EmbeddingModelContract


class DenseSearchRoute(BaseModel):
    """Binds a query encoder identity to exactly one compatible vector space."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    query_model: EmbeddingModelContract
    index_model: EmbeddingModelContract
    index_vector_space: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")
    candidate_limit: int = Field(default=200, ge=1, le=10_000)

    @model_validator(mode="after")
    def query_and_index_contracts_match(self) -> DenseSearchRoute:
        if self.query_model.exact_id != self.index_model.exact_id:
            raise ValueError("query model and index model exact revisions differ")
        if self.query_model.dimensions != self.index_model.dimensions:
            raise ValueError("query and index dimensions differ")
        if self.index_vector_space != self.index_model.vector_space:
            raise ValueError("index vector space does not match its model contract")
        return self
