from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator


class EmbeddingModelContract(BaseModel):
    """Exact, immutable identity and encoder behavior for one vector space."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    model_key: str = Field(min_length=1, max_length=300)
    revision: str = Field(pattern=r"^[a-f0-9]{40}$")
    dimensions: int = Field(ge=1, le=65_536)
    max_tokens: int = Field(ge=1, le=65_536)
    pooling: Literal["attention_mask_mean", "model_native_dense"]
    normalization: Literal["l2"]
    query_prefix: str
    document_prefix: str
    output_modes: tuple[Literal["dense"], ...] = Field(min_length=1, max_length=1)
    encoder_contract_version: str = Field(min_length=1, max_length=200)
    vector_space: str = Field(pattern=r"^[a-z][a-z0-9_]{0,99}$")

    @model_validator(mode="after")
    def dense_mode_is_singular(self) -> EmbeddingModelContract:
        if self.output_modes != ("dense",):
            raise ValueError("only one dense output mode is permitted")
        return self

    @property
    def exact_id(self) -> str:
        return f"{self.model_key}@{self.revision}"

    def prepare_text(self, text: str, *, query: bool) -> str:
        prefix = self.query_prefix if query else self.document_prefix
        return prefix + text


E5_MULTILINGUAL_BASE = EmbeddingModelContract(
    model_key="intfloat/multilingual-e5-base",
    revision="d128750597153bb5987e10b1c3493a34e5a4502a",
    dimensions=768,
    max_tokens=512,
    pooling="attention_mask_mean",
    normalization="l2",
    query_prefix="query: ",
    document_prefix="passage: ",
    output_modes=("dense",),
    encoder_contract_version="e5-attention-mask-mean-l2-prefixes-max512.v1",
    vector_space="e5_multilingual_base_768_v1",
)

BGE_M3 = EmbeddingModelContract(
    model_key="BAAI/bge-m3",
    revision="5617a9f61b028005a4858fdac845db406aefb181",
    dimensions=1024,
    max_tokens=8192,
    pooling="model_native_dense",
    normalization="l2",
    query_prefix="",
    document_prefix="",
    output_modes=("dense",),
    encoder_contract_version="bge-m3-native-dense-only-l2.v1",
    vector_space="bge_m3_dense_1024_v1",
)

_MODELS = {model.model_key: model for model in (E5_MULTILINGUAL_BASE, BGE_M3)}


def model_by_key(model_key: str) -> EmbeddingModelContract:
    try:
        return _MODELS[model_key]
    except KeyError as exc:
        raise ValueError(f"unsupported embedding model: {model_key}") from exc
