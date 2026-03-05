from typing import Any

from pydantic import BaseModel


class DatasetUploadResponse(BaseModel):
    dataset_id: str
    row_count: int
    columns: list[str]
    dtypes: dict[str, str]


class DeleteDatasetResponse(BaseModel):
    dataset_id: str
    deleted: bool


class SchemaColumn(BaseModel):
    name: str
    dtype: str
    role: str
    n_unique: int | None = None


class DatasetSchemaResponse(BaseModel):
    dataset_id: str
    columns: list[SchemaColumn]


class AggregationResponse(BaseModel):
    dataset_id: str
    group_by: list[str]
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    elapsed_ms: float


class DescribeResponse(BaseModel):
    dataset_id: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int


class ValueCountsResponse(BaseModel):
    dataset_id: str
    column: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int


class PipelineResponse(BaseModel):
    dataset_id: str
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    elapsed_ms: float