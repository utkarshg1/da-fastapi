from typing import Any, Literal

from pydantic import BaseModel, Field


class AggregationSpec(BaseModel):
    column: str
    function: Literal[
        "sum",
        "mean",
        "median",
        "min",
        "max",
        "count",
        "n_unique",
        "std",
        "var",
        "first",
        "last",
    ]
    alias: str | None = None


class FilterSpec(BaseModel):
    column: str
    operator: Literal[
        "eq",
        "neq",
        "gt",
        "gte",
        "lt",
        "lte",
        "in",
        "not_in",
        "is_null",
        "is_not_null",
    ]
    value: Any | None = None


class AggregationRequest(BaseModel):
    group_by: list[str] = Field(default_factory=list)
    aggregations: list[AggregationSpec]
    filters: list[FilterSpec] | None = None
    order_by: str | None = None
    order_desc: bool = False
    limit: int | None = Field(default=None, ge=1)
    offset: int = Field(default=0, ge=0)


class DescribeRequest(BaseModel):
    columns: list[str] | None = None


class PipelineStep(BaseModel):
    step: Literal["filter", "aggregate", "sort", "limit", "slice"]
    params: dict[str, Any] = Field(default_factory=dict)


class PipelineRequest(BaseModel):
    steps: list[PipelineStep]