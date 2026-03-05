import polars as pl
from fastapi import APIRouter, Query

from models.requests import DescribeRequest
from models.responses import DescribeResponse, ValueCountsResponse
from services import query_builder, serializer, store

router = APIRouter(prefix="/datasets", tags=["stats"])


@router.post("/{dataset_id}/describe", response_model=DescribeResponse)
def describe_dataset(dataset_id: str, request: DescribeRequest) -> DescribeResponse:
    lf = store.get(dataset_id)
    schema = lf.collect_schema()
    available = set(schema.names())

    columns = request.columns or list(available)
    query_builder.validate_columns(available, columns, "describe")

    df = lf.select(columns).collect().describe()
    payload = serializer.serialize_dataframe(df)

    return DescribeResponse(
        dataset_id=dataset_id,
        columns=payload["columns"],
        rows=payload["rows"],
        row_count=payload["row_count"],
    )


@router.get("/{dataset_id}/value-counts", response_model=ValueCountsResponse)
def value_counts(dataset_id: str, column: str = Query(...), limit: int = Query(100, ge=1)) -> ValueCountsResponse:
    lf = store.get(dataset_id)
    schema = lf.collect_schema()
    available = set(schema.names())
    query_builder.validate_columns(available, [column], "value-counts")

    df = (
        lf.select(pl.col(column))
        .group_by(column)
        .agg(pl.len().alias("count"))
        .sort("count", descending=True)
        .limit(limit)
        .collect()
    )
    payload = serializer.serialize_dataframe(df)

    return ValueCountsResponse(
        dataset_id=dataset_id,
        column=column,
        columns=payload["columns"],
        rows=payload["rows"],
        row_count=payload["row_count"],
    )