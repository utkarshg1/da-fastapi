from time import perf_counter

from fastapi import APIRouter

from models.requests import AggregationRequest, PipelineRequest
from models.responses import AggregationResponse, PipelineResponse
from services import query_builder, serializer, store

router = APIRouter(prefix="/datasets", tags=["aggregate"])


def _validate_aggregation_request(request: AggregationRequest, dataset_id: str) -> None:
    schema = store.get(dataset_id).collect_schema()
    available = set(schema.names())

    query_builder.validate_columns(available, request.group_by, "group_by")
    query_builder.validate_columns(available, [s.column for s in request.aggregations], "aggregation")

    filters = request.filters or []
    query_builder.validate_columns(available, [f.column for f in filters], "filter")


@router.post("/{dataset_id}/aggregate", response_model=AggregationResponse)
def run_aggregate(dataset_id: str, request: AggregationRequest) -> AggregationResponse:
    _validate_aggregation_request(request, dataset_id)

    start = perf_counter()
    lf = store.get(dataset_id)
    result_df = query_builder.run_groupby(lf, request)
    payload = serializer.serialize_dataframe(result_df)
    elapsed_ms = (perf_counter() - start) * 1000

    return AggregationResponse(
        dataset_id=dataset_id,
        group_by=request.group_by,
        columns=payload["columns"],
        rows=payload["rows"],
        row_count=payload["row_count"],
        elapsed_ms=round(elapsed_ms, 3),
    )


@router.post("/{dataset_id}/pipeline", response_model=PipelineResponse)
def run_pipeline(dataset_id: str, request: PipelineRequest) -> PipelineResponse:
    lf = store.get(dataset_id)
    schema = lf.collect_schema()
    available = set(schema.names())

    start = perf_counter()

    for step in request.steps:
        if step.step == "filter":
            filters = step.params.get("filters", [])
            parsed_filters = query_builder.parse_filters(filters)
            query_builder.validate_columns(available, [f.column for f in parsed_filters], "filter")
            lf = query_builder.apply_filters(lf, parsed_filters)

        elif step.step == "aggregate":
            group_by = step.params.get("group_by", [])
            aggregations = query_builder.parse_aggregations(step.params.get("aggregations", []))
            query_builder.validate_columns(available, group_by, "group_by")
            query_builder.validate_columns(available, [a.column for a in aggregations], "aggregation")
            exprs = query_builder.build_agg_exprs(aggregations)
            if group_by:
                lf = lf.group_by(group_by).agg(exprs)
            else:
                lf = lf.select(exprs)
            available = set(lf.collect_schema().names())

        elif step.step == "sort":
            by = step.params.get("by")
            if not by:
                raise ValueError("sort step requires 'by'")
            if by not in available:
                raise ValueError(f"unknown sort column: {by}")
            desc = bool(step.params.get("desc", False))
            lf = lf.sort(by, descending=desc)

        elif step.step == "limit":
            limit = int(step.params.get("n", 0))
            if limit <= 0:
                raise ValueError("limit step requires positive 'n'")
            lf = lf.limit(limit)

        elif step.step == "slice":
            offset = int(step.params.get("offset", 0))
            length = step.params.get("limit")
            if length is None:
                lf = lf.slice(offset)
            else:
                lf = lf.slice(offset, int(length))

    result_df = lf.collect()
    payload = serializer.serialize_dataframe(result_df)
    elapsed_ms = (perf_counter() - start) * 1000

    return PipelineResponse(
        dataset_id=dataset_id,
        columns=payload["columns"],
        rows=payload["rows"],
        row_count=payload["row_count"],
        elapsed_ms=round(elapsed_ms, 3),
    )