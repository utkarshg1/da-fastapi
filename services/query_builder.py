from typing import Any

import polars as pl

from models.requests import AggregationRequest, AggregationSpec, FilterSpec


def assign_role(dtype: pl.DataType) -> str:
    if dtype in {pl.String, pl.Categorical, pl.Boolean} or str(dtype).startswith("Enum"):
        return "categorical"
    if dtype.is_numeric() or dtype == pl.Decimal:
        return "numeric"
    if dtype.is_temporal() or dtype in {pl.Duration, pl.Time}:
        return "temporal"
    return "other"


def _build_filter_expr(filter_spec: FilterSpec) -> pl.Expr:
    column_expr = pl.col(filter_spec.column)
    op = filter_spec.operator
    value = filter_spec.value

    if op == "eq":
        return column_expr == value
    if op == "neq":
        return column_expr != value
    if op == "gt":
        return column_expr > value
    if op == "gte":
        return column_expr >= value
    if op == "lt":
        return column_expr < value
    if op == "lte":
        return column_expr <= value
    if op == "in":
        if not isinstance(value, list):
            raise ValueError("operator 'in' requires a list value")
        return column_expr.is_in(value)
    if op == "not_in":
        if not isinstance(value, list):
            raise ValueError("operator 'not_in' requires a list value")
        return ~column_expr.is_in(value)
    if op == "is_null":
        return column_expr.is_null()
    if op == "is_not_null":
        return column_expr.is_not_null()
    raise ValueError(f"unsupported operator: {op}")


def apply_filters(lf: pl.LazyFrame, filters: list[FilterSpec]) -> pl.LazyFrame:
    for spec in filters:
        lf = lf.filter(_build_filter_expr(spec))
    return lf


def build_agg_exprs(specs: list[AggregationSpec]) -> list[pl.Expr]:
    fn_map = {
        "sum": lambda col: col.sum(),
        "mean": lambda col: col.mean(),
        "median": lambda col: col.median(),
        "min": lambda col: col.min(),
        "max": lambda col: col.max(),
        "count": lambda col: col.count(),
        "n_unique": lambda col: col.n_unique(),
        "std": lambda col: col.std(),
        "var": lambda col: col.var(),
        "first": lambda col: col.first(),
        "last": lambda col: col.last(),
    }
    expressions: list[pl.Expr] = []
    for spec in specs:
        if spec.function not in fn_map:
            raise ValueError(f"unsupported aggregation: {spec.function}")
        expr = fn_map[spec.function](pl.col(spec.column))
        if spec.alias:
            expr = expr.alias(spec.alias)
        expressions.append(expr)
    return expressions


def validate_columns(available_columns: set[str], columns: list[str], label: str) -> None:
    unknown = sorted(set(columns) - available_columns)
    if unknown:
        raise ValueError(f"unknown {label} columns: {', '.join(unknown)}")


def run_groupby(lf: pl.LazyFrame, request: AggregationRequest) -> pl.DataFrame:
    filters = request.filters or []
    lf = apply_filters(lf, filters)

    exprs = build_agg_exprs(request.aggregations)
    if request.group_by:
        lf = lf.group_by(request.group_by).agg(exprs)
    else:
        lf = lf.select(exprs)

    if request.order_by:
        lf = lf.sort(request.order_by, descending=request.order_desc)

    if request.offset:
        if request.limit:
            lf = lf.slice(request.offset, request.limit)
        else:
            lf = lf.slice(request.offset)
    elif request.limit:
        lf = lf.limit(request.limit)

    return lf.collect()


def parse_filters(filters: list[dict[str, Any]] | list[FilterSpec] | None) -> list[FilterSpec]:
    if not filters:
        return []
    parsed: list[FilterSpec] = []
    for item in filters:
        if isinstance(item, FilterSpec):
            parsed.append(item)
        else:
            parsed.append(FilterSpec.model_validate(item))
    return parsed


def parse_aggregations(specs: list[dict[str, Any]] | list[AggregationSpec]) -> list[AggregationSpec]:
    parsed: list[AggregationSpec] = []
    for item in specs:
        if isinstance(item, AggregationSpec):
            parsed.append(item)
        else:
            parsed.append(AggregationSpec.model_validate(item))
    return parsed