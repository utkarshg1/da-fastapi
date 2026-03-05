from datetime import date, datetime, time, timedelta
from decimal import Decimal
import math
from typing import Any

import polars as pl


def _json_safe(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def serialize_dataframe(df: pl.DataFrame) -> dict[str, Any]:
    columns = df.columns
    rows = [[_json_safe(v) for v in row] for row in df.iter_rows()]
    return {"columns": columns, "rows": rows, "row_count": df.height}