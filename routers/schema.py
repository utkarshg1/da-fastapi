import polars as pl
from fastapi import APIRouter

from models.responses import DatasetSchemaResponse, SchemaColumn
from services import query_builder, store

router = APIRouter(prefix="/datasets", tags=["schema"])


@router.get("/{dataset_id}/schema", response_model=DatasetSchemaResponse)
def get_schema(dataset_id: str) -> DatasetSchemaResponse:
    lf = store.get(dataset_id)
    schema = lf.collect_schema()

    columns: list[SchemaColumn] = []
    for name, dtype in schema.items():
        role = query_builder.assign_role(dtype)
        n_unique = None
        if role == "categorical":
            n_unique = int(lf.select(pl.col(name).n_unique()).collect().item())
        columns.append(SchemaColumn(name=name, dtype=str(dtype), role=role, n_unique=n_unique))

    return DatasetSchemaResponse(dataset_id=dataset_id, columns=columns)