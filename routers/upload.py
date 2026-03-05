from pathlib import Path
from uuid import uuid4

import polars as pl
from fastapi import APIRouter, File, HTTPException, UploadFile, status

from config import get_settings
from models.responses import DatasetUploadResponse, DeleteDatasetResponse
from services import store

router = APIRouter(prefix="/datasets", tags=["datasets"])
settings = get_settings()


def _scan_file(path: Path, content_type: str | None) -> pl.LazyFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv" or content_type == "text/csv":
        return pl.scan_csv(path, try_parse_dates=True)
    if suffix == ".parquet" or content_type == "application/x-parquet":
        return pl.scan_parquet(path, use_statistics=True)
    if suffix in {".ndjson", ".jsonl"}:
        return pl.scan_ndjson(path)
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="unsupported file type; use csv, parquet, or ndjson",
    )


@router.post("/upload", response_model=DatasetUploadResponse)
async def upload_dataset(file: UploadFile = File(...)) -> DatasetUploadResponse:
    if not file.filename:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="missing filename")

    suffix = Path(file.filename).suffix.lower()
    target = settings.upload_dir / f"{uuid4()}{suffix}"

    size_bytes = 0
    with target.open("wb") as handle:
        while True:
            chunk = await file.read(1024 * 1024)
            if not chunk:
                break
            size_bytes += len(chunk)
            if size_bytes > settings.max_upload_mb * 1024 * 1024:
                target.unlink(missing_ok=True)
                raise HTTPException(
                    status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
                    detail=f"file exceeds {settings.max_upload_mb}MB limit",
                )
            handle.write(chunk)

    lf = _scan_file(target, file.content_type)
    dataset_id = store.save(lf, target)

    schema = lf.collect_schema()
    row_count = int(lf.select(pl.len().alias("count")).collect().item())
    dtypes = {name: str(dtype) for name, dtype in schema.items()}

    return DatasetUploadResponse(
        dataset_id=dataset_id,
        row_count=row_count,
        columns=list(schema.names()),
        dtypes=dtypes,
    )


@router.delete("/{dataset_id}", response_model=DeleteDatasetResponse)
def delete_dataset(dataset_id: str) -> DeleteDatasetResponse:
    deleted = store.delete(dataset_id)
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="dataset not found")
    return DeleteDatasetResponse(dataset_id=dataset_id, deleted=True)