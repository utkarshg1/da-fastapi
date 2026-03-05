from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

import polars as pl


class DatasetNotFoundError(Exception):
    def __init__(self, dataset_id: str):
        super().__init__(f"dataset '{dataset_id}' was not found")
        self.dataset_id = dataset_id


@dataclass
class StoredDataset:
    lf: pl.LazyFrame
    file_path: Path
    created_at: datetime


_store: dict[str, StoredDataset] = {}


def save(lf: pl.LazyFrame, file_path: Path, dataset_id: str | None = None) -> str:
    ds_id = dataset_id or str(uuid4())
    _store[ds_id] = StoredDataset(lf=lf, file_path=file_path, created_at=datetime.now(timezone.utc))
    return ds_id


def get_record(dataset_id: str) -> StoredDataset:
    record = _store.get(dataset_id)
    if record is None:
        raise DatasetNotFoundError(dataset_id)
    return record


def get(dataset_id: str) -> pl.LazyFrame:
    return get_record(dataset_id).lf


def delete(dataset_id: str) -> bool:
    record = _store.pop(dataset_id, None)
    if record is None:
        return False
    if record.file_path.exists():
        record.file_path.unlink(missing_ok=True)
    return True


def list_expired(ttl_seconds: int) -> list[str]:
    now = datetime.now(timezone.utc)
    return [
        dataset_id
        for dataset_id, record in _store.items()
        if (now - record.created_at).total_seconds() > ttl_seconds
    ]


def evict_expired(ttl_seconds: int) -> list[str]:
    expired = list_expired(ttl_seconds)
    for dataset_id in expired:
        delete(dataset_id)
    return expired