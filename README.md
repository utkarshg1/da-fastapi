# da-fastapi

FastAPI backend for tabular EDA with Polars `LazyFrame` pipelines.

## Run

```bash
uv sync
uv run uvicorn main:app --reload
```

## Test

```bash
uv run pytest -q
```

## API

- `GET /health`
- `POST /datasets/upload`
- `DELETE /datasets/{dataset_id}`
- `GET /datasets/{dataset_id}/schema`
- `POST /datasets/{dataset_id}/aggregate`
- `POST /datasets/{dataset_id}/describe`
- `GET /datasets/{dataset_id}/value-counts`
- `POST /datasets/{dataset_id}/pipeline`