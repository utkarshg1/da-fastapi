from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from config import get_settings
from routers import aggregate, schema, stats, upload
from services.store import DatasetNotFoundError

settings = get_settings()

app = FastAPI(title=settings.app_name, version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(upload.router)
app.include_router(schema.router)
app.include_router(aggregate.router)
app.include_router(stats.router)


@app.get("/health", tags=["system"])
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.exception_handler(DatasetNotFoundError)
def dataset_not_found_handler(_: object, exc: DatasetNotFoundError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_404_NOT_FOUND,
        content={"error": "dataset_not_found", "detail": str(exc)},
    )


@app.exception_handler(ValueError)
def value_error_handler(_: object, exc: ValueError) -> JSONResponse:
    return JSONResponse(
        status_code=status.HTTP_400_BAD_REQUEST,
        content={"error": "validation_error", "detail": str(exc)},
    )


@app.exception_handler(HTTPException)
def http_exception_handler(_: object, exc: HTTPException) -> JSONResponse:
    detail = exc.detail if isinstance(exc.detail, str) else "request failed"
    return JSONResponse(
        status_code=exc.status_code,
        content={"error": "http_error", "detail": detail},
    )