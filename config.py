from functools import lru_cache
from pathlib import Path
import tempfile

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "EDA Backend"
    max_upload_mb: int = 100
    dataset_ttl_seconds: int = 3600
    cors_origins: list[str] = ["*"]
    upload_dir: Path = Path(tempfile.gettempdir()) / "eda_backend_uploads"

    model_config = SettingsConfigDict(env_prefix="EDA_", env_file=".env")


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    settings.upload_dir.mkdir(parents=True, exist_ok=True)
    return settings