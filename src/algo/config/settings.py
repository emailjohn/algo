# src/algo/config/settings.py
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_prefix="ALGO_", extra="ignore")

    project_root: Path = Path(__file__).resolve().parents[3]  # .../repo
    data_dir: Path = project_root / "data"
    artifacts_dir: Path = project_root / "artifacts"


settings = Settings()
