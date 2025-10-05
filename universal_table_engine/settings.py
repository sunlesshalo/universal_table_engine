from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class AppSettings(BaseSettings):
    """Application configuration loaded from environment variables (prefix: UTE_)."""

    # Pydantic v2: csak model_config, NINCS class Config
    model_config = SettingsConfigDict(
        env_prefix="UTE_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="allow",
        json_schema_extra={
            "description": "Application configuration loaded from environment variables prefixed with UTE_."
        },
    )

    # Core
    app_name: str = Field(default="universal-table-engine")
    environment: Literal["development", "staging", "production"] = Field(default="development")
    host: str = Field(default="0.0.0.0")
    port: int = Field(default=8000)
    log_level: str = Field(default="INFO")

    # LLM
    enable_llm: bool = Field(default=False)
    llm_provider: Optional[Literal["openai", "vertex", "anthropic", "mock"]] = Field(default=None)
    llm_api_key: Optional[str] = Field(default=None)
    llm_model: str = Field(default="gpt-4o-mini")
    llm_timeout_seconds: float = Field(default=15.0)
    llm_max_retries: int = Field(default=2)

    # Adapters
    default_adapter: Literal["json", "sheets", "bigquery", "none"] = Field(default="json")
    enable_json_adapter: bool = Field(default=True)
    enable_sheets_adapter: bool = Field(default=False)
    enable_bigquery_adapter: bool = Field(default=False)

    # IO / limits
    output_dir: Path = Field(default=Path("out"))
    persist_logs: bool = Field(default=False)
    max_upload_size_mb: int = Field(default=100)
    csv_sample_rows: int = Field(default=50)
    header_search_rows: int = Field(default=50)

    # Webhook intake
    webhook_enable: bool = Field(default=True)
    webhook_max_upload_size_mb: int = Field(default=100)
    webhook_clock_skew_seconds: int = Field(default=300)
    webhook_require_auth: bool = Field(default=True)
    webhook_api_keys: dict[str, str] = Field(default_factory=dict)
    webhook_hmac_secrets: dict[str, str] = Field(default_factory=dict)
    webhook_allowed_ips: list[str] = Field(default_factory=list)
    webhook_async_default: bool = Field(default=False)

    # Google Sheets
    sheets_spreadsheet_id: Optional[str] = Field(default=None)
    sheets_service_account_file: Optional[Path] = Field(default=None)
    sheets_mode: Literal["append", "replace"] = Field(default="append")

    # BigQuery
    bigquery_project: Optional[str] = Field(default=None)
    bigquery_dataset: Optional[str] = Field(default=None)
    bigquery_table: Optional[str] = Field(default=None)
    bigquery_location: Optional[str] = Field(default=None)

    # Misc
    mask_pii: bool = Field(default=False)
    rules_dir: Path = Field(default=Path(__file__).resolve().parent / "rules")
    presets_dir: Path = Field(default=Path("presets"))
    metrics_namespace: str = Field(default="ute")


@lru_cache(maxsize=1)
def get_settings() -> AppSettings:
    s = AppSettings()
    # Normalize/ensure paths
    s.output_dir = s.output_dir.resolve()
    s.rules_dir = s.rules_dir.resolve()
    s.presets_dir = s.presets_dir.resolve()
    if s.persist_logs:
        s.output_dir.mkdir(parents=True, exist_ok=True)
    s.presets_dir.mkdir(parents=True, exist_ok=True)
    return s


__all__ = ["AppSettings", "get_settings"]
