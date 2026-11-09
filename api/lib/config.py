"""Configuration management for the API."""
import os
from typing import Optional


class Settings:
    """Application settings loaded from environment variables."""

    # ClickHouse configuration
    CLICKHOUSE_HOST: str = os.getenv("API_CLICKHOUSE_HOST", "clickhouse")
    CLICKHOUSE_PORT: int = int(os.getenv("API_CLICKHOUSE_PORT", "8123"))
    CLICKHOUSE_USER: str = os.getenv("API_CLICKHOUSE_USER", "api")
    CLICKHOUSE_PASSWORD: str = os.getenv("API_CLICKHOUSE_PASSWORD", "")
    CLICKHOUSE_DB: str = os.getenv("API_CLICKHOUSE_DB", "api")

    # Rate limiting configuration
    RATE_LIMIT_PER_MINUTE: int = int(os.getenv("API_RATE_LIMIT_PER_MINUTE", "100"))

    # Cache configuration
    CACHE_TTL_SECONDS: int = int(os.getenv("API_CACHE_TTL_SECONDS", "3600"))  # 1 hour

    # API configuration
    API_HOST: str = os.getenv("API_HOST", "0.0.0.0")
    API_PORT: int = int(os.getenv("API_PORT", "8080"))


settings = Settings()

