"""Общие настройки для приложения."""

import time
from pydantic import model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from typing import List, Dict


class Settings(BaseSettings):
    """Конфиг с переменными окружения."""

    CHUNK_DURATION_MS: int | None = 30 * 24 * 60 * 60 * 1000

    REDIS_HOST: str | None = "redis"
    REDIS_PORT: str | None = "6379"
    REDIS_FIND_OUTLIERS_JOB_IS_ACTIVE_NAMESPACE: str | None = "REDIS_FIND_OUTLIERS_JOB_IS_ACTIVE_NAMESPACE-"

    START_MS: int | None = int((time.time() - 360 * 24 * 60 * 60) * 1000)
    # START_MS: int | None = 0
    END_MS: int | None = int(time.time() * 1000)

    DOMAIN_NAME: str | None = "http://hse-coursework-health.ru"
    AUTH_API_BASE_URL: str | None = f"{DOMAIN_NAME}:8081"
    DATA_COLLECTION_API_BASE_URL: str | None = f"{DOMAIN_NAME}:8082"

    NOTIFICATIONS_API_BASE_URL: str = "http://notifications-api:8083/notifications-api/api/v1/notifications"

    model_config = SettingsConfigDict(
        env_file=".env.prod",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore",  # игнорировать лишние переменные в .env
    )


settings = Settings()
