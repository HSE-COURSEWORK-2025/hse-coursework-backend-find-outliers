from pydantic_settings import BaseSettings


class DbSettings(BaseSettings):
    DB_ENGINE: str | None = "postgresql"
    # DB_HOST: str = "172.16.57.2"
    DB_HOST: str | None = "192.168.0.180"

    DB_PORT: int | None = 5432
    # DB_USER: str = "services_user"
    # DB_PASSWORD: str = "services_password"
    # DB_NAME: str = "services"

    DB_USER: str | None = "postgres"
    DB_PASSWORD: str | None = "postgres"
    DB_NAME: str | None = "records"

    class Config:
        env_file = ".env"
        # env_file = ".env.development"
        env_file_encoding = "utf-8"
        case_sensitive = False
        env_nested_delimiter = "__"
        extra = "allow"


settings = DbSettings()
