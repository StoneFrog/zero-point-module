"""Centralised settings, loaded once from environment variables.

Why a single Settings class: every other module reads from here, so secrets and
URLs live in exactly one place. Swap `.env` between local/staging/prod without
touching code.
"""

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ENTSO-E
    entsoe_api_token: str = Field(default="")
    entsoe_use_fixture: bool = Field(default=True)
    entsoe_base_url: str = Field(default="https://web-api.tp.entsoe.eu/api")

    # Object storage
    s3_endpoint: str = Field(default="http://minio:9000")
    s3_region: str = Field(default="us-east-1")
    minio_root_user: str = Field(default="minioadmin")
    minio_root_password: str = Field(default="minioadmin")
    lake_bucket: str = Field(default="lake")

    # Iceberg catalog (Nessie speaks the Iceberg REST protocol)
    nessie_uri: str = Field(default="http://nessie:19120/iceberg/main")


settings = Settings()
