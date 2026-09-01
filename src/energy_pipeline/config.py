"""Centralised settings, loaded once from environment variables.

Why a single Settings class: every other module reads from here, so secrets and
URLs live in exactly one place. Swap `.env` between local/staging/prod without
touching code.
"""

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # ENTSO-E
    # SecretStr, not str: pydantic renders it as `**********` in reprs, so a
    # stray `print(settings)` / logged model dump can't spill the credential.
    # Call .get_secret_value() at the one place that actually needs it
    # (EntsoeResource).
    entsoe_api_token: SecretStr = Field(default=SecretStr(""))
    entsoe_use_fixture: bool = Field(default=True)
    entsoe_base_url: str = Field(default="https://web-api.tp.entsoe.eu/api")

    # Object storage
    s3_endpoint: str = Field(default="http://minio:9000")
    s3_region: str = Field(default="us-east-1")
    minio_root_user: str = Field(default="minioadmin")
    # SecretStr for the same reason as entsoe_api_token above: this is the S3
    # secret access key, and it is handed to s3fs/PyIceberg in several places.
    minio_root_password: SecretStr = Field(default=SecretStr("minioadmin"))
    lake_bucket: str = Field(default="lake")

    # Iceberg catalog (Apache Iceberg REST reference catalog).
    iceberg_catalog_uri: str = Field(default="http://iceberg-catalog:8181")

    # Operational alerting (Phase 2). Optional generic webhook URL, POSTed a
    # small JSON payload on pipeline-run failure. Works as-is with Slack
    # incoming webhooks (which accept a top-level "text" field); left empty
    # by default so the pipeline runs with no external dependency — failures
    # still show up in the Dagster UI and daemon logs either way.
    # SecretStr: for Slack-style incoming webhooks the URL *is* the
    # credential — anyone holding it can post to the channel. httpx puts the
    # full URL in its error messages and sensors.py logs those, so treat it
    # like a password rather than a config value.
    alert_webhook_url: SecretStr = Field(default=SecretStr(""))


settings = Settings()
