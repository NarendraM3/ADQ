from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class Settings(BaseSettings):
    # Resolve project dotenv files from this module, not from the backend process cwd.
    model_config = SettingsConfigDict(
        # Prefer backend/.env when present, while preserving the existing root
        # .env used by local project tools.
        env_file=(Path(__file__).resolve().parents[2] / ".env",
                  Path(__file__).resolve().parents[1] / ".env"),
        env_file_encoding="utf-8-sig",
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ):
        # A blank process variable must not shadow a usable key in the project
        # dotenv file. Keep the usual source order for every nonblank value.
        def nonblank_environment():
            values = env_settings()
            if isinstance(values.get("secret_encryption_key"), str) and not values["secret_encryption_key"].strip():
                values.pop("secret_encryption_key")
            return values

        return init_settings, nonblank_environment, dotenv_settings, file_secret_settings

    database_url: str = "sqlite:///./dq.db"
    local_data_dir: str = "./data"
    max_upload_mb: int = 200
    secret_encryption_key: str | None = None
    databricks_server_hostname: str | None = None
    databricks_http_path: str | None = None
    databricks_token: str | None = None
    databricks_catalog: str = "dq_catalog"
    databricks_schema: str = "dq_schema"
    fabric_api_base: str = "https://api.fabric.microsoft.com/v1"
    fabric_workspace_id: str | None = None
    fabric_lakehouse_id: str | None = None
    fabric_tenant_id: str | None = None
    fabric_client_id: str | None = None
    fabric_client_secret: str | None = None
    fabric_source_directory: str = "Files/dq/source"
    fabric_output_directory: str | None = None
    fabric_source_table: str = "dq_source"
    fabric_pass_table: str = "dq_pass_v2"
    fabric_fail_table: str = "dq_fail_v2"
    fabric_ignored_table: str = "dq_ignored_v2"
    fabric_job_poll_seconds: int = 2
    fabric_job_timeout_seconds: int = 900
    fabric_sql_server: str | None = None
    fabric_sql_database: str | None = None
    snowflake_account: str | None = None
    snowflake_user: str | None = None
    snowflake_password: str | None = None
    snowflake_warehouse: str | None = None
    snowflake_database: str | None = None
    snowflake_schema: str = "PUBLIC"

    @field_validator("secret_encryption_key", mode="before")
    @classmethod
    def normalize_secret_encryption_key(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value
    ai_provider: str = "openai"
    ai_api_key: str | None = None
    openai_api_key: str | None = None
    openai_model: str = "gpt-5.5"
    grok_api_key: str | None = None
    grok_model: str | None = None
    ai_base_url: str | None = None
    ai_huggingface_base_url: str | None = None
    ai_grok_base_url: str | None = None
    onelake_account_url: str | None = None
    azure_blob_account_url_template: str | None = None
    ai_model: str | None = None
    ai_timeout_seconds: int = 180
    ai_suggestion_batch_size: int = 48
    ai_suggestion_concurrency: int = 4
    ai_correction_request_batch_size: int = 24
    source_fetch_batch_size: int = 1000
    source_connect_timeout_seconds: int = 10
    source_read_timeout_seconds: int = 10
    source_discovery_retries: int = 2
    source_discovery_workers: int = 4
    destination_retry_attempts: int = 3
    destination_statement_timeout_seconds: int = 60
    destination_insert_batch_size: int = 250
    log_level: str = "INFO"

    @property
    def resolved_ai_base_url(self) -> str | None:
        return "https://api.openai.com/v1"

    @property
    def resolved_ai_api_key(self) -> str | None:
        return self.openai_api_key

    @property
    def resolved_ai_model(self) -> str | None:
        return self.openai_model or "gpt-5.5"


settings = Settings()
