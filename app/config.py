from pathlib import Path

from pydantic import field_validator
from pydantic_settings import BaseSettings, PydanticBaseSettingsSource, SettingsConfigDict


class Settings(BaseSettings):
    # Resolve the project file from this module, not from the backend process cwd.
    model_config = SettingsConfigDict(
        env_file=Path(__file__).resolve().parent.parent / ".env",
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

    @field_validator("secret_encryption_key", mode="before")
    @classmethod
    def normalize_secret_encryption_key(cls, value):
        if isinstance(value, str):
            value = value.strip()
            return value or None
        return value
    ai_provider: str = "none"
    ai_api_key: str | None = None
    groq_api_key: str | None = None
    groq_model: str | None = None
    grok_api_key: str | None = None
    grok_model: str | None = None
    ai_base_url: str | None = None
    ai_huggingface_base_url: str | None = None
    ai_groq_base_url: str | None = None
    ai_grok_base_url: str | None = None
    onelake_account_url: str | None = None
    azure_blob_account_url_template: str | None = None
    ai_model: str | None = None
    ai_timeout_seconds: int = 45
    log_level: str = "INFO"

    @property
    def resolved_ai_base_url(self) -> str | None:
        provider = self.ai_provider.strip().lower()
        if provider == "groq":
            return self.ai_base_url or self.ai_groq_base_url or "https://api.groq.com/openai/v1"
        if self.ai_base_url:
            return self.ai_base_url
        return {"huggingface":self.ai_huggingface_base_url,
                "grok":self.ai_grok_base_url or "https://api.x.ai/v1"}.get(provider)

    @property
    def resolved_ai_api_key(self) -> str | None:
        provider = self.ai_provider.strip().lower()
        if provider == "groq": return self.groq_api_key or self.ai_api_key
        if provider == "grok": return self.grok_api_key or self.ai_api_key
        return self.ai_api_key

    @property
    def resolved_ai_model(self) -> str | None:
        provider = self.ai_provider.strip().lower()
        if provider == "groq": return self.groq_model or self.ai_model
        if provider == "grok": return self.grok_model or self.ai_model
        return self.ai_model


settings = Settings()
