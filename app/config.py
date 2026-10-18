from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")
    database_url: str = "sqlite:///./dq.db"
    local_data_dir: str = "./data"
    max_upload_mb: int = 200
    secret_encryption_key: str | None = None
    ai_provider: str = "none"
    ai_api_key: str | None = None
    groq_api_key: str | None = None
    groq_model: str | None = None
    ai_base_url: str | None = None
    ai_huggingface_base_url: str | None = None
    ai_groq_base_url: str | None = None
    onelake_account_url: str | None = None
    azure_blob_account_url_template: str | None = None
    ai_model: str | None = None
    ai_timeout_seconds: int = 45
    log_level: str = "INFO"

    @property
    def resolved_ai_base_url(self) -> str | None:
        if self.ai_base_url:
            return self.ai_base_url
        return {"huggingface":self.ai_huggingface_base_url, "groq":self.ai_groq_base_url}.get(self.ai_provider.lower())

    @property
    def resolved_ai_api_key(self) -> str | None:
        return self.ai_api_key or (self.groq_api_key if self.ai_provider.lower() == "groq" else None)

    @property
    def resolved_ai_model(self) -> str | None:
        return self.ai_model or (self.groq_model if self.ai_provider.lower() == "groq" else None)


settings = Settings()
