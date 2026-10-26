from typing import Any, Literal
from pydantic import BaseModel, Field, field_validator, model_validator


PLATFORM_NAMES = {
    "databricks": "Databricks",
    "snowflake": "Snowflake",
    "microsoft fabric": "Microsoft Fabric",
    "microsoftfabric": "Microsoft Fabric",
    "fabric": "Microsoft Fabric",
    "onelake": "Microsoft Fabric",
    "local": "Local",
    "s3": "Amazon S3",
    "amazon s3": "Amazon S3",
    "azure_blob": "Azure Blob Storage",
    "azure blob": "Azure Blob Storage",
    "azure blob storage": "Azure Blob Storage",
}


def normalize_platform(value: str | None) -> str | None:
    if value is None or (isinstance(value, str) and not value.strip()):
        return None
    if not isinstance(value, str):
        return value
    normalized = " ".join(value.strip().replace("_", " ").split()).casefold()
    return PLATFORM_NAMES.get(normalized, value.strip())


class JobCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    dataset_ids: list[int] = Field(min_length=1)
    selected_columns: dict[str, list[str]] = Field(default_factory=dict)
    join_config: dict[str, Any] | None = None
    destination: dict[str, Any] = Field(default_factory=lambda: {"type": "local"})
    environment: str | None = None
    settings: dict[str, Any] = Field(default_factory=dict)

    @field_validator("environment", mode="before")
    @classmethod
    def normalize_environment(cls, value):
        return normalize_platform(value)


class JobUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = None
    dataset_ids: list[int] | None = None
    selected_columns: dict[str, list[str]] | None = None
    join_config: dict[str, Any] | None = None
    destination: dict[str, Any] | None = None
    environment: str | None = None
    settings: dict[str, Any] | None = None

    @field_validator("environment", mode="before")
    @classmethod
    def normalize_environment(cls, value):
        return normalize_platform(value)


class RuleCreate(BaseModel):
    dataset_id: int | None = None
    column_name: str = Field(min_length=1, max_length=255)
    rule_type: str
    parameters: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = False
    approved: bool = False


class JobConfiguration(BaseModel):
    job: JobCreate
    rules: list[RuleCreate] = Field(default_factory=list)


class RuleUpdate(BaseModel):
    column_name: str | None = None
    rule_type: str | None = None
    parameters: dict[str, Any] | None = None
    enabled: bool | None = None
    approved: bool | None = None


class CredentialCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    provider: Literal["s3", "azure_blob", "onelake", "snowflake", "databricks", "fabric"]
    secrets: dict[str, Any]
    config: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_s3_connection(self):
        if self.provider != "s3":
            return self
        if not self.config.get("bucket") or not self.config.get("region"):
            raise ValueError("S3 connections require a bucket name and AWS region")
        auth_method = self.config.get("auth_method", "access_key")
        if auth_method not in ("access_key", "default_chain"):
            raise ValueError("S3 auth_method must be access_key or default_chain")
        if auth_method == "access_key" and (not self.secrets.get("access_key_id") or not self.secrets.get("secret_access_key")):
            raise ValueError("Access key authentication requires an access key ID and secret access key")
        if auth_method == "default_chain" and (self.secrets.get("access_key_id") or self.secrets.get("secret_access_key")):
            raise ValueError("Do not include access keys when using default AWS credentials")
        return self


class CorrectionCreate(BaseModel):
    row_index: int = Field(ge=0)
    column_name: str
    value: str | None
