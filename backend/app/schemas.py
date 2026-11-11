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
    dataset_ids: list[int] = Field(default_factory=list)
    selected_columns: dict[str, list[str]] = Field(default_factory=dict)
    join_config: dict[str, Any] | None = None
    destination: dict[str, Any] = Field(default_factory=lambda: {"type": "local"})
    environment: str | None = None
    settings: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name", mode="before")
    @classmethod
    def trim_name(cls, value):
        return value.strip() if isinstance(value, str) else value

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip() or None
        return value

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

    @field_validator("name", mode="before")
    @classmethod
    def trim_name(cls, value):
        return value.strip() if isinstance(value, str) else value

    @field_validator("description", mode="before")
    @classmethod
    def normalize_description(cls, value):
        if value is None:
            return None
        if isinstance(value, str):
            return value.strip() or None
        return value

    @field_validator("environment", mode="before")
    @classmethod
    def normalize_environment(cls, value):
        return normalize_platform(value)


class RuleCreate(BaseModel):
    dataset_id: int | None = None
    column_name: str = Field(min_length=1, max_length=255)
    rule_type: str = Field(min_length=1, max_length=50)
    parameters: dict[str, Any] = Field(default_factory=dict)
    enabled: bool = False
    approved: bool = False

    @field_validator("column_name", "rule_type", mode="before")
    @classmethod
    def trim_required_strings(cls, value):
        return value.strip() if isinstance(value, str) else value


class JobConfiguration(BaseModel):
    job: JobCreate
    rules: list[RuleCreate] = Field(default_factory=list)


class RuleUpdate(BaseModel):
    column_name: str | None = None
    rule_type: str | None = None
    parameters: dict[str, Any] | None = None
    enabled: bool | None = None
    approved: bool | None = None

    @field_validator("column_name", "rule_type", mode="before")
    @classmethod
    def trim_optional_strings(cls, value):
        return value.strip() or None if isinstance(value, str) else value


class CredentialCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    provider: Literal["s3", "azure_blob", "onelake", "snowflake", "databricks", "fabric", "sqlserver", "mysql", "postgresql"]
    secrets: dict[str, Any]
    config: dict[str, Any] = Field(default_factory=dict)

    @field_validator("name", mode="before")
    @classmethod
    def trim_name(cls, value):
        return value.strip() if isinstance(value, str) else value

    @field_validator("config", mode="before")
    @classmethod
    def trim_config_strings(cls, value):
        if isinstance(value, dict):
            return {key: item.strip() if isinstance(item, str) else item for key, item in value.items()}
        return value

    @model_validator(mode="after")
    def validate_s3_connection(self):
        if self.provider != "s3":
            return self
        bucket = self.config.get("bucket")
        region = self.config.get("region")
        if not isinstance(bucket, str) or not bucket or not isinstance(region, str) or not region:
            raise ValueError("S3 connections require a bucket name and AWS region")
        auth_method = self.config.get("auth_method", "access_key")
        if auth_method not in ("access_key", "default_chain"):
            raise ValueError("S3 auth_method must be access_key or default_chain")
        access_key = self.secrets.get("access_key_id")
        secret_key = self.secrets.get("secret_access_key")
        if auth_method == "access_key" and (not isinstance(access_key, str) or not access_key.strip() or
                                               not isinstance(secret_key, str) or not secret_key.strip()):
            raise ValueError("Access key authentication requires an access key ID and secret access key")
        if auth_method == "default_chain" and (self.secrets.get("access_key_id") or self.secrets.get("secret_access_key")):
            raise ValueError("Do not include access keys when using default AWS credentials")
        return self

    @model_validator(mode="after")
    def validate_database_connection(self):
        if self.provider not in ("sqlserver", "mysql", "postgresql"):
            return self
        required = ("host", "database", "username")
        if any(not isinstance(self.config.get(key), str) or not self.config[key].strip() for key in required):
            raise ValueError("Database connections require a server, database name, and username")
        if not isinstance(self.secrets.get("password"), str) or not self.secrets["password"]:
            raise ValueError("Database connections require a password")
        port = self.config.get("port")
        if port is not None and (not isinstance(port, int) or port < 1 or port > 65535):
            raise ValueError("Database port must be between 1 and 65535")
        return self


class CorrectionCreate(BaseModel):
    row_index: int = Field(ge=0)
    column_name: str = Field(min_length=1, max_length=255)
    value: str | None
    original_value: str | None = None

    @field_validator("column_name", mode="before")
    @classmethod
    def trim_column_name(cls, value):
        return value.strip() if isinstance(value, str) else value
