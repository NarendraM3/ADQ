from typing import Any, Literal
from pydantic import BaseModel, Field


class JobCreate(BaseModel):
    name: str = Field(min_length=1, max_length=200)
    description: str | None = None
    dataset_ids: list[int] = Field(min_length=1)
    selected_columns: dict[str, list[str]] = Field(default_factory=dict)
    join_config: dict[str, Any] | None = None
    destination: dict[str, Any] = Field(default_factory=lambda: {"type": "local"})
    settings: dict[str, Any] = Field(default_factory=dict)


class JobUpdate(BaseModel):
    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = None
    dataset_ids: list[int] | None = None
    selected_columns: dict[str, list[str]] | None = None
    join_config: dict[str, Any] | None = None
    destination: dict[str, Any] | None = None
    settings: dict[str, Any] | None = None


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


class CorrectionCreate(BaseModel):
    row_index: int = Field(ge=0)
    column_name: str
    value: str | None
