"""Platform adapters for resolving a job's saved execution environment."""
import json
from pathlib import PurePosixPath

from cryptography.fernet import Fernet
from sqlalchemy.orm import Session

from app.config import settings
from app.models import Credential


PLATFORM_DESTINATIONS = {
    "databricks": "databricks",
    "snowflake": "snowflake",
    "fabric": "fabric",
    "microsoft fabric": "fabric",
    "microsoftfabric": "fabric",
    "onelake": "fabric",
}


def platform_key(value) -> str | None:
    if value is None:
        return None
    key = " ".join(str(value).strip().replace("_", " ").replace("-", " ").split()).casefold()
    return PLATFORM_DESTINATIONS.get(key)


def destination_for_job(job) -> dict:
    """Resolve a job's persisted destination without global credential fallback."""
    destination = dict(job.destination or {"type": "local"})
    if destination.get("type", "local") == "local":
        settings_snapshot = job.settings if isinstance(job.settings, dict) else {}
        saved_platform = job.environment or settings_snapshot.get("environment") or settings_snapshot.get("platform")
        destination_type = platform_key(saved_platform)
        if destination_type:
            destination["type"] = destination_type
    return destination


class PlatformService:
    credential_providers: set[str]

    def resolve_destination(self, destination: dict) -> tuple[dict, dict]:
        raise NotImplementedError


class DatabricksPlatform(PlatformService):
    credential_providers = {"databricks"}

    def resolve_destination(self, destination: dict) -> tuple[dict, dict]:
        resolved = {
            **destination,
            "server_hostname": destination.get("server_hostname") or settings.databricks_server_hostname,
            "http_path": destination.get("http_path") or settings.databricks_http_path,
            "catalog": destination.get("catalog") or settings.databricks_catalog,
            "schema": destination.get("schema") or settings.databricks_schema,
        }
        secrets = {"access_token": settings.databricks_token} if settings.databricks_token else {}
        missing = [name for name, value in (
            ("DATABRICKS_SERVER_HOSTNAME", resolved.get("server_hostname")),
            ("DATABRICKS_HTTP_PATH", resolved.get("http_path")),
            ("DATABRICKS_TOKEN", secrets.get("access_token")),
        ) if not value]
        if missing:
            raise ValueError("Databricks destination requires a saved Databricks credential or these environment variables: " + ", ".join(missing))
        return resolved, secrets


class FabricPlatform(PlatformService):
    credential_providers = {"fabric", "onelake"}

    def resolve_destination(self, destination: dict) -> tuple[dict, dict]:
        directory = destination.get("directory") or settings.fabric_output_directory or _default_fabric_output_directory()
        resolved = {
            **destination,
            "type": "fabric",
            "provider": "onelake",
            "workspace_id": destination.get("workspace_id") or settings.fabric_workspace_id,
            "lakehouse_id": destination.get("lakehouse_id") or settings.fabric_lakehouse_id,
            "directory": _normalize_onelake_directory(directory),
            "account_url": destination.get("account_url") or settings.onelake_account_url,
            "api_base": destination.get("api_base") or settings.fabric_api_base,
            "sql_server": destination.get("sql_server") or settings.fabric_sql_server,
            "sql_database": destination.get("sql_database") or settings.fabric_sql_database,
            "source_table": destination.get("source_table") or settings.fabric_source_table,
            "pass_table": destination.get("pass_table") or settings.fabric_pass_table,
            "fail_table": destination.get("fail_table") or settings.fabric_fail_table,
            "ignored_table": destination.get("ignored_table") or settings.fabric_ignored_table,
        }
        secrets = {
            "tenant_id": settings.fabric_tenant_id,
            "client_id": settings.fabric_client_id,
            "client_secret": settings.fabric_client_secret,
        }
        missing = [name for name, value in (
            ("FABRIC_WORKSPACE_ID", resolved.get("workspace_id")),
            ("FABRIC_LAKEHOUSE_ID", resolved.get("lakehouse_id")),
            ("FABRIC_TENANT_ID", secrets.get("tenant_id")),
            ("FABRIC_CLIENT_ID", secrets.get("client_id")),
            ("FABRIC_CLIENT_SECRET", secrets.get("client_secret")),
            ("ONELAKE_ACCOUNT_URL", resolved.get("account_url")),
        ) if not value]
        if missing:
            raise ValueError("Microsoft Fabric destination requires a saved Fabric/OneLake credential or these environment variables: " + ", ".join(missing))
        return resolved, secrets


class SnowflakePlatform(PlatformService):
    credential_providers = {"snowflake"}

    def resolve_destination(self, destination: dict) -> tuple[dict, dict]:
        resolved = {
            **destination,
            "account": destination.get("account") or settings.snowflake_account,
            "user": destination.get("user") or settings.snowflake_user,
            "warehouse": destination.get("warehouse") or settings.snowflake_warehouse,
            "database": destination.get("database") or settings.snowflake_database,
            "schema": destination.get("schema") or settings.snowflake_schema,
        }
        secrets = {"password": settings.snowflake_password} if settings.snowflake_password else {}
        missing = [name for name, value in (
            ("SNOWFLAKE_ACCOUNT", resolved.get("account")),
            ("SNOWFLAKE_USER", resolved.get("user")),
            ("SNOWFLAKE_PASSWORD", secrets.get("password")),
            ("SNOWFLAKE_WAREHOUSE", resolved.get("warehouse")),
            ("SNOWFLAKE_DATABASE", resolved.get("database")),
        ) if not value]
        if missing:
            raise ValueError("Snowflake destination requires a saved Snowflake credential or these environment variables: " + ", ".join(missing))
        return resolved, secrets


PLATFORM_SERVICES = {
    "databricks": DatabricksPlatform(),
    "fabric": FabricPlatform(),
    "onelake": FabricPlatform(),
    "snowflake": SnowflakePlatform(),
}


def resolve_destination_credentials(destination: dict, db: Session) -> tuple[dict, dict]:
    provider = destination.get("type", "local")
    if provider == "local":
        return destination, {}
    service = PLATFORM_SERVICES.get(provider)
    credential_id = destination.get("credential_id")
    if credential_id:
        credential = db.get(Credential, credential_id)
        if credential is None:
            raise ValueError(f"Destination {provider} references missing credential {credential_id}")
        expected = service.credential_providers if service else {provider}
        if credential.provider not in expected:
            raise ValueError(f"Credential provider {credential.provider} does not match destination {provider}")
        if not settings.secret_encryption_key:
            raise ValueError("SECRET_ENCRYPTION_KEY is required for encrypted cloud credentials")
        secrets = json.loads(Fernet(settings.secret_encryption_key.encode()).decrypt(credential.secret_ciphertext.encode()))
        return {**(credential.config or {}), **destination}, secrets
    if service:
        return service.resolve_destination(destination)
    raise ValueError(f"Destination {provider} requires credential_id")


def _normalize_onelake_directory(value: str | None) -> str:
    path = str(value or "dq").strip().replace("\\", "/").strip("/")
    for prefix in ("Files/", "files/"):
        if path.startswith(prefix):
            path = path[len(prefix):]
    return path or "dq"


def _default_fabric_output_directory() -> str:
    source = _normalize_onelake_directory(settings.fabric_source_directory)
    parts = PurePosixPath(source).parts
    if parts and parts[-1].casefold() == "source":
        return str(PurePosixPath(*parts[:-1], "outputs")) if len(parts) > 1 else "outputs"
    return str(PurePosixPath(source, "outputs"))
