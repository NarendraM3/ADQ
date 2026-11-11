"""Storage adapters. Optional cloud SDKs are loaded only when selected."""
import logging
from pathlib import Path
import json
import re
from urllib.parse import urlparse
from app.config import settings


logger = logging.getLogger(__name__)
GUID_RE = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


class LocalStorage:
    def __init__(self):
        self.root = Path(settings.local_data_dir).resolve()
        self.root.mkdir(parents=True, exist_ok=True)

    def save(self, relative: str, content: bytes) -> str:
        target = (self.root / relative).resolve()
        if self.root not in target.parents:
            raise ValueError("Invalid storage path")
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(content)
        return str(target)

    def read(self, path: str) -> bytes:
        p = Path(path).resolve()
        if self.root not in p.parents:
            raise ValueError("Dataset path is outside local storage")
        return p.read_bytes()


class S3Storage:
    def __init__(self, config: dict, credentials: dict):
        try:
            import boto3
            from botocore.config import Config
        except ImportError as e:
            raise RuntimeError("S3 integration requires optional dependencies: pip install .[cloud]") from e
        sdk_config = Config(
            connect_timeout=max(1, int(settings.source_connect_timeout_seconds)),
            read_timeout=max(1, int(settings.source_read_timeout_seconds)),
            retries={"max_attempts": max(1, int(settings.source_discovery_retries)), "mode": "standard"},
        )
        auth_method = config.get("auth_method", "access_key")
        if auth_method == "access_key":
            access_key = credentials.get("access_key_id")
            secret_key = credentials.get("secret_access_key")
            if not access_key or not secret_key:
                raise ValueError("Access key authentication requires an access key ID and secret access key")
            self.client = boto3.client("s3", aws_access_key_id=access_key,
                aws_secret_access_key=secret_key, aws_session_token=credentials.get("session_token"),
                region_name=config.get("region"), config=sdk_config)
        elif auth_method == "default_chain":
            # Use the deployment's IAM role or standard AWS credential provider chain.
            self.client = boto3.client("s3", region_name=config.get("region"), config=sdk_config)
        else:
            raise ValueError("Choose access key or IAM role authentication")
        self.bucket = config.get("bucket")
        if not self.bucket or not config.get("region"):
            raise ValueError("S3 config requires bucket and AWS region")
        # Validate bucket access before listing objects so empty buckets also validate.
        self.client.head_bucket(Bucket=self.bucket)

    def read(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()

    def list_files(self, prefix: str = "") -> list[dict]:
        response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        return [{"name": item["Key"], "size": item["Size"]} for item in response.get("Contents", []) if not item["Key"].endswith("/")]

    def check_read(self, key: str) -> None:
        self.client.get_object(Bucket=self.bucket, Key=key, Range="bytes=0-0")["Body"].read()

    def save(self, key: str, content: bytes) -> str:
        self.client.put_object(Bucket=self.bucket, Key=key, Body=content)
        return f"s3://{self.bucket}/{key}"


class AzureBlobStorage:
    def __init__(self, config: dict, credentials: dict):
        try:
            from azure.storage.blob import BlobServiceClient
        except ImportError as e:
            raise RuntimeError("Azure Blob integration requires optional dependencies: pip install .[cloud]") from e
        config = config or {}
        credentials = credentials or {}
        client_options = {
            "retry_total": max(0, int(settings.source_discovery_retries) - 1),
            "connection_timeout": max(1, int(settings.source_connect_timeout_seconds)),
            "read_timeout": max(1, int(settings.source_read_timeout_seconds)),
        }
        self._request_options = {
            "connection_timeout": client_options["connection_timeout"],
            "read_timeout": client_options["read_timeout"],
        }
        connection = _azure_clean(credentials.get("connection_string"))
        account_name = _azure_clean(config.get("account_name"))
        account_url = _azure_clean(config.get("account_url"))
        if connection:
            self.client = BlobServiceClient.from_connection_string(connection, **client_options)
        elif _azure_clean(credentials.get("account_key")):
            from azure.storage.blob import StorageSharedKeyCredential
            account_name = account_name or _azure_account_name_from_url(account_url)
            if not account_name:
                raise ValueError("Azure account key authentication requires a storage account name")
            self.client = BlobServiceClient(account_url=account_url or _azure_account_url(account_name),
                credential=StorageSharedKeyCredential(account_name, credentials["account_key"]), **client_options)
        elif _azure_clean(credentials.get("sas_token")):
            self.client = BlobServiceClient(account_url or _azure_account_url(account_name),
                credential=credentials["sas_token"], **client_options)
        else:
            account_url = account_url or _azure_account_url(account_name)
            from azure.identity import ClientSecretCredential
            if not all(_azure_clean(credentials.get(key)) for key in ("tenant_id", "client_id", "client_secret")):
                raise ValueError("Azure service principal authentication requires tenant ID, client ID, and client secret")
            cred = ClientSecretCredential(credentials["tenant_id"], credentials["client_id"], credentials["client_secret"])
            self.client = BlobServiceClient(account_url, credential=cred, **client_options)
        self.container = _azure_clean(config.get("container"))
        if not self.container:
            raise ValueError("Azure config requires container")
        self.last_metadata = None

    def _container_client(self):
        return self.client.get_container_client(self.container)

    def _verify_container(self, client, prefix: str = ""):
        try:
            client.get_container_properties(**self._request_options)
        except Exception as exc:
            _raise_azure_error(exc, "container_check", self.container, prefix)

    def read(self, key: str) -> bytes:
        try:
            return self._container_client().download_blob(key, **self._request_options).readall()
        except Exception as exc:
            _raise_azure_error(exc, "blob_read", self.container, key)

    def list_files(self, prefix: str = "") -> list[dict]:
        prefix = str(prefix or "").lstrip("/")
        client = self._container_client()
        self._verify_container(client, prefix)
        try:
            files = [{"name": blob.name, "size": blob.size or 0}
                     for blob in client.list_blobs(name_starts_with=prefix, **self._request_options)
                     if not blob.name.endswith("/")]
        except Exception as exc:
            _raise_azure_error(exc, "blob_list", self.container, prefix)
        self.last_metadata = {"container": self.container, "prefix": prefix, "blob_count": len(files)}
        if not files and prefix:
            raise SourceDiscoveryError("AZURE_BLOB_PREFIX_EMPTY",
                f"Container '{self.container}' is accessible, but no blobs were found under prefix '{prefix}'.",
                self.last_metadata)
        if not files:
            raise SourceDiscoveryError("AZURE_BLOB_CONTAINER_EMPTY",
                f"Container '{self.container}' is accessible, but it does not contain importable blobs.",
                self.last_metadata)
        return files

    def check_read(self, key: str) -> None:
        try:
            self._container_client().download_blob(key, offset=0, length=1, **self._request_options).readall()
        except Exception as exc:
            _raise_azure_error(exc, "blob_read_check", self.container, key)

    def save(self, key: str, content: bytes) -> str:
        self.client.get_container_client(self.container).upload_blob(key, content, overwrite=True)
        return f"azure://{self.container}/{key}"


def _azure_account_url(account_name: str | None) -> str:
    account_name = _azure_clean(account_name)
    if not account_name: raise ValueError("Azure config requires account name")
    template = settings.azure_blob_account_url_template
    return (template.format(account_name=account_name) if template else f"https://{account_name}.blob.core.windows.net").rstrip("/")


def _azure_clean(value) -> str:
    return str(value or "").strip()


def _azure_account_name_from_url(account_url: str | None) -> str:
    host = urlparse(_azure_clean(account_url)).netloc
    return host.split(".", 1)[0] if host else ""


def _azure_exception_info(exc: Exception) -> tuple[int | None, str, str]:
    response = getattr(exc, "response", None)
    status = getattr(response, "status_code", None) or getattr(exc, "status_code", None)
    error_code = str(getattr(exc, "error_code", "") or getattr(exc, "code", "") or "")
    message = str(exc).strip() or type(exc).__name__
    message = re.sub(r"(?i)(sig|signature|se|sp|sv)=([^&\s]+)", r"\1=[REDACTED]", message)
    return status, error_code, message[:1200]


def _raise_azure_error(exc: Exception, operation: str, container: str, target: str = ""):
    status, azure_code, message = _azure_exception_info(exc)
    safe = f"{type(exc).__name__}: {message}".casefold()
    code_upper = azure_code.upper()
    metadata = {"azure_status": status, "azure_error_code": azure_code or None,
                "operation": operation, "container": container, "target": target}
    if status in (401, 403) or code_upper in {"AUTHENTICATIONFAILED", "INVALIDAUTHENTICATIONINFO", "AUTHORIZATIONFAILURE", "AUTHORIZATIONPERMISSIONMISMATCH"}:
        if "permission" in safe or "authorizationpermissionmismatch" in code_upper:
            raise SourceDiscoveryError("AZURE_BLOB_PERMISSION_DENIED",
                "Azure authenticated the request but denied access to the selected container or blobs.", metadata) from exc
        raise SourceDiscoveryError("AZURE_BLOB_AUTHENTICATION_FAILED",
            "Azure rejected the supplied Blob Storage credentials.", metadata) from exc
    if status == 404 or "resourcenotfound" in safe or "not found" in safe:
        if operation in {"blob_read", "blob_read_check"} or code_upper == "BLOBNOTFOUND":
            raise SourceDiscoveryError("AZURE_BLOB_NOT_FOUND",
                f"The selected blob '{target}' was not found in container '{container}'.", metadata) from exc
        if code_upper in {"CONTAINERNOTFOUND", "RESOURCENOTFOUND"} or operation in {"container_check", "blob_list"}:
            raise SourceDiscoveryError("AZURE_BLOB_CONTAINER_NOT_FOUND",
                f"Container '{container}' was not found or is not visible to this user.", metadata) from exc
        raise SourceDiscoveryError("AZURE_BLOB_ACCOUNT_NOT_FOUND",
            "The Azure Blob Storage account endpoint was not found.", metadata) from exc
    if any(token in safe for token in ("dns", "name resolution", "getaddrinfo", "temporary failure", "could not connect", "connection refused", "timed out", "timeout")):
        raise SourceDiscoveryError("AZURE_BLOB_UNREACHABLE",
            "Could not reach the Azure Blob Storage endpoint. Check the account URL, DNS, firewall, proxy, and network access.", metadata) from exc
    if any(token in safe for token in ("invalid url", "invalid uri", "incorrect padding", "malformed", "scheme")):
        raise SourceDiscoveryError("AZURE_BLOB_ENDPOINT_INVALID",
            "The Azure Blob Storage endpoint or credential format is invalid.", metadata) from exc
    raise SourceDiscoveryError("AZURE_BLOB_CONNECTION_FAILED",
        "Could not connect to Azure Blob Storage. Check the submitted account, container, endpoint, and credential settings.", metadata) from exc


class OneLakeStorage:
    def __init__(self, config: dict, credentials: dict):
        try:
            from azure.identity import ClientSecretCredential
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError as e:
            raise RuntimeError("OneLake integration requires optional dependencies: pip install .[cloud]") from e
        workspace=config.get("workspace_id") or config.get("workspace")
        lakehouse=config.get("lakehouse_id") or config.get("lakehouse")
        if not workspace or not lakehouse: raise ValueError("OneLake config requires workspace_id and lakehouse_id")
        credential=ClientSecretCredential(credentials["tenant_id"],credentials["client_id"],credentials["client_secret"])
        account_url=config.get("account_url") or settings.onelake_account_url
        if not account_url: raise ValueError("Set ONELAKE_ACCOUNT_URL or provide account_url in the OneLake connection config")
        service=DataLakeServiceClient(account_url=account_url,credential=credential,
            retry_total=max(0, int(settings.source_discovery_retries) - 1),
            connection_timeout=max(1, int(settings.source_connect_timeout_seconds)),
            read_timeout=max(1, int(settings.source_read_timeout_seconds)))
        self.fs=service.get_file_system_client(workspace)
        self.workspace = str(workspace)
        self.lakehouse = str(lakehouse)
        self.account_url = account_url
        self.artifact_path = _onelake_artifact_path(self.workspace, self.lakehouse)
        self.base="/".join(part.strip("/") for part in (self.artifact_path, "Files", config.get("directory", "dq")) if str(part).strip("/"))
        self._request_options={"connection_timeout": max(1, int(settings.source_connect_timeout_seconds)),
            "read_timeout": max(1, int(settings.source_read_timeout_seconds))}
        self._log_operation("init", self.base)

    def read(self,key: str) -> bytes:
        self._log_operation("read", f"{self.base}/{key}")
        return self.fs.get_file_client(f"{self.base}/{key}").download_file(**self._request_options).readall()

    def list_files(self, prefix: str = "") -> list[dict]:
        self._log_operation("list", f"{self.base}/{prefix}".rstrip("/"))
        return [{"name": path.name.removeprefix(self.base + "/"), "size": path.size or 0} for path in self.fs.get_paths(path=f"{self.base}/{prefix}".rstrip("/"), **self._request_options) if not path.is_directory]

    def check_read(self, key: str) -> None:
        self._log_operation("check_read", f"{self.base}/{key}")
        self.fs.get_file_client(f"{self.base}/{key}").download_file(offset=0, length=1, **self._request_options).readall()

    def save(self,key: str,content: bytes) -> str:
        path=f"{self.base}/{key}"
        self._log_operation("save", path)
        parts=path.split("/")[:-1]
        first_creatable_index = len(self.artifact_path.split("/")) + 2
        for i in range(first_creatable_index,len(parts)+1):
            try: self.fs.get_directory_client("/".join(parts[:i])).create_directory()
            except Exception as e:
                if "already exists" not in str(e).lower() and "409" not in str(e): raise
        client=self.fs.get_file_client(path)
        client.create_file(); client.append_data(content,offset=0,length=len(content)); client.flush_data(len(content))
        return f"onelake://{path}"

    def _log_operation(self, operation: str, path: str) -> None:
        logger.info("OneLake operation=%s endpoint=%s workspace_id=%s lakehouse_id=%s artifact_path=%s path=%s",
            operation, self.account_url, _safe_identifier(self.workspace),
            _safe_identifier(self.lakehouse), self.artifact_path, path)


def _safe_identifier(value: str) -> str:
    value = str(value or "")
    if GUID_RE.fullmatch(value):
        return value
    return f"name:{value[:80]}"


def _onelake_artifact_path(workspace: str, lakehouse: str) -> str:
    workspace_is_guid = bool(GUID_RE.fullmatch(str(workspace)))
    lakehouse_is_guid = bool(GUID_RE.fullmatch(str(lakehouse)))
    if workspace_is_guid != lakehouse_is_guid:
        raise ValueError("OneLake config must use GUIDs for both workspace_id and lakehouse_id, or friendly names for both workspace and lakehouse")
    if lakehouse_is_guid:
        # OneLake GUID addressing is /<workspaceGUID>/<itemGUID>/Files/...; appending
        # .Lakehouse turns the item GUID into an invalid ArtifactId.
        return str(lakehouse).strip("/")
    item = str(lakehouse).strip("/")
    return item if item.casefold().endswith(".lakehouse") else f"{item}.Lakehouse"


def get_storage(provider: str, config: dict, credentials: dict | None = None):
    if provider == "local":
        return LocalStorage()
    if provider == "s3":
        return S3Storage(config, credentials or {})
    if provider == "azure_blob":
        return AzureBlobStorage(config, credentials or {})
    if provider == "onelake":
        return OneLakeStorage(config, credentials or {})
    raise ValueError(f"Unsupported storage provider: {provider}")


class SqlTableStorage:
    """Read-only table discovery and extraction for SQL based source providers."""
    def __init__(self, provider: str, config: dict, credentials: dict):
        self.provider = provider
        self.config = config
        self._access_token = credentials.get("access_token") if provider == "databricks" else None
        self._databricks_workspace = None
        if provider == "databricks":
            try:
                from databricks import sql
            except ImportError as e:
                raise RuntimeError("Databricks source requires databricks-sql-connector: install the cloud dependencies") from e
            if not config.get("server_hostname") or not config.get("http_path") or not credentials.get("access_token"):
                raise ValueError("Databricks requires server_hostname, http_path, and access_token")
            options = {key: config[key] for key in ("catalog", "schema") if config.get(key)}
            self.connection = sql.connect(server_hostname=config["server_hostname"], http_path=config["http_path"],
                access_token=credentials["access_token"], _socket_timeout=max(1, int(settings.source_read_timeout_seconds)),
                _retry_stop_after_attempts_count=max(1, int(settings.source_discovery_retries)),
                _retry_stop_after_attempts_duration=max(1, int(settings.source_connect_timeout_seconds) + int(settings.source_read_timeout_seconds)),
                **options)
        elif provider == "snowflake":
            try:
                import snowflake.connector
            except ImportError as e:
                raise RuntimeError("Snowflake source requires snowflake-connector-python: install the cloud dependencies") from e
            self.connection = snowflake.connector.connect(user=config["user"], password=credentials["password"],
                account=config["account"], warehouse=config["warehouse"], database=config["database"],
                schema=config.get("schema", "PUBLIC"), login_timeout=max(1, int(settings.source_connect_timeout_seconds)),
                network_timeout=max(1, int(settings.source_read_timeout_seconds)))
        else:
            raise ValueError(f"Unsupported SQL source provider: {provider}")

    def _identifier(self, value: str) -> str:
        parts = value.split(".")
        if not value or any(not part for part in parts):
            raise ValueError("Select a valid table name")
        if self.provider == "databricks":
            return ".".join("`" + part.replace("`", "``") + "`" for part in parts)
        return ".".join('"' + part.replace('"', '""') + '"' for part in parts)

    def list_files(self, prefix: str = "") -> list[dict]:
        cursor = self.connection.cursor()
        try:
            if self.provider == "databricks":
                def quote(identifier):
                    return "`" + identifier.replace("`", "``") + "`"
                def rows(statement, discovery_operation):
                    try:
                        cursor.execute(statement)
                        columns = [str(d[0]).casefold() for d in (cursor.description or [])]
                        data = cursor.fetchall()
                        return columns, data
                    except Exception as exc:
                        detail = str(exc).casefold()
                        code = str(getattr(exc, "error_code", "") or getattr(exc, "code", "")).upper()
                        sqlstate = str(getattr(exc, "sql_state", None) or getattr(exc, "sqlstate", None) or "").upper()
                        denied = any(term in detail for term in ("permission denied", "not authorized", "insufficient privileges", "access denied", "permission")) or code in ("PERMISSION_DENIED", "INSUFFICIENT_PRIVILEGES") or sqlstate == "42501"
                        code = "TABLE_DISCOVERY_PERMISSION_DENIED" if denied else f"{discovery_operation}_FAILED"
                        message = "Permission denied while listing Databricks metadata." if denied else f"Databricks {discovery_operation.replace('_', ' ').lower()} failed."
                        raise SourceDiscoveryError(code, message) from exc
                _, current_rows = rows("SELECT current_catalog(), current_schema()", "CATALOG_DISCOVERY")
                current = current_rows[0] if current_rows else (None, None)
                current_catalog = current[0] if len(current) > 0 else None
                current_schema = current[1] if len(current) > 1 else None
                catalog = self.config.get("catalog") or current_catalog
                schema = self.config.get("schema") or current_schema
                if not catalog or not schema:
                    raise SourceDiscoveryError("TABLE_DISCOVERY_FAILED", "Databricks did not return a current catalog and schema; provide both explicitly.")
                _, catalog_rows = rows("SHOW CATALOGS", "CATALOG_DISCOVERY")
                catalogs = {str(row[0]).casefold() for row in catalog_rows if row and row[0] is not None}
                if catalog.casefold() not in catalogs:
                    raise SourceDiscoveryError("CATALOG_NOT_FOUND", f"Catalog '{catalog}' was not found or is not visible to this user.")
                _, schema_rows = rows(f"SHOW SCHEMAS IN {quote(catalog)}", "SCHEMA_DISCOVERY")
                schemas = {str(row[0]).casefold() for row in schema_rows if row and row[0] is not None}
                if schema.casefold() not in schemas:
                    raise SourceDiscoveryError("SCHEMA_NOT_FOUND", f"Schema '{schema}' was not found in catalog '{catalog}'.")

                show_columns, show_rows = rows(f"SHOW TABLES IN {quote(catalog)}.{quote(schema)}", "TABLE_DISCOVERY")
                show_table_col = next((i for i, name in enumerate(show_columns) if name in ("tablename", "table_name")), 1)
                names = []
                for row in show_rows:
                    if len(row) <= show_table_col or row[show_table_col] is None:
                        continue
                    # SHOW TABLES can include session temporary views. Keep them when
                    # returned by the selected-schema query; never filter on format.
                    names.append(str(row[show_table_col]))

                # INFORMATION_SCHEMA provides an independent check and can expose
                # table types/ownership without imposing a Delta-only restriction.
                info_sql = (f"SELECT table_catalog, table_schema, table_name, table_type "
                            f"FROM {quote(catalog)}.information_schema.tables "
                            f"WHERE lower(table_schema) = lower('{schema.replace("'", "''")}') ORDER BY table_name")
                info_columns, info_rows = rows(info_sql, "TABLE_DISCOVERY")
                info_indexes = {name: i for i, name in enumerate(info_columns)}
                info_name_col = info_indexes.get("table_name", 2)
                info_schema_col = info_indexes.get("table_schema", 1)
                info_catalog_col = info_indexes.get("table_catalog", 0)
                info_type_col = info_indexes.get("table_type", 3)
                info_names = []
                for row in info_rows:
                    if len(row) <= max(info_name_col, info_schema_col, info_catalog_col):
                        continue
                    if str(row[info_catalog_col]).casefold() != catalog.casefold() or str(row[info_schema_col]).casefold() != schema.casefold():
                        continue
                    info_names.append({"name": str(row[info_name_col]), "type": str(row[info_type_col]) if len(row) > info_type_col else ""})
                metadata = {"catalog": catalog, "schema": schema,
                    "current": {"catalog": current_catalog, "schema": current_schema},
                    "catalogs": sorted(str(r[0]) for r in catalog_rows if r),
                    "schemas": sorted(str(r[0]) for r in schema_rows if r),
                    "show_tables": [str(r[show_table_col]) for r in show_rows if len(r) > show_table_col and r[show_table_col] is not None],
                    "information_schema_tables": info_names}
                self.last_metadata = metadata
                # Include information_schema entries if SHOW TABLES omits a visible
                # managed/external table. Keep its actual name and queryable type.
                names.extend(item["name"] for item in info_names if item["name"].casefold() not in {n.casefold() for n in names})
                if not names:
                    raise SourceDiscoveryError("NO_TABLES_FOUND", f"No tables are available in '{catalog}.{schema}'.", metadata)
                matched = [name for name in names if name.casefold().startswith(prefix.casefold())]
                if prefix and not matched:
                    raise SourceDiscoveryError("NO_TABLES_MATCH_PREFIX", f"No tables in '{catalog}.{schema}' start with '{prefix}'.", metadata)
                return [{"name": f"{catalog}.{schema}.{name}", "catalog": catalog, "schema": schema, "table": name, "size": 0} for name in matched]
            else:
                cursor.execute("SHOW TABLES")
                names = [row[1] for row in cursor.fetchall()]
            return [{"name": name, "size": 0} for name in names if name.startswith(prefix)]
        finally:
            cursor.close()

    def databricks_metadata(self, level: str, folder: str = "", prefix: str = "") -> dict:
        """Discover Unity Catalog hierarchy or list one selected Volume directory."""
        if self.provider != "databricks":
            raise ValueError("Unity Catalog metadata is available only for Databricks")
        cursor = self.connection.cursor()
        def quote(value):
            return "`" + str(value).replace("`", "``") + "`"
        def run(statement, operation):
            try:
                cursor.execute(statement)
                return cursor.fetchall()
            except Exception as exc:
                detail = str(exc).casefold()
                denied = any(term in detail for term in ("permission", "not authorized", "insufficient privilege", "access denied")) or str(getattr(exc, "error_code", "")).upper() == "PERMISSION_DENIED"
                code = "VOLUME_READ_PERMISSION_DENIED" if denied and operation == "volume_files" else "VOLUME_DISCOVERY_PERMISSION_DENIED" if denied else f"{operation.upper()}_FAILED"
                message = "Missing READ VOLUME permission to list or read this Unity Catalog Volume." if denied and operation == "volume_files" else "Permission denied while discovering Unity Catalog metadata." if denied else f"Databricks {operation.replace('_', ' ')} failed."
                raise SourceDiscoveryError(code, message) from exc
        try:
            if level == "catalogs":
                return {"items": [str(row[0]) for row in run("SHOW CATALOGS", "catalog_discovery")]}
            catalog, schema = self.config.get("catalog"), self.config.get("schema")
            if level == "schemas":
                if not catalog: raise ValueError("Select a catalog first")
                return {"items": [str(row[0]) for row in run(f"SHOW SCHEMAS IN {quote(catalog)}", "schema_discovery")]}
            if level in ("volumes", "tables"):
                if not catalog or not schema: raise ValueError("Select a catalog and schema first")
                command = "VOLUMES" if level == "volumes" else "TABLES"
                rows = run(f"SHOW {command} IN {quote(catalog)}.{quote(schema)}", f"{level}_discovery")
                columns = [str(column[0]).casefold() for column in (cursor.description or [])]
                candidates = ("name", "volume_name") if level == "volumes" else ("tablename", "table_name")
                name_col = next((index for index, column in enumerate(columns) if column in candidates), 2 if level == "volumes" else 1)
                return {"items": [str(row[name_col] if len(row) > name_col else row[0]) for row in rows if row]}
            if level == "files":
                if not catalog or not schema or not self.config.get("volume"):
                    raise ValueError("Select a catalog, schema, and volume first")
                from databricks.sdk import WorkspaceClient
                if getattr(self, "_databricks_workspace", None) is None:
                    host = self.config["server_hostname"]
                    if not host.startswith("http"): host = "https://" + host
                    self._databricks_workspace = WorkspaceClient(host=host, token=self._access_token)
                root = f"/Volumes/{catalog}/{schema}/{self.config['volume']}"
                path = root + ("/" + folder.strip("/") if folder.strip("/") else "")
                try:
                    entries = list(self._databricks_workspace.files.list_directory_contents(path))
                except Exception:
                    # Some saved PAT/OAuth connections have SQL warehouse access
                    # but do not include the Files API's separate `files` scope.
                    # Databricks SQL read_files is a supported metadata path and
                    # returns only metadata here, never file content.
                    return {"items": self._sql_volume_entries(cursor, path, root, prefix)}
                eligible = {".csv", ".xls", ".xlsx", ".json", ".parquet"}
                result = []
                for entry in entries:
                    name = str(entry.path).rsplit("/", 1)[-1]
                    is_dir = bool(entry.is_directory)
                    if not is_dir and prefix and not name.casefold().startswith(prefix.casefold()):
                        continue
                    ext = "" if is_dir else __import__("pathlib").Path(name).suffix.lower()
                    if not is_dir and ext not in eligible: continue
                    result.append({"name": name, "path": str(entry.path)[len(root):].lstrip("/"), "object_key": str(entry.path) if not is_dir else None, "is_directory": is_dir,
                        "extension": ext, "size": entry.file_size, "last_modified": entry.last_modified})
                return {"items": result}
            raise ValueError("Unsupported Databricks discovery level")
        finally:
            cursor.close()

    @staticmethod
    def _sql_volume_entries(cursor, path: str, root: str, prefix: str) -> list[dict]:
        import posixpath
        escaped = path.replace("'", "''")
        try:
            cursor.execute(f"SELECT path, modificationTime, length FROM read_files('{escaped}', format => 'binaryFile')")
            rows = cursor.fetchall()
        except Exception as exc:
            detail = str(exc).casefold()
            denied = any(term in detail for term in ("permission denied", "not authorized", "insufficient privileges", "access denied", "read volume"))
            if denied:
                raise SourceDiscoveryError("VOLUME_READ_PERMISSION_DENIED", "Missing READ VOLUME permission to list files in this Unity Catalog Volume.") from exc
            if "not found" in detail or "does not exist" in detail or "404" in detail:
                raise SourceDiscoveryError("VOLUME_DIRECTORY_NOT_FOUND", "The selected folder was not found in this Unity Catalog Volume.") from exc
            raise SourceDiscoveryError("VOLUME_FILE_LIST_FAILED", "Databricks could not list this Volume directory.") from exc
        eligible = {".csv", ".xls", ".xlsx", ".json", ".parquet"}
        directories = {}
        files = []
        for file_path, modified, size in rows:
            file_path = str(file_path)
            if file_path.startswith("dbfs:"):
                file_path = file_path[len("dbfs:"):]
            elif file_path.startswith("file:"):
                file_path = file_path[len("file:"):]
            relative = posixpath.relpath(file_path, root)
            relative_to_current = posixpath.relpath(file_path, path)
            if relative == "." or relative.startswith("../") or relative_to_current.startswith("../"):
                continue
            first, separator, tail = relative_to_current.partition("/")
            if separator:
                folder_path = posixpath.relpath(posixpath.join(path, first), root)
                directories[folder_path] = {"name": first, "path": folder_path, "object_key": None, "is_directory": True,
                    "extension": "", "size": None, "last_modified": None}
                continue
            name = posixpath.basename(file_path)
            extension = posixpath.splitext(name)[1].lower()
            if extension not in eligible or (prefix and not name.casefold().startswith(prefix.casefold())):
                continue
            files.append({"name": name, "path": relative, "object_key": file_path, "is_directory": False,
                "extension": extension, "size": size, "last_modified": modified})
        return list(directories.values()) + files

    @staticmethod
    def _sql_read_volume_file(cursor, path: str) -> bytes:
        escaped = path.replace("'", "''")
        try:
            cursor.execute(f"SELECT content FROM read_files('{escaped}', format => 'binaryFile') LIMIT 1")
            row = cursor.fetchone()
            if not row or row[0] is None:
                raise SourceDiscoveryError("VOLUME_FILE_NOT_FOUND", "The selected file no longer exists in this Unity Catalog Volume.")
            return bytes(row[0])
        except SourceDiscoveryError:
            raise
        except Exception as exc:
            detail = str(exc).casefold()
            denied = any(term in detail for term in ("permission denied", "not authorized", "insufficient privileges", "access denied", "read volume"))
            if denied:
                raise SourceDiscoveryError("VOLUME_READ_PERMISSION_DENIED", "Missing READ VOLUME permission to read this file.") from exc
            if "not found" in detail or "does not exist" in detail or "404" in detail:
                raise SourceDiscoveryError("VOLUME_FILE_NOT_FOUND", "The selected file no longer exists in this Unity Catalog Volume.") from exc
            raise SourceDiscoveryError("VOLUME_FILE_READ_FAILED", "Databricks could not read the selected Volume file.") from exc

    def read(self, table: str) -> bytes:
        if self.provider == "databricks" and table.startswith("/Volumes/"):
            import posixpath
            root = f"/Volumes/{self.config.get('catalog')}/{self.config.get('schema')}/{self.config.get('volume')}"
            normalized = posixpath.normpath(table)
            if not self.config.get("catalog") or not self.config.get("schema") or not self.config.get("volume") or not normalized.startswith(root.rstrip("/") + "/"):
                raise ValueError("Selected file path is outside the chosen Unity Catalog Volume")
            from databricks.sdk import WorkspaceClient
            host = self.config["server_hostname"]
            if not host.startswith("http"): host = "https://" + host
            client = WorkspaceClient(host=host, token=self._access_token)
            try:
                response = client.files.download(normalized)
                return response.contents.read()
            except Exception as api_error:
                try:
                    cursor = self.connection.cursor()
                    try:
                        return self._sql_read_volume_file(cursor, normalized)
                    finally:
                        cursor.close()
                except SourceDiscoveryError:
                    raise
                except Exception:
                    detail = str(api_error).casefold()
                    if "permission" in detail or "not authorized" in detail or "403" in detail:
                        raise SourceDiscoveryError("VOLUME_READ_PERMISSION_DENIED", "Missing READ VOLUME permission to read this file.") from api_error
                    if isinstance(api_error, FileNotFoundError) or "not found" in detail or "404" in detail:
                        raise SourceDiscoveryError("VOLUME_FILE_NOT_FOUND", "The selected file no longer exists in this Unity Catalog Volume.") from api_error
                    raise SourceDiscoveryError("VOLUME_FILE_READ_FAILED", "Databricks could not read the selected Volume file.") from api_error
        import csv, io, json
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self._identifier(table)}")
            names = [d[0] for d in cursor.description]
            output = io.StringIO(newline="")
            writer = csv.DictWriter(output, fieldnames=names)
            writer.writeheader()
            # Keep the Databricks result bounded in memory while retaining the
            # single query/session used for this source read.
            fetchmany = getattr(cursor, "fetchmany", None)
            if fetchmany is None:  # Small test doubles and legacy DBAPI cursors.
                batches = (cursor.fetchall(),)
                for batch in batches:
                    for row in batch:
                        writer.writerow({k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
                                         for k, v in zip(names, row)})
            else:
                while True:
                    batch = fetchmany(max(1, min(int(settings.source_fetch_batch_size), 10000)))
                    if not batch:
                        break
                    for row in batch:
                        writer.writerow({k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v
                                         for k, v in zip(names, row)})
            return output.getvalue().encode("utf-8")
        finally:
            cursor.close()

    def check_read(self, table: str) -> None:
        if self.provider == "databricks" and table.startswith("/Volumes/"):
            self.read(table)
            return
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self._identifier(table)} LIMIT 0")
        finally:
            cursor.close()

    def close(self):
        self.connection.close()


class SourceDiscoveryError(RuntimeError):
    def __init__(self, code: str, message: str, metadata: dict | None = None):
        self.code = code
        self.metadata = metadata
        super().__init__(message)


class DatabaseTableStorage:
    """Read-only SQL Server, MySQL, and PostgreSQL source adapter."""
    DEFAULT_PORTS = {"sqlserver": 1433, "mysql": 3306, "postgresql": 5432}
    DRIVERS = {"sqlserver": "pymssql", "mysql": "pymysql", "postgresql": "psycopg"}

    def __init__(self, provider: str, config: dict, credentials: dict):
        from sqlalchemy import create_engine
        from sqlalchemy.engine import URL
        if provider not in self.DEFAULT_PORTS:
            raise ValueError("Unsupported database type")
        self.provider = provider
        self.config = config or {}
        self.engine = None
        self.connection = None
        driver = self.DRIVERS[provider]
        dialect = {"sqlserver": "mssql", "mysql": "mysql", "postgresql": "postgresql"}[provider]
        required = ("host", "database", "username")
        if any(not str(config.get(key) or "").strip() for key in required) or not str((credentials or {}).get("password") or ""):
            raise SourceDiscoveryError("DATABASE_CONFIG_INVALID", "Enter server, port, database, username, and password.")
        url = URL.create(f"{dialect}+{driver}", username=config["username"], password=credentials["password"],
                         host=config["host"], port=int(config.get("port") or self.DEFAULT_PORTS[provider]),
                         database=config["database"])
        connect_timeout = max(1, int(settings.source_connect_timeout_seconds))
        timeout_args = {"sqlserver": {"login_timeout": connect_timeout}, "mysql": {"connect_timeout": connect_timeout},
                        "postgresql": {"connect_timeout": connect_timeout}}[provider]
        if provider == "postgresql":
            sslmode = str(config.get("sslmode") or "require").strip().lower()
            if sslmode not in {"disable", "allow", "prefer", "require", "verify-ca", "verify-full"}:
                raise SourceDiscoveryError("DATABASE_CONFIG_INVALID", "PostgreSQL sslmode must be disable, allow, prefer, require, verify-ca, or verify-full.")
            timeout_args["sslmode"] = sslmode
            if config.get("sslrootcert"):
                timeout_args["sslrootcert"] = config["sslrootcert"]
        try:
            self.engine = create_engine(url, pool_pre_ping=True, connect_args=timeout_args)
            self.connection = self.engine.connect()
            self.connection.exec_driver_sql("SELECT 1")
        except Exception as exc:
            if self.connection is not None:
                self.connection.close()
            if self.engine is not None:
                self.engine.dispose()
            raise self._connection_error(exc) from exc

    def _connection_error(self, exc: Exception) -> SourceDiscoveryError:
        message = str(exc)
        safe = message.lower()
        if any(token in safe for token in ("password authentication failed", "access denied", "login failed", "authentication failed", "invalid authorization")):
            return SourceDiscoveryError("AUTHENTICATION_FAILED", "Database authentication failed. Check the username and password.")
        if any(token in safe for token in ("does not exist", "unknown database", "cannot open database", "invalid catalog")):
            return SourceDiscoveryError("DATABASE_NOT_FOUND", "The database was not found or is not accessible to this user.")
        if any(token in safe for token in ("timeout", "timed out", "could not connect", "connection refused", "no route to host", "network is unreachable", "name or service not known", "could not translate host name")):
            return SourceDiscoveryError("DATABASE_UNREACHABLE", "Could not reach the database server. Check host, port, network access, and firewall or security-group rules.")
        if "ssl" in safe or "certificate" in safe:
            return SourceDiscoveryError("DATABASE_SSL_FAILED", "The database SSL handshake failed. Check the server SSL requirement and trusted certificate configuration.")
        return SourceDiscoveryError("DATABASE_CONNECTION_FAILED", "Could not connect to the database. Check the submitted connection details and network access.")

    def list_files(self, prefix: str = "") -> list[dict]:
        from sqlalchemy import inspect
        try:
            inspector = inspect(self.engine)
            result = []
            schemas = inspector.get_schema_names()
            if self.provider == "mysql":
                schemas = [self.engine.url.database]
            normalized_prefix = str(prefix or "").casefold()
            for schema in schemas:
                if schema.casefold() in ("information_schema", "mysql", "performance_schema", "sys", "pg_catalog", "pg_toast"):
                    continue
                for table in inspector.get_table_names(schema=schema):
                    name = f"{schema}.{table}"
                    if not normalized_prefix or name.casefold().startswith(normalized_prefix) or table.casefold().startswith(normalized_prefix):
                        result.append({"name": name, "schema": schema, "table": table, "size": 0})
            if not result and normalized_prefix:
                raise SourceDiscoveryError("NO_TABLES_MATCH_PREFIX", f"No tables start with '{prefix}'.")
            if not result:
                raise SourceDiscoveryError("NO_TABLES_FOUND", "No readable tables were found in this database.")
            return result
        except SourceDiscoveryError:
            raise
        except Exception as exc:
            raise SourceDiscoveryError("TABLE_DISCOVERY_FAILED", "Could not list database tables. Check metadata permissions for this user.") from exc

    def read(self, table: str) -> bytes:
        import csv, io, json
        from sqlalchemy import MetaData, Table, select
        schema, separator, name = table.partition(".")
        if not separator or not schema or not name:
            raise ValueError("Select a schema-qualified table")
        metadata = MetaData()
        reflected = Table(name, metadata, schema=schema, autoload_with=self.engine)
        output = io.StringIO()
        result = self.connection.execution_options(stream_results=True).execute(select(reflected))
        output.write("[")
        first = True
        while True:
            rows = result.fetchmany(max(1, min(int(settings.source_fetch_batch_size), 10000)))
            if not rows:
                break
            for row in rows:
                if not first:
                    output.write(",")
                json.dump(dict(zip((column.name for column in reflected.columns), row)), output,
                          default=str, ensure_ascii=False)
                first = False
        output.write("]")
        return output.getvalue().encode("utf-8")

    def check_read(self, table: str) -> None:
        from sqlalchemy import MetaData, Table, select
        schema, separator, name = table.partition(".")
        if not separator:
            raise ValueError("Select a schema-qualified table")
        try:
            reflected = Table(name, MetaData(), schema=schema, autoload_with=self.engine)
            self.connection.execute(select(reflected).limit(0))
        except Exception as exc:
            raise SourceDiscoveryError("TABLE_READ_PERMISSION_DENIED", "Could not read the selected table. Check SELECT permissions for this user.") from exc

    def close(self):
        if self.connection is not None:
            self.connection.close()
        if self.engine is not None:
            self.engine.dispose()


def get_source_adapter(provider: str, config: dict, credentials: dict | None = None):
    if provider in ("databricks", "snowflake"):
        return SqlTableStorage(provider, config, credentials or {})
    if provider in ("sqlserver", "mysql", "postgresql"):
        return DatabaseTableStorage(provider, config, credentials or {})
    return get_storage(provider, config, credentials)
