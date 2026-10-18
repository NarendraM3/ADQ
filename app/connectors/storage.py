"""Storage adapters. Optional cloud SDKs are loaded only when selected."""
from pathlib import Path
import json
from app.config import settings


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
        except ImportError as e:
            raise RuntimeError("S3 integration requires optional dependencies: pip install .[cloud]") from e
        self.client = boto3.client("s3", aws_access_key_id=credentials.get("access_key_id"),
            aws_secret_access_key=credentials.get("secret_access_key"), region_name=config.get("region"))
        self.bucket = config.get("bucket")
        if not self.bucket:
            raise ValueError("S3 config requires bucket")

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
        connection = credentials.get("connection_string")
        if connection:
            self.client = BlobServiceClient.from_connection_string(connection)
        elif credentials.get("account_key"):
            from azure.storage.blob import StorageSharedKeyCredential
            account_name = config.get("account_name")
            if not account_name: raise ValueError("Azure config requires account name when using an account key")
            account_url = config.get("account_url") or _azure_account_url(account_name)
            self.client = BlobServiceClient(account_url=account_url, credential=StorageSharedKeyCredential(account_name, credentials["account_key"]))
        elif credentials.get("sas_token"):
            account_url = config.get("account_url") or _azure_account_url(config.get("account_name"))
            self.client = BlobServiceClient(account_url, credential=credentials["sas_token"])
        else:
            account_url = config.get("account_url") or _azure_account_url(config.get("account_name"))
            from azure.identity import ClientSecretCredential
            cred = ClientSecretCredential(credentials["tenant_id"], credentials["client_id"], credentials["client_secret"])
            self.client = BlobServiceClient(account_url, credential=cred)
        self.container = config.get("container")
        if not self.container:
            raise ValueError("Azure config requires container")
    def read(self, key: str) -> bytes:
        return self.client.get_container_client(self.container).download_blob(key).readall()

    def list_files(self, prefix: str = "") -> list[dict]:
        return [{"name": blob.name, "size": blob.size or 0} for blob in self.client.get_container_client(self.container).list_blobs(name_starts_with=prefix) if not blob.name.endswith("/")]

    def check_read(self, key: str) -> None:
        self.client.get_container_client(self.container).download_blob(key, offset=0, length=1).readall()

    def save(self, key: str, content: bytes) -> str:
        self.client.get_container_client(self.container).upload_blob(key, content, overwrite=True)
        return f"azure://{self.container}/{key}"


def _azure_account_url(account_name: str | None) -> str:
    if not account_name: raise ValueError("Azure config requires account name")
    template = settings.azure_blob_account_url_template
    if not template: raise ValueError("Set AZURE_BLOB_ACCOUNT_URL_TEMPLATE or provide account_url in the Azure connection config")
    return template.format(account_name=account_name)


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
        service=DataLakeServiceClient(account_url=account_url,credential=credential)
        self.fs=service.get_file_system_client(workspace)
        self.base=f"{lakehouse}.Lakehouse/Files/{config.get('directory','dq')}"

    def read(self,key: str) -> bytes:
        return self.fs.get_file_client(f"{self.base}/{key}").download_file().readall()

    def list_files(self, prefix: str = "") -> list[dict]:
        return [{"name": path.name.removeprefix(self.base + "/"), "size": path.size or 0} for path in self.fs.get_paths(path=f"{self.base}/{prefix}".rstrip("/")) if not path.is_directory]

    def check_read(self, key: str) -> None:
        self.fs.get_file_client(f"{self.base}/{key}").download_file(offset=0, length=1).readall()

    def save(self,key: str,content: bytes) -> str:
        path=f"{self.base}/{key}"
        parts=path.split("/")[:-1]
        for i in range(1,len(parts)+1):
            try: self.fs.get_directory_client("/".join(parts[:i])).create_directory()
            except Exception as e:
                if "already exists" not in str(e).lower() and "409" not in str(e): raise
        client=self.fs.get_file_client(path)
        client.create_file(); client.append_data(content,offset=0,length=len(content)); client.flush_data(len(content))
        return f"onelake://{path}"


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
        if provider == "databricks":
            try:
                from databricks import sql
            except ImportError as e:
                raise RuntimeError("Databricks source requires databricks-sql-connector: install the cloud dependencies") from e
            self.connection = sql.connect(server_hostname=config["server_hostname"], http_path=config["http_path"],
                access_token=credentials["access_token"], catalog=config.get("catalog", "hive_metastore"), schema=config.get("schema", "default"))
        elif provider == "snowflake":
            try:
                import snowflake.connector
            except ImportError as e:
                raise RuntimeError("Snowflake source requires snowflake-connector-python: install the cloud dependencies") from e
            self.connection = snowflake.connector.connect(user=config["user"], password=credentials["password"],
                account=config["account"], warehouse=config["warehouse"], database=config["database"], schema=config.get("schema", "PUBLIC"))
        else:
            raise ValueError(f"Unsupported SQL source provider: {provider}")

    @staticmethod
    def _identifier(value: str) -> str:
        if not value or not all(part.replace("_", "").isalnum() for part in value.split(".")):
            raise ValueError("Table names may contain only letters, numbers, underscores, and dots")
        return ".".join('"' + part.replace('"', '""') + '"' for part in value.split("."))

    def list_files(self, prefix: str = "") -> list[dict]:
        cursor = self.connection.cursor()
        try:
            if self.provider == "snowflake":
                cursor.execute("SHOW TABLES")
                rows = cursor.fetchall()
                names = [row[1] for row in rows]
            else:
                cursor.execute("SHOW TABLES")
                names = [row[1] for row in cursor.fetchall()]
            return [{"name": name, "size": 0} for name in names if name.startswith(prefix)]
        finally:
            cursor.close()

    def read(self, table: str) -> bytes:
        import csv, io, json
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self._identifier(table)}")
            names = [d[0] for d in cursor.description]
            output = io.StringIO(newline="")
            writer = csv.DictWriter(output, fieldnames=names)
            writer.writeheader()
            for row in cursor.fetchall():
                writer.writerow({k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v for k, v in zip(names, row)})
            return output.getvalue().encode("utf-8")
        finally:
            cursor.close()

    def check_read(self, table: str) -> None:
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"SELECT * FROM {self._identifier(table)} LIMIT 0")
        finally:
            cursor.close()

    def close(self):
        self.connection.close()


def get_source_adapter(provider: str, config: dict, credentials: dict | None = None):
    if provider in ("databricks", "snowflake"):
        return SqlTableStorage(provider, config, credentials or {})
    return get_storage(provider, config, credentials)
