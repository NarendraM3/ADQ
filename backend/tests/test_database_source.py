import json

from sqlalchemy import create_engine

import pytest

from app.connectors.storage import DatabaseTableStorage, SourceDiscoveryError, SqlTableStorage


def test_database_adapter_discovers_schema_and_streams_quoted_columns():
    engine = create_engine("sqlite://")
    with engine.begin() as connection:
        connection.exec_driver_sql('CREATE TABLE "odd table" ("first name" TEXT, "nullable value" TEXT)')
        connection.exec_driver_sql('INSERT INTO "odd table" VALUES (?, ?)', ("A, B", None))

    adapter = object.__new__(DatabaseTableStorage)
    adapter.engine = engine
    adapter.connection = engine.connect()
    adapter.provider = "postgresql"
    try:
        tables = adapter.list_files()
        assert {item["name"] for item in tables} == {"main.odd table"}
        content = adapter.read("main.odd table")
        rows = json.loads(content)
        assert rows == [{"first name": "A, B", "nullable value": None}]
    finally:
        adapter.close()


def test_postgresql_adapter_uses_submitted_values_ssl_and_timeout(monkeypatch):
    captured = {}

    class FakeConnection:
        def exec_driver_sql(self, statement):
            assert statement == "SELECT 1"
        def close(self):
            captured["closed"] = True

    class FakeEngine:
        def __init__(self, url):
            self.url = url
        def connect(self):
            return FakeConnection()
        def dispose(self):
            captured["disposed"] = True

    def fake_create_engine(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return FakeEngine(url)

    monkeypatch.setattr("sqlalchemy.create_engine", fake_create_engine)

    adapter = DatabaseTableStorage("postgresql",
        {"host": "db.example.invalid", "port": 5432, "database": "appdb", "username": "appuser"},
        {"password": "secret-password"})
    try:
        assert captured["url"].host == "db.example.invalid"
        assert captured["url"].database == "appdb"
        assert captured["url"].username == "appuser"
        assert captured["kwargs"]["connect_args"] == {"connect_timeout": 10, "sslmode": "require"}
        assert captured["kwargs"]["pool_pre_ping"] is True
    finally:
        adapter.close()


def test_postgresql_adapter_keeps_safe_authentication_errors(monkeypatch):
    def fake_create_engine(*args, **kwargs):
        class Engine:
            def connect(self):
                raise RuntimeError("password authentication failed for user appuser with password secret-password")
            def dispose(self):
                pass
        return Engine()

    monkeypatch.setattr("sqlalchemy.create_engine", fake_create_engine)

    with pytest.raises(SourceDiscoveryError) as error:
        DatabaseTableStorage("postgresql",
            {"host": "db.example.invalid", "port": 5432, "database": "appdb", "username": "appuser"},
            {"password": "secret-password"})
    assert error.value.code == "AUTHENTICATION_FAILED"
    assert "secret-password" not in str(error.value)


def test_database_table_prefix_matches_table_name_or_schema(monkeypatch):
    adapter = object.__new__(DatabaseTableStorage)
    adapter.engine = object()
    adapter.connection = None
    adapter.provider = "postgresql"

    class Inspector:
        def get_schema_names(self):
            return ["public"]
        def get_table_names(self, schema=None):
            assert schema == "public"
            return ["customers", "orders"]

    monkeypatch.setattr("sqlalchemy.inspect", lambda engine: Inspector())

    assert adapter.list_files("cust") == [{"name": "public.customers", "schema": "public", "table": "customers", "size": 0}]
    assert adapter.list_files("public.ord") == [{"name": "public.orders", "schema": "public", "table": "orders", "size": 0}]
    with pytest.raises(SourceDiscoveryError) as error:
        adapter.list_files("missing")
    assert error.value.code == "NO_TABLES_MATCH_PREFIX"


class FakeCursor:
    def __init__(self, catalogs=("catalog_a",), schemas=("schema_a",), tables=("test_file_one", "Other"), current=("catalog_a", "schema_a"), failure=None):
        self.catalogs, self.schemas, self.tables, self.current = catalogs, schemas, tables, current
        self.failure = failure
        self.rows = []
        self.description = []
        self.statements = []

    def execute(self, statement):
        self.statements.append(statement)
        if self.failure and self.failure in statement:
            raise PermissionError("permission denied")
        if statement.startswith("SELECT current_catalog"):
            self.rows = [self.current]
            self.description = [("current_catalog",), ("current_schema",)]
        elif statement == "SHOW CATALOGS":
            self.rows = [(name,) for name in self.catalogs]
            self.description = [("catalog",)]
        elif statement.startswith("SHOW SCHEMAS"):
            self.rows = [(name,) for name in self.schemas]
            self.description = [("databaseName",)]
        elif statement.startswith("SHOW TABLES"):
            self.rows = [("schema_a", name, False) for name in self.tables]
            self.description = [("database",), ("tableName",), ("isTemporary",)]
        elif ".information_schema.tables" in statement:
            self.rows = [("catalog_a", "schema_a", name, "MANAGED") for name in self.tables]
            self.description = [("table_catalog",), ("table_schema",), ("table_name",), ("table_type",)]

    def fetchone(self):
        return self.rows[0]

    def fetchall(self):
        return self.rows

    def close(self):
        pass


def db_adapter(cursor, config=None):
    adapter = object.__new__(SqlTableStorage)
    adapter.provider = "databricks"
    adapter.config = config or {}
    adapter.connection = type("Connection", (), {"cursor": lambda self: cursor})()
    return adapter


@pytest.mark.parametrize("catalog,schema", [("catalog_a", "schema_a"), ("another_catalog", "another_schema")])
def test_databricks_discovery_uses_selected_metadata_and_case_insensitive_prefix(catalog, schema):
    cursor = FakeCursor(catalogs=(catalog,), schemas=(schema,), tables=("Test_File_One", "Other"), current=(catalog, schema))
    adapter = db_adapter(cursor, {"catalog": catalog, "schema": schema})
    result = adapter.list_files("test_file")
    assert result == [{"name": f"{catalog}.{schema}.Test_File_One", "catalog": catalog, "schema": schema,
                       "table": "Test_File_One", "size": 0}]
    assert f"SHOW TABLES IN `{catalog}`.`{schema}`" in cursor.statements


def test_databricks_discovery_empty_prefix_returns_all_and_no_match_is_explicit():
    adapter = db_adapter(FakeCursor(tables=("One", "Two")))
    assert len(adapter.list_files("")) == 2
    with pytest.raises(SourceDiscoveryError, match="start with") as error:
        adapter.list_files("missing")
    assert error.value.code == "NO_TABLES_MATCH_PREFIX"


@pytest.mark.parametrize("config,code", [
    ({"catalog": "missing", "schema": "schema_a"}, "CATALOG_NOT_FOUND"),
    ({"catalog": "catalog_a", "schema": "missing"}, "SCHEMA_NOT_FOUND"),
])
def test_databricks_discovery_validates_catalog_and_schema(config, code):
    with pytest.raises(SourceDiscoveryError) as error:
        db_adapter(FakeCursor(), config).list_files()
    assert error.value.code == code


def test_databricks_discovery_empty_schema_and_permission_failure_are_not_empty_success():
    with pytest.raises(SourceDiscoveryError) as error:
        db_adapter(FakeCursor(tables=())).list_files()
    assert error.value.code == "NO_TABLES_FOUND"
    cursor = FakeCursor(failure="SHOW CATALOGS")
    with pytest.raises(SourceDiscoveryError) as denied:
        db_adapter(cursor).list_files()
    assert denied.value.code == "TABLE_DISCOVERY_PERMISSION_DENIED"


def test_databricks_read_check_uses_backtick_quoted_qualified_identifier():
    cursor = FakeCursor()
    adapter = db_adapter(cursor)
    adapter.check_read("catalog_a.schema_a.odd`table")
    assert cursor.statements == ["SELECT * FROM `catalog_a`.`schema_a`.`odd``table` LIMIT 0"]


def test_other_sql_sources_keep_double_quoted_identifier_behavior():
    adapter = object.__new__(SqlTableStorage)
    adapter.provider = "snowflake"
    assert adapter._identifier('db.schema.odd"table') == '"db"."schema"."odd""table"'


def test_databricks_volume_discovery_multiple_and_empty_volumes():
    cursor = FakeCursor()
    cursor.execute_original = cursor.execute
    def execute(statement):
        if statement.startswith("SHOW VOLUMES"):
            cursor.statements.append(statement)
            cursor.rows = [("schema_a", "bronze", "MANAGED", ""), ("schema_a", "empty", "MANAGED", "")]
            cursor.description = [("catalog",), ("name",), ("type",), ("comment",)]
        else:
            cursor.execute_original(statement)
    cursor.execute = execute
    adapter = db_adapter(cursor, {"catalog": "catalog_a", "schema": "schema_a"})
    assert adapter.databricks_metadata("volumes")["items"] == ["bronze", "empty"]
    assert "SHOW VOLUMES IN `catalog_a`.`schema_a`" in cursor.statements


def test_databricks_lists_nested_volume_files_and_reads_real_bytes(monkeypatch):
    import sys
    from types import SimpleNamespace
    cursor = FakeCursor()
    adapter = db_adapter(cursor, {"catalog": "catalog_a", "schema": "schema_a", "volume": "bronze", "server_hostname": "workspace.invalid"})
    adapter._access_token = "mock-token"
    expected_path = "/Volumes/catalog_a/schema_a/bronze/nested"
    class Files:
        def list_directory_contents(self, path):
            assert path == expected_path
            return iter([SimpleNamespace(path=path + "/test file 1.xlsx", is_directory=False, file_size=123, last_modified=10),
                         SimpleNamespace(path=path + "/child", is_directory=True, file_size=None, last_modified=11),
                         SimpleNamespace(path=path + "/ignored.txt", is_directory=False, file_size=4, last_modified=12)])
        def download(self, path):
            assert path == "/Volumes/catalog_a/schema_a/bronze/nested/test file 1.xlsx"
            from io import BytesIO
            return SimpleNamespace(contents=BytesIO(b"real workbook bytes"))
    monkeypatch.setitem(sys.modules, "databricks", SimpleNamespace(sdk=SimpleNamespace(WorkspaceClient=lambda **kwargs: SimpleNamespace(files=Files()))))
    monkeypatch.setitem(sys.modules, "databricks.sdk", SimpleNamespace(WorkspaceClient=lambda **kwargs: SimpleNamespace(files=Files())))
    items = adapter.databricks_metadata("files", "nested", prefix="TEST FILE")["items"]
    assert [item["name"] for item in items] == ["test file 1.xlsx", "child"]
    assert items[0]["extension"] == ".xlsx" and items[0]["size"] == 123
    assert adapter.read(items[0]["object_key"]) == b"real workbook bytes"


def test_databricks_volume_directory_can_be_empty(monkeypatch):
    import sys
    from types import SimpleNamespace
    adapter = object.__new__(SqlTableStorage)
    adapter.provider = "databricks"
    adapter.config = {"catalog": "catalog_a", "schema": "schema_a", "volume": "empty", "server_hostname": "workspace.invalid"}
    adapter.connection = type("Connection", (), {"cursor": lambda self: FakeCursor()})()
    adapter._access_token = "mock-token"
    monkeypatch.setitem(sys.modules, "databricks.sdk", SimpleNamespace(
        WorkspaceClient=lambda **kwargs: SimpleNamespace(files=SimpleNamespace(list_directory_contents=lambda path: iter([])))))
    assert adapter.databricks_metadata("files")["items"] == []


def test_databricks_uses_sql_metadata_and_bytes_when_files_scope_is_unavailable(monkeypatch):
    import sys
    from types import SimpleNamespace
    data = b"actual bytes from the selected file"
    class Cursor:
        description = []
        rows = []
        def execute(self, statement):
            if statement.startswith("SELECT path, modificationTime, length"):
                self.rows = [("dbfs:/Volumes/catalog_a/schema_a/bronze/nested/test file 1.xlsx", 10, len(data))]
                self.description = [("path",), ("modificationTime",), ("length",)]
            elif statement.startswith("SELECT content FROM read_files"):
                self.rows = [(data,)]
            else:
                raise AssertionError(statement)
        def fetchall(self): return self.rows
        def fetchone(self): return self.rows[0] if self.rows else None
        def close(self): pass
    adapter = object.__new__(SqlTableStorage)
    adapter.provider = "databricks"
    adapter.config = {"catalog": "catalog_a", "schema": "schema_a", "volume": "bronze", "server_hostname": "workspace.invalid"}
    adapter._access_token = "mock-token"
    adapter._databricks_workspace = None
    adapter.connection = SimpleNamespace(cursor=lambda: Cursor())
    class Files:
        def list_directory_contents(self, path): raise PermissionError("token requires files scope")
        def download(self, path): raise PermissionError("token requires files scope")
    monkeypatch.setitem(sys.modules, "databricks.sdk", SimpleNamespace(WorkspaceClient=lambda **kwargs: SimpleNamespace(files=Files())))
    folders = adapter.databricks_metadata("files")["items"]
    assert folders[0]["is_directory"] and folders[0]["name"] == "nested"
    listing = adapter.databricks_metadata("files", "nested")["items"]
    assert listing[0]["name"] == "test file 1.xlsx" and listing[0]["size"] == len(data)
    assert adapter.read(listing[0]["object_key"]) == data


def test_databricks_volume_permission_denial_is_actionable():
    cursor = FakeCursor(failure="SHOW VOLUMES")
    adapter = db_adapter(cursor, {"catalog": "catalog_a", "schema": "schema_a"})
    with pytest.raises(SourceDiscoveryError) as error:
        adapter.databricks_metadata("volumes")
    assert error.value.code == "VOLUME_DISCOVERY_PERMISSION_DENIED"
    assert "permission" in str(error.value).lower()


@pytest.mark.parametrize(("failure", "code"), [
    (FileNotFoundError("missing"), "VOLUME_FILE_NOT_FOUND"),
    (PermissionError("permission denied"), "VOLUME_READ_PERMISSION_DENIED"),
    (OSError("transport failed"), "VOLUME_FILE_READ_FAILED"),
])
def test_databricks_volume_read_failures_are_specific(monkeypatch, failure, code):
    import sys
    from types import SimpleNamespace
    adapter = object.__new__(SqlTableStorage)
    adapter.provider = "databricks"
    adapter.config = {"catalog": "cat", "schema": "sch", "volume": "vol", "server_hostname": "workspace.invalid"}
    adapter._access_token = "mock-token"
    monkeypatch.setitem(sys.modules, "databricks.sdk", SimpleNamespace(
        WorkspaceClient=lambda **kwargs: SimpleNamespace(files=SimpleNamespace(download=lambda path: (_ for _ in ()).throw(failure)))))
    with pytest.raises(SourceDiscoveryError) as error:
        adapter.read("/Volumes/cat/sch/vol/file.csv")
    assert error.value.code == code


def test_databricks_volume_read_rejects_paths_outside_selected_volume():
    adapter = object.__new__(SqlTableStorage)
    adapter.provider = "databricks"
    adapter.config = {"catalog": "cat", "schema": "sch", "volume": "vol"}
    with pytest.raises(ValueError, match="outside"):
        adapter.read("/Volumes/cat/sch/other/file.csv")
