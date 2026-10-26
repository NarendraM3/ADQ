from app.connectors.destinations import _bypass_proxy_for_host, _table_name, _write_databricks_table, write_destination


class Cursor:
    def __init__(self):
        self.calls = []
        self.row_count = 0
    def execute(self, statement, params=None):
        self.calls.append((statement, params))
    def executemany(self, statement, values):
        values = list(values)
        self.row_count += len(values)
        self.calls.append((statement, values))
    def fetchone(self):
        return (self.row_count,)
    def close(self):
        pass


class Connection:
    def __init__(self):
        self.cursors = []
    def cursor(self):
        cursor = Cursor()
        self.cursors.append(cursor)
        return cursor


def test_table_names_are_dynamic_and_execution_scoped():
    one = _table_name("Employee Dataset", 4, 12, "pass")
    another = _table_name("Customer Dataset", 4, 13, "pass")
    assert one == "employee_dataset_job_4_execution_12_pass"
    assert another != one
    assert _table_name("Employee Dataset", 4, 12, "fail") != one


def test_databricks_host_is_added_to_proxy_bypass_without_dropping_existing_hosts(monkeypatch):
    monkeypatch.setenv("NO_PROXY", "localhost,127.0.0.1")
    _bypass_proxy_for_host("workspace.example")
    assert "workspace.example" in __import__("os").environ["NO_PROXY"]
    assert "workspace.example" in __import__("os").environ["no_proxy"]
    assert "localhost" in __import__("os").environ["NO_PROXY"]


def test_table_writer_keeps_columns_values_types_and_failure_metadata():
    conn = Connection()
    good = [{"user_id": 9, "email": "ok@example.test", "active": True, "First Name": "Ada"}]
    bad = [{"row_index": 1, "record": {"user_id": 10, "email": "bad", "active": False, "First Name": "Lin"},
            "failure_reasons": ["email invalid"]}]
    _write_databricks_table(conn, "`dq_catalog`.`dq_schema`.`sample_pass`", good,
                            ["user_id", "email", "active", "First Name"])
    _write_databricks_table(conn, "`dq_catalog`.`dq_schema`.`sample_fail`", bad,
                            ["user_id", "email", "active", "First Name"],
                            (("_dq_row_index", "BIGINT"), ("_dq_failure_reasons", "STRING")))
    pass_sql = " ".join(call[0] for call in conn.cursors[0].calls)
    fail_insert = conn.cursors[1].calls[-2]
    assert "`user_id` BIGINT" in pass_sql and "`active` BOOLEAN" in pass_sql
    assert "`First Name` STRING" in pass_sql
    assert "DELETE FROM" in pass_sql
    assert fail_insert[1][0][:6] == (10, "bad", False, "Lin", 1, '["email invalid"]')


def test_table_writer_persists_dynamic_types_and_validation_lineage():
    conn = Connection()
    rows = [{"record": {"odd name`field": 7}, "_dq_job_id": 3, "_dq_execution_id": 9,
             "_dq_source_dataset_ids": "[4,5]", "_dq_validation_status": "PASS",
             "_dq_row_index": 0, "_dq_failure_reasons": None}]
    extras = (("_dq_job_id", "BIGINT"), ("_dq_execution_id", "BIGINT"),
              ("_dq_source_dataset_ids", "STRING"), ("_dq_validation_status", "STRING"),
              ("_dq_row_index", "BIGINT"), ("_dq_failure_reasons", "STRING"))
    _write_databricks_table(conn, "result_table", rows, ["odd name`field"], extras)
    sql = " ".join(call[0] for call in conn.cursors[0].calls)
    inserted = conn.cursors[0].calls[-2][1][0]
    assert "`odd name``field` BIGINT" in sql
    assert inserted[:6] == (7, 3, 9, "[4,5]", "PASS", 0)
    assert inserted[6] is None


def test_databricks_retries_transient_write_errors(monkeypatch):
    calls = []
    monkeypatch.setattr("app.connectors.destinations.time.sleep", lambda _: None)
    from app.connectors import destinations
    def transient(*args, **kwargs):
        calls.append(1)
        if len(calls) < 3:
            raise ConnectionError("temporary")
        return {"status": "saved"}
    monkeypatch.setattr(destinations, "_write_databricks", transient)
    result = write_destination({"type": "databricks"}, {}, "run", [], [], {}, b"", job_id=1, execution_id=3)
    assert result == {"status": "saved"}
    assert len(calls) == 3


def test_empty_results_create_tables_without_inserting(monkeypatch):
    conn = Connection()
    _write_databricks_table(conn, "table_name", [], ["original", "amount"])
    statements = [call[0] for call in conn.cursors[0].calls]
    assert "`original` STRING" in statements[0]
    assert "`amount` STRING" in statements[0]
    assert not any("INSERT" in statement for statement in statements)


def test_writer_reports_failures_after_retries(monkeypatch):
    from app.connectors import destinations
    monkeypatch.setattr(destinations.time, "sleep", lambda _: None)
    calls = []
    def unavailable(*args, **kwargs):
        calls.append(1)
        raise ConnectionError("offline")
    monkeypatch.setattr(destinations, "_write_databricks", unavailable)
    import pytest
    with pytest.raises(ConnectionError):
        write_destination({"type": "databricks"}, {}, "run", [], [], {}, b"", job_id=2, execution_id=8)
    assert len(calls) == 3


def _fake_databricks(monkeypatch, connection_factory):
    import sys
    from types import SimpleNamespace
    monkeypatch.setitem(sys.modules, "databricks", SimpleNamespace(sql=SimpleNamespace(connect=connection_factory)))


def test_databricks_rejects_missing_credentials_before_connecting(monkeypatch):
    import pytest
    from app.connectors.destinations import _write_databricks
    calls = []
    _fake_databricks(monkeypatch, lambda **kwargs: calls.append(kwargs))
    with pytest.raises(ValueError, match="server_hostname, http_path, and access_token"):
        _write_databricks({}, {}, [], [], "dataset", 1, 2)
    assert calls == []


def test_databricks_success_creates_dynamic_tables_and_retry_reuses_them(monkeypatch):
    from app.connectors.destinations import _write_databricks
    connections = []
    class RecordingConnection(Connection):
        def __init__(self):
            super().__init__()
            self.closed = False
        def close(self):
            self.closed = True
    def connect(**kwargs):
        connection = RecordingConnection()
        connections.append(connection)
        return connection
    _fake_databricks(monkeypatch, connect)
    kwargs = ({"server_hostname": "https://workspace.example/", "http_path": "/sql/path",
               "catalog": "dq_catalog", "schema": "dq_schema"}, {"access_token": "mock-token"},
              [{"strange column": 2}], [], "customer data", 7)
    first = _write_databricks(*kwargs, execution_id=10, source_columns=["strange column"],
                              source_dataset_ids=[4], storage_run_id=9)
    second = _write_databricks(*kwargs, execution_id=11, source_columns=["strange column"],
                               source_dataset_ids=[4], storage_run_id=9)
    assert first["passed_table"] == second["passed_table"]
    assert first["failed_table"] == second["failed_table"]
    assert first["catalog"] == "dq_catalog" and first["schema"] == "dq_schema"
    assert all(connection.closed for connection in connections)
    insert = connections[0].cursors[1].calls[-2]
    assert insert[1][0][0] == 2
    assert "`strange column` BIGINT" in " ".join(call[0] for call in connections[0].cursors[1].calls)


def test_databricks_permission_denial_is_not_reported_as_saved(monkeypatch):
    import pytest
    from app.connectors.destinations import _write_databricks
    class DeniedCursor(Cursor):
        def execute(self, statement, params=None):
            self.calls.append((statement, params))
            if statement.startswith("CREATE SCHEMA"):
                raise PermissionError("permission denied")
    class DeniedConnection(Connection):
        def __init__(self):
            super().__init__()
            self.closed = False
        def cursor(self):
            cursor = DeniedCursor()
            self.cursors.append(cursor)
            return cursor
        def close(self):
            self.closed = True
    connection = DeniedConnection()
    _fake_databricks(monkeypatch, lambda **kwargs: connection)
    with pytest.raises(PermissionError):
        _write_databricks({"server_hostname": "host", "http_path": "path"},
                          {"access_token": "mock-token"}, [], [], "dataset", 1, 2)
    assert connection.closed
