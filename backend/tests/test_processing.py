from io import BytesIO
from types import SimpleNamespace

import pytest
from openpyxl import Workbook

from app.processing import destination_for_job, parse_data, schema_from_records, validate_rule_bindings


@pytest.mark.parametrize(("environment", "expected"), [
    ("Databricks", "databricks"), ("Microsoft Fabric", "fabric"), ("Snowflake", "snowflake")])
def test_saved_platform_routes_legacy_jobs_to_matching_destination(environment, expected):
    job = SimpleNamespace(destination={"type": "local"}, environment=environment, settings={})
    assert destination_for_job(job) == {"type": expected}


def test_saved_platform_routing_never_uses_configured_databricks_as_fallback():
    job = SimpleNamespace(destination={"type": "local"}, environment=None, settings={})
    assert destination_for_job(job) == {"type": "local"}


def test_explicit_destination_wins_over_platform_label():
    job = SimpleNamespace(destination={"type": "snowflake"}, environment="Microsoft Fabric", settings={})
    assert destination_for_job(job) == {"type": "snowflake"}


def test_excel_parser_drops_empty_unnamed_columns_without_losing_source_fields():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["EmployeeID", "Email", None, None])
    sheet.append(["EMP001", "a@example.com", None, None])
    content = BytesIO()
    workbook.save(content)

    rows = parse_data("employees.xlsx", content.getvalue())

    assert rows == [{"EmployeeID": "EMP001", "Email": "a@example.com"}]


def test_excel_parser_keeps_populated_column_without_a_header():
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(["EmployeeID", "", "Email"])
    sheet.append(["EMP001", "source value", "a@example.com"])
    content = BytesIO()
    workbook.save(content)

    rows = parse_data("employees.xlsx", content.getvalue())

    assert rows == [{"EmployeeID": "EMP001", "column_2": "source value", "Email": "a@example.com"}]


def test_schema_metadata_is_refreshed_from_actual_records():
    schema = schema_from_records([{"EmployeeID": "EMP001", "Email": "a@example.com"},
                                  {"EmployeeID": "EMP002", "Email": None}])
    assert list(schema["columns"]) == ["EmployeeID", "Email"]
    assert schema["row_count"] == 2
    assert schema["columns"]["Email"]["nullable"] is True


def test_rule_binding_rejects_mismatched_dataset_or_schema():
    rule = SimpleNamespace(id=8, dataset_id=2, column_name="Email")
    rows = {1: [{"emp_id": "EMP001", "email": "a@example.com"}],
            2: [{"EmployeeID": "EMP001", "Email": "a@example.com"}]}

    with pytest.raises(ValueError, match="not selected by this job"):
        validate_rule_bindings([rule], [1], rows, rows[1])
    rule.dataset_id = 1
    rule.column_name = "dob"
    with pytest.raises(ValueError, match="does not exist in the executed dataset"):
        validate_rule_bindings([rule], [1], rows, rows[1])


def test_s3_get_object_body_is_bytes_and_parses_without_changing_columns(monkeypatch):
    import sys
    from app.connectors.storage import S3Storage

    class Body:
        def read(self):
            return b"customer code,signup date\nC-1,2025-01-02\n"

    class Client:
        def head_bucket(self, **kwargs):
            pass
        def get_object(self, **kwargs):
            return {"Body": Body(), "ContentLength": 43, "ETag": '"abc"'}

    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=lambda *args, **kwargs: Client()))
    content = S3Storage({"bucket": "bucket", "region": "us-east-1"},
                        {"access_key_id": "key", "secret_access_key": "secret"}).read("folder/data.csv")
    assert isinstance(content, bytes)
    assert parse_data("folder/data.csv", content) == [{"customer code": "C-1", "signup date": "2025-01-02"}]
    assert parse_data("empty.csv", b"optional field,other field\n") == []
    assert parse_data("optional.csv", b"optional field,other field\n,kept\n") == [{"optional field": "", "other field": "kept"}]


def test_execution_union_with_selected_columns_does_not_require_join_keys(monkeypatch, test_data_dir):
    from pathlib import Path
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool
    from app import processing
    from app.database import Base
    from app.models import Dataset, Execution, Job
    from app.config import settings
    monkeypatch.setattr(settings, "databricks_server_hostname", None)
    monkeypatch.setattr(settings, "databricks_http_path", None)
    monkeypatch.setattr(settings, "databricks_token", None)

    test_data = test_data_dir
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(processing, "SessionLocal", Session)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data))
    db = Session()
    try:
        first_path = test_data / "first.csv"
        second_path = test_data / "second.csv"
        first_path.write_bytes(b"account ref\nA-1\n")
        second_path.write_bytes(b"region name\nwest\n")
        first = Dataset(name="accounts", filename="first.csv", path=str(first_path), schema_json={"columns": {"account ref": {}}})
        second = Dataset(name="regions", filename="second.csv", path=str(second_path), schema_json={"columns": {"region name": {}}})
        db.add_all([first, second]); db.flush()
        job = Job(name="union", dataset_ids=[first.id, second.id],
                  selected_columns={str(first.id): ["account ref"], str(second.id): ["region name"]},
                  settings={"combine_mode": "union"}, destination={"type": "local"})
        db.add(job); db.flush()
        execution = Execution(job_id=job.id, status="queued", rule_snapshot=[
            {"id": 1, "dataset_id": first.id, "column_name": "account ref", "rule_type": "NOT_NULL",
             "parameters": {}, "enabled": True, "approved": True},
            {"id": 2, "dataset_id": second.id, "column_name": "region name", "rule_type": "NOT_NULL",
             "parameters": {}, "enabled": True, "approved": True},
        ])
        db.add(execution); db.commit()
        execution_id = execution.id
    finally:
        db.close()

    processing.execute(execution_id)
    db = Session()
    try:
        result = db.get(Execution, execution_id)
        assert result.status == "succeeded"
        assert (result.passed_count, result.failed_count) == (2, 0)
        report = __import__("json").loads((test_data / "outputs" / f"job-{result.job_id}" / f"execution-{execution_id}" / "report.json").read_text())
        assert (report["total"], report["passed"], report["failed"]) == (2, 2, 0)
        assert result.report_path

        failed_execution = Execution(job_id=result.job_id, status="queued", rule_snapshot=[])
        db.add(failed_execution); db.commit()
        failed_id = failed_execution.id
    finally:
        db.close()

    original_parse = processing.parse_data
    monkeypatch.setattr(processing, "parse_data", lambda *args: (_ for _ in ()).throw(KeyError("left_dataset_id")))
    processing.execute(failed_id)
    db = Session()
    try:
        failed_result = db.get(Execution, failed_id)
        assert failed_result.status == "failed"
        assert failed_result.error and "left_dataset_id" in failed_result.error
        assert (failed_result.passed_count, failed_result.failed_count) == (0, 0)
        assert failed_result.report_path is None
    finally:
        db.close()

    monkeypatch.setattr(processing, "parse_data", original_parse)
    monkeypatch.setattr(processing, "write_destination", lambda *args, **kwargs: (_ for _ in ()).throw(ConnectionError("offline")))
    db = Session()
    try:
        storage_execution = Execution(job_id=job.id, status="queued", rule_snapshot=execution.rule_snapshot)
        db.add(storage_execution); db.commit()
        storage_id = storage_execution.id
    finally:
        db.close()
    processing.execute(storage_id)
    db = Session()
    try:
        storage_result = db.get(Execution, storage_id)
        assert storage_result.status == "failed"
        storage_report = __import__("json").loads(Path(storage_result.report_path).read_text())
        assert storage_report["storage_status"]["status"] == "failed"
        assert storage_report["validation_status"] == "succeeded"
        assert storage_report["status"] == "failed"
        assert (storage_result.passed_count, storage_result.failed_count) == (2, 0)
    finally:
        db.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_execution_join_selection_and_validation_are_the_records_sent_to_storage(monkeypatch, test_data_dir):
    from pathlib import Path
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool
    from app import processing
    from app.database import Base
    from app.models import Dataset, Execution, Job
    from app.config import settings

    test_data = test_data_dir
    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(processing, "SessionLocal", Session)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data))
    left_path, right_path = test_data / "join-left.csv", test_data / "join-right.csv"
    left_path.write_text("id,name,unused\n1,Ada,left\n2,Lin,left\n", encoding="utf-8")
    right_path.write_text("id,score,unused\n1,10,right\n2,bad,right\n", encoding="utf-8")
    db = Session()
    try:
        left = Dataset(name="left", filename="join-left.csv", path=str(left_path), schema_json={"columns": {"id": {}, "name": {}, "unused": {}}})
        right = Dataset(name="right", filename="join-right.csv", path=str(right_path), schema_json={"columns": {"id": {}, "score": {}, "unused": {}}})
        db.add_all([left, right]); db.flush()
        job = Job(name="selected join", dataset_ids=[left.id, right.id],
                  selected_columns={str(left.id): ["id", "name"], str(right.id): ["id", "score"]},
                  join_config={"left_dataset_id": left.id, "right_dataset_id": right.id,
                               "left_key": "id", "right_key": "id", "right_prefix": "right_"},
                  settings={"combine_mode": "join"}, destination={"type": "local"})
        db.add(job); db.flush()
        execution = Execution(job_id=job.id, status="queued", rule_snapshot=[
            {"id": 1, "dataset_id": right.id, "column_name": "score", "rule_type": "DATA_TYPE",
             "parameters": {"type": "integer"}, "enabled": True, "approved": True},
        ])
        db.add(execution); db.commit()
        execution_id = execution.id
    finally:
        db.close()

    stored = {}
    def capture(destination, secrets, base, passed, failed, report, passed_csv, **kwargs):
        stored.update(passed=passed, failed=failed, **kwargs)
        return {"type": "mock", "status": "saved", "pass_status": "saved", "fail_status": "saved"}
    monkeypatch.setattr(processing, "write_destination", capture)
    processing.execute(execution_id)
    db = Session()
    try:
        result = db.get(Execution, execution_id)
        assert result.status == "succeeded"
        assert result.passed_count == 1 and result.failed_count == 1
        assert stored["columns"] == ["id", "name", "right_id", "score"]
        assert stored["source_dataset_ids"] == [left.id, right.id]
        assert stored["passed"] == [{"id": "1", "name": "Ada", "right_id": "1", "score": "10"}]
        assert stored["failed"][0]["record"] == {"id": "2", "name": "Lin", "right_id": "2", "score": "bad"}
        assert stored["failed"][0]["failure_reasons"]
    finally:
        db.close()
        Base.metadata.drop_all(engine)
        engine.dispose()
