from io import BytesIO
from types import SimpleNamespace

import pytest
from openpyxl import Workbook

from app.processing import destination_for_job, parse_data, schema_from_records, validate_rule_bindings
from app.platforms import resolve_destination_credentials
from app.serialization import json_safe


def test_json_safe_normalizes_tabular_scalars_and_nested_records():
    import json
    import math
    import numpy as np
    import pandas as pd

    value = [{"record": {"numpy_int": np.int64(7), "numpy_float": np.float64(2.5),
                          "nan": np.float64(np.nan), "none": None,
                          "timestamp": pd.Timestamp("2025-03-04T05:06:07Z"),
                          "nat": pd.NaT, "missing": pd.NA,
                          "nested": [np.bool_(True), {"empty": float("nan")}]}},
             {"record": pd.Series({"value": np.int32(9), "missing": pd.NA})}]

    normalized = json_safe(value)
    encoded = json.dumps(normalized, allow_nan=False)
    decoded = json.loads(encoded)

    assert decoded[0]["record"] == {
        "numpy_int": 7, "numpy_float": 2.5, "nan": None, "none": None,
        "timestamp": "2025-03-04T05:06:07+00:00", "nat": None, "missing": None,
        "nested": [True, {"empty": None}],
    }
    assert decoded[1]["record"] == {"value": 9, "missing": None}
    assert not any(isinstance(item, float) and math.isnan(item) for item in decoded[0]["record"].values())


def test_execution_persists_normalized_validation_details(monkeypatch, test_data_dir):
    import json
    import numpy as np
    import pandas as pd
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool
    from app import processing
    from app.config import settings
    from app.database import Base
    from app.models import Dataset, Execution, Job

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(processing, "SessionLocal", Session)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir))
    source = test_data_dir / "typed-records.csv"
    source.write_text("required,value\nyes,source\nno,source\n", encoding="utf-8")
    db = Session()
    try:
        dataset = Dataset(name="typed", filename=source.name, path=str(source), schema_json={"columns": {"required": {}, "value": {}}})
        db.add(dataset); db.flush()
        job = Job(name="typed validation", dataset_ids=[dataset.id], destination={"type": "local"})
        db.add(job); db.flush()
        execution = Execution(job_id=job.id, status="queued", rule_snapshot=[
            {"id": 1, "dataset_id": None, "column_name": "required", "rule_type": "IN_SET",
             "parameters": {"values": ["yes"]}, "enabled": True, "approved": True},
        ])
        db.add(execution); db.commit()
        execution_id = execution.id
    finally:
        db.close()

    typed_records = [
        {"required": "yes", "integer": np.int64(3), "timestamp": pd.Timestamp("2025-01-02T03:04:05Z"),
         "missing": np.float64(np.nan), "nested": {"null": pd.NA}},
        {"required": "no", "integer": np.int64(4), "timestamp": pd.Timestamp("2025-01-03T03:04:05Z"),
         "missing": pd.NaT, "nested": {"null": None}},
    ]
    monkeypatch.setattr(processing, "parse_data", lambda *args: typed_records)
    processing.execute(execution_id)

    db = Session()
    try:
        result = db.get(Execution, execution_id)
        assert result.status == "succeeded"
        assert (result.passed_count, result.failed_count) == (1, 1)
        report = json.loads((test_data_dir / "outputs" / f"job-{result.job_id}" / f"execution-{execution_id}" / "report.json").read_text())
        assert (report["passed"], report["failed"], report["total"]) == (1, 1, 2)
        assert report["quality_checks"][0]["rule_type"] == "IN_SET"
        details = json.loads((test_data_dir / "outputs" / f"job-{result.job_id}" / f"execution-{execution_id}" / "failed.json").read_text())
        assert len(details) == 1 and details[0]["record"]["integer"] == 4
        assert details[0]["record"]["missing"] is None
        assert details[0]["record"]["nested"]["null"] is None
        assert details[0]["record"]["timestamp"] == "2025-01-03T03:04:05+00:00"
        from app.main import failed_records
        retrieved = failed_records(execution_id, db)
        assert retrieved == details
    finally:
        db.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


def test_execution_persists_1000_row_counts_and_all_failure_details(monkeypatch, test_data_dir):
    import json
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.pool import StaticPool
    from app import processing
    from app.config import settings
    from app.database import Base
    from app.models import Dataset, Execution, Job

    engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Session = sessionmaker(bind=engine, expire_on_commit=False)
    Base.metadata.create_all(engine)
    monkeypatch.setattr(processing, "SessionLocal", Session)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir))
    rows = ["record_id,required_a,required_b,required_c"]
    for index in range(1000):
        rows.append(",".join([
            str(index),
            "" if index < 356 else "a",
            "" if index < 228 else "b",
            "" if index < 137 else "c",
        ]))
    source = test_data_dir / "records.csv"
    source.write_text("\n".join(rows) + "\n", encoding="utf-8")
    db = Session()
    try:
        dataset = Dataset(name="records", filename=source.name, path=str(source),
                          schema_json={"columns": {"record_id": {}, "required_a": {}, "required_b": {}, "required_c": {}}})
        db.add(dataset); db.flush()
        job = Job(name="1000 row validation", dataset_ids=[dataset.id],
                  selected_columns={str(dataset.id): ["record_id", "required_a", "required_b", "required_c"]},
                  destination={"type": "local"})
        db.add(job); db.flush()
        execution = Execution(job_id=job.id, status="queued", rule_snapshot=[
            {"id": 1, "dataset_id": dataset.id, "column_name": "required_a", "rule_type": "NOT_NULL",
             "parameters": {}, "enabled": True, "approved": True},
            {"id": 2, "dataset_id": dataset.id, "column_name": "required_b", "rule_type": "NOT_NULL",
             "parameters": {}, "enabled": True, "approved": True},
            {"id": 3, "dataset_id": dataset.id, "column_name": "required_c", "rule_type": "NOT_NULL",
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
        assert (result.passed_count, result.failed_count) == (644, 356)
        base = test_data_dir / "outputs" / f"job-{result.job_id}" / f"execution-{execution_id}"
        failed = json.loads((base / "failed.json").read_text())
        assert sum(len(item.get("failure_details", [])) for item in failed) == 721
        assert sum(len(item.get("failure_details", [])) > 1 for item in failed) == 228
        assert len((base / "passed.csv").read_text().splitlines()) == 645
    finally:
        db.close()
        Base.metadata.drop_all(engine)
        engine.dispose()


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


def test_platform_destination_fallbacks_validate_provider_specific_env(monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, "fabric_workspace_id", "workspace")
    monkeypatch.setattr(settings, "fabric_lakehouse_id", "lakehouse")
    monkeypatch.setattr(settings, "fabric_tenant_id", "tenant")
    monkeypatch.setattr(settings, "fabric_client_id", "client")
    monkeypatch.setattr(settings, "fabric_client_secret", "secret")
    monkeypatch.setattr(settings, "fabric_source_directory", "Files/dq/source")
    monkeypatch.setattr(settings, "fabric_output_directory", None)
    monkeypatch.setattr(settings, "onelake_account_url", "https://onelake.dfs.fabric.microsoft.com")
    fabric_destination, fabric_secrets = resolve_destination_credentials({"type": "fabric"}, None)
    assert fabric_destination["provider"] == "onelake"
    assert fabric_destination["workspace_id"] == "workspace"
    assert fabric_destination["lakehouse_id"] == "lakehouse"
    assert fabric_destination["directory"] == "dq/outputs"
    assert fabric_secrets == {"tenant_id": "tenant", "client_id": "client", "client_secret": "secret"}

    monkeypatch.setattr(settings, "snowflake_account", "acct")
    monkeypatch.setattr(settings, "snowflake_user", "user")
    monkeypatch.setattr(settings, "snowflake_password", "pw")
    monkeypatch.setattr(settings, "snowflake_warehouse", "wh")
    monkeypatch.setattr(settings, "snowflake_database", "db")
    snowflake_destination, snowflake_secrets = resolve_destination_credentials({"type": "snowflake"}, None)
    assert snowflake_destination["schema"] == "PUBLIC"
    assert snowflake_secrets == {"password": "pw"}


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


def test_json_and_parquet_ingestion_preserve_real_rows_and_column_names():
    assert parse_data("real.json", b'[{"Customer ID": 7, "active": true}]') == [{"Customer ID": 7, "active": True}]
    import pyarrow as pa
    import pyarrow.parquet as parquet
    content = BytesIO()
    parquet.write_table(pa.table({"Customer ID": [7], "active": [True]}), content)
    assert parse_data("real.parquet", content.getvalue()) == [{"Customer ID": 7, "active": True}]


def test_legacy_xls_uses_xls_reader_and_preserves_headers(monkeypatch):
    import sys
    sheet = SimpleNamespace(nrows=2, row_values=lambda index: ["Customer ID", "Active"] if index == 0 else [7, 1])
    workbook = SimpleNamespace(sheet_by_index=lambda index: sheet, close=lambda: None)
    monkeypatch.setitem(sys.modules, "xlrd", SimpleNamespace(open_workbook=lambda **kwargs: workbook))
    assert parse_data("real.xls", b"mock-xls-bytes") == [{"Customer ID": 7, "Active": 1}]


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
        assert storage_result.error == "offline"
        assert storage_report["storage_status"]["error_message"] == "offline"
        assert (storage_result.passed_count, storage_result.failed_count) == (2, 0)
    finally:
        db.close()

    # An unexpected worker exception after the validation checkpoint must keep
    # the saved counts and detailed results available.
    from app.connectors.storage import LocalStorage
    db = Session()
    try:
        checkpointed = Execution(job_id=job.id, status="queued", rule_snapshot=execution.rule_snapshot)
        db.add(checkpointed); db.commit()
        checkpointed_id = checkpointed.id
    finally:
        db.close()
    original_save = LocalStorage.save
    report_writes = 0
    def fail_final_report(self, relative, content):
        nonlocal report_writes
        if relative.endswith("/report.json"):
            report_writes += 1
            if report_writes == 2:
                raise RuntimeError("worker interrupted after validation checkpoint")
        return original_save(self, relative, content)
    monkeypatch.setattr(LocalStorage, "save", fail_final_report)
    monkeypatch.setattr(processing, "write_destination", lambda *args, **kwargs: {
        "type": "local", "status": "saved", "pass_status": "saved", "fail_status": "saved"})
    processing.execute(checkpointed_id)
    db = Session()
    try:
        checkpointed_result = db.get(Execution, checkpointed_id)
        assert checkpointed_result.status == "failed"
        assert (checkpointed_result.passed_count, checkpointed_result.failed_count) == (2, 0)
        assert checkpointed_result.report_path
        saved_report = __import__("json").loads(Path(checkpointed_result.report_path).read_text())
        assert saved_report["validation_status"] == "succeeded"
        assert saved_report["storage_status"]["status"] == "failed"
        assert Path(checkpointed_result.report_path).parent.joinpath("failed.json").exists()
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
