import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app.main import app
from app.config import settings
from app.database import Base, get_db
import app.processing as processing


def test_capabilities_and_health():
    client = TestClient(app)
    assert client.get("/health").json()["local_workflow"] == "ready"
    body = client.get("/api/v1/capabilities").json()
    assert body["destinations"]["local"] == "ready"


def test_local_file_job_execution(monkeypatch):
    from pathlib import Path
    test_data = Path(".test-data").resolve()
    test_data.mkdir(exist_ok=True)
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data / "data"))

    def override_db():
        db=TestingSession()
        try: yield db
        finally: db.close()

    app.dependency_overrides[get_db] = override_db
    try:
        client=TestClient(app)
        upload=client.post("/api/v1/datasets/upload",files={"file":("customers.csv",b"email,age\nnot-email,15\ngood@example.com,33\n","text/csv")})
        assert upload.status_code == 201, upload.text
        dataset_id=upload.json()["id"]
        job=client.post("/api/v1/jobs",json={"name":"local test","dataset_ids":[dataset_id],"destination":{"type":"local"}})
        assert job.status_code == 201, job.text
        job_id=job.json()["id"]
        rule=client.post(f"/api/v1/jobs/{job_id}/rules",json={"column_name":"email","rule_type":"EMAIL_FORMAT","enabled":True,"approved":True})
        assert rule.status_code == 201, rule.text
        run=client.post(f"/api/v1/jobs/{job_id}/execute")
        assert run.status_code == 202, run.text
        execution_id=run.json()["id"]
        detail=client.get(f"/api/v1/executions/{execution_id}").json()
        assert detail["status"] == "succeeded"
        assert detail["passed_count"] == 1 and detail["failed_count"] == 1
        report=client.get(f"/api/v1/executions/{execution_id}/artifacts/report.json").json()
        assert client.get(f"/api/v1/datasets/{report['output_dataset_id']}").json()["source_type"] == "derived"
        failed=client.get(f"/api/v1/executions/{execution_id}/failed").json()
        assert failed[0]["failure_reasons"] == ["Invalid email format: 'not-email' is not a valid email address."]
        correction=client.post(f"/api/v1/executions/{execution_id}/corrections",json={"row_index":0,"column_name":"email","value":"fixed@example.com"})
        assert correction.status_code == 200
        assert len(client.get(f"/api/v1/executions/{execution_id}/corrections").json()) == 1
        rechecked=client.post(f"/api/v1/executions/{execution_id}/revalidate").json()
        assert rechecked == {"revalidated":1,"passed":1,"failed":0}
        output=client.get(f"/api/v1/executions/{execution_id}/artifacts/passed.csv")
        assert output.status_code == 200 and b"good@example.com" in output.content
        second=client.post("/api/v1/datasets/upload",files={"file":("tiers.csv",b"email,tier\ngood@example.com,gold\nunused@example.com,silver\n","text/csv")})
        second_id=second.json()["id"]
        joined=client.post("/api/v1/jobs",json={"name":"join test","dataset_ids":[dataset_id,second_id],
            "selected_columns":{str(dataset_id):["email"],str(second_id):["tier"]},
            "join_config":{"left_dataset_id":dataset_id,"right_dataset_id":second_id,"left_key":"email","right_key":"email"},
            "destination":{"type":"local"}})
        assert joined.status_code == 201, joined.text
        join_rule=client.post(f"/api/v1/jobs/{joined.json()['id']}/rules",json={
            "column_name":"tier","rule_type":"NOT_NULL","enabled":True,"approved":True})
        assert join_rule.status_code == 201, join_rule.text
        joined_run=client.post(f"/api/v1/jobs/{joined.json()['id']}/execute")
        joined_exec=joined_run.json()["id"]
        joined_csv=client.get(f"/api/v1/executions/{joined_exec}/artifacts/passed.csv").content
        assert b"email,tier" in joined_csv and b"gold" in joined_csv
        removed=client.delete(f"/api/v1/jobs/{job_id}")
        assert removed.status_code == 204
        assert client.get(f"/api/v1/jobs/{job_id}").status_code == 404
        assert client.get(f"/api/v1/executions/{execution_id}").status_code == 404
        assert client.get(f"/api/v1/jobs/{joined.json()['id']}/rules").status_code == 200
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_approved_rules_validate_customer_csv_with_record_counts(monkeypatch):
    from pathlib import Path

    test_data = Path(".test-data-approved").resolve()
    test_data.mkdir(exist_ok=True)
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data / "data"))

    def override_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        csv = ("customer_id,name,email,age\n"
               "1,Alice,alice@example.com,30\n"
               "2,Bob,not-an-email,25\n"
               "3,Charlie,charlie@example.com,-5\n"
               "4,Diana,,40\n"
               "5,Eric,eric@example.com,28\n")
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("customers.csv", csv, "text/csv")})
        assert uploaded.status_code == 201, uploaded.text
        dataset_id = uploaded.json()["id"]
        created = client.post("/api/v1/jobs", json={"name": "customer checks", "dataset_ids": [dataset_id], "destination": {"type": "local"}})
        assert created.status_code == 201, created.text
        job_id = created.json()["id"]
        rules_to_approve = []
        for column, kind, parameters in [
            ("email", "EMAIL_FORMAT", {}),
            ("email", "NOT_NULL", {}),
            ("age", "DATA_TYPE", {"type": "integer"}),
            ("age", "RANGE", {"min": 0}),
        ]:
            response = client.post(f"/api/v1/jobs/{job_id}/rules", json={
                "dataset_id": dataset_id, "column_name": column, "rule_type": kind,
                "parameters": parameters, "enabled": True, "approved": False,
            })
            assert response.status_code == 201, response.text
            rules_to_approve.append(response.json())

        # Exercise the same persisted approval endpoint used by the UI, then
        # reload the collection before execution to verify approval survives.
        for rule in rules_to_approve:
            approved = client.patch(f"/api/v1/rules/{rule['id']}", json={"approved": True})
            assert approved.status_code == 200, approved.text
            assert approved.json()["approved"] is True
        reloaded = client.get(f"/api/v1/jobs/{job_id}/rules")
        assert reloaded.status_code == 200
        assert all(rule["approved"] is True for rule in reloaded.json() if rule["id"] in {r["id"] for r in rules_to_approve})

        pending = client.post(f"/api/v1/jobs/{job_id}/rules", json={
            "dataset_id": dataset_id, "column_name": "name", "rule_type": "NOT_NULL",
            "enabled": False, "approved": False,
        })
        assert pending.status_code == 201, pending.text

        started = client.post(f"/api/v1/jobs/{job_id}/execute")
        assert started.status_code == 202, started.text
        execution = client.get(f"/api/v1/executions/{started.json()['id']}").json()
        assert execution["status"] == "succeeded"
        assert execution["passed_count"] == 2
        assert execution["failed_count"] == 3
        assert any("Executed 4 approved and enabled rules; skipped 1 pending rules" in line for line in execution["logs"])
        report = client.get(f"/api/v1/executions/{execution['id']}/artifacts/report.json").json()
        assert report["rules"] == {"executed": 4, "pending": 1, "disabled": 1, "skipped": 1}
        assert report["validation_complete"] is True
        assert report["total"] == report["records_evaluated"] == 5
        failed = client.get(f"/api/v1/executions/{execution['id']}/failed").json()
        assert [row["row_index"] for row in failed] == [1, 2, 3]
        assert len(failed) == 3
        charlie = next(row for row in failed if row["record"]["customer_id"] == "3")
        assert "Invalid age: '-5' is below the allowed minimum of 0." in charlie["failure_reasons"]
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_zero_approved_rules_are_incomplete_not_passed(monkeypatch):
    from pathlib import Path

    test_data = Path(".test-data-no-rules").resolve()
    test_data.mkdir(exist_ok=True)
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data / "data"))

    def override_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("customers.csv", b"customer_id,email\n1,a@example.com\n2,bad\n", "text/csv")})
        dataset_id = uploaded.json()["id"]
        job = client.post("/api/v1/jobs", json={"name": "pending only", "dataset_ids": [dataset_id], "destination": {"type": "local"}})
        job_id = job.json()["id"]
        pending = client.post(f"/api/v1/jobs/{job_id}/rules", json={"dataset_id": dataset_id, "column_name": "email", "rule_type": "EMAIL_FORMAT", "enabled": True, "approved": False})
        assert pending.status_code == 201, pending.text

        started = client.post(f"/api/v1/jobs/{job_id}/execute")
        execution = client.get(f"/api/v1/executions/{started.json()['id']}").json()
        assert execution["status"] == "incomplete"
        assert execution["passed_count"] == execution["failed_count"] == 0
        assert any("Validation incomplete: no approved rules were executed" in line for line in execution["logs"])
        report = client.get(f"/api/v1/executions/{execution['id']}/artifacts/report.json").json()
        assert report["validation_complete"] is False
        assert report["rules"] == {"executed": 0, "pending": 1, "disabled": 0, "skipped": 1}
        assert report["total"] == 0 and report["records_evaluated"] == 0 and report["records_loaded"] == 2
        assert client.get(f"/api/v1/executions/{execution['id']}/failed").json() == []
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_employee_baseline_is_idempotent_and_validates_25_records(monkeypatch):
    from pathlib import Path
    test_data = Path(".test-data-employee-baseline").resolve()
    test_data.mkdir(exist_ok=True)
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data / "data"))

    def override_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        fields = ["EmployeeID", "Name", "Email", "Age", "Department", "Salary"]
        valid = [[f"EMP{i:03}", f"Employee {i}", f"employee{i}@example.com", 25 + i % 20,
                  "IT", 50000 + i] for i in range(1, 16)]
        invalid = [
            ["EMP016", "", "manoj.nair@example.com", 28, "IT", 54000],
            ["EMP017", "Priya Shah", "", 26, "HR", 49000],
            ["EMP018", "Suresh Babu", "suresh.babu@example.com", "", "Finance", 62000],
            ["EMP019", "Neha Gupta", "neha.gupta@example.com", 27, "", 47000],
            ["EMP020", "Tarun Reddy", "tarun.reddy@example.com", 30, "Engineering", ""],
            ["EMP021", "Kavya Singh", "kavya.singh.example.com", 25, "Marketing", 46000],
            ["EMP022", "Mohit Verma", "mohit.verma@example.com", -4, "IT", 58000],
            ["EMP023", "Nisha Paul", "nisha.paul@example.com", 29, "HR", -1500],
            ["EMP024", "Ravi Teja", "ravi.teja@example.com", 41, "UnknownDept", 65000],
            ["EMP014", "Duplicate", "duplicate@example.com", 34, "Finance", 53000],
        ]
        import csv, io
        content = io.StringIO(newline="")
        writer = csv.writer(content)
        writer.writerow(fields)
        writer.writerows(valid + invalid)
        original = content.getvalue().encode()
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("Employee.csv", original, "text/csv")})
        assert uploaded.status_code == 201, uploaded.text
        dataset_id = uploaded.json()["id"]
        job = client.post("/api/v1/jobs", json={"name": "employee baseline", "dataset_ids": [dataset_id],
            "selected_columns": {str(dataset_id): fields}, "destination": {"type": "local"}})
        assert job.status_code == 201, job.text
        job_id = job.json()["id"]
        baseline = client.post(f"/api/v1/jobs/{job_id}/baseline-rules")
        assert baseline.status_code == 201, baseline.text
        assert baseline.json()["added"] == 13
        assert all(rule["enabled"] and rule["approved"] for rule in baseline.json()["rules"])
        assert client.post(f"/api/v1/jobs/{job_id}/baseline-rules").json()["added"] == 0

        first = client.post(f"/api/v1/jobs/{job_id}/execute").json()
        result = client.get(f"/api/v1/executions/{first['id']}").json()
        assert result["status"] == "succeeded"
        assert (result["passed_count"], result["failed_count"]) == (15, 10)
        report = client.get(f"/api/v1/executions/{first['id']}/artifacts/report.json").json()
        assert report["total"] == report["passed"] + report["failed"] == 25
        failed = client.get(f"/api/v1/executions/{first['id']}/failed").json()
        assert {row["record"]["EmployeeID"] for row in failed} == {f"EMP{i:03}" for i in range(16, 25)} | {"EMP014"}
        assert sum("duplicates" in " ".join(row["failure_reasons"]) for row in failed) == 1

        # The uploaded source remains unchanged and the same data/rules repeat consistently.
        assert Path(uploaded.json()["path"]).read_bytes() == original
        second = client.post(f"/api/v1/jobs/{job_id}/execute").json()
        second_result = client.get(f"/api/v1/executions/{second['id']}").json()
        assert (second_result["passed_count"], second_result["failed_count"]) == (15, 10)
        assert second_result["rule_snapshot"] == result["rule_snapshot"]
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_generated_rules_are_approved_and_scoped_to_each_new_job(monkeypatch):
    from pathlib import Path
    import app.main as main

    test_data = Path(".test-data-generated").resolve()
    test_data.mkdir(exist_ok=True)
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data / "data"))

    def override_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()

    async def suggestions(datasets, db):
        return [{"dataset_id": datasets[0].id, "column_name": "email", "rule_type": "EMAIL_FORMAT",
                 "parameters": {"strict": True}, "severity": "high", "rationale": "Check email format"}]

    monkeypatch.setattr(main, "generate_rule_suggestions", suggestions)
    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("customers.csv", b"email\na@example.com\nbad\n", "text/csv")})
        assert uploaded.status_code == 201, uploaded.text
        dataset_id = uploaded.json()["id"]

        jobs = []
        generated_ids = []
        for number in (1, 2):
            created = client.post("/api/v1/jobs", json={"name": f"generated job {number}", "dataset_ids": [dataset_id]})
            assert created.status_code == 201, created.text
            job_id = created.json()["id"]
            jobs.append(job_id)
            # A preexisting pending rule remains unchanged by suggestion generation.
            old = client.post(f"/api/v1/jobs/{job_id}/rules", json={"dataset_id": dataset_id, "column_name": "email",
                "rule_type": "NOT_NULL", "enabled": True, "approved": False})
            assert old.status_code == 201, old.text
            result = client.post(f"/api/v1/jobs/{job_id}/suggest-rules")
            assert result.status_code == 200, result.text
            assert result.json()["added"] == 1
            stored = client.get(f"/api/v1/jobs/{job_id}/rules").json()
            generated = next(rule for rule in stored if rule["rule_type"] == "EMAIL_FORMAT")
            generated_ids.append(generated["id"])
            assert generated["enabled"] is True and generated["approved"] is False
            assert generated["dataset_id"] == dataset_id
            assert generated["parameters"] == {"strict": True, "severity": "high", "rationale": "Check email format"}
            assert next(rule for rule in stored if rule["id"] == old.json()["id"])["approved"] is False

            # A disabled manually authored rule and the generated pending rule
            # remain ineligible until an explicit approval action.
            disabled = client.post(f"/api/v1/jobs/{job_id}/rules", json={"dataset_id": dataset_id, "column_name": "email",
                "rule_type": "NOT_NULL", "enabled": False, "approved": False})
            assert disabled.status_code == 201 and disabled.json()["enabled"] is False
            approval = client.patch(f"/api/v1/rules/{generated['id']}", json={"approved": True})
            assert approval.status_code == 200 and approval.json()["approved"] is True
            started = client.post(f"/api/v1/jobs/{job_id}/execute")
            execution = client.get(f"/api/v1/executions/{started.json()['id']}").json()
            assert execution["status"] == "succeeded"
            assert execution["passed_count"] == 1 and execution["failed_count"] == 1
            report = client.get(f"/api/v1/executions/{execution['id']}/artifacts/report.json").json()
            assert report["rules"] == {"executed": 1, "pending": 2, "disabled": 1, "skipped": 2}
            assert report["total"] == report["records_evaluated"] == 2
            assert report["passed_path"] and report["failed_path"]

        assert generated_ids[0] != generated_ids[1]
        for job_id, generated_id in zip(jobs, generated_ids):
            rules = client.get(f"/api/v1/jobs/{job_id}/rules").json()
            assert next(rule for rule in rules if rule["id"] == generated_id)["approved"] is True
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_s3_source_connection_validation_and_encrypted_storage(monkeypatch):
    import app.main as main
    from cryptography.fernet import Fernet
    from app.models import Credential

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    def override_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()

    class FakeAdapter:
        def list_files(self, prefix=""):
            return [{"name": f"{prefix}customers.csv", "size": 12}]
    state = {"error": None}
    def fake_adapter(provider, config, secrets):
        if state["error"]:
            raise state["error"]
        return FakeAdapter()
    monkeypatch.setattr(main, "get_source_adapter", fake_adapter)
    monkeypatch.setattr(settings, "secret_encryption_key", Fernet.generate_key().decode())
    app.dependency_overrides[get_db] = override_db
    body = {"name": "test bucket", "provider": "s3", "config": {"bucket": "test-bucket", "region": "us-east-1", "auth_method": "access_key"},
            "secrets": {"access_key_id": "TESTACCESS", "secret_access_key": "TESTSECRET"}}
    try:
        client = TestClient(app)
        connected = client.post("/api/v1/sources/connect?prefix=folder/", json=body)
        assert connected.status_code == 201, connected.text
        assert connected.json()["files"] == [{"name": "folder/customers.csv", "size": 12}]
        assert "TESTSECRET" not in connected.text
        saved = TestingSession().get(Credential, connected.json()["id"])
        assert "TESTSECRET" not in saved.secret_ciphertext

        missing_bucket = {**body, "config": {"region": "us-east-1", "auth_method": "access_key"}}
        assert client.post("/api/v1/sources/connect", json=missing_bucket).status_code == 422

        class AwsFailure(Exception):
            def __init__(self, code):
                self.response = {"Error": {"Code": code}}
        state["error"] = AwsFailure("InvalidAccessKeyId")
        invalid = client.post("/api/v1/sources/connect", json=body)
        assert invalid.status_code == 401
        assert "TESTSECRET" not in invalid.text

        state["error"] = None
        monkeypatch.setattr(settings, "secret_encryption_key", None)
        no_key = client.post("/api/v1/sources/connect", json=body)
        assert no_key.status_code == 503
        assert "SECRET_ENCRYPTION_KEY" in no_key.json()["detail"]

        monkeypatch.setattr(settings, "secret_encryption_key", Fernet.generate_key().decode())
        state["error"] = AwsFailure("AccessDenied")
        denied = client.post("/api/v1/sources/connect", json=body)
        assert denied.status_code == 403
        assert "TESTSECRET" not in denied.text
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_credential_key_rejects_missing_and_invalid_values_without_leaking(monkeypatch):
    import app.main as main

    monkeypatch.setattr(settings, "secret_encryption_key", None)
    with pytest.raises(HTTPException) as missing:
        main.credential_key()
    assert missing.value.status_code == 503
    assert "missing or empty" in missing.value.detail

    invalid_secret = "definitely-not-a-fernet-key"
    monkeypatch.setattr(settings, "secret_encryption_key", invalid_secret)
    with pytest.raises(HTTPException) as invalid:
        main.credential_key()
    assert invalid.value.status_code == 503
    assert "valid Fernet key" in invalid.value.detail
    assert invalid_secret not in invalid.value.detail


def test_credential_key_accepts_valid_fernet_key_with_surrounding_whitespace(monkeypatch):
    from cryptography.fernet import Fernet
    import app.main as main

    key = Fernet.generate_key().decode("ascii")
    monkeypatch.setattr(settings, "secret_encryption_key", f"  {key}\r\n")
    cipher = main.credential_key()
    token = cipher.encrypt(b"credential")
    assert cipher.decrypt(token) == b"credential"


def test_blank_process_encryption_key_falls_back_to_project_dotenv(monkeypatch):
    from pathlib import Path
    from uuid import uuid4
    from cryptography.fernet import Fernet
    from app.config import Settings

    key = Fernet.generate_key().decode("ascii")
    env_file = Path.cwd() / f".test-settings-{uuid4().hex}.env"
    env_file.write_text(f"SECRET_ENCRYPTION_KEY={key}\n", encoding="utf-8")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", "  \t")
    monkeypatch.chdir(env_file.parent.parent)
    try:
        configured = Settings(_env_file=env_file)
        assert configured.secret_encryption_key == key
        assert Fernet(configured.secret_encryption_key.encode("ascii"))
    finally:
        env_file.unlink(missing_ok=True)


def test_s3_storage_uses_request_credentials_and_verifies_bucket(monkeypatch):
    import sys
    from types import SimpleNamespace
    from app.connectors.storage import S3Storage

    calls = []
    class FakeS3Client:
        def head_bucket(self, **kwargs):
            calls.append(("head_bucket", kwargs))
        def list_objects_v2(self, **kwargs):
            calls.append(("list_objects_v2", kwargs))
            return {"Contents": [{"Key": "incoming/a.csv", "Size": 9}, {"Key": "incoming/", "Size": 0}]}
    def client(service, **kwargs):
        calls.append(("client", service, kwargs))
        return FakeS3Client()
    monkeypatch.setitem(sys.modules, "boto3", SimpleNamespace(client=client))

    storage = S3Storage({"bucket": "chosen-bucket", "region": "eu-west-1", "auth_method": "access_key"},
                        {"access_key_id": "REQUESTKEY", "secret_access_key": "REQUESTSECRET", "session_token": "TEMP"})
    assert calls[0] == ("client", "s3", {"aws_access_key_id": "REQUESTKEY", "aws_secret_access_key": "REQUESTSECRET",
                                           "aws_session_token": "TEMP", "region_name": "eu-west-1"})
    assert calls[1] == ("head_bucket", {"Bucket": "chosen-bucket"})
    assert storage.list_files("incoming/") == [{"name": "incoming/a.csv", "size": 9}]

    calls.clear()
    S3Storage({"bucket": "chosen-bucket", "region": "eu-west-1", "auth_method": "default_chain"}, {})
    assert calls[0] == ("client", "s3", {"region_name": "eu-west-1"})
