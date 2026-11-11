import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool
from app.main import app
import app.main as main
from app.config import settings
from app.database import Base, get_db
from app.models import Dataset, Execution, Job, Rule
from app.schemas import JobCreate
from app.main import validate_job_inputs
import app.processing as processing


@pytest.fixture(autouse=True)
def disable_environment_databricks_output(monkeypatch):
    # These API tests intentionally exercise local output even when the developer's
    # project .env contains live Databricks configuration.
    monkeypatch.setattr(settings, "databricks_server_hostname", None)
    monkeypatch.setattr(settings, "databricks_http_path", None)
    monkeypatch.setattr(settings, "databricks_token", None)


def test_capabilities_and_health():
    client = TestClient(app)
    assert client.get("/health").json()["local_workflow"] == "ready"
    body = client.get("/api/v1/capabilities").json()
    assert body["destinations"]["local"] == "ready"


def test_job_string_validation_trims_required_values_and_normalizes_optional_values():
    from pydantic import ValidationError
    from app.schemas import CredentialCreate, RuleCreate

    with pytest.raises(ValidationError):
        JobCreate(name="   ", dataset_ids=[1])
    with pytest.raises(ValidationError):
        RuleCreate(column_name="  ", rule_type="NOT_NULL")
    with pytest.raises(ValidationError):
        CredentialCreate(name="  ", provider="s3", secrets={})
    with pytest.raises(ValidationError):
        CredentialCreate(name="S3", provider="s3", secrets={"access_key_id": " ", "secret_access_key": " "},
                         config={"bucket": "  ", "region": "us-east-1"})

    job = JobCreate(name="  Daily checks  ", description=" \t ", dataset_ids=[1], environment="  ")
    assert job.name == "Daily checks"
    assert job.description is None
    assert job.environment is None
    rule = RuleCreate(column_name=" Employee ID ", rule_type=" NOT_NULL ")
    assert rule.column_name == "Employee ID" and rule.rule_type == "NOT_NULL"


def test_job_can_be_created_as_named_draft_without_dataset(test_data_dir, monkeypatch):
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        empty = client.post("/api/v1/jobs", json={"name": "   ", "dataset_ids": []})
        assert empty.status_code == 422
        created = client.post("/api/v1/jobs", json={"name": "  Draft validation  ", "description": "  optional  ",
            "dataset_ids": [], "selected_columns": {}, "destination": {"type": "local"}})
        assert created.status_code == 201, created.text
        body = created.json()
        assert body["name"] == "Draft validation"
        assert body["description"] == "optional"
        assert body["dataset_ids"] == []
        assert client.get("/api/v1/jobs").json()[0]["id"] == body["id"]
    finally:
        app.dependency_overrides.pop(get_db, None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_s3_excel_import_selection_and_job_configuration_keep_all_rows_and_headers(monkeypatch, test_data_dir):
    from io import BytesIO
    from openpyxl import Workbook
    from cryptography.fernet import Fernet
    from app.models import Credential

    columns = ["Employee ID", "First Name", "Last Name", "Gender", "Age", "Email",
               "Department", "Designation", "Salary", "Joining Date", "Phone", "Location"]
    workbook = Workbook()
    sheet = workbook.active
    sheet.append(columns)
    for index in range(1000):
        sheet.append([index + 1, f"First {index}", f"Last {index}", "Other", 20 + index % 50,
                      f"person{index}@example.com", "Ops", "Analyst", 50000 + index,
                      "2024-01-01", f"555{index:07d}", "Remote"])
    buffer = BytesIO()
    workbook.save(buffer)
    excel_bytes = buffer.getvalue()

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))
    monkeypatch.setattr(settings, "secret_encryption_key", Fernet.generate_key().decode())

    class FakeS3Adapter:
        def read(self, key):
            assert key == "incoming/team roster.xlsx"
            return excel_bytes
        def close(self):
            pass

    monkeypatch.setattr("app.main.get_source_adapter", lambda provider, config, secrets: FakeS3Adapter())

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        with TestingSession() as db:
            credential = Credential(name="test S3", provider="s3", secret_ciphertext="unused",
                                    config={"bucket": "test-bucket", "region": "us-east-1"})
            credential.secret_ciphertext = __import__("app.main", fromlist=["credential_key"]).credential_key().encrypt(b"{}").decode()
            db.add(credential)
            db.commit()
            credential_id = credential.id

        client = TestClient(app)
        imported = client.post(f"/api/v1/datasets/import/{credential_id}?object_key=incoming%2Fteam%20roster.xlsx")
        assert imported.status_code == 201, imported.text
        dataset = imported.json()
        dataset_id = dataset["id"]
        assert dataset["filename"] == "team roster.xlsx"
        assert dataset["source_type"] == "s3"
        assert dataset["schema_json"]["_source"] == {"credential_id": credential_id, "object_key": "incoming/team roster.xlsx"}
        assert dataset["schema_json"]["row_count"] == 1000
        assert list(dataset["schema_json"]["columns"]) == columns

        selected = {str(dataset_id): columns}
        saved_selection = client.put("/api/v1/workflows/workflow-s3/selection", json={
            "dataset_ids": [dataset_id], "source_connection_ids": {str(dataset_id): credential_id},
            "selected_columns": selected,
        })
        assert saved_selection.status_code == 200, saved_selection.text
        assert saved_selection.json()["selected_columns"] == selected
        assert client.get("/api/v1/workflows/workflow-s3/selection").json()["selected_columns"] == selected

        # Previously imported files can have workflow-level source linkage but
        # predate the embedded _source metadata field.
        with TestingSession() as db:
            imported_row = db.get(Dataset, dataset_id)
            imported_row.schema_json = {key: value for key, value in imported_row.schema_json.items() if key != "_source"}
            db.commit()
        legacy_selection = client.put("/api/v1/workflows/workflow-s3/selection", json={
            "dataset_ids": [dataset_id], "source_connection_ids": {str(dataset_id): credential_id},
            "selected_columns": selected,
        })
        assert legacy_selection.status_code == 200, legacy_selection.text

        invalid_name = client.post("/api/v1/jobs", json={"name": " \t ", "dataset_ids": [dataset_id]})
        assert invalid_name.status_code == 422
        created = client.post("/api/v1/jobs", json={"name": "  Team roster checks  ", "description": "   ",
            "dataset_ids": [dataset_id], "selected_columns": selected, "environment": None,
            "destination": {"type": "local"}, "settings": {"combine_mode": "union"}})
        assert created.status_code == 201, created.text
        job = created.json()
        assert job["name"] == "Team roster checks" and job["description"] is None
        assert job["dataset_ids"] == [dataset_id] and job["selected_columns"] == selected
        landing = client.get(f"/api/v1/jobs/{job['id']}/landing-zone").json()
        assert landing["selected_columns"] == selected
        assert landing["files"][0]["schema_json"]["row_count"] == 1000
    finally:
        app.dependency_overrides.pop(get_db, None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_openai_corrections_are_evidence_bound_persistent_and_revalidated(test_data_dir, monkeypatch):
    import json

    test_engine=create_engine("sqlite://",connect_args={"check_same_thread":False},poolclass=StaticPool)
    TestingSession=sessionmaker(bind=test_engine,autoflush=False,expire_on_commit=False)
    monkeypatch.setattr(processing,"SessionLocal",TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings,"local_data_dir",str(test_data_dir/"data"))
    monkeypatch.setattr(settings,"openai_api_key","mock-key")
    monkeypatch.setattr(settings,"openai_model","mock-model")
    captured=[]
    async def fake_openai_chat(messages, *, timeout):
        captured.append(messages)
        data=json.loads(messages[0]["content"])
        if "items" in data:
            return json.dumps({"suggestions": [
                {"key": item["key"], "suggested_value": (item["allowed_candidates"][0]["value"] if item.get("allowed_candidates") else None),
                 "explanation": "Selected from verified evidence." if item.get("allowed_candidates") else "No supported value exists.",
                 "confidence": "high" if item.get("allowed_candidates") else "low",
                 "reason": "Approved rule or matching source record." if item.get("allowed_candidates") else "No matching reference value."}
                for item in data["items"]
            ]})
        candidates=data["allowed_candidates"]
        value=candidates[0]["value"] if candidates else None
        return json.dumps({"suggested_value":value,"explanation":"Selected from verified evidence." if value is not None else "No supported value exists.","confidence":"high" if value is not None else "low","reason":"Approved rule or matching source record." if value is not None else "No matching reference value.","safe_to_apply":value is not None})
    monkeypatch.setattr(main,"_openai_json_chat",fake_openai_chat)
    def override_db():
        with TestingSession() as db: yield db
    app.dependency_overrides[get_db]=override_db
    try:
        client=TestClient(app)
        csv=("employee_id,name,email,department,salary,age\n"
             "1,Alice Example,,Engineering,500,34\n1,Alice Example,alice@example.com,Engineering,500,34\n"
             "2,Bob Example,bob@example.com,Unknown,100,41\n3,Cara Example,cara@example.com,Engineering,-10,-5\n"
             "4,,dave@example.com,Engineering,900,29\n5,Neha Patel,employee324gmail.com,Marketing,777,31\n")
        upload=client.post("/api/v1/datasets/upload",files={"file":("employees.csv",csv.encode(),"text/csv")})
        assert upload.status_code==201,upload.text
        dataset_id=upload.json()["id"]
        job=client.post("/api/v1/jobs",json={"name":"OpenAI correction test","dataset_ids":[dataset_id],"destination":{"type":"local"}})
        assert job.status_code==201,job.text
        job_id=job.json()["id"]
        for column,kind,parameters in [("name","NOT_NULL",{}),("email","EMAIL_FORMAT",{}),("department","IN_SET",{"values":["Engineering","Marketing"]}),("salary","RANGE",{"min":0}),("age","RANGE",{"min":0})]:
            response=client.post(f"/api/v1/jobs/{job_id}/rules",json={"column_name":column,"rule_type":kind,"parameters":parameters,"enabled":True,"approved":True})
            assert response.status_code==201,response.text
        run=client.post(f"/api/v1/jobs/{job_id}/execute")
        assert run.status_code==202,run.text
        execution_id=run.json()["id"]
        assert client.get(f"/api/v1/executions/{execution_id}").json()["status"]=="succeeded"
        suggestions=client.get(f"/api/v1/executions/{execution_id}/correction-suggestions")
        assert suggestions.status_code==200,suggestions.text
        entries=suggestions.json()
        batch=client.get(f"/api/v1/executions/{execution_id}/correction-suggestions?offset=0&limit=2")
        assert batch.status_code==200,batch.text
        assert batch.json()["processed_records"]==2
        cached_batch = client.get(f"/api/v1/executions/{execution_id}/correction-suggestions?offset=0&limit=2").json()
        assert cached_batch["suggestions"] == batch.json()["suggestions"]
        assert cached_batch["ai_requests"] == 0
        email=next(item for item in entries if item["column_name"]=="email" and item["row_index"]==0)
        assert email["suggested_value"]=="alice@example.com" and email["safe_to_apply"] is True
        assert all("allowed_candidates" in call[0]["content"] for call in captured)
        department=next(item for item in entries if item["column_name"]=="department")
        assert department["suggested_value"] in {"Engineering","Marketing"} and department["safe_to_apply"] is True
        salaries=[item for item in entries if item["column_name"]=="salary"]
        assert salaries and all(item["suggested_value"] is None and not item["safe_to_apply"] for item in salaries)
        no_match=next(item for item in entries if item["column_name"]=="email" and item["row_index"]==5)
        assert no_match["suggested_value"] == "employee324@gmail.com" and not no_match["safe_to_apply"]
        assert no_match["verification_status"] == "UNVERIFIED"
        assert "unverified" in no_match["reason"].lower()
        missing_name=next(item for item in entries if item["column_name"]=="name")
        assert missing_name["suggested_value"] is None and not missing_name["safe_to_apply"]
        age=next(item for item in entries if item["column_name"]=="age")
        assert age["suggested_value"] is None and not age["safe_to_apply"]
        single_payloads=[json.loads(call[0]["content"]) for call in captured if "column" in json.loads(call[0]["content"])]
        requested_columns={payload["column"] for payload in single_payloads}
        assert {"name","email","department","salary","age"} <= requested_columns
        row_three_columns={payload["column"] for payload in single_payloads
                           if payload["failed_record"].get("employee_id")=="3"}
        assert {"salary","age"} <= row_three_columns
        for _ in range(2):
            saved=client.post(f"/api/v1/executions/{execution_id}/corrections",json={"row_index":0,"column_name":"email","original_value":"","value":email["suggested_value"]})
            assert saved.status_code==200,saved.text
            assert saved.json()["execution_id"]==execution_id
            assert saved.json()["record"]["email"]==email["suggested_value"]
        assert saved.json()["already_applied"] is True
        ai_applied=client.post(f"/api/v1/executions/{execution_id}/corrections/bulk",json={"corrections":[{
            "row_index":department["row_index"],"column_name":"department","value":department["suggested_value"],
            "suggested_value":department["suggested_value"],"confidence":department["confidence"],
            "verification_status":department["evidence_status"],"safe_to_apply":department["safe_to_apply"]}]})
        assert ai_applied.status_code==200,ai_applied.text
        assert ai_applied.json()["success_count"]==1
        assert len(client.get(f"/api/v1/executions/{execution_id}/corrections").json())==2
        assert client.get(f"/api/v1/executions/{execution_id}/failed").json()[0]["record"]["email"]=="alice@example.com"
        revalidated=client.post(f"/api/v1/executions/{execution_id}/revalidate")
        assert revalidated.status_code==200,revalidated.text
        assert revalidated.json()["total_passed"]>=2
        new_execution_id=revalidated.json()["execution_id"]
        assert new_execution_id != execution_id
        history=client.get(f"/api/v1/executions/{new_execution_id}/corrections").json()
        assert len(history)==2
        assert {item["correction_type"] for item in history}=={"AI","Manual"}
        pending_suggestions=client.get(f"/api/v1/executions/{new_execution_id}/correction-suggestions")
        assert pending_suggestions.status_code==200,pending_suggestions.text
        assert all(not (item["row_index"]==department["row_index"] and item["column_name"]=="department")
                   for item in pending_suggestions.json())
        summary=client.get(f"/api/v1/executions/{new_execution_id}/correction-summary").json()
        assert summary["total_failed_records"] == len(client.get(f"/api/v1/executions/{new_execution_id}/failed").json())
        assert summary["ai_suggestions_generated"] == len({item["row_index"] for item in pending_suggestions.json()
                                                           if item.get("suggested_value") is not None})
        assert summary["ai_corrections_applied"] == 1
        assert summary["manual_corrections_applied"] == 1
        assert summary["revalidated_execution_id"] == new_execution_id
        from io import BytesIO
        from openpyxl import load_workbook
        exported=client.get(f"/api/v1/executions/{new_execution_id}/exports/corrections.xlsx")
        assert exported.status_code==200,exported.text
        workbook=load_workbook(BytesIO(exported.content))
        assert workbook.sheetnames==["All Corrections","AI Corrections","Manual Corrections","Remaining Failures"]
        assert workbook["All Corrections"].max_row==3
        assert workbook["AI Corrections"].max_row==2
        assert workbook["Manual Corrections"].max_row==2
        repeat=load_workbook(BytesIO(client.get(f"/api/v1/executions/{new_execution_id}/exports/corrections.xlsx").content))
        assert [[cell.value for cell in row] for row in repeat["All Corrections"].iter_rows()] == [[cell.value for cell in row] for row in workbook["All Corrections"].iter_rows()]
        remaining=client.get(f"/api/v1/executions/{execution_id}/failed").json()
        assert any(item["row_index"]==0 for item in remaining)
        assert len(remaining)==5
    finally:
        app.dependency_overrides.pop(get_db,None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_csv_and_xlsx_uploads_keep_filename_and_workflow_columns(test_data_dir, monkeypatch):
    from io import BytesIO
    from openpyxl import Workbook

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        workbook = Workbook()
        sheet = workbook.active
        sheet.append(["Employee ID", "Department"])
        sheet.append([101, "Engineering"])
        content = BytesIO()
        workbook.save(content)
        client = TestClient(app)
        uploaded = [
            client.post("/api/v1/datasets/upload", files={"file": ("employee.csv", b"Employee ID,Department\n101,Engineering\n", "text/csv")}),
            client.post("/api/v1/datasets/upload", files={"file": ("employee.xlsx", content.getvalue(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}),
        ]
        assert [response.status_code for response in uploaded] == [201, 201]
        datasets = [response.json() for response in uploaded]
        assert [dataset["filename"] for dataset in datasets] == ["employee.csv", "employee.xlsx"]
        assert list(datasets[0]["schema_json"]["columns"]) == ["Employee ID", "Department"]
        assert list(datasets[1]["schema_json"]["columns"]) == ["Employee ID", "Department"]
        ids = [dataset["id"] for dataset in datasets]
        selected = {str(ids[0]): ["Department"], str(ids[1]): ["Employee ID"]}
        saved = client.put("/api/v1/workflows/test-workflow/selection", json={
            "dataset_ids": ids, "selected_columns": selected,
        })
        assert saved.status_code == 200, saved.text
        assert saved.json()["selected_columns"] == selected
        restored = client.get("/api/v1/workflows/test-workflow/selection").json()
        assert restored["selected_columns"] == selected
        assert [dataset["filename"] for dataset in restored["datasets"]] == ["employee.csv", "employee.xlsx"]
    finally:
        app.dependency_overrides.pop(get_db, None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_approve_all_rules_is_job_scoped_idempotent_and_preserves_disabled_rules(test_data_dir, monkeypatch):
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        with TestingSession() as db:
            dataset = Dataset(name="checks", filename="checks.csv", path="checks.csv",
                              schema_json={"columns": {"alpha": {}, "beta": {}, "gamma": {}}})
            db.add(dataset)
            db.flush()
            job = Job(name="approve all", dataset_ids=[dataset.id], selected_columns={str(dataset.id): ["alpha", "beta", "gamma"]})
            other_job = Job(name="other", dataset_ids=[dataset.id], selected_columns={str(dataset.id): ["alpha"]})
            db.add_all([job, other_job])
            db.flush()
            pending_one = Rule(job_id=job.id, dataset_id=dataset.id, column_name="alpha", rule_type="NOT_NULL",
                               parameters={"rationale": "AI suggested from profiling."}, enabled=True, approved=False)
            pending_two = Rule(job_id=job.id, dataset_id=dataset.id, column_name="beta", rule_type="LENGTH",
                               parameters={"min": 2, "rationale": "AI suggested from profiling."}, enabled=True, approved=False)
            already = Rule(job_id=job.id, dataset_id=dataset.id, column_name="gamma", rule_type="DATA_TYPE",
                           parameters={"type": "string"}, enabled=True, approved=True)
            disabled = Rule(job_id=job.id, dataset_id=dataset.id, column_name="alpha", rule_type="REGEX",
                            parameters={"pattern": "^[A-Z]+$", "rationale": "AI suggested from profiling."}, enabled=False, approved=False)
            invalid = Rule(job_id=job.id, dataset_id=dataset.id, column_name="beta", rule_type="REGEX",
                           parameters={"pattern": "[", "rationale": "AI suggested from profiling."}, enabled=True, approved=False)
            non_ai_pending = Rule(job_id=job.id, dataset_id=dataset.id, column_name="gamma", rule_type="NOT_NULL",
                                  parameters={}, enabled=True, approved=False)
            other_rule = Rule(job_id=other_job.id, dataset_id=dataset.id, column_name="alpha", rule_type="NOT_NULL",
                              parameters={"rationale": "AI suggested from profiling."}, enabled=True, approved=False)
            db.add_all([pending_one, pending_two, already, disabled, invalid, non_ai_pending, other_rule])
            db.commit()
            job_id, other_rule_id = job.id, other_rule.id

        client = TestClient(app)
        response = client.post(f"/api/v1/jobs/{job_id}/rules/approve-all")
        assert response.status_code == 200, response.text
        body = response.json()
        assert body["approved_count"] == 2
        assert body["failed_count"] == 1
        assert body["skipped_count"] == 3

        rules = {rule["id"]: rule for rule in client.get(f"/api/v1/jobs/{job_id}/rules").json()}
        approved_ids = set(body["approved_rule_ids"])
        assert all(rules[rule_id]["approved"] and rules[rule_id]["enabled"] for rule_id in approved_ids)
        assert next(rule for rule in rules.values() if rule["column_name"] == "gamma")["approved"] is True
        disabled_rule = next(rule for rule in rules.values() if rule["enabled"] is False)
        assert disabled_rule["approved"] is False
        invalid_rule = next(rule for rule in rules.values() if rule["parameters"].get("pattern") == "[")
        assert invalid_rule["approved"] is False
        assert next(rule for rule in rules.values() if rule["column_name"] == "gamma" and not rule["parameters"])["approved"] is False
        other = client.get(f"/api/v1/jobs/{other_rule_id}/rules")
        assert other.status_code == 404
        with TestingSession() as db:
            assert db.get(Rule, other_rule_id).approved is False

        repeated = client.post(f"/api/v1/jobs/{job_id}/rules/approve-all").json()
        assert repeated["approved_count"] == 0
        assert repeated["failed_count"] == 1
    finally:
        app.dependency_overrides.pop(get_db, None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_slow_databricks_revalidation_uses_saved_rows_and_new_execution(monkeypatch, test_data_dir):
    """A slow output write uses saved rows and preserves the previous execution."""
    import json
    import time
    from pathlib import Path
    import app.main as main

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(main, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))
    monkeypatch.setattr(settings, "databricks_server_hostname", "workspace.example.com")
    monkeypatch.setattr(settings, "databricks_http_path", "/sql/1")
    monkeypatch.setattr(settings, "databricks_token", "test-token")

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        with TestingSession() as db:
            dataset = Dataset(name="employees", filename="employees.csv", path="employees.csv",
                              schema_json={"columns": {"email": {}}})
            db.add(dataset)
            db.flush()
            dataset_id = dataset.id
            job = Job(name="slow Databricks", dataset_ids=[dataset.id], selected_columns={str(dataset.id): ["email"]},
                      destination={"type": "databricks"})
            db.add(job)
            db.flush()
            execution = Execution(job_id=job.id, status="succeeded", passed_count=0, failed_count=1,
                                  rule_snapshot=[{"id": 1, "job_id": job.id, "dataset_id": dataset.id,
                                      "column_name": "email", "rule_type": "EMAIL_FORMAT",
                                      "parameters": {}, "enabled": True, "approved": True}])
            db.add(execution)
            db.flush()
            execution_id = execution.id
            execution.report_path = str(test_data_dir / "data" / "outputs" / "execution" / "report.json")
            db.commit()

        base = Path(execution.report_path).parent
        base.mkdir(parents=True)
        (base / "passed.csv").write_text("email\n", encoding="utf-8")
        (base / "failed.json").write_text(json.dumps([{
            "row_index": 27, "record": {"email": "bad"},
            "failure_reasons": ["Invalid email format: 'bad' is not a valid email address."],
        }]), encoding="utf-8")
        (base / "report.json").write_text(json.dumps({"records_loaded": 1, "output_dataset_id": dataset_id,
            "destination_result": {"type": "databricks", "failed_table": "employees_fail"}}), encoding="utf-8")

        def unexpected_remote_read(*args, **kwargs):
            raise AssertionError("revalidation must not reread the Databricks FAIL table")

        captured = {}
        def slow_write(destination, secrets, run_path, passed, failed, report, csv_bytes, **kwargs):
            time.sleep(0.05)
            captured.update(destination=destination, execution_id=kwargs["execution_id"],
                            passed=passed, failed=failed,
                            report_passed=report["passed"], report_failed=report["failed"])
            return {"type": "databricks", "status": "saved", "pass_status": "saved",
                    "fail_status": "saved", "passed_table": "employees_pass", "failed_table": "employees_fail"}

        monkeypatch.setattr(main, "read_databricks_table", unexpected_remote_read)
        monkeypatch.setattr(main, "write_destination", slow_write)
        client = TestClient(app)
        saved = client.post(f"/api/v1/executions/{execution_id}/corrections", json={
            "row_index": 27, "column_name": "email", "value": "fixed@example.com"})
        assert saved.status_code == 200, saved.text
        assert saved.json()["row_index"] == 27

        response = client.post(f"/api/v1/executions/{execution_id}/revalidate")
        assert response.status_code == 200, response.text
        assert response.json()["total_passed"] == 1 and response.json()["total_failed"] == 0
        new_execution_id = response.json()["execution_id"]
        assert new_execution_id != execution_id
        assert captured["execution_id"] == new_execution_id
        assert captured["passed"] == [{"email": "fixed@example.com"}]
        assert captured["failed"] == []
        assert (captured["report_passed"], captured["report_failed"]) == (1, 0)
        assert client.get(f"/api/v1/executions/{new_execution_id}/artifacts/revalidated_failed.json").json() == []
        assert client.get(f"/api/v1/executions/{execution_id}").json()["failed_count"] == 1
        assert client.get(f"/api/v1/executions/{new_execution_id}").json()["passed_count"] == 1

        def failed_write(*args, **kwargs):
            raise RuntimeError("warehouse insert failed")

        monkeypatch.setattr(main, "write_destination", failed_write)
        failed_retry = client.post(f"/api/v1/executions/{new_execution_id}/revalidate")
        assert failed_retry.status_code == 502
        assert failed_retry.json()["detail"]["code"] == "sql_execution_failure"
        assert "(RuntimeError): warehouse insert failed" in failed_retry.json()["detail"]["message"]
        assert client.get(f"/api/v1/executions/{execution_id}/corrections").json()[0]["new_value"] == "fixed@example.com"
        assert client.get(f"/api/v1/executions/{execution_id}").json()["id"] == execution_id

        # Retrying the same execution replaces its outputs instead of creating a
        # second run or duplicating the corrected row.
        monkeypatch.setattr(main, "write_destination", slow_write)
        retried = client.post(f"/api/v1/executions/{execution_id}/revalidate")
        assert retried.status_code == 200, retried.text
        assert retried.json()["total_passed"] == 1 and retried.json()["total_failed"] == 0
        assert client.get(f"/api/v1/executions/{execution_id}").json()["id"] == execution_id
    finally:
        app.dependency_overrides.pop(get_db, None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_job_selection_is_explicit_and_columns_keep_dataset_association():
    first = Dataset(id=11, name="first", filename="first.csv", path="first.csv",
                    schema_json={"columns": {"shared": {}, "first_only": {}}})
    second = Dataset(id=12, name="second", filename="second.csv", path="second.csv",
                     schema_json={"columns": {"shared": {}, "second_only": {}}})
    job = JobCreate(name="selected sources", dataset_ids=[11, 12],
                    selected_columns={"11": ["shared"], "12": ["second_only"]},
                    settings={"combine_mode": "union"})
    validate_job_inputs(job, [first, second])
    assert job.selected_columns["11"] == ["shared"]
    assert job.selected_columns["12"] == ["second_only"]

    job.selected_columns = {"11": [], "12": []}
    with pytest.raises(HTTPException, match="Select at least one column"):
        validate_job_inputs(job, [first, second])

    job.selected_columns = {"11": ["shared"], "99": ["shared"]}
    with pytest.raises(HTTPException, match="outside the job"):
        validate_job_inputs(job, [first, second])


def test_landing_selection_is_scoped_to_workflow_and_survives_refresh():
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        with TestingSession() as db:
            selected = Dataset(name="employee", filename="employee.csv", path="selected.csv", source_type="s3",
                               schema_json={"columns": {"name": {}}, "_source": {"credential_id": 31, "object_key": "hr/employee.csv"}})
            same_name = Dataset(name="employee", filename="employee.csv", path="other.csv", source_type="s3",
                                schema_json={"columns": {"name": {}}, "_source": {"credential_id": 32, "object_key": "archive/employee.csv"}})
            local_upload = Dataset(name="market_data", filename="market_data.csv", path="uploads/market_data.csv", source_type="local",
                                   schema_json={"columns": {"price": {}}, "row_count": 8523})
            historical = Dataset(name="run output", filename="passed.csv", path="passed.csv", source_type="derived",
                                 schema_json={"columns": {"name": {}}, "_source": {"artifact_type": "pass_output"}})
            db.add_all([selected, same_name, local_upload, historical]); db.commit()
            selected_id, other_id, local_id, historical_id = selected.id, same_name.id, local_upload.id, historical.id
        client = TestClient(app)
        response = client.put("/api/v1/workflows/workflow-b/selection", json={
            "dataset_ids": [selected_id], "source_connection_ids": {str(selected_id): 31}})
        assert response.status_code == 200
        assert response.json()["workflow_id"] == "workflow-b"
        assert response.json()["dataset_ids"] == [selected_id]
        assert response.json()["datasets"][0]["source_type"] == "s3"
        # A browser refresh reads this workflow's exact backend selection.
        refreshed = client.get("/api/v1/workflows/workflow-b/selection").json()
        assert refreshed["dataset_ids"] == [selected_id]
        assert [item["id"] for item in refreshed["datasets"]] == [selected_id]
        # Workflow A's local upload remains available and does not leak into B.
        workflow_a = client.put("/api/v1/workflows/workflow-a/selection", json={"dataset_ids": [local_id]})
        assert workflow_a.status_code == 200
        assert client.get("/api/v1/workflows/workflow-b/selection").json()["dataset_ids"] == [selected_id]
        assert client.get(f"/api/v1/datasets/{local_id}").json()["filename"] == "market_data.csv"
        # The saved union job payload only receives workflow B's selection.
        job = client.post("/api/v1/jobs", json={
            "name": "workflow B", "dataset_ids": refreshed["dataset_ids"],
            "selected_columns": {str(selected_id): ["name"]}, "settings": {"combine_mode": "union"}})
        assert job.status_code == 201, job.text
        assert job.json()["dataset_ids"] == [selected_id]
        # Deselecting the only file persists an empty selection; it cannot reappear.
        empty = client.put("/api/v1/workflows/workflow-b/selection", json={"dataset_ids": []})
        assert empty.status_code == 200
        assert client.get("/api/v1/workflows/workflow-b/selection").json()["dataset_ids"] == []
        assert client.get(f"/api/v1/datasets/{other_id}").json()["source_type"] == "s3"
        assert client.put("/api/v1/workflows/workflow-b/selection", json={
            "dataset_ids": [selected_id, other_id, historical_id],
            "source_connection_ids": {str(selected_id): 32}}).status_code == 422
    finally:
        app.dependency_overrides.pop(get_db, None)
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_rule_snapshot_is_scoped_to_job_and_selected_datasets():
    from app.main import rule_snapshot

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    Base.metadata.create_all(test_engine)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    try:
        with TestingSession() as db:
            selected = Dataset(name="selected", filename="selected.csv", path="selected.csv", schema_json={"columns": {"value": {}}})
            unrelated = Dataset(name="unrelated", filename="unrelated.csv", path="unrelated.csv", schema_json={"columns": {"value": {}}})
            db.add_all([selected, unrelated]); db.flush()
            job = Job(name="scoped job", dataset_ids=[selected.id], selected_columns={str(selected.id): ["value"]}, destination={"type": "local"})
            other_job = Job(name="other job", dataset_ids=[selected.id], selected_columns={}, destination={"type": "local"})
            db.add_all([job, other_job]); db.flush()
            expected = Rule(job_id=job.id, dataset_id=selected.id, column_name="value", rule_type="NOT_NULL", parameters={}, enabled=True, approved=True)
            wrong_dataset = Rule(job_id=job.id, dataset_id=unrelated.id, column_name="value", rule_type="NOT_NULL", parameters={}, enabled=True, approved=True)
            wrong_job = Rule(job_id=other_job.id, dataset_id=selected.id, column_name="value", rule_type="NOT_NULL", parameters={}, enabled=True, approved=True)
            db.add_all([expected, wrong_dataset, wrong_job]); db.commit()

            snapshot = rule_snapshot(db, job.id)
            assert [item["id"] for item in snapshot] == [expected.id]
            assert snapshot[0]["job_id"] == job.id and snapshot[0]["dataset_id"] == selected.id
    finally:
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_job_environment_persists_on_create_edit_list_and_execution(monkeypatch, test_data_dir):
    from pathlib import Path
    test_data = test_data_dir
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
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("platforms.csv", b"value\n1\n", "text/csv")})
        assert uploaded.status_code == 201, uploaded.text
        dataset_id = uploaded.json()["id"]
        expected = ["Databricks", "Snowflake", "Microsoft Fabric"]
        jobs = []
        for environment in expected:
            created = client.post("/api/v1/jobs", json={
                "name": f"{environment} job", "dataset_ids": [dataset_id],
                "environment": environment, "destination": {"type": "local"},
            })
            assert created.status_code == 201, created.text
            job = created.json()
            jobs.append(job["id"])
            assert job["environment"] == environment
            assert client.get(f"/api/v1/jobs/{job['id']}").json()["environment"] == environment
            with TestingSession() as db:
                assert db.get(Job, job["id"]).environment == environment

            configured = client.post("/api/v1/jobs/configuration", json={"job": {
                "name": f"configured {environment}", "dataset_ids": [dataset_id],
                "environment": environment, "destination": {"type": "local"},
            }, "rules": []})
            assert configured.status_code == 201, configured.text
            assert configured.json()["environment"] == environment
            assert client.get(f"/api/v1/jobs/{configured.json()['id']}").json()["environment"] == environment

        listed = {job["id"]: job for job in client.get("/api/v1/jobs").json()}
        for environment, job_id in zip(expected, jobs):
            assert listed[job_id]["environment"] == environment
            edited = client.patch(f"/api/v1/jobs/{job_id}", json={"name": f"edited {environment}"})
            assert edited.status_code == 200
            assert edited.json()["environment"] == environment
            execution = client.post(f"/api/v1/jobs/{job_id}/execute")
            assert execution.status_code == 202
            assert execution.json()["job_id"] == job_id
            assert client.get(f"/api/v1/executions/{execution.json()['id']}").json()["job_id"] == job_id
            assert client.get(f"/api/v1/jobs/{job_id}").json()["environment"] == environment

        # Replacing one complete configuration must leave sibling environments intact.
        replacement = client.put(f"/api/v1/jobs/{jobs[0]}/configuration", json={"job": {
            "name": "edited Databricks", "dataset_ids": [dataset_id], "environment": "Databricks",
            "destination": {"type": "local"},
        }, "rules": []})
        assert replacement.status_code == 200
        assert replacement.json()["environment"] == "Databricks"
        after_replacement = {job["id"]: job["environment"] for job in client.get("/api/v1/jobs").json()}
        assert [after_replacement[job_id] for job_id in jobs] == expected

        alias = client.post("/api/v1/jobs", json={
            "name": "normalized platform", "dataset_ids": [dataset_id],
            "environment": "dAtAbRiCkS", "destination": {"type": "local"},
        })
        assert alias.status_code == 201
        assert alias.json()["environment"] == "Databricks"

        # Source/destination providers do not determine the platform the user selected.
        destination_only = client.post("/api/v1/jobs", json={
            "name": "destination only", "dataset_ids": [dataset_id], "destination": {"type": "s3"},
        })
        assert destination_only.status_code == 201
        assert destination_only.json()["environment"] is None

        # Recover a legacy value only when its saved metadata identifies a platform.
        with TestingSession() as db:
            legacy = Job(name="legacy fabric", dataset_ids=[dataset_id], selected_columns={},
                         destination={"type": "local"}, environment=None,
                         settings={"platform": "fabric"})
            unknown = Job(name="legacy unknown", dataset_ids=[dataset_id], selected_columns={},
                          destination={"type": "local"}, environment=None, settings={})
            db.add_all([legacy, unknown]); db.commit(); db.refresh(legacy); db.refresh(unknown)
            legacy_id, unknown_id = legacy.id, unknown.id
        assert client.get(f"/api/v1/jobs/{legacy_id}").json()["environment"] == "Microsoft Fabric"
        assert client.get(f"/api/v1/jobs/{unknown_id}").json()["environment"] is None
    finally:
        app.dependency_overrides.pop(get_db, None)


def test_local_file_job_execution(monkeypatch, test_data_dir):
    from pathlib import Path
    test_data = test_data_dir
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
        from io import BytesIO
        from openpyxl import load_workbook
        failed_export=client.get(f"/api/v1/executions/{execution_id}/exports/failed.xlsx")
        assert failed_export.status_code == 200
        failed_sheet=load_workbook(BytesIO(failed_export.content), read_only=True).active
        failed_rows=list(failed_sheet.values)
        assert failed_rows[0][0:3] == ("Original row number", "Record ID", "email")
        assert failed_rows[1][failed_rows[0].index("DQ rule type")] == "EMAIL_FORMAT"
        assert failed_rows[1][failed_rows[0].index("Failure reason")] == failed[0]["failure_reasons"][0]
        rules_xlsx=client.get(f"/api/v1/executions/{execution_id}/exports/rules.xlsx")
        assert rules_xlsx.status_code == 200
        rules_sheet=load_workbook(BytesIO(rules_xlsx.content), read_only=True).active
        rules_rows=list(rules_sheet.values)
        assert rules_rows[1][0] == rule.json()["id"]
        assert rules_rows[1][rules_rows[0].index("Execution ID")] == execution_id
        rules_csv=client.get(f"/api/v1/executions/{execution_id}/exports/rules.csv")
        assert rules_csv.status_code == 200 and "EMAIL_FORMAT" in rules_csv.text
        correction=client.post(f"/api/v1/executions/{execution_id}/corrections",json={"row_index":0,"column_name":"email","value":"fixed@example.com"})
        assert correction.status_code == 200
        assert len(client.get(f"/api/v1/executions/{execution_id}/corrections").json()) == 1
        assert client.get(f"/api/v1/executions/{execution_id}/failed").json()[0]["record"]["email"] == "fixed@example.com"
        rechecked=client.post(f"/api/v1/executions/{execution_id}/revalidate").json()
        assert rechecked["revalidated"] == 1
        assert rechecked["passed"] == 1 and rechecked["failed"] == 0
        assert rechecked["total_passed"] == 2 and rechecked["total_failed"] == 0
        new_execution_id=rechecked["execution_id"]
        assert new_execution_id != execution_id
        assert len(client.get(f"/api/v1/executions/{execution_id}/failed").json()) == 1
        assert client.get(f"/api/v1/executions/{new_execution_id}/artifacts/revalidated_failed.json").json() == []
        refreshed=client.get(f"/api/v1/executions/{new_execution_id}").json()
        assert refreshed["passed_count"] == 2 and refreshed["failed_count"] == 0
        output=client.get(f"/api/v1/executions/{new_execution_id}/artifacts/passed.csv")
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
        employees=client.post("/api/v1/datasets/upload",files={"file":("employees.csv",b"EmployeeID,Age\nEMP018,\nEMP019,30\n","text/csv")})
        assert employees.status_code == 201, employees.text
        employee_job=client.post("/api/v1/jobs",json={"name":"manual integer correction","dataset_ids":[employees.json()["id"]],"destination":{"type":"local"}})
        employee_job_id=employee_job.json()["id"]
        age_rule=client.post(f"/api/v1/jobs/{employee_job_id}/rules",json={"column_name":"Age","rule_type":"DATA_TYPE","parameters":{"type":"integer"},"enabled":True,"approved":True})
        assert age_rule.status_code == 201, age_rule.text
        employee_run=client.post(f"/api/v1/jobs/{employee_job_id}/execute")
        employee_execution_id=employee_run.json()["id"]
        employee_failed=client.get(f"/api/v1/executions/{employee_execution_id}/failed").json()
        assert employee_failed[0]["record"]["EmployeeID"] == "EMP018"
        age_correction=client.post(f"/api/v1/executions/{employee_execution_id}/corrections",json={"row_index":0,"column_name":"Age","value":"25","original_value":""})
        assert age_correction.status_code == 200, age_correction.text
        assert client.get(f"/api/v1/executions/{employee_execution_id}/failed").json()[0]["record"]["Age"] == "25"
        age_rechecked=client.post(f"/api/v1/executions/{employee_execution_id}/revalidate").json()
        assert age_rechecked["passed"] == 1 and age_rechecked["failed"] == 0
        assert age_rechecked["total_passed"] == 2 and age_rechecked["total_failed"] == 0
        removed=client.delete(f"/api/v1/jobs/{job_id}")
        assert removed.status_code == 204
        assert client.get(f"/api/v1/jobs/{job_id}").status_code == 404
        assert client.get(f"/api/v1/executions/{execution_id}").status_code == 404
        assert client.get(f"/api/v1/jobs/{joined.json()['id']}/rules").status_code == 200
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_polling_reports_persisted_queued_execution_without_running_validation(monkeypatch, test_data_dir):
    """Status polling must stay lightweight even if a queued row outlives its worker task."""
    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        upload = client.post("/api/v1/datasets/upload", files={
            "file": ("queued.csv", b"email\na@example.com\nbad\n", "text/csv")})
        job = client.post("/api/v1/jobs", json={
            "name": "queued recovery", "dataset_ids": [upload.json()["id"]], "destination": {"type": "local"}})
        rule = client.post(f"/api/v1/jobs/{job.json()['id']}/rules", json={
            "column_name": "email", "rule_type": "EMAIL_FORMAT", "enabled": True, "approved": True})
        assert upload.status_code == 201 and job.status_code == 201 and rule.status_code == 201

        # Simulate a persisted QUEUED row whose original process-local task was lost.
        with TestingSession() as db:
            execution = Execution(job_id=job.json()["id"], status="queued", rule_snapshot=[{
                "id": rule.json()["id"], "job_id": job.json()["id"], "dataset_id": upload.json()["id"],
                "column_name": "email", "rule_type": "EMAIL_FORMAT", "parameters": {},
                "enabled": True, "approved": True,
            }])
            db.add(execution)
            db.commit()
            execution_id = execution.id

        queued_response = client.get(f"/api/v1/executions/{execution_id}").json()
        assert queued_response["status"] == "queued"
        assert queued_response["stage"] == "queued"
        repeated = client.get(f"/api/v1/executions/{execution_id}").json()
        assert repeated["status"] == "queued"
        assert repeated["stage"] == "queued"
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_validation_worker_dispatches_accepted_execution_and_rejects_duplicate(monkeypatch, test_data_dir):
    import time
    from datetime import datetime, timezone

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(main, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    calls = []
    def slow_execute(execution_id):
        calls.append(execution_id)
        with TestingSession() as db:
            execution = db.get(Execution, execution_id)
            execution.status = "running"
            execution.stage = "validating_records"
            execution.started_at = datetime.now(timezone.utc)
            db.commit()
        time.sleep(0.15)
        with TestingSession() as db:
            execution = db.get(Execution, execution_id)
            execution.status = "succeeded"
            execution.stage = "completed"
            execution.passed_count = 1
            execution.failed_count = 0
            execution.finished_at = datetime.now(timezone.utc)
            db.commit()

    monkeypatch.setattr(main, "execute", slow_execute)
    app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(app) as client:
            upload = client.post("/api/v1/datasets/upload", files={
                "file": ("worker.csv", b"email\nok@example.com\n", "text/csv")})
            assert upload.status_code == 201, upload.text
            job = client.post("/api/v1/jobs", json={
                "name": "worker dispatch", "dataset_ids": [upload.json()["id"]], "destination": {"type": "local"}})
            assert job.status_code == 201, job.text
            rule = client.post(f"/api/v1/jobs/{job.json()['id']}/rules", json={
                "column_name": "email", "rule_type": "EMAIL_FORMAT", "enabled": True, "approved": True})
            assert rule.status_code == 201, rule.text

            started = client.post(f"/api/v1/jobs/{job.json()['id']}/execute")
            assert started.status_code == 202, started.text
            duplicate = client.post(f"/api/v1/jobs/{job.json()['id']}/execute")
            assert duplicate.status_code == 409

            seen_running = False
            final = None
            for _ in range(30):
                status = client.get(f"/api/v1/executions/{started.json()['id']}").json()
                seen_running = seen_running or status["status"] == "running"
                if status["status"] == "succeeded":
                    final = status
                    break
                time.sleep(0.03)
            assert seen_running
            assert final is not None and final["stage"] == "completed"
            assert calls == [started.json()["id"]]
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_execution_status_polling_does_not_execute_queued_work(monkeypatch):
    import app.main as main

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(main, "SessionLocal", TestingSession)

    def override_db():
        with TestingSession() as db:
            yield db

    def forbidden_execute(execution_id):
        raise AssertionError("status polling must not execute validation")

    monkeypatch.setattr(main, "execute", forbidden_execute)
    app.dependency_overrides[get_db] = override_db
    try:
        with TestingSession() as db:
            job = Job(name="queued", dataset_ids=[1], destination={"type": "local"})
            db.add(job); db.flush()
            execution = Execution(job_id=job.id, status="queued")
            db.add(execution); db.commit()
            execution_id = execution.id
        response = TestClient(app).get(f"/api/v1/executions/{execution_id}")
        assert response.status_code == 200
        assert response.json()["status"] == "queued"
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_validation_worker_completes_1000_row_and_larger_datasets(monkeypatch, test_data_dir):
    import time

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(main, "SessionLocal", TestingSession)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    def wait_for_terminal(client, execution_id):
        for _ in range(120):
            status = client.get(f"/api/v1/executions/{execution_id}").json()
            if status["status"] in {"succeeded", "failed", "incomplete"}:
                return status
            time.sleep(0.05)
        pytest.fail(f"Execution {execution_id} did not reach a terminal state")

    app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(app) as client:
            for row_count in (1000, 2500):
                csv = "record_id,score\n" + "\n".join(f"{index},{index % 100}" for index in range(row_count)) + "\n"
                uploaded = client.post("/api/v1/datasets/upload", files={
                    "file": (f"measurements-{row_count}.csv", csv.encode(), "text/csv")})
                assert uploaded.status_code == 201, uploaded.text
                job = client.post("/api/v1/jobs", json={
                    "name": f"{row_count} row validation", "dataset_ids": [uploaded.json()["id"]],
                    "destination": {"type": "local"}})
                assert job.status_code == 201, job.text
                rule = client.post(f"/api/v1/jobs/{job.json()['id']}/rules", json={
                    "dataset_id": uploaded.json()["id"], "column_name": "score",
                    "rule_type": "DATA_TYPE", "parameters": {"type": "integer"},
                    "enabled": True, "approved": True})
                assert rule.status_code == 201, rule.text

                started = client.post(f"/api/v1/jobs/{job.json()['id']}/execute")
                assert started.status_code == 202, started.text
                result = wait_for_terminal(client, started.json()["id"])
                assert result["status"] == "succeeded"
                assert result["stage"] == "completed"
                assert (result["passed_count"], result["failed_count"]) == (row_count, 0)
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_validation_worker_recovers_after_dispatch_failure(monkeypatch, test_data_dir):
    import time
    from datetime import datetime, timezone

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(main, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    calls = []
    fail_ids = set()
    def flaky_execute(execution_id):
        calls.append(execution_id)
        if execution_id in fail_ids:
            raise RuntimeError("simulated dispatch crash")
        with TestingSession() as db:
            execution = db.get(Execution, execution_id)
            execution.status = "running"
            execution.stage = "validating_records"
            execution.started_at = datetime.now(timezone.utc)
            db.commit()
        with TestingSession() as db:
            execution = db.get(Execution, execution_id)
            execution.status = "succeeded"
            execution.stage = "completed"
            execution.passed_count = 1
            execution.finished_at = datetime.now(timezone.utc)
            db.commit()

    def wait_for(client, execution_id, terminal):
        for _ in range(60):
            status = client.get(f"/api/v1/executions/{execution_id}").json()
            if status["status"] == terminal:
                return status
            time.sleep(0.03)
        pytest.fail(f"Execution {execution_id} did not become {terminal}")

    monkeypatch.setattr(main, "execute", flaky_execute)
    app.dependency_overrides[get_db] = override_db
    try:
        with TestClient(app) as client:
            uploaded = client.post("/api/v1/datasets/upload", files={
                "file": ("worker-failure.csv", b"email\nok@example.com\n", "text/csv")})
            job = client.post("/api/v1/jobs", json={
                "name": "worker recovery", "dataset_ids": [uploaded.json()["id"]],
                "destination": {"type": "local"}})
            rule = client.post(f"/api/v1/jobs/{job.json()['id']}/rules", json={
                "dataset_id": uploaded.json()["id"], "column_name": "email",
                "rule_type": "EMAIL_FORMAT", "enabled": True, "approved": True})
            assert uploaded.status_code == 201 and job.status_code == 201 and rule.status_code == 201

            with TestingSession() as db:
                execution = Execution(job_id=job.json()["id"], status="queued", rule_snapshot=[{
                    "id": rule.json()["id"], "job_id": job.json()["id"],
                    "dataset_id": uploaded.json()["id"], "column_name": "email",
                    "rule_type": "EMAIL_FORMAT", "parameters": {},
                    "enabled": True, "approved": True,
                }])
                db.add(execution)
                db.commit()
                first_id = execution.id
            fail_ids.add(first_id)

            failed = wait_for(client, first_id, "failed")
            assert failed["stage"] == "failed"
            assert "simulated dispatch crash" in failed["error"]

            retry = client.post(f"/api/v1/executions/{first_id}/retry")
            assert retry.status_code == 202, retry.text
            recovered = wait_for(client, retry.json()["id"], "succeeded")
            assert recovered["stage"] == "completed"
            assert first_id in calls
            assert retry.json()["id"] in calls
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_failed_record_api_and_excel_export_include_all_failed_rule_details(monkeypatch, test_data_dir):
    from io import BytesIO
    from openpyxl import load_workbook

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        csv = b"alpha,beta,gamma\n,-5,\nok,7,A\n"
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("dynamic.csv", csv, "text/csv")})
        assert uploaded.status_code == 201, uploaded.text
        dataset_id = uploaded.json()["id"]
        job = client.post("/api/v1/jobs", json={
            "name": "detail export", "dataset_ids": [dataset_id], "destination": {"type": "local"}})
        assert job.status_code == 201, job.text
        job_id = job.json()["id"]
        for payload in [
            {"column_name": "alpha", "rule_type": "NOT_NULL"},
            {"column_name": "beta", "rule_type": "DATA_TYPE", "parameters": {"type": "integer"}},
            {"column_name": "beta", "rule_type": "RANGE", "parameters": {"min": 0}},
            {"column_name": "gamma", "rule_type": "IN_SET", "parameters": {"values": ["A", "B"]}},
            {"column_name": "gamma", "rule_type": "NOT_NULL", "enabled": False},
            {"column_name": "alpha", "rule_type": "LENGTH", "parameters": {"min": 10}, "approved": False},
        ]:
            body = {"dataset_id": dataset_id, "enabled": True, "approved": True, **payload}
            response = client.post(f"/api/v1/jobs/{job_id}/rules", json=body)
            assert response.status_code == 201, response.text

        started = client.post(f"/api/v1/jobs/{job_id}/execute")
        assert started.status_code == 202, started.text
        execution_id = started.json()["id"]
        failed = client.get(f"/api/v1/executions/{execution_id}/failed").json()
        assert len(failed) == 1
        assert failed[0]["row_index"] == 0
        details = failed[0]["failure_details"]
        assert {(detail["column_name"], detail["rule_type"]) for detail in details} == {
            ("alpha", "NOT_NULL"), ("beta", "RANGE"), ("gamma", "IN_SET")}

        exported = client.get(f"/api/v1/executions/{execution_id}/exports/failed.xlsx")
        assert exported.status_code == 200, exported.text
        sheet = load_workbook(BytesIO(exported.content), read_only=True).active
        rows = list(sheet.values)
        headers = rows[0]
        exported_details = {
            (row[headers.index("Column name")], row[headers.index("DQ rule type")])
            for row in rows[1:]
        }
        assert exported_details == {("alpha", "NOT_NULL"), ("beta", "RANGE"), ("gamma", "IN_SET")}
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_approved_rules_validate_customer_csv_with_record_counts(monkeypatch, test_data_dir):
    from pathlib import Path

    test_data = test_data_dir
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


def test_zero_approved_rules_are_incomplete_not_passed(monkeypatch, test_data_dir):
    from pathlib import Path

    test_data = test_data_dir
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
        assert any("Validation incomplete: approve and enable at least one applicable rule" in line for line in execution["logs"])
        report = client.get(f"/api/v1/executions/{execution['id']}/artifacts/report.json").json()
        assert report["validation_complete"] is False
        assert report["rules"] == {"executed": 0, "pending": 1, "disabled": 0, "skipped": 1}
        assert report["total"] == 0 and report["records_evaluated"] == 0 and report["records_loaded"] == 2
        assert client.get(f"/api/v1/executions/{execution['id']}/failed").json() == []
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_employee_baseline_is_idempotent_and_validates_25_records(monkeypatch, test_data_dir):
    from pathlib import Path
    test_data = test_data_dir
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


def test_generated_rules_are_approved_and_scoped_to_each_new_job(monkeypatch, test_data_dir):
    from pathlib import Path
    import app.main as main

    test_data = test_data_dir
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

    async def suggestions(datasets, db, selected_columns=None):
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
            approved_disabled = client.patch(f"/api/v1/rules/{disabled.json()['id']}", json={"approved": True})
            assert approved_disabled.status_code == 200
            assert approved_disabled.json()["approved"] is True
            assert approved_disabled.json()["enabled"] is False
            persisted_disabled = client.get(f"/api/v1/jobs/{job_id}/rules").json()
            assert next(rule for rule in persisted_disabled if rule["id"] == disabled.json()["id"])["enabled"] is False
            enabled = client.patch(f"/api/v1/rules/{disabled.json()['id']}", json={"enabled": True})
            assert enabled.status_code == 200
            assert enabled.json()["enabled"] is True and enabled.json()["approved"] is True
            approval = client.patch(f"/api/v1/rules/{generated['id']}", json={"approved": True})
            assert approval.status_code == 200 and approval.json()["approved"] is True
            started = client.post(f"/api/v1/jobs/{job_id}/execute")
            execution = client.get(f"/api/v1/executions/{started.json()['id']}").json()
            assert execution["status"] == "succeeded"
            assert execution["passed_count"] == 1 and execution["failed_count"] == 1
            report = client.get(f"/api/v1/executions/{execution['id']}/artifacts/report.json").json()
            assert report["rules"] == {"executed": 2, "pending": 1, "disabled": 0, "skipped": 1}
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


def test_ai_rule_generation_retries_invalid_rule_and_persists_valid_dynamic_rule(monkeypatch, test_data_dir):
    import json
    import app.main as main

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))
    monkeypatch.setattr(settings, "openai_api_key", "test-key")
    monkeypatch.setattr(settings, "openai_model", "gpt-5.5")

    def override_db():
        with TestingSession() as db:
            yield db

    calls = []

    async def fake_openai_chat(messages, *, timeout, response_format=None):
        calls.append(messages)
        prompt = json.loads(messages[1]["content"])
        columns = prompt["profile"][0]["columns"]
        assert columns["sku"]["distinct_sample"] == ["AB-100", "CD-200", "EF-300"]
        if len(calls) == 1:
            return json.dumps({"suggestions": [{
                "dataset_id": prompt["profile"][0]["dataset_id"], "column_name": "sku", "rule_type": "REGEX",
                "parameters": {"pattern": None}, "severity": "medium",
                "rationale": "SKU samples share a visible structure.",
            }]})
        assert "REGEX requires a pattern" in messages[-1]["content"]
        return json.dumps({"suggestions": [{
            "dataset_id": prompt["profile"][0]["dataset_id"], "column_name": "sku", "rule_type": "REGEX",
            "parameters": {"pattern": "^[A-Z]{2}-\\d{3}$"}, "severity": "medium",
            "rationale": "SKU samples use two uppercase letters, a dash, and three digits.",
        }]})

    monkeypatch.setattr(main, "_openai_json_chat", fake_openai_chat)
    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("products.csv", b"sku,qty\nAB-100,1\nCD-200,2\nEF-300,3\n", "text/csv")})
        assert uploaded.status_code == 201, uploaded.text
        dataset_id = uploaded.json()["id"]
        job = client.post("/api/v1/jobs", json={"name": "dynamic regex", "dataset_ids": [dataset_id],
            "selected_columns": {str(dataset_id): ["sku"]}})
        assert job.status_code == 201, job.text
        result = client.post(f"/api/v1/jobs/{job.json()['id']}/suggest-rules")
        assert result.status_code == 200, result.text
        assert result.json()["added"] == 1
        assert len(calls) == 2
        rules = client.get(f"/api/v1/jobs/{job.json()['id']}/rules").json()
        generated = next(rule for rule in rules if rule["rule_type"] == "REGEX")
        assert generated["dataset_id"] == dataset_id
        assert generated["column_name"] == "sku"
        assert generated["enabled"] is True and generated["approved"] is False
        assert generated["parameters"]["pattern"] == "^[A-Z]{2}-\\d{3}$"
        assert generated["parameters"]["rationale"] == "SKU samples use two uppercase letters, a dash, and three digits."
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


def test_ai_suggestion_reactivates_pending_placeholder_without_approval(monkeypatch, test_data_dir):
    import app.main as main

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    monkeypatch.setattr(processing, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "local_data_dir", str(test_data_dir / "data"))

    def override_db():
        with TestingSession() as db:
            yield db

    async def suggestions(datasets, db, selected_columns=None):
        return [{"dataset_id": datasets[0].id, "column_name": "contact", "rule_type": "NOT_NULL",
                 "parameters": {}, "severity": "medium", "rationale": "Observed missing contact values"}]

    monkeypatch.setattr(main, "generate_rule_suggestions", suggestions)
    app.dependency_overrides[get_db] = override_db
    try:
        client = TestClient(app)
        uploaded = client.post("/api/v1/datasets/upload", files={"file": ("contacts.csv", b"contact,other\n,1\nknown@example.com,2\n", "text/csv")})
        dataset_id = uploaded.json()["id"]
        job = client.post("/api/v1/jobs", json={"name": "placeholder upgrade", "dataset_ids": [dataset_id],
            "selected_columns": {str(dataset_id): ["contact"]}})
        job_id = job.json()["id"]
        placeholder = client.post(f"/api/v1/jobs/{job_id}/rules", json={"dataset_id": dataset_id,
            "column_name": "contact", "rule_type": "NOT_NULL", "enabled": False, "approved": False})
        result = client.post(f"/api/v1/jobs/{job_id}/suggest-rules")
        assert result.status_code == 200, result.text
        rules = client.get(f"/api/v1/jobs/{job_id}/rules").json()
        upgraded = next(rule for rule in rules if rule["id"] == placeholder.json()["id"])
        assert upgraded["enabled"] is True
        assert upgraded["approved"] is False
        assert upgraded["parameters"]["rationale"] == "Observed missing contact values"
        started = client.post(f"/api/v1/jobs/{job_id}/execute")
        execution = client.get(f"/api/v1/executions/{started.json()['id']}").json()
        assert execution["status"] == "incomplete"
        approval = client.patch(f"/api/v1/rules/{upgraded['id']}", json={"approved": True})
        assert approval.status_code == 200
        started = client.post(f"/api/v1/jobs/{job_id}/execute")
        execution = client.get(f"/api/v1/executions/{started.json()['id']}").json()
        assert execution["status"] == "succeeded"
        assert execution["failed_count"] == 1
    finally:
        app.dependency_overrides.clear()
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_databricks_empty_schema_is_returned_as_discovery_error_not_auth_failure(monkeypatch):
    import app.main as main
    from cryptography.fernet import Fernet
    from app.connectors.storage import SourceDiscoveryError

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)
    def override_db():
        db = TestingSession()
        try:
            yield db
        finally:
            db.close()
    class EmptySchemaAdapter:
        last_metadata = {"catalog": "request-catalog", "schema": "request-schema", "show_tables": []}
        def list_files(self, prefix=""):
            raise SourceDiscoveryError("NO_TABLES_FOUND", "No tables are available in 'request-catalog.request-schema'.", self.last_metadata)
        def close(self):
            pass
    monkeypatch.setattr(main, "get_source_adapter", lambda provider, config, secrets: EmptySchemaAdapter())
    monkeypatch.setattr(settings, "secret_encryption_key", Fernet.generate_key().decode())
    app.dependency_overrides[get_db] = override_db
    body = {"name": "test Databricks", "provider": "databricks", "config": {"server_hostname": "example.invalid", "http_path": "/sql/1", "catalog": "request-catalog", "schema": "request-schema"}, "secrets": {"access_token": "TEST_TOKEN"}}
    try:
        response = TestClient(app).post("/api/v1/sources/connect", json=body)
        assert response.status_code == 201, response.text
        assert response.json()["discovery"] == {
            "code": "NO_TABLES_FOUND",
            "message": "No tables are available in 'request-catalog.request-schema'.",
            "metadata": EmptySchemaAdapter.last_metadata,
        }
        assert "AUTHENTICATION_FAILED" not in response.text
        assert "TEST_TOKEN" not in response.text
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
    config = calls[0][2].pop("config")
    assert calls[0] == ("client", "s3", {"aws_access_key_id": "REQUESTKEY", "aws_secret_access_key": "REQUESTSECRET",
                                           "aws_session_token": "TEMP", "region_name": "eu-west-1"})
    assert config.connect_timeout == settings.source_connect_timeout_seconds
    assert config.read_timeout == settings.source_read_timeout_seconds
    assert calls[1] == ("head_bucket", {"Bucket": "chosen-bucket"})
    assert storage.list_files("incoming/") == [{"name": "incoming/a.csv", "size": 9}]

    calls.clear()
    S3Storage({"bucket": "chosen-bucket", "region": "eu-west-1", "auth_method": "default_chain"}, {})
    config = calls[0][2].pop("config")
    assert calls[0] == ("client", "s3", {"region_name": "eu-west-1"})
    assert config.connect_timeout == settings.source_connect_timeout_seconds


def test_azure_blob_storage_builds_default_endpoint_and_maps_container_errors(monkeypatch):
    import sys
    from types import SimpleNamespace
    from app.connectors.storage import AzureBlobStorage, SourceDiscoveryError

    calls = []
    state = {"properties_error": None, "blobs": []}

    class ResourceNotFoundError(Exception):
        status_code = 404
        error_code = "ContainerNotFound"
        response = SimpleNamespace(status_code=404)

    class StorageSharedKeyCredential:
        def __init__(self, account_name, account_key):
            calls.append(("credential", account_name, account_key))

    class FakeContainer:
        def get_container_properties(self, **kwargs):
            if state["properties_error"]:
                raise state["properties_error"]
            calls.append(("get_container_properties", kwargs))

        def list_blobs(self, name_starts_with="", **kwargs):
            calls.append(("list_blobs", name_starts_with, kwargs))
            return state["blobs"]

    class BlobServiceClient:
        def __init__(self, account_url, credential=None, **kwargs):
            calls.append(("client", account_url, credential, kwargs))

        @classmethod
        def from_connection_string(cls, connection, **kwargs):
            calls.append(("from_connection_string", connection, kwargs))
            return cls("from-connection-string", **kwargs)

        def get_container_client(self, container):
            calls.append(("get_container_client", container))
            return FakeContainer()

    monkeypatch.setitem(sys.modules, "azure", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "azure.storage", SimpleNamespace())
    monkeypatch.setitem(sys.modules, "azure.storage.blob",
                        SimpleNamespace(BlobServiceClient=BlobServiceClient,
                                        StorageSharedKeyCredential=StorageSharedKeyCredential))
    monkeypatch.setattr(settings, "azure_blob_account_url_template", None)

    storage = AzureBlobStorage({"account_name": "adqacct", "container": "raw"}, {"account_key": "KEY"})
    assert calls[1][1] == "https://adqacct.blob.core.windows.net"

    state["properties_error"] = ResourceNotFoundError("container does not exist")
    with pytest.raises(SourceDiscoveryError) as missing:
        storage.list_files("")
    assert missing.value.code == "AZURE_BLOB_CONTAINER_NOT_FOUND"
    assert missing.value.metadata["azure_status"] == 404

    state["properties_error"] = None
    with pytest.raises(SourceDiscoveryError) as empty_prefix:
        storage.list_files("incoming/")
    assert empty_prefix.value.code == "AZURE_BLOB_PREFIX_EMPTY"
    assert empty_prefix.value.metadata["prefix"] == "incoming/"


def test_azure_source_connection_returns_specific_backend_error(monkeypatch):
    import app.main as main
    from app.connectors.storage import SourceDiscoveryError

    class MissingContainerAdapter:
        def list_files(self, prefix=""):
            raise SourceDiscoveryError("AZURE_BLOB_CONTAINER_NOT_FOUND",
                "Container 'raw' was not found or is not visible to this user.",
                {"azure_status": 404, "azure_error_code": "ContainerNotFound"})

    monkeypatch.setattr(main, "get_source_adapter", lambda provider, config, secrets: MissingContainerAdapter())
    body = {"name": "azure raw", "provider": "azure_blob",
            "config": {"account_name": "adqacct", "container": "raw"},
            "secrets": {"account_key": "KEY"}}

    response = TestClient(app).post("/api/v1/sources/connect", json=body)
    assert response.status_code == 404
    assert response.json()["detail"]["code"] == "AZURE_BLOB_CONTAINER_NOT_FOUND"
    assert "Container 'raw'" in response.json()["detail"]["message"]


def test_async_source_connection_reuses_running_discovery_and_persists_once(monkeypatch):
    import time
    import app.main as main
    from cryptography.fernet import Fernet
    from app.models import Credential

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)

    class SlowAdapter:
        def list_files(self, prefix=""):
            state["calls"] += 1
            time.sleep(0.2)
            return [{"name": f"{prefix}orders.csv", "size": 12}]

    state = {"calls": 0}
    monkeypatch.setattr(main, "get_source_adapter", lambda provider, config, secrets: SlowAdapter())
    monkeypatch.setattr(main, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "secret_encryption_key", Fernet.generate_key().decode())
    body = {"name": "async bucket", "provider": "s3", "config": {"bucket": "test-bucket", "region": "us-east-1", "auth_method": "access_key"},
            "secrets": {"access_key_id": "TESTACCESS", "secret_access_key": "TESTSECRET"}}
    try:
        client = TestClient(app)
        first = client.post("/api/v1/sources/connect?prefix=incoming/&async=true", json=body)
        second = client.post("/api/v1/sources/connect?prefix=incoming/&async=true", json=body)
        assert first.status_code == 202, first.text
        assert second.json()["operation_id"] == first.json()["operation_id"]
        operation_id = first.json()["operation_id"]
        status = {}
        for _ in range(20):
            status = client.get(f"/api/v1/sources/connect/{operation_id}").json()
            if status["status"] == "succeeded":
                break
            time.sleep(0.05)
        assert status["status"] == "succeeded", status
        assert status["result"]["files"] == [{"name": "incoming/orders.csv", "size": 12}]
        assert state["calls"] == 1
        with TestingSession() as db:
            assert db.query(Credential).count() == 1
    finally:
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()


def test_async_source_connection_failure_reports_sanitized_error_and_can_retry(monkeypatch):
    import time
    import app.main as main
    from cryptography.fernet import Fernet

    test_engine = create_engine("sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool)
    TestingSession = sessionmaker(bind=test_engine, autoflush=False, expire_on_commit=False)
    Base.metadata.create_all(test_engine)

    class Adapter:
        def list_files(self, prefix=""):
            if state["fail"]:
                raise TimeoutError("network timeout while using TESTSECRET")
            return [{"name": "customers.csv", "size": 4}]

    state = {"fail": True}
    monkeypatch.setattr(main, "get_source_adapter", lambda provider, config, secrets: Adapter())
    monkeypatch.setattr(main, "SessionLocal", TestingSession)
    monkeypatch.setattr(settings, "secret_encryption_key", Fernet.generate_key().decode())
    body = {"name": "retry bucket", "provider": "s3", "config": {"bucket": "test-bucket", "region": "us-east-1", "auth_method": "access_key"},
            "secrets": {"access_key_id": "TESTACCESS", "secret_access_key": "TESTSECRET"}}
    try:
        client = TestClient(app)
        started = client.post("/api/v1/sources/connect?async=true", json=body)
        operation_id = started.json()["operation_id"]
        failed = {}
        for _ in range(20):
            failed = client.get(f"/api/v1/sources/connect/{operation_id}").json()
            if failed["status"] == "failed":
                break
            time.sleep(0.05)
        assert failed["status"] == "failed", failed
        assert failed["error"]["code"] == "S3_TIMEOUT"
        assert "TESTSECRET" not in failed["error"]["message"]

        state["fail"] = False
        retried = client.post("/api/v1/sources/connect?async=true", json=body)
        assert retried.json()["operation_id"] != operation_id
        retry_id = retried.json()["operation_id"]
        completed = {}
        for _ in range(20):
            completed = client.get(f"/api/v1/sources/connect/{retry_id}").json()
            if completed["status"] == "succeeded":
                break
            time.sleep(0.05)
        assert completed["status"] == "succeeded", completed
    finally:
        Base.metadata.drop_all(test_engine)
        test_engine.dispose()
