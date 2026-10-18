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
        assert failed[0]["failure_reasons"] == ["email is not a valid email address"]
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
