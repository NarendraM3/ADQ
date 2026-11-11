"""Run paired end-to-end Databricks benchmarks without touching the app DB.

Usage: py -3 benchmark_databricks.py (from backend)
"""
import csv
import json
import os
import tempfile
from pathlib import Path

root = Path(tempfile.mkdtemp(prefix="dq-databricks-benchmark-"))
os.environ["DATABASE_URL"] = f"sqlite:///{(root / 'benchmark.db').as_posix()}"
os.environ["LOCAL_DATA_DIR"] = str(root / "data")

from app.database import Base, SessionLocal, engine
from app.connectors.storage import LocalStorage
from app.models import Dataset, Execution, Job, Rule
from app.processing import execute

Base.metadata.create_all(engine)


def run(rows, fine_grained_progress):
    filename = f"data-{rows}.csv"
    path = root / filename
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.writer(stream)
        writer.writerow(("id", "email", "age", "status"))
        for index in range(rows):
            writer.writerow((index, "invalid" if index % 100 == 0 else f"u{index}@example.test",
                             150 if index % 50 == 0 else 30, "active" if index % 3 else "inactive"))
    dataset_path = LocalStorage().save(f"uploads/{filename}", path.read_bytes())
    with SessionLocal() as db:
        dataset = Dataset(name=f"benchmark-{rows}", filename=filename, path=dataset_path,
                          source_type="local", schema_json={"columns": {"id": {}, "email": {}, "age": {}, "status": {}},
                                                             "row_count": rows})
        db.add(dataset)
        db.flush()
        job = Job(name=f"benchmark-{rows}", dataset_ids=[dataset.id],
                  selected_columns={str(dataset.id): ["id", "email", "age", "status"]},
                  destination={"type": "databricks"}, settings={})
        db.add(job)
        db.flush()
        for column, kind, params in (("id", "NOT_NULL", {}), ("email", "EMAIL_FORMAT", {}),
                                     ("age", "RANGE", {"min": 0, "max": 120}),
                                     ("status", "IN_SET", {"values": ["active", "inactive"]})):
            db.add(Rule(job_id=job.id, dataset_id=dataset.id, column_name=column,
                        rule_type=kind, parameters=params, enabled=True, approved=True))
        execution = Execution(job_id=job.id, status="queued", rule_snapshot=None)
        db.add(execution)
        db.commit()
        execution_id = execution.id

    # The old behavior synchronously commits worker progress after every SQL
    # insert batch. The tuned behavior commits at coarse progress milestones.
    from app import processing
    original_execute = processing.write_destination

    def writer(*args, **kwargs):
        metrics = kwargs.setdefault("performance_metrics", {})
        metrics["fine_grained_progress"] = fine_grained_progress
        return original_execute(*args, **kwargs)

    processing.write_destination = writer
    try:
        execute(execution_id)
    finally:
        processing.write_destination = original_execute
    with SessionLocal() as db:
        finished = db.get(Execution, execution_id)
        report = json.loads(Path(finished.report_path).read_text(encoding="utf-8"))
        if finished.status != "succeeded" or report["storage_status"]["status"] != "saved":
            raise RuntimeError(f"Databricks benchmark failed: {finished.error or report['storage_status']}")
        if finished.passed_count + finished.failed_count != rows:
            raise RuntimeError("PASS/FAIL counts do not match input row count")
        return {"rows": rows, "mode": "before" if fine_grained_progress else "after",
                "status": finished.status, "passed": finished.passed_count,
                "failed": finished.failed_count, "timings_seconds": report["timings_seconds"],
                "performance_metrics": report["performance_metrics"]}


results = []
for row_count in (1_000, 10_000, 50_000, 100_000):
    results.append(run(row_count, True))
    results.append(run(row_count, False))
print(json.dumps({"benchmark_root": str(root), "results": results}, indent=2))
