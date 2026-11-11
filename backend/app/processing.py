import csv, io, json, re
from datetime import datetime, timezone
from pathlib import Path
import logging
from time import perf_counter
from types import SimpleNamespace
from sqlalchemy import or_, select, update
from app.connectors.storage import LocalStorage
from app.connectors.destinations import write_destination
from app.platforms import destination_for_job, resolve_destination_credentials
from app.database import SessionLocal
from app.models import Dataset, Execution, Job, Rule
from app.rules import is_missing, rule_signature, validate_records
from app.serialization import json_safe


def _safe_error_message(exc: Exception) -> str:
    message = str(exc).strip() or type(exc).__name__
    message = re.sub(r"(?i)(access[_-]?key|api[_-]?key|token|password|secret|authorization)(\s*[=:]\s*)[^\s,;]+",
                     r"\1\2[REDACTED]", message)
    return message[:1500]


def parse_data(filename: str, content: bytes):
    suffix = Path(filename).suffix.lower()
    if suffix == ".csv":
        text = content.decode("utf-8-sig")
        return list(csv.DictReader(io.StringIO(text)))
    if suffix == ".tsv":
        text = content.decode("utf-8-sig")
        return list(csv.DictReader(io.StringIO(text), delimiter="\t"))
    if suffix == ".json":
        value = json.loads(content)
        if isinstance(value, dict): value = value.get("records", [value])
        if not isinstance(value, list) or any(not isinstance(x, dict) for x in value):
            raise ValueError("JSON must be an array of objects or an object with a records array")
        return value
    if suffix in (".xlsx", ".xls"):
        if suffix == ".xls":
            try:
                import xlrd
                workbook = xlrd.open_workbook(file_contents=content, on_demand=True)
            except ImportError as e:
                raise ValueError("Legacy XLS support requires xlrd") from e
            sheet = workbook.sheet_by_index(0)
            rows = (sheet.row_values(index) for index in range(sheet.nrows))
        else:
            try:
                from openpyxl import load_workbook
                workbook = load_workbook(io.BytesIO(content), read_only=True, data_only=True)
            except ImportError as e:
                raise ValueError("XLSX support requires openpyxl") from e
            sheet = workbook.active
            rows = sheet.iter_rows(values_only=True)
        raw_headers = list(next(rows, []))
        data_rows = list(rows)
        headers = []
        keep = []
        for index, value in enumerate(raw_headers):
            # Empty Excel columns are common in exported workbooks. Do not
            # create synthetic fields for them unless the column has data.
            populated = any(index < len(row) and row[index] is not None for row in data_rows)
            if value is None or not str(value).strip():
                if not populated:
                    continue
                header = f"column_{index + 1}"
            else:
                header = str(value).strip()
            headers.append(header)
            keep.append(index)
        workbook.close()
        return [{headers[out_index]: row[index] for out_index, index in enumerate(keep) if index < len(row)}
                for row in data_rows if any(index < len(row) and row[index] is not None for index in keep)]
    if suffix == ".parquet":
        try:
            import pyarrow.parquet as parquet
            return parquet.read_table(io.BytesIO(content)).to_pylist()
        except ImportError as e:
            raise ValueError("Parquet support requires pyarrow") from e
    raise ValueError("Supported formats are CSV, TSV, JSON, Excel, and Parquet")


def encode_csv(records: list[dict]) -> bytes:
    if not records: return b""
    cols = list(dict.fromkeys(k for row in records for k in row))
    out = io.StringIO(newline="")
    writer = csv.DictWriter(out, fieldnames=cols, extrasaction="ignore")
    writer.writeheader(); writer.writerows(records)
    return out.getvalue().encode("utf-8")


def schema_from_records(records: list[dict]) -> dict:
    """Build schema metadata from the rows actually read for this execution."""
    columns = list(dict.fromkeys(key for row in records for key in row))
    metadata = {}
    for column in columns:
        values = [row.get(column) for row in records]
        present = [str(value).strip() for value in values if not is_missing(value)]
        if present and all(_is_integer(value) for value in present):
            kind = "integer"
        elif present and all(_is_number(value) for value in present):
            kind = "number"
        elif present and all("@" in value for value in present):
            kind = "email-like"
        else:
            kind = "string"
        metadata[column] = {"inferred_type": kind,
                            "nullable": any(is_missing(value) for value in values)}
    return {"columns": metadata, "row_count": len(records)}


def _is_integer(value):
    try:
        int(value)
        return True
    except (ValueError, TypeError):
        return False


def _is_number(value):
    try:
        float(value)
        return True
    except (ValueError, TypeError):
        return False


def validate_rule_bindings(rules, job_dataset_ids, records_by_dataset, combined_records):
    """Reject stale dataset bindings and columns absent from actual input rows."""
    from app.rules import resolve_column
    allowed = set(job_dataset_ids or [])
    for rule in rules:
        if rule.dataset_id is not None and rule.dataset_id not in allowed:
            raise ValueError(f"Rule {rule.id} references dataset {rule.dataset_id}, which is not selected by this job")
        rows = records_by_dataset.get(rule.dataset_id, []) if rule.dataset_id is not None else combined_records
        if rows:
            resolve_column(rows[0], rule.column_name)


def validate_union_records(record_sets, datasets, rules):
    """Evaluate dataset-scoped rules only against their source rows.

    Job-wide rules still see the complete union, and all outputs retain the
    original row dictionaries and ordering.
    """
    records = [row for rows in record_sets for row in rows]
    failures = {}
    global_rules = [rule for rule in rules if rule.dataset_id is None]
    if global_rules:
        _, global_failures = validate_records(records, global_rules)
        failures.update({item["row_index"]: {"reasons": list(item["failure_reasons"]),
                                             "details": list(item.get("failure_details", []))}
                         for item in global_failures})
    offset = 0
    for dataset, rows in zip(datasets, record_sets):
        scoped_rules = [rule for rule in rules if rule.dataset_id == dataset.id]
        if scoped_rules:
            _, scoped_failures = validate_records(rows, scoped_rules)
            for item in scoped_failures:
                index = offset + item["row_index"]
                failure = failures.setdefault(index, {"reasons": [], "details": []})
                failure["reasons"].extend(reason for reason in item["failure_reasons"] if reason not in failure["reasons"])
                existing_detail_reasons = {detail.get("reason") for detail in failure["details"]}
                failure["details"].extend(detail for detail in item.get("failure_details", [])
                                          if detail.get("reason") not in existing_detail_reasons)
        offset += len(rows)
    passed, failed = [], []
    for index, record in enumerate(records):
        failure = failures.get(index)
        if failure:
            failed.append({"row_index": index, "record": record,
                           "failure_reasons": failure["reasons"],
                           "failure_details": failure["details"]})
        else:
            passed.append(record)
    return passed, failed


def execute(execution_id: int):
    db = SessionLocal()
    execution_started = perf_counter()
    timings = {"source_connection_seconds": 0.0, "dataset_retrieval_seconds": 0.0,
               "rule_evaluation_seconds": 0.0, "pass_persistence_seconds": 0.0,
               "fail_persistence_seconds": 0.0, "pass_fail_persistence_seconds": 0.0}
    databricks_metrics = {"databricks_query_count": 0, "databricks_connection_openings": 0,
                          "databricks_query_seconds": 0.0, "databricks_connection_open_seconds": 0.0,
                          "databricks_statement_counts": {}, "databricks_phase_query_counts": {},
                          "databricks_phase_seconds": {}}
    try:
        # Claim queued work atomically. A duplicate task (or startup recovery
        # racing the original request) must never execute the same run twice.
        claimed = db.execute(update(Execution).where(
            Execution.id == execution_id, Execution.status == "queued"
        ).values(status="running", started_at=datetime.now(timezone.utc),
                 stage="starting", updated_at=datetime.now(timezone.utc), logs=["Execution started"]))
        db.commit()
        if claimed.rowcount != 1:
            return
        execution = db.get(Execution, execution_id)
        if execution is None:
            raise ValueError(f"Execution {execution_id} does not exist")
        def progress(stage, completed=None, total=None):
            execution.stage = stage
            if completed is not None:
                execution.progress_completed = max(0, int(completed))
            if total is not None:
                execution.progress_total = max(0, int(total))
            execution.updated_at = datetime.now(timezone.utc)
            db.commit()
        progress("reading_dataset", 0, 0)
        job = db.get(Job, execution.job_id)
        if job is None:
            raise ValueError(f"Job for execution {execution_id} does not exist")
        destination=destination_for_job(job)
        datasets = [db.get(Dataset, did) for did in (job.dataset_ids or [])]
        if not datasets:
            raise ValueError("Job must reference at least one dataset")
        if any(d is None for d in datasets): raise ValueError("Job references a missing dataset")
        if len(datasets) > 2 and (job.settings or {}).get("combine_mode") != "union": raise ValueError("A join can combine up to two datasets")
        local = LocalStorage()
        retrieval_started = perf_counter()
        # Read each selected source exactly once per execution. LocalStorage
        # files are already the materialized source snapshot, so there is no
        # additional Databricks round trip during validation.
        record_sets = [parse_data(d.filename, local.read(d.path)) for d in datasets]
        timings["dataset_retrieval_seconds"] = perf_counter() - retrieval_started
        # Refresh persisted schema metadata from the selected source bytes so a
        # previous import's stale/synthetic columns do not survive execution.
        for dataset, rows in zip(datasets, record_sets):
            if rows:
                dataset.schema_json = schema_from_records(rows)
        if len(record_sets) == 1:
            records = record_sets[0]
        elif (job.settings or {}).get("combine_mode") == "union":
            records = [row for rows in record_sets for row in rows]
        else:
            cfg = job.join_config
            if not cfg: raise ValueError("Jobs with multiple datasets require join_config")
            left_id, right_id = str(cfg.get("left_dataset_id")), str(cfg.get("right_dataset_id"))
            indexes = {str(d.id): rows for d, rows in zip(datasets, record_sets)}
            if left_id not in indexes or right_id not in indexes: raise ValueError("join_config must reference datasets selected by this job")
            left_key, right_key = cfg.get("left_key"), cfg.get("right_key")
            if not left_key or not right_key: raise ValueError("join_config requires left_key and right_key")
            right_index = {}
            for row in indexes[right_id]: right_index.setdefault(str(row.get(right_key)), []).append(row)
            records = []
            for left in indexes[left_id]:
                for right in right_index.get(str(left.get(left_key)), []):
                    merged = {**left, **{(f"{cfg.get('right_prefix', 'right_')}{k}" if k in left else k): v for k,v in right.items()}}
                    records.append(merged)
        selected = job.selected_columns or {}
        if selected:
            if (job.settings or {}).get("combine_mode") == "union" and len(record_sets) > 1:
                # Apply each source's selection before combining rows. A single
                # global column set would leak same-named, unselected fields
                # from another source into this source's union rows.
                selected_sets = []
                for dataset, rows in zip(datasets, record_sets):
                    source_columns = selected.get(str(dataset.id), selected.get(dataset.id,
                        [] if selected else list((dataset.schema_json or {}).get("columns", {}))))
                    selected_sets.append([{key: value for key, value in row.items() if key in source_columns}
                                          for row in rows])
                record_sets = selected_sets
                records = [row for rows in record_sets for row in rows]
            else:
                chosen=set()
                seen_columns=set()
                cfg=job.join_config or {}
                prefix=cfg.get("right_prefix","right_")
                by_id={d.id:d for d in datasets}
                is_join = len(datasets) == 2 and (job.settings or {}).get("combine_mode") != "union"
                if is_join:
                    # The join configuration was validated above; use its dataset order
                    # only for a join. A union has no left/right dataset keys.
                    ordered_datasets=[by_id[int(cfg[k])] for k in ("left_dataset_id","right_dataset_id")]
                else:
                    ordered_datasets=datasets
                for index,dataset in enumerate(ordered_datasets):
                    for col in selected.get(str(dataset.id), selected.get(dataset.id, [])):
                        output_col=f"{prefix}{col}" if is_join and index > 0 and col in seen_columns else col
                        chosen.add(output_col)
                    seen_columns.update((dataset.schema_json or {}).get("columns",{}).keys())
                records = [{k:v for k,v in row.items() if k in chosen} for row in records]
        if execution.rule_snapshot is not None:
            rules = [SimpleNamespace(**item) for item in execution.rule_snapshot]
        else:  # Backwards compatibility for executions queued before migration.
            scope = [Rule.dataset_id.is_(None)]
            if job.dataset_ids:
                scope.append(Rule.dataset_id.in_(job.dataset_ids))
            rules = list(db.scalars(select(Rule).where(Rule.job_id == job.id, or_(*scope))))
        # A rule is valid only for the dataset it names and the actual input
        # columns selected for this job. Never interpret an absent field as
        # null values on every row.
        # One check may have been suggested more than once with different
        # explanatory metadata. Count and execute only distinct enabled,
        # approved rules; pending and disabled checks must never validate data.
        selected_columns = job.selected_columns or {}
        selected_for = lambda dataset: selected_columns.get(str(dataset.id), [] if selected_columns else list((dataset.schema_json or {}).get("columns", {})))
        active_rules = [r for r in rules if r.enabled and r.approved and
                        (r.column_name in selected_for(next((d for d in datasets if d.id == r.dataset_id), datasets[0])) if r.dataset_id is not None
                         else any(r.column_name in selected_for(dataset) for dataset in datasets))]
        validate_rule_bindings(active_rules, job.dataset_ids,
                               {dataset.id: rows for dataset, rows in zip(datasets, record_sets)}, records)
        signatures = {rule_signature(r) for r in active_rules}
        executed_count = len(signatures)
        pending_count = sum(not bool(r.approved) for r in rules)
        disabled_count = sum(not bool(r.enabled) for r in rules)
        skipped_count = sum(not (r.enabled and r.approved) for r in rules)
        if executed_count:
            progress("validating_records", 0, len(records))
            evaluation_started = perf_counter()
            if (job.settings or {}).get("combine_mode") == "union" and len(record_sets) > 1:
                passed, failed = validate_union_records(record_sets, datasets, active_rules)
            else:
                passed, failed = validate_records(records, active_rules)
            timings["rule_evaluation_seconds"] = perf_counter() - evaluation_started
        else:
            # No executed rules means no record has been evaluated. Do not
            # classify the untouched input rows as passing.
            passed, failed = [], []
        # Connector outputs can include dataframe rows and NumPy/Pandas
        # scalars. Normalize both result branches before any local/cloud write.
        passed = json_safe(passed)
        failed = json_safe(failed)
        if executed_count:
            progress("saving_pass_records", len(records), len(records))
        persistence_started = perf_counter()
        validation_complete = executed_count > 0
        base = f"outputs/job-{job.id}/execution-{execution.id}"
        dest = LocalStorage()
        passed_path = dest.save(base + "/passed.csv", encode_csv(passed))
        passed_records_path = dest.save(base + "/passed.json",
            json.dumps(json_safe(passed), ensure_ascii=False, indent=2, allow_nan=False).encode())
        failed_path = dest.save(base + "/failed.json", json.dumps(failed, ensure_ascii=False, indent=2, allow_nan=False).encode())
        timings["pass_fail_persistence_seconds"] = perf_counter() - persistence_started
        report = {"execution_id": execution.id, "job_id": job.id,
            "status": "succeeded" if validation_complete else "incomplete", "validation_complete": validation_complete,
            "total": len(records) if validation_complete else 0,
            "records_loaded": len(records), "records_evaluated": len(records) if validation_complete else 0,
            "passed": len(passed), "failed": len(failed),
            "rules": {"executed": executed_count, "pending": pending_count, "disabled": disabled_count, "skipped": skipped_count},
            "quality_checks": [{"id": rule.id, "dataset_id": rule.dataset_id,
                "column_name": rule.column_name, "rule_type": rule.rule_type,
                "parameters": rule.parameters or {}, "enabled": bool(rule.enabled),
                "approved": bool(rule.approved)} for rule in active_rules],
            "passed_path": passed_path, "failed_path": failed_path,
            "passed_records_path": passed_records_path,
            "timings_seconds": {**timings, "total_execution_seconds": perf_counter() - execution_started},
            "destination": destination.get("type", "local"),
            "storage_status": {"destination_platform": destination.get("type", "local"),
                "destination_location": destination.get("catalog") and f"{destination.get('catalog')}.{destination.get('schema', '')}" or None,
                "status": "not_started", "pass_storage_status": "not_started",
                "fail_storage_status": "not_started", "report_storage_status": "not_started",
                "error_message": None},
            "generated_at": datetime.now(timezone.utc).isoformat()}
        if validation_complete:
            # Checkpoint validation artifacts before contacting the destination.
            # A process crash during a remote write must not erase calculated
            # counts or the local report needed for a safe storage retry.
            report["validation_status"] = "succeeded"
            report["storage_status"]["status"] = "pending"
            report_path = dest.save(base + "/report.json", json.dumps(json_safe(report), indent=2, allow_nan=False).encode())
            execution.passed_count = len(passed)
            execution.failed_count = len(failed)
            execution.report_path = report_path
            progress("saving_pass_records", 0, len(passed))
            db.commit()
        if not validation_complete:
            report["message"] = "Validation incomplete: approve and enable at least one applicable rule, then run validation again. Records were not validated."
            report_path = dest.save(base + "/report.json", json.dumps(json_safe(report), indent=2, allow_nan=False).encode())
            execution.status = "incomplete"
            execution.passed_count = execution.failed_count = 0
            execution.report_path = report_path
            execution.logs = ["Execution started", f"Loaded {len(records)} records",
                f"Executed 0 approved and enabled rules; skipped {skipped_count} rules ({pending_count} pending, {disabled_count} disabled; categories can overlap)",
                report["message"]]
            execution.finished_at = datetime.now(timezone.utc)
            db.commit()
            return
        output_columns=list(dict.fromkeys(k for row in passed for k in row))
        output_schema={"columns":{c:{"inferred_type":"string","nullable":any(is_missing(r.get(c)) for r in passed)} for c in output_columns},"row_count":len(passed)}
        output_dataset=db.scalar(select(Dataset).where(Dataset.path==passed_path, Dataset.source_type=="derived"))
        if output_dataset is None:
            output_dataset=Dataset(name=f"{job.name} output {execution.id}",filename="passed.csv",path=passed_path,
                                   source_type="derived",schema_json=output_schema)
            db.add(output_dataset)
        else:
            output_dataset.name=f"{job.name} output {execution.id}"
            output_dataset.schema_json=output_schema
        db.flush()
        report["output_dataset_id"] = output_dataset.id
        report_path = dest.save(base + "/report.json", json.dumps(json_safe(report), indent=2, allow_nan=False).encode())
        execution.report_path = report_path
        db.commit()
        provider=destination.get("type","local")
        destination,secrets=resolve_destination_credentials(destination, db)
        dataset_label = "_and_".join(d.name for d in datasets)
        storage_run_id = execution.id
        retry_parent = db.get(Execution, execution.retry_of) if execution.retry_of else None
        while retry_parent:
            storage_run_id = retry_parent.id
            retry_parent = db.get(Execution, retry_parent.retry_of) if retry_parent.retry_of else None
        destination_started = None
        try:
            failed_indices = {item["row_index"] for item in failed}
            passed_row_indices = [index for index in range(len(records)) if index not in failed_indices]
            def destination_progress(info):
                phase = info.get("phase")
                stage = {"pass": "saving_pass_records", "fail": "saving_fail_records",
                         "report": "saving_report", "prepare": "saving_pass_records"}.get(phase)
                if stage:
                    progress(stage, info.get("completed", 0), info.get("total", 0))
            destination_started = perf_counter()
            remote=write_destination(destination,secrets,base,passed,failed,report,encode_csv(passed),
                dataset_name=dataset_label,job_id=job.id,execution_id=execution.id,
                columns=list(dict.fromkeys(k for row in records for k in row)),
                source_dataset_ids=[dataset.id for dataset in datasets], storage_run_id=storage_run_id,
                passed_row_indices=passed_row_indices, progress_callback=destination_progress,
                performance_metrics=databricks_metrics)
            timings["pass_fail_persistence_seconds"] += perf_counter() - destination_started
            destination_started = None
            report["destination_result"]=remote
            report["storage_status"]={"destination_platform": provider,
                "destination_location": remote.get("destination_location") or remote.get("catalog") and f"{remote.get('catalog')}.{remote.get('schema', '')}" or None,
                "status":"saved","pass_storage_status":remote.get("pass_status","saved"),
                "fail_storage_status":remote.get("fail_status","saved"),
                "report_storage_status":remote.get("report_status", "saved"), "error_message":None}
            report["storage_status"]["pass_status"] = report["storage_status"]["pass_storage_status"]
            report["storage_status"]["fail_status"] = report["storage_status"]["fail_storage_status"]
            if report["storage_status"]["report_storage_status"] != "saved":
                report["storage_status"]["status"] = "failed"
                report["storage_status"]["error_message"] = "Destination report was not confirmed written"
        except Exception as storage_error:
            if destination_started is not None:
                timings["pass_fail_persistence_seconds"] += perf_counter() - destination_started
            storage_message = _safe_error_message(storage_error)
            report["storage_status"]={"destination_platform":provider,
                "destination_location": destination.get("catalog") and f"{destination.get('catalog')}.{destination.get('schema', '')}" or None,
                "status":"failed","pass_storage_status":getattr(storage_error,"pass_status","failed"),
                "fail_storage_status":getattr(storage_error,"fail_status","failed"),
                "report_storage_status":getattr(storage_error,"report_status","not_started"),
                "error_message":storage_message}
            report["storage_status"]["pass_status"] = report["storage_status"]["pass_storage_status"]
            report["storage_status"]["fail_status"] = report["storage_status"]["fail_storage_status"]
            report["destination_result"]={"type":provider,"status":"failed",
                "pass_status":report["storage_status"]["pass_status"],"fail_status":report["storage_status"]["fail_status"],
                "error":storage_message}
            report["validation_status"] = "succeeded"
            report["status"] = "failed"
            logging.getLogger(__name__).error("Execution %s validation succeeded but destination storage failed: %s",
                                              execution_id, storage_error, exc_info=True)
        # Persist the final report locally after the real destination operation has completed.
        timings["pass_persistence_seconds"] += databricks_metrics.get("databricks_phase_seconds", {}).get("pass", 0.0)
        timings["fail_persistence_seconds"] += databricks_metrics.get("databricks_phase_seconds", {}).get("fail", 0.0)
        report["timings_seconds"] = {**timings,
            "total_execution_seconds": perf_counter() - execution_started}
        report["performance_metrics"] = databricks_metrics
        report_path = dest.save(base + "/report.json", json.dumps(json_safe(report), indent=2, allow_nan=False).encode())
        storage_succeeded = report["storage_status"]["status"] == "saved"
        report["validation_status"] = "succeeded"
        execution.status = "succeeded" if storage_succeeded else "failed"
        execution.stage = "completed" if storage_succeeded else "failed"
        if storage_succeeded:
            execution.progress_completed = execution.progress_total
        execution.passed_count = len(passed); execution.failed_count = len(failed)
        execution.error = report["storage_status"].get("error_message") if not storage_succeeded else None
        execution.report_path = report_path; execution.logs = ["Execution started", f"Loaded {len(records)} records",
            f"Executed {executed_count} approved and enabled rules; skipped {pending_count} pending rules; {disabled_count} disabled rules",
            f"Passed {len(passed)}; failed {len(failed)}", "Outputs saved locally",
            f"Destination storage {report['storage_status']['status']}"]
        execution.finished_at = datetime.now(timezone.utc); db.commit()
        total_seconds = perf_counter() - execution_started
        timing_message = "Execution timings: " + ", ".join(
            f"{name}={value:.3f}s" for name, value in {
                **timings, "total_execution_seconds": total_seconds}.items())
        execution.logs = (execution.logs or []) + [timing_message]
        logging.getLogger(__name__).info("Execution %s timings: %s", execution_id,
            ", ".join(f"{name}={value:.3f}s" for name, value in {
                **timings, "total_execution_seconds": total_seconds}.items()))
        db.commit()
    except Exception as exc:
        logging.getLogger(__name__).exception("Execution %s failed", execution_id)
        db.rollback()
        execution = db.get(Execution, execution_id)
        if execution:
            # Validation outputs are checkpointed before any remote destination
            # operation. A worker exception after that point must not erase the
            # already calculated counts or detailed local results.
            checkpoint = None
            if execution.report_path:
                try:
                    checkpoint = json.loads(LocalStorage().read(execution.report_path))
                except (OSError, ValueError):
                    checkpoint = None
            validation_saved = bool(checkpoint and checkpoint.get("validation_status") == "succeeded")
            if validation_saved:
                checkpoint["status"] = "failed"
                checkpoint["validation_status"] = "succeeded"
                checkpoint["storage_status"] = {
                    **(checkpoint.get("storage_status") or {}),
                    "status": "failed",
                    "error_message": f"Destination processing stopped ({type(exc).__name__}); validation results remain available.",
                }
                checkpoint["destination_result"] = {
                    **(checkpoint.get("destination_result") or {}),
                    "status": "failed",
                    "error": checkpoint["storage_status"]["error_message"],
                }
                execution.status = "failed"
                execution.stage = "failed"
                execution.error = checkpoint["storage_status"]["error_message"]
                try:
                    execution.report_path = LocalStorage().save(
                        str(Path(execution.report_path).relative_to(LocalStorage().root)),
                        json.dumps(json_safe(checkpoint), indent=2, allow_nan=False).encode())
                except (OSError, ValueError):
                    logging.getLogger(__name__).exception("Could not update checkpoint report for execution %s", execution_id)
            else:
                execution.status = "failed"
                execution.stage = "failed"
                execution.passed_count = 0
                execution.failed_count = 0
                execution.report_path = None
                if isinstance(exc, ValueError):
                    execution.error = f"Configuration error: {exc}"
                elif isinstance(exc, KeyError):
                    missing = exc.args[0] if exc.args else "unknown"
                    execution.error = f"Execution failed: required field or configuration key {missing!r} is missing. Check the job configuration and input schema."
                else:
                    execution.error = f"Execution failed ({type(exc).__name__}). Verify the job configuration and connected provider."
            execution.finished_at = datetime.now(timezone.utc)
            execution.logs = (execution.logs or []) + [f"Execution failed with {type(exc).__name__}; see backend logs for the diagnostic."]
            db.commit()
    finally:
        db.close()
