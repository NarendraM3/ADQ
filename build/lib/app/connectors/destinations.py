"""Destination writers for validation output.

SQL destinations use execution-scoped tables so one run cannot overwrite another.
Object storage destinations retain the existing artifact based contract.
"""
import json
import re
import time
from datetime import date, datetime
from decimal import Decimal

from app.connectors.storage import get_storage


class DestinationWriteError(RuntimeError):
    def __init__(self, message, pass_status="failed", fail_status="failed"):
        super().__init__(message)
        self.pass_status = pass_status
        self.fail_status = fail_status


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _column_identifier(value: str) -> str:
    if not isinstance(value, str) or not value or "\x00" in value:
        raise ValueError("Column names must be non-empty strings without null bytes")
    return "`" + value.replace("`", "``") + "`"


def _slug(value: str) -> str:
    value = re.sub(r"[^A-Za-z0-9_]+", "_", str(value)).strip("_").lower()
    return value[:48] or "dataset"


def _table_name(dataset_name: str, job_id: int, execution_id: int, result: str) -> str:
    return _identifier(f"{_slug(dataset_name)}_job_{job_id}_execution_{execution_id}_{result}")


def _sql_type(values):
    present = [v for v in values if v is not None]
    if not present:
        return "STRING"
    if all(isinstance(v, bool) for v in present):
        return "BOOLEAN"
    if all(isinstance(v, int) and not isinstance(v, bool) for v in present):
        return "BIGINT"
    if all(isinstance(v, (int, float, Decimal)) and not isinstance(v, bool) for v in present):
        return "DOUBLE"
    if all(isinstance(v, datetime) for v in present):
        return "TIMESTAMP"
    if all(isinstance(v, date) for v in present):
        return "DATE"
    if all(isinstance(v, (dict, list)) for v in present):
        return "STRING"
    return "STRING"


def _write_databricks_table(conn, table, records, columns, extra_columns=()):
    cursor = conn.cursor()
    try:
        definitions = []
        for col in columns:
            definitions.append(f"{_column_identifier(col)} {_sql_type([(r.get('record', r)).get(col) for r in records])}")
        for col, typ in extra_columns:
            definitions.append(f"`{_identifier(col)}` {typ}")
        if not definitions:
            definitions = ["`_empty_result` STRING"]
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(definitions)}) USING DELTA")
        # Tables are unique to a run; clearing the same run makes retries idempotent.
        cursor.execute(f"DELETE FROM {table}")
        if records:
            insert_cols = list(columns) + [c for c, _ in extra_columns]
            marks = ", ".join("?" for _ in insert_cols)
            names = ", ".join(_column_identifier(c) for c in insert_cols)
            values = []
            for record in records:
                source = record.get("record", record)
                values.append(tuple(
                    json.dumps(source.get(c), ensure_ascii=False, default=str)
                    if isinstance(source.get(c), (dict, list)) else source.get(c)
                    for c in columns
                ) + tuple(record.get("_dq_metadata", {}).get(c,
                           record.get("_dq_row_index", record.get("row_index")) if c == "_dq_row_index" else
                           json.dumps(record.get("failure_reasons"), ensure_ascii=False) if c == "_dq_failure_reasons" and record.get("failure_reasons") is not None else
                           record.get("_dq_failure_reasons") if c == "_dq_failure_reasons" else record.get(c))
                           for c, _ in extra_columns))
            cursor.executemany(f"INSERT INTO {table} ({names}) VALUES ({marks})", values)
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count_row = cursor.fetchone()
        if not count_row or count_row[0] != len(records):
            raise RuntimeError("Databricks row count did not match the submitted records")
    finally:
        cursor.close()


def _write_databricks(destination, secrets, passed, failed, dataset_name, job_id, execution_id, source_columns=None,
                      source_dataset_ids=None, storage_run_id=None, passed_row_indices=None):
    try:
        from databricks import sql
    except ImportError as e:
        raise RuntimeError("Databricks destination requires optional dependencies: pip install .[cloud]") from e
    cfg = {**destination, **secrets}
    catalog = _identifier(cfg.get("catalog", "dq_catalog"))
    schema = _identifier(cfg.get("schema", "dq_schema"))
    safe_dataset = _slug(dataset_name)
    table_run_id = storage_run_id or execution_id
    passed_table = _table_name(safe_dataset, job_id, table_run_id, "pass")
    failed_table = _table_name(safe_dataset, job_id, table_run_id, "fail")
    qualified = lambda table: f"`{catalog}`.`{schema}`.`{table}`"
    columns = list(source_columns or dict.fromkeys(k for row in passed for k in row))
    failed_columns = columns
    server_hostname = cfg.get("server_hostname")
    http_path = cfg.get("http_path")
    access_token = cfg.get("access_token") or cfg.get("token")
    if not server_hostname or not http_path or not access_token:
        raise ValueError("Databricks requires server_hostname, http_path, and access_token")
    server_hostname = server_hostname.removeprefix("https://").removeprefix("http://").rstrip("/")
    conn = sql.connect(server_hostname=server_hostname, http_path=http_path,
                       access_token=access_token, catalog=catalog, schema=schema)
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
        finally:
            cur.close()
        metadata_values = {"_dq_job_id": job_id, "_dq_execution_id": execution_id,
                           "_dq_source_dataset_ids": json.dumps(source_dataset_ids or []),
                           "_dq_validation_status": None, "_dq_row_index": None,
                           "_dq_failure_reasons": None}
        metadata_types = {"_dq_job_id": "BIGINT", "_dq_execution_id": "BIGINT",
                          "_dq_source_dataset_ids": "STRING", "_dq_validation_status": "STRING",
                          "_dq_row_index": "BIGINT", "_dq_failure_reasons": "STRING"}
        reserved = set(columns)
        metadata_names = {}
        for name in metadata_values:
            unique_name = name
            while unique_name in reserved:
                unique_name = "_" + unique_name
            reserved.add(unique_name)
            metadata_names[name] = unique_name
        metadata = tuple((metadata_names[name], typ) for name, typ in metadata_types.items())
        pass_indices = passed_row_indices or list(range(len(passed)))
        pass_rows = [{"record": row, "_dq_metadata": {
            metadata_names["_dq_job_id"]: job_id,
            metadata_names["_dq_execution_id"]: execution_id,
            metadata_names["_dq_source_dataset_ids"]: metadata_values["_dq_source_dataset_ids"],
            metadata_names["_dq_validation_status"]: "PASS",
            metadata_names["_dq_row_index"]: pass_indices[i],
            metadata_names["_dq_failure_reasons"]: None,
        }} for i, row in enumerate(passed)]
        fail_rows = [{"record": item["record"], "row_index": item["row_index"],
            "failure_reasons": item["failure_reasons"], "_dq_metadata": {
                metadata_names["_dq_job_id"]: job_id,
                metadata_names["_dq_execution_id"]: execution_id,
                metadata_names["_dq_source_dataset_ids"]: metadata_values["_dq_source_dataset_ids"],
                metadata_names["_dq_validation_status"]: "FAIL",
                metadata_names["_dq_row_index"]: item["row_index"],
                metadata_names["_dq_failure_reasons"]: json.dumps(item["failure_reasons"], ensure_ascii=False),
            }} for item in failed]
        try:
            _write_databricks_table(conn, qualified(passed_table), pass_rows, columns, metadata)
        except Exception as exc:
            raise DestinationWriteError(type(exc).__name__) from exc
        try:
            _write_databricks_table(conn, qualified(failed_table), fail_rows, failed_columns, metadata)
        except Exception as exc:
            raise DestinationWriteError(type(exc).__name__, pass_status="saved") from exc
        return {"type": "databricks", "status": "saved", "pass_status": "saved", "fail_status": "saved",
                "catalog": catalog, "schema": schema, "passed_table": passed_table, "failed_table": failed_table}
    finally:
        conn.close()


def _write_snowflake(destination, secrets, passed, failed, dataset_name, job_id, execution_id, source_columns=None):
    try:
        import snowflake.connector
    except ImportError as e:
        raise RuntimeError("Snowflake destination requires optional dependencies: pip install .[cloud]") from e
    cfg = {**destination, **secrets}
    database = _identifier(cfg["database"])
    schema = _identifier(cfg.get("schema", "PUBLIC"))
    passed_table = _table_name(dataset_name, job_id, execution_id, "pass")
    failed_table = _table_name(dataset_name, job_id, execution_id, "fail")
    qualified = lambda table: f'"{database}"."{schema}"."{table}"'
    conn = snowflake.connector.connect(user=cfg["user"], password=cfg["password"], account=cfg["account"],
        warehouse=cfg["warehouse"], database=database, schema=schema)
    try:
        columns = list(source_columns or dict.fromkeys(k for row in passed for k in row))
        failed_columns = list(dict.fromkeys(k for item in failed for k in item["record"])) or columns
        # Reuse the row/type mapping while creating the provider's own table syntax.
        for table, records, cols, extras, is_failed in (
            (passed_table, passed, columns, (), False),
            (failed_table, failed, failed_columns, (("_dq_row_index", "NUMBER(38,0)"),
             ("_dq_failure_reasons", "VARCHAR")), True),
        ):
            cur = conn.cursor()
            try:
                defs = [f'"{c.replace(chr(34), chr(34)*2)}" {_snowflake_type([r.get(c) for r in records])}' for c in cols]
                defs += [f'"{c}" {typ}' for c, typ in extras]
                cur.execute(f"CREATE TABLE IF NOT EXISTS {qualified(table)} ({', '.join(defs) or '"_empty_result" VARCHAR'})")
                cur.execute(f"TRUNCATE TABLE {qualified(table)}")
                if records:
                    names = cols + [c for c, _ in extras]
                    placeholders = ", ".join("%s" for _ in names)
                    quoted = ", ".join(f'"{c.replace(chr(34), chr(34)*2)}"' for c in names)
                    values = []
                    for row in records:
                        source = row.get("record", row)
                        values.append(tuple(json.dumps(source.get(c), default=str) if isinstance(source.get(c), (dict, list)) else source.get(c) for c in cols) +
                tuple(row.get("row_index") if c == "_dq_row_index" else json.dumps(row.get("failure_reasons")) for c, _ in extras))
                    cur.executemany(f"INSERT INTO {qualified(table)} ({quoted}) VALUES ({placeholders})", values)
            finally:
                cur.close()
        conn.commit()
        return {"type":"snowflake","status":"saved","pass_status":"saved","fail_status":"saved",
                "passed_table":passed_table,"failed_table":failed_table,"schema":schema,"database":database}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _snowflake_type(values):
    value_type = _sql_type(values)
    return {"STRING":"VARCHAR","BIGINT":"NUMBER(38,0)","DOUBLE":"DOUBLE","BOOLEAN":"BOOLEAN",
            "DATE":"DATE","TIMESTAMP":"TIMESTAMP_NTZ"}.get(value_type,"VARCHAR")


def _write_with_retry(operation, attempts=3):
    last = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as exc:
            last = exc
            if attempt + 1 < attempts:
                time.sleep(0.1 * (2 ** attempt))
    raise last


def _write_object_storage(provider, destination, secrets, base, passed, failed, report, passed_csv):
    storage = get_storage(provider, destination, secrets)
    passed_key, failed_key = f"{base}/passed.csv", f"{base}/failed.json"
    try:
        passed_path = _write_with_retry(lambda: storage.save(passed_key, passed_csv))
    except Exception as exc:
        raise DestinationWriteError(type(exc).__name__) from exc
    try:
        failed_path = _write_with_retry(lambda: storage.save(
            failed_key, json.dumps(failed, ensure_ascii=False, indent=2).encode()))
    except Exception as exc:
        raise DestinationWriteError(type(exc).__name__, pass_status="saved") from exc
    result = {"type": provider, "status": "saved", "pass_status": "saved", "fail_status": "saved",
              "passed": passed_path, "failed": failed_path}
    report["destination_result"] = {"type": provider, "status": "saved", "outputs": result}
    try:
        result["report"] = storage.save(f"{base}/report.json", json.dumps(report, indent=2).encode())
        result["report_status"] = "saved"
    except Exception:
        result["report_status"] = "failed"
    return result


def write_destination(destination: dict, secrets: dict, base: str, passed: list, failed: list,
                      report: dict, passed_csv: bytes, *, dataset_name="dataset", job_id=0, execution_id=0,
                      columns=None, source_dataset_ids=None, storage_run_id=None, passed_row_indices=None):
    provider = destination.get("type", "local")
    if provider == "local":
        return {"type": "local", "status": "saved", "pass_status": "saved", "fail_status": "saved"}
    if provider == "databricks":
        return _write_with_retry(lambda: _write_databricks(destination, secrets, passed, failed,
                                                           dataset_name, job_id, execution_id, columns,
                                                           source_dataset_ids, storage_run_id,
                                                           passed_row_indices))
    if provider in ("s3", "azure_blob", "onelake"):
        return _write_object_storage(provider, destination, secrets, base, passed, failed, report, passed_csv)
    if provider == "fabric":
        return _write_object_storage("onelake", {**destination, "provider": "onelake"}, secrets,
                                     base, passed, failed, report, passed_csv)
    if provider == "snowflake":
        return _write_with_retry(lambda: _write_snowflake(destination, secrets, passed, failed,
            dataset_name, job_id, execution_id, columns))
    raise ValueError(f"Unsupported destination platform: {provider}")
