"""Destination writers. Each adapter reports success only after provider writes complete."""
import json
import re
from app.connectors.storage import get_storage


def _identifier(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", value or ""):
        raise ValueError(f"Unsafe SQL identifier: {value!r}")
    return value


def _table_rows(connection, table: str, records: list[dict], failed: bool, placeholder: str):
    cursor=connection.cursor()
    if failed:
        cols=["row_index", "record_json", "failure_reasons"]
        ddl=f"CREATE TABLE IF NOT EXISTS {table} (row_index BIGINT, record_json VARCHAR(16777216), failure_reasons VARCHAR(16777216))"
        values=[(r["row_index"],json.dumps(r["record"],ensure_ascii=False),json.dumps(r["failure_reasons"],ensure_ascii=False)) for r in records]
    else:
        cols=sorted({k for r in records for k in r})
        if not cols: cols=["_empty_result"]
        ddl=f"CREATE TABLE IF NOT EXISTS {table} ("+", ".join(f"{_identifier(c)} VARCHAR(16777216)" for c in cols)+")"
        values=[tuple(None if r.get(c) is None else str(r.get(c)) for c in cols) for r in records]
    cursor.execute(ddl)
    cursor.execute(f"DELETE FROM {table}")
    if values:
        marks=", ".join([placeholder]*len(cols))
        cursor.executemany(f"INSERT INTO {table} ({', '.join(cols)}) VALUES ({marks})",values)
    cursor.close()


def write_destination(destination: dict, secrets: dict, base: str, passed: list, failed: list, report: dict, passed_csv: bytes):
    provider=destination.get("type", "local")
    if provider == "local": return {"type":"local","status":"saved"}
    if provider in ("s3", "azure_blob", "onelake"):
        storage=get_storage(provider,destination,secrets)
        result={"type":provider,"passed":storage.save(f"{base}/passed.csv",passed_csv),
            "failed":storage.save(f"{base}/failed.json",json.dumps(failed,ensure_ascii=False,indent=2).encode())}
        report["destination_result"]={"type":provider,"status":"saved","outputs":result}
        result["report"]=storage.save(f"{base}/report.json",json.dumps(report,indent=2).encode())
        return result
    if provider == "snowflake":
        try: import snowflake.connector
        except ImportError as e: raise RuntimeError("Snowflake destination requires optional dependencies: pip install .[cloud]") from e
        cfg={**destination,**secrets}
        conn=snowflake.connector.connect(user=cfg["user"],password=cfg["password"],account=cfg["account"],warehouse=cfg["warehouse"],database=cfg["database"],schema=cfg.get("schema","PUBLIC"))
        try:
            schema=_identifier(cfg.get("schema","PUBLIC")); passed_table=_identifier(cfg.get("passed_table","DQ_PASSED")); failed_table=_identifier(cfg.get("failed_table","DQ_FAILED"))
            _table_rows(conn,f"{schema}.{passed_table}",passed,False,"%s")
            _table_rows(conn,f"{schema}.{failed_table}",failed,True,"%s")
            report["destination_result"]={"type":"snowflake","status":"saved","passed_table":passed_table,"failed_table":failed_table,"report_table":"DQ_REPORT"}
            cursor=conn.cursor(); cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.DQ_REPORT (report_json VARCHAR(16777216))"); cursor.execute(f"DELETE FROM {schema}.DQ_REPORT"); cursor.execute(f"INSERT INTO {schema}.DQ_REPORT VALUES (%s)",(json.dumps(report),)); cursor.close()
            conn.commit()
        except Exception: conn.rollback(); raise
        finally: conn.close()
        return {"type":"snowflake","passed_table":passed_table,"failed_table":failed_table,"report_table":"DQ_REPORT","status":"saved"}
    if provider == "databricks":
        try: from databricks import sql
        except ImportError as e: raise RuntimeError("Databricks destination requires optional dependencies: pip install .[cloud]") from e
        cfg={**destination,**secrets}
        conn=sql.connect(server_hostname=cfg["server_hostname"],http_path=cfg["http_path"],access_token=cfg["access_token"],catalog=cfg.get("catalog","hive_metastore"),schema=cfg.get("schema","default"))
        try:
            schema=_identifier(cfg.get("schema","default")); passed_table=_identifier(cfg.get("passed_table","dq_passed")); failed_table=_identifier(cfg.get("failed_table","dq_failed"))
            _table_rows(conn,f"{schema}.{passed_table}",passed,False,"?")
            _table_rows(conn,f"{schema}.{failed_table}",failed,True,"?")
            report["destination_result"]={"type":"databricks","status":"saved","passed_table":passed_table,"failed_table":failed_table,"report_table":"dq_report"}
            cursor=conn.cursor(); cursor.execute(f"CREATE TABLE IF NOT EXISTS {schema}.dq_report (report_json STRING)"); cursor.execute(f"DELETE FROM {schema}.dq_report"); cursor.execute(f"INSERT INTO {schema}.dq_report VALUES (?)",(json.dumps(report),)); cursor.close()
        finally: conn.close()
        return {"type":"databricks","passed_table":passed_table,"failed_table":failed_table,"report_table":"dq_report","status":"saved"}
    if provider == "fabric":
        config={**destination,"provider":"onelake"}
        storage=get_storage("onelake",config,secrets)
        result={"type":"fabric","passed":storage.save(f"{base}/passed.csv",passed_csv),
            "failed":storage.save(f"{base}/failed.json",json.dumps(failed,ensure_ascii=False,indent=2).encode())}
        report["destination_result"]={"type":"fabric","status":"saved","outputs":result}
        result["report"]=storage.save(f"{base}/report.json",json.dumps(report,indent=2).encode())
        return result
    raise ValueError(f"Unsupported destination platform: {provider}")
