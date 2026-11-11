import hashlib, json, logging, mimetypes, re, threading, time, uuid
import unicodedata
import asyncio
import traceback
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from fastapi import Depends, FastAPI, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from io import BytesIO, StringIO
import csv
from cryptography.fernet import Fernet, InvalidToken
from openai import APIConnectionError, APIError, APIStatusError, APITimeoutError, AsyncOpenAI
from sqlalchemy import delete, or_, select
from sqlalchemy.orm import Session
from app.config import settings
from app.database import Base, engine, get_db, SessionLocal
from app.models import Correction, Credential, Dataset, Execution, Job, Rule, WorkflowSelection
from app.processing import destination_for_job, encode_csv, execute, parse_data
from app.platforms import resolve_destination_credentials
from app.serialization import json_safe
from app.rules import RULE_TYPES, is_missing, rule_signature, validate_rule_definition
from app.connectors.destinations import read_databricks_table, write_destination
from app.schemas import CorrectionCreate, CredentialCreate, JobConfiguration, JobCreate, JobUpdate, RuleCreate, RuleUpdate, normalize_platform
from app.connectors.storage import LocalStorage, get_storage, get_source_adapter

logging.basicConfig(level=settings.log_level)
logger = logging.getLogger(__name__)

_SOURCE_CONNECT_EXECUTOR = ThreadPoolExecutor(max_workers=max(1, int(settings.source_discovery_workers)))
_SOURCE_CONNECT_LOCK = threading.Lock()
_SOURCE_CONNECT_TASKS: dict[str, dict] = {}
_SOURCE_CONNECT_BY_FINGERPRINT: dict[str, str] = {}
CORRECTION_SUGGESTION_CACHE_VERSION = 4


def _safe_execution_error(exc: Exception):
    message = f"{type(exc).__name__}: {exc}"
    message = re.sub(r"(?i)(api[_-]?key|token|password|secret)(\s*[=:]\s*)[^\s,;]+",
                     r"\1\2[REDACTED]", message)
    return message[:2000]


def _mark_validation_dispatch_failed(execution_id: int, exc: Exception):
    """Persist a terminal failure if a worker crashes outside execute()."""
    try:
        with SessionLocal() as db:
            execution = db.get(Execution, execution_id)
            if execution is None or execution.status in {"succeeded", "failed", "incomplete"}:
                return
            error = _safe_execution_error(exc)
            finished_at = datetime.now(timezone.utc)
            execution.status = "failed"
            execution.stage = "failed"
            execution.error = error
            execution.finished_at = finished_at
            execution.updated_at = finished_at
            logs = list(execution.logs or [])
            logs.append(f"Worker dispatch failed: {error}")
            execution.logs = logs[-50:]
            db.commit()
    except Exception:
        logging.getLogger(__name__).exception(
            "Unable to persist validation worker failure for execution %s", execution_id)


async def _validation_worker(app: FastAPI):
    logger = logging.getLogger(__name__)
    while True:
        execution_id = await app.state.validation_queue.get()
        try:
            logger.info("Validation worker picked execution %s", execution_id)
            await asyncio.to_thread(execute, execution_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.exception("Validation worker crashed while dispatching execution %s", execution_id)
            await asyncio.to_thread(_mark_validation_dispatch_failed, execution_id, exc)
        finally:
            async with app.state.validation_queue_lock:
                app.state.validation_enqueued.discard(execution_id)
            app.state.validation_queue.task_done()


async def _enqueue_validation(app: FastAPI, execution_id: int, *, allow_inline: bool = True):
    """Queue an execution for the process worker without duplicating work."""
    workers = getattr(app.state, "validation_workers", [])
    has_live_worker = any(not worker.done() for worker in workers)
    if not hasattr(app.state, "validation_queue") or not has_live_worker:
        if not allow_inline:
            logging.getLogger(__name__).warning(
                "Validation worker is not active; execution %s remains queued", execution_id)
            return
        logging.getLogger(__name__).warning(
            "Validation worker is not active; executing validation %s synchronously", execution_id)
        await asyncio.to_thread(execute, execution_id)
        return
    async with app.state.validation_queue_lock:
        if execution_id in app.state.validation_enqueued:
            return
        app.state.validation_enqueued.add(execution_id)
        await app.state.validation_queue.put(execution_id)


def _databricks_error_details(exc, secrets):
    """Return safe diagnostics without ever including submitted secret values."""
    message = str(exc)
    for value in secrets.values() if isinstance(secrets, dict) else ():
        if isinstance(value, str) and value:
            message = message.replace(value, "[REDACTED]")
    message = re.sub(r"(?i)(token|password|secret)(\s*[=:]\s*)[^\s,;]+", r"\1\2[REDACTED]", message)
    return message[:1000]


def _log_databricks_failure(operation, exc, secrets):
    logger = logging.getLogger(__name__)
    root = exc
    while root.__cause__ is not None:
        root = root.__cause__
    error_code = getattr(root, "error_code", None) or getattr(root, "code", None)
    sqlstate = getattr(root, "sql_state", None) or getattr(root, "sqlstate", None)
    safe_message = _databricks_error_details(root, secrets)
    trace = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
    for value in secrets.values() if isinstance(secrets, dict) else ():
        if isinstance(value, str) and value:
            trace = trace.replace(value, "[REDACTED]")
    trace = re.sub(r"(?i)(token|password|secret)(\s*[=:]\s*)[^\s,;]+", r"\1\2[REDACTED]", trace)
    logger.error("Databricks source failed operation=%s exception=%s error_code=%s sqlstate=%s message=%s\n%s",
                 operation, type(exc).__name__, error_code, sqlstate, safe_message, trace)


def _safe_source_error_message(exc: Exception, secrets: dict | None = None) -> str:
    message = f"{type(exc).__name__}: {exc}"
    for value in secrets.values() if isinstance(secrets, dict) else ():
        if isinstance(value, str) and value:
            message = message.replace(value, "[REDACTED]")
    message = re.sub(r"(?i)(access[_-]?key|api[_-]?key|token|password|secret|authorization)(\s*[=:]\s*)[^\s,;]+",
                     r"\1\2[REDACTED]", message)
    return message[:1500]


def _log_source_failure(provider: str, operation: str, exc: Exception, secrets: dict | None = None):
    logging.getLogger(__name__).error(
        "Source connection failed provider=%s operation=%s exception=%s message=%s",
        provider, operation, type(exc).__name__, _safe_source_error_message(exc, secrets),
    )


def _s3_http_error(exc: Exception):
    response = getattr(exc, "response", {})
    code = str(response.get("Error", {}).get("Code", "")) if isinstance(response, dict) else ""
    class_name = type(exc).__name__
    safe = str(exc).casefold()
    if code in ("InvalidAccessKeyId", "SignatureDoesNotMatch", "InvalidToken", "ExpiredToken", "UnrecognizedClientException"):
        return 401, {"code": "S3_AUTHENTICATION_FAILED", "message": "AWS rejected the supplied credentials. Check the access key ID, secret key, and session token."}
    if code in ("AccessDenied", "403", "Forbidden"):
        return 403, {"code": "S3_PERMISSION_DENIED", "message": "AWS credentials do not have permission to inspect this bucket. Grant s3:ListBucket and bucket access."}
    if code in ("NoSuchBucket", "404", "NotFound"):
        return 404, {"code": "S3_BUCKET_NOT_FOUND", "message": "The S3 bucket was not found. Check the bucket name and AWS region."}
    if code in ("PermanentRedirect", "301", "AuthorizationHeaderMalformed"):
        return 400, {"code": "S3_REGION_MISMATCH", "message": "The bucket is in a different AWS region. Select its correct region."}
    if class_name in ("ConnectTimeoutError", "ReadTimeoutError") or "timed out" in safe or "timeout" in safe:
        return 504, {"code": "S3_TIMEOUT", "message": "Timed out while contacting Amazon S3. Check network, proxy, endpoint, bucket region, and AWS service reachability."}
    if class_name in ("EndpointConnectionError", "ProxyConnectionError", "ConnectionClosedError") or any(token in safe for token in ("could not connect", "endpoint", "proxy", "name resolution", "getaddrinfo", "temporary failure in name resolution", "nodename nor servname")):
        return 503, {"code": "S3_UNREACHABLE", "message": "Could not reach the Amazon S3 endpoint. Check DNS, proxy, firewall, region, and network connectivity."}
    if isinstance(exc, RuntimeError) and "boto3" in str(exc).lower():
        return 503, {"code": "S3_SDK_UNAVAILABLE", "message": "S3 support is unavailable. Install the cloud dependencies (pip install .[cloud]) and restart the backend."}
    return 400, {"code": "S3_CONNECTION_FAILED", "message": "Could not connect to S3; verify the bucket, region, AWS credentials, and network access."}


def _generic_source_http_error(provider: str, exc: Exception):
    label = {
        "azure_blob": "Azure Blob Storage",
        "onelake": "Microsoft Fabric OneLake",
        "snowflake": "Snowflake",
    }.get(provider, provider)
    prefix = {
        "azure_blob": "AZURE_BLOB",
        "onelake": "ONELAKE",
        "snowflake": "SNOWFLAKE",
    }.get(provider, "SOURCE")
    class_name = type(exc).__name__.casefold()
    safe = str(exc).casefold()
    if "timeout" in safe or "timed out" in safe or "timeout" in class_name:
        return 504, {"code": f"{prefix}_TIMEOUT", "message": f"Timed out while contacting {label}. Check network, proxy, endpoint, and service reachability."}
    if any(token in safe for token in ("dns", "name resolution", "getaddrinfo", "could not connect", "connection refused", "no route to host", "network is unreachable", "proxy")):
        return 503, {"code": f"{prefix}_UNREACHABLE", "message": f"Could not reach {label}. Check DNS, proxy, firewall, endpoint, and network connectivity."}
    if any(token in safe for token in ("authentication", "unauthorized", "invalid token", "invalid signature", "invalid access", "login failed")):
        return 401, {"code": f"{prefix}_AUTHENTICATION_FAILED", "message": f"{label} rejected the supplied credentials. Check the submitted credentials and token/account settings."}
    if any(token in safe for token in ("permission", "forbidden", "access denied", "insufficient privileges", "authorization")):
        return 403, {"code": f"{prefix}_PERMISSION_DENIED", "message": f"The supplied credentials do not have permission to discover this {label} source."}
    if "not found" in safe or "does not exist" in safe or "404" in safe:
        return 404, {"code": f"{prefix}_NOT_FOUND", "message": f"The requested {label} resource was not found or is not visible to this user."}
    if "requires optional dependencies" in safe:
        return 503, {"code": f"{prefix}_SDK_UNAVAILABLE", "message": f"{label} support is unavailable. Install the cloud dependencies and restart the backend."}
    return 400, {"code": f"{prefix}_CONNECTION_FAILED", "message": f"Could not connect to {label}; verify the source settings, credentials, permissions, and network access."}


def _source_task_public(task: dict) -> dict:
    result = {"operation_id": task["operation_id"], "status": task["status"],
              "provider": task.get("provider"), "created_at": task.get("created_at"),
              "updated_at": task.get("updated_at")}
    if task.get("result") is not None:
        result["result"] = task["result"]
    if task.get("error") is not None:
        result["error"] = task["error"]
        result["status_code"] = task.get("status_code", 400)
    return result


def _source_fingerprint(body: CredentialCreate, prefix: str) -> str:
    payload = {"name": body.name, "provider": body.provider, "config": body.config, "secrets": body.secrets, "prefix": prefix}
    return hashlib.sha256(json.dumps(payload, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _set_source_task(operation_id: str, **patch):
    with _SOURCE_CONNECT_LOCK:
        task = _SOURCE_CONNECT_TASKS.get(operation_id)
        if task:
            task.update(patch)
            task["updated_at"] = datetime.now(timezone.utc).isoformat()


def _run_source_connect_task(operation_id: str, body_data: dict, prefix: str):
    _set_source_task(operation_id, status="running")
    try:
        body = CredentialCreate.model_validate(body_data)
        with SessionLocal() as db:
            result = _connect_source_now(body, prefix, db)
        _set_source_task(operation_id, status="succeeded", result=json_safe(result), error=None, status_code=201)
    except HTTPException as exc:
        detail = exc.detail if isinstance(exc.detail, dict) else {"code": "SOURCE_CONNECTION_FAILED", "message": str(exc.detail)}
        _set_source_task(operation_id, status="failed", error=json_safe(detail), status_code=exc.status_code)
    except Exception as exc:
        logging.getLogger(__name__).exception("Unhandled source discovery task failure operation_id=%s", operation_id)
        _set_source_task(operation_id, status="failed",
                         error={"code": "SOURCE_CONNECTION_FAILED", "message": _safe_source_error_message(exc)},
                         status_code=500)


def _start_source_connect_task(body: CredentialCreate, prefix: str) -> dict:
    fingerprint = _source_fingerprint(body, prefix)
    now = datetime.now(timezone.utc).isoformat()
    with _SOURCE_CONNECT_LOCK:
        existing_id = _SOURCE_CONNECT_BY_FINGERPRINT.get(fingerprint)
        if existing_id and existing_id in _SOURCE_CONNECT_TASKS:
            existing = _SOURCE_CONNECT_TASKS[existing_id]
            if existing.get("status") != "failed":
                return _source_task_public(existing)
            _SOURCE_CONNECT_TASKS.pop(existing_id, None)
        operation_id = uuid.uuid4().hex
        _SOURCE_CONNECT_BY_FINGERPRINT[fingerprint] = operation_id
        _SOURCE_CONNECT_TASKS[operation_id] = {
            "operation_id": operation_id,
            "fingerprint": fingerprint,
            "provider": body.provider,
            "status": "queued",
            "created_at": now,
            "updated_at": now,
            "result": None,
            "error": None,
            "status_code": None,
        }
    _SOURCE_CONNECT_EXECUTOR.submit(_run_source_connect_task, operation_id, body.model_dump(), prefix)
    with _SOURCE_CONNECT_LOCK:
        return _source_task_public(_SOURCE_CONNECT_TASKS[operation_id])


@asynccontextmanager
async def lifespan(app):
    app.state.validation_queue = asyncio.Queue()
    app.state.validation_enqueued = set()
    app.state.validation_queue_lock = asyncio.Lock()
    app.state.validation_workers = [asyncio.create_task(_validation_worker(app))]
    Base.metadata.create_all(bind=engine)
    # create_all does not add columns to an existing SQLite database.
    if engine.dialect.name == "sqlite":
        from sqlalchemy import inspect
        if "settings" not in {column["name"] for column in inspect(engine).get_columns("jobs")}:
            with engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE jobs ADD COLUMN settings JSON NOT NULL DEFAULT '{}'")
        if "environment" not in {column["name"] for column in inspect(engine).get_columns("jobs")}:
            with engine.begin() as connection:
                # Existing jobs have no reliable record of the platform selected at creation.
                connection.exec_driver_sql("ALTER TABLE jobs ADD COLUMN environment VARCHAR(100)")
        if "selected_columns" not in {column["name"] for column in inspect(engine).get_columns("workflow_selections")}:
            with engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE workflow_selections ADD COLUMN selected_columns JSON NOT NULL DEFAULT '{}'")
        if "rule_snapshot" not in {column["name"] for column in inspect(engine).get_columns("executions")}:
            with engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE executions ADD COLUMN rule_snapshot JSON")
        execution_columns = {column["name"] for column in inspect(engine).get_columns("executions")}
        with engine.begin() as connection:
            if "stage" not in execution_columns:
                connection.exec_driver_sql("ALTER TABLE executions ADD COLUMN stage VARCHAR(40) NOT NULL DEFAULT 'queued'")
            if "progress_completed" not in execution_columns:
                connection.exec_driver_sql("ALTER TABLE executions ADD COLUMN progress_completed INTEGER NOT NULL DEFAULT 0")
            if "progress_total" not in execution_columns:
                connection.exec_driver_sql("ALTER TABLE executions ADD COLUMN progress_total INTEGER NOT NULL DEFAULT 0")
            if "updated_at" not in execution_columns:
                connection.exec_driver_sql("ALTER TABLE executions ADD COLUMN updated_at DATETIME")
        correction_columns = {column["name"] for column in inspect(engine).get_columns("corrections")}
        with engine.begin() as connection:
            if "record_id" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN record_id VARCHAR(255)")
            if "source" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN source VARCHAR(30) NOT NULL DEFAULT 'Manual'")
            if "suggested_value" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN suggested_value TEXT")
            if "confidence" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN confidence VARCHAR(30)")
            if "verification_status" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN verification_status VARCHAR(120)")
            if "approved" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN approved BOOLEAN NOT NULL DEFAULT 1")
            if "status" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN status VARCHAR(40) NOT NULL DEFAULT 'applied'")
            if "revalidation_execution_id" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN revalidation_execution_id INTEGER")
            if "revalidation_status" not in correction_columns:
                connection.exec_driver_sql("ALTER TABLE corrections ADD COLUMN revalidation_status VARCHAR(40)")
    # Resume persisted queued executions after a restart; execute() atomically
    # claims each row before doing work, so duplicate dispatches are harmless.
    with SessionLocal() as db:
        queued_ids = list(db.scalars(select(Execution.id).where(Execution.status == "queued")))
    for execution_id in queued_ids:
        await _enqueue_validation(app, execution_id)
    try:
        yield
    finally:
        for worker in app.state.validation_workers:
            worker.cancel()
        await asyncio.gather(*app.state.validation_workers, return_exceptions=True)


app = FastAPI(title="ADQ Automation API", version="0.1.0", description="AI-assisted data quality jobs. Local CSV/JSON processing is ready; cloud providers require credentials and optional SDKs.", lifespan=lifespan)


def not_found(obj, name):
    if obj is None: raise HTTPException(404, f"{name} not found")
    return obj


def job_environment(job: Job) -> str | None:
    """Return this job's saved platform, with explicit legacy metadata as fallback."""
    settings = job.settings if isinstance(job.settings, dict) else {}
    destination = job.destination if isinstance(job.destination, dict) else {}
    return normalize_platform(
        job.environment
        or settings.get("environment")
        or settings.get("platform")
        or destination.get("platform")
    )


def infer_job_environment(environment: str | None, settings: dict | None = None) -> str | None:
    settings = settings if isinstance(settings, dict) else {}
    # The selected platform is independent of the job's source and output destination.
    return normalize_platform(environment or settings.get("environment") or settings.get("platform"))


def job_response(job: Job) -> dict:
    return {
        "id": job.id, "name": job.name, "description": job.description,
        "dataset_ids": job.dataset_ids, "selected_columns": job.selected_columns,
        "join_config": job.join_config, "destination": job.destination,
        "environment": job_environment(job), "settings": job.settings,
        "created_at": job.created_at, "updated_at": job.updated_at,
    }


def credential_key():
    key = settings.secret_encryption_key
    if not key:
        raise HTTPException(503, "Credential storage is unavailable: SECRET_ENCRYPTION_KEY is missing or empty. Set it in the project .env or backend environment.")
    try:
        return Fernet(key.strip().encode("ascii"))
    except (UnicodeEncodeError, TypeError, ValueError) as e:
        raise HTTPException(503, "Credential storage is unavailable: SECRET_ENCRYPTION_KEY is not a valid Fernet key. Check its format and whether the backend environment overrides the project .env.") from e


def sensitive_config_keys(value, prefix=""):
    found=[]
    if isinstance(value,dict):
        for key,item in value.items():
            name=f"{prefix}.{key}" if prefix else str(key)
            if any(word in str(key).lower() for word in ("secret","password","token","connection_string","private_key")):
                found.append(name)
            else: found.extend(sensitive_config_keys(item,name))
    elif isinstance(value,list):
        for index,item in enumerate(value): found.extend(sensitive_config_keys(item,f"{prefix}[{index}]"))
    return found


@app.get("/health")
def health(): return {"status": "ok", "database": "configured", "local_workflow": "ready", "ai": "openai" if settings.resolved_ai_api_key else "disabled"}


@app.get("/api/v1/capabilities")
def capabilities():
    return {"sources": {"local_csv_json": "ready", "s3": "optional boto3 adapter", "azure_blob": "optional Azure SDK adapter", "onelake": "optional Fabric OAuth adapter", "databricks": "optional SQL connector with table discovery", "snowflake": "optional SQL connector with table discovery", "databases": "SQL Server, MySQL, and PostgreSQL"},
        "destinations": {"local": "ready", "s3": "optional boto3 adapter", "azure_blob": "optional Azure SDK adapter", "onelake": "optional Fabric OAuth adapter", "databricks": "optional SQL connector", "snowflake": "optional SQL connector", "fabric": "optional OneLake adapter"},
        "ai_suggestions": "configured" if settings.resolved_ai_api_key and settings.resolved_ai_model else "disabled; set OPENAI_API_KEY and OPENAI_MODEL"}


@app.get("/api/v1/datasets")
def list_datasets(db: Session = Depends(get_db)):
    return db.scalars(select(Dataset).order_by(Dataset.id.desc())).all()


@app.get("/api/v1/workflows/{workflow_id}/selection")
def get_workflow_selection(workflow_id: str, db: Session = Depends(get_db)):
    selection = db.get(WorkflowSelection, workflow_id)
    if selection is None:
        return {"workflow_id": workflow_id, "dataset_ids": [], "selected_columns": {}, "source_connection_ids": {}, "datasets": []}
    ids = selection.dataset_ids or []
    rows = {d.id: d for d in db.scalars(select(Dataset).where(Dataset.id.in_(ids))).all()} if ids else {}
    valid = [did for did in ids if did in rows]
    selected_columns = selection.selected_columns if isinstance(selection.selected_columns, dict) else {}
    return {"workflow_id": workflow_id, "dataset_ids": valid,
            "selected_columns": {str(did): selected_columns.get(str(did), list((rows[did].schema_json or {}).get("columns", {}))) for did in valid},
            "source_connection_ids": selection.source_connection_ids or {},
            "datasets": [rows[did] for did in valid]}


@app.put("/api/v1/workflows/{workflow_id}/selection")
def set_workflow_selection(workflow_id: str, body: dict, db: Session = Depends(get_db)):
    """Persist exactly the explicitly selected dataset IDs for this workflow."""
    ids = body.get("dataset_ids")
    connections = body.get("source_connection_ids", {})
    requested_columns = body.get("selected_columns", {})
    if not isinstance(workflow_id, str) or not workflow_id.strip() or len(workflow_id) > 36:
        raise HTTPException(422, "A valid workflow ID is required")
    if not isinstance(ids, list) or any(not isinstance(i, int) or i <= 0 for i in ids):
        raise HTTPException(422, "dataset_ids must contain valid dataset IDs")
    if len(set(ids)) != len(ids):
        raise HTTPException(422, "dataset_ids must be unique")
    if not isinstance(requested_columns, dict):
        raise HTTPException(422, "selected_columns must be an object keyed by dataset ID")
    rows = {d.id: d for d in db.scalars(select(Dataset).where(Dataset.id.in_(ids))).all()}
    if len(rows) != len(ids):
        raise HTTPException(404, "A selected dataset no longer exists")
    normalized_columns = {}
    for dataset_id in ids:
        columns = requested_columns.get(str(dataset_id), list((rows[dataset_id].schema_json or {}).get("columns", {})))
        if not isinstance(columns, list) or any(not isinstance(column, str) for column in columns):
            raise HTTPException(422, f"Selected columns for dataset {dataset_id} must be a list of column names")
        available = (rows[dataset_id].schema_json or {}).get("columns", {})
        unknown = set(columns) - set(available)
        if unknown:
            raise HTTPException(422, f"Unknown selected columns for dataset {dataset_id}: {sorted(unknown)}")
        normalized_columns[str(dataset_id)] = list(dict.fromkeys(columns))
    normalized_connections = {}
    for dataset_id in ids:
        expected = connections.get(str(dataset_id)) if isinstance(connections, dict) else None
        source = (rows[dataset_id].schema_json or {}).get("_source", {})
        actual = source.get("credential_id")
        if expected is not None and actual != expected:
            # Older imported datasets may predate embedded _source metadata;
            # accept their saved workflow connection only when its provider
            # still matches the dataset's recorded source type.
            credential = db.get(Credential, expected) if actual is None else None
            if actual is not None or not credential or credential.provider != rows[dataset_id].source_type:
                raise HTTPException(422, "Selected dataset does not belong to the selected source connection")
        if actual is not None:
            normalized_connections[str(dataset_id)] = actual
    selection = db.get(WorkflowSelection, workflow_id)
    if selection is None:
        selection = WorkflowSelection(workflow_id=workflow_id, dataset_ids=ids,
                                      selected_columns=normalized_columns,
                                      source_connection_ids=normalized_connections)
        db.add(selection)
    else:
        selection.dataset_ids = ids
        selection.selected_columns = normalized_columns
        selection.source_connection_ids = normalized_connections
    db.commit()
    return {"workflow_id": workflow_id, "dataset_ids": ids,
            "selected_columns": normalized_columns,
            "source_connection_ids": normalized_connections,
            "datasets": [rows[dataset_id] for dataset_id in ids]}


@app.post("/api/v1/datasets/upload", status_code=201)
async def upload_dataset(file: UploadFile = File(...), name: str | None = None, db: Session = Depends(get_db)):
    fname = Path(file.filename or "").name
    if Path(fname).suffix.lower() not in (".csv", ".tsv", ".json", ".xlsx", ".xls", ".parquet"): raise HTTPException(415, "Upload a CSV, TSV, JSON, Excel, or Parquet file")
    content = await file.read(settings.max_upload_mb * 1024 * 1024 + 1)
    if len(content) > settings.max_upload_mb * 1024 * 1024: raise HTTPException(413, "Upload exceeds MAX_UPLOAD_MB")
    try: records = parse_data(fname, content)
    except Exception as e: raise HTTPException(400, f"Invalid dataset: {e}") from e
    columns = list(dict.fromkeys(k for row in records[:1000] for k in row))
    schema = {col: {"inferred_type": infer_type([r.get(col) for r in records[:1000]]), "nullable": any(is_missing(r.get(col)) for r in records[:1000])} for col in columns}
    path = LocalStorage().save(f"uploads/{__import__('uuid').uuid4().hex}-{fname}", content)
    dataset_name = name.strip() if isinstance(name, str) else ""
    dataset = Dataset(name=dataset_name or Path(fname).stem, filename=fname, path=path, schema_json={"columns": schema, "row_count": len(records)})
    db.add(dataset); db.commit(); db.refresh(dataset); return dataset


def infer_type(values):
    vals = [str(v).strip() for v in values if not is_missing(v)]
    if vals and all(is_int(v) for v in vals): return "integer"
    if vals and all(is_float(v) for v in vals): return "number"
    if vals and all("@" in v for v in vals): return "email-like"
    return "string"


def is_int(v):
    try: int(v); return True
    except (ValueError, TypeError): return False


def is_float(v):
    try: float(v); return True
    except (ValueError, TypeError): return False


@app.get("/api/v1/datasets/{dataset_id}")
def dataset_detail(dataset_id: int, db: Session = Depends(get_db)):
    return not_found(db.get(Dataset, dataset_id), "Dataset")


@app.get("/api/v1/jobs")
def list_jobs(db: Session = Depends(get_db)):
    return [job_response(job) for job in db.scalars(select(Job).order_by(Job.id.desc()))]


@app.post("/api/v1/jobs", status_code=201)
def create_job(body: JobCreate, db: Session = Depends(get_db)):
    datasets=[db.get(Dataset,did) for did in body.dataset_ids]
    if any(d is None for d in datasets): raise HTTPException(422, "Every dataset_ids entry must exist")
    if body.dataset_ids and "selected_columns" in body.model_fields_set and not body.selected_columns:
        raise HTTPException(422,"Select at least one column for the job")
    validate_job_inputs(body,datasets)
    values = body.model_dump()
    values["environment"] = infer_job_environment(values.get("environment"), body.settings)
    job = Job(**values); db.add(job); db.commit(); db.refresh(job); return job_response(job)


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    return job_response(not_found(db.get(Job, job_id), "Job"))


@app.patch("/api/v1/jobs/{job_id}")
def update_job(job_id: int, body: JobUpdate, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    changes=body.model_dump(exclude_unset=True)
    dataset_ids=changes.get("dataset_ids",job.dataset_ids)
    datasets=[db.get(Dataset,did) for did in dataset_ids]
    if any(d is None for d in datasets): raise HTTPException(422, "Every dataset_ids entry must exist")
    merged={"name":changes.get("name",job.name),"description":changes.get("description",job.description),"dataset_ids":dataset_ids,
        "selected_columns":changes.get("selected_columns",job.selected_columns),"join_config":changes.get("join_config",job.join_config),
        "destination":changes.get("destination",job.destination),"environment":changes.get("environment",job_environment(job)),"settings":changes.get("settings",job.settings or {})}
    validate_job_inputs(JobCreate(**merged),datasets)
    for k,v in changes.items(): setattr(job, k, v)
    db.commit(); db.refresh(job); return job_response(job)


def validate_configuration_rules(body: JobCreate, rules: list[RuleCreate], db: Session):
    for rule in rules:
        kind = rule.rule_type.upper()
        try:
            validate_rule_definition(kind, rule.parameters)
        except ValueError as e:
            raise HTTPException(422, str(e)) from e
        if rule.dataset_id is not None:
            if rule.dataset_id not in body.dataset_ids:
                raise HTTPException(422, "Rule dataset_id must be selected by the job")
            ds = db.get(Dataset, rule.dataset_id)
            if not ds or rule.column_name not in ds.schema_json.get("columns", {}):
                raise HTTPException(422, "Rule column does not exist in the selected dataset")


@app.post("/api/v1/jobs/configuration", status_code=201)
def create_job_configuration(body: JobConfiguration, db: Session = Depends(get_db)):
    datasets = [db.get(Dataset, did) for did in body.job.dataset_ids]
    if any(d is None for d in datasets):
        raise HTTPException(422, "Every dataset_ids entry must exist")
    if "selected_columns" in body.job.model_fields_set and not body.job.selected_columns:
        raise HTTPException(422,"Select at least one column for the job")
    validate_job_inputs(body.job, datasets)
    validate_configuration_rules(body.job, body.rules, db)
    values = body.job.model_dump()
    values["environment"] = infer_job_environment(values.get("environment"), body.job.settings)
    job = Job(**values)
    db.add(job)
    db.flush()
    for rule in body.rules:
        values = rule.model_dump()
        values["rule_type"] = values["rule_type"].upper()
        values["enabled"] = bool(values["enabled"])
        values["approved"] = bool(values["approved"])
        db.add(Rule(job_id=job.id, **values))
    db.commit()
    db.refresh(job)
    return job_response(job)


@app.put("/api/v1/jobs/{job_id}/configuration")
def replace_job_configuration(job_id: int, body: JobConfiguration, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    datasets = [db.get(Dataset, did) for did in body.job.dataset_ids]
    if any(d is None for d in datasets):
        raise HTTPException(422, "Every dataset_ids entry must exist")
    validate_job_inputs(body.job, datasets)
    validate_configuration_rules(body.job, body.rules, db)
    values = body.job.model_dump()
    values["environment"] = infer_job_environment(values.get("environment") or job_environment(job), body.job.settings)
    for key, value in values.items():
        setattr(job, key, value)
    db.execute(delete(Rule).where(Rule.job_id == job_id))
    for rule in body.rules:
        values = rule.model_dump()
        values["rule_type"] = values["rule_type"].upper()
        values["enabled"] = bool(values["enabled"])
        values["approved"] = bool(values["approved"])
        db.add(Rule(job_id=job_id, **values))
    db.commit()
    db.refresh(job)
    return job_response(job)


@app.delete("/api/v1/jobs/{job_id}", status_code=204)
def delete_job(job_id: int, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    # Explicitly remove dependent records so deletion is safe with SQLite and
    # databases where foreign-key cascade enforcement is not enabled.
    executions = list(db.scalars(select(Execution).where(Execution.job_id == job_id)))
    execution_ids = [item.id for item in executions]
    if execution_ids:
        db.execute(delete(Correction).where(Correction.execution_id.in_(execution_ids)))
        db.execute(delete(Execution).where(Execution.job_id == job_id))
    db.execute(delete(Rule).where(Rule.job_id == job_id))
    db.delete(job)
    db.commit()


def validate_job_inputs(body,datasets):
    if len(set(body.dataset_ids)) != len(body.dataset_ids): raise HTTPException(422,"dataset_ids must be unique")
    if len(datasets)>2 and (body.settings or {}).get("combine_mode") != "union": raise HTTPException(422,"A join job can combine up to two datasets")
    known={str(d.id):set((d.schema_json or {}).get("columns",{})) for d in datasets}
    for dataset_id,columns in body.selected_columns.items():
        if str(dataset_id) not in known: raise HTTPException(422,f"selected_columns references dataset {dataset_id} outside the job")
        missing=set(columns)-known[str(dataset_id)]
        if missing: raise HTTPException(422,f"Unknown selected columns for dataset {dataset_id}: {sorted(missing)}")
    if body.dataset_ids and body.selected_columns and not any(body.selected_columns.values()):
        raise HTTPException(422,"Select at least one column for the job")
    if len(datasets)>=2 and (body.settings or {}).get("combine_mode") != "union":
        cfg=body.join_config
        if not cfg: raise HTTPException(422,"join_config is required for a two-dataset job")
        try: left_id,right_id=int(cfg.get("left_dataset_id",-1)),int(cfg.get("right_dataset_id",-1))
        except (TypeError,ValueError): raise HTTPException(422,"join_config requires integer dataset IDs")
        if left_id not in [d.id for d in datasets] or right_id not in [d.id for d in datasets]:
            raise HTTPException(422,"join_config dataset IDs must be selected by the job")
        if left_id==right_id: raise HTTPException(422,"join_config must reference two different datasets")
        for did,key in ((left_id,cfg.get("left_key")),(right_id,cfg.get("right_key"))):
            if key not in known[str(did)]: raise HTTPException(422,f"Join key {key!r} does not exist in dataset {did}")


@app.get("/api/v1/jobs/{job_id}/landing-zone")
def landing_zone(job_id: int, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    return {"job_id": job_id, "files": [d for d in db.scalars(select(Dataset).where(Dataset.id.in_(job.dataset_ids))).all()], "selected_columns": job.selected_columns}


@app.get("/api/v1/jobs/{job_id}/rules")
def list_rules(job_id: int, db: Session = Depends(get_db)):
    not_found(db.get(Job, job_id), "Job")
    return db.scalars(select(Rule).where(Rule.job_id == job_id).order_by(Rule.id)).all()


RULE_IMPORT_COLUMNS = ["dataset_id", "column_name", "rule_type", "parameters", "severity", "enabled", "approved"]


def _rules_workbook():
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment
    from openpyxl.worksheet.datavalidation import DataValidation
    wb = Workbook(); ws = wb.active; ws.title = "Rules"
    ws.append(RULE_IMPORT_COLUMNS)
    ws.append(["", "email", "EMAIL_FORMAT", "{}", "high", True, False])
    ws.append(["", "age", "RANGE", '{"min":0,"max":120}', "medium", True, False])
    ws.freeze_panes = "A2"; ws.auto_filter.ref = "A1:G3"
    widths = [16, 26, 20, 38, 14, 12, 12]
    for i, width in enumerate(widths, 1): ws.column_dimensions[chr(64+i)].width = width
    for cell in ws[1]:
        cell.font = Font(bold=True, color="FFFFFF"); cell.fill = PatternFill("solid", fgColor="173B52"); cell.alignment = Alignment(vertical="center")
    dv = DataValidation(type="list", formula1='"' + ",".join(sorted(RULE_TYPES)) + '"'); ws.add_data_validation(dv); dv.add("C2:C1000")
    sev = DataValidation(type="list", formula1='"low,medium,high,critical"'); ws.add_data_validation(sev); sev.add("E2:E1000")
    flags = DataValidation(type="list", formula1='"TRUE,FALSE"'); ws.add_data_validation(flags); flags.add("F2:G1000")
    info=wb.create_sheet("Instructions")
    for row in [["AI Data Quality Rules — Import Guide"], ["Required columns: column_name, rule_type"], ["Optional columns: dataset_id (blank applies to all selected data), parameters (JSON object), severity, enabled, approved"], ["Use dataset_id from your selected datasets. Leave blank to make the rule job-wide."], ["Rule types: "+", ".join(sorted(RULE_TYPES))], ["Examples: RANGE parameters {\"min\":0,\"max\":100}; REGEX {\"pattern\":\"^[A-Z]+$\"}; IN_SET {\"values\":[\"A\",\"B\"]}"], ["Imported rules are validated and shown for review before they are saved."]]: info.append(row)
    info.column_dimensions["A"].width=115
    for c in info[1]: c.font=Font(bold=True,size=14,color="173B52")
    output=BytesIO(); wb.save(output); output.seek(0); return output


@app.get("/api/v1/rules/template")
def download_rules_template():
    return StreamingResponse(_rules_workbook(), media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", headers={"Content-Disposition":"attachment; filename=data-quality-rules-template.xlsx"})


@app.post("/api/v1/jobs/{job_id}/rules/import-preview")
async def preview_rule_import(job_id: int, file: UploadFile = File(...), db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    name=(file.filename or "").lower()
    if not name.endswith((".xlsx", ".xls")): raise HTTPException(415, "Upload an .xlsx or .xls workbook")
    raw=await file.read()
    try:
        if name.endswith(".xlsx"):
            from openpyxl import load_workbook
            sheet=load_workbook(BytesIO(raw), read_only=True, data_only=True).active
            matrix=list(sheet.iter_rows(values_only=True))
        else:
            import xlrd
            book=xlrd.open_workbook(file_contents=raw); sheet=book.sheet_by_index(0)
            matrix=[[sheet.cell_value(r,c) for c in range(sheet.ncols)] for r in range(sheet.nrows)]
    except Exception as e: raise HTTPException(422, "Could not read the workbook. Check that it is a valid Excel file.") from e
    if not matrix: raise HTTPException(422, "The Rules sheet is empty")
    headers=[str(v or "").strip().lower() for v in matrix[0]]
    if "column_name" not in headers or "rule_type" not in headers: raise HTTPException(422, "Required columns column_name and rule_type are missing")
    existing=list(db.scalars(select(Rule).where(Rule.job_id==job_id)))
    known={rule_signature(r) for r in existing}; seen=set(); valid=[]; errors=[]
    datasets={d.id:d for d in db.scalars(select(Dataset).where(Dataset.id.in_(job.dataset_ids))).all()}
    for line, values in enumerate(matrix[1:], 2):
        row={headers[i]: values[i] if i<len(values) else None for i in range(len(headers))}
        if not any(v is not None and str(v).strip() for v in row.values()): continue
        try:
            column=str(row.get("column_name") or "").strip(); kind=str(row.get("rule_type") or "").strip().upper()
            if not column: raise ValueError("column_name is required")
            if not kind: raise ValueError("rule_type is required")
            dsval=row.get("dataset_id"); dsid=int(dsval) if dsval is not None and str(dsval).strip() else None
            if dsid is not None and dsid not in datasets: raise ValueError("dataset_id must belong to this job")
            if dsid is not None and column not in (datasets[dsid].schema_json or {}).get("columns",{}): raise ValueError("column_name is not present in the selected dataset")
            if dsid is None and not any(column in (d.schema_json or {}).get("columns",{}) for d in datasets.values()): raise ValueError("column_name was not found in selected datasets")
            params=row.get("parameters") or {}
            if isinstance(params,str): params=json.loads(params) if params.strip() else {}
            if not isinstance(params,dict): raise ValueError("parameters must be a JSON object")
            validate_rule_definition(kind,params)
            severity=str(row.get("severity") or "medium").strip().lower()
            if severity not in {"low","medium","high","critical"}: raise ValueError("severity must be low, medium, high, or critical")
            rule=Rule(job_id=job_id,dataset_id=dsid,column_name=column,rule_type=kind,parameters={**params,"severity":severity},enabled=str(row.get("enabled") if row.get("enabled") is not None else "false").lower() in ("true","1","yes"),approved=str(row.get("approved") if row.get("approved") is not None else "false").lower() in ("true","1","yes"))
            sig=rule_signature(rule)
            if sig in known or sig in seen: raise ValueError("Duplicate rule (already exists or repeated in this file)")
            seen.add(sig); valid.append({"row_number":line,"dataset_id":dsid,"column_name":column,"rule_type":kind,"parameters":rule.parameters,"enabled":rule.enabled,"approved":rule.approved,"severity":severity})
        except Exception as e: errors.append({"row_number":line,"error":str(e),"row":{k:(v.isoformat() if hasattr(v,"isoformat") else v) for k,v in row.items()}})
    return {"valid":valid,"errors":errors}


@app.post("/api/v1/jobs/{job_id}/rules/import")
def import_rules(job_id: int, body: dict, db: Session = Depends(get_db)):
    job=not_found(db.get(Job,job_id),"Job")
    saved=[]
    existing={rule_signature(r) for r in db.scalars(select(Rule).where(Rule.job_id==job_id))}
    for item in body.get("rules",[]):
        payload=RuleCreate(**item)
        kind=payload.rule_type.upper(); validate_rule_definition(kind,payload.parameters)
        if payload.dataset_id is not None:
            if payload.dataset_id not in job.dataset_ids: raise HTTPException(422,"Rule dataset_id must be selected by the job")
            ds=db.get(Dataset,payload.dataset_id)
            if not ds or payload.column_name not in (ds.schema_json or {}).get("columns",{}): raise HTTPException(422,"Rule column does not exist in the selected dataset")
        rule=Rule(job_id=job_id,**{**payload.model_dump(),"rule_type":kind})
        signature=rule_signature(rule)
        if signature in existing: raise HTTPException(409,"An imported rule is now a duplicate; preview the workbook again")
        existing.add(signature); db.add(rule); saved.append(rule)
    db.commit()
    for rule in saved: db.refresh(rule)
    return {"added":len(saved),"rules":saved}


@app.post("/api/v1/rules/error-report")
def download_rule_errors(body: dict):
    from openpyxl import Workbook
    wb=Workbook(); ws=wb.active; ws.title="Invalid rows"; ws.append(["Excel row","Error","Original row"])
    for item in body.get("errors",[]): ws.append([item.get("row_number"),item.get("error"),json.dumps(item.get("row",{}),default=str)])
    output=BytesIO(); wb.save(output); output.seek(0)
    return StreamingResponse(output,media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",headers={"Content-Disposition":"attachment; filename=data-quality-rule-errors.xlsx"})


@app.post("/api/v1/jobs/{job_id}/baseline-rules", status_code=201)
def add_employee_baseline_rules(job_id: int, db: Session = Depends(get_db)):
    """Idempotently add the application's documented employee baseline checks.

    These fixed product rules are distinct from AI suggestions and are active
    only when the user explicitly adds them from the rule review screen.
    Existing rules and their approval decisions are never edited.
    """
    job = not_found(db.get(Job, job_id), "Job")
    if len(job.dataset_ids) != 1:
        raise HTTPException(422, "Employee baseline rules require a job with one dataset")
    dataset = not_found(db.get(Dataset, job.dataset_ids[0]), "Dataset")
    required = ["EmployeeID", "Name", "Email", "Age", "Department", "Salary"]
    schema_columns = (dataset.schema_json or {}).get("columns", {})
    if any(column not in schema_columns for column in required):
        raise HTTPException(422, "Dataset must contain EmployeeID, Name, Email, Age, Department, and Salary")
    selected = (job.selected_columns or {}).get(str(dataset.id), list(schema_columns))
    if any(column not in selected for column in required):
        raise HTTPException(422, "Select all six employee baseline columns before adding baseline rules")

    baseline_id = "employee-v1"
    definitions = [
        (column, "NOT_NULL", {}) for column in required
    ] + [
        ("Email", "EMAIL_FORMAT", {}),
        ("Age", "DATA_TYPE", {"type": "number"}),
        ("Age", "RANGE", {"min": 0}),
        ("Salary", "DATA_TYPE", {"type": "number"}),
        ("Salary", "RANGE", {"min": 0}),
        ("Department", "IN_SET", {"values": ["IT", "HR", "Finance", "Engineering", "Operations", "Marketing"], "case_sensitive": False}),
        ("EmployeeID", "UNIQUE", {}),
    ]
    existing = list(db.scalars(select(Rule).where(Rule.job_id == job.id)))
    known = {(r.dataset_id, r.column_name, r.rule_type.upper(),
              json.dumps({k:v for k,v in (r.parameters or {}).items() if k not in {"severity", "rationale"}}, sort_keys=True))
             for r in existing if (r.parameters or {}).get("baseline_id") == baseline_id}
    added = []
    for column, kind, params in definitions:
        marked = {**params, "baseline_id": baseline_id}
        key = (dataset.id, column, kind, json.dumps(marked, sort_keys=True))
        if key in known:
            continue
        rule = Rule(job_id=job.id, dataset_id=dataset.id, column_name=column, rule_type=kind,
                    parameters=marked, enabled=True, approved=True)
        db.add(rule)
        added.append(rule)
    db.commit()
    for rule in added:
        db.refresh(rule)
    return {"baseline_id": baseline_id, "added": len(added), "rules": added,
            "note": "Product-defined employee baseline rules are enabled and approved. AI suggestions still require review."}


@app.post("/api/v1/jobs/{job_id}/rules", status_code=201)
def create_rule(job_id: int, body: RuleCreate, db: Session = Depends(get_db)):
    job=not_found(db.get(Job, job_id), "Job")
    kind=body.rule_type.upper()
    try: validate_rule_definition(kind,body.parameters)
    except ValueError as e: raise HTTPException(422,str(e)) from e
    if body.dataset_id is not None:
        if body.dataset_id not in job.dataset_ids: raise HTTPException(422,"Rule dataset_id must be selected by the job")
        ds=db.get(Dataset,body.dataset_id)
        if not ds or body.column_name not in ds.schema_json.get("columns",{}): raise HTTPException(422,"Rule column does not exist in the selected dataset")
    rule = Rule(job_id=job_id, **{**body.model_dump(), "rule_type": kind})
    db.add(rule); db.commit(); db.refresh(rule); return rule


@app.patch("/api/v1/rules/{rule_id}")
def update_rule(rule_id: int, body: RuleUpdate, db: Session = Depends(get_db)):
    rule = not_found(db.get(Rule, rule_id), "Rule")
    changes = {key: value for key, value in body.model_dump(exclude_unset=True).items() if value is not None}
    if "rule_type" in changes:
        changes["rule_type"] = changes["rule_type"].upper()
        if changes["rule_type"] not in RULE_TYPES: raise HTTPException(422, "Unsupported rule type")
    kind=changes.get("rule_type",rule.rule_type)
    params=changes.get("parameters",rule.parameters)
    try: validate_rule_definition(kind,params)
    except ValueError as e: raise HTTPException(422,str(e)) from e
    for k,v in changes.items(): setattr(rule,k,v)
    db.commit(); db.refresh(rule); return rule


@app.post("/api/v1/jobs/{job_id}/rules/approve-all")
def approve_all_rules(job_id: int, db: Session = Depends(get_db)):
    not_found(db.get(Job, job_id), "Job")
    rules = list(db.scalars(select(Rule).where(Rule.job_id == job_id).order_by(Rule.id)))
    approved = []
    failed = []
    skipped = []
    for rule in rules:
        if rule.approved:
            skipped.append({"rule_id": rule.id, "reason": "already approved"})
            continue
        if not rule.enabled:
            skipped.append({"rule_id": rule.id, "reason": "pending rule is disabled"})
            continue
        if not isinstance(rule.parameters, dict) or "rationale" not in rule.parameters:
            skipped.append({"rule_id": rule.id, "reason": "pending rule is not AI-generated"})
            continue
        try:
            validate_rule_definition(rule.rule_type, rule.parameters or {})
        except ValueError as exc:
            failed.append({"rule_id": rule.id, "reason": str(exc)})
            continue
        rule.approved = True
        rule.enabled = True
        approved.append(rule)
    db.commit()
    for rule in approved:
        db.refresh(rule)
    return {"approved_count": len(approved), "failed_count": len(failed),
            "skipped_count": len(skipped), "approved_rule_ids": [rule.id for rule in approved],
            "failed": failed, "skipped": skipped, "rules": rules}


@app.delete("/api/v1/rules/{rule_id}", status_code=204)
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = not_found(db.get(Rule, rule_id), "Rule"); db.delete(rule); db.commit()


RULE_GENERATION_TASKS: dict[str, dict] = {}


RULE_SUGGESTION_PARAMETER_KEYS = ("type", "min", "max", "value", "pattern", "values", "case_sensitive", "strict")
RULE_GENERATION_MAX_ATTEMPTS = 3
RULE_GENERATED_PARAMETER_REQUIREMENTS = {
    "NOT_NULL": "parameters must be an empty object unless optional flags are explicitly justified by profiling evidence.",
    "UNIQUE": "parameters must be an empty object unless optional flags are explicitly justified by profiling evidence.",
    "DUPLICATES": "parameters must be an empty object unless optional flags are explicitly justified by profiling evidence.",
    "EMAIL_FORMAT": "parameters must be an empty object unless optional flags are explicitly justified by profiling evidence.",
    "DATA_TYPE": "parameters.type is required and must be one of string, integer, number, boolean, date, or datetime.",
    "MIN_VALUE": "parameters.min or parameters.value is required and must be numeric.",
    "MAX_VALUE": "parameters.max or parameters.value is required and must be numeric.",
    "RANGE": "at least one numeric boundary, parameters.min or parameters.max, is required.",
    "REGEX": "parameters.pattern is required and must be a non-empty valid regular expression derived from observed schema metadata or samples.",
    "LENGTH": "at least one integer boundary, parameters.min or parameters.max, is required.",
    "IN_SET": "parameters.values is required and must be a non-empty array of observed or metadata-provided allowed values.",
}


RULE_SUGGESTION_RESPONSE_FORMAT = {
    "type": "json_schema",
    "json_schema": {
        "name": "data_quality_rule_suggestions",
        "strict": True,
        "schema": {
            "type": "object",
            "properties": {
                "suggestions": {
                    "type": "array",
                    "maxItems": 200,
                    "items": {
                        "type": "object",
                        "properties": {
                            "dataset_id": {"type": "integer"},
                            "column_name": {"type": "string"},
                            "rule_type": {"type": "string", "enum": sorted(RULE_TYPES)},
                            "parameters": {
                                "type": "object",
                                "properties": {
                                    "type": {"type": ["string", "null"]},
                                    "min": {"type": ["number", "string", "null"]},
                                    "max": {"type": ["number", "string", "null"]},
                                    "value": {"type": ["number", "string", "null"]},
                                    "pattern": {"type": ["string", "null"]},
                                    "values": {
                                        "type": ["array", "null"],
                                        "items": {"type": ["string", "number", "boolean"]},
                                    },
                                    "case_sensitive": {"type": ["boolean", "null"]},
                                    "strict": {"type": ["boolean", "null"]},
                                },
                                "required": list(RULE_SUGGESTION_PARAMETER_KEYS),
                                "additionalProperties": False,
                            },
                            "severity": {"type": "string", "enum": ["low", "medium", "high", "critical"]},
                            "rationale": {"type": "string"},
                        },
                        "required": ["dataset_id", "column_name", "rule_type", "parameters", "severity", "rationale"],
                        "additionalProperties": False,
                    },
                },
            },
            "required": ["suggestions"],
            "additionalProperties": False,
        },
    },
}


def _save_rule_suggestions(job, suggestions, db):
    existing = list(db.scalars(select(Rule).where(Rule.job_id == job.id)))
    existing_by_key = {rule_key(r.dataset_id, r.column_name, r.rule_type, r.parameters): r for r in existing}
    existing_keys = set(existing_by_key)
    added = []
    updated = []
    for item in suggestions:
        params = item["parameters"]
        _validate_generated_rule_parameters(item["rule_type"], params)
        key = rule_key(item["dataset_id"], item["column_name"], item["rule_type"], params)
        if key in existing_keys:
            existing_rule = existing_by_key.get(key)
            if existing_rule and not existing_rule.approved:
                existing_rule.parameters = {**params, "severity": item["severity"], "rationale": item["rationale"]}
                existing_rule.enabled = True
                updated.append(existing_rule)
            continue
        existing_keys.add(key)
        rule = Rule(job_id=job.id, dataset_id=item["dataset_id"], column_name=item["column_name"],
                    rule_type=item["rule_type"], parameters={**params, "severity": item["severity"], "rationale": item["rationale"]},
                    enabled=True, approved=False)
        db.add(rule); added.append(rule)
    db.commit()
    for rule in added:
        db.refresh(rule)
    for rule in updated:
        db.refresh(rule)
    return added + updated


async def _run_rule_generation_task(task_id, job_id):
    task = RULE_GENERATION_TASKS[task_id]
    task.update({"status": "running", "phase": "requesting_openai", "progress": 25})
    try:
        with SessionLocal() as db:
            job = not_found(db.get(Job, job_id), "Job")
            datasets = [not_found(db.get(Dataset, did), "Dataset") for did in job.dataset_ids]
            task.update({"phase": "analyzing_schema", "progress": 40})
            suggestions = await generate_rule_suggestions(datasets, db, job.selected_columns or {})
            task.update({"phase": "saving_rules", "progress": 85})
            added = _save_rule_suggestions(job, suggestions, db)
            task.update({"status": "completed", "phase": "completed", "progress": 100,
                         "suggestions": added, "added": len(added), "finished_at": time.time(),
                         "note": "AI-generated rules are pending review and approval."})
    except HTTPException as exc:
        task.update({"status": "failed", "phase": "failed", "progress": 100,
                     "error": exc.detail, "finished_at": time.time()})
    except Exception as exc:
        logging.getLogger(__name__).exception("AI rule generation task failed (job_id=%s task_id=%s)", job_id, task_id)
        task.update({"status": "failed", "phase": "failed", "progress": 100,
                     "error": f"AI rule generation failed: {type(exc).__name__}", "finished_at": time.time()})
    finally:
        if RULE_GENERATION_TASKS.get(task_id) is task and task.get("status") in {"completed", "failed"}:
            task["running"] = False


def _active_rule_generation_task(job_id):
    for task_id, task in RULE_GENERATION_TASKS.items():
        if task.get("job_id") == job_id and task.get("status") in {"queued", "running"}:
            return task_id, task
    return None, None


@app.post("/api/v1/jobs/{job_id}/suggest-rules")
async def suggest_rules(job_id: int, async_request: bool = Query(False, alias="async"), db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    if async_request:
        active_task_id, active_task = _active_rule_generation_task(job_id)
        if active_task:
            return {"provider":"openai","model":settings.resolved_ai_model,"task_id":active_task_id, **active_task}
        task_id = uuid.uuid4().hex
        RULE_GENERATION_TASKS[task_id] = {"job_id": job_id, "status": "queued", "phase": "queued",
                                          "progress": 0, "added": 0, "suggestions": [],
                                          "error": None, "started_at": time.time(), "running": True}
        asyncio.create_task(_run_rule_generation_task(task_id, job_id))
        return {"provider":"openai","model":settings.resolved_ai_model,"task_id":task_id, **RULE_GENERATION_TASKS[task_id]}
    datasets = [not_found(db.get(Dataset, did), "Dataset") for did in job.dataset_ids]
    suggestions = await generate_rule_suggestions(datasets, db, job.selected_columns or {})
    added = _save_rule_suggestions(job, suggestions, db)
    return {"provider":"openai","model":settings.resolved_ai_model,"suggestions":added,"saved":True,"added":len(added),"note":"AI-generated rules are pending review and approval."}


@app.get("/api/v1/jobs/{job_id}/suggest-rules/{task_id}")
def get_rule_generation_status(job_id: int, task_id: str):
    task = RULE_GENERATION_TASKS.get(task_id)
    if not task or task.get("job_id") != job_id:
        raise HTTPException(404, "Rule generation task not found")
    return {"provider":"openai","model":settings.resolved_ai_model,"task_id":task_id, **task}


@app.post("/api/v1/datasets/{dataset_id}/suggest-rules")
async def suggest_dataset_rules(dataset_id: int, db: Session = Depends(get_db)):
    dataset = not_found(db.get(Dataset, dataset_id), "Dataset")
    suggestions = await generate_rule_suggestions([dataset], db)
    return {"provider":"openai","model":settings.resolved_ai_model,"suggestions":suggestions,"saved":False}


def rule_key(dataset_id, column_name, rule_type, parameters):
    relevant = {k:v for k,v in (parameters or {}).items() if k not in {"severity", "rationale"}}
    return (dataset_id, column_name, rule_type.upper(), json.dumps(relevant, sort_keys=True, default=str))


def _profile_values(values):
    total = len(values)
    present = [value for value in values if not is_missing(value)]
    text_values = [str(value).strip() for value in present]
    numeric_values = []
    for value in present:
        try:
            number = float(value)
            if number == number:
                numeric_values.append(number)
        except (TypeError, ValueError):
            pass
    email_like_values = [value for value in text_values if "@" in value or "." in value]
    invalid_email_values = [value for value in email_like_values if not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value)]
    counts = {}
    for value in text_values:
        counts[value] = counts.get(value, 0) + 1
    duplicate_values = [value for value, count in counts.items() if count > 1]
    return {
        "null_count": total - len(present),
        "null_fraction": (total - len(present)) / max(total, 1),
        "non_null_count": len(present),
        "distinct_count": len(counts),
        "distinct_sample": list(dict.fromkeys(text_values))[:10],
        "duplicate_sample": duplicate_values[:10],
        "duplicate_count": sum(count - 1 for count in counts.values() if count > 1),
        "numeric_min": min(numeric_values) if numeric_values else None,
        "numeric_max": max(numeric_values) if numeric_values else None,
        "numeric_negative_count": sum(1 for number in numeric_values if number < 0),
        "non_numeric_count": max(0, len(present) - len(numeric_values)) if numeric_values else None,
        "invalid_email_count": len(invalid_email_values),
        "invalid_email_sample": invalid_email_values[:5],
    }


def _safe_provider_error(response, api_key):
    """Extract a short provider diagnostic without returning credential material."""
    try:
        body = response.json()
    except (ValueError, TypeError):
        body = None
    error = body.get("error") if isinstance(body, dict) else None
    if isinstance(error, dict):
        detail = error.get("message") or error.get("type") or error.get("code")
    elif isinstance(error, str):
        detail = error
    else:
        detail = None
    if not detail:
        detail = response.text
    detail = str(detail)
    detail = re.sub(r"(?i)bearer\s+\S+", "Bearer [redacted]", detail)
    detail = re.sub(r"(?i)(api[_ -]?key|access[_ -]?token|client[_ -]?secret)\s*[:=]\s*\S+", r"\1=[redacted]", detail)
    detail = re.sub(r"(?i)\b(gsk_[A-Za-z0-9_-]+|sk-[A-Za-z0-9_-]+)\b", "[redacted]", detail)
    if api_key:
        detail = detail.replace(api_key, "[redacted]")
    return detail[:500].strip() or "No error details returned"


def _openai_client(api_key, timeout):
    return AsyncOpenAI(api_key=api_key, timeout=timeout, max_retries=2)


async def _openai_json_chat(messages, *, timeout, response_format=None):
    completion = await _openai_client(settings.resolved_ai_api_key, timeout).chat.completions.create(
        model=settings.resolved_ai_model,
        messages=messages,
        response_format=response_format or {"type": "json_object"},
    )
    content = completion.choices[0].message.content
    if not isinstance(content, str) or not content.strip():
        raise ValueError("OpenAI returned an empty JSON response")
    return content


def _extract_json_text(text: str) -> str:
    if not isinstance(text, str) or not text.strip():
        raise ValueError("OpenAI returned an empty JSON response")
    stripped = text.strip()
    fence = re.fullmatch(r"```(?:json|JSON)?\s*(.*?)\s*```", stripped, flags=re.DOTALL)
    if fence:
        stripped = fence.group(1).strip()
    if not stripped:
        raise ValueError("OpenAI returned an empty JSON response")
    if stripped[0] not in "{[":
        raise ValueError("OpenAI response was not JSON or Markdown-wrapped JSON")
    return stripped


def _parse_openai_json(text: str):
    payload = _extract_json_text(text)
    try:
        return json.loads(payload)
    except json.JSONDecodeError as exc:
        near_end = exc.pos >= max(0, len(payload) - 3)
        if near_end or exc.msg in {"Unterminated string starting at", "Expecting value", "Expecting ',' delimiter"}:
            raise ValueError(f"OpenAI returned malformed or truncated JSON: {exc.msg} at character {exc.pos}") from exc
        raise ValueError(f"OpenAI returned malformed JSON: {exc.msg} at character {exc.pos}") from exc


def _rule_generation_error(exc: Exception) -> str:
    message = str(exc).strip() or type(exc).__name__
    message = re.sub(r"(?i)bearer\s+\S+", "Bearer [redacted]", message)
    message = re.sub(r"(?i)(api[_ -]?key|access[_ -]?token|client[_ -]?secret)\s*[:=]\s*\S+", r"\1=[redacted]", message)
    api_key = settings.resolved_ai_api_key
    if api_key:
        message = message.replace(api_key, "[redacted]")
    return message[:500]


def _normalize_ai_rule_parameters(kind: str, params: dict) -> dict:
    if not isinstance(params, dict):
        raise ValueError("AI response rule parameters must be an object")
    cleaned = {key: value for key, value in (params or {}).items()
               if key in RULE_SUGGESTION_PARAMETER_KEYS and value is not None}
    if kind == "DATA_TYPE" and "type" in cleaned:
        cleaned["type"] = str(cleaned["type"]).lower()
    if kind == "IN_SET" and isinstance(cleaned.get("values"), list):
        cleaned["values"] = [str(value) if not isinstance(value, bool) else value for value in cleaned["values"]]
    return cleaned


def _validate_generated_rule_parameters(kind: str, params: dict):
    validate_rule_definition(kind, params)
    if kind == "DATA_TYPE" and not isinstance(params.get("type"), str):
        raise ValueError("DATA_TYPE requires a type parameter")
    if kind in {"RANGE", "LENGTH"} and params.get("min") is None and params.get("max") is None:
        raise ValueError(f"{kind} requires at least one of min or max")
    if kind == "REGEX" and not str(params.get("pattern", "")).strip():
        raise ValueError("REGEX requires a non-empty pattern")
    if kind == "IN_SET" and not params.get("values"):
        raise ValueError("IN_SET requires a non-empty values array")


def _build_rule_generation_messages(prompt, feedback=None):
    messages = [
        {"role": "system", "content": "You generate only JSON that matches the requested schema. Do not wrap the JSON unless the API does it."},
        {"role": "user", "content": json.dumps(prompt)},
    ]
    if feedback:
        messages.append({"role": "user", "content": json.dumps({
            "task": "Regenerate the full suggestions JSON. The previous response was rejected by local validation.",
            "validation_error": feedback,
            "instruction": "Fix the invalid rule or omit it if the provided dataset profile does not support all required parameters. Do not add fallback rules or hardcoded patterns.",
        })})
    return messages


def _parse_and_validate_rule_suggestions(result, datasets, selected_columns):
    if isinstance(result, dict):
        suggestions = result.get("suggestions", [])
    else:
        raise ValueError("Expected an object with a suggestions array")
    if not isinstance(suggestions, list):
        raise ValueError("AI response suggestions field must be an array")
    known = {ds.id: set((ds.schema_json or {}).get("columns", {})) & set((selected_columns or {}).get(str(ds.id), (ds.schema_json or {}).get("columns", {}))) for ds in datasets}
    clean = []
    if len(suggestions) > 200:
        raise ValueError("Too many suggestions")
    for index, item in enumerate(suggestions):
        if not isinstance(item, dict):
            raise ValueError(f"Suggestion {index + 1} must be an object")
        did = int(item.get("dataset_id", -1))
        column = item.get("column_name")
        kind = str(item.get("rule_type", "")).upper()
        params = _normalize_ai_rule_parameters(kind, item.get("parameters", {}))
        if did not in known or column not in known[did] or kind not in RULE_TYPES or not isinstance(params, dict):
            raise ValueError(f"Suggestion {index + 1} referenced an unknown column or unsupported rule")
        try:
            _validate_generated_rule_parameters(kind, params)
        except ValueError as exc:
            raise ValueError(f"Suggestion {index + 1} {kind} on {column}: {exc}") from exc
        severity = str(item.get("severity", "medium")).lower()
        if severity not in {"low", "medium", "high", "critical"}:
            raise ValueError(f"Suggestion {index + 1} has invalid severity")
        clean.append({"dataset_id": did, "column_name": column, "rule_type": kind, "parameters": params,
                      "severity": severity, "rationale": str(item.get("rationale", "Suggested from schema metadata"))[:500]})
    return clean


async def generate_rule_suggestions(datasets, db, selected_columns=None):
    ai_api_key=settings.resolved_ai_api_key
    ai_model=settings.resolved_ai_model
    if not ai_api_key or not ai_model:
        raise HTTPException(503, "AI rule suggestions are disabled; configure OPENAI_API_KEY and OPENAI_MODEL")
    profile = []
    local = LocalStorage()
    for ds in datasets:
        schema = ds.schema_json or {}
        allowed = (selected_columns or {}).get(str(ds.id))
        cols = {c:dict(meta) for c,meta in schema.get("columns",{}).items() if allowed is None or c in allowed}
        try:
            filename, path = getattr(ds, "filename"), getattr(ds, "path")
            rows = parse_data(filename, local.read(path))
            for c, meta in cols.items():
                vals = [r.get(c) for r in rows]
                meta.update({"inferred_type": infer_type(vals), **_profile_values(vals)})
            schema["row_count"] = len(rows)
        except (AttributeError, OSError, ValueError):
            pass
        profile.append({"dataset_id": ds.id, "dataset_name": ds.name, "source_type": ds.source_type,
                        "row_count": schema.get("row_count"), "metadata":{k:v for k,v in schema.items() if k != "columns"}, "columns": cols})
    prompt = {"task":"Dynamically suggest data quality rules grounded only in the provided dataset schema, actual column names, data types, sampled values, and profiling evidence. Return JSON with a suggestions array. Each item: dataset_id, column_name, rule_type, parameters, severity (low, medium, high, critical), rationale. Use only real columns and supported rule types. Inspect null_count, null_fraction, non_null_count, distinct_count, distinct_sample, duplicate_sample, duplicate_count, numeric_min, numeric_max, non_numeric_count, invalid_email_count, and invalid_email_sample for observed evidence. DATA_TYPE validates parseability only. MIN_VALUE, MAX_VALUE, and RANGE enforce configured numeric boundaries only when the boundary is present in schema constraints, domain metadata, or clearly user-provided configuration. REGEX requires an observed or metadata-provided pattern; never return REGEX with null, empty, guessed, or generic pattern. Do not infer business ranges from column names, dataset names, row numbers, or negative values alone. Do not hardcode domain assumptions, column names, NOT NULL rules, fallback rules, or rules for age, salary, email, or any other domain unless the actual profile evidence supports that exact check. Recommend completeness, validity, uniqueness, consistency, accuracy-related, type, format, and dataset-specific checks only when supported by the profile evidence. If no reliable checks are supported, return an empty suggestions array.", "supported_rule_types":sorted(RULE_TYPES), "rule_parameter_requirements": RULE_GENERATED_PARAMETER_REQUIREMENTS, "profile":profile}
    try:
        feedback = None
        for attempt in range(1, RULE_GENERATION_MAX_ATTEMPTS + 1):
            text = await _openai_json_chat(
                _build_rule_generation_messages(prompt, feedback),
                timeout=settings.ai_timeout_seconds,
                response_format=RULE_SUGGESTION_RESPONSE_FORMAT,
            )
            try:
                result = _parse_openai_json(text)
                return _parse_and_validate_rule_suggestions(result, datasets, selected_columns)
            except (KeyError, ValueError, TypeError) as exc:
                feedback = _rule_generation_error(exc)
                if attempt == RULE_GENERATION_MAX_ATTEMPTS:
                    raise ValueError(f"{feedback}; failed after {RULE_GENERATION_MAX_ATTEMPTS} attempts") from exc
                logging.getLogger(__name__).warning(
                    "OpenAI rule-generation response failed validation; retrying with feedback (model=%s attempt=%s/%s detail=%s)",
                    ai_model, attempt, RULE_GENERATION_MAX_ATTEMPTS, feedback,
                )
    except APIStatusError as e:
        status = e.status_code
        request_id = getattr(e, "request_id", None) or e.response.headers.get("x-request-id")
        provider_detail = _safe_provider_error(e.response, ai_api_key)
        logging.getLogger(__name__).warning(
            "OpenAI returned HTTP %s (model=%s request_id=%s detail=%s)",
            status, ai_model, request_id or "unavailable", provider_detail,
        )
        if status == 401:
            raise HTTPException(502, f"OpenAI returned HTTP 401 (invalid API key): {provider_detail}") from e
        if status == 403:
            raise HTTPException(502, f"OpenAI returned HTTP 403 (permission or model access issue): {provider_detail}") from e
        if status == 429:
            raise HTTPException(503, f"OpenAI returned HTTP 429 (rate limit or quota issue): {provider_detail}") from e
        if status == 400:
            raise HTTPException(502, f"OpenAI returned HTTP 400 (invalid request or unsupported parameter): {provider_detail}") from e
        if status == 404:
            raise HTTPException(502, f"OpenAI returned HTTP 404 (endpoint or model unavailable): {provider_detail}") from e
        if status == 422:
            raise HTTPException(502, f"OpenAI returned HTTP 422 (request validation failed): {provider_detail}") from e
        if status >= 500:
            raise HTTPException(503, f"OpenAI returned HTTP {status} (provider-side failure): {provider_detail}") from e
        raise HTTPException(502, f"OpenAI returned HTTP {status}: {provider_detail}") from e
    except APITimeoutError as e:
        logging.getLogger(__name__).warning("OpenAI request timed out (model=%s)", ai_model)
        raise HTTPException(504, "OpenAI request timed out; try again") from e
    except APIConnectionError as e:
        logging.getLogger(__name__).warning("OpenAI connection failed (model=%s error=%s)", ai_model, type(e).__name__)
        raise HTTPException(502, "Could not connect to OpenAI; verify network access") from e
    except APIError as e:
        logging.getLogger(__name__).warning("OpenAI request failed (model=%s error=%s)", ai_model, type(e).__name__)
        raise HTTPException(502, "OpenAI request failed; no rules were created") from e
    except (KeyError, ValueError, TypeError) as e:
        detail = _rule_generation_error(e)
        logging.getLogger(__name__).warning(
            "OpenAI returned an invalid rule-generation response (model=%s error=%s detail=%s)",
            ai_model, type(e).__name__, detail,
        )
        raise HTTPException(502, f"OpenAI returned an invalid response: {detail}; no rules were created") from e


@app.post("/api/v1/jobs/{job_id}/execute", status_code=202)
async def run_job(job_id: int, request: Request, db: Session = Depends(get_db)):
    not_found(db.get(Job, job_id), "Job")
    active = db.scalar(select(Execution.id).where(
        Execution.job_id == job_id, Execution.status.in_(("queued", "running"))
    ).limit(1))
    if active is not None:
        raise HTTPException(409, f"Job already has an active execution ({active})")
    record = Execution(job_id=job_id, status="queued", rule_snapshot=rule_snapshot(db, job_id))
    db.add(record); db.commit(); db.refresh(record)
    await _enqueue_validation(request.app, record.id)
    return record


@app.post("/api/v1/executions/{execution_id}/retry", status_code=202)
async def retry_execution(execution_id: int, request: Request, db: Session = Depends(get_db)):
    previous=not_found(db.get(Execution,execution_id),"Execution")
    storage_retry = False
    if previous.report_path:
        try:
            saved_report = json.loads(LocalStorage().read(previous.report_path))
            storage_retry = (previous.status in {"succeeded", "failed"} and
                             saved_report.get("validation_status") == "succeeded" and
                             saved_report.get("storage_status", {}).get("status") == "failed")
        except (OSError, ValueError):
            storage_retry = False
    if previous.status != "failed" and not storage_retry:
        raise HTTPException(409,"Only failed executions or completed validations with failed destination storage can be retried")
    active = db.scalar(select(Execution.id).where(
        Execution.job_id == previous.job_id, Execution.status.in_(("queued", "running"))
    ).limit(1))
    if active is not None:
        raise HTTPException(409, f"Job already has an active execution ({active})")
    if storage_retry:
        # Reuse the same execution identifier so execution-scoped destination
        # tables are replaced idempotently and history does not gain duplicates.
        previous.status="queued"; previous.attempt += 1; previous.error=None
        previous.finished_at=None
        retry=previous
    else:
        retry=Execution(job_id=previous.job_id,status="queued",attempt=previous.attempt+1,retry_of=previous.id,
                        rule_snapshot=previous.rule_snapshot if previous.rule_snapshot is not None else rule_snapshot(db, previous.job_id))
    db.add(retry);db.commit();db.refresh(retry)
    await _enqueue_validation(request.app, retry.id)
    return retry


@app.get("/api/v1/jobs/{job_id}/executions")
def job_executions(job_id: int, db: Session = Depends(get_db)):
    not_found(db.get(Job, job_id), "Job")
    return db.scalars(select(Execution).where(Execution.job_id == job_id).order_by(Execution.id.desc())).all()


def rule_snapshot(db: Session, job_id: int):
    """Capture every rule and its lifecycle state when a run is requested."""
    job = not_found(db.get(Job, job_id), "Job")
    scope = [Rule.dataset_id.is_(None)]
    if job.dataset_ids:
        scope.append(Rule.dataset_id.in_(job.dataset_ids))
    rules = db.scalars(select(Rule).where(Rule.job_id == job_id, or_(*scope)).order_by(Rule.id)).all()
    return [{"id": r.id, "job_id": r.job_id, "dataset_id": r.dataset_id,
             "column_name": r.column_name, "rule_type": r.rule_type,
             "parameters": r.parameters or {}, "enabled": bool(r.enabled),
             "approved": bool(r.approved), "created_at": r.created_at.isoformat() if r.created_at else ""} for r in rules]


@app.get("/api/v1/executions/{execution_id}")
async def execution_detail(execution_id: int, request: Request, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution, execution_id), "Execution")
    # A genuinely queued row may have outlived its process-local request task.
    # Status polling must stay read-only and lightweight; POST /execute and the
    # lifespan recovery path are responsible for dispatching work.
    if execution.status == "queued":
        await _enqueue_validation(request.app, execution.id, allow_inline=False)
    return execution


@app.get("/api/v1/executions/{execution_id}/failed")
def failed_records(execution_id: int, db: Session = Depends(get_db)):
    e=not_found(db.get(Execution, execution_id), "Execution")
    if e.status in {"queued", "running"}:
        raise HTTPException(409, f"Validation is still {e.status}; failed records are not available yet")
    if e.status == "failed" and not e.report_path:
        raise HTTPException(409, f"Validation failed before outputs were saved: {e.error or 'see execution logs'}")
    if not e.report_path:
        raise HTTPException(409, "Execution completed without a saved results report")
    path=Path(e.report_path).parent / "failed.json"
    def local_failed_rows():
        try:
            rows=json.loads(LocalStorage().read(str(path)))
        except FileNotFoundError:
            return None
        by_index={item["row_index"]:item for item in rows}
        for correction in db.scalars(select(Correction).where(Correction.execution_id==execution_id).order_by(Correction.id)):
            if correction.row_index in by_index:
                by_index[correction.row_index]["record"][correction.column_name]=correction.new_value
        return json_safe(rows)
    job=not_found(db.get(Job,e.job_id),"Job")
    destination=destination_for_job(job)
    logger.info("Loading failed records execution_id=%s job_id=%s destination_type=%s report_path=%s",
                execution_id, e.job_id, destination.get("type", "local"), e.report_path)
    if destination.get("type")=="databricks":
        report=json.loads(LocalStorage().read(e.report_path))
        result=report.get("destination_result") or {}
        if result.get("type")!="databricks" or not result.get("failed_table"):
            local_rows=local_failed_rows()
            if local_rows is not None:
                return local_rows
            raise HTTPException(409,"Execution has no saved Databricks FAIL table or local failed-record output")
        destination={**destination,"catalog":result.get("catalog") or destination.get("catalog"),
            "schema":result.get("schema") or destination.get("schema")}
        try:
            destination,secrets=resolve_destination_credentials(destination, db)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
        try:
            remote=read_databricks_table(destination,secrets,result["failed_table"])
        except Exception as exc:
            raise HTTPException(502,"Could not load failed records from the Databricks execution table") from exc
        output=[]
        for row in remote:
            index=row.get("_dq_row_index")
            if index is None: raise HTTPException(502,"Databricks FAIL table is missing original row indexes")
            reasons=row.get("_dq_failure_reasons")
            try: reasons=json.loads(reasons) if isinstance(reasons,str) else reasons
            except (TypeError,ValueError): reasons=[]
            record={k:v for k,v in row.items() if not k.startswith("_dq_")}
            output.append({"row_index":int(index),"record":record,"failure_reasons":reasons or []})
        by_index={item["row_index"]:item for item in output}
        for correction in db.scalars(select(Correction).where(Correction.execution_id==execution_id).order_by(Correction.id)):
            if correction.row_index in by_index:
                by_index[correction.row_index]["record"][correction.column_name]=correction.new_value
        return json_safe(output)
    output=local_failed_rows()
    if output is None:
        raise HTTPException(404, "Failed records output not found")
    return output


def _export_rule_fields(rule):
    params = rule.get("parameters") or {}
    expression = json.dumps({k: v for k, v in params.items() if k not in {"severity", "rationale"}}, ensure_ascii=False, sort_keys=True)
    return {
        "Rule ID": rule.get("id"),
        "Rule name": f"{rule.get('rule_type', 'DQ')} · {rule.get('column_name', '')}",
        "Rule type": rule.get("rule_type"),
        "Description": params.get("description", params.get("rationale", "")),
        "Column name": rule.get("column_name"),
        "Rule expression / condition": expression,
        "AI-generated reason / explanation": params.get("rationale", ""),
        "AI-generated timestamp": rule.get("created_at", ""),
        "Approval status": "Approved" if rule.get("approved") else "Proposed / pending approval",
        "Enabled status": "Enabled" if rule.get("enabled") else "Disabled",
        "Job ID": rule.get("job_id"),
        "Execution ID": rule.get("execution_id"),
    }


def _rule_for_failure(reason, rules):
    text = str(reason).casefold()
    for rule in rules:
        col = str(rule.get("column_name") or "").casefold()
        kind = str(rule.get("rule_type") or "").casefold()
        cues = {"not_null": ("missing required", "field is empty"),
                "email_format": ("missing required", "field is empty", "not a valid email address"),
                "unique": ("duplicates value",),
                "duplicates": ("duplicates value",), "range": ("below the allowed", "above the allowed", "must be numeric for the configured range"),
                "data_type": ("cannot be parsed as",),
                "regex": ("does not match required pattern",), "length": ("shorter than", "longer than"),
                "in_set": ("not in the allowed values",)}
        if col and col in text and (kind in text or any(cue in text for cue in cues.get(kind, ()))):
            return rule
    return {}


def _stored_rule_snapshot(db, execution, job):
    """Return the execution's frozen rules, or persisted job rules for legacy runs."""
    if execution.rule_snapshot is not None:
        return execution.rule_snapshot
    scope = [Rule.dataset_id.is_(None)]
    if job.dataset_ids:
        scope.append(Rule.dataset_id.in_(job.dataset_ids))
    return [{"id": rule.id, "job_id": rule.job_id, "dataset_id": rule.dataset_id,
             "column_name": rule.column_name, "rule_type": rule.rule_type,
             "parameters": rule.parameters or {}, "enabled": bool(rule.enabled),
             "approved": bool(rule.approved),
             "created_at": rule.created_at.isoformat() if rule.created_at else ""}
            for rule in db.scalars(select(Rule).where(Rule.job_id == job.id, or_(*scope)).order_by(Rule.id))]


def _xlsx_download(rows, filename):
    from openpyxl import Workbook
    workbook = Workbook()
    sheet = workbook.active
    sheet.title = "Report"
    headers = list(rows[0]) if rows else []
    if headers:
        sheet.append(headers)
        for row in rows:
            sheet.append([json.dumps(row.get(key), ensure_ascii=False, default=str) if isinstance(row.get(key), (dict, list)) else row.get(key) for key in headers])
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})


@app.get("/api/v1/executions/{execution_id}/exports/failed.xlsx")
def export_failed_records(execution_id: int, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution, execution_id), "Execution")
    job = not_found(db.get(Job, execution.job_id), "Job")
    failed = failed_records(execution_id, db)
    snapshot = _stored_rule_snapshot(db, execution, job)
    rows = []
    for item in failed:
        record = item.get("record") or {}
        details = item.get("failure_details") if isinstance(item.get("failure_details"), list) else []
        entries = details or [{"reason": reason, **_rule_for_failure(reason, snapshot)}
                              for reason in (item.get("failure_reasons") or ["Quality validation failed"])]
        for entry in entries:
            reason = entry.get("reason") or "Quality validation failed"
            rule_id = entry.get("rule_id")
            rule = next((rule for rule in snapshot if rule.get("id") == rule_id), None)
            if rule is None:
                rule = next((rule for rule in snapshot
                             if rule.get("column_name") == entry.get("column_name")
                             and rule.get("rule_type") == entry.get("rule_type")), None)
            if rule is None:
                rule = _rule_for_failure(reason, snapshot)
            column = entry.get("column_name") or rule.get("column_name", "")
            row = {"Original row number": (item.get("row_index", 0) + 1), "Record ID": record.get("id", record.get("record_id", ""))}
            row.update(record)
            row.update({"DQ rule name": f"{rule.get('rule_type', 'Unmatched failure')} · {rule.get('column_name', '')}".strip(" ·"),
                        "DQ rule type": rule.get("rule_type", ""), "Column name": rule.get("column_name", ""),
                        "Expected condition / expression": json.dumps(rule.get("parameters") or {}, ensure_ascii=False, sort_keys=True),
                        "Actual value": entry.get("actual_value", record.get(column)),
                        "Failure reason": reason, "Job name": job.name, "Job ID": job.id,
                        "Execution ID": execution.id, "Validation timestamp": execution.finished_at or execution.updated_at,
                        "Failure status": "Failed"})
            rows.append(row)
    if not rows:
        raise HTTPException(404, "This execution has no failed records to export")
    return _xlsx_download(rows, f"ADQ_Failed_Records_Job_{job.id}_Execution_{execution.id}.xlsx")


@app.get("/api/v1/executions/{execution_id}/exports/corrections.xlsx")
def export_corrections(execution_id: int, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution, execution_id), "Execution")
    job = not_found(db.get(Job, execution.job_id), "Job")
    artifact_execution = _correction_artifact_execution(db, execution)
    corrections = [c for c in _lineage_corrections(db, execution) if (c.status or "applied") == "applied" and c.approved]
    if not corrections:
        raise HTTPException(404, "No applied corrections are available to export")
    source_failed = _load_failed_rows_for_execution(artifact_execution)
    source_by_index = {item.get("row_index"): item for item in source_failed}
    suggestions = {f"{item.get('row_index')}:{item.get('column_name')}": item for item in _load_suggestion_cache(artifact_execution)}
    revalidated_execution_id = next((c.revalidation_execution_id for c in reversed(corrections) if c.revalidation_execution_id), None)
    revalidated_execution = db.get(Execution, revalidated_execution_id) if revalidated_execution_id else (execution if execution.retry_of else None)
    remaining_failed = _load_failed_rows_for_execution(revalidated_execution) if revalidated_execution else _load_failed_rows_for_execution(execution)

    def correction_row(correction: Correction):
        source_item = source_by_index.get(correction.row_index, {})
        suggestion = suggestions.get(f"{correction.row_index}:{correction.column_name}", {})
        return {"Record ID": correction.record_id or _record_identifier(source_item.get("record"), correction.row_index),
                "Original Row Number": correction.row_index + 1,
                "Column Name": correction.column_name,
                "Original Value": correction.old_value,
                "Corrected Value": correction.new_value,
                "Correction Type": correction.source or "Manual",
                "AI Confidence": correction.confidence or (suggestion.get("confidence") if str(correction.source or "").casefold() == "ai" else ""),
                "Verification Status": correction.verification_status or (suggestion.get("evidence_status") if str(correction.source or "").casefold() == "ai" else ""),
                "Approval Status": "Approved" if correction.approved else "Not approved",
                "Correction Status": correction.status or "applied",
                "Revalidation Status": correction.revalidation_status or "",
                "Execution ID": correction.execution_id}

    all_rows = [correction_row(correction) for correction in corrections]
    ai_rows = [row for row in all_rows if str(row["Correction Type"]).casefold() == "ai"]
    manual_rows = [row for row in all_rows if str(row["Correction Type"]).casefold() == "manual"]
    remaining_rows = []
    for item in remaining_failed:
        record = item.get("record") or {}
        tasks = _failed_field_tasks(item)
        if not tasks:
            tasks = [{"column": "", "current_value": "", "reason": reason} for reason in item.get("failure_reasons", []) or ["Quality validation failed"]]
        for task in tasks:
            column = task.get("column") or ""
            remaining_rows.append({"Record ID": _record_identifier(record, item.get("row_index")),
                                   "Row Number": (item.get("row_index", 0) + 1),
                                   "Column": column,
                                   "Actual Value": task.get("current_value", record.get(column)),
                                   "Failed Rule": task.get("rule_type") or "",
                                   "Failure Reason": task.get("reason") or "Quality validation failed",
                                   "Revalidation Execution ID": revalidated_execution.id if revalidated_execution else execution.id})
    from openpyxl import Workbook
    workbook = Workbook()
    sheet_specs = [("All Corrections", all_rows), ("AI Corrections", ai_rows),
                   ("Manual Corrections", manual_rows), ("Remaining Failures", remaining_rows)]
    for index, (title, rows) in enumerate(sheet_specs):
        sheet = workbook.active if index == 0 else workbook.create_sheet()
        sheet.title = title
        headers = list(rows[0]) if rows else {
            "All Corrections": ["Record ID","Original Row Number","Column Name","Original Value","Corrected Value","Correction Type","AI Confidence","Verification Status","Approval Status","Correction Status","Revalidation Status","Execution ID"],
            "AI Corrections": ["Record ID","Original Row Number","Column Name","Original Value","Corrected Value","Correction Type","AI Confidence","Verification Status","Approval Status","Correction Status","Revalidation Status","Execution ID"],
            "Manual Corrections": ["Record ID","Original Row Number","Column Name","Original Value","Corrected Value","Correction Type","AI Confidence","Verification Status","Approval Status","Correction Status","Revalidation Status","Execution ID"],
            "Remaining Failures": ["Record ID","Row Number","Column","Actual Value","Failed Rule","Failure Reason","Revalidation Execution ID"],
        }[title]
        sheet.append(headers)
        for row in rows:
            sheet.append([json.dumps(row.get(header), ensure_ascii=False, default=str) if isinstance(row.get(header), (dict, list)) else row.get(header) for header in headers])
        sheet.freeze_panes = "A2"
        sheet.auto_filter.ref = sheet.dimensions
    output = BytesIO()
    workbook.save(output)
    output.seek(0)
    return StreamingResponse(output, media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                             headers={"Content-Disposition": f'attachment; filename="ADQ_Corrections_Job_{job.id}_Execution_{execution.id}.xlsx"'})


@app.get("/api/v1/executions/{execution_id}/exports/rules.{file_format}")
def export_generated_rules(execution_id: int, file_format: str, db: Session = Depends(get_db)):
    if file_format not in {"xlsx", "csv"}:
        raise HTTPException(404, "Use xlsx or csv for generated rule exports")
    execution = not_found(db.get(Execution, execution_id), "Execution")
    job = not_found(db.get(Job, execution.job_id), "Job")
    snapshot = _stored_rule_snapshot(db, execution, job)
    rows = [_export_rule_fields({**rule, "execution_id": execution.id}) for rule in snapshot]
    if not rows:
        raise HTTPException(404, "No generated rules are stored for this execution")
    filename = f"ADQ_Generated_DQ_Rules_Job_{job.id}_Execution_{execution.id}.{file_format}"
    if file_format == "xlsx":
        if not rows:
            raise HTTPException(404, "No generated rules are stored for this execution")
        return _xlsx_download(rows, filename)
    output = StringIO()
    fields = list(rows[0]) if rows else list(_export_rule_fields({"execution_id": execution.id, "job_id": job.id}).keys())
    writer = csv.DictWriter(output, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    output.seek(0)
    return StreamingResponse(output, media_type="text/csv; charset=utf-8",
                             headers={"Content-Disposition": f'attachment; filename="{filename}"'})
async def _openai_correction_suggestion(*, record, column, error, column_metadata, approved_rules, candidates, matching_records, semaphore=None):
    """Ask OpenAI for a contextual correction, then validate it against local rules."""
    fallback = {"suggested_value": None, "explanation": "There is not enough evidence for a correction suggestion. Please correct this value manually.",
                "confidence": "low", "reason": "No safe, unambiguous value was established from approved rules or matching source records.", "safe_to_apply": False, "verification_status":"Unresolved", "generation_status":"completed"}
    if not settings.resolved_ai_api_key or not settings.resolved_ai_model:
        return fallback
    evidence = {"task": "Suggest one plausible correction for the failed field using the complete record context, column metadata, validation error, applicable approved rules, and matching source records. Return the result as a JSON object. Prefer exact values from matching records or approved rule values. If those do not apply, you may generate a contextual candidate (for example, derive a possible email from this record's person name); generated personal contact details are UNVERIFIED and must never be presented as real or verified. Never invent a person's actual age or salary; without reliable supporting evidence return null. Suggest a missing department only if a matching source record or approved rule supports it. Do not invent facts unsupported by the row. Return null only when no useful candidate can be made. A generated candidate always requires explicit user confirmation.",
        "failed_record": record, "column_names": list(record.keys()), "column": column, "column_type": column_metadata, "validation_error": error,
        "approved_rules": approved_rules, "allowed_candidates": candidates, "matching_source_records": matching_records,
        "response_schema": {"suggested_value": "scalar or null", "explanation": "brief explanation", "confidence": "high|medium|low", "reason": "evidence used"}}
    try:
        timeout = max(5, min(settings.ai_timeout_seconds, 60))
        async def request():
            for attempt in range(4):
                try:
                    return await _openai_json_chat(
                        [{"role":"user", "content":json.dumps(evidence, ensure_ascii=False)}],
                        timeout=timeout,
                    )
                except APIStatusError as exc:
                    if exc.status_code not in {429, 500, 502, 503, 504} or attempt == 3:
                        raise
                    headers = getattr(exc.response, "headers", {}) or {}
                    retry_after = headers.get("retry-after")
                    if retry_after is not None:
                        try: delay = float(retry_after)
                        except (TypeError, ValueError): delay = 0.5 * (2 ** attempt)
                    else:
                        body = _safe_provider_error(exc.response, settings.resolved_ai_api_key)
                        match = re.search(r"try again in ([0-9.]+)\s*(ms|s)", body, re.IGNORECASE)
                        delay = (float(match.group(1)) / (1000 if match.group(2).lower() == "ms" else 1)) if match else 0.5 * (2 ** attempt)
                except APIConnectionError:
                    if attempt == 3:
                        raise
                    delay = 0.5 * (2 ** attempt)
                await asyncio.sleep(min(60, max(0.1, delay)))
        if semaphore is None:
            text = await request()
        else:
            async with semaphore:
                text = await request()
        content=_parse_openai_json(text)
        if not isinstance(content, dict):
            raise ValueError("OpenAI returned an invalid correction suggestion schema")
        value=content.get("suggested_value")
        if value is not None and not isinstance(value, (str, int, float, bool)):
            raise ValueError("OpenAI suggested_value must be a scalar or null")
        if value is None or not str(value).strip():
            return {**fallback, "explanation":str(content.get("explanation") or fallback["explanation"])[:500],
                    "reason":str(content.get("reason") or fallback["reason"])[:500]}
        value = str(value).strip()
        allowed={str(item["value"]): item for item in candidates}
        matching_evidence = value in allowed and allowed[value].get("source") in {
            "approved_rule_or_matching_record", "matching_source_record", "approved_rule"
        }
        relevant_rules = [rule for rule in approved_rules if str(rule.get("type", "")).upper() in RULE_TYPES]
        if (column.casefold() == "age" or "salary" in column.casefold()) and not matching_evidence:
            try:
                if float(record.get(column)) < 0:
                    return {**fallback, "reason": f"The original {column} is negative, but no reliable source value supports a replacement. Manual review is required."}
            except (TypeError, ValueError):
                pass
        for rule in relevant_rules:
            kind, params = str(rule.get("type", "")).upper(), rule.get("parameters") or {}
            if kind == "NOT_NULL" and not value: return fallback
            if kind == "EMAIL_FORMAT" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value): return fallback
            if kind == "IN_SET" and value not in {str(item) for item in params.get("values", [])}: return fallback
            if kind == "RANGE":
                try:
                    number = float(value)
                    if params.get("min") is not None and number < float(params["min"]): return fallback
                    if params.get("max") is not None and number > float(params["max"]): return fallback
                except (TypeError, ValueError): return fallback
            if kind == "MIN_VALUE":
                try:
                    if float(value) < float(params.get("min", params.get("value"))): return fallback
                except (TypeError, ValueError): return fallback
            if kind == "MAX_VALUE":
                try:
                    if float(value) > float(params.get("max", params.get("value"))): return fallback
                except (TypeError, ValueError): return fallback
            if kind == "DATA_TYPE" and str(params.get("type", "string")).lower() in {"int", "integer", "float", "number", "decimal"} and not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", value): return fallback
        confidence=str(content.get("confidence", "medium")).lower()
        if confidence not in {"high", "medium", "low"}:
            try: confidence = "high" if float(confidence) >= .85 else "medium" if float(confidence) >= .6 else "low"
            except (TypeError, ValueError): confidence = "medium"
        if confidence not in {"high", "medium", "low"}: confidence="low"
        source = allowed.get(value, {}).get("source")
        generated_email = column.casefold() == "email" and not matching_evidence
        generated_email_reason = "Repaired from the original email-like value; address is unverified." if source == "original_value_repair_unverified" else "Generated from this record's name; address is unverified."
        return {"suggested_value":value, "explanation":str(content.get("explanation") or "Contextual suggestion from the failed record.")[:500],
                "confidence":"low" if generated_email else confidence,
                "reason":(generated_email_reason if generated_email else str(content.get("reason") or "Contextual candidate; review before applying.")[:500]),
                "safe_to_apply":matching_evidence, "verification_status":"UNVERIFIED" if generated_email or not matching_evidence else "Evidence-backed", "generation_status":"completed"}
    except APITimeoutError:
        logging.getLogger(__name__).warning("OpenAI correction suggestion timed out")
        return {**fallback, "reason": "OpenAI timed out while evaluating the verified evidence.", "generation_status":"failed"}
    except APIStatusError as exc:
        status = exc.status_code
        request_id = getattr(exc, "request_id", None) or exc.response.headers.get("x-request-id")
        provider_detail = _safe_provider_error(exc.response, settings.resolved_ai_api_key)
        detail = {429: "OpenAI rate limit reached", 401: "OpenAI API key was rejected", 403: "OpenAI model access was denied", 404: "OpenAI model was not found or is unavailable"}.get(status, f"OpenAI returned HTTP {status}")
        logging.getLogger(__name__).warning("OpenAI correction request failed (HTTP %s model=%s request_id=%s detail=%s)", status, settings.resolved_ai_model, request_id or "unavailable", provider_detail)
        return {**fallback, "reason": f"{detail}: {provider_detail}", "generation_status":"failed"}
    except (APIConnectionError, APIError, ValueError, KeyError, IndexError, TypeError) as exc:
        logging.getLogger(__name__).warning("OpenAI correction suggestion unavailable (%s)", type(exc).__name__)
        return {**fallback, "reason": f"OpenAI returned an unavailable or invalid response ({type(exc).__name__}).", "generation_status":"failed"}


def _correction_fallback(reason: str | None = None, *, generation_status: str = "completed"):
    return {"suggested_value": None,
            "explanation": "There is not enough evidence for a correction suggestion. Please correct this value manually.",
            "confidence": "low",
            "reason": reason or "No safe, unambiguous value was established from approved rules or matching source records.",
            "safe_to_apply": False, "verification_status": "Unresolved", "generation_status": generation_status}


def _candidate_suggestion(entry):
    """Return an evidence-bounded correction without calling a model when possible."""
    fallback = _correction_fallback()
    candidates = entry.get("candidates") or []
    if not candidates:
        return fallback
    by_value = {}
    for candidate in candidates:
        value = candidate.get("value") if isinstance(candidate, dict) else None
        if value is None or not str(value).strip():
            continue
        by_value.setdefault(str(value).strip(), candidate)
    verified_sources = {"approved_rule_or_matching_record", "matching_source_record", "approved_rule"}
    verified = [value for value, candidate in by_value.items() if candidate.get("source") in verified_sources]
    if len(verified) == 1:
        return {"suggested_value": verified[0],
                "explanation": "Suggested from approved rule evidence or a matching source record.",
                "confidence": "high",
                "reason": "Exactly one verified candidate is available for this failed field.",
                "safe_to_apply": True,
                "verification_status": "Evidence-backed",
                "generation_status": "completed"}
    unverified_email = [value for value, candidate in by_value.items()
                        if candidate.get("source") in {"original_value_repair_unverified", "name_pattern_unverified"}]
    if len(unverified_email) == 1:
        source = by_value[unverified_email[0]].get("source")
        repaired = source == "original_value_repair_unverified"
        return {"suggested_value": unverified_email[0],
                "explanation": "Suggested as an unverified email-format repair." if repaired
                               else "Suggested from record context and requires manual confirmation.",
                "confidence": "low",
                "reason": "Repaired from the original email-like value; address is unverified." if repaired
                          else "Generated from this record's context; address is unverified.",
                "safe_to_apply": False,
                "verification_status": "UNVERIFIED",
                "generation_status": "completed"}
    if verified:
        field = entry.get("column") or "field"
        return _correction_fallback(
            f"Multiple verified {field} candidates are available, so manual review is required.")
    return fallback


def _validate_ai_correction(content, *, fallback, record, column, approved_rules, candidates):
    if not isinstance(content, dict):
        raise ValueError("OpenAI returned an invalid correction suggestion schema")
    value = content.get("suggested_value")
    if value is not None and not isinstance(value, (str, int, float, bool)):
        raise ValueError("OpenAI suggested_value must be a scalar or null")
    if value is None or not str(value).strip():
        return {**fallback, "explanation": str(content.get("explanation") or fallback["explanation"])[:500],
                "reason": str(content.get("reason") or fallback["reason"])[:500]}
    value = str(value).strip()
    allowed = {str(item["value"]): item for item in candidates}
    if value not in allowed:
        return fallback
    matching_evidence = allowed[value].get("source") in {
        "approved_rule_or_matching_record", "matching_source_record", "approved_rule"
    }
    relevant_rules = [rule for rule in approved_rules if str(rule.get("type", "")).upper() in RULE_TYPES]
    for rule in relevant_rules:
        kind, params = str(rule.get("type", "")).upper(), rule.get("parameters") or {}
        if kind == "NOT_NULL" and not value: return fallback
        if kind == "EMAIL_FORMAT" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", value): return fallback
        if kind == "IN_SET" and value not in {str(item) for item in params.get("values", [])}: return fallback
        if kind == "RANGE":
            try:
                number = float(value)
                if params.get("min") is not None and number < float(params["min"]): return fallback
                if params.get("max") is not None and number > float(params["max"]): return fallback
            except (TypeError, ValueError): return fallback
        if kind == "MIN_VALUE":
            try:
                if float(value) < float(params.get("min", params.get("value"))): return fallback
            except (TypeError, ValueError): return fallback
        if kind == "MAX_VALUE":
            try:
                if float(value) > float(params.get("max", params.get("value"))): return fallback
            except (TypeError, ValueError): return fallback
        if kind == "DATA_TYPE" and str(params.get("type", "string")).lower() in {"int", "integer", "float", "number", "decimal"} and not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", value):
            return fallback
    confidence = str(content.get("confidence", "medium")).lower()
    if confidence not in {"high", "medium", "low"}:
        try: confidence = "high" if float(confidence) >= .85 else "medium" if float(confidence) >= .6 else "low"
        except (TypeError, ValueError): confidence = "medium"
    if confidence not in {"high", "medium", "low"}:
        confidence = "low"
    source = allowed[value].get("source")
    generated_email = source in {"original_value_repair_unverified", "name_pattern_unverified"}
    generated_email_reason = "Repaired from the original email-like value; address is unverified." if source == "original_value_repair_unverified" else "Generated from this record's context; address is unverified."
    return {"suggested_value": value,
            "explanation": str(content.get("explanation") or "Contextual suggestion from available evidence.")[:500],
            "confidence": "low" if generated_email else confidence,
            "reason": generated_email_reason if generated_email else str(content.get("reason") or "Candidate selected from approved evidence.")[:500],
            "safe_to_apply": matching_evidence,
            "verification_status": "UNVERIFIED" if generated_email or not matching_evidence else "Evidence-backed",
            "generation_status": "completed"}


async def _openai_correction_suggestions_batch(entries, semaphore=None):
    fallback_by_key = {entry["key"]: _correction_fallback() for entry in entries}
    if not entries:
        return {}
    if not settings.resolved_ai_api_key or not settings.resolved_ai_model:
        return {entry["key"]: _candidate_suggestion(entry) for entry in entries}
    results = {entry["key"]: _candidate_suggestion(entry) for entry in entries}
    actionable = [entry for entry in entries
                  if entry.get("candidates") and results[entry["key"]].get("suggested_value") is None
                  and any(candidate.get("source") == "matching_source_record"
                          for candidate in entry.get("candidates") or [])]
    if not actionable:
        return {**fallback_by_key, **results}
    payload = {"task": "Suggest one correction for each failed field. Use only allowed_candidates or return null. Do not invent unsupported personal, numeric, salary, age, name, or contact values. Generated/unverified candidates must remain unsafe. Return JSON only.",
        "items": [{"key": entry["key"], "row_index": entry["row_index"], "failed_column": entry["column"],
                   "failed_value": entry["current_value"], "validation_error": entry["reason"],
                   "column_type": entry["metadata"], "approved_rules": entry["approved_rules"],
                   "allowed_candidates": entry["candidates"][:12],
                   "matching_source_values": [{entry["column"]: row.get(entry["column"])} for row in entry.get("matching", [])[:3]]}
                  for entry in actionable],
        "response_schema": {"suggestions": [{"key": "string", "suggested_value": "scalar or null",
            "explanation": "brief explanation", "confidence": "high|medium|low", "reason": "evidence used"}]}}
    timeout = max(5, min(settings.ai_timeout_seconds, 90))
    try:
        async def request():
            for attempt in range(4):
                try:
                    return await _openai_json_chat(
                        [{"role":"user", "content":json.dumps(payload, ensure_ascii=False)}],
                        timeout=timeout,
                    )
                except APIStatusError as exc:
                    if exc.status_code not in {429, 500, 502, 503, 504} or attempt == 3:
                        raise
                    headers = getattr(exc.response, "headers", {}) or {}
                    retry_after = headers.get("retry-after")
                    if retry_after is not None:
                        try: delay = float(retry_after)
                        except (TypeError, ValueError): delay = 0.5 * (2 ** attempt)
                    else:
                        body = _safe_provider_error(exc.response, settings.resolved_ai_api_key)
                        match = re.search(r"try again in ([0-9.]+)\s*(ms|s)", body, re.IGNORECASE)
                        delay = (float(match.group(1)) / (1000 if match.group(2).lower() == "ms" else 1)) if match else 0.5 * (2 ** attempt)
                except APIConnectionError:
                    if attempt == 3:
                        raise
                    delay = 0.5 * (2 ** attempt)
                await asyncio.sleep(min(60, max(0.1, delay)))
        if semaphore is None:
            text = await request()
        else:
            async with semaphore:
                text = await request()
        content = _parse_openai_json(text)
        suggestions = content.get("suggestions") if isinstance(content, dict) else None
        if not isinstance(suggestions, list):
            raise ValueError("OpenAI returned an invalid batch correction schema")
        by_key = {entry["key"]: entry for entry in actionable}
        for item in suggestions:
            if not isinstance(item, dict) or item.get("key") not in by_key:
                continue
            entry = by_key[item["key"]]
            results[entry["key"]] = _validate_ai_correction(item, fallback=fallback_by_key[entry["key"]],
                record=entry["record"], column=entry["column"], approved_rules=entry["approved_rules"],
                candidates=entry["candidates"])
        for entry in actionable:
            results.setdefault(entry["key"], fallback_by_key[entry["key"]])
        return {**fallback_by_key, **results}
    except APITimeoutError:
        logging.getLogger(__name__).warning("OpenAI batch correction suggestion timed out for %s fields", len(entries))
        return {**results, **{entry["key"]: _correction_fallback("OpenAI timed out while evaluating the verified evidence.", generation_status="failed") for entry in actionable}}
    except APIStatusError as exc:
        status = exc.status_code
        provider_detail = _safe_provider_error(exc.response, settings.resolved_ai_api_key)
        detail = {429: "OpenAI rate limit reached", 401: "OpenAI API key was rejected", 403: "OpenAI model access was denied", 404: "OpenAI model was not found or is unavailable"}.get(status, f"OpenAI returned HTTP {status}")
        logging.getLogger(__name__).warning("OpenAI batch correction request failed (HTTP %s model=%s detail=%s)", status, settings.resolved_ai_model, provider_detail)
        return {**results, **{entry["key"]: _correction_fallback(f"{detail}: {provider_detail}", generation_status="failed") for entry in actionable}}
    except (APIConnectionError, APIError, ValueError, KeyError, IndexError, TypeError) as exc:
        logging.getLogger(__name__).warning("OpenAI batch correction suggestion unavailable (%s)", type(exc).__name__)
        return {**results, **{entry["key"]: _correction_fallback(f"OpenAI returned an unavailable or invalid response ({type(exc).__name__}).", generation_status="failed") for entry in actionable}}


def _name_based_email_draft(record):
    name_key = next((key for key in record if key.casefold() == "name"), None)
    name = str(record.get(name_key) or "").strip() if name_key else ""
    folded = "".join(character for character in unicodedata.normalize("NFKD", name.casefold())
                      if not unicodedata.combining(character))
    normalized = re.sub(r"[^a-z0-9]+", ".", folded).strip(".")
    return f"{normalized}@gmail.com" if normalized else None


def _email_repair_candidate(value):
    text = str(value or "").strip()
    if not text or "@" in text or re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", text):
        return None
    compact = re.sub(r"\s+", "", text)
    for domain in ("gmail.com", "yahoo.com", "outlook.com", "hotmail.com", "example.com"):
        if compact.casefold().endswith(domain) and len(compact) > len(domain):
            local = compact[:-len(domain)].strip(".-_")
            if local and re.fullmatch(r"[A-Za-z0-9._%+-]+", local):
                return f"{local}@{domain}"
    return None


def _failed_field_tasks(item):
    record = item.get("record") or {}
    grouped = {}
    details = item.get("failure_details") if isinstance(item.get("failure_details"), list) else []
    for detail in details:
        if not isinstance(detail, dict):
            continue
        column = detail.get("column_name")
        if not isinstance(column, str) or column not in record:
            continue
        entry = grouped.setdefault(column, {"column": column, "current_value": record.get(column), "reasons": []})
        reason = str(detail.get("reason") or "").strip()
        if reason and reason not in entry["reasons"]:
            entry["reasons"].append(reason)
        if "actual_value" in detail:
            entry["current_value"] = detail.get("actual_value")
    if grouped:
        return [{"column": item["column"], "current_value": item["current_value"],
                 "reason": "; ".join(item["reasons"]) or "Quality validation failed"}
                for item in grouped.values()]
    tasks = []
    seen = set()
    for reason in item.get("failure_reasons", []):
        column = next((c for c in record if re.search(rf"(?<!\w){re.escape(c)}(?!\w)", str(reason), re.IGNORECASE)), None)
        if column and column not in seen:
            tasks.append({"column": column, "current_value": record.get(column), "reason": str(reason)})
            seen.add(column)
    return tasks


def _record_identifier(record: dict | None, row_index: int | None = None):
    record = record or {}
    for key in record:
        normalized = str(key).strip().casefold().replace(" ", "_")
        if normalized == "id" or normalized == "record_id" or normalized.endswith("_id"):
            value = record.get(key)
            if value not in (None, ""):
                return str(value)
    return f"Row {(row_index or 0) + 1}" if row_index is not None else ""


def _read_json_artifact(path: Path, default):
    try:
        return json.loads(LocalStorage().read(str(path)))
    except (FileNotFoundError, TypeError, ValueError):
        return default


def _execution_lineage(db: Session, execution: Execution):
    lineage = []
    current = execution
    seen = set()
    while current is not None and current.id not in seen:
        lineage.append(current)
        seen.add(current.id)
        current = db.get(Execution, current.retry_of) if current.retry_of else None
    return lineage


def _lineage_corrections(db: Session, execution: Execution):
    ids = [item.id for item in _execution_lineage(db, execution)]
    if not ids:
        return []
    return list(db.scalars(select(Correction).where(Correction.execution_id.in_(ids)).order_by(Correction.created_at, Correction.id)))


def _correction_source_execution(db: Session, execution: Execution):
    for item in _execution_lineage(db, execution):
        if db.scalar(select(Correction.id).where(Correction.execution_id == item.id).limit(1)) is not None:
            return item
    return execution


def _correction_artifact_execution(db: Session, execution: Execution):
    for item in reversed(_execution_lineage(db, execution)):
        if item.report_path:
            return item
    return execution


def _load_failed_rows_for_execution(execution: Execution, *, revalidated: bool = False):
    if not execution.report_path:
        return []
    base = Path(execution.report_path).parent
    name = "revalidated_failed.json" if revalidated else "failed.json"
    return _read_json_artifact(base / name, [])


def _load_suggestion_cache(execution: Execution):
    if not execution.report_path:
        return []
    data = _read_json_artifact(Path(execution.report_path).parent / "correction-suggestions.json", {})
    if data.get("version") != CORRECTION_SUGGESTION_CACHE_VERSION:
        return []
    return data.get("suggestions", []) if isinstance(data, dict) and isinstance(data.get("suggestions"), list) else []


def _correction_payload(correction: Correction):
    return {"id": correction.id, "execution_id": correction.execution_id,
            "record_id": correction.record_id or f"Row {correction.row_index + 1}",
            "row_index": correction.row_index, "column_name": correction.column_name,
            "original_value": correction.old_value, "corrected_value": correction.new_value,
            "old_value": correction.old_value, "new_value": correction.new_value,
            "correction_type": correction.source or "Manual",
            "source": correction.source or "Manual",
            "ai_suggestion": correction.suggested_value,
            "ai_confidence": correction.confidence,
            "verification_status": correction.verification_status,
            "approval_status": "Approved" if correction.approved else "Not approved",
            "correction_status": correction.status or "applied",
            "revalidation_status": correction.revalidation_status,
            "revalidation_execution_id": correction.revalidation_execution_id}


@app.get("/api/v1/executions/{execution_id}/correction-suggestions")
async def correction_suggestions(execution_id: int, row_index: int | None = None, offset: int = 0, limit: int | None = None, retry_failed: bool = False, db: Session = Depends(get_db)):
    """Offer suggestions from validation evidence, including clearly unverified candidates."""
    execution = not_found(db.get(Execution, execution_id), "Execution")
    if not execution.report_path: raise HTTPException(409, "Execution has no saved outputs")
    job = not_found(db.get(Job, execution.job_id), "Job")
    destination = destination_for_job(job)
    logger.info("Generating correction suggestions execution_id=%s job_id=%s destination_type=%s offset=%s limit=%s retry_failed=%s",
                execution_id, job.id, destination.get("type", "local"), offset, limit, retry_failed)
    artifact_dir = Path(execution.report_path).parent
    rows = json.loads(LocalStorage().read(str(Path(execution.report_path).parent / "failed.json")))
    by_index = {item["row_index"]: item for item in rows}
    for correction in db.scalars(select(Correction).where(Correction.execution_id == execution_id).order_by(Correction.id)):
        if correction.row_index in by_index:
            by_index[correction.row_index]["record"][correction.column_name] = correction.new_value
    rows = list(by_index.values())
    if row_index is not None:
        rows = [item for item in rows if item.get("row_index") == row_index]
    active_task_keys = {f"{item.get('row_index')}:{task.get('column')}"
                        for item in rows for task in _failed_field_tasks(item)}
    batch_mode = limit is not None
    limit = max(1, min(limit or settings.ai_suggestion_batch_size, 96))
    total_records = len(rows)
    rows = rows[max(0, offset):max(0, offset) + limit] if batch_mode else rows
    cache_path = artifact_dir / "correction-suggestions.json"
    try:
        cached = json.loads(LocalStorage().read(str(cache_path))) if cache_path.exists() else {}
    except (OSError, ValueError):
        cached = {}
    if cached.get("version") != CORRECTION_SUGGESTION_CACHE_VERSION:
        cached = {}
    cached_items = {item.get("key"): item for item in cached.get("suggestions", [])
                    if item.get("key") and item.get("key") in active_task_keys}
    if execution.rule_snapshot is not None:
        from types import SimpleNamespace
        rules = [SimpleNamespace(**item) for item in execution.rule_snapshot]
    else:
        scope = [Rule.dataset_id.is_(None)]
        if job.dataset_ids: scope.append(Rule.dataset_id.in_(job.dataset_ids))
        rules = list(db.scalars(select(Rule).where(Rule.job_id == job.id, or_(*scope))))
    rules = [r for r in rules if r.enabled and r.approved]
    reference_rows = []
    column_metadata = {}
    # The execution's PASS output is available even when its source connector is
    # remote, and gives us source records without another external read.
    for name in ("original_passed.csv", "passed.csv"):
        try:
            reference_rows.extend(parse_data(name, LocalStorage().read(str(artifact_dir/name))))
            break
        except FileNotFoundError:
            continue
    for dataset_id in job.dataset_ids or []:
        dataset = db.get(Dataset, dataset_id)
        if dataset:
            column_metadata.update((dataset.schema_json or {}).get("columns", {}))
        if dataset and dataset.source_type in {"local", "upload"}:
            try: reference_rows.extend(parse_data(dataset.filename, LocalStorage().read(dataset.path)))
            except (OSError, ValueError): pass
    results = []
    pending = []
    semaphore = asyncio.Semaphore(max(1, min(settings.ai_suggestion_concurrency, 8)))
    for item in rows:
        record = item.get("record") or {}
        for failure in _failed_field_tasks(item):
            column, current_value, reason = failure["column"], failure["current_value"], failure["reason"]
            relevant = [r for r in rules if r.column_name.casefold() == column.casefold()]
            candidate_sources = {}
            has_email_rule = any(r.rule_type.upper() == "EMAIL_FORMAT" for r in relevant)
            name_draft = None
            email_repair = _email_repair_candidate(record.get(column)) if has_email_rule else None
            for rule in relevant:
                if rule.rule_type.upper() == "IN_SET":
                    for value in (rule.parameters or {}).get("values", []):
                        candidate_sources.setdefault(str(value), "approved_rule")
            # Match on every other populated field. Suggest only a single distinct
            # value from the actual source records; never synthesize personal data.
            for ref in reference_rows:
                shared = [k for k, v in record.items() if k != column and not is_missing(v) and not is_missing(ref.get(k))]
                if shared and all(str(ref[k]).strip().casefold() == str(record[k]).strip().casefold() for k in shared) and ref.get(column) not in (None, "") and str(ref[column]).strip().casefold() != str(record.get(column, "")).strip().casefold():
                    candidate_sources[str(ref[column])] = "matching_source_record"
            safe = []
            for candidate, source in candidate_sources.items():
                if any(r.rule_type.upper() in {"NOT_NULL", "EMAIL_FORMAT"} and not candidate.strip() for r in relevant): continue
                if any(r.rule_type.upper() == "EMAIL_FORMAT" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", candidate) for r in relevant): continue
                if any(r.rule_type.upper() == "IN_SET" and candidate not in {str(v) for v in (r.parameters or {}).get("values", [])} for r in relevant): continue
                try:
                    if any(r.rule_type.upper() == "RANGE" and ((r.parameters or {}).get("min") is not None and float(candidate) < float(r.parameters["min"]) or (r.parameters or {}).get("max") is not None and float(candidate) > float(r.parameters["max"])) for r in relevant): continue
                    if any(r.rule_type.upper() == "MIN_VALUE" and float(candidate) < float((r.parameters or {}).get("min", (r.parameters or {}).get("value"))) for r in relevant): continue
                    if any(r.rule_type.upper() == "MAX_VALUE" and float(candidate) > float((r.parameters or {}).get("max", (r.parameters or {}).get("value"))) for r in relevant): continue
                except (TypeError, ValueError):
                    continue
                if any(r.rule_type.upper() == "DATA_TYPE" and str((r.parameters or {}).get("type", "string")).lower() in {"int", "integer", "float", "number", "decimal"} and not re.fullmatch(r"[+-]?(?:\d+(?:\.\d*)?|\.\d+)", candidate.strip()) for r in relevant): continue
                safe.append((candidate, source))
            matched = []
            for ref in reference_rows:
                shared = [k for k, v in record.items() if k != column and not is_missing(v) and not is_missing(ref.get(k))]
                if shared and all(str(ref[k]).strip().casefold() == str(record[k]).strip().casefold() for k in shared) and ref.get(column) not in (None, "") and str(ref[column]).strip().casefold() != str(record.get(column, "")).strip().casefold():
                    matched.append(ref)
            # Candidates are sourced only from approved IN_SET rules and matching
            # records. Reject candidates that conflict with any applicable rule.
            allowed = [{"value": value, "source": source}
                       for value, source in sorted(dict(safe).items())]
            if email_repair and email_repair.casefold() != str(record.get(column) or "").strip().casefold():
                allowed.append({"value":email_repair, "source":"original_value_repair_unverified"})
            if name_draft and name_draft.casefold() != str(record.get(column) or "").strip().casefold():
                allowed.append({"value":name_draft, "source":"name_pattern_unverified"})
            pending.append((item["row_index"], column, current_value, reason, record, column_metadata.get(column, {}),
                            [{"type":r.rule_type,"parameters":r.parameters or {}} for r in relevant], allowed, matched[:5]))
    if batch_mode:
        keys = {f"{entry[0]}:{entry[1]}" for entry in pending}
        cached_for_batch = [item for key, item in cached_items.items() if key in keys and not (retry_failed and item.get("generation_status") == "failed")]
        results.extend(cached_for_batch)
        pending = [entry for entry in pending if f"{entry[0]}:{entry[1]}" not in cached_items or retry_failed and cached_items.get(f"{entry[0]}:{entry[1]}", {}).get("generation_status") == "failed"]
    started = time.perf_counter()
    request_count = 0
    if batch_mode:
        entries = [{"key": f"{row_index}:{column}", "row_index": row_index, "column": column,
                    "current_value": current_value, "reason": reason, "record": record,
                    "metadata": metadata, "approved_rules": approved_rules,
                    "candidates": candidates, "matching": matching}
                   for row_index, column, current_value, reason, record, metadata, approved_rules, candidates, matching in pending]
        batch_size = max(1, min(int(settings.ai_correction_request_batch_size), 48))
        chunks = [entries[index:index + batch_size] for index in range(0, len(entries), batch_size)]
        request_count = sum(1 for chunk in chunks if settings.resolved_ai_api_key and settings.resolved_ai_model and any(entry.get("candidates") for entry in chunk))
        batch_results = await asyncio.gather(*(_openai_correction_suggestions_batch(chunk, semaphore=semaphore) for chunk in chunks))
        suggestions_by_key = {key: value for batch in batch_results for key, value in batch.items()}
        suggestion_rows = [(entry["row_index"], entry["column"], entry["current_value"], entry["reason"],
                            suggestions_by_key.get(entry["key"], _correction_fallback())) for entry in entries]
    else:
        pending = [(row_index, column, current_value, reason,
                    _openai_correction_suggestion(record=record, column=column, error=reason, column_metadata=metadata,
                        approved_rules=approved_rules, candidates=candidates, matching_records=matching, semaphore=semaphore))
                   for row_index, column, current_value, reason, record, metadata, approved_rules, candidates, matching in pending]
        request_count = len(pending)
        suggestions = await asyncio.gather(*(entry[4] for entry in pending), return_exceptions=True)
        suggestion_rows = [(row_index, column, current_value, reason, suggestion)
                           for (row_index, column, current_value, reason, _), suggestion in zip(pending, suggestions)]
    request_seconds = time.perf_counter() - started
    for row_index, column, current_value, reason, suggestion in suggestion_rows:
        if isinstance(suggestion, BaseException):
            suggestion = {"suggested_value": None, "explanation": "Suggestion generation failed for this field.", "confidence":"low", "reason": "Temporary suggestion processing error.", "safe_to_apply":False, "verification_status":"Unresolved", "generation_status":"failed"}
        evidence_status = "Verified evidence" if suggestion["safe_to_apply"] else "Unverified candidate; user confirmation required" if suggestion.get("suggested_value") is not None else "Manual correction recommended"
        results.append({"key":f"{row_index}:{column}", "row_index": row_index, "column_name": column, "current_value": current_value,
            "validation_error": reason, **suggestion, "evidence_status": evidence_status,
            "fixable": suggestion["suggested_value"] is not None})
    if batch_mode:
        merged = dict(cached_items)
        for result in results:
            merged[result.get("key", f"{result['row_index']}:{result['column_name']}")] = result
        storage = LocalStorage()
        storage.save(str(cache_path.relative_to(storage.root)),
                     json.dumps({"version": CORRECTION_SUGGESTION_CACHE_VERSION,
                                 "suggestions": list(merged.values())}, ensure_ascii=False).encode("utf-8"))
        all_results = list(merged.values())
        return {"suggestions":results, "offset":max(0, offset), "limit":limit, "total_records":total_records,
                "processed_records":min(total_records, max(0, offset)+limit), "completed":max(0, offset)+limit >= total_records,
                "generated":sum(x.get("suggested_value") is not None for x in all_results),
                "failed":sum(x.get("generation_status")=="failed" for x in all_results),
                "ai_requests": request_count, "request_seconds": request_seconds,
                "average_request_seconds": request_seconds / request_count if request_count else 0}
    storage = LocalStorage()
    storage.save(str(cache_path.relative_to(storage.root)),
                 json.dumps({"version": CORRECTION_SUGGESTION_CACHE_VERSION,
                             "suggestions": results}, ensure_ascii=False).encode("utf-8"))
    return results


@app.get("/api/v1/executions/{execution_id}/correction-summary")
def correction_summary(execution_id: int, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution, execution_id), "Execution")
    artifact_execution = _correction_artifact_execution(db, execution)
    corrections = _lineage_corrections(db, execution)
    fixed = {(c.row_index, c.column_name) for c in corrections}
    revalidated_execution_id = next((c.revalidation_execution_id for c in reversed(corrections) if c.revalidation_execution_id), None)
    revalidated_execution = db.get(Execution, revalidated_execution_id) if revalidated_execution_id else (execution if execution.retry_of else None)
    active_execution = revalidated_execution or execution
    rows = _load_failed_rows_for_execution(artifact_execution)
    active_rows = _load_failed_rows_for_execution(active_execution) if active_execution else rows
    suggestions = _load_suggestion_cache(active_execution) if active_execution else []
    if not suggestions and active_execution and active_execution.id != artifact_execution.id:
        active_keys = {f"{item.get('row_index')}:{task.get('column')}"
                       for item in active_rows for task in _failed_field_tasks(item)}
        suggestions = [item for item in _load_suggestion_cache(artifact_execution)
                       if item.get("key") in active_keys]
    suggested_fields = {(item.get("row_index"), item.get("column_name")) for item in suggestions
                        if item.get("suggested_value") is not None}
    suggestion_rows = {row for row, _ in suggested_fields}
    suggested_row_indices = {item.get("row_index") for item in suggestions}
    manual_suggestion_rows = {item.get("row_index") for item in suggestions
                              if item.get("suggested_value") is None or item.get("generation_status") == "failed"}
    corrected_rows = {row for row, _ in fixed}
    ai_applied_rows = {c.row_index for c in corrections if str(c.source or "").casefold() == "ai" and (c.status or "applied") == "applied"}
    manual_applied_rows = {c.row_index for c in corrections if str(c.source or "").casefold() == "manual" and (c.status or "applied") == "applied"}
    remaining_row_indices = {item.get("row_index") for item in active_rows} if revalidated_execution else set()
    total_failed = len(rows)
    active_failed_row_indices = {item.get("row_index") for item in active_rows}
    manual_review_rows = manual_suggestion_rows | (active_failed_row_indices - suggested_row_indices)
    return {"total_failed_records": len(active_rows) if revalidated_execution else total_failed,
            "total_failed_records_before_correction": total_failed,
            "ai_suggestions_generated": len(suggestion_rows),
            "ai_corrections_applied": len(ai_applied_rows),
            "manual_corrections_applied": len(manual_applied_rows),
            "total_corrections_applied": len(corrected_rows),
            "fixed_successfully": len(corrected_rows),
            "manual_review_required": len(remaining_row_indices) if revalidated_execution else max(0, len(manual_review_rows)),
            "records_passed_after_revalidation": max(0, total_failed - len(remaining_row_indices)) if revalidated_execution else 0,
            "records_still_failing_after_revalidation": len(remaining_row_indices) if revalidated_execution else 0,
            "revalidated_execution_id": revalidated_execution.id if revalidated_execution else None,
            "correction_failed": 0,
            "corrections": [_correction_payload(c) for c in corrections]}


def _execution_report(execution: Execution):
    if not execution.report_path:
        return {}
    return _read_json_artifact(Path(execution.report_path).parent / "report.json", {})


def _report_count(report: dict, execution: Execution, keys: tuple[str, ...], fallback: int = 0):
    for key in keys:
        value = report.get(key) if isinstance(report, dict) else None
        if isinstance(value, (int, float)):
            return int(value)
    return int(fallback or 0)


def _percentage(part: int, total: int):
    return round((part / total) * 100, 1) if total else 0.0


def _latest_revalidated_execution(db: Session, execution: Execution, corrections: list[Correction]):
    for correction in reversed(corrections):
        if correction.revalidation_execution_id:
            revalidated = db.get(Execution, correction.revalidation_execution_id)
            if revalidated is not None:
                return revalidated
    return execution if execution.retry_of else None


def _destination_table_name(destination_result: dict, output_dataset: Dataset | None):
    if not isinstance(destination_result, dict):
        return output_dataset.name if output_dataset else None
    outputs = destination_result.get("outputs") if isinstance(destination_result.get("outputs"), dict) else {}
    return (
        destination_result.get("passed_table")
        or destination_result.get("failed_table")
        or destination_result.get("report_table")
        or outputs.get("passed")
        or outputs.get("failed")
        or (output_dataset.name if output_dataset else None)
    )


@app.get("/api/v1/executions/{execution_id}/final-summary")
def final_execution_summary(execution_id: int, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution, execution_id), "Execution")
    job = not_found(db.get(Job, execution.job_id), "Job")
    corrections = _lineage_corrections(db, execution)
    before_execution = _correction_artifact_execution(db, execution)
    final_execution = _latest_revalidated_execution(db, execution, corrections) or execution
    before_report = _execution_report(before_execution)
    final_report = _execution_report(final_execution)
    before_passed = _report_count(before_report, before_execution, ("passed",), before_execution.passed_count)
    before_failed = _report_count(before_report, before_execution, ("failed",), before_execution.failed_count)
    total_processed = _report_count(
        before_report, before_execution,
        ("records_evaluated", "total", "records_loaded"),
        before_passed + before_failed,
    )
    final_passed = _report_count(final_report, final_execution, ("passed",), final_execution.passed_count)
    final_failed = _report_count(final_report, final_execution, ("failed",), final_execution.failed_count)
    final_total = _report_count(
        final_report, final_execution,
        ("records_evaluated", "total", "records_loaded"),
        final_passed + final_failed,
    )
    revalidated = final_execution.id != before_execution.id or bool(final_execution.retry_of)
    successful_corrected = max(0, before_failed - final_failed) if revalidated else 0
    applied_corrections = {
        correction.row_index for correction in corrections
        if (correction.status or "applied") == "applied" and correction.approved
    }
    storage_status = final_report.get("storage_status") if isinstance(final_report.get("storage_status"), dict) else {}
    destination_result = final_report.get("destination_result") if isinstance(final_report.get("destination_result"), dict) else {}
    output_dataset = db.get(Dataset, final_report.get("output_dataset_id")) if final_report.get("output_dataset_id") else None
    destination = job.destination if isinstance(job.destination, dict) else {}
    provider = storage_status.get("destination_platform") or destination_result.get("type") or destination.get("type") or "local"
    destination_location = (
        storage_status.get("destination_location")
        or destination_result.get("destination_location")
        or (".".join(str(destination_result.get(key, "")) for key in ("catalog", "schema") if destination_result.get(key)) or None)
        or (".".join(str(destination.get(key, "")) for key in ("catalog", "schema", "database") if destination.get(key)) or None)
    )
    saved = storage_status.get("status") == "saved" or destination_result.get("status") == "saved"
    return {
        "execution_id": final_execution.id,
        "initial_execution_id": before_execution.id,
        "job_id": job.id,
        "job_name": job.name,
        "data_quality_summary": {
            "total_rows_processed": total_processed,
            "rows_passed_before_correction": before_passed,
            "rows_failed_before_correction": before_failed,
            "rows_successfully_fixed": successful_corrected,
            "rows_passed_after_correction": final_passed,
            "rows_still_failed_after_correction": final_failed,
        },
        "before_correction": {
            "passed_rows": before_passed,
            "failed_rows": before_failed,
            "pass_percentage": _percentage(before_passed, total_processed),
            "fail_percentage": _percentage(before_failed, total_processed),
        },
        "after_correction": {
            "passed_rows": final_passed,
            "failed_rows": final_failed,
            "successfully_corrected_rows": successful_corrected,
            "remaining_failed_rows": final_failed,
            "final_pass_percentage": _percentage(final_passed, final_total),
            "final_fail_percentage": _percentage(final_failed, final_total),
        },
        "overall": {
            "total_records_processed": final_total or total_processed,
            "total_records_passed": final_passed,
            "total_records_failed": final_failed,
            "total_records_corrected": len(applied_corrections),
            "overall_data_quality_percentage": _percentage(final_passed, final_total or total_processed),
            "remaining_issues": final_failed,
            "revalidated": revalidated,
        },
        "platform_save": {
            "selected_platform_name": job_environment(job) or normalize_platform(str(provider)) or "Local",
            "destination_details": destination_location,
            "dataset_or_table_name": _destination_table_name(destination_result, output_dataset),
            "records_saved": (final_passed + final_failed) if saved else None,
            "save_status": "Successfully Saved" if saved else "Failed",
            "status": storage_status.get("status") or destination_result.get("status") or "unknown",
            "last_saved_at": final_execution.finished_at or final_execution.updated_at,
            "error_message": storage_status.get("error_message") or destination_result.get("error"),
        },
    }


@app.post("/api/v1/executions/{execution_id}/corrections/bulk")
def correct_records_bulk(execution_id: int, body: dict, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution, execution_id), "Execution")
    if not execution.report_path: raise HTTPException(409, "Execution has no saved outputs")
    changes = body.get("corrections")
    if not isinstance(changes, list) or len(changes) > 1000: raise HTTPException(422, "Provide up to 1000 corrections")
    path = Path(execution.report_path).parent / "failed.json"
    rows = json.loads(LocalStorage().read(str(path)))
    by_index = {r["row_index"]: r for r in rows}
    result = {"succeeded": [], "failed": []}
    for change in changes:
        try:
            index, column, value = int(change["row_index"]), str(change["column_name"]), change["value"]
            unverified_approved = change.get("approve_unverified") is True
            if not (change.get("safe_to_apply") and str(change.get("confidence", "")).lower() == "high") and not unverified_approved:
                raise ValueError("Unverified suggestions require explicit approval before applying")
            item = by_index.get(index)
            if not item or column not in item["record"]: raise ValueError("Failed record or column no longer exists")
            if value is None or str(value) != str(change.get("suggested_value")): raise ValueError("Suggestion value mismatch")
            original = item["record"].get(column)
            rules = _stored_rule_snapshot(db, execution, db.get(Job, execution.job_id))
            applicable = [r for r in rules if r.get("enabled") and r.get("approved") and r.get("column_name", "").casefold() == column.casefold()]
            sval = str(value)
            for rule in applicable:
                params = rule.get("parameters") or {}; kind = str(rule.get("rule_type", "")).upper()
                if kind == "NOT_NULL" and not sval.strip(): raise ValueError("Value violates NOT_NULL")
                if kind == "EMAIL_FORMAT" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", sval): raise ValueError("Value violates EMAIL_FORMAT")
                if kind == "IN_SET" and sval not in {str(v) for v in params.get("values", [])}: raise ValueError("Value violates IN_SET")
                if kind == "RANGE" and (params.get("min") is not None and float(sval) < float(params["min"]) or params.get("max") is not None and float(sval) > float(params["max"])): raise ValueError("Value violates RANGE")
                if kind == "MIN_VALUE" and float(sval) < float(params.get("min", params.get("value"))): raise ValueError("Value violates MIN_VALUE")
                if kind == "MAX_VALUE" and float(sval) > float(params.get("max", params.get("value"))): raise ValueError("Value violates MAX_VALUE")
            if unverified_approved and column.casefold() == "email" and not re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+", sval):
                raise ValueError("Email candidate is not in a valid email format")
            record_id = _record_identifier(item.get("record"), index)
            latest = db.scalar(select(Correction).where(Correction.execution_id == execution_id,
                Correction.row_index == index, Correction.column_name == column).order_by(Correction.id.desc()))
            if latest and latest.new_value == sval:
                latest.record_id = latest.record_id or record_id
                latest.source = "AI"
                latest.suggested_value = sval
                latest.confidence = str(change.get("confidence") or "")
                latest.verification_status = str(change.get("verification_status") or change.get("evidence_status") or "")
                latest.approved = True
                latest.status = "applied"
                result["succeeded"].append({"row_index": index, "column_name": column, "already_applied": True})
                continue
            corr = Correction(execution_id=execution_id, row_index=index, column_name=column,
                              record_id=record_id, old_value=None if original is None else str(original), new_value=sval,
                              source="AI", suggested_value=sval, confidence=str(change.get("confidence") or ""),
                              verification_status=str(change.get("verification_status") or change.get("evidence_status") or ""),
                              approved=True, status="applied")
            db.add(corr)
            result["succeeded"].append({"row_index": index, "column_name": column})
        except Exception as exc:
            result["failed"].append({"row_index": change.get("row_index"), "column_name": change.get("column_name"), "reason": str(exc)})
    db.commit()
    return {**result, "success_count": len(result["succeeded"]), "failure_count": len(result["failed"])}


@app.get("/api/v1/executions/{execution_id}/artifacts/{artifact_name}")
def execution_artifact(execution_id: int, artifact_name: str, db: Session = Depends(get_db)):
    e=not_found(db.get(Execution, execution_id), "Execution")
    if not e.report_path: raise HTTPException(409, "Execution has no saved outputs")
    if artifact_name not in {"passed.csv", "failed.json", "report.json", "revalidated_passed.csv", "revalidated_failed.json"}:
        raise HTTPException(404,"Artifact not found")
    path=Path(e.report_path).parent / artifact_name
    try: content=LocalStorage().read(str(path))
    except FileNotFoundError: raise HTTPException(404,"Artifact not found")
    media="text/csv" if artifact_name.endswith(".csv") else "application/json"
    return JSONResponse(content=json.loads(content)) if artifact_name.endswith(".json") else __import__("fastapi").Response(content=content,media_type=media,headers={"Content-Disposition":f'attachment; filename="{artifact_name}"'})


@app.post("/api/v1/executions/{execution_id}/corrections")
def correct_record(execution_id: int, body: CorrectionCreate, db: Session = Depends(get_db)):
    e=not_found(db.get(Execution, execution_id), "Execution")
    if not e.report_path: raise HTTPException(409, "Execution has no saved outputs")
    path=Path(e.report_path).parent / "failed.json"
    rows=json.loads(LocalStorage().read(str(path)))
    item=next((r for r in rows if r["row_index"] == body.row_index), None)
    if item is None: raise HTTPException(404, "Failed row not found")
    if body.column_name not in item["record"]: raise HTTPException(422, "Column does not exist in failed record")
    value="" if body.value is None else str(body.value)
    job=db.get(Job,e.job_id)
    for rule in _stored_rule_snapshot(db,e,job):
        if not rule.get("enabled") or not rule.get("approved") or rule.get("column_name","").casefold()!=body.column_name.casefold(): continue
        params=rule.get("parameters") or {}; kind=str(rule.get("rule_type","")).upper()
        valid=True
        if kind=="NOT_NULL": valid=bool(value.strip())
        elif kind=="EMAIL_FORMAT": valid=bool(re.fullmatch(r"[^\s@]+@[^\s@]+\.[^\s@]+",value))
        elif kind=="IN_SET": valid=value in {str(v) for v in params.get("values",[])}
        elif kind=="REGEX":
            try: valid=bool(re.search(str(params.get("pattern", "")),value))
            except re.error: valid=False
        elif kind=="RANGE":
            try: valid=(params.get("min") is None or float(value)>=float(params["min"])) and (params.get("max") is None or float(value)<=float(params["max"]))
            except (TypeError,ValueError): valid=False
        elif kind=="MIN_VALUE":
            try: valid=float(value)>=float(params.get("min",params.get("value")))
            except (TypeError,ValueError): valid=False
        elif kind=="MAX_VALUE":
            try: valid=float(value)<=float(params.get("max",params.get("value")))
            except (TypeError,ValueError): valid=False
        elif kind=="DATA_TYPE":
            expected=str(params.get("type","string")).lower()
            if expected in {"int","integer"}:
                try: valid=float(value).is_integer()
                except (TypeError,ValueError,OverflowError): valid=False
            elif expected in {"float","number","decimal"}:
                try: float(value)
                except (TypeError,ValueError,OverflowError): valid=False
            elif expected in {"bool","boolean"}:
                valid=value.strip().lower() in {"true","false","0","1"}
            elif expected in {"date","datetime"}:
                try: datetime.fromisoformat(value.strip())
                except (TypeError,ValueError): valid=False
        if not valid: raise HTTPException(422,f"Corrected value does not satisfy approved {kind} rule")
    old=item["record"][body.column_name]
    corr=db.scalar(select(Correction).where(Correction.execution_id==execution_id, Correction.row_index==body.row_index, Correction.column_name==body.column_name).order_by(Correction.id.desc()))
    effective_old = corr.new_value if corr else old
    if corr and corr.new_value == body.value:
        current_record = dict(item["record"])
        current_record[body.column_name] = corr.new_value
        return {"correction_id":corr.id,"execution_id":execution_id,"row_index":body.row_index,"column_name":body.column_name,
                "old_value":corr.old_value,"new_value":corr.new_value,"record":json_safe(current_record),"already_applied":True}
    accepted_originals = {str(value if value is not None else "") for value in (old, effective_old)}
    if body.original_value is not None and body.original_value not in accepted_originals:
        raise HTTPException(409, "The record changed since it was loaded. Refresh failed records before saving this correction.")
    # Append each edit so the full correction history remains auditable.
    corr=Correction(execution_id=execution_id,row_index=body.row_index,column_name=body.column_name,
                    record_id=_record_identifier(item.get("record"), body.row_index),
                    old_value=None if effective_old is None else str(effective_old),new_value=body.value,
                    source="Manual", approved=True, status="applied")
    db.add(corr); db.commit(); db.refresh(corr)
    item["record"][body.column_name]=body.value
    LocalStorage().save(str(path.resolve().relative_to(Path(settings.local_data_dir).resolve())),
                        json.dumps(rows,ensure_ascii=False,indent=2).encode())
    return {"correction_id":corr.id,"execution_id":execution_id,"row_index":body.row_index,"column_name":body.column_name,"old_value":old,"new_value":corr.new_value,"record":json_safe(item["record"])}


@app.post("/api/v1/executions/{execution_id}/revalidate")
def revalidate(execution_id: int, db: Session = Depends(get_db)):
    e=not_found(db.get(Execution, execution_id), "Execution")
    if not e.report_path: raise HTTPException(409, "Execution has no saved outputs")
    # Revalidation is a new immutable run. Copy the correction snapshot and the
    # input artifacts into its execution-specific directory before evaluating.
    previous_id=e.id
    previous_base=Path(e.report_path).parent
    new_execution=Execution(job_id=e.job_id,status="succeeded",retry_of=e.id,
                            rule_snapshot=e.rule_snapshot,passed_count=e.passed_count,
                            failed_count=e.failed_count,logs=list(e.logs or []))
    db.add(new_execution); db.commit(); db.refresh(new_execution)
    root=Path(settings.local_data_dir).resolve()
    base=previous_base.parent/f"execution-{new_execution.id}"
    base.mkdir(parents=True,exist_ok=True)
    for name in ("report.json","failed.json","passed.csv","original_failed.json","original_passed.csv"):
        try:
            content=LocalStorage().read(str(previous_base/name))
            LocalStorage().save(str((base/name).relative_to(root)),content)
        except FileNotFoundError:
            continue
    new_execution.report_path=str(base/"report.json")
    db.commit()
    e=new_execution
    execution_id=e.id
    path=base / "failed.json"
    local=LocalStorage()
    original_failed_path=base / "original_failed.json"
    original_passed_path=base / "original_passed.csv"
    try:
        rows=json.loads(local.read(str(original_failed_path)))
    except FileNotFoundError:
        rows=json.loads(local.read(str(path)))
        local.save(str(original_failed_path.relative_to(Path(settings.local_data_dir).resolve())),json.dumps(rows,ensure_ascii=False,indent=2).encode())
    job=db.get(Job,e.job_id)
    destination=destination_for_job(job)
    # failed.json is the execution-scoped correction source of truth. The
    # failed-record endpoint may read the Databricks table for display, but
    # revalidation must not read it again: that extra SQL session duplicated
    # work immediately before the write and discarded already saved edits.
    # Overlay history so corrections saved by older clients are honored too.
    by_index={item["row_index"]:item for item in rows}
    corrections_for_revalidation = _lineage_corrections(db, e)
    for correction in corrections_for_revalidation:
        if correction.row_index in by_index:
            by_index[correction.row_index]["record"][correction.column_name]=correction.new_value
    rows=list(by_index.values())
    records=[r["record"] for r in rows]
    if e.rule_snapshot is not None:
        from types import SimpleNamespace
        rules=[SimpleNamespace(**item) for item in e.rule_snapshot]
    else:
        scope = [Rule.dataset_id.is_(None)]
        if job.dataset_ids:
            scope.append(Rule.dataset_id.in_(job.dataset_ids))
        rules=list(db.scalars(select(Rule).where(Rule.job_id==job.id, or_(*scope))))
    selected_columns=job.selected_columns or {}
    def applies(rule):
        if not rule.enabled or not rule.approved: return False
        if rule.dataset_id is not None:
            columns=selected_columns.get(str(rule.dataset_id), selected_columns.get(rule.dataset_id, []))
            return not columns or rule.column_name in columns
        return not selected_columns or any(rule.column_name in columns for columns in selected_columns.values())
    rules=[rule for rule in rules if applies(rule)]
    unique_rules=[]; seen_signatures=set()
    for rule in rules:
        signature=rule_signature(rule)
        if signature not in seen_signatures: unique_rules.append(rule); seen_signatures.add(signature)
    rules=unique_rules
    from app.rules import validate_records
    try:
        passed,failed=validate_records(records,rules)
    except Exception as exc:
        logging.getLogger(__name__).exception("Revalidation rule evaluation failed for execution %s", execution_id)
        raise HTTPException(422, {"code":"validation_failure", "message":f"Rule evaluation failed ({type(exc).__name__}). Check the approved rules and source records."}) from exc
    # validate_records indexes the supplied subset; restore the original source
    # row positions so retries/revalidation replace the same execution rows.
    newly_failed_positions={item["row_index"] for item in failed}
    newly_passed_indices=[rows[index]["row_index"] for index in range(len(rows))
                          if index not in newly_failed_positions]
    for item in failed:
        item["row_index"] = rows[item["row_index"]]["row_index"]
    report=json.loads(local.read(e.report_path))
    prior_failed_indices={item["row_index"] for item in rows}
    passed_indices=[index for index in range(report.get("records_loaded",e.passed_count+e.failed_count))
                    if index not in prior_failed_indices]
    try:
        original_passed=parse_data("original_passed.csv", local.read(str(original_passed_path)))
    except FileNotFoundError:
        original_passed=parse_data("passed.csv", local.read(str(base/"passed.csv")))
        local.save(str(original_passed_path.relative_to(root)),encode_csv(original_passed))
    all_passed=original_passed+passed
    all_failed=failed
    destination=destination_for_job(job)
    provider=destination.get("type","local")
    secrets={}
    if provider != "local":
        try:
            destination,secrets=resolve_destination_credentials(destination, db)
        except ValueError as exc:
            raise HTTPException(422, str(exc)) from exc
    source_datasets=[db.get(Dataset,dataset_id) for dataset_id in (job.dataset_ids or [])]
    dataset_label="_and_".join(item.name for item in source_datasets if item)
    storage_run_id=e.id
    parent=db.get(Execution,e.retry_of) if e.retry_of else None
    while parent:
        storage_run_id=parent.id
        parent=db.get(Execution,parent.retry_of) if parent.retry_of else None
    # The destination report is part of the same revalidation write. Prepare its
    # corrected totals before persisting it so remote PASS/FAIL tables and report
    # cannot disagree after a successful write.
    report.update(status="succeeded",passed=len(all_passed),failed=len(all_failed),
                  total=len(all_passed)+len(all_failed),records_evaluated=len(all_passed)+len(all_failed))
    try:
        remote=write_destination(destination,secrets,str(base),all_passed,all_failed,report,encode_csv(all_passed),
            dataset_name=dataset_label or job.name,job_id=job.id,execution_id=e.id,
            columns=list(dict.fromkeys(k for row in all_passed for k in row)),
            source_dataset_ids=[item.id for item in source_datasets if item],storage_run_id=storage_run_id,
            passed_row_indices=passed_indices + newly_passed_indices,
            atomic_update=provider=="databricks")
        report["destination_result"]=remote
        report["storage_status"]={"status":"saved","pass_status":remote.get("pass_status","saved"),"fail_status":remote.get("fail_status","saved")}
        e.status="succeeded";e.error=None
    except Exception as exc:
        # Do not publish changed local outcomes or counts when the destination
        # could not persist them. The failed artifact remains the retry source.
        db.rollback()
        logging.getLogger(__name__).exception("Revalidation destination persistence failed for execution %s", execution_id)
        from app.connectors.destinations import DestinationWriteError
        if isinstance(exc, DestinationWriteError):
            category=exc.category
        elif isinstance(exc, (ConnectionError, TimeoutError, OSError)):
            category="databricks_connection_failure"
        else:
            category="sql_execution_failure"
        message=str(exc).strip() or type(exc).__name__
        # Destination connector errors already redact configured secret values.
        # Avoid returning connection URLs, tokens, or arbitrary provider payloads.
        safe_message=re.sub(r"(?i)(token|password|secret|authorization)(\s*[=:]\s*)[^,;\s]+", r"\1\2[REDACTED]", message)
        raise HTTPException(502, {"code":category, "message":f"Revalidation could not be stored ({type(exc).__name__}): {safe_message}. Corrections are saved; retry after resolving this storage issue."}) from exc
    local.save(str((base/"revalidated_passed.csv").relative_to(root)),encode_csv(passed))
    local.save(str((base/"revalidated_failed.json").relative_to(root)),json.dumps(failed,ensure_ascii=False,indent=2).encode())
    passed_path=local.save(str((base/"passed.csv").relative_to(root)),encode_csv(all_passed))
    failed_path=local.save(str((base/"failed.json").relative_to(root)),json.dumps(all_failed,ensure_ascii=False,indent=2).encode())
    report.update(status=e.status,passed=len(all_passed),failed=len(all_failed),
                   total=len(all_passed)+len(all_failed),records_evaluated=len(all_passed)+len(all_failed),
                   passed_path=passed_path,failed_path=failed_path)
    output_dataset=db.get(Dataset,report.get("output_dataset_id"))
    if output_dataset:
        output_dataset.schema_json={"columns":{column:{"inferred_type":"string","nullable":any(row.get(column) in (None,"") for row in all_passed)} for column in dict.fromkeys(k for row in all_passed for k in row)},"row_count":len(all_passed)}
    remaining_failed_fields = set()
    for item in all_failed:
        for task in _failed_field_tasks(item):
            remaining_failed_fields.add((item.get("row_index"), task.get("column")))
    for correction in corrections_for_revalidation:
        correction.revalidation_execution_id = e.id
        correction.revalidation_status = "failed" if (correction.row_index, correction.column_name) in remaining_failed_fields else "passed"
    local.save(str((base/"report.json").relative_to(root)),json.dumps(report,ensure_ascii=False,indent=2).encode())
    e.passed_count=len(all_passed);e.failed_count=len(all_failed)
    if not all_failed and e.status=="succeeded": e.error=None
    e.logs=(e.logs or [])+[f"Revalidated {len(records)} corrected records; {len(all_passed)} passed; {len(all_failed)} failed",f"Destination storage {report['storage_status']['status']}"]
    db.commit()
    return {"execution_id":e.id,"previous_execution_id":previous_id,"revalidated":len(records),"passed":len(passed),"failed":len(failed),
            "total_passed":len(all_passed),"total_failed":len(all_failed),"storage_status":report["storage_status"]}


@app.get("/api/v1/executions/{execution_id}/corrections")
def correction_history(execution_id: int, db: Session = Depends(get_db)):
    execution = not_found(db.get(Execution,execution_id),"Execution")
    return [_correction_payload(c) for c in _lineage_corrections(db, execution)]


@app.post("/api/v1/credentials", status_code=201)
def create_credential(body: CredentialCreate, db: Session = Depends(get_db)):
    sensitive=sensitive_config_keys(body.config)
    if sensitive: raise HTTPException(422,f"Sensitive values belong in secrets, not config: {sensitive}")
    cipher=credential_key()
    item=Credential(name=body.name,provider=body.provider,secret_ciphertext=cipher.encrypt(json.dumps(body.secrets).encode()).decode(),config=body.config)
    db.add(item); db.commit(); db.refresh(item)
    return {"id":item.id,"name":item.name,"provider":item.provider,"config":item.config,"created_at":item.created_at}


def _connect_source_now(body: CredentialCreate, prefix: str, db: Session):
    """Validate a cloud source by listing its input prefix before storing credentials."""
    provider = "onelake" if body.provider == "fabric" else body.provider
    if provider not in ("s3", "azure_blob", "onelake", "databricks", "snowflake", "sqlserver", "mysql", "postgresql"):
        raise HTTPException(400, "Choose a supported data source")
    if provider == "s3":
        auth_method = body.config.get("auth_method", "access_key")
        if auth_method not in ("access_key", "default_chain"):
            raise HTTPException(422, "Choose access key or IAM role authentication")
        if not body.config.get("bucket") or not body.config.get("region"):
            raise HTTPException(422, "Enter the S3 bucket name and AWS region")
        if auth_method == "access_key" and (not body.secrets.get("access_key_id") or not body.secrets.get("secret_access_key")):
            raise HTTPException(422, "Enter an access key ID and secret access key")
        if auth_method == "default_chain" and (body.secrets.get("access_key_id") or body.secrets.get("secret_access_key")):
            raise HTTPException(422, "Remove access keys when using IAM role authentication")
    if provider == "azure_blob":
        config, secrets = body.config or {}, body.secrets or {}
        has_endpoint = bool(config.get("account_name") or config.get("account_url") or secrets.get("connection_string"))
        has_auth = bool(secrets.get("connection_string") or secrets.get("account_key") or secrets.get("sas_token") or
                        (secrets.get("tenant_id") and secrets.get("client_id") and secrets.get("client_secret")))
        if not config.get("container"):
            raise HTTPException(422, "Azure Blob Storage connections require a container name")
        if not has_endpoint:
            raise HTTPException(422, "Azure Blob Storage connections require a storage account name, account URL, or connection string")
        if not has_auth:
            raise HTTPException(422, "Azure Blob Storage connections require a connection string, account key, SAS token, or service principal credentials")
    adapter = None
    discovery = None
    metadata = None
    operation = "connection"
    try:
        adapter = get_source_adapter(provider, body.config, body.secrets)
        operation = "table_discovery" if provider in ("databricks", "snowflake", "sqlserver", "mysql", "postgresql") else "file_discovery"
        files = adapter.list_files(prefix)
        metadata = getattr(adapter, "last_metadata", None)
        if files and provider != "s3":
            operation = "table_read_check" if provider in ("databricks", "snowflake", "sqlserver", "mysql", "postgresql") else "file_read_check"
            adapter.check_read(files[0]["name"])
    except Exception as e:
        if provider == "databricks":
            from app.connectors.storage import SourceDiscoveryError
            _log_databricks_failure(operation, e, body.secrets)
            if isinstance(e, SourceDiscoveryError):
                if e.code in ("NO_TABLES_FOUND", "NO_TABLES_MATCH_PREFIX"):
                    files = []
                    discovery = {"code": e.code, "message": str(e), "metadata": e.metadata}
                else:
                    status = 403 if e.code.endswith("PERMISSION_DENIED") else 502 if e.code.endswith("_FAILED") else 404
                    root = e
                    while root.__cause__ is not None:
                        root = root.__cause__
                    detail = {"code": e.code, "message": str(e), "metadata": e.metadata}
                    error_code = getattr(root, "error_code", None) or getattr(root, "code", None)
                    sqlstate = getattr(root, "sql_state", None) or getattr(root, "sqlstate", None)
                    if error_code:
                        detail["databricks_error_code"] = str(error_code)
                    if sqlstate:
                        detail["sqlstate"] = str(sqlstate)
                    if root is not e:
                        detail["backend_message"] = _databricks_error_details(root, body.secrets)
                    raise HTTPException(status, detail) from None
            else:
                discovery = None
                safe = str(e).lower()
                error_code = str(getattr(e, "error_code", "") or getattr(e, "code", "")).upper()
                sqlstate = str(getattr(e, "sql_state", None) or getattr(e, "sqlstate", None) or "").upper()
                permission = any(word in safe for word in ("permission denied", "permission_denied", "not authorized", "insufficient privileges", "access denied", "permission")) or error_code in ("PERMISSION_DENIED", "INSUFFICIENT_PRIVILEGES") or sqlstate == "42501"
                if operation == "table_read_check" and permission:
                    code, status = "TABLE_READ_PERMISSION_DENIED", 403
                elif permission:
                    code, status = ("TABLE_DISCOVERY_PERMISSION_DENIED", 403) if operation == "table_discovery" else ("TABLE_READ_PERMISSION_DENIED", 403)
                elif error_code in ("INVALID_ACCESS_TOKEN", "UNAUTHENTICATED", "AUTHENTICATION_FAILED") or sqlstate.startswith("28") or (isinstance(e, PermissionError) and operation == "connection"):
                    code, status = "AUTHENTICATION_FAILED", 401
                elif error_code in ("WAREHOUSE_UNAVAILABLE", "ENDPOINT_UNAVAILABLE", "INVALID_STATE") or sqlstate.startswith("08") or any(word in safe for word in ("warehouse", "http path", "unavailable", "temporarily stopped")):
                    code, status = "WAREHOUSE_UNAVAILABLE", 503
                elif any(word in safe for word in ("authentication", "invalid access token", "unauthorized", "invalid token")):
                    code, status = "AUTHENTICATION_FAILED", 401
                elif isinstance(e, RuntimeError) and "connector" in safe:
                    code, status = "TABLE_DISCOVERY_FAILED", 503
                else:
                    code, status = ("TABLE_READ_PERMISSION_DENIED" if operation == "table_read_check" else "TABLE_DISCOVERY_FAILED"), 502
                root = e
                while root.__cause__ is not None:
                    root = root.__cause__
                message = _databricks_error_details(root, body.secrets)
                if not message:
                    message = "Databricks operation failed. Check the server log for its diagnostic details."
                detail = {"code": code, "message": message}
                error_code = getattr(root, "error_code", None) or getattr(root, "code", None)
                sqlstate = getattr(root, "sql_state", None) or getattr(root, "sqlstate", None)
                if error_code:
                    detail["databricks_error_code"] = str(error_code)
                if sqlstate:
                    detail["sqlstate"] = str(sqlstate)
                raise HTTPException(status, detail) from None
        elif provider == "s3":
            _log_source_failure(provider, operation, e, body.secrets)
            status, detail = _s3_http_error(e)
            raise HTTPException(status, detail) from e
        elif provider == "azure_blob":
            from app.connectors.storage import SourceDiscoveryError
            if isinstance(e, SourceDiscoveryError):
                if e.code in ("AZURE_BLOB_CONTAINER_EMPTY", "AZURE_BLOB_PREFIX_EMPTY"):
                    files = []
                    discovery = {"code": e.code, "message": str(e), "metadata": e.metadata}
                    metadata = e.metadata
                else:
                    status = {
                        "AZURE_BLOB_AUTHENTICATION_FAILED": 401,
                        "AZURE_BLOB_PERMISSION_DENIED": 403,
                        "AZURE_BLOB_ACCOUNT_NOT_FOUND": 404,
                        "AZURE_BLOB_CONTAINER_NOT_FOUND": 404,
                        "AZURE_BLOB_NOT_FOUND": 404,
                        "AZURE_BLOB_ENDPOINT_INVALID": 422,
                        "AZURE_BLOB_UNREACHABLE": 503,
                    }.get(e.code, 400)
                    raise HTTPException(status, {"code": e.code, "message": str(e), "metadata": e.metadata}) from None
            else:
                _log_source_failure(provider, operation, e, body.secrets)
                status, detail = _generic_source_http_error(provider, e)
                raise HTTPException(status, detail) from None
        elif provider in ("sqlserver", "mysql", "postgresql"):
            from app.connectors.storage import SourceDiscoveryError
            if isinstance(e, SourceDiscoveryError):
                if e.code in ("NO_TABLES_FOUND", "NO_TABLES_MATCH_PREFIX"):
                    files = []
                    discovery = {"code": e.code, "message": str(e), "metadata": e.metadata}
                else:
                    status = {
                        "DATABASE_CONFIG_INVALID": 422,
                        "AUTHENTICATION_FAILED": 401,
                        "DATABASE_NOT_FOUND": 404,
                        "DATABASE_UNREACHABLE": 503,
                        "DATABASE_SSL_FAILED": 502,
                        "TABLE_DISCOVERY_FAILED": 502,
                        "TABLE_READ_PERMISSION_DENIED": 403,
                    }.get(e.code, 400)
                    raise HTTPException(status, {"code": e.code, "message": str(e)}) from None
            else:
                # Driver exceptions can contain connection URLs and passwords. Never return or log them.
                _log_source_failure(provider, operation, e, body.secrets)
                raise HTTPException(400, {"code": "DATABASE_CONNECTION_FAILED", "message": "Could not connect to the database. Check the database type, server, port, database name, credentials, and network access."}) from None
        elif provider != "databricks":
            _log_source_failure(provider, operation, e, body.secrets)
            status, detail = _generic_source_http_error(provider, e)
            raise HTTPException(status, detail) from None
    finally:
        if adapter and hasattr(adapter, "close"): adapter.close()
    sensitive = sensitive_config_keys(body.config)
    if sensitive:
        raise HTTPException(422, f"Sensitive values belong in secrets, not config: {sensitive}")
    cipher = credential_key()
    for existing in db.scalars(select(Credential).where(Credential.provider == body.provider, Credential.name == body.name)).all():
        try:
            existing_secrets = json.loads(cipher.decrypt(existing.secret_ciphertext.encode()))
        except (InvalidToken, ValueError):
            continue
        if existing.config == body.config and existing_secrets == body.secrets:
            return {"id": existing.id, "name": existing.name, "provider": existing.provider, "files": files, "discovery": discovery, "metadata": metadata}
    item = Credential(name=body.name, provider=body.provider, secret_ciphertext=cipher.encrypt(json.dumps(body.secrets).encode()).decode(), config=body.config)
    db.add(item); db.commit(); db.refresh(item)
    return {"id": item.id, "name": item.name, "provider": item.provider, "files": files, "discovery": discovery, "metadata": metadata}


@app.post("/api/v1/sources/connect", status_code=201)
def connect_source(body: CredentialCreate, prefix: str = "", async_: bool = Query(False, alias="async"), db: Session = Depends(get_db)):
    if async_:
        task = _start_source_connect_task(body, prefix)
        status_code = 200 if task["status"] in ("succeeded", "failed") else 202
        return JSONResponse(status_code=status_code, content=task)
    return _connect_source_now(body, prefix, db)


@app.get("/api/v1/sources/connect/{operation_id}")
def source_connection_status(operation_id: str):
    with _SOURCE_CONNECT_LOCK:
        task = _SOURCE_CONNECT_TASKS.get(operation_id)
        if task is None:
            raise HTTPException(404, {"code": "SOURCE_OPERATION_NOT_FOUND", "message": "This source discovery operation was not found. Start a new connection test."})
        public = _source_task_public(task)
    return JSONResponse(status_code=200, content=public)


@app.post("/api/v1/sources/databricks/discover")
def discover_databricks_source(body: CredentialCreate, level: str, folder: str = "", prefix: str = ""):
    """Browse Unity Catalog without storing a new connection or exposing its token."""
    if body.provider != "databricks":
        raise HTTPException(400, "This discovery endpoint is for Databricks")
    adapter = None
    try:
        adapter = get_source_adapter("databricks", body.config, body.secrets)
        return adapter.databricks_metadata(level, folder, prefix)
    except Exception as exc:
        from app.connectors.storage import SourceDiscoveryError
        if isinstance(exc, SourceDiscoveryError):
            status = 403 if "PERMISSION_DENIED" in exc.code else 404 if "NOT_FOUND" in exc.code else 502
            raise HTTPException(status, {"code": exc.code, "message": str(exc)}) from None
        raise HTTPException(400, "Could not discover Databricks resources. Verify the connection and selections.") from None
    finally:
        if adapter and hasattr(adapter, "close"):
            adapter.close()


@app.get("/api/v1/credentials")
def list_credentials(db: Session = Depends(get_db)):
    return [{"id":x.id,"name":x.name,"provider":x.provider,"config":x.config,"created_at":x.created_at} for x in db.scalars(select(Credential)).all()]


@app.get("/api/v1/datasets/import-preview/{credential_id}")
def preview_cloud_dataset(credential_id: int, object_key: str, db: Session = Depends(get_db)):
    item = not_found(db.get(Credential, credential_id), "Credential")
    if item.provider != "databricks" or not object_key.startswith("/Volumes/"):
        raise HTTPException(400, "Preview is available for Unity Catalog Volume files")
    try:
        secrets = json.loads(credential_key().decrypt(item.secret_ciphertext.encode()))
        adapter = get_source_adapter("databricks", item.config, secrets)
        try:
            content = adapter.read(object_key)
        finally:
            adapter.close()
        filename = Path(object_key).name
        records = parse_data(filename, content)
        if not records:
            raise HTTPException(422, "The selected file is empty or contains no data rows.")
        columns = list(dict.fromkeys(key for row in records for key in row))
        return {"filename": filename, "columns": columns, "row_count": len(records), "records": records[:20]}
    except HTTPException:
        raise
    except Exception as exc:
        from app.connectors.storage import SourceDiscoveryError
        if isinstance(exc, SourceDiscoveryError):
            status = 403 if "PERMISSION_DENIED" in exc.code else 404 if "NOT_FOUND" in exc.code else 502
            raise HTTPException(status, {"code": exc.code, "message": str(exc)}) from None
        if isinstance(exc, ValueError):
            raise HTTPException(422, str(exc)) from None
        raise HTTPException(502, "Could not read or parse the selected Volume file.") from None


@app.post("/api/v1/datasets/import/{credential_id}", status_code=201)
def import_cloud_dataset(credential_id: int, object_key: str, name: str | None = None, db: Session = Depends(get_db)):
    item=not_found(db.get(Credential,credential_id),"Credential")
    for existing in db.scalars(select(Dataset).where(Dataset.source_type == item.provider)).all():
        source_meta = (existing.schema_json or {}).get("_source", {})
        if source_meta.get("credential_id") == credential_id and source_meta.get("object_key") == object_key:
            return existing
    try: secrets=json.loads(credential_key().decrypt(item.secret_ciphertext.encode()))
    except (InvalidToken, ValueError) as e: raise HTTPException(500,"Stored credential cannot be decrypted") from e
    provider=item.provider
    try:
        storage_provider="onelake" if provider == "fabric" else provider
        if storage_provider not in ("s3","azure_blob","onelake","databricks","snowflake","sqlserver","mysql","postgresql"): raise HTTPException(501,f"{provider} source adapter is not implemented")
        adapter=get_source_adapter(storage_provider,item.config,secrets)
        try: content=adapter.read(object_key)
        finally:
            if hasattr(adapter,"close"): adapter.close()
        if storage_provider in ("sqlserver","mysql","postgresql"):
            fname=Path(object_key).name + ".json"
        elif storage_provider in ("databricks","snowflake") and not object_key.lower().endswith((".csv",".tsv",".json",".xlsx",".xls",".parquet")):
            fname=Path(object_key).name + ".csv"
        else: fname=Path(object_key).name
        records=parse_data(fname if storage_provider in ("databricks","snowflake","sqlserver","mysql","postgresql") else object_key,content)
    except Exception as exc:
        from app.connectors.storage import SourceDiscoveryError
        if isinstance(exc, SourceDiscoveryError):
            status = 403 if "PERMISSION_DENIED" in exc.code else 404 if "NOT_FOUND" in exc.code else 502
            raise HTTPException(status, {"code": exc.code, "message": str(exc)}) from None
        if isinstance(exc, ValueError):
            raise HTTPException(422, str(exc)) from None
        raise HTTPException(502,"Source read failed; verify the connection and selected object") from None
    if not records:
        raise HTTPException(422, "The selected source is empty or contains no data rows.")
    columns=list(dict.fromkeys(k for r in records[:1000] for k in r))
    source_meta={"credential_id":credential_id,"object_key":object_key}
    if provider in ("sqlserver","mysql","postgresql"):
        source_schema, _, source_table = object_key.partition(".")
        source_meta.update({"schema":source_schema,"table":source_table,"database":item.config.get("database"),"database_type":provider})
    schema={"columns":{c:{"inferred_type":infer_type([r.get(c) for r in records[:1000]]),"nullable":any(is_missing(r.get(c)) for r in records[:1000])} for c in columns},"row_count":len(records),"_source":source_meta}
    path=LocalStorage().save(f"uploads/{__import__('uuid').uuid4().hex}-{fname}",content)
    dataset_name = name.strip() if isinstance(name, str) else ""
    ds=Dataset(name=dataset_name or Path(fname).stem,filename=fname,path=path,source_type=provider,schema_json=schema)
    db.add(ds);db.commit();db.refresh(ds);return ds


@app.delete("/api/v1/credentials/{credential_id}", status_code=204)
def delete_credential(credential_id: int, db: Session = Depends(get_db)):
    item = not_found(db.get(Credential, credential_id), "Credential")
    # Keep already imported datasets; only remove the saved connection and its encrypted secret.
    db.delete(item)
    db.commit()


@app.get("/api/v1")
def api_root(): return {"name":"ADQ Automation API","docs":"/docs","version":"0.1.0"}
