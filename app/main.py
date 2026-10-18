import json, logging, mimetypes
from contextlib import asynccontextmanager
from pathlib import Path
from fastapi import BackgroundTasks, Depends, FastAPI, File, HTTPException, UploadFile
from fastapi.responses import FileResponse, JSONResponse
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import delete, select
from sqlalchemy.orm import Session
from app.config import settings
from app.database import Base, engine, get_db
from app.models import Correction, Credential, Dataset, Execution, Job, Rule
from app.processing import execute, parse_data
from app.rules import RULE_TYPES, validate_rule_definition
from app.schemas import CorrectionCreate, CredentialCreate, JobConfiguration, JobCreate, JobUpdate, RuleCreate, RuleUpdate
from app.connectors.storage import LocalStorage, get_storage, get_source_adapter

logging.basicConfig(level=settings.log_level)
@asynccontextmanager
async def lifespan(app):
    Base.metadata.create_all(bind=engine)
    # create_all does not add columns to an existing SQLite database.
    if engine.dialect.name == "sqlite":
        from sqlalchemy import inspect
        if "settings" not in {column["name"] for column in inspect(engine).get_columns("jobs")}:
            with engine.begin() as connection:
                connection.exec_driver_sql("ALTER TABLE jobs ADD COLUMN settings JSON NOT NULL DEFAULT '{}'")
    yield


app = FastAPI(title="ADQ Automation API", version="0.1.0", description="AI-assisted data quality jobs. Local CSV/JSON processing is ready; cloud providers require credentials and optional SDKs.", lifespan=lifespan)


def not_found(obj, name):
    if obj is None: raise HTTPException(404, f"{name} not found")
    return obj


def credential_key():
    if not settings.secret_encryption_key:
        raise HTTPException(503, "Credential storage is disabled. Set SECRET_ENCRYPTION_KEY to a valid Fernet key.")
    try: return Fernet(settings.secret_encryption_key.encode())
    except Exception as e: raise HTTPException(503, "SECRET_ENCRYPTION_KEY is invalid") from e


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
def health(): return {"status": "ok", "database": "configured", "local_workflow": "ready", "ai": settings.ai_provider if settings.ai_provider != "none" else "disabled"}


@app.get("/api/v1/capabilities")
def capabilities():
    return {"sources": {"local_csv_json": "ready", "s3": "optional boto3 adapter", "azure_blob": "optional Azure SDK adapter", "onelake": "optional Fabric OAuth adapter", "databricks": "optional SQL connector with table discovery", "snowflake": "optional SQL connector with table discovery"},
        "destinations": {"local": "ready", "s3": "optional boto3 adapter", "azure_blob": "optional Azure SDK adapter", "onelake": "optional Fabric OAuth adapter", "databricks": "optional SQL connector", "snowflake": "optional SQL connector", "fabric": "optional OneLake adapter"},
        "ai_suggestions": "configured" if settings.ai_provider != "none" and settings.resolved_ai_api_key and settings.resolved_ai_model and settings.resolved_ai_base_url else "disabled; set AI_PROVIDER, AI_API_KEY and AI_MODEL (or GROQ_API_KEY and GROQ_MODEL for Groq)"}


@app.get("/api/v1/datasets")
def list_datasets(db: Session = Depends(get_db)):
    return db.scalars(select(Dataset).order_by(Dataset.id.desc())).all()


@app.post("/api/v1/datasets/upload", status_code=201)
async def upload_dataset(file: UploadFile = File(...), name: str | None = None, db: Session = Depends(get_db)):
    fname = Path(file.filename or "").name
    if Path(fname).suffix.lower() not in (".csv", ".tsv", ".json", ".xlsx", ".xls", ".parquet"): raise HTTPException(415, "Upload a CSV, TSV, JSON, Excel, or Parquet file")
    content = await file.read(settings.max_upload_mb * 1024 * 1024 + 1)
    if len(content) > settings.max_upload_mb * 1024 * 1024: raise HTTPException(413, "Upload exceeds MAX_UPLOAD_MB")
    try: records = parse_data(fname, content)
    except Exception as e: raise HTTPException(400, f"Invalid dataset: {e}") from e
    columns = list(dict.fromkeys(k for row in records[:1000] for k in row))
    schema = {col: {"inferred_type": infer_type([r.get(col) for r in records[:1000]]), "nullable": any(r.get(col) in (None, "") for r in records[:1000])} for col in columns}
    path = LocalStorage().save(f"uploads/{__import__('uuid').uuid4().hex}-{fname}", content)
    dataset = Dataset(name=name or Path(fname).stem, filename=fname, path=path, schema_json={"columns": schema, "row_count": len(records)})
    db.add(dataset); db.commit(); db.refresh(dataset); return dataset


def infer_type(values):
    vals = [str(v) for v in values if v not in (None, "")]
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
    return db.scalars(select(Job).order_by(Job.id.desc())).all()


@app.post("/api/v1/jobs", status_code=201)
def create_job(body: JobCreate, db: Session = Depends(get_db)):
    datasets=[db.get(Dataset,did) for did in body.dataset_ids]
    if any(d is None for d in datasets): raise HTTPException(422, "Every dataset_ids entry must exist")
    validate_job_inputs(body,datasets)
    job = Job(**body.model_dump()); db.add(job); db.commit(); db.refresh(job); return job


@app.get("/api/v1/jobs/{job_id}")
def get_job(job_id: int, db: Session = Depends(get_db)):
    return not_found(db.get(Job, job_id), "Job")


@app.patch("/api/v1/jobs/{job_id}")
def update_job(job_id: int, body: JobUpdate, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    changes=body.model_dump(exclude_unset=True)
    dataset_ids=changes.get("dataset_ids",job.dataset_ids)
    datasets=[db.get(Dataset,did) for did in dataset_ids]
    if any(d is None for d in datasets): raise HTTPException(422, "Every dataset_ids entry must exist")
    merged={"name":changes.get("name",job.name),"description":changes.get("description",job.description),"dataset_ids":dataset_ids,
        "selected_columns":changes.get("selected_columns",job.selected_columns),"join_config":changes.get("join_config",job.join_config),
        "destination":changes.get("destination",job.destination),"settings":changes.get("settings",job.settings or {})}
    validate_job_inputs(JobCreate(**merged),datasets)
    for k,v in changes.items(): setattr(job, k, v)
    db.commit(); db.refresh(job); return job


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
    validate_job_inputs(body.job, datasets)
    validate_configuration_rules(body.job, body.rules, db)
    job = Job(**body.job.model_dump())
    db.add(job)
    db.flush()
    for rule in body.rules:
        values = rule.model_dump()
        values["rule_type"] = values["rule_type"].upper()
        values["enabled"] = True
        values["approved"] = True
        db.add(Rule(job_id=job.id, **values))
    db.commit()
    db.refresh(job)
    return job


@app.put("/api/v1/jobs/{job_id}/configuration")
def replace_job_configuration(job_id: int, body: JobConfiguration, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    datasets = [db.get(Dataset, did) for did in body.job.dataset_ids]
    if any(d is None for d in datasets):
        raise HTTPException(422, "Every dataset_ids entry must exist")
    validate_job_inputs(body.job, datasets)
    validate_configuration_rules(body.job, body.rules, db)
    for key, value in body.job.model_dump().items():
        setattr(job, key, value)
    db.execute(delete(Rule).where(Rule.job_id == job_id))
    for rule in body.rules:
        values = rule.model_dump()
        values["rule_type"] = values["rule_type"].upper()
        values["enabled"] = True
        values["approved"] = True
        db.add(Rule(job_id=job_id, **values))
    db.commit()
    db.refresh(job)
    return job


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
    changes = body.model_dump(exclude_unset=True)
    if any(k in changes and changes[k] is None for k in ("column_name","rule_type","parameters","enabled","approved")):
        raise HTTPException(422,"Rule fields cannot be null; omit fields you do not want to change")
    if "rule_type" in changes:
        changes["rule_type"] = changes["rule_type"].upper()
        if changes["rule_type"] not in RULE_TYPES: raise HTTPException(422, "Unsupported rule type")
    kind=changes.get("rule_type",rule.rule_type)
    params=changes.get("parameters",rule.parameters)
    try: validate_rule_definition(kind,params)
    except ValueError as e: raise HTTPException(422,str(e)) from e
    for k,v in changes.items(): setattr(rule,k,v)
    db.commit(); db.refresh(rule); return rule


@app.delete("/api/v1/rules/{rule_id}", status_code=204)
def delete_rule(rule_id: int, db: Session = Depends(get_db)):
    rule = not_found(db.get(Rule, rule_id), "Rule"); db.delete(rule); db.commit()


@app.post("/api/v1/jobs/{job_id}/suggest-rules")
async def suggest_rules(job_id: int, db: Session = Depends(get_db)):
    job = not_found(db.get(Job, job_id), "Job")
    ai_base_url=settings.resolved_ai_base_url
    ai_api_key=settings.resolved_ai_api_key
    ai_model=settings.resolved_ai_model
    if settings.ai_provider == "none" or not ai_api_key or not ai_model or not ai_base_url:
        raise HTTPException(503, "AI rule suggestions are disabled; configure AI_PROVIDER, AI_API_KEY, AI_MODEL, and AI_BASE_URL for custom providers")
    import httpx
    profile = []
    local = LocalStorage()
    for did in job.dataset_ids:
        ds = db.get(Dataset, did)
        rows = parse_data(ds.filename, local.read(ds.path))
        cols = {}
        for c in (ds.schema_json or {}).get("columns", {}):
            vals = [r.get(c) for r in rows]
            cols[c] = {"inferred_type": infer_type(vals), "null_fraction": sum(v in (None, "") for v in vals)/max(len(vals),1), "distinct_sample": list(dict.fromkeys(str(v) for v in vals if v not in (None,"")))[:10]}
        profile.append({"dataset_id": did, "row_count": len(rows), "columns": cols})
    prompt = {"task":"Suggest data quality rules grounded only in this schema and profile. Return a JSON object with a suggestions array. Each item: dataset_id, column_name, rule_type from supported list, parameters, rationale. Do not invent columns or values.", "supported_rule_types":sorted(RULE_TYPES), "profile":profile}
    url = ai_base_url.rstrip("/") + "/chat/completions"
    try:
        async with httpx.AsyncClient(timeout=settings.ai_timeout_seconds) as client:
            response = await client.post(url, headers={"Authorization":f"Bearer {ai_api_key}"}, json={"model":ai_model,"messages":[{"role":"user","content":json.dumps(prompt)}],"temperature":0.2,"response_format":{"type":"json_object"}})
        response.raise_for_status(); payload=response.json()
        text=payload["choices"][0]["message"]["content"]
        result=json.loads(text)
        if isinstance(result,list): suggestions=result
        elif isinstance(result,dict): suggestions=result.get("suggestions",[])
        else: raise ValueError("Expected an object with a suggestions array")
        if not isinstance(suggestions,list): raise ValueError("AI response suggestions field must be an array")
        for item in suggestions:
            if item.get("rule_type", "").upper() not in RULE_TYPES or int(item.get("dataset_id", -1)) not in job.dataset_ids:
                raise ValueError("AI response contained an unsupported rule or dataset")
        return {"provider":settings.ai_provider,"model":ai_model,"suggestions":suggestions,"saved":False,"note":"Review suggestions and create/approve rules explicitly."}
    except httpx.HTTPError as e: raise HTTPException(502, "AI provider request failed; check the configured endpoint and credentials") from e
    except (KeyError, ValueError, TypeError) as e: raise HTTPException(502, "AI provider returned an invalid response") from e


@app.post("/api/v1/jobs/{job_id}/execute", status_code=202)
def run_job(job_id: int, tasks: BackgroundTasks, db: Session = Depends(get_db)):
    not_found(db.get(Job, job_id), "Job")
    record = Execution(job_id=job_id, status="queued")
    db.add(record); db.commit(); db.refresh(record)
    tasks.add_task(execute, record.id)
    return record


@app.post("/api/v1/executions/{execution_id}/retry", status_code=202)
def retry_execution(execution_id: int, tasks: BackgroundTasks, db: Session = Depends(get_db)):
    previous=not_found(db.get(Execution,execution_id),"Execution")
    if previous.status != "failed": raise HTTPException(409,"Only failed executions can be retried")
    retry=Execution(job_id=previous.job_id,status="queued",attempt=previous.attempt+1,retry_of=previous.id)
    db.add(retry);db.commit();db.refresh(retry);tasks.add_task(execute,retry.id);return retry


@app.get("/api/v1/jobs/{job_id}/executions")
def job_executions(job_id: int, db: Session = Depends(get_db)):
    not_found(db.get(Job, job_id), "Job")
    return db.scalars(select(Execution).where(Execution.job_id == job_id).order_by(Execution.id.desc())).all()


@app.get("/api/v1/executions/{execution_id}")
def execution_detail(execution_id: int, db: Session = Depends(get_db)):
    return not_found(db.get(Execution, execution_id), "Execution")


@app.get("/api/v1/executions/{execution_id}/failed")
def failed_records(execution_id: int, db: Session = Depends(get_db)):
    e=not_found(db.get(Execution, execution_id), "Execution")
    if not e.report_path: raise HTTPException(409, "Execution has no saved outputs")
    path=Path(e.report_path).parent / "failed.json"
    try: return json.loads(LocalStorage().read(str(path)))
    except FileNotFoundError: raise HTTPException(404, "Failed records output not found")


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
    old=item["record"][body.column_name]
    corr=Correction(execution_id=execution_id,row_index=body.row_index,column_name=body.column_name,old_value=None if old is None else str(old),new_value=body.value)
    item["record"][body.column_name]=body.value
    LocalStorage().save(str(path.relative_to(Path(settings.local_data_dir).resolve())), json.dumps(rows,ensure_ascii=False,indent=2).encode())
    db.add(corr); db.commit()
    return {"correction_id":corr.id,"row_index":body.row_index,"column_name":body.column_name,"old_value":old,"new_value":body.value}


@app.post("/api/v1/executions/{execution_id}/revalidate")
def revalidate(execution_id: int, db: Session = Depends(get_db)):
    e=not_found(db.get(Execution, execution_id), "Execution")
    if not e.report_path: raise HTTPException(409, "Execution has no saved outputs")
    path=Path(e.report_path).parent / "failed.json"
    rows=json.loads(LocalStorage().read(str(path)))
    records=[r["record"] for r in rows]
    job=db.get(Job,e.job_id); rules=list(db.scalars(select(Rule).where(Rule.job_id==job.id)))
    from app.rules import validate_records
    passed,failed=validate_records(records,rules)
    root=Path(settings.local_data_dir).resolve(); base=Path(e.report_path).parent
    local=LocalStorage()
    local.save(str((base/"revalidated_passed.csv").relative_to(root)),__import__("app.processing",fromlist=["encode_csv"]).encode_csv(passed))
    local.save(str((base/"revalidated_failed.json").relative_to(root)),json.dumps(failed,indent=2).encode())
    return {"revalidated":len(records),"passed":len(passed),"failed":len(failed)}


@app.get("/api/v1/executions/{execution_id}/corrections")
def correction_history(execution_id: int, db: Session = Depends(get_db)):
    not_found(db.get(Execution,execution_id),"Execution")
    return db.scalars(select(Correction).where(Correction.execution_id==execution_id).order_by(Correction.id)).all()


@app.post("/api/v1/credentials", status_code=201)
def create_credential(body: CredentialCreate, db: Session = Depends(get_db)):
    sensitive=sensitive_config_keys(body.config)
    if sensitive: raise HTTPException(422,f"Sensitive values belong in secrets, not config: {sensitive}")
    cipher=credential_key()
    item=Credential(name=body.name,provider=body.provider,secret_ciphertext=cipher.encrypt(json.dumps(body.secrets).encode()).decode(),config=body.config)
    db.add(item); db.commit(); db.refresh(item)
    return {"id":item.id,"name":item.name,"provider":item.provider,"config":item.config,"created_at":item.created_at}


@app.post("/api/v1/sources/connect", status_code=201)
def connect_source(body: CredentialCreate, prefix: str = "", db: Session = Depends(get_db)):
    """Validate a cloud source by listing its input prefix before storing credentials."""
    provider = "onelake" if body.provider == "fabric" else body.provider
    if provider not in ("s3", "azure_blob", "onelake", "databricks", "snowflake"):
        raise HTTPException(400, "Choose a supported data source")
    adapter = None
    try:
        adapter = get_source_adapter(provider, body.config, body.secrets)
        files = adapter.list_files(prefix)
        if files:
            adapter.check_read(files[0]["name"])
    except Exception as e:
        raise HTTPException(400, "Could not connect or list files; verify the source settings and credentials") from e
    finally:
        if adapter and hasattr(adapter, "close"): adapter.close()
    sensitive = sensitive_config_keys(body.config)
    if sensitive:
        raise HTTPException(422, f"Sensitive values belong in secrets, not config: {sensitive}")
    cipher = credential_key()
    item = Credential(name=body.name, provider=body.provider, secret_ciphertext=cipher.encrypt(json.dumps(body.secrets).encode()).decode(), config=body.config)
    db.add(item); db.commit(); db.refresh(item)
    return {"id": item.id, "name": item.name, "provider": item.provider, "files": files}


@app.get("/api/v1/credentials")
def list_credentials(db: Session = Depends(get_db)):
    return [{"id":x.id,"name":x.name,"provider":x.provider,"config":x.config,"created_at":x.created_at} for x in db.scalars(select(Credential)).all()]


@app.post("/api/v1/datasets/import/{credential_id}", status_code=201)
def import_cloud_dataset(credential_id: int, object_key: str, name: str | None = None, db: Session = Depends(get_db)):
    item=not_found(db.get(Credential,credential_id),"Credential")
    try: secrets=json.loads(credential_key().decrypt(item.secret_ciphertext.encode()))
    except (InvalidToken, ValueError) as e: raise HTTPException(500,"Stored credential cannot be decrypted") from e
    provider=item.provider
    try:
        storage_provider="onelake" if provider == "fabric" else provider
        if storage_provider not in ("s3","azure_blob","onelake","databricks","snowflake"): raise HTTPException(501,f"{provider} source adapter is not implemented")
        adapter=get_source_adapter(storage_provider,item.config,secrets)
        try: content=adapter.read(object_key)
        finally:
            if hasattr(adapter,"close"): adapter.close()
        if storage_provider in ("databricks","snowflake") and not object_key.lower().endswith((".csv",".tsv",".json",".xlsx",".xls",".parquet")):
            fname=Path(object_key).name + ".csv"
        else: fname=Path(object_key).name
        records=parse_data(fname if storage_provider in ("databricks","snowflake") else object_key,content)
    except Exception as e: raise HTTPException(502,"Source read failed; verify the connection and selected object") from e
    columns=list(dict.fromkeys(k for r in records[:1000] for k in r))
    schema={"columns":{c:{"inferred_type":infer_type([r.get(c) for r in records[:1000]]),"nullable":any(r.get(c) in (None,"") for r in records[:1000])} for c in columns},"row_count":len(records)}
    path=LocalStorage().save(f"uploads/{__import__('uuid').uuid4().hex}-{fname}",content)
    ds=Dataset(name=name or Path(fname).stem,filename=fname,path=path,source_type=provider,schema_json=schema)
    db.add(ds);db.commit();db.refresh(ds);return ds


@app.get("/api/v1")
def api_root(): return {"name":"ADQ Automation API","docs":"/docs","version":"0.1.0"}
