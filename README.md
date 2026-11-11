# ADQ Automation

ADQ Automation is a data quality application with a FastAPI backend and a React/TypeScript frontend. It lets a user create data quality jobs, upload or import datasets, select columns, configure validation rules, run validations, review failed records, apply corrections, and export results.

This README describes what is working right now, what is still pending, and the API surface available in the project.

## Current Status

### Working Now

- React frontend routes:
  - `/` home page
  - `/login` demo login/register page
  - `/onboarding` guided workflow
  - `/jobs` saved jobs workspace
- FastAPI backend with versioned routes under `/api/v1`.
- SQLite local database by default, with PostgreSQL support through `DATABASE_URL`.
- Local file upload for CSV, TSV, Excel, Parquet, and JSON parsing.
- Dataset schema extraction with inferred column types and row counts.
- Workflow selection persistence for selected datasets, selected columns, and source connection mapping.
- Job creation, update, delete, configuration replacement, and job listing.
- Rule creation, update, delete, approve-all, Excel rule import preview, Excel rule import, error report export, and employee baseline rule generation.
- Supported validation rules:
  - `NOT_NULL`
  - `UNIQUE`
  - `DUPLICATES`
  - `DATA_TYPE`
  - `MIN_VALUE`
  - `MAX_VALUE`
  - `RANGE`
  - `EMAIL_FORMAT`
  - `REGEX`
  - `LENGTH`
  - `IN_SET`
- Validation execution through background tasks.
- Execution polling, progress stages, retry, failed-record retrieval, and artifact download.
- PASS/FAIL output generation.
- Local result artifacts under the configured local data directory.
- Export failed records to Excel.
- Export correction history to Excel.
- Export generated rules to Excel or CSV.
- Manual correction of failed records.
- Bulk correction application.
- Revalidation after corrections.
- AI rule suggestion flow when `OPENAI_API_KEY` and `OPENAI_MODEL` are configured.
- AI correction suggestion flow for failed records when OpenAI is configured.
- Credential storage with encrypted secrets when `SECRET_ENCRYPTION_KEY` is set.
- Source connection/import flows for:
  - Local upload
  - Amazon S3
  - Azure Blob Storage
  - Microsoft Fabric OneLake
  - Databricks
  - Snowflake
  - SQL Server
  - MySQL
  - PostgreSQL
- Destination writing support for:
  - Local
  - Databricks
  - Snowflake
  - Microsoft Fabric/OneLake
  - Amazon S3
  - Azure Blob Storage
- Frontend tests and backend tests are present.

### Recently Verified

These commands were run successfully in this workspace:

```powershell
Set-Location frontend
npm test -- --run
npm run build

Set-Location ..\backend
py -m pytest -q
```

Backend tests passed with a pytest cache warning caused by local permission limits on `.pytest_cache`; the test suite itself completed.

### Pending / Not Production Ready Yet

- Real authentication is not implemented. The login page is a demo UI and redirects into the app.
- No tenant isolation or user-level authorization exists yet.
- Background validation uses in-process FastAPI background/worker behavior; production should use a durable queue and worker system.
- AI provider configuration currently resolves to OpenAI in code. `.env.example` contains extra AI provider variables, but broader provider switching needs cleanup or implementation.
- Cloud connectors and destinations depend on real credentials, SDKs, network access, and provider permissions. Unit tests mock/localize much of this.
- The frontend is functional, but it is still a single-page app with direct route branching in `main.tsx`; a full router would be cleaner for production.
- No dedicated end-to-end browser test suite is included.
- No production deployment files are finalized.
- No complete role-based access control, audit login trail, or admin user management exists.
- Secrets must be managed carefully before deployment. Do not use blank or rotating `SECRET_ENCRYPTION_KEY` values once credentials are saved.
- Large-file processing and long-running validation need production-scale limits, streaming, queues, and observability before heavy use.

## Project Structure

```text
backend/
  app/
    main.py                 FastAPI routes and workflow orchestration
    schemas.py              Request models
    models.py               SQLAlchemy models
    processing.py           Dataset parsing and execution processing
    rules.py                Validation rule logic
    connectors/             Source and destination adapters
  migrations/               Alembic migrations
  tests/                    Backend tests

frontend/
  src/
    main.tsx                Route selection and app mount
    AuthPage.tsx            Demo auth page
    HomePage.tsx            Home page
    Onboarding.tsx          Guided workflow UI
    JobsPage.tsx            Jobs workspace
    *.mjs / *.test.mjs      Shared frontend logic and tests
```

## Requirements

- Python 3.11 or newer
- Node.js and npm
- SQLite by default
- PostgreSQL optional
- Cloud SDKs optional, depending on connector use

## Backend Setup

```powershell
Set-Location backend
py -m venv .venv
.venv\Scripts\Activate.ps1
py -m pip install -r requirements.txt
if (-not (Test-Path ..\.env)) { Copy-Item .env.example ..\.env }
py -m uvicorn app.main:app --reload
```

Backend URLs:

- API root: `http://127.0.0.1:8000/api/v1`
- Health: `http://127.0.0.1:8000/health`
- Swagger UI: `http://127.0.0.1:8000/docs`

## Frontend Setup

```powershell
Set-Location frontend
npm install
npm run dev
```

By default the frontend calls `/api/v1`. Use Vite proxy/config or `VITE_API_BASE_URL` when the backend is on a different origin.

Useful frontend commands:

```powershell
npm test
npm run build
npm run preview
```

## Configuration

The backend reads the root `.env` file. Start from `backend/.env.example`.

Important variables:

| Variable | Purpose |
| --- | --- |
| `DATABASE_URL` | SQLAlchemy database URL. Defaults to SQLite. |
| `LOCAL_DATA_DIR` | Local upload and output artifact directory. |
| `MAX_UPLOAD_MB` | Maximum upload size. |
| `SECRET_ENCRYPTION_KEY` | Fernet key used to encrypt stored connector secrets. |
| `OPENAI_API_KEY` | Enables AI rule and correction suggestions. |
| `OPENAI_MODEL` | OpenAI model used for AI suggestions. |
| `DATABRICKS_*` | Optional Databricks fallback destination configuration. |
| `ONELAKE_ACCOUNT_URL` | OneLake endpoint override. |
| `AZURE_BLOB_ACCOUNT_URL_TEMPLATE` | Azure Blob endpoint template. |
| `LOG_LEVEL` | Backend logging level. |

Generate a Fernet key:

```powershell
py -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
```

## Workflow

1. Open the frontend and choose a platform.
2. Create a job.
3. Upload a local file or connect a remote source.
4. Select datasets and columns in the landing zone.
5. Add, import, approve, or generate quality rules.
6. Run validation.
7. Review PASS/FAIL results.
8. Export failed records or generated rules.
9. Apply manual or AI-assisted corrections.
10. Revalidate corrected records.

## API Summary

Base URL:

```text
/api/v1
```

### System

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/health` | Health check outside `/api/v1`. |
| `GET` | `/api/v1` | API metadata. |
| `GET` | `/api/v1/capabilities` | Shows available adapters/capabilities. |

### Datasets

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/datasets` | List datasets. |
| `POST` | `/datasets/upload` | Upload a local dataset file. |
| `GET` | `/datasets/{dataset_id}` | Get one dataset. |
| `GET` | `/datasets/import-preview/{credential_id}` | Preview a remote file/table before import. |
| `POST` | `/datasets/import/{credential_id}` | Import a remote file/table as a dataset. |
| `POST` | `/datasets/{dataset_id}/suggest-rules` | Generate rule suggestions for one dataset. |

### Workflow Selection

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/workflows/{workflow_id}/selection` | Read selected datasets, columns, and source connections. |
| `PUT` | `/workflows/{workflow_id}/selection` | Save selected datasets, columns, and source connections. |

### Jobs

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/jobs` | List jobs. |
| `POST` | `/jobs` | Create a job. |
| `GET` | `/jobs/{job_id}` | Get one job. |
| `PATCH` | `/jobs/{job_id}` | Update a job. |
| `DELETE` | `/jobs/{job_id}` | Delete a job. |
| `POST` | `/jobs/configuration` | Create a job with rules in one request. |
| `PUT` | `/jobs/{job_id}/configuration` | Replace job configuration and rules. |
| `GET` | `/jobs/{job_id}/landing-zone` | Get landing-zone datasets for a job. |

Main job body:

```json
{
  "name": "Weekly validation",
  "description": "Validate core customer data",
  "dataset_ids": [1],
  "selected_columns": {"1": ["Email", "Age"]},
  "join_config": null,
  "destination": {"type": "local"},
  "environment": "Local",
  "settings": {}
}
```

### Rules

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/jobs/{job_id}/rules` | List job rules. |
| `POST` | `/jobs/{job_id}/rules` | Create a rule. |
| `PATCH` | `/rules/{rule_id}` | Update a rule. |
| `DELETE` | `/rules/{rule_id}` | Delete a rule. |
| `POST` | `/jobs/{job_id}/rules/approve-all` | Approve all eligible rules. |
| `POST` | `/jobs/{job_id}/baseline-rules` | Add employee baseline rules. |
| `GET` | `/rules/template` | Download Excel rule template. |
| `POST` | `/jobs/{job_id}/rules/import-preview` | Preview uploaded Excel rules. |
| `POST` | `/jobs/{job_id}/rules/import` | Save imported rules. |
| `POST` | `/rules/error-report` | Download rule import error report. |
| `POST` | `/jobs/{job_id}/suggest-rules` | Generate AI suggestions for a job. |
| `GET` | `/jobs/{job_id}/suggest-rules/{task_id}` | Poll async AI rule generation. |

Rule body:

```json
{
  "dataset_id": 1,
  "column_name": "Email",
  "rule_type": "EMAIL_FORMAT",
  "parameters": {"severity": "high"},
  "enabled": true,
  "approved": true
}
```

### Executions

| Method | Path | Purpose |
| --- | --- | --- |
| `POST` | `/jobs/{job_id}/execute` | Start validation. |
| `POST` | `/executions/{execution_id}/retry` | Retry a failed execution. |
| `GET` | `/jobs/{job_id}/executions` | List executions for a job. |
| `GET` | `/executions/{execution_id}` | Get execution status/details. |
| `GET` | `/executions/{execution_id}/failed` | Get failed records. |
| `GET` | `/executions/{execution_id}/artifacts/{artifact_name}` | Download an artifact. |
| `GET` | `/executions/{execution_id}/exports/failed.xlsx` | Download failed records Excel. |
| `GET` | `/executions/{execution_id}/exports/corrections.xlsx` | Download corrections Excel. |
| `GET` | `/executions/{execution_id}/exports/rules.{file_format}` | Download generated rules as `xlsx` or `csv`. |

Common artifact names include:

- `passed.csv`
- `failed.json`
- `report.json`
- `revalidated_failed.json`

### Corrections

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/executions/{execution_id}/correction-suggestions` | Get AI correction suggestions. |
| `GET` | `/executions/{execution_id}/correction-summary` | Get correction summary. |
| `GET` | `/executions/{execution_id}/corrections` | Get correction history. |
| `POST` | `/executions/{execution_id}/corrections` | Save one correction. |
| `POST` | `/executions/{execution_id}/corrections/bulk` | Save multiple corrections. |
| `POST` | `/executions/{execution_id}/revalidate` | Revalidate after corrections. |

Correction body:

```json
{
  "row_index": 0,
  "column_name": "Email",
  "value": "fixed@example.com",
  "original_value": "bad-email"
}
```

### Credentials And Sources

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/credentials` | List saved credentials without secrets. |
| `POST` | `/credentials` | Save a credential. |
| `DELETE` | `/credentials/{credential_id}` | Delete a credential. |
| `POST` | `/sources/connect` | Test/connect/discover a source. Supports `?async=true`. |
| `GET` | `/sources/connect/{operation_id}` | Poll async source connection. |
| `POST` | `/sources/databricks/discover` | Browse Databricks catalogs, schemas, volumes, tables, or files. |

Credential body:

```json
{
  "name": "My S3 Source",
  "provider": "s3",
  "config": {
    "bucket": "example-bucket",
    "region": "us-east-1",
    "auth_method": "access_key"
  },
  "secrets": {
    "access_key_id": "...",
    "secret_access_key": "..."
  }
}
```

Supported credential providers:

- `s3`
- `azure_blob`
- `onelake`
- `snowflake`
- `databricks`
- `fabric`
- `sqlserver`
- `mysql`
- `postgresql`

## Destination Notes

Use `destination.type` on a job to choose where outputs are written.

Supported values:

- `local`
- `databricks`
- `snowflake`
- `fabric`
- `s3`
- `azure_blob`

Local outputs are written under `LOCAL_DATA_DIR/outputs`. Databricks and Snowflake write execution-scoped PASS/FAIL tables. Fabric, S3, and Azure Blob write output artifacts to object storage.

## Tests

Backend:

```powershell
Set-Location backend
py -m pip install -e ".[test]"
py -m pytest -q
```

Frontend:

```powershell
Set-Location frontend
npm test
npm run build
```

## Important Notes

- Do not commit `.env`.
- Do not store real secrets in `config`; use `secrets`.
- Set `SECRET_ENCRYPTION_KEY` before saving credentials.
- Back up the database and local output directory in any real environment.
- Add real authentication, authorization, deployment hardening, and queue-based workers before production use.
