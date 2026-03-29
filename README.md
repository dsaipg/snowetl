# ELT Platform

A self-service Extract-Load platform with a React UI, FastAPI backend, Airflow orchestration, and Snowflake as the target. Runs fully in Docker Compose for local development; designed to swap individual services for AWS equivalents in production.

---

## Feature Index

| # | Feature | Where |
|---|---|---|
| 1 | [Connections — source + destination](#1-connections) | Connections page |
| 2 | [Schema discovery (source only)](#2-schema-discovery) | Connections page |
| 3 | [Pipeline builder (4-step wizard)](#3-pipeline-builder) | Pipelines page |
| 4 | [Edit connections and pipelines](#4-edit-connections-and-pipelines) | Connections / Pipelines pages |
| 5 | [Manual run trigger](#5-manual-run-trigger) | Pipelines page |
| 6 | [Incremental load (watermark)](#6-incremental-load) | Automatic per pipeline |
| 7 | [Snowflake load via write_pandas](#7-snowflake-load) | Backend + Airflow |
| 8 | [Volume anomaly detection](#8-volume-anomaly-detection) | Monitor page |
| 9 | [Run history + 30-day volume chart](#9-monitoring) | Monitor page |
| 10 | [Pipeline dependencies](#10-pipeline-dependencies) | Dashboard DAG view |
| 11 | [Airflow DAG per pipeline (dynamic)](#11-airflow-dags) | Airflow |
| 12 | [AWS Glue production path](#12-aws-glue-path) | Glue + MWAA |

---

## Architecture

### Local (Docker Compose)

```
Browser
  └─► React/Vite (port 3000)
        └─► FastAPI backend (port 8000)
              ├─► Postgres metadata DB (port 5432)
              │     ├── connections
              │     ├── pipelines
              │     ├── job_runs
              │     ├── schedules
              │     └── schemas (cache)
              │
              ├─► Mock Source Postgres (port 5433)   ← sample customers/orders/products
              │
              └─► Airflow (port 8080)
                    └── elt_pipeline_{id} DAG
                          └── run_elt task
                                ├── psycopg2 → source Postgres
                                └── write_pandas → Snowflake (real account)
                                    (Parquet → Snowflake internal stage → COPY INTO)
```

### Production (AWS)

```
Browser
  └─► React/Vite (CloudFront / S3)
        └─► FastAPI (ECS / Lambda)
              ├─► RDS Postgres (metadata)
              ├─► Airflow (MWAA)
              │     └── elt_pipeline_{id} DAG (ENVIRONMENT=aws)
              │           └── run_elt task
              │                 └── boto3 → starts AWS Glue job
              │                       └── polls until SUCCEEDED / FAILED
              │
              └─► AWS Glue (elt_glue_job.py)
                    ├── JDBC read from source (partitioned, parallel)
                    └── Glue Snowflake connector
                          └── internal stage → COPY INTO Snowflake
```

**ENVIRONMENT variable** controls the execution path:
- `ENVIRONMENT=local` → synchronous write_pandas in backend, or local Airflow path
- `ENVIRONMENT=aws`   → backend triggers Airflow REST API (async), Airflow triggers Glue

---

## Stack

| Service | Local | AWS equivalent | Port |
|---|---|---|---|
| Frontend | React + Vite | CloudFront + S3 | 3000 |
| Backend API | FastAPI + uvicorn | ECS / Lambda + ALB | 8000 |
| Metadata DB | Postgres 15 | RDS Postgres | 5432 |
| Mock source DB | Postgres 15 | Real source system | 5433 |
| Object storage | MinIO | S3 | 9000/9001 |
| Orchestrator | Airflow 2.8 | MWAA | 8080 |
| Target | Real Snowflake account | Same | — |
| Glue job | `glue/elt_glue_job.py` | AWS Glue 4.0 | — |

---

## Feature Details

### 1. Connections

Two types of connections:

**Sources** (PostgreSQL)
- Fields: name, host, port, database, user, password
- Tested on save (psycopg2 connect + close)
- Appear in pipeline source dropdown
- Schema browser available

**Destinations** (Snowflake)
- Fields: name, account identifier, database, schema, warehouse, role, user, password
- Tested on save (snowflake.connector connect + close)
- No auto-create of database or schema — they must exist in Snowflake before use
- Appear in pipeline destination dropdown

### 2. Schema Discovery

Click any source connection to open its schema browser.

- Introspects `information_schema.columns` on the source Postgres
- Results cached in the local `schemas` table
- Columns with names/types matching common watermark patterns are flagged (`is_suggested_watermark`)
- Refresh button re-queries the source and updates cache
- **Blocked for Snowflake destinations** (400 error — not applicable)

### 3. Pipeline Builder

4-step wizard on the Pipelines page:

| Step | What you configure |
|---|---|
| 1 — Source | Source connection, source table, Destination connection, Target database, Target schema |
| 2 — Target | Target table name (fully qualified `DB.SCHEMA.TABLE` shown as preview) |
| 3 — Settings | Watermark column (for incremental), Merge key (for upsert), Cron schedule, Volume alert thresholds, Dependencies |
| 4 — Review | Full summary before save |

Pipeline record stores: `connection_id`, `destination_connection_id`, `source_table`, `watermark_column`, `merge_key_column`, `target_database`, `target_schema_name`, `target_table`, `cron_expression`, `volume_alert_upper_pct`, `volume_alert_lower_pct`.

### 4. Edit Connections and Pipelines

- **Edit connection**: pencil icon on any connection row → pre-filled modal → re-tests connection on save. Password field blank = keep existing value.
- **Edit pipeline**: pencil icon on any pipeline row → pre-filled modal with all fields editable.

### 5. Manual Run Trigger

▶ Run button on any pipeline card.

- `POST /pipelines/{id}/trigger`
- Creates a `job_runs` record with `status = running`
- **Local**: runs synchronously (extract → load), returns when done
- **AWS**: triggers Airflow DAG via REST API, returns `{run_id, status: "queued"}` immediately

### 6. Incremental Load

Each pipeline run uses the `ended_at` timestamp of the last successful run as a watermark:

```sql
SELECT * FROM {source_table} WHERE {watermark_column} >= {last_successful_ended_at}
```

Falls back to a full table scan if:
- No successful run exists yet (defaults to `NOW() - 30 days`)
- The watermark column doesn't exist on the source table

### 7. Snowflake Load

**Local path** (write_pandas):
1. Extract rows from source Postgres via psycopg2
2. Build a pandas DataFrame
3. DELETE existing rows matching the merge key from the target Snowflake table (upsert pattern)
4. `write_pandas()` serialises to Parquet, uploads to Snowflake's internal stage, issues `COPY INTO`
5. Parallel-safe: each `write_pandas` call uses a unique stage file name

Target table is auto-created with correct Snowflake types if it doesn't exist. Database and schema must exist.

Postgres → Snowflake type mapping:

| Postgres | Snowflake |
|---|---|
| integer, serial | INTEGER |
| bigint | BIGINT |
| varchar, character varying | VARCHAR(500) |
| text | TEXT |
| boolean | BOOLEAN |
| numeric, decimal | NUMERIC(18,4) |
| timestamp without time zone | TIMESTAMP |
| timestamp with time zone | TIMESTAMP_TZ |
| date | DATE |
| double precision | FLOAT |

**AWS path** (Glue job):
1. Glue reads pipeline config from backend API
2. Spark JDBC reader with partition pushdown (parallel extraction by merge key)
3. Column names uppercased
4. `glueContext.write_dynamic_frame.from_options(connection_type="snowflake")` handles Parquet staging + COPY INTO internally
5. Glue job calls `PATCH /runs/{run_id}` on success/failure

### 8. Volume Anomaly Detection

After each run the backend checks if `rows_loaded` is outside the expected range based on the last 7 completed runs:

```
avg = mean(last 7 rows_loaded values)
anomaly = rows_loaded < avg * (lower_pct / 100)
       OR rows_loaded > avg * (upper_pct / 100)
```

Default thresholds: lower = 50%, upper = 200%. Configurable per pipeline.

Anomalous runs are flagged with `volume_anomaly_flag = true` and shown with a warning indicator in the Monitor page.

### 9. Monitoring

Monitor page per pipeline shows:
- Run history table: started_at, duration, rows_loaded, status, anomaly flag
- 30-day volume bar chart (daily rows loaded, coloured by status/anomaly)

### 10. Pipeline Dependencies

Pipelines can declare upstream dependencies. The dashboard renders a dependency graph. Dependency metadata is stored in `pipeline_dependencies(pipeline_id, depends_on_id)`.

> Note: Airflow currently runs each DAG independently. Dependency-based DAG chaining (using `TriggerDagRunOperator`) is a planned enhancement.

### 11. Airflow DAGs

`airflow/dags/pipeline_dag.py` dynamically generates one DAG per pipeline at import time by calling `GET /pipelines`.

DAG structure (both environments):
```
start_run → run_elt → finish_run
              ↓
        (on_failure_callback → fail_run)
```

| Task | What it does |
|---|---|
| `start_run` | Adopts `run_id` from `dag_run.conf` (backend-triggered) or creates a new run (scheduled) |
| `run_elt` | Dispatches to local write_pandas or AWS Glue path based on `ENVIRONMENT` |
| `finish_run` | Safety net log; actual status set by `run_elt` or the Glue job callback |
| `fail_run` | `on_failure_callback` — marks run failed in backend |

### 12. AWS Glue Path

`glue/elt_glue_job.py` — generic PySpark script deployed to AWS Glue 4.0.

Job parameters (passed by Airflow):

| Parameter | Example |
|---|---|
| `--pipeline_id` | `3` |
| `--run_id` | `47` |
| `--backend_url` | `https://api.myplatform.com` |

**To deploy:**
1. Upload `glue/elt_glue_job.py` to S3
2. Create a Glue 4.0 job pointing to the S3 path
3. Set env vars in MWAA: `ENVIRONMENT=aws`, `GLUE_JOB_NAME=<your-job-name>`, `AWS_REGION=<region>`
4. Glue IAM role needs: `glue:*`, `s3:*`, network access to source DB (via VPC) and Snowflake

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/health` | Health check, returns environment |
| GET | `/connections` | List all connections |
| POST | `/connections` | Create + test connection |
| PATCH | `/connections/{id}` | Update + re-test connection |
| DELETE | `/connections/{id}` | Soft-delete connection |
| GET | `/connections/{id}/schema` | Discover or return cached schema |
| GET | `/pipelines` | List pipelines with last run stats |
| GET | `/pipelines/{id}` | Get single pipeline |
| POST | `/pipelines` | Create pipeline |
| PATCH | `/pipelines/{id}` | Update pipeline |
| DELETE | `/pipelines/{id}` | Soft-delete pipeline |
| POST | `/pipelines/{id}/trigger` | Trigger a run (local: sync, aws: async) |
| GET | `/pipelines/{id}/runs` | Run history |
| GET | `/pipelines/{id}/volume-history` | 30-day daily volume for chart |
| PATCH | `/runs/{id}` | Update run status/rows/error (used by Airflow + Glue) |
| GET | `/stats` | Dashboard counts |
| GET | `/dependencies` | All pipeline dependency edges |

Full interactive docs: `http://localhost:8000/docs`

---

## Database Schema

```
teams                   — team records (seed: Data Engineering, Analytics, Finance)
users                   — platform users
connections             — source + destination connection configs (credentials stored locally)
pipelines               — pipeline definitions
schedules               — cron schedule per pipeline
pipeline_dependencies   — DAG edges between pipelines
job_runs                — run history with status, rows_loaded, anomaly flag
schemas                 — schema cache (introspected from source connections)
schema_drift_log        — column-level schema change log (future use)
```

---

## Quick Start

```bash
git clone https://github.com/dsaipg/snowetl.git
cd snowetl
docker compose up -d --build
```

| UI | URL | Credentials |
|---|---|---|
| Platform | http://localhost:3000 | — |
| API docs | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |

---

## Project Structure

```
.
├── docker-compose.yml
├── glue/
│   └── elt_glue_job.py          # AWS Glue PySpark job (production path)
├── backend/
│   ├── main.py                  # FastAPI — all API endpoints
│   ├── requirements.txt
│   └── Dockerfile
├── frontend/
│   └── src/
│       ├── api.js               # API client
│       └── pages/
│           ├── Dashboard.jsx    # Stats + dependency graph
│           ├── Connections.jsx  # Connection CRUD + schema browser
│           ├── Pipelines.jsx    # Pipeline CRUD + 4-step wizard
│           └── Monitor.jsx      # Run history + volume chart
├── airflow/
│   └── dags/
│       └── pipeline_dag.py      # Dynamic DAG factory (local + aws paths)
├── postgres-init/
│   ├── 00_create_airflow_db.sh  # Creates separate airflow DB before schema init
│   └── 01_schema.sql            # Metadata schema + seed pipelines/runs
└── mock-data/
    └── 01_init.sql              # Source DB: customers, orders, products
```

---

## Roadmap

- [ ] dbt job UI — define and trigger dbt models as a separate section
- [ ] Pipeline dependency-aware scheduling (chain DAGs via `TriggerDagRunOperator`)
- [ ] Schema drift detection alerts in the UI
- [ ] Cognito auth (`AUTH_ENABLED=true` path)
- [ ] Multi-source support (MySQL, Redshift, BigQuery)

---

## License

MIT
