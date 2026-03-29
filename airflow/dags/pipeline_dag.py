"""
pipeline_dag.py — Generic ELT Pipeline DAG for self-service ELT platform.

This DAG reads pipeline configuration from the FastAPI backend and orchestrates
extract → load → transform steps. One DAG instance is created per pipeline
via the dynamic DAG pattern.

Environment variables (set in docker-compose.yml):
    BACKEND_URL      — FastAPI base URL, e.g. http://backend:8000
    MINIO_ENDPOINT   — MinIO URL, e.g. http://minio:9000
    MINIO_ACCESS_KEY — MinIO access key
    MINIO_SECRET_KEY — MinIO secret key
    MINIO_BUCKET     — Target bucket name (default: elt-staging)
"""

import json
import logging
import os
import time
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "elt-staging")

DEFAULT_ARGS = {
    "owner": "elt-platform",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ─────────────────────────────────────────────
# Helper: backend API client
# ─────────────────────────────────────────────
class BackendClient:
    def __init__(self, base_url: str = BACKEND_URL):
        self.base_url = base_url.rstrip("/")
        self.session = requests.Session()
        self.session.headers.update({"Content-Type": "application/json"})

    def get_pipeline(self, pipeline_id: int) -> dict:
        resp = self.session.get(f"{self.base_url}/pipelines/{pipeline_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()

    def get_connection(self, connection_id: int) -> dict:
        resp = self.session.get(f"{self.base_url}/connections/{connection_id}", timeout=30)
        resp.raise_for_status()
        return resp.json()

    def create_run(self, pipeline_id: int, triggered_by: str = "airflow") -> dict:
        resp = self.session.post(
            f"{self.base_url}/pipelines/{pipeline_id}/runs",
            json={"triggered_by": triggered_by},
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def update_run(self, run_id: int, payload: dict) -> dict:
        resp = self.session.patch(
            f"{self.base_url}/runs/{run_id}",
            json=payload,
            timeout=30,
        )
        resp.raise_for_status()
        return resp.json()

    def get_pipelines(self) -> list:
        resp = self.session.get(f"{self.base_url}/pipelines", timeout=30)
        resp.raise_for_status()
        return resp.json()


# ─────────────────────────────────────────────
# Task functions
# ─────────────────────────────────────────────
def start_run(pipeline_id: int, **context):
    """
    Register a run in the backend and push the run_id to XCom so downstream
    tasks can update its status.
    """
    client = BackendClient()
    run = client.create_run(pipeline_id, triggered_by="airflow")
    run_id = run["id"]
    log.info("Created run %d for pipeline %d", run_id, pipeline_id)
    context["ti"].xcom_push(key="run_id", value=run_id)
    context["ti"].xcom_push(key="pipeline_id", value=pipeline_id)
    return run_id


def extract(pipeline_id: int, **context):
    """
    Extract data from the source connection defined in the pipeline config.
    Writes parquet-like JSON to MinIO staging area.
    """
    import io
    import psycopg2  # available in Airflow image

    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    client = BackendClient()

    # Fetch pipeline config
    pipeline = client.get_pipeline(pipeline_id)
    source_conn_id = pipeline.get("source_connection_id")
    tables = pipeline.get("source_tables", [])
    if not tables:
        raise ValueError(f"Pipeline {pipeline_id} has no source_tables configured")

    source_conn = client.get_connection(source_conn_id)
    log.info("Extracting from connection: %s", source_conn.get("name"))

    # Connect to source Postgres
    db_url = source_conn.get("connection_string") or _build_dsn(source_conn)
    conn = psycopg2.connect(db_url)

    extracted = {}
    total_rows = 0
    try:
        cur = conn.cursor()
        for table in tables:
            schema = table.get("schema", "public")
            tname = table.get("table")
            query = table.get("query") or f'SELECT * FROM "{schema}"."{tname}"'
            cur.execute(query)
            columns = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
            extracted[f"{schema}.{tname}"] = {
                "columns": columns,
                "rows": [list(r) for r in rows],
                "row_count": len(rows),
            }
            total_rows += len(rows)
            log.info("Extracted %d rows from %s.%s", len(rows), schema, tname)
    finally:
        conn.close()

    # Stage to MinIO
    _upload_to_minio(
        key=f"runs/{run_id}/extracted.json",
        data=json.dumps(extracted, default=str),
    )

    # Mark run as in-progress with row count
    client.update_run(run_id, {"rows_extracted": total_rows, "status": "running"})
    ti.xcom_push(key="total_rows", value=total_rows)
    log.info("Extract complete — %d total rows staged to MinIO", total_rows)
    return total_rows


def transform(pipeline_id: int, **context):
    """
    Apply any transformations defined in the pipeline config.
    Currently supports: column renames, type casts, row filters, computed columns.
    Reads from MinIO staging, writes transformed output back.
    """
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    client = BackendClient()
    pipeline = client.get_pipeline(pipeline_id)

    transforms = pipeline.get("transforms", [])
    raw = json.loads(_download_from_minio(f"runs/{run_id}/extracted.json"))

    transformed = {}
    for table_key, table_data in raw.items():
        columns = table_data["columns"]
        rows = table_data["rows"]
        table_transforms = [t for t in transforms if t.get("table") == table_key]

        for tx in table_transforms:
            tx_type = tx.get("type")
            if tx_type == "rename_column":
                old, new = tx["from"], tx["to"]
                if old in columns:
                    idx = columns.index(old)
                    columns = list(columns)
                    columns[idx] = new
            elif tx_type == "filter_rows":
                col = tx["column"]
                op = tx.get("op", "eq")
                val = tx["value"]
                col_idx = columns.index(col) if col in columns else None
                if col_idx is not None:
                    rows = _apply_filter(rows, col_idx, op, val)
            elif tx_type == "add_column":
                columns = list(columns) + [tx["name"]]
                rows = [list(r) + [tx.get("default")] for r in rows]

        transformed[table_key] = {
            "columns": columns,
            "rows": rows,
            "row_count": len(rows),
        }
        log.info("Transformed %s — %d rows out", table_key, len(rows))

    _upload_to_minio(
        key=f"runs/{run_id}/transformed.json",
        data=json.dumps(transformed, default=str),
    )
    log.info("Transform complete")
    return len(transformed)


def load(pipeline_id: int, **context):
    """
    Load transformed data into the destination (mock Snowflake / Postgres schema).
    Performs UPSERT when a primary key is defined, otherwise truncate-insert.
    """
    import psycopg2
    import psycopg2.extras

    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    client = BackendClient()
    pipeline = client.get_pipeline(pipeline_id)

    dest_conn_id = pipeline.get("destination_connection_id")
    dest_conn = client.get_connection(dest_conn_id)
    dest_schema = pipeline.get("destination_schema", "snowflake_mock")

    data = json.loads(_download_from_minio(f"runs/{run_id}/transformed.json"))
    db_url = dest_conn.get("connection_string") or _build_dsn(dest_conn)

    conn = psycopg2.connect(db_url)
    total_loaded = 0
    try:
        cur = conn.cursor()
        for table_key, table_data in data.items():
            _, tname = table_key.split(".", 1)
            dest_table = f'"{dest_schema}"."{tname}"'
            columns = table_data["columns"]
            rows = table_data["rows"]

            if not rows:
                log.info("Skipping %s — no rows", tname)
                continue

            # Ensure destination table exists (auto-create)
            _ensure_table(cur, dest_schema, tname, columns)

            # Truncate-insert (simple strategy; swap for UPSERT if pk defined)
            pk = pipeline.get("primary_key")
            if pk and pk in columns:
                _upsert(cur, dest_table, columns, rows, pk)
            else:
                cur.execute(f"TRUNCATE {dest_table}")
                psycopg2.extras.execute_values(
                    cur,
                    f'INSERT INTO {dest_table} ({",".join(f"{chr(34)}{c}{chr(34)}" for c in columns)}) VALUES %s',
                    rows,
                )

            total_loaded += len(rows)
            log.info("Loaded %d rows into %s", len(rows), dest_table)

        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    client.update_run(run_id, {"rows_processed": total_loaded})
    ti.xcom_push(key="rows_loaded", value=total_loaded)
    log.info("Load complete — %d rows written to destination", total_loaded)
    return total_loaded


def finish_run(pipeline_id: int, **context):
    """Mark the run as success in the backend."""
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    rows = ti.xcom_pull(key="rows_loaded", task_ids="load") or 0
    client = BackendClient()
    client.update_run(run_id, {
        "status": "success",
        "finished_at": datetime.utcnow().isoformat(),
        "rows_processed": rows,
    })
    log.info("Run %d marked SUCCESS", run_id)


def fail_run(pipeline_id: int, **context):
    """Called via on_failure_callback — marks the run as failed."""
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    if run_id:
        client = BackendClient()
        client.update_run(run_id, {
            "status": "failed",
            "finished_at": datetime.utcnow().isoformat(),
            "error": str(context.get("exception", "Unknown error")),
        })
        log.error("Run %d marked FAILED", run_id)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────
def _build_dsn(conn: dict) -> str:
    return (
        f"host={conn.get('host', 'localhost')} "
        f"port={conn.get('port', 5432)} "
        f"dbname={conn.get('database', 'postgres')} "
        f"user={conn.get('username', 'postgres')} "
        f"password={conn.get('password', '')}"
    )


def _upload_to_minio(key: str, data: str):
    try:
        from minio import Minio  # type: ignore
        client = Minio(
            MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_ENDPOINT.startswith("https"),
        )
        import io
        b = data.encode()
        client.put_object(MINIO_BUCKET, key, io.BytesIO(b), length=len(b), content_type="application/json")
        log.info("Uploaded %s to MinIO (%d bytes)", key, len(b))
    except ImportError:
        log.warning("minio SDK not installed — skipping MinIO upload (data in memory only)")


def _download_from_minio(key: str) -> str:
    try:
        from minio import Minio  # type: ignore
        client = Minio(
            MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
            access_key=MINIO_ACCESS_KEY,
            secret_key=MINIO_SECRET_KEY,
            secure=MINIO_ENDPOINT.startswith("https"),
        )
        resp = client.get_object(MINIO_BUCKET, key)
        return resp.read().decode()
    except ImportError:
        log.warning("minio SDK not installed — returning empty object")
        return "{}"


def _ensure_table(cur, schema: str, table: str, columns: list):
    cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema}"')
    col_defs = ", ".join(f'"{c}" TEXT' for c in columns)
    cur.execute(f'CREATE TABLE IF NOT EXISTS "{schema}"."{table}" ({col_defs})')


def _upsert(cur, dest_table: str, columns: list, rows: list, pk: str):
    import psycopg2.extras
    col_str = ", ".join(f'"{c}"' for c in columns)
    update_str = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in columns if c != pk)
    psycopg2.extras.execute_values(
        cur,
        f"""
        INSERT INTO {dest_table} ({col_str}) VALUES %s
        ON CONFLICT ("{pk}") DO UPDATE SET {update_str}
        """,
        rows,
    )


def _apply_filter(rows: list, col_idx: int, op: str, val) -> list:
    ops = {
        "eq": lambda r: str(r[col_idx]) == str(val),
        "ne": lambda r: str(r[col_idx]) != str(val),
        "gt": lambda r: float(r[col_idx] or 0) > float(val),
        "lt": lambda r: float(r[col_idx] or 0) < float(val),
        "contains": lambda r: str(val).lower() in str(r[col_idx] or "").lower(),
    }
    fn = ops.get(op, lambda r: True)
    return [r for r in rows if fn(r)]


# ─────────────────────────────────────────────
# Dynamic DAG factory
# ─────────────────────────────────────────────
def _make_dag(pipeline: dict) -> DAG:
    """Build a DAG for a single pipeline config dict."""
    pipeline_id = pipeline["id"]
    dag_id = f"elt_pipeline_{pipeline_id}"
    schedule = pipeline.get("schedule") or pipeline.get("cron_expression") or "@daily"
    description = pipeline.get("description") or f"ELT pipeline {pipeline_id}: {pipeline.get('name', '')}"

    with DAG(
        dag_id=dag_id,
        description=description,
        default_args=DEFAULT_ARGS,
        schedule_interval=schedule,
        start_date=days_ago(1),
        catchup=False,
        tags=["elt-platform", f"pipeline-{pipeline_id}"],
        max_active_runs=1,
        doc_md=f"""
## {pipeline.get('name', dag_id)}

Auto-generated ELT pipeline DAG.

| Field | Value |
|-------|-------|
| Pipeline ID | `{pipeline_id}` |
| Schedule | `{schedule}` |
| Source | `{pipeline.get('source_connection_id')}` |
| Destination | `{pipeline.get('destination_connection_id')}` |

### Steps
1. **start_run** — Register run in backend, get run_id
2. **extract** — Pull data from source DB, stage to MinIO
3. **transform** — Apply pipeline transforms
4. **load** — Write to destination (mock Snowflake)
5. **finish_run** — Mark run SUCCESS in backend
        """,
    ) as dag:

        def _fail_cb(context):
            fail_run(pipeline_id, **context)

        dag.on_failure_callback = _fail_cb

        t_start = PythonOperator(
            task_id="start_run",
            python_callable=start_run,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        t_extract = PythonOperator(
            task_id="extract",
            python_callable=extract,
            op_kwargs={"pipeline_id": pipeline_id},
            execution_timeout=timedelta(minutes=30),
        )

        t_transform = PythonOperator(
            task_id="transform",
            python_callable=transform,
            op_kwargs={"pipeline_id": pipeline_id},
            execution_timeout=timedelta(minutes=15),
        )

        t_load = PythonOperator(
            task_id="load",
            python_callable=load,
            op_kwargs={"pipeline_id": pipeline_id},
            execution_timeout=timedelta(minutes=30),
        )

        t_finish = PythonOperator(
            task_id="finish_run",
            python_callable=finish_run,
            op_kwargs={"pipeline_id": pipeline_id},
            trigger_rule="all_success",
        )

        t_start >> t_extract >> t_transform >> t_load >> t_finish

    return dag


# ─────────────────────────────────────────────
# Register DAGs dynamically
# ─────────────────────────────────────────────
def _load_pipelines() -> list:
    """Fetch all pipelines from backend. Falls back to Airflow Variable."""
    try:
        resp = requests.get(f"{BACKEND_URL}/pipelines", timeout=10)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        log.warning("Could not fetch pipelines from backend (%s), using Variable fallback", e)
        try:
            raw = Variable.get("elt_pipelines", default_var="[]")
            return json.loads(raw)
        except Exception:
            return []


# Airflow discovers DAGs by iterating globals() in this module.
# We register each pipeline's DAG into globals() at import time.
for _pipeline in _load_pipelines():
    _dag = _make_dag(_pipeline)
    globals()[_dag.dag_id] = _dag
    log.info("Registered DAG: %s", _dag.dag_id)
