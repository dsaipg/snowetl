"""
pipeline_dag.py — Generic ELT Pipeline DAG
===========================================
Dynamic DAG factory: one DAG per pipeline fetched from the backend.

Local path  (ENVIRONMENT=local):
  start_run → run_elt  → finish_run
  run_elt connects to source, extracts incrementally, writes to Snowflake
  via write_pandas (Parquet → internal stage → COPY INTO) or mock Postgres.

AWS path (ENVIRONMENT=aws):
  start_run → run_elt  → finish_run
  run_elt submits an AWS Glue job with (pipeline_id, run_id, backend_url),
  then polls until the Glue job run reaches a terminal state.
  The Glue job itself updates rows_loaded via PATCH /runs/{run_id}.

Environment variables (set in docker-compose / MWAA env):
  BACKEND_URL       FastAPI base URL            http://backend:8000
  ENVIRONMENT       local | aws                 local
  AWS_REGION        AWS region for Glue         us-east-1
  GLUE_JOB_NAME     Name of the Glue job        elt-platform-pipeline-job
  AIRFLOW__*        Standard Airflow config
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

# ─── Config ──────────────────────────────────────────────────────────────────
BACKEND_URL = os.getenv("BACKEND_URL", "http://backend:8000")
ENVIRONMENT = os.getenv("ENVIRONMENT", "local")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
GLUE_JOB_NAME = os.getenv("GLUE_JOB_NAME", "elt-platform-pipeline-job")

DEFAULT_ARGS = {
    "owner": "elt-platform",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
    "email_on_retry": False,
}


# ─── Backend API client ───────────────────────────────────────────────────────
class BackendClient:
    def __init__(self, base_url: str = BACKEND_URL):
        self.base = base_url.rstrip("/")
        self._s = requests.Session()
        self._s.headers["Content-Type"] = "application/json"

    def get_pipeline(self, pipeline_id: int) -> dict:
        r = self._s.get(f"{self.base}/pipelines/{pipeline_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    def get_connection(self, conn_id: int) -> dict:
        r = self._s.get(f"{self.base}/connections/{conn_id}", timeout=30)
        r.raise_for_status()
        return r.json()

    def get_pipelines(self) -> list:
        r = self._s.get(f"{self.base}/pipelines", timeout=10)
        r.raise_for_status()
        return r.json()

    def create_run(self, pipeline_id: int, triggered_by: str = "airflow") -> dict:
        r = self._s.post(
            f"{self.base}/pipelines/{pipeline_id}/runs",
            json={"triggered_by": triggered_by},
            timeout=30,
        )
        r.raise_for_status()
        return r.json()

    def update_run(self, run_id: int, payload: dict):
        r = self._s.patch(f"{self.base}/runs/{run_id}", json=payload, timeout=30)
        r.raise_for_status()
        return r.json()


# ─── Task: start_run ─────────────────────────────────────────────────────────
def start_run(pipeline_id: int, **context):
    """
    Use the run_id passed via dag_run.conf (when backend triggered the DAG) or
    create a new run (when Airflow scheduler triggers on cron).
    """
    conf = (context.get("dag_run") and context["dag_run"].conf) or {}
    run_id = conf.get("run_id")

    client = BackendClient()
    if run_id:
        # Backend pre-created this run before calling the Airflow API
        log.info("Using existing run_id=%s from dag_run.conf", run_id)
        client.update_run(run_id, {"status": "running"})
    else:
        run = client.create_run(pipeline_id, triggered_by="airflow_scheduled")
        run_id = run["id"]
        log.info("Created new run_id=%s for pipeline %s", run_id, pipeline_id)

    context["ti"].xcom_push(key="run_id", value=run_id)
    return run_id


# ─── Task: run_elt ───────────────────────────────────────────────────────────
def run_elt(pipeline_id: int, **context):
    """Dispatch to local write_pandas path or AWS Glue path based on ENVIRONMENT."""
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")

    if ENVIRONMENT == "aws":
        _run_via_glue(pipeline_id, run_id)
    else:
        _run_locally(pipeline_id, run_id)


# ─── Local ELT path (write_pandas) ───────────────────────────────────────────
def _run_locally(pipeline_id: int, run_id: int):
    """
    Local demo path:
      1. Connect to source Postgres via psycopg2
      2. Extract rows incrementally by watermark (falls back to full table)
      3. Write to Snowflake via write_pandas (Parquet → internal stage → COPY INTO)
         or to the local mock Postgres snowflake_target schema if no Snowflake destination
    """
    import psycopg2
    import psycopg2.extras

    client = BackendClient()
    pipeline = client.get_pipeline(pipeline_id)
    src_conn = client.get_connection(pipeline["connection_id"])

    dest_conn = None
    if pipeline.get("destination_connection_id"):
        dest_conn = client.get_connection(pipeline["destination_connection_id"])

    # ── Watermark: last successful run ──────────────────────────────────────
    from datetime import datetime, timedelta
    last_run_resp = requests.get(
        f"{BACKEND_URL}/pipelines/{pipeline_id}/runs?limit=1&status=complete",
        timeout=10,
    )
    last_runs = last_run_resp.json() if last_run_resp.ok else []
    last_watermark = (
        last_runs[0]["ended_at"]
        if last_runs and last_runs[0].get("ended_at")
        else (datetime.now() - timedelta(days=30)).isoformat()
    )

    # ── Extract ─────────────────────────────────────────────────────────────
    src = psycopg2.connect(
        host=src_conn["host"], port=src_conn["port"],
        dbname=src_conn["db_name"], user=src_conn["db_user"],
        password=src_conn["db_password"],
    )
    try:
        with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            watermark_col = pipeline.get("watermark_column") or "updated_at"
            try:
                cur.execute(
                    f"SELECT * FROM {pipeline['source_table']} WHERE {watermark_col} >= %s",
                    (last_watermark,),
                )
            except Exception:
                cur.execute(f"SELECT * FROM {pipeline['source_table']}")
            extracted_rows = cur.fetchall()

            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (pipeline["source_table"],))
            col_defs = cur.fetchall()
    finally:
        src.close()

    rows_extracted = len(extracted_rows)
    log.info("Extracted %d rows from %s", rows_extracted, pipeline["source_table"])

    # ── Load ────────────────────────────────────────────────────────────────
    if extracted_rows:
        if dest_conn and dest_conn["db_type"] == "snowflake":
            _write_pandas_to_snowflake(dest_conn, pipeline, col_defs, extracted_rows)
        else:
            _write_to_mock_postgres(pipeline, col_defs, extracted_rows)

    # ── Volume anomaly check ─────────────────────────────────────────────────
    history_resp = requests.get(
        f"{BACKEND_URL}/pipelines/{pipeline_id}/runs?limit=7", timeout=10
    )
    history = history_resp.json() if history_resp.ok else []
    anomaly = False
    if history:
        avg = sum(r.get("rows_loaded") or 0 for r in history) / len(history)
        upper = pipeline.get("volume_alert_upper_pct", 200)
        lower = pipeline.get("volume_alert_lower_pct", 50)
        if avg > 0:
            pct = (rows_extracted / avg) * 100
            anomaly = pct < lower or pct > upper

    client.update_run(run_id, {
        "status": "complete",
        "rows_loaded": rows_extracted,
        "volume_anomaly_flag": anomaly,
        "ended_at": datetime.utcnow().isoformat(),
        "phase_completed": 3,
    })
    log.info("Local ELT complete — %d rows, anomaly=%s", rows_extracted, anomaly)


TYPE_MAP = {
    "integer": "INTEGER", "bigint": "BIGINT", "serial": "INTEGER",
    "varchar": "VARCHAR(500)", "character varying": "VARCHAR(500)",
    "text": "TEXT", "boolean": "BOOLEAN",
    "numeric": "NUMERIC(18,4)", "decimal": "NUMERIC(18,4)",
    "timestamp without time zone": "TIMESTAMP",
    "timestamp with time zone": "TIMESTAMP_TZ",
    "date": "DATE", "double precision": "FLOAT",
}


def _write_pandas_to_snowflake(dest_conn, pipeline, col_defs, extracted_rows):
    import pandas as pd
    import snowflake.connector
    from snowflake.connector.pandas_tools import write_pandas

    database = (pipeline.get("target_database") or dest_conn["db_name"]).upper()
    target_schema = (pipeline.get("target_schema_name") or dest_conn.get("snowflake_schema") or "PUBLIC").upper()
    target_table = pipeline["target_table"].upper()
    merge_key = (pipeline.get("merge_key_column") or "id").upper()

    cols = [c["column_name"] for c in col_defs]
    df = pd.DataFrame([{c: row[c] for c in cols} for row in extracted_rows])
    df.columns = [c.upper() for c in df.columns]

    sf = snowflake.connector.connect(
        account=dest_conn["host"],
        user=dest_conn["db_user"],
        password=dest_conn["db_password"],
        database=dest_conn["db_name"],
        schema=dest_conn.get("snowflake_schema") or "PUBLIC",
        warehouse=dest_conn.get("snowflake_warehouse"),
        role=dest_conn.get("snowflake_role"),
    )
    try:
        with sf.cursor() as cur:
            if dest_conn.get("snowflake_warehouse"):
                cur.execute(f"USE WAREHOUSE {dest_conn['snowflake_warehouse']}")
            col_sql = ", ".join(
                f"{c['column_name'].upper()} {TYPE_MAP.get(c['data_type'], 'TEXT')}"
                for c in col_defs
            )
            cur.execute(
                f"CREATE TABLE IF NOT EXISTS {database}.{target_schema}.{target_table} ({col_sql})"
            )
            if merge_key in df.columns:
                keys = df[merge_key].tolist()
                placeholders = ",".join(["%s"] * len(keys))
                cur.execute(
                    f"DELETE FROM {database}.{target_schema}.{target_table} "
                    f"WHERE {merge_key} IN ({placeholders})",
                    keys,
                )
        success, nchunks, nrows, _ = write_pandas(
            conn=sf, df=df,
            table_name=target_table, database=database, schema=target_schema,
            auto_create_table=False, overwrite=False,
        )
        if not success:
            raise Exception(f"write_pandas failed after {nchunks} chunks / {nrows} rows")
        log.info("write_pandas: %d rows → %s.%s.%s", nrows, database, target_schema, target_table)
    finally:
        sf.close()


def _write_to_mock_postgres(pipeline, col_defs, extracted_rows):
    import psycopg2

    mock = psycopg2.connect(
        host=os.getenv("DB_HOST", "postgres"),
        port=int(os.getenv("DB_PORT", "5432")),
        dbname=os.getenv("DB_NAME", "etlplatform"),
        user=os.getenv("DB_USER", "etluser"),
        password=os.getenv("DB_PASSWORD", "etlpassword"),
    )
    target_schema = "snowflake_target"
    target_table = pipeline["target_table"]
    merge_key = pipeline.get("merge_key_column") or "id"
    cols = [c["column_name"] for c in col_defs]
    col_sql = ", ".join(f"{c['column_name']} {TYPE_MAP.get(c['data_type'], 'TEXT')}" for c in col_defs)
    col_names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_set = ", ".join(f"{c} = EXCLUDED.{c}" for c in cols if c != merge_key)

    with mock.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ({col_sql})")
        for row in extracted_rows:
            vals = [row[c] for c in cols]
            cur.execute(
                f"INSERT INTO {target_schema}.{target_table} ({col_names}) VALUES ({placeholders}) "
                f"ON CONFLICT ({merge_key}) DO UPDATE SET {update_set}",
                vals,
            )
    mock.commit()
    mock.close()


# ─── AWS Glue path ────────────────────────────────────────────────────────────
def _run_via_glue(pipeline_id: int, run_id: int):
    """
    Submit the Glue job elt_glue_job.py with pipeline_id / run_id as params,
    then poll the Glue job run until it reaches a terminal state.
    The Glue job updates rows_loaded itself via PATCH /runs/{run_id}.
    This function only needs to mark the final status on failure.
    """
    import boto3

    glue = boto3.client("glue", region_name=AWS_REGION)

    job_args = {
        "--pipeline_id": str(pipeline_id),
        "--run_id": str(run_id),
        "--backend_url": BACKEND_URL,
    }

    response = glue.start_job_run(JobName=GLUE_JOB_NAME, Arguments=job_args)
    glue_run_id = response["JobRunId"]
    log.info("Submitted Glue job %s run %s for pipeline %s / run %s",
             GLUE_JOB_NAME, glue_run_id, pipeline_id, run_id)

    # ── Poll until terminal ──────────────────────────────────────────────────
    terminal_states = {"SUCCEEDED", "FAILED", "ERROR", "TIMEOUT", "STOPPED"}
    poll_interval = 20  # seconds

    while True:
        time.sleep(poll_interval)
        detail = glue.get_job_run(JobName=GLUE_JOB_NAME, RunId=glue_run_id)
        state = detail["JobRun"]["JobRunState"]
        log.info("Glue run %s state: %s", glue_run_id, state)

        if state in terminal_states:
            if state != "SUCCEEDED":
                error_msg = detail["JobRun"].get("ErrorMessage", f"Glue job {state}")
                # Glue job should have already called back; this is a safety net
                BackendClient().update_run(run_id, {
                    "status": "failed",
                    "error_message": error_msg,
                    "ended_at": datetime.utcnow().isoformat(),
                })
                raise Exception(f"Glue job {GLUE_JOB_NAME}/{glue_run_id} {state}: {error_msg}")
            log.info("Glue job SUCCEEDED for pipeline %s / run %s", pipeline_id, run_id)
            break

        # Increase poll interval gradually to reduce API calls for long jobs
        poll_interval = min(poll_interval + 10, 60)


# ─── Task: finish_run ────────────────────────────────────────────────────────
def finish_run(pipeline_id: int, **context):
    """Mark run as complete (for scheduled local runs where status may not be set yet)."""
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    # For AWS path, Glue job already updated status; this is a no-op safety net.
    # For local path, _run_locally already updated status.
    log.info("finish_run: pipeline %s / run %s complete", pipeline_id, run_id)


def fail_run(pipeline_id: int, **context):
    """on_failure_callback — mark run as failed in the backend."""
    ti = context["ti"]
    run_id = ti.xcom_pull(key="run_id", task_ids="start_run")
    if not run_id:
        return
    exc = context.get("exception")
    BackendClient().update_run(run_id, {
        "status": "failed",
        "error_message": str(exc) if exc else "Airflow task failure",
        "ended_at": datetime.utcnow().isoformat(),
    })
    log.error("Run %s marked FAILED (pipeline %s)", run_id, pipeline_id)


# ─── Dynamic DAG factory ──────────────────────────────────────────────────────
def _make_dag(pipeline: dict) -> DAG:
    pipeline_id = pipeline["id"]
    dag_id = f"elt_pipeline_{pipeline_id}"
    schedule = pipeline.get("cron_expression") or pipeline.get("schedule") or "@daily"
    description = f"ELT pipeline {pipeline_id}: {pipeline.get('name', '')}"

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

Auto-generated ELT pipeline DAG — `ENVIRONMENT={ENVIRONMENT}`

| Field | Value |
|---|---|
| Pipeline ID | `{pipeline_id}` |
| Schedule | `{schedule}` |
| Mode | `{"AWS Glue" if ENVIRONMENT == "aws" else "local write_pandas"}` |

### Steps
1. **start_run** — Register or adopt run in backend
2. **run_elt** — Extract + Load ({'Glue job' if ENVIRONMENT == 'aws' else 'local write_pandas'})
3. **finish_run** — Confirm success
        """,
    ) as dag:

        def _fail_cb(ctx):
            fail_run(pipeline_id, **ctx)

        dag.on_failure_callback = _fail_cb

        t_start = PythonOperator(
            task_id="start_run",
            python_callable=start_run,
            op_kwargs={"pipeline_id": pipeline_id},
        )

        t_run = PythonOperator(
            task_id="run_elt",
            python_callable=run_elt,
            op_kwargs={"pipeline_id": pipeline_id},
            execution_timeout=timedelta(hours=2),  # Glue jobs can be long
        )

        t_finish = PythonOperator(
            task_id="finish_run",
            python_callable=finish_run,
            op_kwargs={"pipeline_id": pipeline_id},
            trigger_rule="all_success",
        )

        t_start >> t_run >> t_finish

    return dag


# ─── Register DAGs ────────────────────────────────────────────────────────────
def _load_pipelines() -> list:
    try:
        r = requests.get(f"{BACKEND_URL}/pipelines", timeout=10)
        r.raise_for_status()
        return r.json()
    except Exception as exc:
        log.warning("Could not fetch pipelines from backend (%s) — using Variable fallback", exc)
        try:
            return json.loads(Variable.get("elt_pipelines", default_var="[]"))
        except Exception:
            return []


for _p in _load_pipelines():
    _d = _make_dag(_p)
    globals()[_d.dag_id] = _d
    log.info("Registered DAG: %s (env=%s)", _d.dag_id, ENVIRONMENT)
