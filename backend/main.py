from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import psycopg2
import psycopg2.extras
import os
import json
import time
import random
from datetime import datetime, timedelta
from typing import Optional
from pydantic import BaseModel

# ─── Config from env vars ─────────────────────────────────────────────
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "etlplatform")
DB_USER = os.getenv("DB_USER", "etluser")
DB_PASSWORD = os.getenv("DB_PASSWORD", "etlpassword")

AIRFLOW_ENDPOINT = os.getenv("AIRFLOW_ENDPOINT", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

ENVIRONMENT = os.getenv("ENVIRONMENT", "local")

# ─── DB helpers ──────────────────────────────────────────────────────
def get_db():
    return psycopg2.connect(
        host=DB_HOST, port=DB_PORT, dbname=DB_NAME,
        user=DB_USER, password=DB_PASSWORD
    )

def get_source_db(host, port, dbname, user, password):
    return psycopg2.connect(
        host=host, port=port, dbname=dbname,
        user=user, password=password
    )

def get_snowflake_conn(conn):
    import snowflake.connector
    return snowflake.connector.connect(
        account=conn['host'],
        user=conn['db_user'],
        password=conn['db_password'],
        database=conn['db_name'],
        schema=conn.get('snowflake_schema') or 'PUBLIC',
        warehouse=conn['snowflake_warehouse'],
        role=conn['snowflake_role'],
    )

def query(sql, params=None, db=None):
    conn = db or get_db()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(sql, params)
            conn.commit()
            try:
                return cur.fetchall()
            except:
                return []
    finally:
        if not db:
            conn.close()

# ─── App ─────────────────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    yield

app = FastAPI(title="ELT Platform API", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── Models ──────────────────────────────────────────────────────────
class ConnectionCreate(BaseModel):
    name: str
    db_type: str
    host: str
    port: int = 443
    db_name: str
    db_user: str
    db_password: str
    snowflake_warehouse: Optional[str] = None
    snowflake_role: Optional[str] = None
    snowflake_schema: Optional[str] = None

class PipelineCreate(BaseModel):
    name: str
    connection_id: int
    destination_connection_id: Optional[int] = None
    source_table: str
    watermark_column: Optional[str] = None
    merge_key_column: Optional[str] = None
    target_database: Optional[str] = None
    target_schema_name: Optional[str] = None
    target_table: str
    cron_expression: str = "0 2 * * *"
    depends_on: list[int] = []
    volume_alert_upper_pct: int = 200
    volume_alert_lower_pct: int = 50

# ─── Health ──────────────────────────────────────────────────────────
@app.get("/health")
def health():
    return {"status": "ok", "environment": ENVIRONMENT}

# ─── Connections ─────────────────────────────────────────────────────
@app.get("/connections")
def list_connections():
    rows = query("""
        SELECT id, name, db_type, host, port, db_name,
               snowflake_warehouse, snowflake_role, snowflake_schema,
               created_at, is_active
        FROM connections ORDER BY created_at DESC
    """)
    return [dict(r) for r in rows]

@app.post("/connections")
def create_connection(body: ConnectionCreate):
    # Test connection
    try:
        if body.db_type == 'snowflake':
            import snowflake.connector
            test = snowflake.connector.connect(
                account=body.host,
                user=body.db_user,
                password=body.db_password,
                database=body.db_name,
                warehouse=body.snowflake_warehouse,
                role=body.snowflake_role,
            )
            test.close()
        else:
            test = psycopg2.connect(
                host=body.host, port=body.port,
                dbname=body.db_name, user=body.db_user,
                password=body.db_password, connect_timeout=5
            )
            test.close()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")

    rows = query(
        """INSERT INTO connections
               (name, db_type, host, port, db_name, db_user, db_password,
                snowflake_warehouse, snowflake_role, snowflake_schema)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (body.name, body.db_type, body.host, body.port, body.db_name,
         body.db_user, body.db_password,
         body.snowflake_warehouse, body.snowflake_role, body.snowflake_schema)
    )
    return {"id": rows[0]["id"], "message": "Connection created"}

@app.patch("/connections/{conn_id}")
def update_connection(conn_id: int, body: ConnectionCreate):
    try:
        if body.db_type == 'snowflake':
            import snowflake.connector
            test = snowflake.connector.connect(
                account=body.host, user=body.db_user, password=body.db_password,
                database=body.db_name, warehouse=body.snowflake_warehouse, role=body.snowflake_role,
            )
            test.close()
        else:
            test = psycopg2.connect(
                host=body.host, port=body.port, dbname=body.db_name,
                user=body.db_user, password=body.db_password, connect_timeout=5
            )
            test.close()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Connection failed: {str(e)}")

    query("""
        UPDATE connections SET
            name=%s, db_type=%s, host=%s, port=%s, db_name=%s,
            db_user=%s, db_password=%s,
            snowflake_warehouse=%s, snowflake_role=%s, snowflake_schema=%s
        WHERE id=%s
    """, (body.name, body.db_type, body.host, body.port, body.db_name,
          body.db_user, body.db_password,
          body.snowflake_warehouse, body.snowflake_role, body.snowflake_schema,
          conn_id))
    return {"message": "Connection updated"}

@app.delete("/connections/{conn_id}")
def delete_connection(conn_id: int):
    query("UPDATE connections SET is_active = false WHERE id = %s", (conn_id,))
    return {"message": "Connection deleted"}

# ─── Schema Discovery ─────────────────────────────────────────────────
@app.get("/connections/{conn_id}/schema")
def discover_schema(conn_id: int, refresh: bool = False):
    conns = query("SELECT * FROM connections WHERE id = %s", (conn_id,))
    if not conns:
        raise HTTPException(status_code=404, detail="Connection not found")
    conn = dict(conns[0])

    if conn['db_type'] == 'snowflake':
        raise HTTPException(status_code=400, detail="Schema discovery is only available for source connections")

    if not refresh:
        cached = query(
            "SELECT * FROM schemas WHERE connection_id = %s ORDER BY table_name, column_name",
            (conn_id,)
        )
        if cached:
            return _group_schema(cached)

    try:
        src = get_source_db(
            host=conn["host"], port=conn["port"],
            dbname=conn["db_name"], user=conn["db_user"],
            password=conn["db_password"]
        )
        with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT table_name, column_name, data_type
                FROM information_schema.columns
                WHERE table_schema = 'public'
                AND table_name NOT LIKE 'pg_%'
                ORDER BY table_name, ordinal_position
            """)
            columns = cur.fetchall()
        src.close()
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Schema discovery failed: {str(e)}")

    query("DELETE FROM schemas WHERE connection_id = %s", (conn_id,))

    watermark_names = {'updated_at', 'modified_at', 'last_modified', 'update_date', 'changed_at', 'modified_date'}
    watermark_types = {'timestamp', 'timestamp without time zone', 'timestamp with time zone', 'date'}

    for col in columns:
        is_watermark = (
            col['column_name'].lower() in watermark_names or
            (col['data_type'].lower() in watermark_types and 'update' in col['column_name'].lower())
        )
        query(
            """INSERT INTO schemas (connection_id, table_name, column_name, data_type, is_suggested_watermark)
               VALUES (%s, %s, %s, %s, %s)""",
            (conn_id, col['table_name'], col['column_name'], col['data_type'], is_watermark)
        )

    cached = query(
        "SELECT * FROM schemas WHERE connection_id = %s ORDER BY table_name, column_name",
        (conn_id,)
    )
    return _group_schema(cached)

def _group_schema(rows):
    tables = {}
    for row in rows:
        row = dict(row)
        t = row['table_name']
        if t not in tables:
            tables[t] = {"table_name": t, "columns": []}
        tables[t]["columns"].append({
            "name": row["column_name"],
            "type": row["data_type"],
            "is_suggested_watermark": row["is_suggested_watermark"]
        })
    return list(tables.values())

# ─── Pipelines ────────────────────────────────────────────────────────
@app.get("/pipelines")
def list_pipelines():
    rows = query("""
        SELECT p.*, c.name as connection_name, c.db_type,
               s.cron_expression,
               dc.name as destination_name, dc.db_type as destination_db_type,
               (SELECT COUNT(*) FROM job_runs jr WHERE jr.pipeline_id = p.id) as total_runs,
               (SELECT status FROM job_runs jr WHERE jr.pipeline_id = p.id ORDER BY started_at DESC LIMIT 1) as last_status,
               (SELECT started_at FROM job_runs jr WHERE jr.pipeline_id = p.id ORDER BY started_at DESC LIMIT 1) as last_run_at,
               (SELECT rows_loaded FROM job_runs jr WHERE jr.pipeline_id = p.id ORDER BY started_at DESC LIMIT 1) as last_rows_loaded
        FROM pipelines p
        LEFT JOIN connections c ON p.connection_id = c.id
        LEFT JOIN connections dc ON p.destination_connection_id = dc.id
        LEFT JOIN schedules s ON s.pipeline_id = p.id
        WHERE p.is_active = true
        ORDER BY p.created_at DESC
    """)
    result = []
    for row in rows:
        r = dict(row)
        deps = query("SELECT depends_on_id FROM pipeline_dependencies WHERE pipeline_id = %s", (r['id'],))
        r['depends_on'] = [d['depends_on_id'] for d in deps]
        result.append(r)
    return result

@app.get("/pipelines/{pipeline_id}")
def get_pipeline(pipeline_id: int):
    rows = query("SELECT * FROM pipelines WHERE id = %s", (pipeline_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    r = dict(rows[0])
    deps = query("SELECT depends_on_id FROM pipeline_dependencies WHERE pipeline_id = %s", (pipeline_id,))
    r['depends_on'] = [d['depends_on_id'] for d in deps]
    schedule = query("SELECT * FROM schedules WHERE pipeline_id = %s", (pipeline_id,))
    r['schedule'] = dict(schedule[0]) if schedule else None
    return r

@app.post("/pipelines")
def create_pipeline(body: PipelineCreate):
    dag_id = f"pipeline_{int(time.time())}"

    rows = query(
        """INSERT INTO pipelines
               (name, connection_id, destination_connection_id, source_table,
                watermark_column, merge_key_column,
                target_database, target_schema_name,
                target_table, volume_alert_upper_pct, volume_alert_lower_pct, dag_id)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING id""",
        (body.name, body.connection_id, body.destination_connection_id,
         body.source_table, body.watermark_column, body.merge_key_column,
         body.target_database, body.target_schema_name,
         body.target_table, body.volume_alert_upper_pct, body.volume_alert_lower_pct, dag_id)
    )
    pipeline_id = rows[0]['id']

    query(
        "INSERT INTO schedules (pipeline_id, cron_expression) VALUES (%s, %s)",
        (pipeline_id, body.cron_expression)
    )

    for dep_id in body.depends_on:
        query(
            "INSERT INTO pipeline_dependencies (pipeline_id, depends_on_id) VALUES (%s, %s)",
            (pipeline_id, dep_id)
        )

    return {"id": pipeline_id, "message": "Pipeline created", "dag_id": dag_id}

@app.patch("/pipelines/{pipeline_id}")
def update_pipeline(pipeline_id: int, body: PipelineCreate):
    query("""
        UPDATE pipelines SET
            name = %s,
            destination_connection_id = %s,
            watermark_column = %s,
            merge_key_column = %s,
            target_database = %s,
            target_schema_name = %s,
            target_table = %s,
            volume_alert_upper_pct = %s,
            volume_alert_lower_pct = %s
        WHERE id = %s
    """, (body.name, body.destination_connection_id, body.watermark_column,
          body.merge_key_column, body.target_database, body.target_schema_name,
          body.target_table, body.volume_alert_upper_pct, body.volume_alert_lower_pct,
          pipeline_id))
    query("UPDATE schedules SET cron_expression = %s WHERE pipeline_id = %s",
          (body.cron_expression, pipeline_id))
    return {"message": "Pipeline updated"}

@app.delete("/pipelines/{pipeline_id}")
def delete_pipeline(pipeline_id: int):
    query("UPDATE pipelines SET is_active = false WHERE id = %s", (pipeline_id,))
    return {"message": "Pipeline deleted"}

# ─── Job Runs ─────────────────────────────────────────────────────────
@app.get("/pipelines/{pipeline_id}/runs")
def get_runs(pipeline_id: int, limit: int = 30):
    rows = query(
        "SELECT * FROM job_runs WHERE pipeline_id = %s ORDER BY started_at DESC LIMIT %s",
        (pipeline_id, limit)
    )
    return [dict(r) for r in rows]

class RunStatusUpdate(BaseModel):
    status: Optional[str] = None
    rows_loaded: Optional[int] = None
    error_message: Optional[str] = None
    volume_anomaly_flag: Optional[bool] = None
    phase_completed: Optional[int] = None
    ended_at: Optional[str] = None

@app.patch("/runs/{run_id}")
def update_run(run_id: int, body: RunStatusUpdate):
    """
    Update a job run's status, rows_loaded, error_message, etc.
    Called by Airflow tasks and the Glue job callback.
    """
    existing = query("SELECT id FROM job_runs WHERE id = %s", (run_id,))
    if not existing:
        raise HTTPException(status_code=404, detail="Run not found")

    fields, vals = [], []
    if body.status is not None:
        fields.append("status = %s"); vals.append(body.status)
    if body.rows_loaded is not None:
        fields.append("rows_loaded = %s"); vals.append(body.rows_loaded)
    if body.error_message is not None:
        fields.append("error_message = %s"); vals.append(body.error_message[:2000])
    if body.volume_anomaly_flag is not None:
        fields.append("volume_anomaly_flag = %s"); vals.append(body.volume_anomaly_flag)
    if body.phase_completed is not None:
        fields.append("phase_completed = %s"); vals.append(body.phase_completed)

    # Auto-set ended_at when transitioning to a terminal status
    terminal = body.status in ("complete", "failed", "success")
    if terminal:
        fields.append("ended_at = NOW()")

    if not fields:
        return {"message": "No fields to update"}

    vals.append(run_id)
    query(f"UPDATE job_runs SET {', '.join(fields)} WHERE id = %s", vals)
    return {"message": "Run updated", "run_id": run_id}

@app.post("/pipelines/{pipeline_id}/trigger")
def trigger_pipeline(pipeline_id: int):
    pipeline_rows = query("SELECT * FROM pipelines WHERE id = %s", (pipeline_id,))
    if not pipeline_rows:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    pipeline = dict(pipeline_rows[0])

    conn_rows = query("SELECT * FROM connections WHERE id = %s", (pipeline['connection_id'],))
    if not conn_rows:
        raise HTTPException(status_code=404, detail="Source connection not found")
    src_conn = dict(conn_rows[0])

    dest_conn = None
    if pipeline.get('destination_connection_id'):
        dest_rows = query("SELECT * FROM connections WHERE id = %s", (pipeline['destination_connection_id'],))
        if dest_rows:
            dest_conn = dict(dest_rows[0])

    run_rows = query(
        """INSERT INTO job_runs (pipeline_id, status, started_at, phase_completed)
           VALUES (%s, 'running', NOW(), 0) RETURNING id""",
        (pipeline_id,)
    )
    run_id = run_rows[0]['id']

    # ── AWS path: hand off to Airflow, return immediately ─────────────
    if ENVIRONMENT == 'aws':
        dag_id = pipeline.get('dag_id') or f'elt_pipeline_{pipeline_id}'
        try:
            import httpx
            resp = httpx.post(
                f"{AIRFLOW_ENDPOINT}/api/v1/dags/{dag_id}/dagRuns",
                json={"conf": {"run_id": run_id, "pipeline_id": pipeline_id}},
                auth=(AIRFLOW_USER, AIRFLOW_PASSWORD),
                timeout=15,
            )
            resp.raise_for_status()
        except Exception as e:
            query(
                "UPDATE job_runs SET status = 'failed', ended_at = NOW(), error_message = %s WHERE id = %s",
                (f"Failed to trigger Airflow DAG: {e}", run_id)
            )
            raise HTTPException(status_code=500, detail=f"Airflow trigger failed: {e}")

        return {"run_id": run_id, "status": "queued", "dag_id": dag_id}

    # ── Local path: run synchronously ─────────────────────────────────
    try:
        query("UPDATE job_runs SET phase_completed = 1 WHERE id = %s", (run_id,))

        src = get_source_db(
            host=src_conn["host"], port=src_conn["port"],
            dbname=src_conn["db_name"], user=src_conn["db_user"],
            password=src_conn["db_password"]
        )

        last_runs = query(
            """SELECT MAX(ended_at) as last_run FROM job_runs
               WHERE pipeline_id = %s AND status = 'complete'""",
            (pipeline_id,)
        )
        last_watermark = (
            last_runs[0]['last_run']
            if last_runs and last_runs[0]['last_run']
            else datetime.now() - timedelta(days=30)
        )

        with src.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            watermark_col = pipeline.get('watermark_column') or 'updated_at'
            try:
                cur.execute(
                    f"SELECT * FROM {pipeline['source_table']} WHERE {watermark_col} >= %s",
                    (last_watermark,)
                )
                extracted_rows = cur.fetchall()
            except Exception:
                cur.execute(f"SELECT * FROM {pipeline['source_table']}")
                extracted_rows = cur.fetchall()

            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = %s AND table_schema = 'public'
                ORDER BY ordinal_position
            """, (pipeline['source_table'],))
            col_defs = cur.fetchall()

        src.close()
        rows_extracted = len(extracted_rows)

        query("UPDATE job_runs SET phase_completed = 2 WHERE id = %s", (run_id,))

        if extracted_rows:
            if dest_conn and dest_conn['db_type'] == 'snowflake':
                _load_to_snowflake(dest_conn, pipeline, col_defs, extracted_rows)
            else:
                _load_to_mock_postgres(pipeline, col_defs, extracted_rows)

        history = query(
            """SELECT rows_loaded FROM job_runs
               WHERE pipeline_id = %s AND status = 'complete'
               ORDER BY started_at DESC LIMIT 7""",
            (pipeline_id,)
        )
        anomaly = False
        if history:
            avg = sum(r['rows_loaded'] or 0 for r in history) / len(history)
            upper = pipeline.get('volume_alert_upper_pct', 200)
            lower = pipeline.get('volume_alert_lower_pct', 50)
            if avg > 0:
                pct = (rows_extracted / avg) * 100
                anomaly = pct < lower or pct > upper

        query(
            """UPDATE job_runs SET status = 'complete', phase_completed = 3,
               ended_at = NOW(), rows_loaded = %s, volume_anomaly_flag = %s
               WHERE id = %s""",
            (rows_extracted, anomaly, run_id)
        )

        return {
            "run_id": run_id,
            "status": "complete",
            "rows_loaded": rows_extracted,
            "volume_anomaly": anomaly
        }

    except Exception as e:
        query(
            "UPDATE job_runs SET status = 'failed', ended_at = NOW(), error_message = %s WHERE id = %s",
            (str(e), run_id)
        )
        raise HTTPException(status_code=500, detail=str(e))


# ─── Load helpers ─────────────────────────────────────────────────────

TYPE_MAP = {
    'integer': 'INTEGER', 'bigint': 'BIGINT', 'serial': 'INTEGER',
    'varchar': 'VARCHAR(500)', 'character varying': 'VARCHAR(500)',
    'text': 'TEXT', 'boolean': 'BOOLEAN',
    'numeric': 'NUMERIC(18,4)', 'decimal': 'NUMERIC(18,4)',
    'timestamp without time zone': 'TIMESTAMP',
    'timestamp with time zone': 'TIMESTAMP_TZ',
    'date': 'DATE', 'double precision': 'FLOAT',
}

def _snowflake_base_type(sf_type: str) -> str:
    """Extract base type name from Snowflake DESCRIBE TABLE output, e.g. VARCHAR(16777216) → VARCHAR."""
    return sf_type.split('(')[0].strip().upper()

def _mapped_base_type(pg_type: str) -> str:
    """Get the base Snowflake type for a Postgres type, e.g. 'character varying' → 'VARCHAR'."""
    return _snowflake_base_type(TYPE_MAP.get(pg_type, 'TEXT'))

def _detect_and_handle_drift(sf_cur, pipeline_id: int, col_defs, database: str, target_schema: str, target_table: str):
    """
    Compare source column definitions against the existing Snowflake table.

    New columns   → ALTER TABLE ADD COLUMN (auto-resolved, pipeline keeps running)
    Type changes  → logged as pending_review (not auto-applied; Snowflake type changes are risky)
    Dropped cols  → logged as pending_review (Snowflake keeps old columns; existing data safe)

    Returns list of drift event dicts written to schema_drift_log.
    """
    full_table = f"{database}.{target_schema}.{target_table}"

    # Fetch current Snowflake columns via DESCRIBE TABLE
    try:
        sf_cur.execute(f"DESCRIBE TABLE {full_table}")
        rows = sf_cur.fetchall()
        # DESCRIBE TABLE columns: name, type, kind, null?, default, pk, uq, check, expr, comment, policy name
        existing = {row[0].upper(): row[1].upper() for row in rows}
    except Exception:
        # Table doesn't exist yet — first run, no drift possible
        return []

    source = {
        c['column_name'].upper(): TYPE_MAP.get(c['data_type'], 'TEXT')
        for c in col_defs
    }

    drift_events = []

    # ── New columns in source not yet in Snowflake → auto-add ───────────────
    for col_name, col_type in source.items():
        if col_name not in existing:
            status = 'auto_resolved'
            try:
                sf_cur.execute(f"ALTER TABLE {full_table} ADD COLUMN {col_name} {col_type}")
            except Exception as e:
                status = f'error: {str(e)[:200]}'
            drift_events.append({
                'pipeline_id': pipeline_id,
                'column_name': col_name,
                'change_type': 'column_added',
                'old_definition': None,
                'new_definition': col_type,
                'status': status,
            })

    # ── Type changes → log only, do not auto-alter ───────────────────────────
    for col_name, col_type in source.items():
        if col_name in existing:
            sf_base = _snowflake_base_type(existing[col_name])
            src_base = _mapped_base_type(
                next((c['data_type'] for c in col_defs if c['column_name'].upper() == col_name), '')
            )
            if sf_base != src_base:
                drift_events.append({
                    'pipeline_id': pipeline_id,
                    'column_name': col_name,
                    'change_type': 'type_changed',
                    'old_definition': existing[col_name],
                    'new_definition': col_type,
                    'status': 'pending_review',
                })

    # ── Columns dropped from source → log only ───────────────────────────────
    for col_name in existing:
        if col_name not in source:
            drift_events.append({
                'pipeline_id': pipeline_id,
                'column_name': col_name,
                'change_type': 'column_dropped',
                'old_definition': existing[col_name],
                'new_definition': None,
                'status': 'pending_review',
            })

    # ── Persist to metadata DB ────────────────────────────────────────────────
    for evt in drift_events:
        query("""
            INSERT INTO schema_drift_log
                (pipeline_id, column_name, change_type, old_definition, new_definition, status)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (evt['pipeline_id'], evt['column_name'], evt['change_type'],
              evt['old_definition'], evt['new_definition'], evt['status']))

    return drift_events


def _load_to_snowflake(dest_conn, pipeline, col_defs, extracted_rows):
    import pandas as pd
    from snowflake.connector.pandas_tools import write_pandas

    database = (pipeline.get('target_database') or dest_conn['db_name']).upper()
    target_schema = (pipeline.get('target_schema_name') or dest_conn.get('snowflake_schema') or 'PUBLIC').upper()
    target_table = pipeline['target_table'].upper()
    merge_key = (pipeline.get('merge_key_column') or 'id').lower()
    pipeline_id = pipeline['id']

    # Build DataFrame from extracted rows
    cols = [c['column_name'] for c in col_defs]
    df = pd.DataFrame([{c: row[c] for c in cols} for row in extracted_rows])
    df.columns = [c.upper() for c in df.columns]
    merge_key_upper = merge_key.upper()

    sf = get_snowflake_conn(dest_conn)

    with sf.cursor() as cur:
        if dest_conn.get('snowflake_warehouse'):
            cur.execute(f"USE WAREHOUSE {dest_conn['snowflake_warehouse']}")

        # Create table if not exists (first run)
        col_sql = ", ".join([
            f"{c['column_name'].upper()} {TYPE_MAP.get(c['data_type'], 'TEXT')}"
            for c in col_defs
        ])
        cur.execute(f"CREATE TABLE IF NOT EXISTS {database}.{target_schema}.{target_table} ({col_sql})")

        # ── Schema drift detection + auto-heal ──────────────────────────────
        # Runs after CREATE TABLE IF NOT EXISTS so the table always exists here.
        # New columns are ALTER-ed in before write_pandas runs, so the DataFrame
        # columns and the Snowflake table columns will match.
        drift = _detect_and_handle_drift(cur, pipeline_id, col_defs, database, target_schema, target_table)
        if drift:
            added = [d for d in drift if d['change_type'] == 'column_added' and d['status'] == 'auto_resolved']
            pending = [d for d in drift if d['status'] == 'pending_review']
            if added:
                print(f"[drift] Auto-added columns: {[d['column_name'] for d in added]}")
            if pending:
                print(f"[drift] Pending review: {[(d['column_name'], d['change_type']) for d in pending]}")

        # Delete existing rows matching merge key (upsert pattern)
        if merge_key_upper in df.columns:
            keys = df[merge_key_upper].tolist()
            placeholders = ','.join(['%s'] * len(keys))
            cur.execute(
                f"DELETE FROM {database}.{target_schema}.{target_table} WHERE {merge_key_upper} IN ({placeholders})",
                keys
            )

    # write_pandas: Parquet → internal stage → COPY INTO (bulk, parallel-safe)
    # By the time we reach here, the Snowflake table has all columns the DataFrame has.
    success, nchunks, nrows, _ = write_pandas(
        conn=sf,
        df=df,
        table_name=target_table,
        database=database,
        schema=target_schema,
        auto_create_table=False,
        overwrite=False,
    )

    sf.close()

    if not success:
        raise Exception(f"write_pandas failed: {nchunks} chunks, {nrows} rows")


def _load_to_mock_postgres(pipeline, col_defs, extracted_rows):
    """Fallback: load into the local mock postgres snowflake_target schema."""
    from contextlib import suppress
    mock = get_db()
    target_schema = 'snowflake_target'
    target_table = pipeline['target_table']
    merge_key = pipeline.get('merge_key_column') or 'id'

    col_sql = ", ".join([
        f"{c['column_name']} {TYPE_MAP.get(c['data_type'], 'TEXT')}"
        for c in col_defs
    ])
    cols = [c['column_name'] for c in col_defs]
    col_names = ", ".join(cols)
    placeholders = ", ".join(["%s"] * len(cols))
    update_set = ", ".join([f"{c} = EXCLUDED.{c}" for c in cols if c != merge_key])

    with mock.cursor() as cur:
        cur.execute(f"CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} ({col_sql})")
        for row in extracted_rows:
            vals = [row[c] for c in cols]
            cur.execute(f"""
                INSERT INTO {target_schema}.{target_table} ({col_names})
                VALUES ({placeholders})
                ON CONFLICT ({merge_key}) DO UPDATE SET {update_set}
            """, vals)

    mock.commit()
    mock.close()


# ─── Dashboard stats ──────────────────────────────────────────────────
@app.get("/stats")
def get_stats():
    pipelines = query("SELECT COUNT(*) as count FROM pipelines WHERE is_active = true")
    connections = query("SELECT COUNT(*) as count FROM connections WHERE is_active = true")
    runs_today = query("""
        SELECT COUNT(*) as count FROM job_runs
        WHERE started_at >= NOW() - INTERVAL '24 hours'
    """)
    failed_today = query("""
        SELECT COUNT(*) as count FROM job_runs
        WHERE started_at >= NOW() - INTERVAL '24 hours' AND status = 'failed'
    """)
    anomalies = query("""
        SELECT COUNT(*) as count FROM job_runs
        WHERE volume_anomaly_flag = true AND started_at >= NOW() - INTERVAL '7 days'
    """)

    drift_pending = query(
        "SELECT COUNT(*) as count FROM schema_drift_log WHERE status = 'pending_review'"
    )
    return {
        "total_pipelines": pipelines[0]['count'],
        "total_connections": connections[0]['count'],
        "runs_today": runs_today[0]['count'],
        "failed_today": failed_today[0]['count'],
        "anomalies_7d": anomalies[0]['count'],
        "drift_pending": drift_pending[0]['count'],
    }

@app.get("/pipelines/{pipeline_id}/volume-history")
def volume_history(pipeline_id: int):
    rows = query("""
        SELECT DATE(started_at) as run_date,
               SUM(rows_loaded) as rows_loaded,
               COUNT(*) as run_count,
               MAX(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as had_failure,
               MAX(CASE WHEN volume_anomaly_flag = true THEN 1 ELSE 0 END) as had_anomaly
        FROM job_runs
        WHERE pipeline_id = %s AND started_at >= NOW() - INTERVAL '30 days'
        GROUP BY DATE(started_at)
        ORDER BY run_date ASC
    """, (pipeline_id,))
    return [dict(r) for r in rows]

@app.get("/dependencies")
def get_all_dependencies():
    rows = query("""
        SELECT pd.pipeline_id, p1.name as pipeline_name,
               pd.depends_on_id, p2.name as depends_on_name
        FROM pipeline_dependencies pd
        JOIN pipelines p1 ON pd.pipeline_id = p1.id
        JOIN pipelines p2 ON pd.depends_on_id = p2.id
    """)
    return [dict(r) for r in rows]


# ─── Schema Drift ─────────────────────────────────────────────────────
@app.get("/pipelines/{pipeline_id}/drift")
def get_drift(pipeline_id: int, status: Optional[str] = None):
    """List schema drift events for a pipeline. Filter by status if provided."""
    sql = """
        SELECT * FROM schema_drift_log
        WHERE pipeline_id = %s
    """
    params = [pipeline_id]
    if status:
        sql += " AND status = %s"
        params.append(status)
    sql += " ORDER BY detected_at DESC"
    rows = query(sql, params)
    return [dict(r) for r in rows]

@app.get("/drift/pending")
def get_all_pending_drift():
    """All unresolved drift events across all pipelines, joined with pipeline name."""
    rows = query("""
        SELECT d.*, p.name as pipeline_name
        FROM schema_drift_log d
        JOIN pipelines p ON d.pipeline_id = p.id
        WHERE d.status = 'pending_review'
        ORDER BY d.detected_at DESC
    """)
    return [dict(r) for r in rows]

class DriftResolve(BaseModel):
    action: str          # 'dismiss' | 'apply_type_change'
    resolved_by: Optional[int] = None

@app.patch("/drift/{drift_id}")
def resolve_drift(drift_id: int, body: DriftResolve):
    """
    Resolve a drift event.

    action=dismiss         — marks as resolved without making a DB change (manual fix done externally)
    action=apply_type_change — attempts ALTER TABLE ALTER COLUMN in Snowflake, then marks resolved
    """
    rows = query("""
        SELECT d.*, p.connection_id, p.destination_connection_id,
               p.target_database, p.target_schema_name, p.target_table
        FROM schema_drift_log d
        JOIN pipelines p ON d.pipeline_id = p.id
        WHERE d.id = %s
    """, (drift_id,))
    if not rows:
        raise HTTPException(status_code=404, detail="Drift event not found")
    drift = dict(rows[0])

    if body.action == 'dismiss':
        query("""
            UPDATE schema_drift_log
               SET status = 'resolved', resolved_at = NOW(), resolved_by = %s
             WHERE id = %s
        """, (body.resolved_by, drift_id))
        return {"message": "Drift dismissed", "drift_id": drift_id}

    if body.action == 'apply_type_change':
        if drift['change_type'] != 'type_changed':
            raise HTTPException(status_code=400, detail="apply_type_change only valid for type_changed events")

        dest_rows = query("SELECT * FROM connections WHERE id = %s", (drift['destination_connection_id'],))
        if not dest_rows:
            raise HTTPException(status_code=404, detail="Destination connection not found")
        dest_conn = dict(dest_rows[0])

        database = (drift['target_database'] or dest_conn['db_name']).upper()
        schema = (drift['target_schema_name'] or dest_conn.get('snowflake_schema') or 'PUBLIC').upper()
        table = drift['target_table'].upper()
        col = drift['column_name']
        new_type = drift['new_definition']

        # Snowflake supports widening casts; narrowing may fail — that's intentional
        try:
            sf = get_snowflake_conn(dest_conn)
            with sf.cursor() as cur:
                if dest_conn.get('snowflake_warehouse'):
                    cur.execute(f"USE WAREHOUSE {dest_conn['snowflake_warehouse']}")
                cur.execute(
                    f"ALTER TABLE {database}.{schema}.{table} ALTER COLUMN {col} SET DATA TYPE {new_type}"
                )
            sf.close()
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"ALTER COLUMN failed: {e}")

        query("""
            UPDATE schema_drift_log
               SET status = 'resolved', resolved_at = NOW(), resolved_by = %s
             WHERE id = %s
        """, (body.resolved_by, drift_id))
        return {"message": f"Column {col} type changed to {new_type}", "drift_id": drift_id}

    raise HTTPException(status_code=400, detail=f"Unknown action: {body.action}")
