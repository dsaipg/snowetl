"""
elt_glue_job.py — Generic ELT Glue Job for ELT Platform
=========================================================
Production-grade AWS Glue PySpark job that:
  1. Reads pipeline/connection config from the ELT Platform backend
  2. Extracts data from a PostgreSQL source via JDBC (parallel partitioned read)
  3. Loads into Snowflake via the AWS Glue Snowflake connector
     (Glue internally stages to an S3 internal stage then runs COPY INTO)
  4. Reports rows_loaded / status back to the backend

Required Job Parameters (pass via --job-bookmark-option and --{key}={value}):
  --pipeline_id   Pipeline ID from the ELT Platform DB
  --run_id        Job run ID created by the backend before submitting this job
  --backend_url   Base URL of the FastAPI backend, e.g. http://my-alb.example.com

AWS Glue setup:
  - Glue version 4.0 (Spark 3.3 / Python 3.10)
  - Add the PostgreSQL JDBC driver as an extra JAR (or use Glue's bundled connector)
  - Snowflake connector: use the AWS Glue Connector for Snowflake (Marketplace)
    OR add spark-snowflake JAR and use format "net.snowflake.spark.snowflake"
  - IAM role needs: glue:*, s3:*, and network access to source DB + Snowflake
"""

import sys
import json
import requests

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext

# ─── Init Glue context ───────────────────────────────────────────────────────
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "pipeline_id",
    "run_id",
    "backend_url",
])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

PIPELINE_ID = int(args["pipeline_id"])
RUN_ID = int(args["run_id"])
BACKEND_URL = args["backend_url"].rstrip("/")


# ─── Backend API helpers ──────────────────────────────────────────────────────
def _get(path: str) -> dict:
    resp = requests.get(f"{BACKEND_URL}{path}", timeout=30)
    resp.raise_for_status()
    return resp.json()


def _update_run(status: str, rows_loaded: int = None, error_message: str = None):
    """PATCH /runs/{run_id} — update status, optional rows/error."""
    payload = {"status": status}
    if rows_loaded is not None:
        payload["rows_loaded"] = rows_loaded
    if error_message:
        payload["error_message"] = error_message[:2000]  # guard against huge tracebacks
    try:
        r = requests.patch(f"{BACKEND_URL}/runs/{RUN_ID}", json=payload, timeout=30)
        r.raise_for_status()
    except Exception as exc:
        # Log but don't raise — we don't want a callback failure to mask the real error
        print(f"[WARN] Failed to update run {RUN_ID}: {exc}")


# ─── Main job logic ───────────────────────────────────────────────────────────
try:
    # ── 1. Fetch pipeline + connection config from backend ───────────────────
    pipeline = _get(f"/pipelines/{PIPELINE_ID}")
    src_conn = _get(f"/connections/{pipeline['connection_id']}")
    dest_conn = _get(f"/connections/{pipeline['destination_connection_id']}")

    database = (pipeline.get("target_database") or dest_conn["db_name"]).upper()
    schema = (pipeline.get("target_schema_name") or dest_conn.get("snowflake_schema") or "PUBLIC").upper()
    table = pipeline["target_table"].upper()
    source_table = pipeline["source_table"]
    merge_key = (pipeline.get("merge_key_column") or "").upper()
    watermark_col = pipeline.get("watermark_column")

    print(f"[INFO] Pipeline {PIPELINE_ID} | {src_conn['name']} → {database}.{schema}.{table}")
    _update_run("running")

    # ── 2. Extract from source via JDBC (parallel partitioned read) ──────────
    # Partitioned read splits the table across Spark workers by the merge key
    # column (typically an auto-increment integer), enabling parallel extraction
    # for large tables without overloading the source.
    jdbc_url = (
        f"jdbc:postgresql://{src_conn['host']}:{src_conn['port']}/{src_conn['db_name']}"
    )

    jdbc_opts = {
        "url": jdbc_url,
        "dbtable": source_table,
        "user": src_conn["db_user"],
        "password": src_conn["db_password"],
        "driver": "org.postgresql.Driver",
        "fetchsize": "10000",
    }

    # If a numeric merge key is available, use partitioned read for large tables
    if merge_key:
        bounds_df = (
            spark.read
            .format("jdbc")
            .options(**jdbc_opts)
            .option("query", f"SELECT MIN({merge_key}) AS lo, MAX({merge_key}) AS hi FROM {source_table}")
            .load()
        )
        row = bounds_df.collect()[0]
        lo, hi = row["lo"], row["hi"]
        if lo is not None and hi is not None and hi > lo:
            jdbc_opts.update({
                "partitionColumn": merge_key,
                "lowerBound": str(lo),
                "upperBound": str(hi),
                "numPartitions": "8",
            })

    source_df = spark.read.format("jdbc").options(**jdbc_opts).load()

    row_count = source_df.count()
    print(f"[INFO] Extracted {row_count} rows from {source_table}")

    if row_count == 0:
        print("[INFO] No rows extracted — marking complete with 0 rows")
        _update_run("complete", rows_loaded=0)
        job.commit()
        sys.exit(0)

    # ── 3. Normalise column names to UPPERCASE (Snowflake convention) ────────
    source_df = source_df.toDF(*[c.upper() for c in source_df.columns])

    # ── 4. Write to Snowflake via AWS Glue Snowflake connector ───────────────
    # The Glue Snowflake connector automatically:
    #   a) Serialises the DataFrame to Parquet
    #   b) Uploads to a Snowflake internal stage (or an S3 external stage if configured)
    #   c) Executes COPY INTO <table> from the stage
    # This is equivalent to write_pandas locally but runs distributed across Glue workers.
    #
    # Connection options match the AWS Glue Connector for Snowflake (v1.x / 2.x).
    # If you're using the spark-snowflake JAR instead, change connection_type to
    # "net.snowflake.spark.snowflake" and use .write.format(...) instead.
    sf_options = {
        "sfURL": f"{dest_conn['host']}.snowflakecomputing.com",
        "sfUser": dest_conn["db_user"],
        "sfPassword": dest_conn["db_password"],
        "sfDatabase": database,
        "sfSchema": schema,
        "sfWarehouse": dest_conn.get("snowflake_warehouse") or "",
        "sfRole": dest_conn.get("snowflake_role") or "",
        "dbtable": table,
        # COPY INTO options
        "preactions": (
            f"CREATE TABLE IF NOT EXISTS {database}.{schema}.{table} LIKE {database}.{schema}.{table};"
            if False  # Table is pre-created by the platform; set True only for auto-create
            else ""
        ),
    }

    # DELETE existing rows matching merge key before loading (upsert pattern)
    if merge_key and merge_key in source_df.columns:
        key_list = ",".join(
            f"'{str(v)}'" for v in
            [r[merge_key] for r in source_df.select(merge_key).collect()]
        )
        sf_options["preactions"] = (
            f"DELETE FROM {database}.{schema}.{table} WHERE {merge_key} IN ({key_list})"
        )
        print(f"[INFO] Will DELETE existing {merge_key} values before COPY INTO")

    dynamic_frame = DynamicFrame.fromDF(source_df, glueContext, "elt_source")

    glueContext.write_dynamic_frame.from_options(
        frame=dynamic_frame,
        connection_type="snowflake",
        connection_options=sf_options,
    )

    print(f"[INFO] Loaded {row_count} rows → {database}.{schema}.{table}")
    _update_run("complete", rows_loaded=row_count)

except Exception as exc:
    import traceback
    tb = traceback.format_exc()
    print(f"[ERROR] Glue job failed:\n{tb}")
    _update_run("failed", error_message=f"{exc}\n\n{tb}")
    job.commit()
    raise  # Glue marks the job run as FAILED in CloudWatch

job.commit()
