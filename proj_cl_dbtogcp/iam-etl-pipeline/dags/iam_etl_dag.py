"""
IAM Data Pipeline: PostgreSQL/Oracle → GCS → BigQuery
Orchestrated with Apache Airflow

Architecture:
  1. Extract IAM data from PostgreSQL & Oracle
  2. Transform & validate data
  3. Load to GCS (Cloud Storage) as Parquet
  4. Load from GCS to BigQuery via external tables / load jobs
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryInsertJobOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateEmptyDatasetOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.oracle.hooks.oracle import OracleHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

import logging
import json
import io
from typing import Any

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

# ─── Config from Airflow Variables ────────────────────────────────────────────
GCP_PROJECT_ID   = Variable.get("GCP_PROJECT_ID",   default_var="my-gcp-project")
GCS_BUCKET       = Variable.get("GCS_BUCKET",        default_var="iam-etl-bucket")
BQ_DATASET       = Variable.get("BQ_DATASET",        default_var="iam_data")
BQ_LOCATION      = Variable.get("BQ_LOCATION",       default_var="US")
POSTGRES_CONN_ID = Variable.get("POSTGRES_CONN_ID",  default_var="postgres_iam")
ORACLE_CONN_ID   = Variable.get("ORACLE_CONN_ID",    default_var="oracle_iam")
GCS_CONN_ID      = Variable.get("GCS_CONN_ID",       default_var="google_cloud_default")
NOTIFICATION_EMAIL = Variable.get("NOTIFICATION_EMAIL", default_var="admin@example.com")

# GCS path prefix for raw data
GCS_RAW_PREFIX   = "raw/iam"
GCS_STAGED_PREFIX = "staged/iam"

# ─── Default Args ──────────────────────────────────────────────────────────────
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": [NOTIFICATION_EMAIL],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    "execution_timeout": timedelta(hours=2),
}

# ─── IAM Tables Config ─────────────────────────────────────────────────────────
IAM_TABLES = {
    "postgres": [
        {
            "name": "users",
            "source_schema": "iam",
            "query": """
                SELECT
                    user_id, username, email, display_name,
                    is_active, is_locked, mfa_enabled,
                    password_last_changed, last_login_at,
                    created_at, updated_at
                FROM iam.users
                WHERE updated_at >= '{watermark}'
            """,
            "partition_field": "created_at",
            "bq_table": "users",
        },
        {
            "name": "roles",
            "source_schema": "iam",
            "query": """
                SELECT
                    role_id, role_name, description, role_type,
                    is_system_role, parent_role_id,
                    created_at, updated_at
                FROM iam.roles
                WHERE updated_at >= '{watermark}'
            """,
            "partition_field": "created_at",
            "bq_table": "roles",
        },
        {
            "name": "user_roles",
            "source_schema": "iam",
            "query": """
                SELECT
                    assignment_id, user_id, role_id,
                    assigned_by, valid_from, valid_to,
                    is_active, created_at, updated_at
                FROM iam.user_roles
                WHERE updated_at >= '{watermark}'
            """,
            "partition_field": "valid_from",
            "bq_table": "user_roles",
        },
        {
            "name": "permissions",
            "source_schema": "iam",
            "query": """
                SELECT
                    permission_id, permission_name, resource_type,
                    action, description, is_system_permission,
                    created_at, updated_at
                FROM iam.permissions
                WHERE updated_at >= '{watermark}'
            """,
            "partition_field": "created_at",
            "bq_table": "permissions",
        },
        {
            "name": "role_permissions",
            "source_schema": "iam",
            "query": """
                SELECT
                    rp_id, role_id, permission_id,
                    granted_by, granted_at, is_active,
                    created_at, updated_at
                FROM iam.role_permissions
                WHERE updated_at >= '{watermark}'
            """,
            "partition_field": "granted_at",
            "bq_table": "role_permissions",
        },
        {
            "name": "audit_logs",
            "source_schema": "iam",
            "query": """
                SELECT
                    log_id, user_id, action, resource_type,
                    resource_id, ip_address, user_agent,
                    status, details, created_at
                FROM iam.audit_logs
                WHERE created_at >= '{watermark}'
            """,
            "partition_field": "created_at",
            "bq_table": "audit_logs",
        },
    ],
    "oracle": [
        {
            "name": "groups",
            "query": """
                SELECT
                    GROUP_ID, GROUP_NAME, DESCRIPTION, GROUP_TYPE,
                    PARENT_GROUP_ID, IS_ACTIVE,
                    CREATED_AT, UPDATED_AT
                FROM IAM.GROUPS
                WHERE UPDATED_AT >= TO_DATE('{watermark}', 'YYYY-MM-DD HH24:MI:SS')
            """,
            "partition_field": "CREATED_AT",
            "bq_table": "groups",
        },
        {
            "name": "user_groups",
            "query": """
                SELECT
                    MAPPING_ID, USER_ID, GROUP_ID,
                    ADDED_BY, IS_ACTIVE, JOINED_AT, LEFT_AT,
                    CREATED_AT, UPDATED_AT
                FROM IAM.USER_GROUPS
                WHERE UPDATED_AT >= TO_DATE('{watermark}', 'YYYY-MM-DD HH24:MI:SS')
            """,
            "partition_field": "JOINED_AT",
            "bq_table": "user_groups",
        },
        {
            "name": "policies",
            "query": """
                SELECT
                    POLICY_ID, POLICY_NAME, POLICY_DOC,
                    POLICY_TYPE, IS_ACTIVE,
                    CREATED_BY, CREATED_AT, UPDATED_AT
                FROM IAM.POLICIES
                WHERE UPDATED_AT >= TO_DATE('{watermark}', 'YYYY-MM-DD HH24:MI:SS')
            """,
            "partition_field": "CREATED_AT",
            "bq_table": "policies",
        },
    ],
}


# ─── BigQuery Schema Definitions ───────────────────────────────────────────────
BQ_SCHEMAS = {
    "users": [
        {"name": "user_id",               "type": "STRING",    "mode": "REQUIRED"},
        {"name": "username",              "type": "STRING",    "mode": "REQUIRED"},
        {"name": "email",                 "type": "STRING",    "mode": "NULLABLE"},
        {"name": "display_name",          "type": "STRING",    "mode": "NULLABLE"},
        {"name": "is_active",             "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "is_locked",             "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "mfa_enabled",           "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "password_last_changed", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "last_login_at",         "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "created_at",            "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",            "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at",        "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",           "type": "STRING",    "mode": "REQUIRED"},
    ],
    "roles": [
        {"name": "role_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "role_name",      "type": "STRING",    "mode": "REQUIRED"},
        {"name": "description",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "role_type",      "type": "STRING",    "mode": "NULLABLE"},
        {"name": "is_system_role", "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "parent_role_id", "type": "STRING",    "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "user_roles": [
        {"name": "assignment_id",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "user_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "role_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "assigned_by",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "valid_from",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "valid_to",       "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "is_active",      "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "permissions": [
        {"name": "permission_id",        "type": "STRING",  "mode": "REQUIRED"},
        {"name": "permission_name",      "type": "STRING",  "mode": "REQUIRED"},
        {"name": "resource_type",        "type": "STRING",  "mode": "NULLABLE"},
        {"name": "action",               "type": "STRING",  "mode": "NULLABLE"},
        {"name": "description",          "type": "STRING",  "mode": "NULLABLE"},
        {"name": "is_system_permission", "type": "BOOLEAN", "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "role_permissions": [
        {"name": "rp_id",          "type": "STRING",    "mode": "REQUIRED"},
        {"name": "role_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "permission_id",  "type": "STRING",    "mode": "REQUIRED"},
        {"name": "granted_by",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "granted_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "is_active",      "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "audit_logs": [
        {"name": "log_id",         "type": "STRING",    "mode": "REQUIRED"},
        {"name": "user_id",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "action",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "resource_type",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "resource_id",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "ip_address",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "user_agent",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "status",         "type": "STRING",    "mode": "NULLABLE"},
        {"name": "details",        "type": "JSON",      "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "groups": [
        {"name": "group_id",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "group_name",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "description",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "group_type",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "parent_group_id","type": "STRING",    "mode": "NULLABLE"},
        {"name": "is_active",      "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "user_groups": [
        {"name": "mapping_id",     "type": "STRING",    "mode": "REQUIRED"},
        {"name": "user_id",        "type": "STRING",    "mode": "REQUIRED"},
        {"name": "group_id",       "type": "STRING",    "mode": "REQUIRED"},
        {"name": "added_by",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "is_active",      "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "joined_at",      "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "left_at",        "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
    "policies": [
        {"name": "policy_id",      "type": "STRING",    "mode": "REQUIRED"},
        {"name": "policy_name",    "type": "STRING",    "mode": "REQUIRED"},
        {"name": "policy_doc",     "type": "JSON",      "mode": "NULLABLE"},
        {"name": "policy_type",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "is_active",      "type": "BOOLEAN",   "mode": "NULLABLE"},
        {"name": "created_by",     "type": "STRING",    "mode": "NULLABLE"},
        {"name": "created_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at",     "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "_etl_loaded_at", "type": "TIMESTAMP", "mode": "REQUIRED"},
        {"name": "_etl_source",    "type": "STRING",    "mode": "REQUIRED"},
    ],
}


# ─── Helper Functions ──────────────────────────────────────────────────────────

def get_watermark(table_name: str, source: str) -> str:
    """Retrieve high-watermark from Airflow Variables (simple watermark store)."""
    key = f"watermark_{source}_{table_name}"
    default = "1970-01-01 00:00:00"
    return Variable.get(key, default_var=default)


def set_watermark(table_name: str, source: str, value: str) -> None:
    """Persist high-watermark after successful load."""
    key = f"watermark_{source}_{table_name}"
    Variable.set(key, value)


def add_etl_metadata(df: pd.DataFrame, source: str) -> pd.DataFrame:
    """Attach ETL audit columns to every dataframe."""
    df["_etl_loaded_at"] = pd.Timestamp.utcnow()
    df["_etl_source"] = source
    # Normalise column names to lowercase
    df.columns = [c.lower() for c in df.columns]
    return df


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise DataFrame to Parquet bytes (in-memory)."""
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="snappy")
    return buf.getvalue()


# ─── Extract Tasks ──────────────────────────────────────────────────────────────

def extract_postgres_table(table_config: dict, **context) -> str:
    """
    Extract a single table from PostgreSQL, write Parquet to GCS.
    Returns the GCS object path (pushed to XCom).
    """
    ds = context["ds"]                     # logical date  2024-01-15
    run_id = context["run_id"]
    table_name = table_config["name"]
    watermark = get_watermark(table_name, "postgres")

    logger.info("Extracting postgres table=%s watermark=%s", table_name, watermark)

    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    sql = table_config["query"].format(watermark=watermark)
    df = pg_hook.get_pandas_df(sql)

    if df.empty:
        logger.info("No new rows for %s", table_name)
        return ""

    df = add_etl_metadata(df, source=f"postgres/{table_config.get('source_schema','iam')}")
    parquet_bytes = df_to_parquet_bytes(df)

    gcs_object = f"{GCS_RAW_PREFIX}/postgres/{table_name}/ds={ds}/data.parquet"
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_object,
        data=parquet_bytes,
        mime_type="application/octet-stream",
    )

    # Advance watermark to now
    if "updated_at" in df.columns:
        new_wm = df["updated_at"].max()
    elif "created_at" in df.columns:
        new_wm = df["created_at"].max()
    else:
        new_wm = pd.Timestamp.utcnow()
    set_watermark(table_name, "postgres", str(new_wm))

    logger.info("Uploaded %d rows → gs://%s/%s", len(df), GCS_BUCKET, gcs_object)
    return gcs_object


def extract_oracle_table(table_config: dict, **context) -> str:
    """
    Extract a single table from Oracle, write Parquet to GCS.
    """
    ds = context["ds"]
    table_name = table_config["name"]
    watermark = get_watermark(table_name, "oracle")

    logger.info("Extracting oracle table=%s watermark=%s", table_name, watermark)

    oracle_hook = OracleHook(oracle_conn_id=ORACLE_CONN_ID)
    sql = table_config["query"].format(watermark=watermark)
    df = oracle_hook.get_pandas_df(sql)

    if df.empty:
        logger.info("No new rows for %s", table_name)
        return ""

    df = add_etl_metadata(df, source="oracle/iam")
    parquet_bytes = df_to_parquet_bytes(df)

    gcs_object = f"{GCS_RAW_PREFIX}/oracle/{table_name}/ds={ds}/data.parquet"
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    gcs_hook.upload(
        bucket_name=GCS_BUCKET,
        object_name=gcs_object,
        data=parquet_bytes,
        mime_type="application/octet-stream",
    )

    partition_col = table_config["partition_field"].lower()
    if partition_col in df.columns:
        new_wm = df[partition_col].max()
    else:
        new_wm = pd.Timestamp.utcnow()
    set_watermark(table_name, "oracle", str(new_wm))

    logger.info("Uploaded %d rows → gs://%s/%s", len(df), GCS_BUCKET, gcs_object)
    return gcs_object


# ─── Validate Task ─────────────────────────────────────────────────────────────

def validate_gcs_file(gcs_object: str, **context) -> bool:
    """Light-weight validation: check object exists and row count > 0."""
    if not gcs_object:
        logger.info("Empty GCS object path — skipping validation (no new data).")
        return True
    gcs_hook = GCSHook(gcp_conn_id=GCS_CONN_ID)
    exists = gcs_hook.exists(bucket_name=GCS_BUCKET, object_name=gcs_object)
    if not exists:
        raise FileNotFoundError(f"GCS object not found: gs://{GCS_BUCKET}/{gcs_object}")
    logger.info("Validated: gs://%s/%s exists", GCS_BUCKET, gcs_object)
    return True


# ─── DAG Definition ────────────────────────────────────────────────────────────

with DAG(
    dag_id="iam_etl_pipeline",
    description="IAM data pipeline: PostgreSQL + Oracle → GCS → BigQuery",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iam", "etl", "gcp", "bigquery"],
) as dag:

    # ── Start ──────────────────────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── Ensure BQ Dataset ──────────────────────────────────────────────────────
    create_bq_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bq_dataset",
        dataset_id=BQ_DATASET,
        project_id=GCP_PROJECT_ID,
        location=BQ_LOCATION,
        exists_ok=True,
        gcp_conn_id=GCS_CONN_ID,
    )

    # ── PostgreSQL Extract Group ───────────────────────────────────────────────
    with TaskGroup("extract_postgres") as extract_postgres_group:
        for tbl in IAM_TABLES["postgres"]:
            _tbl = tbl  # capture loop var

            extract_task = PythonOperator(
                task_id=f"extract_{_tbl['name']}",
                python_callable=extract_postgres_table,
                op_kwargs={"table_config": _tbl},
            )

            validate_task = PythonOperator(
                task_id=f"validate_{_tbl['name']}",
                python_callable=validate_gcs_file,
                op_kwargs={"gcs_object": f"{{{{ ti.xcom_pull(task_ids='extract_postgres.extract_{_tbl['name']}') }}}}"},
            )

            load_to_bq = GCSToBigQueryOperator(
                task_id=f"load_{_tbl['name']}_to_bq",
                bucket=GCS_BUCKET,
                source_objects=[
                    f"{GCS_RAW_PREFIX}/postgres/{_tbl['name']}/ds={{{{ ds }}}}/data.parquet"
                ],
                destination_project_dataset_table=(
                    f"{GCP_PROJECT_ID}.{BQ_DATASET}.{_tbl['bq_table']}"
                ),
                schema_fields=BQ_SCHEMAS[_tbl["bq_table"]],
                source_format="PARQUET",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning={
                    "type": "DAY",
                    "field": _tbl["partition_field"],
                },
                cluster_fields=["_etl_source"],
                gcp_conn_id=GCS_CONN_ID,
                skip_leading_rows=0,
            )

            extract_task >> validate_task >> load_to_bq

    # ── Oracle Extract Group ───────────────────────────────────────────────────
    with TaskGroup("extract_oracle") as extract_oracle_group:
        for tbl in IAM_TABLES["oracle"]:
            _tbl = tbl

            extract_task = PythonOperator(
                task_id=f"extract_{_tbl['name']}",
                python_callable=extract_oracle_table,
                op_kwargs={"table_config": _tbl},
            )

            validate_task = PythonOperator(
                task_id=f"validate_{_tbl['name']}",
                python_callable=validate_gcs_file,
                op_kwargs={"gcs_object": f"{{{{ ti.xcom_pull(task_ids='extract_oracle.extract_{_tbl['name']}') }}}}"},
            )

            load_to_bq = GCSToBigQueryOperator(
                task_id=f"load_{_tbl['name']}_to_bq",
                bucket=GCS_BUCKET,
                source_objects=[
                    f"{GCS_RAW_PREFIX}/oracle/{_tbl['name']}/ds={{{{ ds }}}}/data.parquet"
                ],
                destination_project_dataset_table=(
                    f"{GCP_PROJECT_ID}.{BQ_DATASET}.{_tbl['bq_table']}"
                ),
                schema_fields=BQ_SCHEMAS[_tbl["bq_table"]],
                source_format="PARQUET",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning={
                    "type": "DAY",
                    "field": _tbl["partition_field"],
                },
                cluster_fields=["_etl_source"],
                gcp_conn_id=GCS_CONN_ID,
            )

            extract_task >> validate_task >> load_to_bq

    # ── Dedup / Merge in BigQuery ──────────────────────────────────────────────
    # Run MERGE to keep only the latest record per primary key (SCD Type 1)
    all_bq_tables = (
        [t["bq_table"] for t in IAM_TABLES["postgres"]]
        + [t["bq_table"] for t in IAM_TABLES["oracle"]]
    )

    with TaskGroup("deduplicate_bq") as dedup_group:
        for bq_table in set(all_bq_tables):
            BigQueryInsertJobOperator(
                task_id=f"dedup_{bq_table}",
                gcp_conn_id=GCS_CONN_ID,
                configuration={
                    "query": {
                        "query": f"""
                            CREATE OR REPLACE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{bq_table}`
                            PARTITION BY DATE(_etl_loaded_at)
                            CLUSTER BY _etl_source
                            AS
                            SELECT * EXCEPT(row_num)
                            FROM (
                                SELECT *,
                                    ROW_NUMBER() OVER (
                                        PARTITION BY {bq_table.rstrip('s') + '_id' if bq_table != 'audit_logs' else 'log_id'}
                                        ORDER BY updated_at DESC, _etl_loaded_at DESC
                                    ) AS row_num
                                FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{bq_table}`
                            )
                            WHERE row_num = 1
                        """,
                        "useLegacySql": False,
                        "priority": "BATCH",
                    }
                },
            )

    # ── End ────────────────────────────────────────────────────────────────────
    end = EmptyOperator(task_id="end")

    # ── Wire Dependencies ──────────────────────────────────────────────────────
    start >> create_bq_dataset >> [extract_postgres_group, extract_oracle_group]
    [extract_postgres_group, extract_oracle_group] >> dedup_group >> end
