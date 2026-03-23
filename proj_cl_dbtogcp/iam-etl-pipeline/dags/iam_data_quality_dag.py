"""
IAM Data Quality DAG
Runs after the main ETL to validate row counts, nulls, and freshness.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.models import Variable

GCP_PROJECT_ID = Variable.get("GCP_PROJECT_ID", default_var="my-gcp-project")
GCS_CONN_ID    = Variable.get("GCS_CONN_ID",    default_var="google_cloud_default")
BQ_DATASET     = Variable.get("BQ_DATASET",     default_var="iam_data")
BQ_AUDIT_TABLE = f"{GCP_PROJECT_ID}.{BQ_DATASET}.etl_audit_log"

default_args = {
    "owner": "data-engineering",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

DQ_CHECKS = [
    {
        "name": "users_row_count",
        "sql": f"""
            INSERT INTO `{BQ_AUDIT_TABLE}` (check_name, table_name, metric, value, run_at)
            SELECT
              'row_count' AS check_name,
              'users'     AS table_name,
              'total_rows' AS metric,
              COUNT(*)    AS value,
              CURRENT_TIMESTAMP() AS run_at
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.users`
        """,
    },
    {
        "name": "users_null_email_check",
        "sql": f"""
            INSERT INTO `{BQ_AUDIT_TABLE}` (check_name, table_name, metric, value, run_at)
            SELECT
              'null_check'  AS check_name,
              'users'       AS table_name,
              'null_emails' AS metric,
              COUNTIF(email IS NULL) AS value,
              CURRENT_TIMESTAMP() AS run_at
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.users`
        """,
    },
    {
        "name": "users_freshness_check",
        "sql": f"""
            INSERT INTO `{BQ_AUDIT_TABLE}` (check_name, table_name, metric, value, run_at)
            SELECT
              'freshness_hours' AS check_name,
              'users'           AS table_name,
              'hours_since_last_load' AS metric,
              TIMESTAMP_DIFF(CURRENT_TIMESTAMP(), MAX(_etl_loaded_at), HOUR) AS value,
              CURRENT_TIMESTAMP() AS run_at
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.users`
        """,
    },
    {
        "name": "orphan_user_roles_check",
        "sql": f"""
            INSERT INTO `{BQ_AUDIT_TABLE}` (check_name, table_name, metric, value, run_at)
            SELECT
              'referential_integrity' AS check_name,
              'user_roles'            AS table_name,
              'orphan_user_ids'       AS metric,
              COUNT(*)                AS value,
              CURRENT_TIMESTAMP()     AS run_at
            FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.user_roles` ur
            LEFT JOIN `{GCP_PROJECT_ID}.{BQ_DATASET}.users` u USING (user_id)
            WHERE u.user_id IS NULL
        """,
    },
]

# Ensure audit log table exists before running checks
CREATE_AUDIT_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS `{BQ_AUDIT_TABLE}` (
        check_name  STRING,
        table_name  STRING,
        metric      STRING,
        value       INT64,
        run_at      TIMESTAMP
    )
"""

with DAG(
    dag_id="iam_data_quality",
    description="Data quality checks for IAM BigQuery tables",
    default_args=default_args,
    schedule_interval="30 2 * * *",   # 30 min after main ETL
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["iam", "dq", "bigquery"],
) as dag:

    start = EmptyOperator(task_id="start")

    create_audit_table = BigQueryInsertJobOperator(
        task_id="create_audit_table",
        gcp_conn_id=GCS_CONN_ID,
        configuration={
            "query": {
                "query": CREATE_AUDIT_TABLE_SQL,
                "useLegacySql": False,
            }
        },
    )

    dq_tasks = []
    for check in DQ_CHECKS:
        t = BigQueryInsertJobOperator(
            task_id=f"dq_{check['name']}",
            gcp_conn_id=GCS_CONN_ID,
            configuration={
                "query": {
                    "query": check["sql"],
                    "useLegacySql": False,
                }
            },
        )
        dq_tasks.append(t)

    end = EmptyOperator(task_id="end")

    start >> create_audit_table >> dq_tasks >> end
