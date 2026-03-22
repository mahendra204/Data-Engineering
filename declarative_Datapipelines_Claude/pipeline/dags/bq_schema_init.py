"""
DAG: bq_schema_init
Description: One-time DAG to create BigQuery tables with explicit schemas, partitioning, and clustering.
Run manually before the first pipeline execution.
"""

from datetime import datetime
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.models import Variable

PROJECT_ID = Variable.get("gcp_project_id", default_var="my-gcp-project")
BQ_DATASET = Variable.get("bq_dataset",     default_var="analytics")
REGION     = Variable.get("gcp_region",     default_var="us-central1")
GCP_CONN   = "google_cloud_default"

SCHEMAS = {
    "orders": {
        "fields": [
            {"name": "order_id",         "type": "STRING",    "mode": "REQUIRED"},
            {"name": "customer_id",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "status",           "type": "STRING",    "mode": "NULLABLE"},
            {"name": "amount",           "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "currency",         "type": "STRING",    "mode": "NULLABLE"},
            {"name": "exchange_rate",    "type": "FLOAT64",   "mode": "NULLABLE"},
            {"name": "order_value_usd",  "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "is_high_value",    "type": "BOOL",      "mode": "NULLABLE"},
            {"name": "order_year",       "type": "INT64",     "mode": "NULLABLE"},
            {"name": "order_month",      "type": "INT64",     "mode": "NULLABLE"},
            {"name": "order_day",        "type": "INT64",     "mode": "NULLABLE"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_pipeline_date",   "type": "DATE",      "mode": "NULLABLE"},
            {"name": "_pipeline_timestamp","type":"TIMESTAMP","mode": "NULLABLE"},
            {"name": "_pipeline_version","type": "STRING",    "mode": "NULLABLE"},
        ],
        "time_partitioning": {"type": "DAY", "field": "created_at"},
        "clustering_fields": ["customer_id", "status"],
    },
    "customers": {
        "fields": [
            {"name": "customer_id",      "type": "STRING",    "mode": "REQUIRED"},
            {"name": "full_name",        "type": "STRING",    "mode": "NULLABLE"},
            {"name": "email",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "email_domain",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "phone",            "type": "STRING",    "mode": "NULLABLE"},
            {"name": "status",           "type": "STRING",    "mode": "NULLABLE"},
            {"name": "is_active",        "type": "BOOL",      "mode": "NULLABLE"},
            {"name": "country_code",     "type": "STRING",    "mode": "NULLABLE"},
            {"name": "customer_age_days","type": "INT64",     "mode": "NULLABLE"},
            {"name": "created_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "updated_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_pipeline_date",   "type": "DATE",      "mode": "NULLABLE"},
            {"name": "_pipeline_timestamp","type":"TIMESTAMP","mode": "NULLABLE"},
            {"name": "_pipeline_version","type": "STRING",    "mode": "NULLABLE"},
        ],
        "time_partitioning": {"type": "DAY", "field": "updated_at"},
        "clustering_fields": ["country_code", "status"],
    },
    "products": {
        "fields": [
            {"name": "product_id",       "type": "STRING",    "mode": "REQUIRED"},
            {"name": "sku",              "type": "STRING",    "mode": "NULLABLE"},
            {"name": "name",             "type": "STRING",    "mode": "NULLABLE"},
            {"name": "category_l1",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "category_l2",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "category_l3",      "type": "STRING",    "mode": "NULLABLE"},
            {"name": "category_path",    "type": "STRING",    "mode": "NULLABLE"},
            {"name": "list_price",       "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "sale_price",       "type": "NUMERIC",   "mode": "NULLABLE"},
            {"name": "discount_pct",     "type": "FLOAT64",   "mode": "NULLABLE"},
            {"name": "inventory_qty",    "type": "INT64",     "mode": "NULLABLE"},
            {"name": "in_stock",         "type": "BOOL",      "mode": "NULLABLE"},
            {"name": "updated_at",       "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "_pipeline_date",   "type": "DATE",      "mode": "NULLABLE"},
            {"name": "_pipeline_timestamp","type":"TIMESTAMP","mode": "NULLABLE"},
            {"name": "_pipeline_version","type": "STRING",    "mode": "NULLABLE"},
        ],
        "time_partitioning": {"type": "DAY", "field": "updated_at"},
        "clustering_fields": ["category_l1", "category_l2"],
    },
    "payments": {
        "fields": [
            {"name": "payment_id",           "type": "STRING",  "mode": "REQUIRED"},
            {"name": "order_id",             "type": "STRING",  "mode": "REQUIRED"},
            {"name": "customer_id",          "type": "STRING",  "mode": "NULLABLE"},
            {"name": "payment_method",       "type": "STRING",  "mode": "NULLABLE"},
            {"name": "payment_method_group", "type": "STRING",  "mode": "NULLABLE"},
            {"name": "status",               "type": "STRING",  "mode": "NULLABLE"},
            {"name": "amount",               "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "refund_amount",        "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "net_amount",           "type": "NUMERIC", "mode": "NULLABLE"},
            {"name": "is_refunded",          "type": "BOOL",    "mode": "NULLABLE"},
            {"name": "currency",             "type": "STRING",  "mode": "NULLABLE"},
            {"name": "payment_date",         "type": "TIMESTAMP","mode": "NULLABLE"},
            {"name": "_pipeline_date",       "type": "DATE",    "mode": "NULLABLE"},
            {"name": "_pipeline_timestamp",  "type":"TIMESTAMP","mode": "NULLABLE"},
            {"name": "_pipeline_version",    "type": "STRING",  "mode": "NULLABLE"},
        ],
        "time_partitioning": {"type": "DAY", "field": "payment_date"},
        "clustering_fields": ["order_id", "payment_method_group"],
    },
}

with DAG(
    dag_id="bq_schema_init",
    description="One-time BigQuery schema creation",
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["setup", "bigquery"],
) as dag:

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=BQ_DATASET,
        project_id=PROJECT_ID,
        location=REGION,
        gcp_conn_id=GCP_CONN,
        exists_ok=True,
    )

    table_tasks = []
    for table_name, schema in SCHEMAS.items():
        create_table = BigQueryCreateEmptyTableOperator(
            task_id=f"create_table_{table_name}",
            dataset_id=BQ_DATASET,
            table_id=table_name,
            project_id=PROJECT_ID,
            schema_fields=schema["fields"],
            time_partitioning=schema["time_partitioning"],
            cluster_fields=schema["clustering_fields"],
            gcp_conn_id=GCP_CONN,
            exists_ok=True,
        )
        table_tasks.append(create_table)

    create_dataset >> table_tasks
