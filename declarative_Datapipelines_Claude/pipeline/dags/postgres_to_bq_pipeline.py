"""
DAG: postgres_to_bq_pipeline
Description: Full pipeline - Postgres → GCS (raw) → Dataproc Spark (transform) → BigQuery
Schedule: Daily at 1AM UTC
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator,
)
from airflow.providers.google.cloud.operators.gcs import GCSCreateBucketOperator
from airflow.providers.google.cloud.transfers.postgres_to_gcs import PostgresToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable
import logging

# ─── Config ────────────────────────────────────────────────────────────────────
PROJECT_ID      = Variable.get("gcp_project_id", default_var="my-gcp-project")
REGION          = Variable.get("gcp_region",     default_var="us-central1")
ZONE            = Variable.get("gcp_zone",       default_var="us-central1-a")
GCS_BUCKET      = Variable.get("gcs_bucket",     default_var="my-data-pipeline-bucket")
BQ_DATASET      = Variable.get("bq_dataset",     default_var="analytics")
CLUSTER_NAME    = "spark-etl-cluster-{{ ds_nodash }}"
POSTGRES_CONN   = "postgres_default"
GCP_CONN        = "google_cloud_default"

# Tables to extract from Postgres
TABLES = [
    {"name": "orders",    "schema": "public", "partition_col": "created_at"},
    {"name": "customers", "schema": "public", "partition_col": "updated_at"},
    {"name": "products",  "schema": "public", "partition_col": "updated_at"},
    {"name": "payments",  "schema": "public", "partition_col": "payment_date"},
]

# Dataproc cluster config
CLUSTER_CONFIG = {
    "master_config": {
        "num_instances": 1,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
    },
    "worker_config": {
        "num_instances": 2,
        "machine_type_uri": "n1-standard-4",
        "disk_config": {"boot_disk_type": "pd-ssd", "boot_disk_size_gb": 100},
    },
    "secondary_worker_config": {   # preemptible workers for cost savings
        "num_instances": 2,
        "is_preemptible": True,
    },
    "software_config": {
        "image_version": "2.1-debian11",
        "properties": {
            "spark:spark.executor.memory":       "4g",
            "spark:spark.executor.cores":        "2",
            "spark:spark.dynamicAllocation.enabled": "true",
            "spark:spark.sql.adaptive.enabled":  "true",
        },
        "optional_components": ["JUPYTER"],
    },
    "gce_cluster_config": {
        "zone_uri": ZONE,
        "metadata": {"PIP_PACKAGES": "google-cloud-bigquery db-dtypes"},
    },
    "autoscaling_config": {
        "policy_uri": f"projects/{PROJECT_ID}/regions/{REGION}/autoscalingPolicies/spark-autoscale"
    },
}

# ─── Default Args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email":            ["data-alerts@company.com"],
    "email_on_failure": True,
    "email_on_retry":   False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=4),
}

# ─── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id="postgres_to_bq_pipeline",
    default_args=default_args,
    description="Postgres → GCS → Dataproc Spark → BigQuery",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "bigquery", "dataproc"],
    doc_md=__doc__,
) as dag:

    start = DummyOperator(task_id="start")
    end   = DummyOperator(task_id="end")

    # ── 1. Ensure GCS bucket & BQ dataset exist ─────────────────────────────────
    with TaskGroup("setup_infra") as setup_infra:
        ensure_bucket = GCSCreateBucketOperator(
            task_id="ensure_gcs_bucket",
            bucket_name=GCS_BUCKET,
            project_id=PROJECT_ID,
            location=REGION,
            gcp_conn_id=GCP_CONN,
        )
        ensure_dataset = BigQueryCreateEmptyDatasetOperator(
            task_id="ensure_bq_dataset",
            dataset_id=BQ_DATASET,
            project_id=PROJECT_ID,
            location=REGION,
            gcp_conn_id=GCP_CONN,
            exists_ok=True,
        )

    # ── 2. Extract: Postgres → GCS (raw Parquet/CSV) ────────────────────────────
    with TaskGroup("extract_postgres") as extract_group:
        extract_tasks = []
        for table in TABLES:
            extract = PostgresToGCSOperator(
                task_id=f"extract_{table['name']}",
                postgres_conn_id=POSTGRES_CONN,
                sql=f"""
                    SELECT *
                    FROM {table['schema']}.{table['name']}
                    WHERE {table['partition_col']} >= '{{{{ ds }}}}' ::date
                      AND {table['partition_col']} <  '{{{{ next_ds }}}}' ::date
                """,
                bucket=GCS_BUCKET,
                filename=f"raw/{table['name']}/{{{{ ds_nodash }}}}/{table['name']}_{{{{{{:04d}}}}}}.parquet",
                export_format="parquet",
                gzip=False,
                gcp_conn_id=GCP_CONN,
            )
            extract_tasks.append(extract)

    # ── 3. Create Dataproc Cluster ───────────────────────────────────────────────
    create_cluster = DataprocCreateClusterOperator(
        task_id="create_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_config=CLUSTER_CONFIG,
        region=REGION,
        cluster_name=CLUSTER_NAME,
        gcp_conn_id=GCP_CONN,
        delete_on_error=True,
    )

    # ── 4. Submit Spark Transform Jobs ──────────────────────────────────────────
    with TaskGroup("spark_transform") as spark_group:
        spark_tasks = []
        for table in TABLES:
            spark_job = {
                "reference":  {"project_id": PROJECT_ID},
                "placement":  {"cluster_name": CLUSTER_NAME},
                "pyspark_job": {
                    "main_python_file_uri": f"gs://{GCS_BUCKET}/spark_jobs/transform_{table['name']}.py",
                    "args": [
                        f"--input_path=gs://{GCS_BUCKET}/raw/{table['name']}/{{{{ ds_nodash }}}}/",
                        f"--output_path=gs://{GCS_BUCKET}/processed/{table['name']}/{{{{ ds_nodash }}}}/",
                        f"--execution_date={{{{ ds }}}}",
                        f"--project_id={PROJECT_ID}",
                        f"--bq_dataset={BQ_DATASET}",
                    ],
                    "python_file_uris": [
                        f"gs://{GCS_BUCKET}/spark_jobs/common/utils.py",
                    ],
                    "jar_file_uris": [
                        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                    ],
                    "properties": {
                        "spark.sql.parquet.compression.codec": "snappy",
                        "spark.sql.shuffle.partitions":        "200",
                    },
                },
            }
            submit = DataprocSubmitJobOperator(
                task_id=f"spark_transform_{table['name']}",
                job=spark_job,
                region=REGION,
                project_id=PROJECT_ID,
                gcp_conn_id=GCP_CONN,
            )
            spark_tasks.append(submit)

    # ── 5. Load processed data → BigQuery ───────────────────────────────────────
    with TaskGroup("load_bigquery") as bq_group:
        bq_tasks = []
        for table in TABLES:
            load = GCSToBigQueryOperator(
                task_id=f"load_{table['name']}_to_bq",
                bucket=GCS_BUCKET,
                source_objects=[f"processed/{table['name']}/{{{{ ds_nodash }}}}/*.parquet"],
                destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{table['name']}",
                source_format="PARQUET",
                write_disposition="WRITE_APPEND",
                create_disposition="CREATE_IF_NEEDED",
                time_partitioning={
                    "type":  "DAY",
                    "field": table["partition_col"],
                },
                clustering_fields=["id"] if table["name"] != "payments" else ["order_id"],
                gcp_conn_id=GCP_CONN,
                autodetect=True,
            )
            bq_tasks.append(load)

    # ── 6. Delete cluster (always runs) ─────────────────────────────────────────
    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_dataproc_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        gcp_conn_id=GCP_CONN,
        trigger_rule="all_done",   # runs even if upstream fails
    )

    # ── 7. Data quality check ────────────────────────────────────────────────────
    dq_check = DataprocSubmitJobOperator(
        task_id="data_quality_check",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": CLUSTER_NAME},
            "pyspark_job": {
                "main_python_file_uri": f"gs://{GCS_BUCKET}/spark_jobs/data_quality.py",
                "args": [
                    f"--project_id={PROJECT_ID}",
                    f"--bq_dataset={BQ_DATASET}",
                    f"--execution_date={{{{ ds }}}}",
                    f"--tables={','.join(t['name'] for t in TABLES)}",
                ],
                "jar_file_uris": [
                    "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar",
                ],
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id=GCP_CONN,
    )

    # ── DAG Dependencies ─────────────────────────────────────────────────────────
    start >> setup_infra >> extract_group >> create_cluster
    create_cluster >> spark_group >> dq_check >> delete_cluster >> end
    spark_group >> bq_group >> delete_cluster
