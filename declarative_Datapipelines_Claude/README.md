# Postgres → GCS → BigQuery Data Pipeline

End-to-end data pipeline using **Cloud Composer (Airflow)**, **Dataproc (Spark)**, **GCS**, and **BigQuery**.

```
PostgreSQL ──► GCS (raw Parquet) ──► Dataproc Spark ──► GCS (processed) ──► BigQuery
                                          │
                                    Data Quality
                                       Checks
```

---

## Architecture

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Orchestration | Cloud Composer 2 (Airflow 2.7) | DAG scheduling, dependency management |
| Extraction | `PostgresToGCSOperator` | Incremental daily pull from Postgres → GCS raw zone |
| Processing | Dataproc Spark 3.4 (PySpark) | Transformations, deduplication, enrichment |
| Storage | GCS (Standard → Nearline → Coldline) | Raw + processed Parquet with lifecycle rules |
| Warehouse | BigQuery | Partitioned + clustered analytical tables |
| IaC | Terraform | Reproducible infrastructure provisioning |

---

## Project Structure

```
pipeline/
├── dags/
│   ├── postgres_to_bq_pipeline.py   # Main ETL DAG (runs daily)
│   └── bq_schema_init.py            # One-time schema setup DAG
├── spark_jobs/
│   ├── transform_generic.py         # Generic Spark transform driver
│   ├── data_quality.py              # Post-load DQ checks
│   └── common/
│       └── utils.py                 # Shared Spark helpers
├── config/
│   └── airflow_connections.sh       # Register Airflow connections
├── scripts/
│   └── deploy.sh                    # Deploy DAGs + Spark jobs
├── terraform/
│   └── main.tf                      # GCP infrastructure
├── tests/
│   └── test_transforms.py           # PySpark unit tests
└── requirements.txt
```

---

## Pipeline DAG Flow

```
start
  └─► setup_infra (ensure GCS bucket + BQ dataset)
        └─► extract_postgres (PostgresToGCS per table, parallel)
              └─► create_dataproc_cluster
                    ├─► spark_transform (per table, parallel)
                    │     └─► data_quality_check
                    └─► load_bigquery (per table, parallel)
                          └─► delete_dataproc_cluster  ◄── trigger_rule=all_done
                                └─► end
```

---

## Tables Extracted

| Table | Partition Column | PK for Dedup | BQ Clustering |
|-------|-----------------|--------------|---------------|
| orders | created_at | order_id | customer_id, status |
| customers | updated_at | customer_id | country_code, status |
| products | updated_at | product_id | category_l1, category_l2 |
| payments | payment_date | payment_id | order_id, payment_method_group |

---

## Prerequisites

- GCP project with APIs enabled: Composer, Dataproc, BigQuery, GCS, IAM
- Terraform >= 1.5
- `gcloud` CLI authenticated
- Postgres instance accessible from GCP (Cloud SQL or external with VPC peering)

---

## Setup

### 1. Provision Infrastructure

```bash
cd terraform
terraform init
terraform apply -var="project_id=my-project" -var="region=us-central1"
```

### 2. Register Airflow Connections

```bash
export GCP_PROJECT_ID=my-project
export COMPOSER_ENV=data-pipeline-composer-prod
export GCP_REGION=us-central1
export POSTGRES_HOST=10.0.0.5
export POSTGRES_DB=mydb
export POSTGRES_USER=etl_user
export POSTGRES_PASSWORD=secret

bash config/airflow_connections.sh
```

### 3. Deploy DAGs and Spark Jobs

```bash
export GCS_BUCKET_NAME=my-project-data-pipeline
export COMPOSER_ENV_NAME=data-pipeline-composer-prod
export INIT_SCHEMA=true   # first time only

bash scripts/deploy.sh
```

### 4. Run Tests

```bash
pip install -r requirements.txt
pytest tests/ -v
```

---

## Cost Controls

- **Preemptible workers**: Dataproc uses 2 preemptible secondary workers
- **Cluster ephemeral**: Cluster created at job start, deleted at end (`trigger_rule=all_done`)
- **Autoscaling**: Yarn-based autoscaling (2–10 standard, up to 20 preemptible)
- **GCS lifecycle**: Raw data moves to Nearline after 30d, deleted after 90d

---

## Airflow Variables

Set in Composer via UI or `airflow variables set`:

| Variable | Default | Description |
|----------|---------|-------------|
| `gcp_project_id` | — | GCP project ID |
| `gcp_region` | us-central1 | GCP region |
| `gcp_zone` | us-central1-a | GCP zone |
| `gcs_bucket` | — | Pipeline GCS bucket name |
| `bq_dataset` | analytics | BigQuery dataset name |

---

## Extending the Pipeline

To add a new table:

1. Add entry to `TABLES` list in `dags/postgres_to_bq_pipeline.py`
2. Add a transform function in `spark_jobs/transform_generic.py` and register in `TRANSFORMS`
3. Add schema to `dags/bq_schema_init.py`
4. Add DQ checks to `spark_jobs/data_quality.py`
5. Re-run `deploy.sh`
