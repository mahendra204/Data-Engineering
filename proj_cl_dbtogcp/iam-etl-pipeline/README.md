# IAM ETL Pipeline — PostgreSQL / Oracle → GCS → BigQuery

Orchestrated with **Apache Airflow 2.9**

---

## Architecture

```
┌─────────────┐     ┌──────────────┐     ┌──────────────────┐     ┌─────────────┐
│  PostgreSQL │────▶│              │────▶│  Cloud Storage   │────▶│  BigQuery   │
│  (IAM DB)   │     │   Airflow    │     │  (Parquet files) │     │  (Tables)   │
└─────────────┘     │  Scheduler   │     └──────────────────┘     └─────────────┘
                    │              │              │                      │
┌─────────────┐     │  DAG:        │     GCS Path structure:     Partitioned by
│   Oracle    │────▶│  iam_etl_    │     raw/iam/postgres/{tbl}  created_at /
│  (IAM DB)   │     │  pipeline    │     raw/iam/oracle/{tbl}    load date
└─────────────┘     └──────────────┘
```

### Key design decisions

| Decision | Rationale |
|---|---|
| **Parquet on GCS** | Columnar, compressed, schema-preserving — ideal for BQ loads |
| **High-watermark incremental** | Efficient; only pulls `updated_at >= last_run` |
| **Snappy compression** | Fast, CPU-efficient; good for pipeline throughput |
| **BQ WRITE_APPEND + daily dedup** | Keeps a full audit trail; dedup step gives you SCD-1 view |
| **Time-partitioned BQ tables** | Cheaper queries, automatic partition pruning |
| **Separate DQ DAG** | Decoupled quality checks; easy to extend with Great Expectations |
| **GCS lifecycle policy** | Raw files → Nearline (30d) → Coldline (90d) → Delete (1y) |

---

## Project Structure

```
iam-etl-pipeline/
├── dags/
│   ├── iam_etl_dag.py            # Main ETL DAG (extract → GCS → BigQuery)
│   └── iam_data_quality_dag.py   # Data quality checks DAG
├── sql/
│   └── bigquery_setup.sql        # BQ dataset, views, audit table
├── config/
│   ├── airflow_variables.json    # Airflow Variables template
│   └── gcs_lifecycle.json        # GCS bucket lifecycle rules
├── scripts/
│   ├── setup_connections.sh      # Register Airflow connections
│   ├── setup_gcp.sh              # Create GCS bucket + BQ dataset
│   └── seed_postgres.sql         # Local dev seed data
├── docker/
│   └── docker-compose.yml        # Local Airflow + Postgres stack
├── tests/
│   └── test_etl_helpers.py       # Unit tests (pytest)
├── requirements.txt
└── README.md
```

---

## Quick Start (Local Development)

### 1. Prerequisites

- Docker & Docker Compose
- `gcloud` CLI (authenticated)
- `bq` CLI
- A GCP project with Cloud Storage and BigQuery APIs enabled

### 2. Clone / unzip and configure

```bash
cd iam-etl-pipeline

# Copy and edit variables
cp config/airflow_variables.json config/airflow_variables.json.local
# Edit: set your GCP_PROJECT_ID, GCS_BUCKET, etc.

# Place your GCP service account key
mkdir -p secrets
cp /path/to/your/sa-key.json secrets/sa-key.json
```

### 3. GCP setup

```bash
export GCP_PROJECT_ID=my-gcp-project
export GCS_BUCKET=iam-etl-bucket
bash scripts/setup_gcp.sh
```

### 4. Start Airflow

```bash
cd docker
docker compose up -d
```

Airflow UI → http://localhost:8080  
Username: `admin` / Password: `admin`

### 5. Register connections

```bash
docker exec -it docker-airflow-scheduler-1 bash
bash /opt/airflow/scripts/setup_connections.sh
```

### 6. Trigger the DAG

In Airflow UI, unpause and trigger `iam_etl_pipeline`.

---

## Production Deployment (Cloud Composer)

### Recommended setup

```
Cloud Composer 2 (Airflow 2.9)
  ├── Workload Identity for GCP auth (no key files)
  ├── Secret Manager for DB passwords
  └── DAGs synced from Cloud Storage bucket
```

### Steps

```bash
# Create Composer environment
gcloud composer environments create iam-airflow \
  --location us-central1 \
  --image-version composer-2.7.0-airflow-2.9.3 \
  --service-account YOUR_SA@PROJECT.iam.gserviceaccount.com

# Upload DAGs
gcloud composer environments storage dags import \
  --environment iam-airflow \
  --location us-central1 \
  --source dags/

# Set variables
gcloud composer environments run iam-airflow \
  --location us-central1 \
  variables -- import /home/airflow/gcs/data/airflow_variables.json
```

### IAM permissions required for the Airflow service account

| Permission | Purpose |
|---|---|
| `roles/storage.objectAdmin` | Read/write Parquet files on GCS |
| `roles/bigquery.dataEditor` | Load data into BigQuery |
| `roles/bigquery.jobUser` | Run BQ load and query jobs |

---

## DAGs

### `iam_etl_pipeline` (daily @ 02:00 UTC)

```
start
  └── create_bq_dataset
        ├── extract_postgres (TaskGroup)
        │     ├── extract_users → validate_users → load_users_to_bq
        │     ├── extract_roles → validate_roles → load_roles_to_bq
        │     ├── extract_user_roles → ...
        │     ├── extract_permissions → ...
        │     ├── extract_role_permissions → ...
        │     └── extract_audit_logs → ...
        └── extract_oracle (TaskGroup)
              ├── extract_groups → validate_groups → load_groups_to_bq
              ├── extract_user_groups → ...
              └── extract_policies → ...
  └── deduplicate_bq (TaskGroup)
        └── dedup_<each_table>
  └── end
```

### `iam_data_quality` (daily @ 02:30 UTC)

Runs after the main ETL. Writes results to `iam_data.etl_audit_log`.

Checks:
- Row count per table
- NULL rate on critical columns
- Data freshness (hours since last load)
- Referential integrity (e.g., orphan user_role records)

---

## BigQuery Tables

| Table | Source | Partitioned by |
|---|---|---|
| `users` | PostgreSQL | `created_at` |
| `roles` | PostgreSQL | `created_at` |
| `user_roles` | PostgreSQL | `valid_from` |
| `permissions` | PostgreSQL | `created_at` |
| `role_permissions` | PostgreSQL | `granted_at` |
| `audit_logs` | PostgreSQL | `created_at` |
| `groups` | Oracle | `created_at` |
| `user_groups` | Oracle | `joined_at` |
| `policies` | Oracle | `created_at` |

All tables include `_etl_loaded_at` and `_etl_source` audit columns.

### Useful views

| View | Description |
|---|---|
| `v_active_user_roles` | Active users with their current roles |
| `v_user_permission_matrix` | Full user → permission breakdown |
| `v_recent_audit_logs` | Last 30 days of audit events |
| `v_etl_load_summary` | Daily row counts per table (monitoring) |

---

## Configuration

### Airflow Variables

| Variable | Default | Description |
|---|---|---|
| `GCP_PROJECT_ID` | `my-gcp-project` | GCP project |
| `GCS_BUCKET` | `iam-etl-bucket` | GCS bucket name |
| `BQ_DATASET` | `iam_data` | BigQuery dataset |
| `BQ_LOCATION` | `US` | BQ dataset location |
| `POSTGRES_CONN_ID` | `postgres_iam` | Airflow connection ID |
| `ORACLE_CONN_ID` | `oracle_iam` | Airflow connection ID |
| `GCS_CONN_ID` | `google_cloud_default` | GCP connection ID |

### Watermarks

High-watermarks are stored as Airflow Variables with the key pattern:
`watermark_{source}_{table}` (e.g., `watermark_postgres_users`).

They are automatically updated after each successful load.
To force a full reload, delete or reset the variable in the Airflow UI.

---

## Running Tests

```bash
pip install -r requirements.txt
pytest tests/ -v --cov=dags
```

---

## GCS File Layout

```
gs://iam-etl-bucket/
├── raw/
│   └── iam/
│       ├── postgres/
│       │   ├── users/ds=2024-01-15/data.parquet
│       │   ├── roles/ds=2024-01-15/data.parquet
│       │   └── ...
│       └── oracle/
│           ├── groups/ds=2024-01-15/data.parquet
│           └── ...
└── staged/
    └── iam/  (reserved for future transformation layer)
```

---

## Extending the Pipeline

**Add a new table:**
1. Add an entry to `IAM_TABLES["postgres"]` or `IAM_TABLES["oracle"]` in `iam_etl_dag.py`
2. Add a schema to `BQ_SCHEMAS`
3. The DAG dynamically generates all tasks — no other changes needed

**Add a new source DB:**
1. Create a new `TaskGroup` following the postgres/oracle pattern
2. Register the Airflow connection
3. Wire it into the DAG dependencies

---

## License

Internal use. Replace with your organisation's license.
