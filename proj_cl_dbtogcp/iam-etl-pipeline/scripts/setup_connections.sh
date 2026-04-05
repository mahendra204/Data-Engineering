#!/usr/bin/env bash
# =============================================================================
# setup_connections.sh
# Run this once to register all Airflow connections.
# Update the values below before running.
# =============================================================================

set -euo pipefail

# ── PostgreSQL IAM connection ─────────────────────────────────────────────────
airflow connections add postgres_iam \
  --conn-type    postgres \
  --conn-host    "YOUR_POSTGRES_HOST" \
  --conn-port    5432 \
  --conn-schema  "iam" \
  --conn-login   "YOUR_POSTGRES_USER" \
  --conn-password "YOUR_POSTGRES_PASSWORD"

echo "✅ postgres_iam registered"

# ── Oracle IAM connection ─────────────────────────────────────────────────────
airflow connections add oracle_iam \
  --conn-type    oracle \
  --conn-host    "YOUR_ORACLE_HOST" \
  --conn-port    1521 \
  --conn-schema  "IAM" \
  --conn-login   "YOUR_ORACLE_USER" \
  --conn-password "YOUR_ORACLE_PASSWORD" \
  --conn-extra   '{"service_name": "ORCLPDB1"}'

echo "✅ oracle_iam registered"

# ── Google Cloud (GCS + BigQuery) ─────────────────────────────────────────────
# Option A: Workload Identity (recommended on GKE / Cloud Composer)
#   Just ensure the Airflow service account has the right IAM bindings.

# Option B: Service Account key file
airflow connections add google_cloud_default \
  --conn-type    google_cloud_platform \
  --conn-extra   "{\"project\": \"YOUR_GCP_PROJECT\", \"key_path\": \"/opt/airflow/secrets/sa-key.json\"}"

echo "✅ google_cloud_default registered"

# ── Import Airflow Variables ───────────────────────────────────────────────────
airflow variables import /opt/airflow/config/airflow_variables.json

echo "✅ Variables imported"
echo ""
echo "All connections and variables set up successfully."
