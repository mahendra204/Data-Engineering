#!/usr/bin/env bash
# scripts/deploy.sh — Deploy DAGs and Spark jobs to GCS / Composer
set -euo pipefail

###############################################################################
# Config — override via environment variables
###############################################################################
PROJECT_ID="${GCP_PROJECT_ID:?Set GCP_PROJECT_ID}"
REGION="${GCP_REGION:-us-central1}"
GCS_BUCKET="${GCS_BUCKET_NAME:?Set GCS_BUCKET_NAME}"
COMPOSER_ENV="${COMPOSER_ENV_NAME:?Set COMPOSER_ENV_NAME}"

###############################################################################
# Derived paths
###############################################################################
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"

DAGS_DIR="$ROOT_DIR/dags"
SPARK_DIR="$ROOT_DIR/spark_jobs"

COMPOSER_DAG_BUCKET=$(gcloud composer environments describe "$COMPOSER_ENV" \
  --location "$REGION" \
  --format "value(config.dagGcsPrefix)")

echo "============================================================"
echo "Deploying to Project : $PROJECT_ID"
echo "GCS Bucket           : $GCS_BUCKET"
echo "Composer DAG Bucket  : $COMPOSER_DAG_BUCKET"
echo "============================================================"

###############################################################################
# 1. Upload Spark jobs to GCS
###############################################################################
echo ">>> Uploading Spark jobs..."
gsutil -m rsync -r -d "$SPARK_DIR" "gs://$GCS_BUCKET/spark_jobs"
echo "    Done."

###############################################################################
# 2. Upload DAGs to Composer bucket
###############################################################################
echo ">>> Uploading DAGs to Composer..."
gsutil -m rsync -r -d "$DAGS_DIR" "${COMPOSER_DAG_BUCKET}/"
echo "    Done."

###############################################################################
# 3. Set Airflow Variables in Composer
###############################################################################
echo ">>> Setting Airflow Variables..."
set_var() {
  gcloud composer environments run "$COMPOSER_ENV" \
    --location "$REGION" \
    variables -- set "$1" "$2"
}

set_var gcp_project_id "$PROJECT_ID"
set_var gcp_region     "$REGION"
set_var gcs_bucket     "$GCS_BUCKET"
set_var bq_dataset     "analytics"
set_var gcp_zone       "${GCP_ZONE:-us-central1-a}"

echo "    Variables set."

###############################################################################
# 4. Trigger schema-init DAG (first time only)
###############################################################################
if [[ "${INIT_SCHEMA:-false}" == "true" ]]; then
  echo ">>> Triggering bq_schema_init DAG..."
  gcloud composer environments run "$COMPOSER_ENV" \
    --location "$REGION" \
    dags trigger -- bq_schema_init
fi

echo "============================================================"
echo "Deployment complete!"
echo "============================================================"
