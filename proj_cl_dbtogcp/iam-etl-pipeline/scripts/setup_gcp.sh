#!/usr/bin/env bash
# =============================================================================
# setup_gcp.sh
# Creates GCS bucket, applies lifecycle policy, and creates BQ dataset.
# Requires gcloud and bq CLI tools authenticated.
# =============================================================================

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:-my-gcp-project}"
BUCKET_NAME="${GCS_BUCKET:-iam-etl-bucket}"
BQ_DATASET="${BQ_DATASET:-iam_data}"
BQ_LOCATION="${BQ_LOCATION:-US}"
REGION="${GCS_REGION:-us-central1}"

echo "=== Setting up GCP resources for IAM ETL Pipeline ==="
echo "Project : $PROJECT_ID"
echo "Bucket  : $BUCKET_NAME"
echo "Dataset : $BQ_DATASET"
echo ""

# ── 1. Create GCS Bucket ──────────────────────────────────────────────────────
if gsutil ls -b "gs://${BUCKET_NAME}" &>/dev/null; then
  echo "ℹ️  Bucket gs://${BUCKET_NAME} already exists"
else
  gsutil mb \
    -p "$PROJECT_ID" \
    -l "$REGION" \
    -c STANDARD \
    --uniform-bucket-level-access \
    "gs://${BUCKET_NAME}"
  echo "✅ Bucket created: gs://${BUCKET_NAME}"
fi

# ── 2. Enable versioning (protect against accidental deletes) ─────────────────
gsutil versioning set on "gs://${BUCKET_NAME}"
echo "✅ Versioning enabled"

# ── 3. Apply lifecycle policy ─────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
gsutil lifecycle set "${SCRIPT_DIR}/../config/gcs_lifecycle.json" "gs://${BUCKET_NAME}"
echo "✅ Lifecycle policy applied"

# ── 4. Create folder structure (dummy objects) ────────────────────────────────
for prefix in raw/iam/postgres raw/iam/oracle staged/iam; do
  echo "" | gsutil cp - "gs://${BUCKET_NAME}/${prefix}/.keep" 2>/dev/null || true
done
echo "✅ Folder structure created"

# ── 5. BigQuery Dataset ───────────────────────────────────────────────────────
if bq ls --project_id="$PROJECT_ID" "$BQ_DATASET" &>/dev/null; then
  echo "ℹ️  BigQuery dataset ${BQ_DATASET} already exists"
else
  bq mk \
    --project_id="$PROJECT_ID" \
    --dataset \
    --location="$BQ_LOCATION" \
    --description="IAM data from PostgreSQL and Oracle" \
    "${PROJECT_ID}:${BQ_DATASET}"
  echo "✅ BigQuery dataset created: ${BQ_DATASET}"
fi

# ── 6. Run BigQuery setup SQL ─────────────────────────────────────────────────
# Replace placeholder project id in SQL before running
SQL_FILE="${SCRIPT_DIR}/../sql/bigquery_setup.sql"
TMP_SQL="/tmp/bigquery_setup_${PROJECT_ID}.sql"
sed "s/my-gcp-project/${PROJECT_ID}/g" "$SQL_FILE" > "$TMP_SQL"
bq query --project_id="$PROJECT_ID" --use_legacy_sql=false < "$TMP_SQL"
echo "✅ BigQuery tables and views created"

echo ""
echo "=== GCP setup complete ==="
