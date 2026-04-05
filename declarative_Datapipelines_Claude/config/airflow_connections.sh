# config/airflow_connections.sh
# Run once to register Airflow connections in Composer via gcloud CLI
# Usage: GCP_PROJECT_ID=xxx COMPOSER_ENV=xxx GCP_REGION=xxx bash config/airflow_connections.sh

set -euo pipefail

PROJECT_ID="${GCP_PROJECT_ID:?}"
COMPOSER_ENV="${COMPOSER_ENV:?}"
REGION="${GCP_REGION:-us-central1}"

run_airflow() {
  gcloud composer environments run "$COMPOSER_ENV" --location "$REGION" "$@"
}

echo ">>> Setting up Airflow connections..."

# Postgres connection (replace values with your actual Postgres credentials)
run_airflow connections -- add postgres_default \
  --conn-type postgres \
  --conn-host "${POSTGRES_HOST:-localhost}" \
  --conn-schema "${POSTGRES_DB:-mydb}" \
  --conn-login "${POSTGRES_USER:-postgres}" \
  --conn-password "${POSTGRES_PASSWORD:?Set POSTGRES_PASSWORD}" \
  --conn-port "${POSTGRES_PORT:-5432}" \
  --conn-extra '{"sslmode": "require"}'

# GCP connection (uses Workload Identity / default SA in Composer)
run_airflow connections -- add google_cloud_default \
  --conn-type google_cloud_platform \
  --conn-extra "{\"project\": \"${PROJECT_ID}\", \"scope\": \"https://www.googleapis.com/auth/cloud-platform\"}"

echo ">>> Connections registered."
