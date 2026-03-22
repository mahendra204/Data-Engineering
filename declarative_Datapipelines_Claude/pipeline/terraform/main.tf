# ─── terraform/main.tf ─────────────────────────────────────────────────────────
# Provisions: GCS buckets, Composer environment, BigQuery dataset,
#             Dataproc autoscaling policy, IAM service account

terraform {
  required_version = ">= 1.5"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
  backend "gcs" {
    bucket = "my-tf-state-bucket"
    prefix = "pipeline/state"
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ─── Variables ──────────────────────────────────────────────────────────────────
variable "project_id" { type = string }
variable "region"     { type = string; default = "us-central1" }
variable "zone"       { type = string; default = "us-central1-a" }
variable "env"        { type = string; default = "prod" }

locals {
  bucket_name      = "${var.project_id}-data-pipeline"
  spark_tmp_bucket = "${var.project_id}-spark-tmp"
  bq_dataset       = "analytics"
  sa_name          = "pipeline-runner"
}

# ─── Service Account ────────────────────────────────────────────────────────────
resource "google_service_account" "pipeline_sa" {
  account_id   = local.sa_name
  display_name = "Data Pipeline Runner SA"
}

locals {
  sa_roles = [
    "roles/bigquery.dataEditor",
    "roles/bigquery.jobUser",
    "roles/storage.objectAdmin",
    "roles/dataproc.editor",
    "roles/composer.worker",
    "roles/cloudsql.client",
  ]
}

resource "google_project_iam_member" "sa_roles" {
  for_each = toset(local.sa_roles)
  project  = var.project_id
  role     = each.value
  member   = "serviceAccount:${google_service_account.pipeline_sa.email}"
}

# ─── GCS Buckets ────────────────────────────────────────────────────────────────
resource "google_storage_bucket" "pipeline_bucket" {
  name          = local.bucket_name
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = false

  lifecycle_rule {
    action { type = "SetStorageClass"; storage_class = "NEARLINE" }
    condition { age = 30; matches_prefix = ["raw/"] }
  }
  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 90; matches_prefix = ["raw/"] }
  }
  lifecycle_rule {
    action { type = "SetStorageClass"; storage_class = "COLDLINE" }
    condition { age = 365; matches_prefix = ["processed/"] }
  }

  versioning { enabled = true }

  uniform_bucket_level_access = true
}

resource "google_storage_bucket" "spark_tmp" {
  name          = local.spark_tmp_bucket
  location      = var.region
  storage_class = "STANDARD"
  force_destroy = true

  lifecycle_rule {
    action { type = "Delete" }
    condition { age = 7 }
  }
}

# ─── BigQuery Dataset ───────────────────────────────────────────────────────────
resource "google_bigquery_dataset" "analytics" {
  dataset_id                 = local.bq_dataset
  location                   = var.region
  delete_contents_on_destroy = false
  description                = "Analytics data from the ETL pipeline"

  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }
  access {
    role          = "WRITER"
    user_by_email = google_service_account.pipeline_sa.email
  }
}

# ─── Dataproc Autoscaling Policy ────────────────────────────────────────────────
resource "google_dataproc_autoscaling_policy" "spark_autoscale" {
  policy_id = "spark-autoscale"
  location  = var.region

  worker_config {
    max_instances = 10
    min_instances = 2
    weight        = 1
  }

  secondary_worker_config {
    max_instances = 20
    weight        = 1
  }

  basic_algorithm {
    yarn_config {
      scale_up_factor   = 1.0
      scale_down_factor = 1.0
      scale_up_min_worker_fraction   = 0.0
      scale_down_min_worker_fraction = 0.0
      graceful_decommission_timeout  = "3600s"
    }
    cooldown_period = "120s"
  }
}

# ─── Cloud Composer Environment ─────────────────────────────────────────────────
resource "google_composer_environment" "pipeline_composer" {
  name   = "data-pipeline-composer-${var.env}"
  region = var.region

  config {
    software_config {
      image_version = "composer-2.6.6-airflow-2.7.3"
      python_version = "3"
      airflow_config_overrides = {
        "core-max_active_runs_per_dag"  = "1"
        "core-parallelism"              = "32"
        "scheduler-dag_dir_list_interval" = "60"
      }
      env_variables = {
        "GCP_PROJECT_ID" = var.project_id
        "GCS_BUCKET"     = local.bucket_name
        "BQ_DATASET"     = local.bq_dataset
        "GCP_REGION"     = var.region
      }
      pypi_packages = {
        "apache-airflow-providers-google"   = ">=10.0.0"
        "apache-airflow-providers-postgres" = ">=5.0.0"
      }
    }

    node_config {
      zone            = var.zone
      service_account = google_service_account.pipeline_sa.email
    }

    workloads_config {
      scheduler {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 5
        count      = 2
      }
      web_server {
        cpu       = 2
        memory_gb = 7.5
        storage_gb = 5
      }
      worker {
        cpu        = 2
        memory_gb  = 7.5
        storage_gb = 10
        min_count  = 2
        max_count  = 6
      }
    }

    environment_size = "ENVIRONMENT_SIZE_MEDIUM"
  }

  depends_on = [google_project_iam_member.sa_roles]
}

# ─── Outputs ────────────────────────────────────────────────────────────────────
output "composer_airflow_uri" {
  value = google_composer_environment.pipeline_composer.config[0].airflow_uri
}
output "gcs_bucket" {
  value = google_storage_bucket.pipeline_bucket.name
}
output "pipeline_sa_email" {
  value = google_service_account.pipeline_sa.email
}
