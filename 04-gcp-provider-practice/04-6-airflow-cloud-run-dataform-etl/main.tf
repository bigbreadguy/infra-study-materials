resource "google_service_account" "etl" {
  for_each = local.service_accounts

  project      = var.project_id
  account_id   = each.value.account_id
  display_name = each.value.display_name
  description  = each.value.description
  disabled     = false

  depends_on = [
    google_project_service.required["iam.googleapis.com"],
  ]
}

resource "google_storage_bucket" "raw" {
  name     = local.raw_bucket_name
  project  = var.project_id
  location = var.raw_bucket_location

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy_raw_bucket
  labels                      = local.common_labels

  versioning {
    enabled = var.raw_bucket_versioning_enabled
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = var.raw_object_retention_days
    }
  }

  depends_on = [
    google_project_service.required["storage.googleapis.com"],
  ]
}

resource "google_artifact_registry_repository" "worker_images" {
  project       = var.project_id
  location      = var.region
  repository_id = local.artifact_repository_id
  description   = "Docker images for the study ETL Cloud Run worker jobs."
  format        = "DOCKER"
  labels        = local.common_labels

  docker_config {
    immutable_tags = var.artifact_repository_immutable_tags
  }

  depends_on = [
    google_project_service.required["artifactregistry.googleapis.com"],
  ]
}

resource "google_bigquery_dataset" "raw" {
  project                    = var.project_id
  dataset_id                 = "raw_web_${var.environment}"
  friendly_name              = "Raw web scrape data (${var.environment})"
  description                = "Raw loaded scrape records from immutable GCS landing objects."
  location                   = var.bq_location
  labels                     = local.common_labels
  delete_contents_on_destroy = var.delete_bigquery_contents_on_destroy

  depends_on = [
    google_project_service.required["bigquery.googleapis.com"],
  ]
}

resource "google_bigquery_dataset" "staging" {
  project                    = var.project_id
  dataset_id                 = "stg_web_${var.environment}"
  friendly_name              = "Staging web models (${var.environment})"
  description                = "Dataform-owned staging outputs."
  location                   = var.bq_location
  labels                     = local.common_labels
  delete_contents_on_destroy = var.delete_bigquery_contents_on_destroy

  depends_on = [
    google_project_service.required["bigquery.googleapis.com"],
  ]
}

resource "google_bigquery_dataset" "mart" {
  project                    = var.project_id
  dataset_id                 = "mart_web_${var.environment}"
  friendly_name              = "Mart web models (${var.environment})"
  description                = "Dataform-owned final modeled outputs."
  location                   = var.bq_location
  labels                     = local.common_labels
  delete_contents_on_destroy = var.delete_bigquery_contents_on_destroy

  depends_on = [
    google_project_service.required["bigquery.googleapis.com"],
  ]
}

resource "google_bigquery_dataset" "ops" {
  project                    = var.project_id
  dataset_id                 = "ops_etl_${var.environment}"
  friendly_name              = "ETL operations ledger (${var.environment})"
  description                = "Airflow run ledger and quality-check tables."
  location                   = var.bq_location
  labels                     = local.common_labels
  delete_contents_on_destroy = var.delete_bigquery_contents_on_destroy

  depends_on = [
    google_project_service.required["bigquery.googleapis.com"],
  ]
}

resource "google_bigquery_table" "webpage_scrape_raw" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.raw.dataset_id
  table_id            = "webpage_scrape_raw"
  deletion_protection = var.bigquery_table_deletion_protection
  labels              = local.common_labels

  description = "Stable raw landing table loaded from GCS scrape outputs."

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }

  clustering = [
    "target_id",
    "schema_version",
  ]

  schema = jsonencode([
    {
      name        = "run_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Airflow or manual run identifier."
    },
    {
      name        = "run_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Logical run date used for partitioning."
    },
    {
      name        = "target_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Stable public placeholder target identifier."
    },
    {
      name        = "source_url"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Source URL recorded by the private action plan, never committed here."
    },
    {
      name        = "fetched_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Worker-side fetch timestamp."
    },
    {
      name        = "http_status"
      type        = "INTEGER"
      mode        = "NULLABLE"
      description = "HTTP status returned by the target, if applicable."
    },
    {
      name        = "content_hash"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Checksum used for idempotency checks."
    },
    {
      name        = "object_uri"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "GCS object URI for the immutable raw payload."
    },
    {
      name        = "schema_version"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Scraper output schema version."
    },
    {
      name        = "payload"
      type        = "JSON"
      mode        = "NULLABLE"
      description = "Raw parsed payload loaded from the GCS object."
    },
  ])
}

resource "google_bigquery_table" "etl_run_ledger" {
  project             = var.project_id
  dataset_id          = google_bigquery_dataset.ops.dataset_id
  table_id            = "etl_run_ledger"
  deletion_protection = var.bigquery_table_deletion_protection
  labels              = local.common_labels

  description = "Airflow-managed run ledger for scrape, load, transform, and quality status."

  time_partitioning {
    type  = "DAY"
    field = "run_date"
  }

  clustering = [
    "source_id",
    "target_set",
    "status",
  ]

  schema = jsonencode([
    {
      name        = "run_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Airflow or manual run identifier."
    },
    {
      name        = "dag_id"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Airflow DAG id."
    },
    {
      name        = "run_date"
      type        = "DATE"
      mode        = "REQUIRED"
      description = "Logical run date used for partitioning."
    },
    {
      name        = "source_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Generic source identifier."
    },
    {
      name        = "target_set"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Generic target-set identifier."
    },
    {
      name        = "action_plan_uri"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "GCS URI of the private action plan used by this run."
    },
    {
      name        = "raw_prefix"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "GCS raw prefix used by the run."
    },
    {
      name        = "status"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Run status recorded by Airflow."
    },
    {
      name        = "started_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Run start timestamp."
    },
    {
      name        = "finished_at"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Run finish timestamp."
    },
    {
      name        = "quality_summary"
      type        = "JSON"
      mode        = "NULLABLE"
      description = "Compact quality-check result summary."
    },
  ])
}

resource "google_secret_manager_secret" "scraper_credentials" {
  count = var.enable_scraper_credentials_secret ? 1 : 0

  project   = var.project_id
  secret_id = local.scraper_credentials_secret_id
  labels    = local.common_labels

  replication {
    auto {}
  }

  depends_on = [
    google_project_service.required["secretmanager.googleapis.com"],
  ]
}

resource "google_dataform_repository" "etl" {
  provider = google-beta

  project         = var.project_id
  region          = var.region
  name            = local.dataform_repository_id
  display_name    = "Study web ETL"
  service_account = google_service_account.etl["dataform_runner"].email
  labels          = local.common_labels

  depends_on = [
    google_project_service.required["dataform.googleapis.com"],
    google_service_account_iam_member.dataform_service_agent_act_as,
    google_service_account_iam_member.dataform_service_agent_token_creator,
  ]
}

resource "google_dataform_repository_release_config" "etl" {
  provider = google-beta

  project       = google_dataform_repository.etl.project
  region        = google_dataform_repository.etl.region
  repository    = google_dataform_repository.etl.name
  name          = local.dataform_release_config_id
  git_commitish = var.dataform_git_commitish

  code_compilation_config {
    default_database = var.project_id
    default_schema   = google_bigquery_dataset.staging.dataset_id
    default_location = var.bq_location
    assertion_schema = google_bigquery_dataset.ops.dataset_id

    vars = {
      environment     = var.environment
      raw_dataset     = google_bigquery_dataset.raw.dataset_id
      staging_dataset = google_bigquery_dataset.staging.dataset_id
      mart_dataset    = google_bigquery_dataset.mart.dataset_id
      ops_dataset     = google_bigquery_dataset.ops.dataset_id
    }
  }
}

resource "google_dataform_repository_workflow_config" "etl" {
  provider = google-beta

  project        = google_dataform_repository.etl.project
  region         = google_dataform_repository.etl.region
  repository     = google_dataform_repository.etl.name
  name           = local.dataform_workflow_config_id
  release_config = google_dataform_repository_release_config.etl.id

  invocation_config {
    included_tags                            = var.dataform_included_tags
    transitive_dependencies_included         = true
    transitive_dependents_included           = false
    fully_refresh_incremental_tables_enabled = var.dataform_fully_refresh_incremental_tables
    service_account                          = google_service_account.etl["dataform_runner"].email
  }
}
