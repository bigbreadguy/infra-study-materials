output "project_id" {
  description = "GCP project configured for this lesson."
  value       = var.project_id
}

output "location" {
  description = "Location used for GCS and BigQuery resources in this lesson."
  value       = var.location
}

output "buckets" {
  description = "Created GCS buckets keyed by logical role (raw, temp, ...)."
  value = {
    for key, bucket in google_storage_bucket.etl :
    key => {
      name = bucket.name
      url  = bucket.url
    }
  }
}

output "airflow_service_account_email" {
  description = "Email for the Airflow GCP connection base service account."
  value       = google_service_account.airflow_orchestrator.email
}

output "airflow_gcs_uploader_service_account_email" {
  description = "Email for the Airflow GCS upload impersonation-chain service account."
  value       = google_service_account.airflow_gcs_uploader.email
}

output "airflow_gcs_bucket_access" {
  description = "Bucket-level IAM grant configured for Airflow's GCS upload impersonation-chain identity."
  value = {
    bucket_key  = var.airflow_gcs_target_bucket_key
    bucket_name = google_storage_bucket.etl[var.airflow_gcs_target_bucket_key].name
    role        = local.airflow_gcs_bucket_role
    member      = "serviceAccount:${google_service_account.airflow_gcs_uploader.email}"
    delete_permission_included = (
      (local.airflow_gcs_uses_custom_cru_role && var.airflow_gcs_allow_object_replacement)
      || local.airflow_gcs_bucket_role == "roles/storage.objectUser"
      || local.airflow_gcs_bucket_role == "roles/storage.objectAdmin"
    )
    object_replacement_allowed = var.airflow_gcs_allow_object_replacement
  }
}

output "airflow_service_account_key_json" {
  description = "Decoded Airflow orchestrator service account key JSON. Null unless create_airflow_service_account_key=true; sensitive because Terraform state contains the private key."
  value       = var.create_airflow_service_account_key ? base64decode(google_service_account_key.airflow_orchestrator[0].private_key) : null
  sensitive   = true
}

output "enabled_project_services" {
  description = "APIs managed by google_project_service with disable_on_destroy=false."
  value       = sort(tolist(local.required_project_services))
}

output "bigquery_dataset" {
  description = "BigQuery dataset configured for Bloomberg sample data."
  value = {
    project    = google_bigquery_dataset.securities.project
    dataset_id = google_bigquery_dataset.securities.dataset_id
    id         = google_bigquery_dataset.securities.id
    location   = google_bigquery_dataset.securities.location
  }
}

output "bigquery_table_schema_contract" {
  description = "Active BigQuery table contract: mongo for the Python/Airflow transform-load schema, legacy only for deletion-protection migration staging."
  value       = var.bigquery_table_schema_contract
}

output "bigquery_tables" {
  description = "Stable BigQuery tables keyed by table id."
  value = {
    for key, table in google_bigquery_table.securities :
    key => {
      dataset_id = table.dataset_id
      table_id   = table.table_id
      full_name  = "${table.project}.${table.dataset_id}.${table.table_id}"
    }
  }
}

output "airflow_orchestrator_service_account_email" {
  description = "Email for the Airflow service account that orchestrates upload and BigQuery transform-load tasks."
  value       = google_service_account.airflow_orchestrator.email
}

output "bigquery_transformer_service_account_email" {
  description = "Email for the service account Airflow impersonates to run BigQuery transform-load jobs."
  value       = google_service_account.bigquery_transformer.email
}

output "airflow_impersonation_chains" {
  description = "Service accounts the local Airflow DAG should set as task-level impersonation chains."
  value = {
    gcp_connection_service_account = google_service_account.airflow_orchestrator.email
    gcs_impersonation_chain        = google_service_account.airflow_gcs_uploader.email
    bigquery_impersonation_chain   = google_service_account.bigquery_transformer.email
  }
}

output "bigquery_transform_load" {
  description = "Airflow Variable values for the Python/Airflow BigQuery transform-load workflow. The DAG derives raw_gcs_uri from raw_bucket_name and raw_object_prefix."
  value = {
    bigquery_project_id = var.project_id
    bigquery_dataset_id = google_bigquery_dataset.securities.dataset_id
    bigquery_region     = google_bigquery_dataset.securities.location
    raw_bucket_name     = google_storage_bucket.etl[var.airflow_gcs_target_bucket_key].name
    raw_object_prefix   = local.raw_object_prefix
  }
}

###############################################################################
# Scraper Cloud Run Job (Phase 4)
###############################################################################

output "scraper_artifact_repository" {
  description = "Shared Artifact Registry Docker repo hosting the scraper image."
  value = {
    id       = google_artifact_registry_repository.docker.repository_id
    location = google_artifact_registry_repository.docker.location
  }
}

output "scraper_image_uri" {
  description = "Full image URI the Cloud Run Job pulls. Build and push this tag before applying the job."
  value       = local.scraper_image_uri
}

output "scraper_job" {
  description = "Scraper Cloud Run Job coordinates for the orchestrating DAG."
  value = {
    name     = google_cloud_run_v2_job.scraper.name
    location = google_cloud_run_v2_job.scraper.location
    project  = var.project_id
  }
}

output "scraper_service_account_email" {
  description = "Runtime identity of the scraper Cloud Run Job."
  value       = google_service_account.scraper_sa.email
}

output "scraper_scrape_bucket_name" {
  description = "Bucket holding scrape/requests and scrape/results objects (dfml-<env>-raw)."
  value       = google_storage_bucket.scrape_raw.name
}

output "scraper_secret_ids" {
  description = "Per-target credential secret ids created (values added out of band)."
  value       = sort([for s in google_secret_manager_secret.scraper : s.secret_id])
}

output "scraper_airflow_variables" {
  description = "Values for the WI3 scraper DAG's Airflow Variables. gcp_conn_id identity is the orchestrator SA."
  value = {
    gcp_connection_service_account = google_service_account.airflow_orchestrator.email
    cloud_run_job_name             = google_cloud_run_v2_job.scraper.name
    cloud_run_region               = google_cloud_run_v2_job.scraper.location
    gcp_project_id                 = var.project_id
    scrape_bucket_name             = google_storage_bucket.scrape_raw.name
  }
}

output "safety_contract" {
  description = "Safety confirmations and boundaries for this lesson."
  value       = local.safety_contract
}
