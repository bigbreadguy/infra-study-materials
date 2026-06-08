output "project_id" {
  description = "GCP project configured for this ETL lesson."
  value       = var.project_id
}

output "region" {
  description = "Region used for Cloud Run, Artifact Registry, and Dataform."
  value       = var.region
}

output "bq_location" {
  description = "BigQuery dataset location."
  value       = var.bq_location
}

output "raw_bucket_name" {
  description = "Raw landing bucket for immutable scraper outputs."
  value       = google_storage_bucket.raw.name
}

output "bigquery_datasets" {
  description = "Dataset ids used by Airflow and Dataform."
  value = {
    raw     = google_bigquery_dataset.raw.dataset_id
    staging = google_bigquery_dataset.staging.dataset_id
    mart    = google_bigquery_dataset.mart.dataset_id
    ops     = google_bigquery_dataset.ops.dataset_id
  }
}

output "stable_bigquery_tables" {
  description = "Terraform-owned stable table ids. Dataform owns staging and mart model tables."
  value = {
    raw_webpage_scrape = google_bigquery_table.webpage_scrape_raw.id
    ops_run_ledger     = google_bigquery_table.etl_run_ledger.id
  }
}

output "scraper_job_name" {
  description = "Cloud Run Job name Airflow should execute for scraping."
  value       = google_cloud_run_v2_job.scrape_actions.name
}

output "scraper_job_location" {
  description = "Cloud Run Job region Airflow should use."
  value       = google_cloud_run_v2_job.scrape_actions.location
}

output "scraper_job_service_account_email" {
  description = "Runtime identity used by the scraper Cloud Run Job."
  value       = google_service_account.etl["scraper_worker"].email
}

output "scraper_image_repository" {
  description = "Artifact Registry Docker repository created for worker images."
  value = {
    repository_id = google_artifact_registry_repository.worker_images.repository_id
    registry_uri  = google_artifact_registry_repository.worker_images.registry_uri
  }
}

output "loader_job_name" {
  description = "Optional Cloud Run loader job name. Null when Airflow BigQuery loading is the selected path."
  value       = var.enable_loader_job ? google_cloud_run_v2_job.load_raw_to_bigquery[0].name : null
}

output "airflow_orchestrator_service_account_email" {
  description = "Service account for local Airflow impersonation."
  value       = google_service_account.etl["airflow_orchestrator"].email
}

output "dataform_repository_id" {
  description = "Dataform repository id."
  value       = google_dataform_repository.etl.name
}

output "dataform_release_config_id" {
  description = "Dataform release config resource id."
  value       = google_dataform_repository_release_config.etl.id
}

output "dataform_workflow_config_id" {
  description = "Dataform workflow config resource id."
  value       = google_dataform_repository_workflow_config.etl.id
}

output "service_account_emails" {
  description = "Service account emails created by this lesson."
  value       = local.service_account_emails
}

output "scraper_credentials_secret_id" {
  description = "Optional scraper credentials secret metadata id. Terraform never creates secret versions."
  value       = var.enable_scraper_credentials_secret ? google_secret_manager_secret.scraper_credentials[0].secret_id : null
}

output "enabled_project_services" {
  description = "APIs managed by google_project_service with disable_on_destroy=false."
  value       = sort(tolist(local.required_project_services))
}

output "airflow_runtime_contract" {
  description = "Values Airflow needs when configuring the DAG or connection variables."
  value = {
    project_id          = var.project_id
    region              = var.region
    bq_location         = var.bq_location
    raw_bucket          = google_storage_bucket.raw.name
    raw_dataset         = google_bigquery_dataset.raw.dataset_id
    staging_dataset     = google_bigquery_dataset.staging.dataset_id
    mart_dataset        = google_bigquery_dataset.mart.dataset_id
    ops_dataset         = google_bigquery_dataset.ops.dataset_id
    scraper_job_name    = google_cloud_run_v2_job.scrape_actions.name
    loader_job_name     = var.enable_loader_job ? google_cloud_run_v2_job.load_raw_to_bigquery[0].name : null
    dataform_repository = google_dataform_repository.etl.name
    dataform_release    = google_dataform_repository_release_config.etl.id
    dataform_workflow   = google_dataform_repository_workflow_config.etl.id
    impersonation_chain = google_service_account.etl["airflow_orchestrator"].email
  }
}

output "safety_contract" {
  description = "Safety confirmations and boundaries for this lesson."
  value       = local.safety_contract
}
