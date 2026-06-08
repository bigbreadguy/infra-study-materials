resource "google_service_account_iam_member" "airflow_impersonators" {
  for_each = var.airflow_impersonators

  service_account_id = google_service_account.etl["airflow_orchestrator"].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

resource "google_service_account_iam_member" "terraform_deployer_act_as" {
  for_each = local.terraform_deployer_act_as_grants

  service_account_id = each.value.service_account_name
  role               = "roles/iam.serviceAccountUser"
  member             = each.value.principal
}

resource "google_service_account_iam_member" "dataform_service_agent_act_as" {
  service_account_id = google_service_account.etl["dataform_runner"].name
  role               = "roles/iam.serviceAccountUser"
  member             = google_project_service_identity.dataform.member
}

resource "google_service_account_iam_member" "dataform_service_agent_token_creator" {
  service_account_id = google_service_account.etl["dataform_runner"].name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = google_project_service_identity.dataform.member
}

resource "google_project_iam_member" "project" {
  for_each = local.project_iam_members

  project = var.project_id
  role    = each.value.role
  member  = each.value.member
}

resource "google_storage_bucket_iam_member" "raw_bucket" {
  for_each = local.bucket_iam_members

  bucket = google_storage_bucket.raw.name
  role   = each.value.role
  member = each.value.member
}

resource "google_bigquery_dataset_iam_member" "datasets" {
  for_each = local.dataset_iam_members

  project    = var.project_id
  dataset_id = each.value.dataset_id
  role       = each.value.role
  member     = each.value.member
}

resource "google_secret_manager_secret_iam_member" "scraper_credentials_accessor" {
  count = var.enable_scraper_credentials_secret ? 1 : 0

  project   = var.project_id
  secret_id = google_secret_manager_secret.scraper_credentials[0].secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = local.scraper_worker_member
}

resource "google_cloud_run_v2_job_iam_member" "scraper_executor" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.scrape_actions.name
  role     = "roles/run.jobsExecutor"
  member   = local.airflow_member
}

resource "google_cloud_run_v2_job_iam_member" "scraper_executor_with_overrides" {
  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.scrape_actions.name
  role     = "roles/run.jobsExecutorWithOverrides"
  member   = local.airflow_member
}

resource "google_cloud_run_v2_job_iam_member" "loader_executor" {
  count = var.enable_loader_job ? 1 : 0

  project  = var.project_id
  location = var.region
  name     = google_cloud_run_v2_job.load_raw_to_bigquery[0].name
  role     = "roles/run.jobsExecutor"
  member   = local.airflow_member
}

resource "google_dataform_repository_iam_member" "airflow_repository_access" {
  provider = google-beta

  project    = var.project_id
  region     = var.region
  repository = google_dataform_repository.etl.name
  role       = var.airflow_dataform_repository_role
  member     = local.airflow_member
}
