# Narrow identity for Airflow GCSHook tasks. Local Airflow authenticates to GCP
# as the orchestrator service account, then uses this account as the GCS
# impersonation chain.
resource "google_service_account" "airflow_gcs_uploader" {
  project      = var.project_id
  account_id   = var.airflow_service_account_id
  display_name = var.airflow_service_account_display_name
  description  = "Used by Apache Airflow GCSHook tasks to upload raw ETL objects to Cloud Storage."
  disabled     = false

  depends_on = [
    google_project_service.required["iam.googleapis.com"],
  ]
}

# Custom GCS object role for the Airflow GCS upload identity. It includes
# storage.objects.delete by default because the current Airflow upload path may
# replace an object at an existing key.
resource "google_project_iam_custom_role" "airflow_gcs_object_cru" {
  count = local.airflow_gcs_uses_custom_cru_role ? 1 : 0

  project     = var.project_id
  role_id     = var.airflow_gcs_custom_cru_role_id
  title       = var.airflow_gcs_allow_object_replacement ? "Airflow GCS object replace" : "Airflow GCS object CRU"
  description = var.airflow_gcs_allow_object_replacement ? "Allows Airflow to create, read, list, update, and replace GCS objects." : "Allows Airflow to create, read, list, and update GCS objects without delete access."
  stage       = "GA"

  permissions = sort(tolist(local.airflow_gcs_custom_role_permissions))

  depends_on = [
    google_project_service.required["iam.googleapis.com"],
    google_project_service.required["storage.googleapis.com"],
  ]
}

# Optional keyless local-Airflow path: grant specific developer/user/service
# principals permission to impersonate the GCS-only service account directly.
resource "google_service_account_iam_member" "airflow_gcs_impersonators" {
  for_each = var.airflow_service_account_impersonators

  service_account_id = google_service_account.airflow_gcs_uploader.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

# Escape hatch only. Prefer ADC + impersonation or workload identity. If this is
# enabled, the generated private key is stored in Terraform state.
resource "google_service_account_key" "airflow_orchestrator" {
  count = var.create_airflow_service_account_key ? 1 : 0

  service_account_id = google_service_account.airflow_orchestrator.name
  private_key_type   = "TYPE_GOOGLE_CREDENTIALS_FILE"
}

# Airflow's Google Cloud connection identity. The DAG uses task-level
# impersonation chains for the GCS uploader and BigQuery transformer accounts.
resource "google_service_account" "airflow_orchestrator" {
  project      = var.project_id
  account_id   = var.airflow_orchestrator_service_account_id
  display_name = var.airflow_orchestrator_service_account_display_name
  description  = "Used by Apache Airflow tasks to orchestrate raw uploads and BigQuery transform-load jobs."
  disabled     = false

  depends_on = [
    google_project_service.required["iam.googleapis.com"],
  ]
}

resource "google_service_account_iam_member" "airflow_orchestrator_impersonators" {
  for_each = var.airflow_orchestrator_impersonators

  service_account_id = google_service_account.airflow_orchestrator.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = each.value
}

resource "google_service_account_iam_member" "airflow_orchestrator_gcs_uploader_token_creator" {
  service_account_id = google_service_account.airflow_gcs_uploader.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.airflow_orchestrator.email}"
}

# Default is the custom role above. Prefer this narrow role before broader
# predefined roles such as roles/storage.objectUser. The grant is intentionally
# on the GCS uploader identity because the DAG uses it as the GCS
# impersonation_chain.
resource "google_storage_bucket_iam_member" "airflow_gcs_upload" {
  bucket = google_storage_bucket.etl[var.airflow_gcs_target_bucket_key].name
  role   = local.airflow_gcs_bucket_role
  member = "serviceAccount:${google_service_account.airflow_gcs_uploader.email}"
}

# Airflow BigQuery transform-load tasks impersonate this account to run query
# jobs, edit only the Bloomberg dataset, and read raw NDJSON from GCS.
resource "google_service_account" "bigquery_transformer" {
  project      = var.project_id
  account_id   = var.bigquery_transform_service_account_id
  display_name = var.bigquery_transform_service_account_display_name
  description  = "Used by Airflow BigQuery transform-load tasks to upsert Bloomberg sample rows."
  disabled     = false

  depends_on = [
    google_project_service.required["iam.googleapis.com"],
  ]
}

resource "google_service_account_iam_member" "airflow_orchestrator_bigquery_transformer_token_creator" {
  service_account_id = google_service_account.bigquery_transformer.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.airflow_orchestrator.email}"
}
