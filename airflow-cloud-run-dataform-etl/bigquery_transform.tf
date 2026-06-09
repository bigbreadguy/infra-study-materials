# BigQuery needs project-level job execution permission, while table access is
# kept dataset-scoped. jobUser alone does not grant data access.
resource "google_project_iam_member" "bigquery_transformer_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.bigquery_transformer.email}"

  depends_on = [
    google_project_service.required["bigquery.googleapis.com"],
  ]
}

resource "google_bigquery_dataset_iam_member" "bigquery_transformer_dataset_editor" {
  project    = var.project_id
  dataset_id = google_bigquery_dataset.securities.dataset_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.bigquery_transformer.email}"
}

resource "google_storage_bucket_iam_member" "bigquery_transformer_raw_object_viewer" {
  bucket = google_storage_bucket.etl[var.airflow_gcs_target_bucket_key].name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.bigquery_transformer.email}"
}
