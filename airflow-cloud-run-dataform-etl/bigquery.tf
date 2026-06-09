# Terraform owns the stable BigQuery contract that the Airflow DAG depends on:
# the dataset plus the durable target tables. The SQL that upserts sample rows
# belongs in the Python package; this file keeps the storage contract reviewed
# in IaC.
resource "google_bigquery_dataset" "securities" {
  project    = var.project_id
  dataset_id = var.bigquery_dataset_id
  location   = var.location

  friendly_name = "Bloomberg data layer"
  description   = "Dataset for Bloomberg sample dimensions and fact values managed by the Python/Airflow transform-load lesson."

  delete_contents_on_destroy = var.bigquery_dataset_delete_contents_on_destroy
  deletion_policy            = var.bigquery_dataset_deletion_policy

  labels = merge(local.common_labels, {
    data_layer = "bloomberg_data"
  })

  depends_on = [
    google_project_service.required["bigquery.googleapis.com"],
  ]
}

resource "google_bigquery_table" "securities" {
  for_each = local.bigquery_tables

  project    = var.project_id
  dataset_id = google_bigquery_dataset.securities.dataset_id
  table_id   = each.key

  friendly_name = each.value.friendly_name
  description   = each.value.description

  schema = jsonencode(each.value.schema)

  clustering               = length(each.value.clustering) > 0 ? each.value.clustering : null
  deletion_protection      = var.bigquery_table_deletion_protection
  require_partition_filter = each.value.partition_field != null

  labels = merge(local.common_labels, {
    data_layer = "bloomberg_data"
    table_role = each.value.table_role
  })

  dynamic "time_partitioning" {
    for_each = each.value.partition_field == null ? [] : [each.value.partition_field]

    content {
      type  = "DAY"
      field = time_partitioning.value
    }
  }
}
