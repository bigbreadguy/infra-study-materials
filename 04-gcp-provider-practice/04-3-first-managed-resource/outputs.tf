output "bucket_context" {
  description = "Identity and location of the managed learning bucket."
  value = {
    name          = google_storage_bucket.lesson.name
    url           = "gs://${google_storage_bucket.lesson.name}"
    location      = google_storage_bucket.lesson.location
    storage_class = google_storage_bucket.lesson.storage_class
  }
}

output "common_labels" {
  description = "Labels attached to the learning bucket."
  value       = local.common_labels
}

output "safety_contract" {
  description = "Safety confirmations used by this managed-resource lesson."
  value = {
    billing_budget_confirmed = var.billing_budget_confirmed
    storage_api_enabled      = var.storage_api_enabled
    force_destroy            = var.force_destroy
  }
}
