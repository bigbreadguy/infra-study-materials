output "state_backend_context" {
  description = "Values needed to configure the GCS backend after the bucket exists."
  value = {
    bucket = google_storage_bucket.terraform_state.name
    prefix = local.backend_prefix
  }
}

output "state_bucket_safety" {
  description = "Safety settings for the Terraform state bucket."
  value = {
    location                    = google_storage_bucket.terraform_state.location
    storage_class               = google_storage_bucket.terraform_state.storage_class
    uniform_bucket_level_access = google_storage_bucket.terraform_state.uniform_bucket_level_access
    public_access_prevention    = google_storage_bucket.terraform_state.public_access_prevention
    versioning_enabled          = true
    force_destroy_state_bucket  = var.force_destroy_state_bucket
  }
}

output "common_labels" {
  description = "Labels attached to the state bucket."
  value       = local.common_labels
}

output "safety_contract" {
  description = "Safety confirmations used by this remote state lesson."
  value = {
    billing_budget_confirmed = var.billing_budget_confirmed
    storage_api_enabled      = var.storage_api_enabled
  }
}
