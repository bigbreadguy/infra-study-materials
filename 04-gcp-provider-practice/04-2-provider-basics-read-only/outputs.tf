output "project_context" {
  description = "Read-only project metadata returned by the Google provider."
  value = {
    project_id     = data.google_project.selected.project_id
    project_number = data.google_project.selected.number
    region         = var.region
    zone           = var.zone
  }
}

output "client_context" {
  description = "Provider context without exposing tokens or credential material."
  value = {
    configured_project = data.google_client_config.current.project
    configured_region  = data.google_client_config.current.region
    configured_zone    = data.google_client_config.current.zone
  }
}

output "common_labels" {
  description = "Labels that later GCP resources should reuse."
  value       = local.common_labels
}

output "safety_contract" {
  description = "Safety expectations for this read-only lesson."
  value       = local.safety_contract
}
