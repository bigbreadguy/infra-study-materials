output "service_account_context" {
  description = "Managed service account identity."
  value = {
    account_id = google_service_account.practice.account_id
    email      = google_service_account.practice.email
    name       = google_service_account.practice.name
  }
}

output "granted_project_roles" {
  description = "Project roles granted to the managed service account."
  value       = sort(tolist(var.project_roles))
}

output "safety_contract" {
  description = "Safety expectations for the IAM lesson."
  value       = local.safety_contract
}
