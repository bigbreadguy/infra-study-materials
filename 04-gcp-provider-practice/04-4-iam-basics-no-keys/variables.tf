variable "project_id" {
  description = "Dedicated GCP study project id."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be 6 to 30 characters, start with a lower-case letter, and use only lower-case letters, numbers, or hyphens."
  }
}

variable "billing_budget_confirmed" {
  description = "Set true only after a budget alert exists for the dedicated study project."
  type        = bool

  validation {
    condition     = var.billing_budget_confirmed
    error_message = "billing_budget_confirmed must be true before running this GCP lesson."
  }
}

variable "iam_api_enabled" {
  description = "Set true only after iam.googleapis.com is enabled for the study project."
  type        = bool

  validation {
    condition     = var.iam_api_enabled
    error_message = "iam_api_enabled must be true before planning this IAM lesson."
  }
}

variable "region" {
  description = "Default provider region for this GCP practice track."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.region)
    error_message = "region must be one of: us-central1, us-east1, or us-west1."
  }
}

variable "service_account_id" {
  description = "Service account id without the project domain suffix."
  type        = string
  default     = "tf-practice-runner"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.service_account_id))
    error_message = "service_account_id must be 6 to 30 characters, start with a lower-case letter, and use lower-case letters, numbers, or hyphens."
  }
}

variable "service_account_display_name" {
  description = "Human-readable service account display name."
  type        = string
  default     = "Terraform Practice Runner"

  validation {
    condition     = length(trimspace(var.service_account_display_name)) >= 3
    error_message = "service_account_display_name must contain at least three non-space characters."
  }
}

variable "project_roles" {
  description = "Narrow project roles to grant to the study service account."
  type        = set(string)
  default     = ["roles/storage.objectViewer"]

  validation {
    condition = alltrue([
      for role in var.project_roles :
      can(regex("^roles/[A-Za-z0-9.]+$", role))
    ])
    error_message = "project_roles must contain predefined role names such as roles/storage.objectViewer."
  }
}
