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

variable "storage_api_enabled" {
  description = "Set true only after storage.googleapis.com is enabled for the study project."
  type        = bool

  validation {
    condition     = var.storage_api_enabled
    error_message = "storage_api_enabled must be true before planning this Cloud Storage lesson."
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

variable "bucket_location" {
  description = "Cloud Storage bucket location. Keep early practice in a Free Tier eligible US region."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.bucket_location)
    error_message = "bucket_location must be one of: us-central1, us-east1, or us-west1."
  }
}

variable "bucket_name_prefix" {
  description = "Lower-case prefix for the globally unique bucket name. A random suffix is appended."
  type        = string
  default     = "tf-gcp-provider-practice"

  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9-]{1,50}[a-z0-9]$", var.bucket_name_prefix))
    error_message = "bucket_name_prefix must be 3 to 52 lower-case letters, numbers, or hyphens and cannot end with a hyphen."
  }
}

variable "force_destroy" {
  description = "Allow Terraform to delete the learning bucket even if objects are added during practice."
  type        = bool
  default     = true
}

variable "environment" {
  description = "Practice environment label."
  type        = string
  default     = "sandbox"

  validation {
    condition     = contains(["dev", "sandbox"], var.environment)
    error_message = "environment must be dev or sandbox for this study track."
  }
}

variable "owner" {
  description = "Lower-case label value identifying the learner."
  type        = string
  default     = "kenny"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_-]{0,62}$", var.owner))
    error_message = "owner must be a GCP-label-safe value up to 63 characters."
  }
}

variable "extra_labels" {
  description = "Additional GCP labels to merge into the shared label contract."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for key, value in var.extra_labels :
      can(regex("^[a-z][a-z0-9_-]{0,62}$", key))
      && can(regex("^[a-z0-9_-]{1,63}$", value))
    ])
    error_message = "extra_labels must use GCP-label-safe keys and non-empty values."
  }
}
