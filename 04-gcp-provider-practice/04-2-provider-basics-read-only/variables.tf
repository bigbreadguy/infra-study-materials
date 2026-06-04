variable "project_id" {
  description = "Dedicated GCP study project id. Use a local ignored terraform.tfvars file for the real value."
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

variable "region" {
  description = "Default GCP region for early free-tier-aligned practice."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.region)
    error_message = "region must be one of the early practice regions: us-central1, us-east1, or us-west1."
  }
}

variable "zone" {
  description = "Default GCP zone for resources that later need zonal placement."
  type        = string
  default     = "us-central1-a"

  validation {
    condition     = can(regex("^us-(central1|east1|west1)-[a-z]$", var.zone))
    error_message = "zone must be in us-central1, us-east1, or us-west1."
  }
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
