terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 8.0"
    }
  }
}

provider "google" {
  # The provider uses Application Default Credentials from gcloud unless a more
  # specific supported credential source is configured outside this repository.
  project = var.project_id
  region  = var.region
  zone    = var.zone
}

data "google_project" "selected" {
  project_id = var.project_id
}

data "google_client_config" "current" {}

locals {
  common_labels = merge(
    {
      study_workspace = "terraform_basics"
      study_track     = "gcp_provider_practice"
      study_session   = "02_provider_basics_read_only"
      environment     = var.environment
      owner           = var.owner
    },
    var.extra_labels
  )

  safety_contract = {
    billing_budget_confirmed = var.billing_budget_confirmed
    managed_resource_count   = 0
  }
}
