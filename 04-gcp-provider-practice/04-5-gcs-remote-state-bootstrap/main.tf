terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 8.0"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

locals {
  backend_prefix = "gcp-provider-practice/05-gcs-remote-state-bootstrap"

  common_labels = merge(
    {
      study_workspace = "terraform_basics"
      study_track     = "gcp_provider_practice"
      study_session   = "05_gcs_remote_state_bootstrap"
      environment     = var.environment
      owner           = var.owner
    },
    var.extra_labels
  )
}

resource "random_id" "state_bucket_suffix" {
  byte_length = 4

  keepers = {
    project_id          = var.project_id
    state_bucket_prefix = var.state_bucket_prefix
  }
}

resource "google_storage_bucket" "terraform_state" {
  name     = "${var.state_bucket_prefix}-${random_id.state_bucket_suffix.hex}"
  project  = var.project_id
  location = var.bucket_location

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy_state_bucket
  labels                      = local.common_labels

  versioning {
    enabled = true
  }
}
