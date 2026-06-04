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
  common_labels = merge(
    {
      study_workspace = "terraform_basics"
      study_track     = "gcp_provider_practice"
      study_session   = "03_first_managed_resource"
      environment     = var.environment
      owner           = var.owner
    },
    var.extra_labels
  )
}

resource "random_id" "bucket_suffix" {
  byte_length = 4

  keepers = {
    project_id         = var.project_id
    bucket_name_prefix = var.bucket_name_prefix
  }
}

resource "google_storage_bucket" "lesson" {
  name     = "${var.bucket_name_prefix}-${random_id.bucket_suffix.hex}"
  project  = var.project_id
  location = var.bucket_location

  storage_class               = "STANDARD"
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"
  force_destroy               = var.force_destroy
  labels                      = local.common_labels
}
