terraform {
  required_version = ">= 1.6.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = ">= 5.0, < 8.0"
    }

    google-beta = {
      source  = "hashicorp/google-beta"
      version = ">= 5.0, < 8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

resource "google_project_service" "required" {
  for_each = local.required_project_services

  project = var.project_id
  service = each.value

  # This is a study project. Destroying this lesson should not disable shared
  # project APIs that another lesson or manual cleanup flow might still need.
  disable_on_destroy = false
}

resource "google_project_service_identity" "dataform" {
  provider = google-beta

  project = var.project_id
  service = "dataform.googleapis.com"

  depends_on = [
    google_project_service.required["dataform.googleapis.com"],
  ]
}
