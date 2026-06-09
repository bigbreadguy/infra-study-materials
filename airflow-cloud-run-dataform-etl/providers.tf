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
  project = var.project_id
  region  = var.region
}

# Enable one API per resource so a lesson teardown never disables a shared
# project API that another lesson still needs. Add an API to
# local.required_project_services only when a phase actually uses it.
resource "google_project_service" "required" {
  for_each = local.required_project_services

  project = var.project_id
  service = each.value

  disable_on_destroy = false
}
