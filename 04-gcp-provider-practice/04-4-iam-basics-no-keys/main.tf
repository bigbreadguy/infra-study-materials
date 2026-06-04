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

locals {
  service_account_email = google_service_account.practice.email

  safety_contract = {
    billing_budget_confirmed = var.billing_budget_confirmed
    iam_api_enabled          = var.iam_api_enabled
    key_files_created        = false
  }
}

resource "google_service_account" "practice" {
  project      = var.project_id
  account_id   = var.service_account_id
  display_name = var.service_account_display_name
  description  = "Study service account managed by Terraform without key files."
  disabled     = false
}

resource "google_project_iam_member" "practice" {
  for_each = var.project_roles

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${local.service_account_email}"
}
