# Per-target credential secrets for the scraper. For each target T we create two
# EMPTY secrets: <prefix><T>-user and <prefix><T>-password. The job resolves them
# at runtime by target (PRD section 5.4).
#
# Security: only the secret *containers* are managed by Terraform. The secret
# *values* (versions) are added out of band (gcloud / console) so plaintext
# credentials never live in Terraform code or state. Adding a target is a one-line
# change to var.scraper_targets; the job SA's prefix-scoped accessor (see
# cloud_run_scraper.tf) already covers the new secrets with no IAM change.

locals {
  # Flatten {target} x {user, password} into addressable secret keys, e.g.
  # "kosa-user" => { secret_id = "scrape-kosa-user", target = "kosa" }.
  scraper_secret_suffixes = toset(["user", "password"])

  scraper_secrets = {
    for pair in setproduct(var.scraper_targets, local.scraper_secret_suffixes) :
    "${pair[0]}-${pair[1]}" => {
      target    = pair[0]
      secret_id = "${var.scraper_secret_name_prefix}${pair[0]}-${pair[1]}"
    }
  }
}

resource "google_secret_manager_secret" "scraper" {
  for_each = local.scraper_secrets

  project   = var.project_id
  secret_id = each.value.secret_id

  labels = merge(local.common_labels, {
    component = "scraper"
    target    = each.value.target
  })

  replication {
    auto {}
  }

  depends_on = [
    google_project_service.required["secretmanager.googleapis.com"],
  ]
}
