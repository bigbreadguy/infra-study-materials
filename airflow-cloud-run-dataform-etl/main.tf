# One google_storage_bucket per entry in local.buckets. for_each keeps each
# bucket addressable by its logical role (raw, temp, ...) so adding a bucket is
# a one-line change to var.buckets, not a new resource block.
resource "google_storage_bucket" "etl" {
  for_each = local.buckets

  name     = each.value.name
  project  = var.project_id
  location = var.location

  storage_class = "STANDARD"

  # Route all access through IAM (no legacy ACLs) and make it impossible for any
  # future binding to expose the bucket publicly. These two stay on forever.
  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Opt-in only. Keep false so Terraform never deletes a non-empty bucket by
  # accident; flip per bucket in var.buckets for an intentional cleanup.
  force_destroy = each.value.force_destroy

  labels = merge(local.common_labels, {
    bucket_role = each.key
  })

  versioning {
    enabled = each.value.versioning_enabled
  }

  lifecycle_rule {
    action {
      type = "Delete"
    }

    condition {
      age = each.value.retention_days
    }
  }

  depends_on = [
    google_project_service.required["storage.googleapis.com"],
  ]
}
