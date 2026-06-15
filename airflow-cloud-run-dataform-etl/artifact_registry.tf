# Artifact Registry Docker repository that hosts the scraper image. The Cloud Run
# Job pulls from here; building/pushing the image is a separate manual step (see
# the apply-ordering note in cloud_run_scraper.tf). Region is pinned to var.region
# so the image and the job sit in the same region (faster cold-start pulls).
resource "google_artifact_registry_repository" "scraper" {
  project       = var.project_id
  location      = var.region
  repository_id = var.scraper_artifact_repository_id
  description   = "Docker images for the dfml-scraper Cloud Run Job."
  format        = "DOCKER"

  labels = merge(local.common_labels, {
    component = "scraper"
  })

  depends_on = [
    google_project_service.required["artifactregistry.googleapis.com"],
  ]
}
