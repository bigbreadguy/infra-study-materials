# Shared Artifact Registry Docker repository (deepfl-infra convention:
# <env>-dfml-docker). The scraper image lives here alongside other dfml images.
# The Cloud Run Job pulls from here; building/pushing the image is a separate
# manual step (see the apply-ordering note in cloud_run_scraper.tf). Region is
# pinned to var.region so the image and the job sit in the same region.
resource "google_artifact_registry_repository" "docker" {
  project       = var.project_id
  location      = var.region
  repository_id = var.scraper_artifact_repository_id
  description   = "Docker images for dfml Cloud Run workloads (scraper, ...)."
  format        = "DOCKER"

  labels = local.common_labels

  depends_on = [
    google_project_service.required["artifactregistry.googleapis.com"],
  ]
}
