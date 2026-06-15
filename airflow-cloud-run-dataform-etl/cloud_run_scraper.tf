# Generic, recipe-agnostic scraper Cloud Run Job (Phase 4) plus its identity and
# least-privilege IAM. One job serves every target; targets are data (recipes +
# secrets), so this file stays constant as targets grow. Resource names follow
# the deepfl-infra conventions (<domain>-sa, <env>-dfml-docker, dfml-<env>-raw).
#
# Apply ordering: the job references an image that must already exist in Artifact
# Registry. On a clean project:
#   1. terraform apply -target=google_artifact_registry_repository.docker
#   2. build & push the image (from the scraper repo):
#        gcloud builds submit --tag \
#          <region>-docker.pkg.dev/<project>/dev-dfml-docker/scraper:latest
#   3. terraform apply   (creates the job + IAM)

locals {
  scraper_image_uri = join("", [
    "${var.region}-docker.pkg.dev/",
    "${var.project_id}/",
    "${var.scraper_artifact_repository_id}/",
    "${var.scraper_image_name}:${var.scraper_image_tag}",
  ])

  # deepfl-infra raw data lake naming (dfml-<env>-raw), project-prefixed for global
  # GCS uniqueness since the bare dfml-dev-raw name is already taken by the company
  # project. Deepfl itself uses the ${project_id}-* prefix for the same reason.
  scrape_bucket_name = coalesce(var.scraper_raw_bucket_name, "${var.project_id}-dfml-${var.environment}-raw")
}

# Project number is needed for the Secret Manager IAM Condition: the resource name
# evaluated by IAM uses the project NUMBER, not the id (a common gotcha).
data "google_project" "this" {
  project_id = var.project_id
}

###############################################################################
# Scrape bucket: dfml-<env>-raw (shared raw data lake, deepfl-infra convention)
#
# The scraper reads requests from scrape/requests/ and writes results to
# scrape/results/ under this bucket. No delete-lifecycle: this is the raw data
# lake, so auto-deletion would be destructive (matches deepfl-infra raw_data).
###############################################################################

resource "google_storage_bucket" "scrape_raw" {
  project       = var.project_id
  name          = local.scrape_bucket_name
  location      = var.location
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  labels = merge(local.common_labels, {
    bucket_role = "scrape"
  })
}

###############################################################################
# Scraper job identity (scraper-sa)
###############################################################################

resource "google_service_account" "scraper_sa" {
  project      = var.project_id
  account_id   = var.scraper_service_account_id
  display_name = "Scraper domain service account (Cloud Run Job)"
  description  = "Runtime identity for the generic dfml scraper Cloud Run Job."
  disabled     = false

  depends_on = [
    google_project_service.required["iam.googleapis.com"],
  ]
}

# Read requests + write results on the scrape bucket only.
resource "google_storage_bucket_iam_member" "scraper_job_bucket" {
  bucket = google_storage_bucket.scrape_raw.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.scraper_sa.email}"
}

# Accessor scoped by an IAM Condition to the scrape- secret name prefix, so new
# target secrets are covered without an IAM change per target. GCP has no native
# prefix/label IAM for Secret Manager; this condition is the mechanism. The
# resource.name in the condition is the secret VERSION path and uses the project
# NUMBER (hence data.google_project.this.number). Validate after apply with:
#   gcloud secrets versions access latest --secret=scrape-kosa-user \
#     --impersonate-service-account=<scraper_sa_email>
resource "google_project_iam_member" "scraper_job_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.scraper_sa.email}"

  condition {
    title       = "scrape-prefixed secrets only"
    description = "Restricts access to secrets whose id starts with the scrape- prefix."
    expression  = "resource.name.startsWith(\"projects/${data.google_project.this.number}/secrets/${var.scraper_secret_name_prefix}\")"
  }
}

# Write structured logs to Cloud Logging.
resource "google_project_iam_member" "scraper_job_log_writer" {
  project = var.project_id
  role    = "roles/logging.logWriter"
  member  = "serviceAccount:${google_service_account.scraper_sa.email}"
}

# Pull its own image from Artifact Registry.
resource "google_artifact_registry_repository_iam_member" "scraper_job_reader" {
  project    = var.project_id
  location   = google_artifact_registry_repository.docker.location
  repository = google_artifact_registry_repository.docker.repository_id
  role       = "roles/artifactregistry.reader"
  member     = "serviceAccount:${google_service_account.scraper_sa.email}"
}

###############################################################################
# The job
###############################################################################

resource "google_cloud_run_v2_job" "scraper" {
  name     = var.scraper_job_name
  project  = var.project_id
  location = var.region

  # Study sandbox: allow terraform destroy. Set true in real use.
  deletion_protection = false

  labels = merge(local.common_labels, {
    component = "scraper"
  })

  # Per deepfl-infra convention: ignore env drift. REQUEST_URI / OUTPUT_URI are
  # supplied per-execution by Airflow's CloudRunExecuteJobOperator overrides; the
  # job spec keeps only the static GCP_PROJECT.
  lifecycle {
    ignore_changes = [
      template[0].template[0].containers[0].env,
    ]
  }

  template {
    template {
      service_account = google_service_account.scraper_sa.email
      max_retries     = var.scraper_job_max_retries
      timeout         = "${var.scraper_job_timeout_seconds}s"

      containers {
        image = local.scraper_image_uri

        resources {
          limits = {
            cpu    = var.scraper_job_cpu
            memory = var.scraper_job_memory
          }
        }

        # GCP_PROJECT lets the job build Secret Manager names at runtime.
        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }
      }
    }
  }

  depends_on = [
    google_project_service.required["run.googleapis.com"],
    google_artifact_registry_repository_iam_member.scraper_job_reader,
  ]
}

###############################################################################
# Airflow caller grants (the orchestrator SA triggers the job + writes requests)
###############################################################################

# Run the job and read its executions. Scoped to this job (not project-wide).
resource "google_cloud_run_v2_job_iam_member" "airflow_run_developer" {
  project  = var.project_id
  location = google_cloud_run_v2_job.scraper.location
  name     = google_cloud_run_v2_job.scraper.name
  role     = "roles/run.developer"
  member   = "serviceAccount:${google_service_account.airflow_orchestrator.email}"
}

# Act as the scraper job SA in order to run the job as that identity.
resource "google_service_account_iam_member" "airflow_acts_as_scraper_job" {
  service_account_id = google_service_account.scraper_sa.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.airflow_orchestrator.email}"
}

# Write the request object and read the result on the scrape bucket.
resource "google_storage_bucket_iam_member" "airflow_scrape_bucket" {
  bucket = google_storage_bucket.scrape_raw.name
  role   = "roles/storage.objectUser"
  member = "serviceAccount:${google_service_account.airflow_orchestrator.email}"
}
