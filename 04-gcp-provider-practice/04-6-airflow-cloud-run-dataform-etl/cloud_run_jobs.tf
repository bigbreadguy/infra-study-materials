resource "google_cloud_run_v2_job" "scrape_actions" {
  name     = local.scraper_job_name
  location = var.region
  project  = var.project_id
  labels   = local.common_labels

  template {
    task_count  = var.scraper_task_count
    parallelism = var.scraper_parallelism

    template {
      service_account = google_service_account.etl["scraper_worker"].email
      max_retries     = var.scraper_max_retries
      timeout         = var.scraper_task_timeout

      containers {
        name    = "scraper"
        image   = var.scraper_image
        command = var.scraper_command
        args    = var.scraper_args

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }

        env {
          name  = "ENV"
          value = var.environment
        }

        env {
          name  = "RAW_BUCKET"
          value = google_storage_bucket.raw.name
        }

        env {
          name  = "RAW_PREFIX_BASE"
          value = "raw"
        }

        env {
          name  = "OUTPUT_FORMAT"
          value = var.scraper_output_format
        }

        env {
          name  = "SCHEMA_VERSION"
          value = var.scrape_schema_version
        }

        env {
          name  = "DEFAULT_ACTION_PLAN_URI"
          value = var.default_action_plan_uri
        }

        dynamic "env" {
          for_each = var.enable_scraper_credentials_secret ? [1] : []

          content {
            name = "SCRAPER_CREDENTIALS_JSON"

            value_source {
              secret_key_ref {
                secret  = google_secret_manager_secret.scraper_credentials[0].id
                version = var.scraper_credentials_secret_version
              }
            }
          }
        }

        resources {
          limits = {
            cpu    = var.scraper_cpu
            memory = var.scraper_memory
          }
        }
      }
    }
  }

  depends_on = [
    google_project_service.required["run.googleapis.com"],
    google_service_account_iam_member.terraform_deployer_act_as,
  ]
}

resource "google_cloud_run_v2_job" "load_raw_to_bigquery" {
  count = var.enable_loader_job ? 1 : 0

  name     = local.loader_job_name
  location = var.region
  project  = var.project_id
  labels   = local.common_labels

  template {
    task_count  = 1
    parallelism = 1

    template {
      service_account = google_service_account.etl["loader_worker"].email
      max_retries     = var.loader_max_retries
      timeout         = var.loader_task_timeout

      containers {
        name  = "loader"
        image = var.loader_image

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }

        env {
          name  = "ENV"
          value = var.environment
        }

        env {
          name  = "RAW_BUCKET"
          value = google_storage_bucket.raw.name
        }

        env {
          name  = "RAW_DATASET"
          value = google_bigquery_dataset.raw.dataset_id
        }

        env {
          name  = "STAGING_DATASET"
          value = google_bigquery_dataset.staging.dataset_id
        }

        resources {
          limits = {
            cpu    = var.loader_cpu
            memory = var.loader_memory
          }
        }
      }
    }
  }

  depends_on = [
    google_project_service.required["run.googleapis.com"],
    google_service_account_iam_member.terraform_deployer_act_as,
  ]
}
