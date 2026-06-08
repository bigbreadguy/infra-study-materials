locals {
  required_project_services = toset([
    "serviceusage.googleapis.com",
    "cloudresourcemanager.googleapis.com",
    "iam.googleapis.com",
    "storage.googleapis.com",
    "run.googleapis.com",
    "bigquery.googleapis.com",
    "dataform.googleapis.com",
    "artifactregistry.googleapis.com",
    "secretmanager.googleapis.com",
  ])

  common_labels = merge(
    {
      study_workspace = "terraform_basics"
      study_track     = "gcp_provider_practice"
      study_session   = "06_airflow_cloud_run_dataform_etl"
      environment     = var.environment
      owner           = var.owner
    },
    var.extra_labels
  )

  raw_bucket_name = var.raw_bucket_name != "" ? var.raw_bucket_name : "${var.project_id}-${var.environment}-etl-raw"

  artifact_repository_id = var.artifact_repository_id != "" ? var.artifact_repository_id : "${var.environment}-etl-workers"

  scraper_job_name = var.scraper_job_name != "" ? var.scraper_job_name : "${var.environment}-scrape-actions"
  loader_job_name  = var.loader_job_name != "" ? var.loader_job_name : "${var.environment}-load-raw-to-bigquery"

  dataform_repository_id      = var.dataform_repository_id != "" ? var.dataform_repository_id : "${var.environment}-web-etl"
  dataform_release_config_id  = var.dataform_release_config_id != "" ? var.dataform_release_config_id : "${var.environment}_release"
  dataform_workflow_config_id = var.dataform_workflow_config_id != "" ? var.dataform_workflow_config_id : "${var.environment}_workflow"

  scraper_credentials_secret_id = var.scraper_credentials_secret_id != "" ? var.scraper_credentials_secret_id : "${var.environment}-scraper-credentials"

  service_accounts = {
    airflow_orchestrator = {
      account_id   = var.airflow_orchestrator_service_account_id
      display_name = "Airflow ETL orchestrator"
      description  = "Local Airflow impersonates this account to run Cloud Run jobs and Dataform workflows."
    }

    scraper_worker = {
      account_id   = var.scraper_worker_service_account_id
      display_name = "Cloud Run scraper worker"
      description  = "Runtime identity for the Cloud Run scraper action job."
    }

    loader_worker = {
      account_id   = var.loader_worker_service_account_id
      display_name = "Cloud Run loader worker"
      description  = "Optional runtime identity for a Cloud Run raw-to-BigQuery loader job."
    }

    dataform_runner = {
      account_id   = var.dataform_runner_service_account_id
      display_name = "Dataform workflow runner"
      description  = "Custom service account used by Dataform workflow invocations."
    }
  }

  service_account_emails = {
    for key, account in google_service_account.etl :
    key => account.email
  }

  airflow_member         = "serviceAccount:${google_service_account.etl["airflow_orchestrator"].email}"
  scraper_worker_member  = "serviceAccount:${google_service_account.etl["scraper_worker"].email}"
  loader_worker_member   = "serviceAccount:${google_service_account.etl["loader_worker"].email}"
  dataform_runner_member = "serviceAccount:${google_service_account.etl["dataform_runner"].email}"

  terraform_act_as_targets = {
    scraper_worker  = google_service_account.etl["scraper_worker"].name
    loader_worker   = google_service_account.etl["loader_worker"].name
    dataform_runner = google_service_account.etl["dataform_runner"].name
  }

  terraform_deployer_act_as_grants = {
    for pair in setproduct(var.terraform_deployer_principals, keys(local.terraform_act_as_targets)) :
    "${pair[1]}:${pair[0]}" => {
      principal            = pair[0]
      service_account_name = local.terraform_act_as_targets[pair[1]]
    }
  }

  project_iam_members = merge(
    {
      airflow_bigquery_job_user = {
        role   = "roles/bigquery.jobUser"
        member = local.airflow_member
      }

      scraper_bigquery_job_user = {
        role   = "roles/bigquery.jobUser"
        member = local.scraper_worker_member
      }

      dataform_bigquery_job_user = {
        role   = "roles/bigquery.jobUser"
        member = local.dataform_runner_member
      }
    },
    var.enable_loader_job ? {
      loader_bigquery_job_user = {
        role   = "roles/bigquery.jobUser"
        member = local.loader_worker_member
      }
    } : {}
  )

  bucket_iam_members = merge(
    {
      scraper_raw_object_creator = {
        role   = "roles/storage.objectCreator"
        member = local.scraper_worker_member
      }

      airflow_raw_object_viewer = {
        role   = "roles/storage.objectViewer"
        member = local.airflow_member
      }
    },
    var.enable_loader_job ? {
      loader_raw_object_viewer = {
        role   = "roles/storage.objectViewer"
        member = local.loader_worker_member
      }
    } : {}
  )

  dataset_iam_members = merge(
    {
      airflow_raw_data_editor = {
        dataset_id = google_bigquery_dataset.raw.dataset_id
        role       = "roles/bigquery.dataEditor"
        member     = local.airflow_member
      }

      airflow_ops_data_editor = {
        dataset_id = google_bigquery_dataset.ops.dataset_id
        role       = "roles/bigquery.dataEditor"
        member     = local.airflow_member
      }

      dataform_raw_data_viewer = {
        dataset_id = google_bigquery_dataset.raw.dataset_id
        role       = "roles/bigquery.dataViewer"
        member     = local.dataform_runner_member
      }

      dataform_staging_data_editor = {
        dataset_id = google_bigquery_dataset.staging.dataset_id
        role       = "roles/bigquery.dataEditor"
        member     = local.dataform_runner_member
      }

      dataform_mart_data_editor = {
        dataset_id = google_bigquery_dataset.mart.dataset_id
        role       = "roles/bigquery.dataEditor"
        member     = local.dataform_runner_member
      }
    },
    var.enable_loader_job ? {
      loader_raw_data_editor = {
        dataset_id = google_bigquery_dataset.raw.dataset_id
        role       = "roles/bigquery.dataEditor"
        member     = local.loader_worker_member
      }

      loader_staging_data_editor = {
        dataset_id = google_bigquery_dataset.staging.dataset_id
        role       = "roles/bigquery.dataEditor"
        member     = local.loader_worker_member
      }
    } : {}
  )

  safety_contract = {
    billing_budget_confirmed = var.billing_budget_confirmed
    adc_credentials_reviewed = var.adc_credentials_reviewed
    remote_state_reviewed    = var.remote_state_reviewed
    terraform_executes_jobs  = false
    secret_versions_created  = false
    loader_job_enabled       = var.enable_loader_job
  }
}
