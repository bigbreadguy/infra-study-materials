variable "project_id" {
  description = "Dedicated GCP study project id."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.project_id))
    error_message = "project_id must be 6 to 30 characters, start with a lower-case letter, and use only lower-case letters, numbers, or hyphens."
  }
}

variable "billing_budget_confirmed" {
  description = "Set true only after a budget alert exists for the dedicated study project."
  type        = bool

  validation {
    condition     = var.billing_budget_confirmed
    error_message = "billing_budget_confirmed must be true before planning this ETL lesson."
  }
}

variable "adc_credentials_reviewed" {
  description = "Set true after confirming Application Default Credentials point at the intended study project or impersonation chain."
  type        = bool

  validation {
    condition     = var.adc_credentials_reviewed
    error_message = "adc_credentials_reviewed must be true before planning this ETL lesson."
  }
}

variable "remote_state_reviewed" {
  description = "Set true after deciding whether this advanced lesson should use the GCS remote backend from lesson 04-5."
  type        = bool

  validation {
    condition     = var.remote_state_reviewed
    error_message = "remote_state_reviewed must be true before planning this ETL lesson."
  }
}

variable "region" {
  description = "Default region for Cloud Run Jobs, Artifact Registry, Dataform, and provider operations."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1"], var.region)
    error_message = "region must be one of: us-central1, us-east1, or us-west1."
  }
}

variable "bq_location" {
  description = "BigQuery dataset location. Keep this aligned with Dataform and raw storage for the first pass."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1", "US"], var.bq_location)
    error_message = "bq_location must be one of: us-central1, us-east1, us-west1, or US."
  }
}

variable "raw_bucket_location" {
  description = "Cloud Storage location for immutable raw landing objects."
  type        = string
  default     = "us-central1"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1", "US"], var.raw_bucket_location)
    error_message = "raw_bucket_location must be one of: us-central1, us-east1, us-west1, or US."
  }
}

variable "environment" {
  description = "Single environment name for this first ETL lesson."
  type        = string
  default     = "dev"

  validation {
    condition     = can(regex("^[a-z][a-z0-9]{0,15}$", var.environment))
    error_message = "environment must start with a lower-case letter and then use only lower-case letters or numbers."
  }
}

variable "owner" {
  description = "Lower-case label value identifying the learner."
  type        = string
  default     = "kenny"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_-]{0,62}$", var.owner))
    error_message = "owner must be a GCP-label-safe value up to 63 characters."
  }
}

variable "extra_labels" {
  description = "Additional GCP labels to merge into the shared label contract."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for key, value in var.extra_labels :
      can(regex("^[a-z][a-z0-9_-]{0,62}$", key))
      && can(regex("^[a-z0-9_-]{1,63}$", value))
    ])
    error_message = "extra_labels must use GCP-label-safe keys and non-empty values."
  }
}

variable "raw_bucket_name" {
  description = "Optional explicit raw bucket name. Leave empty to use <project_id>-<environment>-etl-raw."
  type        = string
  default     = ""

  validation {
    condition     = var.raw_bucket_name == "" || can(regex("^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$", var.raw_bucket_name))
    error_message = "raw_bucket_name must be empty or a simple lower-case Cloud Storage bucket name."
  }
}

variable "raw_object_retention_days" {
  description = "Number of days before old raw objects are deleted by the bucket lifecycle rule."
  type        = number
  default     = 30

  validation {
    condition     = var.raw_object_retention_days >= 1 && var.raw_object_retention_days <= 365
    error_message = "raw_object_retention_days must be between 1 and 365."
  }
}

variable "raw_bucket_versioning_enabled" {
  description = "Whether to enable object versioning on the raw landing bucket."
  type        = bool
  default     = true
}

variable "force_destroy_raw_bucket" {
  description = "Whether Terraform may delete the raw bucket when it contains objects. Keep false unless intentionally cleaning up."
  type        = bool
  default     = false
}

variable "delete_bigquery_contents_on_destroy" {
  description = "Whether Terraform may delete BigQuery datasets that contain tables. Keep false unless intentionally cleaning up."
  type        = bool
  default     = false
}

variable "bigquery_table_deletion_protection" {
  description = "Whether stable BigQuery tables should block Terraform deletion."
  type        = bool
  default     = false
}

variable "airflow_orchestrator_service_account_id" {
  description = "Service account id for local Airflow impersonation."
  type        = string
  default     = "sa-airflow-orchestrator"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,28}[a-z0-9]$", var.airflow_orchestrator_service_account_id))
    error_message = "airflow_orchestrator_service_account_id must be 3 to 30 lower-case letters, numbers, or hyphens."
  }
}

variable "scraper_worker_service_account_id" {
  description = "Service account id for the Cloud Run scraper job runtime identity."
  type        = string
  default     = "sa-cr-scraper-worker"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,28}[a-z0-9]$", var.scraper_worker_service_account_id))
    error_message = "scraper_worker_service_account_id must be 3 to 30 lower-case letters, numbers, or hyphens."
  }
}

variable "loader_worker_service_account_id" {
  description = "Service account id for the optional Cloud Run loader job runtime identity."
  type        = string
  default     = "sa-cr-loader-worker"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,28}[a-z0-9]$", var.loader_worker_service_account_id))
    error_message = "loader_worker_service_account_id must be 3 to 30 lower-case letters, numbers, or hyphens."
  }
}

variable "dataform_runner_service_account_id" {
  description = "Service account id for Dataform workflow execution."
  type        = string
  default     = "sa-dataform-runner"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,28}[a-z0-9]$", var.dataform_runner_service_account_id))
    error_message = "dataform_runner_service_account_id must be 3 to 30 lower-case letters, numbers, or hyphens."
  }
}

variable "airflow_impersonators" {
  description = "IAM principals allowed to impersonate the Airflow orchestrator service account, for example user:name@example.com or group:name@example.com."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for principal in var.airflow_impersonators :
      can(regex("^(user|group|serviceAccount):[^[:space:]]+$", principal))
    ])
    error_message = "airflow_impersonators must be IAM principals such as user:name@example.com, group:name@example.com, or serviceAccount:name@project.iam.gserviceaccount.com."
  }
}

variable "terraform_deployer_principals" {
  description = "IAM principals that may attach worker service accounts to Cloud Run Jobs or Dataform resources during Terraform apply."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for principal in var.terraform_deployer_principals :
      can(regex("^(user|group|serviceAccount):[^[:space:]]+$", principal))
    ])
    error_message = "terraform_deployer_principals must be IAM principals such as user:name@example.com, group:name@example.com, or serviceAccount:name@project.iam.gserviceaccount.com."
  }
}

variable "artifact_repository_id" {
  description = "Artifact Registry Docker repository id. Leave empty to use <environment>-etl-workers."
  type        = string
  default     = ""

  validation {
    condition     = var.artifact_repository_id == "" || can(regex("^[a-z][a-z0-9-]{1,62}$", var.artifact_repository_id))
    error_message = "artifact_repository_id must be empty or a lower-case Artifact Registry repository id."
  }
}

variable "artifact_repository_immutable_tags" {
  description = "Whether Artifact Registry should reject tag mutation for the worker image repository."
  type        = bool
  default     = true
}

variable "scraper_image" {
  description = "Already-pushed scraper container image URI or digest. Terraform creates the repository but does not build or push images."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9.-]+-docker\\.pkg\\.dev/.+/.+/.+(:.+|@sha256:[a-f0-9]{64})$", var.scraper_image))
    error_message = "scraper_image must be an Artifact Registry image URI with a tag or sha256 digest."
  }
}

variable "scraper_job_name" {
  description = "Optional Cloud Run scraper job name. Leave empty to use <environment>-scrape-actions."
  type        = string
  default     = ""

  validation {
    condition     = var.scraper_job_name == "" || can(regex("^[a-z][a-z0-9-]{1,62}[a-z0-9]$", var.scraper_job_name))
    error_message = "scraper_job_name must be empty or a lower-case Cloud Run Job name."
  }
}

variable "scraper_command" {
  description = "Stable scraper container command. Run-specific values belong in Airflow overrides."
  type        = list(string)
  default     = ["python"]
}

variable "scraper_args" {
  description = "Stable scraper container args. Airflow overrides these for a real run."
  type        = list(string)
  default     = ["-m", "scraper_job"]
}

variable "scraper_task_count" {
  description = "Default Cloud Run task count. Keep 1 until deterministic sharding is proven."
  type        = number
  default     = 1

  validation {
    condition     = var.scraper_task_count >= 1 && var.scraper_task_count <= 100
    error_message = "scraper_task_count must be between 1 and 100."
  }
}

variable "scraper_parallelism" {
  description = "Default Cloud Run parallelism. Keep 1 until politeness and sharding are proven."
  type        = number
  default     = 1

  validation {
    condition     = var.scraper_parallelism >= 1 && var.scraper_parallelism <= 100
    error_message = "scraper_parallelism must be between 1 and 100."
  }
}

variable "scraper_task_timeout" {
  description = "Default Cloud Run task timeout duration."
  type        = string
  default     = "1800s"

  validation {
    condition     = can(regex("^[1-9][0-9]*s$", var.scraper_task_timeout))
    error_message = "scraper_task_timeout must be a duration string like 1800s."
  }
}

variable "scraper_max_retries" {
  description = "Cloud Run task retries. Coordinate this with Airflow retries."
  type        = number
  default     = 1

  validation {
    condition     = var.scraper_max_retries >= 0 && var.scraper_max_retries <= 3
    error_message = "scraper_max_retries must be between 0 and 3."
  }
}

variable "scraper_cpu" {
  description = "Cloud Run scraper container CPU limit."
  type        = string
  default     = "1"

  validation {
    condition     = contains(["1", "2", "4"], var.scraper_cpu)
    error_message = "scraper_cpu must be one of: 1, 2, or 4."
  }
}

variable "scraper_memory" {
  description = "Cloud Run scraper container memory limit."
  type        = string
  default     = "2Gi"

  validation {
    condition     = contains(["1Gi", "2Gi", "4Gi", "8Gi"], var.scraper_memory)
    error_message = "scraper_memory must be one of: 1Gi, 2Gi, 4Gi, or 8Gi."
  }
}

variable "scrape_schema_version" {
  description = "Default schema version advertised to the scraper worker."
  type        = string
  default     = "v1"

  validation {
    condition     = can(regex("^v[0-9]+$", var.scrape_schema_version))
    error_message = "scrape_schema_version must look like v1."
  }
}

variable "default_action_plan_uri" {
  description = "Optional non-secret default action plan GCS URI. Airflow should usually override it per run."
  type        = string
  default     = ""

  validation {
    condition     = var.default_action_plan_uri == "" || can(regex("^gs://[a-z0-9][a-z0-9.-]*/.+$", var.default_action_plan_uri))
    error_message = "default_action_plan_uri must be empty or a gs:// URI."
  }
}

variable "scraper_output_format" {
  description = "Default raw output format expected from the scraper."
  type        = string
  default     = "ndjson.gz"

  validation {
    condition     = contains(["ndjson", "ndjson.gz", "json"], var.scraper_output_format)
    error_message = "scraper_output_format must be ndjson, ndjson.gz, or json."
  }
}

variable "enable_scraper_credentials_secret" {
  description = "Whether to create Secret Manager metadata and IAM for scraper credentials. Secret versions are created outside Terraform."
  type        = bool
  default     = false
}

variable "attach_scraper_credentials_secret_to_job" {
  description = "Whether to attach the scraper credentials secret to the Cloud Run Job. Enable only after a secret version exists."
  type        = bool
  default     = false
}

variable "scraper_credentials_secret_id" {
  description = "Optional Secret Manager secret id for scraper credentials metadata. No secret versions are created by Terraform."
  type        = string
  default     = ""

  validation {
    condition     = var.scraper_credentials_secret_id == "" || can(regex("^[a-z][a-z0-9-]{1,62}$", var.scraper_credentials_secret_id))
    error_message = "scraper_credentials_secret_id must be empty or a lower-case Secret Manager secret id."
  }
}

variable "scraper_credentials_secret_version" {
  description = "Secret Manager version reference exposed to the scraper job when attach_scraper_credentials_secret_to_job is enabled."
  type        = string
  default     = "latest"

  validation {
    condition     = var.scraper_credentials_secret_version == "latest" || can(regex("^[1-9][0-9]*$", var.scraper_credentials_secret_version))
    error_message = "scraper_credentials_secret_version must be latest or a positive integer version."
  }
}

variable "enable_loader_job" {
  description = "Whether to create the optional Cloud Run loader job. The default path uses Airflow BigQuery load operators instead."
  type        = bool
  default     = false
}

variable "loader_image" {
  description = "Already-pushed loader container image URI or digest. Used only when enable_loader_job is true."
  type        = string
  default     = ""

  validation {
    condition     = var.loader_image == "" || can(regex("^[a-z0-9.-]+-docker\\.pkg\\.dev/.+/.+/.+(:.+|@sha256:[a-f0-9]{64})$", var.loader_image))
    error_message = "loader_image must be empty or an Artifact Registry image URI with a tag or sha256 digest."
  }
}

variable "loader_job_name" {
  description = "Optional Cloud Run loader job name. Leave empty to use <environment>-load-raw-to-bigquery."
  type        = string
  default     = ""

  validation {
    condition     = var.loader_job_name == "" || can(regex("^[a-z][a-z0-9-]{1,62}[a-z0-9]$", var.loader_job_name))
    error_message = "loader_job_name must be empty or a lower-case Cloud Run Job name."
  }
}

variable "loader_task_timeout" {
  description = "Default Cloud Run task timeout duration for the optional loader job."
  type        = string
  default     = "1800s"

  validation {
    condition     = can(regex("^[1-9][0-9]*s$", var.loader_task_timeout))
    error_message = "loader_task_timeout must be a duration string like 1800s."
  }
}

variable "loader_max_retries" {
  description = "Cloud Run task retries for the optional loader job."
  type        = number
  default     = 1

  validation {
    condition     = var.loader_max_retries >= 0 && var.loader_max_retries <= 3
    error_message = "loader_max_retries must be between 0 and 3."
  }
}

variable "loader_cpu" {
  description = "Cloud Run loader container CPU limit."
  type        = string
  default     = "1"

  validation {
    condition     = contains(["1", "2", "4"], var.loader_cpu)
    error_message = "loader_cpu must be one of: 1, 2, or 4."
  }
}

variable "loader_memory" {
  description = "Cloud Run loader container memory limit."
  type        = string
  default     = "1Gi"

  validation {
    condition     = contains(["1Gi", "2Gi", "4Gi", "8Gi"], var.loader_memory)
    error_message = "loader_memory must be one of: 1Gi, 2Gi, 4Gi, or 8Gi."
  }
}

variable "dataform_repository_id" {
  description = "Optional Dataform repository id. Leave empty to use <environment>-web-etl."
  type        = string
  default     = ""

  validation {
    condition     = var.dataform_repository_id == "" || can(regex("^[a-z][a-z0-9-]{1,62}$", var.dataform_repository_id))
    error_message = "dataform_repository_id must be empty or a lower-case Dataform repository id."
  }
}

variable "dataform_release_config_id" {
  description = "Optional Dataform release config id. Leave empty to use <environment>-release."
  type        = string
  default     = ""

  validation {
    condition     = var.dataform_release_config_id == "" || can(regex("^[a-z][a-z0-9_]{1,62}$", var.dataform_release_config_id))
    error_message = "dataform_release_config_id must be empty or a lower-case release config id."
  }
}

variable "dataform_workflow_config_id" {
  description = "Optional Dataform workflow config id. Leave empty to use <environment>-workflow."
  type        = string
  default     = ""

  validation {
    condition     = var.dataform_workflow_config_id == "" || can(regex("^[a-z][a-z0-9_]{1,62}$", var.dataform_workflow_config_id))
    error_message = "dataform_workflow_config_id must be empty or a lower-case workflow config id."
  }
}

variable "dataform_git_commitish" {
  description = "Git branch, tag, or commit that Dataform release configuration compiles."
  type        = string
  default     = "main"

  validation {
    condition     = can(regex("^[A-Za-z0-9._/-]+$", var.dataform_git_commitish))
    error_message = "dataform_git_commitish must be a branch, tag, or commit-like value."
  }
}

variable "dataform_included_tags" {
  description = "Dataform action tags included by the workflow config. Airflow may still invoke narrower runs at runtime."
  type        = set(string)
  default     = ["etl"]

  validation {
    condition = alltrue([
      for tag in var.dataform_included_tags :
      can(regex("^[A-Za-z0-9_-]+$", tag))
    ])
    error_message = "dataform_included_tags must contain simple tag names."
  }
}

variable "dataform_fully_refresh_incremental_tables" {
  description = "Whether the default workflow config fully refreshes incremental tables."
  type        = bool
  default     = false
}

variable "airflow_dataform_repository_role" {
  description = "Repository-level Dataform role granted to the Airflow orchestrator. Replace with a custom role after permissions are proven."
  type        = string
  default     = "roles/dataform.editor"

  validation {
    condition     = can(regex("^roles/dataform\\.[A-Za-z]+$", var.airflow_dataform_repository_role)) || can(regex("^projects/.+/roles/.+$", var.airflow_dataform_repository_role))
    error_message = "airflow_dataform_repository_role must be a Dataform predefined role or a project custom role name."
  }
}
