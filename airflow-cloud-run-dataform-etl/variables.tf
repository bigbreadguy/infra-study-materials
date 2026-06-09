###############################################################################
# Project + safety gates
###############################################################################

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
    error_message = "billing_budget_confirmed must be true before planning this lesson."
  }
}

variable "adc_credentials_reviewed" {
  description = "Set true after confirming Application Default Credentials point at the intended study project or impersonation chain."
  type        = bool

  validation {
    condition     = var.adc_credentials_reviewed
    error_message = "adc_credentials_reviewed must be true before planning this lesson."
  }
}

variable "remote_state_reviewed" {
  description = "Set true after deciding whether this lesson should use a GCS remote backend with state locking."
  type        = bool

  validation {
    condition     = var.remote_state_reviewed
    error_message = "remote_state_reviewed must be true before planning this lesson."
  }
}

###############################################################################
# Location + labels
###############################################################################

variable "region" {
  description = "Default region for provider operations."
  type        = string
  default     = "asia-northeast3"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1", "asia-northeast3"], var.region)
    error_message = "region must be one of: us-central1, us-east1, us-west1, or asia-northeast3 (Seoul)."
  }
}

variable "location" {
  description = "Cloud Storage and BigQuery location for the ETL lesson."
  type        = string
  default     = "asia-northeast3"

  validation {
    condition     = contains(["us-central1", "us-east1", "us-west1", "US", "asia-northeast3"], var.location)
    error_message = "location must be one of: us-central1, us-east1, us-west1, US, or asia-northeast3 (Seoul)."
  }
}

variable "environment" {
  description = "Single environment name for this lesson."
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

###############################################################################
# Buckets
###############################################################################

# Map-driven bucket definition. Each key is a logical bucket role; the value
# configures that bucket. The optional() attributes give safe defaults, so a
# new bucket only has to declare its name_suffix. The final bucket name is
# "<project_id>-<environment>-<name_suffix>".
variable "buckets" {
  description = "Logical GCS buckets to create for the ETL pipeline, keyed by role."
  type = map(object({
    name_suffix        = string
    versioning_enabled = optional(bool, false)
    retention_days     = optional(number, 30)
    force_destroy      = optional(bool, false)
  }))

  default = {
    # System of record: immutable raw landing for scraper output. Versioned
    # and retained longest because everything downstream is re-derived from it.
    raw = {
      name_suffix        = "etl-raw"
      versioning_enabled = true
      retention_days     = 30
    }

    # Disposable scratch: BigQuery load staging and future scratch work. No
    # versioning, short retention so forgotten artifacts self-clean.
    temp = {
      name_suffix        = "etl-temp"
      versioning_enabled = false
      retention_days     = 7
    }
  }

  validation {
    condition = alltrue([
      for key, cfg in var.buckets :
      can(regex("^[a-z][a-z0-9_]{0,30}$", key))
      && can(regex("^[a-z0-9][a-z0-9-]{1,30}$", cfg.name_suffix))
    ])
    error_message = "buckets keys must be label-safe and each name_suffix must be a lower-case bucket-name fragment."
  }

  validation {
    condition = alltrue([
      for key, cfg in var.buckets :
      cfg.retention_days >= 1 && cfg.retention_days <= 365
    ])
    error_message = "each bucket retention_days must be between 1 and 365."
  }
}

###############################################################################
# Airflow GCS upload service account
###############################################################################

variable "airflow_service_account_id" {
  description = "Service account id for Airflow's GCS upload impersonation-chain identity."
  type        = string
  default     = "sa-airflow-gcs-uploader"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.airflow_service_account_id))
    error_message = "airflow_service_account_id must be 6 to 30 characters, start with a lower-case letter, end with a lower-case letter or number, and use only lower-case letters, numbers, or hyphens."
  }
}

variable "airflow_service_account_display_name" {
  description = "Human-readable display name for the Airflow GCS upload service account."
  type        = string
  default     = "Airflow GCS uploader"

  validation {
    condition     = length(var.airflow_service_account_display_name) >= 1 && length(var.airflow_service_account_display_name) <= 100
    error_message = "airflow_service_account_display_name must be between 1 and 100 characters."
  }
}

variable "airflow_gcs_target_bucket_key" {
  description = "Logical key from var.buckets that Airflow can upload to. Defaults to raw because GCSHook uploads raw Mongo extracts."
  type        = string
  default     = "raw"

  validation {
    condition     = can(regex("^[a-z][a-z0-9_]{0,30}$", var.airflow_gcs_target_bucket_key))
    error_message = "airflow_gcs_target_bucket_key must use the same lower-case logical key format as var.buckets."
  }
}

variable "airflow_gcs_bucket_role" {
  description = "Optional bucket-level IAM role override for Airflow's GCS upload service account. Null uses the custom CRU role, which grants create/read/update and optional delete for replacement uploads."
  type        = string
  default     = null

  validation {
    condition = (
      var.airflow_gcs_bucket_role == null
      || contains([
        "roles/storage.objectCreator",
        "roles/storage.objectViewer",
        "roles/storage.objectUser",
        "roles/storage.objectAdmin",
      ], coalesce(var.airflow_gcs_bucket_role, ""))
      || can(regex("^projects/[a-z][a-z0-9-]{4,28}[a-z0-9]/roles/[A-Za-z][A-Za-z0-9_]{2,63}$", coalesce(var.airflow_gcs_bucket_role, "")))
    )
    error_message = "airflow_gcs_bucket_role must be null, a supported Storage object role, or a project custom role name like projects/<project_id>/roles/<role_id>."
  }
}

variable "airflow_gcs_custom_cru_role_id" {
  description = "Project custom role id used when airflow_gcs_bucket_role is null. The role grants object create/read/update and can optionally add delete for replacement uploads."
  type        = string
  default     = "airflowGcsObjectCru"

  validation {
    condition     = can(regex("^[A-Za-z][A-Za-z0-9_]{2,63}$", var.airflow_gcs_custom_cru_role_id))
    error_message = "airflow_gcs_custom_cru_role_id must be 3 to 64 characters, start with a letter, and use only letters, numbers, or underscores."
  }
}

variable "airflow_gcs_allow_object_replacement" {
  description = "Set false only when Airflow never replaces an object at an existing GCS key. When true, the custom GCS role includes storage.objects.delete because GCS requires delete permission for replacement uploads."
  type        = bool
  default     = true
}

variable "airflow_service_account_impersonators" {
  description = "Additional principals allowed to impersonate the Airflow GCS upload service account directly. The orchestrator service account is granted this by default."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for member in var.airflow_service_account_impersonators :
      !contains(["allUsers", "allAuthenticatedUsers"], member)
    ])
    error_message = "airflow_service_account_impersonators must not include public principals."
  }
}

variable "create_airflow_service_account_key" {
  description = "Set true only if local Airflow cannot use ADC or service account impersonation. Creates a key for the Airflow orchestrator service account; Terraform state will contain the private key."
  type        = bool
  default     = false
}

###############################################################################
# Airflow orchestration service account
###############################################################################

variable "airflow_orchestrator_service_account_id" {
  description = "Service account id for the Airflow GCP connection identity. It can impersonate the task-specific GCS uploader and BigQuery transformer accounts."
  type        = string
  default     = "sa-airflow-orchestrator"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.airflow_orchestrator_service_account_id))
    error_message = "airflow_orchestrator_service_account_id must be 6 to 30 characters, start with a lower-case letter, end with a lower-case letter or number, and use only lower-case letters, numbers, or hyphens."
  }
}

variable "airflow_orchestrator_service_account_display_name" {
  description = "Human-readable display name for Airflow's runtime orchestration service account."
  type        = string
  default     = "Airflow BigQuery orchestrator"

  validation {
    condition     = length(var.airflow_orchestrator_service_account_display_name) >= 1 && length(var.airflow_orchestrator_service_account_display_name) <= 100
    error_message = "airflow_orchestrator_service_account_display_name must be between 1 and 100 characters."
  }
}

variable "airflow_orchestrator_impersonators" {
  description = "Principals allowed to impersonate the Airflow GCP connection service account for keyless local Airflow. Example: user:you@example.com."
  type        = set(string)
  default     = []

  validation {
    condition = alltrue([
      for member in var.airflow_orchestrator_impersonators :
      !contains(["allUsers", "allAuthenticatedUsers"], member)
    ])
    error_message = "airflow_orchestrator_impersonators must not include public principals."
  }
}

###############################################################################
# BigQuery
###############################################################################

variable "bigquery_dataset_id" {
  description = "BigQuery dataset id for the Bloomberg sample mart."
  type        = string
  default     = "dl_bloomberg_data"

  validation {
    condition     = length(var.bigquery_dataset_id) >= 1 && length(var.bigquery_dataset_id) <= 1024 && can(regex("^[A-Za-z0-9_]+$", var.bigquery_dataset_id))
    error_message = "bigquery_dataset_id must contain only letters, numbers, or underscores."
  }
}

variable "bigquery_dataset_delete_contents_on_destroy" {
  description = "Set true only for intentional cleanup when the dataset contains tables or views."
  type        = bool
  default     = false
}

variable "bigquery_dataset_deletion_policy" {
  description = "Deletion policy for the BigQuery dataset. PREVENT is safest for stateful learning data."
  type        = string
  default     = "PREVENT"

  validation {
    condition     = contains(["DELETE", "PREVENT", "ABANDON"], var.bigquery_dataset_deletion_policy)
    error_message = "bigquery_dataset_deletion_policy must be one of: DELETE, PREVENT, or ABANDON."
  }
}

variable "bigquery_table_deletion_protection" {
  description = "When true, Terraform refuses to delete the stable BigQuery tables until this is intentionally disabled."
  type        = bool
  default     = true
}

variable "bigquery_table_schema_contract" {
  description = "BigQuery table schema contract to manage. Use mongo for the Python/Airflow transform-load target schema. Use legacy only as a temporary migration step to disable deletion protection on already-created legacy tables before replacing them."
  type        = string
  default     = "mongo"

  validation {
    condition     = contains(["mongo", "legacy"], var.bigquery_table_schema_contract)
    error_message = "bigquery_table_schema_contract must be one of: mongo or legacy."
  }
}

###############################################################################
# Airflow BigQuery transform-load
###############################################################################

variable "bigquery_transform_service_account_id" {
  description = "Service account id used by Airflow BigQuery transform-load tasks."
  type        = string
  default     = "sa-airflow-bq-transformer"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{4,28}[a-z0-9]$", var.bigquery_transform_service_account_id))
    error_message = "bigquery_transform_service_account_id must be 6 to 30 characters, start with a lower-case letter, end with a lower-case letter or number, and use only lower-case letters, numbers, or hyphens."
  }
}

variable "bigquery_transform_service_account_display_name" {
  description = "Human-readable display name for the Airflow BigQuery transform-load service account."
  type        = string
  default     = "Airflow BigQuery transformer"

  validation {
    condition     = length(var.bigquery_transform_service_account_display_name) >= 1 && length(var.bigquery_transform_service_account_display_name) <= 100
    error_message = "bigquery_transform_service_account_display_name must be between 1 and 100 characters."
  }
}

variable "raw_object_prefix" {
  description = "GCS object prefix, inside the raw bucket, that the Python/Airflow BigQuery external table scans for uploaded Mongo NDJSON."
  type        = string
  default     = "bloomberg/raw"

  validation {
    condition = (
      length(trim(var.raw_object_prefix, "/")) > 0
      && can(regex("^[A-Za-z0-9_./=-]+$", trim(var.raw_object_prefix, "/")))
      && !can(regex("//", trim(var.raw_object_prefix, "/")))
    )
    error_message = "raw_object_prefix must be a non-empty GCS prefix using only letters, numbers, underscore, dot, slash, equals, or hyphen, without double slashes."
  }
}
