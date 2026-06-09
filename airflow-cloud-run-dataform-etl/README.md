# Airflow ETL On GCP - GCS + BigQuery Transform Load

## Goal

Provision the GCP data surface for an Airflow-orchestrated transform-load
workflow:

```text
Airflow uploads raw Mongo NDJSON to GCS
  -> Airflow creates or replaces a BigQuery external table
  -> Airflow runs BigQuery MERGE scripts for dl_bloomberg_data tables
```

This directory keeps the lesson deliberately small: GCS buckets, BigQuery's
stable dataset/table contract, and the minimum service accounts/IAM for that
flow. Cloud Run, Artifact Registry, Secret Manager payloads, and broader
production environment structure remain deferred.

Confidence: 94/100.

## What This Builds

- Required project APIs (`bigquery`, `iam`, `serviceusage`, `storage`) via one
  `google_project_service` per API with `disable_on_destroy = false`.
- A `for_each` map of GCS buckets (default: `raw` + `temp`), each with uniform
  bucket-level access, public access prevention, lifecycle rules, optional
  versioning, and opt-in `force_destroy`.
- An Airflow orchestration service account (`sa-airflow-orchestrator` by
  default) used as the Airflow Google Cloud connection identity.
- A narrow Airflow GCS upload service account (`sa-airflow-gcs-uploader` by
  default), which the orchestrator can impersonate for raw uploads.
- A custom bucket-level CRU role for the GCS upload identity, with delete added
  when object replacement is enabled.
- A BigQuery dataset, `dl_bloomberg_data` by default, with three durable
  tables:
  - `dl_bloomberg_data.dim_grains`
  - `dl_bloomberg_data.dim_metrics`
  - `dl_bloomberg_data.fact_values`
- A BigQuery transform-load service account (`sa-airflow-bq-transformer` by
  default), which the orchestrator can impersonate for query jobs.
- Scoped transform-load permissions:
  - `roles/bigquery.jobUser` at project scope.
  - `roles/bigquery.dataEditor` on the Bloomberg dataset.
  - `roles/storage.objectViewer` on the raw bucket.
- An optional service account JSON key resource that is disabled by default.

Confidence: 94/100.

## Files

- `providers.tf`: GA `google` provider and required APIs.
- `variables.tf`: typed inputs, safety gates, BigQuery knobs, transform-load
  identity settings, and the `buckets` map.
- `locals.tf`: labels, derived bucket names, table schemas, raw object prefix,
  and the safety contract.
- `main.tf`: `google_storage_bucket` via `for_each = local.buckets`.
- `bigquery.tf`: `dl_bloomberg_data` dataset plus the three durable tables.
- `bigquery_transform.tf`: BigQuery job, dataset, and raw bucket IAM for the
  transform-load service account.
- `iam.tf`: Airflow service accounts, impersonation grants, the custom GCS CRU
  role, and optional key.
- `outputs.tf`: bucket, dataset/table, Airflow identity, transform-load, and
  safety outputs.
- `terraform.tfvars.example`: placeholder values only.
- `backend.tf.example`: GCS remote backend template. Copy it to the ignored
  `backend.tf` only when enabling remote state.

Confidence: 95/100.

## Commands

Run from this directory after copying `terraform.tfvars.example` to an ignored
local `terraform.tfvars` and replacing placeholders:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
```

The plan refuses to proceed until these safety gates are true:

- `billing_budget_confirmed`: true only after a budget alert exists.
- `adc_credentials_reviewed`: true only after confirming ADC points at the
  intended project or impersonation chain.
- `remote_state_reviewed`: true only after deciding whether this lesson uses a
  GCS remote backend.

Never run `terraform apply` or `terraform destroy` against real GCP resources
without an explicit review of the plan. Confidence: 95/100.

## Airflow Variables

Use the outputs to configure the `mongo-data-ingestion` DAG:

- `gcp_conn_id`: usually `google_cloud_default`.
- `gcs_bucket_name`: `airflow_gcs_bucket_access.bucket_name`.
- `gcs_raw_prefix`: `bigquery_transform_load.raw_object_prefix`.
- `gcs_impersonation_chain`: `airflow_impersonation_chains.gcs_impersonation_chain`.
- `bigquery_project_id`: `bigquery_transform_load.bigquery_project_id`.
- `bigquery_dataset_id`: `bigquery_transform_load.bigquery_dataset_id`.
- `bigquery_region`: `bigquery_transform_load.bigquery_region`.
- `bigquery_impersonation_chain`:
  `airflow_impersonation_chains.bigquery_impersonation_chain`.

The DAG derives `raw_gcs_uri` from `gcs_bucket_name` and `gcs_raw_prefix`; it is
not a separate Airflow Variable.

The DAG writes deterministic raw object keys under this prefix shape:

```text
bloomberg/raw/grain_id=<grain_id>/YYYY/MM/DD/HH/raw-*.ndjson
```

The default `raw_object_prefix = "bloomberg/raw"` should match the DAG's
`gcs_raw_prefix`. Confidence: 94/100.

## IAM Shape

The Airflow Google Cloud connection identity should be
`sa-airflow-orchestrator`. The DAG then uses task-level impersonation chains:

```text
gcs_impersonation_chain:
sa-airflow-gcs-uploader@<project>.iam.gserviceaccount.com

bigquery_impersonation_chain:
sa-airflow-bq-transformer@<project>.iam.gserviceaccount.com
```

Terraform grants the orchestrator `roles/iam.serviceAccountTokenCreator` on
both task identities. The orchestrator intentionally does not receive direct
BigQuery writer access. Confidence: 96/100.

## BigQuery Contract

Terraform owns the durable tables. The Python package in the Airflow repo owns
the executable SQL that creates the `raw_data_samples` external table
and merges:

- `dim_grains`
- `dim_metrics`
- `fact_values`

The `mongo` schema contract matches Mongo Extended JSON extraction from raw
NDJSON. The `legacy` contract exists only for staged deletion-protection
migration of older tables. Confidence: 93/100.

## Key Handling

Prefer ADC and service account impersonation. If a JSON key is unavoidable for
a local Airflow UI test, set `create_airflow_service_account_key = true` only in
ignored local variables. Terraform state will contain the private key even when
CLI output is marked sensitive, so rotate/delete the key after the test.

Confidence: 98/100.
