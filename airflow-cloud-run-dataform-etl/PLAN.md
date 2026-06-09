# Airflow + GCS + BigQuery Transform Load - Current Plan

## Architecture

```text
Mongo source
  -> Airflow extracts one logical interval
  -> Airflow uploads deterministic NDJSON to raw GCS
  -> Airflow runs Python-packaged BigQuery SQL
  -> BigQuery upserts dl_bloomberg_data dimensions and facts
```

Confidence: 94/100.

## Terraform Boundary

Terraform owns:

- Raw and temp GCS buckets.
- The `dl_bloomberg_data` BigQuery dataset.
- Durable target tables:
  - `dim_grains`
  - `dim_metrics`
  - `fact_values`
- Airflow orchestration identity.
- Airflow GCS upload identity and bucket IAM.
- Airflow BigQuery transform identity and scoped BigQuery/GCS IAM.

Terraform does not own executable transform SQL. The Airflow repository's Python
package owns the BigQuery scripts that create the external table and run MERGE
statements. Confidence: 95/100.

## IAM Boundary

- `sa-airflow-orchestrator`: base Airflow Google Cloud connection identity.
- `sa-airflow-gcs-uploader`: task identity for raw GCS upload.
- `sa-airflow-bq-transformer`: task identity for BigQuery transform-load jobs.

The orchestrator receives `roles/iam.serviceAccountTokenCreator` on both task
identities. The BigQuery transformer receives:

- `roles/bigquery.jobUser` at project scope.
- `roles/bigquery.dataEditor` on the Bloomberg dataset.
- `roles/storage.objectViewer` on the raw bucket.

Confidence: 96/100.

## Runtime Boundary

Airflow Variables should come from Terraform outputs:

- `gcs_bucket_name`
- `gcs_raw_prefix`
- `gcs_impersonation_chain`
- `bigquery_project_id`
- `bigquery_dataset_id`
- `bigquery_region`
- `bigquery_impersonation_chain`

The DAG derives `raw_gcs_uri` from `gcs_bucket_name` and `gcs_raw_prefix`.

The raw URI is a wildcard under `raw_object_prefix`, defaulting to
`gs://<raw-bucket>/bloomberg/raw/*`. The DAG still logs the exact uploaded raw
object for traceability, but BigQuery scans the configured prefix and relies on
idempotent merge keys. Confidence: 93/100.

## Verification

Run from this directory:

```sh
terraform fmt
terraform validate
terraform plan
```

Review planned destroys carefully. After this migration, expected destroys are
limited to previously managed transform-orchestration resources that are no
longer part of the active architecture. Confidence: 90/100.
