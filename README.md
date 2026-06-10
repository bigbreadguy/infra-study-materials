# Infra Study Materials

This repository contains a local Airflow cluster configuration for infrastructure study material.

## Airflow DAGs

Project DAGs live in `dags/` and are mounted into Airflow at `/opt/airflow/dags`.
Shared DAG helper modules live in `dags/common/` so Airflow can import them from
the same mounted DAG tree. After changing files under `dags/`, restart the
scheduler, DAG processor, and workers; a Docker rebuild is only needed when
runtime image dependencies change.

Airflow bundled example DAGs are disabled for cluster initialization:

- `docker-compose.yaml` sets `AIRFLOW__CORE__LOAD_EXAMPLES` to `false`.
- `config/airflow.cfg` sets `load_examples` to `False`.

With a fresh metadata database, Airflow should load only DAGs from the repository `dags/` directory.
If the cluster was already initialized before this setting changed, stale example DAG metadata may remain until the Airflow metadata database is cleaned or recreated.

## Local Private DAG Inputs

This public repository can run DAGs that import local-only code without committing that code or its paths.

- Keep private DAGs under ignored paths such as `dags/private/` or `dags/local/`.
- Keep local source mounts in an ignored Compose override file.
- Mount only the importable source tree read-only, and set `PYTHONPATH` to the mounted parent directory.
- Keep credentials, target URLs, selectors, and private response payloads in Airflow Connections, Airflow Variables, a secrets backend, or ignored env files.

The Airflow image is built from `Dockerfile` so runtime dependencies are repeatable. Rebuild it after changing `requirements/airflow-runtime.txt`.
Leave `_PIP_ADDITIONAL_REQUIREMENTS` unset or empty for normal runs; use it only for temporary experiments.

## Worker Node Scraper DAG

The `scraper-worker-node` DAG runs the local scraper package on an Airflow Celery worker. Store the action plan as an Airflow Variable named `scraper_worker_action_plan`, or trigger the DAG with `{"action_plan_variable": "scraper_worker_action_plan"}` to use another generic variable key.
The local Airflow image installs only the Chromium Playwright browser, so set `browser_name` to `chromium`.

The action plan may reference Airflow's `logical_date` with placeholders. Use `{{ year }}` and `{{ month }}` for the same year/month values used by the local scraper script.

Example placeholder shape:

```json
{
  "entrypoint_url": "https://example.invalid",
  "browser_name": "chromium",
  "headless": true,
  "sleep_time": 0.5,
  "actions": [
    {"select_option": {"selector": "title=example-year", "value": "{{ year }}"}},
    {"select_month_option": {"selector": "title=example-month", "month": "{{ month }}"}},
    {"check_month_box": {"start_year": "{{ year }}", "month": "{{ month }}"}},
    {"return_stats": {}}
  ]
}
```

Do not commit real target URLs, selectors, headers, cookies, credentials, response payloads, or private action plans. Use Airflow Variables, Airflow Connections, a secrets backend, or ignored local files for those values.

## Bloomberg Raw Data Ingestion

The `mongo-data-ingestion` DAG extracts Mongo/Bloomberg sample rows, uploads raw
NDJSON to GCS, and then runs reusable BigQuery scripts from `dags/common` that
preserve the former Dataform SQL semantics. Airflow creates or replaces the
`raw_data_samples` external table, then merges `dim_grains`, `dim_metrics`, and
`fact_values` in order.

Configure these Airflow Variables:

- `mongo_conn_id`: Mongo Airflow Connection id. Required.
- `mongo_database_name`: Mongo database name. Required.
- `mongo_collection_name`: Mongo collection name. Required.
- `mongo_grain_id`: Mongo `grainId` to extract. Required.
- `gcp_conn_id`: Google Cloud Airflow Connection id. Required.
- `gcs_bucket_name`: raw GCS bucket. Required.
- `gcs_raw_prefix`: raw object prefix, for example `bloomberg/raw`. Required.
- `gcs_impersonation_chain`: service account to impersonate for raw GCS upload.
  Required.
- `bigquery_project_id`: BigQuery project id for transform-load jobs. Required.
- `bigquery_dataset_id`: BigQuery dataset id, for example `dl_bloomberg_data`.
  Required.
- `bigquery_region`: BigQuery job location. Required.
- `bigquery_impersonation_chain`: service account to impersonate for BigQuery
  transform-load jobs. Required.

The raw object path is deterministic:

```text
<gcs_raw_prefix>/grain_id=<grain_id>/YYYY/MM/DD/HH/raw-YYYYMMDDTHHMMSS+0000.ndjson
```

For example:

```text
bloomberg/raw/grain_id=SX5E_Index/1996/04/16/00/raw-19960416T000000+0000.ndjson
```

Reruns upload to the same deterministic object key and may replace the existing
object. The impersonated GCS uploader service account therefore needs object
replacement permission on the raw bucket. If an upload fails with a missing
`storage.objects.delete` permission, check that Terraform has completed the
staged IAM apply that restores that permission.

The current DAG runs the BigQuery transform-load steps in parallel per grain
id and recreates one raw external table per grain after each raw upload.

The `fact_values` script manages duplicate-row conflicts on its own:

- The merge source keeps only the freshest row per merge key, so the same
  Mongo document appearing in multiple raw objects (for example stale
  `raw-...-001.ndjson` files left behind by an earlier unique-name upload
  scheme) can no longer fail the MERGE with
  `UPDATE/MERGE must match at most one source row for each target row`.
- Before merging, the script deletes previously duplicated `fact_values` rows
  inside the candidate date window, keeping the latest ingested row, so the
  warehouse self-heals from duplicates inserted by earlier runs.

If impersonation fails with `iam.serviceAccounts.getAccessToken`, the Airflow
runtime ADC principal cannot impersonate the configured service account. Grant
that principal `roles/iam.serviceAccountTokenCreator` on the GCS uploader service
account for upload tasks, and on the BigQuery transformer service account for
transform-load tasks.

Do not commit service account keys, Mongo credentials, or plaintext secrets. Use
ADC, attached service accounts, impersonation, Secret Manager, Airflow
Connections, Airflow Variables, or environment-provided credentials.

Terraform owns the `dl_bloomberg_data` BigQuery dataset, durable target tables,
GCS bucket access, and Airflow impersonation identities. The reusable modules in
`dags/common` own the executable BigQuery transform-load SQL.
