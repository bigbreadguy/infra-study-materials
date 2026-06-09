# Infra Study Materials

This repository contains a local Airflow cluster configuration for infrastructure study material.

## Airflow DAGs

Project DAGs live in `dags/` and are mounted into Airflow at `/opt/airflow/dags`.

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
NDJSON to GCS, and then triggers the configured Dataform workflow. Airflow does
not write to BigQuery and does not own SQL upsert logic for `dl_securities`.
Dataform normalizes the raw records into `dim_grains`, `dim_metrics`, and
`fact_market_indexes`.

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
- `dataform_project_id`: Dataform project id. Required.
- `dataform_region`: Dataform repository region. Required.
- `dataform_repository_id`: Dataform repository id. Required.
- `dataform_workflow_config`: fully-qualified Dataform workflow config name.
  Required.
- `dataform_impersonation_chain`: service account to impersonate for Dataform
  workflow orchestration. Required.
- `dataform_wait_time_seconds`: polling interval for Dataform completion.
  Optional; defaults to `10`.
- `dataform_timeout_seconds`: Dataform wait timeout. Optional; blank means use
  provider default behavior.

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

The current DAG triggers a Dataform workflow config after the raw upload. It
does not pass the uploaded object path as a per-run Dataform variable, so the
Dataform SQLX actions should resolve raw files from the configured bucket and
prefix. If Dataform must process only the exact object uploaded by a single DAG
run, change the orchestration design to create a per-run Dataform compilation
result with compilation vars such as `raw_uri`, then invoke that compilation
result instead of invoking the workflow config directly.

If impersonation fails with `iam.serviceAccounts.getAccessToken`, the Airflow
runtime ADC principal cannot impersonate the configured service account. Grant
that principal `roles/iam.serviceAccountTokenCreator` on the GCS uploader service
account for upload tasks, and on the Dataform orchestration service account for
Dataform trigger tasks.

Do not commit service account keys, Mongo credentials, Dataform
`workflow_settings.yaml` contents, or plaintext secrets. Use ADC, attached
service accounts, impersonation, Secret Manager, Airflow Connections, Airflow
Variables, or environment-provided credentials.

Terraform/Dataform owns the `dl_securities` BigQuery schema and SQLX MERGE
logic. The current schema migration is staged: first disable deletion protection
on legacy tables, then replace them with the Mongo/Dataform contract, then
re-enable deletion protection. Until that finishes, Airflow can upload raw data,
but the Dataform upsert may fail or target the wrong schema.
