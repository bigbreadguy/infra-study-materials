# Airflow, Cloud Run, GCS, BigQuery, And Dataform ETL Plan

## Purpose

Plan a Terraform-managed GCP environment for a local Airflow cluster that
orchestrates an ETL workflow:

```text
local airflow
  -> cloud run job runs a scraper container against an action plan
  -> gcs stores immutable raw objects
  -> bigquery loads raw landing data
  -> dataform transforms raw/staging data into final datasets
  -> airflow records quality checks and run status
```

This document is a planning artifact only. It does not create resources and it
does not replace a reviewed `terraform plan`.

Confidence: 96/100.

## Design Principles

- Keep Terraform responsible for infrastructure, IAM, and stable resource
  configuration. Airflow should execute jobs and workflow invocations.
  Confidence: 98/100.
- Keep local Airflow as the orchestration control plane. Cloud Run Jobs should
  do bounded worker execution, not scheduling.
  Confidence: 95/100.
- Prefer Cloud Run Jobs over running scraper code inside Airflow workers for
  the scraping workload. The scraper has browser/runtime dependencies and
  target-specific execution behavior that are better isolated in a job
  container.
  Confidence: 94/100.
- Load or expose raw GCS data in BigQuery before running Dataform. Dataform is a
  BigQuery SQL transformation layer, not a webpage scraper or object processor.
  Confidence: 97/100.
- Use separate service accounts for orchestration, Cloud Run workers, and
  Dataform execution. Avoid service account keys.
  Confidence: 96/100.
- Prefer additive IAM member resources in this study repo to avoid overwriting
  unmanaged IAM bindings.
  Confidence: 96/100.
- Keep the first version single-environment (`dev`) and expand to staging/prod
  only after the pattern is understood.
  Confidence: 92/100.

## Execution Option Decision

Choose the Cloud Run Job option for the scraper workload in this lesson.
Terraform should configure the job, IAM, Artifact Registry, Secret Manager
references, and stable runtime defaults. Airflow should trigger executions and
pass run-specific action plan inputs.

| Option | Fit | Risks | Recommendation | Confidence |
| --- | --- | --- | --- | --- |
| Run scraper in Airflow worker | Simple for local-only experiments and avoids one extra GCP resource | Couples Airflow image to browser dependencies, increases worker blast radius, makes scaling/politeness controls harder, and can leak private target details through shared Airflow code/logs | Do not use for this Terraform lesson except as a fallback smoke-test path | 90/100 |
| Run scraper as Cloud Run Job | Isolates scraper runtime, gives per-execution logs, retries, task parallelism, service account scoping, and a clean Airflow orchestration boundary | Requires image build/push outside Terraform and explicit IAM for job execution/overrides | Use this as the primary workload-serving pattern | 94/100 |

Terraform must not execute the job as part of `apply`. Cloud Run execution
belongs to Airflow or a manual `gcloud run jobs execute` smoke test after
reviewing `terraform plan`.

Confidence: 95/100.

## Public Repository Guardrails

This lesson lives in a public repository, so committed files must contain only
generic configuration and examples. Terraform may create secret containers such
as Secret Manager secret metadata, but it must not contain secret values.

- Do not commit real target URLs, selectors, cookies, request headers, API
  tokens, passwords, OAuth values, service account keys, ADC files, or inline
  action plans.
  Confidence: 98/100.
- Do not put secret values in Terraform variables, locals, outputs, example
  tfvars, README text, or Airflow DAG source. Terraform state and plan output
  can preserve those values even when variables are marked sensitive.
  Confidence: 97/100.
- Commit only placeholder names such as `example-source`, `example-target-set`,
  and `gs://example-bucket/action-plan.json`; keep real action plans in ignored
  local files, private storage, or GCS objects created outside Terraform.
  Confidence: 95/100.
- Add Secret Manager secret versions with `gcloud`, CI/CD, or another private
  deployment path after Terraform creates the secret resource and IAM binding.
  Confidence: 94/100.
- Before any commit, scan tracked and untracked files for private paths,
  credential-shaped strings, saved plans, state, tfvars, and key material.
  Confidence: 96/100.

Confidence: 97/100.

## Proposed Repository Shape

For this study workspace, start with a self-contained lesson directory:

```text
04-gcp-provider-practice/
`-- 04-6-airflow-cloud-run-dataform-etl/
    |-- PLAN.md
    |-- README.md
    |-- providers.tf
    |-- variables.tf
    |-- locals.tf
    |-- main.tf
    |-- iam.tf
    |-- cloud_run_jobs.tf
    |-- outputs.tf
    `-- terraform.tfvars.example
```

If the example grows beyond a lesson, split it into modules later:

```text
terraform/
|-- modules/
|   |-- project-services/
|   |-- iam/
|   |-- storage/
|   |-- artifact-registry/
|   |-- cloud-run-job/
|   |-- bigquery/
|   `-- dataform/
`-- envs/
    `-- dev/
        |-- backend.tf
        |-- providers.tf
        |-- main.tf
        |-- variables.tf
        |-- outputs.tf
        `-- terraform.tfvars.example
```

Confidence: 92/100.

## Provider Baseline

Use both Google providers:

| Provider | Usage | Confidence |
| --- | --- | --- |
| `hashicorp/google` | GA resources: IAM, GCS, Artifact Registry, Cloud Run v2 jobs, BigQuery | 95/100 |
| `hashicorp/google-beta` | Dataform repository, release config, workflow config, and repository IAM if required | 98/100 |

Pin provider versions conservatively, for example `>= 5.0, < 8.0`, unless a
specific Dataform field requires a newer version.

Confidence: 94/100.

## Terraform Resources

### Project APIs

Enable one API per `google_project_service` resource and use
`disable_on_destroy = false` in this study project.

Prerequisite: Service Usage must already be usable by the Terraform caller,
usually from the project bootstrap lesson. Terraform can then manage the rest
of the API set, but a project where API management is unavailable needs manual
or earlier bootstrap first.

Confidence: 92/100.

Required APIs:

| API | Why | Confidence |
| --- | --- | --- |
| `serviceusage.googleapis.com` | API management | 90/100 |
| `cloudresourcemanager.googleapis.com` | Project metadata and BigQuery Terraform operations | 98/100 |
| `iam.googleapis.com` | Service accounts and IAM bindings | 98/100 |
| `storage.googleapis.com` | GCS raw landing bucket | 98/100 |
| `run.googleapis.com` | Cloud Run Jobs | 98/100 |
| `bigquery.googleapis.com` | BigQuery datasets, tables, and jobs | 98/100 |
| `dataform.googleapis.com` | Dataform repository and workflow configuration | 98/100 |
| `artifactregistry.googleapis.com` | Container image repository | 92/100 |
| `secretmanager.googleapis.com` | Optional scraper credentials or API tokens | 86/100 |

Avoid `google_project_services` unless you intentionally want an authoritative
list of every enabled API.

Confidence: 95/100.

### Storage

Create one raw landing bucket with `google_storage_bucket`:

- Name pattern: `<project_id>-<env>-etl-raw`
- Location: align with the BigQuery/Dataform location where possible
- Uniform bucket-level access: enabled
- Public access prevention: enforced
- Versioning: optional for learning, recommended for raw-data safety
- Lifecycle: expire old raw objects after a configurable retention window
- IAM: grant worker access with `google_storage_bucket_iam_member`

Use `google_storage_bucket_object` only for seed/config files, not runtime ETL
outputs.

Confidence: 95/100.

### Artifact Registry

Create an Artifact Registry Docker repository with
`google_artifact_registry_repository`.

Terraform should create the repository, but worker image builds and pushes
should stay outside Terraform. The Cloud Run Job resources should reference
already-pushed image tags or digests. Prefer immutable digests for repeatable
runs after the first learning pass.

Confidence: 92/100.

### Cloud Run Jobs

Create a reusable scraper action Cloud Run Job with
`google_cloud_run_v2_job`. Start with one job that can run a series of actions
for one source/target set; do not create one Terraform-managed job per target.

Recommended first jobs:

| Job | Purpose | Runtime identity | Confidence |
| --- | --- | --- | --- |
| `scrape-actions` | Execute an action plan in the scraper container and write raw objects/manifests to GCS | `sa-cr-scraper-worker` | 95/100 |
| `load-raw-to-bigquery` | Optional: read raw objects and load/normalize into BigQuery raw or staging tables | `sa-cr-loader-worker` | 86/100 |

If Airflow uses native BigQuery load operators instead of a loader job, skip the
`load-raw-to-bigquery` Cloud Run Job and grant the Airflow orchestrator the
minimal BigQuery/GCS permissions needed for loading.

Cloud Run execution should be triggered by Airflow, not by Terraform
`start_execution_token` or `run_execution_token`.

Confidence: 96/100.

#### Scraper Job Terraform Shape

The scraper job should have stable infrastructure settings in Terraform and
run-specific values from Airflow overrides. Default to a single task that runs
the action series sequentially; increase `task_count` and `parallelism` only
after the scraper supports deterministic sharding through
`CLOUD_RUN_TASK_INDEX` and `CLOUD_RUN_TASK_COUNT`.

Key Terraform inputs:

| Input | Purpose | Default recommendation | Confidence |
| --- | --- | --- | --- |
| `scraper_image` | Already-pushed container image URI or digest | Required variable; use Artifact Registry | 94/100 |
| `scraper_job_name` | Cloud Run Job name | `<env>-scrape-actions` | 93/100 |
| `scraper_task_count` | Number of Cloud Run tasks per execution | `1` for action-series execution | 92/100 |
| `scraper_parallelism` | Max concurrent tasks | `1` until target politeness and sharding are proven | 92/100 |
| `scraper_task_timeout` | Max task runtime | Start with `1800s`; raise only for known slow targets | 88/100 |
| `scraper_max_retries` | Cloud Run task retries | `1` or `2`; coordinate with Airflow retries | 90/100 |
| `scraper_cpu` | Container CPU limit | `1` for lightweight browser work, `2` if pages are heavy | 86/100 |
| `scraper_memory` | Container memory limit | `2Gi` for browser-based scraping | 88/100 |
| `default_action_plan_uri` | Optional non-secret GCS URI for a default action plan | Empty by default; Airflow should normally override | 84/100 |

Example resource shape:

```hcl
resource "google_cloud_run_v2_job" "scrape_actions" {
  name     = var.scraper_job_name
  location = var.region
  project  = var.project_id

  labels = local.common_labels

  template {
    task_count  = var.scraper_task_count
    parallelism = var.scraper_parallelism

    template {
      service_account = google_service_account.cr_scraper_worker.email
      max_retries     = var.scraper_max_retries
      timeout         = var.scraper_task_timeout

      containers {
        name  = "scraper"
        image = var.scraper_image

        command = ["python"]
        args    = ["-m", "scraper_job"]

        env {
          name  = "GCP_PROJECT"
          value = var.project_id
        }

        env {
          name  = "ENV"
          value = var.env
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
          value = "ndjson.gz"
        }

        env {
          name  = "SCHEMA_VERSION"
          value = var.scrape_schema_version
        }

        env {
          name  = "DEFAULT_ACTION_PLAN_URI"
          value = var.default_action_plan_uri
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
}
```

If a target requires credentials, create the Secret Manager secret metadata and
IAM in Terraform first, add secret versions outside Terraform so secret values
do not enter state, and only then wire the Cloud Run Job to that secret version:

```hcl
env {
  name = "SCRAPER_CREDENTIALS_JSON"

  value_source {
    secret_key_ref {
      secret  = google_secret_manager_secret.scraper_credentials.secret_id
      version = "latest"
    }
  }
}
```

This two-step flow avoids asking Terraform to create a Cloud Run Job that
references a secret version before that version exists.

Confidence: 95/100.

#### Scraper Container Contract

The image should expose a small CLI entrypoint that can run a deterministic
series of actions:

```text
python -m scraper_job
  --run-id <run_id>
  --run-date <yyyy-mm-dd>
  --source-id <source_id>
  --target-set <target_set>
  --action-plan-uri <gs://.../action-plan.json>
  --raw-prefix <raw/source=.../dt=.../run_id=...>
```

The action plan can be a JSON document in GCS or generated by Airflow and passed
as a short override. Prefer a GCS URI over inline JSON for non-trivial action
series because it is easier to audit, retry, and keep out of Terraform state.

The action plan should define:

- action order and action parameters
- source and target identifiers
- output entity name
- schema version
- expected result format
- optional politeness delay or per-target timeout

Do not commit private target URLs, selectors, credentials, or action payloads in
Terraform files, `terraform.tfvars.example`, README text, or Airflow DAG source.

Confidence: 94/100.

### BigQuery

Create datasets with `google_bigquery_dataset`:

| Dataset | Purpose | Terraform ownership | Confidence |
| --- | --- | --- | --- |
| `raw_web_<env>` | Raw loaded scrape records | Dataset and optional raw table | 92/100 |
| `stg_web_<env>` | Dataform staging outputs | Dataset only | 91/100 |
| `mart_web_<env>` | Final modeled outputs | Dataset only | 91/100 |
| `ops_etl_<env>` | Airflow run ledger and quality results | Dataset and optional ledger tables | 90/100 |

Use `google_bigquery_table` for stable raw or ops tables. Let Dataform own
tables and views it creates in staging and mart datasets.

Prefer `google_bigquery_dataset_iam_member` for dataset IAM. Do not mix dataset
IAM resources with `access` blocks or `google_bigquery_dataset_access` on the
same dataset unless the interaction is intentional.

Confidence: 92/100.

### Dataform

Create:

- `google_dataform_repository`
- `google_dataform_repository_release_config`
- `google_dataform_repository_workflow_config`
- `google_dataform_repository_iam_member` as needed

Use `google-beta` if the required Dataform resources or fields are still beta in
the selected provider version.

Do not try to manage Dataform workspaces in Terraform for the first version.
Treat workspaces as developer/runtime state.

Leave Dataform scheduling disabled or unused if local Airflow is the scheduler.
Airflow should create workflow invocations on demand.

Confidence: 90/100.

## Service Account Design

| Service account | Purpose | Confidence |
| --- | --- | --- |
| `sa-airflow-orchestrator` | Local Airflow impersonates this identity to invoke Cloud Run and Dataform | 96/100 |
| `sa-cr-scraper-worker` | Cloud Run runtime identity for webpage scraping | 95/100 |
| `sa-cr-loader-worker` | Cloud Run runtime identity for loading or normalizing raw data | 90/100 |
| `sa-dataform-runner` | Dataform workflow execution identity | 94/100 |
| Dataform service agent | Google-managed identity that can impersonate `sa-dataform-runner` | 95/100 |

Avoid creating service account key files. Use short-lived credentials through
ADC impersonation or Airflow impersonation chains.

Confidence: 97/100.

## IAM Plan

| Principal | Resource | Role | Why | Confidence |
| --- | --- | --- | --- | --- |
| Local user or local developer group | `sa-airflow-orchestrator` | `roles/iam.serviceAccountTokenCreator` | Allow local ADC impersonation | 95/100 |
| Terraform deployer identity | Cloud Run runtime service accounts | `roles/iam.serviceAccountUser` | Allow attaching service accounts to Cloud Run Jobs | 95/100 |
| `sa-airflow-orchestrator` | Each Cloud Run Job | `roles/run.jobsExecutor` | Execute jobs without broad Cloud Run admin rights | 94/100 |
| `sa-airflow-orchestrator` | Scraper Cloud Run Job | `roles/run.jobsExecutorWithOverrides` | Pass per-run action plan args/env/task overrides from Airflow | 92/100 |
| `sa-airflow-orchestrator` | Dataform repository | Custom role with workflow invocation permissions, or fallback `roles/dataform.editor` | Trigger Dataform workflows | 82/100 |
| `sa-cr-scraper-worker` | Raw GCS bucket | `roles/storage.objectCreator` | Append raw scrape outputs | 92/100 |
| `sa-cr-scraper-worker` | Secret Manager secrets | `roles/secretmanager.secretAccessor` | Only if scrapers need credentials | 86/100 |
| `sa-cr-loader-worker` | Raw GCS bucket | `roles/storage.objectViewer` | Read raw objects for loading | 92/100 |
| Cloud Run worker service accounts | Project | `roles/bigquery.jobUser` | Submit BigQuery jobs | 91/100 |
| `sa-cr-loader-worker` | Raw/staging BigQuery datasets | `roles/bigquery.dataEditor` | Write raw or staging tables | 90/100 |
| `sa-dataform-runner` | Project | `roles/bigquery.jobUser` | Execute BigQuery jobs from Dataform | 91/100 |
| `sa-dataform-runner` | Raw/staging datasets | `roles/bigquery.dataViewer` | Read source data | 90/100 |
| `sa-dataform-runner` | Mart/assertion datasets | `roles/bigquery.dataEditor` | Create and update modeled outputs | 88/100 |
| Dataform service agent | `sa-dataform-runner` | `roles/iam.serviceAccountUser` and `roles/iam.serviceAccountTokenCreator` | Let Dataform run as the custom service account | 95/100 |

Avoid:

- `roles/owner` and `roles/editor`
- Project-level `roles/storage.admin`
- Project-level `roles/bigquery.admin`
- Broad `roles/iam.serviceAccountTokenCreator`
- Default Compute Engine service account for Cloud Run Jobs
- `allUsers` and `allAuthenticatedUsers`
- Service account key JSON files

Confidence: 97/100.

## Airflow Orchestration Contract

Airflow should use ADC impersonation:

```sh
gcloud auth application-default login \
  --impersonate-service-account sa-airflow-orchestrator@PROJECT_ID.iam.gserviceaccount.com
```

If Airflow runs in Docker, mount the ADC file read-only into the container user
home directory. Alternatively, keep ADC as the local user and configure Airflow
Google operators with `impersonation_chain`.

Confidence: 90/100.

Recommended DAG shape:

```text
build_run_context
  -> extract_targets_with_cloud_run
  -> validate_raw_landing_manifest
  -> load_raw_to_bigquery
  -> create_dataform_compilation_result
  -> create_dataform_workflow_invocation
  -> wait_for_dataform_workflow
  -> quality_checks
  -> write_run_ledger
```

Use Airflow `CloudRunExecuteJobOperator` for Cloud Run Jobs and Dataform
operators for compilation results and workflow invocations. The scraper task
should execute the Terraform-managed job with runtime overrides for run-specific
metadata and action plan location:

```python
CloudRunExecuteJobOperator(
    task_id="extract_targets_with_cloud_run",
    project_id=project_id,
    region=region,
    job_name=scraper_job_name,
    overrides={
        "container_overrides": [
            {
                "name": "scraper",
                "args": [
                    "--run-id",
                    run_id,
                    "--run-date",
                    run_date,
                    "--source-id",
                    source_id,
                    "--target-set",
                    target_set,
                    "--action-plan-uri",
                    action_plan_uri,
                    "--raw-prefix",
                    raw_prefix,
                ],
            }
        ]
    },
    impersonation_chain=airflow_orchestrator_service_account_email,
)
```

Keep the action plan URI, target set, and raw prefix deterministic so Airflow
retries and manual re-runs address the same intended workload.

Confidence: 94/100.

## Runtime Parameters

Airflow DAG parameters:

- `project_id`
- `region`
- `bq_location`
- `env`
- `raw_bucket`
- `dataform_repository_id`
- `dataform_git_commitish`
- `dataform_tags`
- `source_id`
- `target_set`
- `action_plan_uri`
- `scraper_job_name`
- `scraper_job_task_count`
- `run_date`
- `run_id`
- `force_reload`
- `dry_run`
- `max_targets_per_job`

Cloud Run worker env/args:

- `RUN_ID`
- `RUN_DATE`
- `SOURCE_ID`
- `TARGET_SET`
- `TARGET_ID`
- `TARGET_URL`
- `ACTION_PLAN_URI`
- `ACTION_SET_ID`
- `RAW_BUCKET`
- `RAW_PREFIX`
- `OUTPUT_FORMAT`
- `SCHEMA_VERSION`
- `IDEMPOTENCY_KEY`
- `LOG_LEVEL`
- `AIRFLOW_DAG_ID`
- `AIRFLOW_TASK_ID`
- `AIRFLOW_TRY_NUMBER`
- `CLOUD_RUN_TASK_INDEX`
- `CLOUD_RUN_TASK_COUNT`

Confidence: 94/100.

## Naming And Data Layout

Raw GCS object layout:

```text
gs://<project_id>-<env>-etl-raw/
`-- raw/
    `-- source=<source_id>/
        `-- entity=<entity>/
            `-- dt=<yyyy-mm-dd>/
                `-- run_id=<run_id>/
                    |-- target_id=<target_id>/part-00000.ndjson.gz
                    `-- _manifest.json
```

Raw BigQuery table baseline:

```text
raw_web_<env>.webpage_scrape_raw
```

Recommended raw columns:

- `run_id`
- `run_date`
- `target_id`
- `source_url`
- `fetched_at`
- `http_status`
- `content_hash`
- `object_uri`
- `schema_version`
- `payload`

Confidence: 92/100.

## Idempotency And Failure Handling

- Write raw objects under run-specific prefixes.
  Confidence: 94/100.
- Record the action plan URI and a plan content hash in each manifest so the
  same run can prove which action series produced the output.
  Confidence: 91/100.
- Write data objects first and `_manifest.json` last.
  Confidence: 94/100.
- Treat an existing object with the same checksum as success.
  Confidence: 91/100.
- Treat an existing object with a different checksum as a failure.
  Confidence: 92/100.
- Use deterministic BigQuery load job IDs.
  Confidence: 93/100.
- Prefer batch load jobs over streaming inserts for this workflow.
  Confidence: 93/100.
- Keep Airflow retries and Cloud Run retries coordinated to avoid retry
  multiplication.
  Confidence: 90/100.
- Start with `max_active_runs = 1` per source while learning.
  Confidence: 90/100.
- Use Airflow pools or concurrency limits to respect target website politeness
  and Cloud Run quotas.
  Confidence: 91/100.

## Dependency Order

1. Confirm the dedicated study project, billing guardrails, and ADC.
   Confidence: 96/100.
2. Enable required APIs with `google_project_service`.
   Confidence: 95/100.
3. Create service accounts.
   Confidence: 96/100.
4. Create Artifact Registry and push worker images outside Terraform.
   Confidence: 90/100.
5. Create GCS buckets and BigQuery datasets.
   Confidence: 95/100.
6. Create optional Secret Manager secret metadata for scraper credentials, but
   add secret versions outside Terraform before wiring the Cloud Run Job to the
   secret.
   Confidence: 88/100.
7. Apply resource-level IAM bindings for GCS, BigQuery, Cloud Run Jobs, Dataform,
   Secret Manager, and service account impersonation.
   Confidence: 94/100.
8. Create the `scrape-actions` Cloud Run Job referencing an existing scraper
   image and stable runtime defaults.
   Confidence: 94/100.
9. Create Dataform repository, release config, and workflow config.
   Confidence: 90/100.
10. Output bucket names, dataset IDs, Cloud Run job names, service account emails,
   and Dataform identifiers for Airflow configuration.
   Confidence: 95/100.

## Terraform Outputs For Airflow

The Terraform lesson should output:

- `project_id`
- `region`
- `bq_location`
- `raw_bucket_name`
- `raw_dataset_id`
- `staging_dataset_id`
- `mart_dataset_id`
- `ops_dataset_id`
- `scraper_job_name`
- `scraper_job_location`
- `scraper_job_service_account_email`
- `scraper_image_repository`
- `loader_job_name`
- `airflow_orchestrator_service_account_email`
- `dataform_repository_id`
- `dataform_release_config_id`
- `dataform_workflow_config_id`

Confidence: 95/100.

## Open Decisions Before Implementation

| Decision | Default recommendation | Confidence |
| --- | --- | --- |
| Use a Cloud Run loader job or Airflow BigQuery load operator | Start with Airflow load operator for clarity unless transform/load logic needs custom code | 84/100 |
| Use BigQuery native tables or external tables over GCS for raw data | Start with native raw tables loaded from GCS | 90/100 |
| Use Dataform custom role for Airflow | Prefer custom role after identifying exact Dataform permissions; fallback to repository-level `roles/dataform.editor` for the lesson | 82/100 |
| Use Cloud Run runtime overrides | Use overrides for action plan URI, run metadata, raw prefix, and optional task count; keep image, service account, bucket, and resource limits static in Terraform | 92/100 |
| Store scraper secrets | Add Secret Manager only when a target requires credentials | 86/100 |

## Safety Notes

- Do not run `terraform apply` or `terraform destroy` against real GCP resources
  without explicit confirmation.
- Review `terraform plan` before any apply.
- Keep real values in ignored `terraform.tfvars`, not in committed files.
- Do not commit service account keys, ADC files, saved plans, state, or raw data.
- Do not put private target URLs, selectors, credentials, or inline action plans
  in Terraform variables because they can appear in state and plan output.
- Add cleanup instructions before implementing managed resources in this lesson.

Confidence: 98/100.

## References

- [Cloud Run job execution](https://docs.cloud.google.com/run/docs/execute/jobs)
- [Cloud Run IAM roles](https://docs.cloud.google.com/run/docs/reference/iam/roles)
- [Cloud Run job parallelism](https://cloud.google.com/run/docs/samples/cloudrun-jobs-task-parallelism-create)
- [Cloud Run job secrets](https://docs.cloud.google.com/run/docs/configuring/jobs/secrets)
- [Terraform `google_cloud_run_v2_job`](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/cloud_run_v2_job)
- [Dataform Terraform support](https://docs.cloud.google.com/dataform/docs/terraform)
- [Dataform access control](https://cloud.google.com/dataform/docs/table-access)
- [Dataform workflow invocation API](https://docs.cloud.google.com/dataform/reference/rest/v1/projects.locations.repositories.workflowInvocations/create)
- [Airflow Cloud Run operators](https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/cloud_run.html)
- [Dataform scheduling with Airflow](https://docs.cloud.google.com/dataform/docs/schedule-runs)
- [Terraform BigQuery dataset IAM](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset_iam)

Confidence: 94/100.
