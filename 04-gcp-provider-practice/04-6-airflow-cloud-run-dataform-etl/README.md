# Airflow, Cloud Run, BigQuery, And Dataform ETL

## Goal

Implement the advanced ETL design in [PLAN.md](./PLAN.md) as a Terraform-managed
GCP environment. By the end of this lesson, you should be able to explain where
Terraform ownership ends, where Airflow runtime orchestration begins, and what
must be reviewed before any GCP resources are created.

This directory now contains Terraform configuration, but it must still be
treated as review-first infrastructure. Do not apply it until the project,
budget, remote state choice, IAM principals, image references, Dataform
repository state, and cleanup path are reviewed.

Confidence: 94/100.

## Learning Goals

1. Map the end-to-end workflow from local Airflow to Cloud Run Jobs, Cloud
   Storage raw landing objects, BigQuery raw datasets, Dataform transformations,
   and Airflow run ledgers.
   Confidence: 95/100.

2. Separate stable infrastructure from run-specific behavior. Terraform should
   own providers, APIs, buckets, datasets, service accounts, IAM, Artifact
   Registry, Cloud Run Job definitions, Dataform repository configuration, and
   outputs. Airflow should trigger job executions, pass action-plan metadata,
   invoke Dataform workflows, and record run status.
   Confidence: 96/100.

3. Define public-repository guardrails for this lesson. Committed files may use
   placeholders, but they must not contain private target URLs, selectors,
   cookies, request headers, action plans, credentials, tokens, ADC files,
   service account keys, saved plans, state, or secret values.
   Confidence: 98/100.

4. Identify the Terraform resource inventory before writing configuration:
   `google` and `google-beta` providers, required project APIs, raw Cloud
   Storage bucket, Artifact Registry repository, Cloud Run Jobs, BigQuery
   datasets and stable tables, Dataform repository objects, optional Secret
   Manager metadata, and additive IAM member resources.
   Confidence: 94/100.

5. Design least-privilege identities for the orchestration boundary:
   `sa-airflow-orchestrator`, `sa-cr-scraper-worker`, optional
   `sa-cr-loader-worker`, `sa-dataform-runner`, and the Google-managed Dataform
   service agent. Avoid service account key files.
   Confidence: 96/100.

6. Specify the runtime contract for the scraper job: immutable image reference,
   default task count and parallelism, stable environment variables, Airflow
   overrides for run metadata, deterministic raw prefixes, manifest output, and
   coordinated retry behavior.
   Confidence: 94/100.

7. Choose the data and idempotency baseline: raw GCS object layout, native
   BigQuery raw table shape, Dataform-owned staging and mart outputs,
   manifest-last writes, checksum handling, deterministic load job IDs, and
   conservative Airflow concurrency.
   Confidence: 92/100.

8. Use implementation gates before Terraform resources are applied: unresolved
   decisions, expected outputs for Airflow, cleanup requirements, cost
   boundaries, and a reviewed `terraform plan` workflow.
   Confidence: 94/100.

## What This Builds

- Required project APIs with one `google_project_service` resource per API and
  `disable_on_destroy = false`.
- Four service accounts: Airflow orchestrator, Cloud Run scraper worker,
  optional Cloud Run loader worker, and Dataform runner.
- A private raw Cloud Storage bucket with uniform bucket-level access, public
  access prevention, versioning, lifecycle deletion, and worker IAM.
- An Artifact Registry Docker repository for worker images. Terraform creates
  the repository but does not build or push images.
- BigQuery raw, staging, mart, and ops datasets plus stable raw and run-ledger
  tables.
- A Cloud Run `scrape-actions` job with stable runtime settings and Airflow
  override boundaries.
- Optional Secret Manager secret metadata and IAM for scraper credentials.
  Terraform never creates secret versions.
- Optional Cloud Run `load-raw-to-bigquery` job. The default path uses Airflow
  BigQuery load operators instead.
- Dataform repository, release config, workflow config, repository IAM, and
  service-agent impersonation grants.
- Outputs that Airflow can consume for project, region, datasets, bucket, Cloud
  Run job, service account, and Dataform identifiers.

Confidence: 92/100.

## Files

- `providers.tf`: Google providers, required APIs, and Dataform service
  identity.
- `variables.tf`: typed inputs, placeholder-safe defaults, and safety gates.
- `locals.tf`: common labels, derived names, IAM maps, and safety contract.
- `main.tf`: service accounts, GCS, Artifact Registry, BigQuery, optional
  secret metadata, and Dataform resources.
- `iam.tf`: additive IAM member grants for service accounts, bucket, datasets,
  Cloud Run Jobs, Secret Manager, and Dataform.
- `cloud_run_jobs.tf`: scraper job and optional loader job definitions.
- `outputs.tf`: Airflow contract values and safety outputs.
- `terraform.tfvars.example`: placeholder values only.

Confidence: 94/100.

## Non-Goals For This Lesson

- Do not execute Cloud Run Jobs from Terraform.
- Do not create Dataform workflow invocations from Terraform.
- Do not build or push scraper container images.
- Do not create an Airflow DAG or Dataform SQL project here.
- Do not commit real target metadata, action plans, credentials, or secret
  values.

Confidence: 97/100.

## Commands

Run from this directory after replacing placeholders in an ignored local
`terraform.tfvars`:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
```

If Terraform prompts for required variables during `terraform plan`, answer
with values from your dedicated study project:

- `var.project_id`: the dedicated GCP project id, for example
  `example-study-proj`.
- `var.billing_budget_confirmed`: `true` only after a budget alert exists for
  that project.
- `var.adc_credentials_reviewed`: `true` only after confirming ADC points to
  the intended project or impersonation chain.
- `var.remote_state_reviewed`: `true` only after deciding whether this lesson
  uses the GCS backend from lesson `04-5`.
- `var.scraper_image`: a non-secret Artifact Registry image URI with a tag or
  digest, for example
  `us-central1-docker.pkg.dev/example-study-proj/dev-etl-workers/scraper:replace-with-tag`.

Prefer copying `terraform.tfvars.example` to an ignored local
`terraform.tfvars`, replacing placeholders there, and then running
`terraform plan` without interactive prompts.

Confidence: 96/100.

Apply only after reviewing the target project, enabled APIs, IAM grants,
container image references, Dataform repository readiness, cost exposure, and
cleanup path:

```sh
terraform apply
terraform destroy
```

Confidence: 90/100.

## Things To Observe

1. `google_project_service.required` uses one resource per API, avoiding the
   authoritative behavior of `google_project_services`.
   Confidence: 95/100.

2. IAM uses additive `*_iam_member` resources so the lesson does not replace
   unmanaged project, bucket, dataset, job, or repository policies.
   Confidence: 96/100.

3. Cloud Run Jobs have stable defaults in Terraform, while run-specific values
   such as run id, target set, action plan URI, and raw prefix belong in Airflow
   execution overrides.
   Confidence: 95/100.

4. Secret Manager creates metadata only when enabled. Secret values must be
   added outside Terraform to keep values out of state.
   Confidence: 97/100.

5. Dataform uses a custom runner service account, and the Dataform service
   agent receives the required service-account impersonation roles.
   Confidence: 94/100.

6. The loader Cloud Run Job is optional. With `enable_loader_job = false`,
   Airflow receives GCS viewer and BigQuery data editor access for native load
   operators.
   Confidence: 89/100.

## Review Gates Before Apply

1. `billing_budget_confirmed`, `adc_credentials_reviewed`, and
   `remote_state_reviewed` are true for the right project.
   Confidence: 95/100.

2. `scraper_image` points to a real, already-pushed image tag or digest. Prefer
   a digest after the first learning pass.
   Confidence: 93/100.

3. `airflow_impersonators` and `terraform_deployer_principals` contain only the
   principals that need those powers.
   Confidence: 94/100.

4. `default_action_plan_uri` is empty or a non-secret placeholder. Real action
   plans stay in private storage created outside Terraform.
   Confidence: 96/100.

5. If `enable_scraper_credentials_secret = true`, secret versions are added
   with `gcloud`, CI/CD, or another private path after Terraform creates the
   secret metadata.
   Confidence: 97/100.

6. `terraform plan` shows no unexpected project-level broad roles, public IAM
   members, service account keys, secret versions, job executions, or
   destructive dataset/bucket deletion.
   Confidence: 96/100.

## Start Here

Read [PLAN.md](./PLAN.md) for the design, service account boundaries, IAM
shape, public-repository guardrails, Airflow runtime contract, and open
implementation decisions.

Confidence: 95/100.

## Cleanup Notes

- Empty the raw bucket or set `force_destroy_raw_bucket = true` only for an
  intentional cleanup.
- Keep `delete_bigquery_contents_on_destroy = false` unless destroying Dataform
  output tables is intentional.
- Remove secret versions outside Terraform before deleting optional secret
  metadata.
- Disable APIs manually only after confirming no other lesson or resource still
  depends on them.

Confidence: 93/100.

## Safety Notes

- Do not run `terraform apply` or `terraform destroy` until the plan, cleanup
  steps, and cost boundaries are reviewed.
- Keep real target URLs, selectors, cookies, request headers, action plans,
  certificates, API tokens, passwords, and service account keys out of committed
  files.
- Use placeholder examples in committed docs and keep private runtime inputs in
  ignored local files, Secret Manager, or private GCS objects created outside
  Terraform.

Confidence: 98/100.
