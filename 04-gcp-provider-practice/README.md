# GCP Provider Practice Track

## Goal

Practice Terraform with the Google Cloud provider after completing the local
workshop through `03-docker-stack`. This track uses a new isolated GCP project,
starts read-only, and introduces small free-tier-aligned resources only after
budget and authentication guardrails are in place.

Confidence: 96/100.

## Session Sequence

1. `04-1-project-safety-bootstrap`
   - Create or select a dedicated study project.
   - Configure billing/free trial awareness, budget alerts, ADC, and default
     region choices.
   - Confidence: 96/100.

2. `04-2-provider-basics-read-only`
   - Configure the `hashicorp/google` provider and inspect project context with
     data sources.
   - No resources are created.
   - Confidence: 95/100.

3. `04-3-first-managed-resource`
   - Create one empty, labelled Cloud Storage bucket in a free-tier-eligible US
     region.
   - Practice plan review, apply, state inspection, and destroy.
   - Confidence: 90/100.

4. `04-4-iam-basics-no-keys`
   - Create a service account and grant narrow IAM roles without key files.
   - Confidence: 89/100.

5. `04-5-gcs-remote-state-bootstrap`
   - Create a dedicated GCS bucket for Terraform state and learn the backend
     migration flow.
   - Confidence: 90/100.

6. `04-6-airflow-cloud-run-dataform-etl`
   - Implement a review-first local Airflow orchestration pattern using Cloud
     Run Jobs, GCS, BigQuery, and Dataform.
   - Do not apply until IAM principals, worker images, secret handling,
     Dataform repository readiness, cost exposure, and cleanup are reviewed.
   - Confidence: 90/100.

## Shared Safety Rules

- Use a new study project dedicated to this repository.
- Confirm budget alerts before any GCP plan or apply.
- Keep real values in local ignored files such as `terraform.tfvars`.
- Commit only `terraform.tfvars.example` placeholders.
- Do not commit `.terraform/`, state files, saved plans, credentials, or ADC
  files.
- Do not commit real ETL target URLs, selectors, cookies, request headers,
  action plans, certificates, or secret values.
- Prefer ADC first; introduce service account impersonation later.

Confidence: 97/100.

## Shared Commands

Run commands from each session directory:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
```

Only run `terraform apply` in sessions that explicitly include a managed
resource and only after reviewing the plan and cleanup instructions.

Confidence: 95/100.
