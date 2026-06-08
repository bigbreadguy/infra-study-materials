# Terraform Basics and GCP Practices Study Workspace

This repository is a Terraform study workspace with two stages. First, learn the
core tool with safe local examples. Then, practice GCP provider basics in a new,
isolated, free-tier-aligned GCP project with explicit cost and credential
guardrails.

Start with [PLAN.md](./PLAN.md). It defines the workshop sequence, local stack
options, GCP practice track, safety rules, expected repository shape, and next
implementation steps, including the later Airflow, Cloud Run, BigQuery, and
Dataform ETL planning extension. The local workshop lessons are
[01-core-hcl-local](./01-core-hcl-local/),
[02-modules-and-composition](./02-modules-and-composition/), and
[03-docker-stack](./03-docker-stack/). The GCP provider practice track starts at
[04-gcp-provider-practice](./04-gcp-provider-practice/).

Confidence: 96/100.

## Study Direction

1. Learn Terraform basics with local-only examples.
2. Practice provider lifecycle behavior with Docker.
3. Treat `03-docker-stack` as the capstone for the local workshop.
4. Bootstrap a dedicated GCP study project with budget alerts and ADC.
5. Practice GCP provider configuration, read-only data sources, Cloud Storage,
   IAM without key files, and GCS remote state.
6. Implement an advanced local Airflow orchestration pattern that uses Cloud Run
   Jobs, GCS, BigQuery, and Dataform without committing secrets or target data.

Confidence: 95/100.

## Safety Rules

- Do not run `terraform apply` or `terraform destroy` against real GCP resources
  without explicit confirmation.
- Use a dedicated study GCP project, not a shared or production project.
- Create budget alerts before running Terraform against GCP. Budget alerts do
  not hard-cap spending.
- Avoid service account key JSON files; use Application Default Credentials for
  early local practice.
- Prefer `terraform fmt -recursive`, `terraform validate`, `terraform plan`, and
  `terraform test` while learning.
- Keep examples small, annotated, and self-contained.
- Treat state files, plans, tfvars, and credentials as sensitive.
- Commit only placeholder examples for env files, action plans, target
  metadata, certificates, and secret references.

Confidence: 97/100.
