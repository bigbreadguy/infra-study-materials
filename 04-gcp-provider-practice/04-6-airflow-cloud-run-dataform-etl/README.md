# Airflow, Cloud Run, BigQuery, And Dataform ETL

## Goal

Plan a Terraform-managed GCP ETL environment where local Airflow triggers Cloud
Run Jobs, lands immutable raw data in Cloud Storage, loads BigQuery, and invokes
Dataform transformations.

This directory is planning-only right now. It does not contain Terraform
configuration and should not create resources.

Confidence: 96/100.

## Start Here

Read [PLAN.md](./PLAN.md) for the design, service account boundaries, IAM
shape, public-repository guardrails, Airflow runtime contract, and open
implementation decisions.

Confidence: 95/100.

## Safety Notes

- Do not run `terraform apply` or `terraform destroy` for this lesson until
  Terraform resources, cleanup steps, and cost boundaries are added and
  reviewed.
- Keep real target URLs, selectors, cookies, request headers, action plans,
  certificates, API tokens, passwords, and service account keys out of committed
  files.
- Use placeholder examples in committed docs and keep private runtime inputs in
  ignored local files, Secret Manager, or private GCS objects created outside
  Terraform.

Confidence: 98/100.
