# GCP Practice 01: Project Safety Bootstrap

## Goal

Prepare a dedicated GCP project for Terraform practice before the Google
provider creates or reads anything. This session is intentionally procedural:
the outcome is a safe project boundary, budget visibility, ADC authentication,
and a small set of defaults for later Terraform sessions.

Confidence: 96/100.

## What This Prepares

- A new or clearly isolated GCP project for this study workspace.
- A linked billing account or free trial account.
- A small project-scoped budget alert.
- Application Default Credentials for local Terraform.
- Default location choices for early free-tier-aligned labs.

Confidence: 95/100.

## Safety Checklist

1. Create or select a project used only for this study repository.
   Confidence: 98/100.

2. Link billing or confirm the free trial billing account.
   Confidence: 97/100.

3. Create a budget alert for this project before running Terraform against GCP.
   A budget sends alerts; it does not stop resources automatically.
   Confidence: 98/100.

4. Use `us-central1` as the default region and `us-central1-a` as the default
   zone for the first GCP provider exercises.
   Confidence: 92/100.

5. Do not create or download service account key JSON files.
   Confidence: 97/100.

## Suggested Commands

Use your real project id in your local shell. Do not commit it in normal
`*.tfvars` files.

```sh
gcloud init
gcloud auth application-default login
gcloud config set project PROJECT_ID
gcloud billing projects describe PROJECT_ID
gcloud services list --enabled --project PROJECT_ID
```

Confidence: 93/100.

## Budget Alert Guidance

Create a small budget such as USD 1 or USD 5 with alerts at 50, 90, and 100
percent. Keep the budget scoped to the study project when your permissions
allow it.

Confidence: 94/100.

## Completion Criteria

- You know the dedicated `project_id`.
- You can see the linked billing or free trial status.
- Budget alerts exist for the project.
- `gcloud auth application-default login` has completed locally.
- You can run `gcloud config get-value project` and see the study project.

Confidence: 95/100.

## Production Note

In production, project creation, billing linkage, budget policy, and IAM would
usually be governed by organization-level controls. This study setup is smaller
so Terraform provider basics can be practiced without bringing in an entire
landing-zone design.

Confidence: 94/100.
