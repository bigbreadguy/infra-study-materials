# GCP Practice 02: Provider Basics Read Only

## Goal

Configure the Google provider and inspect GCP project context without creating
resources. This is the first Terraform session that talks to GCP, so it stays
read-only and focuses on provider configuration, ADC, variables, data sources,
and outputs.

Confidence: 95/100.

## What This Reads

- The selected GCP project metadata.
- The provider client configuration Terraform derives from ADC and variables.
- Shared labels that later managed resources should use.

Confidence: 94/100.

## Files

- `main.tf`: provider requirements, provider configuration, data sources, and
  local label contract.
- `variables.tf`: project, location, owner, and safety confirmation inputs.
- `outputs.tf`: project and provider context to inspect during plan review.
- `terraform.tfvars.example`: placeholder values for local `terraform.tfvars`.

Confidence: 94/100.

## Commands

Run from this directory after completing
`../01-project-safety-bootstrap/README.md`:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
```

Do not run `terraform apply`; this lesson has no managed resources.

Confidence: 96/100.

## Things To Observe

1. The provider configuration is in the root module. Child modules would inherit
   this provider configuration later unless explicitly overridden.
   Confidence: 95/100.

2. `data.google_project.selected` reads project metadata from GCP but does not
   manage the project lifecycle.
   Confidence: 94/100.

3. `data.google_client_config.current` exposes provider context, but outputs
   intentionally avoid credentials or access tokens.
   Confidence: 95/100.

4. The free-tier-aligned region validation keeps early practice in `us-east1`,
   `us-west1`, or `us-central1`.
   Confidence: 92/100.

## Workshop Exercise

1. Create a local ignored `terraform.tfvars` from the example values.
2. Set `project_id` to the dedicated study project.
3. Set `billing_budget_confirmed = true` only after budget alerts exist.
4. Run `terraform plan` and inspect the project id, project number, region,
   zone, and labels.

Confidence: 93/100.

## Production Note

User ADC is convenient for learning on a workstation. In production, prefer a
managed execution environment or service account impersonation, and avoid
long-lived service account key files.

Confidence: 96/100.
