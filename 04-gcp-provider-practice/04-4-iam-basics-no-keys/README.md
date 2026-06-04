# GCP Practice 04: IAM Basics No Keys

## Goal

Practice Google Cloud IAM with Terraform while avoiding service account key
files. This lesson creates a study service account and grants narrow project
roles using IAM member resources.

Confidence: 89/100.

## What This Builds

- One user-managed service account for Terraform IAM practice.
- One or more project-level IAM member grants for that service account.
- Outputs that show the service account email and granted roles.

Confidence: 89/100.

## Files

- `main.tf`: Google provider, service account, and IAM member resources.
- `variables.tf`: project, service account, role, and safety inputs.
- `outputs.tf`: service account identity and grants.
- `terraform.tfvars.example`: placeholder values for local `terraform.tfvars`.

Confidence: 93/100.

## Commands

Run from this directory after confirming the budget and IAM API:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
```

Apply only after reviewing the exact IAM role grants:

```sh
terraform apply
terraform destroy
```

Confidence: 91/100.

## Things To Observe

1. `google_service_account.practice` creates an identity, not credentials.
   Confidence: 96/100.

2. This lesson intentionally does not use `google_service_account_key`.
   Confidence: 98/100.

3. `google_project_iam_member.practice` adds specific role/member pairs without
   replacing the whole project IAM policy.
   Confidence: 92/100.

4. Destroying the configuration removes the IAM grants before deleting the
   service account.
   Confidence: 90/100.

## Workshop Exercise

1. Start with the default `roles/storage.objectViewer` grant.
2. Run `terraform plan` and inspect the service account email and IAM member.
3. Add another narrow role only if you can explain why it is needed.
4. Destroy the resources when finished.

Confidence: 88/100.

## Production Note

Production Terraform should usually use service account impersonation or a
managed runner identity. Long-lived service account keys increase operational
risk and should be avoided unless there is no safer option.

Confidence: 96/100.
