# GCP Practice 05: GCS Remote State Bootstrap

## Goal

Create a dedicated Cloud Storage bucket for Terraform state and learn how to
migrate from local state to the GCS backend. This is its own lesson because the
backend bucket must exist before Terraform can use it as a backend.

Confidence: 90/100.

## What This Builds

- A random suffix for a globally unique state bucket name.
- One private Standard regional Cloud Storage bucket for Terraform state.
- Object versioning for state recovery.
- A `backend.tf.example` file showing the backend block to enable after the
  bucket exists.

Confidence: 91/100.

## Files

- `main.tf`: Google and Random providers plus the state bucket resource.
- `variables.tf`: project, bucket, label, and safety inputs.
- `outputs.tf`: state bucket name, prefix, and migration reminders.
- `backend.tf.example`: example backend block; Terraform ignores this file
  until you intentionally create a real `backend.tf`.
- `terraform.tfvars.example`: placeholder values for local `terraform.tfvars`.

Confidence: 93/100.

## Commands

Run from this directory while state is still local:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
terraform apply
```

After the bucket exists, use the output bucket name to create a local
`backend.tf` from `backend.tf.example`, then migrate:

```sh
terraform init -migrate-state
terraform plan
```

Confidence: 88/100.

## Things To Observe

1. Terraform starts with local state because the GCS backend bucket does not
   exist yet.
   Confidence: 96/100.

2. The GCS backend supports state locking, which helps prevent concurrent state
   writes.
   Confidence: 94/100.

3. Object versioning improves recovery options if state is overwritten or
   deleted accidentally.
   Confidence: 94/100.

4. `force_destroy_state_bucket` defaults to false because state buckets should
   resist accidental deletion.
   Confidence: 95/100.

## Workshop Exercise

1. Apply the state bucket with local state.
2. Inspect the bucket name output.
3. Create a local `backend.tf` using `backend.tf.example` as the template.
4. Run `terraform init -migrate-state`.
5. Run `terraform plan` again and confirm there are no infrastructure changes.

Confidence: 89/100.

## Production Note

A production state bucket should have tightly controlled IAM, versioning,
lifecycle policy for old versions, audit logging, and a recovery process. This
lesson focuses on the core backend migration flow first.

Confidence: 94/100.
