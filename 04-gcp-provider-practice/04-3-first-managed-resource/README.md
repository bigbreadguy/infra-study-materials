# GCP Practice 03: First Managed Resource

## Goal

Create and destroy one small Cloud Storage bucket so Terraform manages a real
GCP object with low cost exposure. This lesson practices globally unique names,
labels, public access prevention, uniform bucket-level access, state review, and
cleanup.

Confidence: 90/100.

## What This Builds

- A random suffix for a globally unique bucket name.
- One empty Standard regional Cloud Storage bucket.
- A shared label contract for later GCP resources.
- Outputs that make the bucket identity and cleanup target visible.

Confidence: 90/100.

## Files

- `main.tf`: Google and Random providers plus the bucket resource.
- `variables.tf`: project, region, label, and safety inputs.
- `outputs.tf`: bucket name, location, labels, and cleanup reminder.
- `terraform.tfvars.example`: placeholder values for local `terraform.tfvars`.

Confidence: 93/100.

## Commands

Run from this directory after completing the safety bootstrap and enabling the
Cloud Storage API for the study project:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
```

Apply only after reviewing the plan and confirming the target project:

```sh
terraform apply
terraform destroy
```

Confidence: 91/100.

## Things To Observe

1. Bucket names are globally unique, so this lesson appends `random_id` to a
   readable prefix.
   Confidence: 95/100.

2. `uniform_bucket_level_access = true` keeps access controlled by IAM instead
   of object ACLs.
   Confidence: 94/100.

3. `public_access_prevention = "enforced"` prevents accidental public exposure.
   Confidence: 95/100.

4. `force_destroy = true` is convenient for a learning bucket, but production
   state or data buckets should usually be more conservative.
   Confidence: 90/100.

## Workshop Exercise

1. Set `project_id`, `billing_budget_confirmed`, and `storage_api_enabled` in a
   local ignored `terraform.tfvars`.
2. Run `terraform plan` and confirm exactly one bucket and one random id are
   planned.
3. Apply only if the project and bucket location are correct.
4. Run `terraform state list` after apply.
5. Run `terraform destroy` when finished.

Confidence: 91/100.

## Production Note

Production storage buckets require stronger design choices: retention policy,
encryption, lifecycle management, IAM ownership, logging, backup, and naming
standards. This lesson stays intentionally small so provider lifecycle behavior
is the main topic.

Confidence: 94/100.
