# Lesson 01: Core HCL With Local Providers

## Goal

Learn Terraform's core language and workflow without creating any cloud
resources. This lesson uses local-only resources so the first run is about
Terraform mechanics: configuration, variables, locals, resources, outputs,
dependencies, state, `count`, `for_each`, and tests.

Confidence: 96/100.

## What This Builds

- A random workshop id that stays stable until selected inputs change.
- A generated text summary under `generated/`.
- Topic markers created with `for_each`.
- Checkpoint markers created with `count`.
- Outputs that make known values and known-after-apply values visible.

Confidence: 95/100.

## Files

- `main.tf`: provider requirements, locals, resources, and dependency examples.
- `variables.tf`: typed inputs and validation rules.
- `outputs.tf`: values to inspect after `plan` or `apply`.
- `tests/lesson_contract.tftest.hcl`: plan-time checks for the lesson contract.

Confidence: 94/100.

## Commands

Run from this directory:

```sh
terraform init
terraform fmt
terraform validate
terraform plan
terraform test
```

Use `terraform apply` only if you want Terraform to actually write the local
summary file. That is safe in this lesson because it only writes inside this
directory, but `plan` is enough to learn the graph and state model first.

Confidence: 95/100.

## Things To Observe

1. `random_id.workshop` keeps the same value across plans until one of its
   `keepers` changes.
   Confidence: 96/100.

2. `local_file.summary` depends implicitly on values it references and explicitly
   on `terraform_data.lesson_context`.
   Confidence: 94/100.

3. `terraform_data.topic` uses `for_each`, so each instance is addressed by a
   stable key such as `terraform_data.topic["state"]`.
   Confidence: 95/100.

4. `terraform_data.checkpoint` uses `count`, so each instance is addressed by a
   numeric index such as `terraform_data.checkpoint[0]`.
   Confidence: 95/100.

5. Outputs are stored in state after apply. During plan review, compare values
   Terraform already knows with values marked as known only after apply.
   Confidence: 94/100.

## Workshop Exercise

1. Run `terraform plan` with the defaults.
2. Change `checkpoint_count` with a CLI variable:

   ```sh
   terraform plan -var="checkpoint_count=2"
   ```

3. Change `topic_tags` and compare how `for_each` addresses change:

   ```sh
   terraform plan -var='topic_tags=["hcl","state","dependencies"]'
   ```

4. Change `workspace_name` and observe why the random id must be replaced.

Confidence: 93/100.

## Production Note

This lesson writes local state because that is the clearest way to learn the
mechanics. In a real team or GCP environment, state should move to a remote
backend such as GCS, with access control, versioning, and environment isolation.

Confidence: 96/100.
