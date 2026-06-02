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

## How Terraform Loads Files

Terraform does not import `variables.tf` or `outputs.tf` from `main.tf`.
Instead, Terraform loads all `.tf` files in the current directory and treats
them as one root module.

For this lesson, running Terraform from `01-core-hcl-local/` means these files
are combined into one configuration:

```text
main.tf
variables.tf
outputs.tf
```

That is why `main.tf` can reference `var.workspace_name`, even though the
`variable "workspace_name"` block lives in `variables.tf`.

Subdirectories are different. Terraform does not automatically load `.tf` files
from child directories. To use configuration from another directory, define a
`module` block with a `source` path.

Confidence: 99/100.

## Variables And Resources

Variables are inputs to the configuration. They let a caller change behavior
without editing resource blocks directly. In this lesson, `variables.tf`
declares inputs such as `workspace_name`, `environment`, `topic_tags`, and
`checkpoint_count`.

Reference variables with the `var.` prefix:

```hcl
var.workspace_name
var.checkpoint_count
```

Variables do not create infrastructure by themselves, and they do not have a
create, update, or delete lifecycle. They are knobs that influence the rest of
the configuration.

Confidence: 99/100.

Resources are managed objects. A resource block tells Terraform to track
something in state and manage its lifecycle. In this lesson, examples include
`random_id.workshop`, `terraform_data.topic`, `terraform_data.checkpoint`, and
`local_file.summary`.

Reference resource attributes with this pattern:

```hcl
resource_type.resource_name.attribute
```

For example:

```hcl
random_id.workshop.hex
```

That reads the `hex` attribute from the `workshop` resource whose type is
`random_id`.

Confidence: 99/100.

Variables often control resources. This resource uses the input variable
`checkpoint_count` to decide how many checkpoint instances Terraform should
plan:

```hcl
resource "terraform_data" "checkpoint" {
  count = var.checkpoint_count
}
```

A useful mental model:

```text
variables = knobs you set
resources = objects Terraform manages because of those knobs
```

Confidence: 98/100.

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
