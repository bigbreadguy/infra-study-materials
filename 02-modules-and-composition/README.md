# Lesson 02: Modules And Composition

## Goal

Learn how to split Terraform configuration into small reusable modules with
clear input and output contracts. This lesson stays local-only and uses
Terraform's built-in `terraform_data` resource so the focus is module design,
type constraints, validation, and composition rather than provider behavior.

Confidence: 95/100.

## What This Builds

- A reusable label module that normalizes shared metadata.
- A reusable learning path module that models topics and checkpoints.
- Multiple module instances created with `for_each`.
- Root outputs that compose child module outputs into one lesson contract.

Confidence: 94/100.

## Files

- `main.tf`: root module composition and cross-module wiring.
- `variables.tf`: root inputs with object types and validation rules.
- `outputs.tf`: composed outputs from child modules.
- `modules/label_set`: a small module for shared labels and name prefixes.
- `modules/learning_path`: a small module for reusable track configuration.
- `tests/lesson_contract.tftest.hcl`: plan-time checks for module contracts.

Confidence: 94/100.

## Commands

Run from this directory:

```sh
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform test
```

`terraform apply` is not necessary for this lesson. If you do apply it, Terraform
only records local `terraform_data` resources in state.

Confidence: 96/100.

## Things To Observe

1. `module.standard_labels` is called once and reused by every learning path
   module instance.
   Confidence: 95/100.

2. `module.learning_path` uses `for_each`, so each module instance has a stable
   address such as `module.learning_path["core"]`.
   Confidence: 95/100.

3. Module inputs use object, map, set, string, number, and bool types. This makes
   the expected contract visible before Terraform reaches any provider logic.
   Confidence: 94/100.

4. Validation rules catch malformed track names, empty topics, and invalid
   checkpoint counts close to the module boundary.
   Confidence: 93/100.

5. Root outputs compose child module outputs into a shape that later lessons can
   compare with environment or cloud module outputs.
   Confidence: 92/100.

## Workshop Exercise

1. Run `terraform plan` with the defaults and inspect the module addresses.
2. Add a new learning track under `learning_tracks` with a unique key.
3. Change one track's `checkpoint_count` and compare which instances change.
4. Temporarily set an invalid track key such as `Core Track` and run
   `terraform validate` to see the validation error.

Confidence: 93/100.

## Production Note

These modules are intentionally small so the boundaries are easy to study. In a
production GCP repository, modules should still have clear contracts, but they
would also include provider-specific concerns such as IAM assumptions, naming
standards, labels, lifecycle rules, tests, and README examples for supported
environments.

Confidence: 94/100.
