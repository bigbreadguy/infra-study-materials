# Terraform Local Practice Workshop Plan

## Purpose

Build a local-first Terraform workshop that teaches Terraform fundamentals before
touching real GCP resources. Use local, Docker, Kubernetes, and cloud-emulator
stacks so practice can happen without public cloud credentials or spend.

Confidence: 95/100.

## Principles

- Keep each lesson incremental, self-contained, and safe to run locally.
- Prefer annotated `.tf` examples and short README notes explaining why each
  pattern matters.
- Default to `terraform plan`, `terraform validate`, `terraform fmt`, and
  `terraform test`.
- Do not run `terraform apply` or `terraform destroy` against real GCP without
  explicit confirmation.
- Track `.terraform.lock.hcl`; ignore state, plans, tfvars, and credentials.

Confidence: 96/100.

## Recommended Workshop Sequence

1. `01-core-hcl-local`
   - Use built-in Terraform features plus `local`, `random`, and
     `terraform_data`.
   - Practice blocks, arguments, expressions, variables, locals, outputs,
     dependencies, state, `count`, and `for_each`.
   - Confidence: 95/100.

2. `02-modules-and-composition`
   - Build small reusable modules with clear inputs and outputs.
   - Practice module boundaries, type constraints, validation, and composition.
   - Confidence: 93/100.

3. `03-docker-stack`
   - Use the Docker provider to create local networks, volumes, images, and
     containers.
   - Practice real provider lifecycle, replacement behavior, resource graph
     reasoning, and cleanup.
   - Confidence: 90/100.

4. `04-local-kubernetes`
   - Use kind, k3d, or minikube, then Terraform Kubernetes and Helm providers.
   - Practice namespaces, ConfigMaps, Deployments, Services, Helm releases, and
     kubeconfig provider wiring.
   - Confidence: 88/100.

5. `05-localstack-aws-shaped-cloud`
   - Use LocalStack for AWS-shaped APIs such as S3, SQS, DynamoDB, and Lambda
     style workflows.
   - Practice cloud-like provider endpoint overrides and service dependencies.
   - Caveat: useful for cloud concepts, but not a GCP parity layer.
   - Confidence: 82/100.

6. `06-terraform-test-mocks`
   - Use `terraform test`, `mock_provider`, `mock_resource`, and assertions.
   - Practice module verification without credentials or infrastructure.
   - Caveat: mocks validate Terraform logic, not real provider API behavior.
   - Confidence: 85/100.

7. `07-gcp-readiness-dry-run`
   - Prepare GCP provider configuration, naming, labels, state design, and IAM
     assumptions without applying resources.
   - Practice production differences, remote GCS state design, and plan review.
   - Confidence: 86/100.

## Local Stack Options

- Core local providers: best for Terraform language and state fundamentals.
  Confidence: 95/100.
- Docker provider: best first realistic local infrastructure target.
  Confidence: 90/100.
- Local Kubernetes: best for platform and deployment resource practice.
  Confidence: 88/100.
- LocalStack: best for AWS-shaped cloud API practice, not GCP-specific practice.
  Confidence: 82/100.
- GCP service emulators: useful for application integration tests, limited for
  Terraform infrastructure practice.
  Confidence: 78/100.
- DevStack/OpenStack: powerful but heavy; use only in a dedicated VM if local
  IaaS concepts are needed.
  Confidence: 70/100.

## Expected Repository Shape

```text
.
|-- PLAN.md
|-- AGENTS.md
|-- README.md
|-- 01-core-hcl-local/
|-- 02-modules-and-composition/
|-- 03-docker-stack/
|-- 04-local-kubernetes/
|-- 05-localstack-aws-shaped-cloud/
|-- 06-terraform-test-mocks/
`-- 07-gcp-readiness-dry-run/
```

Each lesson directory should include:

- `README.md`: goal, commands, expected observations, production notes.
- `main.tf`: annotated Terraform configuration.
- `variables.tf`: typed inputs with validation where useful.
- `outputs.tf`: outputs that teach state and dependency behavior.
- `tests/*.tftest.hcl`: added when the lesson has testable logic.

Confidence: 92/100.

## Workshop Commands

Use these commands repeatedly unless a lesson says otherwise:

```sh
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform test
```

Use `terraform apply` only for explicitly local stacks such as Docker,
Kubernetes, or LocalStack labs. Use `terraform destroy` only after confirming the
target is local.

Confidence: 94/100.

## Immediate Next Steps

1. Run `01-core-hcl-local` after Terraform CLI is installed.
2. Review plan output for local state, variables, locals, outputs, `random_id`,
   `local_file`, `terraform_data`, `count`, and `for_each`.
3. Add `02-modules-and-composition` after the first lesson is working.
4. Add Docker and Kubernetes labs only after the Terraform core lessons are clear.
5. Keep each new lesson documented with commands, expected observations, and
   production differences.

Confidence: 93/100.
