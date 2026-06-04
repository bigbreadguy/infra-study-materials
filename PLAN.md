# Terraform Basics And GCP Provider Practice Plan

## Purpose

Build a two-stage Terraform study path. Stage 1 teaches Terraform fundamentals
with safe local examples and ends at lesson `03`. Stage 2 pivots into a new,
separate GCP provider practice track that uses an isolated free-tier-aligned GCP
project with explicit cost and credential guardrails.

Confidence: 96/100.

## Principles

- Keep each lesson incremental, self-contained, and safe to run.
- Prefer annotated `.tf` examples and README notes explaining why each pattern
  matters.
- Default to `terraform fmt`, `terraform validate`, `terraform plan`, and
  `terraform test` where tests are available.
- Treat `terraform apply` against real GCP as a deliberate action that requires
  explicit confirmation and a cleanup path.
- Use Application Default Credentials for early local GCP practice; avoid
  committing or creating service account key JSON files.
- Track `.terraform.lock.hcl`; ignore state, plans, tfvars, credentials, and
  other local-only artifacts.

Confidence: 97/100.

## Stage 1: Local Terraform Workshop

The local workshop is complete after `03-docker-stack`. The previously planned
workshop sessions after `03` are intentionally skipped so the next learning
step can focus on real GCP provider basics.

Confidence: 96/100.

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
     reasoning, drift detection, labels, and cleanup.
   - Confidence: 91/100.

## Stage 2: GCP Provider Practice Track

The GCP track lives under `04-gcp-provider-practice/` and starts its own numbering
so the local workshop can still end cleanly at `03`.

Confidence: 95/100.

1. `04-1-project-safety-bootstrap`
   - Create or select a new isolated GCP project dedicated to this study repo.
   - Link billing or a free trial, create a small budget alert, choose
     `us-central1` as the default free-tier-aligned region, and configure ADC.
   - No Terraform resources are applied in this session.
   - Confidence: 96/100.

2. `04-2-provider-basics-read-only`
   - Configure the `hashicorp/google` provider with project, region, and zone
     variables.
   - Use read-only data sources to inspect project and provider context.
   - Practice `init`, `fmt`, `validate`, and `plan` without creating resources.
   - Confidence: 95/100.

3. `04-3-first-managed-resource`
   - Create one empty, labelled, Standard regional Cloud Storage bucket in a
     free-tier-eligible US region.
   - Practice globally unique names, labels, public access prevention, uniform
     bucket-level access, state review, and explicit cleanup.
   - Confidence: 90/100.

4. `04-4-iam-basics-no-keys`
   - Create a study service account and grant a narrow project role without
     generating service account key files.
   - Practice IAM member resources, least privilege, and destroy review.
   - Confidence: 89/100.

5. `04-5-gcs-remote-state-bootstrap`
   - Create a dedicated GCS bucket for Terraform state with object versioning,
     uniform bucket-level access, and public access prevention.
   - Learn the local-state-to-GCS-backend migration flow after the bucket
     exists.
   - Confidence: 90/100.

## Deferred Future Extensions

The following topics are useful, but they are no longer part of the immediate
workshop path:

- Local Kubernetes and Helm provider practice. Defer until Docker provider
  lifecycle behavior and GCP basics are comfortable.
  Confidence: 90/100.
- LocalStack and AWS-shaped cloud APIs. Defer because they do not directly
  support the GCP provider learning goal.
  Confidence: 95/100.
- A dedicated Terraform mock-testing lesson. Keep the existing lesson contract
  tests and revisit deeper mocks later.
  Confidence: 86/100.
- GKE, Cloud SQL, load balancers, NAT, static IPs, and broad networking labs.
  Defer because free-tier footnotes and cleanup risk are higher than the first
  GCP provider lessons need.
  Confidence: 94/100.

## Expected Repository Shape

```text
.
|-- PLAN.md
|-- AGENTS.md
|-- README.md
|-- 01-core-hcl-local/
|-- 02-modules-and-composition/
|-- 03-docker-stack/
`-- 04-gcp-provider-practice/
    |-- README.md
    |-- 04-1-project-safety-bootstrap/
    |-- 04-2-provider-basics-read-only/
    |-- 04-3-first-managed-resource/
    |-- 04-4-iam-basics-no-keys/
    `-- 04-5-gcs-remote-state-bootstrap/
```

Each Terraform lesson directory should include:

- `README.md`: goal, commands, expected observations, safety notes, and cleanup.
- `main.tf`: annotated Terraform configuration.
- `variables.tf`: typed inputs with validation where useful.
- `outputs.tf`: outputs that teach state, graph, and provider behavior.
- `terraform.tfvars.example`: placeholder values only, never real secrets.
- `tests/*.tftest.hcl`: added when the lesson has safe testable logic.

Confidence: 93/100.

## Common Commands

Use these commands repeatedly unless a lesson says otherwise:

```sh
terraform init
terraform fmt -recursive
terraform validate
terraform plan
terraform test
```

For local lessons, `terraform apply` is optional and should stay local. For GCP
lessons, run `terraform apply` only after confirming the target project,
budget alerts, enabled APIs, planned changes, and cleanup path.

Confidence: 95/100.

## GCP Safety Defaults

- Use a dedicated study project, not a shared or production project.
  Confidence: 98/100.
- Create a small project-scoped budget alert before Terraform touches GCP.
  Budgets notify; they do not hard-cap spending.
  Confidence: 98/100.
- Start with `us-central1` because it is eligible for current Cloud Storage and
  Compute Engine Free Tier limits.
  Confidence: 92/100.
- Enable only the APIs required by the current lesson.
  Confidence: 94/100.
- Avoid service account key JSON files. Use ADC first, then service account
  impersonation later for production-shaped workflows.
  Confidence: 96/100.
- Keep remote state as a dedicated lesson because the GCS backend requires a
  pre-existing bucket.
  Confidence: 94/100.

## Immediate Next Steps

1. Complete or review `01-core-hcl-local`, `02-modules-and-composition`, and
   `03-docker-stack`.
2. Work through `04-1-project-safety-bootstrap` before any
   GCP Terraform plan.
3. Run `04-2-provider-basics-read-only` as the first GCP
   provider exercise.
4. Apply real GCP resources only in the later GCP sessions after plan review
   and explicit cleanup confirmation.

Confidence: 94/100.
