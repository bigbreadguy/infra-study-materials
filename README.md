# Terraform Basics and GCP Practices Study Workspace

This repository is a local-first Terraform study workspace. The goal is to learn
Terraform fundamentals safely before practicing real GCP provisioning patterns.

Start with [PLAN.md](./PLAN.md). It defines the workshop sequence, local stack
options, safety rules, expected repository shape, and next implementation steps.
The current runnable lessons are [01-core-hcl-local](./01-core-hcl-local/),
[02-modules-and-composition](./02-modules-and-composition/), and
[03-docker-stack](./03-docker-stack/).

Confidence: 96/100.

## Study Direction

1. Learn Terraform basics with local-only examples.
2. Practice provider lifecycle behavior with Docker.
3. Practice platform resource management with a local Kubernetes cluster.
4. Explore cloud-shaped APIs with LocalStack where useful.
5. Move toward GCP provider configuration, remote state design, IAM, labels, and
   plan review before applying anything in a real project.

Confidence: 94/100.

## Safety Rules

- Do not run `terraform apply` or `terraform destroy` against real GCP resources
  without explicit confirmation.
- Prefer `terraform fmt -recursive`, `terraform validate`, `terraform plan`, and
  `terraform test` while learning.
- Keep examples small, annotated, and self-contained.
- Treat state files, plans, tfvars, and credentials as sensitive.

Confidence: 97/100.
