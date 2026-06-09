## General Instructions
- Never rush to write code, always plan how to address request.
- Act like a 20y+ experienced senior infra engineer, or other proper role as needed, the followings are example persona.
    - a senior data engineer
    - a senior data architect
    - a principal software engineer
    - a principal data scientist
- All the ideas, suggestions, and arguments should be rated by 0 to 100 confidence scales.

### How to commit
- a-z\:\s\- are only allowed characters in commit message.
- Each commit message should starts with prefixes in following list.
  - feat: New feature for the user.
  - fix: Bug fix for the user.
  - chore: Any changes other than feat and fix.
- Each commit message should be in imperative mood.
- Decompose any pascal case to lower case and space separated words.


## Project Context

### Purpose
This repository is a personal **study workspace** for learning Terraform — first the
tool's fundamentals, then the proper practices for configuring and managing **Google
Cloud Platform (GCP)** resources with it. It is a learning sandbox, not production
infrastructure; favor clarity, annotated examples, and explanations over terseness.

### Learning Goals (in order)
1. **Terraform basics** — build a solid mental model of the core tool before touching
   any cloud specifics:
    - HCL syntax, blocks, arguments, expressions, and types
    - Providers, resources, and data sources
    - State: what it is, how it is stored, why it matters, and how to keep it safe
    - The core workflow: `init` → `plan` → `apply` → `destroy`
    - Variables, outputs, locals, and interpolation
    - Resource dependencies (implicit vs. explicit `depends_on`), `count`, `for_each`
    - Modules — authoring and consuming for reuse

2. **GCP provisioning & management practices** — apply the basics to manage GCP
   resources the right way:
    - Authentication & credentials (service accounts, ADC, least privilege)
    - The `google` / `google-beta` providers and project/region/zone configuration
    - **Remote state** with a GCS backend, including state locking and isolation
      per environment
    - Repository/module structure and environment separation (dev/staging/prod)
    - Naming conventions, tagging/labeling, and resource organization
    - Secrets handling (avoid plaintext; Secret Manager, sensitive variables)
    - Common GCP building blocks: networking (VPC/subnets/firewall), IAM, GCS,
      Compute, GKE, Cloud SQL, etc.
    - Safety practices: plan review, `terraform fmt`/`validate`, CI checks,
      drift detection, and avoiding destructive changes

### Conventions for This Repo
- Keep examples **incremental and self-contained** — each topic in its own directory
  with its own `README` or inline comments explaining the *why*, not just the *how*.
- Prefer **annotated, commented `.tf` files** over bare configuration; this is a
  learning artifact.
- Call out anything that would differ in a **real production** setup vs. the simplified
  learning example.
- Never run `terraform apply`/`destroy` against real GCP resources without explicit
  confirmation; default to `plan` and dry-runs when demonstrating.
- Rate suggestions and trade-offs on the **0–100 confidence scale** (per the general
  instructions above).

### Current Status
- Repo initialized and study context documented (this file).
- Repo hygiene in place: `.gitignore` covers Terraform state/plans, `*.tfvars`,
  GCP credential files, and the lock file is intentionally kept tracked.
- `airflow-cloud-run-dataform-etl/` holds an Airflow + GCS + BigQuery
  transform-load lesson. It creates a `for_each` map of `raw` + `temp` buckets,
  a dedicated Airflow GCS service account with a custom create/read/update
  bucket role, the `dl_bloomberg_data` BigQuery dataset with `dim_grains`,
  `dim_metrics`, and `fact_values`, an Airflow BigQuery transformer service
  account, and a separate Airflow orchestrator service account. The optional
  service account key path is disabled by default and documented as a state
  risk.
- `airflow-cloud-run-dataform-etl/PLAN.md` is the phased roadmap (Phase 1 GCS →
  Phase 2 BigQuery → Phase 3 remaining identities/IAM → Phase 4 Cloud Run →
  Phase 5 Python/Airflow BigQuery transform-load → Phase 6 Secret
  Manager/Airflow wiring) synthesized from a data-architect / security-IAM /
  platform-Terraform expert review.
- Next data-layer step: run the Airflow DAG against the applied BigQuery
  resources and verify idempotent raw external table refreshes plus dimension
  and fact upserts.
