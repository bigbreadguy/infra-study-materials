locals {
  normalized_workspace_name = lower(replace(trimspace(var.project_name), " ", "-"))
  normalized_owner          = lower(replace(trimspace(var.owner), " ", "-"))

  normalized_extra_labels = {
    for key, value in var.extra_labels : key => lower(replace(trimspace(value), " ", "-"))
  }

  required_labels = {
    environment = var.environment
    owner       = local.normalized_owner
    workspace   = local.normalized_workspace_name
  }

  labels      = merge(local.normalized_extra_labels, local.required_labels)
  name_prefix = "${local.normalized_workspace_name}-${var.environment}"
}

# terraform_data gives the module a concrete resource without contacting any
# provider API. That lets us study module state addresses safely.
resource "terraform_data" "label_contract" {
  input = {
    name_prefix = local.name_prefix
    labels      = local.labels
  }
}
