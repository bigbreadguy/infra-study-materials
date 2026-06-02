terraform {
  required_version = ">= 1.6.0"
}

locals {
  normalized_learning_tracks = {
    for track_name, config in var.learning_tracks : track_name => {
      description      = trimspace(config.description)
      topics           = sort(tolist(toset([for topic in config.topics : lower(trimspace(topic))])))
      checkpoint_count = config.checkpoint_count
      required         = config.required
    }
  }
}

# This module owns the shared metadata contract. Keeping it separate makes it
# easy to reuse the same labels when later lessons introduce real providers.
module "standard_labels" {
  source = "./modules/label_set"

  project_name = var.workspace_name
  environment  = var.environment
  owner        = var.owner
  extra_labels = var.extra_labels
}

# Calling the same module with for_each creates stable module instance addresses.
# This is the module-level version of the for_each resource pattern from lesson 01.
module "learning_path" {
  for_each = local.normalized_learning_tracks

  source = "./modules/learning_path"

  track_name       = each.key
  description      = each.value.description
  topics           = each.value.topics
  checkpoint_count = each.value.checkpoint_count
  required         = each.value.required
  labels           = module.standard_labels.labels
  name_prefix      = module.standard_labels.name_prefix
}

# A root-level contract is useful when modules are composed together. It gives
# callers one predictable object to inspect without knowing every child detail.
resource "terraform_data" "lesson_contract" {
  input = {
    name_prefix = module.standard_labels.name_prefix
    labels      = module.standard_labels.labels
    track_ids = {
      for track_name, track in module.learning_path : track_name => track.track_id
    }
    required_tracks = [
      for track_name, track in module.learning_path : track_name
      if track.required
    ]
  }
}
