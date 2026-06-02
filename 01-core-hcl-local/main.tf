terraform {
  required_version = ">= 1.6.0"

  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.5"
    }

    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
  }
}

locals {
  normalized_workspace_name = lower(replace(trimspace(var.workspace_name), " ", "-"))
  normalized_topics         = sort(tolist(toset([for tag in var.topic_tags : lower(trimspace(tag))])))

  topic_map = {
    for index, topic in local.normalized_topics : topic => {
      order = index + 1
      slug  = replace(topic, " ", "-")
    }
  }

  common_labels = {
    environment = var.environment
    owner       = var.owner
    lesson      = "01-core-hcl-local"
  }

  summary_lines = concat(
    [
      "Terraform basics workshop",
      "workspace: ${var.workspace_name}",
      "environment: ${var.environment}",
      "owner: ${var.owner}",
      "workshop_id: ${random_id.workshop.hex}",
      "",
      "topics:"
    ],
    [for topic, config in local.topic_map : "- ${config.order}. ${topic} (${config.slug})"],
    [
      "",
      "checkpoint_count: ${var.checkpoint_count}",
      "summary_enabled: ${var.create_summary_file}"
    ]
  )

  summary_content = join("\n", local.summary_lines)
}

# The random id is stable until one of the keeper values changes. This is a
# useful pattern when an identifier should move with a meaningful input change.
resource "random_id" "workshop" {
  byte_length = 4

  keepers = {
    workspace_name = var.workspace_name
    environment    = var.environment
  }
}

# terraform_data is built into Terraform and is useful for learning graph and
# state behavior without relying on a cloud API.
resource "terraform_data" "lesson_context" {
  input = {
    labels = local.common_labels
    topics = local.normalized_topics
  }
}

# for_each creates resource instances addressed by stable keys. Adding or
# removing one topic affects that specific key instead of shifting every index.
resource "terraform_data" "topic" {
  for_each = local.topic_map

  input = {
    name   = each.key
    order  = each.value.order
    slug   = each.value.slug
    labels = local.common_labels
  }
}

# count creates resource instances addressed by numeric indexes. It is simple
# for interchangeable instances, but index shifts can matter in real resources.
resource "terraform_data" "checkpoint" {
  count = var.checkpoint_count

  input = {
    number = count.index + 1
    label  = "checkpoint-${count.index + 1}"
  }
}

# This resource only writes inside this lesson directory. Plan first, then apply
# only when you intentionally want to create the generated summary file.
resource "local_file" "summary" {
  count = var.create_summary_file ? 1 : 0

  filename = "${path.module}/generated/${local.normalized_workspace_name}-summary.txt"
  content  = local.summary_content

  depends_on = [terraform_data.lesson_context]
}
