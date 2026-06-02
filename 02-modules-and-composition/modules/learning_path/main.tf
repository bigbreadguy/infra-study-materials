locals {
  track_slug        = replace(lower(trimspace(var.track_name)), "_", "-")
  normalized_topics = sort(tolist(toset([for topic in var.topics : lower(trimspace(topic))])))

  topic_map = {
    for index, topic in local.normalized_topics : replace(topic, " ", "-") => {
      name  = topic
      order = index + 1
    }
  }

  checkpoint_map = {
    for index in range(var.checkpoint_count) : tostring(index + 1) => {
      number = index + 1
      label  = "${var.track_name}-checkpoint-${index + 1}"
    }
  }

  module_labels = merge(var.labels, {
    required = tostring(var.required)
    track    = local.track_slug
  })

  track_id = "${var.name_prefix}-${local.track_slug}"

  summary = {
    track_id         = local.track_id
    description      = trimspace(var.description)
    required         = var.required
    topic_count      = length(local.normalized_topics)
    checkpoint_count = var.checkpoint_count
    topics           = local.normalized_topics
    labels           = local.module_labels
  }
}

# Topics use for_each because topic names are meaningful stable keys.
resource "terraform_data" "topic" {
  for_each = local.topic_map

  input = {
    track_id = local.track_id
    name     = each.value.name
    order    = each.value.order
    labels   = local.module_labels
  }
}

# Checkpoints use for_each with generated numeric keys. This keeps the lesson
# parallel with real modules that expose repeated child resources.
resource "terraform_data" "checkpoint" {
  for_each = local.checkpoint_map

  input = {
    track_id = local.track_id
    number   = each.value.number
    label    = each.value.label
    labels   = local.module_labels
  }
}

resource "terraform_data" "summary" {
  input = local.summary

  depends_on = [
    terraform_data.topic,
    terraform_data.checkpoint
  ]
}
