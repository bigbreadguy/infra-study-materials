output "track_id" {
  description = "Stable id composed from the shared name prefix and this track name."
  value       = local.track_id
}

output "required" {
  description = "Whether this track is required."
  value       = var.required
}

output "summary" {
  description = "Compact module contract for callers."
  value       = local.summary
}

output "topic_order" {
  description = "Topic order after normalization and sorting."
  value = {
    for topic_key, topic in local.topic_map : topic_key => topic.order
  }
}

output "checkpoint_labels" {
  description = "Checkpoint labels generated from the requested count."
  value       = [for checkpoint in local.checkpoint_map : checkpoint.label]
}
