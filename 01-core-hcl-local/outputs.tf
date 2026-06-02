output "workshop_id" {
  description = "Random id derived from keeper inputs. It changes when keepers change."
  value       = random_id.workshop.hex
}

output "summary_file_path" {
  description = "Path Terraform will manage when create_summary_file is true."
  value       = try(local_file.summary[0].filename, null)
}

output "summary_preview" {
  description = "Generated summary content. Values derived from resources may be known after apply."
  value       = local.summary_content
}

output "topic_order" {
  description = "Topic order derived from normalized for_each keys."
  value = {
    for topic, config in local.topic_map :
    topic => config.order
  }
}

output "checkpoint_labels" {
  description = "Labels derived from the same indexes used by count-based checkpoints."
  value       = [for index in range(var.checkpoint_count) : "checkpoint-${index + 1}"]
}

output "common_labels" {
  description = "Shared metadata that later maps to cloud labels or tags."
  value       = local.common_labels
}
