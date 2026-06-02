output "name_prefix" {
  description = "Normalized naming prefix shared across module calls."
  value       = module.standard_labels.name_prefix
}

output "common_labels" {
  description = "Normalized labels returned by the shared label module."
  value       = module.standard_labels.labels
}

output "track_summaries" {
  description = "Composed learning path summaries keyed by module instance name."
  value = {
    for track_name, track in module.learning_path : track_name => track.summary
  }
}

output "required_tracks" {
  description = "Required tracks derived from child module outputs."
  value = [
    for track_name, track in module.learning_path : track_name
    if track.required
  ]
}

output "checkpoint_matrix" {
  description = "Checkpoint labels from each child module."
  value = {
    for track_name, track in module.learning_path : track_name => track.checkpoint_labels
  }
}

output "lesson_contract" {
  description = "Root-level contract object stored by terraform_data.lesson_contract."
  value       = terraform_data.lesson_contract.input
}
