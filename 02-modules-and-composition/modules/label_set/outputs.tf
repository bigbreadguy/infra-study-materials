output "name_prefix" {
  description = "Normalized prefix suitable for naming resources in later lessons."
  value       = local.name_prefix
}

output "labels" {
  description = "Merged and normalized labels for downstream modules."
  value       = local.labels
}

output "label_keys" {
  description = "Sorted label keys to make the metadata contract easy to inspect."
  value       = sort(keys(local.labels))
}
