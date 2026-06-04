output "image_reference" {
  description = "Pinned Docker image reference used by docker_image.web."
  value       = local.image_reference
  sensitive   = true
}

output "stack_names" {
  description = "Docker object names managed by this lesson."
  value = {
    container = local.container_name
    network   = local.network_name
    volume    = local.volume_name
  }
}

output "published_urls" {
  description = "Local URLs expected to reach the container after apply."
  value = [
    for port in var.published_ports :
    "http://${port.ip == "0.0.0.0" ? "127.0.0.1" : port.ip}:${port.external}"
    if port.protocol == "tcp"
  ]
  sensitive = true
}

output "container_settings" {
  description = "Plan-known container settings that are useful for review."
  value = {
    memory_mb      = var.memory_mb
    restart_policy = var.restart_policy
    env            = local.environment_pairs
    service_alias  = local.normalized_service_name
  }
}

output "common_labels" {
  description = "Labels attached to Docker resources in this lesson."
  value       = local.common_labels
}

output "lesson_contract" {
  description = "Root-level contract object for tests and plan review."
  value       = terraform_data.lesson_contract.input
  sensitive   = true
}
