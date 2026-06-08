terraform {
  required_version = ">= 1.6.0"

  required_providers {
    docker = {
      source  = "kreuzwerker/docker"
      version = "~> 4.4"
    }
  }
}

provider "docker" {
  # Leave null for the provider default, or set a value such as
  # unix://$HOME/.docker/run/docker.sock on Docker Desktop for macOS.
  host = var.docker_host
}

locals {
  normalized_project_name = lower(replace(trimspace(var.project_name), " ", "-"))
  normalized_service_name = lower(replace(trimspace(var.service_name), " ", "-"))
  normalized_owner        = lower(trimspace(var.owner))

  name_prefix     = "${local.normalized_project_name}-${var.environment}"
  image_reference = "${var.image_repository}:${var.image_tag}"

  common_labels = merge(
    {
      "study.workspace"   = local.normalized_project_name
      "study.environment" = var.environment
      "study.owner"       = local.normalized_owner
      "study.lesson"      = "03-docker-stack"
    },
    var.extra_labels
  )

  container_name = "${local.name_prefix}-${local.normalized_service_name}"
  network_name   = "${local.name_prefix}-network"
  volume_name    = "${local.name_prefix}-html"

  environment_pairs = [
    for key in sort(keys(var.environment_variables)) :
    "${key}=${var.environment_variables[key]}"
  ]

  index_html = <<-EOT
  <!doctype html>
  <html lang="en">
  <head>
    <meta charset="utf-8">
    <title>${var.project_name}</title>
  </head>
  <body>
    <h1>${var.project_name}</h1>
    <p>Managed by Terraform lesson 03.</p>
    <p>Environment: ${var.environment}</p>
  </body>
  </html>
  EOT
}

# The image is a real provider-managed object. Pinning the tag keeps plan output
# meaningful and avoids accidental changes caused by a moving latest tag.
resource "docker_image" "web" {
  name         = local.image_reference
  keep_locally = var.keep_image_locally
}

# A user-defined bridge network gives the container a lesson-owned boundary.
# Later Kubernetes and GCP networking lessons build on this same mental model.
resource "docker_network" "lesson" {
  name   = local.network_name
  driver = "bridge"

  dynamic "labels" {
    for_each = local.common_labels

    content {
      label = labels.key
      value = labels.value
    }
  }
}

# This named volume is intentionally small. It exists to make storage lifecycle
# visible without binding arbitrary host paths into the container.
resource "docker_volume" "html" {
  name = local.volume_name

  dynamic "labels" {
    for_each = local.common_labels

    content {
      label = labels.key
      value = labels.value
    }
  }
}

resource "docker_container" "web" {
  name  = local.container_name
  image = docker_image.web.image_id

  must_run = true
  restart  = var.restart_policy
  memory   = var.memory_mb
  env      = local.environment_pairs

  networks_advanced {
    name    = docker_network.lesson.name
    aliases = [local.normalized_service_name]
  }

  volumes {
    volume_name    = docker_volume.html.name
    container_path = "/usr/share/nginx/html/workshop-data"
  }

  upload {
    file    = "/usr/share/nginx/html/index.html"
    content = local.index_html
  }

  dynamic "ports" {
    for_each = var.published_ports

    content {
      internal = ports.value.internal
      external = ports.value.external
      protocol = ports.value.protocol
      ip       = ports.value.ip
    }
  }

  dynamic "labels" {
    for_each = local.common_labels

    content {
      label = labels.key
      value = labels.value
    }
  }
}

# A root-level contract makes the stack easy to inspect in plan output and keeps
# the lesson testable with a mocked provider.
resource "terraform_data" "lesson_contract" {
  input = {
    image_reference = local.image_reference
    names = {
      container = local.container_name
      network   = local.network_name
      volume    = local.volume_name
    }
    labels          = local.common_labels
    published_ports = var.published_ports
    restart_policy  = var.restart_policy
  }
}
