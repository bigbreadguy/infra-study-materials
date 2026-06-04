variable "project_name" {
  description = "Human-readable name for this Docker provider lesson."
  type        = string
  default     = "Terraform Docker Workshop"

  validation {
    condition     = length(trimspace(var.project_name)) >= 3
    error_message = "project_name must contain at least three non-space characters."
  }
}

variable "environment" {
  description = "Practice environment label. This is local metadata in lesson 03."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod", "sandbox"], var.environment)
    error_message = "environment must be one of: dev, staging, prod, sandbox."
  }
}

variable "owner" {
  description = "Name or handle of the learner responsible for this local Docker stack."
  type        = string
  default     = "kenny"

  validation {
    condition     = length(trimspace(var.owner)) > 0
    error_message = "owner must not be empty."
  }
}

variable "service_name" {
  description = "Short service name used in the container name and network alias."
  type        = string
  default     = "web"

  validation {
    condition     = can(regex("^[a-z][a-z0-9-]*$", var.service_name))
    error_message = "service_name must start with a lower-case letter and use only lower-case letters, numbers, or hyphens."
  }
}

variable "image_repository" {
  description = "Docker image repository to run for the workshop container."
  type        = string
  default     = "nginx"

  validation {
    condition     = length(trimspace(var.image_repository)) > 0
    error_message = "image_repository must not be empty."
  }
}

variable "image_tag" {
  description = "Pinned Docker image tag. Avoid latest so plan output stays reviewable."
  type        = string
  default     = "1.27-alpine"

  validation {
    condition     = length(trimspace(var.image_tag)) > 0 && var.image_tag != "latest"
    error_message = "image_tag must be a non-empty pinned tag and must not be latest."
  }
}

variable "published_ports" {
  description = "Container ports to publish on the local host."
  type = list(object({
    internal = number
    external = number
    protocol = string
    ip       = string
  }))

  default = [
    {
      internal = 80
      external = 8080
      protocol = "tcp"
      ip       = "127.0.0.1"
    }
  ]

  validation {
    condition     = length(var.published_ports) > 0
    error_message = "published_ports must contain at least one port mapping."
  }

  validation {
    condition = alltrue([
      for port in var.published_ports :
      port.internal >= 1
      && port.internal <= 65535
      && floor(port.internal) == port.internal
      && port.external >= 1
      && port.external <= 65535
      && floor(port.external) == port.external
    ])
    error_message = "published port numbers must be whole numbers from 1 to 65535."
  }

  validation {
    condition = alltrue([
      for port in var.published_ports :
      contains(["tcp", "udp", "sctp"], port.protocol)
    ])
    error_message = "published port protocols must be tcp, udp, or sctp."
  }

  validation {
    condition = alltrue([
      for port in var.published_ports :
      can(regex("^(127\\.0\\.0\\.1|0\\.0\\.0\\.0|::1)$", port.ip))
    ])
    error_message = "published port ip must be 127.0.0.1, 0.0.0.0, or ::1."
  }
}

variable "environment_variables" {
  description = "Environment variables passed into the container as KEY=VALUE pairs."
  type        = map(string)
  default = {
    WORKSHOP = "terraform-docker-stack"
  }

  validation {
    condition = alltrue([
      for key, value in var.environment_variables :
      can(regex("^[A-Z_][A-Z0-9_]*$", key)) && length(trimspace(value)) > 0
    ])
    error_message = "environment variable keys must be uppercase shell-style names and values must not be empty."
  }
}

variable "restart_policy" {
  description = "Docker restart policy for the local container."
  type        = string
  default     = "no"

  validation {
    condition     = contains(["no", "on-failure", "always", "unless-stopped"], var.restart_policy)
    error_message = "restart_policy must be one of: no, on-failure, always, unless-stopped."
  }
}

variable "memory_mb" {
  description = "Memory limit for the container in MB."
  type        = number
  default     = 128

  validation {
    condition     = var.memory_mb >= 32 && var.memory_mb <= 1024 && floor(var.memory_mb) == var.memory_mb
    error_message = "memory_mb must be a whole number from 32 to 1024."
  }
}

variable "keep_image_locally" {
  description = "Whether docker_image should leave the pulled image on disk during destroy."
  type        = bool
  default     = true
}

variable "extra_labels" {
  description = "Additional Docker labels to attach to every managed Docker object."
  type        = map(string)
  default = {
    curriculum = "terraform-basics"
  }

  validation {
    condition = alltrue([
      for key, value in var.extra_labels :
      can(regex("^[a-z][a-z0-9_.-]*$", key)) && length(trimspace(value)) > 0
    ])
    error_message = "extra_labels keys must start with a lower-case letter and use lower-case letters, numbers, dots, underscores, or hyphens; values must not be empty."
  }
}

variable "docker_host" {
  description = "Optional Docker API host URI. Leave null to use the provider default."
  type        = string
  default     = null
  nullable    = true

  validation {
    condition     = var.docker_host == null || can(regex("^(unix|tcp|ssh)://", var.docker_host))
    error_message = "docker_host must be null or start with unix://, tcp://, or ssh://."
  }
}
