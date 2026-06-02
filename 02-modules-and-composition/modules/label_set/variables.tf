variable "project_name" {
  description = "Human-readable workspace or project name used to build a name prefix."
  type        = string

  validation {
    condition     = length(trimspace(var.project_name)) >= 3
    error_message = "project_name must contain at least three non-space characters."
  }
}

variable "environment" {
  description = "Environment label to include in the shared metadata."
  type        = string

  validation {
    condition     = contains(["dev", "staging", "prod", "sandbox"], var.environment)
    error_message = "environment must be one of: dev, staging, prod, sandbox."
  }
}

variable "owner" {
  description = "Owner label value to normalize."
  type        = string

  validation {
    condition     = length(trimspace(var.owner)) > 0
    error_message = "owner must not be empty."
  }
}

variable "extra_labels" {
  description = "Additional label-like metadata. Required labels win on key conflicts."
  type        = map(string)
  default     = {}

  validation {
    condition = alltrue([
      for key, value in var.extra_labels :
      can(regex("^[a-z][a-z0-9_-]*$", key)) && length(trimspace(value)) > 0
    ])
    error_message = "extra_labels keys must be lower snake/kebab style and values must not be empty."
  }
}
