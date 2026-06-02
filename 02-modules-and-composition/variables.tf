variable "workspace_name" {
  description = "Human-readable name for this module composition lesson."
  type        = string
  default     = "Terraform Module Workshop"

  validation {
    condition     = length(trimspace(var.workspace_name)) >= 3
    error_message = "workspace_name must contain at least three non-space characters."
  }
}

variable "environment" {
  description = "Practice environment label. This is metadata only in lesson 02."
  type        = string
  default     = "dev"

  validation {
    condition     = contains(["dev", "staging", "prod", "sandbox"], var.environment)
    error_message = "environment must be one of: dev, staging, prod, sandbox."
  }
}

variable "owner" {
  description = "Name or handle of the learner responsible for this local run."
  type        = string
  default     = "kenny"

  validation {
    condition     = length(trimspace(var.owner)) > 0
    error_message = "owner must not be empty."
  }
}

variable "extra_labels" {
  description = "Additional label-like metadata to pass through the shared label module."
  type        = map(string)
  default = {
    curriculum = "terraform-basics"
    lesson     = "02-modules-and-composition"
  }

  validation {
    condition = alltrue([
      for key, value in var.extra_labels :
      can(regex("^[a-z][a-z0-9_-]*$", key)) && length(trimspace(value)) > 0
    ])
    error_message = "extra_labels keys must be lower snake/kebab style and values must not be empty."
  }
}

variable "learning_tracks" {
  description = "Reusable module instances to create, keyed by stable track name."
  type = map(object({
    description      = string
    topics           = set(string)
    checkpoint_count = number
    required         = bool
  }))

  default = {
    core = {
      description      = "Module boundaries, inputs, outputs, and local composition."
      topics           = ["module inputs", "module outputs", "composition"]
      checkpoint_count = 3
      required         = true
    }

    practice = {
      description      = "Practice extending a module call without changing the module internals."
      topics           = ["type constraints", "validation"]
      checkpoint_count = 2
      required         = false
    }
  }

  validation {
    condition     = length(var.learning_tracks) > 0
    error_message = "learning_tracks must contain at least one track."
  }

  validation {
    condition = alltrue([
      for track_name in keys(var.learning_tracks) :
      can(regex("^[a-z][a-z0-9_-]*$", track_name))
    ])
    error_message = "learning_tracks keys must start with a lower-case letter and use only lower-case letters, numbers, underscores, or hyphens."
  }

  validation {
    condition = alltrue([
      for config in values(var.learning_tracks) :
      length(trimspace(config.description)) > 0
    ])
    error_message = "each learning track description must not be empty."
  }

  validation {
    condition = alltrue([
      for config in values(var.learning_tracks) :
      length(config.topics) > 0 && alltrue([for topic in config.topics : length(trimspace(topic)) > 0])
    ])
    error_message = "each learning track must include at least one non-empty topic."
  }

  validation {
    condition = alltrue([
      for config in values(var.learning_tracks) :
      config.checkpoint_count >= 1
      && config.checkpoint_count <= 5
      && floor(config.checkpoint_count) == config.checkpoint_count
    ])
    error_message = "each learning track checkpoint_count must be a whole number from 1 to 5."
  }
}
