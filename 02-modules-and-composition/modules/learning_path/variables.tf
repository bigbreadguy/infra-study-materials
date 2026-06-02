variable "track_name" {
  description = "Stable track key supplied by the root module for this module instance."
  type        = string

  validation {
    condition     = can(regex("^[a-z][a-z0-9_-]*$", var.track_name))
    error_message = "track_name must start with a lower-case letter and use only lower-case letters, numbers, underscores, or hyphens."
  }
}

variable "description" {
  description = "Short explanation of what this learning path covers."
  type        = string

  validation {
    condition     = length(trimspace(var.description)) > 0
    error_message = "description must not be empty."
  }
}

variable "topics" {
  description = "Topic names modeled as stable for_each keys inside the module."
  type        = set(string)

  validation {
    condition     = length(var.topics) > 0
    error_message = "topics must contain at least one topic."
  }

  validation {
    condition     = alltrue([for topic in var.topics : length(trimspace(topic)) > 0])
    error_message = "topics must not contain empty strings."
  }
}

variable "checkpoint_count" {
  description = "Number of checkpoints this module should model."
  type        = number

  validation {
    condition     = var.checkpoint_count >= 1 && var.checkpoint_count <= 5 && floor(var.checkpoint_count) == var.checkpoint_count
    error_message = "checkpoint_count must be a whole number from 1 to 5."
  }
}

variable "required" {
  description = "Whether this track is required in the composed workshop path."
  type        = bool
}

variable "labels" {
  description = "Shared labels supplied by the root module or another child module."
  type        = map(string)

  validation {
    condition = alltrue([
      for key, value in var.labels :
      can(regex("^[a-z][a-z0-9_-]*$", key)) && length(trimspace(value)) > 0
    ])
    error_message = "labels keys must be lower snake/kebab style and values must not be empty."
  }
}

variable "name_prefix" {
  description = "Normalized name prefix supplied by the label module."
  type        = string

  validation {
    condition     = length(trimspace(var.name_prefix)) > 0
    error_message = "name_prefix must not be empty."
  }
}
