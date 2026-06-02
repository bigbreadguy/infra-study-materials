variable "workspace_name" {
  description = "Human-readable name for this local Terraform study workspace."
  type        = string
  default     = "Terraform Basics Workshop"

  validation {
    condition     = length(trimspace(var.workspace_name)) >= 3
    error_message = "workspace_name must contain at least three non-space characters."
  }
}

variable "environment" {
  description = "Practice environment label. This is only metadata in lesson 01."
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

variable "topic_tags" {
  description = "Topics to model with for_each so stable instance keys are visible."
  type        = set(string)
  default     = ["hcl", "state", "dependencies", "outputs"]

  validation {
    condition     = length(var.topic_tags) > 0
    error_message = "topic_tags must contain at least one topic."
  }

  validation {
    condition     = alltrue([for tag in var.topic_tags : length(trimspace(tag)) > 0])
    error_message = "topic_tags must not contain empty strings."
  }
}

variable "checkpoint_count" {
  description = "Number of count-based checkpoint markers to create."
  type        = number
  default     = 3

  validation {
    condition     = var.checkpoint_count >= 1 && var.checkpoint_count <= 5 && floor(var.checkpoint_count) == var.checkpoint_count
    error_message = "checkpoint_count must be a whole number from 1 to 5."
  }
}

variable "create_summary_file" {
  description = "Whether Terraform should manage a generated local summary file."
  type        = bool
  default     = true
}
