run "default_plan_shape" {
  command = plan

  assert {
    condition     = output.summary_file_path != null
    error_message = "The default lesson should plan a generated summary file."
  }

  assert {
    condition     = keys(terraform_data.topic) == ["dependencies", "hcl", "outputs", "state"]
    error_message = "The default lesson should include four for_each topic markers."
  }

  assert {
    condition     = length(terraform_data.checkpoint) == 3
    error_message = "The default lesson should include three count-based checkpoints."
  }
}

run "custom_plan_shape" {
  command = plan

  variables {
    workspace_name      = "Custom Workshop"
    environment         = "sandbox"
    owner               = "learner"
    topic_tags          = ["hcl", "state"]
    checkpoint_count    = 2
    create_summary_file = false
  }

  assert {
    condition     = output.summary_file_path == null
    error_message = "Disabling create_summary_file should remove the managed local file."
  }

  assert {
    condition     = keys(terraform_data.topic) == ["hcl", "state"]
    error_message = "Custom topic tags should produce stable for_each keys and sorted order."
  }

  assert {
    condition     = length(terraform_data.checkpoint) == 2
    error_message = "Custom checkpoint_count should control the number of count instances."
  }
}
